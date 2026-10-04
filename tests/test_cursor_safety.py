"""位点提交安全约束：未完成事件阻挡、阻挡明细、幂等提交。"""

from __future__ import annotations

import pytest

from app.errors import (
    CursorBlockedError,
    CursorRegressionError,
    NonRetryableError,
    RetryableError,
)
from app.models import EventStatus
from app.service import RetryPolicy


def test_commit_blocked_by_pending_event(queue):
    """目标位置之前有待处理事件：拒绝并说明被谁挡住。"""
    queue.receive("e1", "t", "")
    queue.receive("e2", "t", "")
    queue.process_once(lambda e: "ok", limit=1)  # 只有 e1 成功

    with pytest.raises(CursorBlockedError) as exc:
        queue.commit_cursor(2)
    assert "拒绝位点提交" in str(exc.value)
    assert "e2" in str(exc.value)
    assert "PENDING" in str(exc.value)
    # 阻挡明细可编程读取
    assert exc.value.blockers == [(2, "e2", "PENDING")]
    # 位点保持不变
    assert queue.get_cursor().value == 1


def test_commit_blocked_by_processing_event(queue):
    """正在处理中的事件同样阻挡位点提交。"""
    queue.receive("p1", "t", "")
    queue.storage.claim_due(
        queue._clock(), 1,
        session_id=queue.session_id,
        lease_duration=queue.processing_timeout,
    )
    with pytest.raises(CursorBlockedError) as exc:
        queue.commit_cursor(1)
    assert exc.value.blockers[0][2] == "PROCESSING"


def test_commit_blocked_by_retrying_and_dead_events(queue):
    """重试中与隔离区事件都算未到终态，都会阻挡。"""
    queue.retry_policy = RetryPolicy(max_attempts=2, base_delay=100.0)
    queue.receive("r1", "t", "")
    queue.receive("d1", "t", "")
    # r1 -> FAILED（重试中）
    queue.process_once(
        lambda e: (_ for _ in ()).throw(RetryableError("x")), limit=1
    )
    # d1 -> DEAD（隔离区）
    queue.process_once(
        lambda e: (_ for _ in ()).throw(NonRetryableError("bad")), limit=1
    )
    assert queue.storage.get_event_by_key("r1").status is EventStatus.FAILED
    assert queue.storage.get_event_by_key("d1").status is EventStatus.DEAD

    with pytest.raises(CursorBlockedError) as exc:
        queue.commit_cursor(2)
    statuses = {b[2] for b in exc.value.blockers}
    assert statuses == {"FAILED", "DEAD"}
    assert queue.get_cursor().value == 0


def test_commit_allowed_when_all_preceding_succeeded(queue):
    """前面事件全部确认成功后允许提交。"""
    queue.receive("a", "t", "")
    queue.receive("b", "t", "")
    queue.receive("c", "t", "")
    queue.process_once(lambda e: "ok", limit=3)
    assert queue.get_cursor().value == 3

    # 后续新事件尚未处理，不影响已成功区段的提交语义
    queue.receive("d", "t", "")
    state = queue.commit_cursor(3)
    assert state.value == 3
    # 但试图越过未处理的 d 会被拒绝
    with pytest.raises(CursorBlockedError):
        queue.commit_cursor(4)


def test_commit_same_position_is_idempotent(queue):
    """重复提交当前位点：幂等成功，不报错不变更。"""
    queue.receive("i1", "t", "")
    queue.process_once(lambda e: "ok")
    assert queue.get_cursor().value == 1

    # 此时来了一条新的未处理事件；重复提交旧位点仍然幂等成功
    queue.receive("i2", "t", "")
    state = queue.commit_cursor(1)
    assert state.value == 1
    state = queue.commit_cursor(1)
    assert state.value == 1
    assert queue.get_cursor().value == 1


def test_commit_regression_still_rejected(queue):
    """回退拒绝行为保持不变。"""
    queue.receive("g1", "t", "")
    queue.process_once(lambda e: "ok")
    with pytest.raises(CursorRegressionError):
        queue.commit_cursor(0)
    assert queue.get_cursor().value == 1


def test_commit_unblocked_after_stragglers_finish(queue):
    """被挡住的事件处理成功后，同一位置即可提交。"""
    queue.receive("s1", "t", "")
    queue.receive("s2", "t", "")
    queue.process_once(lambda e: "ok", limit=1)

    with pytest.raises(CursorBlockedError):
        queue.commit_cursor(2)
    # 把剩下的处理完，再提交同一位置成功
    queue.process_once(lambda e: "ok", limit=1)
    state = queue.commit_cursor(2)
    assert state.value == 2
