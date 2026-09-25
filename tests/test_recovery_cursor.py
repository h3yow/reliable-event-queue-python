"""进程重启恢复与位点语义测试。"""

from __future__ import annotations

import pytest

from app.errors import (
    CursorOutOfRangeError,
    CursorRegressionError,
    RetryableError,
)
from app.models import DLQStatus, EventStatus
from app.service import ReliableQueue, RetryPolicy


def test_cursor_and_dlq_survive_restart(db_path):
    """位点与隔离区状态从持久化文件恢复。"""
    policy = RetryPolicy(max_attempts=2, base_delay=0.0)
    q = ReliableQueue(db_path, retry_policy=policy)
    q.start()
    q.receive("a", "t", "")
    q.receive("b", "t", "")
    q.process_once(lambda e: "ok", limit=10)
    q.commit_cursor(2)

    q.receive("bad", "t", "")
    q.process_once(lambda e: (_ for _ in ()).throw(RetryableError("x")))
    q.process_once(lambda e: (_ for _ in ()).throw(RetryableError("x")))
    assert q.list_dead_letters()[0].key == "bad"
    q.close()

    # 模拟进程重启
    q2 = ReliableQueue(db_path, retry_policy=policy)
    q2.start()
    assert q2.get_cursor().value == 2  # 位点恢复
    dl = q2.list_dead_letters()
    assert len(dl) == 1 and dl[0].key == "bad"
    assert dl[0].status is DLQStatus.ACTIVE
    events = {e.key: e for e in q2.list_events()}
    assert events["a"].status is EventStatus.SUCCEEDED
    assert events["b"].status is EventStatus.SUCCEEDED
    assert events["bad"].status is EventStatus.DEAD
    q2.close()


def test_processing_event_recovered_after_restart(db_path):
    """处理中途崩溃（停在 PROCESSING）：重启后回收并可重新处理。"""
    clock = {"t": 1000.0}
    policy = RetryPolicy(max_attempts=3, base_delay=0.0)
    q = ReliableQueue(
        db_path, retry_policy=policy, processing_timeout=0.05,
        clock=lambda: clock["t"],
    )
    q.start()
    q.receive("crash", "t", "")
    claimed = q.storage.claim_due(clock["t"], 1)
    assert len(claimed) == 1
    # 未标记任何结果直接“崩溃”
    q.close()

    # 重启时已超过认领超时
    clock["t"] += 1.0
    q2 = ReliableQueue(
        db_path, retry_policy=policy, processing_timeout=0.05,
        clock=lambda: clock["t"],
    )
    q2.start()
    events = q2.list_events()
    # 超时回收为 FAILED（attempts 在认领时已计 1）
    assert events[0].status is EventStatus.FAILED
    assert events[0].attempts == 1

    outcomes = q2.process_once(lambda e: "recovered")
    assert outcomes[0].status is EventStatus.SUCCEEDED
    assert outcomes[0].result == "recovered"
    assert q2.get_cursor().value == 1
    q2.close()


def test_cursor_regression_rejected_with_reason(queue):
    queue.receive("c1", "t", "")
    queue.process_once(lambda e: "ok")
    assert queue.get_cursor().value == 1

    with pytest.raises(CursorRegressionError) as exc:
        queue.commit_cursor(0)
    assert "拒绝位点回退" in str(exc.value)
    assert "1" in str(exc.value) and "0" in str(exc.value)
    # 位点保持不变
    assert queue.get_cursor().value == 1

    # 相等提交允许（幂等提交）
    queue.commit_cursor(1)
    assert queue.get_cursor().value == 1


def test_cursor_jump_out_of_range_rejected(queue):
    queue.receive("c1", "t", "")
    with pytest.raises(CursorOutOfRangeError):
        queue.commit_cursor(5)
    assert queue.get_cursor().value == 0


def test_cursor_auto_advances_in_offset_order(queue):
    """位点按偏移顺序连续推进；前序未成功时不跳过。"""
    queue.receive("first", "t", "")
    queue.receive("second", "t", "")

    # 只让 first 成功 -> cursor=1
    got = queue.process_once(lambda e: "ok", limit=1)
    assert got[0].key == "first"
    assert queue.get_cursor().value == 1

    # second 成功 -> cursor=2
    queue.process_once(lambda e: "ok", limit=1)
    assert queue.get_cursor().value == 2
