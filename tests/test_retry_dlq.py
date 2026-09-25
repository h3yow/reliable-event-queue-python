"""失败重试、退避策略、隔离区与重放测试。"""

from __future__ import annotations

import pytest

from app.errors import NonRetryableError, RetryableError
from app.models import DLQStatus, EventStatus
from app.service import RetryPolicy


def test_retry_then_success(queue):
    queue.receive("flaky", "t", "p")
    attempts = {"n": 0}

    def handler(event):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RetryableError("临时故障")
        return "eventually-ok"

    o1 = queue.process_once(handler)[0]
    assert o1.status is EventStatus.FAILED and o1.requeued
    assert o1.attempts == 1 and o1.error == "临时故障"

    o2 = queue.process_once(handler)[0]
    assert o2.attempts == 2 and o2.status is EventStatus.FAILED

    o3 = queue.process_once(handler)[0]
    assert o3.attempts == 3 and o3.status is EventStatus.SUCCEEDED
    assert o3.result == "eventually-ok"
    assert queue.list_dead_letters() == []


def test_exhausting_retries_moves_to_dlq(queue):
    queue.receive("doomed", "t", "p")

    def handler(event):
        raise RetryableError("一直失败")

    outcomes = [queue.process_once(handler)[0] for _ in range(3)]
    assert [o.attempts for o in outcomes] == [1, 2, 3]
    assert outcomes[0].status is EventStatus.FAILED
    assert outcomes[-1].status is EventStatus.DEAD and outcomes[-1].dead

    dl = queue.list_dead_letters()
    assert len(dl) == 1
    assert dl[0].key == "doomed"
    assert dl[0].attempts == 3
    assert dl[0].last_error == "一直失败"
    assert dl[0].status is DLQStatus.ACTIVE

    # 进入隔离区后不再被普通消费取出
    assert queue.process_once(handler) == []


def test_non_retryable_error_goes_directly_to_dlq(queue):
    queue.receive("poison", "t", "p")

    def handler(event):
        raise NonRetryableError("数据格式非法")

    o = queue.process_once(handler)[0]
    assert o.status is EventStatus.DEAD and o.attempts == 1
    dl = queue.list_dead_letters()
    assert len(dl) == 1 and dl[0].key == "poison"


def test_backoff_delay_policy():
    policy = RetryPolicy(base_delay=0.1, backoff=2.0, max_delay=1.0)
    assert policy.delay_for(1) == pytest.approx(0.1)
    assert policy.delay_for(2) == pytest.approx(0.2)
    assert policy.delay_for(3) == pytest.approx(0.4)
    assert policy.delay_for(10) == pytest.approx(1.0)  # 封顶


def test_failed_event_waits_for_backoff(queue):
    """退避未到期的事件不得被提前认领。"""
    import app.service as svc

    t = {"now": 100.0}
    queue._clock = lambda: t["now"]
    queue.retry_policy = RetryPolicy(
        max_attempts=3, base_delay=10.0, backoff=1.0
    )
    queue.receive("backoff-k", "t", "p")

    def handler(event):
        raise RetryableError("x")

    assert queue.process_once(handler)[0].status is EventStatus.FAILED
    assert queue.process_once(handler) == []  # 10s 退避内
    t["now"] = 111.0
    assert len(queue.process_once(handler)) == 1  # 退避到期可重试


def test_replay_dead_letter_success_resolves(queue):
    queue.receive("recoverable", "t", "p")

    def bad(event):
        raise RetryableError("fail")

    for _ in range(3):
        queue.process_once(bad)
    target = queue.list_dead_letters()[0]

    def good(event):
        return "fixed"

    outcome = queue.replay_dead_letter(target.event_id, good)
    assert outcome.status is EventStatus.SUCCEEDED
    assert outcome.result == "fixed"

    record = queue.list_dead_letters(active_only=False)[0]
    assert record.status is DLQStatus.RESOLVED
    assert queue.storage.get_event(target.event_id).status is EventStatus.SUCCEEDED


def test_replay_failure_keeps_dead(queue):
    queue.receive("still-bad", "t", "p")

    def bad(event):
        raise RetryableError("fail")

    for _ in range(3):
        queue.process_once(bad)
    target = queue.list_dead_letters()[0]

    outcome = queue.replay_dead_letter(target.event_id, bad)
    assert outcome.status is EventStatus.DEAD
    assert queue.list_dead_letters()[0].status is DLQStatus.ACTIVE


def test_reprocessing_success_does_not_change_result(queue):
    """重放/重复处理不得改变已成功事件的结果。"""
    queue.receive("immutable", "t", "p")
    queue.process_once(lambda e: "first-result")

    event_id = queue.storage.get_event_by_key("immutable").id
    # 已成功事件不允许按隔离区重放
    with pytest.raises(ValueError):
        queue.replay_dead_letter(event_id, lambda e: "second-result")
    assert queue.storage.get_event(event_id).result == "first-result"
