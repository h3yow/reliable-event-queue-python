"""失败重试、退避、隔离区、成功结果不可变。"""
from __future__ import annotations

import logging

from app.models import Event, EventStatus, FatalError, RetryableError


def _drain(service, handler, clock, max_rounds=50):
    """配合可推进的假时钟排空队列：退避到期后立刻可再次领取。"""
    for _ in range(max_rounds):
        row = service.storage._conn.execute(
            "SELECT MIN(next_visible_at) AS m FROM events WHERE status='pending'"
        ).fetchone()
        if row["m"] is not None:
            clock[0] = max(clock[0], row["m"])
        events = service.poll()
        for event in events:
            service.handle_one(event, handler)
        if service.storage.first_non_terminal_offset() is None:
            return
    raise AssertionError("队列在限定轮次内未排空")


def test_retryable_error_retried_with_backoff_then_success(service, clock):
    service.receive("evt-retry", "p")
    calls = {"n": 0}

    def flaky(event: Event) -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RetryableError("temporary failure")
        return "recovered"

    _drain(service, flaky, clock)
    event = service.storage.get_event(1)
    assert event.status is EventStatus.SUCCEEDED
    assert event.attempts == 3
    assert event.result == "recovered"
    assert event.last_error is None


def test_retry_exhausted_enters_dead_letter_with_reason(service, clock):
    service.receive("evt-dead", "p")

    def always_fail(event: Event) -> str:
        raise RetryableError("always down")

    _drain(service, always_fail, clock)
    event = service.storage.get_event(1)
    assert event.status is EventStatus.DEAD_LETTER
    assert event.attempts == 3

    dead = service.list_dead_letters()
    assert len(dead) == 1
    entry = dead[0]
    assert entry.event_id == "evt-dead" and entry.offset == 1
    assert entry.attempts == 3
    assert "达到上限" in entry.reason and "always down" in entry.last_error


def test_fatal_error_goes_straight_to_dead_letter(service):
    service.receive("evt-fatal", "p")

    def boom(event: Event) -> str:
        raise FatalError("poison message")

    service.process_batch(boom)
    event = service.storage.get_event(1)
    assert event.status is EventStatus.DEAD_LETTER
    assert event.attempts == 1  # 不重试
    dead = service.list_dead_letters()
    assert len(dead) == 1
    assert "不可重试错误" in dead[0].reason


def test_successful_event_never_mutated_by_reprocess(service):
    service.receive("evt-ok", "p")
    service.process_batch(lambda e: "first-result")
    event = service.storage.get_event(1)
    assert event.status is EventStatus.SUCCEEDED and event.result == "first-result"

    # 重新处理同一事件对象：终态保护直接跳过，handler 不会被调用
    def must_not_run(event):
        raise AssertionError('已成功事件不得被再次处理')
    status = service.handle_one(event, must_not_run)
    assert status is EventStatus.SUCCEEDED
    assert service.poll() == []
    again = service.storage.get_event(1)
    assert again.result == "first-result"
    assert again.status is EventStatus.SUCCEEDED

    # 重放一个已成功位点也必须无效
    assert service.replay_dead(1) is False
    assert service.storage.get_event(1).result == "first-result"


def test_replay_dead_event_can_succeed(service):
    service.receive("evt-replay", "p")
    service.process_batch(lambda e: (_ for _ in ()).throw(FatalError("bad")))
    assert service.storage.get_event(1).status is EventStatus.DEAD_LETTER

    assert service.replay_dead(1) is True
    service.process_batch(lambda e: "now-ok")
    assert service.storage.get_event(1).status is EventStatus.SUCCEEDED
    # 隔离区记录被标记为已重放，默认查询不再出现
    assert service.list_dead_letters() == []
    assert len(service.list_dead_letters(include_replayed=True)) == 1


def test_logs_include_event_id_offset_and_attempts(service, caplog):
    service.receive("evt-log", "p")

    def fail_once(event: Event) -> str:
        raise RetryableError("need-retry")

    with caplog.at_level(logging.WARNING, logger="reliable_event_queue"):
        service.process_batch(fail_once)

    messages = [r.getMessage() for r in caplog.records]
    retry_log = [m for m in messages if "将重试" in m]
    assert retry_log, messages
    line = retry_log[0]
    assert "event_id=evt-log" in line
    assert "位点=1" in line
    assert "重试次数=1" in line


def test_replay_after_retry_exhaustion_gets_fresh_budget(service, clock):
    service.receive("evt-exhausted", "p")
    calls = {"n": 0}

    def fail_always(event: Event) -> str:
        raise RetryableError("down")

    _drain(service, fail_always, clock)
    assert service.storage.get_event(1).status is EventStatus.DEAD_LETTER
    assert service.storage.get_event(1).attempts == 3

    assert service.replay_dead(1) is True
    requeued = service.storage.get_event(1)
    assert requeued.status is EventStatus.PENDING
    assert requeued.attempts == 0  # 重放获得完整重试预算
    assert requeued.last_error is None

    calls2 = {"n": 0}

    def recover(event: Event) -> str:
        calls2["n"] += 1
        if calls2["n"] < 2:
            raise RetryableError("still wobbly")
        return "finally-ok"

    _drain(service, recover, clock)
    event = service.storage.get_event(1)
    assert event.status is EventStatus.SUCCEEDED
    assert event.attempts == 2
    assert event.result == "finally-ok"
    # 历史隔离记录仍可追溯
    history = service.list_dead_letters(include_replayed=True)
    assert len(history) == 1 and history[0].attempts == 3
