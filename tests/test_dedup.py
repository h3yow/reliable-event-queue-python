"""重复投递、幂等接收与去重窗口测试。"""

from __future__ import annotations

from app.models import DuplicateReason, EventStatus
from app.service import ReliableQueue, RetryPolicy


def test_duplicate_delivery_while_pending(queue):
    r1 = queue.receive("order-1", "order.created", '{"v":1}')
    assert r1.accepted and r1.duplicate is False
    assert r1.reason is DuplicateReason.NONE

    r2 = queue.receive("order-1", "order.created", '{"v":1}')
    assert r2.accepted is False
    assert r2.duplicate is True
    assert r2.reason is DuplicateReason.INFLIGHT_OR_RETRYING
    assert r2.event.id == r1.event.id  # 指向同一条事件
    assert len(queue.list_events()) == 1


def test_different_events_not_confused(queue):
    """不同业务键不得互相覆盖或误判为重复。"""
    a = queue.receive("key-a", "t", "a")
    b = queue.receive("key-b", "t", "b")
    c = queue.receive("key-c", "t", "c")
    assert a.accepted and b.accepted and c.accepted
    assert {e.key for e in queue.list_events()} == {"key-a", "key-b", "key-c"}
    assert [e.offset for e in queue.list_events()] == [1, 2, 3]
    assert a.event.id != b.event.id != c.event.id


def test_duplicate_after_success_is_idempotent(queue):
    queue.receive("order-2", "t", "p")
    calls = []

    def handler(event):
        calls.append(event.key)
        return "RESULT-42"

    outcomes = queue.process_once(handler, limit=10)
    assert len(outcomes) == 1
    assert outcomes[0].status is EventStatus.SUCCEEDED

    # 再次投递同一事件：永久幂等
    dup = queue.receive("order-2", "t", "p")
    assert dup.duplicate is True
    assert dup.reason is DuplicateReason.ALREADY_SUCCEEDED

    # 重复投递不会触发再次处理，已成功结果保持不变
    queue.process_once(handler, limit=10)
    assert calls == ["order-2"]
    stored = queue.storage.get_event_by_key("order-2")
    assert stored.result == "RESULT-42"
    assert stored.status is EventStatus.SUCCEEDED


def test_dedup_window_expiry_treats_as_new(db_path):
    """去重窗口过期后，相同标识按新事件处理。"""
    t = {"now": 1000.0}
    q = ReliableQueue(
        db_path,
        retry_policy=RetryPolicy(max_attempts=1),
        dedup_window=300.0,
        clock=lambda: t["now"],
    )
    q.start()
    first = q.receive("periodic-key", "t", "v1")
    assert first.accepted

    # 窗口内：重复
    t["now"] = 1299.0
    dup = q.receive("periodic-key", "t", "v1")
    assert dup.duplicate is True

    # 窗口过期：作为新事件接收，位点与记录均独立
    t["now"] = 1301.0
    second = q.receive("periodic-key", "t", "v2")
    assert second.accepted and second.duplicate is False
    assert second.reason is DuplicateReason.NONE
    assert second.event.id != first.event.id
    assert second.event.offset == 2
    q.close()


def test_duplicate_reason_is_explainable(queue):
    """重复判定需给出可解释的原因（消息中包含键与状态）。"""
    queue.receive("k", "t", "p")
    r = queue.receive("k", "t", "p")
    assert r.reason.value == "INFLIGHT_OR_RETRYING"
    assert "k" in r.message and "重复" in r.message
