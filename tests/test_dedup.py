"""重复投递 / 去重窗口 / 不同事件互不干扰。"""
from __future__ import annotations

from app.models import ReceiveDecision


def test_first_delivery_accepted(service):
    r = service.receive("evt-1", "payload-1")
    assert r.decision is ReceiveDecision.ACCEPTED
    assert r.offset == 1
    assert "首次投递" in r.reason


def test_duplicate_within_window_applied_once(service):
    r1 = service.receive("evt-1", "payload-1")
    r2 = service.receive("evt-1", "payload-1")
    r3 = service.receive("evt-1", "payload-1")
    assert r1.offset == r2.offset == r3.offset == 1
    assert r2.decision is ReceiveDecision.DUPLICATE_IN_WINDOW
    assert r3.decision is ReceiveDecision.DUPLICATE_IN_WINDOW
    # 判定原因可解释：包含窗口与原位点信息
    assert "去重窗口内" in r2.reason and "位点 1" in r2.reason
    # 只生效一次：存储中只有一条事件
    assert service.storage.get_event(1).payload == "payload-1"
    assert service._max_offset() == 1


def test_different_event_ids_never_collide(service):
    a = service.receive("evt-A", "a")
    b = service.receive("evt-B", "b")
    a2 = service.receive("evt-A", "a")
    assert a.offset == 1 and b.offset == 2
    assert a2.decision is ReceiveDecision.DUPLICATE_IN_WINDOW and a2.offset == 1
    assert service.storage.get_event(2).event_id == "evt-B"


def test_duplicate_payload_does_not_overwrite(service):
    service.receive("evt-1", "original")
    service.receive("evt-1", "resend-different-body")
    event = service.storage.get_event(1)
    assert event.payload == "original"  # 重复投递不得覆盖


def test_same_id_after_window_is_new_event(service, clock):
    r1 = service.receive("evt-1", "v1")
    clock[0] += 5  # 窗口内
    assert service.receive("evt-1", "v1").decision is ReceiveDecision.DUPLICATE_IN_WINDOW
    clock[0] += 6  # 距首次 11s > 10s 窗口
    r2 = service.receive("evt-1", "v2")
    assert r2.decision is ReceiveDecision.NOT_DUPLICATE_WINDOW_EXPIRED
    assert r2.offset == 2
    assert "超过去重窗口" in r2.reason
    # 旧事件结果不受影响，新事件独立
    assert service.storage.get_event(1).payload == "v1"
    assert service.storage.get_event(2).payload == "v2"


def test_concurrent_duplicate_deliveries_enqueued_once(db_path, clock):
    """并发重复投递同一 event_id：只能生效一次。"""
    import threading

    from app.models import QueueConfig
    from app.service import EventQueueService

    svc = EventQueueService(str(db_path), QueueConfig(), clock=lambda: clock[0])
    decisions = []
    barrier = threading.Barrier(8)

    def deliver() -> None:
        barrier.wait()
        decisions.append(svc.receive("hot", "p").decision)

    threads = [threading.Thread(target=deliver) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    accepted = [d for d in decisions if d is ReceiveDecision.ACCEPTED]
    duplicate = [d for d in decisions if d is ReceiveDecision.DUPLICATE_IN_WINDOW]
    assert len(accepted) == 1
    assert len(duplicate) == 7
    assert svc._max_offset() == 1
    svc.close()
