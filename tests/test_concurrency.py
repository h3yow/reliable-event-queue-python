"""并发消费：多消费者并行时不重复生效、不丢事件。"""
from __future__ import annotations

import threading
import time

from app.consumer import ConsumerPool
from app.models import Event, EventStatus, RetryableError


def test_parallel_consumers_no_duplicate_no_loss(real_service):
    service = real_service
    n = 120
    for i in range(n):
        service.receive(f"evt-{i:03d}", str(i))

    processed: list[int] = []
    lock = threading.Lock()

    def handler(event: Event) -> str:
        with lock:
            processed.append(event.offset)
        time.sleep(0.001)  # 制造交错，放大竞争
        return f"done-{event.offset}"

    pool = ConsumerPool(service, handler, workers=8, batch_size=16, poll_interval=0.005)
    total = pool.run_until_drained(max_wait=5.0)

    assert total == n
    assert sorted(processed) == list(range(1, n + 1))
    assert len(set(processed)) == n  # 无重复生效

    for offset in range(1, n + 1):
        event = service.storage.get_event(offset)
        assert event.status is EventStatus.SUCCEEDED
        assert event.attempts == 1
        assert event.result == f"done-{offset}"
    assert service.storage.first_non_terminal_offset() is None
    assert service.auto_advance() == n  # 位点连续推进到 n


def test_parallel_consumers_with_failures(real_service):
    service = real_service
    """并发下部分事件失败重试：最终全部终态，退避期间事件仍只被单线程处理。"""
    n = 40
    for i in range(n):
        service.receive(f"evt-{i:03d}", str(i))

    attempts: dict[str, int] = {}
    attempts_lock = threading.Lock()

    def handler(event: Event) -> str:
        with attempts_lock:
            attempts[event.event_id] = attempts.get(event.event_id, 0) + 1
            seen = attempts[event.event_id]
        time.sleep(0.0005)
        # 偶数事件失败两次后成功
        if int(event.event_id.split("-")[1]) % 2 == 0 and seen < 3:
            raise RetryableError("transient")
        return "ok"

    pool = ConsumerPool(service, handler, workers=6, batch_size=8, poll_interval=0.002)
    pool.run_until_drained(max_wait=8.0)

    for i in range(n):
        event = service.storage.get_event(i + 1)
        assert event.status is EventStatus.SUCCEEDED, (i, event.status)
        if i % 2 == 0:
            assert attempts[event.event_id] == 3
            assert event.attempts == 3
        else:
            assert event.attempts == 1
    assert service.auto_advance() == n


def test_concurrent_receive_and_consume(real_service):
    service = real_service
    """接收与消费同时进行：不丢事件、位点最终连续。"""
    received = 0
    recv_lock = threading.Lock()

    def handler(event: Event) -> str:
        time.sleep(0.0002)
        return "ok"

    pool = ConsumerPool(service, handler, workers=4, batch_size=8, poll_interval=0.001)
    pool.start()

    def producer() -> None:
        nonlocal received
        for i in range(60):
            service.receive(f"evt-{i:03d}", str(i))
            with recv_lock:
                received += 1
            time.sleep(0.0005)

    t = threading.Thread(target=producer)
    t.start()
    t.join()
    pool.stop()
    # 排空剩余
    service.process_batch(handler)
    # 再循环几轮（均一次成功，所以一轮即可）
    assert service.storage.first_non_terminal_offset() is None
    assert received == 60
    assert service.auto_advance() == 60
