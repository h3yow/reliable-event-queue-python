"""并发消费下的状态一致性测试。"""

from __future__ import annotations

import threading
from collections import Counter

from app.models import EventStatus
from app.service import ReliableQueue, RetryPolicy


def test_concurrent_consumers_no_duplicate_effect(db_path):
    """多个消费者并行处理：每条事件恰好生效一次，无丢失。"""
    q = ReliableQueue(db_path)
    q.start()
    n = 60
    for i in range(n):
        q.receive(f"evt-{i:03d}", "t", str(i))

    processed: list[str] = []
    lock = threading.Lock()

    def handler(event):
        with lock:
            processed.append(event.key)
        return f"ok-{event.key}"

    errors: list[Exception] = []

    def worker():
        try:
            while True:
                outcomes = q.process_once(handler, limit=5)
                if not outcomes:
                    break
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    # 无重复生效
    counter = Counter(processed)
    assert len(processed) == n
    assert all(v == 1 for v in counter.values()), counter
    # 无丢失：全部成功，位点推进到 n
    events = q.list_events(limit=n)
    assert all(e.status is EventStatus.SUCCEEDED for e in events)
    assert q.get_cursor().value == n
    q.close()


def test_concurrent_receive_same_key_deduped(db_path):
    """并发接收同一业务键：只有一条真正生效。"""
    q = ReliableQueue(db_path)
    q.start()
    results = []
    lock = threading.Lock()

    def receive():
        r = q.receive("hot-key", "t", "p")
        with lock:
            results.append(r)

    threads = [threading.Thread(target=receive) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if r.accepted]
    duplicates = [r for r in results if r.duplicate]
    assert len(accepted) == 1
    assert len(duplicates) == 9
    # 所有重复投递都指向同一个事件
    assert all(r.event.id == accepted[0].event.id for r in duplicates)
    assert len(q.list_events()) == 1
    q.close()


def test_concurrent_failures_still_retry_once_each(db_path):
    """并发下失败重试不重复计数、不丢失：最终全部成功。"""
    q = ReliableQueue(
        db_path,
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.0),
    )
    q.start()
    n = 30
    for i in range(n):
        q.receive(f"r-{i}", "t", "")

    flaky = Counter()

    def handler(event):
        flaky[event.key] += 1
        if flaky[event.key] == 1:
            from app.errors import RetryableError

            raise RetryableError("first fails")
        return "ok"

    def worker():
        for _ in range(20):
            if not q.process_once(handler, limit=3):
                pass

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    events = q.list_events(limit=n)
    assert all(e.status is EventStatus.SUCCEEDED for e in events)
    # 每个键恰好两次处理尝试（第一次失败 + 第二次成功）
    assert all(flaky[f"r-{i}"] == 2 for i in range(n))
    assert q.list_dead_letters() == []
    q.close()
