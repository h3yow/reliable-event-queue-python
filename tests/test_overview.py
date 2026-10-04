"""运行概览：状态计数、积压、最老待处理、位点差距、会话持有明细与一致快照。"""

from __future__ import annotations

import threading

from app.errors import RetryableError
from app.models import EventStatus, SessionStatus
from app.service import ReliableQueue, RetryPolicy


def test_overview_counts_backlog_and_cursor_gap(queue):
    """各状态数量、积压与位点差距对得上明细。"""
    queue.receive("ok-1", "t", "")
    queue.receive("ok-2", "t", "")
    queue.receive("retry-1", "t", "")
    queue.receive("dead-1", "t", "")
    queue.receive("pending-1", "t", "")

    # 退避拉长，保证失败事件在测试期间不会被重新认领
    queue.retry_policy = RetryPolicy(max_attempts=3, base_delay=100.0)
    # offset 1,2 成功
    queue.process_once(lambda e: "ok", limit=2)
    # offset 3 失败进入重试
    queue.process_once(
        lambda e: (_ for _ in ()).throw(RetryableError("boom")), limit=1
    )
    # offset 4 用不可重试错误进隔离区；offset 5 保持待处理
    from app.errors import NonRetryableError

    queue.process_once(
        lambda e: (_ for _ in ()).throw(NonRetryableError("bad")), limit=1
    )

    ov = queue.overview()
    assert ov.counts["SUCCEEDED"] == 2
    assert ov.counts["PENDING"] == 1
    assert ov.counts["FAILED"] == 1
    assert ov.counts["DEAD"] == 1
    assert ov.counts["PROCESSING"] == 0
    # 积压 = 待处理 + 重试中
    assert ov.backlog == 2
    # 位点：前两条成功 -> cursor=2，最新接收 max_offset=5
    assert ov.cursor == 2
    assert ov.max_offset == 5
    assert ov.cursor_gap == 3
    # 数字与明细对得上
    events = queue.list_events()
    for status, count in ov.counts.items():
        actual = sum(1 for e in events if e.status.value == status)
        assert actual == count, status


def test_overview_oldest_pending_age(queue):
    """最老一条待处理事件的等待时长。"""
    t = {"now": 1000.0}
    queue._clock = lambda: t["now"]
    queue.receive("old", "t", "")
    t["now"] = 1005.0
    queue.receive("new", "t", "")

    t["now"] = 1012.0
    ov = queue.overview()
    assert ov.oldest_pending_age == 12.0  # 最老的是 1000.0 接收的那条

    queue.process_once(lambda e: "ok", limit=2)
    ov = queue.overview()
    assert ov.oldest_pending_age is None  # 没有待处理事件


def test_overview_shows_session_held_events_and_lease(queue):
    """概览中能看到每个会话占着哪些事件以及租约剩余时间。"""
    queue.receive("held-1", "t", "")
    queue.receive("held-2", "t", "")
    now = queue._clock()
    queue.storage.claim_due(
        now, 2,
        session_id=queue.session_id,
        lease_duration=queue.processing_timeout,
    )

    ov = queue.overview()
    active = [s for s in ov.sessions if s.status is SessionStatus.ACTIVE]
    assert len(active) == 1
    sess = active[0]
    assert sess.session_id == queue.session_id
    assert len(sess.held_events) == 2
    held = {h.key: h for h in sess.held_events}
    assert set(held) == {"held-1", "held-2"}
    for h in held.values():
        # 租约剩余时间约为 processing_timeout，且未过期
        assert 0 < h.lease_remaining <= queue.processing_timeout
        assert h.lease_expires_at > now
    assert ov.counts["PROCESSING"] == 2


def test_overview_consistent_snapshot_under_concurrency(db_path):
    """并发消费 + 人工重放同时进行时，概览数字与明细始终来自同一快照。"""
    q = ReliableQueue(
        db_path,
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0),
        processing_timeout=30.0,
    )
    q.start()
    n = 40
    for i in range(n):
        q.receive(f"c-{i:03d}", "t", "")

    stop = threading.Event()
    inconsistencies: list[str] = []

    def checker():
        while not stop.is_set():
            ov = q.overview()
            total = sum(ov.counts.values())
            if total != n:
                inconsistencies.append(f"counts total {total} != {n}")
            if ov.cursor + ov.cursor_gap != ov.max_offset:
                inconsistencies.append("cursor + gap != max_offset")
            if ov.backlog != ov.counts["PENDING"] + ov.counts["FAILED"]:
                inconsistencies.append("backlog mismatch")
            # 会话持有明细必须真的是 PROCESSING
            held = sum(len(s.held_events) for s in ov.sessions)
            if held > ov.counts["PROCESSING"]:
                inconsistencies.append("held > PROCESSING")

    flaky = {"n": 0}
    lock = threading.Lock()

    def handler(event):
        with lock:
            flaky["n"] += 1
            if flaky["n"] % 7 == 0:
                raise RetryableError("flaky")
        return "ok"

    def consumer():
        while not stop.is_set():
            q.process_once(handler, limit=3)

    threads = [threading.Thread(target=checker) for _ in range(2)]
    threads += [threading.Thread(target=consumer) for _ in range(4)]
    for t in threads:
        t.start()
    # 让并发跑一段时间，期间穿插一次人工重放
    import time

    deadline = time.time() + 1.0
    while time.time() < deadline:
        dl = q.list_dead_letters()
        if dl:
            try:
                q.replay_dead_letter(dl[0].event_id, lambda e: "fixed")
            except Exception:
                pass
        time.sleep(0.01)
    stop.set()
    for t in threads:
        t.join()

    assert inconsistencies == []
    # 最终概览与真实状态一致
    ov = q.overview()
    events = q.list_events(limit=n)
    for status, count in ov.counts.items():
        assert sum(1 for e in events if e.status.value == status) == count
    q.close()
