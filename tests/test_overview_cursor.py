"""运行概览一致快照与位点提交安全约束测试。"""

from __future__ import annotations

import threading

import pytest

from app.errors import CursorBlockedError, NonRetryableError, RetryableError
from app.models import EventStatus, SessionStatus
from app.service import ReliableQueue, RetryPolicy


def test_overview_counts_backlog_and_cursor_gap(queue):
    """概览给出各状态数量、积压、位点差距，数字与明细对得上。"""
    queue.receive("done-1", "t", "")
    queue.receive("wait-1", "t", "")
    queue.receive("wait-2", "t", "")
    queue.process_once(lambda e: "ok", limit=1)  # done-1 成功

    ov = queue.get_overview()
    assert ov.counts["SUCCEEDED"] == 1
    assert ov.counts["PENDING"] == 2
    assert ov.backlog == 2
    assert sum(ov.counts.values()) == 3
    assert ov.cursor == 1
    assert ov.max_offset == 3
    assert ov.cursor_gap == 2
    assert ov.oldest_pending_wait >= 0.0


def test_overview_shows_session_lease(queue):
    """概览能看到每个会话占着哪些事件以及租约剩余时间。"""
    queue.receive("held", "t", "")
    now = queue._clock()
    claimed = queue.storage.claim_due(
        now, 1, session_id=queue.session.id,
        lease_duration=queue.lease_duration,
    )
    assert len(claimed) == 1

    ov = queue.get_overview()
    assert ov.counts["PROCESSING"] == 1
    mine = [s for s in ov.sessions if s.session_id == queue.session.id]
    assert len(mine) == 1
    assert mine[0].status is SessionStatus.ACTIVE
    assert len(mine[0].held_events) == 1
    held = mine[0].held_events[0]
    assert held.key == "held"
    assert held.offset == 1
    # 租约剩余时间与事件上的租约到期时间一致
    assert 0 < held.lease_remaining <= queue.lease_duration
    assert held.lease_expires_at == claimed[0].lease_expires_at
    # 明细与计数一致：PROCESSING 条数 == 各会话持有事件总数
    total_held = sum(len(s.held_events) for s in ov.sessions)
    assert total_held == ov.counts["PROCESSING"]


def test_overview_consistent_under_concurrency(db_path):
    """并发消费 + 人工重放期间，概览始终给出自洽快照：
    计数总和等于事件总数，持有明细与 PROCESSING 计数一致。"""
    q = ReliableQueue(
        db_path,
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0),
        lease_duration=5.0,
    )
    q.start()
    n = 40
    for i in range(n):
        q.receive(f"c-{i}", "t", "")

    stop = threading.Event()
    errors: list[Exception] = []

    def handler(event):
        if event.key.endswith("7"):
            raise RetryableError("flaky")
        return "ok"

    def worker():
        try:
            while not stop.is_set():
                q.process_once(handler, limit=3)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def replayer():
        try:
            while not stop.is_set():
                for d in q.list_dead_letters():
                    try:
                        q.replay_dead_letter(d.event_id, lambda e: "ok")
                    except (KeyError, ValueError):
                        pass
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    threads.append(threading.Thread(target=replayer))
    for t in threads:
        t.start()
    try:
        for _ in range(100):
            ov = q.get_overview()
            assert sum(ov.counts.values()) == n
            assert ov.backlog == (
                ov.counts["PENDING"] + ov.counts["FAILED"]
            )
            total_held = sum(len(s.held_events) for s in ov.sessions)
            assert total_held == ov.counts["PROCESSING"]
            assert ov.cursor_gap == ov.max_offset - ov.cursor
            assert ov.max_offset == n
    finally:
        stop.set()
        for t in threads:
            t.join()
    assert errors == []
    q.close()


def test_commit_cursor_blocked_by_pending_and_processing(queue):
    """目标位置前有待处理 / 处理中事件时拒绝提交，并说明被谁挡住。"""
    queue.receive("a", "t", "")
    queue.receive("b", "t", "")

    with pytest.raises(CursorBlockedError) as exc:
        queue.commit_cursor(2)
    msg = str(exc.value)
    assert "拒绝位点提交" in msg
    assert "key=a" in msg and "key=b" in msg
    assert "PENDING" in msg
    assert queue.get_cursor().value == 0

    # a 处理成功（位点自动推进到 1），b 仍在处理中 -> 提交 2 仍被挡
    now = queue._clock()
    queue.storage.claim_due(
        now, 2, session_id=queue.session.id,
        lease_duration=queue.lease_duration,
    )
    queue.storage.mark_succeeded(
        queue.list_events()[0].id, "ok", now,
        session_id=queue.session.id,
    )
    with pytest.raises(CursorBlockedError) as exc2:
        queue.commit_cursor(2)
    assert "key=b" in str(exc2.value)
    assert "PROCESSING" in str(exc2.value)
    # 阻挡信息结构化可查
    assert exc2.value.blockers[0]["key"] == "b"


def test_commit_cursor_blocked_by_retrying_and_dead(queue):
    """重试中与隔离区事件同样阻挡位点提交；全部成功后才允许。"""
    q = queue
    q.receive("flaky", "t", "")
    q.receive("doomed", "t", "")

    # flaky 失败一次进入重试；doomed 不可重试直接进隔离区
    def handler(event):
        if event.key == "flaky":
            raise RetryableError("try again")
        raise NonRetryableError("broken")

    q.process_once(handler, limit=2)
    assert {e.key: e.status for e in q.list_events()} == {
        "flaky": EventStatus.FAILED,
        "doomed": EventStatus.DEAD,
    }

    with pytest.raises(CursorBlockedError) as exc:
        q.commit_cursor(2)
    msg = str(exc.value)
    assert "FAILED" in msg and "DEAD" in msg

    # flaky 重试成功、doomed 重放成功后，位点自动推进到 2
    q.process_once(lambda e: "ok", limit=1)
    dl = q.list_dead_letters()
    q.replay_dead_letter(dl[0].event_id, lambda e: "ok")
    assert q.get_cursor().value == 2

    # 前面全部成功：允许提交；重复提交同一位置幂等
    q.commit_cursor(2)
    q.commit_cursor(2)
    assert q.get_cursor().value == 2


def test_commit_cursor_idempotent_same_value(queue):
    """重复提交当前位点幂等成功，即使更低位点已无意义。"""
    queue.receive("x", "t", "")
    queue.process_once(lambda e: "ok")
    assert queue.get_cursor().value == 1
    queue.commit_cursor(1)  # 幂等
    queue.commit_cursor(1)  # 再次幂等
    assert queue.get_cursor().value == 1
