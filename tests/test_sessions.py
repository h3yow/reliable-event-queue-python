"""消费者会话与租约：接管、防重复处理、干净收尾。"""

from __future__ import annotations

from app.models import EventStatus, SessionStatus
from app.service import ReliableQueue, RetryPolicy


def _make_queue(db_path, clock, lease=0.5, timeout=60.0):
    return ReliableQueue(
        db_path,
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.0),
        processing_timeout=timeout,
        lease_duration=lease,
        clock=lambda: clock["t"],
    )


def test_session_registered_on_start(queue):
    """每次启动生成独立会话并登记，可查询。"""
    sessions = queue.storage.list_sessions()
    assert len(sessions) == 1
    assert sessions[0].id == queue.session.id
    assert sessions[0].status is SessionStatus.ACTIVE


def test_claim_binds_session_and_lease(queue):
    """认领后事件上能看出是哪个会话占着、租约到什么时候。"""
    queue.receive("job", "t", "")
    claimed = queue.storage.claim_due(
        queue._clock(), 1,
        session_id=queue.session.id,
        lease_duration=queue.lease_duration,
    )
    assert len(claimed) == 1
    event = claimed[0]
    assert event.session_id == queue.session.id
    assert event.lease_expires_at > queue._clock()


def test_fast_restart_takeover_after_kill(db_path):
    """会话被强杀 / 进程重启：租约一到期，新会话立即接管继续处理，
    且不会处理两遍。"""
    clock = {"t": 1000.0}
    q1 = _make_queue(db_path, clock)
    q1.start()
    q1.receive("job", "t", "")
    # q1 认领了事件但没处理完
    claimed = q1.storage.claim_due(
        clock["t"], 1,
        session_id=q1.session.id, lease_duration=q1.lease_duration,
    )
    assert len(claimed) == 1
    event_id = claimed[0].id
    old_session = q1.session.id
    # 模拟强杀：直接断连接，不走干净收尾
    q1.storage.close()

    # 新进程立即启动（新会话）
    q2 = _make_queue(db_path, clock)
    q2.start()
    assert q2.session.id != old_session

    handled: list[str] = []

    def handler(event):
        handled.append(event.key)
        return "done"

    # 租约未到期：新会话不能接管，事件不会被两个会话同时处理
    assert q2.process_once(handler) == []
    assert handled == []

    # 租约一到期：新会话马上接手，不必等 processing_timeout（60s）
    clock["t"] += q1.lease_duration + 0.01
    outcomes = q2.process_once(handler)
    assert len(outcomes) == 1
    assert outcomes[0].status is EventStatus.SUCCEEDED
    assert handled == ["job"]

    # 旧会话的迟到写入被会话守卫拒绝，不会重复生效
    assert q2.storage.mark_succeeded(
        event_id, "stale", clock["t"], session_id=old_session
    ) is False
    event = q2.storage.get_event(event_id)
    assert event.result == "done"
    assert q2.get_cursor().value == 1
    q2.close()


def test_no_double_claim_while_lease_valid(db_path):
    """租约有效期内，同一事件不会被两个活会话同时认领。"""
    clock = {"t": 1000.0}
    q1 = _make_queue(db_path, clock, lease=100.0)
    q1.start()
    for i in range(5):
        q1.receive(f"e-{i}", "t", "")
    q2 = _make_queue(db_path, clock, lease=100.0)
    q2.start()

    first = q1.storage.claim_due(
        clock["t"], 5, session_id=q1.session.id, lease_duration=100.0
    )
    second = q2.storage.claim_due(
        clock["t"], 5, session_id=q2.session.id, lease_duration=100.0
    )
    assert len(first) == 5
    assert second == []  # 全部在 q1 租约内，q2 一条也拿不到
    q1.close()
    q2.close()


def test_clean_shutdown_releases_events(db_path):
    """会话正常退出：手里的事件立即释放回可投递状态，会话标记关闭。"""
    clock = {"t": 1000.0}
    q1 = _make_queue(db_path, clock, lease=100.0)
    q1.start()
    q1.receive("pending-job", "t", "")
    claimed = q1.storage.claim_due(
        clock["t"], 1, session_id=q1.session.id, lease_duration=100.0
    )
    assert len(claimed) == 1
    q1.close()  # 正常退出，干净收尾

    q2 = _make_queue(db_path, clock, lease=100.0)
    q2.start()
    # 事件已释放：租约远未到期（100s）也能立刻被新会话投递
    outcomes = q2.process_once(lambda e: "resumed")
    assert len(outcomes) == 1
    assert outcomes[0].status is EventStatus.SUCCEEDED

    sessions = {s.id: s for s in q2.storage.list_sessions()}
    assert sessions[q1.session.id].status is SessionStatus.CLOSED
    assert sessions[q2.session.id].status is SessionStatus.ACTIVE
    q2.close()
