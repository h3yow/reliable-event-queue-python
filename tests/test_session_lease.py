"""消费者会话与租约：快速重启接管、防重复生效、干净退出。"""

from __future__ import annotations

from app.models import EventStatus, SessionStatus
from app.service import ReliableQueue, RetryPolicy


def _make_queue(db_path, clock, timeout=0.05):
    return ReliableQueue(
        db_path,
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.0),
        processing_timeout=timeout,
        clock=lambda: clock["t"],
    )


def test_session_created_on_start_and_closed_on_exit(queue):
    """启动即创建独立会话；正常退出后会话关闭。"""
    assert queue.session_id.startswith("sess-")
    sess = queue.storage.get_session(queue.session_id)
    assert sess is not None and sess.status is SessionStatus.ACTIVE

    sid = queue.session_id
    queue.close()
    sess = queue.storage.get_session(sid)
    assert sess.status is SessionStatus.CLOSED
    assert sess.closed_at > 0


def test_claim_marks_session_and_lease(queue):
    """处理期间事件上能看出持有会话与租约到期时间。"""
    queue.receive("leased", "t", "")
    claimed = queue.storage.claim_due(
        queue._clock(), 1,
        session_id=queue.session_id,
        lease_duration=queue.processing_timeout,
    )
    assert len(claimed) == 1
    ev = claimed[0]
    assert ev.session_id == queue.session_id
    # 租约到期时间 ≈ 当前时间 + 租约时长（processing_timeout）
    assert ev.lease_expires_at > queue._clock()
    assert ev.lease_expires_at <= queue._clock() + queue.processing_timeout + 1


def test_fast_restart_takeover_of_killed_session(db_path):
    """会话被强杀（未 close）：新会话启动后立刻接管其未完成事件。"""
    clock = {"t": 1000.0}
    q1 = _make_queue(db_path, clock)
    q1.start()
    q1.receive("stuck", "t", "payload")
    # q1 认领后未处理完就被 kill -9（不调用 close）
    claimed = q1.storage.claim_due(
        clock["t"], 1,
        session_id=q1.session_id, lease_duration=q1.processing_timeout,
    )
    assert len(claimed) == 1
    killed_session = q1.session_id
    # 注意：不 q1.close()，直接“进程消失”；仅释放 Python 对象连接
    q1.storage.close()

    # 租约刚一过期的时刻，新进程启动
    clock["t"] += 0.06
    q2 = _make_queue(db_path, clock)
    q2.start()
    assert q2.session_id != killed_session

    # 事件已被回收到可投递状态，新会话马上接手处理成功
    outcomes = q2.process_once(lambda e: "taken-over")
    assert len(outcomes) == 1
    assert outcomes[0].status is EventStatus.SUCCEEDED
    assert outcomes[0].result == "taken-over"
    assert outcomes[0].attempts == 2  # 认领次数：q1 一次 + q2 一次
    assert q2.get_cursor().value == 1
    q2.close()


def test_dead_session_events_recovered_immediately_without_lease_wait(db_path):
    """整个进程重启：旧会话已 CLOSED 的事件不等待租约到期即被接管。"""
    clock = {"t": 2000.0}
    q1 = _make_queue(db_path, clock, timeout=3600.0)  # 租约很长
    q1.start()
    q1.receive("held", "t", "")
    q1.storage.claim_due(
        clock["t"], 1,
        session_id=q1.session_id, lease_duration=q1.processing_timeout,
    )
    # 进程退出但只来得及关闭会话记录（例如优雅停机只走到一半）
    q1.storage.close_session(q1.session_id, clock["t"])
    q1.storage.close()

    # 租约远未到期，但会话已死：新进程启动时立即接管
    q2 = _make_queue(db_path, clock, timeout=3600.0)
    q2.start()
    ev = q2.list_events()[0]
    assert ev.status is EventStatus.FAILED  # 已回收待重投
    outcomes = q2.process_once(lambda e: "ok")
    assert outcomes[0].status is EventStatus.SUCCEEDED
    q2.close()


def test_stale_session_cannot_double_apply_after_takeover(db_path):
    """租约过期被接管后，旧会话的迟到的成功写入被拒绝，事件只生效一次。"""
    clock = {"t": 3000.0}
    q1 = _make_queue(db_path, clock)
    q1.start()
    q1.receive("contended", "t", "")
    q1.storage.claim_due(
        clock["t"], 1,
        session_id=q1.session_id, lease_duration=q1.processing_timeout,
    )

    # 租约过期，q2 接管并认领同一事件
    clock["t"] += 0.06
    q2 = _make_queue(db_path, clock)
    q2.start()
    claimed = q2.storage.claim_due(
        clock["t"], 1,
        session_id=q2.session_id, lease_duration=q2.processing_timeout,
    )
    assert len(claimed) == 1
    event_id = claimed[0].id

    # q1 迟到完成：写入被会话守卫拒绝
    ok = q1.storage.mark_succeeded(
        event_id, "stale-result", clock["t"], session_id=q1.session_id
    )
    assert ok is False
    # q2 的写入生效，且结果唯一
    ok = q2.storage.mark_succeeded(
        event_id, "fresh-result", clock["t"], session_id=q2.session_id
    )
    assert ok is True
    assert q2.storage.get_event(event_id).result == "fresh-result"
    q1.storage.close()
    q2.close()


def test_clean_shutdown_releases_held_events(db_path):
    """会话正常退出：持有的事件立即释放，新会话无需等待即可接手。"""
    clock = {"t": 4000.0}
    q1 = _make_queue(db_path, clock, timeout=3600.0)  # 租约很长
    q1.start()
    q1.receive("release-me", "t", "")
    q1.storage.claim_due(
        clock["t"], 1,
        session_id=q1.session_id, lease_duration=q1.processing_timeout,
    )
    q1.close()  # 干净收尾

    # q1 已关闭，改用新会话查看与处理
    q2 = _make_queue(db_path, clock, timeout=3600.0)
    q2.start()
    ev = q2.storage.get_event_by_key("release-me")
    assert ev.status is EventStatus.FAILED  # 已释放回可投递状态
    assert ev.session_id == ""
    # 租约原本还有近 1 小时，但无需等待：立即可被认领处理
    outcomes = q2.process_once(lambda e: "resumed")
    assert outcomes[0].status is EventStatus.SUCCEEDED
    assert outcomes[0].result == "resumed"
    q2.close()


def test_takeover_does_not_double_process(db_path):
    """端到端：被接管事件最终只成功生效一次，结果不被覆盖。"""
    clock = {"t": 5000.0}
    q1 = _make_queue(db_path, clock)
    q1.start()
    q1.receive("exactly-once", "t", "")
    q1.storage.claim_due(
        clock["t"], 1,
        session_id=q1.session_id, lease_duration=q1.processing_timeout,
    )
    q1.storage.close()  # 强杀

    clock["t"] += 0.06
    q2 = _make_queue(db_path, clock)
    q2.start()
    effects: list[str] = []

    def handler(event):
        effects.append(event.key)
        return "done"

    q2.process_once(handler)
    # 再次轮询不会有第二次生效
    assert q2.process_once(handler) == []
    assert effects == ["exactly-once"]
    ev = q2.storage.get_event_by_key("exactly-once")
    assert ev.status is EventStatus.SUCCEEDED and ev.result == "done"
    q2.close()
