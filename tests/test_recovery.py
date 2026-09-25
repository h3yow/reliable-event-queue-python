"""进程重启恢复 + 位点语义。"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.models import Event, EventStatus, FatalError, QueueConfig, RetryableError
from app.service import EventQueueService, OffsetRollbackError


def _new_service(db_path: Path) -> EventQueueService:
    return EventQueueService(
        str(db_path),
        QueueConfig(dedup_window_seconds=60.0, max_attempts=3,
                    base_backoff_seconds=0.005, max_backoff_seconds=0.05),
    )


def test_offset_and_dead_letters_survive_restart(db_path: Path):
    svc = _new_service(db_path)
    svc.receive("ok-1", "p")
    svc.receive("ok-2", "p")
    svc.process_batch(lambda e: "result")  # 两个成功
    svc.receive("fatal-2", "p")
    svc.process_batch(lambda e: (_ for _ in ()).throw(FatalError("poison")))
    assert svc.commit_offset(2) == 2

    svc.close()
    svc2 = _new_service(db_path)
    # 位点从持久化恢复
    assert svc2.committed_offset == 2
    # 成功结果保持
    assert svc2.storage.get_event(1).status is EventStatus.SUCCEEDED
    assert svc2.storage.get_event(1).result == "result"
    # 隔离区可查询且内容完整
    dead = svc2.list_dead_letters(include_replayed=True)
    assert [d.event_id for d in dead] == ["fatal-2"]
    assert "不可重试错误" in dead[0].reason
    svc2.close()


def test_processing_events_recovered_after_crash(db_path: Path):
    """模拟崩溃：事件停在 PROCESSING，重启后回退 PENDING 并可继续处理，不丢不重。"""
    svc = _new_service(db_path)
    svc.receive("c1", "p")
    svc.receive("c2", "p")
    claimed = svc.poll(10)
    assert {e.offset for e in claimed} == {1, 2}
    # 不处理、不优雅停机 => 模拟进程崩溃
    svc.storage._conn.close()

    svc2 = _new_service(db_path)
    assert svc2.storage.get_event(1).status is EventStatus.PENDING
    assert svc2.storage.get_event(2).status is EventStatus.PENDING
    n = svc2.process_batch(lambda e: "after-recovery")
    assert n == 2
    assert svc2.storage.get_event(1).result == "after-recovery"
    assert svc2.storage.get_event(2).attempts == 1
    svc2.close()


def test_retry_state_survives_restart(db_path: Path):
    svc = _new_service(db_path)
    svc.receive("retrying", "p")

    calls = {"n": 0}

    def fail_once(event: Event) -> str:
        raise RetryableError("transient")

    svc.process_batch(fail_once)
    event = svc.storage.get_event(1)
    assert event.status is EventStatus.PENDING and event.attempts == 1
    next_visible = event.next_visible_at
    svc.close()

    # 重启后 attempts / 退避时刻保留，不会从头计数
    svc2 = _new_service(db_path)
    assert svc2.poll() == []  # 退避未到，不能领取
    assert svc2.storage.get_event(1).attempts == 1
    assert svc2.storage.get_event(1).next_visible_at == next_visible
    svc2.close()


def test_offset_rollback_rejected_with_explanation(db_path: Path):
    svc = _new_service(db_path)
    svc.receive("a", "p")
    svc.process_batch(lambda e: "ok")
    assert svc.commit_offset(1) == 1

    with pytest.raises(OffsetRollbackError) as exc:
        svc.commit_offset(0)
    assert "位点回退被拒绝" in str(exc.value)
    assert "1" in str(exc.value) and "0" in str(exc.value)
    # 拒绝后原位点不变
    assert svc.committed_offset == 1
    svc.close()


def test_commit_offset_rejected_when_gap_exists(db_path: Path):
    """位点 1 未完成时，不允许“跳过空洞”提交位点 2。"""
    svc = _new_service(db_path)
    svc.receive("a", "p")
    svc.receive("b", "p")
    # 只处理位点 2（通过直接调用存储模拟）——更真实的做法：让位点1一直失败重试
    ev2 = svc.storage.get_event(2)
    # 手工把 2 置成功而 1 保持 PENDING
    svc.storage.mark_success(2, "ok", 1)
    with pytest.raises(OffsetRollbackError) as exc:
        svc.commit_offset(2)
    assert "未到终态" in str(exc.value)
    assert svc.committed_offset == -1
    svc.close()
