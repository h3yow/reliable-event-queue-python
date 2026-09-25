"""队列门面服务：串联去重、存储、重试与消费。

并发模型：进程内多线程共享同一个 Storage（单连接 + 锁）。claim_due 在
BEGIN IMMEDIATE 事务中把到期事件原子地置为 PROCESSING，因此同一事件不会
被两个消费者同时处理；崩溃后 PROCESSING 事件由存储层在重开时回退为 PENDING。
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from .dedup import Deduplicator
from .models import (
    DeadLetterEntry,
    Event,
    EventStatus,
    QueueConfig,
    ReceiveResult,
)
from .retry import backoff_delay, dead_reason, should_retry
from .storage import Storage

logger = logging.getLogger("reliable_event_queue")


class OffsetRollbackError(ValueError):
    """位点回退被拒绝。"""


class EventQueueService:
    def __init__(
        self,
        db_path: str,
        config: QueueConfig | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config or QueueConfig()
        self._clock = clock
        self.storage = Storage(db_path)
        self.deduplicator = Deduplicator(self.storage, self.config, clock)

    def close(self) -> None:
        self.storage.close()

    # ---------- 接收 ----------
    def receive(self, event_id: str, payload: str) -> ReceiveResult:
        result = self.deduplicator.receive(event_id, payload)
        logger.info(
            "接收事件 event_id=%s 判定=%s 位点=%s 原因=%s",
            event_id, result.decision.value, result.offset, result.reason,
        )
        return result

    # ---------- 消费 ----------
    def poll(self, limit: int | None = None) -> list[Event]:
        now = self._clock()
        return self.storage.claim_due(now, limit or self.config.poll_limit)

    def handle_one(self, event: Event, handler: Callable[[Event], Any]) -> EventStatus:
        """处理单个已认领事件；handler 可返回结果字符串（或任意可 str 化对象）。"""
        # 终态保护：已成功/已隔离的事件不允许被再次处理（成功结果不可变）
        fresh = self.storage.get_event(event.offset)
        if fresh is not None and fresh.status in (EventStatus.SUCCEEDED, EventStatus.DEAD_LETTER):
            logger.info(
                "跳过处理 event_id=%s 位点=%d：事件已处于终态 %s，结果保持不变",
                event.event_id, event.offset, fresh.status.value,
            )
            return fresh.status
        committed = self.storage.get_committed_offset()
        attempts = event.attempts + 1
        try:
            # 不在持锁状态下执行业务逻辑，避免阻塞其它消费者的写操作
            output = handler(event)
            result_text = output if isinstance(output, str) else str(output)
            self.storage.mark_success(event.offset, result_text, attempts)
            logger.info(
                "处理成功 event_id=%s 位点=%d 当前提交位点=%d 重试次数=%d 结果=%s",
                event.event_id, event.offset, committed, attempts - 1, result_text,
            )
            return EventStatus.SUCCEEDED
        except Exception as exc:  # 消费者失败：重试或隔离
            delay = 0.0
            if should_retry(exc, attempts, self.config):
                delay = backoff_delay(attempts, self.config)
                self.storage.mark_retry(
                    event.offset, f"{type(exc).__name__}: {exc}",
                    attempts, self._clock() + delay,
                )
                logger.warning(
                    "处理失败将重试 event_id=%s 位点=%d 当前提交位点=%d 重试次数=%d "
                    "退避=%.3fs 错误=%s",
                    event.event_id, event.offset, committed, attempts, delay, exc,
                )
                return EventStatus.PENDING
            reason = dead_reason(exc, attempts, self.config)
            self.storage.mark_dead(
                event.offset, f"{type(exc).__name__}: {exc}", attempts, self._clock()
            )
            self.storage.upsert_dead_letter(
                DeadLetterEntry(
                    event_id=event.event_id, offset=event.offset, payload=event.payload,
                    attempts=attempts, last_error=f"{type(exc).__name__}: {exc}",
                    reason=reason, dead_at=self._clock(),
                )
            )
            logger.error(
                "处理失败进入隔离区 event_id=%s 位点=%d 当前提交位点=%d 重试次数=%d 原因=%s",
                event.event_id, event.offset, committed, attempts, reason,
            )
            return EventStatus.DEAD_LETTER

    def process_batch(self, handler: Callable[[Event], Any], limit: int | None = None) -> int:
        """领取并处理一批，返回处理数量（失败重试/隔离也算处理了一次）。"""
        events = self.poll(limit)
        for event in events:
            self.handle_one(event, handler)
        return len(events)

    # ---------- 位点 ----------
    @property
    def committed_offset(self) -> int:
        return self.storage.get_committed_offset()

    def commit_offset(self, offset: int) -> int:
        """提交位点（单调递增）。

        - 回退（offset < 当前提交位点）明确拒绝，抛出 OffsetRollbackError。
        - 位点之前若仍有事件未到终态，也拒绝提交，避免提交了尚未生效的位置。
        - 无新进展的重复提交（相等）幂等返回，不报错。
        """
        current = self.storage.get_committed_offset()
        if offset < current:
            raise OffsetRollbackError(
                f"位点回退被拒绝：请求提交 {offset}，但当前已提交位点为 {current}，"
                f"位点只能单调递增（事件位点从 1 开始分配，-1 表示尚无提交）"
            )
        if offset == current:
            return current
        if not self.storage.all_terminal_through(offset):
            gap = self.storage.first_non_terminal_offset()
            raise OffsetRollbackError(
                f"位点提交被拒绝：位点 {offset} 之前仍有未到终态的事件"
                f"（首个未完成位点 {gap}，当前提交位点 {current}），"
                f"需先处理成功或进入隔离区后才能提交连续位点"
            )
        self.storage.set_committed_offset(offset)
        logger.info("提交位点 %d（原位点 %d）", offset, current)
        return offset

    def auto_advance(self) -> int:
        """把提交位点推进到“最长连续终态前缀”的末尾。"""
        first_gap = self.storage.first_non_terminal_offset()
        max_offset = self._max_offset()
        target = (first_gap - 1) if first_gap is not None else max_offset
        if target > self.storage.get_committed_offset():
            self.commit_offset(target)
        return self.storage.get_committed_offset()

    def _max_offset(self) -> int:
        with self.storage.lock:
            row = self.storage._conn.execute(  # noqa: SLF001
                "SELECT MAX(offset_n) AS m FROM events"
            ).fetchone()
            return -1 if row["m"] is None else row["m"]

    # ---------- 隔离区 ----------
    def list_dead_letters(self, include_replayed: bool = False) -> list[DeadLetterEntry]:
        return self.storage.list_dead_letters(include_replayed)

    def replay_dead(self, offset: int) -> bool:
        """重放隔离区事件；已成功事件的结果不会被改变。"""
        event = self.storage.get_event(offset)
        if event is None:
            raise KeyError(f"位点 {offset} 不存在")
        if event.status is EventStatus.SUCCEEDED:
            logger.info("跳过重放 event_id=%s 位点=%d：已成功，结果保持不变", event.event_id, offset)
            return False
        reopened = self.storage.reopen_dead_event(offset, self._clock())
        if reopened:
            self.storage.mark_dead_replayed(offset)
            logger.warning("重放隔离区事件 event_id=%s 位点=%d", event.event_id, offset)
        return reopened
