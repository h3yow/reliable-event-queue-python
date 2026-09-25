"""可靠事件队列服务。

- 幂等接收：基于业务键 + 去重窗口，重复投递给出可解释原因
- 退避重试：可重试错误按指数退避，超过上限或不可重试错误进入隔离区
- 并发消费：认领（claim）在单条 SQL 内原子完成，成功终态不可变
- 崩溃恢复：认领超时的事件退回重试；位点与隔离区持久化、重启可恢复
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from app.errors import (
    CursorOutOfRangeError,
    CursorRegressionError,
    HandlerError,
    NonRetryableError,
    RetryableError,
)
from app.models import (
    CursorState,
    DeadLetter,
    DLQStatus,
    DuplicateReason,
    Event,
    EventStatus,
    ProcessOutcome,
    ReceiveResult,
)
from app.storage import Storage

logger = logging.getLogger("reliable_queue")

EventHandler = Callable[[Event], str]


@dataclass
class RetryPolicy:
    """指数退避策略。

    第 attempts 次失败后等待：
        min(base_delay * backoff ** (attempts - 1), max_delay)
    attempts 从 1 开始计数；达到 max_attempts 不再重试，进入隔离区。
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    backoff: float = 2.0
    max_delay: float = 5.0

    def delay_for(self, attempts: int) -> float:
        delay = self.base_delay * (self.backoff ** max(0, attempts - 1))
        return min(delay, self.max_delay)


class ReliableQueue:
    def __init__(
        self,
        db_path: str | Path = "queue.db",
        retry_policy: RetryPolicy | None = None,
        dedup_window: float = 300.0,
        processing_timeout: float = 30.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.storage = Storage(db_path)
        self.retry_policy = retry_policy or RetryPolicy()
        self.dedup_window = dedup_window
        self.processing_timeout = processing_timeout
        self._clock = clock or time.time
        self._started = False
        self._cursor_lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        if self._started:
            return
        self.storage.init_schema()
        recovered = self.storage.recover_orphans(
            self._clock(), self.processing_timeout
        )
        if recovered:
            logger.warning(
                "恢复认领超时事件回重试队列 count=%d", recovered
            )
        self._started = True
        logger.info("可靠队列已启动 cursor=%d", self.get_cursor().value)

    def close(self) -> None:
        if self._started:
            self.storage.close()
            self._started = False

    def __enter__(self) -> "ReliableQueue":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # 接收（幂等去重）
    # ------------------------------------------------------------------ #
    def receive(self, key: str, event_type: str, payload: str) -> ReceiveResult:
        """接收一次外部事件投递。

        去重判定（可解释）：
        - 指向事件已 SUCCEEDED：ALREADY_SUCCEEDED（永久幂等，结果不变）
        - 指向事件仍 PENDING/PROCESSING/FAILED：INFLIGHT_OR_RETRYING
        - 指向事件已 DEAD：INFLIGHT_OR_RETRYING（在隔离区中，需显式重放）
        - 去重注册已过 dedup_window：视为新事件重新接收（NONE）
        """
        if not key:
            raise ValueError("event key 不能为空")
        now = self._clock()
        event = Event(
            id=uuid.uuid4().hex,
            key=key,
            type=event_type,
            payload=payload,
            status=EventStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        # 去重检查 + 位点分配 + 插入在单个 SQLite 事务内原子完成，
        # 并发投递同一业务键时只有一条真正生效
        accepted, existing = self.storage.receive_if_new(
            event, dedup_expires_at=now + self.dedup_window, now=now
        )
        if not accepted:
            if existing.status is EventStatus.SUCCEEDED:
                reason = DuplicateReason.ALREADY_SUCCEEDED
                msg = f"事件 {key} 已成功处理，重复投递被忽略，结果保持不变"
            else:
                reason = DuplicateReason.INFLIGHT_OR_RETRYING
                msg = (
                    f"事件 {key} 已存在且状态为 {existing.status.value}，"
                    "仍在去重窗口内，按重复投递忽略"
                )
            logger.info(
                "重复投递 key=%s offset=%d reason=%s status=%s",
                key, existing.offset, reason.value, existing.status.value,
            )
            return ReceiveResult(
                accepted=False, event=existing, duplicate=True,
                reason=reason, message=msg,
            )

        logger.info(
            "接收新事件 key=%s offset=%d event_id=%s", key, event.offset, event.id
        )
        return ReceiveResult(
            accepted=True, event=event, duplicate=False,
            reason=DuplicateReason.NONE, message="新事件已接收",
        )

    # ------------------------------------------------------------------ #
    # 消费
    # ------------------------------------------------------------------ #
    def process_once(
        self, handler: EventHandler, limit: int = 1
    ) -> list[ProcessOutcome]:
        """认领并处理至多 limit 条到期事件，逐条记录处理结果。"""
        now = self._clock()
        events = self.storage.claim_due(now, limit)
        outcomes: list[ProcessOutcome] = []
        for event in events:
            outcomes.append(self._handle_event(event, handler))
        return outcomes

    def run_worker(
        self,
        handler: EventHandler,
        count: int | None = None,
        poll_interval: float = 0.05,
    ) -> int:
        """简单的轮询工作循环，处理 count 条后返回（count=None 则直到空闲
        一轮）。主要供演示与测试使用。"""
        processed = 0
        idle_rounds = 0
        while True:
            outcomes = self.process_once(handler, limit=1)
            if outcomes:
                processed += len(outcomes)
                idle_rounds = 0
            else:
                idle_rounds += 1
                if count is not None and processed >= count:
                    break
                if count is None and idle_rounds >= 1:
                    break
                time.sleep(poll_interval)
        return processed

    def _handle_event(
        self, event: Event, handler: EventHandler
    ) -> ProcessOutcome:
        """处理一条已认领事件，所有分支均给出终局状态并落盘。"""
        now = self._clock()
        logger.info(
            "开始处理 key=%s offset=%d attempts=%d",
            event.key, event.offset, event.attempts,
        )
        try:
            result = handler(event)
        except NonRetryableError as exc:
            return self._fail_event(
                event, str(exc.message), retryable=False,
                detail="不可重试错误，直接进入隔离区",
            )
        except (RetryableError, HandlerError) as exc:
            return self._fail_event(
                event, str(exc.message), retryable=True,
                detail="处理抛出可重试错误",
            )
        except Exception as exc:  # 未知异常按可重试处理，保证不丢事件
            return self._fail_event(
                event, f"{type(exc).__name__}: {exc}", retryable=True,
                detail="未预期异常按可重试处理",
            )

        stored = self.storage.get_event(event.id)
        if stored is not None and stored.status is EventStatus.SUCCEEDED:
            # 理论上认领保证不会走到这里；保留对“不可改变成功结果”的守护
            logger.warning(
                "事件已成功，跳过重复生效 key=%s offset=%d attempts=%d",
                event.key, event.offset, event.attempts,
            )
            return ProcessOutcome(
                event_id=event.id, key=event.key,
                status=EventStatus.SUCCEEDED, attempts=event.attempts,
                result=stored.result,
            )

        ok = self.storage.mark_succeeded(event.id, result, self._clock())
        if not ok:
            logger.error(
                "成功写入被终态守护拒绝 key=%s offset=%d", event.key, event.offset
            )
            refreshed = self.storage.get_event(event.id)
            return ProcessOutcome(
                event_id=event.id, key=event.key,
                status=refreshed.status if refreshed else EventStatus.DEAD,
                attempts=event.attempts, error="事件已离开 PROCESSING 状态",
            )
        logger.info(
            "处理成功 key=%s offset=%d attempts=%d result=%s",
            event.key, event.offset, event.attempts, result,
        )
        self._advance_cursor_after(event)
        return ProcessOutcome(
            event_id=event.id, key=event.key, status=EventStatus.SUCCEEDED,
            attempts=event.attempts, result=result,
        )

    def _fail_event(
        self, event: Event, error: str, retryable: bool, detail: str
    ) -> ProcessOutcome:
        now = self._clock()
        attempts = event.attempts  # 认领时已经 +1
        can_retry = retryable and attempts < self.retry_policy.max_attempts
        if can_retry:
            available_at = now + self.retry_policy.delay_for(attempts)
            self.storage.mark_retry(event.id, error, available_at, now)
            logger.warning(
                "%s key=%s offset=%d attempts=%d next_retry_in=%.3fs",
                detail, event.key, event.offset, attempts,
                available_at - now,
            )
            return ProcessOutcome(
                event_id=event.id, key=event.key, status=EventStatus.FAILED,
                attempts=attempts, error=error, requeued=True,
            )
        self.storage.mark_dead(event.id, error, now)
        logger.error(
            "进入隔离区 key=%s offset=%d attempts=%d reason=%s error=%s",
            event.key, event.offset, attempts,
            detail if not retryable else "超过最大重试次数", error,
        )
        return ProcessOutcome(
            event_id=event.id, key=event.key, status=EventStatus.DEAD,
            attempts=attempts, error=error, dead=True,
        )

    # ------------------------------------------------------------------ #
    # 隔离区重放
    # ------------------------------------------------------------------ #
    def replay_dead_letter(
        self, event_id: str, handler: EventHandler
    ) -> ProcessOutcome:
        """将隔离区事件重新置为可投递并立即处理一次。

        成功：事件回到 SUCCEEDED、隔离记录置 RESOLVED，位点照常推进；
        失败：按正常重试/隔离规则再次落盘，已成功事件的结果永不变更。
        """
        event = self.storage.get_event(event_id)
        if event is None:
            raise KeyError(f"隔离区中不存在事件 {event_id}")
        if event.status is not EventStatus.DEAD:
            raise ValueError(
                f"事件 {event_id} 当前状态 {event.status.value}，不在隔离区"
            )
        now = self._clock()
        self.storage.reset_for_replay(event_id, now)
        claimed = self.storage.claim_due(now, 1000)
        target = next((e for e in claimed if e.id == event_id), None)
        if target is None:  # 并发等极端情况下兜底
            target = self.storage.get_event(event_id)
        logger.info(
            "隔离区重放 key=%s offset=%d attempts=%d",
            target.key, target.offset, target.attempts,
        )
        outcome = self._handle_event(target, handler)
        if outcome.status is EventStatus.SUCCEEDED:
            self.storage.resolve_dead_letter(event_id, self._clock())
        return outcome

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def list_events(self, limit: int = 100) -> list[Event]:
        return self.storage.list_events(limit)

    def list_dead_letters(self, active_only: bool = True) -> list[DeadLetter]:
        status_filter = DLQStatus.ACTIVE.value if active_only else None
        return self.storage.list_dead_letters(status_filter)

    # ------------------------------------------------------------------ #
    # 位点
    # ------------------------------------------------------------------ #
    def get_cursor(self) -> CursorState:
        return self.storage.get_cursor()

    def commit_cursor(self, value: int) -> CursorState:
        """显式提交位点。

        - 回退（小于当前位点）：拒绝，抛 CursorRegressionError
        - 超出已知最大位点：拒绝，抛 CursorOutOfRangeError
        """
        with self._cursor_lock:
            current = self.storage.get_cursor().value
            if value < current:
                raise CursorRegressionError(
                    f"拒绝位点回退：当前位点 {current}，请求提交 {value}；"
                    "位点只能单调递增"
                )
            if value > self.storage.max_offset():
                raise CursorOutOfRangeError(
                    f"拒绝位点跳跃：请求提交 {value} 超出最大已知位点 "
                    f"{self.storage.max_offset()}"
                )
            now = self._clock()
            self.storage.commit_cursor(value, now)
            logger.info("位点提交 old=%d new=%d", current, value)
            return CursorState(value=value, updated_at=now)

    def _advance_cursor_after(self, event: Event) -> None:
        """事件成功后自动推进位点：仅当成功事件的偏移恰好是“当前位点 + 1”
        时连续推进，保证位点语义为“此前事件均已成功处理”。位点永不回退。"""
        with self._cursor_lock:
            value = self.storage.get_cursor().value
            if event.offset != value + 1:
                return
            events = {e.offset: e for e in self.storage.list_events(100000)}
            while (value + 1) in events and events[value + 1].status in (
                EventStatus.SUCCEEDED,
            ):
                value += 1
            if value > self.storage.get_cursor().value:
                self.storage.commit_cursor(value, self._clock())
                logger.info(
                    "位点自动推进 cursor=%d（由 key=%s offset=%d 触发）",
                    value, event.key, event.offset,
                )
