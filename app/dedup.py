"""幂等接收与去重窗口判定。

规则：
- 以 event_id 作为幂等键（不同标识天然互不可见，不会互相覆盖或误判）。
- 首次出现：登记事件 + 去重键（窗口起点 = 首次接收时间），判定 ACCEPTED。
- 窗口内再次出现：判定 DUPLICATE_IN_WINDOW，不写入、不覆盖任何已有结果。
- 窗口过期后再次出现：删除旧键，作为全新事件入队并分配新位点。
"""
from __future__ import annotations

import time
from collections.abc import Callable

from .models import QueueConfig, ReceiveDecision, ReceiveResult
from .storage import Storage


class Deduplicator:
    def __init__(
        self,
        storage: Storage,
        config: QueueConfig,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._storage = storage
        self._config = config
        self._clock = clock

    def receive(self, event_id: str, payload: str) -> ReceiveResult:
        if not event_id:
            raise ValueError("event_id 不能为空")
        now = self._clock()
        window = self._config.dedup_window_seconds

        # 检查-登记必须原子，避免并发首次投递重复入队
        with self._storage.lock:
            existing = self._storage.get_dedup(event_id)
            if existing is None:
                offset = self._storage.insert_event(event_id, payload, now)
                return ReceiveResult(
                    decision=ReceiveDecision.ACCEPTED,
                    reason=f"首次投递 event_id={event_id}，作为新事件受理，分配位点 {offset}",
                    offset=offset,
                    event_id=event_id,
                )

            first_seen, old_offset = existing
            age = now - first_seen
            if age < window:
                return ReceiveResult(
                    decision=ReceiveDecision.DUPLICATE_IN_WINDOW,
                    reason=(
                        f"重复投递 event_id={event_id}：去重窗口内再次出现"
                        f"（首次位点 {old_offset}，距今 {age:.3f}s < 窗口 {window:.3f}s，"
                        f"约 {window - age:.3f}s 后过期），忽略本次投递且不改变原结果"
                    ),
                    offset=old_offset,
                    event_id=event_id,
                )

            # 窗口过期：旧键失效，作为新事件处理
            self._storage.drop_dedup(event_id)
            new_offset = self._storage.insert_event(event_id, payload, now)
        return ReceiveResult(
            decision=ReceiveDecision.NOT_DUPLICATE_WINDOW_EXPIRED,
            reason=(
                f"非重复 event_id={event_id}：距首次投递 {age:.3f}s 已超过去重窗口 "
                f"{window:.3f}s，按新事件处理（旧位点 {old_offset} -> 新位点 {new_offset}）"
            ),
            offset=new_offset,
            event_id=event_id,
        )
