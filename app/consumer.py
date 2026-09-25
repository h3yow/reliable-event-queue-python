"""并发消费者协调器。

多个 worker 线程共享同一个 EventQueueService；事件的互斥由存储层
claim_due 的 BEGIN IMMEDIATE 事务保证——每个事件一次只会被一个线程认领，
因此并行消费不会重复生效，也不会丢事件（失败回退/退避，崩溃回退 PENDING）。
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

from .models import Event
from .service import EventQueueService

logger = logging.getLogger("reliable_event_queue.consumer")


class ConsumerPool:
    def __init__(
        self,
        service: EventQueueService,
        handler: Callable[[Event], Any],
        workers: int = 4,
        batch_size: int = 8,
        poll_interval: float = 0.02,
    ) -> None:
        self._service = service
        self._handler = handler
        self._workers = max(1, workers)
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.processed = 0
        self._counter_lock = threading.Lock()

    @property
    def running(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    def start(self) -> None:
        if self._threads:
            return
        self._stop.clear()
        for i in range(self._workers):
            t = threading.Thread(target=self._run, name=f"consumer-{i}", daemon=True)
            t.start()
            self._threads.append(t)
        logger.info("启动消费者线程池 workers=%d", self._workers)

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=timeout)
        self._threads = []
        # 所有 worker 已退出：回收“已认领但未处理”的在途事件，
        # 它们仍是 PROCESSING，回退为 PENDING 等待下次消费，避免丢失
        recovered = self._service.storage.recover_inflight()
        if recovered:
            logger.info("优雅停机回收在途事件 %d 个，已重置为待处理", recovered)

    def _run(self) -> None:
        while not self._stop.is_set():
            events = self._service.poll(self._batch_size)
            if not events:
                self._stop.wait(self._poll_interval)
                continue
            for event in events:
                if self._stop.is_set():
                    break
                self._service.handle_one(event, self._handler)
                with self._counter_lock:
                    self.processed += 1

    def run_until_drained(self, max_wait: float = 5.0) -> int:
        """便捷方法：启动线程池并等待所有事件到达终态（成功/隔离）。

        注意：处于退避等待中的事件仍非终态，会等到退避结束被再次处理。
        """
        import time as _time

        self.start()
        deadline = _time.monotonic() + max_wait
        while _time.monotonic() < deadline:
            _time.sleep(self._poll_interval)
            if self._service.storage.first_non_terminal_offset() is None:
                break
        self.stop()
        return self.processed
