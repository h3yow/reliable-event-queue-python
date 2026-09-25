from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from app.models import QueueConfig
from app.service import EventQueueService


@pytest.fixture
def db_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as d:
        yield Path(d) / "test.db"


@pytest.fixture
def clock() -> list[float]:
    return [1000.0]


@pytest.fixture
def service(db_path: Path, clock: list[float]) -> EventQueueService:
    svc = EventQueueService(
        str(db_path),
        QueueConfig(dedup_window_seconds=10.0, max_attempts=3,
                    base_backoff_seconds=0.001, max_backoff_seconds=1.0),
        clock=lambda: clock[0],
    )
    yield svc
    svc.close()


@pytest.fixture
def real_service(db_path: Path) -> EventQueueService:
    """使用真实单调时钟 + 极短退避，供消费者池/重启测试。"""
    svc = EventQueueService(
        str(db_path),
        QueueConfig(dedup_window_seconds=60.0, max_attempts=3,
                    base_backoff_seconds=0.005, max_backoff_seconds=0.05),
    )
    yield svc
    svc.close()


def configure_caplog(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="reliable_event_queue")
