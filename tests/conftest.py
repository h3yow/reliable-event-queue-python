"""pytest 公共夹具。

配置 reliable_queue logger 输出到标准错误，单测运行时会打印
事件标识(key/event_id)、当前位点(offset/cursor)与重试次数(attempts)。
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile

import pytest

from app.service import ReliableQueue, RetryPolicy


@pytest.fixture
def db_path() -> str:
    fd, path = tempfile.mkstemp(prefix="rq-test-", suffix=".db")
    os.close(fd)
    os.remove(path)
    yield path
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass


@pytest.fixture
def queue(db_path):
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )
    q = ReliableQueue(
        db_path,
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.0,
                                 backoff=2.0, max_delay=1.0),
        dedup_window=3600.0,
        processing_timeout=5.0,
    )
    q.start()
    yield q
    q.close()
