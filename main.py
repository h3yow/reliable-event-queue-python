"""本地可靠事件队列服务入口。

启动：uv run python main.py （或 uv run uvicorn main:app --reload）
默认仅监听 127.0.0.1，数据落在本地 SQLite 文件（RELIABLE_QUEUE_DB 可覆盖）。
"""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.api import get_service, router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield
    get_service().close()


app = FastAPI(title="Local Reliable Event Queue", lifespan=lifespan)
app.include_router(router)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "local-reliable-event-queue", "docs": "/docs"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
