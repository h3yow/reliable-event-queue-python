"""FastAPI 路由：本地 HTTP 入口（无外部依赖、无真实凭据）。"""
from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .models import QueueConfig
from .service import EventQueueService, OffsetRollbackError

_DB_PATH = os.environ.get("RELIABLE_QUEUE_DB", "reliable_queue.db")
_service: EventQueueService | None = None


def get_service() -> EventQueueService:
    global _service
    if _service is None:
        _service = EventQueueService(_DB_PATH, QueueConfig())
    return _service


router = APIRouter()


class EventIn(BaseModel):
    event_id: str = Field(min_length=1, description="幂等键")
    payload: str = ""


class CommitIn(BaseModel):
    offset: int


@router.post("/events")
def post_event(body: EventIn) -> dict[str, Any]:
    result = get_service().receive(body.event_id, body.payload)
    return {
        "event_id": result.event_id,
        "decision": result.decision.value,
        "reason": result.reason,
        "offset": result.offset,
    }


@router.get("/events/{offset}")
def get_event(offset: int) -> dict[str, Any]:
    event = get_service().storage.get_event(offset)
    if event is None:
        raise HTTPException(status_code=404, detail=f"位点 {offset} 不存在")
    return {
        "event_id": event.event_id,
        "offset": event.offset,
        "status": event.status.value,
        "attempts": event.attempts,
        "result": event.result,
        "last_error": event.last_error,
        "next_visible_at": event.next_visible_at,
    }


@router.get("/offset")
def get_offset() -> dict[str, Any]:
    svc = get_service()
    return {"committed_offset": svc.committed_offset}


@router.post("/offset")
def post_offset(body: CommitIn) -> dict[str, Any]:
    try:
        new_offset = get_service().commit_offset(body.offset)
    except OffsetRollbackError as exc:
        # 回退 / 越洞提交统一按 409 拒绝，消息中说明原因
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"committed_offset": new_offset}


@router.get("/dead-letters")
def list_dead_letters(include_replayed: bool = False) -> dict[str, Any]:
    entries = get_service().list_dead_letters(include_replayed)
    return {
        "count": len(entries),
        "dead_letters": [
            {
                "event_id": e.event_id,
                "offset": e.offset,
                "attempts": e.attempts,
                "last_error": e.last_error,
                "reason": e.reason,
                "dead_at": e.dead_at,
                "replayed": e.replayed,
            }
            for e in entries
        ],
    }


@router.post("/dead-letters/{offset}/replay")
def replay_dead(offset: int) -> dict[str, Any]:
    svc = get_service()
    try:
        reopened = svc.replay_dead(offset)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"offset": offset, "reopened": reopened}
