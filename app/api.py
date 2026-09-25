"""FastAPI 路由：事件投递、处理、查询、隔离区与位点管理。"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.errors import CursorOutOfRangeError, CursorRegressionError
from app.models import (
    DeadLetter,
    Event,
    ProcessOutcome,
    ReceiveResult,
)
from app.service import EventHandler, ReliableQueue


class EventIn(BaseModel):
    key: str
    type: str = "event"
    payload: str = ""


class CursorIn(BaseModel):
    value: int


def event_to_dict(e: Event) -> dict[str, Any]:
    return {
        "id": e.id,
        "key": e.key,
        "type": e.type,
        "payload": e.payload,
        "status": e.status.value,
        "offset": e.offset,
        "attempts": e.attempts,
        "available_at": e.available_at,
        "last_error": e.last_error,
        "result": e.result,
        "created_at": e.created_at,
        "updated_at": e.updated_at,
    }


def receive_to_dict(r: ReceiveResult) -> dict[str, Any]:
    return {
        "accepted": r.accepted,
        "duplicate": r.duplicate,
        "reason": r.reason.value,
        "message": r.message,
        "event": event_to_dict(r.event) if r.event else None,
    }


def outcome_to_dict(o: ProcessOutcome) -> dict[str, Any]:
    return {
        "event_id": o.event_id,
        "key": o.key,
        "status": o.status.value,
        "attempts": o.attempts,
        "result": o.result,
        "error": o.error,
        "requeued": o.requeued,
        "dead": o.dead,
    }


def dead_letter_to_dict(d: DeadLetter) -> dict[str, Any]:
    return {
        "event_id": d.event_id,
        "key": d.key,
        "attempts": d.attempts,
        "last_error": d.last_error,
        "status": d.status.value,
        "dead_at": d.dead_at,
        "event": event_to_dict(d.event) if d.event else None,
    }


def create_app(queue: ReliableQueue, handler: EventHandler | None = None) -> FastAPI:
    """创建 FastAPI 应用。

    handler 为事件处理函数，供 /process 与隔离区重放调用；若未提供，则
    使用恒成功的默认处理器。
    """
    app = FastAPI(title="本地可靠事件队列", version="0.1.0")
    default_handler: EventHandler = lambda e: f"processed:{e.key}"
    resolve = handler or default_handler

    @app.get("/")
    def root() -> dict[str, Any]:
        return {
            "service": "reliable-event-queue",
            "cursor": queue.get_cursor().value,
            "endpoints": [
                "POST /events", "GET /events", "GET /events/{event_id}",
                "POST /process", "GET /dead-letters",
                "POST /dead-letters/{event_id}/replay",
                "GET /cursor", "POST /cursor",
            ],
        }

    @app.post("/events")
    def post_event(body: EventIn) -> dict[str, Any]:
        try:
            result = queue.receive(body.key, body.type, body.payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return receive_to_dict(result)

    @app.get("/events")
    def list_events(limit: int = 100) -> dict[str, Any]:
        return {"events": [event_to_dict(e) for e in queue.list_events(limit)]}

    @app.get("/events/{event_id}")
    def get_event(event_id: str) -> dict[str, Any]:
        event = queue.storage.get_event(event_id)
        if event is None:
            raise HTTPException(status_code=404, detail="事件不存在")
        return event_to_dict(event)

    @app.post("/process")
    def process(limit: int = 1) -> dict[str, Any]:
        outcomes = queue.process_once(resolve, limit=limit)
        return {"processed": len(outcomes),
                "outcomes": [outcome_to_dict(o) for o in outcomes]}

    @app.get("/dead-letters")
    def list_dead(active: bool = True) -> dict[str, Any]:
        return {
            "dead_letters": [
                dead_letter_to_dict(d) for d in queue.list_dead_letters(active)
            ]
        }

    @app.post("/dead-letters/{event_id}/replay")
    def replay(event_id: str) -> dict[str, Any]:
        try:
            outcome = queue.replay_dead_letter(event_id, resolve)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return outcome_to_dict(outcome)

    @app.get("/cursor")
    def get_cursor() -> dict[str, Any]:
        state = queue.get_cursor()
        return {"cursor": state.value, "updated_at": state.updated_at,
                "max_offset": queue.storage.max_offset()}

    @app.post("/cursor")
    def post_cursor(body: CursorIn) -> dict[str, Any]:
        try:
            state = queue.commit_cursor(body.value)
        except CursorRegressionError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        except CursorOutOfRangeError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"cursor": state.value, "updated_at": state.updated_at}

    return app
