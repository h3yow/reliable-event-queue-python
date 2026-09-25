"""HTTP API 端到端：幂等接收、查询、位点、隔离区、重放。"""
from __future__ import annotations

import importlib
import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(db_path, monkeypatch):
    monkeypatch.setenv("RELIABLE_QUEUE_DB", str(db_path))
    import app.api as api

    importlib.reload(api)  # 以新的 DB 路径重建单例
    import main

    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c
    api.get_service().close()


def test_post_and_query_event(client):
    r1 = client.post("/events", json={"event_id": "e1", "payload": "hello"})
    assert r1.status_code == 200
    assert r1.json()["decision"] == "accepted"
    assert r1.json()["offset"] == 1

    r2 = client.post("/events", json={"event_id": "e1", "payload": "hello"})
    body = r2.json()
    assert body["decision"] == "duplicate_in_window"
    assert body["offset"] == 1
    assert "去重窗口内" in body["reason"]

    r3 = client.get("/events/1")
    assert r3.status_code == 200
    assert r3.json()["status"] == "pending"

    assert client.get("/events/999").status_code == 404


def test_offset_endpoint_rejects_rollback(client):
    client.post("/events", json={"event_id": "e1"})
    # 事件未到终态，提交位点 1 被拒（存在空洞）
    r = client.post("/offset", json={"offset": 1})
    assert r.status_code == 409
    assert "未到终态" in r.json()["detail"]

    # 直接通过服务层处理成功后再提交
    import app.api as api

    svc = api.get_service()
    svc.process_batch(lambda e: "ok")
    assert client.post("/offset", json={"offset": 1}).status_code == 200
    r = client.post("/offset", json={"offset": 0})
    assert r.status_code == 409
    assert "位点回退被拒绝" in r.json()["detail"]
    assert client.get("/offset").json()["committed_offset"] == 1


def test_dead_letters_and_replay(client):
    import app.api as api

    svc = api.get_service()
    from app.models import FatalError

    svc.receive("poison", "p")
    svc.process_batch(lambda e: (_ for _ in ()).throw(FatalError("bad")))

    r = client.get("/dead-letters")
    assert r.status_code == 200
    entries = r.json()["dead_letters"]
    assert len(entries) == 1 and entries[0]["event_id"] == "poison"
    assert entries[0]["reason"]

    r = client.post("/dead-letters/1/replay")
    assert r.status_code == 200 and r.json()["reopened"] is True
    assert client.get("/dead-letters").json()["count"] == 0

    # 不存在的位点
    assert client.post("/dead-letters/999/replay").status_code == 404
