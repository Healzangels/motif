"""v0.51.299 — holistic review wave 8: saved-filters endpoints, behaviorally.

The presets feature was guarded exclusively by frontend source pins (nine
test files pin app.js/CSS around loadLibraryPresets) while its persistence
endpoints — GET/POST /api/saved-filters and DELETE /api/saved-filters/{id},
including the (scope, name) update-vs-insert dedup — had ZERO behavioral
coverage (the v1.18.81 phantom-guard class: the pipe was never exercised).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s.db_path


def test_post_then_get_round_trips_scoped(client):
    c, _db = client
    r = c.post("/api/saved-filters", headers=AUTH, json={
        "name": "My Loud Rows", "query_json": "attn_pills=fail&sort=title"})
    assert r.status_code == 200, r.text
    fid = r.json().get("id")
    assert fid
    listed = c.get("/api/saved-filters", headers=AUTH).json()
    rows = listed if isinstance(listed, list) else listed.get("filters", [])
    assert [x["name"] for x in rows] == ["My Loud Rows"]
    assert rows[0]["query_json"] == "attn_pills=fail&sort=title"
    # a different scope's listing is empty — namespace isolation.
    other = c.get("/api/saved-filters?scope=queue", headers=AUTH).json()
    orows = other if isinstance(other, list) else other.get("filters", [])
    assert orows == []


def test_same_scope_name_updates_in_place(client):
    c, db = client
    r1 = c.post("/api/saved-filters", headers=AUTH, json={
        "name": "Dupe", "query_json": "a=1"})
    r2 = c.post("/api/saved-filters", headers=AUTH, json={
        "name": "Dupe", "query_json": "b=2"})
    assert r1.status_code == r2.status_code == 200
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT id, query_json FROM saved_filters WHERE name='Dupe'"
        ).fetchall()
    assert len(rows) == 1, "the (scope, name) dedup must UPDATE, not insert"
    assert rows[0][1] == "b=2", "the newer body wins"
    assert r2.json().get("id") == rows[0][0]


def test_same_name_different_scope_coexists(client):
    c, db = client
    c.post("/api/saved-filters", headers=AUTH,
           json={"name": "N", "query_json": "x=1"})
    c.post("/api/saved-filters", headers=AUTH,
           json={"name": "N", "scope": "queue", "query_json": "y=2"})
    with sqlite3.connect(db) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM saved_filters WHERE name='N'").fetchone()[0]
    assert n == 2, "scopes are separate namespaces"


def test_delete_removes_only_the_named_row(client):
    c, db = client
    a = c.post("/api/saved-filters", headers=AUTH,
               json={"name": "A", "query_json": "a=1"}).json()["id"]
    c.post("/api/saved-filters", headers=AUTH,
           json={"name": "B", "query_json": "b=1"})
    r = c.delete(f"/api/saved-filters/{a}", headers=AUTH)
    assert r.status_code == 200, r.text
    with sqlite3.connect(db) as conn:
        names = [x[0] for x in conn.execute(
            "SELECT name FROM saved_filters").fetchall()]
    assert names == ["B"]


def test_rejects_garbage_bodies(client):
    c, _db = client
    assert c.post("/api/saved-filters", headers=AUTH,
                  json={"query_json": "a=1"}).status_code == 400
    assert c.post("/api/saved-filters", headers=AUTH,
                  json=["not", "an", "object"]).status_code == 400
