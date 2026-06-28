"""v1.23.70 — diagnostic timing on /api/library (tab-switch-lag investigation).

the user reported intermittent lag switching between library tabs / collections.
WAL is on (so this is NOT reader-vs-writer lock wait), and tab switching is a full
page navigation, so the lag is either the subquery-heavy browse query or the
full-nav app.js re-parse. To find out WITHOUT optimizing the regression-risky
query blind, _library_main_query now records its own duration: a WARNING when it
crosses the slow threshold, and a `query_ms` field echoed in the response so the
per-request cost is visible in devtools' Network tab during a real tab switch.

Read-only instrumentation — comes back out once the bottleneck is characterized.
These tests prove it's wired end-to-end (per the v1.18.81 phantom-fix rule: test
the actual data pipe, not just the source shape).
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


def test_api_library_echoes_query_ms(admin_client):
    r = admin_client.get("/api/library?tab=movies&per_page=50", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert "query_ms" in body, (
        "v1.23.70: /api/library must echo the diagnostic query_ms"
    )
    assert isinstance(body["query_ms"], (int, float)) and body["query_ms"] >= 0


def test_collections_tab_also_timed(admin_client):
    r = admin_client.get("/api/library?tab=collections&per_page=50", headers=AUTH)
    assert r.status_code == 200, r.text
    assert "query_ms" in r.json()


def test_slow_query_warning_is_wired():
    src = (Path(__file__).resolve().parent.parent
           / "app" / "web" / "api.py").read_text()
    assert "slow /api/library query:" in src
    assert "_q_ms >= 750" in src
