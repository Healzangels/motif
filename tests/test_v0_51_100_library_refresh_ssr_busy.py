"""v0.51.100 — the // REFRESH <NAME> button SSR-locks for its OWN per-tab enum.

v1.13.77's `_pipeline_in_flight` only SSR-locked the button for the global
cascade/scan_all pipeline (dashboard // REFRESH PLEX → nav). A per-tab REFRESH
tags its plex_enum job scope='movies'/'movies-4k' etc — so navigating back to
/movies while THAT enum drained left the button clickable for ~1s until
refreshTopbarStatus's poll locked it (residual click-flash).

v0.51.100 replaces the SSR signal with `_lib_refresh_in_flight(db, tab, fourk)`,
a full mirror of app.js `libRefreshBusy = myTabBusy || globalEnumPipeline`:
  * myTabBusy — this tab+variant's own plex_enum (running|pending), mapped from
    the section's is_anime/type/is_4k (same as /api/stats plex_enum_active);
    collections lock on the scope='collections' count.
  * globalEnumPipeline — cascade/scan_all pipeline OR (tdb-sync + auto_enum)
    OR ≥2 tabs running an enum.

These behavioral tests drive the real GET /movies|/tv|/anime|/collections ->
SSR SQL pipe and assert the button's disabled state matches each signal.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s.db_path


def _section(db, sid, *, typ, is_anime=0, is_4k=0):
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, "
            " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES (?,?,?,?,?,?,1,?,?)",
            (sid, f"S{sid}", typ, is_anime, is_4k, f"s{sid}", NOW, NOW),
        )
        c.commit()


def _job(db, *, job_type="plex_enum", status="running",
         section_id=None, scope=None):
    payload = {}
    if section_id is not None:
        payload["section_id"] = section_id
    if scope is not None:
        payload["scope"] = scope
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO jobs (job_type, status, payload, created_at) "
            "VALUES (?,?,?,?)",
            (job_type, status, json.dumps(payload) if payload else None, NOW),
        )
        c.commit()


def _btn(html: str) -> str:
    i = html.index('id="library-refresh-btn"')
    start = html.rfind("<button", 0, i)
    end = html.index("</button>", i) + len("</button>")
    return html[start:end]


def _locked(client, path):
    r = client.get(path, headers=AUTH)
    assert r.status_code == 200, r.text
    return "disabled" in _btn(r.text)


# ── myTabBusy: this tab+variant's own enum locks the button ──────────


def test_own_tab_standard_enum_locks_refresh(client):
    c, db = client
    _section(db, "1", typ="movie", is_4k=0)
    _job(db, section_id="1")  # scope-less per-tab enum → the residual case
    assert _locked(c, "/movies?fourk=0")


def test_no_enum_leaves_refresh_clickable(client):
    c, db = client
    _section(db, "1", typ="movie", is_4k=0)
    assert not _locked(c, "/movies?fourk=0")


def test_standard_enum_does_not_lock_4k_view(client):
    """Per-variant independence: a STANDARD movies enum must not lock the
    4K MOVIES button (they target different Plex sections)."""
    c, db = client
    _section(db, "1", typ="movie", is_4k=0)
    _job(db, section_id="1")
    assert not _locked(c, "/movies?fourk=1")


def test_4k_enum_locks_4k_view(client):
    c, db = client
    _section(db, "2", typ="movie", is_4k=1)
    _job(db, section_id="2")
    assert _locked(c, "/movies?fourk=1")


def test_movies_enum_does_not_lock_tv(client):
    """Per-tab independence: a movies enum must not lock the TV button."""
    c, db = client
    _section(db, "1", typ="movie", is_4k=0)
    _job(db, section_id="1")
    assert not _locked(c, "/tv?fourk=0")


def test_anime_maps_by_is_anime_flag(client):
    """An anime section is 'show'/'movie'-typed too; is_anime is the axis."""
    c, db = client
    _section(db, "3", typ="show", is_anime=1, is_4k=0)
    _job(db, section_id="3")
    assert _locked(c, "/anime?fourk=0")
    assert not _locked(c, "/tv?fourk=0")  # not the (non-anime) TV tab


def test_pending_enum_also_locks(client):
    """v1.14.73: a QUEUED (pending) enum for this tab locks it too."""
    c, db = client
    _section(db, "1", typ="movie", is_4k=0)
    _job(db, section_id="1", status="pending")
    assert _locked(c, "/movies?fourk=0")


# ── collections: scope='collections' count ──────────────────────────


def test_collections_enum_locks_collections(client):
    c, db = client
    _job(db, scope="collections")
    assert _locked(c, "/collections")


def test_plain_movies_enum_does_not_lock_collections(client):
    c, db = client
    _section(db, "1", typ="movie", is_4k=0)
    _job(db, section_id="1")
    assert not _locked(c, "/collections")


# ── globalEnumPipeline: back-compat + broader terms ─────────────────


def test_cascade_pipeline_still_locks_all_tabs(client):
    """Back-compat: the v1.13.77 cascade/scan_all pipeline still locks
    every tab's button (dashboard // REFRESH PLEX → nav)."""
    c, db = client
    _job(db, scope="cascade")
    assert _locked(c, "/movies?fourk=0")
    assert _locked(c, "/tv?fourk=0")


def test_tdb_sync_with_auto_enum_locks(client):
    """A running tdb sync (auto_enum on by default) guarantees a cascade
    enum follows, so the button locks through the sync phase."""
    c, db = client
    _job(db, job_type="sync")
    assert _locked(c, "/movies?fourk=0")


# ── plumbing guard ──────────────────────────────────────────────────


def test_helper_mirrors_librefreshbusy_terms():
    anchor = API_PY.index("def _lib_refresh_in_flight(")
    body = API_PY[anchor:anchor + 3000]
    # globalEnumPipeline terms + myTabBusy.
    assert "_pipeline_in_flight(db)" in body  # cascade/scan_all
    assert "settings.sync_auto_enum_after_sync" in body  # tdb-sync term
    assert "json_extract(j.payload, '$.section_id')" in body  # section map
    assert "'collections'" in body  # collections branch
    # routes must feed it (not the old pipeline-only signal).
    assert '"refresh_in_flight": _lib_refresh_in_flight(db, "movies"' in API_PY
