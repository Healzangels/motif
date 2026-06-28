"""v1.22.62 (audit round 2, Batch A #4) — ACCEPT ALL acted on rows the
UI deliberately hides.

api_accept_all_updates' row-selection WHERE was missing
`_pending_update_actionable_sql` — the gate present at all 13 sibling
sites including `/api/updates/count` (the number the confirm dialog
shows: "Accept N pending updates?") and DECLINE ALL. So ACCEPT ALL
also accepted the rows the read gates hide: v1.22.12 pure-P
`new_theme_available` detections (anime is ~100% Plex-served — the
detections still sit in pending_updates, just not rendered) and
url-less `upstream_changed` no-ops. The user confirms "3", and behind
one {"ok": true} potentially hundreds of hidden rows get their
decision flipped to accepted + a yt-dlp download enqueued each.

Fix: the actionable gate added to the tuples WHERE, restoring the
"accept-all acts on exactly what the count shows" contract.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-06-11T00:00:00+00:00"
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
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


def _seed_new_theme_row(db, tmdb_id, *, has_theme):
    """A new_theme_available pending row. has_theme=1 with no motif
    presence = the pure-P shape the v1.22.12 read gate HIDES;
    has_theme=0 = the visible/actionable shape."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
            "is_anime, is_4k, themes_subdir, included, discovered_at, "
            "last_seen_at) VALUES ('3','Anime','show',1,0,'anime',1,?,?)",
            (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES ('tv',?,'X','imdb',?,?,'https://yt/new')",
            (tmdb_id, NOW, NOW))
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "theme_id, guid_tmdb, title, year, has_theme, first_seen_at, "
            "last_seen_at) VALUES (?,'3','show',?,?,'X',2024,?,?,?)",
            (f"rk{tmdb_id}", tid, tmdb_id, has_theme, NOW, NOW))
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',?,'3','new_theme_available','https://yt/new',"
            "'pending',?)", (tmdb_id, NOW))
        conn.commit()


def _decision(db, tmdb_id) -> str:
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT decision FROM pending_updates WHERE tmdb_id = ?",
            (tmdb_id,)).fetchone()[0]


def test_accept_all_matches_the_confirm_dialog_count(admin_client):
    """The contract: ACCEPT ALL acts on exactly the rows
    /api/updates/count reports (what the user confirmed). A hidden
    pure-P new_theme detection must be untouched; the visible row
    must be accepted."""
    client, db = admin_client
    _seed_new_theme_row(db, 9301, has_theme=0)  # visible / actionable
    _seed_new_theme_row(db, 9302, has_theme=1)  # pure-P → hidden

    count = client.get("/api/updates/count", headers=AUTH).json()
    assert count["pending"] == 1, (
        f"confirm-dialog count must see only the visible row: {count}")

    r = client.post("/api/updates/accept-all", headers=AUTH)
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["accepted"] == 1, (
        f"v1.22.62: accept-all must act on exactly the counted rows — got "
        f"{j} (pre-fix the hidden pure-P detection was accepted too)"
    )
    assert _decision(db, 9301) == "accepted"
    assert _decision(db, 9302) == "pending", (
        "the hidden pure-P new_theme detection must remain untouched"
    )


def test_accept_all_where_carries_the_actionable_gate():
    """Source pin: the tuples WHERE includes the shared actionable
    helper, mirroring DECLINE ALL (the v1.22.10 one-helper rule)."""
    i = API_PY.index("async def api_accept_all_updates(")
    body = API_PY[i:API_PY.index("\n    @app.", i + 1)]
    assert "_pending_update_actionable_sql('t2', 'pi2')" in body, (
        "v1.22.62: accept-all's row selection must include the "
        "actionable gate used by /api/updates/count + decline-all"
    )