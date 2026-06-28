"""v1.22.62 (audit round 2, Batch A #3) — ACCEPT/REVERT destroyed
backup-intent on rollback.

Per-row ACCEPT UPDATE and REVERT both fetch the override with
`SELECT youtube_url FROM user_overrides` (one column), then stamp the
v1.19.36 rollback field via
    prior_intent = override["intent"] if "intent" in override.keys()
                   else "replace"
— a guard that was ALWAYS False against a youtube_url-only row, so
prior_intent was constantly 'replace'. When the accepted/reverted
download failed terminally, the worker rollback restored a
backup-intent override as intent='replace': the BACKUP READY banner
vanished and the next place flow could steal Plex's serving slot,
against recorded user intent. The bulk endpoint fetched
`youtube_url, intent` correctly (v1.20.12, whose comment even claims
it "mirrors api_accept_update's v1.19.36 fix") — the per-row SELECTs
lost the column in the v1.21.87/.73 edition rewrites.

Fix: all six per-row SELECTs (2 in ACCEPT, 4 in REVERT) fetch
`youtube_url, intent`.
"""
from __future__ import annotations

import json
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


def _seed_row_with_backup_override(db, tmdb_id, *, with_previous_url=False):
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
            "last_seen_at) VALUES (?,'3','show',?,?,'X',2024,0,?,?)",
            (f"rk{tmdb_id}", tid, tmdb_id, NOW, NOW))
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',?,'3','upstream_changed','https://yt/new',"
            "'pending',?)", (tmdb_id, NOW))
        # The KEEP-AS-BACKUP override (different URL → url_match False).
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, "
            "set_at, section_id, edition_key, intent) "
            "VALUES ('tv',?,'https://yt/backup',?,'3','','backup')",
            (tmdb_id, NOW))
        if with_previous_url:
            # REVERT's precondition: a captured previous URL to swap back to.
            conn.execute(
                "INSERT INTO previous_urls (media_type, tmdb_id, section_id, "
                "youtube_url, kind, captured_at) "
                "VALUES ('tv',?,'3','https://yt/prev','themerrdb',?)",
                (tmdb_id, NOW))
        conn.commit()


def _job_rollback(db, tmdb_id) -> dict:
    with sqlite3.connect(db) as conn:
        r = conn.execute(
            "SELECT payload FROM jobs WHERE job_type='download' AND tmdb_id=?",
            (tmdb_id,)).fetchone()
    assert r, "accept/revert must enqueue a download job"
    return json.loads(r[0]).get("rollback") or {}


def test_accept_rollback_preserves_backup_intent(admin_client):
    """Behavioral: ACCEPT on a row with a backup-intent override must
    stamp rollback.prior_intent='backup' (pre-fix: always 'replace')."""
    client, db = admin_client
    _seed_row_with_backup_override(db, 9201)
    r = client.post("/api/updates/tv/9201/accept?section_id=3", headers=AUTH)
    assert r.status_code == 200, r.text
    rb = _job_rollback(db, 9201)
    assert rb.get("prior_intent") == "backup", (
        "v1.22.62: the rollback must carry the override's REAL intent — "
        f"got {rb.get('prior_intent')!r} (pre-fix the youtube_url-only "
        "SELECT made the keys() guard a dead branch → always 'replace')"
    )


def test_revert_rollback_preserves_backup_intent(admin_client):
    """Behavioral: REVERT with a backup-intent override present must
    stamp rollback.prior_intent='backup' too (same dead guard, 4 SELECTs)."""
    client, db = admin_client
    _seed_row_with_backup_override(db, 9202, with_previous_url=True)
    r = client.post("/api/items/tv/9202/revert?section_id=3", headers=AUTH)
    assert r.status_code == 200, r.text
    rb = _job_rollback(db, 9202)
    assert rb.get("prior_intent") == "backup", (
        f"v1.22.62: REVERT rollback intent — got {rb.get('prior_intent')!r}"
    )


def test_no_intent_less_override_select_feeds_the_guards():
    """Source pin: the ACCEPT + REVERT override fetches all carry the
    intent column. Anchored per-function so unrelated youtube_url-only
    subqueries elsewhere don't false-positive."""
    for anchor, n_selects in (
        ("async def api_accept_update(", 2),
        ("async def api_revert_to_themerrdb(", 4),
    ):
        i = API_PY.index(anchor)
        body = API_PY[i:API_PY.index("\n    @app.", i + 1)]
        assert body.count('"SELECT youtube_url, intent FROM user_overrides "') \
            == n_selects, (
            f"{anchor} must fetch youtube_url, intent at its {n_selects} "
            "override SELECT(s) — a youtube_url-only fetch makes the "
            "prior_intent keys() guard a dead branch"
        )
        assert '"SELECT youtube_url FROM user_overrides "' not in body
