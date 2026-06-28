"""v1.21.73 — REVERT edition scope (rating_key plumbing).

Audit-found. api_revert_to_themerrdb reset the accepted pending_update with
`WHERE media_type=? AND tmdb_id=? AND decision='accepted'` — not even
section-scoped — so REVERT on ONE edition resurrected the blue !UPD pill on
EVERY edition/section of the title. The restored override was also written
at edition_key='' (not the clicked edition), and the themerrdb-branch drop
wasn't edition-scoped.

REVERT now takes the clicked row's rating_key, resolves its edition, and
scopes the override INSERT/DELETE + the pending_update reset to
(section, edition). The previous_urls snapshot stays per-section (3-col PK,
intentionally not widened) — the override WRITE is what's per-edition.
"""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.editions import edition_key_for_folder
from app.core.events import now_iso


NOW = now_iso()
A_RK = "167699"
B_RK = "676271"
A_FOLDER = "/data/Movies/LotR (2001) {edition-A}"
B_FOLDER = "/data/Movies/LotR (2001) {edition-B}"
A_KEY = edition_key_for_folder(A_FOLDER)   # 'a'
B_KEY = edition_key_for_folder(B_FOLDER)   # 'b'
PREV = "https://www.youtube.com/watch?v=prev0000001"


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    return TestClient(app), db


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',120,'LotR','2001','imdb',?,?,'u')",
            (NOW, NOW))
        tid = cur.lastrowid
        for rk, ek, fp in ((A_RK, A_KEY, A_FOLDER), (B_RK, B_KEY, B_FOLDER)):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1',"
                "'movie',?,120,'LotR','2001',?,?,1,?,?)",
                (rk, tid, ek, fp, NOW, NOW))
            # Both editions have an ACCEPTED pending update.
            conn.execute(
                "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
                " edition_key, detected_at, kind, decision, new_youtube_url)"
                " VALUES ('movie',120,'1',?,?,'upstream_changed','accepted',?)",
                (ek, NOW, f"https://www.youtube.com/watch?v=new00000{ek}"))
        # Section-level 'user' snapshot to revert to.
        conn.execute(
            "INSERT INTO previous_urls (media_type, tmdb_id, section_id,"
            " youtube_url, kind, captured_at) VALUES ('movie',120,'1',?,"
            "'user',?)", (PREV, NOW))
        conn.commit()


def _pu_decisions(db):
    with sqlite3.connect(db) as conn:
        return dict(conn.execute(
            "SELECT edition_key, decision FROM pending_updates"
            " WHERE tmdb_id=120").fetchall())


def test_revert_resets_only_clicked_editions_pending_update(app_client):
    client, db = app_client
    _seed(db)
    r = client.post(
        f"/api/items/movie/120/revert?section_id=1&rating_key={A_RK}",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text

    decisions = _pu_decisions(db)
    assert decisions[A_KEY] == "pending", decisions
    assert decisions[B_KEY] == "accepted", (
        f"REVERT on edition A reset edition B's accepted update: {decisions}")

    # The restored override landed on edition A (not the shared '').
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT edition_key, youtube_url FROM user_overrides"
            " WHERE tmdb_id=120").fetchall()
    ovr = {ek: url for ek, url in rows}
    assert ovr.get(A_KEY) == PREV, ovr
