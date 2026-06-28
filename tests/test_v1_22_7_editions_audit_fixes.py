"""v1.22.7 — editions code-review fixes (3 verified data-loss/drift findings).

#1 INFO-card LET PLEX SERVE (purge-and-ack) threaded no rating_key → backend
   section-wide DELETE nuked every edition's placement (v1.22.2 bleed, 3rd site).
#2 REVERT's pre-enqueue download-cancel was title-wide while its enqueue is
   per-edition+section → cancelled a sibling edition's pending download.
#3 api_item INFO '' read-fallback was ungated while the unplace WRITE gates on
   single-edition → card claimed "placed" on editions LPS withholds (the user's
   Watchmen "card says placed, LPS does 0/0").
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


NOW = "2026-06-06T00:00:00Z"
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


def _section(conn):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))


def _theme(conn, tid, tmdb, mt='movie'):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at) VALUES (?,?,?,'X','imdb',?,?)",
        (tid, mt, tmdb, NOW, NOW))


def _item(conn, rk, tid, tmdb, ek, folder, mt='movie'):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_tmdb, title, edition_key, folder_path, has_theme, first_seen_at,"
        " last_seen_at) VALUES (?,'1',?,?,?,'X',?,?,1,?,?)",
        (rk, mt, tid, tmdb, ek, folder, NOW, NOW))


def _placement(conn, tid, tmdb, ek, folder, mt='movie'):
    conn.execute(
        "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
        " edition_key, media_folder, placed_at, placement_kind, plex_refreshed)"
        " VALUES (?,?,?,'1',?,?,?,'hardlink',1)", (tid, mt, tmdb, ek, folder, NOW))


def _local(conn, tmdb, ek, mt='movie'):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind)"
        " VALUES (?,?,'1',?,?,?,'v','auto','themerrdb')",
        (mt, tmdb, ek, f"movies/X {ek or 'std'}.mp3", NOW))


# ── #3: api_item INFO '' read-fallback gated on single-edition ──


def test_info_card_no_empty_fallback_on_multi_edition(client):
    c, db = client
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 90, 900)
        # TWO tagged editions, no own placement for either; one shared '' row.
        _item(conn, "rk-a", 90, 900, "theatrical", "/d/X (2000) {edition-Theatrical}")
        _item(conn, "rk-b", 90, 900, "midnight", "/d/X (2000) {edition-Midnight}")
        _placement(conn, 90, 900, "", "/d/X (2000)")
        _local(conn, 900, "")
        conn.commit()
    # INFO on Midnight (no own placement) — multi-edition → must NOT fall back
    # to the shared '' rows (the read/write drift the gate closes).
    r = c.get("/api/items/movie/900?section_id=1&rating_key=rk-b", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["placements"] == [], (
        "multi-edition INFO card must NOT surface the shared '' placement as "
        "this edition's — the unplace write withholds it", body["placements"])
    assert body["local_files"] == []


def test_info_card_empty_fallback_still_works_single_edition(client):
    """The Godzilla single-edition recovery (count==1) must still fall back."""
    c, db = client
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 91, 901)
        # ONE tagged edition + a shared/legacy '' placement (the mis-key shape).
        _item(conn, "rk-c", 91, 901, "minus color", "/d/Y (2000) {edition-Minus Color}")
        _placement(conn, 91, 901, "", "/d/Y (2000)")
        _local(conn, 901, "")
        conn.commit()
    r = c.get("/api/items/movie/901?section_id=1&rating_key=rk-c", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["placements"]) == 1, (
        "single-edition must still fall back to the '' placement (Godzilla fix)")


# ── #2: REVERT cancel is edition-scoped ──


def test_revert_cancel_does_not_cancel_sibling_edition(client):
    c, db = client
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 92, 902, mt='tv')
        for rk, ek, folder in (("rk-std", "", "/d/W (2000)"),
                               ("rk-ext", "extended", "/d/W (2000) {edition-Extended}")):
            _item(conn, rk, 92, 902, ek, folder, mt='show')  # plex_items: tv→show
        # A snapshot so REVERT doesn't 409.
        conn.execute(
            "INSERT INTO previous_urls (media_type,tmdb_id,section_id,youtube_url,"
            "kind,captured_at) VALUES ('tv',902,'1','https://y/watch?v=prev','user',?)",
            (NOW,))
        # A pending STANDARD ('' ) download already queued.
        cur = conn.execute(
            "INSERT INTO jobs (job_type,media_type,tmdb_id,section_id,payload,"
            "status,created_at) VALUES ('download','tv',902,'1','{}','pending',?)",
            (NOW,))
        std_job = cur.lastrowid
        conn.commit()

    r = c.post("/api/items/tv/902/revert?section_id=1&rating_key=rk-ext", headers=AUTH)
    assert r.status_code == 200, r.text
    with sqlite3.connect(db) as conn:
        st = conn.execute("SELECT status FROM jobs WHERE id=?", (std_job,)).fetchone()[0]
    assert st == "pending", (
        "REVERT on Extended must NOT cancel the Standard edition's pending download")


# ── #1: INFO-card LET PLEX SERVE threads rating_key (source pin) ──


def test_info_card_lps_threads_rating_key():
    src = (Path(__file__).resolve().parent.parent
           / "app" / "web" / "static" / "app.js").read_text()
    idx = src.index("act === 'purge-and-ack'")
    block = src[idx:idx + 4000]
    # The unplace URL must carry the clicked rating_key (was section_id only).
    assert "rating_key=${encodeURIComponent(ratingKey)}" in block, (
        "purge-and-ack unplace must thread rating_key or it nukes all editions")
    # The folder-hint resolver must prefer the clicked rk, not an ambiguous find.
    assert "String(row.rating_key) === String(ratingKey)" in block


def test_v1_22_7_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
