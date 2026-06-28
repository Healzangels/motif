"""v1.21.87 — ACCEPT UPDATE override-delete + provenance-flip + download, and
REVERT download, respect editions.

v1.21.81 made the ACCEPT/DECLINE *decision* per-edition, but the audit found the
rest of ACCEPT still section-only: the user_overrides delete, the url_match
local_files provenance flip, and the _enqueue_download all ignored the edition,
so accepting one edition dropped a sibling's override, flipped its provenance,
and downloaded the standard edition. REVERT's re-download enqueue was likewise
edition-blind. All now thread the resolved edition.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent
NOW = now_iso()
AUTH = {"X-Authentik-Username": "testadmin"}
TMDB = 120
THEAT_RK = "111"
EXT_RK = "222"
THEAT_FOLDER = "/data/Movies/LotR (2001)"
EXT_FOLDER = "/data/Movies/LotR (2001) {edition-Extended}"
OLD = "https://www.youtube.com/watch?v=oldoldold01"
NEW = "https://www.youtube.com/watch?v=newnewnew01"
THEAT_URL = "https://www.youtube.com/watch?v=theatheat01"


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
    return TestClient(create_app(settings)), db


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',?,'LotR','2001','imdb',?,?,?)",
            (TMDB, NOW, NOW, OLD))
        tid = cur.lastrowid
        # url for each edition: Extended's == NEW (url_match on accept).
        for rk, ek, fp, url in (
            (THEAT_RK, "", THEAT_FOLDER, THEAT_URL),
            (EXT_RK, "extended", EXT_FOLDER, NEW),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1',"
                "'movie',?,?,'LotR','2001',?,?,1,?,?)",
                (rk, tid, TMDB, ek, fp, NOW, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',?,'1',?,?,?,"
                "'v','manual','url')", (TMDB, ek, f"movies/{ek or 'std'}.mp3", NOW))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " theme_id, media_folder, placed_at, placement_kind,"
                " plex_refreshed, provenance, edition_key) VALUES ('movie',"
                "?,'1',?,?,?, 'hardlink',1,'manual',?)", (TMDB, tid, fp, NOW, ek))
            conn.execute(
                "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,"
                " set_at, set_by, section_id, edition_key) VALUES ('movie',"
                "?,?,?,'testadmin','1',?)", (TMDB, url, NOW, ek))
        # detection at edition_key='' — new url == Extended's override (url_match)
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
            " decision, detected_at, old_youtube_url, new_youtube_url, kind)"
            " VALUES ('movie',?,'1','pending',?,?,?,'upstream_changed')",
            (TMDB, NOW, OLD, NEW))
        conn.commit()
    return tid


def _ovr(db, ek):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT youtube_url FROM user_overrides WHERE media_type='movie'"
            " AND tmdb_id=? AND edition_key=?", (TMDB, ek)).fetchone()


def _src_kind(db, ek):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT source_kind FROM local_files WHERE media_type='movie'"
            " AND tmdb_id=? AND edition_key=?", (TMDB, ek)).fetchone()[0]


def test_accept_scopes_override_provenance_and_download_to_edition(app_client):
    client, db = app_client
    _seed(db)
    r = client.post(
        f"/api/updates/movie/{TMDB}/accept?section_id=1&rating_key={EXT_RK}",
        headers=AUTH)
    assert r.status_code == 200, r.text

    # Extended's override deleted (url_match accept); Theatrical's survives.
    assert _ovr(db, "extended") is None, "Extended's override should be dropped"
    assert _ovr(db, "") is not None, "Theatrical's override must survive"

    # Extended's local_files flipped to themerrdb; Theatrical's stays url.
    assert _src_kind(db, "extended") == "themerrdb", "Extended flips to T"
    assert _src_kind(db, "") == "url", "Theatrical stays U"

    # The enqueued download is keyed to Extended.
    with sqlite3.connect(db) as conn:
        payloads = [json.loads(p) for (p,) in conn.execute(
            "SELECT payload FROM jobs WHERE job_type='download' AND tmdb_id=?",
            (TMDB,))]
    assert payloads and payloads[0].get("edition_key") == "extended", payloads


def test_revert_download_carries_edition():
    # source pin: both REVERT _enqueue_download calls thread the resolved edition.
    src = (REPO / "app" / "web" / "api.py").read_text()
    rev = src.index("async def api_revert_to_themerrdb")
    body = src[rev:rev + 24000]
    assert body.count("edition_key=_rev_edition or \"\"") == 2, (
        "both REVERT _enqueue_download calls must pass the resolved edition")


def test_v1_21_87_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
