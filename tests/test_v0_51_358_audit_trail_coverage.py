"""v0.51.358 — the state changes audit_events exists for all write to it.

`_record_audit`'s own docstring scopes the table: "the long-lived provenance log for URL changes,
override set/clear, accept/decline decisions, and destructive theme actions", kept precisely so
"who changed Willy Wonka's theme on 2026-04-12, and what was it before?" is answerable months
later. It is deliberately not rotated; `events` prunes at 30 days.

Four changes inside that scope wrote nothing to it:

  * bulk DECLINE — the per-row DECLINE has audited since v1.12.80, so the same decision was
    recorded or not depending on which button the operator used. ACCEPT's pair was made
    symmetric in v1.19.39 for exactly this reason.
  * CONVERT TO MANUAL — writes a user_overrides row (an override set).
  * UPLOAD MP3 — creates the themes + local_files rows that decide what plays.
  * DELETE — theme row, every FK'd child, the files on disk. The least reversible action motif
    has, and after 30 days nothing said who did it.

These drive the real endpoints; a source-level check would not have caught the bulk/per-row split
that started this.
"""
from __future__ import annotations

import io
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

AUTH = {"X-Authentik-Username": "testadmin"}
TMDB = 4242


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def app_db(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    (tmp_path / "themes" / "movies").mkdir(parents=True)
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    now = _now()
    with sqlite3.connect(settings.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id,title,type,is_anime,is_4k,themes_subdir,included,"
                  "discovered_at,last_seen_at) VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (now, now))
        c.commit()
    return TestClient(create_app(settings)), settings.db_path, tmp_path


def _seed_theme(db, *, upstream="imdb", dropped=False, pending=False, url="https://y.t/watch?v=a1"):
    now = _now()
    with sqlite3.connect(db) as c:
        tid = c.execute("INSERT INTO themes (media_type,tmdb_id,title,upstream_source,last_seen_sync_at,"
                        "first_seen_sync_at,youtube_url,tdb_dropped_at) VALUES ('movie',?,?,?,?,?,?,?)",
                        (TMDB, "Willy Wonka", upstream, now, now, url, now if dropped else None)).lastrowid
        c.execute("INSERT INTO plex_items (rating_key,section_id,media_type,theme_id,guid_tmdb,title,year,"
                  "edition_key,folder_path,has_theme,local_theme_file,plex_independent_theme,"
                  "plex_theme_verified_ok,first_seen_at,last_seen_at)"
                  " VALUES ('rk1','1','movie',?,?,'Willy Wonka',1971,'','/media/1',0,0,0,1,?,?)",
                  (tid, TMDB, now, now))
        if pending:
            c.execute("INSERT INTO pending_updates (media_type,tmdb_id,section_id,edition_key,kind,"
                      "old_youtube_url,new_youtube_url,decision,detected_at)"
                      " VALUES ('movie',?,'1','','upstream_changed',?,?,'pending',?)",
                      (TMDB, url, "https://y.t/watch?v=b2", now))
            c.execute("INSERT INTO local_files (media_type,tmdb_id,section_id,edition_key,file_path,"
                      "downloaded_at,source_video_id,provenance,source_kind)"
                      " VALUES ('movie',?,'1','','movies/1.mp3',?,'a1','auto','themerrdb')", (TMDB, now))
            c.execute("INSERT INTO placements (media_type,tmdb_id,section_id,edition_key,media_folder,placed_at,"
                      "placement_kind,plex_refreshed,theme_present)"
                      " VALUES ('movie',?,'1','','/media/1',?,'hardlink',1,1)", (TMDB, now))
        c.commit()


def _audit(db, action=None):
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        sql = "SELECT * FROM audit_events"
        rows = c.execute(sql + (" WHERE action = ?" if action else ""),
                         (action,) if action else ()).fetchall()
    return [dict(r) for r in rows]


# ── the four ────────────────────────────────────────────────


def test_bulk_decline_records_each_decision(app_db):
    """The asymmetry this tag closes: the same decision, recorded or not by which button was used."""
    client, db, _ = app_db
    _seed_theme(db, pending=True)
    body = client.post("/api/updates/decline-all?tab=movies&fourk=0", headers=AUTH).json()
    assert body["declined"] == 1, body
    rows = _audit(db, "decline_update")
    assert len(rows) == 1, rows
    assert rows[0]["tmdb_id"] == TMDB and rows[0]["actor"] == "testadmin"


def test_the_per_row_decline_still_records_one(app_db):
    """The half that already worked — pinned so the two paths stay a pair."""
    client, db, _ = app_db
    _seed_theme(db, pending=True)
    r = client.post(f"/api/updates/movie/{TMDB}/decline?section_id=1", headers=AUTH)
    assert r.status_code == 200, r.text
    assert len(_audit(db, "decline_update")) == 1


def test_convert_to_manual_records_the_override_it_sets(app_db):
    client, db, _ = app_db
    _seed_theme(db, upstream="imdb", dropped=True)
    r = client.post(f"/api/items/movie/{TMDB}/convert-to-manual?section_id=1", headers=AUTH)
    assert r.status_code == 200, r.text
    rows = _audit(db, "convert_to_manual")
    assert len(rows) == 1, rows
    assert "y.t" in (rows[0]["details"] or ""), rows[0]


def test_delete_records_what_it_destroyed(app_db):
    """Only plex_orphan rows can be deleted — the endpoint's own rule."""
    client, db, _ = app_db
    _seed_theme(db, upstream="plex_orphan")
    r = client.delete(f"/api/items/movie/{TMDB}", headers=AUTH)
    assert r.status_code == 204, r.text
    rows = _audit(db, "deleted")
    assert len(rows) == 1, rows
    assert "Willy Wonka" in (rows[0]["details"] or ""), rows[0]


def test_the_delete_record_survives_the_cascade(app_db):
    """Written inside the delete's own transaction, and audit_events has no FK to themes — so the
    row that explains the deletion is not deleted with it."""
    client, db, _ = app_db
    _seed_theme(db, upstream="plex_orphan")
    client.delete(f"/api/items/movie/{TMDB}", headers=AUTH)
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT COUNT(*) FROM themes WHERE tmdb_id = ?", (TMDB,)).fetchone()[0] == 0
    assert len(_audit(db, "deleted")) == 1


def test_upload_mp3_records_the_source_it_created(app_db):
    client, db, _ = app_db
    _seed_theme(db)
    mp3 = io.BytesIO(b"ID3" + b"\0" * 2048)
    r = client.post("/api/plex_items/rk1/upload-theme", headers=AUTH,
                    files={"file": ("theme.mp3", mp3, "audio/mpeg")})
    assert r.status_code in (200, 202), r.text
    rows = _audit(db, "upload_theme")
    assert len(rows) == 1, rows
    assert rows[0]["actor"] == "testadmin"
