"""v0.51.11 — round-4 audit Batch A (data integrity).

#1 (HIGH): cancelling an ACCEPT UPDATE / REVERT download must run the rollback
   recipe the endpoint stamped at enqueue time — worker _JobCancelled + pre-yt-dlp
   checkpoint, and the API pending-cancel + bulk cancel-pending paths all skipped
   it, stranding the half-applied override/decision state (no theme, no !UPD retry).
#9: migrate_themes_subdirs_inplace's local_files rewrite was off-by-one → wrote
   double-slash paths.
#8: the imdb→tmdb de-orphan re-key walker omitted the jobs table → a pending
   download at the synthetic id died when the walker promoted the id mid-window.
#3: place_theme classified motif's OWN just-placed hardlink as a foreign sidecar
   when the prior placements-row write had failed.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import get_conn, init_db

REPO = Path(__file__).resolve().parent.parent
AUTH = {"X-Authentik-Username": "testadmin"}
_NOW = "2026-07-02T00:00:00+00:00"


# ── #1 cancel runs the rollback recipe ──────────────────────────────────

@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


def _seed_accept_prep(db, tmdb, section="1"):
    """Simulate the state ACCEPT UPDATE leaves: decision='accepted', the user's
    override deleted, and a pending download carrying the rollback recipe."""
    recipe = {"kind": "accept_update", "replaced_user_url": "https://yt/mine",
              "prior_intent": "replace", "edition_key": "", "section_id": section}
    with get_conn(db) as c:
        c.execute(  # pending_updates FK → themes(media_type, tmdb_id)
            "INSERT INTO themes (media_type,tmdb_id,title,year,upstream_source,"
            "first_seen_sync_at,last_seen_sync_at) VALUES "
            "('movie',?,'X','2020','themoviedb',?,?)", (tmdb, _NOW, _NOW))
        c.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "decision, edition_key, detected_at) VALUES "
            "('movie', ?, ?, 'accepted', '', ?)", (tmdb, section, _NOW))
        cur = c.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, "
            "payload, status, created_at) VALUES "
            "('download', 'movie', ?, ?, ?, 'pending', ?)",
            (tmdb, section, json.dumps({"rollback": recipe}), _NOW))
        return cur.lastrowid


def _decision(db, tmdb):
    with get_conn(db) as c:
        r = c.execute("SELECT decision FROM pending_updates WHERE tmdb_id=?",
                      (tmdb,)).fetchone()
    return r["decision"] if r else None


def _override_url(db, tmdb):
    with get_conn(db) as c:
        r = c.execute("SELECT youtube_url FROM user_overrides WHERE tmdb_id=?",
                      (tmdb,)).fetchone()
    return r["youtube_url"] if r else None


def test_single_cancel_runs_accept_rollback(admin_client):
    client, db = admin_client
    jid = _seed_accept_prep(db, 5001)
    r = client.post(f"/api/jobs/{jid}/cancel", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["outcome"] == "cancelled"
    # decision back to pending + the deleted override restored
    assert _decision(db, 5001) == "pending"
    assert _override_url(db, 5001) == "https://yt/mine"


def test_bulk_cancel_runs_accept_rollback(admin_client):
    client, db = admin_client
    _seed_accept_prep(db, 5002)
    r = client.post("/api/jobs/cancel-pending?job_type=download", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["cancelled"] == 1
    assert _decision(db, 5002) == "pending"
    assert _override_url(db, 5002) == "https://yt/mine"


# ── #9 subdir migration writes single-slash paths ───────────────────────

def test_migrate_subdir_no_double_slash():
    from app.core.sections import migrate_themes_subdirs_inplace
    d = Path(tempfile.mkdtemp()) / "motif.db"
    init_db(d)
    with get_conn(d) as c:
        # stored subdir deliberately != the computed slug for 'Movies' → migrates
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,is_anime,"
            "is_4k,themes_subdir,location_paths,discovered_at,last_seen_at) "
            "VALUES ('s','Movies','movie',1,0,0,'oldmovies','[]',?,?)",
            (_NOW, _NOW))
        c.execute(  # local_files FK → themes(media_type, tmdb_id) + plex_sections
            "INSERT INTO themes (media_type,tmdb_id,title,year,upstream_source,"
            "first_seen_sync_at,last_seen_sync_at) VALUES "
            "('movie',7001,'Foo','2020','themoviedb',?,?)", (_NOW, _NOW))
        c.execute(
            "INSERT INTO local_files (media_type,tmdb_id,section_id,file_path,"
            "source_video_id,source_kind,downloaded_at) VALUES "
            "('movie',7001,'s','oldmovies/Foo (2020)/theme.mp3','vid1',"
            "'themerrdb',?)", (_NOW,))
    migrate_themes_subdirs_inplace(d, Path(tempfile.mkdtemp()))
    with get_conn(d) as c:
        fp = c.execute("SELECT file_path FROM local_files WHERE tmdb_id=7001"
                       ).fetchone()["file_path"]
    assert "//" not in fp, f"double-slash path: {fp!r}"
    assert fp.endswith("/Foo (2020)/theme.mp3"), fp


# ── #8 de-orphan re-key follows in-flight jobs ──────────────────────────

class _FakeTMDB:
    def __init__(self, api_key, db_path=None):
        self.api_key = api_key

    def lookup_by_imdb(self, imdb_id):
        if imdb_id == "tt900001":
            return {"tmdb_id": 1295026, "kind": "movie"}
        return None


def test_deorphan_rekeys_pending_job(monkeypatch):
    from app.core import deorphan
    monkeypatch.setattr("app.core.tmdb.TMDBClient", _FakeTMDB)
    d = Path(tempfile.mkdtemp()) / "motif.db"
    init_db(d)
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,is_anime,"
            "is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('s','Movies','movie',1,0,0,'m',?,?)", (_NOW, _NOW))
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,imdb_id,title,year,"
            "upstream_source,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('movie',-1,'tt900001','100 METERS','2025','plex_orphan',?,?)",
            (_NOW, _NOW))
        cur = c.execute(
            "INSERT INTO jobs (job_type,media_type,tmdb_id,status,created_at) "
            "VALUES ('download','movie',-1,'pending',?)", (_NOW,))
        jid = cur.lastrowid
    assert deorphan.resolve_orphans_in_background(
        d, api_key="k", trigger="test") is True
    # wait for the daemon thread
    for _ in range(50):
        with get_conn(d) as c:
            tid = c.execute("SELECT tmdb_id FROM jobs WHERE id=?",
                            (jid,)).fetchone()["tmdb_id"]
        if tid == 1295026:
            break
        time.sleep(0.05)
    assert tid == 1295026, f"pending job not re-keyed (still {tid})"


# ── #3 motif's own hardlink is not misread as a foreign sidecar ─────────

def test_place_own_hardlink_not_skipped_as_existing():
    from app.core.placement import place_theme, FolderIndex
    root = Path(tempfile.mkdtemp())
    src = root / "src" / "theme.mp3"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"themebytes")
    target = root / "media" / "Movie (2020)"
    target.mkdir(parents=True)
    os.link(src, target / "theme.mp3")  # motif's OWN placement (same inode)

    outcome = place_theme(
        media_type="movie", title="Movie", year="2020", edition_raw="",
        source_file=src, index=FolderIndex(), plex=None,
        cached_folder_path=str(target), skip_if_plex_has_theme=False,
        force_overwrite=False, analyze_after=False,
    )
    # own file falls through to (idempotent) re-place — NOT an existing_theme skip
    assert not (outcome.reason or "").startswith("existing_theme"), outcome.reason
    assert outcome.placed, outcome.reason


def test_place_foreign_sidecar_still_skipped():
    from app.core.placement import place_theme, FolderIndex
    root = Path(tempfile.mkdtemp())
    src = root / "src" / "theme.mp3"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"themebytes")
    target = root / "media" / "Movie (2020)"
    target.mkdir(parents=True)
    (target / "theme.mp3").write_bytes(b"someone-elses")  # foreign, distinct inode

    outcome = place_theme(
        media_type="movie", title="Movie", year="2020", edition_raw="",
        source_file=src, index=FolderIndex(), plex=None,
        cached_folder_path=str(target), skip_if_plex_has_theme=False,
        force_overwrite=False, analyze_after=False,
    )
    assert (outcome.reason or "").startswith("existing_theme"), outcome.reason
    assert not outcome.placed
