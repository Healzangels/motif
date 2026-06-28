"""v1.21.82 — re-download / replace / backup cancel-jobs respect editions.

The RE-DOWNLOAD TDB / DOWNLOAD TDB BACKUP / REPLACE TDB actions all enqueue an
edition-scoped download (v1.21.62-66) but PRE-cancelled in-flight jobs by
(media_type, tmdb_id, section_id) only — so re-downloading one edition
silently cancelled a SIBLING edition's queued download in the same section
(enqueue edition-scoped, cancel not — asymmetric).

The cancel is now edition-scoped to match the enqueue, via the v74
json_extract(payload,'$.edition_key') pattern (jobs has no edition_key column;
a standard '' job omits it -> COALESCE '').
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
THEAT_FOLDER = "/data/Movies/LotR (2001) {edition-Theatrical}"
EXT_FOLDER = "/data/Movies/LotR (2001) {edition-Extended}"


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
            " youtube_url) VALUES ('movie',?,'LotR','2001','imdb',?,?,"
            "'https://www.youtube.com/watch?v=tdburl00001')", (TMDB, NOW, NOW))
        tid = cur.lastrowid
        for rk, ek, fp in ((THEAT_RK, "theatrical", THEAT_FOLDER),
                           (EXT_RK, "extended", EXT_FOLDER)):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1',"
                "'movie',?,?,'LotR','2001',?,?,1,?,?)",
                (rk, tid, TMDB, ek, fp, NOW, NOW))
        conn.commit()
    return tid


def _seed_pending_download(db, edition_key):
    """Queue a pending download job for one edition (payload carries the key)."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at, next_run_at)"
            " VALUES ('download','movie',?,'1',?,'pending',?,?)",
            (TMDB, json.dumps({"edition_key": edition_key}), NOW, NOW))
        conn.commit()


def _job_statuses_by_edition(db):
    out = {}
    with sqlite3.connect(db) as conn:
        for payload, status in conn.execute(
                "SELECT payload, status FROM jobs WHERE job_type='download'"
                " AND tmdb_id=?", (TMDB,)):
            ek = (json.loads(payload).get("edition_key", "") if payload else "")
            out.setdefault(ek, []).append(status)
    return out


# ── behavioral ───────────────────────────────────────────────────────


def test_redownload_does_not_cancel_sibling_edition_download(app_client):
    client, db = app_client
    _seed(db)
    _seed_pending_download(db, "theatrical")   # sibling, must survive

    r = client.post(
        f"/api/items/movie/{TMDB}/redownload?section_id=1&rating_key={EXT_RK}",
        headers=AUTH)
    assert r.status_code == 200, r.text

    by_ed = _job_statuses_by_edition(db)
    assert by_ed.get("theatrical") == ["pending"], (
        "Theatrical's queued download must survive a re-download of Extended",
        by_ed)
    # Extended got a fresh download enqueued.
    assert "pending" in by_ed.get("extended", []), by_ed


def test_backup_does_not_cancel_sibling_edition_download(app_client):
    client, db = app_client
    _seed(db)
    _seed_pending_download(db, "theatrical")

    r = client.post(
        f"/api/items/movie/{TMDB}/download-backup?section_id=1&rating_key={EXT_RK}",
        headers=AUTH)
    assert r.status_code == 200, r.text
    by_ed = _job_statuses_by_edition(db)
    assert by_ed.get("theatrical") == ["pending"], (
        "Theatrical's queued download must survive a backup of Extended", by_ed)


def test_redownload_still_cancels_same_edition_stale_job(app_client):
    """The cancel must still fire for the TARGET edition's own stale job
    (the reason it exists) — only the cross-edition bleed is removed."""
    client, db = app_client
    _seed(db)
    _seed_pending_download(db, "extended")     # a stale Extended job

    client.post(
        f"/api/items/movie/{TMDB}/redownload?section_id=1&rating_key={EXT_RK}",
        headers=AUTH)
    # The stale Extended job was cancelled; the fresh enqueue (ON CONFLICT
    # upsert on the same PK) leaves exactly one pending Extended job.
    by_ed = _job_statuses_by_edition(db)
    assert by_ed.get("extended", []).count("pending") == 1, by_ed


# ── source lint: all three re-download-family cancels carry the clause ──


def test_all_redownload_family_cancels_are_edition_scoped():
    api = (REPO / "app" / "web" / "api.py").read_text()
    adopt = (REPO / "app" / "core" / "adopt.py").read_text()
    clause = "json_extract(payload, '$.edition_key')"
    # api_redownload + api_download_backup each cancel in two branches.
    assert api.count(clause) >= 4, (
        "api_redownload + api_download_backup cancels must filter by "
        "payload edition_key in both section + fan-out branches")
    assert clause in adopt, (
        "adopt.replace_with_themerrdb cancel must filter by payload edition_key")


def test_v1_21_82_version_pin():
    # Prefix-only — the canonical exact-version guard is test_v1_13_79.
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
