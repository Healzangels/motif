"""v1.22.1 — SET URL's pre-enqueue download-cancel must be edition+section
scoped, not title-wide.

The v1.22.0 line-close made the SET URL *enqueue* edition-scoped (the download
job carries payload.edition_key). But the cancel two lines above it stayed
title-wide (`media_type=? AND tmdb_id=?` only) since v1.11.0 — so a SET URL on
the Extended edition flipped a still-valid Standard (or sibling-section)
pending download to 'cancelled' before enqueuing Extended's. The code-review of
the v1.22.0 cut flagged it as the one site the v1.21.82 edition-scoped-cancel
arc missed. This pins the fix: SET URL on one edition leaves a sibling's
pending download untouched, and SET URL on the SAME edition still replaces its
own queued download.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


NOW = "2026-06-04T00:00:00Z"
AUTH = {"X-Authentik-Username": "testadmin"}
YT = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"


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


def _seed_two_editions(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at) VALUES (90,'movie',900,'W',"
            "'imdb',?,?)", (NOW, NOW))
        for rk, ek, folder in (
            ("rk-std", "", "/d/W (2000)"),
            ("rk-ext", "extended", "/d/W (2000) {edition-Extended}"),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1','movie',"
                "90,900,'W',?,?,0,?,?)", (rk, ek, folder, NOW, NOW))
        conn.commit()


def _add_download_job(db, *, section_id, payload, status="pending"):
    with sqlite3.connect(db) as conn:
        cur = conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at) VALUES ('download','movie',900,?,"
            "?,?,?)", (section_id, payload, status, NOW))
        conn.commit()
        return cur.lastrowid


def _status(db, job_id):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()[0]


def test_set_url_on_extended_does_not_cancel_standard_pending(client):
    """The anti-regression: a pending STANDARD ('' ) download must survive a
    SET URL on the Extended edition (pre-fix it was cancelled title-wide)."""
    c, db = client
    _seed_two_editions(db)
    # A standard-edition pending download already queued (payload omits
    # edition_key, the v1.22.0 shape for '' → COALESCE resolves '').
    std_job = _add_download_job(db, section_id="1", payload="{}")

    r = c.post("/api/plex_items/rk-ext/manual-url", headers=AUTH,
               json={"youtube_url": YT})
    assert r.status_code == 200, r.text

    # The Standard download is a different edition — it must NOT be cancelled.
    assert _status(db, std_job) == "pending", (
        "SET URL on Extended cancelled the sibling Standard download")
    # And a fresh Extended download was enqueued.
    with sqlite3.connect(db) as conn:
        ext = [json.loads(p) for (p, st) in conn.execute(
            "SELECT payload, status FROM jobs WHERE job_type='download' AND"
            " tmdb_id=900 AND status='pending'")
            if json.loads(p).get("edition_key") == "extended"]
    assert len(ext) == 1, "expected exactly one fresh Extended download"


def test_set_url_on_extended_replaces_its_own_pending(client):
    """The same-edition case still works: a pending Extended download IS
    cancelled + re-enqueued (the SET URL supersedes it)."""
    c, db = client
    _seed_two_editions(db)
    ext_job = _add_download_job(
        db, section_id="1", payload=json.dumps({"edition_key": "extended"}))

    r = c.post("/api/plex_items/rk-ext/manual-url", headers=AUTH,
               json={"youtube_url": YT})
    assert r.status_code == 200, r.text

    # The prior Extended job is cancelled; a new Extended job replaces it.
    assert _status(db, ext_job) == "cancelled", (
        "SET URL on Extended should cancel its OWN prior pending download")
    with sqlite3.connect(db) as conn:
        fresh = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='download' AND tmdb_id=900"
            " AND status='pending' AND id != ?", (ext_job,)).fetchone()[0]
    assert fresh == 1, "expected exactly one fresh pending Extended download"


def test_set_url_does_not_cancel_other_section_pending(client):
    """Cross-section scope: the same title queued in a sibling section keeps
    its pending download (mirrors the re-download v1.12.73 section scope)."""
    c, db = client
    _seed_two_editions(db)
    # A second section owns the same title with its own standard pending dl.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('2','Movies 4K','movie',0,1,'movies4k',1,?,?)", (NOW, NOW))
        conn.commit()
    other = _add_download_job(db, section_id="2", payload="{}")

    r = c.post("/api/plex_items/rk-ext/manual-url", headers=AUTH,
               json={"youtube_url": YT})
    assert r.status_code == 200, r.text
    assert _status(db, other) == "pending", (
        "SET URL in section 1 cancelled a sibling section's pending download")


def test_v1_22_1_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
