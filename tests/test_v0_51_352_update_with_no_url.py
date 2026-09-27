"""v0.51.352 — an update with nothing to apply is not an update.

the user, on /tv with 5 pending updates: accepting them changed nothing and the pill stayed lit. The container log
shows the loop for every one of them:

    Job 8600 (download) starting
    Job 8600 permanently failed: no YouTube URL configured for this theme — SET URL or UPLOAD MP3 to provide a source
    rollback: re-pended accept-update failure on tv/241554/2 + restored override

Those titles have `themes.youtube_url IS NULL` — ThemerrDB publishes the title but carries no theme url for it. The
row is only themed because the operator set their own url. Accepting is defined as "take ThemerrDB's version", so it
deletes the override and queues a download, and the download then resolves override → theme url → nothing. The
rollback puts the override back and re-pends the row, so the action churns for ever and the blue ! never clears.

The gate that decides whether a pending is worth surfacing already means to exclude this shape (its own docstring
calls out "an url-less upstream_changed"), but only through the old→new diff branch: a row admitted by any other
branch still lit the pill. It now requires, for every branch, that there is a url to apply.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
AUTH = {"X-Authentik-Username": "testadmin"}
TV_SCOPE = "?tab=tv&fourk=0"
OVERRIDE_URL = "https://www.youtube.com/watch?v=mine00000001"
TDB_URL = "https://www.youtube.com/watch?v=tdb000000001"


def _now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings.db_path


def _seed(db, tmdb_id, *, theme_url, new_url, kind, override=OVERRIDE_URL, source_kind="url"):
    """A themed TV row carrying the operator's own url, with a pending update from ThemerrDB."""
    now = _now()
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir,"
                     " included, discovered_at, last_seen_at) VALUES ('2','TV','show',0,0,'tv',1,?,?)", (now, now))
        tid = conn.execute("INSERT INTO themes (media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                           " first_seen_sync_at, youtube_url) VALUES ('tv',?,?,'imdb',?,?,?)",
                           (tmdb_id, f"Show {tmdb_id}", now, now, theme_url)).lastrowid
        conn.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, year,"
                     " edition_key, folder_path, has_theme, local_theme_file, plex_independent_theme,"
                     " plex_theme_verified_ok, first_seen_at, last_seen_at)"
                     " VALUES (?, '2', 'show', ?, ?, ?, 2025, '', ?, 1, 0, 0, 1, ?, ?)",
                     (f"rk{tmdb_id}", tid, tmdb_id, f"Show {tmdb_id}", f"/media/{tmdb_id}", now, now))
        conn.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path,"
                     " downloaded_at, source_video_id, provenance, source_kind)"
                     " VALUES ('tv',?, '2','',?,?,'mine00000001','manual',?)",
                     (tmdb_id, f"tv/{tmdb_id}.mp3", now, source_kind))
        conn.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder, placed_at,"
                     " placement_kind, plex_refreshed, theme_present) VALUES ('tv',?,'2','',?,?,'hardlink',1,1)",
                     (tmdb_id, f"/media/{tmdb_id}", now))
        if override:
            conn.execute("INSERT INTO user_overrides (media_type, tmdb_id, section_id, youtube_url, set_by, set_at)"
                         " VALUES ('tv',?, '2',?,'testadmin',?)", (tmdb_id, override, now))
        conn.execute("INSERT INTO pending_updates (media_type, tmdb_id, section_id, edition_key, kind,"
                     " old_youtube_url, new_youtube_url, decision, detected_at)"
                     " VALUES ('tv',?, '2','',?,?,?,'pending',?)", (tmdb_id, kind, OVERRIDE_URL, new_url, now))
        conn.commit()


def _state(db, tmdb_id):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        pu = conn.execute("SELECT decision FROM pending_updates WHERE tmdb_id = ?", (tmdb_id,)).fetchone()
        ovr = conn.execute("SELECT youtube_url FROM user_overrides WHERE tmdb_id = ?", (tmdb_id,)).fetchone()
        jobs = conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'download'").fetchone()[0]
    return {"decision": pu["decision"] if pu else None, "override": ovr["youtube_url"] if ovr else None,
            "download_jobs": jobs}


# the three ways a url-less pending reached the page: a detection whose url ThemerrDB later removed, a convert offer,
# and a row whose local content is not a url at all (the branch that admitted the operator's rows)
URL_LESS = [("new_theme_available", None, "url"), ("urls_match", None, "url"), ("upstream_changed", None, "adopt")]


@pytest.mark.parametrize("kind,new_url,source_kind", URL_LESS, ids=[k for k, _, _ in URL_LESS])
def test_a_pending_with_no_url_to_apply_is_not_counted(admin_client, kind, new_url, source_kind):
    client, db = admin_client
    _seed(db, 241554, theme_url=None, new_url=new_url, kind=kind, source_kind=source_kind)
    assert client.get(f"/api/updates/count{TV_SCOPE}", headers=AUTH).json()["pending"] == 0, kind


@pytest.mark.parametrize("kind,new_url,source_kind", URL_LESS, ids=[k for k, _, _ in URL_LESS])
def test_accepting_one_keeps_the_operators_url_and_queues_nothing(admin_client, kind, new_url, source_kind):
    """The churn: accept deletes the override so the row can fall back to ThemerrDB's url — and there is none."""
    client, db = admin_client
    _seed(db, 241554, theme_url=None, new_url=new_url, kind=kind, source_kind=source_kind)
    body = client.post(f"/api/updates/accept-all{TV_SCOPE}", headers=AUTH).json()
    assert body["accepted"] == 0, body
    after = _state(db, 241554)
    assert after["override"] == OVERRIDE_URL, "the operator's url must survive — the download had nothing to replace it with"
    assert after["download_jobs"] == 0, "queueing a download with no source is the failure the log shows"
    assert after["decision"] == "pending"


def test_an_empty_url_counts_as_none(admin_client):
    """ThemerrDB has carried '' as well as NULL; both mean there is nothing to download."""
    client, db = admin_client
    _seed(db, 241554, theme_url="", new_url="", kind="upstream_changed")
    assert client.get(f"/api/updates/count{TV_SCOPE}", headers=AUTH).json()["pending"] == 0
    body = client.post(f"/api/updates/accept-all{TV_SCOPE}", headers=AUTH).json()
    assert body["accepted"] == 0, body
    after = _state(db, 241554)
    assert (after["override"], after["download_jobs"]) == (OVERRIDE_URL, 0), after


def test_the_per_row_accept_refuses_it_too(admin_client):
    client, db = admin_client
    _seed(db, 241554, theme_url=None, new_url=None, kind="new_theme_available")
    r = client.post("/api/updates/tv/241554/accept?section_id=2&rating_key=rk241554", headers=AUTH)
    assert r.status_code == 409, r.text
    assert "no url" in r.json()["detail"].lower() or "nothing to" in r.json()["detail"].lower(), r.json()
    after = _state(db, 241554)
    assert (after["override"], after["download_jobs"], after["decision"]) == (OVERRIDE_URL, 0, "pending")


def test_a_real_update_still_works(admin_client):
    """The premise: with a url on the ThemerrDB side, the same row accepts and queues its download."""
    client, db = admin_client
    _seed(db, 299167, theme_url=TDB_URL, new_url=TDB_URL, kind="upstream_changed")
    assert client.get(f"/api/updates/count{TV_SCOPE}", headers=AUTH).json()["pending"] == 1
    body = client.post(f"/api/updates/accept-all{TV_SCOPE}", headers=AUTH).json()
    assert body["accepted"] == 1, body
    after = _state(db, 299167)
    assert after["download_jobs"] == 1 and after["override"] is None, after


def test_a_url_arriving_later_lights_the_row_again(admin_client):
    """ThemerrDB publishing a url for the title is what makes the pending actionable again."""
    client, db = admin_client
    _seed(db, 241609, theme_url=None, new_url=None, kind="new_theme_available")
    assert client.get(f"/api/updates/count{TV_SCOPE}", headers=AUTH).json()["pending"] == 0
    with sqlite3.connect(db) as conn:  # the next sync finds one
        conn.execute("UPDATE themes SET youtube_url = ? WHERE tmdb_id = 241609", (TDB_URL,))
        conn.commit()
    assert client.get(f"/api/updates/count{TV_SCOPE}", headers=AUTH).json()["pending"] == 1
