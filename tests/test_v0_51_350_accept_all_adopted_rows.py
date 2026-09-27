"""v0.51.350 — ACCEPT ALL UPDATES accepted nothing on a page full of pending updates.

the user, on /movies with the ATTN ! filter and 8 pending updates: the confirm said "Accept 8 pending ThemerrDB
updates in MOVIES?", and the button then read "// 0 ACCEPTED" with nothing downloaded.

Cause: sync writes the new TDB url straight into `themes.youtube_url` when it DETECTS the change (sync.py's themes
UPDATE), so a row with no `user_overrides` entry — every SRC=T, A and M row — has an "applied URL" that already
equals the pending update's new_youtube_url. The bulk handler's own no-op gate compared exactly those two and
`continue`d, so it skipped every such row: only a row with an override, a urls_match convert or a
new_theme_available detection ever got through. The count the confirm shows, the blue ! pill and the per-row accept
all use `_pending_update_actionable_sql` instead, which is why the page said 8 and the action said 0.

The gate goes: the tuples query has applied that same actionable predicate since v1.22.62, and the per-row accept
(the behaviour this bulk documents itself as mirroring) has no second gate at all.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
AUTH = {"X-Authentik-Username": "testadmin"}


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
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


NEW_URL = "https://www.youtube.com/watch?v=newvideo001"
OLD_URL = "https://www.youtube.com/watch?v=oldvideo001"


def _seed_row(db, tmdb_id, *, source_kind="adopt", override=None, section_id="1", kind="upstream_changed",
              new_url=NEW_URL, old_url=OLD_URL, themes_url=NEW_URL):
    """A row the library paints with the blue ! (a pending upstream change) and a downloaded local file.

    themes.youtube_url already holds the NEW url, which is what sync does at detection time.
    """
    now = _now()
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir,"
                     " included, discovered_at, last_seen_at) VALUES ('1','Movies','movie',0,0,'movies',1,?,?)",
                     (now, now))
        tid = conn.execute("INSERT INTO themes (media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                           " first_seen_sync_at, youtube_url) VALUES ('movie',?,?,'imdb',?,?,?)",
                           (tmdb_id, f"Title {tmdb_id}", now, now, themes_url)).lastrowid
        conn.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, year,"
                     " edition_key, folder_path, has_theme, local_theme_file, plex_independent_theme,"
                     " plex_theme_verified_ok, first_seen_at, last_seen_at)"
                     " VALUES (?, ?, 'movie', ?, ?, ?, 2024, '', ?, 1, 0, 0, 1, ?, ?)",
                     (f"rk{tmdb_id}", section_id, tid, tmdb_id, f"Title {tmdb_id}", f"/media/{tmdb_id}", now, now))
        conn.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path, downloaded_at,"
                     " source_video_id, provenance, source_kind) VALUES ('movie',?,?,'',?,?,?,'auto',?)",
                     (tmdb_id, section_id, f"movies/{tmdb_id}.mp3", now, "oldvideo001", source_kind))
        conn.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder, placed_at,"
                     " placement_kind, plex_refreshed, theme_present) VALUES ('movie',?,?,'',?,?,'hardlink',1,1)",
                     (tmdb_id, section_id, f"/media/{tmdb_id}", now))
        if override:
            conn.execute("INSERT INTO user_overrides (media_type, tmdb_id, section_id, youtube_url, set_by, set_at)"
                         " VALUES ('movie',?,?,?,'testadmin',?)", (tmdb_id, section_id, override, now))
        conn.execute("INSERT INTO pending_updates (media_type, tmdb_id, section_id, edition_key, kind,"
                     " old_youtube_url, new_youtube_url, decision, detected_at)"
                     " VALUES ('movie',?,?,'',?,?,?,'pending',?)",
                     (tmdb_id, section_id, kind, old_url, new_url, now))
        conn.commit()


def _jobs(db):
    with sqlite3.connect(db) as conn:
        return conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'download'").fetchone()[0]


def _decisions(db):
    with sqlite3.connect(db) as conn:
        return [r[0] for r in conn.execute("SELECT decision FROM pending_updates ORDER BY tmdb_id")]


SCOPE = "?tab=movies&fourk=0"


def test_accept_all_accepts_the_rows_the_count_promised(admin_client):
    client, db = admin_client
    for n in range(1, 9):  # the operator's page: 8 pending updates, none of them with a user override
        _seed_row(db, 7000 + n)
    assert client.get(f"/api/updates/count{SCOPE}", headers=AUTH).json()["pending"] == 8
    r = client.post(f"/api/updates/accept-all{SCOPE}", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["accepted"] == 8, body
    assert body["downloads_queued"] == 8, body
    assert _jobs(db) == 8
    assert _decisions(db) == ["accepted"] * 8
    # and the page it came from now has nothing pending
    assert client.get(f"/api/updates/count{SCOPE}", headers=AUTH).json()["pending"] == 0


@pytest.mark.parametrize("source_kind", ["themerrdb", "adopt", "url"])
def test_every_local_source_kind_is_accepted_not_just_the_overridden_one(admin_client, source_kind):
    client, db = admin_client
    _seed_row(db, 8001, source_kind=source_kind)
    assert client.get(f"/api/updates/count{SCOPE}", headers=AUTH).json()["pending"] == 1
    body = client.post(f"/api/updates/accept-all{SCOPE}", headers=AUTH).json()
    assert (body["accepted"], _decisions(db)) == (1, ["accepted"]), (source_kind, body)
    assert _jobs(db) == 1, source_kind


def test_a_row_whose_override_already_plays_the_new_url_is_still_accepted(admin_client):
    # kind=urls_match: the accept is the classification flip, which is why it was exempt from the old gate too
    client, db = admin_client
    _seed_row(db, 8002, override=NEW_URL, kind="urls_match", old_url=NEW_URL)
    body = client.post(f"/api/updates/accept-all{SCOPE}", headers=AUTH).json()
    # an eager flip resolves the row by REMOVING its pending entry, so "no pending left" is the outcome to assert
    assert body["accepted"] == 1 and body["eager_flipped"] == 1, body
    assert _decisions(db) in ([], ["accepted"]), _decisions(db)
    assert client.get(f"/api/updates/count{SCOPE}", headers=AUTH).json()["pending"] == 0


def test_a_pending_that_changes_nothing_is_still_left_alone(admin_client):
    """The rows the no-op gate was written for stay out — the SQL actionable gate is what keeps them out.

    TDB republished the same url on a row motif only knows from TDB: no diff, no local non-url content, so the
    count never counts it and accept-all must not touch it either.
    """
    client, db = admin_client
    now = _now()
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir,"
                     " included, discovered_at, last_seen_at) VALUES ('1','Movies','movie',0,0,'movies',1,?,?)",
                     (now, now))
        tid = conn.execute("INSERT INTO themes (media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                           " first_seen_sync_at, youtube_url) VALUES ('movie',9001,'Same','imdb',?,?,?)",
                           (now, now, NEW_URL)).lastrowid
        conn.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, year,"
                     " edition_key, folder_path, has_theme, local_theme_file, plex_independent_theme,"
                     " plex_theme_verified_ok, first_seen_at, last_seen_at)"
                     " VALUES ('rk9001','1','movie',?,9001,'Same',2024,'','/media/9001',1,0,1,1,?,?)", (tid, now, now))
        conn.execute("INSERT INTO pending_updates (media_type, tmdb_id, section_id, edition_key, kind,"
                     " old_youtube_url, new_youtube_url, decision, detected_at)"
                     " VALUES ('movie',9001,'1','','upstream_changed',?,?,'pending',?)", (NEW_URL, NEW_URL, now))
        conn.commit()
    # and the same shape on a row motif DOES track (a downloaded TDB row): the presence gate admits it, so only the
    # actionable predicate keeps it out — that is the guard the removed no-op gate must not be replaced by
    _seed_row(db, 9002, source_kind="themerrdb", old_url=NEW_URL, new_url=NEW_URL, themes_url=NEW_URL)
    assert client.get(f"/api/updates/count{SCOPE}", headers=AUTH).json()["pending"] == 0
    body = client.post(f"/api/updates/accept-all{SCOPE}", headers=AUTH).json()
    assert body["accepted"] == 0 and set(_decisions(db)) == {"pending"}, body
    assert _jobs(db) == 0


def test_the_other_tabs_rows_are_left_alone(admin_client):
    # the v0.51.316 scope still holds with the gate gone
    client, db = admin_client
    _seed_row(db, 8100)
    now = _now()
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir,"
                     " included, discovered_at, last_seen_at) VALUES ('2','TV','show',0,0,'tv',1,?,?)", (now, now))
        tid = conn.execute("INSERT INTO themes (media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                           " first_seen_sync_at, youtube_url) VALUES ('tv',8101,'Show','imdb',?,?,?)",
                           (now, now, NEW_URL)).lastrowid
        conn.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, year,"
                     " edition_key, folder_path, has_theme, local_theme_file, plex_independent_theme,"
                     " plex_theme_verified_ok, first_seen_at, last_seen_at)"
                     " VALUES ('rk8101','2','show',?,8101,'Show',2024,'','/media/8101',1,0,0,1,?,?)", (tid, now, now))
        conn.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path, downloaded_at,"
                     " source_video_id, provenance, source_kind)"
                     " VALUES ('tv',8101,'2','','tv/8101.mp3',?,'oldvideo001','auto','adopt')", (now,))
        conn.execute("INSERT INTO pending_updates (media_type, tmdb_id, section_id, edition_key, kind,"
                     " old_youtube_url, new_youtube_url, decision, detected_at)"
                     " VALUES ('tv',8101,'2','','upstream_changed',?,?,'pending',?)", (OLD_URL, NEW_URL, now))
        conn.commit()
    body = client.post(f"/api/updates/accept-all{SCOPE}", headers=AUTH).json()
    assert body["accepted"] == 1, body
    with sqlite3.connect(db) as conn:
        rows = dict(conn.execute("SELECT tmdb_id, decision FROM pending_updates").fetchall())
    assert rows == {8100: "accepted", 8101: "pending"}, rows
