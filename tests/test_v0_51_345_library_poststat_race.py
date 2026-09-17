"""v0.51.345: two-phase /api/library rows keep their slot when a placement changes mid-request."""
from __future__ import annotations

import contextlib
import sqlite3
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db

AUTH = {"X-Authentik-Username": "testadmin"}
PER_PAGE = 6
FOLDER_A = "/nonexistent/Charlie A"
FOLDER_B = "/nonexistent/Charlie B"
# dl on+off+broken matches every row, and "on" routes the request through the post-stat path.
POST_STAT = {"dl_pills": "on,off,broken"}
# (rating_key, title, downloaded, placement folders); r3's two folders fan its row out in the join.
ROWS = [
    ("r1", "Alpha", True, ()),
    ("r2", "Bravo", True, ("/nonexistent/Bravo",)),
    ("r3", "Charlie", True, (FOLDER_A, FOLDER_B)),
    ("r4", "Delta", False, ()),
    ("r5", "Echo", True, ("/nonexistent/Echo",)),
    ("r6", "Foxtrot", False, ()),
]


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _seed(db):
    now = _now()
    with contextlib.closing(sqlite3.connect(db)) as c, c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included,"
                  " discovered_at, last_seen_at) VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)", (now, now))
        for n, (rk, title, downloaded, folders) in enumerate(ROWS, start=1):
            tmdb = 900 + n
            c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                      " first_seen_sync_at, youtube_url) VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, ?)",
                      (n, tmdb, title, now, now, f"https://www.youtube.com/watch?v=vid{n:08d}"))
            c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title,"
                      " edition_key, folder_path, has_theme, local_theme_file, plex_independent_theme,"
                      " plex_theme_verified_ok, first_seen_at, last_seen_at)"
                      " VALUES (?, '1', 'movie', ?, ?, ?, '', ?, 0, 0, 0, 1, ?, ?)",
                      (rk, n, tmdb, title, f"/nonexistent/{title}", now, now))
            if downloaded:
                c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path,"
                          " downloaded_at, source_video_id, provenance, source_kind)"
                          " VALUES ('movie', ?, '1', '', ?, ?, ?, 'auto', 'themerrdb')",
                          (tmdb, f"movie/{title}.mp3", now, f"vid{n:08d}"))
            for folder in folders:
                c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder,"
                          " placed_at, placement_kind) VALUES ('movie', ?, '1', '', ?, ?, 'hardlink')",
                          (tmdb, folder, now))


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.web import api
    monkeypatch.setattr(api, "log_event", lambda *a, **k: None)
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    _seed(s.db_path)
    return TestClient(api.create_app(s)), s.db_path, api


def _page(tc, page, **params):
    r = tc.get("/api/library", params={"tab": "movies", "page": page, "per_page": PER_PAGE, "sort": "title",
                                       **params}, headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()


def _walk(tc, **params):
    pages = []
    for page in range(1, len(ROWS) * 4):
        items = _page(tc, page, **params)["items"]
        if not items:
            return pages
        pages.append(items)
    raise AssertionError("pagination never ran out of rows")


@pytest.mark.parametrize("params", [{}, {"sort": "src"}, POST_STAT], ids=["default", "sort-src", "post-stat"])
def test_each_placement_folder_surfaces_once_per_slot(client, params):
    tc, _db, _api = client
    pages = _walk(tc, **params)
    rows = [it for items in pages for it in items]
    assert all(len(items) <= PER_PAGE for items in pages)
    assert {it["rating_key"] for it in rows} == {rk for rk, *_ in ROWS}
    assert {it["media_folder"] for it in rows if it["rating_key"] == "r3"} == {FOLDER_A, FOLDER_B}


def _ident(c, rk):
    return c.execute("SELECT t.media_type, t.tmdb_id, pi.section_id FROM plex_items pi"
                     " JOIN themes t ON t.id = pi.theme_id WHERE pi.rating_key = ?", (rk,)).fetchone()


def _place(c, rk):
    c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder, placed_at,"
              " placement_kind) VALUES (?, ?, ?, '', '/nonexistent/placed mid-request', ?, 'hardlink')",
              (*_ident(c, rk), _now()))


def _unplace(c, rk):
    c.execute("DELETE FROM placements WHERE media_type = ? AND tmdb_id = ? AND section_id = ?", _ident(c, rk))


def _move(c, rk):
    c.execute("UPDATE placements SET media_folder = media_folder || ' moved'"
              " WHERE media_type = ? AND tmdb_id = ? AND section_id = ?", _ident(c, rk))


def _drop_folder_b(c, rk):
    c.execute("DELETE FROM placements WHERE media_type = ? AND tmdb_id = ? AND section_id = ? AND media_folder = ?",
              (*_ident(c, rk), FOLDER_B))


def _delete_item(c, rk):
    c.execute("DELETE FROM plex_items WHERE rating_key = ?", (rk,))


RACES = [
    ("unplaced-becomes-placed", "r1", _place, True),
    ("placed-becomes-unplaced", "r2", _unplace, True),
    ("placement-folder-moved", "r2", _move, True),
    ("fan-out-loses-a-folder", "r3", _drop_folder_b, True),
    ("plex-item-deleted", "r2", _delete_item, False),
]


@pytest.mark.parametrize("victim,write,survives", [r[1:] for r in RACES], ids=[r[0] for r in RACES])
def test_post_stat_rows_keep_their_slot_across_a_write_before_hydration(client, monkeypatch, victim, write,
                                                                          survives):
    tc, db, api = client
    unraced = _page(tc, 1, **POST_STAT)["items"]
    before = [it["rating_key"] for it in unraced]
    assert victim in before and len(before) == PER_PAGE
    fired = []
    stat_pass = api._annotate_canonical_state

    def stat_pass_then_write(items, *, themes_dir):
        out = stat_pass(items, themes_dir=themes_dir)
        if not fired:
            fired.append(victim)
            with contextlib.closing(sqlite3.connect(db)) as c, c:
                write(c, victim)
        return out

    monkeypatch.setattr(api, "_annotate_canonical_state", stat_pass_then_write)
    raced = _page(tc, 1, **POST_STAT)["items"]
    assert fired == [victim]
    after = [it["rating_key"] for it in raced]
    if survives:
        assert after == before
    else:
        assert [rk for rk in after if rk != victim] == [rk for rk in before if rk != victim]
        assert after.count(victim) <= before.count(victim)
    if victim != "r3":
        assert {it["media_folder"] for it in raced if it["rating_key"] == "r3"} == {FOLDER_A, FOLDER_B}
