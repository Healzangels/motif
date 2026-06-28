"""v1.23.37 — DL sort groups by the rendered dot via a stored canonical flag.

Audit follow-up to v1.23.35 (LINK) / v1.23.24-25 (PL): the DL column has three
states — on (green), broken (red: file_path set but the canonical theme.mp3 is
gone from motif's storage), off (gray). The old DL sort was
`file_path IS NOT NULL THEN 0 ELSE 1`, so broken (file_path present) collapsed
into bucket 0 with green 'on' and the two interleaved by the title tiebreak.

`canonical_missing` is a per-render filesystem stat the paginated server-side
sort can't see — exactly the problem v1.23.25 solved for PL with a stored
placements.theme_present flag. This tag mirrors that: schema v67 adds
local_files.canonical_present (1/0/NULL), verify_canonical_health stamps it
each enum, and the DL sort ranks broken → on → off off the stored flag.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import plex_enum
from app.core.db import CURRENT_SCHEMA_VERSION, init_db, _migrate_v66_to_v67


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()
DB_PY = (REPO / "app" / "core" / "db.py").read_text()
NOW = "2026-06-14T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


# ── 1. Schema v67 ──────────────────────────────────────────


def test_schema_at_v67_with_canonical_health_columns(tmp_path):
    assert CURRENT_SCHEMA_VERSION >= 67
    db = tmp_path / "m.db"
    init_db(db)
    cols = {r[1] for r in sqlite3.connect(db).execute(
        "PRAGMA table_info(local_files)")}
    assert "canonical_present" in cols
    assert "canonical_health_checked_at" in cols


def test_migration_v66_to_v67_idempotent(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    conn = sqlite3.connect(db)
    # already has the columns — re-running must not raise.
    _migrate_v66_to_v67(conn)
    conn.commit()
    cols = {r[1] for r in conn.execute("PRAGMA table_info(local_files)")}
    assert "canonical_present" in cols


def test_migration_dispatch_wired():
    assert "_migrate_v66_to_v67(conn)" in DB_PY
    assert "current == 66" in DB_PY


# ── 2. verify_canonical_health (behavioral) ──────────────────


def _seed_lf(conn, *, tid, tmdb, file_path):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','M','movie',0,0,'movies',1,?,?)"
        " ON CONFLICT(section_id) DO NOTHING", (NOW, NOW))
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, 'X', 'imdb', ?, ?, 'u')", (tid, tmdb, NOW, NOW))
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind,"
        " edition_key) VALUES ('movie', ?, '1', ?, ?, ?, 'V', 'auto',"
        " 'themerrdb', '')", (tmdb, tid, file_path, NOW))


def test_verify_stamps_present_and_missing(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    themes = tmp_path / "themes"
    with sqlite3.connect(db) as conn:
        _seed_lf(conn, tid=1, tmdb=101, file_path="movies/101/theme.mp3")
        _seed_lf(conn, tid=2, tmdb=102, file_path="movies/102/theme.mp3")
        conn.commit()
    # 101 present on disk, 102 missing.
    (themes / "movies/101").mkdir(parents=True)
    (themes / "movies/101/theme.mp3").write_bytes(b"x")

    res = plex_enum.verify_canonical_health(db, themes)
    assert res == {"checked": 2, "missing": 1, "skipped": 0}
    flags = dict(sqlite3.connect(db).execute(
        "SELECT tmdb_id, canonical_present FROM local_files").fetchall())
    assert flags[101] == 1   # present
    assert flags[102] == 0   # verified missing
    # the checked_at stamp is written.
    stamped = sqlite3.connect(db).execute(
        "SELECT COUNT(*) FROM local_files "
        "WHERE canonical_health_checked_at IS NOT NULL").fetchone()[0]
    assert stamped == 2


def test_verify_caps_implausible_missing_as_mount_fault(tmp_path):
    # is_file() returns False (not OSError) when the mount's parent is gone, so
    # a /data outage reads EVERY canonical missing — the cap must skip the
    # 0-stamps so one blip can't flip the whole library to false-broken.
    db = tmp_path / "m.db"
    init_db(db)
    themes = tmp_path / "themes"   # nothing created → all stat False
    with sqlite3.connect(db) as conn:
        for i in range(60):        # 60 > cap max(50, 60//4=15) = 50
            _seed_lf(conn, tid=i + 1, tmdb=1000 + i,
                     file_path=f"movies/{1000 + i}/theme.mp3")
        conn.commit()
    res = plex_enum.verify_canonical_health(db, themes)
    assert res["missing"] == 0, "capped: missing-stamps skipped this run"
    # none stamped 0 — they keep NULL (unverified), behaving as 'on'.
    zero = sqlite3.connect(db).execute(
        "SELECT COUNT(*) FROM local_files WHERE canonical_present = 0").fetchone()[0]
    assert zero == 0


def test_verify_wired_into_enum_cycle():
    assert "verify_canonical_health(db_path" in PLEX_ENUM_PY
    # guarded on themes_dir being configured.
    assert "_themes_dir = Settings().themes_dir" in PLEX_ENUM_PY
    assert "if _themes_dir is not None:" in PLEX_ENUM_PY


# ── 3. DL sort groups broken / on / off (behavioral) ─────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings.db_path


def _item(conn, *, rk, tid, tmdb, title):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, plex_independent_theme,"
        " first_seen_at, last_seen_at)"
        " VALUES (?, '1', 'movie', ?, ?, ?, ?, 2012, 1, 0, ?, ?)",
        (rk, tid, f"tt{tmdb}", tmdb, title, NOW, NOW))


def _lf(conn, *, tid, tmdb, canonical_present):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind,"
        " canonical_present, edition_key) VALUES ('movie', ?, '1', ?, ?, ?, 'V',"
        " 'auto', 'themerrdb', ?, '')",
        (tmdb, tid, f"movies/{tmdb}/theme.mp3", NOW, canonical_present))


def test_dl_sort_groups_broken_on_off(admin_client):
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        # Titles run Z..W against rank 0..2 so only the sort key (not the
        # title tiebreak) can produce the grouped order.
        # BROKEN: file present in DB but canonical_present=0 (verified gone).
        conn.execute("INSERT INTO themes (id,media_type,tmdb_id,title,"
                     "upstream_source,last_seen_sync_at,first_seen_sync_at,"
                     "youtube_url) VALUES (1,'movie',101,'Z','imdb',?,?,'u')",
                     (NOW, NOW))
        _item(conn, rk="rk-broken", tid=1, tmdb=101, title="Z broken")
        _lf(conn, tid=1, tmdb=101, canonical_present=0)
        # ON: canonical_present=1.
        conn.execute("INSERT INTO themes (id,media_type,tmdb_id,title,"
                     "upstream_source,last_seen_sync_at,first_seen_sync_at,"
                     "youtube_url) VALUES (2,'movie',102,'Y','imdb',?,?,'u')",
                     (NOW, NOW))
        _item(conn, rk="rk-on", tid=2, tmdb=102, title="Y on")
        _lf(conn, tid=2, tmdb=102, canonical_present=1)
        # UNVERIFIED: file present, canonical_present=NULL → sorts as 'on'.
        conn.execute("INSERT INTO themes (id,media_type,tmdb_id,title,"
                     "upstream_source,last_seen_sync_at,first_seen_sync_at,"
                     "youtube_url) VALUES (3,'movie',103,'X','imdb',?,?,'u')",
                     (NOW, NOW))
        _item(conn, rk="rk-unver", tid=3, tmdb=103, title="X unver")
        _lf(conn, tid=3, tmdb=103, canonical_present=None)
        # OFF: no local_files row at all.
        conn.execute("INSERT INTO themes (id,media_type,tmdb_id,title,"
                     "upstream_source,last_seen_sync_at,first_seen_sync_at,"
                     "youtube_url) VALUES (4,'movie',104,'W','imdb',?,?,'u')",
                     (NOW, NOW))
        _item(conn, rk="rk-off", tid=4, tmdb=104, title="W off")
        conn.commit()

    r = client.get("/api/library?tab=movies&sort=dl&sort_dir=asc", headers=AUTH)
    assert r.status_code == 200, r.text
    order = [it["rating_key"] for it in r.json()["items"]]
    for rk in ("rk-broken", "rk-on", "rk-unver", "rk-off"):
        assert rk in order, f"{rk} missing from {order}"
    # The fix: broken (bucket 0) leads despite its "Z" title; off (bucket 2) is
    # last; on + unverified sit in the middle (bucket 1), separated from broken.
    assert order.index("rk-broken") < order.index("rk-on"), order
    assert order.index("rk-broken") < order.index("rk-unver"), order
    assert order.index("rk-on") < order.index("rk-off"), order
    assert order.index("rk-unver") < order.index("rk-off"), order
    assert order[0] == "rk-broken", order


def test_dl_sort_key_uses_canonical_present():
    i = API_PY.index('"dl": (')
    block = API_PY[i:i + 600]
    assert "canonical_present, lf_g.canonical_present) = 0 THEN 0" in block
    # the old flat file-present/absent CASE is gone.
    assert ('"dl":    "CASE WHEN COALESCE(lf_e.file_path, lf_g.file_path) '
            'IS NOT NULL THEN 0 ELSE 1 END"') not in API_PY
