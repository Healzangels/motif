"""v1.23.25 — stored placement health surfaces broken placements.

the user's report: an HL row whose theme.mp3 is gone from the Plex folder (red
PL dot) did NOT surface under NEEDS WORK. Cause: 'broken' is a post-SQL
filesystem stat (api.py _annotate_canonical_state.placement_missing), so the
paginated server-side attention SQL sort couldn't see it — the row fell to
ELSE 7 and sank.

Fix (the user chose "background verification"): plex_enum's
verify_placement_health pass stats each sidecar placement's theme.mp3 and
stamps placements.theme_present (1/0/NULL). The attention sort ranks a verified
-missing placement (=0) at priority 0 (top), and the PL sort groups it in a
'broken' bucket ahead of green. Health is as fresh as the last enum.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
NOW = "2026-06-13T00:00:00"


# ── migration: additive, idempotent ─────────────────────────


def test_migrate_v65_to_v66_adds_columns_idempotently():
    from app.core.db import _migrate_v65_to_v66
    conn = sqlite3.connect(":memory:")
    # a v65-shape placements table (no health columns).
    conn.execute(
        "CREATE TABLE placements (media_type TEXT, tmdb_id INTEGER, "
        " section_id TEXT, media_folder TEXT, placement_kind TEXT, "
        " edition_key TEXT)")
    _migrate_v65_to_v66(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(placements)")}
    assert "theme_present" in cols
    assert "placement_health_checked_at" in cols
    # idempotent — a second run is a no-op, not a duplicate-column error.
    _migrate_v65_to_v66(conn)


def test_fresh_db_is_v66_with_health_columns(tmp_path):
    from app.core.db import init_db, CURRENT_SCHEMA_VERSION
    db = tmp_path / "m.db"
    init_db(db)
    # v1.23.37: was == 66; loosened to >= 66 so a later schema bump (v67 added
    # local_files.canonical_present) doesn't trip this — the point is the
    # placement-health columns exist from v66 onward, not the exact version.
    assert CURRENT_SCHEMA_VERSION >= 66
    with sqlite3.connect(db) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(placements)")}
    assert {"theme_present", "placement_health_checked_at"} <= cols


# ── verify_placement_health: stat → stamp ────────────────────


def _seed_placement(conn, *, tmdb, media_folder, kind="hardlink"):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)",
        (NOW, NOW)) if not conn.execute(
            "SELECT 1 FROM plex_sections WHERE section_id='1'").fetchone() else None
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at) VALUES ('movie', ?, ?, 'imdb', ?, ?)",
        (tmdb, f"T{tmdb}", NOW, NOW))
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
        " placed_at, placement_kind, provenance, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, ?, 'auto', '')",
        (tmdb, media_folder, NOW, kind))


def test_verify_stamps_present_missing_and_skips_plex_upload(tmp_path):
    from app.core.db import init_db
    from app.core.plex_enum import verify_placement_health
    db = tmp_path / "m.db"
    init_db(db)
    present_dir = tmp_path / "present"
    present_dir.mkdir()
    (present_dir / "theme.mp3").write_bytes(b"x")
    missing_dir = tmp_path / "missing"
    missing_dir.mkdir()  # exists, but no theme.mp3
    with sqlite3.connect(db) as conn:
        _seed_placement(conn, tmdb=1, media_folder=str(present_dir))
        _seed_placement(conn, tmdb=2, media_folder=str(missing_dir))
        # plex_upload — media_folder='' — must be skipped (no on-disk sidecar).
        _seed_placement(conn, tmdb=3, media_folder="", kind="plex_upload")
        conn.commit()

    res = verify_placement_health(db)
    # v1.23.26: + pruned key (these missing sidecars have no plex_upload
    # sibling, so nothing is pruned — they stamp broken as before).
    # v1.24.28: + plex_upload_stale key. The plex_upload row here has a NULL
    # plex_rating_key, so the new staleness pass leaves it untouched (still
    # theme_present=NULL, asserted below) and counts 0 stale.
    # v1.24.39: + plex_upload_skipped key. This seed has NO plex_items rows, so
    # the empty-guard trips and the 0-stamp pass is skipped (the NULL-rk upload
    # would be untouched anyway).
    assert res == {"checked": 2, "missing": 1, "pruned": 0, "skipped": 0,
                   "plex_upload_stale": 0, "plex_upload_skipped": True}
    with sqlite3.connect(db) as conn:
        rows = {r[0]: (r[1], r[2]) for r in conn.execute(
            "SELECT tmdb_id, theme_present, placement_health_checked_at "
            "FROM placements")}
    assert rows[1][0] == 1 and rows[1][1]                  # present, stamped
    assert rows[2][0] == 0 and rows[2][1]                  # missing, stamped
    assert rows[3][0] is None and rows[3][1] is None       # plex_upload untouched


# ── sorts read the stored flag (behavioral) ──────────────────


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
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings.db_path


AUTH = {"X-Authentik-Username": "testadmin"}


def _full_row(conn, *, rk, tmdb, title, theme_present, has_theme=1):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)",
        (NOW, NOW)) if not conn.execute(
            "SELECT 1 FROM plex_sections WHERE section_id='1'").fetchone() else None
    cur = conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at) VALUES ('movie', ?, ?, 'imdb', ?, ?)",
        (tmdb, title, NOW, NOW))
    tid = cur.lastrowid
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_tmdb, title, year, has_theme, plex_independent_theme,"
        " first_seen_at, last_seen_at)"
        " VALUES (?, '1', 'movie', ?, ?, ?, 2012, ?, 0, ?, ?)",
        (rk, tid, tmdb, title, has_theme, NOW, NOW))
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind,"
        " edition_key) VALUES ('movie', ?, '1', ?, ?, ?, 'V', 'auto',"
        " 'themerrdb', '')", (tmdb, tid, f"movies/{tmdb}/theme.mp3", NOW))
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
        " media_folder, placed_at, placement_kind, provenance, edition_key,"
        " theme_present) VALUES ('movie', ?, '1', ?, ?, ?, 'hardlink', 'auto',"
        " '', ?)", (tmdb, tid, f"/data/m/{title}", NOW, theme_present))


def test_broken_placement_tops_needs_work_and_pl_broken_bucket(admin_client):
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        # BROKEN: theme_present=0. Title sorts LAST so it can only lead by the
        # broken priority, not the title tiebreaker.
        _full_row(conn, rk="rk-broken", tmdb=1, title="Z Broken", theme_present=0)
        # CLEAN placed: theme_present=1. Title sorts FIRST.
        _full_row(conn, rk="rk-clean", tmdb=2, title="A Clean", theme_present=1)
        conn.commit()

    # NEEDS WORK (attention): broken ranks above the clean priority-7 row.
    r = client.get("/api/library?tab=movies&sort=attention&sort_dir=asc",
                   headers=AUTH)
    assert r.status_code == 200, r.text
    order = [it["rating_key"] for it in r.json()["items"]]
    assert order.index("rk-broken") < order.index("rk-clean"), order

    # PL sort: broken (bucket 0) precedes green clean (bucket 1).
    r = client.get("/api/library?tab=movies&sort=pl&sort_dir=asc", headers=AUTH)
    order = [it["rating_key"] for it in r.json()["items"]]
    assert order.index("rk-broken") < order.index("rk-clean"), order
