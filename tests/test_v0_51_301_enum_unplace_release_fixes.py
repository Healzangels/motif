"""v0.51.301 — holistic round 2, wave 10: three confirmed mediums.

  1. plex.py's bulk folder-path fetch degrades to '' per failed batch
     while the enum reports success — _upsert_items then overwrote a
     known-good folder_path/edition_key with '' and the freshness stamp
     hid it until the next lucky refresh. The upsert now preserves the
     prior path for movie/show rows when the incoming path is empty
     (collections legitimately carry none).
  2. The UNPLACE fallback chooser matched motif's entry by canonical
     sha1 only — the v1.24.47 downscale uploads RE-ENCODED bytes, so the
     chooser picked motif's own theme as the "fallback" and re-selected
     it, defeating the unplace. It now also skips the SELECTED upload.
  3. release.yml never verified the pushed tag against __version__ (the
     documented pre-v1.13.79 drift class) — a blocking gate step now
     compares them on tag pushes.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
# v0.51.305: clock-derived, not a fixed calendar date — the .300 file's fixed
# anchor aged past a julianday('now') window one real day after authoring.
from datetime import datetime as _dt, timezone as _tz
NOW = _dt.now(_tz.utc).isoformat()
SEC = "1"


# ── 1. preserve-on-empty, driven through the real upsert ─────


def test_empty_folder_path_preserves_the_prior(tmp_path):
    from app.core.db import get_conn, init_db, transaction
    from app.core.plex import PlexLibraryItem
    from app.core.plex_enum import _upsert_items
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES (?, 'TV', 'show', 0, 0, 'tv', 1, ?, ?)""",
            (SEC, NOW, NOW))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, folder_path, edition_key, has_theme,
                 first_seen_at, last_seen_at)
               VALUES ('rk-s', ?, 'show', 'S',
                       301001, '/data/tv/S {edition-extended}', 'extended',
                       0, ?, ?)""",
            (SEC, NOW, NOW))
    degraded = PlexLibraryItem(
        rating_key="rk-s", section_id=SEC, media_type="show", title="S",
        year=None, guid_imdb=None, guid_tmdb=301001, guid_tvdb=None,
        folder_path="", has_theme=False)   # the failed-batch shape
    _upsert_items(db, [degraded], section_id=SEC)
    with get_conn(db) as conn:
        row = conn.execute(
            "SELECT folder_path, edition_key FROM plex_items "
            "WHERE rating_key='rk-s'").fetchone()
    assert row["folder_path"] == "/data/tv/S {edition-extended}", (
        "a degraded '' path must not clobber the known-good folder")
    assert row["edition_key"] == "extended", (
        "edition_key is folder-derived — blanking it poisons every "
        "edition-scoped surface")


def test_real_folder_change_still_updates(tmp_path):
    from app.core.db import get_conn, init_db, transaction
    from app.core.plex import PlexLibraryItem
    from app.core.plex_enum import _upsert_items
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES (?, 'TV', 'show', 0, 0, 'tv', 1, ?, ?)""",
            (SEC, NOW, NOW))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, folder_path, has_theme, first_seen_at,
                 last_seen_at)
               VALUES ('rk-m', ?, 'show', 'M', 301002, '/data/tv/Old', 0,
                       ?, ?)""",
            (SEC, NOW, NOW))
    moved = PlexLibraryItem(
        rating_key="rk-m", section_id=SEC, media_type="show", title="M",
        year=None, guid_imdb=None, guid_tmdb=301002, guid_tvdb=None,
        folder_path="/data/tv/New", has_theme=False)
    _upsert_items(db, [moved], section_id=SEC)
    with get_conn(db) as conn:
        fp = conn.execute("SELECT folder_path FROM plex_items "
                          "WHERE rating_key='rk-m'").fetchone()[0]
    assert fp == "/data/tv/New", "a REAL move must still update"


# ── 2 + 3. wiring pins ───────────────────────────────────────


def test_unplace_chooser_skips_the_selected_upload():
    api = (REPO / "app" / "web" / "api.py").read_text()
    i = api.index('if not ratingKey.startswith("upload://")')
    blk = api[i:api.index("fallback_rk = ratingKey", i)]
    assert 'if e.get("selected"):' in blk, (
        "the selected upload on a plex_upload row IS motif's entry even "
        "when its hash differs (the v1.24.47 re-encode) — hash-only "
        "matching re-selected motif's own theme, defeating the unplace")


def test_release_gate_verifies_tag_matches_version():
    wf = (REPO / ".github" / "workflows" / "release.yml").read_text()
    i = wf.index("Tag matches __version__")
    blk = wf[i:wf.index("- name:", i + 10)]   # to the next step (structural)
    assert "startsWith(github.ref, 'refs/tags/v')" in blk
    assert "__version__" in blk and "exit 1" in blk, (
        "the pre-v1.13.79 drift class: a mismatched tag must not publish")
    # the step must live in the gate job, before the build can depend on it.
    assert wf.index("Tag matches __version__") < wf.index("Build and push")


def test_v0_51_301_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.301: " in init_py
