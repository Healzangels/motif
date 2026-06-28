"""v1.21.42 — an empty folder_path preserves local_theme_file (audit M2).

Silent-failure audit finding M2: _upsert_items' Phase-1 sidecar stat called
stat_theme_sidecar(it.folder_path), and stat_theme_sidecar('') returned
False (not None). So when a transient get_item_paths_bulk failure left a TV
show's folder_path='' for one enum, `1 if res else 0` wrote
local_theme_file=0 and an M-row silently lost its sidecar flag (SRC flipped
M→P) until a clean enum repopulated the path.

Fix: stat_theme_sidecar('') returns None (indeterminate — matches
find_theme_sidecar_path), routing the empty case through the existing
v1.11.67 indeterminate-preservation (keep the previously-known value).
"""
from __future__ import annotations

from pathlib import Path

import pytest


# ── unit: empty path is indeterminate, wrapper still bool ─────


def test_stat_theme_sidecar_empty_is_none():
    from app.core.plex_enum import stat_theme_sidecar
    assert stat_theme_sidecar("") is None
    assert stat_theme_sidecar(None) is None


def test_folder_has_theme_sidecar_empty_still_false():
    # The bool wrapper maps None→False, so direct bool-callers are
    # unaffected by the None change.
    from app.core.plex_enum import folder_has_theme_sidecar
    assert folder_has_theme_sidecar("") is False


# ── behavioral: enum with an empty folder_path preserves the flag ──


def _seed(db):
    from app.core.db import get_conn
    with get_conn(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
            "  is_anime, is_4k, themes_subdir, included, discovered_at, "
            "  last_seen_at) "
            "VALUES ('1','TV','show',0,0,'tv',1,'2026-01-01','2026-01-01')")
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  title, year, folder_path, has_theme, local_theme_file, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('rk-m2','1','show','Show','2021','/good/path',1,1,"
            "        '2026-01-01','2026-01-01')")


def _local_theme_file(db):
    from app.core.db import get_conn
    with get_conn(db) as conn:
        r = conn.execute(
            "SELECT local_theme_file FROM plex_items WHERE rating_key='rk-m2'"
        ).fetchone()
    return r["local_theme_file"]


def test_empty_folder_path_preserves_local_theme_file(tmp_path):
    from app.core.db import init_db
    from app.core.plex import PlexLibraryItem
    from app.core.plex_enum import _upsert_items
    db = tmp_path / "m.db"
    init_db(db)
    _seed(db)
    assert _local_theme_file(db) == 1
    # A transient bulk-path failure → folder_path='' for this enum.
    item = PlexLibraryItem(
        rating_key="rk-m2", section_id="1", media_type="show",
        title="Show", year="2021", guid_imdb=None, guid_tmdb=None,
        guid_tvdb=None, folder_path="", has_theme=True)
    _upsert_items(db, [item], section_id="1")
    assert _local_theme_file(db) == 1, (
        "v1.21.42: an empty folder_path must PRESERVE local_theme_file, "
        "not stomp the M-row's sidecar flag to 0")


def test_real_folder_without_sidecar_still_clears_flag(tmp_path):
    """Discriminator: a NON-empty folder_path that's confirmed to have no
    sidecar still clears local_theme_file (preservation is only for the
    indeterminate/empty case, not a confirmed-empty scan)."""
    from app.core.db import init_db
    from app.core.plex import PlexLibraryItem
    from app.core.plex_enum import _upsert_items
    db = tmp_path / "m.db"
    init_db(db)
    _seed(db)
    empty_dir = tmp_path / "real_no_sidecar"
    empty_dir.mkdir()
    item = PlexLibraryItem(
        rating_key="rk-m2", section_id="1", media_type="show",
        title="Show", year="2021", guid_imdb=None, guid_tmdb=None,
        guid_tvdb=None, folder_path=str(empty_dir), has_theme=True)
    _upsert_items(db, [item], section_id="1")
    assert _local_theme_file(db) == 0, (
        "a confirmed sidecar-less real folder must still clear the flag")
