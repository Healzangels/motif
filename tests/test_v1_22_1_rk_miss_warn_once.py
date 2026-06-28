"""v1.22.1 — edition_key_for_rating_key's rk-miss breadcrumb is warn-once.

The v1.22.0 breadcrumb logged an unconditional WARNING on every rk that doesn't
resolve to a plex_items row. But the function runs once-per-row inside the
bulk-download loop, so a batch carrying N stale rks (the v1.18.90 Plex re-add
scenario) floods the log with N warnings. v1.22.1 applies the v1.17.11 hot-path
rule: first miss WARNs, subsequent misses drop to DEBUG.
"""
from __future__ import annotations

import logging
import sqlite3

from app.core import editions
from app.core.db import init_db
from app.core.editions import edition_key_for_rating_key


def test_rk_miss_warns_once_then_debug(tmp_path, caplog):
    db = tmp_path / "m.db"
    init_db(db)
    editions._RK_MISS_WARNED = False  # reset the process-wide latch for the test
    conn = sqlite3.connect(db)
    try:
        with caplog.at_level(logging.DEBUG, logger="editions"):
            assert edition_key_for_rating_key(conn, "ghost-1") == ""
            assert edition_key_for_rating_key(conn, "ghost-2") == ""
    finally:
        conn.close()

    recs = [r for r in caplog.records
            if "not in plex_items" in r.getMessage()]
    warns = [r for r in recs if r.levelno == logging.WARNING]
    debugs = [r for r in recs if r.levelno == logging.DEBUG]
    # First miss warns (operator sees it); the second drops to debug.
    assert len(warns) == 1, [r.getMessage() for r in recs]
    assert len(debugs) == 1, [r.getMessage() for r in recs]
    # Both still carry the offending rk so the breadcrumb stays diagnosable.
    assert "ghost-1" in warns[0].getMessage()
    assert "ghost-2" in debugs[0].getMessage()


def test_resolved_rk_does_not_warn(tmp_path, caplog):
    """A real rk resolves silently — no breadcrumb noise on the happy path."""
    db = tmp_path / "m.db"
    init_db(db)
    editions._RK_MISS_WARNED = False
    NOW = "2026-06-04T00:00:00Z"
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " guid_tmdb, title, edition_key, folder_path, has_theme,"
            " first_seen_at, last_seen_at) VALUES ('rk-1','1','movie',900,'W',"
            "'extended','/d/W (2000) {edition-Extended}',0,?,?)", (NOW, NOW))
        conn.commit()
        with caplog.at_level(logging.DEBUG, logger="editions"):
            assert edition_key_for_rating_key(conn, "rk-1") == "extended"
    finally:
        conn.close()
    assert not [r for r in caplog.records if "not in plex_items" in r.getMessage()]
