"""v0.51.101 — scope (or skip) the three plex_enum end passes.

`reconcile_placement_paths`, `verify_placement_health`, `verify_canonical_health`
ran table-wide, serial-FS, at the end of EVERY enum — even a single-section
REFRESH or a run where every section was contentChangedAt-skipped. On a large
library over a network mount that's thousands of `is_file()` calls for nothing.

v0.51.101 adds an optional `section_ids` scope to all three (default None =
table-wide, unchanged for the nightly all-sections cron) and, in run_plex_enum,
computes the walked/targeted section set → skips all three on a no-work run,
scopes them otherwise.

These tests drive the three functions directly against a seeded DB with two
sections and assert the scoped call touches ONLY the scoped section's rows.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.core.db import get_conn, init_db
from app.core.plex_enum import (
    reconcile_placement_paths,
    verify_canonical_health,
    verify_placement_health,
)

REPO = Path(__file__).resolve().parent.parent
ENUM_SRC = (REPO / "app" / "core" / "plex_enum.py").read_text()
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")


def _db(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    return db


def _placement(db, *, tmdb, section, folder, kind="hardlink"):
    with sqlite3.connect(db) as c:  # raw sqlite3 → FK enforcement off
        c.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            " media_folder, placed_at, placement_kind, edition_key) "
            "VALUES ('movie', ?, ?, ?, ?, ?, '')",
            (tmdb, section, folder, NOW, kind),
        )
        c.commit()


def _local_file(db, *, tmdb, section, file_path):
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            " file_path, downloaded_at, source_video_id) "
            "VALUES ('movie', ?, ?, ?, ?, 'vid')",
            (tmdb, section, file_path, NOW),
        )
        c.commit()


def _plex_item(db, *, rk, tmdb, section, folder):
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            " guid_tmdb, title, edition_key, has_theme, folder_path, "
            " first_seen_at, last_seen_at) "
            "VALUES (?, ?, 'movie', ?, ?, '', 0, ?, ?, ?)",
            (rk, section, tmdb, f"m{tmdb}", folder, NOW, NOW),
        )
        c.commit()


# ── verify_placement_health scoping ─────────────────────────────────


def test_placement_health_scopes_to_given_sections(tmp_path):
    db = _db(tmp_path)
    d1 = tmp_path / "s1" / "m1"; d1.mkdir(parents=True); (d1 / "theme.mp3").write_bytes(b"x")
    d2 = tmp_path / "s2" / "m2"; d2.mkdir(parents=True); (d2 / "theme.mp3").write_bytes(b"x")
    _placement(db, tmdb=1, section="1", folder=str(d1))
    _placement(db, tmdb=2, section="2", folder=str(d2))

    verify_placement_health(db, section_ids=["1"])
    with get_conn(db) as conn:
        c1 = conn.execute("SELECT placement_health_checked_at FROM placements "
                          "WHERE section_id='1'").fetchone()[0]
        c2 = conn.execute("SELECT placement_health_checked_at FROM placements "
                          "WHERE section_id='2'").fetchone()[0]
    assert c1 is not None, "scoped section must be stamped"
    assert c2 is None, "out-of-scope section must be left untouched"


def test_placement_health_unscoped_touches_all(tmp_path):
    db = _db(tmp_path)
    d1 = tmp_path / "s1" / "m1"; d1.mkdir(parents=True); (d1 / "theme.mp3").write_bytes(b"x")
    d2 = tmp_path / "s2" / "m2"; d2.mkdir(parents=True); (d2 / "theme.mp3").write_bytes(b"x")
    _placement(db, tmdb=1, section="1", folder=str(d1))
    _placement(db, tmdb=2, section="2", folder=str(d2))

    verify_placement_health(db)  # None → table-wide (cron default)
    with get_conn(db) as conn:
        n = conn.execute("SELECT COUNT(*) FROM placements "
                         "WHERE placement_health_checked_at IS NOT NULL").fetchone()[0]
    assert n == 2


# ── verify_canonical_health scoping ─────────────────────────────────


def test_canonical_health_scopes_to_given_sections(tmp_path):
    db = _db(tmp_path)
    themes = tmp_path / "themes"; themes.mkdir()
    (themes / "a.mp3").write_bytes(b"x"); (themes / "b.mp3").write_bytes(b"x")
    _local_file(db, tmdb=1, section="1", file_path="a.mp3")
    _local_file(db, tmdb=2, section="2", file_path="b.mp3")

    verify_canonical_health(db, themes, section_ids=["1"])
    with get_conn(db) as conn:
        c1 = conn.execute("SELECT canonical_health_checked_at FROM local_files "
                          "WHERE section_id='1'").fetchone()[0]
        c2 = conn.execute("SELECT canonical_health_checked_at FROM local_files "
                          "WHERE section_id='2'").fetchone()[0]
    assert c1 is not None
    assert c2 is None


# ── reconcile_placement_paths scoping ───────────────────────────────


def test_reconcile_scopes_relink_to_given_sections(tmp_path):
    db = _db(tmp_path)
    # both sections have a placement whose media_folder diverges from the
    # current plex_items.folder_path → both are relink candidates.
    _plex_item(db, rk="r1", tmdb=1, section="1", folder="/new/1")
    _plex_item(db, rk="r2", tmdb=2, section="2", folder="/new/2")
    _placement(db, tmdb=1, section="1", folder="/old/1")
    _placement(db, tmdb=2, section="2", folder="/old/2")

    n = reconcile_placement_paths(db, section_ids=["1"])
    assert n == 1, "only the scoped section's divergence should relink"
    with get_conn(db) as conn:
        # section 2's placement is untouched (still the old folder).
        f2 = conn.execute("SELECT media_folder FROM placements "
                          "WHERE section_id='2'").fetchone()[0]
        # a place job was enqueued for the scoped tmdb only.
        j1 = conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type='place' "
                          "AND tmdb_id=1").fetchone()[0]
        j2 = conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type='place' "
                          "AND tmdb_id=2").fetchone()[0]
    assert f2 == "/old/2"
    assert j1 == 1 and j2 == 0


def test_reconcile_unscoped_relinks_all(tmp_path):
    db = _db(tmp_path)
    _plex_item(db, rk="r1", tmdb=1, section="1", folder="/new/1")
    _plex_item(db, rk="r2", tmdb=2, section="2", folder="/new/2")
    _placement(db, tmdb=1, section="1", folder="/old/1")
    _placement(db, tmdb=2, section="2", folder="/old/2")

    n = reconcile_placement_paths(db)  # None → table-wide
    assert n == 2


# ── run_plex_enum wiring guard ──────────────────────────────────────


def test_run_plex_enum_scopes_or_skips_end_passes():
    i = ENUM_SRC.index("def run_plex_enum(")
    # the end passes sit near the tail of this large function.
    body = ENUM_SRC[i:i + 45000]
    # tracks the walked sections.
    assert "worked_sections.add(section_id)" in body
    # computes the scope: targeted section for a single-section REFRESH, else
    # the walked set (None → skip).
    assert "_scope_sections: list[str] | None = [only_section_id]" in body
    assert "_scope_sections = sorted(worked_sections) or None" in body
    # all three end passes are gated on / scoped by it.
    assert "reconcile_placement_paths(db_path, section_ids=_scope_sections)" in body
    assert "verify_placement_health(db_path, section_ids=_scope_sections)" in body
    # canonical health call (whitespace-tolerant across the wrapped args).
    assert "verify_canonical_health(" in body
    assert "_themes_dir, section_ids=_scope_sections)" in body
    # canonical health is gated on the scope too (skip on a no-work run).
    assert "if _themes_dir is not None and _scope_sections is not None:" in body
    assert "if _scope_sections is None:" in body
