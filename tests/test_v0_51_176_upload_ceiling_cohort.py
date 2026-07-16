"""v0.51.176 — size the propagation constraint before designing around it.

The audition's first target was 10.5MB and blew Plex's ~10MB theme-upload ceiling. That
matters far more than one unlucky pick, because re-upload is one of only TWO ways to tell
Plex a theme's bytes changed — and the other (refresh?force=1) is MEASURED dead (v0.51.173:
Plex still served -5.15 against a -18.7 canonical, same entry, minutes later).

So the ceiling decides the design:
  - a handful over 10MB → re-upload is the mechanism, handle the rest by hand.
  - a large cohort over  → re-upload is not a strategy and delete+re-detect must work.

Guessing that blast radius is exactly the mistake that cost the last four tags. It's a
read-only COUNT off local_files.file_size — measure it.

This also stopped being a backfill-only concern once the operator pointed out that
normalize-at-download only avoids propagation for the FIRST delivery: every later undo /
re-apply of a download-normalized theme still has to tell Plex. Propagation is permanent.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.core import loudness_audit as la
from app.core import plex as plex_mod
from app.core.db import get_conn, init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
REPO = Path(__file__).resolve().parent.parent


def _seed(db, sizes):
    """sizes: list of file_size values (None = unknown)."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        for i, size in enumerate(sizes, start=1):
            c.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                " file_path, file_size, downloaded_at, source_video_id) "
                "VALUES ('movie', ?, '1', '', ?, ?, ?, 'vid')",
                (i, f"movies/{i}/theme.mp3", size, NOW),
            )
        c.commit()


def test_ceiling_matches_the_plex_client_constant():
    """One truth. A second copy that drifts from plex.THEME_UPLOAD_CEILING_BYTES would make
    the report lie about what's pushable."""
    assert la._UPLOAD_CEILING_BYTES == plex_mod.THEME_UPLOAD_CEILING_BYTES


def test_counts_over_under_and_largest(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    ceiling = la._UPLOAD_CEILING_BYTES
    _seed(db, [100, 500, ceiling, ceiling + 1, ceiling * 2])
    with get_conn(db) as conn:
        c = la.upload_ceiling_counts(conn)
    assert c["ceiling_bytes"] == ceiling
    assert c["over_ceiling"] == 2          # ceiling+1 and ceiling*2
    assert c["under_ceiling"] == 3         # exactly-at-ceiling counts as under (<=)
    assert c["largest_bytes"] == ceiling * 2


def test_unknown_size_is_surfaced_not_counted_as_small(tmp_path):
    """A NULL file_size is an UNKNOWN, not a small file. Folding it into `under` would let
    a measurement gap read as "re-upload covers this" (class-9)."""
    db = tmp_path / "motif.db"
    init_db(db)
    _seed(db, [100, None, None])
    with get_conn(db) as conn:
        c = la.upload_ceiling_counts(conn)
    assert c["under_ceiling"] == 1
    assert c["unknown_size"] == 2
    assert c["over_ceiling"] == 0


def test_empty_library_does_not_crash(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn:
        c = la.upload_ceiling_counts(conn)
    assert c["over_ceiling"] == 0
    assert c["largest_bytes"] is None      # MAX() of nothing → None, not 0


def test_report_ships_the_ceiling_cohort(tmp_path):
    """The report is where the decision gets made, so the number has to reach it."""
    db = tmp_path / "motif.db"
    init_db(db)
    _seed(db, [100, la._UPLOAD_CEILING_BYTES + 1])
    with get_conn(db) as conn:
        rep = la.build_report(conn)
    assert "upload_ceiling" in rep
    assert rep["upload_ceiling"]["over_ceiling"] == 1
    assert rep["upload_ceiling"]["ceiling_bytes"] == la._UPLOAD_CEILING_BYTES


def test_report_page_renders_the_cohort():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # threaded from the payload into the tiles, not computed client-side
    assert "renderStats(rep.stats, rep.recommended, rep.upload_ceiling)" in js
    assert "over 10MB (un-pushable)" in js
