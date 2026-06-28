"""v1.14.36 — storage_copies_bytes JOIN includes section_id.

Closes audit finding api M2 (KPI inflation on multi-section
copy placements). Both /api/stats and /api/public/stats
computed:

    SELECT COALESCE(SUM(lf.file_size), 0)
    FROM placements p
    JOIN local_files lf
      ON lf.media_type = p.media_type
     AND lf.tmdb_id = p.tmdb_id
    WHERE p.placement_kind = 'copy'

The JOIN omits section_id. For a multi-section title (e.g.,
"Movie X" in standard + 4K) where ONE section has a copy
placement, the JOIN matches that copy placement against EVERY
local_files row sharing (mt, tmdb) — including sibling sections
— and SUMs file_size N times.

## Concrete bug

Imagine "Movie X" exists in section A (4K, copy placement, 5MB
canonical) AND section B (standard, hardlink placement, 4MB
canonical). The pre-fix query:
  • placements: 1 row for ("movie", X, "A", copy)
  • local_files: 2 rows — ("movie", X, "A", 5MB), ("movie", X, "B", 4MB)
  • JOIN on (mt, tmdb): 2 result rows — both bound to the A
    copy placement
  • SUM(lf.file_size): 5+4 = 9MB

Correct answer: 5MB (the actual on-disk copy bytes for that
placement). Pre-fix the dashboard storage.copies KPI lied by
4MB — silently accumulates as the user adds more sections.

## Fix

Add `AND lf.section_id = p.section_id` to the JOIN. The
local_files PK is (media_type, tmdb_id, section_id) since
schema v18 — so this is the natural join column.

The /api/storage/copies page query already had this clause; the
two stats endpoints had drifted.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent


# ── Static guards on both call sites ─────────────────────────


def test_stats_storage_copies_join_includes_section_id():
    """Both occurrences of the storage_copies_bytes subquery
    must include the section_id JOIN clause. Pin every piece so
    a refactor that "simplifies" by dropping the third column
    fails loud."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Two occurrences total (api_public_stats + _stats_sync).
    n = src.count("WHERE p.placement_kind = 'copy') AS storage_copies_bytes")
    assert n == 2, (
        f"Expected exactly 2 storage_copies_bytes subqueries "
        f"(public stats + /api/stats), found {n}."
    )
    # Both must have the section_id JOIN clause. Count the
    # distinct site marker.
    n_sec = src.count("AND lf.section_id = p.section_id")
    assert n_sec >= 2, (
        f"Expected ≥2 `AND lf.section_id = p.section_id` "
        f"clauses (one per stats site), found {n_sec}."
    )
    # And the v1.14.36 archaeology marker explains why.
    assert "v1.14.36: JOIN includes section_id" in src


def test_pre_fix_unscoped_join_no_longer_present():
    """The pre-fix shape — JOIN on (mt, tmdb) without section_id
    — must NOT survive in either stats endpoint. Pin the bare
    pattern so a regression is loud."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Old shape collapsed into a single substring search.
    pre_fix = (
        "JOIN local_files lf\n"
        "                     ON lf.media_type = p.media_type "
        "AND lf.tmdb_id = p.tmdb_id\n"
        "                   WHERE p.placement_kind = 'copy'"
    )
    assert pre_fix not in src, (
        "Pre-fix JOIN shape (no section_id) survives — KPI "
        "would still inflate on multi-section copies."
    )


# ── Behavioral: SUM matches per-section reality ──────────────


def test_section_scoped_join_sums_correctly(tmp_path):
    """End-to-end: seed a multi-section title with ONE copy
    placement in section A + a hardlink in section B (different
    file sizes). The post-fix SUM must equal A's size only.
    Pre-fix would have summed A + B."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = now_iso()
    # Two sections.
    for sid, title, subdir, is_4k in (
        ("A", "Movies 4K", "movies-4k", 1),
        ("B", "Movies", "movies", 0),
    ):
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  included, is_anime, is_4k, themes_subdir, "
            "  discovered_at, last_seen_at) "
            "VALUES (?, ?, 'movie', 1, 0, ?, ?, ?, ?)",
            (sid, title, is_4k, subdir, now, now),
        )
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, first_seen_sync_at, "
        "                    last_seen_sync_at) "
        "VALUES ('movie', 333, 'Multi-Section', 2020, "
        "        'imdb', ?, ?)",
        (now, now),
    )
    # Section A: 5MB local_files + copy placement.
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "  file_path, file_size, downloaded_at, source_video_id, "
        "  source_kind, provenance) "
        "VALUES ('movie', 333, 'A', '4k/theme.mp3', 5000000, ?, "
        "        'aaa11111111', 'themerrdb', 'auto')",
        (now,),
    )
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, "
        "  media_folder, placed_at, placement_kind, provenance) "
        "VALUES ('movie', 333, 'A', '/data/4k/Movie', ?, 'copy', "
        "        'auto')",
        (now,),
    )
    # Section B: 4MB local_files + hardlink placement (must NOT
    # be counted by the copy SUM).
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "  file_path, file_size, downloaded_at, source_video_id, "
        "  source_kind, provenance) "
        "VALUES ('movie', 333, 'B', 'std/theme.mp3', 4000000, ?, "
        "        'bbb22222222', 'themerrdb', 'auto')",
        (now,),
    )
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, "
        "  media_folder, placed_at, placement_kind, provenance) "
        "VALUES ('movie', 333, 'B', '/data/std/Movie', ?, 'hardlink', "
        "        'auto')",
        (now,),
    )
    conn.commit()
    # Run BOTH the post-fix (with section_id) and pre-fix (without)
    # SUMs against this fixture so the bug is empirically captured.
    post_fix = conn.execute("""
        SELECT COALESCE(SUM(lf.file_size), 0)
          FROM placements p
          JOIN local_files lf
            ON lf.media_type = p.media_type
           AND lf.tmdb_id = p.tmdb_id
           AND lf.section_id = p.section_id
         WHERE p.placement_kind = 'copy'
    """).fetchone()[0]
    pre_fix = conn.execute("""
        SELECT COALESCE(SUM(lf.file_size), 0)
          FROM placements p
          JOIN local_files lf
            ON lf.media_type = p.media_type
           AND lf.tmdb_id = p.tmdb_id
         WHERE p.placement_kind = 'copy'
    """).fetchone()[0]
    conn.close()
    # Post-fix: only A's 5MB (the actual copy bytes).
    assert post_fix == 5000000, (
        f"Post-fix SUM should be 5,000,000 (A only), got {post_fix}. "
        "If higher, the section_id JOIN clause regressed."
    )
    # Pre-fix: 5MB + 4MB = 9MB (the bug).
    assert pre_fix == 9000000, (
        f"Pre-fix SUM should be 9,000,000 (A+B cartesian), got "
        f"{pre_fix}. The fixture is supposed to exhibit the bug "
        "the post-fix query closes."
    )


def test_section_scoped_join_handles_no_copy_placements(tmp_path):
    """Negative case: a library with zero copy placements must
    SUM to 0 (not error, not return inflated SUM via cartesian
    against hardlink placements that would no-op the WHERE)."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    now = now_iso()
    # One hardlink-only setup; SUM should be 0.
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES ('A', 'Movies', 'movie', 1, 0, 0, "
        "        'movies', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, first_seen_sync_at, "
        "                    last_seen_sync_at) "
        "VALUES ('movie', 444, 'No Copies', 2021, "
        "        'imdb', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "  file_path, file_size, downloaded_at, source_video_id, "
        "  source_kind, provenance) "
        "VALUES ('movie', 444, 'A', 'a/theme.mp3', 1000000, ?, "
        "        'ccc33333333', 'themerrdb', 'auto')",
        (now,),
    )
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, "
        "  media_folder, placed_at, placement_kind, provenance) "
        "VALUES ('movie', 444, 'A', '/data/Movies', ?, 'hardlink', "
        "        'auto')",
        (now,),
    )
    conn.commit()
    n = conn.execute("""
        SELECT COALESCE(SUM(lf.file_size), 0)
          FROM placements p
          JOIN local_files lf
            ON lf.media_type = p.media_type
           AND lf.tmdb_id = p.tmdb_id
           AND lf.section_id = p.section_id
         WHERE p.placement_kind = 'copy'
    """).fetchone()[0]
    conn.close()
    assert n == 0
