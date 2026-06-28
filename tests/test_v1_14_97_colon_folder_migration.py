"""v1.14.97 — one-shot rename sweep for v1.14.94's colon-folder fix.

the user: "1.14.94 folder name but can we make it so it updates
existing?"

v1.14.94 changed canonical_theme_subdir so titles like
`Terminator 2: Judgment Day` produce `Terminator 2 - Judgment
Day (1991)` instead of `Terminator 2- Judgment Day (1991)`.
But only NEW downloads got the corrected name; existing
folders kept the legacy hyphen-no-space shape.

This test suite covers `migrate_v1_14_94_colon_folders` which
runs once at startup, walks local_files, and renames any
folder matching the legacy pattern to the v1.14.94 form.

## Safety properties under test

1. Idempotent — re-running after success does nothing.
2. Targets EXACTLY the v1.14.94 pattern. Title drift (folder
   matches neither legacy nor new shape) is left alone.
3. Crash recovery — if a previous run renamed the folder but
   crashed before the DB update, the next run detects this
   (old missing, new present) and just updates the DB.
4. Conflict skip — if the new name already exists with
   content, log + skip rather than clobber.
5. canonical_missing rows (folder gone entirely) are
   left alone — the existing missing-state UI handles them.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from app.core import db as db_module
from app.core.canonical import (
    canonical_theme_subdir,
    legacy_canonical_theme_subdir_pre_v1_14_94,
)
from app.core.sections import migrate_v1_14_94_colon_folders


# ── Helpers ────────────────────────────────────────────────────


def _setup_db(tmp_path):
    """Fresh DB at <tmp_path>/motif.db with a single managed
    section ('movies') ready for local_files inserts."""
    db_path = tmp_path / "motif.db"
    db_module.init_db(db_path)
    with db_module.get_conn(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "(section_id, title, type, included, "
            " discovered_at, last_seen_at, themes_subdir) "
            "VALUES ('1', 'Movies', 'movie', 1, "
            "        datetime('now'), datetime('now'), 'movies')"
        )
    return db_path


def _insert_theme_and_local_file(
    db_path, *, media_type, tmdb_id, title, year, file_path,
):
    """Insert a themes + local_files row for a test fixture."""
    with db_module.get_conn(db_path) as conn:
        conn.execute(
            "INSERT INTO themes "
            "(media_type, tmdb_id, title, year, upstream_source, "
            " youtube_url, last_seen_sync_at, first_seen_sync_at) "
            "VALUES (?, ?, ?, ?, 'themoviedb', "
            "        'https://example.com/x', "
            "        datetime('now'), datetime('now'))",
            (media_type, tmdb_id, title, year),
        )
        conn.execute(
            "INSERT INTO local_files "
            "(media_type, tmdb_id, section_id, file_path, "
            " downloaded_at, source_video_id) "
            "VALUES (?, ?, '1', ?, datetime('now'), 'vid123')",
            (media_type, tmdb_id, file_path),
        )


def _read_file_path(db_path, media_type, tmdb_id):
    with db_module.get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT file_path FROM local_files "
            "WHERE media_type = ? AND tmdb_id = ?",
            (media_type, tmdb_id),
        ).fetchone()
    return row["file_path"] if row else None


# ── The legacy + new sanitizer outputs (sanity) ────────────────


def test_legacy_sanitizer_matches_pre_v1_14_94_shape():
    """The legacy sanitizer must produce the exact pre-v1.14.94
    output for a colon-bearing title — that's the signature the
    migration uses to identify legacy folders."""
    assert legacy_canonical_theme_subdir_pre_v1_14_94(
        "Terminator 2: Judgment Day", "1991",
    ) == "Terminator 2- Judgment Day (1991)"
    assert legacy_canonical_theme_subdir_pre_v1_14_94(
        "Yu-Gi-Oh!: The Dark Side of Dimensions", "2016",
    ) == "Yu-Gi-Oh!- The Dark Side of Dimensions (2016)"


def test_new_sanitizer_produces_v1_14_94_shape():
    """The current sanitizer must produce the v1.14.94 form
    (space-dash-space)."""
    assert canonical_theme_subdir(
        "Terminator 2: Judgment Day", "1991",
    ) == "Terminator 2 - Judgment Day (1991)"


# ── Migration behaviors ────────────────────────────────────────


def test_renames_legacy_folder_and_updates_db(tmp_path):
    """The exact the user case: a folder named with the legacy
    `Title- Subtitle (year)` shape gets renamed to
    `Title - Subtitle (year)` and the local_files DB row's
    file_path is updated."""
    db_path = _setup_db(tmp_path)
    themes_dir = tmp_path / "themes"
    # Create the legacy-shape folder + theme.mp3.
    legacy = themes_dir / "movies" / "Terminator 2- Judgment Day (1991)"
    legacy.mkdir(parents=True)
    (legacy / "theme.mp3").write_bytes(b"fake mp3 content")
    # DB row points at the legacy path.
    _insert_theme_and_local_file(
        db_path,
        media_type="movie", tmdb_id=280,
        title="Terminator 2: Judgment Day", year=1991,
        file_path="movies/Terminator 2- Judgment Day (1991)/theme.mp3",
    )

    stats = migrate_v1_14_94_colon_folders(db_path, themes_dir)

    assert stats["renamed"] == 1
    assert stats["scanned"] == 1
    # On disk: new path exists, old gone.
    new_path = themes_dir / "movies" / "Terminator 2 - Judgment Day (1991)"
    assert new_path.is_dir()
    assert (new_path / "theme.mp3").read_bytes() == b"fake mp3 content"
    assert not legacy.exists()
    # DB: file_path updated.
    assert _read_file_path(db_path, "movie", 280) == (
        "movies/Terminator 2 - Judgment Day (1991)/theme.mp3"
    )


def test_idempotent_second_run_is_noop(tmp_path):
    """After a successful migration, re-running does nothing —
    the folder already matches the new canonical so the row is
    skipped at the `current_dir == new_canonical` check."""
    db_path = _setup_db(tmp_path)
    themes_dir = tmp_path / "themes"
    # Already on the new shape from the start.
    new_dir = themes_dir / "movies" / "Mission - Impossible (1996)"
    new_dir.mkdir(parents=True)
    (new_dir / "theme.mp3").write_bytes(b"x")
    _insert_theme_and_local_file(
        db_path,
        media_type="movie", tmdb_id=954,
        title="Mission: Impossible", year=1996,
        file_path="movies/Mission - Impossible (1996)/theme.mp3",
    )

    stats = migrate_v1_14_94_colon_folders(db_path, themes_dir)
    assert stats["scanned"] == 1
    assert stats["renamed"] == 0
    assert stats["db_repaired"] == 0
    assert stats["drift_skipped"] == 0


def test_no_colon_titles_are_skipped_entirely(tmp_path):
    """Titles without colons → both legacy and new sanitizers
    produce the same output → migration short-circuits before
    even checking the filesystem. Don't touch."""
    db_path = _setup_db(tmp_path)
    themes_dir = tmp_path / "themes"
    plain = themes_dir / "movies" / "Inception (2010)"
    plain.mkdir(parents=True)
    (plain / "theme.mp3").write_bytes(b"x")
    _insert_theme_and_local_file(
        db_path,
        media_type="movie", tmdb_id=27205,
        title="Inception", year=2010,
        file_path="movies/Inception (2010)/theme.mp3",
    )

    stats = migrate_v1_14_94_colon_folders(db_path, themes_dir)
    assert stats["scanned"] == 1
    assert stats["renamed"] == 0
    assert stats["drift_skipped"] == 0
    # Folder untouched.
    assert plain.is_dir()


def test_title_drift_leaves_folder_alone(tmp_path):
    """If the title in DB doesn't match the folder via EITHER the
    legacy or new sanitizer, the title drifted independently of
    the v1.14.94 fix. Renaming would be wrong — leave alone."""
    db_path = _setup_db(tmp_path)
    themes_dir = tmp_path / "themes"
    drift = themes_dir / "movies" / "Some Old Name (2020)"
    drift.mkdir(parents=True)
    (drift / "theme.mp3").write_bytes(b"x")
    _insert_theme_and_local_file(
        db_path,
        media_type="movie", tmdb_id=999,
        title="A New Title: With Colon", year=2020,
        file_path="movies/Some Old Name (2020)/theme.mp3",
    )

    stats = migrate_v1_14_94_colon_folders(db_path, themes_dir)
    assert stats["scanned"] == 1
    assert stats["renamed"] == 0
    assert stats["drift_skipped"] == 1
    # Original folder untouched.
    assert drift.is_dir()


def test_conflict_skip_when_new_path_already_exists(tmp_path):
    """If both old and new folders exist on disk (user manually
    created the new one), don't clobber — log + skip + leave
    DB on the old pointer."""
    db_path = _setup_db(tmp_path)
    themes_dir = tmp_path / "themes"
    legacy = themes_dir / "movies" / "Mission- Impossible (1996)"
    legacy.mkdir(parents=True)
    (legacy / "theme.mp3").write_bytes(b"old content")
    new = themes_dir / "movies" / "Mission - Impossible (1996)"
    new.mkdir(parents=True)
    (new / "theme.mp3").write_bytes(b"user content")
    _insert_theme_and_local_file(
        db_path,
        media_type="movie", tmdb_id=954,
        title="Mission: Impossible", year=1996,
        file_path="movies/Mission- Impossible (1996)/theme.mp3",
    )

    stats = migrate_v1_14_94_colon_folders(db_path, themes_dir)
    assert stats["conflict_skipped"] == 1
    assert stats["renamed"] == 0
    # Both folders survive; user's content untouched.
    assert legacy.is_dir()
    assert (legacy / "theme.mp3").read_bytes() == b"old content"
    assert (new / "theme.mp3").read_bytes() == b"user content"
    # DB stays on the old pointer.
    assert _read_file_path(db_path, "movie", 954) == (
        "movies/Mission- Impossible (1996)/theme.mp3"
    )


def test_crash_recovery_db_pointer_repair(tmp_path):
    """A previous run renamed the folder on disk but crashed
    before the DB update committed. Next run: old folder
    missing, new folder exists with the theme.mp3. Migration
    detects this and updates the DB pointer without re-renaming."""
    db_path = _setup_db(tmp_path)
    themes_dir = tmp_path / "themes"
    # Only the new folder exists (simulating post-rename, pre-DB-update state).
    new = themes_dir / "movies" / "Mission - Impossible (1996)"
    new.mkdir(parents=True)
    (new / "theme.mp3").write_bytes(b"rescued content")
    _insert_theme_and_local_file(
        db_path,
        media_type="movie", tmdb_id=954,
        title="Mission: Impossible", year=1996,
        file_path="movies/Mission- Impossible (1996)/theme.mp3",
    )

    stats = migrate_v1_14_94_colon_folders(db_path, themes_dir)
    assert stats["db_repaired"] == 1
    assert stats["renamed"] == 0
    # DB pointer updated to the new path.
    assert _read_file_path(db_path, "movie", 954) == (
        "movies/Mission - Impossible (1996)/theme.mp3"
    )


def test_canonical_missing_left_alone(tmp_path):
    """If neither old nor new folder exists on disk, the row is
    canonical_missing — the next stat-driven UI render handles
    it. Don't touch the DB pointer (would just shift the broken
    state to a different missing folder)."""
    db_path = _setup_db(tmp_path)
    themes_dir = tmp_path / "themes"
    (themes_dir / "movies").mkdir(parents=True)
    # No folder for this row at all.
    _insert_theme_and_local_file(
        db_path,
        media_type="movie", tmdb_id=954,
        title="Mission: Impossible", year=1996,
        file_path="movies/Mission- Impossible (1996)/theme.mp3",
    )

    stats = migrate_v1_14_94_colon_folders(db_path, themes_dir)
    assert stats["canonical_missing"] == 1
    assert stats["renamed"] == 0
    # DB stays as-is.
    assert _read_file_path(db_path, "movie", 954) == (
        "movies/Mission- Impossible (1996)/theme.mp3"
    )


def test_multiple_rows_one_pass(tmp_path):
    """A representative mix in one sweep: 1 needs-rename, 1
    no-colon (skip), 1 already-migrated (skip), 1 drift (skip).
    Counts add up correctly."""
    db_path = _setup_db(tmp_path)
    themes_dir = tmp_path / "themes"
    # 1. Needs rename
    legacy = themes_dir / "movies" / "Terminator 2- Judgment Day (1991)"
    legacy.mkdir(parents=True)
    (legacy / "theme.mp3").write_bytes(b"x")
    _insert_theme_and_local_file(
        db_path, media_type="movie", tmdb_id=280,
        title="Terminator 2: Judgment Day", year=1991,
        file_path="movies/Terminator 2- Judgment Day (1991)/theme.mp3",
    )
    # 2. No colon — skip
    plain = themes_dir / "movies" / "Inception (2010)"
    plain.mkdir(parents=True)
    (plain / "theme.mp3").write_bytes(b"x")
    _insert_theme_and_local_file(
        db_path, media_type="movie", tmdb_id=27205,
        title="Inception", year=2010,
        file_path="movies/Inception (2010)/theme.mp3",
    )
    # 3. Already migrated
    already = themes_dir / "movies" / "Mission - Impossible (1996)"
    already.mkdir(parents=True)
    (already / "theme.mp3").write_bytes(b"x")
    _insert_theme_and_local_file(
        db_path, media_type="movie", tmdb_id=954,
        title="Mission: Impossible", year=1996,
        file_path="movies/Mission - Impossible (1996)/theme.mp3",
    )
    # 4. Drift
    drift = themes_dir / "movies" / "Some Old Name (2020)"
    drift.mkdir(parents=True)
    (drift / "theme.mp3").write_bytes(b"x")
    _insert_theme_and_local_file(
        db_path, media_type="movie", tmdb_id=999,
        title="A New Title: With Colon", year=2020,
        file_path="movies/Some Old Name (2020)/theme.mp3",
    )

    stats = migrate_v1_14_94_colon_folders(db_path, themes_dir)
    assert stats["scanned"] == 4
    assert stats["renamed"] == 1
    assert stats["drift_skipped"] == 1
    assert stats["db_repaired"] == 0
    assert stats["conflict_skipped"] == 0
    # The renamed row's path now reflects the new shape.
    assert _read_file_path(db_path, "movie", 280) == (
        "movies/Terminator 2 - Judgment Day (1991)/theme.mp3"
    )


def test_main_startup_wires_the_migration(tmp_path):
    """main.py must call migrate_v1_14_94_colon_folders right
    after migrate_themes_subdirs_inplace so the rename runs at
    the next docker restart after upgrading."""
    main_src = (
        Path(__file__).resolve().parent.parent / "app" / "main.py"
    ).read_text()
    assert "migrate_v1_14_94_colon_folders" in main_src
    # The call must be inside the is_paths_ready guard (themes_dir
    # may not be configured yet on first-run installs).
    paths_idx = main_src.index("if settings.is_paths_ready():")
    next_block_idx = main_src.index(
        "Plex section discovery", paths_idx,
    )
    paths_block = main_src[paths_idx:next_block_idx]
    assert "migrate_v1_14_94_colon_folders" in paths_block, (
        "migrate_v1_14_94_colon_folders must be wired inside the "
        "is_paths_ready() guarded block"
    )
