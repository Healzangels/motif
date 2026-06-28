"""v1.18.5 — CRITICAL: fix v55 migration FK cascade + recover lost data.

## The v1.18.0 bug

`_widen_check_constraint` rebuilt CHECK-constrained tables via
the canonical SQLite four-step dance:

  1. CREATE TABLE <name>_new with widened CHECK
  2. INSERT INTO <name>_new SELECT FROM <name>
  3. DROP TABLE <name>
  4. ALTER TABLE <name>_new RENAME TO <name>

`_migrate_v54_to_v55` invoked the helper for `themes` FIRST in
its widening list. The migration set `PRAGMA defer_foreign_keys
= ON` under the assumption it would suppress FK cascades during
step 3 (the DROP TABLE).

That was wrong. `defer_foreign_keys` ONLY defers FK violation
CHECKS to COMMIT time — it does NOT defer cascading actions.
When `DROP TABLE themes` fired, SQLite immediately ran:

  * `ON DELETE CASCADE` on `local_files (media_type, tmdb_id)
    REFERENCES themes (media_type, tmdb_id)` → every local_files
    row deleted.
  * `ON DELETE CASCADE` on `placements (media_type, tmdb_id)
    REFERENCES themes (media_type, tmdb_id)` → every placements
    row deleted.
  * `ON DELETE SET NULL` on `plex_items.theme_id REFERENCES
    themes(id)` → every plex_items.theme_id nulled.

Net effect on every install that ran the v55 migration with
existing data: total loss of motif's tracking metadata across
all libraries. The on-disk theme.mp3 files survived (the
filesystem wasn't touched); Plex's local-media-assets agent
still sees them so `pi.local_theme_file=1`; but the SRC letter
SQL classifies every previously-T/A/U row as 'M' because
`p.media_folder IS NULL AND pi.local_theme_file = 1` is the M
predicate.

## Fix in this version (v1.18.5)

1. `_migrate_v54_to_v55` switches from `defer_foreign_keys = ON`
   to `foreign_keys = OFF` — the SQLite-recommended pattern
   (sqlite.org/lang_altertable.html "Making Other Kinds Of Table
   Schema Changes"). foreign_keys=OFF disables BOTH violation
   checks AND cascading actions for the duration. Closed-out
   with `PRAGMA foreign_key_check` (defensive integrity probe)
   and `foreign_keys = ON` (restore enforcement).

2. `app/core/recovery_v55.py`:
   `maybe_recover_post_v55_data_loss(db_path, themes_dir)` —
   one-shot recovery walker invoked from `main.py` after
   `init_db`. Detects the loss pattern (themes populated,
   local_files+placements empty, plex_items have
   local_theme_file=1) and rebuilds tracking from on-disk state:

     * For each themes row T and managed section S, check if the
       expected canonical path (themes_dir/S.themes_subdir/
       canonical_theme_subdir(T.title, T.year)/theme.mp3) exists.
     * If yes, INSERT local_files with provenance/source_kind
       inferred from T.upstream_source + user_overrides presence.
     * For each plex_items row pi with local_theme_file=1 that
       matches T (by guid_tmdb OR title+year), INSERT placements
       pointing at pi.folder_path.
     * After the walk, run resolve_theme_ids to repopulate
       plex_items.theme_id from the rebuilt themes.

   Stamps `runtime_settings.recovery_v55_done_at` so subsequent
   boots skip the walk. Idempotent via `INSERT OR IGNORE`.
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

DB_PY = REPO / "app" / "core" / "db.py"
MAIN_PY = REPO / "app" / "main.py"
RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"


# ── Migration fix (source-level pin) ──────────────────────────


def test_v55_migration_uses_foreign_keys_off_not_defer():
    """`_migrate_v54_to_v55` must use `PRAGMA foreign_keys = OFF`
    around the rebuild loop, NOT `defer_foreign_keys = ON`. The
    latter only defers CHECK validation — cascading actions
    still fire on DROP TABLE."""
    import re
    src = DB_PY.read_text()
    fn_start = src.index("def _migrate_v54_to_v55(")
    fn_end = src.index("\ndef _widen_check_constraint(", fn_start + 1)
    body = src[fn_start:fn_end]
    # Strip docstrings + comments so the counter-pin below only
    # sees executable code (the docstring legitimately discusses
    # the historical defer_foreign_keys bug).
    body_no_docstring = re.sub(
        r'""".*?"""', '', body, count=1, flags=re.DOTALL,
    )
    body_no_comments = re.sub(
        r'#.*$', '', body_no_docstring, flags=re.MULTILINE,
    )
    assert 'PRAGMA foreign_keys = OFF' in body_no_comments, (
        "v1.18.5: migration must turn foreign_keys OFF before "
        "the rebuild loop — defer_foreign_keys doesn't suppress "
        "cascading actions"
    )
    # Counter-pin: defer_foreign_keys must NOT appear in
    # executable code (it CAN appear in narrative comments
    # describing the historical bug — that's fine).
    assert 'PRAGMA defer_foreign_keys = ON' not in body_no_comments, (
        "v1.18.5: the v1.18.0 defer_foreign_keys = ON line must "
        "be REMOVED from executable code — it caused the data-"
        "loss bug. (Narrative comments referencing the historical "
        "bug are fine.)"
    )


def test_v55_migration_restores_foreign_keys_in_finally():
    """The PRAGMA foreign_keys = ON must run via try/finally so
    a crash mid-rebuild doesn't leave the connection with FK
    enforcement OFF — that would mask later integrity bugs."""
    src = DB_PY.read_text()
    fn_start = src.index("def _migrate_v54_to_v55(")
    fn_end = src.index("\ndef _widen_check_constraint(", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "finally:" in body
    finally_idx = body.index("finally:")
    finally_block = body[finally_idx:finally_idx + 200]
    assert "PRAGMA foreign_keys = ON" in finally_block, (
        "v1.18.5: foreign_keys = ON restoration must live in the "
        "finally block so a crash mid-rebuild doesn't leak FK-off "
        "state"
    )


def test_v55_migration_runs_foreign_key_check_post_rebuild():
    """The migration must `PRAGMA foreign_key_check` after the
    rebuild loop to surface any orphaned references the rebuild
    might have introduced. The CHECK widening can't create
    violations (only adds legal values), but defensive insurance
    + surfaces pre-existing corruption masked by FKs-off."""
    src = DB_PY.read_text()
    fn_start = src.index("def _migrate_v54_to_v55(")
    fn_end = src.index("\ndef _widen_check_constraint(", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "PRAGMA foreign_key_check" in body


# ── End-to-end: the fixed migration preserves data ────────────


def _build_v54ish_db(db: Path) -> int:
    """Minimal v54-shaped fixture for the migration end-to-end
    test. Returns themes_id of the seeded movie row."""
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE themes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
            tmdb_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            UNIQUE (media_type, tmdb_id)
        );
        CREATE TABLE plex_items (
            rating_key TEXT PRIMARY KEY,
            media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'show')),
            title TEXT NOT NULL,
            theme_id INTEGER REFERENCES themes(id) ON DELETE SET NULL
        );
        CREATE TABLE local_files (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL,
            PRIMARY KEY (media_type, tmdb_id, section_id),
            FOREIGN KEY (media_type, tmdb_id)
              REFERENCES themes(media_type, tmdb_id) ON DELETE CASCADE
        );
        CREATE TABLE placements (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL,
            media_folder TEXT NOT NULL,
            placement_kind TEXT NOT NULL CHECK (
                placement_kind IN ('hardlink', 'copy', 'symlink')),
            PRIMARY KEY (media_type, tmdb_id, section_id, media_folder),
            FOREIGN KEY (media_type, tmdb_id)
              REFERENCES themes(media_type, tmdb_id) ON DELETE CASCADE
        );
        CREATE TABLE previous_urls (
            media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            youtube_url TEXT,
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        CREATE TABLE section_failure_acks (
            media_type TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL,
            acked_at TEXT,
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
    """)
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title) "
        "VALUES ('movie', 100, 'Test Movie')"
    )
    themes_id = conn.execute(
        "SELECT id FROM themes"
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO plex_items VALUES (?, ?, ?, ?)",
        ("rk-1", "movie", "Test Movie", themes_id),
    )
    conn.execute(
        "INSERT INTO local_files VALUES ('movie', 100, '1')"
    )
    conn.execute(
        "INSERT INTO placements VALUES "
        "('movie', 100, '1', '/data/foo', 'hardlink')"
    )
    conn.commit()
    conn.close()
    return themes_id


def test_fixed_migration_preserves_local_files_placements_theme_id(tmp_path: Path):
    """The fixed `_widen_check_constraint` (called by
    `_migrate_v54_to_v55` under `foreign_keys = OFF`) must
    preserve every dependent row + the plex_items.theme_id
    linkage. This is the exact regression that wiped the user's
    install."""
    db = tmp_path / "v54.db"
    themes_id = _build_v54ish_db(db)
    conn = sqlite3.connect(db)
    try:
        from app.core.db import _widen_check_constraint
        # Mirror _migrate_v54_to_v55 shape: PRAGMA dance + rebuild.
        conn.execute("PRAGMA foreign_keys = OFF")
        _widen_check_constraint(
            conn, "themes", "media_type",
            ("movie", "tv"), ("movie", "tv", "collection"),
        )
        _widen_check_constraint(
            conn, "plex_items", "media_type",
            ("movie", "show"), ("movie", "show", "collection"),
        )
        _widen_check_constraint(
            conn, "placements", "placement_kind",
            ("hardlink", "copy", "symlink"),
            ("hardlink", "copy", "symlink", "plex_upload"),
        )
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        conn.execute("PRAGMA foreign_keys = ON")
        conn.commit()
        assert violations == [], (
            f"v1.18.5: foreign_key_check must report zero "
            f"violations after the rebuild — got {violations}"
        )
        assert conn.execute("SELECT COUNT(*) FROM local_files").fetchone()[0] == 1, (
            "v1.18.5: local_files row must SURVIVE the rebuild"
        )
        assert conn.execute("SELECT COUNT(*) FROM placements").fetchone()[0] == 1, (
            "v1.18.5: placements row must SURVIVE the rebuild"
        )
        pi_theme_id = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='rk-1'"
        ).fetchone()[0]
        assert pi_theme_id == themes_id, (
            f"v1.18.5: plex_items.theme_id must remain {themes_id}, "
            f"got {pi_theme_id}"
        )
    finally:
        conn.close()


# ── Recovery walker (source-level pins) ────────────────────────


def test_recovery_module_exists_and_exports_entry_point():
    """`app/core/recovery_v55.py` must exist and export
    `maybe_recover_post_v55_data_loss`."""
    assert RECOVERY_PY.is_file(), (
        "v1.18.5: app/core/recovery_v55.py must exist"
    )
    src = RECOVERY_PY.read_text()
    assert "def maybe_recover_post_v55_data_loss(" in src


def test_recovery_detects_loss_pattern():
    """v1.18.7: ratio-based detection. The detector must:
      - Check themes populated (non-orphan) — anchor on
        `upstream_source != 'plex_orphan'`.
      - Read pi.local_theme_file=1 count as the sidecar signal.
      - Compare local_files count to sidecar count via ratio,
        NOT require local_files==0 (the v1.18.5 over-strict
        gate that silently disqualified the user's install after
        a single post-bug manual SET URL inserted ONE
        local_files row)."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def _detect_loss_pattern(")
    fn_end = src.index("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "upstream_source != 'plex_orphan'" in body
    assert "local_theme_file = 1" in body
    # The new threshold: lf < sidecars * 0.5
    assert "pi_sidecar_n * 0.5" in body, (
        "v1.18.7: detector must use the ratio threshold "
        "(lf < sidecars * 0.5), not the v1.18.5 strict "
        "lf==0 gate"
    )


def test_recovery_walker_is_idempotent():
    """The walker must use `INSERT OR IGNORE` so a re-run
    (operator manually re-triggers, or a parallel boot race)
    doesn't double-insert. Stamping
    `runtime_settings.recovery_v55_done_at` prevents the
    re-walk; INSERT OR IGNORE is the second layer of
    defense."""
    src = RECOVERY_PY.read_text()
    assert "INSERT OR IGNORE INTO local_files" in src
    assert "INSERT OR IGNORE INTO placements" in src
    assert "recovery_v55_done_at" in src


def test_recovery_walker_runs_resolve_theme_ids_post_walk():
    """After rebuilding local_files + placements, the walker
    must call `resolve_theme_ids` so plex_items.theme_id picks
    up the linkage (the nulled column is what makes everything
    show as M)."""
    src = RECOVERY_PY.read_text()
    assert "from .plex_enum import resolve_theme_ids" in src
    assert "resolve_theme_ids(db_path)" in src


def test_recovery_walker_inferred_source_kind_branches():
    """`_infer_source_kind` must return:
      - ('manual', 'adopt') for plex_orphan upstream
      - ('manual', 'url') for TDB-tracked with user override
      - ('auto', 'themerrdb') for TDB-tracked without override
    So the recovered local_files rows render with the correct
    SRC letter (A / U / T respectively)."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def _infer_source_kind(")
    fn_end = src.index("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    assert 'return ("manual", "adopt")' in body
    assert 'return ("manual", "url")' in body
    assert 'return ("auto", "themerrdb")' in body


# ── End-to-end recovery: rebuild from disk ─────────────────────


@pytest.fixture
def recovery_fixture(tmp_path: Path, monkeypatch):
    """Build a v55-broken-state DB + on-disk theme.mp3 files so
    the recovery walker has something to find. v1.18.7: lower
    the sidecar-evidence floor so the single-row fixture
    triggers detection (real-world libraries have thousands of
    sidecars; the fixture's one is sufficient signal to test
    the walk logic)."""
    monkeypatch.setattr(
        "app.core.recovery_v55._SIDECAR_EVIDENCE_FLOOR", 1,
    )
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T00:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # Seed plex_sections — themes_subdir mirrors what
        # _allocate_themes_subdir would have written.
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        # Seed themes (the TDB sync survived the bug).
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   youtube_url, youtube_video_id, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 100, 'Test Movie', 'test movie', '2020', "
            "        'https://youtube.com/watch?v=ABC1234567X', "
            "        'ABC1234567X', 'themoviedb', ?, ?)",
            (ts, ts),
        )
        # v1.18.11: walker now checks pi.folder_path/theme.mp3
        # for existence directly (was: trust pi.local_theme_file).
        # Use a tmp-relative path so the test can create the
        # sidecar file at the same location.
        plex_folder = tmp_path / "plex_movies" / "Test Movie (2020)"
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, guid_tmdb, folder_path, "
            "   local_theme_file, first_seen_at, last_seen_at) "
            "VALUES ('rk-100', '1', 'movie', 'Test Movie', "
            "        'test movie', '2020', 100, ?, "
            "        1, ?, ?)",
            (str(plex_folder), ts, ts),
        )
        conn.commit()
    # Now create the on-disk canonical file the recovery walker
    # will find. Path: themes_dir/movies/Test Movie (2020)/theme.mp3
    movie_dir = themes_dir / "movies" / "Test Movie (2020)"
    movie_dir.mkdir(parents=True)
    canonical_file = movie_dir / "theme.mp3"
    canonical_file.write_bytes(b"\xff\xfb\x90" + b"\x00" * 1024)
    # v1.18.11: also seed the on-disk Plex sidecar so the walker's
    # disk-check finds it and can INSERT a placements row. Same
    # bytes as canonical so the walker's last-resort size match
    # (catches orphans with completely different titles) passes.
    plex_folder.mkdir(parents=True)
    (plex_folder / "theme.mp3").write_bytes(
        b"\xff\xfb\x90" + b"\x00" * 1024,
    )
    return db_path, themes_dir


def test_recovery_walker_rebuilds_local_files_from_disk(recovery_fixture):
    """End-to-end: detect-and-recover walker finds the on-disk
    canonical theme.mp3, inserts the local_files row, and stamps
    the recovery marker."""
    db_path, themes_dir = recovery_fixture
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    stats = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    assert stats["detected"] is True
    assert stats["local_files_inserted"] >= 1, (
        "v1.18.5: walker must INSERT local_files for the seeded "
        "themes row whose canonical file exists on disk"
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lf = conn.execute(
            "SELECT * FROM local_files "
            "WHERE media_type='movie' AND tmdb_id=100 AND section_id='1'"
        ).fetchone()
    assert lf is not None
    assert lf["file_path"] == "movies/Test Movie (2020)/theme.mp3"
    assert lf["provenance"] == "auto", (
        "v1.18.5: TDB-tracked row without user override must "
        "recover as provenance='auto'"
    )
    assert lf["source_kind"] == "themerrdb", (
        "v1.18.5: TDB-tracked row must recover as "
        "source_kind='themerrdb' so SRC letter renders as T"
    )


def test_recovery_walker_rebuilds_placements_from_plex_match(recovery_fixture):
    """The walker must also INSERT placements for plex_items
    rows that match the themes row via guid_tmdb."""
    db_path, themes_dir = recovery_fixture
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    stats = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    assert stats["placements_inserted"] >= 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        pl = conn.execute(
            "SELECT * FROM placements "
            "WHERE media_type='movie' AND tmdb_id=100 AND section_id='1'"
        ).fetchone()
    assert pl is not None
    # v1.18.11: media_folder is whatever pi.folder_path was at
    # walker time (now using a tmp-relative path so the fixture
    # can create the sidecar file there). End suffix check
    # rather than full-path equality.
    assert pl["media_folder"].endswith(
        "plex_movies/Test Movie (2020)"
    )
    assert pl["plex_rating_key"] == "rk-100"
    assert pl["placement_kind"] == "hardlink"


def test_recovery_walker_relinks_theme_id_post_walk(recovery_fixture):
    """resolve_theme_ids must run after the walk so
    plex_items.theme_id is repopulated from the rebuilt themes
    (the original SET NULL nuked the linkage column)."""
    db_path, themes_dir = recovery_fixture
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    maybe_recover_post_v55_data_loss(db_path, themes_dir)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        pi_theme_id = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='rk-100'"
        ).fetchone()["theme_id"]
        themes_row = conn.execute(
            "SELECT id FROM themes WHERE media_type='movie' AND tmdb_id=100"
        ).fetchone()
    assert pi_theme_id == themes_row["id"], (
        "v1.18.5: walker must call resolve_theme_ids to re-link "
        "plex_items.theme_id"
    )


def test_recovery_walker_is_self_gating(recovery_fixture):
    """Second call must be a no-op (zero new inserts) because
    the first call stamped the marker."""
    db_path, themes_dir = recovery_fixture
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    stats1 = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    assert stats1["detected"] is True
    stats2 = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    assert stats2["detected"] is False, (
        "v1.18.5: second call must be a no-op (marker stamped)"
    )
    assert stats2["local_files_inserted"] == 0


def test_recovery_walker_noops_when_no_loss_pattern(tmp_path: Path):
    """Walker must early-return on installs that don't match the
    loss pattern — fresh installs, healthy installs, etc."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    # No themes, no plex_items — fresh install.
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    stats = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    assert stats["detected"] is False
    assert stats["local_files_inserted"] == 0


def test_recovery_walker_noops_when_themes_dir_missing(tmp_path: Path):
    """Walker must early-return when themes_dir is None or
    doesn't exist on disk. Avoids a stat-storm on first-run
    installs where themes_dir hasn't been configured yet."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    # themes_dir=None case.
    stats = maybe_recover_post_v55_data_loss(db_path, None)
    assert stats["detected"] is False
    # themes_dir-doesn't-exist case.
    stats = maybe_recover_post_v55_data_loss(
        db_path, tmp_path / "nonexistent",
    )
    assert stats["detected"] is False
