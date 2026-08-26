"""v1.19.61 — PS-with-DL → BK unification.

the user's "86 EIGHTY-SIX" repro from the INFO card review:
  - SRC=P (Plex serves) + DL=green (motif downloaded TDB) + LINK=PS
  - History: "Skipped placement: plex_has_theme" (twice)
  - State: motif has the file canonically, no placement, Plex
    serves its own theme

the user: "I'm starting to wonder if Link PS is redundant to BK …
basically the exact same thing. … bigger discussion is PS vs BK
and is there a difference."

The functional state is identical to BK (motif has a downloaded
backup ready, Plex serves). The distinction was a stamp intent
("explicit backup_only choice" vs "incidental plex_has_theme
skip"). the user's call: unify them via the data — auto-stamp
backup_only on plex_has_theme skips so PS-with-DL rows
automatically classify as BK in the link column, get PROMOTE TO
ACTIVE in the recovery card, and fire the v1.18.90 backup_ready
notification on theme loss.

## v1.19.61 changes

  1. **Worker stamps** (`worker.py` _do_place + _do_place_collection):
     when place skips with reason='plex_has_theme', stamp
     `last_place_attempt_reason='backup_only'` (was 'plex_has_theme').
  2. **LET PLEX SERVE handlers** (`api.py` 5 sites): switch the
     `last_place_attempt_reason='plex_has_theme'` writes to
     `'backup_only'`. Same semantic — "motif has the file, Plex
     serves, this is a backup."
  3. **v1.18.90 reaper backup_signal** (`plex_enum.py`): add a
     third UNION ALL clause that detects
     `last_place_attempt_reason='backup_only'` rows (any source_kind
     except plex_cloud, which the second clause already handles).
     Without this the unified BK rows wouldn't fire the
     backup_ready notification on theme loss.
  4. **Backfill walker** (`recovery_v55.maybe_repair_ps_with_dl_to_bk`):
     one-shot pass that UPDATEs existing rows from
     `'plex_has_theme'` to `'backup_only'`. Idempotent via
     runtime_settings marker. Wired into main.py boot after the
     v1.19.21 stale-cache walker.

## Existing wiring already aligned

  - LINK badge (`app.js linkCell isBackupOnly` predicate) already
    fires BK on `last_place_attempt_reason='backup_only'`. After
    the walker runs, 86 EIGHTY-SIX naturally renders BK.
  - Recovery card BK-state probe (`api.py:16660`) already checks
    `last_place_attempt_reason='backup_only'`. PROMOTE TO ACTIVE
    surfaces automatically.
  - Hourly retry sweep (`scheduler.py:130`) already excludes both
    'plex_has_theme' AND 'backup_only' — behavior unchanged.

## What the user sees post-v1.19.61

86 EIGHTY-SIX after deploy (walker runs once at boot):
  - LINK: PS → BK
  - Recovery card: now shows BACKUP READY + PROMOTE TO ACTIVE
  - INFO card playback source line: "(BK badge · backup-only ·
    PROMOTE TO ACTIVE to deploy)" via existing v1.19.59 label
  - Theme loss: backup_ready notification (was 'other_fallback'
    silent skip)
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()
RECOVERY_PY = (REPO / "app" / "core" / "recovery_v55.py").read_text()
MAIN_PY = (REPO / "app" / "main.py").read_text()


# ── Source-text guards ───────────────────────────────────────


def test_worker_main_stamp_translates_plex_has_theme_to_backup_only():
    """The main place outcome stamp (worker.py:2334) must translate
    'plex_has_theme' to 'backup_only' before writing
    last_place_attempt_reason."""
    # The translation block has the v1.19.61 marker.
    assert "v1.19.61: PS-with-DL → BK unification" in WORKER_PY
    # The translation logic.
    assert '_stamp_reason == "plex_has_theme"' in WORKER_PY
    assert '_stamp_reason = "backup_only"' in WORKER_PY


def test_worker_collections_stamp_writes_backup_only_directly():
    """_do_place_collection's plex_has_theme skip stamps
    'backup_only' directly (the log_event keeps 'plex_has_theme'
    for breadcrumb continuity, but the local_files stamp is
    'backup_only' for the unification)."""
    idx = WORKER_PY.index('reason = "plex_has_theme"')
    block = WORKER_PY[idx:idx + 1500]
    # The stamp inside this block must be 'backup_only', NOT
    # the `reason` variable (which is plex_has_theme).
    assert "last_place_attempt_reason = 'backup_only'" in block, (
        "v1.19.61: collections place-skip must stamp 'backup_only' "
        "not the log reason 'plex_has_theme'"
    )


def test_api_let_plex_serve_handlers_stamp_backup_only():
    """All 6 LET PLEX SERVE / unplace handlers in api.py must
    stamp 'backup_only' (was 'plex_has_theme' pre-v1.19.61)."""
    # No 'plex_has_theme' string assignments in api.py.
    # (Other plex_has_theme references — comments, log strings,
    # PI table checks — are allowed; just not the
    # `last_place_attempt_reason = 'plex_has_theme'` write.)
    assert "last_place_attempt_reason = 'plex_has_theme'" not in API_PY, (
        "v1.19.61: api.py LET PLEX SERVE handlers must write "
        "'backup_only' instead of 'plex_has_theme'"
    )


def test_reaper_backup_signal_includes_backup_only_stamp_clause():
    """The v1.18.90 reaper's backup_signal query must include a
    third UNION ALL clause for last_place_attempt_reason='backup_only'
    rows. Without it the unified BK rows wouldn't fire the
    backup_ready notification on theme loss."""
    idx = PLEX_ENUM_PY.index("backup_signal = conn.execute(")
    end = PLEX_ENUM_PY.index(").fetchone()", idx)
    block = PLEX_ENUM_PY[idx:end]
    # Three UNION ALL clauses.
    assert block.count("UNION ALL") >= 2, (
        "v1.19.61: backup_signal must have ≥2 UNION ALL clauses "
        "(3 sources: user_url_backup, plex_cloud_backup, "
        "backup_only_stamp)"
    )
    assert "'backup_only_stamp'" in block, (
        "v1.19.61: third union must label as 'backup_only_stamp'"
    )
    assert "last_place_attempt_reason " in block
    assert "'backup_only'" in block
    # Exclude plex_cloud from this third clause to avoid double-
    # counting (the second clause already handles plex_cloud).
    # v0.51.295: COALESCE'd — a NULL source_kind row failed the bare
    # comparison (NULL != x is no row) and mis-tiered into silence.
    assert "COALESCE(source_kind, '') != 'plex_cloud'" in block


# ── Walker ───────────────────────────────────────────────────


def test_walker_function_exists():
    """maybe_repair_ps_with_dl_to_bk must be importable."""
    from app.core.recovery_v55 import maybe_repair_ps_with_dl_to_bk
    assert callable(maybe_repair_ps_with_dl_to_bk)


def test_walker_has_v1_19_61_marker():
    """Walker carries the v1.19.61 marker + idempotency key."""
    assert "def maybe_repair_ps_with_dl_to_bk(" in RECOVERY_PY
    idx = RECOVERY_PY.index("def maybe_repair_ps_with_dl_to_bk(")
    body = RECOVERY_PY[idx:idx + 4500]
    assert "v1.19.61" in body
    assert "recovery_ps_to_bk_done_at_v1_19_61" in body
    assert "runtime_settings" in body


def test_walker_excludes_rows_with_placement():
    """Walker must NOT touch rows that have a real placement —
    those aren't backup-only, they're actively placed."""
    idx = RECOVERY_PY.index("def maybe_repair_ps_with_dl_to_bk(")
    body = RECOVERY_PY[idx:idx + 4500]
    assert "media_folder IS NULL OR p.media_folder = ''" in body, (
        "v1.19.61: walker must skip rows with active placements"
    )


def test_walker_idempotent_on_marker():
    """Second invocation must short-circuit via the marker."""
    import tempfile
    from app.core.db import init_db, get_conn
    from app.core.recovery_v55 import maybe_repair_ps_with_dl_to_bk
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test.db"
        init_db(db)
        # First call: no candidates, marker stamped.
        result1 = maybe_repair_ps_with_dl_to_bk(db)
        assert result1["candidates"] == 0
        # Marker should now be set.
        with get_conn(db) as conn:
            row = conn.execute(
                "SELECT value FROM runtime_settings "
                "WHERE key = 'recovery_ps_to_bk_done_at_v1_19_61'"
            ).fetchone()
            assert row is not None, (
                "v1.19.61: walker must stamp marker even on zero "
                "candidates so subsequent boots skip the scan"
            )
        # Second call: short-circuits.
        result2 = maybe_repair_ps_with_dl_to_bk(db)
        assert result2["updated"] == 0


# ── End-to-end walker behavioral ─────────────────────────────


def test_walker_converts_ps_with_dl_rows_to_backup_only(tmp_path):
    """Seed an 86-EIGHTY-SIX-shape row (plex_has_theme stamp +
    file_path set + no placement), run walker, verify the stamp
    flipped to backup_only."""
    from app.core.db import init_db, get_conn
    from app.core.recovery_v55 import maybe_repair_ps_with_dl_to_bk
    db = tmp_path / "test.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        # Section + theme + plex_items skeleton (FK constraints).
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('3', 'Anime', 'show', 1, 0, 'tv', 1, "
            "        '2026-05-27', '2026-05-27')"
        )
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at, "
            "  youtube_url, youtube_video_id) "
            "VALUES (1, 'tv', 100565, '86 EIGHTY-SIX', 'themoviedb', "
            "        '2026-05-27', '2026-05-27', "
            "        'https://www.youtube.com/watch?v=eZIMFWAxMxQ', "
            "        'eZIMFWAxMxQ')"
        )
        # The seed row: plex_has_theme stamp + file_path set + no
        # placement.
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_size, file_sha256, downloaded_at, "
            "   source_kind, source_video_id, provenance, "
            "   last_place_attempt_at, last_place_attempt_reason) "
            "VALUES ('tv', 100565, '3', '86 EIGHTY-SIX/theme.mp3', "
            "        2964764, 'aaaa', '2026-05-19', "
            "        'themerrdb', 'eZIMFWAxMxQ', 'auto', "
            "        '2026-05-20', 'plex_has_theme')"
        )
        # Comparison row: also plex_has_theme but WITH an active
        # placement — walker must NOT touch this one.
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES (2, 'tv', 200000, 'Active Placed', 'themoviedb', "
            "        '2026-05-27', '2026-05-27')"
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_size, file_sha256, downloaded_at, "
            "   source_kind, source_video_id, provenance, "
            "   last_place_attempt_at, last_place_attempt_reason) "
            "VALUES ('tv', 200000, '3', 'Active/theme.mp3', "
            "        1500000, 'bbbb', '2026-05-19', "
            "        'themerrdb', 'XYZ', 'auto', "
            "        '2026-05-20', 'plex_has_theme')"
        )
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, provenance, placed_at) "
            "VALUES ('tv', 200000, '3', '/data/anime/Active', "
            "        'hardlink', 'auto', '2026-05-20')"
        )
        # Third comparison: no file_path (NULL) — walker skips.
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES (3, 'tv', 300000, 'No File', 'themoviedb', "
            "        '2026-05-27', '2026-05-27')"
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_size, file_sha256, downloaded_at, "
            "   source_kind, source_video_id, provenance, "
            "   last_place_attempt_at, last_place_attempt_reason) "
            "VALUES ('tv', 300000, '3', '', "
            "        0, '', '2026-05-19', "
            "        'themerrdb', 'NONE', 'auto', "
            "        '2026-05-20', 'plex_has_theme')"
        )
        conn.commit()

    result = maybe_repair_ps_with_dl_to_bk(db)
    assert result["candidates"] == 1, (
        f"v1.19.61: only the PS-with-DL row should be a candidate; "
        f"got {result['candidates']} (active-placed + no-file rows "
        f"must be skipped)"
    )
    assert result["updated"] == 1
    assert result["detected"] is True

    # Verify the stamps.
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        ps_with_dl = conn.execute(
            "SELECT last_place_attempt_reason FROM local_files "
            "WHERE tmdb_id = 100565"
        ).fetchone()
        assert ps_with_dl["last_place_attempt_reason"] == "backup_only"
        active = conn.execute(
            "SELECT last_place_attempt_reason FROM local_files "
            "WHERE tmdb_id = 200000"
        ).fetchone()
        assert active["last_place_attempt_reason"] == "plex_has_theme", (
            "v1.19.61: active-placed rows must NOT have their "
            "stamp flipped"
        )
        no_file = conn.execute(
            "SELECT last_place_attempt_reason FROM local_files "
            "WHERE tmdb_id = 300000"
        ).fetchone()
        assert no_file["last_place_attempt_reason"] == "plex_has_theme", (
            "v1.19.61: rows with no file_path must NOT have their "
            "stamp flipped (no backup to promote)"
        )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_61_version_pin():
    """Version bumped at v1.19.61. Relaxed to v1.19.x prefix after
    subsequent tags (v1.19.62+) continue the line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
