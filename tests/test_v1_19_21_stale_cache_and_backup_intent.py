"""v1.19.21 — stale Plex cache override + backup-only intent + BK badge.

Three coordinated fixes from the user's Indiana Jones (movie/335977
sec=1) repro:

  1. **Worker stale-cache override**: when plex_items.has_theme=1
     but plex_theme_verified_ok=0 (Plex CLAIMS a theme but our
     HEAD verification said it returns 404), the place worker
     historically skipped with reason='plex_has_theme', trusting
     the lie. v1.19.21 makes the worker treat the cache as stale
     and force-place. Mirrors the SRC SQL's v1.12.112 logic
     (verified_ok=0 demotes 'P' to '-' classification).

  2. **bulk_backup permanence**: bulk DOWNLOAD TDB BACKUP sets
     auto_place=false on the download job. Pre-v1.19.21 the
     hourly retry sweep saw local_files+no placement and
     enqueued a place job anyway, defeating the user's "backup
     only" intent. v1.19.21: worker stamps
     last_place_attempt_reason='backup_only' after backup downloads
     + scheduler retry skips that reason.

  3. **BK link badge**: per the user's choice ("keep SRC =
     what's actually playing; add a separate indicator"), new
     link-glyph-bk shows in the LINK column when motif has a
     file ready but intentionally didn't place. Blue color for
     informational/neutral.

Plus a one-shot walker to repair the rows currently stuck in
state (1) — without it, existing damage stays broken since the
retry sweep skips on the old 'plex_has_theme' reason stamp.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
RECOVERY_PY = (REPO / "app" / "core" / "recovery_v55.py").read_text()
MAIN_PY = (REPO / "app" / "main.py").read_text()


# ── Fix 1: worker stale-cache override ───────────────────────


def test_worker_pulls_plex_theme_verified_ok_in_do_place():
    """The _do_place SELECT must include plex_theme_verified_ok
    so the stale-cache check below has access to it."""
    fn_start = WORKER_PY.index("def _do_place(self, job:")
    next_def = WORKER_PY.find("\n    def ", fn_start + 1)
    body = WORKER_PY[fn_start:next_def if next_def > 0 else len(WORKER_PY)]
    assert "plex_theme_verified_ok" in body, (
        "v1.19.21: _do_place must pull plex_theme_verified_ok "
        "for the stale-cache override check"
    )


def test_worker_pulls_plex_theme_verified_ok_in_do_place_collection():
    """Same as above for _do_place_collection (the API-upload path
    that handled the user's Indiana Jones row)."""
    fn_start = WORKER_PY.index("def _do_place_collection(")
    next_def = WORKER_PY.find("\n    def ", fn_start + 1)
    body = WORKER_PY[fn_start:next_def if next_def > 0 else len(WORKER_PY)]
    assert "plex_theme_verified_ok" in body


def test_worker_force_places_on_verified_ok_zero():
    """When has_theme=1 + verified_ok=0, the worker must treat
    cached_has_theme as False (force placement to repair the
    stale Plex cache)."""
    # Check both _do_place and _do_place_collection have the override.
    for fn_name in ("def _do_place(self, job:",
                    "def _do_place_collection("):
        fn_start = WORKER_PY.index(fn_name)
        next_def = WORKER_PY.find("\n    def ", fn_start + 1)
        body = WORKER_PY[fn_start:next_def if next_def > 0 else len(WORKER_PY)]
        # The override sets cached_has_theme = False when verified=0.
        assert "verified_ok == 0" in body, (
            f"v1.19.21: {fn_name} missing the verified_ok=0 check"
        )
        assert "cached_has_theme = False" in body, (
            f"v1.19.21: {fn_name} must override cached_has_theme "
            "when Plex's cache is verified-stale"
        )


# ── Fix 2: bulk_backup permanence ────────────────────────────


def test_worker_stamps_backup_only_after_bulk_backup():
    """When _record_local_file finishes a bulk_backup download
    (auto_place=false + reason='bulk_backup'), it must stamp
    last_place_attempt_reason='backup_only' on the local_files
    row so the retry sweep recognizes the intent."""
    fn_start = WORKER_PY.index("def _record_local_file(")
    next_def = WORKER_PY.find("\n    def ", fn_start + 1)
    body = WORKER_PY[fn_start:next_def if next_def > 0 else len(WORKER_PY)]
    assert "backup_only" in body, (
        "v1.19.21: _record_local_file must stamp 'backup_only' "
        "after a bulk_backup download with auto_place=false"
    )
    assert "bulk_backup" in body


def test_scheduler_retry_sweep_skips_backup_only_reason():
    """The hourly retry sweep WHERE clause must add 'backup_only'
    to the list of skip-reasons (alongside 'plex_has_theme' and
    'existing_theme:%')."""
    assert "backup_only" in SCHED_PY, (
        "v1.19.21: retry sweep must skip rows whose "
        "last_place_attempt_reason='backup_only'"
    )


# ── Fix 3: BK link badge ─────────────────────────────────────


def test_api_library_select_includes_last_place_attempt_reason():
    """The /api/library SELECT must include
    last_place_attempt_reason so the JS can render the BK badge."""
    assert "AS last_place_attempt_reason" in API_PY, (
        "v1.19.21: library SELECT must surface "
        "last_place_attempt_reason to the JS layer"
    )


def test_app_js_renders_bk_link_badge():
    """The linkCell render must include a BK branch gated on
    last_place_attempt_reason='backup_only'."""
    assert "link-glyph-bk" in APP_JS
    assert "'backup_only'" in APP_JS or '"backup_only"' in APP_JS
    # Must check NOT placed AND has download AND reason match.
    assert "isBackupOnly" in APP_JS


def test_css_link_glyph_bk_defined_with_blue_token():
    """The .link-glyph-bk rule must exist.

    v1.19.21 introduced this rule with --blue (informational
    /neutral). v1.19.63 switched it to --violet-bright when
    the badge label was renamed BK → BU (Backup User) so the
    color sits in the user-content family alongside SRC=U
    (violet). The CSS classname stayed .link-glyph-bk for
    backwards compat + URL deep-link stability."""
    assert ".link-glyph-bk {" in APP_CSS
    block_start = APP_CSS.index(".link-glyph-bk {")
    block = APP_CSS[block_start:block_start + 400]
    assert "color:" in block
    # Either the v1.19.21 blue or the v1.19.63 violet-bright.
    assert (
        "var(--blue)" in block
        or "var(--violet-bright)" in block
    ), "v1.19.63: rule must use --blue (legacy) or --violet-bright"


# ── One-shot walker for existing damage ──────────────────────


def test_walker_function_exists():
    """maybe_repair_stale_plex_cache_placements must be
    importable from app.core.recovery_v55."""
    assert "def maybe_repair_stale_plex_cache_placements(" in RECOVERY_PY


def test_walker_queries_stale_cache_pattern():
    """The walker must find rows matching the Indiana Jones
    signature: has_theme=1 + verified_ok=0 + no placement +
    prior skip reason was plex_has_theme."""
    fn_start = RECOVERY_PY.index(
        "def maybe_repair_stale_plex_cache_placements("
    )
    next_def = RECOVERY_PY.find("\ndef ", fn_start + 1)
    body = RECOVERY_PY[fn_start:next_def if next_def > 0 else len(RECOVERY_PY)]
    assert "pi.has_theme = 1" in body
    assert "pi.plex_theme_verified_ok = 0" in body
    assert "last_place_attempt_reason = 'plex_has_theme'" in body
    assert "media_folder IS NULL" in body


def test_walker_enqueues_with_kind_file_and_force():
    """The enqueued place jobs must use kind=file (v1.19.17
    convention) + force=true so the worker re-hardlinks the
    canonical to the Plex folder."""
    fn_start = RECOVERY_PY.index(
        "def maybe_repair_stale_plex_cache_placements("
    )
    next_def = RECOVERY_PY.find("\ndef ", fn_start + 1)
    body = RECOVERY_PY[fn_start:next_def if next_def > 0 else len(RECOVERY_PY)]
    assert '"kind":"file"' in body
    assert '"force":true' in body
    assert "v1.19.21" in body  # reason marker


def test_walker_has_independent_marker():
    assert (
        "recovery_stale_plex_cache_placements_done_at_v1_19_21"
        in RECOVERY_PY
    )


# ── End-to-end behavioral: walker repair ─────────────────────


def _seed_install(tmp_path: Path):
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T05:11:20+00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        conn.commit()
    return db_path


def _seed_indiana_jones_state(db_path: Path, tmdb_id: int = 335977):
    """Reproduce the user's row state — local_files present, no
    placement, plex_items has_theme=1 + verified_ok=0,
    last_place_attempt_reason='plex_has_theme'."""
    ts = "2026-05-24T18:55:40+00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "                    upstream_source, last_seen_sync_at, "
            "                    first_seen_sync_at) "
            "VALUES ('movie', ?, 'Indiana Jones', '2023', "
            "        'imdb', ?, ?)",
            (tmdb_id, ts, ts),
        )
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "                        media_type, guid_tmdb, title, "
            "                        folder_path, has_theme, "
            "                        local_theme_file, "
            "                        plex_independent_theme, "
            "                        plex_theme_verified_ok, "
            "                        first_seen_at, last_seen_at) "
            f"VALUES ('rk-{tmdb_id}', '1', 'movie', ?, "
            "        'Indiana Jones', "
            "        '/data/media/movies/IJ', "
            "        1, 0, 1, 0, ?, ?)",
            (tmdb_id, ts, ts),
        )
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, "
            "                         section_id, file_path, "
            "                         downloaded_at, source_video_id, "
            "                         provenance, source_kind, "
            "                         last_place_attempt_at, "
            "                         last_place_attempt_reason) "
            "VALUES ('movie', ?, '1', 'movies/IJ/theme.mp3', "
            "        ?, 'glf5KmRGvOk', 'auto', 'themerrdb', "
            "        ?, 'plex_has_theme')",
            (tmdb_id, ts, ts),
        )
        conn.commit()


def test_walker_enqueues_for_stale_cache_row(tmp_path):
    """Walker finds the Indiana Jones signature → enqueues
    force-place job with kind=file."""
    db_path = _seed_install(tmp_path)
    _seed_indiana_jones_state(db_path, tmdb_id=335977)
    from app.core.recovery_v55 import (
        maybe_repair_stale_plex_cache_placements,
    )
    stats = maybe_repair_stale_plex_cache_placements(db_path)
    assert stats["candidates"] == 1
    assert stats["enqueued"] == 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        job = conn.execute(
            "SELECT job_type, status, payload, section_id "
            "FROM jobs WHERE tmdb_id = 335977"
        ).fetchone()
    assert job["job_type"] == "place"
    assert job["status"] == "pending"
    assert '"kind":"file"' in job["payload"]
    assert '"force":true' in job["payload"]
    assert "v1.19.21" in job["payload"]


def test_walker_skips_row_with_verified_ok_1(tmp_path):
    """Row where Plex's verification SUCCEEDED — Plex genuinely
    serves a working theme. Walker must NOT touch."""
    db_path = _seed_install(tmp_path)
    _seed_indiana_jones_state(db_path, tmdb_id=400000)
    # Flip verified_ok to 1 — Plex actually serves.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE plex_items SET plex_theme_verified_ok = 1 "
            "WHERE guid_tmdb = 400000"
        )
        conn.commit()
    from app.core.recovery_v55 import (
        maybe_repair_stale_plex_cache_placements,
    )
    stats = maybe_repair_stale_plex_cache_placements(db_path)
    assert stats["candidates"] == 0
    assert stats["enqueued"] == 0


def test_walker_skips_row_without_skip_reason_match(tmp_path):
    """Row where the skip reason ISN'T 'plex_has_theme' (e.g.,
    'backup_only' from v1.19.21 itself). Walker must NOT touch
    these — they're a different intent."""
    db_path = _seed_install(tmp_path)
    _seed_indiana_jones_state(db_path, tmdb_id=500000)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE local_files SET "
            "  last_place_attempt_reason = 'backup_only' "
            "WHERE tmdb_id = 500000"
        )
        conn.commit()
    from app.core.recovery_v55 import (
        maybe_repair_stale_plex_cache_placements,
    )
    stats = maybe_repair_stale_plex_cache_placements(db_path)
    assert stats["candidates"] == 0


def test_walker_skips_in_flight_place_job(tmp_path):
    """If a place job is already pending/running, don't pile
    on another."""
    db_path = _seed_install(tmp_path)
    _seed_indiana_jones_state(db_path, tmdb_id=600000)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, "
            "                  section_id, payload, status, "
            "                  created_at, next_run_at) "
            "VALUES ('place', 'movie', 600000, '1', '{}', "
            "        'pending', '2026-05-25T00:00:00', "
            "        '2026-05-25T00:00:00')"
        )
        conn.commit()
    from app.core.recovery_v55 import (
        maybe_repair_stale_plex_cache_placements,
    )
    stats = maybe_repair_stale_plex_cache_placements(db_path)
    assert stats["enqueued"] == 0
    assert stats["skipped_job_in_flight"] == 1


def test_walker_marker_prevents_re_run(tmp_path):
    db_path = _seed_install(tmp_path)
    _seed_indiana_jones_state(db_path, tmdb_id=700000)
    from app.core.recovery_v55 import (
        maybe_repair_stale_plex_cache_placements,
    )
    stats1 = maybe_repair_stale_plex_cache_placements(db_path)
    assert stats1["enqueued"] == 1
    _seed_indiana_jones_state(db_path, tmdb_id=700001)
    stats2 = maybe_repair_stale_plex_cache_placements(db_path)
    assert stats2["candidates"] == 0


def test_walker_no_op_preserves_marker(tmp_path):
    """No candidates → don't stamp marker (so future drift can
    re-trigger)."""
    db_path = _seed_install(tmp_path)
    from app.core.recovery_v55 import (
        maybe_repair_stale_plex_cache_placements,
    )
    stats = maybe_repair_stale_plex_cache_placements(db_path)
    assert stats["candidates"] == 0
    with sqlite3.connect(db_path) as conn:
        marker = conn.execute(
            "SELECT 1 FROM runtime_settings WHERE key = "
            "'recovery_stale_plex_cache_placements_done_at_v1_19_21'"
        ).fetchone()
    assert marker is None
