"""v1.18.7 — recovery walker uses ratio-based detection.

## What broke in v1.18.5's detection

The original `_detect_loss_pattern` required `lf_n == 0 AND
pl_n == 0` — total emptiness of local_files + placements.

the user's case: v1.18.0 nuked his 16K-row local_files + placements
via the FK cascade bug. He installed v1.18.5 expecting the
recovery walker to fire. BUT — between the v1.18.0 deploy and the
v1.18.5 deploy, he'd done a SET URL on the Willy Wonka Collection,
which inserted ONE local_files row (movie/-27). At v1.18.5
startup the detector saw `lf_n=1` and silently returned False —
"tracking exists, no recovery needed." 9,999 other broken rows
stayed in the M-everywhere state, no log lines printed.

## v1.18.7 fix

Detection now uses a ratio + floor heuristic:
  - Skip if no TDB themes (fresh install / pre-sync)
  - Skip if plex_items.local_theme_file=1 count is below the
    sidecar-evidence floor (default 50 — well below any realistic
    populated install but high enough to dodge fresh-install
    false positives)
  - Skip if local_files count is ≥ 50% of the sidecar count
    (healthy coverage)
  - Otherwise: detected — walk + recover

The walker's INSERT OR IGNORE pattern (already present) handles
the already-tracked minority safely, so we don't need to gate on
"completely empty."

Every branch logs the decision explicitly so the operator can see
WHY a boot skipped — the v1.18.5 silent-skip made debugging
"recovery isn't firing" a black box.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"


# ── Source-level pins for the new detection logic ─────────────


def test_sidecar_evidence_floor_is_module_level_constant():
    """The 50-sidecar floor must be a module-level constant so
    tests can lower it without seeding hundreds of fixture rows
    AND a future operator can tune it via a one-line patch."""
    src = RECOVERY_PY.read_text()
    assert "_SIDECAR_EVIDENCE_FLOOR = 50" in src, (
        "v1.18.7: floor must be a module-level constant"
    )


def test_detector_skips_when_local_files_coverage_healthy():
    """A healthy install (lf ≥ 50% of sidecars) must skip the
    walk with an explicit log line. Without this gate, the walker
    would re-walk every startup on already-recovered installs."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def _detect_loss_pattern(")
    fn_end = src.index("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    # The healthy-coverage branch must use the ratio threshold.
    assert "lf_n >= pi_sidecar_n * 0.5" in body, (
        "v1.18.7: detector must compare lf_n to sidecar count "
        "via the 50% ratio threshold"
    )
    # And log the skip decision.
    assert "coverage looks healthy" in body


def test_detector_logs_every_skip_branch():
    """All four early-return branches must emit an explicit log
    line so the operator can see WHY recovery didn't fire."""
    src = RECOVERY_PY.read_text()
    # entry point's themes_dir / missing-themes_dir skip
    assert 'themes_dir=%r not configured' in src
    # entry point's already-recovered skip
    assert "runtime_settings marker already" in src
    # detector's no-TDB-themes skip
    assert "no TDB themes" in src
    # detector's insufficient-sidecar-evidence skip
    assert "insufficient on-disk evidence" in src
    # detector's healthy-coverage skip
    assert "coverage looks healthy" in src
    # detector's detection success
    assert "LOSS PATTERN DETECTED" in src


# ── End-to-end: the post-manual-SET-URL case (the user's repro) ──


@pytest.fixture
def post_manual_url_fixture(tmp_path: Path, monkeypatch):
    """Reproduce the user's exact scenario: most local_files lost
    to the v1.18.0 bug, ONE row inserted post-bug via manual SET
    URL. Pre-v1.18.7 the detector returned False here; v1.18.7
    detects via the ratio threshold and walks."""
    # Lower the floor so a 5-row fixture is sufficient (the floor
    # exists to dodge fresh-install false positives; behavior
    # under the threshold ratio is what we're pinning).
    monkeypatch.setattr(
        "app.core.recovery_v55._SIDECAR_EVIDENCE_FLOOR", 5,
    )
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T00:00:00"
    with sqlite3.connect(db_path) as conn:
        # Seed Movies section.
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        # Seed 10 themes rows (the TDB sync survived the bug).
        for tmdb_id in range(100, 110):
            conn.execute(
                "INSERT INTO themes "
                "  (media_type, tmdb_id, title, title_norm, "
                "   year, youtube_url, youtube_video_id, "
                "   upstream_source, "
                "   last_seen_sync_at, first_seen_sync_at) "
                "VALUES ('movie', ?, ?, ?, '2020', "
                "        'https://youtube.com/watch?v=X', "
                "        'X', 'themoviedb', ?, ?)",
                (tmdb_id, f"Movie {tmdb_id}",
                 f"movie {tmdb_id}", ts, ts),
            )
        # Seed 10 plex_items rows with sidecars (Plex sees them
        # all — the on-disk evidence the bug couldn't touch).
        for tmdb_id in range(100, 110):
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, title, "
                "   title_norm, year, guid_tmdb, folder_path, "
                "   local_theme_file, first_seen_at, last_seen_at) "
                "VALUES (?, '1', 'movie', ?, ?, '2020', ?, "
                "        ?, 1, ?, ?)",
                (f"rk-{tmdb_id}", f"Movie {tmdb_id}",
                 f"movie {tmdb_id}", tmdb_id,
                 f"/data/media/movies/Movie {tmdb_id} (2020)",
                 ts, ts),
            )
        # Seed ONE local_files row — the user's post-bug
        # manual-SET-URL case. Pre-v1.18.7 this was the exact
        # trigger that silently disqualified his entire library
        # from auto-recovery.
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, "
            "   file_path, downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES ('movie', 100, '1', "
            "        'movies/Movie 100 (2020)/theme.mp3', "
            "        ?, 'X', 'manual', 'url')",
            (ts,),
        )
        conn.commit()
    # Seed on-disk canonical files for several of the themes
    # rows so the walker has something to discover.
    for tmdb_id in range(100, 110):
        movie_dir = (themes_dir / "movies"
                     / f"Movie {tmdb_id} (2020)")
        movie_dir.mkdir(parents=True)
        (movie_dir / "theme.mp3").write_bytes(
            b"\xff\xfb\x90" + b"\x00" * 256,
        )
    return db_path, themes_dir


def test_recovery_fires_despite_one_existing_local_files_row(
    post_manual_url_fixture,
):
    """The post-v1.18.5 detector must NOT skip on lf=1 when
    sidecar evidence is 10× larger. v1.18.5's lf>0 short-circuit
    silently disqualified the user's install — the v1.18.7 ratio
    threshold catches it."""
    db_path, themes_dir = post_manual_url_fixture
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    stats = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    assert stats["detected"] is True, (
        "v1.18.7: detector must DETECT loss pattern when "
        "local_files (1) is < 50% of sidecars (10). The "
        "pre-fix gate `lf_n > 0` would silently return False."
    )
    # The walker must have inserted the missing rows (9 new
    # locals — one was already present).
    assert stats["local_files_inserted"] >= 9, (
        f"v1.18.7: expected ≥9 new local_files rows; got "
        f"{stats['local_files_inserted']}"
    )


def test_recovery_skips_when_coverage_healthy(tmp_path: Path,
                                              monkeypatch):
    """Sanity counter-pin: a HEALTHY install (lf ≈ sidecars)
    must SKIP recovery. Otherwise the walker would re-run every
    startup on already-recovered installs, wasting cycles."""
    monkeypatch.setattr(
        "app.core.recovery_v55._SIDECAR_EVIDENCE_FLOOR", 5,
    )
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T00:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)",
            (ts, ts),
        )
        # 10 themes + 10 sidecars + 10 local_files = 100% coverage.
        for tmdb_id in range(100, 110):
            conn.execute(
                "INSERT INTO themes "
                "  (media_type, tmdb_id, title, title_norm, "
                "   upstream_source, "
                "   last_seen_sync_at, first_seen_sync_at) "
                "VALUES ('movie', ?, ?, ?, 'themoviedb', ?, ?)",
                (tmdb_id, f"M{tmdb_id}", f"m{tmdb_id}", ts, ts),
            )
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, title, "
                "   local_theme_file, "
                "   first_seen_at, last_seen_at) "
                "VALUES (?, '1', 'movie', ?, 1, ?, ?)",
                (f"rk-{tmdb_id}", f"M{tmdb_id}", ts, ts),
            )
            conn.execute(
                "INSERT INTO local_files "
                "  (media_type, tmdb_id, section_id, "
                "   file_path, downloaded_at, source_video_id) "
                "VALUES ('movie', ?, '1', ?, ?, 'X')",
                (tmdb_id, f"movies/m{tmdb_id}/theme.mp3", ts),
            )
        conn.commit()
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    stats = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    assert stats["detected"] is False, (
        "v1.18.7: healthy install (lf=sidecars) must skip"
    )


def test_recovery_skips_when_sidecar_evidence_below_floor(
    tmp_path: Path, monkeypatch,
):
    """Counter-pin: a fresh install with <floor sidecars must
    skip even though `lf` is also low — the absolute floor dodges
    fresh-install false positives."""
    # Use the production floor (50) for this test.
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T00:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)",
            (ts, ts),
        )
        # 100 themes (TDB sync ran) but only 5 sidecars (fresh
        # install — Plex enumerated, downloads haven't started).
        for tmdb_id in range(100, 200):
            conn.execute(
                "INSERT INTO themes "
                "  (media_type, tmdb_id, title, title_norm, "
                "   upstream_source, "
                "   last_seen_sync_at, first_seen_sync_at) "
                "VALUES ('movie', ?, ?, ?, 'themoviedb', ?, ?)",
                (tmdb_id, f"M{tmdb_id}", f"m{tmdb_id}", ts, ts),
            )
        # Only 5 plex_items with sidecars (below floor=50).
        for tmdb_id in range(100, 105):
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, title, "
                "   local_theme_file, "
                "   first_seen_at, last_seen_at) "
                "VALUES (?, '1', 'movie', ?, 1, ?, ?)",
                (f"rk-{tmdb_id}", f"M{tmdb_id}", ts, ts),
            )
        conn.commit()
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    stats = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    assert stats["detected"] is False, (
        "v1.18.7: sidecar count below floor must skip — "
        "avoids false positive on fresh installs"
    )
