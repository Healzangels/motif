"""v0.51.205 — hardening from the full normalization audit (M1/M2/M3 + L1/L2/L3).

M1: a non-finite (legacy -inf) loudness must not crash normalize_file (was: round(inf)
    → OverflowError, breaking its "never raises" contract).
M2: the PCM-decode ffmpeg command caps duration (-t) so a crafted low-bitrate file can't
    balloon the captured decode in memory.
M3: bulk_normalize_counts + the bulk-run SELECT use EXISTS, not a fanning placements JOIN,
    so a row hardlinked into 2+ folders is counted once, not N times.
L1: _undo_one_row refuses when the row was pushed to Plex but Plex is now unconfigured.
L3: record_measurement returns rowcount so a zero-row (PK-moved) write isn't counted.
"""
from __future__ import annotations

import math
import sqlite3
from pathlib import Path

from app.core import loudness_apply as la
from app.core.loudness_audit import bulk_normalize_counts, record_measurement
from app.core.db import init_db, get_conn
from app.core.plex import THEME_UPLOAD_CEILING_BYTES

REPO = Path(__file__).resolve().parent.parent
NOW = "2026-07-18T00:00:00"


# ── M1: non-finite loudness never raises ─────────────────────────────────────


def test_normalize_file_rejects_non_finite_loudness_without_raising():
    for bad in (float("-inf"), float("inf"), float("nan")):
        res = la.normalize_file("/does/not/matter", target_lufs=-18.0,
                                measured_i=bad, true_peak=-2.0)
        assert res["ok"] is False
        assert res["error"] and "finite" in res["error"]
        assert res["steps"] in (0, None)   # never computed a gain


def test_gain_steps_would_overflow_on_inf_but_normalize_file_guards_first():
    # the raw helper is where the OverflowError lived; the guard is in normalize_file.
    # A silent theme measures loudness_i AND true_peak as -inf together (the -inf poison
    # class), so the +inf boost isn't peak-clamped → round(inf) overflows.
    import pytest
    with pytest.raises(OverflowError):
        la.gain_steps_for_target(-18.0, float("-inf"), float("-inf"))
    # …and normalize_file never reaches it for a -inf row (asserted above).


# ── M2: the decode is duration-capped ────────────────────────────────────────


def test_pcm_decode_is_duration_capped():
    src = (REPO / "app" / "core" / "loudness_apply.py").read_text()
    assert "_PCM_DECODE_CAP_S" in src
    # the -t cap is passed to the decode ffmpeg command (before -f f32le).
    assert '"-t", str(_PCM_DECODE_CAP_S)' in src


# ── M3: counts/run don't fan out on a multi-folder placement ─────────────────


def _seed_leveled_candidate(db, *, hardlink_folders):
    """One eligible-to-level row (raw, measured-current, under ceiling) with N hardlink
    placements — one per media_folder — to exercise the JOIN-fanout path."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys=OFF")
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
                  " last_seen_sync_at, first_seen_sync_at) VALUES (1,'movie',1,'M1','imdb',?,?)",
                  (NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, loudness_measured_sha256, loudness_i, loudness_tp, "
                  " file_size, downloaded_at, source_video_id, norm_state) "
                  "VALUES ('movie',1,'1','', 'm/1.mp3', 's', 's', -5.0, -2.0, ?, ?, 'v', NULL)",
                  (1_000_000, NOW))
        for folder in hardlink_folders:
            c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, "
                      " edition_key, placement_kind, placed_at) "
                      "VALUES ('movie',1,'1',?, '', 'hardlink', ?)", (folder, NOW))
        c.commit()


def test_bulk_counts_do_not_fan_out_on_multiple_hardlink_placements(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    _seed_leveled_candidate(db, hardlink_folders=["/data/movies/A", "/data/movies/B"])
    with get_conn(db) as conn:
        counts = bulk_normalize_counts(conn, ceiling_bytes=THEME_UPLOAD_CEILING_BYTES,
                                       target=-18.0)
    # ONE distinct theme, despite TWO hardlink placement rows.
    assert counts["eligible"] == 1
    assert counts["outliers"] == 1   # -5.0 > -18 + 6


def test_bulk_run_select_uses_exists_not_a_fanning_join():
    api = (REPO / "app" / "web" / "api.py").read_text()
    # the _bulk_normalize_run row SELECT must not JOIN placements (which fans out).
    i = api.index("def _bulk_normalize_run(")
    block = api[i:api.index("def _bulk_normalize_undo_run(", i)]
    assert "EXISTS (SELECT 1 FROM placements p" in block
    assert "JOIN placements p ON" not in block


# ── L1: undo refuses a pushed row when Plex is unconfigured ──────────────────


class _NoPlex:
    plex_url = None
    plex_token = None
    themes_dir = None


def test_undo_one_refuses_a_plex_pushed_row_without_plex(tmp_path):
    from app.web import api
    db = tmp_path / "u.db"
    init_db(db)
    # a leveled row that WAS pushed to Plex (norm_plex_entry_uri set).
    row = {
        "media_type": "movie", "tmdb_id": 1, "section_id": "1", "edition_key": "",
        "file_path": "/abs/theme.mp3", "norm_state": "normalized",
        "norm_orig_sha256": "o", "norm_orig_pcm_sha256": "op",
        "norm_plex_entry_uri": "entry://x", "title": "M1",
    }
    res = api._undo_one_row(db, _NoPlex(), row)
    assert res["ok"] is False
    assert "Plex is not configured" in res["error"]


# ── L3: record_measurement reports whether it actually wrote ─────────────────


def test_record_measurement_returns_zero_on_a_pk_that_no_longer_exists(tmp_path):
    db = tmp_path / "r.db"
    init_db(db)
    m = {"loudness_i": -18.0, "true_peak": -2.0, "lra": 5.0}
    row = {"file_sha256": "s", "media_type": "movie", "tmdb_id": 999,
           "section_id": "1", "edition_key": ""}   # no such local_files row
    with get_conn(db) as conn:
        rc = record_measurement(conn, row, m, NOW)
    assert rc == 0   # the caller must NOT count this as measured
