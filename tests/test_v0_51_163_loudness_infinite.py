"""v0.51.163 — the loudness report survives a -inf measurement (silent theme).

ffmpeg's loudnorm reports a SILENT / near-silent theme as -inf LUFS. -inf is poison:
build_report crashed on math.floor(-inf) (OverflowError → the report endpoint 500'd)
and -inf serialises to "-Infinity" (invalid JSON the browser rejects). Either killed
the /admin/loudness distribution — data-dependent, so it only bit once a silent theme
got measured. This pins the three-layer fix.
"""
from __future__ import annotations

import json

import pytest

from app.core import loudness
from app.core import loudness_audit as la
from app.core.db import get_conn, init_db


# ── layer 1: never STORE a non-finite measurement ────────────────────────────

@pytest.mark.parametrize("i", ["-inf", "inf", "+inf", "nan"])
def test_parse_rejects_non_finite(i):
    s = f'{{"input_i":"{i}","input_tp":"-2.0","input_lra":"5.0"}}'
    assert loudness._parse_loudnorm_json(s) is None


def test_parse_rejects_non_finite_true_peak():
    s = '{"input_i":"-18.0","input_tp":"-inf","input_lra":"5.0"}'
    assert loudness._parse_loudnorm_json(s) is None


def test_parse_keeps_finite():
    s = '{"input_i":"-18.0","input_tp":"-2.0","input_lra":"5.0"}'
    assert loudness._parse_loudnorm_json(s) == {
        "loudness_i": -18.0, "true_peak": -2.0, "lra": 5.0}


# ── layer 2: build_report robust to -inf ALREADY in the DB ────────────────────

def _seed(conn, tid, i, tp):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
        "file_path, file_sha256, downloaded_at, source_video_id, loudness_i, loudness_tp) "
        "VALUES ('movie', ?, '1', '', ?, ?, '2026-07-15', 'v', ?, ?)",
        (tid, f"movie/{tid}.mp3", f"sha{tid}", i, tp),
    )


def test_build_report_survives_stored_inf(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        _seed(conn, 1, -18.0, -2.0)
        _seed(conn, 2, float("-inf"), float("-inf"))   # silent theme, pre-fix storage
        _seed(conn, 3, -22.0, -5.0)
        conn.commit()
        rep = la.build_report(conn)   # must NOT raise OverflowError

    # only the finite rows count / appear
    assert rep["measured"] == 2
    assert rep["stats"]["min"] == -22.0 and rep["stats"]["max"] == -18.0
    lufs = [o["loudness_i"] for o in rep["quietest"]]
    assert float("-inf") not in lufs and all(v == v for v in lufs)  # no -inf, no nan

    # the payload must be STRICT-JSON serialisable (browser JSON.parse rejects Infinity)
    s = json.dumps(rep, allow_nan=False)
    assert "Infinity" not in s and "NaN" not in s


def test_build_report_nulls_non_finite_true_peak_in_values(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        _seed(conn, 1, -18.0, float("-inf"))   # finite loudness, -inf peak
        conn.commit()
        rep = la.build_report(conn)
    # the row is kept (loudness_i finite) but its -inf peak is nulled for valid JSON
    assert rep["values"] == [[-18.0, None]]
    json.dumps(rep, allow_nan=False)  # must not raise
