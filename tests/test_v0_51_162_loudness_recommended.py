"""v0.51.162 — the LOUDNESS AUDIT report recommends a target LUFS.

build_report() returns `recommended` = the library median clamped to a comfortable
ambient-hover band [-23, -18] and rounded to 0.5, so the operator gets a sensible
starting target instead of guessing. The report seeds the slider there + surfaces it.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core import loudness_audit
from app.core.db import get_conn, init_db


def _seed(conn, tid, i, tp=-3.0):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
        "file_path, file_sha256, downloaded_at, source_video_id, loudness_i, loudness_tp) "
        "VALUES ('movie', ?, '1', '', ?, ?, '2026-07-15', 'v', ?, ?)",
        (tid, f"movie/{tid}.mp3", f"sha{tid}", i, tp),
    )


@pytest.mark.parametrize("median, expected", [
    (-15.9, -18.0),   # loud library → clamped up to the -18 ceiling
    (-16.5, -18.0),   # still louder than the band → -18
    (-20.0, -20.0),   # inside the band → the median itself
    (-19.0, -19.0),   # inside
    (-25.0, -23.0),   # very quiet → clamped down to the -23 floor
    (-18.0, -18.0),   # exactly the ceiling
    (-23.0, -23.0),   # exactly the floor
])
def test_recommended_target_clamps_to_hover_band(median, expected):
    assert loudness_audit._recommended_target(median) == expected


def test_recommended_target_none_on_empty():
    assert loudness_audit._recommended_target(None) is None


def test_build_report_includes_recommended(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        # median of these is -15.0 (loud) → recommendation -18.0
        for tid, i in [(1, -14.0), (2, -15.0), (3, -16.0)]:
            _seed(conn, tid, i)
        conn.commit()
        rep = loudness_audit.build_report(conn)
    assert rep["stats"]["median"] == -15.0
    assert rep["recommended"] == -18.0


def test_build_report_recommended_none_when_empty(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn:
        rep = loudness_audit.build_report(conn)
    assert rep["recommended"] is None
