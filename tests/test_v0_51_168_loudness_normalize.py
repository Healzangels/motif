"""v0.51.168 — theme loudness NORMALIZE / UNDO: the first tag that mutates a theme.

// PROBE MP3GAIN passed on the real library (v0.51.165: attenuate AND boost both restore
the decoded AUDIO bit-exactly; the only byte-level diff is mp3gain's own APE undo tag), so
the engine is proven and the per-item apply can be built on it.

Two layers guarded here:
  1. schema v73 — the five additive local_files normalization columns. norm_orig_sha256 is
     load-bearing: it's the PRE-normalize sha, so UNDO can PROVE it restored the original
     bytes instead of assuming the tag worked.
  2. normalize_file / undo_file — apply -> re-measure -> report, and undo -> re-measure ->
     VERIFY bit-exact. mp3gain + ffmpeg are container-only, so both are stubbed; the stub
     for apply actually mutates the file so the real _sha256 round-trip runs.

The endpoints' edition-scoped write (keyed on the full local_files PK) mirrors
record_measurement — a normalize must never bleed onto a sibling edition.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import app.core.loudness as loud_mod
from app.core import db as core_db
from app.core import loudness_apply as la
from app.core.db import get_conn, init_db

_NORM_COLS = {
    "norm_state": "TEXT",
    "norm_gain_db": "REAL",
    "norm_target": "REAL",
    "norm_at": "TEXT",
    "norm_orig_sha256": "TEXT",
}


def _local_files_cols(conn: sqlite3.Connection) -> dict[str, str]:
    return {r[1]: r[2] for r in conn.execute("PRAGMA table_info(local_files)")}


# ── schema v73 ───────────────────────────────────────────────────────────────

def test_schema_version_is_at_least_73():
    # floor, not exact head — an exact pin breaks this guard on every later migration
    assert core_db.CURRENT_SCHEMA_VERSION >= 73


def test_fresh_db_has_normalization_columns(tmp_path: Path):
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    with get_conn(db_path) as conn:
        cols = _local_files_cols(conn)
        for name, decl in _NORM_COLS.items():
            assert name in cols, f"{name} missing from local_files after fresh init"
            assert cols[name].upper() == decl


def test_v73_migration_is_idempotent(tmp_path: Path):
    # crash-loop safety: the runner can re-enter a half-applied migration (column
    # committed, version stamp not) — re-running must NOT raise "duplicate column".
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    with get_conn(db_path) as conn:
        core_db._migrate_v72_to_v73(conn)
        core_db._migrate_v72_to_v73(conn)
        assert all(n in _local_files_cols(conn) for n in _NORM_COLS)


def test_norm_values_round_trip(tmp_path: Path):
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    with get_conn(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, "
            "downloaded_at, source_video_id, norm_state, norm_gain_db, norm_target, "
            "norm_at, norm_orig_sha256) "
            "VALUES ('movie', 777, '1', 'a/theme.mp3', '2026-07-16', 'vid', "
            "'normalized', -3.01, -18.0, '2026-07-16T00:00:00Z', 'origsha')"
        )
        r = conn.execute(
            "SELECT norm_state, norm_gain_db, norm_target, norm_orig_sha256 "
            "FROM local_files WHERE tmdb_id = 777"
        ).fetchone()
        assert r[0] == "normalized" and r[1] == -3.01 and r[2] == -18.0
        assert r[3] == "origsha"


# ── normalize_file ───────────────────────────────────────────────────────────

def _theme(tmp_path) -> Path:
    t = tmp_path / "theme.mp3"
    t.write_bytes(b"ID3fake-audio" * 100)
    return t


def _stub_apply(monkeypatch, *, ok=True):
    """mp3gain -g stub that actually mutates the file, so the real _sha256 round-trip runs."""
    calls = []

    def _apply(path, steps, timeout=None):
        calls.append(steps)
        if not ok:
            return False
        p = Path(path)
        p.write_bytes(p.read_bytes() + b"\x00" * abs(steps))
        return True

    monkeypatch.setattr(la, "apply_gain", _apply)
    return calls


def _stub_measure(monkeypatch, result):
    monkeypatch.setattr(loud_mod, "measure_loudness", lambda p, *a, **k: result)


def test_normalize_attenuates_and_remeasures(tmp_path, monkeypatch):
    """The dominant production op: a loud theme (-14.5) pulled to -18 → -2 steps."""
    calls = _stub_apply(monkeypatch)
    _stub_measure(monkeypatch, {"loudness_i": -18.1, "true_peak": -5.0, "lra": 6.0})
    theme = _theme(tmp_path)

    res = la.normalize_file(theme, -18.0, -14.5, -2.0)
    assert res["ok"] is True
    assert res["changed"] is True
    assert calls == [-2]                      # attenuate, not boost
    assert res["steps"] == -2
    assert res["applied_db"] == la.applied_db(-2)
    assert res["new_i"] == -18.1
    assert res["old_sha"] != res["new_sha"]   # bytes really moved


def test_normalize_is_a_clean_noop_within_one_step(tmp_path, monkeypatch):
    """Already within one mp3gain step of target → no write, no fake success."""
    calls = _stub_apply(monkeypatch)
    _stub_measure(monkeypatch, {"loudness_i": -99.0, "true_peak": 0.0, "lra": 0.0})
    theme = _theme(tmp_path)
    before = theme.read_bytes()

    res = la.normalize_file(theme, -18.0, -18.5, -10.0)
    assert res["ok"] is True
    assert res["changed"] is False
    assert res["steps"] == 0
    assert calls == []                        # mp3gain never invoked
    assert res["old_sha"] == res["new_sha"]
    assert "no change" in res["note"]
    assert theme.read_bytes() == before


def test_normalize_missing_file_is_reported(tmp_path, monkeypatch):
    _stub_apply(monkeypatch)
    res = la.normalize_file(tmp_path / "gone.mp3", -18.0, -14.0, -2.0)
    assert res["ok"] is False
    assert "missing" in res["error"]


def test_normalize_apply_failure_is_reported(tmp_path, monkeypatch):
    _stub_apply(monkeypatch, ok=False)
    theme = _theme(tmp_path)
    res = la.normalize_file(theme, -18.0, -14.5, -2.0)
    assert res["ok"] is False
    assert res["changed"] is False
    assert "mp3gain" in res["error"]


def test_normalize_remeasure_failure_leaves_loudness_unstamped(tmp_path, monkeypatch):
    """Gain applied (undo tag in place → still reversible) but re-measure failed: report the
    gap instead of storing a fake loudness (class-9). The endpoint NULLs measured_sha on
    new_i=None so the audit re-measures rather than trusting a hole."""
    _stub_apply(monkeypatch)
    _stub_measure(monkeypatch, None)
    theme = _theme(tmp_path)

    res = la.normalize_file(theme, -18.0, -14.5, -2.0)
    assert res["ok"] is True
    assert res["changed"] is True
    assert res["new_i"] is None
    assert "re-measure failed" in res["note"]


# ── undo_file ────────────────────────────────────────────────────────────────

def test_undo_reports_file_bit_exact_informationally(tmp_path, monkeypatch):
    # v0.51.170: file_bit_exact is INFORMATIONAL only — the verdict moved to
    # audio_restored (see test_v0_51_170). Real mp3gain leaves an APE tag, so this is
    # expected False in production; here the stub doesn't, so it reads True.
    theme = _theme(tmp_path)
    orig_sha = la._sha256(theme)
    monkeypatch.setattr(la, "undo_via_tag", lambda p, timeout=None: True)
    _stub_measure(monkeypatch, {"loudness_i": -14.5, "true_peak": -2.0, "lra": 7.0})

    res = la.undo_file(theme, expect_sha=orig_sha)
    assert res["ok"] is True
    assert res["file_bit_exact"] is True
    assert res["new_i"] == -14.5


def test_undo_file_sha_diff_is_not_treated_as_failure(tmp_path, monkeypatch):
    """v0.51.170: a differing FILE sha must NOT be the failure signal — mp3gain's APE tag
    guarantees it differs on a perfectly good restore, and treating it as the verdict cried
    wolf on the operator's first real audition."""
    theme = _theme(tmp_path)
    monkeypatch.setattr(la, "undo_via_tag", lambda p, timeout=None: True)
    _stub_measure(monkeypatch, {"loudness_i": -14.5, "true_peak": -2.0, "lra": 7.0})

    res = la.undo_file(theme, expect_sha="a-different-sha")
    assert res["ok"] is True
    assert res["file_bit_exact"] is False      # informational
    assert res["audio_restored"] is None       # no PCM reference given → unknown, not False


def test_undo_failure_is_reported(tmp_path, monkeypatch):
    theme = _theme(tmp_path)
    monkeypatch.setattr(la, "undo_via_tag", lambda p, timeout=None: False)
    res = la.undo_file(theme)
    assert res["ok"] is False
    assert "mp3gain" in res["error"]


def test_undo_missing_file_is_reported(tmp_path):
    res = la.undo_file(tmp_path / "gone.mp3")
    assert res["ok"] is False
    assert "missing" in res["error"]
