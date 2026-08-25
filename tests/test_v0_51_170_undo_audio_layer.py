"""v0.51.170 — undo verified the WRONG LAYER, and cried wolf on the first real audition.

The operator normalized a tv theme (-5.15 -> -18.7 LUFS, -9 steps), pressed // UNDO, and
got `bit_exact: false` — even though the fresh re-measure came back -5.15 LUFS / +2.6 dBTP,
identical to the original to the decimal. The audio HAD been restored; the check was wrong.

Cause: undo_file compared the whole-FILE sha256 against norm_orig_sha256. mp3gain leaves
its APE tag on the file, so a restored file can NEVER byte-match the pre-normalize file.
v0.51.165's probe had already MEASURED this on these very files
(restored_file_bit_exact=false + restored_diff_is_tag_only=true) — the same layer mistake
v0.51.164's probe made, fixed there in v0.51.165, then repeated on the undo path.

Fix: schema v74 stores norm_orig_pcm_sha256 (the DECODED-PCM hash of the original, taken
BEFORE gain is applied). undo_file compares the restored samples against it and reports
`audio_restored` as the verdict; file_bit_exact is demoted to informational.

Second gap, same root cause (proving the easy case): the probe only ever tested ±2 steps,
while the first real normalize applied -9. Magnitude is exactly what decides global_gain
clamping, so `ok` now also requires a DEEP attenuation to be reversible.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import app.core.loudness as loud_mod
from app.core import db as core_db
from app.core import loudness_apply as la
from app.core.db import get_conn, init_db


def _theme(tmp_path) -> Path:
    t = tmp_path / "theme.mp3"
    t.write_bytes(b"ID3fake-audio" * 100)
    return t


def _stub_measure(monkeypatch, result=None):
    monkeypatch.setattr(loud_mod, "measure_loudness",
                        lambda p, *a, **k: result or {"loudness_i": -14.5,
                                                      "true_peak": -2.0, "lra": 7.0})


# ── schema v74 ───────────────────────────────────────────────────────────────

def test_schema_version_is_current():
    assert core_db.CURRENT_SCHEMA_VERSION >= 67  # v0.51.277: floor, not mirror


def test_fresh_db_has_the_pcm_reference_column(tmp_path: Path):
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    with get_conn(db_path) as conn:
        cols = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(local_files)")}
        assert "norm_orig_pcm_sha256" in cols
        assert cols["norm_orig_pcm_sha256"].upper() == "TEXT"


def test_v74_migration_is_idempotent(tmp_path: Path):
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    with get_conn(db_path) as conn:
        core_db._migrate_v73_to_v74(conn)
        core_db._migrate_v73_to_v74(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(local_files)")}
        assert "norm_orig_pcm_sha256" in cols


# ── normalize captures the PCM reference ─────────────────────────────────────

def test_normalize_captures_the_originals_pcm_hash_before_applying(tmp_path, monkeypatch):
    """The reference must be taken from the ORIGINAL — after gain it's worthless."""
    seen = []
    monkeypatch.setattr(la, "_decode_pcm_sha",
                        lambda p, *a, **k: "PCM-ORIG" if not seen else "PCM-AFTER")
    monkeypatch.setattr(la, "apply_gain",
                        lambda p, s, timeout=None: seen.append(s) or True)
    _stub_measure(monkeypatch, {"loudness_i": -18.0, "true_peak": -5.0, "lra": 6.0})

    res = la.normalize_file(_theme(tmp_path), -18.0, -14.5, -2.0)
    assert res["ok"] is True
    assert res["old_pcm_sha"] == "PCM-ORIG"     # captured pre-gain


# ── the regression: a correct restore must NOT report failure ────────────────

def test_correct_restore_is_not_flagged_when_only_the_tag_differs(tmp_path, monkeypatch):
    """THE v0.51.169 BUG. mp3gain leaves its APE tag, so the restored FILE differs from the
    original — but the samples are identical. That must read as success."""
    theme = _theme(tmp_path)
    monkeypatch.setattr(la, "undo_via_tag", lambda p, timeout=None: True)
    monkeypatch.setattr(la, "_decode_pcm_sha", lambda p, *a, **k: "PCM-ORIG")
    _stub_measure(monkeypatch)

    res = la.undo_file(theme, expect_sha="the-pre-normalize-FILE-sha-that-cannot-match",
                       expect_pcm_sha="PCM-ORIG")
    assert res["ok"] is True
    assert res["file_bit_exact"] is False    # expected — the APE tag remains
    assert res["audio_restored"] is True     # the verdict: the sound came back


def test_genuinely_broken_restore_is_flagged(tmp_path, monkeypatch):
    """The real failure mode (a deep attenuation clamping global_gain, so -u over-restores)
    must still be caught — the fix must not make undo unconditionally optimistic."""
    theme = _theme(tmp_path)
    monkeypatch.setattr(la, "undo_via_tag", lambda p, timeout=None: True)
    monkeypatch.setattr(la, "_decode_pcm_sha", lambda p, *a, **k: "PCM-DIFFERENT")
    _stub_measure(monkeypatch)

    res = la.undo_file(theme, expect_sha="x", expect_pcm_sha="PCM-ORIG")
    # v0.51.204 (audit H1): a False audio verdict must make ok=False — the caller commits raw
    # + re-pushes to Plex on ok, so ok=True here silently shipped a degraded theme. None
    # (legacy / undecodable) stays ok; only an explicit mismatch fails.
    assert res["ok"] is False
    assert res["audio_restored"] is False
    assert res["error"]   # a reason is surfaced


def test_legacy_row_without_a_pcm_reference_is_unknown_not_failed(tmp_path, monkeypatch):
    """Rows normalized by v0.51.168/169 have no norm_orig_pcm_sha256. Report unknown —
    never a false alarm (which is the whole bug this tag fixes)."""
    theme = _theme(tmp_path)
    monkeypatch.setattr(la, "undo_via_tag", lambda p, timeout=None: True)
    _stub_measure(monkeypatch)

    res = la.undo_file(theme, expect_sha="x", expect_pcm_sha=None)
    assert res["ok"] is True
    assert res["audio_restored"] is None


def test_undecodable_restore_is_unknown_not_failed(tmp_path, monkeypatch):
    """ffmpeg can't decode → we don't know; don't assert failure."""
    theme = _theme(tmp_path)
    monkeypatch.setattr(la, "undo_via_tag", lambda p, timeout=None: True)
    monkeypatch.setattr(la, "_decode_pcm_sha", lambda p, *a, **k: None)
    _stub_measure(monkeypatch)

    res = la.undo_file(theme, expect_sha="x", expect_pcm_sha="PCM-ORIG")
    assert res["audio_restored"] is None


# ── the probe now proves the magnitude production actually applies ───────────

def test_probe_tests_a_production_deep_attenuation_not_just_two_steps():
    """±2 steps is not what production does — the first real normalize applied -9. Only a
    deep attenuation can drive global_gain to its floor, so that's the case worth proving."""
    src = (Path(__file__).resolve().parent.parent / "app" / "core"
           / "loudness_apply.py").read_text()
    assert "_DEEP_PROBE_STEPS = 9" in src
    i = src.index("def probe_mp3gain")
    block = src[i:]
    assert "atten_deep" in block
    assert "attenuate_deep_reversible_audio" in block


def test_probe_verdict_requires_the_deep_path(monkeypatch, tmp_path):
    """A build where the SHALLOW attenuate is reversible but the DEEP one is not must NOT
    report ok — that is exactly the case that would wreck a -9-step normalize."""
    theme = _theme(tmp_path)
    orig = "PCM-ORIG"

    def _run(cmd, timeout=None):
        if cmd[:2] == ["mp3gain", "-v"]:
            return True, "mp3gain version 1.6.2 (stub)\n", ""
        return True, "", ""

    state = {"net": 0}

    def _apply(path, steps, timeout=None):
        state["net"] += steps
        return True

    def _undo(path, timeout=None):
        # model a FLOOR clamp: a deep attenuation can't fully reverse
        state["net"] = 0 if abs(state["net"]) < 5 else -3
        return True

    def _decode(path, *a, **k):
        return orig if state["net"] == 0 else f"PCM@{state['net']}"

    monkeypatch.setattr(la, "_run", _run)
    monkeypatch.setattr(la, "apply_gain", _apply)
    monkeypatch.setattr(la, "undo_via_tag", _undo)
    monkeypatch.setattr(la, "_decode_pcm_sha", _decode)

    rep = la.probe_mp3gain(theme)
    assert rep["attenuate_reversible_audio"] is True        # shallow is fine
    assert rep["attenuate_deep_reversible_audio"] is False  # deep clamps
    assert rep["ok"] is False                               # so the verdict is NOT ok


# ── the UI warns on the verdict, not on the tag ──────────────────────────────

def test_ui_warns_on_audio_restored_not_on_file_sha():
    js = (Path(__file__).resolve().parent.parent / "app" / "web" / "static"
          / "app.js").read_text()
    # anchor on the UNDO handler, not a byte offset from the function start — a fixed
    # window silently slides out of range as the function grows (it did, one tag later).
    i = js.index("undoBtn.addEventListener")
    block = js[i:i + 2600]
    assert "rep.audio_restored === false" in block
    # the old file-sha criterion must not drive the warning any more
    assert "rep.bit_exact === false" not in block
