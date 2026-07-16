"""v0.51.164 — theme loudness normalization: the mp3gain apply/undo primitive (Phase 1/1).

Engine = mp3gain (chosen off the real prod spread). This lands the leaf primitive +
the reversibility PROBE. mp3gain is container-only (not on the dev box), so the
subprocess is stubbed with a FAITHFUL reversible byte-op so the probe's bit-exact
logic is genuinely exercised; the gain math is pure + fully tested.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core import loudness_apply as la


# ── gain math (pure) ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("target, measured, tp, expected_steps", [
    (-18.0, -14.5, -2.0, -2),   # loud → attenuate ~3.5 dB → -2 steps (uncapped)
    (-18.0, -4.0, 3.7, -9),     # loud + CLIPPING → attenuate ~14 dB (fixes the clip)
    (-18.0, -18.0, -3.0, 0),    # already at target → no change
    (-18.0, -25.0, -10.0, 5),   # quiet, lots of headroom → boost ~7 dB → +5 steps
    (-18.0, -30.0, -3.0, 1),    # quiet but little headroom → boost capped to ~2 dB (peak-limited)
    (-18.0, -20.0, 2.0, 0),     # already clipping (tp>ceiling) → NO boost
    (-18.0, -25.0, None, 5),    # missing true_peak → uncapped boost
])
def test_gain_steps_for_target(target, measured, tp, expected_steps):
    assert la.gain_steps_for_target(target, measured, tp) == expected_steps


def test_applied_db_is_steps_times_step():
    assert la.applied_db(-2) == round(-2 * la._STEP_DB, 2)
    assert la.applied_db(0) == 0.0


# ── apply / undo command shape (subprocess stubbed) ──────────────────────────

def test_apply_gain_command(monkeypatch):
    seen = {}

    class _P:
        returncode = 0
        stdout = ""
        stderr = ""

    monkeypatch.setattr(la.subprocess, "run",
                        lambda cmd, *a, **k: (seen.update(cmd=cmd) or _P()))
    assert la.apply_gain("/x/theme.mp3", 3) is True
    assert seen["cmd"] == ["mp3gain", "-g", "3", "/x/theme.mp3"]


def test_apply_gain_zero_is_noop(monkeypatch):
    called = {"n": 0}
    monkeypatch.setattr(la.subprocess, "run",
                        lambda *a, **k: called.update(n=called["n"] + 1))
    assert la.apply_gain("/x/theme.mp3", 0) is True
    assert called["n"] == 0   # no subprocess for a 0-step no-op


def test_undo_command(monkeypatch):
    seen = {}

    class _P:
        returncode = 0
        stdout = ""
        stderr = ""

    monkeypatch.setattr(la.subprocess, "run",
                        lambda cmd, *a, **k: (seen.update(cmd=cmd) or _P()))
    assert la.undo_via_tag("/x/theme.mp3") is True
    assert seen["cmd"] == ["mp3gain", "-u", "/x/theme.mp3"]


def test_apply_gain_missing_binary_returns_false(monkeypatch):
    def _raise(*a, **k):
        raise FileNotFoundError("mp3gain")
    monkeypatch.setattr(la.subprocess, "run", _raise)
    assert la.apply_gain("/x/theme.mp3", 3) is False


# ── the reversibility probe (faithful reversible stub) ───────────────────────

def _install_reversible_mp3gain(monkeypatch):
    """Stub la._run so mp3gain -g N appends N bytes (N>0) / truncates |N| (N<0), and
    -u truncates the last applied amount — a genuinely reversible byte op, so the
    probe's sha256 bit-exact comparisons run for real."""
    applied = {"n": 0}

    def _run(cmd, timeout=None):
        if cmd[:2] == ["mp3gain", "-v"]:
            return True, "mp3gain 1.6.2 ...\n", ""
        if cmd[1] == "-g":
            n = int(cmd[2]); p = Path(cmd[3]); data = p.read_bytes()
            if n > 0:
                p.write_bytes(data + b"\x00" * n)
            elif n < 0:
                p.write_bytes(data[:len(data) + n])
            applied["n"] += n
            return True, "", ""
        if cmd[1] == "-u":
            p = Path(cmd[2]); data = p.read_bytes()
            p.write_bytes(data[:len(data) - applied["n"]])
            applied["n"] = 0
            return True, "", ""
        return True, "", ""

    monkeypatch.setattr(la, "_run", _run)


def test_probe_reports_bit_exact_reversibility(tmp_path, monkeypatch):
    _install_reversible_mp3gain(monkeypatch)
    theme = tmp_path / "theme.mp3"
    theme.write_bytes(b"ID3fake-mp3-bytes" * 100)
    orig = theme.read_bytes()

    rep = la.probe_mp3gain(theme)
    assert rep["mp3gain_present"] is True
    assert rep["apply_changes_bytes"] is True     # -g actually mutated the copy
    assert rep["inverse_g_bit_exact"] is True      # +N then -N restored bit-exact
    assert rep["undo_tag_bit_exact"] is True        # -u restored bit-exact
    assert rep["ok"] is True
    assert theme.read_bytes() == orig              # the REAL theme was never touched


def test_probe_reports_mp3gain_absent(tmp_path, monkeypatch):
    # the real absence signal: _run's FileNotFoundError sentinel.
    monkeypatch.setattr(la, "_run", lambda cmd, timeout=None: (False, "", la._NOT_FOUND))
    theme = tmp_path / "theme.mp3"
    theme.write_bytes(b"x" * 50)
    rep = la.probe_mp3gain(theme)
    assert rep["mp3gain_present"] is False
    assert rep["ok"] is False
    assert "not installed" in rep["error"]
