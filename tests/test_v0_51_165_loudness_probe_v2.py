"""v0.51.165 — loudness probe v2: AUDIO-level reversibility, not whole-file bytes.

v0.51.164's probe sha256'd the WHOLE FILE and read a false-negative on the user's real
files (ok=false): mp3gain appends an APEv2 undo tag, so a restored file ALWAYS differs
from the original by that tag — even when every audio sample is restored exactly. The
safety layer is the decoded PCM, not the container bytes.

probe_mp3gain now decodes both files with ffmpeg and compares the SAMPLES, tests the
ATTENUATE direction production actually uses (target below the loud median → global_gain
moves away from the clamp ceiling), and keys the verdict on that path.

mp3gain + ffmpeg are container-only (not on the dev box), so a FAITHFUL stub models
mp3gain's real irreversibility mode — global_gain CLAMPING, where a forward -g that hits
the frame ceiling still records the NOMINAL delta in its undo tag, so a later -u reverses
too much and the audio doesn't come back — plus ffmpeg decode. Both probe layers run for
real against the stub.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from app.core import loudness_apply as la


_APE = b"APETAGEX" + b"\x00" * 32   # a fake trailing APEv2 tag marker


def _strip_tag(data: bytes) -> bytes:
    idx = data.rfind(b"APETAGEX")
    return data[:idx] if idx != -1 else data


def _install_mp3gain_stub(monkeypatch, *, floor=None, ceil=None):
    """Faithful mp3gain model. Per-file (nominal, actual) net gain: `-g N` bumps the
    nominal by N and clamps the ACTUAL to [floor, ceil]; `-u` reverses the NOMINAL total.
    When a forward apply clamped, actual != nominal, so -u overshoots and the restored
    audio differs — mp3gain's real global_gain-clamp irreversibility. Every op rewrites an
    APE tag so the file bytes differ (exercising apply_changes_bytes / tag-only diff), while
    _decode_pcm_sha reads the ACTUAL net gain so the decoded 'audio' tracks the real gain."""
    nominal: dict[str, int] = {}
    actual: dict[str, int] = {}

    def _clamp(v: int) -> int:
        if ceil is not None:
            v = min(v, ceil)
        if floor is not None:
            v = max(v, floor)
        return v

    def _rewrite_tag(p: str) -> None:
        data = _strip_tag(Path(p).read_bytes())
        Path(p).write_bytes(data + _APE)

    def _run(cmd, timeout=None):
        if cmd[:2] == ["mp3gain", "-v"]:
            return True, "mp3gain version 1.6.2 (stub)\n", ""
        if cmd[1] == "-g":
            n = int(cmd[2]); p = str(cmd[3])
            nominal[p] = nominal.get(p, 0) + n
            actual[p] = _clamp(actual.get(p, 0) + n)
            _rewrite_tag(p)
            return True, "", ""
        if cmd[1] == "-u":
            p = str(cmd[2])
            actual[p] = actual.get(p, 0) - nominal.get(p, 0)   # reverse the nominal
            nominal[p] = 0
            _rewrite_tag(p)
            return True, "", ""
        return True, "", ""

    def _decode(path, timeout=None):
        return hashlib.sha256(
            ("pcm@%d" % actual.get(str(path), 0)).encode()).hexdigest()

    monkeypatch.setattr(la, "_run", _run)
    monkeypatch.setattr(la, "_decode_pcm_sha", _decode)


def _theme(tmp_path) -> Path:
    t = tmp_path / "theme.mp3"
    t.write_bytes(b"ID3fake-mp3-audio-frames" * 200)   # audio-only, no tag
    return t


def test_probe_ok_when_attenuate_audio_reversible(tmp_path, monkeypatch):
    """No clamping → attenuate→undo restores the audio (though the file gains an APE
    tag). ok=True even though the file is NOT bit-exact — that's the whole point."""
    _install_mp3gain_stub(monkeypatch)
    theme = _theme(tmp_path)
    orig = theme.read_bytes()

    rep = la.probe_mp3gain(theme)
    assert rep["mp3gain_present"] is True
    assert rep["ffmpeg_present"] is True
    assert rep["apply_changes_bytes"] is True         # -g wrote an APE tag → bytes moved
    assert rep["apply_changes_audio"] is True          # -g changed the samples
    assert rep["attenuate_reversible_audio"] is True
    assert rep["attenuate_reversible_inverse_g"] is True
    assert rep["boost_reversible_audio"] is True
    assert rep["restored_file_bit_exact"] is False      # APE tag remains after undo
    assert rep["restored_diff_is_tag_only"] is True     # ...and the ONLY diff is that tag
    assert rep["ok"] is True
    assert theme.read_bytes() == orig                   # the REAL theme was never touched


def test_probe_fails_when_attenuate_clamps(tmp_path, monkeypatch):
    """floor models global_gain hitting its MIN on a -2 attenuate: the forward clamps to
    -1 but -u reverses the nominal -2 → lands at +1, audio NOT restored → not safe."""
    _install_mp3gain_stub(monkeypatch, floor=-1)
    theme = _theme(tmp_path)

    rep = la.probe_mp3gain(theme)
    assert rep["mp3gain_present"] is True
    assert rep["apply_changes_audio"] is True
    assert rep["attenuate_reversible_audio"] is False
    assert rep["ok"] is False


def test_probe_boost_asymmetry_keeps_verdict_on_attenuate(tmp_path, monkeypatch):
    """ceil models the global_gain MAX: a +2 boost clamps (irreversible) while the -2
    attenuate is clean. Verdict stays OK (keyed on the attenuate path we use), but the
    boost is flagged so we know to exclude quiet-tail boosts."""
    _install_mp3gain_stub(monkeypatch, ceil=1)
    theme = _theme(tmp_path)

    rep = la.probe_mp3gain(theme)
    assert rep["attenuate_reversible_audio"] is True
    assert rep["boost_reversible_audio"] is False
    assert rep["ok"] is True


def test_probe_ffmpeg_absent_is_not_ok(tmp_path, monkeypatch):
    """mp3gain present but ffmpeg can't decode → audio reversibility unverifiable → not ok
    (never silently green — CLAUDE.md class-9)."""
    def _run(cmd, timeout=None):
        if cmd[:2] == ["mp3gain", "-v"]:
            return True, "mp3gain version 1.6.2\n", ""
        return True, "", ""
    monkeypatch.setattr(la, "_run", _run)
    monkeypatch.setattr(la, "_decode_pcm_sha", lambda path, timeout=None: None)
    theme = _theme(tmp_path)

    rep = la.probe_mp3gain(theme)
    assert rep["mp3gain_present"] is True
    assert rep["ffmpeg_present"] is False
    assert rep["ok"] is False
    assert "ffmpeg" in (rep["error"] or "")


def test_probe_never_touches_real_theme_on_clamp(tmp_path, monkeypatch):
    """Even in the failing/clamping path, the real theme file is untouched (all work is
    on temp copies)."""
    _install_mp3gain_stub(monkeypatch, floor=-1)
    theme = _theme(tmp_path)
    orig = theme.read_bytes()
    la.probe_mp3gain(theme)
    assert theme.read_bytes() == orig
