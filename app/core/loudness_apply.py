"""Theme loudness normalization — the mp3gain APPLY / UNDO primitive (Phase 1).

The engine chosen (2026-07-15, off the real prod spread — median -14.5 LUFS, wide,
lots of +dBTP clipping): **mp3gain**. It edits each MP3 frame's global_gain field
(no re-encode, lossless) in ~1.505 dB steps and is reversible in-file. Because the
library is mostly TOO LOUD, the dominant op is attenuation (always safe + it pulls the
clipping tail back under 0 dBTP); the quiet tail gets boosted, capped so a boost never
induces clipping — a peak-limited theme lands BELOW target rather than clip (gain-only,
dynamics preserved: the user's "don't ruin the feel").

mp3gain targets ReplayGain (89 dB), NOT LUFS — so we DON'T use its analysis. We already
measure LUFS + true-peak with ffmpeg (loudness.py); here we compute the exact dB delta
to the target and apply it directly via `mp3gain -g <steps>` (no re-analysis).

Reversibility is the whole safety case for a 2,800-theme bulk, and mp3gain is
container-only (not on the dev box), so nothing here is trusted blind: probe_mp3gain()
proves apply→undo is BIT-EXACT on a throwaway copy of a real theme before any real file
is touched (the // PROBE MP3GAIN diagnostic). Everything is best-effort + logged.
"""
from __future__ import annotations

import hashlib
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

log = logging.getLogger("motif.loudness_apply")

# mp3gain moves audio in units of the MP3 spec's global_gain field: 1 step ≈ 1.505 dB.
# A LUFS target rarely lands on a step boundary, so we round — the ~0.75 dB worst-case
# residual is inaudible for a hover theme (the plan accepted this coarseness).
_STEP_DB = 1.505
# leave 1 dB of true-peak headroom below full scale; a boost is capped so the result's
# true peak can't exceed this (no induced clipping).
_PEAK_CEILING_DBTP = -1.0
_TIMEOUT_S = 120
# exact stderr _run returns when the binary is absent (FileNotFoundError) — the one
# signal that unambiguously means "mp3gain isn't installed" (vs a non-zero exit).
_NOT_FOUND = "mp3gain not found on PATH"


def gain_steps_for_target(target_lufs: float, measured_i: float,
                          true_peak: float | None, *,
                          ceiling: float = _PEAK_CEILING_DBTP) -> int:
    """mp3gain -g steps to move `measured_i` LUFS toward `target_lufs`.

    Attenuation (target below current) is uncapped — turning down never clips + it
    fixes an already-hot peak. A BOOST is capped by the peak headroom (ceiling -
    true_peak) so it can't push the true peak over the ceiling; an already-clipping
    track (true_peak >= ceiling) gets no boost. Returns an int (0 = no change)."""
    want_db = target_lufs - measured_i
    if want_db > 0 and true_peak is not None:
        headroom = ceiling - true_peak
        want_db = min(want_db, max(0.0, headroom))
    return round(want_db / _STEP_DB)


def applied_db(steps: int) -> float:
    """The actual dB an mp3gain step count applies (for storage / display)."""
    return round(steps * _STEP_DB, 2)


def _run(cmd: list[str], timeout: int = _TIMEOUT_S) -> tuple[bool, str, str]:
    """Run an mp3gain command best-effort. Returns (ok, stdout, stderr); ok=False on a
    missing binary / timeout / non-zero exit (never raises)."""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        return False, "", _NOT_FOUND
    except subprocess.TimeoutExpired:
        return False, "", f"mp3gain timed out ({timeout}s)"
    except Exception as e:  # noqa: BLE001 — best-effort
        return False, "", f"{type(e).__name__}: {e}"
    return p.returncode == 0, p.stdout or "", p.stderr or ""


def mp3gain_version() -> str | None:
    """mp3gain's version line, or None if the binary is ABSENT. Keyed on _run's
    FileNotFoundError sentinel — a present mp3gain can still exit non-zero on `-v`, so
    `ok` alone isn't the absence signal."""
    ok, out, err = _run(["mp3gain", "-v"], timeout=10)
    if err == _NOT_FOUND:
        return None
    text = (out + err).strip()
    return text.splitlines()[0] if text else "mp3gain (present)"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def apply_gain(path: Path | str, steps: int, timeout: int = _TIMEOUT_S) -> bool:
    """Apply `steps` of gain to the MP3 in place (mp3gain -g, no re-analysis; writes the
    MP3GAIN_UNDO tag). steps==0 is a no-op. Returns True on success."""
    if steps == 0:
        return True
    ok, _out, err = _run(["mp3gain", "-g", str(steps), str(path)], timeout)
    if not ok:
        log.warning("loudness apply: mp3gain -g %s failed on %s: %s", steps, path, err)
    return ok


def undo_via_tag(path: Path | str, timeout: int = _TIMEOUT_S) -> bool:
    """Reverse the applied gain from the MP3GAIN_UNDO tag (mp3gain -u) → original."""
    ok, _out, err = _run(["mp3gain", "-u", str(path)], timeout)
    if not ok:
        log.warning("loudness undo: mp3gain -u failed on %s: %s", path, err)
    return ok


def probe_mp3gain(theme_path: Path | str) -> dict:
    """Read-only characterization: copy `theme_path` to a temp file and run apply→undo
    on the COPY, proving mp3gain's reversibility WITHOUT touching the real theme.

    Tests BOTH restore paths so we know which to trust before building the real apply:
      - inverse -g: apply +N then -N; bit-exact if no global_gain clamp.
      - tag -u:     apply +N then `mp3gain -u`.

    Returns a report dict (never raises; never mutates theme_path)."""
    theme_path = Path(theme_path)
    report: dict = {
        "ok": False, "mp3gain_present": False, "version": None,
        "step_db": _STEP_DB, "theme": str(theme_path),
        "apply_changes_bytes": None, "inverse_g_bit_exact": None,
        "undo_tag_bit_exact": None, "error": None,
    }
    ver = mp3gain_version()
    report["version"] = ver
    if ver is None:
        report["error"] = "mp3gain not installed in the container"
        return report
    report["mp3gain_present"] = True
    if not theme_path.is_file():
        report["error"] = f"probe theme missing: {theme_path}"
        return report

    try:
        with tempfile.TemporaryDirectory() as td:
            # ── inverse-g path ──
            a = Path(td) / "a.mp3"
            shutil.copy2(theme_path, a)
            sha0 = _sha256(a)
            apply_gain(a, 2)
            sha_applied = _sha256(a)
            report["apply_changes_bytes"] = sha_applied != sha0
            apply_gain(a, -2)
            report["inverse_g_bit_exact"] = _sha256(a) == sha0

            # ── tag-u path (fresh copy) ──
            b = Path(td) / "b.mp3"
            shutil.copy2(theme_path, b)
            sha0b = _sha256(b)
            apply_gain(b, 2)
            undo_via_tag(b)
            report["undo_tag_bit_exact"] = _sha256(b) == sha0b
    except Exception as e:  # noqa: BLE001 — diagnostic, never break the caller
        report["error"] = f"{type(e).__name__}: {e}"
        return report

    report["ok"] = bool(report["apply_changes_bytes"]) and (
        report["inverse_g_bit_exact"] or report["undo_tag_bit_exact"])
    return report
