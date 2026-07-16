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
proves apply→undo restores the AUDIO bit-exactly on a throwaway copy of a real theme
before any real file is touched (the // PROBE MP3GAIN diagnostic).

**Why the safety criterion is decoded-audio, NOT whole-file bytes** (v0.51.165): mp3gain
writes an APEv2 undo tag (MP3GAIN_UNDO / MP3GAIN_MINMAX) to the END of the file so it can
reverse itself. That appended tag means a restored file ALWAYS differs from the original
by a few hundred bytes of tag metadata — even when every audio frame is restored exactly.
So a whole-file sha256 can essentially never match real mp3gain (v0.51.164's probe checked
the wrong layer and read false-negative). The layer that decides safety is the decoded
PCM: the probe decodes with ffmpeg and compares the samples. It also tests both directions
— production overwhelmingly ATTENUATES (target below the loud median), which moves global_gain
AWAY from the clamp ceiling a boost can hit — so the verdict is keyed on the attenuate path
we actually use. Everything is best-effort + logged.
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


def _decode_pcm_sha(path: Path | str, timeout: int = _TIMEOUT_S) -> str | None:
    """sha256 of the decoded PCM samples (ffmpeg → raw f32le). This is the AUDIO layer:
    two files with identical PCM sound identical, regardless of container/tag bytes.
    `-vn` drops embedded cover art (mirrors loudness.py measure). None if ffmpeg is
    absent / the decode fails."""
    try:
        p = subprocess.run(
            ["ffmpeg", "-hide_banner", "-nostats", "-i", str(path),
             "-vn", "-f", "f32le", "-"],
            capture_output=True, timeout=timeout,
        )
    except FileNotFoundError:
        return None
    except Exception as e:  # noqa: BLE001 — best-effort
        log.warning("loudness probe: ffmpeg decode failed on %s: %s", path, e)
        return None
    if p.returncode != 0 or not p.stdout:
        return None
    return hashlib.sha256(p.stdout).hexdigest()


def _diff_is_trailing_tag(orig: Path, restored: Path) -> bool | None:
    """Best-effort corroboration of the PCM verdict at the byte layer: after a full
    apply→undo, the restored file should match the original for the whole audio region
    and differ ONLY by an appended/edited APEv2 (or ID3v1) tag. True if the bytes that
    differ look like just that tag; None if it can't be read."""
    try:
        a = orig.read_bytes()
        b = restored.read_bytes()
    except Exception:  # noqa: BLE001
        return None
    if a == b:
        return True
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    tail = b[i:]
    # APEv2 tags are marked "APETAGEX"; a bare ID3v1 trailer starts with "TAG".
    return b"APETAGEX" in tail or tail[:3] == b"TAG"


def probe_mp3gain(theme_path: Path | str) -> dict:
    """Read-only characterization: copy `theme_path` to temp files and run apply→undo on
    the COPIES, proving mp3gain restores the AUDIO bit-exactly WITHOUT touching the real
    theme. See the module docstring for why the criterion is decoded PCM, not file bytes.

    Tests the production-realistic ATTENUATE direction (the dominant op) via both restore
    paths, plus a BOOST cycle (the peak-limited quiet tail — the direction that can clamp):
      - attenuate -u:  −2 then `mp3gain -u`   → decoded PCM must equal the original.
      - attenuate -g:  −2 then +2 (no tag)     → cross-check via inverse gain.
      - boost -u:      +2 then `mp3gain -u`    → is a boost reversible on this file?

    Returns a report dict (never raises; never mutates theme_path)."""
    theme_path = Path(theme_path)
    report: dict = {
        "ok": False, "mp3gain_present": False, "ffmpeg_present": True,
        "version": None, "step_db": _STEP_DB, "theme": str(theme_path),
        # file-level (informational only — mp3gain appends an APEv2 undo tag, so a
        # restored file is EXPECTED to differ from the original by that tag).
        "apply_changes_bytes": None,
        "restored_file_bit_exact": None,
        "restored_diff_is_tag_only": None,
        # audio-level (authoritative): decoded-PCM reversibility.
        "apply_changes_audio": None,
        "attenuate_reversible_audio": None,
        "attenuate_reversible_inverse_g": None,
        "boost_reversible_audio": None,
        "error": None,
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

    orig_pcm = _decode_pcm_sha(theme_path)
    if orig_pcm is None:
        report["ffmpeg_present"] = False
        report["error"] = ("ffmpeg could not decode the theme — audio-level "
                           "reversibility can't be verified")
        return report

    sha0 = _sha256(theme_path)
    try:
        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)

            def _cycle(name: str, first_steps: int, restore: str) -> dict:
                """Copy → apply first_steps → restore → decode. restore ∈ {'u','inverse'}."""
                c = tdp / f"{name}.mp3"
                shutil.copy2(theme_path, c)
                apply_gain(c, first_steps)
                applied_pcm = _decode_pcm_sha(c)
                applied_bytes_changed = _sha256(c) != sha0
                if restore == "u":
                    undo_via_tag(c)
                else:
                    apply_gain(c, -first_steps)
                return {
                    "applied_pcm": applied_pcm,
                    "applied_bytes_changed": applied_bytes_changed,
                    "restored_pcm": _decode_pcm_sha(c),
                    "restored_file_exact": _sha256(c) == sha0,
                    "restored_tag_only": _diff_is_trailing_tag(theme_path, c),
                }

            atten_u = _cycle("atten_u", -2, "u")
            atten_g = _cycle("atten_g", -2, "inverse")
            boost_u = _cycle("boost_u", 2, "u")

            report["apply_changes_bytes"] = atten_u["applied_bytes_changed"]
            report["apply_changes_audio"] = (
                atten_u["applied_pcm"] is not None
                and atten_u["applied_pcm"] != orig_pcm)
            report["attenuate_reversible_audio"] = atten_u["restored_pcm"] == orig_pcm
            report["attenuate_reversible_inverse_g"] = atten_g["restored_pcm"] == orig_pcm
            report["boost_reversible_audio"] = boost_u["restored_pcm"] == orig_pcm
            report["restored_file_bit_exact"] = atten_u["restored_file_exact"]
            report["restored_diff_is_tag_only"] = atten_u["restored_tag_only"]
    except Exception as e:  # noqa: BLE001 — diagnostic, never break the caller
        report["error"] = f"{type(e).__name__}: {e}"
        return report

    # Safety verdict: the production-dominant op is ATTENUATION (target below the loud
    # median), applied + reversed via the `mp3gain -u` undo tag. Require THAT path to
    # restore the AUDIO bit-exactly. (boost_reversible_audio is surfaced separately so
    # we decide whether to allow the quiet-tail boost — it's the direction that can clamp.)
    report["ok"] = bool(
        report["apply_changes_audio"]
        and report["attenuate_reversible_audio"])
    return report
