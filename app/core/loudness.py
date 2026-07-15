"""Theme loudness measurement — the read-only primitive behind the LOUDNESS AUDIT.

motif's download → transcode pipeline (downloader.py) does ZERO loudness
conditioning, so themes from different sources (YouTube / SoundCloud / IG / FB /
manual MP3) play back at wildly different volumes on Plex hover — the #1
perceived-quality defect. Phase 0 of the loudness feature is a read-only audit:
measure every local-bytes theme, surface the spread, and let the operator pick a
target — WITHOUT touching a single file. This module is that measurement primitive.

It shells out to ffmpeg's EBU R128 `loudnorm` filter in ANALYSIS mode
(`print_format=json -f null`), which measures the integrated loudness / true peak
/ loudness range and prints them as JSON WITHOUT writing any output. Nothing here
mutates a file — apply/undo (a later phase, engine TBD: mp3gain vs loudnorm) lives
elsewhere.

ffmpeg is invoked as a subprocess from PATH (the same ffmpeg the container installs
for yt-dlp's FFmpegExtractAudio). Everything is best-effort: a missing binary, a
non-audio file, or a timeout returns None rather than raising — a measurement gap
must never break a caller (class-9: logged, not silent).
"""
from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path

log = logging.getLogger("motif.loudness")

# ffmpeg analysis can be slow on a large file; cap it so one pathological theme
# can't wedge the audit worker. Hover themes are short (seconds), so 120s is ample.
_MEASURE_TIMEOUT_S = 120

# class-9 warn-once: if ffmpeg is missing the FIRST failure logs loud (the operator
# needs to know the audit can't run), the rest downgrade to debug.
_FFMPEG_MISSING_WARNED = False


def _parse_loudnorm_json(stderr_text: str) -> dict | None:
    """Extract the input measurements from ffmpeg's loudnorm JSON block.

    ffmpeg prints the loudnorm stats as a flat JSON object to STDERR at the end of
    the run, e.g.::

        {
            "input_i" : "-24.30",
            "input_tp" : "-5.12",
            "input_lra" : "7.30",
            ...
        }

    Values are strings. We only need the INPUT figures (what real normalization
    would act on): integrated loudness (LUFS), true peak (dBTP), loudness range.
    Returns {loudness_i, true_peak, lra} as floats, or None if the block is absent
    / malformed (best-effort — a caller must tolerate a measurement gap)."""
    if not stderr_text:
        return None
    # The loudnorm block is flat (no nested braces — values are numeric strings),
    # and it prints LAST, so the final {...} span is it.
    start = stderr_text.rfind("{")
    end = stderr_text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        d = json.loads(stderr_text[start:end + 1])
    except (ValueError, TypeError):
        return None
    try:
        return {
            "loudness_i": float(d["input_i"]),
            "true_peak": float(d["input_tp"]),
            "lra": float(d["input_lra"]),
        }
    except (KeyError, ValueError, TypeError):
        return None


def measure_loudness(file_path: Path | str,
                     timeout: int = _MEASURE_TIMEOUT_S) -> dict | None:
    """Measure one audio file's loudness via ffmpeg loudnorm analysis (read-only).

    Returns {loudness_i, true_peak, lra} (floats) or None on any failure (missing
    ffmpeg / missing file / non-audio / timeout / unparseable output). Blocking —
    call from a worker thread / ThreadPoolExecutor, never the event loop
    (CLAUDE.md class-12)."""
    p = str(file_path)
    # -hide_banner -nostats keep stderr to the loudnorm JSON + errors; -f null
    # discards the (unwritten) output; `-` sink. No re-encode, no file written.
    cmd = ["ffmpeg", "-hide_banner", "-nostats", "-i", p,
           "-af", "loudnorm=print_format=json", "-f", "null", "-"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        global _FFMPEG_MISSING_WARNED
        if not _FFMPEG_MISSING_WARNED:
            log.warning("loudness: ffmpeg not found on PATH — the LOUDNESS AUDIT "
                        "cannot measure themes. Subsequent misses downgrade to "
                        "debug. (ffmpeg ships in the container for yt-dlp.)")
            _FFMPEG_MISSING_WARNED = True
        else:
            log.debug("loudness: ffmpeg still missing for %s", p)
        return None
    except subprocess.TimeoutExpired:
        log.warning("loudness: ffmpeg measure timed out (%ds) on %s", timeout, p)
        return None
    except Exception as e:  # noqa: BLE001 — best-effort, never break the caller
        log.warning("loudness: ffmpeg measure failed on %s: %s", p, e)
        return None
    result = _parse_loudnorm_json(proc.stderr or "")
    if result is None:
        # ffmpeg ran but we couldn't read the numbers (non-audio, empty file, a
        # loudnorm quirk) — log a breadcrumb, don't silently swallow (class-9).
        log.debug("loudness: no loudnorm measurement parsed for %s (rc=%s)",
                  p, proc.returncode)
    return result
