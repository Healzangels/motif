"""v0.51.281 — feature-brief C: trim/fade editing (backend).

The design blocker this feature was parked on has dissolved by construction:
a trim requires an ffmpeg re-encode (lossy), which used to mean destroying
the only copy of the source — but revision history (v0.51.277) retains the
pre-edit bytes as a restorable revision, so an edit is non-destructive by the
same mechanism every other replacement is.

Deliberate deviations from the brief's §7.2, recorded:
  * NO loudness normalization in the editor. motif's loudness pipeline is
    mp3gain — lossless, tag-based, undoable (v0.51.170) — and duplicating it
    here with ffmpeg's loudnorm would re-encode a second time and break the
    undo anchors. Trim/fade here; level with the existing // LEVEL LOUDNESS.
  * Silence-detection/suggested trim points deferred (the brief marks them
    optional).

Pipeline (the brief's shape): render to a CANDIDATE under
themes_dir/.edit-candidates/ (dot-prefixed like .revisions — invisible to the
orphan scan and Plex; same filesystem so the save is an atomic replace) →
ffprobe-validate → the operator previews the candidate → SAVE captures the
outgoing current as a revision, replaces the canonical, clears the mp3gain
norm columns (new bytes, old undo anchors invalid), and enqueues a place job
(the v0.51.272 lesson). SAVE takes the sha the edit was based on and refuses
when the canonical changed underneath — the brief's concurrent-edit guard as
an optimistic lock, no held locks across ffmpeg work (the standing rule).
"""
from __future__ import annotations

import hashlib
import json
import logging
import secrets
import shutil
import subprocess
import time
from pathlib import Path

from .db import get_conn, transaction
from .events import log_event, now_iso
from .revisions import capture_revision

log = logging.getLogger(__name__)

_CAND_DIR = ".edit-candidates"
_CAND_TTL_S = 3600
_FFMPEG_TIMEOUT_S = 120
_MAX_FADE_S = 30.0


class EditError(ValueError):
    """Operator-readable refusal (bad bounds, missing tools, stale base)."""


def probe_duration(path: Path) -> float | None:
    if shutil.which("ffprobe") is None:
        return None
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "json", str(path)],
            capture_output=True, text=True, timeout=_FFMPEG_TIMEOUT_S)
        dur = float(json.loads(out.stdout)["format"]["duration"])
        return dur if dur > 0 else None
    except (subprocess.SubprocessError, ValueError, KeyError, OSError):
        return None


def _validate_bounds(duration: float, trim_start: float, trim_end: float,
                     fade_in: float, fade_out: float) -> None:
    if trim_start < 0 or trim_end <= trim_start:
        raise EditError("trim bounds must satisfy 0 <= start < end")
    if trim_end > duration + 0.05:
        raise EditError(f"trim end {trim_end:.2f}s is past the file's "
                        f"{duration:.2f}s")
    if fade_in < 0 or fade_out < 0:
        raise EditError("fades must be >= 0")
    if fade_in > _MAX_FADE_S or fade_out > _MAX_FADE_S:
        raise EditError(f"fades are capped at {_MAX_FADE_S:.0f}s")
    if fade_in + fade_out > (trim_end - trim_start):
        raise EditError("fades cannot overlap — together they exceed the "
                        "trimmed duration")


def _sweep_stale(cand_dir: Path) -> None:
    # opportunistic: every render tidies candidates older than the TTL, so an
    # abandoned preview can't accumulate (no scheduler entry needed).
    try:
        cutoff = time.time() - _CAND_TTL_S
        for f in cand_dir.glob("*.mp3"):
            if f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)
    except OSError:
        pass


def render_candidate(themes_dir: Path, src_rel: str, *, trim_start: float,
                     trim_end: float, fade_in: float = 0.0,
                     fade_out: float = 0.0, quality: int = 0) -> dict:
    """Render the edit to a candidate file; returns id + measured facts.

    ffmpeg is invoked as an ARG ARRAY (never a shell), with -ss/-to AFTER -i
    for sample-accurate trims (audio is small; the slow accurate seek is
    fine). Fades are computed on the OUTPUT timeline."""
    if shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None:
        raise EditError("ffmpeg/ffprobe not available on this install")
    src = themes_dir / src_rel
    if not src.exists():
        raise EditError("the canonical file is missing on disk")
    duration = probe_duration(src)
    if duration is None:
        raise EditError("could not probe the source duration")
    _validate_bounds(duration, trim_start, trim_end, fade_in, fade_out)
    out_dur = trim_end - trim_start
    afades = []
    if fade_in > 0:
        afades.append(f"afade=t=in:st=0:d={fade_in:.3f}")
    if fade_out > 0:
        afades.append(f"afade=t=out:st={max(0.0, out_dur - fade_out):.3f}"
                      f":d={fade_out:.3f}")
    cand_dir = themes_dir / _CAND_DIR
    cand_dir.mkdir(parents=True, exist_ok=True)
    _sweep_stale(cand_dir)
    cid = secrets.token_hex(16)
    dest = cand_dir / f"{cid}.mp3"
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
           "-i", str(src), "-ss", f"{trim_start:.3f}", "-to", f"{trim_end:.3f}",
           "-vn"]
    if afades:
        cmd += ["-af", ",".join(afades)]
    cmd += ["-codec:a", "libmp3lame", "-q:a", str(int(quality)), str(dest)]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=_FFMPEG_TIMEOUT_S)
    except subprocess.SubprocessError as e:
        dest.unlink(missing_ok=True)
        raise EditError(f"render failed: {e}")
    if proc.returncode != 0 or not dest.exists():
        err = (proc.stderr or "").strip()[-300:]
        dest.unlink(missing_ok=True)
        raise EditError(f"ffmpeg failed: {err or 'unknown error'}")
    got = probe_duration(dest)
    if got is None:
        dest.unlink(missing_ok=True)
        raise EditError("rendered candidate failed validation (unreadable)")
    sha = hashlib.sha256(dest.read_bytes()).hexdigest()
    return {"candidate_id": cid, "duration_s": round(got, 2),
            "file_size": dest.stat().st_size, "sha256": sha,
            "source_duration_s": round(duration, 2)}


def candidate_path(themes_dir: Path, candidate_id: str) -> Path:
    """Traversal-safe: the id must be exactly 32 hex chars."""
    if not (len(candidate_id) == 32
            and all(c in "0123456789abcdef" for c in candidate_id)):
        raise EditError("invalid candidate id")
    return themes_dir / _CAND_DIR / f"{candidate_id}.mp3"


def discard_candidate(themes_dir: Path, candidate_id: str) -> None:
    candidate_path(themes_dir, candidate_id).unlink(missing_ok=True)


def save_edit(db_path: Path, themes_dir: Path, *, media_type: str,
              tmdb_id: int, section_id: str, edition_key: str,
              candidate_id: str, base_sha: str, actor: str = "admin") -> dict:
    """Promote a previewed candidate to the active canonical.

    Requires NO ffmpeg (the render already validated) — the save is: verify
    the optimistic lock, capture the outgoing revision, atomic replace,
    update local_files (clearing the mp3gain norm columns — new bytes, old
    undo anchors invalid), enqueue the re-place."""
    cand = candidate_path(themes_dir, candidate_id)
    if not cand.exists():
        raise EditError("candidate expired or already used — preview again")
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT file_path, file_sha256 FROM local_files "
            " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
            "   AND COALESCE(edition_key, '') = ?",
            (media_type, tmdb_id, section_id, edition_key or "")).fetchone()
    if row is None or not row["file_path"]:
        raise EditError("the row no longer has a canonical to edit")
    if row["file_sha256"] and base_sha and row["file_sha256"] != base_sha:
        raise EditError("the theme changed while you were editing — reopen "
                        "the editor to work from the current audio")
    capture_revision(db_path, themes_dir, media_type=media_type,
                     tmdb_id=tmdb_id, section_id=section_id,
                     edition_key=edition_key or "",
                     reason="replaced_by_edit", actor=actor)
    target = themes_dir / row["file_path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    new_sha = hashlib.sha256(cand.read_bytes()).hexdigest()
    new_size = cand.stat().st_size
    cand.replace(target)                   # same FS (both under themes_dir)
    from .worker import _cond_columns
    _lc = _cond_columns(None, new_sha)     # clears all 11 norm columns
    with get_conn(db_path) as conn, transaction(conn):
        conn.execute(
            "UPDATE local_files SET file_sha256 = ?, file_size = ?, "
            "       downloaded_at = ?, canonical_present = 1, "
            "       loudness_i=?, loudness_tp=?, loudness_lra=?, "
            "       loudness_measured_at=?, loudness_measured_sha256=?, "
            "       norm_state=?, norm_gain_db=?, norm_target=?, norm_at=?, "
            "       norm_orig_sha256=?, norm_orig_pcm_sha256=? "
            " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
            "   AND COALESCE(edition_key, '') = ?",
            (new_sha, new_size, now_iso(), *_lc,
             media_type, tmdb_id, section_id, edition_key or ""))
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, "
            "                  payload, status, created_at, next_run_at) "
            "VALUES ('place', ?, ?, ?, ?, 'pending', ?, ?)",
            (media_type, tmdb_id, section_id,
             json.dumps({"edition_key": edition_key})
             if edition_key else "{}", now_iso(), now_iso()))
    summary = {"media_type": media_type, "tmdb_id": tmdb_id,
               "section_id": section_id, "edition_key": edition_key or "",
               "sha256": new_sha, "file_size": new_size}
    log_event(db_path, level="INFO", component="audio-edit",
              media_type=media_type, tmdb_id=tmdb_id, section_id=section_id,
              message=(f"Theme edited for {media_type}/{tmdb_id} — previous "
                       f"audio kept as a revision, re-place queued"),
              detail=summary)
    return summary
