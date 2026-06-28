"""v1.24.45 — over-ceiling collection theme: auto-downscale + surface.

the user's Middle-Earth Collection theme is 12.5MB, over Plex's ~10MB upload
ceiling → HTTP 500. Collections have NO folder on disk, so (unlike movies/TV)
there's no sidecar fallback — the upload terminal-failed with nothing placed
("nothing happened"). Fix (the user: "Both"):
  - AUTO-DOWNSCALE: _do_place_collection re-encodes an over-ceiling theme down to
    fit (ffmpeg, duration-aware bitrate) before upload, so it actually deploys.
  - SURFACE: if it STILL can't fit (theme too long / no ffmpeg), stamp a distinct
    `plex_rejected:over_ceiling` reason (kept under the plex_rejected: prefix so
    the scheduler retry-skip gate is unchanged) + a red ⊘ row glyph so it's not
    silent.
"""
from __future__ import annotations

import shutil
import subprocess
import wave
from pathlib import Path

import pytest

from app.core.worker import _downscale_audio_to_fit

REPO = Path(__file__).resolve().parent.parent
HAS_FFMPEG = shutil.which("ffmpeg") is not None


# ── downscale helper ─────────────────────────────────────────────────────

def test_downscale_returns_none_without_ffmpeg(tmp_path, monkeypatch):
    # graceful: no ffmpeg → None (caller uploads the original + surfaces).
    monkeypatch.setattr("shutil.which", lambda _name: None)
    src = tmp_path / "x.mp3"
    src.write_bytes(b"\x00" * 1000)
    assert _downscale_audio_to_fit(src, 500) is None


@pytest.mark.skipif(not HAS_FFMPEG, reason="ffmpeg not installed locally")
def test_downscale_shrinks_a_real_mp3(tmp_path):
    # Build a real ~20s WAV → encode a high-bitrate MP3 → downscale to a small
    # target and assert the result is smaller and a valid file.
    wav = tmp_path / "tone.wav"
    with wave.open(str(wav), "w") as w:
        w.setnchannels(1); w.setsampwidth(2); w.setframerate(44100)
        w.writeframes(b"\x01\x02" * (44100 * 20))
    big = tmp_path / "big.mp3"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(wav),
                    "-b:a", "320k", str(big)], check=True)
    target = 40_000
    out = _downscale_audio_to_fit(big, target)
    assert out is not None and len(out) < big.stat().st_size


# ── worker wiring + the surface reason ───────────────────────────────────

def test_collection_place_downscales_over_ceiling():
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # the downscale is invoked from the collection place path
    assert "_downscale_audio_to_fit(" in src
    assert "len(audio_bytes) >= _ceiling_bytes" in src


def test_over_ceiling_reason_keeps_plex_rejected_prefix():
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # distinct token for the row surface, but UNDER plex_rejected: so the
    # scheduler retry-skip (LIKE 'plex_rejected:%') is unchanged (no loop).
    assert '"plex_rejected:over_ceiling"' in src
    # the scheduler still skips it via the prefix gate
    sched = (REPO / "app" / "core" / "scheduler.py").read_text()
    assert "LIKE 'plex_rejected:%'" in sched


def test_row_surfaces_over_ceiling_glyph():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "it.last_place_attempt_reason === 'plex_rejected:over_ceiling'" in js
    assert "title-glyph-toobig" in js
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    assert ".title-glyph-toobig" in css
