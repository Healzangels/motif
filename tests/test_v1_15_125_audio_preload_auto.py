"""v1.15.125 — audio preload="auto" + download escape hatch.

the user on v1.15.123:

> Still seeing the theme as unplayable. I also tested downloading
> the theme.mp3 from the downloads location and was able to
> playback fine in VLC.

So the file IS a valid MP3 (VLC = ffmpeg-based decoder). v1.15.123's
MIME sniff would have correctly detected it as audio/mpeg. The bug
is at a different layer.

## Root cause: Chrome + VBR MP3 without Xing header

yt-dlp's FFmpegExtractAudio postprocessor passes SoundCloud-source
MP3s through with `-c:a copy` semantics when the source is already
.mp3 — no re-encode, no Xing/Info header rewrite. If the original
SoundCloud upload was a VBR MP3 without a Xing header (common for
user-uploaded mp3s), the copied output ALSO lacks the Xing tag.

Chrome's MP3 decoder needs either:
  - the Xing/Info header at the start of the file (for VBR
    duration computation), OR
  - the full file (to compute duration by counting frames)

`<audio preload="metadata">` only fetches enough bytes for codec
detection. Without a Xing header, Chrome can't compute duration
from metadata alone → shows 0:00 / 0:00 and refuses to play.

VLC is more permissive — it pre-scans the whole file or estimates
duration from the average frame size — so it plays the same file
fine.

## The fix

`<audio preload="auto">` fetches enough bytes for Chrome to
compute duration even when the Xing header is missing. Trade-off:
~7-8 MB pre-fetch when the dialog opens. Acceptable for the
homelab use case.

A new `<a href="..." download="theme.mp3">↓</a>` link sits next
to the player as an escape hatch — if the inline element STILL
can't decode for whatever reason, the user can grab the file
locally without leaving the dialog. the user's verified-VLC-works
test would have been one click instead of navigating the
filesystem.

## Backend follow-up (not in this tag)

To force a Xing header on FUTURE downloads, the
`FFmpegExtractAudio` postprocessor could pass an explicit
re-encode flag instead of allowing source-stream copy on SC MP3
inputs. That fix doesn't help the already-downloaded Watchmen
file (would need a re-download). Defer to a separate audit pass
once the user confirms the preload=auto switch + download link
solves the immediate UX gap.

## Tests
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_audio_uses_preload_auto():
    """Pre-v1.15.125 the audio element used `preload="metadata"`
    which fails to compute duration for VBR MP3s without a Xing
    header. preload="auto" is the user-side workaround."""
    src = APP_JS.read_text()
    # The audioBlock construction must use preload="auto" — the
    # old preload="metadata" must be gone.
    audio_idx = src.index("class=\"info-audio\"")
    audio_window = src[max(0, audio_idx - 500):audio_idx]
    assert 'preload="auto"' in audio_window, (
        "v1.15.125: <audio> element must use preload=\"auto\" so "
        "Chrome can compute duration on VBR MP3s that lack a "
        "Xing/Info header (yt-dlp passes through source-stream MP3 "
        "without re-encoding when the source is already .mp3)."
    )
    assert 'preload="metadata"' not in audio_window, (
        "v1.15.125: preload=\"metadata\" must be gone from the "
        "info-card audio block (it's the regression source)."
    )


def test_audio_has_download_escape_hatch():
    """Next to the <audio> element there must be a `<a download>`
    link so the user can grab the file locally if the inline
    decoder still struggles. The Watchmen case was: VLC plays
    fine, Chrome doesn't — the download link lets the user
    confirm the file's intact without leaving the dialog."""
    src = APP_JS.read_text()
    audio_idx = src.index("class=\"info-audio\"")
    audio_window = src[audio_idx:audio_idx + 600]
    # The download link uses the same src URL + a download
    # attribute so the browser downloads instead of playing
    # inline.
    assert 'download="theme.mp3"' in audio_window, (
        "v1.15.125: a `<a href=\"...\" download=\"theme.mp3\">` "
        "escape hatch must sit next to the inline player so the "
        "user has a one-click fallback for browser-decode "
        "failures."
    )
