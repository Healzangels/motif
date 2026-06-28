"""v1.15.123 — RESOLVED VIA tone colors + audio content-sniff.

the user's report on v1.15.116 (Watchmen case):

> in the info card the Resolved Via URL text, let's make that
> purple to be in line with the action that was taken a user
> provided url. If the resolved via was adopted then it would be
> a blue adopted color, if it was let plex server then it would
> be amber. I want the resolution text to match the color of the
> action that was the resolution so at a quick glance you can
> tell what was done.

Plus:

> this show was a U which was switched to let plex server and
> now is P — however when I go to the info card I cannot
> preview the the theme with the play option. I've also
> confirmed the theme.mp3 is actually in the download location.
> Wondering if it's since the applied url is a soundcloud url.

## Part 1: RESOLVED VIA tone colors

The server already emits `tone="user"` / `tone="adopt"` on the
RESOLVED VIA info tile (api.py:13407). The JS info-tile
renderer at app.js:11523 ignored the tone — only interactive
button options applied tone classes.

Pre-fix the `.recovery-option-label` CSS hardcoded
`color: var(--cyan)` so every RESOLVED VIA banner rendered cyan
regardless of resolution path.

Fix:
  - JS: stamp `recovery-option-info-tone-{tone}` class on the
    info tile when `opt.tone` is present
  - CSS: per-tone label color matching the SRC chip palette:
      user   → violet (matches U pill)
      adopt  → cyan   (matches A pill — was the default anyway,
                       explicit rule pins the contract)
      plex   → amber  (matches P pill — reserved for future
                       plex-resolved tile emission)
      cookies → lemon (matches cookies family)

## Part 2: audio endpoint MIME sniff

The /api/items/{mt}/{id}/theme.mp3 endpoint always returned
`Content-Type: audio/mpeg`. If yt-dlp + ffmpeg's postprocessor
silently left a non-MP3 container with an .mp3 filename
(possible on SoundCloud's opus / m4a originals if ffmpeg
re-encode failed silently), the browser receives audio/mpeg
header on non-MP3 bytes — and can't decode → renders 0:00 / 0:00.

Pre-fix this was invisible; the user just saw a non-functional
audio element with no diagnostic trail.

Fix: read the first 12 bytes of the file and detect actual
format via the standard magic-number prefixes:

    ID3 / 0xFF 0xFB-family  → audio/mpeg  (real MP3)
    OggS                    → audio/ogg
    fLaC                    → audio/flac
    ftyp (offset 4)         → audio/mp4

Defaults to audio/mpeg for backward compat. Unknown headers
log at WARNING with the head bytes so the operator can grep
the log + `file path/to/theme.mp3` to diagnose.

The MIME sniff fixes the browser-decode problem for non-MP3
files served under an .mp3 path; the log line catches the
class of cases where the postprocessor silently produced the
wrong container.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


def _strip_css_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


# ── Part 1: tone colors on RESOLVED VIA tiles ───────────────

def test_info_tile_stamps_tone_class_from_opt():
    """JS must apply the recovery-option-info-tone-{tone} class
    to the info tile so the label color can vary by resolution
    path. Pre-fix the tone field was dropped on info tiles."""
    src = APP_JS.read_text()
    assert "recovery-option-info-tone-${opt.tone}" in src, (
        "v1.15.123: info-tile rendering must compose the "
        "tone class from opt.tone."
    )


def test_recovery_option_label_per_tone_colors_defined():
    """Four tone-specific label color rules — user (violet),
    adopt (cyan), plex (amber), cookies (lemon). The rules sit
    in a single block right after the base `.recovery-option-
    label` rule."""
    src = _strip_css_comments(APP_CSS.read_text())
    expected = {
        "user":    "var(--violet)",
        "adopt":   "var(--cyan)",
        "plex":    "var(--amber)",
        "cookies": "var(--lemon)",
    }
    for tone, color in expected.items():
        rule = (
            f".recovery-option-info-tone-{tone} "
            f".recovery-option-label   "
            f"{{ color: {color}; }}"
        )
        # Normalize whitespace for flexible match.
        rule_normalized = re.sub(r"\s+", " ", rule)
        src_normalized = re.sub(r"\s+", " ", src)
        assert rule_normalized in src_normalized, (
            f"v1.15.123: missing tone color rule for {tone} "
            f"(expected `{color}`)"
        )


# ── Part 2: audio endpoint MIME sniff ────────────────────────

def test_audio_endpoint_sniffs_file_header():
    """The endpoint must read the first bytes of the file and
    select Content-Type by magic number, not always send
    audio/mpeg. Catches the SoundCloud-postprocessor-leaves-non-
    MP3-bytes case."""
    src = API_PY.read_text()
    fn_start = src.index("def api_item_theme_audio(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # Must read the file header.
    assert "fh.read(12)" in fn_body or "fh.read(8)" in fn_body, (
        "v1.15.123: audio endpoint must read file header for "
        "format detection"
    )
    # Must check the standard magic prefixes.
    assert "OggS" in fn_body
    assert "ID3" in fn_body
    assert "ftyp" in fn_body


def test_audio_endpoint_returns_per_format_mime():
    """The FileResponse Content-Type must come from the sniff,
    not be hardcoded to audio/mpeg unconditionally."""
    src = API_PY.read_text()
    fn_start = src.index("def api_item_theme_audio(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The function must reference at least 3 distinct MIME types.
    mimes = {"audio/mpeg", "audio/ogg", "audio/flac", "audio/mp4"}
    found = {m for m in mimes if m in fn_body}
    assert len(found) >= 3, (
        f"v1.15.123: expected ≥3 audio MIME types in endpoint, "
        f"found {found}. The sniff result should dispatch into "
        "the right Content-Type."
    )
    # FileResponse must use the variable, not the hardcoded string.
    assert 'FileResponse(full, media_type=media_type_header' in fn_body
