"""v1.15.26 — PLEX TV count excludes anime + probe yt-dlp client
alignment with download path.

## Two bugs from the user's v1.15.25 deploy

### Bug 1: PLEX TV ThemerrDB count contaminated with anime

the user: "the Plex TB ThemerrDB is inaccurate since it would be
including anime shows that are in the TV Shows list from
themerrdb, let's treat that number the same as anime and only
show the number that correlates to the tv show library section."

ThemerrDB tracks anime under media_type='show' (no separate
anime category upstream). Motif's PLEX ANIME card aggregates
from /api/sections/coverage with is_anime=1. The PLEX TV card
was hydrated from /api/coverage/plex's tv_rows query which
filtered `media_type='show'` but NOT `is_anime=0` — so anime
items were counted in BOTH cards.

Fix: add `AND ps.is_anime = 0` to the tv_rows SQL.

### Bug 2: probe returns "indeterminate" for downloadable URLs

the user: KWXXC228g24 (Rubble & Crew theme) shown as
`?Unknown error (ambiguous — could be transient anti-bot/cookies/
throttling, or actually dead)` in the INFO card probe button.
docker logs:
    ERROR: [youtube] KWXXC228g24: This video is not available
But moments later:
    Downloaded theme for Rubble & Crew | size: 1110572,
    video_id: KWXXC228g24

Same yt-dlp + same cookies + same URL — different verdicts.
Root cause already documented in downloader.py:326-330
(v1.12.89 _opts() comment): without a JS runtime declared,
yt-dlp falls back to the `android_vr` player client which
"returns 'This video is not available' for many otherwise-
playable videos". The download path sets `js_runtimes`,
`remote_components`, and `extractor_args.youtube.player_client`
to fix this. The probe path didn't — so the probe used the
degraded client and the download used the working one.

Fix: mirror the YouTube extractor opts from `_opts()` into
`probe_youtube_url`. Gated on YouTube URLs (matches _opts()
source-aware gating; SoundCloud doesn't need them).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
DOWNLOADER_PY = REPO / "app" / "core" / "downloader.py"


# ── Bug 1: PLEX TV anime exclusion ────────────────────────────


def test_coverage_plex_tv_query_excludes_anime():
    """The /api/coverage/plex tv_rows query must filter
    `is_anime = 0`. Otherwise anime shows are double-counted
    (once in PLEX TV card, once in PLEX ANIME card)."""
    src = API_PY.read_text()
    fn_anchor = src.index('@app.get("/api/coverage/plex")')
    fn_end = src.index('@app.', fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # Find the tv_rows query block.
    tv_anchor = fn_body.index("tv_rows = conn.execute(")
    tv_end = fn_body.index(".fetchall()", tv_anchor)
    tv_query = fn_body[tv_anchor:tv_end]
    assert "media_type = 'show'" in tv_query
    assert "ps.is_anime = 0" in tv_query, (
        "v1.15.26: tv_rows query must exclude anime sections "
        "(ps.is_anime = 0) — otherwise anime shows are counted "
        "in both PLEX TV and PLEX ANIME cards"
    )


def test_coverage_plex_movies_query_gates_is_anime():
    """v1.23.90: movies_rows is `media_type = 'movie' AND ps.is_anime = 0` —
    anime-section films (a movie-typed row in an is_anime=1 section) count on
    the ANIME card, matching the library's MOVIES tab. Pre-tag this query had no
    is_anime filter, so anime films inflated the MOVIES card (the 1,341 bug)."""
    src = API_PY.read_text()
    fn_anchor = src.index('@app.get("/api/coverage/plex")')
    fn_end = src.index('@app.', fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    movies_anchor = fn_body.index("movies_rows = conn.execute(")
    movies_end = fn_body.index(".fetchall()", movies_anchor)
    movies_query = fn_body[movies_anchor:movies_end]
    assert "media_type = 'movie'" in movies_query
    # v1.23.90: anime-section films belong to the ANIME tab, not MOVIES.
    assert "ps.is_anime = 0" in movies_query


# ── Bug 2: probe vs download client alignment ─────────────────


def test_probe_youtube_url_declares_js_runtime():
    """probe_youtube_url must declare nodejs as the JS runtime
    so yt-dlp can use the full `web` player client. Without
    this, yt-dlp falls back to `android_vr` which returns
    'This video is not available' for many otherwise-playable
    videos (v1.12.89 _opts() comment + the user's KWXXC228g24
    case)."""
    src = DOWNLOADER_PY.read_text()
    fn_anchor = src.index("def probe_youtube_url(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert '"js_runtimes"' in fn_body, (
        "v1.15.26: probe must declare js_runtimes so yt-dlp uses "
        "a JS-capable player client (mirrors _opts() download "
        "path; pre-fix probe got 'This video is not available' "
        "from the android_vr fallback for the user's KWXXC228g24)"
    )
    assert '{"node": {}}' in fn_body


def test_probe_youtube_url_enables_remote_components():
    """yt-dlp's EJS bundle (remote_components) gates higher-
    quality format extraction. Mirror the download path's
    setting so probe behavior matches."""
    src = DOWNLOADER_PY.read_text()
    fn_anchor = src.index("def probe_youtube_url(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert '"remote_components"' in fn_body
    assert '"ejs:github"' in fn_body


def test_probe_youtube_url_specifies_player_client_fallbacks():
    """Multi-client fallback chain (default / android / ios /
    mweb) — same shape as _opts(). If `web` (default) trips
    anti-bot, yt-dlp falls through to the next client instead
    of giving up immediately."""
    src = DOWNLOADER_PY.read_text()
    fn_anchor = src.index("def probe_youtube_url(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # extractor_args block must be present with player_client.
    assert '"extractor_args"' in fn_body
    assert '"player_client"' in fn_body
    # All four clients from _opts().
    for client in ('"default"', '"android"', '"ios"', '"mweb"'):
        assert client in fn_body, (
            f"v1.15.26: probe player_client list must include "
            f"{client} (matches _opts() download path)"
        )


def test_probe_yt_extractor_opts_gated_on_youtube_url():
    """The YouTube extractor opts must be conditional on the URL
    being a YouTube URL — SoundCloud and other extractors don't
    need js_runtimes / EJS / player_client list and yt-dlp
    chokes on them in some 2025+ builds (v1.14.5 comment in
    _opts())."""
    src = DOWNLOADER_PY.read_text()
    fn_anchor = src.index("def probe_youtube_url(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # The opts must sit inside an `if "youtube.com" in url or
    # "youtu.be" in url` guard.
    js_runtime_anchor = fn_body.index('"js_runtimes"')
    # Walk back to find the gating if.
    pre_block = fn_body[max(0, js_runtime_anchor - 400):js_runtime_anchor]
    assert "youtube.com" in pre_block
    assert "youtu.be" in pre_block
