"""v1.14.9 — SET URL preserves SoundCloud URLs (don't wrap as YouTube).

Critical regression revealed by the user's repro: pasting a SC URL
into the SET URL dialog stored it as a fake YouTube URL with the
synthetic SC video-id sentinel as the watch param:

    user pasted:
      https://soundcloud.com/stevecomposer/monty-dons-italian-gardens-pre-title-theme-music

    motif persisted:
      https://www.youtube.com/watch?v=sc-stevecomposer-monty-dons-italian-gardens-pre-title-theme-music

Download then immediately failed:

    ERROR: [youtube] sc-stevecom: Video unavailable

Root cause: the manual-url + override-url endpoints had this
unconditional canonicalization shape:

    vid = extract_video_id(url)              # 'sc-<artist>-<slug>'
    canonical_url = f"https://www.youtube.com/watch?v={vid}"

extract_video_id() returns the synthetic `sc-<artist>-<slug>`
sentinel for SoundCloud URLs (the v1.14.0 spike's design — the
sentinel goes into local_files.source_video_id so the SRC=U
letter logic can detect "user-provided audio"). The canonical-
URL reconstruction was YouTube-only legacy from before SC
support; nothing in v1.14.0-8 updated it.

v1.14.9 fix: route canonicalization by source. YouTube URLs
keep the existing watch?v=VID normalization (strips tracking
params, normalizes youtu.be / shorts). SoundCloud URLs store
as-is — yt-dlp's SC extractor reads the URL not an ID, and the
URL is already in canonical
`soundcloud.com/{artist}/{slug}` shape.

Tests pin the routing on both endpoints, verify the rejection
shape on unrecognized hosts, and pin that YouTube URLs still
get canonicalized (regression guard for the existing path).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Source detection imported into api.py ─────────────────────


def test_url_source_imported_in_api():
    """The fix relies on url_source() being imported alongside
    extract_video_id. Pin so a refactor doesn't drop the import
    and make the new gate reference an undefined name."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "from ..core.sync import extract_video_id, url_source" in src


# ── api_manual_url (the SET URL dialog) ───────────────────────


def test_manual_url_endpoint_routes_canonicalization_by_source():
    """The api_manual_url handler must check url_source() and
    branch: YouTube → reconstruct as watch?v=VID, SoundCloud →
    preserve the input URL verbatim."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn = src.index('async def api_manual_url(')
    body = src[fn:fn + 4000]
    # Source check + 400 on unknown.
    assert "src = url_source(url)" in body
    assert 'src == "unknown"' in body
    # v1.20.26: message widened to include Instagram.
    # v1.22.90: the detail literal wraps across two source lines.
    assert "URL must be a YouTube, SoundCloud, Instagram, " in body
    assert "or Facebook link" in body
    # YouTube branch keeps the canonicalization.
    assert 'if src == "youtube":' in body
    assert 'canonical_url = f"https://www.youtube.com/watch?v={vid}"' in body
    # SoundCloud branch preserves the input.
    assert "canonical_url = url" in body


def test_manual_url_endpoint_no_longer_unconditionally_wraps_as_youtube():
    """Regression guard: the pre-fix line

        canonical_url = f"https://www.youtube.com/watch?v={vid}"

    must not appear OUTSIDE the `if src == \"youtube\":` branch.
    A revert that flattens the gate would re-introduce the bug
    silently — SC tests on the route would still pass because
    yt-dlp would reject the wrapped URL with a YT-extractor
    error, masking the real bug.

    Verify by checking that both occurrences of the canonical_url
    assignment in the manual-url handler sit AFTER the
    `if src == "youtube":` branch."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn = src.index('async def api_manual_url(')
    body = src[fn:fn + 4000]
    # The YT branch + the SC branch must both exist.
    yt_branch = body.index('if src == "youtube":')
    sc_branch = body.index("canonical_url = url", yt_branch)
    # And there must be exactly ONE canonical_url = f"...watch?v..."
    # occurrence, sitting under the YT branch.
    yt_assign = 'canonical_url = f"https://www.youtube.com/watch?v={vid}"'
    assert body.count(yt_assign) == 1
    assert body.index(yt_assign) > yt_branch
    assert body.index(yt_assign) < sc_branch


# ── api_override (the failure-dialog SET URL) ─────────────────


def test_override_endpoint_routes_canonicalization_by_source():
    """Same fix on the api_override handler — the failure-dialog
    SET URL surface also wrapped SC URLs as YT pre-fix."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Find the override handler — it's the one at api.py:10295's
    # canonical assignment site. Anchor on the unique v1.12.47
    # marker right above its enqueue call.
    fn_anchor = src.index("v1.12.47: SET URL via the /override dialog")
    # Walk back to the function header.
    body_start = src.rindex("async def ", 0, fn_anchor)
    body = src[body_start:fn_anchor + 2000]
    assert "src = url_source(youtube_url)" in body
    assert 'src == "unknown"' in body
    # Branched canonicalization.
    assert 'if src == "youtube":' in body
    assert 'canonical = f"https://www.youtube.com/watch?v={vid}"' in body
    assert "canonical = youtube_url" in body


def test_override_endpoint_no_unconditional_yt_wrap():
    """Mirror of the manual-url regression guard — pin the YT
    canonical assignment in the override handler sits behind
    the source gate."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("v1.12.47: SET URL via the /override dialog")
    body_start = src.rindex("async def ", 0, fn_anchor)
    body = src[body_start:fn_anchor + 2000]
    yt_branch = body.index('if src == "youtube":')
    sc_assign = body.index("canonical = youtube_url", yt_branch)
    yt_assign = 'canonical = f"https://www.youtube.com/watch?v={vid}"'
    assert body.count(yt_assign) == 1
    assert body.index(yt_assign) > yt_branch
    assert body.index(yt_assign) < sc_assign


# ── Behavior of url_source / extract_video_id on the user's URL ─


def test_users_repro_url_classifies_as_soundcloud():
    """The exact URL the user pasted must round-trip through the
    helpers as (soundcloud, sc-stevecomposer-...). If this test
    starts failing the v1.14.0 regex regressed."""
    from app.core.sync import url_source, extract_video_id
    url = ("https://soundcloud.com/stevecomposer/"
           "monty-dons-italian-gardens-pre-title-theme-music")
    assert url_source(url) == "soundcloud"
    assert extract_video_id(url) == (
        "sc-stevecomposer-monty-dons-italian-gardens-"
        "pre-title-theme-music"
    )


def test_users_repro_url_would_be_preserved_by_v1_14_9_logic():
    """End-to-end logic check (not hitting the endpoint): given
    the SC URL the user pasted, the v1.14.9 branch picks the
    'preserve-as-is' path and the URL persisted to user_overrides
    matches what was pasted."""
    from app.core.sync import url_source, extract_video_id
    url = ("https://soundcloud.com/stevecomposer/"
           "monty-dons-italian-gardens-pre-title-theme-music")
    src = url_source(url)
    vid = extract_video_id(url)
    assert vid is not None
    # Apply the v1.14.9 routing.
    if src == "youtube":
        canonical = f"https://www.youtube.com/watch?v={vid}"
    else:
        canonical = url
    assert canonical == url, (
        "v1.14.9: SC URLs must be persisted verbatim, not wrapped "
        "as a YT watch URL with the synthetic ID as the watch param"
    )


def test_youtube_url_still_gets_canonicalized():
    """Regression guard for the YT path — a youtu.be short URL
    should still normalize to the watch?v= shape."""
    from app.core.sync import url_source, extract_video_id
    url = "https://youtu.be/abcDEF12345"
    src = url_source(url)
    vid = extract_video_id(url)
    if src == "youtube":
        canonical = f"https://www.youtube.com/watch?v={vid}"
    else:
        canonical = url
    assert canonical == "https://www.youtube.com/watch?v=abcDEF12345"


def test_unknown_host_url_would_400():
    """A URL on a host we don't support — vimeo, bandcamp,
    example.com — must take the 400 branch via url_source ==
    'unknown'."""
    from app.core.sync import url_source
    for url in (
        "https://vimeo.com/12345",
        "https://bandcamp.com/track/foo",
        "https://example.com/file.mp3",
    ):
        assert url_source(url) == "unknown", (
            f"v1.14.9: {url} must classify as 'unknown' so the "
            f"endpoints reject it with 400 rather than wrapping it"
        )
