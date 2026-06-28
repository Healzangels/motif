"""v1.15.129 — SoundCloud thumbnails in info card + URL preview.

the user on v1.15.126:

> Wondering for the soundcloud link if its possible to display
> the thumbnail in the info card same as the youtube url. Also
> would be nice to add this to box when providing a url and
> pasting a soundcloud url it shows a preview of the thumbnail
> similar to how we treat youtube links

## Pre-fix

YouTube URLs got both surfaces:
  - Info card: a bottom thumbnail block with
    img.youtube.com/vi/{vid}/hqdefault.jpg + click-to-watch link
  - SET URL dialog: live preview as the user types, with the
    same img.youtube.com thumbnail + oembed-fetched title

SoundCloud URLs got neither — the info card skipped the
thumbnail entirely (the H3 v1.14.20 fix correctly guarded
against rendering a 404'd img.youtube.com URL for SC overrides,
but never added an SC-specific replacement), and the SET URL
preview hid itself when `extractYouTubeVideoId()` returned null.

## Fix

Both surfaces now branch by `urlSource()`:

**Info card thumbnail block** — an IIFE renders the block with
the right source-specific shape:
  - YouTube: `<img src="https://img.youtube.com/vi/{vid}/hqdefault.jpg">`
    — synchronous, deterministic
  - SoundCloud: `<img data-sc-oembed-url="{url}">` inside a
    `[data-sc-thumbnail-wrap hidden]` wrapper that
    `hydrateSourceThumbnails` (new) post-paint hydrates from
    /api/source/oembed's thumbnail_url field. Failures keep the
    wrapper hidden so the user doesn't see a broken-image icon.

**SET URL dialog preview** — `updatePreview()` refactored:
  - Detects source via `urlSource(raw)`; bails to hidden preview
    on 'unknown'
  - Builds a `key = src + ':' + (vid_or_url)` for the race-guard
    instead of vid-only
  - YouTube: thumbnail set synchronously from vid + oembed for
    title only
  - SoundCloud: thumbnail set asynchronously from the same
    oembed call that fetches the title (one round-trip for both)

## Tests
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_info_card_thumbnail_branches_by_source():
    """The thumbnail-block IIFE must branch on `urlSource(ytUrl)`
    so SoundCloud and YouTube each get the right shape."""
    js = APP_JS.read_text()
    assert "tUrlSrc = urlSource(ytUrl)" in js
    assert "tUrlSrc === 'youtube'" in js
    assert "tUrlSrc === 'soundcloud'" in js


def test_soundcloud_thumbnail_uses_hydrator_pattern():
    """The SC branch must emit `<img data-sc-oembed-url="...">`
    inside a `[data-sc-thumbnail-wrap hidden]` wrapper so the
    async hydrator can fill src + reveal the wrapper without
    flashing a broken-image icon during the round-trip."""
    js = APP_JS.read_text()
    assert "data-sc-thumbnail-wrap" in js
    assert "data-sc-oembed-url=" in js
    # Wrapper starts hidden (revealed only on successful oembed).
    sc_branch = js[js.index("data-sc-thumbnail-wrap"):]
    assert "hidden>" in sc_branch[:300]


def test_hydrate_source_thumbnails_function_exists():
    """`hydrateSourceThumbnails(root)` runs post-paint, scans
    `img[data-sc-oembed-url]`, fetches /api/source/oembed, and
    fills img.src from data.thumbnail_url."""
    js = APP_JS.read_text()
    assert "async function hydrateSourceThumbnails(root)" in js
    # Must scan the right attribute selector.
    fn_idx = js.index("async function hydrateSourceThumbnails(root)")
    fn_body = js[fn_idx:fn_idx + 1500]
    assert "img[data-sc-oembed-url]" in fn_body
    # Must hit the source-aware oembed endpoint.
    assert "/api/source/oembed?url=" in fn_body
    # Must read thumbnail_url specifically.
    assert "thumbnail_url" in fn_body
    # Must reveal the wrap on success.
    assert "[data-sc-thumbnail-wrap]" in fn_body


def test_info_card_calls_hydrate_source_thumbnails():
    """The hydrator must be wired into the info-card render —
    called after innerHTML alongside `hydrateDiffTitles`."""
    js = APP_JS.read_text()
    # hydrateDiffTitles + hydrateSourceThumbnails should both
    # appear within the same render block (~adjacent calls).
    diff_idx = js.index("hydrateDiffTitles(body);")
    window = js[diff_idx:diff_idx + 500]
    assert "hydrateSourceThumbnails(body)" in window


def test_manual_url_preview_branches_by_source():
    """`updatePreview()` is source-aware now: the early bail on
    `extractYouTubeVideoId` returning null is gone; instead the
    function checks `urlSource(raw)` and dispatches per source."""
    js = APP_JS.read_text()
    # The new key-based race guard replaces previewLastVid.
    assert "previewLastKey" in js
    # Function must reference both source types explicitly.
    fn_idx = js.index("async function updatePreview()")
    # v1.15.140 widened: inline narrative comments (the v1.15.140
    # !preview.hidden rationale) push the SC branch past the
    # pre-fix 4000-char window. Bumped to 5000 to keep the contract.
    fn_body = js[fn_idx:fn_idx + 5000]
    assert "src === 'youtube'" in fn_body
    assert "src === 'soundcloud'" in fn_body
    # SC branch must fill thumbnail from oembed.thumbnail_url
    # (not from img.youtube.com).
    assert "data.thumbnail_url" in fn_body or "data?.thumbnail_url" in fn_body


def test_manual_url_preview_avoids_broken_image_icon():
    """When the SC oembed round-trip is in flight, the preview
    thumb's src starts blank + visibility hidden so the user
    doesn't see a flash of broken-image icon before the
    thumbnail_url lands."""
    js = APP_JS.read_text()
    fn_idx = js.index("async function updatePreview()")
    # v1.15.140 widened: inline narrative comments (the v1.15.140
    # !preview.hidden rationale) push the SC branch past the
    # pre-fix 4000-char window. Bumped to 5000 to keep the contract.
    fn_body = js[fn_idx:fn_idx + 5000]
    # The pre-fill should set src='' and visibility='hidden' for SC.
    assert "previewThumb.src = ''" in fn_body
    assert "visibility = 'hidden'" in fn_body
