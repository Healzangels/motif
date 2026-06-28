"""v1.15.144 — info-card YT + SC thumbnails share dimensions.

the user on v1.15.143:

> took a look at the soundcloud thumbnail in the info card is
> too large or large(r) than the youtube version can we make
> those match if they aren't already.

## Root cause — `app/web/static/app.js`

Both thumbnail branches in `renderInfoCard` (~line 11360) used
the same inline `width:100%; max-width:480px` style but no
height constraint. Source images have different natural
aspect ratios:

  - YouTube `hqdefault.jpg` is 4:3 (480×360) → 360px tall at
    480px wide
  - SoundCloud oembed `thumbnail_url` is typically square 1:1
    → 480px tall at the same width

That's a 120px height difference for the same width — the SC
thumbnail visibly overshoots the YT one.

## Fix

New CSS primitive in `app.css`:

    .info-source-thumb {
      width: 100%;
      max-width: 480px;
      aspect-ratio: 4 / 3;
      object-fit: cover;
      display: block;
      margin: 0 auto;
      border: 1px solid var(--line);
    }

`aspect-ratio: 4 / 3` forces the box to YouTube's native
hqdefault shape — no crop on YT, center-crop on SC's 1:1
artwork. `object-fit: cover` fills the box rather than
letterboxing with black bars (cleaner for square album art
which is composed center-out anyway).

Both thumbnails now render at identical 480×360 dimensions.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _strip_css_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


# ── primitive defined ─────────────────────────────────────────────

def test_info_source_thumb_classes_defined_in_css():
    """v1.16.1: the aspect constraint moved from <img> to the
    wrapper <div>. Two classes now carry the contract together:
      - .info-source-thumb-wrap (the box: aspect-ratio + max-width)
      - .info-source-thumb (the img: object-fit:cover, 100% fill)
    Reliable on all browsers — aspect-ratio on non-replaced
    elements is well-supported and free of the replaced-element
    intrinsic-dimension quirks that broke v1.15.144's pure-img
    approach."""
    src = _strip_css_comments(APP_CSS.read_text())
    # Wrapper rule.
    wrap_idx = src.index(".info-source-thumb-wrap {")
    wrap_block = src[wrap_idx:wrap_idx + 500]
    assert "width: 100%" in wrap_block
    assert "max-width: 480px" in wrap_block
    assert "aspect-ratio: 4 / 3" in wrap_block, (
        "v1.16.1: aspect-ratio MUST live on the wrapper "
        "(non-replaced element). v1.15.144 put it on the <img>; "
        "browser behavior with intrinsic dimensions was "
        "inconsistent and SC still rendered taller than YT."
    )
    assert "overflow: hidden" in wrap_block, (
        "v1.16.1: wrapper must clip so the image's object-fit "
        "cover doesn't bleed out of the 4:3 box."
    )
    # Img rule.
    img_idx = src.index(".info-source-thumb {")
    img_block = src[img_idx:img_idx + 300]
    assert "width: 100%" in img_block
    assert "height: 100%" in img_block, (
        "v1.16.1: img must fill the wrapper's height (100%) so "
        "object-fit has a constrained box to cover."
    )
    assert "object-fit: cover" in img_block


# ── both thumbnails use the class ─────────────────────────────────

def _info_card_thumb_block() -> str:
    """Slice out the renderInfoCard IIFE that produces the
    source-thumbnail markup. Anchored on the v1.15.129 marker."""
    js = APP_JS.read_text()
    start = js.index("// v1.15.129: source-aware thumbnail block")
    # Walk forward to the closing `})()` of the IIFE.
    end_marker = "return '';\n      })()"
    end = js.index(end_marker, start)
    return js[start:end + len(end_marker)]


def test_youtube_thumbnail_uses_wrapper_div_pattern():
    """v1.16.1: YT branch emits .info-source-thumb-wrap > img.
    Both the wrapper class AND the inner class must be present
    AND the wrapper must come BEFORE the img in source order."""
    block = _info_card_thumb_block()
    # Find a wrapper div followed by an img with the YT src.
    yt_pattern = re.compile(
        r'<div\s+class="info-source-thumb-wrap">\s*'
        r'<img[^>]*\bclass="info-source-thumb"[^>]*'
        r'src="https://img\.youtube\.com/vi/',
        re.DOTALL,
    )
    assert yt_pattern.search(block), (
        "v1.16.1: YouTube thumbnail must be wrapped in "
        ".info-source-thumb-wrap so the 4:3 aspect-ratio "
        "applies to a non-replaced element (reliable across "
        "browsers, unlike aspect-ratio directly on <img>)."
    )


def test_soundcloud_thumbnail_uses_wrapper_div_pattern():
    """v1.16.1: SC branch also uses the wrapper-div pattern. The
    wrapper is INSIDE the data-sc-thumbnail-wrap outer container
    (which the hydrator un-hides after oembed lands) but OUTSIDE
    the <img> that the hydrator fills with thumbnail_url."""
    block = _info_card_thumb_block()
    sc_pattern = re.compile(
        r'<div\s+class="info-source-thumb-wrap">\s*'
        r'<img[^>]*\bclass="info-source-thumb"[^>]*'
        r'data-sc-oembed-url=',
        re.DOTALL,
    )
    assert sc_pattern.search(block), (
        "v1.16.1: SoundCloud thumbnail must use the same "
        "wrapper-div pattern as YT. Without the wrapper, the "
        "1:1 oembed thumbnail's intrinsic dimensions defeat any "
        "aspect-ratio applied to the img directly."
    )


# ── old inline width style is gone ────────────────────────────────

def test_no_residual_inline_width_max_width_on_thumb_imgs():
    """The pre-fix inline `width:100%;max-width:480px;…;border:…`
    inline style on the <img>s must be gone — otherwise the
    contract is split between CSS + inline and a future tweak to
    the class won't take effect."""
    block = _info_card_thumb_block()
    # Per-img style attribute (with the load-bearing width/
    # border combo) must NOT be on either <img>. We check the
    # full pre-fix pattern.
    forbidden = re.compile(
        r"<img[^>]*\bstyle\s*=\s*\"[^\"]*max-width:\s*480px"
    )
    assert not forbidden.search(block), (
        "v1.15.144: an <img> in the source-thumbnail block "
        "still carries an inline style with max-width:480px — "
        "this duplicates the .info-source-thumb class contract "
        "and risks override drift. Move all width/border styling "
        "into the class."
    )


# ── alt text + lazy loading preserved ─────────────────────────────

def test_alt_and_lazy_loading_preserved():
    """The accessibility (alt) + perf (loading=lazy) attrs must
    survive the class migration. Match the actual <img> tags
    via regex so we don't latch onto the v1.15.129 narrative
    comment block above."""
    block = _info_card_thumb_block()
    yt_img = re.search(
        r'<img\b[^>]*src="https://img\.youtube\.com/vi/[^"]*"[^>]*/?>',
        block, re.DOTALL,
    )
    assert yt_img, "YouTube <img> tag not found"
    assert 'alt="YouTube thumbnail"' in yt_img.group(0)
    assert 'loading="lazy"' in yt_img.group(0)

    sc_img = re.search(
        r'<img\b[^>]*data-sc-oembed-url="[^"]*"[^>]*/?>',
        block, re.DOTALL,
    )
    assert sc_img, "SoundCloud <img> tag not found"
    # v1.20.26: alt is now source-aware (SoundCloud OR Instagram share
    # this oembed-hydrated branch), so it's a template literal.
    assert 'alt="${_igLabel} thumbnail"' in sc_img.group(0)
    assert 'loading="lazy"' in sc_img.group(0)


# ── hydrator still wired ──────────────────────────────────────────

def test_hydrate_source_thumbnails_still_called():
    """The async oembed hydrator (v1.15.129) must still run after
    innerHTML — without it the SC <img>'s src stays empty and
    the thumbnail never appears, class or no class."""
    js = APP_JS.read_text()
    assert "hydrateSourceThumbnails(body)" in js, (
        "v1.15.144: hydrateSourceThumbnails() call must remain "
        "in renderInfoCard — the v1.15.129 SC oembed hydration "
        "still drives the <img>'s src; the v1.15.144 class only "
        "fixes the dimensions, not the data flow."
    )
