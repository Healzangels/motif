"""v1.15.43 — cookies UX → biscuit brown, !M (mismatch) → orange.

the user: "can we also make the yellow tdb pillow and yellow lock a
brown hue that matches our theme for cookies the yellow is still
too similar to the plex hue. also does !M make sense being the
plex hue given what it does or does a another color make more
sense?"

Two related palette shifts driven by hue-clustering complaints:

1. **Cookies → biscuit brown.** v1.15.17 introduced `--yellow`
   (#ffe066) to separate the cookies-needed UX from `--amber`
   (the Plex/placement tone). It didn't go far enough — yellow
   and amber sit close in hue space, so the cookies pill still
   read as Plex-adjacent. This tag drops `--yellow` entirely in
   favor of `--brown` (#c08552, biscuit), shifting hue AND
   dropping luminance so the three cookies surfaces
   (`.tdb-pill-cookies` row pill, `.attn-pill-cookies` filter
   chip, `.op-tone-cookies` topbar chip + `.btn-cookies` action
   button) read as their own family.

2. **!M (mismatch) → orange.** Mismatch is content-layer
   divergence (canonical bytes ≠ placement bytes, usually from
   SET URL / UPLOAD MP3 over an existing placement). Pre-fix
   `.attn-pill-mismatch` + `.state-pill.mismatch` shared --amber
   with the Plex-side attention pills (!P await, ↺ broken), which
   misled the operator into thinking !M was a Plex action. Orange
   (`--orange: #ff7a3a`, already defined for the HL link-pill)
   sits between amber (warning) and red (error) — exactly where
   mismatch belongs semantically. `.kind-content_mismatch` on the
   /scans page moves too so every mismatch surface agrees.

The LINK-column `.link-glyph-mismatch` stays red (already different)
— it's a column away and signals "this row is actively in a broken
placement state," which is a heavier semantic notch than the !M
filter chip.

Static-text guards (consistent with v1.15.17/41/42 patterns).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"


# ── 1. --brown token replaces --yellow ───────────────────────


def test_lemon_token_defined_with_lemon_gold_value():
    """v1.15.121 superseded v1.15.43's --brown with --lemon. the user:
    "I don't like the brown tdb cookies pill or brown lock filter;
    I think yellow made more sense and looked better but its looks
    so similar to the plex color." The new lemon gold has lower R
    + higher G + lower B than v1.15.17's #ffe066 yellow, so it
    reads as yellow without clustering with amber.

    Pin the new value so a future "tweak" doesn't drift back
    toward either amber-adjacent yellow or muddy brown."""
    css = APP_CSS.read_text()
    assert '--lemon: #f5dd2b;' in css, (
        "v1.15.121: --lemon must be #f5dd2b (clean lemon gold). "
        "Distinct from --amber #ffb84a (Plex tone) AND from "
        "v1.15.17's #ffe066 yellow (which was too close to amber)."
    )
    assert '--lemon-bright: #f8e664;' in css, (
        "v1.15.121: bright variant for hover/active states"
    )


def test_yellow_token_removed():
    """`--yellow` and `--yellow-bright` from v1.15.17 must be
    removed (not just unused) — keeping them invites a future
    edit to reach for the wrong token. Per CLAUDE.md: no
    backwards-compat shims for removed features."""
    css = APP_CSS.read_text()
    # The :root declaration block specifically.
    root_anchor = css.index(':root {')
    root_close = css.index('}', root_anchor)
    root_block = css[root_anchor:root_close]
    assert '--yellow:' not in root_block, (
        "v1.15.43: --yellow token must be removed from :root — "
        "rename consumers to --brown, don't leave the old token "
        "as a tempting shortcut for future edits"
    )
    assert '--yellow-bright:' not in root_block


def test_cookies_surfaces_use_lemon_not_brown_not_yellow():
    """The three live cookies surfaces (.btn-cookies, .tdb-pill-cookies,
    --tone-cookies) must all reference `--lemon` (v1.15.121+) so they read
    as one family (v0.51.79: .attn-pill-cookies dropped — dead since the
    v1.19.67 STATUS-chip removal). Any leftover
    --brown or --yellow consumer becomes an undefined-var fallback
    (silently no-op'd by the browser) and breaks the unified
    appearance."""
    css = APP_CSS.read_text()
    ops_css = OPS_CSS.read_text()
    # .btn-cookies
    btn_anchor = css.index('.btn-cookies {')
    btn_block = css[btn_anchor:btn_anchor + 200]
    assert 'var(--lemon)' in btn_block, (
        "v1.15.121: .btn-cookies must use --lemon (cookies action "
        "button)"
    )
    # .tdb-pill-cookies
    tdb_anchor = css.index('.tdb-pill-cookies {')
    tdb_block = css[tdb_anchor:tdb_anchor + 200]
    assert 'var(--lemon)' in tdb_block, (
        "v1.15.121: .tdb-pill-cookies must use --lemon (row pill)"
    )
    # v0.51.79 (audit): .attn-pill-cookies removed — the STATUS filter chip it
    # styled was dropped in v1.19.67 (dead CSS, zero live emitters).
    # --tone-cookies (ops.css topbar tone)
    assert '--tone-cookies: var(--lemon);' in ops_css, (
        "v1.15.121: --tone-cookies must alias --lemon so the topbar "
        "COOKIES chip + the rest of the cookies surfaces agree"
    )


def test_no_lingering_yellow_var_refs_in_css():
    """No `var(--yellow)` or `var(--yellow-bright)` calls anywhere
    in the CSS files — those would silently fall back to the
    UA default + break the unified cookies tone. Mirror-principle
    guard against missed renames."""
    for path in (APP_CSS, OPS_CSS):
        src = path.read_text()
        assert 'var(--yellow)' not in src, (
            f"v1.15.43: {path.name} still calls var(--yellow) — "
            "rename to var(--brown) so the cookies tone stays unified"
        )
        assert 'var(--yellow-bright)' not in src


# ── 2. Mismatch surfaces use --orange, not --amber ───────────


def _rule_body(css: str, selector: str) -> str:
    """Slice the CSS body of `selector { ... }` so neighbor rules
    don't pollute substring checks. Anchors on `selector {` (with
    the brace) so a stray mention in a comment elsewhere in the
    file can't mis-anchor — the v1.15.43 author hit exactly that
    when a marker comment cross-referenced .attn-pill-mismatch
    by name."""
    anchor = css.index(selector + ' {')
    open_brace = css.index('{', anchor)
    close_brace = css.index('}', open_brace)
    return css[open_brace:close_brace]


def test_attn_pill_mismatch_uses_orange():
    """The !M filter chip must reference `--orange`. Pre-v1.15.43
    used `--amber` which clustered it with the Plex-side attention
    pills (await/broken) and made !M read as a Plex action when
    it's actually content-layer divergence."""
    block = _rule_body(APP_CSS.read_text(), '.attn-pill-mismatch')
    assert 'var(--orange)' in block, (
        "v1.15.43: .attn-pill-mismatch must use --orange (between "
        "amber warning and red error in hue space — semantic notch "
        "for content-layer divergence)"
    )
    assert 'var(--amber)' not in block, (
        "v1.15.43: leftover --amber in .attn-pill-mismatch — "
        "the user's complaint: '!M reading as plex hue'"
    )


# v0.51.138: test_state_pill_mismatch_uses_orange removed — `.state-pill.mismatch`
# was deleted as dead CSS (CSS-audit T5). v1.12.81 retired 'mismatch' as a DL/PL dot
# state, so renderLibraryRow's dot suffix never computes it (only on/broken/pushed/
# await/'') and the rule styled nothing. The ACTUAL live mismatch surfaces are the
# LINK-column M glyph (.link-glyph-mismatch — test_link_glyph_mismatch_stays_red
# below) + the !M filter chip (.attn-pill-mismatch — test_attn_pill_mismatch_uses_orange
# above); both stay guarded. Removal is pinned by test_v0_51_138.

# v0.51.70: test_scans_kind_content_mismatch_uses_orange removed — the /scans page's
# .kind-content_mismatch badge (and the whole dead Scans-page CSS block) was deleted as
# dead; the /scans cross-page-consistency check no longer has a surface.


def test_link_glyph_mismatch_stays_red():
    """Counter-guard: the LINK-column M pill (`.link-glyph-mismatch`)
    must STAY red. It's a different surface — the row-level "this
    is actively broken at the placement layer" indicator, a heavier
    semantic notch than the !M filter chip. Moving it to orange
    would dilute the "red = blocked" signal in the LINK column."""
    css = APP_CSS.read_text()
    anchor = css.index('.link-glyph-mismatch {')
    block = css[anchor:anchor + 400]
    assert 'var(--red)' in block, (
        "v1.15.43: .link-glyph-mismatch must STAY red — it's the "
        "LINK column 'broken placement' indicator, not the !M "
        "filter chip. Different surface, different semantic weight."
    )
