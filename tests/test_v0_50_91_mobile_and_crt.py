"""v0.50.91 — mobile hover-stick + login clip fixes + CRT turn-on rebuild.

1. Touch devices latch :hover after a tap, so buttons stayed highlighted like
   they were selected (the user). A @media (hover: none) block suppresses the
   stuck fill/lift/glow/brighten; real selected state uses .*-active classes.
2. On narrow screens the round login card's stacked fields clipped through the
   ::after ring. A small-viewport oval + trimmed inner safe-zone keeps the ring
   a continuous loop with the form in its fat middle.
3. The CRT power-ON was rebuilt as the exact inverse of the power-OFF (scanline
   blooms from a centre point + the veil lifts) instead of the green flash.
4. The first-run SETUP form (lede + 3 fields + hint + forward-auth foot) never
   fit the round vinyl card: it lacked login's .auth-card-inner column wrapper,
   so head/lede/form/foot laid out as flex-ROW columns and the foot spilled off
   the right edge. It now wraps in .auth-card-inner and the card degrades to the
   rounded-rect .auth-card-setup "sleeve" that sizes to content — no clip at any
   width. Login stays a circle (scoped to .auth-card-setup).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
SETUP_HTML = (REPO / "app" / "web" / "templates" / "setup.html").read_text()
LOGIN_HTML = (REPO / "app" / "web" / "templates" / "login.html").read_text()


# ── 1. touch hover-stick suppression ────────────────────────────────────


def test_hover_none_block_exists_and_covers_key_controls():
    assert "@media (hover: none)" in CSS
    i = CSS.index("@media (hover: none)")
    # capture the whole media block (brace-count)
    depth = 0
    end = i
    for j in range(CSS.index("{", i), len(CSS)):
        if CSS[j] == "{":
            depth += 1
        elif CSS[j] == "}":
            depth -= 1
            if depth == 0:
                end = j
                break
    block = CSS[i:end + 1]
    for sel in (".btn:hover", ".chip:hover", ".library-clear-all-btn:hover",
                ".pill-filter-clear:hover", ".tab:hover"):
        assert sel in block, f"hover-none block must neutralize {sel}"


def test_hover_none_does_not_zero_transform_globally():
    """.library-search-clear uses a base transform for centering — the shared
    reset must NOT set transform:none on it (only .chip, which has no base
    transform, gets transform:none)."""
    i = CSS.index("@media (hover: none)")
    depth = 0
    end = i
    for j in range(CSS.index("{", i), len(CSS)):
        if CSS[j] == "{":
            depth += 1
        elif CSS[j] == "}":
            depth -= 1
            if depth == 0:
                end = j
                break
    block = CSS[i:end + 1]
    # transform:none appears only on the .chip rule, never in the big shared
    # selector group (which includes .library-search-clear).
    assert "transform: none" in block  # for .chip
    # the shared group's DECLARATION body (the {...} right after the big
    # selector list, anchored on its last selector) must not carry transform.
    anchor = block.index(".source-pie-restore-chip:hover")
    decl_open = block.index("{", anchor)
    shared_decl = block[decl_open:block.index("}", decl_open)]
    assert "transform" not in shared_decl, (
        "the shared hover-none group must not reset transform (would break "
        ".library-search-clear's base translateY centering)"
    )


# ── 2. login card no longer clips on small screens ──────────────────────


def test_auth_card_small_viewport_oval_and_trim():
    assert "@media (max-width: 560px)" in CSS
    i = CSS.index("@media (max-width: 560px)")
    block = CSS[i:i + 260]
    assert ".auth-card" in block and "min-height" in block
    assert "max-width: 64%" in block


# ── 2b. op-mini job strip becomes a full-width bar on mobile ────────────


def test_op_mini_mobile_full_width_strip():
    i = CSS.index("@media (max-width: 600px)")
    # capture the whole media block
    depth = 0
    end = i
    for j in range(CSS.index("{", i), len(CSS)):
        if CSS[j] == "{":
            depth += 1
        elif CSS[j] == "}":
            depth -= 1
            if depth == 0:
                end = j
                break
    block = CSS[i:end + 1]
    # the strip is pinned to the bottom of the topbar, full width, only while live
    assert ".topbar:has(#op-mini:not([hidden]))" in block
    assert "#op-mini:not([hidden])" in block
    mini = block[block.index("#op-mini:not([hidden]) {"):]
    mini = mini[:mini.index("}")]
    assert "position: absolute" in mini
    assert "bottom: 0" in mini
    assert "left: 0" in mini and "right: 0" in mini


# ── 3. CRT power-on is the inverse of power-off ─────────────────────────


def test_power_on_uses_scanline_and_veil_like_power_off():
    # the scanline pseudo-element + its bloom keyframes
    assert ".crt-power-on::before" in CSS
    assert "@keyframes crt-power-on-line" in CSS
    assert "@keyframes crt-power-on-bg" in CSS
    line = CSS[CSS.index("@keyframes crt-power-on-line"):]
    line = line[:line.index("}", line.index("100%"))]
    # blooms from a collapsed centre point outward (the inverse of the collapse)
    assert "scaleX(0)" in line
    assert "scaleX(1)" in line
    # shares the power-off's --fg scanline + green-bright glow
    j = CSS.index(".crt-power-on::before")
    before = CSS[j:CSS.index("}", j)]
    assert "var(--fg)" in before
    assert "green-bright" in before


def test_power_on_no_longer_a_green_flash():
    i = CSS.index(".crt-power-on {")
    block = CSS[i:CSS.index("}", i)]
    # the veil is the --bg black tube, not the old rgba(green) flash
    assert "var(--bg)" in block
    assert "green-rgb), 0.85" not in block


def test_power_on_still_inert_and_one_shot():
    i = CSS.index(".crt-power-on {")
    block = CSS[i:CSS.index("}", i)]
    assert "opacity: 0" in block and "pointer-events: none" in block
    j = CSS.index(".crt-power-on.playing {")
    pblock = CSS[j:CSS.index("}", j)]
    assert "infinite" not in pblock and "forwards" not in pblock


# ── 4. first-run setup form no longer overflows the round card ───────────


def test_setup_wraps_content_in_inner_column_like_login():
    """The row-flex overflow root cause: setup.html had no .auth-card-inner
    column wrapper, so its children laid out side-by-side. It must now mirror
    login.html and wrap everything (incl. the foot) in one .auth-card-inner."""
    assert "auth-card-inner" in LOGIN_HTML  # the sibling it mirrors
    assert "auth-card-inner" in SETUP_HTML
    # the forward-auth foot must live INSIDE the inner wrapper, not as a
    # bare flex-row sibling of the form (which is what spilled off-screen).
    inner_open = SETUP_HTML.index("auth-card-inner")
    section_close = SETUP_HTML.index("</section>")
    foot = SETUP_HTML.index("auth-foot")
    inner_close = SETUP_HTML.rindex("</div>", inner_open, section_close)
    assert inner_open < foot < inner_close, "auth-foot must be inside .auth-card-inner"


def test_setup_card_degrades_to_rounded_rect_scoped_to_setup():
    """The setup variant becomes a content-sized rounded-rect so the taller
    form never clips against the circle's poles — scoped so login stays round."""
    assert "auth-card-setup" in SETUP_HTML  # the modifier is applied
    i = CSS.index(".auth-card-setup {")
    block = CSS[i:CSS.index("}", i)]
    # not a forced circle: radius token + no circle diameter min-height
    assert "border-radius: var(--radius)" in block
    assert "min-height: 0" in block
    # the ring pseudo-element follows the rect (not border-radius:50%)
    assert ".auth-card-setup::after" in CSS
    ring = CSS[CSS.index(".auth-card-setup::after"):]
    ring = ring[:ring.index("}")]
    assert "border-radius: var(--radius)" in ring and "50%" not in ring
    # the inner is un-clamped (the circle's 70% safe-zone doesn't apply to a rect)
    inner = CSS[CSS.index(".auth-card-setup .auth-card-inner"):]
    inner = inner[:inner.index("}")]
    assert "max-width: none" in inner


def test_login_card_untouched_still_a_circle():
    """The scoped fix must not regress the round login label."""
    assert "auth-card-setup" not in LOGIN_HTML
    i = CSS.index(".auth-card {")
    block = CSS[i:CSS.index("}", i)]
    assert "border-radius: 50%" in block
