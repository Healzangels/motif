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
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


# ── 1. touch hover-stick suppression ────────────────────────────────────


def _hover_is_gated(sel: str) -> bool:
    """True if `sel { ... }` (a rule at line start) is immediately preceded by
    `@media (hover: hover) {` — i.e. it only applies on hover-capable pointers,
    so a touch device (hover:none) gets no hover and nothing latches."""
    import re
    m = re.search(r"(?m)^[ \t]*" + re.escape(sel) + r"[ \t]*\{", CSS)
    if not m:
        return False
    before = CSS[:m.start()].rstrip()
    return before.endswith("@media (hover: hover) {")


def test_control_hovers_are_gated_not_reset():
    """v0.50.93: the leaky @media(hover:none) reset was replaced by gating the
    control-primitive :hover rules under @media(hover:hover). On touch they
    simply don't apply → base + variant styling shows, nothing latches."""
    assert "@media (hover: none)" not in CSS, (
        "the leaky hover:none reset block must be gone"
    )
    for sel in (".btn:hover", ".chip:hover", ".tab:hover", ".dlg-close:hover",
                ".title-glyph:hover", ".tdb-pill-btn:hover",
                ".attn-pill-btn:hover", ".pill-filter-clear:hover",
                ".library-clear-all-btn:hover",
                ".source-pie-restore-chip:hover",
                ".lib-flag-pill:hover:not(:disabled)"):
        assert _hover_is_gated(sel), (
            f"{sel} must be gated under @media (hover: hover) so it doesn't "
            f"latch on touch"
        )


def test_gating_preserves_variant_styling():
    """The regression the reset caused: it stripped the tinted variant bg /
    state glow. Gating fixes this by construction — the pill/glyph :hover rules
    change ONLY filter (base variant bg/glow untouched), and the gate means
    touch never applies even that."""
    import re
    # the tinted variant classes still carry their base background (not gated,
    # not stripped) — they're plain rules, not :hover.
    assert re.search(r"\.tdb-pill-yes\s*\{[^}]*background", CSS)
    assert re.search(r"\.attn-pill-fail\s*\{[^}]*background", CSS)
    # the pill hover only brightens (gated); it never sets background:transparent.
    m = re.search(r"(?m)^[ \t]*\.tdb-pill-btn:hover[ \t]*\{([^}]*)\}", CSS)
    assert m and "filter" in m.group(1) and "background" not in m.group(1)


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


# ── 3. CRT power-on shutters slide apart with glowing edges (v0.51.10) ───


def test_power_on_shutters_slide_apart_with_glowing_edges():
    # v0.51.10: two black shutters (::before top / ::after bottom) SLIDE apart
    # (translateY) on an ease-out so the tube springs open, and each carries a green
    # phosphor bloom on its reveal edge (box-shadow) so a glowing scanline sweeps
    # outward WITH the picture. Replaces the v0.50.98 scaleY unfold — its ease-IN
    # start crawled open (the user: reveal "off and clunky") and the bars retracted
    # as bare edges; scaleY would also squish the edge box-shadow, so translateY.
    assert ".crt-power-on::before" in CSS and ".crt-power-on::after" in CSS
    assert "@keyframes crt-power-on-shutter-top" in CSS
    assert "@keyframes crt-power-on-shutter-bot" in CSS
    top = CSS[CSS.index("@keyframes crt-power-on-shutter-top"):]
    top = top[:top.index("}", top.index("100%"))]
    assert "translateY(0)" in top and "translateY(-100%)" in top  # closed → slid off top pole
    bot = CSS[CSS.index("@keyframes crt-power-on-shutter-bot"):]
    bot = bot[:bot.index("}", bot.index("100%"))]
    assert "translateY(100%)" in bot  # slid off the bottom pole
    # the old scaleY unfold + transform-origin poles are gone.
    assert "@keyframes crt-power-on-unfold" not in CSS
    assert "transform-origin: center top" not in CSS
    # each shutter carries a phosphor glow on its reveal edge (green bloom + fg core).
    before = CSS[CSS.index(".crt-power-on::before {"):]
    before = before[:before.index("}")]
    assert "box-shadow" in before and "green-rgb" in before


def test_power_on_has_a_bright_fold_line_child():
    # the fold-line is a SEPARATE child element (.crt-on-line) so its flash + bloom
    # doesn't ride the container opacity (which would wash out the black shutters).
    assert "crt-on-line" in BASE_HTML, "base.html renders the fold-line child span"
    assert ".crt-on-line {" in CSS
    j = CSS.index(".crt-on-line {")
    line = CSS[j:CSS.index("}", j)]
    assert "var(--fg)" in line and "green-bright" in line  # scanline + glow
    # the v0.50.91 inverse-of-power-off is gone: no crt-power-on-bg veil, no scaleX.
    assert "@keyframes crt-power-on-bg" not in CSS
    on_line = CSS[CSS.index("@keyframes crt-power-on-line"):]
    on_line = on_line[:on_line.index("}", on_line.index("100%"))]
    assert "scaleX" not in on_line


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
    # v0.50.92 (code review): anchor on the real <div>, not the bare class
    # string — setup.html's Jinja {# #} comment also mentions "auth-card-inner",
    # and matching that let a regressed template (foot before the real wrapper)
    # pass. index the opening tag itself.
    inner_open = SETUP_HTML.index('<div class="auth-card-inner"')
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
