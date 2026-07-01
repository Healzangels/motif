"""v1.23.51 — the // LEGEND toggle reads as a proper motif chip.

v1.23.51 first restyled the dedicated .library-legend-pill rule to mirror .chip.
v1.23.53 went further: the toggle now literally CARRIES the .chip class in the
markup, so it renders as an outlined chip from any cached stylesheet (a stale
app.css missing the .library-legend-pill rule had been painting it as an
unstyled native white button). The pill rule is now slim — only the caret gap
and the open (active) green accent; border/padding/transparent/dim come from
.chip.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


def _rule(selector: str) -> str:
    i = APP_CSS.index(selector + " {")
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_legend_toggle_carries_the_chip_class():
    """The look (transparent / outlined / dim / hover) is inherited from .chip —
    the long-standing class present in every cached stylesheet — so a stale CSS
    can't render the toggle as a bare native button."""
    assert 'class="chip library-legend-pill"' in LIBRARY_HTML


def test_legend_pill_rule_is_slim_not_a_duplicate_chip():
    """The pill rule must NOT re-declare the .chip basics (that's the whole point
    of reusing .chip); it only adds the caret gap + the open accent."""
    pill = _rule(".library-legend-pill")
    for prop in ("background:", "padding:", "border:", "appearance:"):
        assert prop not in pill, f"pill should delegate {prop} to .chip"
    assert "gap: var(--gap-2)" in pill, "caret spacing is pill-specific"


def test_legend_pill_green_accent_only_when_open():
    # closed = dim .chip; open = green, signalling the active toggle. v1.23.54
    # added the explicit .open:hover so the green survives a hover (.chip:hover
    # would otherwise win by source order).
    assert ".library-legend-pill.open:hover { color: var(--green)" in APP_CSS
    # v0.50.94 regression guard: the OPEN accent must live in its OWN standalone
    # rule (a `{` directly after `.open`), applied UNGATED so a pressed toggle
    # reads active on touch too. v0.50.93 wrapped the (hover: hover) gate around
    # the two-line selector list, leaving `.library-legend-pill.open,` dangling
    # before the @media at-rule → the whole rule was invalid and the active green
    # vanished. `.library-legend-pill.open {` is present ONLY when the base rule
    # is standalone (the broken form had `.open,` / `.open:hover {`, never `.open {`).
    assert ".library-legend-pill.open { color: var(--green)" in APP_CSS
    assert ".library-legend-pill.open,\n@media" not in APP_CSS, (
        "the .open base accent must not be swallowed into a media query"
    )
    # and the base rule must NOT sit inside any @media block — verify the nearest
    # preceding `@media` (if any) has already closed before the base rule opens.
    base_i = APP_CSS.index(".library-legend-pill.open { color: var(--green)")
    prev_media = APP_CSS.rfind("@media", 0, base_i)
    if prev_media != -1:
        assert APP_CSS.count("}", prev_media, base_i) >= APP_CSS.count("{", prev_media, base_i), (
            "the ungated .open base rule must not be nested inside an open @media block"
        )


def test_legend_pill_has_keyboard_focus_outline():
    # the toggle is covered by both .chip:focus-visible and its own entry; the
    # shared block paints the cyan keyboard outline.
    i = APP_CSS.index(".library-legend-pill:focus-visible,")
    assert "outline: 2px solid var(--cyan)" in APP_CSS[i:i + 800]
