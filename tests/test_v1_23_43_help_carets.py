"""v1.23.43 — discoverable expand affordances (help feature, Tag 1 of 3).

the user wants a help/discovery layer for new users. Phase 1, smallest piece
first: make the "click to expand for more detail" interactions VISIBLE. Pre-tag
they were bare clickables with no hint — the user: "when you click on the drawer
you could then click the fields to expand them for more detail … to a new user
they wouldn't know that's possible."

Two disclosure surfaces gain a rotating caret (▸ → ▾ on open):
- the LIVE OPS drawer op-cards (RUN INSIGHT expands on click; had NO affordance)
- the info-card // HISTORY / // PROVENANCE <details> (native marker was hidden,
  leaving only the browser's tiny triangle).

Source pins (consistent with motif's UI-tweak tags); the carets are pure
CSS-driven rotation so there's no new JS behavior to exercise.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── op-card caret ───────────────────────────────────────────────


def test_op_card_renders_caret_gated_on_real_cards():
    """The op-card head renders an op-card-caret, but only for real
    (non-synthetic) cards — synthetic queue rows don't expand."""
    assert "op-card-caret" in OPS_JS
    i = OPS_JS.index('<div class="op-card-head">')
    head = OPS_JS[i:i + 300]
    assert "isSynthetic ? '' :" in head, "caret must be gated on !isSynthetic"
    assert "op-card-caret" in head
    assert 'aria-hidden="true"' in head


def test_op_card_caret_rotates_when_expanded():
    """CSS rotates the caret 90deg when the card carries op-card-expanded,
    and the reduced-motion guard disables the transition."""
    assert ".op-card-caret {" in OPS_CSS
    assert "transition: transform var(--motion-fast)" in OPS_CSS
    i = OPS_CSS.index(".op-card.op-card-expanded .op-card-caret")
    assert "rotate(90deg)" in OPS_CSS[i:i + 120]
    # status pinned right so the leading caret + kind stay grouped left.
    si = OPS_CSS.index(".op-card-status {")
    assert "margin-left: auto" in OPS_CSS[si:si + 160]


# ── info-card HISTORY / PROVENANCE caret ────────────────────────


def test_history_section_summary_has_caret():
    """The // HISTORY / // PROVENANCE disclosure gains a custom caret
    (the native marker is hidden) that rotates open."""
    assert ".history-section > summary::before" in APP_CSS
    i = APP_CSS.index(".history-section > summary::before")
    block = APP_CSS[i:i + 260]
    assert "\\25B8" in block, "caret glyph (▸) via content"
    assert "transition: transform var(--motion-fast)" in block
    oi = APP_CSS.index(".history-section[open] > summary::before")
    assert "rotate(90deg)" in APP_CSS[oi:oi + 120]
