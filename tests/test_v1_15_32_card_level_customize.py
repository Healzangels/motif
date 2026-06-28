"""v1.15.32 — per-card dashboard customize: reorder + hide
individual cards across rows.

## Why

the user:

> "can we make it so we can reorder the rows separate from one
>  another so for example I could reorder and put user provided
>  next to hardlinked or the row above if I wanted"

The v1.13.48 customize feature operates at the section (row)
level — drag/reorder/hide whole rows. the user wanted card-level
control: move USER PROVIDED next to HARDLINKS within the
STORAGE row, OR up to a different row entirely.

## Design

1. Each card gets a stable global `data-dash-card="<id>"`. IDs
   are unique across the entire dashboard so a card can be
   moved between sections (e.g. user-provided can sit under
   STORAGE or get bumped up to OPERATIONS).
2. Layout JSON gains an optional `cards` array per section:
   `{sections: [{id, hidden, cards?: [{id, hidden}, ...]}]}`.
   Order in the cards array = render order within that
   section. A card listed under a section's cards array
   "lives in" that section regardless of where the template
   put it originally.
3. Per-card customize controls (◀ ▶ + // HIDE):
     • ◀ moves the card one slot left within its section. If
       it's the leftmost card, it walks back to the END of
       the previous grid section (a section with at least one
       data-dash-card child).
     • ▶ symmetrical — rightmost card walks forward to the
       START of the next grid section.
   This gives the "cards reflow across rows" feel the user
   asked for.
4. Per-card hide uses `.dash-user-hidden-card` class (separate
   axis from the section-level `.dash-user-hidden` so the same
   article can be card-hidden inside a section-hidden parent
   without ambiguity).
5. Server-side: `_clean_dashboard_sections` extended to validate
   the optional `cards` field with the same dedupe + bool-coerce
   shape as the section list.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.web.api import _clean_dashboard_sections


REPO = Path(__file__).resolve().parent.parent
DASH = REPO / "app" / "web" / "templates" / "dashboard.html"
CUSTOMIZE_JS = REPO / "app" / "web" / "static" / "dashboard-customize.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


# ── 1. data-dash-card attrs on every grid card ────────────────


def test_every_grid_section_card_has_data_dash_card_attr():
    """The 5 grid sections (top-stats, operations, activity,
    plex-coverage, storage) hold 13 cards total. Each one must
    carry a stable `data-dash-card` ID so the customize feature
    can address it. IDs are globally unique across the dashboard
    so a card can be moved between sections."""
    html = DASH.read_text()
    # The 13 IDs assigned in v1.15.32 (alphabetical for clarity).
    expected = [
        "added-this-week", "added-today",
        "copies", "hardlinks",
        "plex-anime", "plex-movies", "plex-tv",
        "queue-failed", "queue-pending", "queue-running",
        "tdb-movies", "tdb-tv-series",
        "user-provided",
    ]
    for cid in expected:
        assert f'data-dash-card="{cid}"' in html, (
            f"v1.15.32: card {cid} must carry data-dash-card attr"
        )


def test_data_dash_card_ids_are_unique():
    """Card IDs must be unique across the dashboard so a single
    saved layout entry resolves to exactly one DOM node."""
    import re
    html = DASH.read_text()
    ids = re.findall(r'data-dash-card="([^"]+)"', html)
    assert len(ids) == len(set(ids)), (
        f"v1.15.32: data-dash-card IDs must be unique — got "
        f"duplicates: {[i for i in ids if ids.count(i) > 1]}"
    )


def test_grid_sections_carry_at_least_one_card():
    """Each section that should have card-level customize must
    contain at least one data-dash-card child. Pin so a future
    layout change doesn't silently strip the attrs from all
    cards in a section (which would make injectControls skip
    that section's cards entirely)."""
    html = DASH.read_text()
    for section in (
        "top-stats", "operations", "activity",
        "plex-coverage", "storage",
    ):
        # Locate the section block.
        section_anchor = html.index(f'data-dash-section="{section}"')
        section_end = html.index('</section>', section_anchor)
        section_block = html[section_anchor:section_end]
        assert "data-dash-card=" in section_block, (
            f"v1.15.32: section {section} must contain "
            f"≥1 data-dash-card child"
        )


# ── 2. Server-side: _clean accepts + validates `cards` field ──


def test_clean_passes_through_valid_cards_field():
    """A section entry with a well-formed cards array must
    survive cleaning intact (id + hidden per card)."""
    out = _clean_dashboard_sections([
        {
            "id": "storage",
            "hidden": False,
            "cards": [
                {"id": "hardlinks", "hidden": False},
                {"id": "copies", "hidden": True},
                {"id": "user-provided", "hidden": False},
            ],
        },
    ])
    assert out == [{
        "id": "storage",
        "hidden": False,
        "cards": [
            {"id": "hardlinks", "hidden": False},
            {"id": "copies", "hidden": True},
            {"id": "user-provided", "hidden": False},
        ],
    }]


def test_clean_drops_malformed_card_entries():
    """Per-card validation mirrors the section validation:
    drop non-dict entries, drop entries without a string id,
    coerce hidden to bool, dedupe by id."""
    out = _clean_dashboard_sections([
        {
            "id": "storage",
            "cards": [
                "not-a-dict",                              # drop
                {"hidden": True},                          # no id → drop
                {"id": ""},                                # empty id → drop
                {"id": "hardlinks", "hidden": "true"},     # coerce hidden
                {"id": "hardlinks", "hidden": False},      # dupe → drop
            ],
        },
    ])
    assert out == [{
        "id": "storage",
        "hidden": False,
        "cards": [
            {"id": "hardlinks", "hidden": True},
        ],
    }]


def test_clean_omits_cards_field_when_invalid_or_empty():
    """If the cards array is missing, not a list, or contains
    only invalid entries, the cleaned section should NOT carry
    a `cards` field at all (avoids bloating layout state with
    empty arrays)."""
    out = _clean_dashboard_sections([
        {"id": "a"},                       # no cards → omit
        {"id": "b", "cards": "not-a-list"},
        {"id": "c", "cards": []},          # empty → omit
        {"id": "d", "cards": [{"bad": 1}]},  # all invalid → omit
    ])
    for entry in out:
        assert "cards" not in entry


def test_clean_preserves_card_order():
    """The `cards` array order is significant — it dictates the
    render order within the section. Cleaner must not sort or
    rearrange."""
    out = _clean_dashboard_sections([
        {
            "id": "storage",
            "cards": [
                {"id": "user-provided"},
                {"id": "hardlinks"},
                {"id": "copies"},
            ],
        },
    ])
    assert [c["id"] for c in out[0]["cards"]] == [
        "user-provided", "hardlinks", "copies",
    ]


# ── 3. JS: applyLayout reorders cards across sections ─────────


def test_apply_layout_reads_per_section_cards_array():
    """applyLayout must read each saved section's `cards` array
    and move the matching <article data-dash-card> nodes into
    that section in the saved order. Cards may have moved
    sections — the layout is the source of truth."""
    src = CUSTOMIZE_JS.read_text()
    fn_anchor = src.index("function applyLayout()")
    fn_end = src.index("\n  function ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # Walks every [data-dash-card] in the container (cards may
    # have moved sections).
    assert "querySelectorAll('[data-dash-card]')" in fn_body
    # Iterates each section's saved cards.
    assert "entry.cards" in fn_body
    # Moves the card into its section via appendChild (which
    # repositions the node).
    assert "sectionNode.appendChild(node)" in fn_body
    # Applies per-card hidden flag.
    assert "dash-user-hidden-card" in fn_body


def test_apply_layout_registers_unsaved_cards_under_their_default_section():
    """A card present in the DOM but missing from every saved
    section's cards array must get registered under its CURRENT
    parent section (the template default) so the next save
    captures its position. Don't move it — the template order
    is the baseline for unsaved cards."""
    src = CUSTOMIZE_JS.read_text()
    fn_anchor = src.index("function applyLayout()")
    fn_end = src.index("\n  function ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert "seenCardIds" in fn_body
    assert "node.closest('[data-dash-section]')" in fn_body
    assert 'entry.cards.push' in fn_body


# ── 4. JS: per-card customize controls ────────────────────────


def test_inject_controls_adds_per_card_controls():
    """In customize mode, every <article data-dash-card> must
    receive a `.dash-card-controls` bar with ◀ ▶ + // HIDE
    buttons. Anchored on the v1.15.32 marker block."""
    src = CUSTOMIZE_JS.read_text()
    inject_anchor = src.index("function injectControls()")
    inject_end = src.index("\n  function ", inject_anchor + 1)
    inject_body = src[inject_anchor:inject_end]
    # Iterates every data-dash-card.
    assert "querySelectorAll('[data-dash-card]')" in inject_body
    # ◀ + ▶ buttons via data-dash-card-move attrs.
    assert 'data-dash-card-move="prev"' in inject_body
    assert 'data-dash-card-move="next"' in inject_body
    # // HIDE button via data-dash-card-toggle.
    assert "data-dash-card-toggle" in inject_body
    # Renders the dash-card-controls container.
    assert "dash-card-controls" in inject_body


def test_remove_controls_tears_down_per_card_controls():
    """Exiting customize mode must remove every injected
    .dash-card-controls bar so the dashboard is clean again."""
    src = CUSTOMIZE_JS.read_text()
    remove_anchor = src.index("function removeControls()")
    remove_end = src.index("\n  function ", remove_anchor + 1)
    remove_body = src[remove_anchor:remove_end]
    assert "querySelectorAll('[data-dash-card]')" in remove_body
    assert ".dash-card-controls" in remove_body


# ── 5. JS: cross-section card movement ────────────────────────


def test_card_arrow_move_walks_across_section_boundaries():
    """The ◀ / ▶ arrow handler must:
      • move within section when there's a sibling card in the
        target direction
      • walk into the previous/next GRID section (one with
        ≥1 data-dash-card child) when at the boundary
    the user's "user provided next to hardlinked or the row above"
    requirement — cards reflow across rows like text."""
    src = CUSTOMIZE_JS.read_text()
    fn_anchor = src.index("function onCardArrowMove(")
    fn_end = src.index("\n  function ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # Within-section move: insertBefore against previous/next
    # sibling card.
    assert "previousElementSibling" in fn_body
    assert "nextElementSibling" in fn_body
    # Cross-section walk via dedicated helpers.
    assert "previousGridSection(currentSection)" in fn_body
    assert "nextGridSection(currentSection)" in fn_body
    # Saves debounced after every move.
    assert "rebuildLayoutFromDOM()" in fn_body
    assert "scheduleSave()" in fn_body


def test_grid_section_walker_skips_non_grid_sections():
    """previousGridSection / nextGridSection must skip sections
    that have no data-dash-card children (e.g. PER-SECTION
    COVERAGE, FAILURE BREAKDOWN — block-style sections). A card
    can only "walk" into another grid section."""
    src = CUSTOMIZE_JS.read_text()
    # Both helpers gate on the presence of a data-dash-card
    # child via querySelector(':scope > [data-dash-card]').
    prev_anchor = src.index("function previousGridSection(")
    prev_end = src.index("\n  function ", prev_anchor + 1)
    prev_body = src[prev_anchor:prev_end]
    assert ":scope > [data-dash-card]" in prev_body, (
        "previousGridSection must check for a data-dash-card "
        "child to identify grid sections"
    )

    next_anchor = src.index("function nextGridSection(")
    next_end = src.index("\n  function ", next_anchor + 1)
    next_body = src[next_anchor:next_end]
    assert ":scope > [data-dash-card]" in next_body


# ── 6. JS: per-card hide toggle ───────────────────────────────


def test_card_hide_toggle_writes_dash_user_hidden_card_class():
    """The per-card // HIDE button must toggle
    `.dash-user-hidden-card` on the article (NOT the section's
    `.dash-user-hidden` — separate axis so a card-hidden card
    inside a section-hidden parent stays unambiguously
    classified)."""
    src = CUSTOMIZE_JS.read_text()
    fn_anchor = src.index("function onCardToggle(")
    fn_end = src.index("\n  function ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert "dash-user-hidden-card" in fn_body
    assert "rebuildLayoutFromDOM()" in fn_body
    assert "scheduleSave()" in fn_body


# ── 7. CSS: per-card classes defined ──────────────────────────


def test_per_card_customize_css_classes_present():
    """The per-card controls + hidden state need their own CSS
    rules. .dash-user-hidden-card hides outside customize mode;
    .dash-card-controls bar shows only inside customize mode."""
    css = APP_CSS.read_text()
    # Hidden card class hides the article entirely outside
    # customize mode.
    assert "[data-dash-card].dash-user-hidden-card" in css
    assert "display: none !important" in css  # already used by section-hidden too
    # Controls only render in customize mode.
    assert ".dash-card-controls" in css
    assert "body.dash-customize-mode .dash-card-controls" in css


# ── 8. rebuildLayoutFromDOM captures cards order per section ──


def test_rebuild_layout_captures_cards_per_section():
    """After any customize action (drag, arrow, hide), the
    layout snapshot must capture the per-section cards array
    so the next /api/dashboard/layout PUT persists the new
    order + per-card hidden flags."""
    src = CUSTOMIZE_JS.read_text()
    fn_anchor = src.index("function rebuildLayoutFromDOM()")
    fn_end = src.index("\n  function ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # Walks each section's :scope > [data-dash-card] children.
    assert ":scope > [data-dash-card]" in fn_body
    # Builds a cards array per section + attaches it to the
    # entry when non-empty.
    assert "cards.push" in fn_body
    assert "if (cards.length > 0) entry.cards = cards" in fn_body
    # Captures per-card hidden state.
    assert "dash-user-hidden-card" in fn_body
