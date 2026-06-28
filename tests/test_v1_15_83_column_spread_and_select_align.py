"""v1.15.83 — spread bunched right-cluster + center Apply dropdown.

the user's v1.15.82 feedback after testing the priority picker:
"can we take better advantage of the space our Imdb, current,
imported url, action so all pushed to the right. I don't want
to take up too much space since a long title might be cut off
but right now its a little too on top of each other. also the
action button is not centered in the row its at the bottom"

Two fixes:

1. Right-cluster columns widened so content has breathing room
   within each cell. Pre-fix totals: state 36 + status 88 +
   imdb 100 + current 90 + url 140 + action 120 = 574 + TITLE.
   Post-fix: state 36 + status 100 + imdb 130 + current 120 +
   url 200 + action 160 = 746 + TITLE. The structural overflow
   fix from v1.15.80 (border-box + max-width:100% on the
   select) means the select still can't exceed its cell — the
   width-shaving budget no longer doubles as overflow defense.

2. Action <select> alignment. The td has vertical-align:middle
   (line 900 of app.css, applies to all .table td), but the
   <select> defaults to vertical-align:baseline. When the
   select is taller than text content elsewhere in the row,
   baseline alignment pulls its rendered position toward the
   bottom of the line box instead of the middle. v1.15.83 adds
   explicit vertical-align:middle on the select itself so its
   center aligns to the row midline.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP_CSS = ROOT / "app" / "web" / "static" / "app.css"


# ── 1. Right-cluster column widening ─────────────────────────

def test_imdb_column_widened():
    """v1.15.83: imdb 100 → 130. The tt-ID link sits centered;
    extra cell width = visible whitespace flanking the link, so
    it doesn't read as crammed up against CURRENT next door."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .col-imdb {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    m = re.search(r"width:\s*(\d+)px", rule)
    assert m, "v1.15.83: missing pixel width for .col-imdb"
    assert int(m.group(1)) >= 130, (
        f".col-imdb width {m.group(1)}px is narrower than v1.15.83's "
        "130px — the IMDB cluster needs the extra breathing room."
    )


def test_current_column_widened():
    """v1.15.83: current 90 → 120. CURRENT holds the U badge +
    info button (.row-info-btn, 30px); they need surrounding
    whitespace so the cell doesn't read as edge-to-edge content."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .col-import-current {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    m = re.search(r"width:\s*(\d+)px", rule)
    assert m, "v1.15.83: missing pixel width for .col-import-current"
    assert int(m.group(1)) >= 120, (
        f".col-import-current width {m.group(1)}px is narrower than "
        "v1.15.83's 120px."
    )


def test_url_column_widened():
    """v1.15.83 originally pinned >= 200px to spread the
    bunched cluster. v1.15.84 trims to 170px because 200 left
    URL with ~90px of flanking space vs IMDB's ~50px — visually
    uneven gaps between centered content (the user: "un-even
    spaced between them"). 170px equalizes flanking to ~60px
    matching the neighboring columns. The lower bound here
    relaxes to 160 so v1.15.84's equal-spacing tune doesn't
    trip the counter-guard."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .col-import-url {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    m = re.search(r"width:\s*(\d+)px", rule)
    assert m, "v1.15.83: missing pixel width for .col-import-url"
    assert int(m.group(1)) >= 160, (
        f".col-import-url width {m.group(1)}px is below the 160px "
        "floor — v1.15.83's spread intent was that URL stays "
        "visibly wider than the pre-spread 140px even after the "
        "v1.15.84 equal-flanking trim."
    )


def test_action_column_widened():
    """v1.15.83: action 120 → 160. With CONFLICT P-rows now
    offering a 3-option dropdown including 'Download backup
    (Plex stays)', extra cell width gives the rendered <select>
    + native chevron + the right-padding gutter room without
    the dropdown text feeling cramped."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .col-import-action {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    m = re.search(r"width:\s*(\d+)px", rule)
    assert m, "v1.15.83: missing pixel width for .col-import-action"
    assert int(m.group(1)) >= 160, (
        f".col-import-action width {m.group(1)}px is narrower than "
        "v1.15.83's 160px."
    )


def test_status_column_widened():
    """v1.15.83: status 88 → 100. 'DUPLICATE' and 'INVALID URL'
    are the longest labels; 100px gives them comfortable single-
    line room without truncation."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .col-import-status {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    m = re.search(r"width:\s*(\d+)px", rule)
    assert m, "v1.15.83: missing pixel width for .col-import-status"
    assert int(m.group(1)) >= 100, (
        f".col-import-status width {m.group(1)}px is narrower than "
        "v1.15.83's 100px."
    )


# ── 2. Action <select> vertical centering ────────────────────


def test_action_select_vertical_align_middle():
    """v1.15.83: form control inside .col-import-action gets explicit
    vertical-align:middle so its center aligns to the row's text
    baseline. Without it the select defaults to baseline align
    and sits visually at the bottom of the row (the user: 'the
    action button is not centered in the row its at the bottom').
    The td vertical-align:middle (line 900) centers the line box
    but doesn't help when the select's own alignment within the
    line box is baseline.

    v1.15.88: this declaration migrated from the per-site
    `.col-import-action select` rule to the `.input-tiny` primitive.
    Check `.input-tiny` directly."""
    css = APP_CSS.read_text()
    rule_start = css.index(".input-tiny {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    assert "vertical-align: middle" in rule or "vertical-align:middle" in rule, (
        "v1.15.83: .col-import-action select missing vertical-align:"
        "middle. Without it the dropdown sits at the row bottom."
    )
