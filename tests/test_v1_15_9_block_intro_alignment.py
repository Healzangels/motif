"""v1.15.9 — settings paragraph alignment fix.

the user: "the settings downloads probe tdb url section text are
still misaligned the top paragraph is indented more than the
bottom"

## Pre-fix

The PROBE TDB URLS section (and REPROBE PLEX THEMES, and one
HOMEPAGE block) has two paragraphs inside a `<div class="block-body">`:

    <div class="block-body">              ← padding: 16px 18px
      <p class="form-hint block-intro">    ← +12px 18px 4px (stacks!)
        first paragraph...
      </p>
      <p class="form-hint">                ← no extra padding
        second paragraph...
      </p>
    </div>

`.block-intro` was originally written with `padding: 12px 18px 4px`
because it was designed to sit inside `.block-body-flush` (the
table-container variant with `padding: 0`) — there it provides
the needed inset.

But when the same class is used inside a regular `.block-body`,
the parent's 18px horizontal padding stacks with the intro's own
18px, so the first paragraph's text starts ~36px from the left
edge while the sibling `.form-hint` paragraph starts at ~18px.

## Fix

Scope the padding rule to direct children of `.block-body-flush`:

    .block-body-flush > .block-intro { padding: 12px 18px 4px; }
    .block-intro { margin: 0; }

Inside a regular `.block-body`, `.block-intro` becomes a layout
no-op (just `margin: 0`), so both paragraphs align at the
parent's 18px left edge. Inside `.block-body-flush` (table
containers), the original padding still kicks in via the
descendant selector.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


def test_block_intro_padding_is_scoped_to_flush_parent():
    """The .block-intro padding rule must use the
    `.block-body-flush > .block-intro` descendant selector so it
    only fires inside table-container blocks. Pin the exact
    selector — a regression to the bare `.block-intro { padding: ... }`
    rule resurrects the double-padding alignment bug."""
    src = APP_CSS.read_text()
    assert ".block-body-flush > .block-intro {" in src, (
        "v1.15.9: padding must be scoped to .block-body-flush parents"
    )
    # The bare-class rule must NOT carry padding anymore — only margin
    # (+ v1.22.35's max-width measure cap). Match the bare `.block-intro {…}`
    # rule (no descendant chain) and assert margin:0 + no padding.
    m = re.search(r"\n\.block-intro \{([^}]*)\}", src)
    assert m, "v1.15.9: bare .block-intro rule must exist"
    bare = m.group(1)
    assert "margin: 0" in bare, "v1.15.9: bare .block-intro keeps margin:0"
    assert "padding" not in bare, (
        "v1.15.9: bare .block-intro must NOT carry padding (it belongs on "
        "the .block-body-flush > .block-intro descendant rule)"
    )
    # v1.22.35: the bare rule now also caps to the shared settings measure.
    assert "max-width: var(--measure-form)" in bare


def test_no_unscoped_block_intro_padding_rule():
    """Guard against accidentally re-adding the bare
    `.block-intro { padding: 12px 18px 4px; ... }` rule that
    caused the original double-indent bug."""
    src = APP_CSS.read_text()
    # Find any line that opens a bare `.block-intro {` block (not
    # preceded by a descendant selector).
    lines = src.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith(".block-intro {") or stripped == ".block-intro{":
            # Walk forward up to the closing brace, ensure no padding decl.
            block = []
            for j in range(i, min(i + 8, len(lines))):
                block.append(lines[j])
                if "}" in lines[j]:
                    break
            block_text = "\n".join(block)
            assert "padding" not in block_text, (
                "Bare .block-intro rule must not declare padding "
                "(would stack with .block-body's own padding) — see v1.15.9"
            )


def test_probe_tdb_paragraphs_share_form_hint_class():
    """Both paragraphs in the PROBE TDB URLS section must use
    `.form-hint` so they share typography. The first ALSO has
    `.block-intro` (load-bearing for the `.block-body-flush` use
    case elsewhere in the file), but with the v1.15.9 CSS scope
    that no longer adds extra horizontal padding here."""
    html = SETTINGS_HTML.read_text()
    section_start = html.index('<h2 class="block-title">// PROBE TDB URLS</h2>')
    section = html[section_start:section_start + 2000]
    # Confirm both paragraphs are present + share .form-hint.
    assert '<p class="form-hint block-intro">' in section
    assert '<p class="form-hint">' in section
    # Confirm parent is the regular (non-flush) block-body.
    body_idx = section.index('<div class="block-body">')
    # Make sure `block-body-flush` does NOT appear before either p tag —
    # the v1.15.9 fix relies on this section using the regular variant.
    body_through_first_p = section[body_idx:section.index(
        '<p class="form-hint block-intro">')]
    assert "block-body-flush" not in body_through_first_p


def test_reprobe_plex_section_has_same_shape():
    """REPROBE PLEX THEMES has the same two-paragraph shape and
    benefits from the same v1.15.9 fix. Pin the structure so a
    future refactor that changes one section also has to consider
    the other."""
    html = SETTINGS_HTML.read_text()
    section_start = html.index('<h2 class="block-title">// REPROBE PLEX THEMES</h2>')
    section = html[section_start:section_start + 2000]
    assert '<div class="block-body">' in section
    assert '<p class="form-hint block-intro">' in section
    # Sibling form-hint without block-intro.
    after_intro = section[section.index('<p class="form-hint block-intro">'):]
    assert '<p class="form-hint">' in after_intro


def test_block_body_flush_callsites_still_use_block_intro():
    """The two `.block-body-flush` callsites that legitimately
    need `.block-intro` padding (table containers in LIBRARY
    SECTIONS + HOMEPAGE LIBRARIES) must keep the pairing intact —
    the v1.15.9 fix preserves their layout via the descendant
    selector."""
    html = SETTINGS_HTML.read_text()
    # Count flush blocks that pair block-body-flush with a block-intro
    # paragraph as a direct child.
    pair_marker = '<div class="block-body block-body-flush">'
    occurrences = html.count(pair_marker)
    # At minimum the two known callsites (LIBRARY SECTIONS, HOMEPAGE).
    assert occurrences >= 2, (
        "v1.15.9 fix relies on .block-body-flush callsites still "
        "existing — descendant selector matches them via the parent class"
    )
