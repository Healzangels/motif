"""v1.18.74 — close the PROBE TDB URLS → REPROBE FAILURES gap (3rd attempt).

the user's recurring report — third time in five tags:
  - v1.17.5  : tightened margins on .form-actions when adjacent to .form-hint
  - v1.18.57 : tightened `.form-hint-divider` margins 18px/10px → 10px/6px
  - v1.18.59 : added `white-space: normal` to `.form-actions` + `.form-hint-divider`
  - v1.18.74 (this tag): the root cause was a multi-line Jinja `{# … #}` comment
                          in the template, not the CSS

## Root cause the user's prior reports kept missing

`.block-body` has `white-space: pre-wrap` (app.css:767). This
preserves source newlines + indentation in the rendered HTML as
visible vertical whitespace BETWEEN sibling children. The v1.18.59
attempt set `white-space: normal` on the children themselves
(.form-actions + .form-hint-divider) — but that only affects text
INSIDE those elements. It does NOT affect text nodes sitting in
the parent (.block-body) BETWEEN sibling elements.

The settings.html template had:

  <div class="form-actions">...</div>
  {# v1.15.24: 10-line Jinja comment block
     explaining why this divider exists … #}
  <hr class="form-hint-divider" />

Jinja strips the comment TEXT at render time but leaves the
surrounding newlines:

  </div>\\n    \\n    <hr />

Under `.block-body`'s pre-wrap, that `\\n    \\n    ` text node
renders as ~2 line-heights of preserved vertical gap. The
REPROBE FAILURES section below the divider has just `\\n    `
between its hr and its <p> (one newline + indent), producing
~1 line-height of gap. Visible asymmetry that the user's eye
caught immediately.

## Fix

Delete the multi-line Jinja comment from the template. The v1.15.24
archaeology now lives ONLY in the CSS comment at `.form-hint-divider`
(where it was already mirrored). Future template authors get a
warning in the CSS comment: multi-line Jinja `{# … #}` blocks in
pre-wrap contexts are visually load-bearing.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── The template no longer has the multi-line Jinja comment ─


def test_settings_template_no_multiline_jinja_between_button_and_divider():
    """The PROBE TDB URLS form-actions div must be IMMEDIATELY
    followed (modulo a single newline + indent) by the
    `.form-hint-divider` hr. The multi-line Jinja `{# v1.15.24: … #}`
    comment block that lived between them is gone — its archaeology
    moved into the CSS comment on `.form-hint-divider`."""
    src = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    # Anchor on the PROBE TDB URLS button row.
    btn_idx = src.index('id="bulk-probe-tdb-btn"')
    # Walk forward to the divider hr.
    hr_idx = src.index('<hr class="form-hint-divider"', btn_idx)
    between = src[btn_idx:hr_idx]
    # The form-status span + </div> + ONE newline + indent should
    # be all that separates the button from the hr. The
    # multi-line Jinja comment (which used to live here) would
    # show up as `{# … #}` spanning multiple newlines.
    assert "{# v1.15.24" not in between, (
        "v1.18.74: the v1.15.24 multi-line Jinja comment must be "
        "removed from this position — under .block-body's "
        "white-space: pre-wrap, the comment's surrounding source "
        "newlines render as a ~2 line-height vertical gap that "
        "the user's X-marked screenshot called out. Archaeology "
        "moved to the CSS at .form-hint-divider."
    )
    # And no other multi-line Jinja comment slipped in via the
    # same shape.
    assert between.count("\n") <= 4, (
        "v1.18.74: button row → divider should span AT MOST a "
        "few lines (button line, status-span line, </div> line, "
        "hr line). More lines means surplus whitespace text nodes "
        "rendering as visible gap"
    )


def test_settings_template_probe_button_directly_precedes_divider():
    """The exact source structure: `</div>\\n    <hr ... />` —
    one newline + 4 spaces of indent between the closing div
    and the divider. Matches the spacing between the divider
    and the REPROBE FAILURES paragraph (the canonical 'looks
    right' spacing the user referenced)."""
    src = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    # The exact substring pattern.
    assert (
        '<span class="form-status" id="bulk-probe-tdb-status"></span>\n'
        '    </div>\n'
        '    <hr class="form-hint-divider" />'
    ) in src, (
        "v1.18.74: closing </div> of the PROBE TDB URLS form-"
        "actions must be on its own line, followed DIRECTLY by "
        "the divider hr (one newline + indent only). Any "
        "intervening Jinja/HTML comment block would re-introduce "
        "the gap."
    )


def test_settings_template_reprobe_section_unchanged():
    """The REPROBE FAILURES section's spacing was the reference
    (the 'spacing as been below' the user pointed at). Pin its
    `<hr> ... <p class="form-hint">` shape so a future cleanup
    doesn't break the reference layout."""
    src = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert (
        '<hr class="form-hint-divider" />\n'
        '    <p class="form-hint">'
    ) in src, (
        "v1.18.74: divider → REPROBE FAILURES paragraph spacing "
        "is the canonical 'looks right' reference"
    )


# ── CSS archaeology preserved + warning for future authors ──


def test_v1_18_74_marker_in_css_explains_root_cause():
    """The v1.18.74 marker must live in the CSS so future code-
    archaeologists tracing 'why does this hr have so many
    comments' understand the template-side root cause + the
    warning for future Jinja-in-pre-wrap authoring."""
    src = (REPO / "app" / "web" / "static" / "app.css").read_text()
    div_idx = src.index(".form-hint-divider")
    fn_end = src.index("\n}", div_idx)
    block = src[div_idx:fn_end]
    assert "v1.18.74" in block, (
        "v1.18.74: marker required in .form-hint-divider so future "
        "code-archaeologists see WHY the prior v1.17.5/.57/.59 "
        "CSS fixes weren't enough"
    )
    block_flat = " ".join(block.split())
    # The root-cause explanation.
    assert "text nodes" in block_flat.lower() or "between sibling" in block_flat.lower()
    # The "future authors warning" so the same trap can't be
    # re-introduced.
    assert "multi-line Jinja" in block_flat or "pre-wrap" in block_flat


def test_v1_18_74_warning_documented_for_future_jinja_comments():
    """The CSS comment must include the actionable warning for
    future template authors: multi-line Jinja `{# … #}` blocks
    in pre-wrap contexts are visually load-bearing. Pin the
    warning so a future CSS cleanup that strips the marker
    doesn't lose the institutional memory."""
    src = (REPO / "app" / "web" / "static" / "app.css").read_text()
    div_idx = src.index(".form-hint-divider")
    fn_end = src.index("\n}", div_idx)
    block = src[div_idx:fn_end]
    block_flat = " ".join(block.split())
    # The warning shape — either calls out the strip modifiers
    # OR explicitly says "load-bearing".
    assert (
        "{#-" in block_flat
        or "single line" in block_flat.lower()
        or "load-bearing" in block_flat.lower()
    ), (
        "v1.18.74: CSS comment must warn future template authors "
        "about multi-line Jinja comments in pre-wrap contexts "
        "(use {#- ... -#} strip modifiers or stay on one line)"
    )


def test_prior_attempts_archaeology_preserved():
    """The v1.17.5 / v1.18.57 / v1.18.59 attempts MUST stay
    referenced in the CSS so future code-archaeologists can see
    the FULL series of attempts that didn't fully close the gap.
    Pin so a future 'tidy up comments' pass doesn't drop them."""
    src = (REPO / "app" / "web" / "static" / "app.css").read_text()
    div_idx = src.index(".form-hint-divider")
    fn_end = src.index("\n}", div_idx)
    block = src[div_idx:fn_end]
    assert "v1.18.57" in block, "v1.18.57 attempt archaeology required"
    assert "v1.18.59" in block, "v1.18.59 attempt archaeology required"
    # v1.17.5 referenced at the .form-hint + .form-actions block
    # nearby (not necessarily in this exact CSS rule); checked by
    # surrounding context.
    nearby = src[max(0, div_idx - 2000):div_idx + 3000]
    assert "v1.17.5" in nearby, (
        "v1.17.5 archaeology should remain in the related "
        ".form-hint + .form-actions adjacency rule"
    )
