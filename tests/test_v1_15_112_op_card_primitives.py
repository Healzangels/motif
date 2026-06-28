"""v1.15.112 — promote `.op-card-cancel-note` + `.op-card-meta-value` to real CSS rules.

DESIGN_SYSTEM.md § 6 flagged both as "structural-only" classes
referenced in ops.js but with no CSS rule. The v1.15.89 lint
allowlist carried entries for both with a "could be promoted"
comment. v1.15.112 promotes them:

  - `.op-card-cancel-note` had inline
    `style="margin-top:10px;text-align:center;opacity:0.6"` at
    ops.js:585 — moved to a proper rule using `--gap-2` so a
    future "softer warning band" variant has an anchor.

  - `.op-card-meta-value` was used as a JS querySelector hook
    (ops.js:718) AND as a child of `.op-card-meta`. The cascade
    handled visual styling; the rule now explicitly states
    `font-variant-numeric: inherit` so the inheritance is
    load-bearing rather than implicit.

Both entries dropped from the v1.15.89 lint allowlist.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"
LINT = REPO / "tests" / "test_v1_15_89_js_classname_hygiene_lint.py"


def test_op_card_cancel_note_rule_exists():
    src = OPS_CSS.read_text()
    assert ".op-card-cancel-note {" in src, (
        "v1.15.112: .op-card-cancel-note must have a real CSS rule."
    )
    # Rule should carry the values that used to live inline.
    rule_idx = src.index(".op-card-cancel-note {")
    rule_block = src[rule_idx:rule_idx + 400]
    assert "text-align: center" in rule_block
    assert "opacity: 0.6" in rule_block
    # Uses the spacing-scale token rather than raw 10px.
    assert "var(--gap-2)" in rule_block


def test_op_card_meta_value_rule_exists():
    src = OPS_CSS.read_text()
    assert ".op-card-meta-value {" in src, (
        "v1.15.112: .op-card-meta-value must have a real CSS rule."
    )


def test_op_card_cancel_note_no_inline_style():
    """The promotion is incomplete if the inline style is still in
    ops.js — the rule and the inline would both apply (rule loses,
    being lower-specificity) and a future rule edit wouldn't take."""
    src = OPS_JS.read_text()
    assert 'op-card-cancel-note muted small">' in src or \
           'op-card-cancel-note muted small"' in src, (
        "Sanity: the class should still be applied in the JS template."
    )
    # The inline-style attribute must be GONE.
    cancel_note_idx = src.index("op-card-cancel-note")
    # Take ~200 chars after the class — the next element starts
    # before then, so any inline style on this element would be
    # in this window.
    window = src[cancel_note_idx:cancel_note_idx + 200]
    # The pre-fix inline contained `margin-top:10px;text-align`.
    assert "margin-top:10px" not in window
    assert "text-align:center" not in window


def test_v1_15_89_allowlist_entries_removed():
    """Both classes must be gone from the v1.15.89 lint allowlist.
    Pre-fix they were listed under 'Documented design-system gaps'
    as candidates for promotion."""
    src = LINT.read_text()
    # The ALLOWLIST dict body must not contain either entry as a
    # quoted key. Match the `"X": (` opener.
    assert '"op-card-cancel-note": (' not in src, (
        "v1.15.112: .op-card-cancel-note now has a CSS rule — "
        "remove from the v1.15.89 lint allowlist."
    )
    assert '"op-card-meta-value": (' not in src, (
        "v1.15.112: .op-card-meta-value now has a CSS rule — "
        "remove from the v1.15.89 lint allowlist."
    )
