"""v1.14.17 — bundle: TDB blue-pill no-auto-bulk + Pp deep-link parser fix.

## Part A — blue TDB ↑ pill no longer auto-surfaces bulk update actions

the user: "when filtered by blue pill TDB we shouldn't see the bulk
actions automatically since those features have been moved to the
blue !, let's make it so you can still do those bulk actions but
only after selecting rows".

Pre-fix `tdb_pills=update` (the legacy blue TDB ↑ pill) auto-
surfaced ACCEPT ALL UPDATES / KEEP ALL CURRENT with no selection.
That made sense pre-v1.13.79 when the topbar UPD badge routed
there; v1.13.79 migrated the badge to `attn_pills=update` (the
canonical "review pending updates" surface).

v1.14.17 narrows the legacy TDB↑ surfacing to selection-driven
only. The auto-no-selection bulk path lives ONLY on
`attn_pills=update` now. Selection-driven still works on TDB↑ —
SELECT ALL FILTERED → ACCEPT ALL UPDATES is still reachable.

The DOWNLOAD-FROM-TDB / ADOPT / PUSH TO PLEX hide-on-update-filter
gate also flips from `tdbPills.has('update')` to
`attnPills.has('update')` — same canonical-surface reasoning
(only the ATTN axis is "the ACCEPT/KEEP workflow surface" now).

## Part B — ?src_pills=Pp deep-link parser fix

Same mirror-principle leak class as v1.14.11's _pset miss, one
layer up the chain. The deep-link parser's valid-values set for
src_pills hardcoded `{'T','U','A','M','P','-'}` and silently
stripped `Pp` from URL params. A user opening ?src_pills=Pp
landed with no chip lit + no filter applied.

v1.14.17 adds 'Pp' to the deep-link parser's set, completing the
v1.14.10/v1.14.11 trio (button render + _pset query parser +
deep-link parser).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Part A: TDB blue-pill no-auto-bulk ────────────────────────


def test_show_bar_for_updates_now_selection_driven_only():
    """The showBarForUpdates expression must reduce to
    `(selectedEligibleUpdates > 0)` — no longer ORs in the
    TDB↑-filter no-selection branch."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The new shape is a single-clause assignment.
    assert "const showBarForUpdates = (selectedEligibleUpdates > 0);" in js


def test_old_tdb_filter_no_selection_branch_is_removed():
    """Regression guard: the pre-fix OR clause

        (onUpdateFilter && noOtherFilters && visiblePendingUpdates > 0)
        || selectedEligibleUpdates > 0

    must not survive. A revert that re-adds the TDB↑-filter
    auto-show would silently re-conflate the two surfaces.

    Strip line comments so the rationale comment quoting the
    deleted shape doesn't trip the guard."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "(onUpdateFilter && noOtherFilters && visiblePendingUpdates > 0)" not in js


def test_visiblePendingUpdates_text_now_only_fires_for_attn_filter():
    """The "N pending update(s) · bulk actions below" detail
    text was previously gated on `showBarForUpdates && n === 0`.
    Post-v1.14.17 that combination is impossible (showBarForUpdates
    requires selection). The text now fires only when on
    showBarForAttn AND specifically attnPills.has('update')."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "if (showBarForAttn && n === 0)" in js
    # Inside that branch, the visiblePendingUpdates text is gated
    # on attnPills.has('update') so it doesn't show on fail/await/etc.
    block_start = js.index("if (showBarForAttn && n === 0)")
    block = js[block_start:block_start + 1500]
    assert "libraryState.attnPills.has('update') && visiblePendingUpdates > 0" in block


def test_dl_adopt_push_visibility_uses_attn_axis():
    """DOWNLOAD-FROM-TDB / ADOPT / PUSH TO PLEX visibility gates
    must use `attnPills.has('update')` (the canonical workflow
    surface) instead of the legacy `tdbPills.has('update')`.
    Pin the v1.14.17 onAttnUpdateFilter variable + its three
    usages.

    v1.15.49: ADOPT SELECTED's third clause changed from
    `hasSidecarOnly` to `adoptOnlyCount > 0` (bucket-based
    visibility — M sidecars without Plex independent theme).
    Test intent unchanged: the gate still ANDs with
    !onAttnUpdateFilter so the update-axis filter hides it."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const onAttnUpdateFilter = libraryState.attnPills.has('update');" in js
    # Three call sites use the new variable.
    assert "(!onTdbOnly && !onAttnUpdateFilter && hasTdbEligible)" in js
    # v1.15.49: ADOPT SELECTED block is now multi-line — match on
    # adjacency (both clauses within a short window) instead of an
    # exact-substring single-line check.
    import re
    pattern = re.compile(
        r"!onTdbOnly\s*&&\s*!onAttnUpdateFilter\s*&&\s*adoptOnlyCount\s*>\s*0",
        re.DOTALL,
    )
    assert pattern.search(js), (
        "v1.15.49: ADOPT SELECTED visibility must AND "
        "!onTdbOnly && !onAttnUpdateFilter && adoptOnlyCount > 0 "
        "(line breaks OK)"
    )
    assert "(!onTdbOnly && !onAttnUpdateFilter && pushableCount > 0)" in js


def test_dl_adopt_push_no_longer_hide_on_legacy_tdb_pill():
    """Regression guard: the pre-fix gates used `onUpdateFilter`
    (`tdbPills.has('update')`). Comment-stripped check that no
    live code path still references it."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    # `onUpdateFilter` should no longer appear as a live identifier
    # (the v1.14.17 rewrite removed both its declaration and its
    # three usages).
    assert "onUpdateFilter" not in js


# ── Part B: ?src_pills=Pp deep-link parser fix ────────────────


def test_src_pills_deeplink_parser_includes_pp():
    """The PILL_DEEP_LINKS table at app.js:6510 must include 'Pp'
    in the src_pills allowed-values set so URL deep-links like
    ?src_pills=Pp hydrate the chip on page load."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the src_pills entry.
    anchor = js.index("param: 'src_pills'")
    block = js[anchor:anchor + 800]
    assert "values: new Set(['T','U','A','M','P','Pp','-'])" in block


def test_src_pills_deeplink_parser_pre_fix_set_is_gone():
    """Regression guard: the pre-fix letter-only set must not
    survive in the deep-link parser. Comment-stripped to dodge
    the rationale-comment trap.

    Anchor the check inside the src_pills block specifically —
    other deep-link entries (link_pills, attn_pills) have
    legitimately different values sets that we shouldn't
    accidentally match."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    anchor = js.index("param: 'src_pills'")
    block = js[anchor:anchor + 800]
    assert "new Set(['T','U','A','M','P','-'])" not in block


def test_v1_14_10_v1_14_11_v1_14_17_form_complete_pp_chain():
    """End-to-end mirror-principle check: Pp must be recognized
    at all three layers that filter src tokens.

      Layer 1 — button render: data-src-filter='Pp' on the
                +P chip in library.html.
      Layer 2 — _pset query parser: api.py:6289.
      Layer 3 — deep-link parser: app.js PILL_DEEP_LINKS.

    Each layer's omission silently breaks the chip without
    erroring. Pin all three so a future Pp-related refactor
    finds the leaks."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    py = (REPO / "app" / "web" / "api.py").read_text()
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Layer 1: button render.
    assert 'data-src-filter="Pp"' in html
    # Layer 2: _pset query parser (added v1.14.11).
    assert 'src_set = _pset(src_pills, {"T", "U", "A", "M", "P", "Pp", "-"})' in py
    # Layer 3: deep-link parser (added v1.14.17).
    assert "values: new Set(['T','U','A','M','P','Pp','-'])" in js
