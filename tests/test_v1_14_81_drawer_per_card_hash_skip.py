"""v1.14.81 — drawer per-card hash-skip (kills cross-card flicker on poll).

the user: "I've circled in red the sections I see flicker every
second in the drawer."

The pre-fix renderDrawerBody computed the full body HTML on
every poll and replaced body.innerHTML when it changed. During
an active op the bar/counter/elapsed values tick every second
so the body hash differs every poll → full DOM teardown +
rebuild → every card's nodes recreated, including stable
historical timestamps inside finished cards. Visible flicker
on the static parts.

## Fix

Per-card render with a stable per-card identity (`data-card-key`)
and a content hash (`data-card-hash`). On each poll:
  - Existing card whose key + hash match the desired card →
    leave in place (DOM nodes preserved → no flicker).
  - Existing card whose key matches but hash differs → replace
    only that card.
  - Desired card with no existing match → insert.
  - Existing card with no desired match → drop.

Stable cards (finished ops, the // ACTIVE / // LAST OPS
headers) stop flickering entirely. Only the actively-changing
card paints each tick.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"


def _render_drawer_body() -> str:
    """Slice from `function renderDrawerBody(ops) {` through
    its closing brace so assertions stay scoped to that function."""
    src = OPS_JS.read_text()
    fn_start = src.index("function renderDrawerBody(ops) {")
    # Walk forward to the next top-level function.
    next_fn = src.index("\n  function ", fn_start + 30)
    return src[fn_start:next_fn]


# ── Per-card identity + hash on the new render path ────────────


def test_each_card_carries_data_card_key_and_hash():
    """Every desired card pushed onto the render list must
    carry both a `key` (stable identity) and a `hash` (content
    fingerprint). The diff loop reads these to decide reuse vs.
    replace."""
    body = _render_drawer_body()
    # The desired-list pushes have `key`, `hash`, `html` shape.
    assert "desired.push({" in body
    assert "key:" in body
    assert "hash:" in body
    assert "html:" in body


def test_op_cards_keyed_by_op_id():
    """Each op-card must be keyed by `op:<op_id>` so the same
    op survives polls (its DOM stays put when content is
    unchanged)."""
    body = _render_drawer_body()
    assert "key: `op:${op.op_id}`" in body


def test_active_and_lastops_headers_have_stable_keys():
    """The // ACTIVE and // LAST OPS section headers each carry
    a stable key + hash so they're never re-rendered on poll."""
    body = _render_drawer_body()
    assert "key: 'header:active'" in body
    assert "key: 'header:lastops'" in body


def test_card_hash_is_rendered_html_itself():
    """Hash = rendered HTML is the simplest exact-comparison
    cache key. Cheap to compute, captures every visible field
    change, no false positives or negatives."""
    body = _render_drawer_body()
    # The op-card hash is set to the rendered html string.
    assert "hash: html" in body


# ── Reuse-or-replace diff loop ────────────────────────────────


def test_diff_loop_indexes_existing_by_key():
    """The diff loop must build a map of existing children by
    `data-card-key` so lookups during the desired walk are
    O(1)."""
    body = _render_drawer_body()
    assert "existingByKey" in body
    assert "el.dataset.cardKey" in body


def test_diff_loop_reuses_node_when_hash_matches():
    """Existing card with matching key + hash → keep node
    (append to fragment, NOT recreate). This is the load-bearing
    no-flicker path."""
    body = _render_drawer_body()
    # The reuse branch checks dataset.cardHash === desired hash
    # and appends the existing node to the fragment.
    assert "existing.dataset.cardHash === card.hash" in body
    assert "frag.appendChild(existing)" in body


def test_diff_loop_replaces_node_when_hash_differs():
    """Same key, different hash → build a fresh DOM node from
    the new HTML and append THAT to the fragment instead. The
    existing node is dropped (not consumed from existingByKey
    map) so the final replaceChildren GC's it."""
    body = _render_drawer_body()
    # The else branch creates a temp div + parses new HTML.
    assert "tmp.innerHTML = card.html" in body
    assert "frag.appendChild(newEl)" in body


def test_diff_loop_uses_replaceChildren_for_atomic_swap():
    """`body.replaceChildren(frag)` swaps all children in one
    DOM op — atomic, no intermediate empty state. Stale
    children not consumed during the desired walk get dropped
    here."""
    body = _render_drawer_body()
    assert "body.replaceChildren(frag)" in body


# ── Old body-level hash skip is retired (regression guard) ────


def test_body_level_html_cache_var_retired():
    """The pre-fix `_lastDrawerHtml` body-level hash cache is
    retired — per-card hash is finer-grained AND covers the
    case the body-level cache missed (any single field change
    in any card triggered a full body rewrite)."""
    src = OPS_JS.read_text()
    # The variable declaration must be gone.
    assert "let _lastDrawerHtml = ''" not in src
    # No reads of the variable anywhere.
    assert "_lastDrawerHtml === " not in src
    assert "_lastDrawerHtml =" not in src
    # The v1.14.81 marker explains the retirement.
    assert "v1.14.81: replaced by the per-card hash-skip" in src


# ── v1.14.81 marker pinned at the canonical site ──────────────


def test_v1_14_81_marker_explains_per_card_approach():
    """The v1.14.81 marker on renderDrawerBody explains the
    per-card hash-skip rationale + the data-card-key /
    data-card-hash contract so a future reader can grep here
    for the why."""
    body = _render_drawer_body()
    assert "v1.14.81: per-card render + per-card hash-skip" in body
