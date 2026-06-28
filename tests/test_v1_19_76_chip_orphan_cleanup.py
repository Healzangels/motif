"""v1.19.76 — chip-axis cleanup: drop orphan tokens from JS // ALL.

Audit follow-up to v1.19.75. The chip filter pipeline was clean
end-to-end (no broken filters) but had five LOW orphans:

1. `link_pills=ps` — chip removed v1.19.66 (PS chip), but the
   token still lived in JS click-handler ALL → clicking LINK //
   ALL polluted the URL with `?link_pills=...,ps,...` even
   though no PS chip exists to render active. Server side
   intentionally preserves the URL param as a `(1 = 0)` no-op
   SQL branch so old bookmarks return empty instead of 500ing —
   that's correct, the orphan is purely the click-ALL side.
2. `attn_pills=cookies` — same shape, chip removed v1.19.67.
3. EDITION row rendered on /collections — collection
   `folder_path` never contains `{edition-` patterns, so both
   ED=has and ED=none match degenerate subsets. Per the v1.18.50
   dead-chip precedent ("wider scope for impossible filters in
   the collections section"), the whole ED row should be hidden
   on /collections.
4. SRC click-handler ALL adds A/M on /collections. SRC=A and
   SRC=M chips are already hidden on /collections via v1.18.50,
   but the static `allLetters` list in app.js unconditionally
   added them to the URL on // ALL click.
5. LINK click-handler ALL adds hl/c/m on /collections. Symmetric
   to #4 — chips hidden in HTML, but the JS ALL list wasn't
   tab-aware.

## Fix shape

Both click-ALL handlers (the pillAxes loop + the SRC handler)
now DERIVE the ALL set from chips actually present in the DOM
rather than static value lists. Auto-handles every existing
template-time chip hide. Static value lists become permissive
allowlists used by the deep-link parser only (preserving the
bookmark safety net for URL params that no longer have a chip).

EDITION row gets a `{% if tab != 'collections' %}` wrap mirroring
the v1.18.50 LINK/SRC precedent.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()


# ── pillAxes click-ALL: DOM-derivation ──────────────────────────


def test_pillaxes_all_handler_derives_from_dom():
    """The // ALL click handler for the generic pillAxes loop
    (TDB / DL / PL / LINK / ED / ATTN) must add ONLY chips
    actually present in the DOM, not the static `axis.values`
    list. Pre-fix retired chips (PS post-v1.19.66, ATTN cookies
    post-v1.19.67) still got added to URL state."""
    # Anchor on the `if (b.dataset[allKey])` branch.
    idx = APP_JS.index("if (b.dataset[allKey])")
    block = APP_JS[idx: idx + 1500]
    # The fix: iterate DOM-present chips, intersected with the
    # axis.values allowlist.
    assert "querySelectorAll(`[data-${kebab}]`)" in block, (
        "v1.19.76: ALL handler must derive from "
        "querySelectorAll on the kebab attr, not from "
        "axis.values directly"
    )
    # The allowlist intersection still happens (so stray
    # data-link-pill tokens can't sneak in).
    assert "allowedValues" in block
    assert "axis.values" in block, (
        "v1.19.76: axis.values must remain as the allowlist set"
    )
    # Old static loop must be gone.
    assert "axis.values.forEach((v) => set.add(v));" not in block, (
        "v1.19.76: legacy static-list `.forEach((v) => set.add(v))` "
        "must be replaced by DOM-derived loop"
    )


def test_pillaxes_values_lists_still_serve_as_allowlists():
    """The static `axis.values` lists are still used as
    intersection allowlists in the // ALL handler + by the
    deep-link parser. Keep them present so retired-chip URL
    params still hydrate to the libraryState set for the
    server's no-op SQL branch."""
    # Click-ALL allowlists. (v1.19.90 added 'tb'; v1.20.17 added 'ab';
    # v1.24.28 added 'rp' for the stale-plex_upload re-push chip.)
    assert "'hl', 'c', 'm', 'none', 'ps', 'pu', 'rp', 'b', 'bk', 'tb', 'ab'" in APP_JS
    # v1.23.7: 'restore' joined the ATTN axis. v1.24.40: 'repush' (⟳ re-push).
    assert ("'fail', 'cookies', 'update', 'mismatch', 'await', "
            "'broken', 'restore', 'repush'") in APP_JS
    # Deep-link parser allowlists.
    assert "new Set(['hl','c','m','none','ps','pu','rp','b','bk','tb','ab'])" in APP_JS
    assert ("new Set(['fail','cookies','update','mismatch','await',"
            "'broken','restore','repush'])") in APP_JS


# ── SRC click-ALL: DOM-derivation ───────────────────────────────


def test_src_all_handler_derives_from_dom():
    """The SRC // ALL click handler must derive from DOM-present
    src-filter chips, not the static `allLetters` list. v1.18.50
    hides A/M on /collections; pre-fix ALL added them anyway."""
    idx = APP_JS.index("b.dataset.srcFilterAll")
    block = APP_JS[idx: idx + 1200]
    # The fix: iterate DOM-present src-filter chips.
    assert "querySelectorAll('[data-src-filter]')" in block, (
        "v1.19.76: SRC ALL must use querySelectorAll('[data-src-filter]')"
    )
    # Intersection allowlist preserved (allLetters used as gate).
    assert "allowedLetters" in block
    # Old static loop gone.
    assert "allLetters.forEach((l) => libraryState.srcFilter.add(l));" not in block, (
        "v1.19.76: legacy static `allLetters.forEach` must be gone"
    )


# ── EDITION row hidden on /collections ─────────────────────────


def test_edition_row_wrapped_in_tab_check():
    """EDITION row gets a `{% if tab != 'collections' %}` wrap
    per v1.18.50 dead-chip precedent."""
    # Anchor on the EDITION pill-filter label.
    idx = LIBRARY_HTML.index('aria-label="EDITION pill filter"')
    # The opening `{% if tab != 'collections' %}` must sit
    # above the `<div ... aria-label="EDITION ...">` line.
    upstream = LIBRARY_HTML[max(0, idx - 800):idx]
    assert "{% if tab != 'collections' %}" in upstream, (
        "v1.19.76: EDITION row must be wrapped in "
        "{% if tab != 'collections' %} per v1.18.50 precedent"
    )
    # The closing endif must sit below the </div>.
    downstream = LIBRARY_HTML[idx:idx + 1500]
    assert "{% endif %}" in downstream


def test_v1_18_50_marker_referenced_in_edition_wrap():
    """The v1.19.76 EDITION wrap must reference v1.18.50 as the
    precedent — searchable archaeology for the next person who
    wonders why this hide exists."""
    idx = LIBRARY_HTML.index('aria-label="EDITION pill filter"')
    block = LIBRARY_HTML[max(0, idx - 1000):idx + 200]
    assert "v1.18.50" in block, (
        "v1.19.76: the EDITION-hide comment must reference "
        "v1.18.50 as the dead-chip precedent so future "
        "archaeology can trace the pattern"
    )
    assert "v1.19.76" in block


# ── End-to-end: no orphans in click-ALL paths ───────────────────


def test_no_static_link_pills_all_with_ps_orphan():
    """The legacy `values: ['hl', 'c', 'm', 'none', 'ps', ...]
    .forEach((v) => set.add(v))` pattern must be gone — that
    was the v1.19.66 PS orphan. The values array stays as the
    allowlist (test above pins it); the iteration shape changed."""
    # Look for the broken forEach pattern in the LINK area —
    # if it survived anywhere it'd add 'ps' to URL on ALL.
    pattern = (
        "values: ['hl', 'c', 'm', 'none', 'ps', 'pu', 'rp', 'b', 'bk', 'tb', 'ab'] }"
    )
    assert pattern in APP_JS, (
        "v1.19.76: the values allowlist itself stays (it's the "
        "intersection set + deep-link allowlist). What changed "
        "is the // ALL loop body."
    )
    # The forEach iteration must be on the DOM, not on the
    # static values list, inside the ALL branch.
    idx = APP_JS.index("if (b.dataset[allKey])")
    block = APP_JS[idx: idx + 1500]
    # Pre-fix shape (must be gone).
    assert "axis.values.forEach((v) => set.add(v));" not in block


# ── Version pin ────────────────────────────────────────────────


def test_v1_19_76_version_pin():
    """Loose prefix — later tags continue the v1.19.x line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
