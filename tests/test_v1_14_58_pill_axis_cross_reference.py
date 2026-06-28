"""v1.14.58 — static cross-reference: every pill token agrees across template + JS.

Extension of v1.14.50's tone-class cross-ref pattern, applied to
the pill-axis class-P drift family. Same bug shape as v1.14.23
(TDB-`dropped` + PL-`broken`), v1.14.43 (LINK PS chip wiring), and
v1.14.52 H4 (LINK ALL set missing 'ps'): a new pill token gets
added on one surface but a sibling surface silently keeps the
old shape.

Three sources of truth per axis:

  1. **Template** (`library.html`):
     `data-{axis}-pill="<token>"` button attrs — the user-clickable
     UI surface; must include every supported token as a button.
  2. **JS click-handler `pillAxes`** (app.js:6940):
     `values: [...]` arrays — the // ALL-button bound list; if a
     token is missing here, // ALL leaves that pill chip inactive
     and the inverse-select pattern leaks rows.
  3. **JS deep-link parser `PILL_DEEP_LINKS`** (app.js:6754):
     `values: new Set([...])` — URL deep-link token whitelist; if
     missing, ?{axis}_pills=<token> deep-links silently strip.

The test asserts all three sets agree per axis. A new token added
to template/parser without updating pillAxes (or vice versa)
fails immediately.

Axes covered: tdbPill, dlPill, plPill, linkPill, edPill, attnPill
(the 6 multi-select pill rows that follow the same shape).

NOT covered: SRC pill (uses its own click handler with composite
tokens like `Pp`; out-of-shape for this cross-ref). The status-
chip filter (data-status) and the TDB scope chip (data-tdb) are
single-select shapes, not multi-select pill axes.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"
TEMPLATE = REPO / "app" / "web" / "templates" / "library.html"


# ── Axis definitions ─────────────────────────────────────────

# Map: pillAxes JS-side `attr` name → (template data-attr suffix,
# deep-link URL param). All three sources for each axis must
# expose the SAME token set.
AXES = [
    # (pillAxes_attr_name, template_data_attr, deep_link_param)
    ("tdbPill", "tdb-pill", "tdb_pills"),
    ("dlPill", "dl-pill", "dl_pills"),
    ("plPill", "pl-pill", "pl_pills"),
    ("linkPill", "link-pill", "link_pills"),
    ("edPill", "ed-pill", "ed_pills"),
    ("attnPill", "attn-pill", "attn_pills"),
]


# ── Source-of-truth extractors ───────────────────────────────


def _pill_axes_block() -> str:
    """Return the slice of app.js between `const pillAxes = [` and
    its closing `];`. The same `attr: 'X',` shape appears in
    HYDRATE_MAP later in the file (no values: there) — anchoring
    by the pillAxes parent block avoids the wrong-block hit."""
    js = JS.read_text()
    start = js.index("const pillAxes = [")
    end = js.index("];", start) + 2
    return js[start:end]


def _extract_pill_axes_values(axis_attr: str) -> set[str]:
    """Pull the `values: [...]` array for a pillAxes entry by its
    `attr:` name. Scoped to the pillAxes block so HYDRATE_MAP
    entries (same `attr:` key, no `values:`) don't false-match.

    v1.19.50: window widened 500 → 1500 to accommodate the
    accreted rationale comments above the `values:` array.
    Each LINK-token add (v1.14.43 'ps', v1.14.52 BK badge,
    v1.18.22 'pu', v1.19.43 'b', v1.19.50 'bk') brought ~5
    lines of WHY comment — by v1.19.50 the linkPill block
    crossed the original 500-char boundary."""
    block = _pill_axes_block()
    needle = f"attr: '{axis_attr}',"
    idx = block.find(needle)
    assert idx != -1, f"pillAxes entry for {axis_attr!r} not found"
    sub = block[idx:idx + 1500]
    m = re.search(r"values:\s*\[\s*([^\]]+?)\s*\]", sub, flags=re.DOTALL)
    assert m, f"values: [...] not found in pillAxes entry for {axis_attr!r}"
    return set(re.findall(r"'([^']+)'", m.group(1)))


def _extract_deep_link_values(param: str) -> set[str]:
    """Pull the `values: new Set([...])` for a PILL_DEEP_LINKS
    entry by its `param:` name."""
    js = JS.read_text()
    needle = f"param: '{param}',"
    idx = js.find(needle)
    assert idx != -1, f"PILL_DEEP_LINKS entry for {param!r} not found"
    # Walk forward a bigger window — these entries have multi-line
    # rationale comments inside.
    block = js[idx:idx + 1500]
    m = re.search(
        r"values:\s*new Set\(\[\s*([^\]]+?)\s*\]\)", block, flags=re.DOTALL
    )
    assert m, f"values: new Set([...]) not found for param {param!r}"
    return set(re.findall(r"'([^']+)'", m.group(1)))


def _extract_template_tokens(data_attr: str) -> set[str]:
    """Pull every `data-{data_attr}="<token>"` from library.html.
    Excludes the empty `data-{data_attr}=""` (the // CLEAR button)
    and the `data-{data_attr}-all="1"` (the // ALL button) — both
    are control buttons, not tokens."""
    html = TEMPLATE.read_text()
    pattern = re.compile(
        rf'\bdata-{re.escape(data_attr)}="([^"]+)"'
    )
    tokens = set(pattern.findall(html))
    # Filter out empty (CLEAR button) — pattern excludes empty by
    # construction, but be defensive.
    tokens.discard("")
    return tokens


# ── The cross-ref test, per axis ─────────────────────────────


def test_all_pill_axes_agree_across_template_pill_axes_and_deep_link_parser():
    """For every pill axis, the token set must be identical across
    all three sources of truth. Drift in any direction is a class-P
    bug that historically required 6 layer-updates per fix
    (CLAUDE.md class R).

    v1.19.66 exception: 'ps' was removed from the LINK template
    (chip dropped) but kept in pillAxes + deep-link parser for
    URL backwards compat (existing bookmarks return empty via
    the no-op SQL). One explicit asymmetry, documented inline."""
    # Deprecation map: tokens present in pillAxes / deep-link
    # but intentionally absent from the template (URL backwards
    # compat with old bookmarks).
    #   v1.19.66 → ('linkPill', 'ps'): PS chip dropped.
    #   v1.19.67 → ('attnPill', 'cookies'): ⚿ chip removed from
    #     ATTN row (duplicate with TDB ⚿). Cookies cohort
    #     reachable via the TDB axis instead.
    DEPRECATED_URL_ONLY = {
        ("linkPill", "ps"),
        ("attnPill", "cookies"),
    }
    drift: list[str] = []
    for axis_attr, tmpl_attr, dl_param in AXES:
        tmpl = _extract_template_tokens(tmpl_attr)
        ax = _extract_pill_axes_values(axis_attr)
        dl = _extract_deep_link_values(dl_param)
        # Apply v1.19.66 deprecation: any URL-only token is
        # subtracted from both ax and dl before the comparison
        # so the test only fires on UNINTENTIONAL drift.
        for axis_name, deprecated_token in DEPRECATED_URL_ONLY:
            if axis_attr == axis_name:
                ax = ax - {deprecated_token}
                dl = dl - {deprecated_token}
        if not (tmpl == ax == dl):
            drift.append(
                f"\n  {axis_attr} (template {tmpl_attr}, deep-link {dl_param}):\n"
                f"    template:    {sorted(tmpl)}\n"
                f"    pillAxes:    {sorted(ax)}\n"
                f"    deep-links:  {sorted(dl)}\n"
                f"    template - pillAxes = {sorted(tmpl - ax)}\n"
                f"    pillAxes - template = {sorted(ax - tmpl)}\n"
                f"    template - deep:    = {sorted(tmpl - dl)}\n"
                f"    deep - template:    = {sorted(dl - tmpl)}"
            )
    assert not drift, (
        "Pill-axis token drift across template / pillAxes / deep-link "
        "parser:\n" + "\n".join(drift) +
        "\n\nFix: add the missing token to whichever side is short. "
        "Same shape as v1.14.23 (TDB-`dropped` / PL-`broken`), "
        "v1.14.43 (LINK PS), v1.14.52 H4 (LINK ALL missing 'ps')."
    )


# ── Per-axis assertions (richer error messages on failure) ──


def test_tdb_pill_tokens_agree():
    tmpl = _extract_template_tokens("tdb-pill")
    ax = _extract_pill_axes_values("tdbPill")
    dl = _extract_deep_link_values("tdb_pills")
    assert tmpl == ax == dl, f"tdb: tmpl={tmpl}, ax={ax}, dl={dl}"


def test_dl_pill_tokens_agree():
    tmpl = _extract_template_tokens("dl-pill")
    ax = _extract_pill_axes_values("dlPill")
    dl = _extract_deep_link_values("dl_pills")
    assert tmpl == ax == dl, f"dl: tmpl={tmpl}, ax={ax}, dl={dl}"


def test_pl_pill_tokens_agree():
    tmpl = _extract_template_tokens("pl-pill")
    ax = _extract_pill_axes_values("plPill")
    dl = _extract_deep_link_values("pl_pills")
    assert tmpl == ax == dl, f"pl: tmpl={tmpl}, ax={ax}, dl={dl}"


def test_link_pill_tokens_agree():
    """The v1.14.52 H4 fix added 'ps' to the LINK pillAxes list —
    this test would have caught the original drift at static-test
    time and prevented the bug from shipping.

    v1.19.66: 'ps' is now deliberately URL-only — the template
    chip was removed but pillAxes + deep-link parser keep it
    for backwards compat. Subtract it before comparison."""
    tmpl = _extract_template_tokens("link-pill")
    ax = _extract_pill_axes_values("linkPill") - {"ps"}
    dl = _extract_deep_link_values("link_pills") - {"ps"}
    assert tmpl == ax == dl, f"link: tmpl={tmpl}, ax={ax}, dl={dl}"


def test_ed_pill_tokens_agree():
    tmpl = _extract_template_tokens("ed-pill")
    ax = _extract_pill_axes_values("edPill")
    dl = _extract_deep_link_values("ed_pills")
    assert tmpl == ax == dl, f"ed: tmpl={tmpl}, ax={ax}, dl={dl}"


def test_attn_pill_tokens_agree():
    """v1.19.67: 'cookies' is now URL-only — the ⚿ chip was
    removed from the ATTN row (duplicated with TDB ⚿) but
    pillAxes + deep-link parser keep it for backwards compat.
    Subtract before comparison."""
    tmpl = _extract_template_tokens("attn-pill")
    ax = _extract_pill_axes_values("attnPill") - {"cookies"}
    dl = _extract_deep_link_values("attn_pills") - {"cookies"}
    assert tmpl == ax == dl, f"attn: tmpl={tmpl}, ax={ax}, dl={dl}"
