"""v1.18.30 — Remove the cyan PL=pushed FILTER chip (redundant with LINK=PU).

the user's observation: the cyan PL=pushed filter chip (added
v1.18.25) and the cyan LINK=PU filter chip (added v1.18.22)
both selected rows where `placement_kind = 'plex_upload'`. The
two SQL predicates were the literal same string — clicking
either filter chip always yielded the identical row set. The
PL chip was dead UI weight.

v1.18.30 retires the PL FILTER chip. The per-row PL DOT (cyan
.state-pill.pushed) survives — it's a separate concern: visual
identity for plex_upload rows in the table without forcing the
user to scan the LINK column.

## What was removed

  - HTML: <button data-pl-pill="pushed"> in library.html.
  - CSS: .state-pill-btn-pushed rule (the FILTER chip styling
    — note this is NOT the per-row .state-pill.pushed dot
    styling, which survives).
  - api.py SQL: `elif p == "pushed":` branch in pl_pills handler.
  - api.py _row_matches_pl: `if p == "pushed"` post-stat matcher.
  - api.py _pset whitelist: "pushed" dropped from pl_pills set.
  - app.js PILL_DEEP_LINKS pl_pills values Set: 'pushed' dropped.
  - app.js pillAxes plPill values array: 'pushed' dropped.

## What was preserved

  - JS row-state derivation: `(placed && isPlexUpload) ? 'pushed'`
    still fires so the per-row pill renders the cyan dot.
    (v1.19.67 briefly removed this; v1.19.82 restored it — see
    test_v1_19_82_pl_pushed_restore.py for why.)
  - JS tooltip: `pl === 'pushed'` branch still serves the
    cyan-dot tooltip (same v1.19.67→v1.19.82 round-trip).
  - CSS .state-pill.pushed: per-row cyan dot styling preserved
    (never removed — only the FILTER chip rule went).

## What was widened

  - api.py _row_matches_pl: 'on' branch broadened back to its
    v1.18.22 unified shape — matches BOTH sidecar AND
    plex_upload placements. The PL FILTER axis is now a pure
    health axis (on=any healthy placement, await=staged,
    broken=lost, off=none). Mechanism filtering lives on the
    LINK axis (LINK=PU for API push specifically).
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"
API_PY = REPO / "app" / "web" / "api.py"


# ── HTML chip removed ────────────────────────────────────────


def test_template_pl_pushed_filter_chip_removed():
    """The data-pl-pill='pushed' button must not survive in
    library.html. Pre-v1.18.30 it sat next to the green PL=on
    chip; v1.18.30 deletes it as redundant with LINK=PU."""
    html = LIBRARY_HTML.read_text()
    assert 'data-pl-pill="pushed"' not in html, (
        "v1.18.30: the cyan PL FILTER chip must be removed "
        "(redundant with LINK=PU's identical predicate)"
    )
    assert "state-pill-btn-pushed" not in html, (
        "v1.18.30: the chip's CSS class reference must also "
        "be removed from library.html"
    )


def test_template_pl_on_tooltip_widened_back():
    """The green PL='on' chip tooltip must cover BOTH sidecar
    and API-push rows now that 'pushed' isn't its own filter.
    Pre-v1.18.30 the tooltip narrowed to 'sidecar file' (it
    was distinguishing from the now-removed pushed chip)."""
    html = LIBRARY_HTML.read_text()
    idx = html.index('data-pl-pill="on"')
    block = html[max(0, idx - 200):idx + 400]
    # Title must mention both placement kinds (or use a
    # more generic phrasing that covers both).
    title_idx = block.index("title=\"")
    title_block = block[title_idx:title_idx + 300]
    assert (
        "API push" in title_block
        or "sidecar or API" in title_block
    ), (
        "v1.18.30: PL='on' tooltip must explain it covers both "
        "sidecar and API-push placements (chip is no longer "
        "split into on/pushed)"
    )


# ── CSS rules ────────────────────────────────────────────────


def test_css_filter_chip_pushed_rule_removed():
    """`.state-pill-btn-pushed` (the FILTER chip rule) must be
    gone. `.state-pill.pushed` (the per-row DOT rule) must
    survive — they're different selectors for different UI."""
    css = APP_CSS.read_text()
    # Strip CSS comments so the v1.18.30 removal-marker block
    # doesn't get matched as if it were a live rule.
    import re
    css_no_comments = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    assert ".state-pill-btn-pushed" not in css_no_comments, (
        "v1.18.30: the FILTER chip's CSS rule must be removed"
    )
    assert ".state-pill.pushed {" in css_no_comments, (
        "v1.18.30: per-row PL DOT CSS rule MUST survive — it "
        "is a separate concern from the filter chip"
    )


# ── api.py — SQL branch removed ──────────────────────────────


def test_api_pl_pills_pushed_sql_branch_removed():
    """`elif p == "pushed":` must be gone from the pl_pills
    SQL handler — there's no chip to dispatch into it.
    Comment-tolerant: only the LIVE source-line (no leading
    `#`) counts."""
    src = API_PY.read_text()
    live_lines = [
        line for line in src.splitlines()
        if not line.lstrip().startswith("#")
    ]
    assert all(
        'elif p == "pushed":' not in line for line in live_lines
    ), (
        "v1.18.30: pl_pills SQL handler must no longer have a "
        "live 'pushed' branch — the chip is gone"
    )


def test_api_pset_pl_whitelist_excludes_pushed():
    """The `_pset(pl_pills, {...})` whitelist must NOT include
    'pushed'. Stale deep-link `?pl_pills=pushed` URLs will be
    silently dropped by _pset (whitelist semantics)."""
    src = API_PY.read_text()
    assert '_pset(pl_pills, {"on", "await", "off", "broken"})' in src, (
        "v1.18.30: pl_pills whitelist must shrink to the 4 "
        "axis values (chip retirement)"
    )
    assert (
        '_pset(pl_pills, {"on", "await", "off", "broken", "pushed"})'
        not in src
    ), (
        "v1.18.30: the v1.18.25 5-value whitelist must be gone"
    )


# ── api.py — _row_matches_pl: 'on' broadened, 'pushed' gone ──


def test_row_matches_pl_on_includes_plex_upload():
    """'on' branch must match plex_upload rows too — it's
    back to v1.18.22's unified semantics now that the cyan
    PL FILTER chip's mutex requirement is gone."""
    src = API_PY.read_text()
    fn_idx = src.index("def _row_matches_pl(")
    body = src[fn_idx:fn_idx + 3000]
    # Find the 'on' branch and scope tightly — stop at the
    # next `if p ==` to keep the slice off the 'await' /
    # 'off' branches which legitimately use is_plex_upload.
    on_idx = body.index('if p == "on"')
    next_branch_idx = body.index('if p == "', on_idx + 1)
    on_block = body[on_idx:next_branch_idx]
    assert (
        'it.get("media_folder") or is_plex_upload' in on_block
    ), (
        "v1.18.30: 'on' branch must match BOTH sidecar "
        "(media_folder set) AND plex_upload placements"
    )
    # The v1.18.25 mutex exclusion (`and not is_plex_upload`)
    # must be gone FROM THIS BRANCH specifically.
    assert "and not is_plex_upload" not in on_block, (
        "v1.18.30: 'on' branch must no longer exclude "
        "plex_upload (that exclusion only made sense when the "
        "v1.18.25 'pushed' chip was carving out its own slice)"
    )


def test_row_matches_pl_pushed_branch_removed():
    """'pushed' branch must be gone from _row_matches_pl. With
    the chip retired, no caller dispatches into this branch
    anyway — pruning prevents future drift where the matcher
    survives without its UI surface."""
    src = API_PY.read_text()
    fn_idx = src.index("def _row_matches_pl(")
    body = src[fn_idx:fn_idx + 3000]
    assert 'p == "pushed"' not in body, (
        "v1.18.30: _row_matches_pl 'pushed' branch must be "
        "removed alongside the chip"
    )


# ── JS deep-link Set + pillAxes ALL array ────────────────────


def test_js_pill_deep_links_pl_set_excludes_pushed():
    """The PILL_DEEP_LINKS pl_pills values Set must not include
    'pushed' so the // ALL handler doesn't reach for a
    non-existent chip. Comment-tolerant: strip //-comments
    before the membership check so the v1.18.30 removal-marker
    referencing 'pushed' doesn't keep the assertion false."""
    js = APP_JS.read_text()
    anchor = js.index("param: 'pl_pills'")
    block = js[anchor:anchor + 1200]
    block_no_comments = "\n".join(
        line for line in block.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "'pushed'" not in block_no_comments, (
        "v1.18.30: pl_pills PILL_DEEP_LINKS Set must drop "
        "'pushed' (no chip to dispatch onto)"
    )
    assert (
        "values: new Set(['on','await','off','broken'])" in block
    ), (
        "v1.18.30: pl_pills PILL_DEEP_LINKS Set must shrink to 4"
    )


def test_js_pill_axes_pl_all_array_excludes_pushed():
    """The // ALL handler's plPill values array must shrink to
    4 entries — clicking // ALL on the PL row would otherwise
    try to activate a chip that doesn't exist."""
    js = APP_JS.read_text()
    anchor = js.index("attr: 'plPill', allAttr: 'plPillAll'")
    block = js[anchor:anchor + 500]
    assert "values: ['on', 'await', 'off', 'broken']" in block, (
        "v1.18.30: plPill ALL set must shrink to 4 entries"
    )


# ── JS row-state derivation PRESERVED (cyan dot, restored v1.19.82) ──


def test_js_row_state_pushed_derivation_present():
    """The per-row 'pushed' state derivation paints the cyan PL
    dot for plex_upload placements. v1.18.30 removed the FILTER
    chip but kept this per-row state; v1.19.67 briefly removed
    it too (audit R2: "duplicate of LINK=PU"); v1.19.82 restored
    it because that premise was incomplete — a mismatched
    plex_upload row renders LINK=M (not PU), so cyan PL is the
    only at-a-glance API-push signal in that state. Full
    rationale + the decoupling proof live in
    test_v1_19_82_pl_pushed_restore.py."""
    js = APP_JS.read_text()
    assert "(placed && isPlexUpload) ? 'pushed'" in js, (
        "v1.19.82: 'pushed' PL state restored — plex_upload "
        "placements paint the cyan PL dot again (LINK=M can hide "
        "the PU chip on mismatched rows)."
    )


def test_js_row_pushed_tooltip_present():
    """Companion to the state derivation — the 'pushed' tooltip
    branch serves the cyan-dot hover text (restored v1.19.82)."""
    js = APP_JS.read_text()
    assert "pl === 'pushed'" in js, (
        "v1.19.82: 'pushed' tooltip branch restored alongside "
        "the cyan PL dot"
    )


# ── LINK=PU still the canonical filter for plex_upload ──────


def test_link_pills_pu_still_present():
    """LINK=PU is now THE filter for plex_upload rows. Pin
    the SQL branch so a future cleanup doesn't accidentally
    remove BOTH chips."""
    src = API_PY.read_text()
    assert 'elif p == "pu":' in src
    idx = src.index('elif p == "pu":')
    # Widened from 600 → 1200; the v1.18.22 explanatory comment
    # before the actual branches.append push the SQL past 600.
    block = src[idx:idx + 1200]
    assert "COALESCE(p_e.placement_kind, p_g.placement_kind) = 'plex_upload'" in block
