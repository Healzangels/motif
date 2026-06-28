"""v1.19.67 — filter UX cleanup (R2 + R3 from the v1.19.66 audit).

The v1.19.66 audit identified three high-payoff small wins:
  R2 — kill PL=pushed cyan dot, keep LINK=PU only
  R3 — rename STATUS! filter row → ATTN, drop duplicate ⚿
  R4 — remove the faded T-only filter chip
v1.19.67 ships R2 + R3. R4 (T-only removal) intentionally
deferred — the user uses that filter occasionally and wants to
keep it for now.

## R2 — kill PL=pushed cyan  ⚠ REVERSED in v1.19.82

Pre-v1.19.67 the PL column rendered cyan for `plex_upload`
placements (the v1.18.25 'pushed' state) distinct from green
for sidecar placements ('on'). The LINK column already shows
PU for the same rows, sitting right next to PL — so two cyan
signals adjacent for the same state. Audit recommendation:
one axis per signal. PL now answers "is there a placement?"
(green=yes); LINK answers "what kind?" (HL/C/PU).

  - `app/web/static/app.js` — `pl` derivation simplified to
    `placed → 'on'` regardless of placement_kind.
  - `app/web/static/app.js` — `plTip` ternary drops the
    `'pushed'` branch.

**v1.19.82 reversed R2.** The "pure duplicate" premise was
incomplete: the LINK chain renders M (mismatch) BEFORE the
plex_upload branch, so a mismatched plex_upload row shows
LINK=M, not PU — and mismatch hides placement-kind for HL/C
alike. In that state cyan PL is the ONLY at-a-glance "this is
an API placement" signal. The cyan dot + tooltip were restored
(visual-only — the server still filters plex_upload as PL=on).
The three R2 tests that pinned the removal were dropped here;
the restored behavior is pinned in
`tests/test_v1_19_82_pl_pushed_restore.py`.

## R3 — rename STATUS! → ATTN + drop duplicate ⚿

Pre-v1.19.67 the library had TWO filter rows labeled
"STATUS:": the ALL/THEMED/UNTHEMED row at the top and the
attention-glyphs row further down. First-time users couldn't
tell them apart at a glance. Audit's biggest single fix.

The ⚿ (cookies-needed) chip duplicated the TDB axis ⚿ — same
SQL (`failure_kind='cookies_expired'`), same rows. v1.19.67
removes it from the ATTN row; users filter to that cohort via
the TDB axis chip going forward.

  - `app/web/templates/library.html` — STATUS: label → ATTN:,
    aria-label updated, ⚿ button removed.

## URL backwards compat

`attn_pills=cookies` URL param remains validated by the JS
PILL_DEEP_LINKS allowlist + the server's `_pset` whitelist —
old bookmarks won't 400. The filter just becomes a no-op via
the now-missing template chip (no SQL branch matches because
the data-attn-pill='cookies' button doesn't exist to apply it
client-side).
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


# ── R2: PL=pushed cyan dot ── REVERSED in v1.19.82 ──────────
# The three tests that pinned the v1.19.67 removal of the
# per-row 'pushed' state + tooltip were dropped when v1.19.82
# restored the cyan dot (see this module's docstring + the
# new pins in test_v1_19_82_pl_pushed_restore.py). The LINK=PU
# chip preserved-check below stays valid — both surfaces now
# co-exist, which was the whole point of the reversal.


def test_link_pu_chip_preserved():
    """The LINK=PU chip survives — that's where the API-push
    distinction lives now (a one-axis-per-signal cleanup)."""
    assert 'data-link-pill="pu"' in LIBRARY_HTML
    assert "link-glyph-pu" in LIBRARY_HTML


# ── R3: STATUS! → ATTN + ⚿ removed ───────────────────────────


def test_attn_row_aria_label_renamed():
    """The attention-glyphs filter row's aria-label must be
    'ATTN pill filter' (was 'STATUS pill filter')."""
    assert 'aria-label="ATTN pill filter"' in LIBRARY_HTML, (
        "v1.19.67: ATTN-row aria-label must use 'ATTN'"
    )
    # Old label gone (no leftover STATUS-row aria-label that
    # would compete with the ALL/THEMED/UNTHEMED one at the
    # top of the filter bar).
    attn_anchor = LIBRARY_HTML.index('aria-label="ATTN pill filter"')
    # The OLD aria-label for this row was 'STATUS pill filter'.
    # The page still has 'STATUS' chips at the top (ALL/THEMED/
    # UNTHEMED) but they're in a different aria-label structure
    # — pin only that the attn row uses 'ATTN'.
    assert 'aria-label="STATUS pill filter"' not in LIBRARY_HTML, (
        "v1.19.67: legacy ATTN-row aria-label 'STATUS pill "
        "filter' must be gone (collided with the ALL/THEMED/"
        "UNTHEMED row's STATUS framing)"
    )


def test_attn_row_label_text_is_attn():
    """The visible row label must read 'ATTN:' not 'STATUS:'.
    Pin only the rendered <span> text — Jinja comments can
    still reference the historical 'STATUS:' label."""
    attn_anchor = LIBRARY_HTML.index('aria-label="ATTN pill filter"')
    row_end = LIBRARY_HTML.index("</div>", attn_anchor)
    row = LIBRARY_HTML[attn_anchor:row_end]
    assert '<span class="pill-filter-label muted small">ATTN:</span>' in row, (
        "v1.19.67: ATTN row's visible label must read 'ATTN:'"
    )
    assert '<span class="pill-filter-label muted small">STATUS:</span>' not in row, (
        "v1.19.67: ATTN row's <span> label must not still "
        "display 'STATUS:'"
    )


def test_cookies_chip_removed_from_attn_row():
    """⚿ button gone from the ATTN row (duplicate with TDB ⚿)."""
    attn_anchor = LIBRARY_HTML.index('aria-label="ATTN pill filter"')
    row_end = LIBRARY_HTML.index("</div>", attn_anchor)
    row = LIBRARY_HTML[attn_anchor:row_end]
    assert 'data-attn-pill="cookies"' not in row, (
        "v1.19.67: ⚿ button must be removed from ATTN row "
        "(duplicate with TDB ⚿)"
    )


def test_other_attn_chips_preserved():
    """Removing the ⚿ chip must NOT have removed any of the
    other 5 ATTN chips (fail/update/mismatch/await/broken)."""
    attn_anchor = LIBRARY_HTML.index('aria-label="ATTN pill filter"')
    row_end = LIBRARY_HTML.index("</div>", attn_anchor)
    row = LIBRARY_HTML[attn_anchor:row_end]
    for token in ("fail", "update", "mismatch", "await", "broken"):
        assert f'data-attn-pill="{token}"' in row, (
            f"v1.19.67: ATTN chip '{token}' missing from row"
        )


def test_tdb_axis_still_has_cookies_chip():
    """The cookies cohort migration story: removed from ATTN,
    survives on the TDB axis. The TDB ⚿ chip must still exist."""
    assert 'data-tdb-pill="cookies"' in LIBRARY_HTML, (
        "v1.19.67: TDB ⚿ chip is now the SOLE filter chip for "
        "the cookies-needed cohort — must still exist"
    )


# ── URL backwards compat ─────────────────────────────────────


def test_attn_pills_cookies_still_in_js_allowlist():
    """JS deep-link parser must still accept ?attn_pills=cookies
    so old bookmarks don't 400. The token becomes a client-side
    no-op (no chip to apply it) but the URL parses."""
    idx = APP_JS.index("param: 'attn_pills'")
    block = APP_JS[idx:idx + 1000]
    assert "'cookies'" in block, (
        "v1.19.67: 'cookies' must stay in attn_pills deep-link "
        "allowlist for URL backwards compat"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_67_version_pin():
    """v1.19.67 bumped. Relaxed to v1.19.x prefix after v1.19.68
    continued the line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
