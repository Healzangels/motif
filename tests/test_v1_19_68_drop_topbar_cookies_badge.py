"""v1.19.68 — drop the topbar COOKIES badge; fold cookies-needed into FAIL.

the user's v1.19.66 audit follow-up: "let's get rid of yellow ⚿
all together [as a separate attention surface]. If something
reports back missing cookies needed to download then let's use
our existing red ! to flag it for the user to ack or deal with.
But then the yellow pill tdb will allow us to filter for it
again later."

The cookies-needed state had THREE yellow ⚿ surfaces pre-v1.19.68:
  1. TDB-column row pill (kept — identifies which rows are
     cookies-needed at a glance)
  2. TDB-axis filter chip (kept — surfaces the cohort when the
     operator wants to act)
  3. Standalone topbar COOKIES badge (REMOVED — overkill alert
     duplication, especially since cookies-needed rows also
     contribute to the red FAIL pulse count via
     `_FAILURES_SFA_WHERE_SQL` which matches `failure_kind IS
     NOT NULL AND unacked`)

## v1.19.68 changes

**`app/web/templates/base.html`**:
  - `<a id="topbar-cookies-badge">` element removed entirely
  - FAIL pill href repointed: `?tdb_pills=dead` → `?attn_pills=fail`.
    The pre-fix href filtered to dead-URL-only (narrower than the
    FAIL count, which included every failure_kind). Now click-
    through matches the count + matches what ACK FAILURES sees.
  - Inline comment block documents the removal + rationale.

**`app/web/static/app.js`**:
  - `refreshTopbarStatus` cookies-badge toggle block removed
    (lines 782-792 pre-fix).

## What stays the same

  - **Title-cell red ⚠** already fires on `failure_kind &&
    !failure_acked_at` — cookies_expired matches that gate. No
    code change; the v1.19.66 audit confirmed the existing
    behavior matches what the user wants.
  - **TDB-column row pill** still renders yellow ⚿ via
    `computeTdbPill`'s 'cookies' branch (app.js:7743).
  - **TDB-axis filter chip** (yellow ⚿) preserved at
    `library.html:132`.
  - **`_COOKIES_PILL_WHERE_SQL`** + the `cookies_count` field
    in /api/stats preserved for the settings page diagnostics
    + any other downstream consumer. Only the topbar UI
    surface goes away.
  - **`.btn-cookies` recovery-button styling** untouched —
    used by the FIX COOKIES action in the recovery card.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


# ── Topbar COOKIES badge gone ────────────────────────────────


def test_topbar_cookies_badge_element_removed():
    """The <a id="topbar-cookies-badge"> element must be absent
    from base.html."""
    assert 'id="topbar-cookies-badge"' not in BASE_HTML
    assert 'id="topbar-cookies-count"' not in BASE_HTML


def test_topbar_cookies_link_removed():
    """The badge's href (?attn_pills=cookies in base.html) is
    gone too — the chip was the only consumer of that template
    string."""
    # We removed the badge that contained 'attn_pills=cookies'.
    assert 'attn_pills=cookies' not in BASE_HTML


def test_app_js_cookies_badge_toggle_removed():
    """The refreshTopbarStatus block that read stats.cookies
    and toggled the badge's hidden state must be gone."""
    assert "$('#topbar-cookies-badge')" not in APP_JS
    assert "$('#topbar-cookies-count')" not in APP_JS


def test_v1_19_68_marker_documents_removal():
    """A v1.19.68 marker near where the badge sat must explain
    WHY it was removed (the FAIL pulse already counts cookies)."""
    assert "v1.19.68" in BASE_HTML
    assert "v1.19.68" in APP_JS


# ── FAIL pill href repointed ─────────────────────────────────


def test_fail_pill_href_repointed_to_attn_fail():
    """Pre-v1.19.68 the FAIL pulse linked to /movies?tdb_pills=dead
    (a narrower subset of the count). v1.19.68 repointed to
    /movies?attn_pills=fail so click-through matches the FAIL
    count exactly."""
    # Locate the FAIL badge element.
    fail_idx = BASE_HTML.index('id="topbar-failures-badge"')
    # Walk back ~300 chars to capture the href.
    pre = BASE_HTML[max(0, fail_idx - 300):fail_idx]
    assert "/movies?attn_pills=fail" in pre, (
        "v1.19.68: FAIL pulse must link to attn_pills=fail "
        "(matches the count + matches ACK FAILURES)"
    )
    assert "/movies?tdb_pills=dead" not in pre, (
        "v1.19.68: legacy narrower href must be replaced"
    )


def test_fail_pill_tooltip_mentions_cookies_folded_in():
    """The FAIL pulse tooltip should signal that cookies-needed
    rows are now part of the count (since v1.19.68 removed the
    standalone COOKIES badge that previously surfaced them)."""
    fail_idx = BASE_HTML.index('id="topbar-failures-badge"')
    # Walk through to the closing </a>.
    end = BASE_HTML.index("</a>", fail_idx)
    block = BASE_HTML[fail_idx:end]
    assert "cookies" in block.lower(), (
        "v1.19.68: FAIL tooltip should mention that cookies-"
        "needed is folded into this count"
    )


# ── What stays unchanged ─────────────────────────────────────


def test_tdb_row_pill_cookies_render_preserved():
    """computeTdbPill must still return 'cookies' when the row
    has failure_kind='cookies_expired' and no cookies present —
    that drives the yellow ⚿ row pill which the user wants to
    keep as the identification signal."""
    # The branch that returns 'cookies' from computeTdbPill.
    assert "failure_kind === 'cookies_expired'" in APP_JS
    assert "return 'cookies'" in APP_JS


def test_tdb_filter_chip_cookies_preserved_in_library_html():
    """The TDB-axis ⚿ chip stays — that's how the operator
    filters to the cookies-needed cohort post-v1.19.68."""
    library_html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    assert 'data-tdb-pill="cookies"' in library_html, (
        "v1.19.68: TDB-axis ⚿ filter chip must be preserved"
    )


def test_title_cell_red_glyph_still_fires_for_cookies():
    """The title-cell red ⚠ already fires on any unacked
    failure_kind — including cookies_expired. v1.19.68 doesn't
    touch this; the audit confirmed it already worked."""
    # Find the title-cell glyph cascade.
    idx = APP_JS.index("title-glyph-fail")
    block = APP_JS[max(0, idx - 1500):idx + 500]
    # The cascade gate is failure_kind && !failure_acked_at —
    # any failure_kind matches, including cookies_expired.
    assert "it.failure_kind && !it.failure_acked_at" in block, (
        "v1.19.68: title-cell red ⚠ must fire on any unacked "
        "failure_kind (cookies_expired included)"
    )


def test_cookies_expired_in_human_label_map():
    """The title-cell glyph's `human` label map must still
    include 'cookies_expired' so the hover tooltip reads
    'Cookies expired — click to view in INFO and ACK' on
    cookies-needed rows."""
    idx = APP_JS.index("'cookies_expired': 'Cookies expired'")
    assert idx > 0, (
        "v1.19.68: cookies_expired human label preserved in the "
        "title-cell glyph cascade"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_68_version_pin():
    """v1.19.68 bumped. Relaxed to v1.19.x prefix after v1.19.69
    continued the line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
