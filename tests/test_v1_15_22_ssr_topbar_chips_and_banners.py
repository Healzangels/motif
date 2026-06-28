"""v1.15.22 — SSR topbar chips + banners (broaden v1.15.21 flash fix).

the user (after v1.15.21 nav fix): "lets check everywhere make sure
this bug doesn't exist anywhere else"

## Audit findings

Same flash class (JS-reveal-after-paint of server-hidden elements)
exists across multiple surfaces:

HIGH IMPACT (visible on every page):
- Topbar FAIL chip (#topbar-failures-badge) — `hidden` attr, JS
  reveals on stats.failures.total > 0
- Topbar COOKIES chip (#topbar-cookies-badge) — same pattern
  (added v1.15.17)
- Topbar DROP chip (#topbar-drops-badge) — same pattern
- Topbar DISK chip (#topbar-disk-badge) — same pattern
- Banner #dry-run-banner (style="display:none") — JS reveals on
  stats.dry_run
- Banner #paths-banner (style="display:none") — JS reveals when
  paths not configured

NOT INCLUDED (deferred):
- UPD chip — updates_pending SQL is a much larger query with
  per-section gating + URL-diff. Extraction left for follow-up.
- Dashboard cards (only visible on /dash, not every page).
- Settings ENV badges (only flash for env-override users).
- Library resolution chips (one paint per library page).
- Settings tab panels (only flash on /settings#hash deep link).

## Fix

Register `topbar_ssr_state()` Jinja env global that returns a
dict with all the data needed to SSR the high-impact surfaces in
a single SQL query. base.html uses Jinja conditionals on
`hidden` / `style="display:none"` so HTML the browser receives
already has the right state. JS-side refresh continues to update
on the next /api/stats poll and is a no-op when SSR matched.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"


# ── Helper + Jinja registration ───────────────────────────────


def test_topbar_ssr_state_helper_defined_and_registered():
    """The helper must exist and be registered as a Jinja env
    global. Pin both so a future refactor can't silently break
    SSR by renaming or unregistering."""
    src = API_PY.read_text()
    assert "def _topbar_ssr_state(" in src
    assert 'templates.env.globals["topbar_ssr_state"]' in src


def test_helper_returns_all_documented_keys():
    """The dict shape is part of base.html's contract — every
    SSR-eligible chip + banner reads its own key. Pin every
    documented key so a partial refactor can't silently drop one."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _topbar_ssr_state(")
    fn_end = src.index("templates.env.globals", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    expected = [
        '"failures_count"',
        '"cookies_count"',
        '"drops_count"',
        '"disk_low"',
        '"disk_free_mb"',
        '"dry_run"',
        '"paths_ready"',
    ]
    for key in expected:
        assert key in fn_body, (
            f"v1.15.22: topbar SSR state must include {key} "
            f"(consumed by base.html template)"
        )


def test_helper_uses_canonical_failures_predicate():
    """failures_count + cookies_count must use _FAILURES_SFA_FROM_SQL
    + _FAILURES_SFA_WHERE_SQL — the canonical FAIL chip predicate
    that /api/stats uses. SSR must match the JS-side count exactly,
    otherwise the JS-side update on the next poll will visibly
    correct the SSR — defeating the purpose."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _topbar_ssr_state(")
    fn_end = src.index("templates.env.globals", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    # FAIL count: still uses the canonical ack-aware predicate.
    assert "_FAILURES_SFA_FROM_SQL" in fn_body
    assert "_FAILURES_SFA_WHERE_SQL" in fn_body
    # COOKIES count: v1.15.38 switched to the broader
    # _COOKIES_PILL_WHERE_SQL (no ack predicates) so chip-count
    # matches the v1.15.38 attn_pills=cookies filter + the
    # row-level yellow ⚿ pill. The shared predicate is
    # `failure_kind = 'cookies_expired'`, captured in the
    # constant.
    assert "_COOKIES_PILL_WHERE_SQL" in fn_body


def test_helper_falls_back_safely_on_db_error():
    """If the DB read fails (corrupt SQLite, schema mid-migration,
    first-run state), the helper must return safe defaults
    (counts=0, banners hidden) rather than raising. JS still
    recovers from /api/stats on the next poll. Same fallback
    shape as v1.15.21's nav helper.
    v1.15.34: bare `except Exception:` was upgraded to
    `except Exception as e:` + log.warning so DB hiccups become
    visible (pre-fix the silent absorb hid FAIL/COOKIES/DISK
    LOW banners on every page-load with zero operator
    visibility). The fallback behavior (safe defaults) is
    unchanged — the upgrade is logging-only."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _topbar_ssr_state(")
    fn_end = src.index("templates.env.globals", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    # v1.15.34 form: "except Exception as e:" + log.warning(...).
    assert "except Exception as e:" in fn_body
    assert "log.warning(" in fn_body
    assert "topbar_ssr_state failed" in fn_body


# ── base.html consumption ──────────────────────────────────────


def test_base_html_calls_topbar_ssr_state_once():
    """The Jinja `{% set _ssr_top = topbar_ssr_state() %}` must
    fire exactly once per render (not per-chip) so the SQL query
    runs once. Pin via substring presence + a sanity check that
    the variable name is reused, not redeclared."""
    src = BASE_HTML.read_text()
    assert "{% set _ssr_top = topbar_ssr_state() %}" in src
    # Helper should only be invoked once — exactly one set
    # statement.
    assert src.count("topbar_ssr_state()") == 1, (
        "v1.15.22: helper should be called exactly once per render "
        "(repeated calls = repeated SQL queries)"
    )


def test_dry_run_banner_uses_ssr_visibility():
    """The dry-run banner must conditionally apply
    style="display:none" based on _ssr_top.dry_run. Pre-fix the
    style was unconditional and JS revealed it via stats.dry_run
    — flash on every page load for dry-run users."""
    src = BASE_HTML.read_text()
    banner_anchor = src.index('id="dry-run-banner"')
    banner_block = src[banner_anchor:banner_anchor + 600]
    assert "_ssr_top.dry_run" in banner_block
    # Defensive: must NOT have unconditional display:none anymore.
    forbidden = (
        '<div id="dry-run-banner" class="dry-run-banner" '
        'style="display:none">'
    )
    assert forbidden not in src, (
        "v1.15.22: unconditional display:none on dry-run-banner "
        "resurfaced — would re-introduce the flash for dry-run users"
    )


def test_paths_banner_uses_ssr_visibility():
    """The paths banner shows when paths_ready is False (inverse
    of the chip pattern). Pin the inverted condition + defensive
    guard against the unconditional display:none."""
    src = BASE_HTML.read_text()
    banner_anchor = src.index('id="paths-banner"')
    banner_block = src[banner_anchor:banner_anchor + 600]
    assert "_ssr_top.paths_ready" in banner_block
    forbidden = (
        '<div id="paths-banner" class="paths-banner" '
        'style="display:none">'
    )
    assert forbidden not in src


def test_topbar_failures_chip_uses_ssr_count():
    """The FAIL chip must:
    1. SSR `hidden` attribute based on _ssr_top.failures_count > 0
    2. SSR the count text inside #topbar-failures-count
    Both are needed: hidden gates layout, count text gates the
    visible "X FAIL" label."""
    src = BASE_HTML.read_text()
    chip_anchor = src.index('id="topbar-failures-badge"')
    chip_end = src.index("</a>", chip_anchor)
    chip_block = src[chip_anchor:chip_end]
    # SSR hidden conditional.
    assert "_ssr_top.failures_count" in chip_block
    # SSR count text.
    assert "{{ _ssr_top.failures_count }}" in chip_block


def test_topbar_cookies_chip_removed_in_v1_19_68():
    """v1.15.17 added the COOKIES chip with the same JS-reveal
    pattern. v1.19.68 removed the chip entirely (cookies-needed
    now flows through the FAIL pulse — same failure_kind
    family). No SSR consumer for cookies_count on the topbar."""
    src = BASE_HTML.read_text()
    assert 'id="topbar-cookies-badge"' not in src, (
        "v1.19.68: COOKIES badge removed; this SSR pin is now "
        "a negative assertion"
    )


def test_topbar_drops_chip_uses_ssr_count():
    """DROP chip — themes ThemerrDB stopped publishing."""
    src = BASE_HTML.read_text()
    chip_anchor = src.index('id="topbar-drops-badge"')
    chip_end = src.index("</a>", chip_anchor)
    chip_block = src[chip_anchor:chip_end]
    assert "_ssr_top.drops_count" in chip_block
    assert "{{ _ssr_top.drops_count }}" in chip_block


def test_topbar_disk_chip_uses_ssr_visibility():
    """DISK chip is binary visible (disk_low True/False) + a
    free-mb count. Pin both."""
    src = BASE_HTML.read_text()
    chip_anchor = src.index('id="topbar-disk-badge"')
    chip_end = src.index("</a>", chip_anchor)
    chip_block = src[chip_anchor:chip_end]
    assert "_ssr_top.disk_low" in chip_block
    # disk_free_mb may be None (paths not configured) so the
    # template handles that with `is not none`.
    assert "_ssr_top.disk_free_mb" in chip_block
