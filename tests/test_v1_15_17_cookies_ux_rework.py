"""v1.15.17 — cookies UX rework: yellow tone + ⚿ glyph + topbar
chip + STATUS filter + info-card FIX COOKIES action.

the user v1.15.16 planning:
- "change the TDB Source cookies needed from amber to something
  else as it makes it seem like its plex related"
- "we may have removed the amber ! that went with it? if that's
  true I think I would like to add it back but make it match the
  new color"
- "Let's also make it function the same as a failure with a
  status bar indicator, a filter in the filter section with the
  new !"
- "the appropriate info card actions ack failure with the color
  of the cookies issue since it's not the same kind of failure
  and suggestion of trying to add a cookies.txt to your config"

## Pre-fix

`.tdb-pill-cookies` was amber, sharing the tone with .stat-plex-
primary and other Plex-side surfaces — the cookies-needed pill
read as a Plex problem. There was no topbar chip for the
cookies-needed count (operators had to scroll the library to
find affected rows), no STATUS filter button to drill into them,
and the info-card recovery card had a non-actionable info tile
("DROP cookies.txt") instead of a clickable button + an ACK
FAILURE in the same red used by truly-dead URLs.

## Fix

Five-surface cookies family in warm yellow + ⚿ key glyph:

1. New `--yellow` CSS variable + `.tdb-pill-cookies` updated
   (color + ⚿ glyph in JS row pill renderer)
2. New topbar chip `op-tone-cookies` with badge ID
   `topbar-cookies-badge`, count from `/api/stats` `cookies.total`
3. New STATUS filter pill `attn-pill-cookies` (⚿) in
   library.html, server-side filter via `attn_pills=cookies`
4. New `/api/stats` payload key `cookies.total` sourced from
   the existing `failures_cookies` SQL bucket (canonical FAIL
   predicate restricted to cookies_expired)
5. Info-card cookies recipe: FIX COOKIES (interactive button,
   tone cookies, action `fix-cookies-link` → /settings#paths)
   + ACK FAILURE in cookies tone (yellow, not danger-red)
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"
API_PY = REPO / "app" / "web" / "api.py"


# ── 1. CSS color + variable ────────────────────────────────────


def test_cookies_css_variable_defined():
    """Color history for the cookies family token:
       v1.15.17: introduced --yellow (#ffe066) — too close to amber
       v1.15.43: --yellow → --brown (#c08552) biscuit brown
       v1.15.121: --brown → --lemon (#f5dd2b) clean lemon gold —
         the user: "I don't like the brown tdb cookies pill; yellow
         made more sense and looked better" — picked a yellow with
         lower R + higher G + lower B than v1.15.17 so it's
         distinct from amber's orange-warmth.
    The token must live in :root with the canonical shade pinned
    so a palette refactor can't change the cookies family by
    accident."""
    src = APP_CSS.read_text()
    assert "--lemon:" in src, (
        "v1.15.121: --lemon CSS variable required for cookies "
        "family (was --brown v1.15.43-.120, --yellow v1.15.17-.42)"
    )
    # Specific shade — distinct from --amber (#ffb84a).
    assert "#f5dd2b" in src, (
        "v1.15.121: --lemon value must be #f5dd2b (clean lemon "
        "gold, distinct from --amber #ffb84a)"
    )


def test_tdb_pill_cookies_uses_lemon():
    """The row pill must use --lemon (v1.15.121+; was --brown
    v1.15.43-.120, --yellow v1.15.17-.42, --amber pre-v1.15.17).
    Pin the var() reference so a future search-and-replace can't
    silently revert the tone."""
    src = APP_CSS.read_text()
    rule_anchor = src.index(".tdb-pill-cookies {")
    rule_block = src[rule_anchor:rule_anchor + 300]
    assert "color: var(--lemon);" in rule_block
    assert "color: var(--amber);" not in rule_block
    assert "color: var(--yellow);" not in rule_block
    assert "color: var(--brown);" not in rule_block


def test_op_tone_cookies_topbar_chip_style_defined():
    """v1.19.68 removed the separate COOKIES topbar badge (cookies-
    needed folded into the FAIL pill); v1.20.47 (CSS audit) removed the
    then-dead `.op-pill.op-tone-cookies` rule. The `--tone-cookies`
    semantic alias STAYS — it's still consumed by `.btn-cookies`, the
    TDB cookies-row pill, and the cookies STATUS filter chip."""
    src = OPS_CSS.read_text()
    assert ".op-pill.op-tone-cookies" not in src, (
        "v1.20.47: the op-tone-cookies topbar chip was removed (dead "
        "since the v1.19.68 badge removal) — don't reintroduce it"
    )
    assert "--tone-cookies" in src, (
        "--tone-cookies alias must remain — still used by .btn-cookies "
        "+ the cookies STATUS filter chip"
    )


def test_attn_pill_cookies_filter_style_removed():
    """v0.51.79 (CSS audit): the STATUS filter pill this styled was removed in
    v1.19.67; .attn-pill-cookies then had zero live emitters and was deleted as
    dead CSS. Was: asserted the rule existed + used --lemon (v1.15.43 --yellow →
    --brown, v1.15.121 --brown → --lemon). Now a removal guard so it can't creep
    back — the live cookies surfaces are pinned by test_btn_cookies_* below."""
    src = APP_CSS.read_text()
    assert ".attn-pill-cookies {" not in src, (
        "v0.51.79: .attn-pill-cookies is dead (STATUS chip gone v1.19.67) — "
        "don't re-add it"
    )


def test_btn_cookies_recovery_action_style_defined():
    """The info-card FIX COOKIES + ACK FAILURE buttons use the
    .btn-cookies class. Defined alongside the other btn-* tones.
    v1.15.43: --yellow → --brown. v1.15.121: --brown → --lemon."""
    src = APP_CSS.read_text()
    assert ".btn-cookies {" in src
    rule_anchor = src.index(".btn-cookies {")
    rule_block = src[rule_anchor:rule_anchor + 300]
    assert "var(--lemon)" in rule_block


# ── 2. Row pill glyph ──────────────────────────────────────────


def test_row_pill_uses_key_glyph():
    """Row pill for cookies_expired (when cookies are not present)
    must render with the ⚿ squared-key glyph, not the old amber
    ⚠. The key glyph anchors the auth-credential meaning at-a-
    glance even if the user isn't focused on the color."""
    src = APP_JS.read_text()
    # Find the row pill renderer's cookies-expired branch.
    cookies_anchor = src.index("if (it.failure_kind === 'cookies_expired')")
    cookies_block = src[cookies_anchor:cookies_anchor + 1500]
    # The cookies-not-present branch must use ⚿ glyph.
    assert "TDB ⚿" in cookies_block, (
        "v1.15.17: cookies row pill must use ⚿ (squared key) glyph"
    )
    # Old amber-warning glyph must be gone from this branch.
    no_present_anchor = cookies_block.index(
        "tdb-pill tdb-pill-cookies")
    no_present_section = cookies_block[no_present_anchor:no_present_anchor + 400]
    assert "TDB ⚠" not in no_present_section, (
        "v1.15.17: ⚠ glyph removed from cookies pill (clustered "
        "with other amber warnings — no longer reads as cookies-"
        "specific)"
    )


# ── 3. Topbar chip ─────────────────────────────────────────────


def test_topbar_cookies_badge_removed_in_v1_19_68():
    """v1.15.17 added a standalone <a id="topbar-cookies-badge">
    chip to surface cookies-needed rows distinctly from the
    dead-URL FAIL pulse. Brown tone, separate count.

    v1.19.68 removed it. Cookies-needed is still a failure_kind
    so the FAIL pulse counts it (via _FAILURES_SFA_WHERE_SQL,
    which already matches `failure_kind IS NOT NULL`). The
    yellow ⚿ row pill (TDB column) + the yellow ⚿ TDB-axis
    filter chip still let the operator identify + filter to
    cookies-needed rows. A standalone topbar badge added a
    third yellow ⚿ alert surface — the user's v1.19.66 audit
    follow-up flagged it as overkill duplication."""
    src = BASE_HTML.read_text()
    assert 'id="topbar-cookies-badge"' not in src, (
        "v1.19.68: topbar COOKIES badge must be removed"
    )
    assert 'id="topbar-cookies-count"' not in src, (
        "v1.19.68: topbar COOKIES count element must be removed"
    )


def test_topbar_cookies_badge_refresh_removed_from_app_js():
    """Companion to the badge removal — the JS that toggled the
    badge's hidden state must also be gone."""
    src = APP_JS.read_text()
    assert "$('#topbar-cookies-badge')" not in src, (
        "v1.19.68: refreshTopbarStatus must not still try to "
        "toggle the removed COOKIES badge"
    )


# ── 4. STATUS filter button + SQL ──────────────────────────────


def test_status_filter_has_cookies_button():
    """v1.15.17 added a ⚿ button to the STATUS (later ATTN)
    filter row.

    v1.19.67 removed it — same SQL was already reachable via
    the TDB axis ⚿ chip, so two chips per cohort was pure
    duplication. The cookies cohort is still filterable via
    the TDB axis (`?tdb_pills=cookies`) or — for URL backwards
    compat — via `?attn_pills=cookies` (kept as a no-op in the
    JS allowlist).

    Test now asserts the button is GONE from the ATTN row."""
    src = LIBRARY_HTML.read_text()
    attn_row_start = src.index('aria-label="ATTN pill filter"')
    attn_row_end = src.index("</div>", attn_row_start)
    attn_row = src[attn_row_start:attn_row_end]
    assert 'data-attn-pill="cookies"' not in attn_row, (
        "v1.19.67: ⚿ button removed from ATTN row (duplicate of "
        "the TDB axis ⚿ chip)"
    )


def test_attn_pills_cookies_branch_in_api():
    """Server-side: attn_pills=cookies must filter to rows whose
    failure_kind = 'cookies_expired'.

    v1.15.17 originally also gated on the title + per-section
    ack predicates (so the filter agreed with the FAIL chip's
    count). v1.15.38 BROADENED the cookies filter — the user
    reported "the yellow TDB Pill filter wasn't filtering
    properly," and the row-level yellow ⚿ pill renders
    regardless of ack state. The COOKIES count was also
    realigned in v1.15.38 so chip-count + filter match again,
    just at the broader (no-ack) shape.

    NOTE: there are two `elif p == "cookies":` blocks in api.py
    — the older one is in the `tdb_pills` axis (gated on `not
    cookies_present`) and predates v1.15.17. Anchor on the
    distinctive v1.15.17 marker comment to find the new
    attn_pills branch specifically."""
    src = API_PY.read_text()
    marker = "# v1.15.17: cookies-needed STATUS pill"
    marker_anchor = src.index(marker)
    branch_block = src[marker_anchor:marker_anchor + 1500]
    assert "t.failure_kind = 'cookies_expired'" in branch_block
    # v1.15.38: ack predicates are GONE from the filter SQL.
    # See test_v1_15_38_cookies_pill_filter_count_alignment for
    # the canonical assertion.
    sql_anchor = branch_block.index("attn_branches.append(")
    sql_block = branch_block[sql_anchor:sql_anchor + 400]
    assert "failure_acked_at IS NULL" not in sql_block
    assert "sfa.acked_at IS NULL" not in sql_block


def test_api_stats_payload_includes_cookies_total():
    """The /api/stats response must include a top-level cookies
    block with a `total` field sourced from the existing
    failures_cookies SQL bucket. The JS topbar refresh reads
    this exact path."""
    src = API_PY.read_text()
    # Find the cookies surface in the response payload.
    payload_anchor = src.index('"cookies": {')
    payload_block = src[payload_anchor:payload_anchor + 500]
    assert '"total":' in payload_block
    assert "failures_cookies" in payload_block, (
        "v1.15.17: cookies.total must come from the existing "
        "failures_cookies SQL bucket (which uses the canonical "
        "FAIL predicate)"
    )


# ── 5. Info-card recovery actions ─────────────────────────────


def test_cookies_recipe_has_fix_cookies_action():
    """The cookies_expired recipe must include a FIX COOKIES
    interactive button with action 'fix-cookies-link' and
    cookies tone. Replaces the pre-fix non-interactive 'DROP
    cookies.txt' info tile."""
    src = API_PY.read_text()
    recipe_anchor = src.index('"cookies_expired": [')
    recipe_block = src[recipe_anchor:recipe_anchor + 2500]
    assert '"action": "fix-cookies-link"' in recipe_block
    assert '"label": "FIX COOKIES"' in recipe_block
    # Must be interactive (clickable button, not info tile).
    fix_anchor = recipe_block.index('"label": "FIX COOKIES"')
    fix_block = recipe_block[fix_anchor:fix_anchor + 500]
    assert '"interactive": True' in fix_block
    assert '"tone": "cookies"' in fix_block


def test_cookies_recipe_has_ack_in_cookies_tone():
    """The cookies_expired recipe must include an ACK FAILURE
    action in the cookies (yellow) tone, not danger (red).
    the user's framing: cookies-needed isn't a permanent loss
    like a dead URL, so the ack visual should reflect that."""
    src = API_PY.read_text()
    recipe_anchor = src.index('"cookies_expired": [')
    recipe_block = src[recipe_anchor:recipe_anchor + 2500]
    # Find the ACK FAILURE entry inside the cookies recipe.
    ack_anchor = recipe_block.index('"label": "ACK FAILURE"')
    ack_block = recipe_block[ack_anchor:ack_anchor + 500]
    assert '"tone": "cookies"' in ack_block, (
        "v1.15.17: ACK FAILURE under cookies_expired must use "
        "tone='cookies' (yellow), not the danger-red used by "
        "dead-URL kinds"
    )


def test_tone_class_maps_cookies_to_btn_cookies():
    """The JS TONE_CLASS dispatch must map tone='cookies' to
    the .btn-cookies CSS class. Without this, the server's
    tone='cookies' falls through to no class and the button
    renders without color."""
    src = APP_JS.read_text()
    tone_class_anchor = src.index("const TONE_CLASS = {")
    tone_class_block = src[tone_class_anchor:tone_class_anchor + 800]
    assert "cookies: 'btn-cookies'" in tone_class_block


def test_info_card_dispatcher_handles_fix_cookies_link():
    """The recovery-option click handler must intercept
    action='fix-cookies-link' and navigate to /settings#paths."""
    src = APP_JS.read_text()
    # Find the dispatcher block (starts with "if (act === 'fix-cookies-link')").
    assert "if (act === 'fix-cookies-link')" in src, (
        "v1.15.17: dispatcher must include a fix-cookies-link branch"
    )
    handler_anchor = src.index("if (act === 'fix-cookies-link')")
    handler_block = src[handler_anchor:handler_anchor + 500]
    assert "/settings#paths" in handler_block
    # Closes the info dialog before navigating.
    assert "closeInfoDialog()" in handler_block
