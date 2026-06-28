"""v1.19.72 — audit follow-ups to v1.19.71's new_theme_available kind.

Post-ship audit of v1.19.71 surfaced surfaces that the v1.19.71
pass didn't update — places where the SRC=— gate hard-blocked
new_theme_available rows that should have been let through.

## H1 — SOURCE-menu ACCEPT UPDATE button gate

Both `app/web/static/app.js` (line ~8645) and
`app/web/static/lib/menu-actions.js` (line ~117) gated the
ACCEPT UPDATE branch on `srcLetter !== "-"` without the
v1.19.71 new_theme_available exception. Net effect: blue !UPD
glyph + TDB↑ pill rendered on a SRC=— new-theme row, but
opening the SOURCE menu showed no ACCEPT UPDATE button. Cardinal
flow silently broken.

Fix: widen both gates with the same
`|| pending_update_kind === 'new_theme_available'` exception
used in the pill / glyph paths. Symmetric tighten of the
PRIMARY ACQUISITION fallthrough gate at app.js:~8732 +
menu-actions.js:~157 so a SRC=— new-theme row doesn't render
both ACCEPT UPDATE and DOWNLOAD TDB.

## H2 — attn_pills=update SQL filter

`app/web/api.py:~2164` — the `attn_branches` `update` chip
required `({_SRC_LETTER_SQL}) != '-'` AND the presence-gate
sub-chain. No new_theme_kind helper. Operators clicking the
ATTN!update chip filtered out the very rows the feature
targets.

Fix: widen the SRC=— gate + presence gate with the helper.

## M1 — ACCEPT confirm dialog

`app/web/static/app.js:~3416` — T-source branch said "overwrite
the current theme file in this section. The previous file is
unrecoverable." Wrong for SRC=— new-theme rows where there is
no current file.

Fix: add a 3rd branch for SRC=— new_theme_available with
truthful copy.

## M2 — INFO-card pending diff tile

`app/web/static/app.js` `renderPendingUpdateDiff` tile renderer
returns "no recorded source" when both vid + url are empty.
For SRC=— new-theme rows that's always the CURRENT tile state
(no pu.old_youtube_url, no lf). "No recorded source" reads as
a bug.

Fix: branch the empty-state copy on
`pu.kind === 'new_theme_available' && slot === 'current'`.

## M3 — themes_added_by_sync notification title

`app/core/worker.py:~1122` — title was "🎵 N new themes added
by sync". After v1.19.71, most "new" themes are pending_updates
the user has to ACCEPT manually — not auto-added. "Discovered"
is truthful regardless of which branch each row took.

## M4 — bulk decline-all confirm dialog

`app/web/templates/library.html:~545` button tooltip + the JS
confirm prompt at app.js:~13037 — neither mentioned that
DISMISS ALL now also dismisses new_theme_available discoveries
on SRC=— rows.

## M5 — `_pending_update_new_theme_kind_sql` decision-blindness

The helper at api.py:~1351 returns TRUE for accepted / declined
rows too. Safe at every current call site because each outer
query gates on `decision='pending'` BEFORE evaluating the
helper. Document the contract in the docstring so a future
caller doesn't forget the outer gate.

## L1 — Dead fallthrough TDB-pill block tooltip

`app/web/static/app.js:~8425` — the v1.18.65 "unreachable
post-fix" defensive fallthrough block was updated by v1.19.71
to honor the new SRC=— exception but didn't gain the
new_theme_available tooltip variant.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
MENU_JS = (
    REPO / "app" / "web" / "static" / "lib" / "menu-actions.js"
).read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()


# ── H1: SOURCE-menu ACCEPT UPDATE gate ──────────────────────────


def test_h1_app_js_accept_gate_widened_with_new_theme_exception():
    """app.js SOURCE-menu CONTEXTUAL PROMPT block must include the
    SRC=— exception for new_theme_available — otherwise the blue
    !UPD glyph fires on a row with no ACCEPT UPDATE button."""
    # The gate predicate must reference both srcLetter !== '-' AND
    # the new kind exception.
    assert "acceptUpdateGateOk" in APP_JS, (
        "v1.19.72 H1: gate should be hoisted into a named const "
        "so the symmetric PRIMARY ACQUISITION exclusion can mirror it"
    )
    # v1.20.0: the gate is now the consolidated pendingUpdateActionable()
    # helper; the SRC=— + new_theme_available exception lives there
    # (single source of truth — the rollover-audit altitude fix).
    gate_idx = APP_JS.index("const acceptUpdateGateOk")
    gate_block = APP_JS[gate_idx: gate_idx + 80]
    assert "pendingUpdateActionable(it)" in gate_block
    helper_idx = APP_JS.index("function pendingUpdateActionable(")
    helper = APP_JS[helper_idx: helper_idx + 300]
    assert "computeSrcLetter(it) !== '-'" in helper
    assert "pending_update_kind === 'new_theme_available'" in helper


def test_h1_app_js_primary_acquisition_excludes_new_theme_dash():
    """The PRIMARY ACQUISITION fallthrough (DOWNLOAD TDB) must NOT
    fire for SRC=— new_theme_available rows — ACCEPT UPDATE covers
    that case and we don't want two buttons doing the same thing."""
    # Find the const that names the exception.
    assert "srcDashEscapeOk" in APP_JS, (
        "v1.19.72 H1: PRIMARY ACQUISITION should hoist the SRC=— "
        "escape hatch into a named const so its exclusion of the "
        "new-theme path is visible"
    )
    idx = APP_JS.index("const srcDashEscapeOk")
    block = APP_JS[idx: idx + 200]
    assert "srcLetter === '-'" in block
    assert "pending_update_kind !== 'new_theme_available'" in block


def test_h1_menu_actions_accept_gate_widened():
    """The headless menu-actions.js mirror must apply the same
    widening (mirror-drift class-9)."""
    # The CONTEXTUAL PROMPT block.
    idx = MENU_JS.index("CONTEXTUAL PROMPT")
    block = MENU_JS[idx: idx + 800]
    assert 'srcLetter !== "-"' in block
    assert 'pending_update_kind === "new_theme_available"' in block


def test_h1_menu_actions_primary_acquisition_excludes_new_theme():
    """Same PRIMARY ACQUISITION exclusion in the menu-actions
    mirror."""
    assert "srcDashEscapeOk" in MENU_JS
    idx = MENU_JS.index("const srcDashEscapeOk")
    block = MENU_JS[idx: idx + 200]
    assert 'srcLetter === "-"' in block
    assert 'pending_update_kind !== "new_theme_available"' in block


def test_h1_accept_tooltip_has_new_theme_dash_variant():
    """The ACCEPT UPDATE tooltip ternary must include a 3rd branch
    for SRC=— new_theme_available so the operator gets accurate
    "first theme" copy instead of "replace the current theme"."""
    idx = APP_JS.index("const acceptTip = ")
    block = APP_JS[idx: idx + 1500]
    # Tooltip must mention the new-theme case + describe the action
    # without invoking "replace" / "overwrite" (no existing file).
    assert "'new_theme_available'" in block
    assert (
        "first" in block.lower()
        or "newly-discovered" in block.lower()
    ), (
        "v1.19.72 H1: tooltip for SRC=— new-theme must avoid "
        "'replace/overwrite' phrasing (there's no existing file)"
    )


# ── H2: attn_pills=update SQL filter ────────────────────────────


def test_h2_attn_update_filter_widens_src_gate():
    """The attn_pills=update SQL must allow new_theme_available
    rows through its SRC=— gate. The widening + the new kind
    helper must coexist."""
    # Anchor: the v1.19.72 marker on the attn-pills SRC=—
    # widening. There are two `urls_match prompts on P rows`
    # sites in api.py; the v1.19.72 marker is unique to the
    # attn site so anchoring on it doesn't drift.
    marker_idx = API_PY.index(
        "v1.19.72: SRC=— exception for new_theme_available\n"
        "                    # mirrors the JS computeTdbPill"
    )
    # The widening must immediately follow the marker.
    block = API_PY[marker_idx: marker_idx + 600]
    assert (
        "_pending_update_new_theme_kind_sql('t', 'pi')" in block
    )


def test_h2_attn_update_filter_widens_presence_gate():
    """Same query — presence gate also widened (local_files OR
    overrides OR placements OR sidecar OR new_theme_kind)."""
    # Find the SRC=— widening and walk forward to find the
    # presence gate (containing `local_theme_file = 1`).
    src_idx = API_PY.index(
        "v1.19.72: SRC=— exception for new_theme_available\n"
        "                    # mirrors the JS computeTdbPill"
    )
    # Walk forward ~3000 chars to capture the presence gate.
    block = API_PY[src_idx: src_idx + 3000]
    # The presence gate must include a new_theme_kind OR-branch.
    pre_count = block.count("_pending_update_new_theme_kind_sql")
    assert pre_count >= 2, (
        f"v1.19.72 H2: expected >=2 references to the kind helper "
        f"in the attn_update query (one in SRC=— gate, one in "
        f"presence gate), found {pre_count}"
    )


# ── M1: acceptUpdate confirm dialog 3rd branch ──────────────────


def test_m1_accept_confirm_has_new_theme_dash_branch():
    """acceptUpdate's promptText ladder must have a branch for
    SRC=— new_theme_available with truthful copy."""
    idx = APP_JS.index("async function acceptUpdate")
    fn = APP_JS[idx: idx + 4000]
    # Branch predicate.
    assert "pending_update_kind === 'new_theme_available'" in fn
    assert "computeSrcLetter(rowItem) === '-'" in fn
    # Copy: must mention "no existing theme" / "first one" /
    # "brand-new" and AVOID "overwrite" / "unrecoverable".
    new_theme_idx = fn.index("pending_update_kind === 'new_theme_available'")
    nearby = fn[new_theme_idx: new_theme_idx + 800]
    has_truthful = (
        "no existing theme" in nearby
        or "first one" in nearby
        or "brand-new" in nearby
    )
    assert has_truthful, (
        "v1.19.72 M1: new-theme branch must use accurate language "
        "('no existing theme', 'first one', 'brand-new'), not the "
        "overwrite/unrecoverable language from the T-source branch"
    )


# ── M2: renderPendingUpdateDiff empty tile copy ─────────────────


def test_m2_diff_tile_empty_branches_on_new_theme_kind():
    """The diff-tile empty branch must use kinder copy for SRC=—
    new_theme rows (always hits this branch for CURRENT tile)."""
    # Anchor on the function definition (skips the first
    # `renderPendingUpdateDiff` reference which is a call site).
    idx = APP_JS.index("function renderPendingUpdateDiff")
    fn = APP_JS[idx: idx + 10000]
    # New copy must reference the new-theme case AND mention
    # "no prior theme" / "brand-new" instead of just "no recorded
    # source".
    assert "pu.kind === 'new_theme_available'" in fn
    assert "no prior theme" in fn or "brand-new" in fn


# ── M3: notification title says "discovered", not "added" ───────


def test_m3_themes_added_by_sync_title_says_discovered():
    """v1.21.6: themes_added_by_sync no longer fires a standalone
    notification (with its "discovered by sync" title) — it folds a
    "🎵 New:" titles section into the single sync_completed message.
    The v1.19.72 M3 concern (don't claim "added" for the mixed
    pending/auto path) is now moot: the neutral "New:" header makes
    no claim about how each title got there. Pin the section header
    + the absence of the old standalone-title phrasings."""
    # v1.22.45: the "🎵 New:" header is now handed to the section-grouping
    # formatter (grouped by Plex library) rather than appended inline; the
    # neutral header literal is still present.
    assert '"🎵 New:"' in WORKER_PY
    assert "format_section_grouped_lines" in WORKER_PY
    assert "new themes discovered by sync" not in WORKER_PY
    assert "new themes added by sync" not in WORKER_PY


# ── M4: decline-all tooltip + confirm mentions new themes ───────


def test_m4_decline_all_button_tooltip_mentions_new_themes():
    """The library.html KEEP ALL CURRENT button tooltip must
    explain the v1.19.71 new-theme dismissal."""
    idx = LIBRARY_HTML.index("library-decline-all-updates-btn")
    block = LIBRARY_HTML[idx: idx + 600]
    assert "newly-discovered" in block.lower() or "new theme" in block.lower()


def test_m4_global_decline_confirm_mentions_new_themes():
    """The JS confirm() prompt for the no-selection (global)
    decline-all path must also mention the new-theme dismissal."""
    # Anchor: the confirm() with "Dismiss N pending update".
    idx = APP_JS.index("Dismiss ${pending} pending update")
    block = APP_JS[idx: idx + 800]
    assert "newly-discovered" in block.lower() or "new_theme_available" in block


# ── M5: kind helper decision-blindness documented ───────────────


def test_m5_kind_helper_docstring_notes_decision_blindness():
    """The helper docstring must call out that it doesn't filter
    on pu.decision (relies on outer gates)."""
    idx = API_PY.index("def _pending_update_new_theme_kind_sql")
    fn = API_PY[idx: idx + 2500]
    # Must mention decision-blind / decision='pending' / outer gate.
    has_note = (
        "DECISION-BLIND" in fn
        or "decision-blind" in fn
        or "decision='pending'" in fn
    )
    assert has_note, (
        "v1.19.72 M5: docstring must document that this helper "
        "doesn't filter on pu.decision and relies on outer gate"
    )


# ── L1: dead-fallthrough TDB pill has new tooltip ───────────────


def test_l1_dead_fallthrough_pill_has_new_theme_tooltip():
    """The v1.18.65 'unreachable' fallthrough block in the inline
    TDB-pill render must have a new_theme_available tooltip variant
    so a future re-ordering of branches doesn't surface stale copy."""
    # The fallthrough is anchored by the v1.12.108 comment.
    idx = APP_JS.index("v1.12.108: restore the blue .tdb-pill-update")
    # Walk forward to find the second `it.pending_update` gate
    # block in the inline render.
    block = APP_JS[idx: idx + 3000]
    # Count new_theme_available tooltip occurrences in this region.
    occ = block.count("new_theme_available")
    assert occ >= 2, (
        f"v1.19.72 L1: dead-fallthrough must have at least 2 "
        f"references to new_theme_available (gate + tooltip), "
        f"found {occ}"
    )


# ── Version pin ────────────────────────────────────────────────


def test_v1_19_72_version_pin():
    """Loose prefix — later tags continue the v1.19.x line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py


# ── Mirror-drift guard: count gate sites ────────────────────────


def test_v1_19_71_gate_sites_still_all_present():
    """v1.19.72 must not have regressed the v1.19.71 gate count.
    The 4 JS sites (computeTdbPill + 2 inline + title-glyph) +
    the SOURCE-menu gate + helper-callable sites must still all
    carry the SRC=— exception."""
    # Simple substring count of the canonical exception literal.
    # Both quoted forms (single vs double) get counted.
    n_app = (
        APP_JS.count("pending_update_kind === 'new_theme_available'")
    )
    # Lower-bound: 5 sites in app.js (computeTdbPill + title-glyph
    # gate + 2 inline TDB-pill blocks with gate AND tooltip
    # branches + the SOURCE-menu acceptUpdateGateOk + the
    # acceptUpdate confirm dialog branch + the M2 diff-tile
    # branch). Reality: more.
    assert n_app >= 5, (
        f"v1.19.72: expected >=5 SRC=— exception gates in app.js, "
        f"found {n_app}. Regression of v1.19.71 + v1.19.72 H1."
    )
    # menu-actions.js uses double-quotes.
    n_menu = (
        MENU_JS.count('pending_update_kind === "new_theme_available"')
    )
    assert n_menu >= 1, (
        f"v1.19.72: expected >=1 SRC=— exception gate in "
        f"menu-actions.js, found {n_menu}"
    )
