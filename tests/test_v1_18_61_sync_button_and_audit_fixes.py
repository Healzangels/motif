"""v1.18.61 — SYNC button live-sync + undefined CSS tokens + audit findings.

the user's report + full audit follow-up.

## Fix A — SYNC THEMERRDB button text live-sync

the user: "Dashboard I am still only seeing sync themerrdb when the
joint action of themerrdb and refresh plex is checked in
settings. That button text should update to reflect that."

Pre-v1.18.61, `refreshTopbarStatus` updated `dataset.origLabel`
on every poll based on `autoEnum`, but the `<button>.textContent`
only resynced when transitioning OUT of busy state via
`setSyncButtonState('idle')`. Toggling the setting in /settings
while the button was idle left the stale label visible until
the next manual sync completed.

Fix: when the button is idle (not busy, not in optimistic
placeholder, not in ✓ DONE flash), sync textContent to
dataset.origLabel on every tick. Refactored busy/unlock checks
to use a `knownBusyLabels` Set so the live-sync guard reads
cleanly.

## Fix B — undefined CSS tokens render colorless

Audit found 5 token references with no `:root` definition:
  - --bg-ridge (4 use sites, including .form-actions /
    .form-hint-divider dashed borders)
  - --bg-recess (1 use, .kpi card background)
  - --ink (1 use, .scan-filter-active border)
  - --ink-dim (2 uses, .kpi-lbl + .kind-exact_match)
  - --ink-faint (1 use, .scan-filter-active background)

All rules silently rendered with `color: unset` / `border-color:
transparent`. Visible breakage on /admin/orphans + the scan
diagnostic pages.

Fix: remap to canonical motif tokens:
  --bg-ridge  → --line       (separator role)
  --bg-recess → --bg         (deepest background)
  --ink       → --fg         (primary text)
  --ink-dim   → --fg-dim     (dimmed text)
  --ink-faint → --fg-mute    (faintest text)

## Fix C — v1.18.60 helper mis-maps plex_mt for collections

`_teardown_plex_api_artifacts_for_placements` (v1.18.60) had:

  plex_mt = "show" if media_type == "tv" else "movie"

For media_type='collection', this returned 'movie'. The fallback
rk lookup queried `WHERE guid_tmdb=? AND media_type='movie'`,
which silently missed every collection row whose theme_id JOIN
didn't return a hit. Primary path usually succeeded, so the bug
was rarely visible — but orphan-promotion races + pre-resolve
windows + fresh installs would silently skip teardown.

Caught by the v1.18.61 audit. Fix uses an explicit map:
{"tv": "show", "collection": "collection"} with 'movie' default.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
API_PY = REPO / "app" / "web" / "api.py"
PROJECT_HISTORY = REPO / "docs" / "PROJECT_HISTORY.md"
# Note: per the .gitignore convention, docs/AUDIT_*.md are local
# working artifacts — the findings + roadmap that live in the
# repo go into docs/PROJECT_HISTORY.md (a tracked file). The
# v1.18.61 audit findings are catalogued there under § 20.


# ── Fix A: SYNC button live-sync ────────────────────────────


def test_sync_button_live_sync_block_exists():
    """refreshTopbarStatus must contain the v1.18.61 live-sync
    block that resyncs textContent to dataset.origLabel when
    the auto_enum_after_sync setting toggles."""
    src = APP_JS.read_text()
    assert "knownBusyLabels" in src, (
        "v1.18.61: must define a knownBusyLabels Set for the "
        "live-sync guard"
    )
    # The Set must include both candidate busy labels.
    idx = src.index("knownBusyLabels")
    block = src[idx:idx + 500]
    assert "'// SYNCING THEMERRDB…'" in block
    assert "'// SYNCING THEMERRDB + REFRESH PLEX…'" in block


def test_sync_button_live_sync_guards_against_busy_state():
    """The live-sync must NOT fire when the button is busy
    (`dashSyncBtnBusy` true) or owned by syncWatcher. Pin the
    guard predicate so a future refactor can't strip the
    safety conditions."""
    src = APP_JS.read_text()
    idx = src.index("knownBusyLabels.has(syncBtn.textContent)")
    # Walk back to the if-statement that contains it.
    block_start = src.rfind("if (", 0, idx)
    guard = src[block_start:idx + 200]
    # Must check: !dashSyncBtnBusy + !syncWatcher + !known-busy
    # + !✓ DONE + textContent != origLabel.
    assert "!dashSyncBtnBusy" in guard
    assert "!syncWatcher" in guard
    assert "✓ DONE" in guard
    assert "syncBtn.textContent !== syncBtn.dataset.origLabel" in guard


def test_sync_button_live_sync_writes_textcontent():
    """The live-sync branch must actually assign textContent
    (regression guard against an empty if-body)."""
    src = APP_JS.read_text()
    # Find the live-sync block's assignment.
    idx = src.index("syncBtn.textContent !== syncBtn.dataset.origLabel")
    block = src[idx:idx + 400]
    assert "syncBtn.textContent = syncBtn.dataset.origLabel" in block


# ── Fix B: undefined CSS tokens remapped ────────────────────


def test_no_undefined_ink_tokens_remain():
    """The five undefined --ink* / --bg-ridge / --bg-recess
    tokens must NOT appear in app.css after the v1.18.61 remap.
    They had no :root definition so every reference silently
    rendered with the CSS default."""
    src = APP_CSS.read_text()
    forbidden = ["var(--ink)", "var(--ink-dim)", "var(--ink-faint)",
                 "var(--bg-ridge)", "var(--bg-recess)"]
    for token in forbidden:
        # Allow in comments — strip comment lines first.
        stripped = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
        assert token not in stripped, (
            f"v1.18.61: undefined token {token!r} must not survive "
            f"in the cascade (no :root definition exists; would "
            f"render colorless)"
        )


def test_form_actions_uses_defined_token():
    """`.form-actions` border-top must use --line (defined)
    not --bg-ridge (undefined). Pre-fix the dashed top border
    rendered transparent."""
    src = APP_CSS.read_text()
    rule_idx = src.index(".form-actions {")
    rule_end = src.index("}", rule_idx)
    rule = src[rule_idx:rule_end]
    assert "border-top: 1px dashed var(--line)" in rule


def test_form_hint_divider_uses_defined_token():
    """`.form-hint-divider` border-top must use --line."""
    src = APP_CSS.read_text()
    rule_idx = src.index(".form-hint-divider {")
    rule_end = src.index("}", rule_idx)
    rule = src[rule_idx:rule_end]
    assert "border-top: 1px dashed var(--line)" in rule


# v0.51.70: test_kpi_card_uses_defined_tokens removed — the whole Scans-page CSS block
# (.kpi/.kind-*/.scan-*) it pinned was deleted as dead (zero template/JS emitters; the
# emitting UI was removed v0.50.89). The docstring's "/admin/orphans" claim was stale —
# orphans.html never used .kpi.


def test_audit_guard_for_undefined_tokens():
    """Forward-looking guard: scan app.css for any `var(--name)`
    references where `--name` is not defined in `:root {}` or
    a scoped rule. If a future tag adds a new undefined token,
    this test fires loud."""
    src = APP_CSS.read_text()
    # Strip /* ... */ comments first so historical mentions
    # ("v1.10.26: was var(--bg-deep) (undefined…)") don't
    # trip the scan.
    code_only = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
    # Extract the :root token defs.
    root_idx = code_only.index(":root {")
    root_end = code_only.index("}", root_idx)
    root_block = code_only[root_idx:root_end]
    defined = set(re.findall(r"--([a-z0-9-]+)\s*:", root_block))
    # Extract all referenced tokens that have NO fallback —
    # `var(--foo, fallback)` is tolerant, only `var(--foo)`
    # without a comma is at risk.
    referenced_no_fallback = set(re.findall(
        r"var\(--([a-z0-9-]+)\s*\)",
        code_only,
    ))
    undefined = referenced_no_fallback - defined
    # Allow tokens defined anywhere in the file (scoped rules,
    # not just :root).
    for token in list(undefined):
        if re.search(rf"--{token}\s*:", code_only):
            undefined.discard(token)
    assert not undefined, (
        f"v1.18.61 audit guard: app.css references undefined "
        f"tokens (no :root or scoped definition): {undefined}. "
        f"Add a definition or fix the reference."
    )


# ── Fix C: v1.18.60 plex_mt mis-mapping corrected ──────────


def test_teardown_helper_uses_plex_mt_map():
    """The v1.18.60 helper must use an explicit map covering
    all three motif media_types — not the `'show' if 'tv' else
    'movie'` fallback that mis-mapped collections to 'movie'."""
    src = API_PY.read_text()
    fn_idx = src.index("def _teardown_plex_api_artifacts_for_placements(")
    body = src[fn_idx:fn_idx + 5000]
    # The new explicit map must exist.
    assert "_PLEX_MT_MAP" in body
    assert '"collection": "collection"' in body
    assert '"tv": "show"' in body
    # The pre-fix one-liner must NOT survive.
    assert 'plex_mt = "show" if media_type == "tv" else "movie"' not in body


def test_teardown_helper_collection_resolves_correctly():
    """Walk through the helper's logic for media_type='collection':
    the resulting plex_mt must be 'collection' (matches the value
    actually written to plex_items.media_type for collection rows)."""
    src = API_PY.read_text()
    fn_idx = src.index("def _teardown_plex_api_artifacts_for_placements(")
    body = src[fn_idx:fn_idx + 5000]
    # Find the _PLEX_MT_MAP definition.
    map_idx = body.index("_PLEX_MT_MAP")
    map_end = body.index("}", map_idx)
    map_block = body[map_idx:map_end + 1]
    # Simulate: media_type='collection' → 'collection'
    assert '"collection": "collection"' in map_block, (
        "v1.18.61: collection → 'collection' mapping required"
    )


# ── Audit doc landed in repo ────────────────────────────────


def test_audit_findings_catalogued_in_project_history():
    """The v1.18.61 audit findings + deferred-items roadmap must
    be checked into PROJECT_HISTORY.md (which IS tracked). The
    local docs/AUDIT_*.md doc is a working artifact per the
    .gitignore convention but the findings themselves are repo
    content."""
    src = PROJECT_HISTORY.read_text()
    # The new § for the v1.18.61 audit must exist.
    assert "v1.18.61" in src and "audit" in src.lower(), (
        "v1.18.61: PROJECT_HISTORY.md must include a section "
        "describing the v1.18.61 audit findings + roadmap"
    )
    # The deferred HIGH items must be enumerated so follow-up
    # tags know what to grab.
    for tag in ("HIGH-A", "HIGH-B", "HIGH-C", "HIGH-D",
                "HIGH-E", "HIGH-F", "HIGH-G"):
        assert tag in src, (
            f"PROJECT_HISTORY.md audit section must enumerate {tag}"
        )


# ── Version marker ──────────────────────────────────────────


def test_v1_18_61_markers_present():
    assert "v1.18.61" in APP_JS.read_text()
    assert "v1.18.61" in APP_CSS.read_text()
    assert "v1.18.61" in API_PY.read_text()
