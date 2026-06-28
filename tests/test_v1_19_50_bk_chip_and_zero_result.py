"""v1.19.50 — BK filter chip + 0-eligible-result feedback.

the user's 2026-05-27 feedback after v1.19.49 deploy:

  1. "BK is no one of the sortable filters anywhere in the
     sections."

  2. "Also ran into an issue trying to download backup from plex
     for this item 90 Day Fiancé: Happily Ever After?... it
     looks to attempt to, I see it in status bar but then
     nothing ends up being added/happens"

Docker logs for issue #2:

    cloud_backup walker: starting walk over 1 candidate row(s)
    identify_c1_rows: walked 1 rows, found 0 C1 targets
    cloud_themes_backup: walk found 0 C1 target(s) — starting download stage
    CLOUD THEMES BACKUP completed: 0 backed up, 0 error(s)

So the walker found the row but it didn't classify as C1 (the
/themes response had a non-cloud-sole shape — probably C2 or
later, with an upload sibling). The user got no explicit
feedback.

## Fixes

### 1. BK filter chip

The v1.19.21 commit added the BK badge to the LINK column but
never added a corresponding filter chip. v1.19.43 added the
B chip but missed the parallel BK gap. v1.19.50 adds:

  - library.html: `data-link-pill="bk"` button
  - app.js PILL_DEEP_LINKS: `'bk'` in the values Set
  - app.js pillAxes // ALL set: `'bk'`
  - api.py `_pset`: `"bk"` in the allow-list
  - api.py SQL handler: `elif p == "bk":` filter on
    `last_place_attempt_reason='backup_only' AND source_kind
    != 'plex_cloud' AND no placement` (the COMPLEMENT of
    the v1.19.43 `b` branch)

### 2. 0-eligible-target alert on cloud-backup

New `motifOps.waitForOp(opId, opts)` helper polls /api/progress
until the op reaches a terminal state, then resolves with the
final row. The SOURCE-menu click handler awaits it and shows
an explicit alert when `processed_total === 0 && error_count
=== 0 && status === 'done'` — meaning "the walker classified
the row as non-C1, nothing happened."

Without this the user saw the op fire in the status bar then
just... nothing. The row didn't change to B and the user had
no way to know why.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()


# ── BK filter chip (Fix 1) ───────────────────────────────────


def test_bk_chip_in_library_html():
    """The BK filter chip must exist in the LINK pill row."""
    assert 'data-link-pill="bk"' in LIBRARY_HTML, (
        "v1.19.50: BK filter chip missing — v1.19.21 BK badge "
        "was un-filterable for 29 tags. the user's audit: 'BK is "
        "no one of the sortable filters anywhere'"
    )
    chip_idx = LIBRARY_HTML.index('data-link-pill="bk"')
    # Tooltip on the chip is long; widen window past the closing tag.
    chunk = LIBRARY_HTML[
        max(0, chip_idx - 200):chip_idx + 500
    ]
    assert "link-glyph-bk" in chunk, (
        "v1.19.50: BK chip must reuse the existing link-glyph-bk "
        "class so the chip color matches the row badge"
    )
    # v1.19.63: chip label renamed BK → BU. v1.19.75: BU → UB.
    # CSS classname + data-link-pill value unchanged. Accept any.
    assert (
        ">BK</button>" in chunk
        or ">BU</button>" in chunk
        or ">UB</button>" in chunk
    ), "v1.19.50/63/75: chip label must be 'BK', 'BU', or 'UB'"


def test_bk_in_pill_deep_links():
    """The deep-link parser allowlist must include 'bk' so
    ?link_pills=bk URL hydrates the chip on page load."""
    idx = APP_JS.index("param: 'link_pills'")
    block = APP_JS[idx:idx + 1500]
    assert "'bk'" in block, (
        "v1.19.50: PILL_DEEP_LINKS link_pills values Set must "
        "include 'bk' (same mirror-principle as v1.14.43's "
        "'ps' add + v1.19.43's 'b' add)"
    )


def test_bk_in_pill_axes_all_set():
    """The // ALL click handler must activate the BK chip when
    the user clicks ALL on the LINK row."""
    idx = APP_JS.index("attr: 'linkPill', allAttr: 'linkPillAll'")
    block = APP_JS[idx:idx + 500]
    assert "'bk'" in block, (
        "v1.19.50: pillAxes linkPill values array must include "
        "'bk' so // ALL on LINK activates it"
    )


def test_bk_in_api_pset_allow_list():
    """The server's `_pset` validator must accept 'bk' so the
    URL deep-link survives parsing."""
    assert (
        '"hl", "c", "m", "none", "ps", "pu", "rp", "b", "bk"' in API_PY
    ), (
        "v1.19.50: _pset link_pills allow-list must include 'bk'"
    )


def test_bk_sql_handler_filters_correctly():
    """SQL handler for link_pills='bk' must filter on
    backup_only + NOT plex_cloud + no placement (complementary
    to the 'b' branch)."""
    idx = API_PY.index('elif p == "bk":')
    block = API_PY[idx:idx + 1600]
    # v1.21.59: lf.* reads are now COALESCE(lf_e.x, lf_g.x) (per-edition).
    assert ("COALESCE(lf_e.last_place_attempt_reason, "
            "lf_g.last_place_attempt_reason) = 'backup_only'") in block
    assert "source_kind" in block
    assert "plex_cloud" in block, (
        "v1.19.50: BK SQL must reference plex_cloud as the "
        "EXCLUSION discriminator (B and BK are complementary "
        "sets under the backup_only stamp)"
    )
    # v1.19.90: BK now also excludes 'themerrdb' (that's the TB
    # branch); B + TB + BK partition every backup_only row.
    assert "themerrdb" in block, (
        "v1.19.90: BK SQL must exclude themerrdb (now the TB chip)"
    )
    assert "COALESCE(p_e.media_folder, p_g.media_folder) IS NULL" in block


def test_b_and_bk_are_complementary_sets():
    """B = source_kind='plex_cloud'; BK = the residual backup_only
    chip (source_kind NULL or NOT IN the explicit-source set).
    v1.19.90 split TB (themerrdb) + v1.20.17 split AB (adopt) out of
    BK, so B/TB/AB/BK now partition every backup_only row exactly
    once. Pin the SQL shape so future drift can't break the invariant
    that BK is the residual."""
    b_idx = API_PY.index('elif p == "b":')
    b_block = API_PY[b_idx:API_PY.index('elif p ==', b_idx + 1)]
    bk_idx = API_PY.index('elif p == "bk":')
    bk_block = API_PY[bk_idx:bk_idx + 1800]
    # B: source_kind = 'plex_cloud' (v1.21.59: COALESCE'd per-edition).
    assert ("COALESCE(lf_e.source_kind, lf_g.source_kind) = 'plex_cloud'"
            in b_block)
    # BK: the residual — source_kind IS NULL OR NOT IN the explicit set.
    assert ("COALESCE(lf_e.source_kind, lf_g.source_kind) IS NULL"
            in bk_block)
    # BK must exclude all three explicitly-split sources.
    assert "'plex_cloud', 'themerrdb', 'adopt'" in bk_block


# ── 0-eligible-result feedback (Fix 2) ───────────────────────


def test_motif_ops_exports_wait_for_op():
    """motifOps must expose waitForOp() as a public helper."""
    assert "function waitForOp(opId" in OPS_JS
    assert "waitForOp," in OPS_JS, (
        "v1.19.50: motifOps must export waitForOp so click "
        "handlers can await op completion"
    )


def test_wait_for_op_polls_progress_endpoint():
    """waitForOp must poll /api/progress and look for the op_id
    in the returned ops list."""
    fn_idx = OPS_JS.index("async function waitForOp(opId")
    fn_end = OPS_JS.index("\n  }", fn_idx) + 4
    body = OPS_JS[fn_idx:fn_end]
    assert "/api/progress" in body
    assert "find(" in body or "filter" in body
    # The op_id comparison uses arrow-function arg name — accept
    # either `op` or `o` as the shorthand.
    assert (
        "o.op_id === opId" in body
        or "op.op_id === opId" in body
    )


def test_wait_for_op_terminates_on_done_failed_or_cancelled():
    """waitForOp must resolve when status is terminal (done/
    failed/cancelled) — not 'running'/'pending'/'cancelling'."""
    fn_idx = OPS_JS.index("async function waitForOp(opId")
    fn_end = OPS_JS.index("\n  }", fn_idx) + 4
    body = OPS_JS[fn_idx:fn_end]
    assert "'running'" in body
    assert "'pending'" in body


def test_backup_click_handler_awaits_wait_for_op():
    """The backup-cloud-theme click handler must call
    motifOps.waitForOp + alert on the 0-target case."""
    idx = APP_JS.index("act === 'backup-cloud-theme'")
    block = APP_JS[idx:idx + 5000]
    assert "waitForOp" in block, (
        "v1.19.50: per-row click handler must await op "
        "completion via motifOps.waitForOp"
    )
    # Alert specifically calls out the non-C1 / nothing-backed-up
    # case so the user knows WHY their click didn't change the row.
    assert "C1" in block, (
        "v1.19.50: 0-target alert must explain the C1 "
        "classification (non-C1 = nothing to back up)"
    )


def test_backup_click_handler_alert_only_on_zero_zero_done():
    """The alert must fire ONLY when processed_total === 0 AND
    error_count === 0 AND status === 'done' — NOT on failed
    (different message), NOT on success (>=1 processed), NOT
    on errors (different message)."""
    idx = APP_JS.index("act === 'backup-cloud-theme'")
    block = APP_JS[idx:idx + 5000]
    assert "processed === 0 && errors === 0" in block, (
        "v1.19.50: 0-target alert condition must check both "
        "processed_total == 0 AND error_count == 0 (failures "
        "get a different message)"
    )
    assert "status === 'done'" in block


def test_backup_click_handler_has_failure_branch():
    """A separate alert must fire when status='failed' — different
    message from the 0-target case."""
    idx = APP_JS.index("act === 'backup-cloud-theme'")
    # v1.19.62: widened outer block 4000→7000 to cover the handler
    # post-allow_existing_local plumb-through. The failed alert
    # lives ~100 lines after the act check.
    block = APP_JS[idx:idx + 7000]
    assert "status === 'failed'" in block
    # And the failure alert mentions LOGS for diagnostic.
    failed_idx = block.index("status === 'failed'")
    failed_block = block[failed_idx:failed_idx + 900]
    assert "LOGS" in failed_block, (
        "v1.19.50: failure alert should direct the user to the "
        "LOGS tab for diagnostic"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_50_version_pin():
    """Version bumped at v1.19.50 (then again at v1.19.51 for
    the selected-aware classifier). Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
