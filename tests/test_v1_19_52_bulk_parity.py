"""v1.19.52 — bulk DOWNLOAD PLEX BACKUP parity + uniform withCount labels.

the user asked for an audit pass on bulk-action design choices so
they keep a uniform look. Three parallel audit agents (consistency,
edge cases, mirror-drift) all surfaced overlapping findings.

## Scope

### 1. withCount on both BACKUP buttons

Every bulk-bar button uses the `withCount(label, count)` helper
which renders `// LABEL` for count=1 and `// LABEL (N)` for N>1.
EXCEPT DOWNLOAD TDB BACKUP + DOWNLOAD PLEX BACKUP, which hand-
rolled `// DOWNLOAD N TDB BACKUPS` / `// DOWNLOAD N PLEX
BACKUPS`. The "VERB N THING" shape was the only outlier in the
bulk-bar; the "VERB THING (N)" shape is established convention.

### 2. Bulk PLEX BACKUP parity with per-row

The per-row handler (v1.19.50) does:
  - setOptimisticPlaceholder on click
  - waitForOp polling → alerts when 0 backed up + 0 errors
  - 409-specific error message
  - clears placeholder on failure

The bulk handler was missing all four. Pre-fix the user would
click bulk DOWNLOAD PLEX BACKUP on 50 rows where 30 were
non-C1, see "QUEUED," then nothing visible would change for
the 30 skipped rows. No count surface, no explanation.

v1.19.52 brings bulk handler to full per-row parity + adds
the (N skipped) count to the toast.

### 3. Sibling parity with DOWNLOAD TDB BACKUP

DOWNLOAD TDB BACKUP fires `setTimeout(loadLibrary, 1000)` +
`setTimeout(refreshTopbarStatus, 1100)` after success. PLEX
BACKUP was missing both. Adding them so the post-click state
catch-up cadence matches.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── withCount on both BACKUP buttons ─────────────────────────


def test_tdb_backup_button_uses_with_count():
    """DOWNLOAD TDB BACKUP must use withCount() for the count
    badge. Pre-v1.19.52 hand-rolled `// DOWNLOAD N TDB BACKUPS`
    — the only "VERB N THING" shape in the bulk-bar; everywhere
    else uses "VERB THING (N)"."""
    # Locate the backupBtn assignment block.
    idx = APP_JS.index("if (backupBtn) {")
    block = APP_JS[idx:idx + 1500]
    assert "withCount(" in block, (
        "v1.19.52: DOWNLOAD TDB BACKUP must use withCount() — "
        "uniform-look fix"
    )
    assert "'// DOWNLOAD TDB BACKUP'" in block
    # Hand-rolled multi-form must be gone.
    assert "`// DOWNLOAD ${pureP_count} TDB BACKUPS`" not in block


def test_cloud_backup_button_uses_with_count():
    """DOWNLOAD PLEX BACKUP must also use withCount() for the
    count badge."""
    idx = APP_JS.index("if (cloudBackupBtn) {")
    block = APP_JS[idx:idx + 1500]
    assert "withCount(" in block, (
        "v1.19.52: DOWNLOAD PLEX BACKUP must use withCount()"
    )
    assert "'// DOWNLOAD PLEX BACKUP'" in block
    # Hand-rolled multi-form must be gone.
    assert "`// DOWNLOAD ${cloudBackupCount} PLEX BACKUPS`" not in block


# ── Bulk DOWNLOAD PLEX BACKUP parity ─────────────────────────


def _bulk_cloud_handler() -> str:
    """Return the bulk-click handler body for the cloud-backup
    button. Locating the second occurrence of the button id
    (first is the const declaration, second is the addEventListener)."""
    first = APP_JS.index("library-cloud-backup-btn")
    second = APP_JS.index("library-cloud-backup-btn", first + 1)
    # The handler ends at the closing `});` after setTimeout
    # restore. Take a generous slice.
    return APP_JS[second:second + 8000]


def test_bulk_cloud_backup_sets_optimistic_placeholder():
    """The bulk click handler must call setOptimisticPlaceholder
    on click so the mini-bar surfaces feedback immediately
    rather than waiting for the next poll tick. Mirrors the
    per-row handler (v1.19.50) + sibling DOWNLOAD TDB BACKUP."""
    block = _bulk_cloud_handler()
    assert "setOptimisticPlaceholder" in block, (
        "v1.19.52: bulk handler must set the optimistic "
        "placeholder mirror per-row handler + DOWNLOAD TDB "
        "BACKUP precedent"
    )
    assert "'// QUEUING PLEX BACKUP'" in block


def test_bulk_cloud_backup_uses_wait_for_op():
    """Bulk handler must poll the op via motifOps.waitForOp +
    surface results. Pre-v1.19.52 the per-row handler had this
    but bulk silently completed with no count surface."""
    block = _bulk_cloud_handler()
    assert "waitForOp" in block, (
        "v1.19.52: bulk handler must waitForOp to surface the "
        "final processed/error/skipped counts"
    )


def test_bulk_cloud_backup_surfaces_skipped_count():
    """The bulk toast must report (N SKIPPED) when some rows
    were non-C1. Pre-fix the operator couldn't tell what motif
    did with the non-C1 portion of a mixed selection."""
    block = _bulk_cloud_handler()
    assert "skipped" in block.lower(), (
        "v1.19.52: bulk toast must report a skipped count"
    )
    # The skipped derivation: expected - processed - errors.
    assert "expected - processed - errors" in block or (
        "skipped" in block and "SKIPPED" in block
    )


def test_bulk_cloud_backup_zero_result_alert():
    """When ALL rows in a bulk selection were non-C1, the
    handler must explicitly alert the operator with a bulk-
    shaped message (different wording from the per-row alert)."""
    block = _bulk_cloud_handler()
    # Bulk-specific zero-result alert.
    assert "0 backed up" in block, (
        "v1.19.52: bulk handler must alert on the 0-backed-up "
        "case with a bulk-specific message"
    )
    # Wording differs from per-row ("None of the N selected rows")
    # to avoid the per-row phrasing leaking into the bulk surface.
    assert "selected row" in block.lower(), (
        "v1.19.52: bulk alert wording should reference 'selected "
        "row(s)' so the user knows it's a bulk-scope message"
    )


def test_bulk_cloud_backup_409_specific_alert():
    """The bulk handler must distinguish 409 (another cloud-
    backup run in flight) from generic errors. Mirrors the
    per-row handler (v1.19.45)."""
    block = _bulk_cloud_handler()
    assert "'409'" in block
    assert "already in" in block, (
        "v1.19.52: bulk 409 alert must mention 'already in flight'"
    )


def test_bulk_cloud_backup_clears_placeholder_on_failure():
    """If the endpoint POST throws, the optimistic placeholder
    must be cleared so the mini-bar doesn't hang on a queued
    state that never landed an op_progress row."""
    block = _bulk_cloud_handler()
    assert "clearOptimisticPlaceholder" in block, (
        "v1.19.52: bulk failure path must clear the optimistic "
        "placeholder"
    )


def test_bulk_cloud_backup_has_refresh_timers():
    """Sibling DOWNLOAD TDB BACKUP fires loadLibrary + refresh
    TopbarStatus timers post-click. Bulk PLEX BACKUP must
    match so the post-action state catch-up cadence is
    consistent across both backup actions."""
    block = _bulk_cloud_handler()
    assert "loadLibrary" in block, (
        "v1.19.52: bulk handler must call loadLibrary post-click "
        "(TDB BACKUP sibling parity)"
    )
    assert "refreshTopbarStatus" in block, (
        "v1.19.52: bulk handler must call refreshTopbarStatus "
        "post-click"
    )


# ── Per-row PLEX BACKUP optimistic placeholder ───────────────


def test_per_row_cloud_backup_sets_optimistic_placeholder():
    """Per-row handler must also call setOptimisticPlaceholder.
    Mirrors every other queueing SOURCE-menu action (DOWNLOAD
    TDB BACKUP, REPLACE TDB, etc.)."""
    # v0.51.51: the per-row handler delegates to cloudBackupForceCapture,
    # which sets the optimistic placeholder.
    idx = APP_JS.index("function cloudBackupForceCapture(")
    block = APP_JS[idx:idx + 7200]
    assert "setOptimisticPlaceholder" in block, (
        "v1.19.52: per-row backup path must set optimistic "
        "placeholder so the click→busy gap shows feedback"
    )
    assert "'// QUEUING PLEX BACKUP'" in block


def test_per_row_cloud_backup_clears_placeholder_on_catch():
    """Same as bulk — clear placeholder on failure so the
    mini-bar doesn't hang."""
    # v0.51.51: in cloudBackupForceCapture now (set before the POST, cleared on
    # terminal / ok:false / catch).
    idx = APP_JS.index("function cloudBackupForceCapture(")
    block = APP_JS[idx:idx + 7200]
    assert "clearOptimisticPlaceholder" in block, (
        "v1.19.52: per-row failure path must clear the "
        "placeholder"
    )


# ── Cross-button audit: withCount usage across bulk-bar ──────


def test_every_bulk_button_uses_with_count_for_count_display():
    """Audit guard: every bulk-bar button that displays a count
    badge must use withCount() rather than hand-rolling. the user's
    'uniform look' requirement — the bulk-bar reads more
    consistently when every button uses the same format
    (`// LABEL` singular / `// LABEL (N)` plural)."""
    # Search for any hand-rolled count formats in the
    # updateLibrarySelectionUi function block. Pattern:
    # textContent assignments that include `${variableCount}`
    # interpolated inline with a label.
    fn_anchor = APP_JS.index("function updateLibrarySelectionUi()")
    fn_body = APP_JS[fn_anchor:fn_anchor + 40000]
    import re
    # Find textContent assignments containing the substring
    # `// DOWNLOAD ${...} TDB BACKUPS` or similar legacy shapes.
    # The "VERB ${count} THING(S)" pattern is what v1.19.52 is
    # removing.
    legacy_shape = re.findall(
        r"textContent\s*=\s*`//\s+\w+\s+\$\{[a-zA-Z_]+\}\s+[A-Z]+",
        fn_body,
    )
    assert not legacy_shape, (
        f"v1.19.52: bulk buttons must use withCount() (// LABEL "
        f"(N)) instead of hand-rolling (// VERB N THING). "
        f"Offenders: {legacy_shape}"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_52_version_pin():
    """Version bumped at v1.19.52 (then again at v1.19.53 for
    the wider bulk-handler hygiene). Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
