"""v1.14.46 — two LPS-flow polish fixes from the user's testing.

## Bug 1: SOURCE-menu DOWNLOAD TDB BACKUP missing libraryRapidPoll

the user repro on v1.14.45: clicked DOWNLOAD TDB BACKUP from the
SOURCE menu, the row's amber DL pulse stayed lit until a manual
page refresh. The recovery-card path uses `closeAndReload()`
which calls `libraryRapidPoll()`; the SOURCE-menu dispatcher I
added in v1.14.45 only called `loadLibrary()` once — the row's
state-pill-pending pulse stayed amber until the next 30s
auto-poll caught the post-download green state.

Fix: add `libraryRapidPoll()` to the SOURCE-menu dispatcher,
mirroring the pattern in `redownload()` (line ~2285) and the
other action helpers.

## Bug 2 (the user's design point): REPLACE TDB hides on LPS rows

the user: "we should make the replace Tdb option once we have a
dl in place but not pushed since we could just use the push to
plex option to save us a download."

For LPS rows (motif's canonical exists, no placement, Plex
serves its own theme):
  • REPLACE TDB → re-downloads from TDB + force-places → wasteful
  • PLACE → PUSH TO PLEX (already exists in PLACE menu) → uses
    the existing canonical via /replace endpoint → no re-download

Fix: extend the REPLACE TDB SOURCE-menu gate to suppress when
`isPlexAgent && downloaded` (the LPS-state predicate from the
row data). PUSH TO PLEX from the PLACE menu remains the
canonical path for "push motif's existing canonical to Plex".

Pure-P rows (no canonical) still see REPLACE TDB — the gate
only suppresses the LPS subset.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Bug 1: SOURCE-menu dispatcher calls libraryRapidPoll ────


def test_source_menu_download_tdb_backup_calls_library_rapid_poll():
    """The SOURCE-menu dispatcher branch for download-tdb-backup
    must invoke libraryRapidPoll() so the amber DL chip
    transitions cleanly. Pre-fix on v1.14.45 the chip stayed
    amber until the next 30s auto-poll."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the v1.14.45 SOURCE-menu marker (distinct from
    # the recovery-card branch's marker text).
    src_anchor = js.index(
        "// v1.14.45: SOURCE-menu DOWNLOAD TDB BACKUP for pure-P"
    )
    # v1.15.142 widened: the click handler grew an
    # enqueued_sections===0 branch (~600 chars of narrative
    # comment + alert body), pushing the DOM-presence guard
    # past 2500. Bumped to 4500 to keep the contract.
    block = js[src_anchor:src_anchor + 4500]
    # The v1.14.46 marker explains the addition.
    assert "v1.14.46: also fire libraryRapidPoll()" in block
    # The actual call.
    assert "libraryRapidPoll()" in block
    # The DOM-presence guard mirrors the pattern in redownload().
    assert "document.getElementById('library-body')" in block


def test_recovery_card_dispatcher_unchanged_still_uses_close_and_reload():
    """Pin the closeAndReload-style refresh contract for the
    surviving DOWNLOAD TDB BACKUP click path.

    v1.14.47 reorg: the recovery-card branch was removed (the
    option no longer surfaces in the no-fail TRY THIS NEXT —
    it lives in the SOURCE menu now). The v1.14.46 SOURCE-menu
    fix (libraryRapidPoll() + loadLibrary()) IS the surviving
    closeAndReload-equivalent — both produce the same end state
    (post-action row refresh + amber→green chip transition
    inside the rapid-poll window). Pin the SOURCE-menu branch
    still calls libraryRapidPoll() + loadLibrary()."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    branch_anchor = js.index("} else if (act === 'download-tdb-backup') {")
    block = js[branch_anchor:branch_anchor + 4500]
    assert "libraryRapidPoll()" in block
    assert "loadLibrary()" in block


# ── Bug 2: REPLACE TDB suppressed on LPS rows ───────────────


def test_replace_tdb_suppressed_on_lps_state():
    """REPLACE TDB SOURCE-menu gate must add `!lpsHasCanonical`
    so LPS rows don't see the REPLACE TDB option. PUSH TO PLEX
    from the PLACE menu is the canonical path for that case
    (uses existing canonical, no re-download).

    v1.19.49: the canonical-check predicate switched from bare
    `downloaded` to `hasNonCloudCanonical` so plex_cloud
    backups (insurance, not commitment) don't suppress REPLACE
    TDB. The conjunction shape is unchanged — just the operand."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the REPLACE TDB block.
    replace_anchor = js.index("'replace-with-themerrdb', 'REPLACE TDB'")
    # The gate sits above this — walk back ~4500 chars (widened
    # for v1.19.49 predicate-definition comments + the v1.20.2
    # tdbActionPendingOk comment block + the v1.24.71 it.youtube_url
    # gate note + the v1.24.72 accepted_update-relaxation comment).
    gate_block = js[replace_anchor - 4500:replace_anchor]
    # The new variable + gate clause.
    assert (
        "const lpsHasCanonical = isPlexAgent && hasNonCloudCanonical"
        in gate_block
    ), (
        "v1.19.49: lpsHasCanonical must use hasNonCloudCanonical "
        "so plex_cloud backups don't suppress REPLACE TDB"
    )
    assert "!lpsHasCanonical" in gate_block
    # v1.14.46 marker explains the rationale.
    assert "v1.14.46:" in gate_block
    assert "PUSH TO PLEX" in gate_block


def test_replace_tdb_still_fires_for_pure_p_no_canonical():
    """Sanity: REPLACE TDB stays available for pure-P rows that
    DON'T have a canonical (the original use case — Plex serves
    its own, motif fetches TDB's version + places). Pin the
    gate clause `(sidecarOnly || isManualPlacement ||
    isPlexAgent)` is still present — the suppression is only
    additive."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    replace_anchor = js.index("'replace-with-themerrdb', 'REPLACE TDB'")
    gate_block = js[replace_anchor - 2000:replace_anchor]
    # Original disjunction still required.
    assert "sidecarOnly || isManualPlacement || isPlexAgent" in gate_block


def test_replace_tdb_still_fires_for_pure_m_and_pure_a():
    """Pure-M (sidecarOnly trigger) and pure-A (isManualPlacement
    trigger) cases must still see REPLACE TDB — the v1.14.46
    suppression is specific to the LPS state (isPlexAgent +
    non-cloud canonical). Sanity test that the gate doesn't
    accidentally affect those cases.

    Logical proof: when isPlexAgent=False (which is true for
    pure-M and pure-A — they have placements), `lpsHasCanonical
    = isPlexAgent && hasNonCloudCanonical` is False. So
    `!lpsHasCanonical` = True. The gate becomes the original
    disjunction. Same behavior as before for non-LPS cases.

    v1.19.49: predicate operand changed from `downloaded` to
    `hasNonCloudCanonical` (excludes plex_cloud backups). The
    pure-M/pure-A logical proof above still holds — isPlexAgent
    is still the gating factor."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    replace_anchor = js.index("'replace-with-themerrdb', 'REPLACE TDB'")
    gate_block = js[replace_anchor - 2500:replace_anchor]
    # Confirm the suppression still requires isPlexAgent. The
    # conjunction ensures non-LPS paths (where isPlexAgent=False)
    # are unaffected.
    assert "isPlexAgent && hasNonCloudCanonical" in gate_block


# ── Reuse pin: PUSH TO PLEX still in PLACE menu ─────────────


def test_push_to_plex_still_available_in_place_menu():
    """The PLACE-menu PUSH TO PLEX option (which v1.14.46's
    REPLACE TDB suppression points the user toward for LPS
    rows) must still exist. Pin so a future refactor doesn't
    accidentally remove the path the suppression assumes is
    available."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The PLACE menu block contains 'replace' action with
    # 'PUSH TO PLEX' label, gated on `themed && downloaded
    # && !placed && !dlBroken` — exactly the LPS state shape.
    assert "'replace', 'PUSH TO PLEX'" in js
    assert "themed && downloaded && !placed && !dlBroken" in js
