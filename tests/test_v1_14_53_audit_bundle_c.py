"""v1.14.53 — audit Bundle C: LPS-flow polish + frontend dead-code sweep.

From the v1.14.50 holistic audit. Three MEDIUM findings in the
v1.14.47 LPS-flow code I shipped + two LOW dead-code items
flagged in AUDIT_FRONTEND.md M1/M2 still-open.

  • M1: adoptAndLetPlexServeFlow's chained /unplace call had no
    standalone catch — a post-adopt /unplace failure surfaced as
    the generic outer "ADOPT + LET PLEX SERVE failed" alert with
    no hint that step 1 already landed (motif owns the inode AND
    the file is still at the Plex folder).
  • M2: letPlexServeFlow's folderHint find predicate used `!sectionId`
    which short-circuits on empty-string. Combined with the dispatcher
    passing `btn.dataset.sectionId || ''`, an empty sectionId silently
    matched the FIRST mt+id row in libraryState (wrong section's
    media_folder in the confirm dialog) AND the unplace URL fell
    through to the unscoped server-wide endpoint.
  • M4: _enqueueing Set missing the v1.14.45/v1.14.47 SOURCE-menu
    actions (download-tdb-backup, purge-revert-to-plex, adopt-and-
    let-plex-serve). Topbar mini-bar lagged ~10s before the queued
    op surfaced.
  • L3 (AUDIT_FRONTEND M1): paintTopbarSyncing(_label) shim was
    a no-op label arg + duplicate of setOptimisticPlaceholder's
    boostPoll cascade everywhere except the section-refresh
    site. Removed the shim + 4 callers; section-refresh site
    now wires setOptimisticPlaceholder directly.
  • L4 (AUDIT_FRONTEND M2): #topbar-status click handler read
    data-failed-link === '1' but no JS sets the attribute since
    v1.12.118. Dead listener.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── M1: adopt+lps surfaces partial-state on /unplace failure ──


def test_adopt_lps_helper_wraps_unplace_in_own_catch():
    """The /unplace POST inside adoptAndLetPlexServeFlow must be
    wrapped in its own try/catch so a post-adopt failure surfaces
    a recovery hint (motif now owns the inode, file is still at
    Plex folder, run LET PLEX SERVE from SOURCE menu)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function adoptAndLetPlexServeFlow(")
    body = js[fn_anchor:fn_anchor + 5000]
    # The unplace call sits inside its own try block now.
    assert "try {\n        await api('POST', unplaceUrl);" in body
    # The catch surfaces a specific hint.
    assert "ADOPT succeeded but UNPLACE failed:" in body
    # Apostrophe escaped in JS source: row\'s
    assert "LET PLEX SERVE from the row\\'s SOURCE" in body
    # And the post-failure path still refreshes so the row reflects
    # the adopt-succeeded state instead of looking unchanged.
    catch_anchor = body.index("ADOPT succeeded but UNPLACE failed:")
    catch_block = body[catch_anchor:catch_anchor + 1500]
    assert "loadLibrary()" in catch_block


# ── M2: empty-string sectionId no longer drifts cross-section ─


def test_dispatcher_passes_undefined_for_missing_section_id():
    """Both dispatcher branches must pass `|| undefined` (not
    `|| ''`) so the helpers can distinguish "no section context"
    from a falsy section_id that should match every section.
    Pre-fix `|| ''` short-circuited the helpers' predicate AND
    silently fell through the URL builder's ternary to the
    unscoped server-wide endpoint."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Both dispatcher branches use `|| undefined` now.
    purge_anchor = js.index(
        "letPlexServeFlow(btn.dataset.mt, btn.dataset.id,"
    )
    purge_block = js[purge_anchor:purge_anchor + 400]
    assert "btn.dataset.sectionId || undefined" in purge_block
    adopt_anchor = js.index("adoptAndLetPlexServeFlow(\n          btn.dataset.mt")
    adopt_block = js[adopt_anchor:adopt_anchor + 600]
    assert "btn.dataset.sectionId || undefined" in adopt_block


def test_let_plex_serve_helper_uses_strict_null_check():
    """The folderHint find predicate inside letPlexServeFlow must
    use `sectionId == null` (strict null/undefined check) instead
    of `!sectionId` so empty-string sectionId doesn't short-
    circuit and match the first mt+id row from a different
    section."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function letPlexServeFlow(")
    body = js[fn_anchor:fn_anchor + 3500]
    assert "sectionId == null || row.section_id === sectionId" in body
    # The pre-fix `!sectionId` form must NOT survive in the
    # find-predicate position.
    assert "(!sectionId || row.section_id === sectionId)" not in body


# ── M4: _enqueueing covers the new SOURCE-menu actions ───────


def test_enqueueing_set_includes_v1_14_45_and_v1_14_47_actions():
    """The _enqueueing Set must include the three new SOURCE-menu
    actions so boostPoll fires on click and the topbar mini-bar
    surfaces the queued op within ~1s instead of the 10s
    auto-poll gap."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    anchor = js.index("const _enqueueing = new Set([")
    end = js.index("]);", anchor)
    block = js[anchor:end]
    assert "'download-tdb-backup'" in block
    assert "'purge-revert-to-plex'" in block
    assert "'adopt-and-let-plex-serve'" in block


# ── L3: paintTopbarSyncing shim deleted ──────────────────────


def test_paint_topbar_syncing_function_definition_gone():
    """The shim function definition must be removed. The 4
    callers each had setOptimisticPlaceholder above (or now have
    one wired in the section-refresh site). The shim was a
    no-op label arg + duplicate cascade."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js_no_comments = "\n".join(
        line for line in js.splitlines()
        if not line.lstrip().startswith("//")
    )
    # No function definition.
    assert "function paintTopbarSyncing" not in js_no_comments
    # No callers.
    assert "paintTopbarSyncing(" not in js_no_comments


def test_section_refresh_now_wires_set_optimistic_placeholder():
    """The section-refresh button (the 4th paintTopbarSyncing
    caller, the only one without its own setOptimisticPlaceholder
    above) now wires the placeholder directly so the topbar mini-
    bar still surfaces a // REFRESHING PLEX pill on click."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the section-refresh handler's POST.
    anchor = js.index("`/api/libraries/${encodeURIComponent(sid)}/refresh`")
    block = js[anchor:anchor + 2000]
    # setOptimisticPlaceholder('plex_enum', ...) wired in.
    assert "setOptimisticPlaceholder(\n              'plex_enum'" in block


# ── L4: dead data-failed-link handler deleted ────────────────


def test_data_failed_link_handler_removed():
    """The dead #topbar-status data-failed-link click handler
    must be gone (no JS sets the attribute since v1.12.118).
    Pin: the addEventListener('click', ...) for #topbar-status
    that gates on data-failed-link no longer exists."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js_no_comments = "\n".join(
        line for line in js.splitlines()
        if not line.lstrip().startswith("//")
    )
    # The attribute lookup is gone.
    assert "getAttribute('data-failed-link')" not in js_no_comments
    # The /queue?status=failed redirect via that handler is gone.
    # (other unrelated /queue refs may exist; this specific gate is
    # what we want gone.)
    assert "data-failed-link" not in js_no_comments
