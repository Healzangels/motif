"""v1.15.4 — bulk DOWNLOAD TDB BACKUP for pure-P selections.

the user: "I would like to make it so movies that are just P
have a bulk action when there is a themerrdb source also
available so we can bulk download TDB Backup as right now have
to go one by one"

## Pre-fix

The existing DOWNLOAD & REPLACE FROM TDB bulk action shows
when ANY selected row has a usable TDB URL. For pure-P
selections (Plex serves its own theme; motif owns nothing
locally) the action would download AND place — i.e. switch
Plex from its own theme to motif's TDB version. That's
"replace" semantics; the user wanted "backup" semantics
(download only, leave Plex's served theme alone).

The pre-fix workaround was per-row REPLACE WITH TDB clicks,
which is fine for a handful of rows but unworkable for the
1290-row pure-P filter the user was working through.

## Fix

Two surfaces:

1. **Backend** — `/api/library/download-batch` accepts a new
   optional `place` body flag (default true for backwards
   compat). When `false`, the worker runs the download with
   `auto_place=False` so the chained place job never fires.
   Net effect: motif's canonical lands in /themes/ but Plex's
   folder is untouched.

2. **Frontend** — new `// DOWNLOAD TDB BACKUP` bulk action
   button. Visibility gate: ENTIRE selection is pure-P AND
   each row has a usable TDB URL. Mixed selections (P +
   others) route through the existing DOWNLOAD & REPLACE
   flow — no need to duplicate the action there.

Confirm prompt explains the semantics: download to /themes/,
DON'T place into Plex, manual PUSH TO PLEX later if needed.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"


# ── Backend ────────────────────────────────────────────────────


def test_download_batch_route_accepts_place_flag():
    """The /api/library/download-batch route must parse an
    optional `place` body flag. Default true preserves the
    existing DOWNLOAD & REPLACE behavior; false unlocks the
    backup-only path."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_library_download_batch(")
    # Walk to the end of the function (next @app.post decorator).
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert 'body.get("place"' in fn_body, (
        "Route must read the optional `place` body flag"
    )
    # Default to True so existing callers without the flag
    # land in the same behavior.
    assert "True) is not False" in fn_body or "default=True" in fn_body or \
        '"place", True' in fn_body, (
        "place flag must default to True for backwards compat"
    )


def test_download_batch_uses_auto_place_false_in_backup_branch():
    """When `place: false` the route must call _enqueue_download
    with `auto_place=False` so the worker skips the chained
    place job. Pre-fix the only branch was force_place=True
    which would have hardlinked the canonical into the Plex
    folder — wrong for backup mode."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_library_download_batch(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The backup branch must use auto_place=False.
    assert "auto_place=False" in fn_body, (
        "Backup branch must pass auto_place=False so the worker "
        "skips the chained place job"
    )
    # The reason string distinguishes the call site for log_event
    # diagnostics.
    assert '"bulk_backup"' in fn_body, (
        "Backup branch should use a distinct reason string for "
        "the log_event detail"
    )


def test_download_batch_log_event_distinguishes_backup_vs_place():
    """log_event must include the place flag so /queue events
    distinguish backup-only runs from full DOWNLOAD & REPLACE
    runs. Helps diagnosis when post-action state doesn't match
    expectations."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_library_download_batch(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert '"place": place_canonical' in fn_body or \
        'place_canonical' in fn_body
    assert "backup-only" in fn_body or "with place" in fn_body, (
        "log_event message should mention which mode fired"
    )


# ── Frontend: button + visibility gate ─────────────────────────


def test_template_has_tdb_backup_button():
    """library.html must define the new // DOWNLOAD TDB BACKUP
    button. Hidden by default — visibility gate in JS."""
    html = LIBRARY_HTML.read_text()
    assert 'id="library-tdb-backup-btn"' in html
    assert "DOWNLOAD TDB BACKUP" in html
    # Default hidden — visibility logic decides when to show.
    backup_idx = html.index('id="library-tdb-backup-btn"')
    backup_block = html[backup_idx - 200:backup_idx + 500]
    assert 'style="display:none"' in backup_block


def test_visibility_gate_is_pure_p_only_with_full_selection_match():
    """The button must only show when the ENTIRE selection is
    pure-P + TDB-actionable. pureP_count must equal n (total
    selection) — mixed selections fall through to the existing
    DOWNLOAD & REPLACE flow."""
    src = APP_JS.read_text()
    # Anchor on the v1.15.4 visibility logic.
    anchor = src.index("v1.15.4: DOWNLOAD TDB BACKUP visibility")
    block = src[anchor:anchor + 2500]
    # Gate condition must require pureP_count > 0 AND == n.
    assert "pureP_count > 0" in block
    assert "pureP_count === n" in block
    # Guarded by the same onTdbOnly + onAttnUpdateFilter checks
    # as the regular dlBtn — consistency.
    assert "!onTdbOnly" in block
    assert "!onAttnUpdateFilter" in block


def test_pure_p_count_increments_only_on_p_themed_actionable():
    """The pureP_count must only increment when the row is
    P-source AND themed AND TDB-actionable. Pre-fix the count
    didn't exist; pin the criteria so a future change to the
    selection-stats loop doesn't accidentally widen the gate."""
    src = APP_JS.read_text()
    # Anchor on the v1.15.4 increment block.
    anchor = src.index("v1.15.4: count pure-P-with-TDB rows")
    block = src[anchor:anchor + 800]
    assert "srcLetter === 'P'" in block
    assert "themed" in block
    assert "tdbActionable" in block
    assert "pureP_count++" in block


def test_visibility_label_pluralizes():
    """The button label must show a count badge for selections > 1.
    v1.19.52: switched from hand-rolled `// DOWNLOAD N TDB
    BACKUPS` template to withCount() helper which renders
    `// DOWNLOAD TDB BACKUP (N)` for uniformity with the other
    bulk buttons (the user's "uniform look" feedback). The
    helper internalizes the singular/plural distinction."""
    src = APP_JS.read_text()
    anchor = src.index("v1.15.4: DOWNLOAD TDB BACKUP visibility")
    block = src[anchor:anchor + 2500]
    assert "// DOWNLOAD TDB BACKUP" in block
    # withCount() handles the count badge — uniform with every
    # other bulk button.
    assert "withCount(" in block, (
        "v1.19.52: bulk TDB BACKUP must use withCount() for "
        "the count badge (uniform-look fix)"
    )
    assert "pureP_count" in block


def test_click_handler_posts_place_false():
    """The click handler must post place: false so the backend
    routes to the backup branch (auto_place=False, no Plex-
    folder write)."""
    src = APP_JS.read_text()
    handler_anchor = src.index(
        "library-tdb-backup-btn')?.addEventListener('click'"
    )
    # Walk to the next handler (next addEventListener for a
    # different button).
    block = src[handler_anchor:handler_anchor + 4500]
    assert "/api/library/download-batch" in block
    assert "place: false" in block, (
        "Backup handler must post place:false to route to the "
        "backend's no-place branch"
    )


def test_click_handler_filters_to_pure_p_defensively():
    """Even though the visibility gate enforces pure-P, the
    handler must defensively filter the items list to only
    P-source rows before posting. Rapid selection changes
    between toggle + click could produce a stale gate state;
    the handler is the second line of defense."""
    src = APP_JS.read_text()
    handler_anchor = src.index(
        "library-tdb-backup-btn')?.addEventListener('click'"
    )
    block = src[handler_anchor:handler_anchor + 4500]
    assert "computeSrcLetter" in block
    assert "srcLetter !== 'P'" in block, (
        "Handler must skip non-P rows even when the gate fired "
        "(defensive against stale gate state)"
    )


def test_confirm_prompt_explains_no_plex_change():
    """The confirm prompt must clearly explain that Plex's
    served theme is NOT changed by this action — the whole
    point of the BACKUP semantics is preserving Plex's
    current state."""
    src = APP_JS.read_text()
    handler_anchor = src.index(
        "library-tdb-backup-btn')?.addEventListener('click'"
    )
    block = src[handler_anchor:handler_anchor + 4500]
    assert "NOT place" in block, (
        "Prompt must call out that the action does NOT place"
    )
    assert "PUSH TO" in block, (
        "Prompt should reference PUSH TO PLEX as the manual "
        "follow-up if backup needs to be deployed"
    )


def test_v1_15_4_marker_explains_intent():
    """v1.15.4 markers reference the user's repro phrase so
    future readers see the rationale."""
    src = APP_JS.read_text()
    handler_anchor = src.index(
        "library-tdb-backup-btn')?.addEventListener('click'"
    )
    block = src[max(0, handler_anchor - 1500):handler_anchor + 200]
    assert "v1.15.4" in block
    # the user's framing as evidence of intent.
    assert "TDB Backup" in block or "TDB BACKUP" in block or \
        "bulk action" in block
