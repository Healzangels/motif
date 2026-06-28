"""v1.14.28 — probe TDB URL feature foundations.

Ships the single-row probe surface so the user can confirm a TDB
URL is alive before destructive actions like LET PLEX SERVE. Per
the user's safety-net concern: "I'm a bit worried about doing this
when there is a chance the url that themerrdb could be providing
could be a dead url ... we wouldn't be able to revert back to
themerrdb url because of a hidden failing url".

## Scope

1. Schema v45 — `themes.last_probed_at TIMESTAMP`
2. Migration v44→v45 (pure column add, no backfill)
3. POST /api/items/{mt}/{tmdb_id}/probe-tdb endpoint
   - Calls existing `probe_youtube_url` (yt-dlp --simulate)
   - Cookies-aware (uses settings.cookies_file when present)
   - Targets the row's CURRENT URL (override or TDB)
   - Always stamps last_probed_at
   - Writes failure_kind preemptively for actually-dead URLs
     (the user's option B): video_removed/private/age/geo
   - Returns indeterminate=true for cookies_expired (transient)
4. INFO card "PROBE TDB URL" button + inline result render
5. Probe-on-confirm in both LET PLEX SERVE click handlers
   (purge-and-ack + purge-revert-to-plex). Shows result inline
   in the confirm dialog text.

Bulk probe (background job + live-ops drawer card + cooldown
SQL filter) is v1.14.29 — substantial enough to deserve its
own tag.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db, CURRENT_SCHEMA_VERSION


REPO = Path(__file__).resolve().parent.parent


# ── Schema v45 ────────────────────────────────────────────────


def test_schema_version_at_least_v45():
    """v1.14.28 introduced schema v45 (themes.last_probed_at).
    Schema has progressed since (v46 added in v1.14.74) so the
    pin is `>= 45` rather than `== 45` — what matters for this
    test file is that the v45 column shape is intact and any
    later migration didn't drop it. The dedicated v46 test
    (test_v1_14_74) pins the current head."""
    assert CURRENT_SCHEMA_VERSION >= 45


def test_themes_has_last_probed_at_column(tmp_path):
    """Fresh DB at v45 has the new column."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(themes)")}
    assert "last_probed_at" in cols


def test_v44_to_v45_migration_function_exists():
    """The migration function must exist + add the column (no backfill needed;
    NULL means never probed). v1.24.50: routed through the idempotent _add_column
    helper so a crash-then-reboot re-run doesn't hit 'duplicate column name'."""
    src = (REPO / "app" / "core" / "db.py").read_text()
    assert "def _migrate_v44_to_v45(conn: sqlite3.Connection) -> None:" in src
    fn_anchor = src.index("def _migrate_v44_to_v45(")
    body = src[fn_anchor:fn_anchor + 15000]
    assert '_add_column(conn, "themes", "last_probed_at", "TEXT")' in body


def test_v44_to_v45_step_registered_in_ladder():
    """The migration step must be wired into the elif ladder."""
    src = (REPO / "app" / "core" / "db.py").read_text()
    assert "elif current == 44:" in src
    anchor = src.index("elif current == 44:")
    block = src[anchor:anchor + 200]
    assert "_migrate_v44_to_v45(conn)" in block
    assert "current = 45" in block


# ── /api/items/{mt}/{id}/probe-tdb endpoint ──────────────────


def test_probe_tdb_endpoint_defined():
    """The new endpoint must exist with the exact path."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert '@app.post("/api/items/{media_type}/{tmdb_id}/probe-tdb")' in src
    assert "async def api_probe_tdb(" in src


def test_probe_tdb_targets_themes_youtube_url():
    """v1.14.33: the endpoint targets the TDB URL specifically
    — NOT the override. v1.18.63 refactored: target_url is now
    derived from `pending_url or committed_url` where both
    come from themes/pending_updates (no user_overrides
    lookup). The override-exclusion invariant holds; only the
    variable name and dispatch shape changed."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    fn_end = src.index("@app.post", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    # v1.18.63: target_url is dispatched from pending vs committed,
    # both sourced from themes.youtube_url / pending_updates.
    # SELECT youtube_url FROM themes is still the committed
    # source; user_overrides is still NOT consulted.
    assert "SELECT youtube_url FROM themes" in body, (
        "v1.14.28 (refreshed v1.18.63): committed URL still "
        "comes from themes.youtube_url"
    )
    assert "target_url = pending_url or committed_url" in body, (
        "v1.18.63: target_url dispatch through pending → committed"
    )
    # Override lookup must still be absent.
    assert "FROM user_overrides" not in body


def test_probe_tdb_uses_probe_youtube_url_helper():
    """The endpoint must call the existing probe_youtube_url
    function (downloader.py) rather than hand-rolling yt-dlp."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    fn_end = src.index("@app.post", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert "from ..core.downloader import probe_youtube_url" in body
    assert "probe_youtube_url(target_url, cookies_file=cookies)" in body


def test_probe_tdb_always_stamps_last_probed_at():
    """Every probe (success, failure, indeterminate) must stamp
    last_probed_at so the bulk-probe cooldown (v1.14.29) can
    skip recently-probed rows."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    fn_end = src.index("@app.post", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert "UPDATE themes SET last_probed_at = ?" in body


def test_probe_tdb_writes_failure_kind_preemptively_for_dead_urls():
    """Per the user's option B: dead URLs (video_removed/private/
    age/geo) get failure_kind written preemptively so the row
    visibly turns red BEFORE the user takes a destructive
    action. cookies_expired stays indeterminate (don't change
    state)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    # v1.14.60: window widened from 5000 → 9000 chars (function
    # grew when v1.14.54 H3 extracted the sync block + v1.14.60
    # added the transaction(conn) wrap + multi-paragraph marker).
    body = src[fn_anchor:fn_anchor + 15000]
    # Gates on the FailureKind.needs_manual_override property
    # which v1.14.28's probe-feature design says is the
    # canonical "actually dead" predicate.
    assert "result.needs_manual_override" in body
    # Writes failure_kind, message, at, and clears any prior ack.
    assert "UPDATE themes SET failure_kind = ?" in body
    assert "failure_acked_at = NULL" in body


def test_probe_tdb_response_shape():
    """Response must include {ok, kind, message, indeterminate,
    url_probed}. The frontend uses indeterminate to show amber
    "?" instead of red "✗".

    v1.14.42: window enlarged + indeterminate-flag form softened
    to allow EITHER the v1.14.28 form (`result ==
    FailureKind.COOKIES_EXPIRED`) OR the v1.14.42 set form
    (`result in indeterminate_set`). Both prove the contract
    that the response shape carries the indeterminate flag."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    # v1.14.54: window widened from 8000 → 12000 chars (function
    # grew when the sync block was extracted into _probe_sync for
    # run_in_threadpool dispatch).
    body = src[fn_anchor:fn_anchor + 12000]
    # ok=True branch.
    assert '"ok": True, "kind": None, "message": None,' in body
    # ok=False branch with indeterminate flag.
    assert '"ok": False,' in body
    assert (
        '"indeterminate": result == FailureKind.COOKIES_EXPIRED,' in body
        or '"indeterminate": result in indeterminate_set' in body
    )


# ── INFO card PROBE button ────────────────────────────────────


def test_info_card_renders_probe_button():
    """The INFO card body template must include a PROBE TDB URL
    button when there's a URL to probe (currentUrl OR tdbUrl)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The probe button block.
    assert "v1.14.28: PROBE TDB URL — fires yt-dlp simulate" in js
    assert 'data-act="probe-tdb"' in js
    assert ">// PROBE TDB URL</button>" in js


def test_info_card_renders_last_probed_at_when_present():
    """If themes.last_probed_at is set, render a "last probed:
    <time>" hint next to the button so the user knows whether
    to click again."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "data.theme.last_probed_at" in js
    assert "last probed:" in js


def test_info_card_probe_button_skipped_when_no_tdb_url():
    """v1.14.33: plex_orphan rows with no TDB URL have nothing
    TDB-side to probe — the button must hide. Pre-v1.14.33 the
    gate was `currentUrl || tdbUrl` which incorrectly showed the
    button for plex_orphan + override rows (button click then
    409'd). The new gate is `tdbUrl` only."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const probeBtnHtml = tdbUrl" in js
    # Pre-fix gate must be gone.
    assert "const probeTargetUrl = currentUrl || tdbUrl;" not in js


def test_info_card_probe_click_handler_renders_inline_result():
    """The click handler must render the probe result inline in
    #probe-result with color (green=alive, amber=indeterminate,
    red=dead)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index('body.querySelector(\'button[data-act="probe-tdb"]\')')
    body = js[handler_anchor:handler_anchor + 3000]
    # The three result branches.
    assert "✓ alive" in body
    # Indeterminate (cookies needed).
    assert "cookies needed for conclusive probe" in body
    # Dead branch refreshes the library so the row's red ✗ pill
    # appears (server already wrote failure_kind).
    assert "loadLibrary" in body
    # Color tags.
    assert "var(--green-bright)" in body
    assert "var(--amber)" in body
    assert "var(--red)" in body


# ── Probe-on-confirm in LET PLEX SERVE handlers ──────────────


def test_probe_helper_function_defined():
    """The shared probe-then-confirm helper must exist + be
    used by both LET PLEX SERVE handlers (purge-and-ack +
    purge-revert-to-plex)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "async function _probeAndConfirmLetPlexServe(mt, id, baseDialogText)" in js
    # Calls the probe endpoint.
    fn_anchor = js.index("async function _probeAndConfirmLetPlexServe")
    body = js[fn_anchor:fn_anchor + 15000]
    assert "/api/items/${mt}/${id}/probe-tdb" in body
    # Three result branches in the helper.
    assert "✓ TDB URL probed: alive" in body
    assert "TDB URL probe inconclusive" in body
    assert "TDB URL probed: DEAD" in body


def test_purge_and_ack_handler_uses_probe_helper():
    """The purge-and-ack click handler must call the probe
    helper before showing the confirm dialog."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("} else if (act === 'purge-and-ack') {")
    body = js[handler_anchor:handler_anchor + 4000]
    assert "_probeAndConfirmLetPlexServe(mt, id, baseText)" in body
    # Button label flips to "PROBING…" during the probe.
    assert "btn.textContent = '// PROBING…';" in body


def test_purge_revert_to_plex_handler_uses_probe_helper():
    """Same probe-then-confirm flow on the non-failed +P composite
    LET PLEX SERVE path.

    v1.14.47 reorg: the dispatcher branch was extracted to the
    top-level `letPlexServeFlow` helper so the SOURCE-menu
    dispatcher can call it (the inline branch lived in
    hydrateRecoveryOptions, scope-trapped). The flow renamed its
    helper to `_probeAndConfirmLPSAtTopLevel` (top-level twin of
    the inner `_probeAndConfirmLetPlexServe`) — same probe-then-
    confirm contract."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    helper_anchor = js.index("async function letPlexServeFlow(")
    body = js[helper_anchor:helper_anchor + 3000]
    assert "_probeAndConfirmLPSAtTopLevel(mt, id, baseText)" in body


# ── Reuse: probe_youtube_url + FailureKind unchanged ─────────


def test_probe_youtube_url_unchanged():
    """v1.14.28 reuses the existing v1.10.43 probe_youtube_url
    helper. Pin its existence + signature so a refactor doesn't
    accidentally break the new endpoint."""
    src = (REPO / "app" / "core" / "downloader.py").read_text()
    assert "def probe_youtube_url(" in src
    assert "cookies_file: Path | None = None" in src
    # Returns FailureKind | None.
    assert "-> FailureKind | None:" in src


def test_failure_kind_needs_manual_override_unchanged():
    """The needs_manual_override property is the canonical
    "actually dead" predicate the probe endpoint uses to gate
    failure_kind writes. Pin so a refactor doesn't drop the
    set."""
    src = (REPO / "app" / "core" / "downloader.py").read_text()
    fn_anchor = src.index("def needs_manual_override(self) -> bool:")
    body = src[fn_anchor:fn_anchor + 15000]
    assert "FailureKind.VIDEO_PRIVATE" in body
    assert "FailureKind.VIDEO_REMOVED" in body
    assert "FailureKind.VIDEO_AGE_RESTRICTED" in body
    assert "FailureKind.GEO_BLOCKED" in body
