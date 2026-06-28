"""v1.17.0 — Apprise notifications integration.

the user: "I want to add the ability to integrate with apprise so we
can have notification support." Goes ON in /settings → NOTIFICATIONS.

Design (v1.17.0 MVP, "phase 1"):

  * Two sinks coexist (Option C from the design discussion):
      - Embedded: `notifications.apprise_urls` list, dispatched
        via the `apprise` Python package in-process.
      - External: `notifications.apprise_external_url` POSTs
        {title, body, type} to a caronc/apprise-api container.
    Both can be populated; motif fires both paths concurrently
    via a 4-worker thread pool.
  * Per-event toggles in `notifications.events: dict[str, bool]`.
    Default state: 3 ON (sync_completed, sync_failed,
    bulk_action_completed) + 1 OFF opt-in (themes_added_by_sync).
    The remaining 7 toggles from the design discussion are
    deferred to v1.17.1 — their dispatch sites need per-event
    dedupe logic that's out of scope for this ship.
  * Dispatcher is BEST-EFFORT: a failed send never crashes the
    caller. Notify-health surfaces through events table
    (component='notify') with no topbar pill.

Tests pin the foundation contract + verify each wired hook site
calls notify.dispatch with the expected event_kind.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
WORKER_PY = REPO / "app" / "core" / "worker.py"
NOTIFY_PY = REPO / "app" / "core" / "notify.py"
CONFIG_PY = REPO / "app" / "core" / "config_file.py"


# ── 1. Foundation: dataclass + defaults + dispatcher exists ───


def test_notifications_config_dataclass_exists():
    """NotificationsConfig must live in config_file.py with the
    canonical three fields: apprise_urls (list), apprise_external_url
    (str), events (dict)."""
    from app.core.config_file import (
        NotificationsConfig, MotifConfig)
    nc = NotificationsConfig()
    assert isinstance(nc.apprise_urls, list)
    assert nc.apprise_urls == []
    assert isinstance(nc.apprise_external_url, str)
    assert nc.apprise_external_url == ""
    assert isinstance(nc.events, dict)
    # MotifConfig must hold a notifications section.
    cfg = MotifConfig()
    assert hasattr(cfg, "notifications")
    assert isinstance(cfg.notifications, NotificationsConfig)


def test_default_event_set_phase1():
    """v1.17.0 phase-1 keys ship with these defaults.

    v1.17.10 update: the original assertion test-locked the
    literal 4-key dict — the same footgun that caused v1.17.1 H1
    (`themes.created_at`). v1.17.4 wired hook sites for 6 more
    events + added UI toggles for them, but `_DEFAULT_NOTIFY_EVENTS`
    was never extended; the api.py closed-set filter silently
    dropped every PATCH the new toggles fired. This test would
    have caught the gap if it had been a contract test rather
    than a literal-equality test — but the rigid assertion
    instead masked the bug for 6 tags. Softening to "phase-1 keys
    exist with these defaults" so future phase additions can
    extend the map without breaking the test, while still
    catching a regression that changes the original 4 defaults."""
    from app.core.config_file import _DEFAULT_NOTIFY_EVENTS
    expected = {
        "sync_completed":        True,
        "sync_failed":           True,
        "bulk_action_completed": True,
        "themes_added_by_sync":  False,
    }
    for key, default in expected.items():
        assert key in _DEFAULT_NOTIFY_EVENTS, (
            f"v1.17.0 phase-1: `{key}` must be in "
            f"_DEFAULT_NOTIFY_EVENTS."
        )
        assert _DEFAULT_NOTIFY_EVENTS[key] == default, (
            f"v1.17.0 phase-1: `{key}` default should be {default}; "
            f"found {_DEFAULT_NOTIFY_EVENTS[key]}."
        )


def test_apprise_dependency_in_requirements():
    """The apprise package must be in requirements.txt with a
    floor that includes the URL formats motif documents."""
    req = (REPO / "requirements.txt").read_text()
    assert "apprise>=" in req


# ── 2. _apply_partial_config: dict merge for events ───────────


def test_apply_partial_config_merges_events_dict():
    """The /api/config PATCH endpoint hands the partial body to
    _apply_partial_config. notifications.events is a dict[str,
    bool] — partial updates must MERGE (preserve untouched keys),
    not REPLACE (which would drop event toggles added in newer
    versions when an old client patches)."""
    from app.core.config_file import MotifConfig
    from app.web.api import _apply_partial_config
    cfg = MotifConfig()
    cfg.notifications.events["sync_completed"] = True
    cfg.notifications.events["themes_added_by_sync"] = False
    _apply_partial_config(cfg, {
        "notifications": {
            "events": {"themes_added_by_sync": True}
        }
    })
    # Touched key flipped; untouched key preserved.
    assert cfg.notifications.events["themes_added_by_sync"] is True
    assert cfg.notifications.events["sync_completed"] is True


def test_apply_partial_config_section_round_trip():
    """End-to-end shape check: a save with all three notification
    fields populates them correctly."""
    from app.core.config_file import MotifConfig
    from app.web.api import _apply_partial_config
    cfg = MotifConfig()
    _apply_partial_config(cfg, {
        "notifications": {
            "apprise_urls": ["discord://a/b", "pushover://c@d"],
            "apprise_external_url": "http://apprise:8000/notify/x",
            "events": {"sync_completed": False},
        }
    })
    assert cfg.notifications.apprise_urls == ["discord://a/b", "pushover://c@d"]
    assert cfg.notifications.apprise_external_url == "http://apprise:8000/notify/x"
    assert cfg.notifications.events["sync_completed"] is False


# ── 3. Dispatcher gating ──────────────────────────────────────


def test_dispatch_unknown_event_kind_warns_and_returns():
    """Unknown event_kind (e.g. a typo) must NOT raise — the
    dispatcher's contract is best-effort. Logs a WARNING via the
    notify logger so the typo is visible without breaking the
    caller."""
    from app.core.notify import dispatch
    from app.core.config_file import NotificationsConfig
    nc = NotificationsConfig()
    # Add a URL so the gate would NOT trip on empty sinks.
    nc.apprise_urls = ["discord://nope"]
    # Should return cleanly without raising.
    dispatch(Path("/tmp/motif-test.db"), nc,
             event_kind="bogus_event", title="X", body="Y")


def test_dispatch_no_sinks_no_op():
    """When neither sink is configured, dispatch must no-op
    silently (no thread pool submit, no log entry — the user
    hasn't configured notifications yet, that's not an error)."""
    from app.core.notify import dispatch
    from app.core.config_file import NotificationsConfig
    nc = NotificationsConfig()
    # Default event set has sync_completed ON, but no URLs/external.
    with patch("app.core.notify._get_pool") as mock_pool:
        dispatch(Path("/tmp/motif-test.db"), nc,
                 event_kind="sync_completed", title="X", body="Y")
        mock_pool.assert_not_called()


def test_dispatch_disabled_event_no_op():
    """If the event toggle is OFF, dispatch must no-op even with
    sinks configured."""
    from app.core.notify import dispatch
    from app.core.config_file import NotificationsConfig
    nc = NotificationsConfig()
    nc.apprise_urls = ["discord://a/b"]
    nc.events["sync_completed"] = False
    with patch("app.core.notify._get_pool") as mock_pool:
        dispatch(Path("/tmp/motif-test.db"), nc,
                 event_kind="sync_completed", title="X", body="Y")
        mock_pool.assert_not_called()


def test_dispatch_enabled_event_submits_to_pool():
    """Happy path: toggle ON + ≥1 sink configured → submit to the
    pool. The submit is fire-and-forget; we don't await."""
    from app.core.notify import dispatch
    from app.core.config_file import NotificationsConfig
    nc = NotificationsConfig()
    nc.apprise_urls = ["discord://a/b"]
    nc.events["sync_completed"] = True
    with patch("app.core.notify._get_pool") as mock_pool:
        executor = MagicMock()
        mock_pool.return_value = executor
        dispatch(Path("/tmp/motif-test.db"), nc,
                 event_kind="sync_completed", title="X", body="Y")
        executor.submit.assert_called_once()


# ── 4. test_dispatch synchronous helper ───────────────────────


def test_test_dispatch_no_sinks_returns_error():
    """The TEST NOTIFICATION button surface — when no sinks
    configured, return a structured error the UI can render."""
    from app.core.notify import test_dispatch
    from app.core.config_file import NotificationsConfig
    nc = NotificationsConfig()
    result = test_dispatch(nc)
    assert "error" in result
    assert "No notification sinks" in result["error"]


def test_test_dispatch_shape():
    """Return shape: {embedded: {configured, ok, fail},
    external: {configured, ok, fail}}. The UI surfaces per-sink
    outcome inline."""
    from app.core.notify import test_dispatch
    from app.core.config_file import NotificationsConfig
    nc = NotificationsConfig()
    nc.apprise_urls = []
    nc.apprise_external_url = ""
    result = test_dispatch(nc)
    assert "embedded" in result
    assert "external" in result
    assert "configured" in result["embedded"]
    assert "configured" in result["external"]


# ── 5. Wired hook sites (static guards) ───────────────────────


def test_worker_sync_completed_hook_wired():
    """The sync job dispatcher in worker.py must call
    notify.dispatch with event_kind='sync_completed' after a
    successful run_sync."""
    src = WORKER_PY.read_text()
    assert 'event_kind="sync_completed"' in src, (
        "v1.17.0: worker.py must dispatch sync_completed after "
        "the run_sync() return path"
    )


def test_worker_sync_failed_hook_wired():
    """And event_kind='sync_failed' in the except branch."""
    src = WORKER_PY.read_text()
    assert 'event_kind="sync_failed"' in src, (
        "v1.17.0: worker.py must dispatch sync_failed in the "
        "run_sync() except branch"
    )


def test_worker_sync_failed_does_not_mask_underlying_exception():
    """The except block around run_sync wraps notify.dispatch in
    its own try/except so a notify failure doesn't swallow the
    original sync exception. After dispatching, the original
    exception must `raise` through."""
    src = WORKER_PY.read_text()
    # Find the sync_failed dispatch + verify there's a `raise`
    # statement after it in the same except branch.
    failed_idx = src.index('event_kind="sync_failed"')
    # Window forward to the next `raise` and ensure it's within a
    # reasonable distance (not too far away, which would suggest
    # an unrelated raise).
    # v1.23.82: 1500→1800 — the sync_failed title-unification comment
    # lengthened the dispatch block, pushing `raise` past the old window.
    after = src[failed_idx:failed_idx + 1800]
    assert "\n            raise" in after, (
        "v1.17.0: worker.py sync_failed except branch must `raise` "
        "after notify.dispatch so the worker's normal failure "
        "handling (retry / mark failed) takes over"
    )


def test_worker_themes_added_by_sync_hook_wired():
    """themes_added_by_sync queries the themes table for rows
    created within the sync window (started_at filter) and
    samples the first 10 titles.

    v1.17.1: original test pinned the literal `"created_at >= ?"`
    but `themes` has no `created_at` column — the runtime feature
    was dead code, but the test passed by inspecting source
    strings. The H1 fix in v1.17.1 switched to
    `first_seen_sync_at` (which DOES exist). This test now pins
    the CONTRACT (some timestamp column compared against a
    bound parameter) without naming the literal column, so a
    future schema migration that renames the timestamp doesn't
    break the test as long as the contract holds."""
    src = WORKER_PY.read_text()
    # v1.21.6: themes_added_by_sync no longer fires a separate
    # notification — it gates a "🎵 New:" titles section inside the
    # single sync_completed message. The query + toggle gate live
    # there now.
    assert '_events.get("themes_added_by_sync"' in src
    # The query must filter by a per-theme timestamp >= the sync
    # window start. v1.17.1 ships `first_seen_sync_at`; future
    # rename should still match this pattern.
    import re
    block_start = src.index('_events.get("themes_added_by_sync"')
    block_end = src.index("# v1.12.126", block_start)
    block = src[block_start:block_end]
    # Look for a `WHERE <something>_at >= ?` filter clause —
    # standard timestamp comparison form. The actual column name
    # is implementation-detail; the contract is the WHERE shape.
    assert re.search(r"WHERE\s+\w+_at\s*>=\s*\?", block), (
        "v1.17.1: themes_added_by_sync query must filter by a "
        "timestamp >= bound-parameter clause. Pre-v1.17.1 the "
        "query referenced themes.created_at, a column that does "
        "not exist; H1 fix switched to first_seen_sync_at. The "
        "literal column name is implementation-detail — the WHERE "
        "shape is the test contract."
    )
    # Sample cap at 10 (+ 1 for the "+N more" overflow check).
    assert "LIMIT 11" in src or "LIMIT 10" in src


def test_bulk_lps_action_completed_hook_wired():
    """_bulk_lps_run must dispatch bulk_action_completed at its
    terminal log_event site."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert 'event_kind="bulk_action_completed"' in body, (
        "v1.17.0: _bulk_lps_run terminal must dispatch "
        "bulk_action_completed with the summary string used in "
        "the events log"
    )


def test_bulk_probe_tdb_action_completed_hook_wired():
    """_bulk_probe_tdb_run must dispatch bulk_action_completed
    at its terminal log_event site."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert 'event_kind="bulk_action_completed"' in body, (
        "v1.17.0: _bulk_probe_tdb_run terminal must dispatch "
        "bulk_action_completed"
    )


# ── 6. TEST NOTIFICATION endpoint ─────────────────────────────


def test_test_notification_endpoint_exists():
    """POST /api/admin/test-notification must be defined."""
    src = API_PY.read_text()
    assert '@app.post("/api/admin/test-notification")' in src
    assert "async def api_admin_test_notification(" in src


def test_test_notification_endpoint_calls_test_dispatch():
    """The endpoint reads settings.cfg.notifications and calls
    notify.test_dispatch — bypasses the per-event gate."""
    src = API_PY.read_text()
    anchor = src.index("async def api_admin_test_notification(")
    fn_end = src.index("\n    @app.post(", anchor + 1)
    body = src[anchor:fn_end]
    assert "settings.cfg.notifications" in body
    assert "test_dispatch" in body


# ── 7. Settings UI surface ────────────────────────────────────


def test_settings_html_has_notifications_tab():
    """The /settings tab list must include NOTIFICATIONS between
    RUNTIME and TOKENS."""
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'data-tab="notifications"' in html
    assert 'data-panel="notifications"' in html


def test_settings_html_has_apprise_url_fields():
    """The notifications panel must have both the URLs textarea
    and the external API URL input."""
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'data-cfg-field-lines="notifications.apprise_urls"' in html
    assert 'data-cfg-field="notifications.apprise_external_url"' in html


def test_settings_html_has_phase1_event_checkboxes():
    """The notifications panel must surface checkboxes for the
    4 phase-1 events. v1.17.1+ events are NOT in this ship."""
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    for kind in ("sync_completed", "sync_failed",
                 "bulk_action_completed", "themes_added_by_sync"):
        assert f'data-cfg-field="notifications.events.{kind}"' in html, (
            f"v1.17.0: settings.html must have a checkbox for {kind}"
        )
    # v1.17.4: 6 more event toggles landed in the UI (cookies_needed,
    # disk_low, worker_restarted, theme_added, theme_deleted,
    # release_available — all wired this tag).
    # v1.21.5: new_tdb_theme_available is now surfaced too — shipped
    # as the "theme available to add" push by hooking the plex_enum
    # refresh path instead of resolve_theme_ids' hot loop (the
    # original deferral blocker). Its toggle must now be present.
    assert ('data-cfg-field="notifications.events.new_tdb_theme_available"'
            in html), (
        "v1.21.5: new_tdb_theme_available is surfaced — the toggle "
        "must be in settings.html now that the event ships."
    )


def test_settings_html_has_test_notification_button():
    """The TEST NOTIFICATION button must exist with the canonical
    ID the bindTestNotification handler hooks onto."""
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'id="test-notification-btn"' in html
    assert 'id="test-notification-status"' in html


def test_app_js_has_test_notification_handler():
    """The bindTestNotification handler exists + is called from
    the settings initializer."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "function bindTestNotification(" in js
    assert "bindTestNotification();" in js


def test_app_js_supports_data_cfg_field_lines():
    """The v1.17.0 newline-separated list variant must be handled
    in both populate (load) and collect (save) paths."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Load path
    assert 'data-cfg-field-lines]' in js
    assert "v.join('\\n')" in js
    # Save path
    assert "data-cfg-field-lines^=" in js
    assert "el.value.split('\\n')" in js


# ── 8. _ALLOWED_TOP_LEVEL gate ─────────────────────────────────


def test_notifications_section_in_allowed_top_level():
    """The PATCH /api/config gate must accept the 'notifications'
    section. Without this, every save 400s."""
    from app.web.api import _ALLOWED_TOP_LEVEL
    assert "notifications" in _ALLOWED_TOP_LEVEL
