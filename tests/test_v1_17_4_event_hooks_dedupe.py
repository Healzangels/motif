"""v1.17.4 — phase 2 notification event hooks + dedupe.

v1.17.0 wired 4 of the 11 events defined in
`_DEFAULT_NOTIFY_EVENTS` (sync_completed, sync_failed,
bulk_action_completed, themes_added_by_sync). v1.17.4 wires 6
more, leaves 1 deferred (new_tdb_theme_available — needs to land
inside `resolve_theme_ids`'s 500-row chunk loop, hot path).

New events + their dedupe strategy:

  * **cookies_needed** — `app/core/worker.py` download-failure
    branch when kind == FailureKind.COOKIES_EXPIRED. 6h rate-
    limit via `notify_dedupe`; a sync run that hits N cookie-
    failed rows pings once.
  * **disk_low** — `app/core/worker.py` job-dispatch disk guard.
    12h rate-limit. Edge-trigger was considered but needs a
    recovery-reset path (added scope); rate-limit gives roughly
    the same UX without the new state machine.
  * **worker_restarted** — `app/main.py` boot zombie-sweep, ONLY
    when cur.rowcount > 0 (signals unclean prior shutdown). No
    dedupe — these should be rare and each one is meaningful.
  * **theme_added** — `app/core/worker.py` `_do_place` success
    path. Per-row, OFF by default — opt-in is the dedupe.
  * **theme_deleted** — `app/web/api.py` api_unmanage_item /
    api_forget_item / api_delete_item. Per-row, OFF by default.
  * **release_available** — `app/core/scheduler.py`
    `_check_release_update` on new tag detected. Tag-based dedupe
    (`edge_value=tag_name`) — each new release fires exactly once
    even across motif process restarts.

`new_tdb_theme_available` stays defined in
`_DEFAULT_NOTIFY_EVENTS` (so the dispatcher knows about it +
PATCH /api/config accepts it) but is NOT surfaced in the
settings UI in v1.17.4 — wiring it requires per-link dispatch
inside the chunked `resolve_theme_ids` loop, which is a hot
path. Deferred for a follow-up that can batch dispatches safely.

`notify_dedupe.py` (new module) owns the per-event state. Two
primitives:

  * `should_fire(db, kind, rate_limit_seconds=N, edge_value=V)`
    — returns True if not deduped. Both gates AND'd; pass `None`
    to skip a gate. Fail-open on DB read errors.
  * `record_fire(db, kind, value=V)` — stores last_at + optional
    last_value in `runtime_settings` keyed by
    `notify_dedupe.<event_kind>`. No schema migration needed
    (the runtime_settings table is already a key/value store).
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from unittest.mock import patch


REPO = Path(__file__).resolve().parent.parent
APP_INIT = REPO / "app" / "__init__.py"
APP_MAIN = REPO / "app" / "main.py"
APP_WORKER = REPO / "app" / "core" / "worker.py"
APP_SCHEDULER = REPO / "app" / "core" / "scheduler.py"
APP_API = REPO / "app" / "web" / "api.py"
NOTIFY_DEDUPE_PY = REPO / "app" / "core" / "notify_dedupe.py"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Version pin ──────────────────────────────────────────────


def test_version_at_least_v1_17_4():
    import re
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    major, minor, patch_ = int(m.group(1)), int(m.group(2)), int(m.group(3))
    assert (major, minor, patch_) >= (0, 17, 4)


# ── notify_dedupe module: primitives ─────────────────────────


def test_notify_dedupe_module_exists():
    """`app/core/notify_dedupe.py` ships with the two primitive
    helpers + a clear() escape hatch (for tests + future admin
    surfaces)."""
    src = NOTIFY_DEDUPE_PY.read_text()
    assert "def should_fire(" in src
    assert "def record_fire(" in src
    assert "def clear(" in src


def _bootstrap_runtime_settings_table(db_path: Path) -> None:
    """Helper: create the runtime_settings table for an in-test
    sqlite DB so the dedupe primitives have somewhere to read /
    write. Mirrors the SCHEMA fragment in db.py."""
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS runtime_settings (
                key         TEXT PRIMARY KEY,
                value       TEXT NOT NULL,
                updated_at  TEXT NOT NULL,
                updated_by  TEXT
            );
        """)


def test_notify_dedupe_fail_open_returns_true_when_state_missing(tmp_path):
    """`should_fire` returns True if there's no recorded state for
    the event — first fire is always allowed."""
    db = tmp_path / "t.db"
    _bootstrap_runtime_settings_table(db)
    from app.core import notify_dedupe
    assert notify_dedupe.should_fire(
        db, "cookies_needed", rate_limit_seconds=3600) is True


def test_notify_dedupe_rate_limit_blocks_second_fire(tmp_path):
    """After record_fire, a should_fire call within the rate-
    limit window returns False; outside the window returns True."""
    db = tmp_path / "t.db"
    _bootstrap_runtime_settings_table(db)
    from app.core import notify_dedupe
    notify_dedupe.record_fire(db, "cookies_needed")
    # Immediate re-check is within the 1h window.
    assert notify_dedupe.should_fire(
        db, "cookies_needed", rate_limit_seconds=3600) is False
    # Effectively-zero rate-limit means "always allow."
    assert notify_dedupe.should_fire(
        db, "cookies_needed", rate_limit_seconds=0) is True


def test_notify_dedupe_edge_value_blocks_same_value(tmp_path):
    """edge_value gate: should_fire returns False when the
    proposed edge_value equals the last-recorded value. Used by
    release_available (last_value = tag_name) so each new
    release fires exactly once."""
    db = tmp_path / "t.db"
    _bootstrap_runtime_settings_table(db)
    from app.core import notify_dedupe
    notify_dedupe.record_fire(db, "release_available", value="v1.17.4")
    # Same tag — deduped.
    assert notify_dedupe.should_fire(
        db, "release_available", edge_value="v1.17.4") is False
    # Different tag — fires.
    assert notify_dedupe.should_fire(
        db, "release_available", edge_value="v1.17.5") is True


def test_notify_dedupe_clear_resets_state(tmp_path):
    """clear() drops the dedupe row, so the next should_fire is
    fresh-state (returns True)."""
    db = tmp_path / "t.db"
    _bootstrap_runtime_settings_table(db)
    from app.core import notify_dedupe
    notify_dedupe.record_fire(db, "cookies_needed")
    assert notify_dedupe.should_fire(
        db, "cookies_needed", rate_limit_seconds=3600) is False
    notify_dedupe.clear(db, "cookies_needed")
    assert notify_dedupe.should_fire(
        db, "cookies_needed", rate_limit_seconds=3600) is True


def test_notify_dedupe_fail_open_on_db_read_error(tmp_path):
    """If the runtime_settings table doesn't exist (or any other
    read failure), should_fire returns True. Losing a dedupe
    round beats swallowing a notification the operator wanted."""
    db = tmp_path / "nonexistent.db"
    from app.core import notify_dedupe
    # No table bootstrap — read will fail.
    assert notify_dedupe.should_fire(
        db, "anything", rate_limit_seconds=3600) is True


# ── cookies_needed hook (worker.py) ──────────────────────────


def test_cookies_needed_hook_wired_with_rate_limit():
    """worker.py's COOKIES_EXPIRED branch must dispatch
    cookies_needed gated by notify_dedupe.should_fire with a 6h
    rate-limit."""
    src = APP_WORKER.read_text()
    # Anchor on the cookies_needed dispatch block; widen the
    # post-anchor window so the record_fire call (which lives
    # AFTER the dispatch) lands inside the window.
    anchor = src.index('event_kind="cookies_needed"')
    block = src[max(0, anchor - 2000):anchor + 2000]
    assert "FailureKind.COOKIES_EXPIRED" in block
    assert "should_fire" in block
    assert "rate_limit_seconds=6 * 3600" in block, (
        "v1.17.4: cookies_needed rate-limit must be 6 hours so a "
        "sync run that hits many cookie-failed rows pings once."
    )
    assert "record_fire" in block


# ── disk_low hook (worker.py) ────────────────────────────────


def test_disk_low_hook_wired_with_rate_limit():
    """worker.py's min_free_disk_mb guard must dispatch disk_low
    gated by a 12h rate-limit."""
    src = APP_WORKER.read_text()
    anchor = src.index('event_kind="disk_low"')
    block = src[max(0, anchor - 2000):anchor + 500]
    assert "min_free_disk_mb" in block
    assert "rate_limit_seconds=12 * 3600" in block, (
        "v1.17.4: disk_low rate-limit must be 12 hours."
    )


# ── worker_restarted hook (main.py) ──────────────────────────


def test_worker_restarted_hook_wired_on_zombie_sweep_hit():
    """app/main.py's boot zombie-sweep must fire worker_restarted
    ONLY when cur.rowcount > 0 (signal: previous shutdown was
    unclean). Clean restarts must NOT ping."""
    src = APP_MAIN.read_text()
    # Find the zombie-sweep block. Anchor on the v1.12.113 marker
    # comment (more stable than the "Zombie running-job sweep"
    # phrase which also appears in the except's log.warning).
    sweep_idx = src.index("v1.12.113: zombie running-job sweep")
    sweep_end = src.index('log.warning("Zombie running-job sweep skipped',
                          sweep_idx)
    sweep_block = src[sweep_idx:sweep_end]
    assert 'event_kind="worker_restarted"' in sweep_block
    # The dispatch must be INSIDE the `if cur.rowcount:` block.
    rowcount_idx = sweep_block.index("if cur.rowcount:")
    dispatch_idx = sweep_block.index('event_kind="worker_restarted"')
    assert rowcount_idx < dispatch_idx, (
        "v1.17.4: worker_restarted dispatch must be gated on "
        "cur.rowcount > 0 so clean restarts don't ping."
    )


# ── theme_added hook (worker.py _do_place) ───────────────────


def test_theme_added_hook_wired_on_place_success():
    """worker.py's `_do_place` success path (outcome.placed
    True) must fire theme_added. Per-row, no dedupe — the
    OFF-by-default toggle is the dedupe."""
    src = APP_WORKER.read_text()
    fn_anchor = src.index("def _do_place(")
    fn_end = src.index("\n    def ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert 'event_kind="theme_added"' in body
    # The dispatch must be gated on outcome.placed.
    dispatch_idx = body.index('event_kind="theme_added"')
    # Look backward for the `if outcome.placed:` gate.
    gate_idx = body.rfind("if outcome.placed:", 0, dispatch_idx)
    assert gate_idx != -1, (
        "v1.17.4: theme_added dispatch must be gated on "
        "outcome.placed (only fires when the place actually "
        "produced a theme — skipped placements don't ping)."
    )


# ── theme_deleted hook (3 api endpoints) ─────────────────────


def test_theme_deleted_hook_wired_on_unmanage():
    src = APP_API.read_text()
    fn_anchor = src.index("async def api_unmanage_item(")
    fn_end = src.index("\n    @app.post(", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert 'event_kind="theme_deleted"' in body


def test_theme_deleted_hook_wired_on_forget():
    src = APP_API.read_text()
    fn_anchor = src.index("async def api_forget_item(")
    fn_end = src.index("\n    @app.delete(", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert 'event_kind="theme_deleted"' in body


def test_theme_deleted_hook_wired_on_delete():
    src = APP_API.read_text()
    fn_anchor = src.index("async def api_delete_item(")
    fn_end = src.index("\n    @app.post(", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert 'event_kind="theme_deleted"' in body


# ── release_available hook (scheduler.py) ────────────────────


def test_release_available_hook_wired_with_tag_dedupe():
    """scheduler.py's _check_release_update must dispatch
    release_available with tag-based edge dedupe so each new
    release pings exactly once across motif process restarts."""
    src = APP_SCHEDULER.read_text()
    anchor = src.index('event_kind="release_available"')
    block = src[max(0, anchor - 2500):anchor + 500]
    assert 'edge_value=payload["tag_name"]' in block, (
        "v1.17.4: release_available must use tag-name edge "
        "dedupe so each new release pings exactly once."
    )
    # Also gated on "tag != running motif version" so the
    # release check doesn't ping on motif's own version.
    assert "running" in block and "motif_version" in block


# ── Settings UI surfaces the 6 new toggles ────────────────────


def test_settings_html_surfaces_six_new_event_toggles():
    """Each of the 6 newly-wired events must have a checkbox in
    the settings.html EVENTS block."""
    html = SETTINGS_HTML.read_text()
    for event_kind in (
        "cookies_needed",
        "disk_low",
        "worker_restarted",
        "theme_added",
        "theme_deleted",
        "release_available",
    ):
        assert f'data-cfg-field="notifications.events.{event_kind}"' in html, (
            f"v1.17.4: settings.html must surface a checkbox for "
            f"notifications.events.{event_kind}"
        )


def test_settings_html_surfaces_new_tdb_theme_available():
    """v1.21.5: `new_tdb_theme_available` shipped as the "theme
    available to add" push (hooked on the plex_enum refresh path,
    not resolve_theme_ids' hot loop). Its toggle is now surfaced and
    the old "deferred — hook lives in resolve_theme_ids' hot loop"
    form-hint is gone."""
    html = SETTINGS_HTML.read_text()
    notif_section = html[html.index('data-panel="notifications"'):]
    notif_section = notif_section[:notif_section.index("============================ TOKENS")]
    assert ('data-cfg-field="notifications.events.new_tdb_theme_available"'
            in notif_section), (
        "v1.21.5: the new_tdb_theme_available toggle must be surfaced "
        "in the notifications panel."
    )
    # The stale deferred rationale must NOT linger as a form-hint
    # claiming the toggle isn't surfaced.
    assert "not yet UI-surfaced" not in notif_section, (
        "v1.21.5: the 'not yet UI-surfaced' deferred note must be "
        "removed now that the toggle ships."
    )
