"""v1.21.46 — backup-landing notification + per-kind coalesce window.

the user's report: a bulk ACCEPT on newly-added ThemerrDB P-rows
landed N backups (the rows went to link state TB) with NO
notification, and the existing coalesced notifications "feel
delayed."

Root cause of the silence: theme_added / theme_pushed in
worker._do_place are gated on `outcome.placed`. A backup is
downloaded but deliberately NOT placed (the worker stamps the
local_file `last_place_attempt_reason='backup_only'`), so neither
event ever fired for a backup landing.

Two fixes:

  B. New `theme_backed_up` event kind (default ON) dispatched at the
     backup_only stamp in worker._record_local_file — covers P-row
     bulk ACCEPT, DOWNLOAD TDB BACKUP, SET URL + KEEP AS BACKUP, CSV
     import-as-backup. Coalesced like theme_added: the first backup
     pings immediately (leading edge), a bulk collapses to one summary.

  C. Per-kind trailing window. theme_backed_up arrives on the rate-
     limited DOWNLOAD cadence (~15-30s apart), so a 60s window still
     coalesces a bulk but lands the summary a full minute sooner than
     the 120s default. theme_added/theme_pushed KEEP 120s — shortening
     those re-introduces the per-place spam the user reported on
     2026-05-30 (v1.20.44). Centralized in
     notify._COALESCE_WINDOW_BY_KIND.
"""
from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent


# ── B: formatters ────────────────────────────────────────────


def test_title_uses_backed_up_verb_and_floppy_emoji():
    from app.core import notify_content as nc
    title = nc.format_theme_backed_up_title({"display_title": "Solo (2024)"})
    assert title.startswith("💾 ")
    assert "backed up" in title.lower()
    assert "Solo (2024)" in title


def test_body_states_non_actionable_and_carries_url():
    from app.core import notify_content as nc
    body = nc.format_theme_backed_up_body({
        "title": "Solo", "year": 2024,
        "provenance": "themerrdb",
        "theme_url": "https://youtu.be/abcdefghijk",
    })
    # Non-actionable framing — the current theme keeps playing.
    assert "keeps playing" in body.lower()
    # Source line (provenance) present.
    assert "Source:" in body
    # URL on its own line for the auto-embed preview.
    assert "https://youtu.be/abcdefghijk" in body


def test_body_survives_missing_url():
    from app.core import notify_content as nc
    body = nc.format_theme_backed_up_body({"title": "Solo"})
    assert "keeps playing" in body.lower()
    assert "http" not in body  # no URL line when none staged


def test_batch_title_and_body_shapes():
    from app.core import notify_content as nc
    assert nc.format_theme_backed_up_batch_title(3) == "💾 3 themes backed up"
    body = nc.format_theme_backed_up_batch_body(["A (2024)", "B (2023)"])
    assert "* A (2024)" in body
    assert "* B (2023)" in body
    # No URLs in a batch body (would spawn N embed cards).
    assert "http" not in body


def test_item_label_is_display_title():
    from app.core import notify_content as nc
    assert nc.format_theme_backed_up_item(
        {"display_title": "Solo (2024)"}) == "Solo (2024)"


# ── B: default toggle is ON + registered in the closed set ───


def test_theme_backed_up_default_on():
    from app.core.config_file import _DEFAULT_NOTIFY_EVENTS
    assert "theme_backed_up" in _DEFAULT_NOTIFY_EVENTS, (
        "theme_backed_up must be in the closed-set registry or api.py's "
        "PATCH filter drops it"
    )
    assert _DEFAULT_NOTIFY_EVENTS["theme_backed_up"] is True, (
        "ON by default — the backup landing was previously silent"
    )


def test_settings_html_has_toggle():
    """Belt-and-suspenders alongside the test_v1_19_57 registry-walk:
    a backed-up toggle must be surfaced so the operator can turn it OFF
    if per-backup pings get noisy."""
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert (
        'data-cfg-field="notifications.events.theme_backed_up"' in html
    )


# ── C: per-kind window ───────────────────────────────────────


def test_window_map_has_60s_for_backed_up():
    from app.core import notify as n
    assert n._COALESCE_WINDOW_BY_KIND.get("theme_backed_up") == 60.0


def test_theme_added_pushed_keep_120s_default():
    """Shortening theme_added/theme_pushed re-introduces the per-place
    spam the user reported on 2026-05-30 — they must NOT be in the
    override map and must resolve to the 120s default."""
    from app.core import notify as n
    assert "theme_added" not in n._COALESCE_WINDOW_BY_KIND
    assert "theme_pushed" not in n._COALESCE_WINDOW_BY_KIND
    assert n._COALESCE_WINDOW_S == 120.0


def _cfg(*kinds):
    return SimpleNamespace(
        events={k: True for k in kinds},
        apprise_urls=["discord://x/y"],
        apprise_external_url="",
    )


@pytest.fixture
def notify_capture(monkeypatch):
    """Reset coalesce state + capture the window each dispatch arms."""
    from app.core import notify as n
    n._COALESCE_BUF.clear()
    n._COALESCE_TIMERS.clear()
    n._COALESCE_CFG.clear()  # v0.51.257: replaced the write-only ACTIVE flag
    armed = []
    monkeypatch.setattr(
        n, "_arm_coalesce_timer",
        lambda db, cfg, event_kind, window_seconds:
            armed.append((event_kind, window_seconds)),
    )
    # Swallow the leading-edge real send.
    monkeypatch.setattr(
        n, "dispatch",
        lambda db, cfg, *, event_kind, title, body, body_format="text": None,
    )
    return n, armed


def test_backed_up_resolves_to_60s_when_caller_omits_window(notify_capture):
    """A BULK theme_backed_up with no window_seconds → the per-kind map wins
    (v1.23.46: the window/timer only applies to the bulk path)."""
    from app.core import notify_content as nc
    n, armed = notify_capture
    n.dispatch_coalesced(
        Path("/x"), _cfg("theme_backed_up"),
        event_kind="theme_backed_up",
        item_label="Solo (2024)",
        single_title="💾 Theme backed up — Solo (2024)",
        single_body="body",
        batch_title_fn=nc.format_theme_backed_up_batch_title,
        batch_body_fn=nc.format_theme_backed_up_batch_body,
        body_format="markdown",
        bulk=True,
    )
    assert armed, "timer should be armed when a bulk item buffers"
    assert armed[-1] == ("theme_backed_up", 60.0)


def test_theme_pushed_resolves_to_120s_default(notify_capture):
    from app.core import notify_content as nc
    n, armed = notify_capture
    n.dispatch_coalesced(
        Path("/x"), _cfg("theme_pushed"),
        event_kind="theme_pushed",
        item_label="Solo (2024)",
        single_title="📤 Theme pushed — Solo (2024)",
        single_body="body",
        batch_title_fn=nc.format_theme_pushed_batch_title,
        batch_body_fn=nc.format_theme_pushed_batch_body,
        body_format="markdown",
        bulk=True,
    )
    assert armed[-1] == ("theme_pushed", 120.0)


def test_explicit_window_override_still_wins(notify_capture):
    """A caller passing window_seconds explicitly bypasses the map
    (the test_v1_19_95 huge-window pattern must keep working)."""
    from app.core import notify_content as nc
    n, armed = notify_capture
    n.dispatch_coalesced(
        Path("/x"), _cfg("theme_backed_up"),
        event_kind="theme_backed_up",
        item_label="Solo (2024)",
        single_title="t", single_body="b",
        batch_title_fn=nc.format_theme_backed_up_batch_title,
        batch_body_fn=nc.format_theme_backed_up_batch_body,
        window_seconds=3600,
        bulk=True,
    )
    assert armed[-1] == ("theme_backed_up", 3600)


# ── B: worker wiring (source pins) ───────────────────────────


def test_worker_dispatches_backed_up_at_backup_stamp():
    """The dispatch must be wired at the backup_only stamp branch,
    gated on did_backup_stamp, and OUTSIDE the transaction (so the
    Apprise HTTP call doesn't hold the DB lock)."""
    worker = (REPO / "app" / "core" / "worker.py").read_text()
    assert 'event_kind="theme_backed_up"' in worker
    assert "did_backup_stamp = True" in worker
    assert "if did_backup_stamp:" in worker
    # The dispatch (and its flag-read guard) must sit AFTER the stamp's
    # UPDATE, which itself sits inside the backup-reason whitelist.
    stamp = worker.index("last_place_attempt_reason = 'backup_only' ")
    flag_set = worker.index("did_backup_stamp = True")
    dispatch = worker.index('event_kind="theme_backed_up"')
    assert stamp < flag_set < dispatch


def test_worker_backed_up_uses_default_window():
    """The worker call must NOT pass window_seconds — it relies on the
    per-kind map resolving 60s. A future edit that hardcodes a window
    here would silently bypass the centralized tuning."""
    worker = (REPO / "app" / "core" / "worker.py").read_text()
    start = worker.index('event_kind="theme_backed_up"')
    # The dispatch_coalesced call spans from its open to the next
    # close-paren-at-call-indent; scan a generous window.
    block = worker[start - 200:start + 600]
    assert "format_theme_backed_up_item" in block
    assert "window_seconds" not in block, (
        "let _COALESCE_WINDOW_BY_KIND resolve the 60s window"
    )
