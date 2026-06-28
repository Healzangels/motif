"""v1.22.60 (audit round 2, Batch A #1) — tier-1/tier-2 theme-lost
notifications were dead since v1.19.41.

The reaper dispatches `theme_lost_backup_ready` ("🎯 deploy your
backup") and `theme_lost_sidecar_available` ("ADOPT your sidecar"),
but neither kind was ever added to `_DEFAULT_NOTIFY_EVENTS` — and
`notify.dispatch` drops unknown kinds with only a docker-log WARN.
Net effect on every install: the actionable tiers of the v1.19.41
four-way split never sent a single notification (only Tier-4
`plex_theme_lost`, OFF by default, could fire), while the 24h dedupe
still stamped each attempt. The v1.19.41 tests were the v1.18.81
phantom shape — they pinned the kind strings in source but never
exercised dispatch.

This tag: adds both kinds to the defaults (ON — rare, actionable,
24h-dedupe-limited, per the backup_ready_to_deploy precedent), adds
the settings EVENTS toggles, and lands the recurrence LINT: every
event-kind literal anywhere in app/ must exist in
_DEFAULT_NOTIFY_EVENTS — including kinds assigned to a variable
before the dispatch call, the exact shape the naive literal-grep
(and three releases of review) missed.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


# ── the two kinds are live in the defaults ───────────────────


def test_tiered_kinds_present_and_on_by_default():
    from app.core.config_file import _DEFAULT_NOTIFY_EVENTS
    assert _DEFAULT_NOTIFY_EVENTS.get("theme_lost_backup_ready") is True
    assert _DEFAULT_NOTIFY_EVENTS.get("theme_lost_sidecar_available") is True
    # Tier-4 stays opt-in (its OFF default was a deliberate call).
    assert _DEFAULT_NOTIFY_EVENTS.get("plex_theme_lost") is False


def test_dispatch_no_longer_drops_the_tiered_kinds(tmp_path):
    """Behavioral (not source-text): dispatch with a DEFAULT events
    config must get PAST the unknown-kind gate and the enabled gate
    for both tiers — i.e. reach the sink check. We give it one
    apprise URL and stub the pool submit to observe arrival."""
    from app.core import notify
    from app.core.config_file import NotificationsConfig
    cfg = NotificationsConfig(apprise_urls=["json://localhost/stub"])
    for kind in ("theme_lost_backup_ready", "theme_lost_sidecar_available"):
        with patch.object(notify, "_get_pool") as pool:
            notify.dispatch(
                tmp_path / "motif.db", cfg,
                event_kind=kind, title="t", body="b",
            )
            assert pool.return_value.submit.called, (
                f"{kind} must reach the pool-submit send path with "
                "default config (pre-v1.22.60 it was dropped at the "
                "unknown-kind gate)"
            )


def test_settings_ui_has_toggles_for_both_tiers():
    assert ('data-cfg-field="notifications.events.theme_lost_backup_ready"'
            in SETTINGS_HTML)
    assert ('data-cfg-field="notifications.events.'
            'theme_lost_sidecar_available"' in SETTINGS_HTML)


# ── recurrence lint: every dispatched kind exists in defaults ─


def _collect_dispatched_kinds() -> set[str]:
    """Every string that can reach notify.dispatch's event_kind:
    (a) `event_kind="..."` keyword literals, and (b) assignments to
    a name ending in `event_kind` (the variable-then-dispatch shape
    that hid the v1.19.41 kinds from naive grep)."""
    kinds: set[str] = set()
    for py in (REPO / "app").rglob("*.py"):
        src = py.read_text()
        if "event_kind" not in src:
            continue
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if (kw.arg == "event_kind"
                            and isinstance(kw.value, ast.Constant)
                            and isinstance(kw.value.value, str)):
                        kinds.add(kw.value.value)
            elif isinstance(node, ast.Assign):
                for tgt in node.targets:
                    if (isinstance(tgt, ast.Name)
                            and tgt.id.endswith("event_kind")
                            and isinstance(node.value, ast.Constant)
                            and isinstance(node.value.value, str)):
                        kinds.add(node.value.value)
    return kinds


def test_every_dispatched_kind_is_in_defaults():
    """The contract-drift guard (v1.17.10 class): a dispatch site
    using a kind missing from _DEFAULT_NOTIFY_EVENTS is a DEAD
    notification — dispatch drops unknown kinds, and the api.py
    closed-set PATCH filter prevents users from ever enabling them.
    Fails loud the moment a new kind lands without its default."""
    from app.core.config_file import _DEFAULT_NOTIFY_EVENTS
    dispatched = _collect_dispatched_kinds()
    assert dispatched, "lint must find the known dispatch sites"
    missing = dispatched - set(_DEFAULT_NOTIFY_EVENTS)
    assert missing == set(), (
        f"event kind(s) dispatched but missing from "
        f"_DEFAULT_NOTIFY_EVENTS (the notification is DEAD): {missing}"
    )


def test_lint_sees_the_variable_assignment_shape():
    """Meta-test: the collector must pick up `_event_kind = "..."`
    assignments — that shape is exactly what hid the v1.19.41 bug."""
    dispatched = _collect_dispatched_kinds()
    assert "theme_lost_backup_ready" in dispatched
    assert "theme_lost_sidecar_available" in dispatched
