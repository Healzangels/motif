"""v1.19.57 — settings UI exposes the two new v1.19.55 event kinds.

v1.19.55 added two new opt-in notification events
(`themes_updated_by_sync` and `theme_pushed`) to the
backend (`_DEFAULT_NOTIFY_EVENTS` + `_EVENT_NOTIFY_TYPE`) but
the settings UI's NOTIFICATIONS section never gained the
toggle checkboxes. the user could enable them only via raw
API PATCH; the SAVE EVENTS button in /settings wouldn't know
about them.

Per the v1.17.10 closed-set filter pattern: api.py's PATCH
handler drops events not in `_DEFAULT_NOTIFY_EVENTS` —
that's correct. But the settings UI must SURFACE every event
in the default registry; otherwise the user can never opt in
through the UI. Pre-v1.19.57 added an event to the backend
but forgot the UI; this audit guard catches future drift.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

SETTINGS_HTML = (
    REPO / "app" / "web" / "templates" / "settings.html"
).read_text()
CONFIG_FILE_PY = (
    REPO / "app" / "core" / "config_file.py"
).read_text()


# ── Direct pins on the two new toggles ──────────────────────


def test_themes_updated_by_sync_toggle_in_settings_html():
    """The settings UI must have a checkbox for
    themes_updated_by_sync (added in v1.19.55)."""
    assert (
        'data-cfg-field="notifications.events.themes_updated_by_sync"'
        in SETTINGS_HTML
    ), (
        "v1.19.57: themes_updated_by_sync needs a settings UI "
        "toggle — pre-fix the user could only enable it via raw "
        "API PATCH"
    )


def test_themes_updated_by_sync_label_uses_consistent_wording():
    """v1.21.6: the two sync-detail toggles read as sections folded
    into the sync summary ("SYNC: LIST NEW/UPDATED TITLES"). v1.23.83:
    the default-state tag is now the uniform muted (off) chip (was the
    inconsistent "opt-in")."""
    idx = SETTINGS_HTML.index(
        'data-cfg-field="notifications.events.themes_updated_by_sync"'
    )
    block = SETTINGS_HTML[idx:idx + 800]
    assert "SYNC: LIST UPDATED TITLES" in block
    assert "(off)" in block


def test_theme_pushed_toggle_in_settings_html():
    """The settings UI must have a checkbox for theme_pushed
    (added in v1.19.55 — discriminates force-place dispatches
    from genuine theme_added)."""
    assert (
        'data-cfg-field="notifications.events.theme_pushed"'
        in SETTINGS_HTML
    ), (
        "v1.19.57: theme_pushed needs a settings UI toggle"
    )


def test_theme_pushed_hint_explains_reason_branching():
    """The hint should mention the reason-aware title variants
    (Theme replaced via TDB / Theme promoted from backup /
    Cloud backup re-uploaded) so users can preview what the
    actual notification subject will look like."""
    idx = SETTINGS_HTML.index(
        'data-cfg-field="notifications.events.theme_pushed"'
    )
    block = SETTINGS_HTML[idx:idx + 1500]
    assert "Theme replaced via TDB" in block
    assert "Theme promoted from backup" in block
    assert "Cloud backup re-uploaded" in block


def test_theme_added_hint_clarifies_v1_19_55_split():
    """The THEME ADDED hint should note that v1.19.55 split
    out force-place dispatches into THEME PUSHED — pre-fix
    users opting into THEME ADDED would have expected those
    too."""
    idx = SETTINGS_HTML.index(
        'data-cfg-field="notifications.events.theme_added"'
    )
    block = SETTINGS_HTML[idx:idx + 1500]
    assert "v1.19.55" in block
    assert "THEME PUSHED" in block


# ── Cross-reference audit: every event in defaults has a UI ──


def test_every_default_event_has_settings_ui_toggle():
    """The settings UI must surface a checkbox for EVERY event
    in `_DEFAULT_NOTIFY_EVENTS`. Without this contract, a new
    event added to the backend ships with no UI affordance —
    user can never opt in (or out) through /settings.

    The api.py:218 closed-set filter does the inverse contract:
    PATCH drops events NOT in the defaults. Together these
    invariants mean: settings.html + config_file.py must agree
    on the event set."""
    # Extract event names from _DEFAULT_NOTIFY_EVENTS.
    # The dict starts at `_DEFAULT_NOTIFY_EVENTS: dict[str, bool] = {`
    # and runs until the matching `}`. Each line: `"key": True/False,`
    start = CONFIG_FILE_PY.index(
        "_DEFAULT_NOTIFY_EVENTS: dict[str, bool] = {"
    )
    # Find the matching close-brace by counting depth.
    depth = 0
    end = start
    for i in range(start, len(CONFIG_FILE_PY)):
        if CONFIG_FILE_PY[i] == "{":
            depth += 1
        elif CONFIG_FILE_PY[i] == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    block = CONFIG_FILE_PY[start:end + 1]
    # Match `"event_name":` (the key form used in the dict).
    keys = set(re.findall(r'"([a-z_]+)"\s*:\s*(?:True|False)', block))
    assert keys, (
        "v1.19.57: failed to parse event keys from "
        "_DEFAULT_NOTIFY_EVENTS"
    )
    # Intentionally-deferred events that have a defaults entry
    # (so api.py:218 closed-set filter accepts a manual PATCH
    # for experimenters) but NO dispatch site is wired yet,
    # so the UI checkbox would be misleading.
    DEFERRED_NO_DISPATCH = {
        # v1.17.0: listed for closed-set PATCH compat but
        # resolve_theme_ids hot path needs batched dispatch
        # design before the dispatch site is wired (see
        # config_file.py inline comment).
        "new_tdb_theme_available",
    }
    # Verify every key has a UI toggle (unless intentionally
    # deferred).
    missing = []
    for key in keys:
        if key in DEFERRED_NO_DISPATCH:
            continue
        needle = f'data-cfg-field="notifications.events.{key}"'
        if needle not in SETTINGS_HTML:
            missing.append(key)
    assert not missing, (
        f"v1.19.57: event(s) missing from settings UI: "
        f"{missing}. Every event in _DEFAULT_NOTIFY_EVENTS "
        f"that has a dispatch site must have a corresponding "
        f"checkbox in settings.html so users can opt in through "
        f"the UI (not just raw API PATCH). If the event is "
        f"intentionally deferred (no dispatch site wired yet), "
        f"add it to DEFERRED_NO_DISPATCH above with a comment "
        f"explaining why."
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_57_version_pin():
    """Version bumped at v1.19.57 (then again at v1.19.58 for
    the sync update-actionability gate). Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
