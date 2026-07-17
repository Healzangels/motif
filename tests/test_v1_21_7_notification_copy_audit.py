"""v1.21.7 — notification copy-clarity fixes (post-audit).

A full audit of the notification surface (inventory + duplication +
copy) confirmed the system is structurally sound: no duplicate-trigger
bugs (all paths mutually exclusive by design after v1.21.6's sync
consolidation), every event has a dispatch site + severity, and the
load-bearing CTAs are correct. It surfaced one real copy bug + two
micro consistency fixes:

  1. The BULK ACTION COMPLETED toggle hint claimed it covers "LPS /
     PROBE TDB / DOWNLOAD / ADOPT", but bulk_action_completed only
     fires for PROBE TDB + LET PLEX SERVE (2 dispatch sites). Bulk
     download/adopt surface via the coalesced THEME ADDED event. The
     hint overclaimed → corrected.

  2. new_tdb_theme_available body said "...// MOTIF INFO card and //
     DOWNLOAD" — missing the "click" verb the other CTA bodies use.

  3. The test-notification body's "...back on the settings page" was
     awkward → "Settings → NOTIFICATIONS" + "no action needed".
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
NOTIFY_CONTENT = (REPO / "app" / "core" / "notify_content.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_bulk_hint_no_longer_overclaims():
    """The BULK ACTION COMPLETED hint must not claim DOWNLOAD / ADOPT
    fire it (they don't — they surface via THEME ADDED)."""
    idx = SETTINGS_HTML.index(
        'data-cfg-field="notifications.events.bulk_action_completed"')
    block = SETTINGS_HTML[idx:idx + 600]
    assert "PROBE TDB / DOWNLOAD / ADOPT" not in block, (
        "v1.21.7: stale overclaim — bulk_action_completed does not "
        "fire for bulk download/adopt"
    )
    assert "THEME ADDED" in block, (
        "v1.21.7: the hint should point bulk download/adopt at the "
        "THEME ADDED event"
    )


def test_bulk_action_completed_still_only_two_dispatch_sites():
    """Guard: bulk_action_completed fires from exactly 3 sites (PROBE TDB + LET PLEX
    SERVE + v0.51.195 BULK NORMALIZE). If a fourth lands (e.g. a real bulk download/adopt
    summary), the settings copy must be revisited so it stops being a lie."""
    assert API_PY.count('event_kind="bulk_action_completed"') == 3


def test_theme_available_body_uses_click_verb():
    """CTA consistency — other bodies say 'click // X'. v1.21.9
    sharpened this to the real action: SOURCE menu → DOWNLOAD TDB.
    Flatten the adjacent string literals before checking (the CTA
    spans a line break in source)."""
    import re
    idx = NOTIFY_CONTENT.index("def format_theme_available_body(")
    nxt = NOTIFY_CONTENT.find("\ndef ", idx + 1)
    body = NOTIFY_CONTENT[idx:nxt]
    flat = re.sub(r'"\s*"', "", body)
    assert "click // DOWNLOAD TDB" in flat


def test_test_notification_body_clarified():
    """The test-notification body should name Settings → NOTIFICATIONS
    and signal 'no action needed', not the awkward 'back on the
    settings page'."""
    assert "back on the settings page" not in API_PY
    assert "Settings → NOTIFICATIONS" in API_PY
    assert "no action needed" in API_PY


def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
