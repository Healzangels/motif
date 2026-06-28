"""v1.17.2 — three small UX fixes on the NOTIFICATIONS page +
bulk-bar M / M+P clarity.

the user flagged two UI nits on /settings → NOTIFICATIONS:

  1. The events block needed its own SAVE button below the
     toggles — the single SAVE NOTIFICATIONS button up top
     visually grouped URL config and event toggles into a
     single concept, but they're conceptually distinct.

  2. The TEST NOTIFICATION "✓ embedded 1/1" status lingered
     forever after a click. Other status pings in motif auto-
     dismiss after a few seconds (save buttons clear at 2.5s);
     the test button should follow the same pattern.

Plus a third, surfaced when the user filtered SRC=M with 1342
rows selected and asked why ADOPT SELECTED showed (860) instead
of (1342). The answer: 482 of the 1342 are M+P composites
(yellow-dot M chip — Plex serves its own theme alongside the
sidecar), and v1.15.49 split those off into // ADOPT + LET
PLEX SERVE to prevent the ambiguous-double-theme footgun. But
the UI didn't communicate that split clearly. v1.17.2 adds
tooltips to both buttons explaining the bucket separation +
the design intent.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_INIT = REPO / "app" / "__init__.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Version pin ──────────────────────────────────────────────


def test_version_at_least_v1_17_2():
    """v1.17.2 ships normal patch. Soft pin to allow future bumps
    without breaking this test; v1.13.79 link-fixes pins the
    current bump-before-tag value."""
    import re
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))
    assert (major, minor, patch) >= (0, 17, 2)


# ── Item 1: events block has its own SAVE button ──────────────


def test_settings_events_block_has_dedicated_save_button():
    """The events block now ships with a `// SAVE EVENTS` button
    below the toggle grid. Same `data-save="notifications"` value
    as the top button — both fire the same PATCH (backend
    handles partials idempotently), so the second button is
    purely a UX-grouping affordance."""
    html = SETTINGS_HTML.read_text()
    # Anchor on the events block close (the LAST closing form-grid
    # inside the notifications section).
    notif_anchor = html.index('data-panel="notifications"')
    notif_end = html.index("============================ TOKENS", notif_anchor)
    notif_section = html[notif_anchor:notif_end]
    # Two SAVE buttons + two status spans, both keyed on notifications.
    save_buttons = notif_section.count('data-save="notifications"')
    status_spans = notif_section.count('data-save-status="notifications"')
    assert save_buttons == 2, (
        f"v1.17.2: NOTIFICATIONS section must have 2 SAVE buttons "
        f"(one for URLs + sinks up top, one for events below), "
        f"found {save_buttons}."
    )
    assert status_spans == 2, (
        f"v1.17.2: each SAVE button needs its own form-status "
        f"sibling, found {status_spans}."
    )
    # The new button uses the // SAVE EVENTS label.
    assert ">// SAVE EVENTS</button>" in notif_section, (
        "v1.17.2: the second SAVE button must read '// SAVE EVENTS' "
        "(matches the user's mental model of 'save the toggle "
        "section' separate from 'save the URLs')."
    )


def test_bindConfigSaves_updates_every_matching_status_element():
    """When two SAVE buttons share the same `data-save` tab value,
    clicking either must update BOTH status spans (so the user
    sees the ✓ saved flash next to whichever button they clicked,
    regardless of which one fired the click)."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function bindConfigSaves()")
    fn_end = js.index("\n  }\n", fn_anchor + 1)
    body = js[fn_anchor:fn_end]
    assert 'document.querySelectorAll(' in body, (
        "v1.17.2: status lookup must use querySelectorAll to "
        "catch all matching status spans, not querySelector."
    )
    assert "statuses.forEach(" in body, (
        "v1.17.2: every status span (not just the first match) "
        "must update on save."
    )


# ── Item 2: TEST NOTIFICATION status auto-dismisses ──────────


def test_test_notification_status_auto_dismisses():
    """`bindTestNotification` must schedule a setTimeout in the
    finally block to clear the status text after a short delay.
    Mirrors the save-button auto-clear pattern (which clears at
    2500ms). v1.17.2 uses 4000ms for test status — a bit longer
    so the user can read the per-sink outcome line."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function bindTestNotification()")
    fn_end = js.index("\n  }\n", fn_anchor + 1)
    body = js[fn_anchor:fn_end]
    assert "setTimeout(" in body, (
        "v1.17.2: TEST NOTIFICATION must schedule an auto-clear "
        "setTimeout in the finally block."
    )
    # The timeout fires inside finally so it always runs, even on
    # error responses (the user shouldn't have to manually clear
    # a stale fail message either).
    finally_idx = body.index("} finally {")
    finally_block = body[finally_idx:]
    assert "setTimeout(" in finally_block, (
        "v1.17.2: the auto-clear must live in the finally block "
        "so it fires on both success + error paths."
    )
    # Status text + class both cleared.
    assert "status.textContent = ''" in finally_block
    assert "status.className = 'form-status'" in finally_block


# ── Item 3: ADOPT / ADOPT+LPS bucket-split tooltips ──────────


def test_adopt_selected_tooltip_explains_m_vs_m_plus_p_split():
    """The // ADOPT SELECTED button now carries a title tooltip
    that surfaces the v1.15.49 bucket split: M-only rows route
    here, M+P composites route to // ADOPT + LET PLEX SERVE.
    Pre-fix the user saw 1342 M-filtered rows but only (860) on
    ADOPT — the missing 482 M+P composites were invisible."""
    js = APP_JS.read_text()
    # Anchor on the adoptBtn configuration block (just after the
    # withCount call).
    anchor = js.index("withCount('// ADOPT SELECTED', adoptOnlyCount)")
    block = js[anchor:anchor + 2000]
    assert "adoptBtn.title" in block, (
        "v1.17.2: adoptBtn must have a title tooltip explaining "
        "the M vs M+P bucket split."
    )
    # The tooltip references the M+P composite routing.
    assert "ADOPT + LET PLEX SERVE" in block
    assert "yellow-dot" in block, (
        "v1.17.2: the tooltip should reference the visual cue "
        "(yellow dot on the M chip) so the user can spot M+P "
        "rows in the table."
    )


def test_adopt_lps_tooltip_explains_two_step_intent():
    """// ADOPT + LET PLEX SERVE button gets a paired tooltip
    explaining what it does differently from plain ADOPT (the
    v1.15.49 footgun-prevention)."""
    js = APP_JS.read_text()
    anchor = js.index("withCount('// ADOPT + LET PLEX SERVE', adoptLpsCount)")
    block = js[anchor:anchor + 1500]
    assert "adoptLpsBtn.title" in block
    assert "M+P composite" in block, (
        "v1.17.2: tooltip should name the row class this button "
        "targets so the user can correlate with the SRC chip."
    )
    assert "unplace" in block.lower(), (
        "v1.17.2: tooltip should explain the unplace step (the "
        "key difference from plain ADOPT)."
    )
