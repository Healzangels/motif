"""v1.14.69 — SYNC THEMERRDB busy label matches REFRESHING PLEX… form.

the user v1.14.67 follow-up:

> "on the dashboard when we click sync themerrdb can we make
>  syncing themerrdb to match the way it's phrased when we're
>  doing a refresh for plex."

REFRESH PLEX uses an explicit verb+scope+ellipsis form for its
busy label: `// REFRESHING PLEX…`. SYNC THEMERRDB used a
generic `// SYNCING…` (introduced v1.13.19 to give immediate
post-click confirmation). v1.14.69 spells it out as
`// SYNCING THEMERRDB…` so the two dash buttons read
symmetrically.

The change is purely a label refresh — busy-state ownership,
unlock paths, and tooltip behavior are unchanged. The poll-
driven cross-tab/cron unlock branch's === comparison is
updated to the new string so the cross-tab unlock still
matches.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"


# ── setSyncButtonState owns the running-state label ────────────


def test_set_sync_button_state_running_uses_explicit_label():
    """The setSyncButtonState('running') branch sets the busy
    label. v1.14.69 spelled it out from `// SYNCING…` to
    `// SYNCING THEMERRDB…`. v1.18.49 widened the setter to a
    ternary picking between the THEMERRDB-only form and the
    THEMERRDB+REFRESH-PLEX form when auto_enum_after_sync is on,
    but both branches still spell THEMERRDB explicitly — the
    v1.14.69 symmetry guarantee is preserved."""
    js = JS.read_text()
    fn_start = js.index("function setSyncButtonState(state) {")
    fn_body = js[fn_start:fn_start + 2500]
    # Both explicit forms must be reachable from the ternary —
    # one for autoEnum=true, one for autoEnum=false. v1.14.69's
    # intent (no bare // SYNCING…) holds because BOTH spell
    # THEMERRDB.
    assert "'// SYNCING THEMERRDB…'" in fn_body
    assert "'// SYNCING THEMERRDB + REFRESH PLEX…'" in fn_body
    # The generic form must NOT survive in this function.
    assert "btn.textContent = '// SYNCING…';" not in fn_body


# ── refreshTopbarStatus poll path matches ──────────────────────


def _sync_btn_block() -> str:
    """Anchor on the syncBtn block inside refreshTopbarStatus —
    the function is large; a fixed slice from the function start
    misses this block. Slice from the canonical
    `const syncBtn = document.getElementById('sync-now-btn');`
    line through the next ~5000 chars (covers set + unlock + the
    v1.14.65 tooltip block + the v1.18.61 live-sync addition).

    v1.18.61: widened 3000 → 5000. v1.18.61 added ~1000 chars
    for the live-sync-on-toggle path (knownBusyLabels Set +
    textContent override + guard comments)."""
    js = JS.read_text()
    anchor = js.index(
        "const syncBtn = document.getElementById('sync-now-btn');"
    )
    return js[anchor:anchor + 5000]


def test_poll_driven_busy_label_set_uses_explicit_form():
    """The cross-tab/cron poll path in refreshTopbarStatus sets
    the busy label when /api/stats sees TDB activity but the
    button still shows the idle label. Must use the same
    explicit form so the unlock comparison matches later.

    v1.18.49 widened the assignment to read from a `busyLabel`
    local that picks between the THEMERRDB-only and
    THEMERRDB+REFRESH-PLEX forms based on autoEnum.

    v1.18.61 added a live-sync-on-toggle path that uses a
    `knownBusyLabels` Set instead of a single literal — the
    busy-label assignment + the unlock branch both consult it.
    Both candidate strings still spell THEMERRDB so v1.14.69's
    symmetry guarantee is preserved on both branches."""
    body = _sync_btn_block()
    # The new pattern: a knownBusyLabels Set lists both forms.
    assert "'// SYNCING THEMERRDB…'" in body
    assert "'// SYNCING THEMERRDB + REFRESH PLEX…'" in body
    # The setter must actually assign textContent from busyLabel.
    assert "syncBtn.textContent = busyLabel;" in body
    # The Set should exist to back the unlock-branch + live-sync
    # path (v1.18.61).
    assert "knownBusyLabels" in body


def test_poll_driven_unlock_branch_compares_explicit_form():
    """The unlock branch compares textContent against the busy
    label. If the comparison string drifts from the set string,
    the cross-tab/cron unlock breaks (button stays locked
    forever after the sync finishes from another tab/cron).

    v1.18.61 collapsed the OR-chain into knownBusyLabels.has(...)
    — semantically identical, but the literal `=== '// SYNCING…'`
    no longer appears. The Set itself contains both candidates."""
    body = _sync_btn_block()
    # v1.18.61: the unlock check is `knownBusyLabels.has(textContent)`.
    assert "knownBusyLabels.has(syncBtn.textContent)" in body, (
        "v1.14.69 unlock branch must check textContent against "
        "the knownBusyLabels Set"
    )
    # The pre-v1.14.69 generic form must NOT survive as a
    # comparison or as a Set entry.
    assert "syncBtn.textContent === '// SYNCING…'" not in body
    assert "'// SYNCING…'" not in body.split("//")[0]  # not in code (allow in comments)


# ── No bare `// SYNCING…` literal remains as a setter ──────────


def test_no_bare_sync_ellipsis_literal_in_setter_or_compare():
    """Belt-and-suspenders: no `'// SYNCING…'` literal survives
    as a setter (`= '...';`) or comparison (`=== '...'`)
    anywhere in app.js. Historical comment mentions
    (`// // SYNCING…`) are fine — they document prior shape.
    Restricting the regex to JS-string-literal contexts keeps
    the comment guard from tripping."""
    js = JS.read_text()
    # Strip JS line comments so the historical mentions don't
    # trip the assertion.
    stripped = "\n".join(
        line for line in js.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "'// SYNCING…'" not in stripped, (
        "A bare '// SYNCING…' literal survives in code (not a "
        "comment). v1.14.69 spelled the busy label out — this "
        "form would re-introduce the symmetry-break with "
        "REFRESHING PLEX…."
    )


# ── Idle label is NOT touched (still // SYNC THEMERRDB[ + …]) ──


def test_idle_label_preserved():
    """v1.14.69 only changes the BUSY label. The idle/origLabel
    forms (`// SYNC THEMERRDB` and the auto_enum-on combined
    `// SYNC THEMERRDB + REFRESH PLEX`) must be intact — those
    are stable identities the user knows."""
    js = JS.read_text()
    # The combined idle label survives.
    assert "'// SYNC THEMERRDB + REFRESH PLEX'" in js
    # The sync-only idle label survives (used as the dataset
    # default + the refresh-poll unlock target).
    assert "'// SYNC THEMERRDB'" in js


# ── v1.14.69 marker pinned at the canonical change site ────────


def test_v1_14_69_marker_present_at_set_sync_button_state():
    """The v1.14.69 marker should sit on setSyncButtonState
    (the canonical owner of the busy label) so a future grep
    for '// SYNCING…' lands on the rationale instead of
    re-introducing the generic form."""
    js = JS.read_text()
    fn_start = js.index("function setSyncButtonState(state) {")
    fn_body = js[fn_start:fn_start + 2500]
    assert "v1.14.69" in fn_body, (
        "setSyncButtonState lacks a v1.14.69 marker — future "
        "readers won't see why the busy label is spelled out."
    )
