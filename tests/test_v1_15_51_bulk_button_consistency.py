"""v1.15.51 — standardize bulk button labels across PUSH/REVERT/
RESTORE/ACK/DOWNLOAD per the user's consistency ask.

the user: "let's standardizing // RUNNING X/N + count badges across
the OTHER bulk buttons (PUSH TO PLEX, RESTORE FROM PLEX, REVERT
MISMATCH, ACK FAILURES, DOWNLOAD FROM TDB). As a rule let's try
to standardize across sets that are similar to keep style
consistent across the project."

Pre-v1.15.51 each handler had its own conventions:
* PUSH: idle bare → "// PUSHING…" (no progress) → "// N QUEUED, F failed"
* REVERT: idle bare → "// REVERTING…" (no progress) → "// N REVERTED, F failed"
* RESTORE: idle bare → "// RESTORING…" (no progress) → "// N RESTORED, F failed"
* ACK: idle bare → "// ACKING X/N" → "// N ACKED · F FAILED"
* DOWNLOAD: idle bare → "// QUEUING" → "// N QUEUED"

ADOPT SELECTED + ADOPT + LET PLEX SERVE already followed the
v1.15.46/49 pattern: "// LABEL (N)" idle + "// VERBING X/N"
running + "// N VERB · F FAILED" done.

This tag aligns the per-row iterator handlers (PUSH/REVERT/
RESTORE/ACK) to that pattern. DOWNLOAD stays on its server-side
batch pattern (single POST → "// N QUEUED") but gains the
count badge in the idle label.

## Standardized pattern

Per-row iterator (PUSH/REVERT/RESTORE/ACK/ADOPT/ADOPT+LPS):
  idle:    "// LABEL" (N=1) or "// LABEL (N)" (N>1)
  during:  "// VERBING X/N"
  done:    "// N VERB" or "// N VERB · F FAILED"

Server-side batch (DOWNLOAD):
  idle:    "// LABEL" or "// LABEL (N)"
  during:  "// QUEUING"
  done:    "// N QUEUED"

Static-text guards consistent with v1.15.46/49 patterns.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. effectiveCount + withCount helpers exist ─────────────


def test_helpers_defined_in_update_selection_ui():
    """`effectiveCount` + `withCount` helpers must be defined
    inside updateLibrarySelectionUi so all per-button blocks
    share the same counting + labeling logic."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    fn_body = js[fn_anchor:fn_anchor + 30000]
    assert "const effectiveCount = (predicate) =>" in fn_body, (
        "v1.15.51: effectiveCount helper required — counts rows "
        "the click handler will operate on (selection mode vs "
        "no-selection visible-page fallback)"
    )
    assert "const withCount = (label, count) =>" in fn_body, (
        "v1.15.51: withCount formatter required — '// LABEL (N)' "
        "when count > 1, bare label when count === 1"
    )


def test_helpers_hoisted_above_button_blocks():
    """`effectiveCount` + `withCount` declarations must come
    BEFORE every consumer block (ACK / PUSH / REVERT / RESTORE /
    DOWNLOAD). const doesn't hoist; consumer-before-declaration =
    ReferenceError at runtime."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    # v1.19.43: window widened from 30000 → 40000 chars. The
    # v1.19.43 cloud-backup additions (cloudBackupCount +
    # cloudBackupBtn block) pushed RESTORE FROM PLEX past the
    # original 30K window. Function is still self-contained;
    # the test's intent (helpers declared before consumers)
    # is preserved.
    fn_body = js[fn_anchor:fn_anchor + 40000]
    helper_idx = fn_body.index("const withCount = (label, count) =>")
    # First consumer is the ACK block.
    ack_idx = fn_body.index("withCount('// ACK FAILURES'")
    push_idx = fn_body.index("withCount('// PUSH TO PLEX'")
    dl_idx = fn_body.index("withCount('// DOWNLOAD FROM TDB'")
    revert_idx = fn_body.index("withCount('// REVERT MISMATCH'")
    restore_idx = fn_body.index("withCount('// RESTORE FROM PLEX'")
    for name, idx in [("ACK", ack_idx), ("PUSH", push_idx),
                      ("DOWNLOAD", dl_idx), ("REVERT", revert_idx),
                      ("RESTORE", restore_idx)]:
        assert helper_idx < idx, (
            f"v1.15.51: {name} consumer at offset {idx} comes before "
            f"withCount helper declaration at {helper_idx} — "
            "ReferenceError at runtime"
        )


# ── 2. All 5 buttons use withCount() ─────────────────────────


BUTTONS = [
    ("// PUSH TO PLEX", "pushCount"),
    ("// REVERT MISMATCH", "revertCount"),
    ("// RESTORE FROM PLEX", "restoreCount"),
    ("// ACK FAILURES", "ackCount"),
    ("// DOWNLOAD FROM TDB", "downloadCount"),
    ("// DOWNLOAD & REPLACE FROM TDB", "downloadCount"),
]


def test_all_five_buttons_use_with_count_for_idle_label():
    """Each per-row iterator button + the DOWNLOAD variants must
    set its idle textContent via withCount(LABEL, COUNT). Drift
    here = some buttons render (N), others render bare = the user's
    'inconsistent across the board' complaint returns."""
    js = APP_JS.read_text()
    failures = []
    for label, count_var in BUTTONS:
        needle = f"withCount('{label}', {count_var})"
        if needle not in js:
            failures.append(f"missing call: {needle}")
    assert not failures, "v1.15.51: " + "; ".join(failures)


# ── 3. Per-row iteration handlers show X/N progress ─────────


PER_ROW_HANDLERS = [
    ("library-push-selected-btn", "PUSHING"),
    ("library-revert-mismatch-btn", "REVERTING"),
    ("library-restore-from-plex-btn", "RESTORING"),
    ("library-ack-selected-btn", "ACKING"),
]


def test_per_row_handlers_show_x_over_n_progress():
    """Each per-row iterator handler must update its label as it
    iterates: `// VERBING X/N`. Pre-v1.15.51 PUSH/REVERT/RESTORE
    just showed `// VERBING…` for the full duration; on a 50-row
    op the operator saw no progress for ~10s. Mirrors v1.15.46's
    // RUNNING X/N pattern."""
    js = APP_JS.read_text()
    failures = []
    for btn_id, verb in PER_ROW_HANDLERS:
        anchor = js.index(f"{btn_id}')?.addEventListener")
        # v0.51.254: was a fixed byte window (5000 → widened to 5400 in
        # v1.21.85, then red AGAIN here when bulk PUSH gained its bulk=1
        # comment). That is the treadmill: every growth in the handler needs
        # another bump. Bound by the handler's own end — the next
        # getElementById(...) addEventListener registration — so it can never
        # go stale on size alone.
        _next = js.find("')?.addEventListener", anchor + 40)
        body = js[anchor:_next if _next > 0 else len(js)]
        # Initial label sets count: "// VERBING 0/N"
        if f"// {verb} 0/" not in body and f"// {verb} 0/${{" not in body:
            # Some handlers use backtick templates: `// VERBING 0/${count.length}`
            if not re.search(rf"// {verb} 0/\$\{{", body):
                failures.append(
                    f"{btn_id}: missing initial '// {verb} 0/N' label"
                )
                continue
        # Per-iter update: "// VERBING {i+1}/N"
        if not re.search(rf"// {verb} \$\{{i \+ 1\}}/", body):
            failures.append(
                f"{btn_id}: missing per-iter '// {verb} ${{i+1}}/N' "
                "label update"
            )
    assert not failures, "v1.15.51: " + "; ".join(failures)


# ── 4. Done labels use ` · ` separator ──────────────────────


def test_per_row_handler_done_labels_use_bullet_separator():
    """When the action completes with failures, the result label
    must use ` · ` (middle-dot) as the separator between segments,
    not `, ` or other punctuation. the user's screenshot complaint
    was about inconsistent format; this pins the bullet separator
    everywhere."""
    js = APP_JS.read_text()
    # Each handler's done label must NOT contain the old comma form.
    legacy_patterns = [
        "` REVERTED${failed ? `, ${failed} failed",
        "` RESTORED${failed ? `, ${failed} failed",
    ]
    for legacy in legacy_patterns:
        assert legacy not in js, (
            f"v1.15.51: legacy comma separator still present: {legacy!r} "
            "— must use ' · ' (middle dot)"
        )
    # Each handler must include the bullet form somewhere.
    bullet_done_labels = [
        "${ok} REVERTED · ${failed} FAILED",
        "${ok} RESTORED · ${failed} FAILED",
    ]
    for needle in bullet_done_labels:
        assert needle in js, (
            f"v1.15.51: expected bullet-separator done label "
            f"'{needle!r}' not found"
        )


# ── 5. effectiveCount handles both selection + no-selection modes ──


def test_effective_count_falls_through_to_visible_when_no_selection():
    """effectiveCount must count visible rows when no selection
    (n === 0) so the count badge is honest in attn-pill
    no-selection-fallback mode (// PUSH ALL TO PLEX,
    // ACK FAILURES on chip-driven view, etc)."""
    js = APP_JS.read_text()
    fn_anchor = js.index("const effectiveCount = (predicate) =>")
    body = js[fn_anchor:fn_anchor + 600]
    # The helper checks `if (n > 0)` then falls through to
    # `return items.filter(predicate).length`.
    assert "if (n > 0)" in body, (
        "v1.15.51: effectiveCount must check selection size — "
        "selection mode counts selected matches; no-selection "
        "counts visible page matches"
    )
    # v1.16.10: the visible-page filter is now inlined since the
    # local `items` binding moved to the n>0 branch only. Anchor
    # on the canonical libraryState.items source instead.
    assert "(libraryState.items || []).filter(predicate).length" in body, (
        "v1.15.51 / v1.16.10: no-selection mode must return "
        "visible-page matching count (the attn-pill bulk "
        "fallback's scope) — anchored on the canonical "
        "libraryState.items source"
    )
