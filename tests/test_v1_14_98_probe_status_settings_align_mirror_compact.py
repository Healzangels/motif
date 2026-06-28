"""v1.14.98 — three improvements bundled:

## A. Probe button status updates on completion

the user: "when running a probe its hard to tell other than
opening the drawer to see that it ran, could we make the
status on the reprobe button the text to the right update
once its completed"

Pre-fix both probe buttons (REPROBE PLEX THEMES + PROBE TDB
URLS) wrote `✓ started — see // LIVE OPS for progress` then
forgot. The text never updated when the op finished — the
user had to open the drawer to confirm completion.

Fix: shared `_watchOpForCompletion(kind, statusEl)` helper
polls `window.motifOps.state()` every 2s for an op with the
matching kind. Tracks "saw active" so the disappearance after
finishing flips status to "done" (vs the brief click → worker
pickup gap where the op isn't yet visible). On terminal
status, updates text to done/cancelled/failed accordingly.
30-min safety timeout so a hung op doesn't leave the watcher
running indefinitely.

## B. Settings text alignment

the user: "can we also fix the alignment of the settings text"

`.block-body` carries `white-space: pre-wrap` (originally for
log-style content). The settings template's `<p
class="form-hint">` paragraphs span multiple HTML source
lines, each indented by 6 spaces. pre-wrap preserved those
spaces literally, so each soft-wrap landed under a visibly
misaligned indent.

Fix: override on `.form-hint` to `white-space: normal`.
Paragraph hints use normal text flow; logs/code blocks that
need pre-wrap can opt back in.

## C. Git mirror growth bounded

the user: "also it looks like my git mirror keeps growing in
size" (334 MB)

Shallow fetch (depth=1) keeps each fetch's pack file
alongside prior packs. Old objects become unreachable as the
shallow line advances, but dulwich has no reliable gc API.

Fix: `_compact_if_oversized()` checks total mirror bytes after
a successful fetch on the no-changes path (safe — no delta to
process from this fetch). When over 150 MB, drop + re-clone
fresh (~30 MB single pack). Preserves MOTIF_LAST_SYNC across
the re-clone so the next sync's `old_head == new_head`
short-circuit still fires.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
SYNC_PY = REPO / "app" / "core" / "sync.py"


# ── A. Probe button status updates ─────────────────────────────


def test_watch_op_for_completion_helper_exists():
    """A shared helper must be defined so both probe buttons
    can use it. Avoids divergence if a future change updates
    one button's status logic but forgets the other."""
    src = APP_JS.read_text()
    assert "function _watchOpForCompletion(" in src


def test_reprobe_button_calls_completion_watcher():
    """The REPROBE PLEX THEMES button handler must call the
    shared completion watcher with kind='reprobe_plex_themes'
    after the start request returns 200."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindReprobePlexThemes()")
    fn_block = src[fn_start:fn_start + 3000]
    assert "_watchOpForCompletion('reprobe_plex_themes'" in fn_block, (
        "REPROBE handler must call _watchOpForCompletion with the "
        "matching kind"
    )


def test_bulk_probe_button_calls_completion_watcher():
    """The PROBE TDB URLS button handler must call the watcher
    with kind='bulk_probe_tdb'."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindBulkProbeTdb()")
    fn_block = src[fn_start:fn_start + 3000]
    assert "_watchOpForCompletion('bulk_probe_tdb'" in fn_block


def test_watcher_handles_terminal_states():
    """The watcher must distinguish done / cancelled / failed
    so the user gets accurate completion feedback (not just
    'finished').

    v1.17.20 widened the window — the timeout-message block
    pushed the terminal-state branches further down the
    function body."""
    src = APP_JS.read_text()
    fn_start = src.index("function _watchOpForCompletion(")
    fn_block = src[fn_start:fn_start + 5500]
    assert "op.status === 'done'" in fn_block
    assert "op.status === 'cancelled'" in fn_block
    # 'failed' is the implicit else branch — pin via the error
    # message extraction.
    assert "error_message" in fn_block


def test_watcher_distinguishes_pre_pickup_from_post_completion():
    """When the op isn't in the list, it could be either:
       (a) hasn't started yet (worker pickup gap), OR
       (b) already finished + got pruned out of the active set.
    The watcher must track sawActive so case (b) updates the
    status (vs case (a) which keeps waiting)."""
    src = APP_JS.read_text()
    fn_start = src.index("function _watchOpForCompletion(")
    fn_block = src[fn_start:fn_start + 4000]
    assert "sawActive" in fn_block
    # The "saw active then disappeared = done" branch.
    assert "if (sawActive)" in fn_block


def test_watcher_has_safety_timeout():
    """A 30-min timeout bounds the watcher so a hung op doesn't
    leave the polling interval running indefinitely."""
    src = APP_JS.read_text()
    fn_start = src.index("function _watchOpForCompletion(")
    fn_block = src[fn_start:fn_start + 4000]
    assert "TIMEOUT_MS" in fn_block
    assert "30 * 60 * 1000" in fn_block


# ── B. Settings text alignment ─────────────────────────────────


def test_form_hint_overrides_pre_wrap_to_normal():
    """The .form-hint CSS rule must override the parent
    .block-body's white-space:pre-wrap. Pre-fix the HTML source
    indentation rendered literally inside paragraph hints,
    misaligning soft-wrapped text.

    Anchor on `\\n.form-hint {` (top-level rule, not the
    `.form-grid-tight .form-hint` descendant selector that
    happens to share the suffix)."""
    src = APP_CSS.read_text()
    rule_start = src.index("\n.form-hint {")
    rule_end = src.index("}", rule_start)
    rule_block = src[rule_start:rule_end]
    assert "white-space: normal" in rule_block, (
        ".form-hint must explicitly set white-space:normal so it "
        "overrides .block-body's pre-wrap inheritance"
    )


def test_block_body_keeps_pre_wrap_for_log_content():
    """The .block-body pre-wrap setting must stay — it's still
    needed for log-style content that wants source line breaks
    preserved. Only .form-hint paragraphs opt out."""
    src = APP_CSS.read_text()
    rule_start = src.index(".block-body {")
    rule_end = src.index("}", rule_start)
    rule_block = src[rule_start:rule_end]
    assert "white-space: pre-wrap" in rule_block, (
        ".block-body must keep pre-wrap; only .form-hint overrides"
    )


# ── C. Git mirror compaction ───────────────────────────────────


def test_compact_threshold_exists():
    """The _GitMirror class must define a size threshold for
    triggering compaction. Pin it as a constant so future
    tuning is grep-able."""
    src = SYNC_PY.read_text()
    assert "_COMPACT_THRESHOLD_BYTES" in src
    # The threshold should be in the 100-300 MB range —
    # comfortably above a fresh clone, not so high that a
    # runaway mirror sits unbounded.
    anchor = src.index("_COMPACT_THRESHOLD_BYTES = ")
    line = src[anchor:anchor + 200].split("\n")[0]
    assert "150 * 1024 * 1024" in line, (
        f"Threshold should be 150 MB; got line: {line}"
    )


def test_compact_method_exists_on_git_mirror():
    """`_GitMirror` must define `_compact_if_oversized` so the
    fetch path can call it on the no-changes branch."""
    src = SYNC_PY.read_text()
    assert "def _compact_if_oversized(self)" in src


def test_fetch_calls_compact_on_unchanged_branch():
    """The compaction call must happen ONLY on the _unchanged
    branch (no delta to process from this fetch). Calling on
    the delta branch would invalidate self._old_head before
    list_changes / upsert run."""
    src = SYNC_PY.read_text()
    fetch_start = src.index("def _fetch(self)")
    fetch_end = src.index("def _post_acquire_validate", fetch_start)
    fetch_body = src[fetch_start:fetch_end]
    # The compact call must appear inside the unchanged branch.
    unchanged_idx = fetch_body.index("self._unchanged = True")
    branch_block = fetch_body[unchanged_idx:unchanged_idx + 800]
    assert "self._compact_if_oversized()" in branch_block, (
        "Compaction must be called on the no-changes path so it "
        "doesn't invalidate the old_head needed for delta processing"
    )


def test_compact_preserves_motif_last_sync():
    """The compaction must read MOTIF_LAST_SYNC bytes BEFORE
    rmtree and write them back AFTER re-clone. Otherwise the
    next sync would treat as first-run (full tree walk),
    defeating the no-changes fast-path."""
    src = SYNC_PY.read_text()
    fn_start = src.index("def _compact_if_oversized(self)")
    fn_block = src[fn_start:fn_start + 4000]
    assert "self.last_sync_path.read_bytes()" in fn_block
    assert "self.last_sync_path.write_bytes(" in fn_block
    # The read must happen BEFORE rmtree (preserve), and the
    # write AFTER clone (restore). Pin the order.
    read_idx = fn_block.index("self.last_sync_path.read_bytes()")
    rmtree_idx = fn_block.index("shutil.rmtree(self.repo_path")
    clone_idx = fn_block.index("clone(")
    write_idx = fn_block.index("self.last_sync_path.write_bytes(")
    assert read_idx < rmtree_idx < clone_idx < write_idx, (
        "Order must be: read MOTIF_LAST_SYNC → rmtree → clone "
        "→ write MOTIF_LAST_SYNC"
    )


def test_compact_uses_same_clone_shape_as_first_run():
    """The re-clone must use the same dulwich.porcelain.clone
    args as the first-run _clone() (bare=True, branch=..,
    depth=1) so the new mirror has the same shape — single
    pack, current HEAD only."""
    src = SYNC_PY.read_text()
    fn_start = src.index("def _compact_if_oversized(self)")
    fn_block = src[fn_start:fn_start + 4000]
    clone_call_idx = fn_block.index("clone(")
    clone_call = fn_block[clone_call_idx:clone_call_idx + 500]
    assert "bare=True" in clone_call
    assert "depth=1" in clone_call
    assert "branch=self.branch.encode()" in clone_call
