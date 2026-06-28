"""v1.23.80 — git mirror compacts after a changed sync + apply-loop timing.

the user: "ThemerrDB sync — Applying 9943 changed paths extremely slowly." Root
cause: the mirror had grown to 168 MB across 6 packs (a fresh clone is ~30 MB /
1 pack), and reading 9943 changed objects out of fragmented packs (delta-chain
resolution across them) crawled. The v1.14.98 opportunistic compaction
(_compact_if_oversized, 150 MB threshold) was the fix for exactly this — but it
only fired on the NO-CHANGES path, so a run of big-diff days never gave it a
window. v1.23.80 also fires it after a CHANGED sync's commit_sync_ok() (where
the old HEAD is already retired → re-clone is safe), and instruments the apply
loop so a future slow sync names its own bottleneck (read_json vs DB flush).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()


def test_changed_sync_compacts_after_commit():
    # the differential (git-diff) path must compact right after commit_sync_ok
    # advances the baseline — that's when the old HEAD is safely retired.
    # v1.24.0: commit + compact moved into the post-detection
    # `if ran_git_diff and detection_ok:` gate (per-phase error labels), and
    # compaction is now the else-branch of the commit try (skipped if the
    # advance itself failed). Anchor on that gate, not the detect call.
    # v1.24.14: the gate gained `and stats.errors == 0` (read-failure hold).
    i = SYNC_PY.index("if ran_git_diff and detection_ok and stats.errors == 0:")
    block = SYNC_PY[i:i + 1400]
    ci = block.index("git_mirror.commit_sync_ok()")
    assert "git_mirror._compact_if_oversized()" in block[ci:ci + 900], (
        "compaction must fire after the changed-sync commit_sync_ok (was "
        "no-change-path only → the mirror ballooned to 168 MB / 6 packs)"
    )


def test_compact_docstring_allows_post_commit():
    # the safety docstring must reflect that post-commit (changed path) is now
    # an allowed trigger, not just the unchanged path.
    i = SYNC_PY.index("def _compact_if_oversized(")
    doc = SYNC_PY[i:i + 700]
    assert "commit_sync_ok()" in doc and "retired the old HEAD" in doc


def test_threshold_unchanged_at_150mb():
    # the user chose size-only (keep 150 MB), just fire on more paths.
    assert "_COMPACT_THRESHOLD_BYTES = 150 * 1024 * 1024" in SYNC_PY


def test_apply_loop_has_phase_timing():
    # the apply loop times read_json vs DB flush so a slow sync is diagnosable.
    i = SYNC_PY.index("_apply_t0 = time.monotonic()")
    block = SYNC_PY[i:i + 7000]
    assert "_t_read += time.monotonic() - _r0" in block, "read_json timed"
    assert "_t_flush += time.monotonic() - _f0" in block, "DB flush timed"
    assert "sync apply timing:" in block, "the breakdown is logged"
