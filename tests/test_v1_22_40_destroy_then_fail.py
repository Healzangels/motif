"""v1.22.40 (holistic audit) — destroy-then-fail FS ordering made atomic.

Two sites destroyed the old theme file BEFORE the replacement was secured, so a
failed write left the row themeless on disk:

- worker._do_download: should_unlink fires on a source_video_id change (the TDB
  URL now points at a DIFFERENT, often dead/private video → the re-download
  frequently fails). Pre-fix it unlinked theme.mp3 first → canonical gone on a
  failed download. Now rename-aside + restore-in-except + drop-on-success.
- placement.place_theme (force_overwrite): unlinked the existing theme before
  the place → folder empty on a place I/O error. Now place atomically first
  (_safe_link_or_copy = temp + os.replace), then remove a differently-named
  leftover.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
PLACEMENT_PY = (REPO / "app" / "core" / "placement.py").read_text()


# ── worker _do_download rename-aside ──────────────────────────


def test_worker_stashes_stale_instead_of_unlinking():
    # Rename-aside stash replaces the destructive pre-download unlink.
    assert "target_mp3.replace(_stale_candidate)" in WORKER_PY, (
        "v1.22.40: stale theme.mp3 must be stashed (rename-aside), not unlinked")
    # Scope to the should_unlink region (the destroy-then-fail site). A bare
    # `target_mp3.unlink()` still legitimately appears in the sibling-reuse
    # re-link path lower down, where the replacement (sibling_abs) is already
    # confirmed on disk — so a whole-file check would over-match.
    region_start = WORKER_PY.index("should_unlink = target_mp3.exists()")
    region_end = WORKER_PY.index("result = download_theme(", region_start)
    region = WORKER_PY[region_start:region_end]
    assert "target_mp3.unlink()" not in region, (
        "v1.22.40: the pre-download unlink (destroy-then-fail) must be gone")


def test_worker_restores_stash_on_failure_and_drops_on_success():
    assert "_stale_backup.replace(target_mp3)" in WORKER_PY, (
        "v1.22.40: a failed re-download must restore the stashed canonical")
    assert "_stale_backup.unlink()" in WORKER_PY, (
        "v1.22.40: a successful download must drop the stash")


# ── placement.place_theme atomic-first ────────────────────────


def test_placement_places_before_removing_existing():
    place_idx = PLACEMENT_PY.index("_safe_link_or_copy(source_file, dst)")
    rm_idx = PLACEMENT_PY.index("existing.name != dst.name")
    assert rm_idx > place_idx, (
        "v1.22.40: the existing-theme removal must run AFTER the atomic place")
    # The pre-fix unconditional pre-place unlink (with its error string) is gone.
    assert "could not remove existing" not in PLACEMENT_PY, (
        "v1.22.40: the destroy-then-fail pre-place unlink must be gone")


def test_placement_leftover_removal_guards_case_insensitive_fs():
    # On a case-insensitive FS "Theme.mp3" and "theme.mp3" are one inode; the
    # leftover-removal must samefile-guard so it can't delete the placed file.
    rm_idx = PLACEMENT_PY.index("existing.name != dst.name")
    region = PLACEMENT_PY[rm_idx:rm_idx + 800]
    assert "samefile(dst)" in region, (
        "v1.22.40: leftover removal must samefile-guard against the "
        "case-insensitive-FS same-inode trap")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
