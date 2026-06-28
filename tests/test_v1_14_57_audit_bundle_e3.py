"""v1.14.57 — audit Bundle E3: log-hygiene + safety nets.

Final audit-driven sweep. Five LOW findings + one drop.

  • L7: errno-aware OSError handling on parent-dir cleanup in
    api_unmanage_item + api_forget_item. Pre-fix `except OSError:
    pass` masked permission errors / unexpected FS state alongside
    the expected ENOTEMPTY no-op.
  • L9 NOT shipped — investigated. The cancel branch DOES call
    `_flush_batch()` before exiting (api.py:2419), so the verdict
    batch IS persisted. Only in-flight futures get cancelled
    (intentional). Audit was wrong about the silent-drop.
  • L10: `_stats_cache` 3-key dict mutation race → atomic tuple
    swap via single-element list box. Sequence of 3 dict-item
    writes was not atomic across the read; a reader could observe
    half-state with mismatched (key, ts, value).
  • L11: `_safe_link` cross-FS fallback bumped log.info → log.warning
    so a misconfigured Unraid mount that doubles motif's disk use
    surfaces in /queue's logs view instead of getting buried.
  • L12: `_do_relink` bulk SELECT capped at LIMIT 5000. Pre-fix
    unbounded — fine for typical libraries but a freshly-imported
    large library where every placement landed cross-FS could do
    thousands of os.link attempts in one writer-locked sweep.
  • L16: `/mnt/disks/` → `/` translation rule documented inline
    so a future contributor sees the broad-clobber rationale +
    the safer per-disk shape recommendation.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── L7: errno-aware OSError handling on parent-dir cleanup ───


def test_unmanage_item_parent_dir_cleanup_logs_unexpected_errors():
    """The api_unmanage_item parent-rmdir block must filter on
    errno: ENOTEMPTY/ENOENT are no-op silent; everything else
    log.warning. Pre-fix the bare `except OSError: pass` masked
    permission / unexpected FS errors alongside the benign cases."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_unmanage_item(")
    body = src[fn_anchor:fn_anchor + 12000]
    # The errno-aware shape.
    assert "import errno as _errno" in body
    assert "rmd_err.errno not in (_errno.ENOTEMPTY," in body
    assert "_errno.ENOENT)" in body
    assert 'log.warning(\n                                "unmanage: parent rmdir' in body
    # v1.14.57 marker.
    assert "v1.14.57:" in body


def test_forget_item_parent_dir_cleanup_logs_unexpected_errors():
    """Same fix on api_forget_item (mirror of api_unmanage_item)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_forget_item(")
    # v1.22.75: slice to the function's actual end — the fixed window
    # (widened once already in v1.20.67) went stale again when the
    # v1.22.75 teardown-count + PU-filter additions grew the body.
    body = src[fn_anchor:src.index("\n    @app.", fn_anchor)]
    assert "import errno as _errno" in body
    assert "rmd_err.errno not in (_errno.ENOTEMPTY," in body
    assert 'log.warning(\n                                "forget: parent rmdir' in body


# ── L10: atomic _stats_cache tuple-swap ──────────────────────


def test_stats_cache_uses_atomic_tuple_box():
    """The _stats_cache must be a single-element list holding a
    (key, ts, value) tuple, replaced via single-statement
    assignment. Pre-fix the 3 dict-item writes weren't atomic
    as a sequence; a reader interleaved between any two writes
    could see a half-state."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # The new declaration shape.
    assert "_stats_cache_box: list = [(None, 0.0, None)]" in src
    # Atomic write at the end of api_stats.
    assert "_stats_cache_box[0] = (str(db), now, response)" in src
    # Atomic read at the start of api_stats.
    assert "cached_key, cached_ts, cached_value = _stats_cache_box[0]" in src
    # Pre-fix mutating-dict shape is gone.
    assert '_stats_cache["key"] = str(db)' not in src
    assert '_stats_cache["value"] = response' not in src


# ── L11: _safe_link fallback log level ───────────────────────


def test_safe_link_cross_fs_fallback_logs_warning():
    """The _safe_link helper must log.warning (was log.info) when
    falling back from os.link to shutil.copy2. The fallback
    doubles motif's disk footprint per affected row — a
    misconfigured Unraid mount that triggers cross-FS for every
    placement silently doubles disk use across the library."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _safe_link(")
    body = src[fn_anchor:fn_anchor + 1500]
    assert 'log.warning("Hardlink failed (%s); copying %s' in body
    # Pre-fix log.info is gone.
    assert 'log.info("Hardlink failed' not in body


# ── L12: _do_relink bulk SELECT bounded ──────────────────────


def test_do_relink_bulk_select_has_limit():
    """The bulk-relink SELECT (no media_type/tmdb_id on the job)
    must have a LIMIT clause so a single sweep can't hold the
    writer lock for thousands of os.link attempts in one shot.

    v1.14.60 added an ORDER BY clause between the WHERE and the
    LIMIT (forward-progress under cancel-retry). The contract
    this test pins (LIMIT exists) survives — softened literal
    match to avoid coupling to layout."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _do_relink(self, job: sqlite3.Row) -> None:")
    body = src[fn_anchor:fn_anchor + 5000]
    # The LIMIT clause is in the bulk branch.
    assert "LIMIT 5000" in body
    assert "WHERE p.placement_kind = 'copy'" in body
    # v1.14.57 marker.
    assert "v1.14.57:" in body


# ── L16: /mnt/disks/ namespace clobber documented ────────────


def test_mnt_disks_translation_documented():
    """The `_PATH_PREFIX_TRANSLATIONS` table must carry an inline
    marker explaining why the broad `/mnt/disks/` → `/` rule
    exists + recommending the safer per-disk shape if it ever
    becomes a real problem.

    v1.15.100: the tuple was renamed
    `_PATH_PREFIX_TRANSLATIONS` → `_HARDCODED_PATH_PREFIX_TRANSLATIONS`
    when env-var-driven user pairs were added. The v1.14.57
    documentation sits above the hardcoded definition; anchor
    there."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    anchor = src.index("_HARDCODED_PATH_PREFIX_TRANSLATIONS:")
    # v1.15.100: widened from 1500 → 3000 chars. The v1.15.100
    # env-var documentation block sits between the v1.14.57
    # documentation and the symbol anchor, pushing the v1.14.57
    # text further upstream.
    block = src[max(0, anchor - 3000):anchor]
    assert "v1.14.57 audit clarification:" in block
    assert "individual-disk mount" in block
    assert "/mnt/disks/" in block
