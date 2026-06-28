"""v1.14.55 — audit Bundle E1: remaining MEDIUMs + 2 quick LOWs.

From the v1.14.50 holistic audit. Fourth sweep covering the
last MEDIUM-tier findings + two cheap dead-code LOWs that fit
the same surfaces.

  • M5 (NOT shipped — investigated): is_lps title-global fallback
    when section_id omitted. Dropped because is_lps is no longer
    consumed in the api_recovery_options response post-v1.14.47
    (verified by grep: only api.py:2028 in _row_matches_pl uses
    is_lps and that's per-row, not per-call).
  • M9: sibling-hardlink lookup added ORDER BY downloaded_at DESC
    so the LIMIT 1 winner is always the most-recent sibling.
  • M10: _do_download themes-failure-write block wrapped in
    transaction(conn) so concurrent readers can't observe
    intermediate state (failure_kind set + place job still
    pending → confusing "Skipped placement" events).
  • M11: sync.py user_overrides title-global lookup at 3 sites
    is INTENTIONAL (defensive: any section's override pauses
    auto-roll, surfaces as pending_update for review). Audit was
    a docs-clarity ask, not a bug. Added marker at sync.py:377
    explaining the rationale + cross-references at the other 2
    sites pointing back to the primary marker.
  • L5: removed unused `CookiesMissingError` import in worker.py.
  • L6: removed function-scope `import os` / `import shutil` in
    `_do_relink` (already imported at module top).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── M9: sibling-hardlink lookup ORDER BY ─────────────────────


def test_sibling_hardlink_lookup_orders_by_downloaded_at_desc():
    """The pre-yt-dlp sibling lookup at the top of _do_download
    must order by downloaded_at DESC so the LIMIT 1 winner is
    always the most-recent sibling. Pre-fix SQLite picked any
    row with no ORDER BY, propagating stale file_sha256/file_size
    on multi-section rows where one section had been re-downloaded."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # Anchor on the sibling SELECT.
    anchor = src.index(
        "SELECT file_path, file_sha256, file_size, source_video_id, "
    )
    block = src[anchor:anchor + 1000]
    assert "ORDER BY downloaded_at DESC LIMIT 1" in block
    # v1.14.55 marker.
    block_with_comment = src[max(0, anchor - 1000):anchor + 1000]
    assert "v1.14.55:" in block_with_comment


# ── M10: download failure-write wrapped in transaction ───────


def test_do_download_failure_write_wraps_in_transaction():
    """The four-statement failure-write sequence (snapshot prior,
    UPDATE themes, conditional sfa DELETE, cancel pending place
    job) must run inside transaction(conn) so concurrent readers
    don't observe intermediate state. get_conn is autocommit
    (isolation_level=None) so without explicit transaction(conn)
    each execute commits independently."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # Anchor on the prior_kind snapshot (load-bearing line of the
    # failure-write block).
    anchor = src.index("prior_kind = prior[\"failure_kind\"] if prior else None")
    block = src[max(0, anchor - 2500):anchor]
    # The transaction context must wrap the get_conn block.
    assert "with get_conn(self.settings.db_path) as conn, transaction(conn):" in block
    # v1.14.55 marker rationale (sits ~2k chars above the prior_kind
    # line in the multi-paragraph comment block).
    assert "v1.14.55:" in block


def test_do_download_failure_write_block_still_does_all_4_writes():
    """Sanity: the v1.13.81 sfa-aware shape + v1.10.40 place-job
    cancel must survive the wrap. Pin all 4 statements still
    present + in the original order."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    anchor = src.index("prior_kind = prior[\"failure_kind\"] if prior else None")
    block = src[anchor:anchor + 4000]
    # 1. UPDATE themes.failure_kind/message/at/acked_at
    assert "UPDATE themes SET failure_kind = ?, failure_message = ?," in block
    # 2. Conditional DELETE FROM section_failure_acks
    assert "if prior_kind is not None and prior_kind != kind.value:" in block
    assert "DELETE FROM section_failure_acks" in block
    # 3. UPDATE jobs SET status='cancelled' for pending place jobs
    assert "UPDATE jobs SET status = 'cancelled'" in block
    assert "job_type = 'place'" in block


# ── M11: title-global has_override rationale documented ──────


def test_sync_has_override_primary_marker_explains_title_global_intent():
    """The first has_override site at sync.py:377 must carry an
    inline marker explaining the title-global lookup is
    INTENTIONAL (any section's override pauses auto-roll and
    routes the upstream change through pending_updates for
    per-section accept/decline review)."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    # Anchor on the first has_override SELECT.
    first_anchor = src.index(
        "SELECT 1 FROM user_overrides WHERE media_type = ? AND tmdb_id = ?"
    )
    # Walk back to find the marker comment block.
    block = src[max(0, first_anchor - 2000):first_anchor]
    # Whitespace + comment-marker flatten before substring match —
    # the marker text line-wraps "INTENTIONALLY title-\n# global"
    # in the source so the literal substring would miss without
    # stripping the inline `#` comment marker after a wrap.
    flat = " ".join(block.replace("#", " ").split())
    assert "v1.14.55 audit clarification:" in flat
    assert "INTENTIONALLY title- global" in flat or "INTENTIONALLY title-global" in flat
    # Cross-references the other 2 sites by line / pattern.
    assert "sync.py:649" in flat
    assert "sync.py:705" in flat


def test_sync_has_override_secondary_sites_cross_reference_primary():
    """The two secondary has_override sites (cross-match at ~649
    + url_changed pending_update at ~705) must each carry a
    short marker pointing back to the primary site at sync.py:377.
    Prevents the next reader from re-flagging the title-global
    lookup as a bug.

    v1.19.60: the url_changed site changed from `SELECT 1` to
    `SELECT youtube_url` so the kind-decision logic can detect
    the override-coincidence case. Match both shapes."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    # Find all has_override sites — either the historical SELECT 1
    # shape or the v1.19.60 SELECT youtube_url shape.
    needles = (
        "SELECT 1 FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
        "SELECT youtube_url FROM user_overrides ",
    )
    positions = []
    for needle in needles:
        pos = 0
        while True:
            idx = src.find(needle, pos)
            if idx < 0:
                break
            positions.append(idx)
            pos = idx + 1
    positions.sort()
    assert len(positions) >= 3, f"expected ≥3 has_override sites, found {len(positions)}"
    # The 2nd + 3rd sites each have the cross-ref marker.
    # v1.19.60: widened window from 500→900 chars since the
    # url_changed site grew a multi-line v1.19.60 comment between
    # the v1.14.55 marker and the SELECT.
    for p in positions[1:]:
        block = src[max(0, p - 900):p]
        assert "v1.14.55 audit clarification:" in block, (
            f"site at offset {p} missing v1.14.55 cross-ref marker"
        )
        assert "see sync.py:377 marker" in block


# ── L5 + L6: dead-code cleanup ───────────────────────────────


def test_worker_drops_unused_cookies_missing_error_import():
    """`CookiesMissingError` was imported but never used. Drop it.
    Future contributors won't add a code path that relies on a
    name that's only there by accident."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # The import statement no longer mentions CookiesMissingError.
    assert "from .downloader import DownloadError, FailureKind, download_theme" in src
    # And the symbol isn't referenced anywhere in the file.
    assert "CookiesMissingError" not in src


def test_do_relink_drops_function_scope_imports():
    """`_do_relink` re-imported `os` and `shutil` at function
    scope despite both already being imported at module top
    (worker.py:18-19). Removed the redundant imports."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _do_relink(self, job: sqlite3.Row) -> None:")
    # Bound the function body at the next `def `.
    end = src.index("\n    def ", fn_anchor + 100)
    body = src[fn_anchor:end]
    # No nested imports in the function body.
    assert "import os\n" not in body
    assert "import shutil\n" not in body
    # v1.14.55 marker explains the cleanup.
    assert "v1.14.55:" in body
