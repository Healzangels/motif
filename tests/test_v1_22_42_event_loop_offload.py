"""v1.22.42 (holistic audit) — blocking Plex / large-file I/O moved off the
event loop via run_in_threadpool.

Three async endpoints did synchronous Plex round-trips (and a 50MB read) directly
on the event loop, freezing every concurrent request for the duration:

- A1 api_set_override_intent (plex_cloud PROMOTE): get_themes drift-probe +
  abs_path.read_bytes() (50MB) + upload_collection_theme — all INSIDE the
  endpoint's BEGIN IMMEDIATE. The write lock still rides across the awaits (the
  worker retries on lock, class 8) but the loop is no longer frozen.
- A2 api_unplace_item LPS restore loop: per-placement get_themes + delete_theme
  + set_active_theme_via_reupload.
- A3 _teardown_plex_api_artifacts_for_placements (sync, own conn): the per-rk
  delete_theme loop, called directly from api_forget_item + api_delete_item.

These pins assert each blocking call is now awaited through run_in_threadpool.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _region(start_anchor: str) -> str:
    """Slice from the def to the next sibling def/decorator at the same
    indentation, so the region is exactly that one function's body (these
    endpoints are huge, and a fixed window would either truncate or bleed)."""
    start = API_PY.index(start_anchor)
    line_start = API_PY.rfind("\n", 0, start) + 1
    indent = " " * (start - line_start)
    after = API_PY[start + len(start_anchor):]
    m = re.search(r"\n" + indent + r"(?:async def |def |@)", after)
    end = (start + len(start_anchor) + m.start()) if m else len(API_PY)
    return API_PY[start:end]


# ── A1: api_set_override_intent plex_cloud PROMOTE ────────────


def test_a1_promote_offloads_get_themes_read_and_upload():
    region = _region("async def api_set_override_intent")
    # get_themes drift-probe off the loop
    assert "themes_resp = await run_in_threadpool(" in region
    # 50MB read off the loop
    assert "audio_bytes = await run_in_threadpool(abs_path.read_bytes)" in region
    # the re-upload off the loop
    assert "await run_in_threadpool(" in region
    assert "plex.upload_collection_theme," in region
    # the pre-fix synchronous forms must be gone
    assert "audio_bytes = abs_path.read_bytes()" not in region
    assert "themes_resp = plex_probe.get_themes(" not in region


# ── A2: api_unplace_item LPS restore loop ─────────────────────


def test_a2_unplace_restore_offloads_all_three_plex_calls():
    region = _region("async def api_unplace_item")
    assert "themes_resp = await run_in_threadpool(\n" in region
    assert "deleted = await run_in_threadpool(" in region
    assert "restored = await run_in_threadpool(" in region
    # pre-fix synchronous forms gone
    assert "themes_resp = plex.get_themes(rating_key=rk)" not in region
    assert "deleted = plex.delete_theme(rating_key=rk)" not in region
    assert "restored = plex.set_active_theme_via_reupload(" not in region


# ── A3: teardown helper off-loaded at both async callers ──────


def test_a3_forget_and_delete_offload_teardown():
    # Both async callers must await the sync teardown through a thread.
    forget = _region("async def api_forget_item")
    assert (
        "api_deleted = await run_in_threadpool(\n"
        "            _teardown_plex_api_artifacts_for_placements,"
    ) in forget
    delete = _region("async def api_delete_item")
    assert (
        "api_deleted = await run_in_threadpool(\n"
        "            _teardown_plex_api_artifacts_for_placements,"
    ) in delete
    # The direct synchronous call must not survive at an async caller.
    assert (
        "api_deleted = _teardown_plex_api_artifacts_for_placements(" not in forget
    )
    assert (
        "api_deleted = _teardown_plex_api_artifacts_for_placements(" not in delete
    )


def test_a3_background_bulk_lps_caller_unchanged():
    # The bulk-LPS caller runs in a threading.Thread (off the loop already), so
    # it must keep the direct synchronous call — wrapping it in
    # run_in_threadpool there would be wrong (no event loop in that thread).
    region = _region("def _bulk_lps_run")
    assert "_teardown_plex_api_artifacts_for_placements(" in region
    assert "await run_in_threadpool" not in region
