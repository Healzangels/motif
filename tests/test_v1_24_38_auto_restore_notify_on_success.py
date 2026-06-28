"""v1.24.38 — theme_auto_restored fires on the place LANDING, not at enqueue.

Review #6: the scheduler's hourly _restore_lost_placements sweep dispatched the
"🛠️ motif restored N missing themes" notification at ENQUEUE — right after
inserting the place jobs, before any of them ran. A place that then FAILED (the
canonical turned out gone, an upload 500'd, the folder vanished) still pinged the
operator with a success they never got. The v1.24.35 #2 fix removed the most
common doomed case (canonical_present=0) but other post-enqueue failures stayed.

Fix: the sweep now only ENQUEUES (tagged payload reason='auto_restore', no
dispatch); the worker fires theme_auto_restored from _do_place /
_do_place_collection on outcome.placed — the place actually landing — coalesced
(bulk=True) so a Plex-re-add burst still collapses to one summary.
"""
from __future__ import annotations

from pathlib import Path

from app.core import notify_content as nc

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()

CTX = {"display_title": "Avenue Q (2003)", "source_kind": "themerrdb",
       "provenance": "themerrdb", "theme_url": "https://youtu.be/abc",
       "thumb_url": "https://i.ytimg.com/vi/abc/mqdefault.jpg"}


# ── formatters ──────────────────────────────────────────────────────────

def test_title_and_item():
    assert nc.format_theme_auto_restored_title(CTX) == (
        "🛠️ Theme restored from backup — Avenue Q (2003)")
    assert nc.format_theme_auto_restored_item(CTX) == "Avenue Q (2003)"


def test_batch_title_singular_plural():
    assert nc.format_theme_auto_restored_batch_title(1) == (
        "🛠️ motif restored 1 missing theme")
    assert nc.format_theme_auto_restored_batch_title(3) == (
        "🛠️ motif restored 3 missing themes")


def test_body_explains_self_heal_and_carries_url():
    body = nc.format_theme_auto_restored_body(CTX)
    assert "went missing" in body and "backup" in body
    # reuses _render_url_lines → the v1.24.31 uniform-thumbnail embed suppression
    assert "<https://youtu.be/abc>" in body


def test_batch_body_is_bulleted():
    body = nc.format_theme_auto_restored_batch_body(["A (2001)", "B (2002)"])
    assert "* A (2001)" in body and "* B (2002)" in body


# ── worker fires on landing (both place sites) ──────────────────────────

def test_do_place_has_auto_restore_branch():
    # the movie/TV place path routes reason='auto_restore' to theme_auto_restored
    i = WORKER_PY.index("def _do_place(")
    j = WORKER_PY.index("def _do_place_collection(")
    block = WORKER_PY[i:j]
    assert 'if _reason == "auto_restore":' in block
    assert block.count('event_kind="theme_auto_restored"') == 1
    # it must sit on the outcome.placed success path (after the guard)
    assert block.index("if outcome.placed:") < block.index(
        'if _reason == "auto_restore":')


def test_do_place_collection_has_auto_restore_branch():
    k = WORKER_PY.index("def _do_place_collection(")
    block = WORKER_PY[k:]
    assert 'if _reason == "auto_restore":' in block
    assert 'event_kind="theme_auto_restored"' in block


# ── scheduler only enqueues (no premature dispatch) ─────────────────────

def test_scheduler_tags_payload_and_does_not_dispatch():
    i = SCHED_PY.index("def _restore_lost_placements(")
    j = SCHED_PY.index("\n\ndef ", i + 1)
    block = SCHED_PY[i:j]
    assert '"reason": "auto_restore"' in block, "place payload must be tagged"
    # the premature enqueue-time dispatch is gone — the worker owns it now.
    assert "event_kind=\"theme_auto_restored\"" not in block
    assert "_notify.dispatch(" not in block
