"""v0.51.231 — audit wave 4: the silent-failure cluster.

Four findings, each a different flavour of "fails without telling anyone":

  1. login_post ran bcrypt (rounds=12, ~235ms of pure CPU per auth.py's own comment)
     INLINE in an async handler, freezing the single asyncio loop for that quarter-second
     — every concurrent /api/stats, /api/library, /api/progress and /healthz stalled with
     it, and an attacker could drive it to the rate-limit ceiling. CLAUDE.md class 12. The
     standing lint can't see it: its blocklist enumerates PlexClient methods, nothing
     CPU-bound.
  2. notify._prepare_attachment leaked its mkstemp'd jpeg on any post-download failure
     (ffmpeg TimeoutExpired, getsize FileNotFoundError) and reported it only at DEBUG —
     the un-fixed twin of the v1.23.0 class-9 fix ten lines above.
  3. progress._synthesize_queue_ops used `del` on module dicts while /api/progress runs
     on the threadpool: two concurrent cache misses -> KeyError -> 500 -> the ops drawer
     blanks mid-burst. Every sibling line already used .pop(k, None).
  4. scheduler's release_available compared tags for INEQUALITY, so with nightly running
     ahead of the release branch it pushed an "upgrade" pointing backwards. api.py's
     sibling compared parsed tuples — mirror drift, now one shared helper.
"""
from __future__ import annotations

from pathlib import Path

from app.core.versioning import is_newer, parse_version

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()
PROGRESS_PY = (REPO / "app" / "core" / "progress.py").read_text()
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()


# ── 1: bcrypt off the event loop ─────────────────────────────────────────────

def test_login_offloads_bcrypt():
    i = API_PY.index("async def login_post(")
    body = API_PY[i:API_PY.index("\n    @app.", i)]
    assert "await run_in_threadpool(\n            authenticate_password," in body, (
        "bcrypt at rounds=12 is ~235ms of CPU — inline it freezes the single event loop")
    assert "\n        ok = authenticate_password(" not in body, "the inline call must be gone"


# ── 2: the attachment temp must never be stranded ────────────────────────────

def test_attachment_failure_cleans_up_and_is_visible():
    i = NOTIFY_PY.index("def _prepare_attachment(")
    body = NOTIFY_PY[i:NOTIFY_PY.index("\ndef ", i + 1)]
    tail = body[body.index("except Exception as e:"):]
    assert "os.unlink" in tail, "the mkstemp'd file must be removed on the failure path"
    assert "log.warning" in tail, (
        "a silent DEBUG made the leak invisible at default log level (class 9)")


# ── 3: concurrent-safe dict eviction ─────────────────────────────────────────

def test_burst_dicts_are_evicted_idempotently():
    i = PROGRESS_PY.index("def _synthesize_queue_ops(")
    body = PROGRESS_PY[i:PROGRESS_PY.index("\ndef ", i + 1)]
    assert "del _QUEUE_BURST_HW[" not in body
    assert "del _QUEUE_BURST_START[" not in body
    assert "_QUEUE_BURST_HW.pop(jt, None)" in body
    assert "_QUEUE_BURST_START.pop(jt, None)" in body


# ── 4: one definition of "newer" ─────────────────────────────────────────────

def test_release_check_never_nags_backwards():
    """The actual reported shape: nightly runs ahead of the release branch."""
    assert is_newer("v0.51.220", "0.51.227") is False
    assert is_newer("v0.51.240", "0.51.227") is True
    assert is_newer("v0.51.227", "0.51.227") is False


def test_version_parse_tolerates_real_tag_shapes():
    assert parse_version("v1.13.8-rc1") == (1, 13, 8)
    assert parse_version("0.51.227") == (0, 51, 227)
    assert parse_version("garbage") is None
    assert parse_version("") is None
    # an unreadable tag must never fire a notification
    assert is_newer("garbage", "0.51.227") is False


def test_both_sites_use_the_shared_helper():
    """They disagreed before; a shared definition is the only thing that stops it
    happening again (the SRC-axis mirror-drift lesson)."""
    assert "is_newer(payload[\"tag_name\"], running)" in SCHED_PY
    assert 'update_available = is_newer(' in API_PY
    # v0.51.231: check CODE lines only — the fix's own comment quotes the old expression
    # verbatim to explain what changed, so a whole-file substring check matches the
    # comment and fails on correct code. (Caught by this test failing on its first run.)
    code = [ln for ln in SCHED_PY.splitlines() if not ln.lstrip().startswith("#")]
    assert not any('payload["tag_name"] != running' in ln for ln in code), (
        "the inequality compare must be gone from the CODE")
