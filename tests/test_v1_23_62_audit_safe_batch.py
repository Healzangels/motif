"""v1.23.62 — audit safe-batch: async event-loop blocks, a credential leak, a
mis-handled cancel, an incomplete notify severity map, and a too-broad warn-once.

From the holistic bug + silent-failure sweep. Each is a small, contained fix:
  #1/#10 — /api/tmdb/test + /api/admin/test-notification ran a synchronous
           network call (TMDB httpx GET / Apprise send) directly in the async
           body → froze the event loop. Now offloaded via run_in_threadpool, and
           the v1.22.58 lint extended to catch the two method names.
  #6     — GET /api/config masked git_url + database_url userinfo but not db_url
           (the third credential-capable sync URL) → cleartext leak.
  #5/#15 — a user-cancelled sync (_JobCancelled) was caught by the worker's broad
           `except Exception` → spurious "Sync failed" notification + the held
           auto-downloads released and run anyway. Now caught first + re-raised.
  #8     — three dispatched theme-loss/backup event kinds were missing from
           notify._EVENT_NOTIFY_TYPE → defaulted to neutral 'info'.
  #18    — the payload-parse warn-once flag was a single process-wide bool, so a
           corrupt payload in one handler muted the warning for every other.
"""
from __future__ import annotations

import ast
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()
WORKER = (REPO / "app" / "core" / "worker.py").read_text()
CONFIG_FILE = (REPO / "app" / "core" / "config_file.py").read_text()


# ── #1/#10: async event-loop blocks offloaded + lint extended ──


def test_tmdb_and_notification_tests_are_offloaded():
    assert "await run_in_threadpool(client.test_credentials)" in API
    assert "ok, msg = client.test_credentials()" not in API
    assert "await run_in_threadpool(\n            _notify.test_dispatch," in API
    assert "result = _notify.test_dispatch(" not in API


def test_blocking_lint_now_catches_tmdb_and_notify_methods():
    """The v1.22.58 lint derived its blocking set only from PlexClient, so it
    missed TMDBClient.test_credentials + notify.test_dispatch. The extended lint
    must FIRE on an inline call to either inside an async def."""
    import tests.test_v1_22_58_async_no_blocking_calls as lint
    assert "test_credentials" in lint.OTHER_BLOCKING_ATTRS
    assert "test_dispatch" in lint.OTHER_BLOCKING_ATTRS
    bad_tmdb = "async def h(r):\n    ok, m = client.test_credentials()\n    return m\n"
    bad_notify = "async def h(r):\n    res = _notify.test_dispatch(cfg)\n    return res\n"
    assert lint._scan(bad_tmdb), "lint must flag inline test_credentials()"
    assert lint._scan(bad_notify), "lint must flag inline test_dispatch()"


def test_real_api_still_passes_the_blocking_lint():
    import tests.test_v1_22_58_async_no_blocking_calls as lint
    assert lint._scan(API) == [], "no inline blocking calls should remain"


# ── #6: db_url credential masking + scheme validation ──


def test_db_url_masked_and_validated_alongside_the_other_sync_urls():
    # GET /api/config redaction loop now covers db_url.
    assert 'for _uk in ("git_url", "database_url", "db_url"):' in API
    # config_file scheme-validation loop now covers db_url.
    assert 'for _u_field in ("database_url", "git_url", "db_url"):' in CONFIG_FILE


# ── #5/#15: a cancelled sync is not a failure ──


def test_sync_cancel_caught_before_broad_except():
    """_JobCancelled must be handled BEFORE the broad `except Exception` in
    _do_sync, so a cancel skips the 'Sync failed' notification + the held-download
    release."""
    i = WORKER.index("def _do_sync(")
    body = WORKER[i:i + 4000]
    cancel = body.index("except _JobCancelled:")
    broad = body.index("except Exception as e:")
    assert cancel < broad, "_JobCancelled must be caught before `except Exception`"
    # the cancel branch must NOT run the failure-only side effects.
    cancel_block = body[cancel:broad]
    assert "release_sync_held_downloads" not in cancel_block
    assert "sync_failed" not in cancel_block


# ── #8: theme-loss/backup kinds in the severity map ──


def test_notify_severity_map_covers_the_dispatched_theme_loss_kinds():
    from app.core.notify import _EVENT_NOTIFY_TYPE
    assert _EVENT_NOTIFY_TYPE.get("theme_lost_backup_ready") == "warning"
    assert _EVENT_NOTIFY_TYPE.get("theme_lost_sidecar_available") == "warning"
    assert _EVENT_NOTIFY_TYPE.get("theme_backed_up") == "info"


# ── #18: payload-parse warn-once is per call-site, not process-wide ──


def test_payload_parse_warn_once_is_per_callsite():
    from app.core import worker as w
    assert isinstance(w._PAYLOAD_PARSE_WARNED, set), "keyed by call-site, not a bool"
    assert "if where not in _PAYLOAD_PARSE_WARNED:" in WORKER
    assert "_PAYLOAD_PARSE_WARNED.add(where)" in WORKER
