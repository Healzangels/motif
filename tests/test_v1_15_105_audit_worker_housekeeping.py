"""v1.15.105 — AUDIT_WORKER housekeeping: M6, M7, L1.

Three independent low-risk fixes bundled in one tag:

  M6: failure-message truncation now adds a visible `...` suffix
      so the UI/operator sees the truncation marker. Pre-fix the
      raw `str(e)[:500]` / `err_text[:120]` slice gave no signal
      that the message was cut.

  M7: `_DOWNLOAD_PROGRESS` had no TTL. If a worker died outside
      the finally that calls `clear_download_progress`, the entry
      lingered for process lifetime. v1.15.105 adds a
      `sweep_download_progress(active)` helper called once per
      `_synthesize_queue_ops` render (1Hz on /api/progress).

  L1: `_is_cancelled`'s blanket `except Exception: return False`
      swallowed silently. v1.15.105 adds a `log.debug` breadcrumb
      so persistent failures (DB lock storms, schema corruption)
      leave a signal in the log.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WORKER_PY = REPO / "app" / "core" / "worker.py"
PROGRESS_PY = REPO / "app" / "core" / "progress.py"


# ── M6: truncation helper ─────────────────────────────────────

def test_truncate_err_helper_exists():
    """The shared `_truncate_err` helper must exist at module
    scope so all 4 callsites use one definition."""
    from app.core.worker import _truncate_err
    assert callable(_truncate_err)


def test_truncate_err_adds_ellipsis_when_over_limit():
    from app.core.worker import _truncate_err
    msg = "x" * 600
    out = _truncate_err(msg, 500)
    assert out.endswith("...")
    assert len(out) == 500


def test_truncate_err_passthrough_when_under_limit():
    from app.core.worker import _truncate_err
    msg = "short message"
    out = _truncate_err(msg, 500)
    assert out == msg
    assert "..." not in out


def test_truncate_err_callsites_replaced():
    """All 4 known slicing sites must use the helper now. Pre-fix
    they were `str(e)[:500]`, `str(e)[:300]`, `err_text[:120]`."""
    src = WORKER_PY.read_text()
    # Strip docstrings + comments before checking — the M6 audit
    # narrative references the pre-fix patterns by name.
    import re
    code = re.sub(r'""".*?"""', "", src, flags=re.DOTALL)
    code = re.sub(r"#.*", "", code)
    # The slice-without-helper pattern must be gone from real code.
    assert "str(e)[:500]" not in code
    assert "str(e)[:300]" not in code
    assert "err_text[:120]" not in code
    # And the helper must be referenced.
    assert "_truncate_err(" in code


# ── M7: _DOWNLOAD_PROGRESS sweep ──────────────────────────────

def test_sweep_download_progress_drops_stale_entries():
    """sweep should remove entries whose job_id is not in the
    active list. The dict must persist across calls so set/clear
    semantics aren't broken."""
    from app.core import progress as p
    # Stage 3 entries; mark only 1 active.
    p.set_download_progress(101, 0.1)
    p.set_download_progress(102, 0.5)
    p.set_download_progress(103, 0.9)
    p.sweep_download_progress([102])
    assert 101 not in p._DOWNLOAD_PROGRESS
    assert 102 in p._DOWNLOAD_PROGRESS
    assert 103 not in p._DOWNLOAD_PROGRESS
    # Cleanup so we don't leak into sibling tests.
    p.clear_download_progress(102)


def test_sweep_download_progress_noop_when_all_active():
    from app.core import progress as p
    p.set_download_progress(201, 0.3)
    p.set_download_progress(202, 0.7)
    p.sweep_download_progress([201, 202])
    assert 201 in p._DOWNLOAD_PROGRESS
    assert 202 in p._DOWNLOAD_PROGRESS
    p.clear_download_progress(201)
    p.clear_download_progress(202)


def test_synthesize_queue_ops_invokes_sweep():
    """The sweep must be wired into _synthesize_queue_ops so it
    runs every /api/progress poll without explicit caller action."""
    src = PROGRESS_PY.read_text()
    fn_anchor = src.index("def _synthesize_queue_ops(")
    # Capture enough of the body to see the sweep call.
    body = src[fn_anchor:fn_anchor + 3000]
    assert "sweep_download_progress(running_dl_jobs)" in body


# ── L1: _is_cancelled breadcrumb ──────────────────────────────

def test_is_cancelled_logs_debug_on_exception():
    """The except-clause must log.debug so persistent failures
    (DB lock storms, schema corruption) leave a signal."""
    src = WORKER_PY.read_text()
    fn_start = src.index("def _is_cancelled(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The bare `except Exception:` must now bind the exception
    # so it can be logged.
    assert "except Exception as e:" in fn_body
    assert "log.debug(" in fn_body
    assert "cancel-check failed" in fn_body
