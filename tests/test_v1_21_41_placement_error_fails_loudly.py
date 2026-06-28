"""v1.21.41 — a placement I/O failure surfaces as a real failure, not a skip.

Silent-failure audit finding M1: place_theme returns reason='placement_error:...'
for a genuine hardlink/copy I/O failure (disk full, EPERM, cross-FS), but
_do_place logged it at INFO as "Skipped placement" (indistinguishable from
the intentional plex_has_theme skip) and let dispatch mark the job DONE —
so a real failure never lit the topbar FAIL dot and was laundered into a
successful no-op.

Fix (the user's call: WARNING log + FAIL dot): detect the placement_error
reason → log at WARNING with failure wording AND mark the job FAILED via
_mark_failed_terminal (no backoff ladder — the hourly retry sweep, which
keys on last_place_attempt_reason='placement_error:...', stays the single
recovery path).

_do_place builds a FolderIndex + PlexClient before place_theme, so this is
source-pinned to the function slice (mirrors test_v1_19_61's approach to
_do_place outcome handling).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


def _do_place_slice():
    start = WORKER_PY.index("def _do_place(self, job: sqlite3.Row)")
    end = WORKER_PY.index("def _do_place_collection(", start)
    return WORKER_PY[start:end]


def test_placement_error_detected_from_reason():
    fn = _do_place_slice()
    assert ('_placement_io_failed = (\n'
            '            not outcome.placed\n'
            '            and (outcome.reason or "").startswith("placement_error"))'
            in fn), "placement_error must be detected from outcome.reason"


def test_placement_error_logs_warning_not_skip():
    fn = _do_place_slice()
    assert 'if _placement_io_failed:' in fn
    assert '_place_log_level = "WARNING"' in fn
    # Failure wording, NOT the "Skipped placement" of an intentional skip.
    assert '_place_msg = f"Placement FAILED: {outcome.reason}"' in fn


def test_placement_error_marks_job_failed_for_fail_dot():
    fn = _do_place_slice()
    # Terminal-fail (no backoff ladder) so the FAIL dot lights and the
    # hourly sweep remains the single recovery path.
    assert "self._mark_failed_terminal(" in fn
    mark_idx = fn.index("self._mark_failed_terminal(")
    # The mark must be guarded by the placement_error flag.
    guard_idx = fn.rindex("if _placement_io_failed:", 0, mark_idx)
    assert guard_idx < mark_idx
    assert "placement failed:" in fn[mark_idx:mark_idx + 200]


def test_v1_21_41_version_pin():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
