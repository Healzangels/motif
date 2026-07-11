"""v0.51.129 — manual REFRESH PLEX forces a full enum (bypasses the CCA-skip).

Code-review follow-up to v0.51.128. The reaper miss-counter only advances on
enums that actually WALK the section, but the contentChangedAt-skip short-
circuits before the reaper. After a removal bumps contentChangedAt once, later
enums skip until the 24h-overdue bypass — so a genuine removal would take ~24h
to reap and a manual REFRESH couldn't force it (run_plex_enum had no force path).

v0.51.129 threads a `force` flag: user-initiated refresh endpoints stamp
`force=true` on the plex_enum job → `_do_plex_enum` passes `force=True` →
`run_plex_enum` bypasses the skip. Cron + the post-sync cascade omit it, so the
skip optimization is preserved for automatic enums.

The behavioral bypass (force=True runs the enum even when CCA matches) is proved
in test_v1_14_74_content_changed_at_delta_gate.py::
test_force_bypasses_skip_even_when_cca_matches. These are the plumbing pins.
"""
from __future__ import annotations

import inspect
from pathlib import Path

from app.core import plex_enum

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_run_plex_enum_accepts_force_kwarg():
    sig = inspect.signature(plex_enum.run_plex_enum)
    assert "force" in sig.parameters
    assert sig.parameters["force"].default is False


def test_worker_reads_and_forwards_payload_force():
    # _do_plex_enum reads the payload flag and threads it into run_plex_enum.
    assert 'force_enum = bool(payload.get("force"))' in WORKER_PY
    assert "force=force_enum," in WORKER_PY


def test_post_sync_cascade_does_not_force():
    # The automatic post-sync cascade payload must NOT carry force — cron-cadence
    # enums keep the contentChangedAt-skip; only user refreshes force.
    assert '{"section_id": sid, "scope": "cascade"}' in WORKER_PY
    # the cascade INSERT block has no force flag adjacent to its scope tag.
    cascade_idx = WORKER_PY.index('"scope": "cascade"')
    assert '"force"' not in WORKER_PY[cascade_idx:cascade_idx + 200]


def test_manual_refresh_endpoints_stamp_force():
    # All four user-initiated plex_enum enqueue sites carry force=True:
    #   per-section refresh, tab-scoped REFRESH FROM PLEX, global scan_all,
    #   and manual discovery scan_all.
    assert '{"section_id": section_id, "force": True}' in API_PY
    assert '{"scope": "scan_all", "force": True}' in API_PY
    assert API_PY.count('"force": True') >= 4
