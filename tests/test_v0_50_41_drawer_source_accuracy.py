"""v0.50.41 — Live Ops drawer accuracy (audit, backend half).

From the 4-agent drawer-numbers audit:
  - reset_stale_on_boot + sweep_stuck flipped a stale op to 'failed' with a raw bulk
    UPDATE that never closed the in-flight stage timing, so a crashed op's final
    (often longest) stage was dropped from the RUN INSIGHT waterfall and the internal
    _stage_* keys leaked into the finished detail_json. Both now go per-row through
    _finalize_stale_detail (mirrors finish_progress's teardown).
  - bulk_lps reset processed_total to u_done at the unplace stage, so the op-wide
    'items processed' counter (+ avg/s) jumped backward n_to_probe→0 at the
    probe→unplace boundary. Now carries the probe count forward.
"""
from __future__ import annotations

import json
from pathlib import Path

from app.core.progress import _finalize_stale_detail

REPO = Path(__file__).resolve().parent.parent


def test_finalize_stale_detail_closes_stage_and_pops_keys():
    detail = {
        "stage_timings": [],
        "_stage_key": "fetch",
        "_stage_label": "Fetching",
        "_stage_started": "2026-06-28T00:00:00+00:00",
    }
    out = json.loads(_finalize_stale_detail(json.dumps(detail),
                                            "2026-06-28T00:00:05+00:00"))
    # the in-flight stage is closed into stage_timings (not dropped)
    assert out["stage_timings"] == [
        {"stage": "fetch", "label": "Fetching", "seconds": 5.0}]
    # the internal trackers no longer leak into the finished detail
    assert "_stage_key" not in out
    assert "_stage_started" not in out
    assert "_stage_label" not in out


def test_finalize_is_a_noop_when_no_stage_open():
    out = json.loads(_finalize_stale_detail('{"stage_timings": []}', "2026-06-28T00:00:05+00:00"))
    assert out["stage_timings"] == []


def test_both_stale_sweeps_finalize_per_row():
    src = (REPO / "app" / "core" / "progress.py").read_text()
    # both reset_stale_on_boot + sweep_stuck route the reaped row through the
    # finalizer, closing the stage at the row's last-progress time (v0.50.47).
    assert src.count('_finalize_stale_detail(r["detail_json"], r["updated_at"])') == 2
    # the old bulk UPDATE-without-detail form is gone from both
    assert "WHERE status IN ('pending', 'running', 'cancelling')\",\n            (now, now)," not in src


def test_bulk_lps_processed_total_carries_probe_forward():
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert "processed_total=n_to_probe + u_done" in api
    # the backward-jumping form is gone
    assert "stage_current=u_done, processed_total=u_done," not in api
