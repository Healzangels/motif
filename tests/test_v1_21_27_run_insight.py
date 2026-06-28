"""v1.21.27 — RUN INSIGHT drawer redesign + expand flicker fix.

the user: the expanded drawer view (a meta line + raw event log) didn't add
much over the activity feed already on the card, and clicking to expand
flickered. Redesign:

  - Backend: central per-stage duration capture in progress.py
    (start/update/finish) → detail.stage_timings, powering a "where did the
    time go" waterfall. One place, every multi-stage op gets it free.
  - Frontend: renderExpandedDetail → stage waterfall + stat readout
    (done_summary as big numbers + peak/avg throughput) + a taller
    throughput chart + the run log demoted to the bottom.
  - Flicker fix: exp/evn removed from _structuralHash (they forced a full
    card DOM replace on expand); expand/collapse + insight refresh now go
    through _updateCardInPlace (inject/remove .op-card-detail in place).
"""
from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


# ── backend: stage-timing capture ────────────────────────────

def test_stage_timings_captured_in_order():
    from app.core.db import init_db, get_conn
    from app.core import progress as P
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    P.start_progress(db, "op1", "tdb_sync", stage="git_fetch", stage_label="Git fetch")
    time.sleep(0.02)
    P.update_progress(db, "op1", stage="index", stage_label="Index", processed_total=10)
    time.sleep(0.02)
    P.update_progress(db, "op1", stage="prune", stage_label="Prune")
    P.finish_progress(db, "op1", status="done")
    with get_conn(db) as c:
        det = json.loads(c.execute(
            "SELECT detail_json FROM op_progress WHERE op_id='op1'").fetchone()["detail_json"])
    st = det.get("stage_timings")
    assert [s["stage"] for s in st] == ["git_fetch", "index", "prune"]
    assert all(isinstance(s["seconds"], (int, float)) and s["seconds"] >= 0 for s in st)
    # internal trackers cleaned up on finish
    assert "_stage_key" not in det and "_stage_started" not in det


def test_finish_before_any_stage_is_safe():
    # tvdb_bridge-style: start with a stage, then fail before transitioning.
    from app.core.db import init_db, get_conn
    from app.core import progress as P
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    P.start_progress(db, "op2", "tvdb_bridge", stage="bridge")
    P.finish_progress(db, "op2", status="failed", error_message="no TMDB key")
    with get_conn(db) as c:
        det = json.loads(c.execute(
            "SELECT detail_json FROM op_progress WHERE op_id='op2'").fetchone()["detail_json"])
    # the single in-flight stage is closed out; no crash; error preserved
    assert [s["stage"] for s in det.get("stage_timings", [])] == ["bridge"]
    assert det.get("error_message") == "no TMDB key"


def test_finish_on_missing_op_is_noop():
    from app.core.db import init_db
    from app.core import progress as P
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    P.finish_progress(db, "ghost", status="done")  # must not raise


# ── frontend: RUN INSIGHT panel ──────────────────────────────

def test_run_insight_sections_present():
    assert "// RUN INSIGHT" in OPS_JS
    assert "function _renderWaterfall(" in OPS_JS
    assert "function _renderInsightStats(" in OPS_JS
    assert "function _renderThroughputChart(" in OPS_JS
    # all three are composed into the expanded detail
    idx = OPS_JS.index("function renderExpandedDetail(")
    body = OPS_JS[idx:idx + 2500]
    assert "_renderWaterfall(op" in body
    assert "_renderInsightStats(op)" in body
    assert "_renderThroughputChart(op)" in body


def test_waterfall_reads_stage_timings_and_live_stage():
    idx = OPS_JS.index("function _renderWaterfall(")
    body = OPS_JS[idx:idx + 1200]
    assert "d.stage_timings" in body
    assert "d._stage_key" in body and "d._stage_started" in body  # live in-flight bar


# ── flicker fix: in-place expand ─────────────────────────────

def test_structural_hash_no_longer_keys_on_expand():
    idx = OPS_JS.index("function _structuralHash(")
    body = OPS_JS[idx:OPS_JS.index("function _updateCardInPlace(")]
    assert "exp:" not in body
    assert "evn:" not in body


def test_update_in_place_handles_expand_collapse():
    idx = OPS_JS.index("function _updateCardInPlace(")
    body = OPS_JS[idx:OPS_JS.index("function renderDrawerBody(")]
    assert "state.expandedOpId" in body
    assert "op-card-detail" in body
    assert "insertAdjacentHTML" in body
    assert "renderExpandedDetail(op)" in body


# ── css ──────────────────────────────────────────────────────

def test_run_insight_css_present():
    for sel in (".op-wf-bar", ".op-wf-track", ".op-insight-stats",
                ".op-stat-value", ".op-tpchart-bar"):
        assert sel in OPS_CSS, sel
    # bars inherit the card tone
    assert ".op-card.op-tone-tdb   .op-wf-bar" in OPS_CSS


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
