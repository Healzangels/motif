"""v0.50.43 — plex_enum RUN INSIGHT waterfall: break out a 'health' stage.

From the drawer-numbers audit (the deferred D4): plex_enum's post-loop tail ran
reconcile_placement_paths + verify_placement_health + verify_canonical_health all
under the single 'reconcile' waterfall bar. The two stat-every-theme.mp3 health
passes can dominate the tail on a slow Unraid/NFS mount, yet read as folder-rename
time. v0.50.43 transitions to a distinct 'health' stage before the health passes so
the STAGE BREAKDOWN waterfall splits them out — a post-loop, forward-only transition
(enumerate → reconcile → health), so the step strip stays monotonic with no
per-section bouncing.
"""
from __future__ import annotations

import json
from pathlib import Path

from app.core import progress
from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
ENUM = (REPO / "app" / "core" / "plex_enum.py").read_text()
OPS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


def test_plex_enum_transitions_to_health_before_the_health_passes():
    # the new stage transition exists
    assert 'stage="health",' in ENUM
    assert 'stage_label="Verifying theme files on disk"' in ENUM
    # ordering: reconcile stage set → health transition → verify_placement_health call
    i_reconcile = ENUM.index('stage="reconcile"')
    i_health = ENUM.index('stage="health"')
    # v0.51.101: the call now carries section_ids=_scope_sections (scoped).
    i_verify = ENUM.index("verify_placement_health(db_path, section_ids=")
    assert i_reconcile < i_health < i_verify, (
        "health stage must transition after reconcile is set but before the "
        "verify_placement_health pass it wraps")


def test_stage_timeline_lists_health_after_reconcile():
    idx = OPS.index("plex_enum: [")
    block = OPS[idx:OPS.index("],", idx)]
    assert "key: 'enumerate'" in block
    assert "key: 'reconcile'" in block
    assert "key: 'health'" in block
    # forward-only order in the strip
    assert (block.index("key: 'enumerate'")
            < block.index("key: 'reconcile'")
            < block.index("key: 'health'"))


def test_three_stage_sequence_yields_an_ordered_waterfall(tmp_path):
    # exercise the real progress machinery: the enumerate→reconcile→health
    # transitions each close the prior stage into stage_timings, in order.
    db = tmp_path / "m.db"
    init_db(db)
    progress.start_progress(db, op_id="plex_enum", kind="plex_enum",
                            stage="enumerate", stage_label="Fetching libraries from Plex")
    progress.update_progress(db, "plex_enum", stage="reconcile",
                             stage_label="Reconciling placement paths")
    progress.update_progress(db, "plex_enum", stage="health",
                             stage_label="Verifying theme files on disk")
    progress.finish_progress(db, "plex_enum", status="done")

    with progress.get_conn(db) as conn:
        row = conn.execute(
            "SELECT detail_json FROM op_progress WHERE op_id='plex_enum'").fetchone()
    detail = json.loads(row["detail_json"])
    stages = [t["stage"] for t in detail.get("stage_timings", [])]
    assert stages == ["enumerate", "reconcile", "health"]
    # the internal trackers are cleaned up by finish_progress
    assert "_stage_key" not in detail
