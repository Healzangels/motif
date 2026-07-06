"""v1.20.19 — definitive force-capture outcome message.

The v1.20.18 force-capture flow showed a CONDITIONAL alert
("if Plex's theme differed ... PB badge; if byte-identical, left
as-is"), which left the user guessing which branch fired on 10,000 BC
(it was byte-identical — Plex echoing motif's own uploaded TB).

Fix: the worker stamps a per-run outcome breakdown
(downloaded / skipped_identical / errors) into the op's detail_json
via set_detail_field, surfaced through /api/progress -> finalOp.detail.
The SOURCE-menu force-capture handler reads it and gives a definitive
message: "Captured" vs "No swap — byte-identical" vs "nothing to
capture".
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── worker stamps the outcome before finishing ───────────────


def test_worker_stamps_outcome_before_finish():
    fn_idx = API_PY.index("def _cloud_themes_backup_run(")
    # v0.50.45: widened 12000→13000 — the walk-count carry-forward added lines
    # ahead of the backup_outcome stamp, pushing final_status past the old window.
    # v0.51.16 (audit #26): widened 13000→15000 — the unmint_stale_orphans
    # compensation blocks (cancel-during-walk + post-loop) land before the stamp.
    # v0.51.86: widened 15000→16000 — the replaced-count stamp + its comment
    # added lines ahead of final_status.
    fn = API_PY[fn_idx:fn_idx + 16000]
    assert 'set_detail_field(' in fn
    assert '"backup_outcome"' in fn
    # the stamp must precede finish_progress (waitForOp returns the
    # instant status flips to done, so the detail must be there first).
    stamp_idx = fn.index('"backup_outcome"')
    finish_idx = fn.index('final_status = "cancelled"')
    assert stamp_idx < finish_idx, (
        "v1.20.19: backup_outcome must be stamped BEFORE finish_progress"
    )
    # outcome breakdown carries the counts. v0.51.86: window 300→600 (the
    # "replaced" key + its comment sit between downloaded and skipped_identical).
    block = fn[stamp_idx:stamp_idx + 600]
    assert '"downloaded"' in block
    assert '"replaced"' in block
    assert '"skipped_identical"' in block
    assert '"errors"' in block


# ── outcome surfaces through load_active -> finalOp.detail ───


def test_outcome_surfaces_through_load_active(tmp_path):
    """Behavioral: a stamped backup_outcome must reach the parsed
    `detail` dict that /api/progress (load_active) returns — the same
    field the JS reads as finalOp.detail.backup_outcome."""
    from app.core.db import init_db
    from app.core import progress as op
    db = tmp_path / "t.db"
    init_db(db)
    op.start_progress(db, "cloud-themes-backup", "cloud_themes_backup",
                      stage="walk", stage_label="x")
    op.set_detail_field(db, "cloud-themes-backup", "backup_outcome",
                        {"downloaded": 0, "skipped_identical": 1,
                         "errors": 0})
    op.finish_progress(db, "cloud-themes-backup", status="done")
    rows = op.load_active(db)
    match = [r for r in rows if r.get("op_id") == "cloud-themes-backup"]
    assert match, "the finished op must surface in load_active"
    detail = match[0]["detail"]
    assert detail["backup_outcome"]["skipped_identical"] == 1, (
        "v1.20.19: the stamped outcome must reach finalOp.detail so the "
        "JS can message definitively"
    )


# ── JS reads the definitive outcome ──────────────────────────


def test_js_reads_definitive_outcome():
    idx = APP_JS.index("function cloudBackupForceCapture(")
    # v0.51.51: widened 5000→6800 — the helper gained the optimistic-placeholder
    # set/clear plumbing ahead of the outcome-reading branches.
    # v0.51.86: widened 6800→8400 — the first-capture-vs-replaced split added
    # lines ahead of the skipped_identical branch.
    fn = APP_JS[idx:idx + 8400]
    # reads the stamped outcome off the finished op.
    assert "fin.detail && fin.detail.backup_outcome" in fn
    # definitive branches: captured (downloaded>0) and identical.
    assert "outcome && outcome.downloaded > 0" in fn
    assert "outcome && outcome.skipped_identical > 0" in fn
    # v0.51.86: the captured branch now splits first-capture vs replaced.
    assert "outcome.replaced > 0" in fn
    # the identical branch says "byte-identical ... nothing distinct".
    assert "byte-identical" in fn


def test_v1_20_19_version_pin():
    # Loose pin (canonical exact pin lives in test_v1_13_79).
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
