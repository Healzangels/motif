"""v1.21.25 — LIVE OPS display consistency pass.

the user: audit the status/ops display so different ops are treated the same
(uniform info, colors, % etc). The audit + fixes:

  HIGH-1: finished ops now mark the WHOLE step strip done/green instead of
    leaving the last step amber-pulsing (the all-done branch was dead code
    because every kind finishes ON its last stage). Two render sites.
  HIGH-2: uniform done-summary line for EVERY op kind — each worker stamps
    detail.done_summary as an ordered [{l,v}] list, formatted identically
    by _doneHeadline ("Done — N x · M y · …"). Previously only THEMERRDB
    SYNC got a breakdown; siblings showed bare "Done — N processed".
  MED-1: every real op_progress kind now has a STAGE_TIMELINE strip
    (bulk_lps probe/unplace, cloud_themes_backup walk/download, plus 1-chip
    bulk_probe_tdb + tvdb_bridge to match reprobe).
  LOW: tdb_sync's 4 never-lit index chips collapsed to one INDEX (matching
    the single emitted `index` stage); PENDING badge got a tone.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
SYNC = (REPO / "app" / "core" / "sync.py").read_text()
PLEX_ENUM = (REPO / "app" / "core" / "plex_enum.py").read_text()
API = (REPO / "app" / "web" / "api.py").read_text()

# the real op_progress kinds (db.py CHECK) — every one must have a strip now
REAL_KINDS = [
    "tdb_sync", "plex_enum", "reprobe_plex_themes", "bulk_probe_tdb",
    "bulk_lps", "tvdb_bridge", "cloud_themes_backup",
]


# ── HIGH-1: finished strip → all green, both render sites ─────

def test_finished_timeline_done_only_both_sites():
    # v1.21.26: the all-green strip is gated on status==='done' ONLY (not
    # "anything not running"), so a pending op (try_acquire inserts the row
    # with stage=NULL) or a failed-before-first-stage op is NOT painted
    # all-green. Both render sites (renderTimeline + the in-place updater).
    assert OPS_JS.count("const finished = (op.status === 'done');") == 2
    # finished is checked BEFORE the currentIdx branch in both
    assert OPS_JS.count("if (finished) {") == 2
    # the old over-broad predicate must be gone (regression guard)
    assert "op.status !== 'running' && op.status !== 'cancelling'" not in OPS_JS


# ── HIGH-2: uniform done-summary formatter + per-worker stamps ─

def test_done_headline_formats_uniform_list():
    idx = OPS_JS.index("function _doneHeadline(")
    body = OPS_JS[idx:idx + 1100]
    assert "Array.isArray(ds)" in body
    assert "parts.join(' · ')" in body
    assert "`Done — ${parts.join(' · ')}`" in body


def test_every_real_worker_stamps_done_summary():
    # sync + plex_enum each stamp once; api.py stamps the other 5 kinds.
    assert SYNC.count('"done_summary"') >= 1
    assert '{"l": "items", "v": stats.movies_seen' in SYNC
    assert PLEX_ENUM.count('"done_summary"') >= 1
    assert '{"l": "items", "v": stats["items_seen"]}' in PLEX_ENUM
    # reprobe / bulk_probe_tdb / bulk_lps / tvdb_bridge / cloud_backup
    assert API.count('"done_summary"') >= 5
    for needle in ('{"l": "Plex-served", "v": set_indep}',   # reprobe
                   '{"l": "alive", "v": n_alive}',           # bulk_probe_tdb
                   '{"l": "unplaced", "v": n_unplaced}',     # bulk_lps
                   '{"l": "linked", "v": result["linked"]}',  # tvdb_bridge
                   '{"l": "backed up", "v": len(downloaded)}'):  # cloud
        assert needle in API, needle


# ── MED-1: every real kind has a STAGE_TIMELINE strip ────────

def test_stage_timeline_covers_every_real_kind():
    idx = OPS_JS.index("const STAGE_TIMELINE = {")
    block = OPS_JS[idx:OPS_JS.index("};", idx)]
    for kind in REAL_KINDS:
        assert f"{kind}: [" in block, kind
    # the two genuine two-phase flows carry both stage keys
    assert "key: 'unplace'" in block      # bulk_lps phase 2
    assert "key: 'walk'" in block         # cloud_themes_backup phase 1
    assert "key: 'download'" in block     # cloud_themes_backup phase 2
    assert "key: 'bridge'" in block       # tvdb_bridge


# ── LOW: tdb_sync index chips + pending tone ─────────────────

def test_tdb_sync_index_chips_collapsed():
    idx = OPS_JS.index("tdb_sync: [")
    block = OPS_JS[idx:OPS_JS.index("],", idx)]
    assert "key: 'index'" in block
    # the 4 never-emitted granular chips are gone
    for dead in ("index_movie", "fetch_movie", "index_tv", "fetch_tv"):
        assert dead not in block, dead


def test_pending_badge_has_tone():
    assert ".op-card.op-status-pending .op-card-status" in OPS_CSS


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
