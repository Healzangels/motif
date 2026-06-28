"""v1.20.20 — audit round 4 fixes (parallel-agent sweep).

MED  scheduler.py: the lockout-SUMMARY query (reports which rows the
     retry-sweep gate blocked) still used the lexicographic
     `finished_at > datetime('now','-24 hours')` compare that v1.19.5
     fixed in its sibling GATE. now_iso() writes ISO-T-with-offset;
     datetime('now',...) writes space-separated; 'T'(0x54)>' '(0x20),
     so the two queries disagree on the 24h boundary by time-of-day and
     the "N rows blocked" event names rows the gate actually
     re-enqueued. Both must use julianday().
LOW-MED worker.py _do_refresh: discarded plex.refresh()'s bool → a
     refresh that ran and returned False (stale rk / Plex unreachable)
     was marked ✓ DONE silently — the exact fake-✓-DONE the v1.14.52
     marker above warns against. Now logs + log_events a WARNING.
LOW  worker.py _do_refresh: the `if not plex: return` early-return had
     no breadcrumb (every sibling handler logs why it no-ops). Now logs.
LOW  stale AB comments (api.py + app.js) refreshed for the four-way
     PB/TB/AB/UB partition.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SCHED = (REPO / "app" / "core" / "scheduler.py").read_text()
WORKER = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── MED: scheduler lockout-summary julianday migration ───────


def test_scheduler_lockout_summary_uses_julianday():
    fn = SCHED[SCHED.index("def _retry_pending_placements("):]
    fn = fn[:fn.index("\ndef ", 1)]
    # The buggy lexicographic compare must be gone from BOTH the gate
    # and the summary query.
    assert "j2.finished_at > datetime('now'" not in fn, (
        "v1.20.20: the lexicographic timestamp compare must be gone — "
        "use julianday() (matches the v1.19.5 gate fix)"
    )
    # Both the gate AND the summary query use julianday on finished_at.
    assert fn.count("julianday(j2.finished_at)") >= 2, (
        "v1.20.20: BOTH the lockout gate and the summary query must "
        "compare via julianday(j2.finished_at) so they agree"
    )


# ── LOW-MED: refresh surfaces a failed nudge ─────────────────


def test_refresh_surfaces_failed_nudge():
    fn = WORKER[WORKER.index("    def _do_refresh("):]
    fn = fn[:fn.index("\n    def ", 1)]
    assert "refreshed = plex.refresh(rk)" in fn, (
        "v1.20.20: capture plex.refresh()'s bool instead of discarding it"
    )
    assert "if not refreshed:" in fn
    # surfaced via both the logger and the events table.
    assert "log.warning(" in fn
    assert "not confirmed by Plex" in fn


# ── LOW: refresh logs the Plex-unconfigured skip ─────────────


def test_refresh_logs_unconfigured_skip():
    fn = WORKER[WORKER.index("    def _do_refresh("):]
    fn = fn[:fn.index("\n    def ", 1)]
    idx = fn.index("if not plex:")
    block = fn[idx:idx + 700]
    assert "Plex not configured" in block, (
        "v1.20.20: the early return must log WHY it no-ops (v1.18.5 "
        "cold-path rule) — it silently marked the job done before"
    )


# ── LOW: stale AB comments refreshed ─────────────────────────


def test_backup_chip_comments_document_four_way_partition():
    # api.py TB branch comment + app.js linkCell comment both name the
    # four-way PB/TB/AB/UB partition now (no longer the pre-AB "BK=user"
    # / "url/upload/adopt" wording).
    assert "PB=plex_cloud, TB=themerrdb, AB=adopt, UB=user" in API_PY
    assert "url/upload/adopt" not in APP_JS, (
        "v1.20.20: app.js comment must not still lump adopt into UB"
    )


def test_v1_20_20_version_pin():
    # Loose pin (canonical exact pin lives in test_v1_13_79).
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
