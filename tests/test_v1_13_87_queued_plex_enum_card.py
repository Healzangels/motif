"""v1.13.87 — queued plex_enum drawer card + unraid template URL fix.

Two fixes:

1. **Queued plex_enum card**: the user's repro — click REFRESH on
   library A, navigate to library B and click REFRESH again. The
   second job pendings behind the first. The drawer shows only
   the running one as a card; the pending one is invisible.
   The (X of Y) topbar suffix from v1.13.27 hints at the queue
   depth but truncates at narrow widths and isn't always
   noticeable.

   Fix: synthesize a separate drawer card with kind=
   'plex_enum_pending' that appears when plex_enum has pending
   jobs. Same plex tone so it reads as related to the running
   card; distinct KIND_LABEL ('PLEX REFRESH (QUEUED)') so the
   user can tell which is which.

2. **Unraid template URLs** still pointed at the deleted `main`
   branch (v1.13.74-prep replaced main with release/nightly).
   `<TemplateURL>` and `<Icon>` both updated to `release`.
   Same root cause as the v1.13.86 favicon fix; just a different
   surface.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── Fix 1: queued plex_enum card synth ───────────────────────


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _seed_plex_enum_jobs(conn, *, running: int, pending: int):
    """Seed the jobs table with the requested mix of running +
    pending plex_enum rows. Mirror real worker output shape."""
    now = _now_iso()
    for _ in range(running):
        conn.execute(
            "INSERT INTO jobs ("
            "  job_type, payload, status, created_at, next_run_at,"
            "  started_at"
            ") VALUES ('plex_enum', '{}', 'running', ?, ?, ?)",
            (now, now, now),
        )
    for _ in range(pending):
        conn.execute(
            "INSERT INTO jobs ("
            "  job_type, payload, status, created_at, next_run_at"
            ") VALUES ('plex_enum', '{}', 'pending', ?, ?)",
            (now, now),
        )


def _load_ops(db: Path):
    """Call progress.load_active and return the list of ops."""
    from app.core.progress import load_active
    return load_active(db)


def _find_kind(ops, kind: str):
    """Return the first op with the given kind, or None."""
    return next((o for o in ops if o.get("kind") == kind), None)


def test_queued_plex_enum_card_appears_with_pending_jobs(db):
    """The reported scenario: 1 running + 1 pending plex_enum.
    Drawer should now have a 'plex_enum_pending' card alongside
    the (real) running one."""
    with sqlite3.connect(db) as conn:
        _seed_plex_enum_jobs(conn, running=1, pending=1)
    ops = _load_ops(db)
    pending = _find_kind(ops, "plex_enum_pending")
    assert pending is not None, (
        "v1.13.87 must synthesize a card for queued plex_enum "
        "so the user sees the second refresh waiting"
    )
    assert pending["status"] == "pending"
    assert "1 library refresh" in pending["stage_label"]


def test_queued_plex_enum_card_uses_singular_for_one(db):
    """Singular wording when count is 1: 'library refresh'
    (not 'library refreshes'). Pin the label.

    v1.14.77: input shape changed from (running=0, pending=1)
    to (running=1, pending=1). Pre-fix the synth fired on
    single-pending-no-running too — but that's the click →
    worker-pickup window where "queued" is the wrong framing
    (nothing's actually running yet to be queued behind). The
    optimistic placeholder covers that gap. The synth still
    fires for the v1.13.87 intent case (queued behind running),
    where the singular wording assertion still applies.

    v1.15.2: label format unified to `<count> library refresh(es)
    queued behind <blocker>` so plex_enum_pending matches the
    tdb_sync_pending shape. Blocker phrase distinguishes
    plex-vs-other-long-worker for diagnostic clarity."""
    with sqlite3.connect(db) as conn:
        _seed_plex_enum_jobs(conn, running=1, pending=1)
    ops = _load_ops(db)
    pending = _find_kind(ops, "plex_enum_pending")
    assert pending is not None
    assert pending["stage_label"] == (
        "1 library refresh queued behind another Plex refresh"
    )


def test_queued_plex_enum_card_uses_plural_for_many(db):
    """Plural wording when count > 1: '3 library refreshes
    queued behind another Plex refresh'.

    v1.15.2: label format unified, see the singular test above
    for the rationale."""
    with sqlite3.connect(db) as conn:
        _seed_plex_enum_jobs(conn, running=1, pending=3)
    ops = _load_ops(db)
    pending = _find_kind(ops, "plex_enum_pending")
    assert pending is not None
    assert pending["stage_label"] == (
        "3 library refreshes queued behind another Plex refresh"
    )


def test_no_queued_card_when_only_running(db):
    """Negative case: 1 running + 0 pending → no synth card.
    The running plex_enum already has its own real op_progress
    card; we don't want to duplicate it."""
    with sqlite3.connect(db) as conn:
        _seed_plex_enum_jobs(conn, running=1, pending=0)
    ops = _load_ops(db)
    assert _find_kind(ops, "plex_enum_pending") is None


def test_no_queued_card_when_no_jobs(db):
    """Empty state: zero plex_enum activity → no synth card."""
    ops = _load_ops(db)
    assert _find_kind(ops, "plex_enum_pending") is None


def test_queued_card_excludes_decided_jobs(db):
    """A done / failed / cancelled plex_enum must NOT count as
    pending. Only status='pending' triggers the synth."""
    now = _now_iso()
    with sqlite3.connect(db) as conn:
        _seed_plex_enum_jobs(conn, running=1, pending=0)
        for status in ("done", "failed", "cancelled"):
            conn.execute(
                "INSERT INTO jobs ("
                "  job_type, payload, status, created_at, next_run_at,"
                "  finished_at"
                ") VALUES ('plex_enum', '{}', ?, ?, ?, ?)",
                (status, now, now, now),
            )
    ops = _load_ops(db)
    assert _find_kind(ops, "plex_enum_pending") is None


def test_queued_card_op_id_is_stable(db):
    """The synth's op_id must be stable across polls so the
    drawer doesn't churn (re-render → losing the card's expand
    state, animation hiccups). Pin the literal id."""
    with sqlite3.connect(db) as conn:
        _seed_plex_enum_jobs(conn, running=1, pending=2)
    ops = _load_ops(db)
    pending = _find_kind(ops, "plex_enum_pending")
    assert pending["op_id"] == "queue:plex_enum_pending"


# ── ops.js KIND_LABEL + TONE_BY_KIND wiring ──────────────────

def test_ops_js_has_plex_enum_pending_kind_label():
    """ops.js KIND_LABEL must have an entry for plex_enum_pending
    so the drawer card renders 'PLEX REFRESH (QUEUED)' instead
    of the bare kind id."""
    src = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    assert "plex_enum_pending:   'PLEX REFRESH (QUEUED)'" in src


def test_ops_js_has_plex_enum_pending_tone():
    """ops.js TONE_BY_KIND must color plex_enum_pending with the
    plex tone so it reads as related to the running PLEX REFRESH
    card (not as a different op family)."""
    src = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    assert "plex_enum_pending:   'plex'" in src


# ── Fix 2: unraid template URLs ──────────────────────────────


def test_unraid_template_uses_release_branch():
    """unraid/motif.xml's <TemplateURL> + <Icon> must point at
    the release branch. Pre-fix they pointed at the deleted main
    branch — the public Unraid template would 404 on auto-update
    + show a broken icon in the apps page."""
    xml = (REPO / "unraid" / "motif.xml").read_text()
    # New: release branch refs.
    assert (
        "<TemplateURL>https://raw.githubusercontent.com/healzangels/motif/release/unraid/motif.xml</TemplateURL>"
        in xml
    )
    assert (
        "<Icon>https://raw.githubusercontent.com/healzangels/motif/release/unraid/icon.png</Icon>"
        in xml
    )


def test_unraid_template_no_main_branch_refs_in_attributes():
    """The deleted main branch must NOT appear in any actual
    <TemplateURL> or <Icon> attribute value (allowed in
    comments documenting the rename)."""
    xml = (REPO / "unraid" / "motif.xml").read_text()
    # Pin the literal attribute forms with .../main/... — these
    # are the 404-producers. Comments mentioning main are fine.
    assert (
        ">https://raw.githubusercontent.com/healzangels/motif/main/"
        not in xml
    )
