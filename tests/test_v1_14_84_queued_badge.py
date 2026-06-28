"""v1.14.84 — replace inline (N of M) suffix with prominent +N QUEUED badge.

the user: "is that what the 1 of 3 meant? I thought it was saying
1 of 3 tasks for movies section."

The v1.13.27 inline `(N of M)` suffix on the topbar mini-bar
label was meant to signal "this job is N of M in the queue
burst" — but attached to a section name (`MOVIES (upsert)
(1 of 3)`) it read naturally as per-section phase progress.
the user's misread is the bug.

## Fix (Option B from the proposal)

Drop the inline suffix. Repurpose the v1.13.45-hidden
`#op-mini-overflow` pill as a sibling badge that reads
`+N QUEUED` in plex tone, only when there's a real queue
(post-v1.14.77: pending+running >= 2 OR pending >= 2). The
badge sits next to the mini-bar so the queue-depth signal
can't be misread as section-internal phase progress.

Plex tone matches the drawer's `PLEX REFRESH (QUEUED)` card
so the queued-state visual identity is consistent across
surfaces.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core import db as core_db  # noqa: E402
from app.core.progress import load_active  # noqa: E402

OPS_JS = REPO / "app" / "web" / "static" / "ops.js"
PROGRESS_PY = REPO / "app" / "core" / "progress.py"


# ── Server: synth row carries detail.queue_depth ──────────────


def _seed_jobs(db_path: Path,
               pending: int = 0,
               running: int = 0) -> None:
    with sqlite3.connect(db_path) as conn:
        for _ in range(pending):
            conn.execute(
                "INSERT INTO jobs (job_type, status, payload, "
                " created_at, next_run_at) "
                "VALUES ('plex_enum', 'pending', '{}', "
                " '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
            )
        for _ in range(running):
            conn.execute(
                "INSERT INTO jobs (job_type, status, payload, "
                " created_at, next_run_at, started_at) "
                "VALUES ('plex_enum', 'running', '{}', "
                " '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z', "
                " '2025-01-01T00:00:00Z')"
            )
        conn.commit()


def test_synth_row_carries_queue_depth_in_detail(tmp_path):
    """The plex_enum_pending synth row's detail must include
    a numeric queue_depth field. The client topbar reads this
    to render the +N QUEUED badge — pre-fix it would have to
    parse the stage_label string ('N library refreshes
    queued') with a fragile regex."""
    db = tmp_path / "motif.db"
    core_db.init_db(db)
    _seed_jobs(db, pending=3, running=1)
    rows = load_active(db)
    synth = next(r for r in rows
                 if r.get("kind") == "plex_enum_pending")
    # v1.15.2: detail also includes synthetic=True (suppresses
    # the dead CANCEL button on the drawer card). Check
    # queue_depth specifically rather than the whole dict.
    assert synth["detail"]["queue_depth"] == 3
    assert synth["detail"].get("synthetic") is True


def test_synth_row_queue_depth_matches_pending_count(tmp_path):
    """queue_depth equals the COUNT of pending plex_enum jobs
    (NOT pending + running). The badge labels "queued" jobs
    so it shouldn't include the currently-running one."""
    db = tmp_path / "motif.db"
    core_db.init_db(db)
    _seed_jobs(db, pending=5, running=1)
    rows = load_active(db)
    synth = next(r for r in rows
                 if r.get("kind") == "plex_enum_pending")
    assert synth["detail"]["queue_depth"] == 5


def test_no_synth_means_no_queue_depth_signal(tmp_path):
    """Single-pending-no-running case (the v1.14.77 suppression
    branch) must NOT emit the synth — and therefore no
    queue_depth signal. The mini-bar shows nothing in the
    overflow slot."""
    db = tmp_path / "motif.db"
    core_db.init_db(db)
    _seed_jobs(db, pending=1, running=0)
    rows = load_active(db)
    assert not any(r.get("kind") == "plex_enum_pending"
                   for r in rows)


def test_v1_14_84_marker_explains_queue_depth_field():
    """A v1.14.84 marker on the synth row's detail block
    explains why queue_depth is structured (not parsed from
    the label)."""
    py = PROGRESS_PY.read_text()
    assert "v1.14.84: structured pending count" in py


# ── Client: badge logic in renderTopbar ───────────────────────


def _render_topbar() -> str:
    src = OPS_JS.read_text()
    fn_start = src.index("function renderTopbar(ops) {")
    next_fn = src.index("\n  function ", fn_start + 30)
    return src[fn_start:next_fn]


def test_inline_n_of_m_suffix_retired():
    """The v1.13.27 inline `(N of M)` suffix on the mini-bar
    label is gone — that's the literal source of the user's
    misread."""
    body = _render_topbar()
    # The labelText assignment is the simple form (no suffix
    # mutation around it).
    assert "(${position} of ${q.hw})" not in body
    # The position computation is gone too.
    assert "Math.max(1, q.hw - q.current + 1)" not in body


def test_overflow_pill_renders_queued_badge_when_synth_present():
    """When the plex_enum_pending synth row is in ops, the
    #op-mini-overflow pill must show `+N QUEUED` text using
    the count from detail.queue_depth.
    v1.15.30 generalized the QUEUED label by appending an
    optional `${probeSuffix}` (composes a concurrent probe
    indicator into the same pill) — the literal "QUEUED" still
    leads, the suffix is just appended."""
    body = _render_topbar()
    # Find the synth row by kind.
    assert "ops.find(" in body
    assert "o.kind === 'plex_enum_pending'" in body
    # Read queue_depth from detail.
    assert "pendingSynth.detail.queue_depth" in body
    # +N QUEUED label form (count in op-pill-count span, label
    # in op-pill-label). v1.15.30: label is now `QUEUED${probeSuffix}`.
    assert "+${queueDepth}</span>" in body
    assert ">QUEUED${probeSuffix}</span>" in body


def test_overflow_pill_uses_plex_tone():
    """The badge tone must be op-tone-plex (green) — same
    family as the drawer's PLEX REFRESH (QUEUED) card so the
    queued-state visual identity is consistent across
    surfaces. The template's static op-tone-warn must be
    overridden via JS each render."""
    body = _render_topbar()
    assert "overflow.className = 'op-pill op-tone-plex';" in body


def test_overflow_pill_hides_when_no_queue():
    """When queue_depth is 0 (no synth row OR detail missing),
    the pill must hide. Otherwise a stale "+0 QUEUED" badge
    would persist after the queue drains."""
    body = _render_topbar()
    # The hide branch.
    assert "overflow.hidden = true;" in body
    # The show branch is gated on queueDepth > 0.
    assert "if (queueDepth > 0)" in body


def test_overflow_pill_tooltip_explains_the_count():
    """A title attribute on the pill spells out what the
    number means — defends against future "is that 3 sections
    or 3 jobs?" misreads."""
    body = _render_topbar()
    # Tooltip contains the count + 'queued behind the running one'.
    assert "queued behind the running one" in body


def test_v1_14_84_marker_explains_badge_repurpose():
    """A v1.14.84 marker on the renderTopbar overflow block
    explains the repurpose (was hidden in v1.13.45, now used
    as the queued badge)."""
    body = _render_topbar()
    assert "v1.14.84: repurpose the v1.13.45-hidden" in body
