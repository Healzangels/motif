"""v1.19.36 — MEDIUM cleanup pass closing the post-v1.19.35 audit.

Six small drift fixes:

  1. CSS: `.split-half > .block-head + .jobs-scroll-x` was dead
     after v1.19.25 inserted `.scroll-chevron-wrap` between the
     siblings. Adjacency broken → rule never matched. Retargeted
     to the wrap.
  2. CSS: `.scroll-chevron-wrap {}` had zero declarations after
     v1.19.27 dropped the `position: relative`. Phantom rule
     collapsed; rationale moved to a doc-block above the
     pseudos so archaeology still finds it.
  3. queue.html: chips-bar was nested INSIDE the RIGHT pane's
     `.scroll-chevron-wrap` (v1.19.26 height-equalization hack).
     v1.19.27 made that vestigial by switching the chevron
     anchor to .split-half. v1.19.36 restored DOM symmetry —
     chips-bar back outside the wrap on both panes.
  4. Worker rollback: INSERT INTO user_overrides omitted the
     `intent` column → ON CONFLICT UPDATE used schema default
     ('replace'). A failed ACCEPT UPDATE / REVERT rollback on a
     previously-intent='backup' override silently flipped it to
     'replace', which would force-place on the next worker tick.
     Writer side now stamps `prior_intent` into the rollback
     dict; worker side propagates it through the INSERT.
  5. Walker (recovery_v55 maybe_repair_stale_plex_cache_placements):
     enqueues `{"kind":"file","force":true}` for collection rows
     too, but `_do_place` routes collections to
     `_do_place_collection` (API upload) ignoring kind. Behavior
     was correct; just add a per-row INFO log so plex_upload
     jobs originating from this walker are traceable.
  6. DESIGN_SYSTEM.md: documented the v1.19.25 reversal of the
     "no `<label for=>` around file inputs" rule.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
QUEUE_HTML = (REPO / "app" / "web" / "templates" / "queue.html").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
RECOVERY_PY = (REPO / "app" / "core" / "recovery_v55.py").read_text()
DESIGN_MD = (REPO / "docs" / "DESIGN_SYSTEM.md").read_text()


# ── 1: dead CSS rule retargeted ──────────────────────────────


# ── 2: empty .scroll-chevron-wrap rule collapsed ─────────────


# ── 3: chevron-wrap nesting symmetry ─────────────────────────


# ── 4: worker rollback preserves intent ──────────────────────


def test_worker_accept_update_rollback_writes_intent():
    """The ACCEPT UPDATE rollback branch must INSERT user_overrides with
    the intent column populated from rollback.prior_intent.
    v1.20.12: branch restructured (re-pend unconditional, override-restore
    conditional) — anchor on the branch bounds, not the log text."""
    fn_start = WORKER_PY.index('if kind == "accept_update":')
    body = WORKER_PY[fn_start:WORKER_PY.index('elif kind == "revert":',
                                              fn_start)]
    assert "prior_intent" in body, (
        "v1.19.36: ACCEPT UPDATE rollback must read prior_intent "
        "from the rollback payload"
    )
    assert "VALUES (?, ?, ?, ?, 'system', ?, ?, ?, ?)" in body, (
        "v1.19.36: INSERT must include the intent slot; v1.22.21 added the "
        "edition_key column → 9 placeholders"
    )
    assert "intent = excluded.intent" in body, (
        "v1.19.36: ON CONFLICT UPDATE must propagate intent so "
        "the conflict path also restores it"
    )


def test_worker_revert_rollback_writes_intent():
    """Same fix on the REVERT branch — same drift class."""
    revert_marker = (
        'rollback after REVERT download failed'
    )
    revert_idx = WORKER_PY.index(revert_marker)
    fn_start = WORKER_PY.rfind('elif kind == "revert":', 0, revert_idx)
    body = WORKER_PY[fn_start:revert_idx + 400]
    assert "prior_intent" in body, (
        "v1.19.36: REVERT rollback must also read prior_intent"
    )
    assert "VALUES (?, ?, ?, ?, 'system', ?, ?, ?, ?)" in body  # v1.22.21: +edition_key
    assert "intent = excluded.intent" in body


def test_api_accept_update_stamps_prior_intent_in_rollback():
    """Writer side: api_accept_update must include prior_intent
    in the rollback dict so the worker has data to restore."""
    fn_start = API_PY.index("async def api_accept_update(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    # v1.20.12: rollback is now stamped as a baseline dict + conditional
    # field assignment (so override-less accepts still re-pend), so
    # prior_intent is set via `rollback["prior_intent"] = prior_intent`.
    assert 'rollback["prior_intent"] = prior_intent' in body, (
        "v1.19.36: ACCEPT UPDATE must stamp prior_intent into "
        "the rollback dict so the worker rollback writes it back"
    )


def test_api_revert_stamps_prior_intent_in_rollback():
    """Same on REVERT — writer side."""
    fn_start = API_PY.index("async def api_revert_to_themerrdb(")
    fn_end = API_PY.index("@app.get", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    assert '"prior_intent": prior_intent' in body, (
        "v1.19.36: REVERT must stamp prior_intent into the "
        "rollback dict"
    )


# ── 5: walker collection log ─────────────────────────────────


def test_walker_logs_per_row_enqueue():
    """The v1.19.21 walker must INFO-log each enqueued place job
    so plex_upload jobs traceable back here (collections take
    that path because _do_place_collection ignores kind=file)."""
    fn_start = RECOVERY_PY.index(
        "def maybe_repair_stale_plex_cache_placements("
    )
    next_def = RECOVERY_PY.find("\ndef ", fn_start + 1)
    body = RECOVERY_PY[fn_start:next_def if next_def > 0 else len(RECOVERY_PY)]
    assert "v1.19.21 walker enqueued place" in body, (
        "v1.19.36: walker must log per-row enqueue at INFO"
    )
    assert "_do_place_collection takes API route" in body, (
        "v1.19.36: walker's collection-row note must explain "
        "why kind=file is ignored for collections"
    )


# ── 6: DESIGN_SYSTEM.md documents v1.19.25 reversal ─────────


def test_design_system_documents_label_for_file_reversal():
    """DESIGN_SYSTEM.md § 2 Form layout must reflect the v1.19.25
    reversal of the 'no <label for=> around file inputs' rule."""
    assert "v1.19.25" in DESIGN_MD, (
        "v1.19.36: DESIGN_SYSTEM.md must reference v1.19.25 in "
        "the form-layout section's update note"
    )
    # The new shape: input wrapped INSIDE the label (no for=).
    assert "wrapped INSIDE the label" in DESIGN_MD or \
           "wrap in `<label" in DESIGN_MD, (
        "v1.19.36: design doc must describe the post-v1.19.25 "
        "wrapped-input shape"
    )
