"""v1.14.38 — deadcode sweep across frontend audit findings.

Five YAGNI cleanups bundled into one tag:

  • M3: `_enqueueing` Set typo — `'restore'` → `'restore-canonical'`
        (REAL BUG: boostPoll never fired for RESTORE FROM PLEX
        clicks, so the ops mini-bar took up to 10s to surface)
  • L1: STAGE_TIMELINE_QUEUE const + empty forEach in ops.js
  • L2: dead `data-act === 'clear-failure'` row branch in app.js
        (no template emits it post-v1.12.87)
  • L4: unused `settingsRefreshBusy` const + acknowledgment block
        (consumer retired in v1.13.63 reorg)
  • L6: local `fmt` shadow in renderSyncHistory shadows module-
        level `fmt` helpers object — renamed to `fmtN`

All changes from the AUDIT_FRONTEND.md "Risk-prioritized fix
order" steps 5-6 (M3 + L1+L2+L4+L6 cleanup batch).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── M3: _enqueueing Set typo ─────────────────────────────────


def test_enqueueing_set_uses_correct_restore_canonical_action():
    """The _enqueueing Set must include `'restore-canonical'` (the
    real action key from the dispatcher branch + bulk handler) —
    not the pre-fix `'restore'` typo. Pre-fix the boostPoll
    trigger never fired for RESTORE FROM PLEX clicks because the
    Set lookup quietly missed."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The Set definition.
    set_anchor = js.index("const _enqueueing = new Set([")
    set_block = js[set_anchor:set_anchor + 600]
    # The corrected token.
    assert "'restore-canonical'" in set_block
    # The pre-fix token (bare 'restore') must not survive — it
    # was never a real action key; ensure it didn't sneak back.
    # The Set lists tokens comma-separated so we look for the
    # literal "'restore'," substring (with trailing comma).
    assert "'restore'," not in set_block, (
        "Pre-fix typo `'restore'` survives in _enqueueing — "
        "boostPoll won't fire for RESTORE FROM PLEX. The real "
        "action key is `'restore-canonical'` (audit M3)."
    )


def test_enqueueing_v1_14_38_marker_present():
    """The archaeology comment captures the typo + dispatcher
    cross-reference. Pin so a future refactor that "shortens"
    the Set entries can't drop the correct token by mistake."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "v1.14.38: 'restore' was a typo" in js
    # References the real dispatcher branch line for traceability.
    assert "'restore-canonical'" in js


def test_restore_canonical_is_a_real_dispatcher_branch():
    """Sanity: the dispatcher actually has an `act ===
    'restore-canonical'` branch (the audit's claim about the
    real action key). If the dispatcher disappeared, the
    boostPoll fix would be moot."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "act === 'restore-canonical'" in js


# ── L1: STAGE_TIMELINE_QUEUE deadcode in ops.js ──────────────


def test_stage_timeline_queue_deadcode_removed():
    """The unused const + empty forEach are gone. v1.14.38
    marker references the cleanup so a refactor reading the
    archaeology can see why the const isn't there."""
    js = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    # The pre-fix const is gone.
    assert "const STAGE_TIMELINE_QUEUE = [];" not in js
    # The pre-fix empty-body forEach is gone.
    assert "['refresh_queue', 'relink_queue', 'adopt_queue'].forEach" not in js
    # And the v1.14.38 marker explains why.
    assert "v1.14.38: dropped STAGE_TIMELINE_QUEUE" in js


def test_stage_timeline_map_still_referenced():
    """STAGE_TIMELINE (the live map) MUST stay — it's used by
    the renderer at line ~215. Pin so the deadcode sweep didn't
    over-correct and drop the live const too."""
    js = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    # The const definition.
    assert "const STAGE_TIMELINE = {" in js
    # The consumer.
    assert "STAGE_TIMELINE[op.kind]" in js


# ── L2: dead `clear-failure` row branch ──────────────────────


def test_dead_row_clear_failure_branch_removed():
    """The unreachable `act === 'clear-failure'` branch in the
    library-body row click handler is gone. Per v1.12.87 the
    ACK FAILURE button was moved to the INFO card's TRY THIS
    NEXT section — no row template emits this data-act."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Only ONE `act === 'clear-failure'` remains — the recovery-
    # options dispatcher one (~line 9522). The pre-fix row
    # handler at ~8471 is gone.
    n = js.count("act === 'clear-failure'")
    assert n == 1, (
        f"Expected exactly 1 `act === 'clear-failure'` branch "
        f"(recovery dispatcher only), found {n}. "
        "If 2+, the deadcode sweep didn't drop the row branch."
    )
    # The v1.14.38 marker references the v1.12.87 reorg.
    assert "v1.14.38: dropped row-handler `clear-failure`" in js


def test_no_template_emits_data_act_clear_failure():
    """Sanity: confirm no row template emits `data-act=
    "clear-failure"` — that's the precondition for the L2
    cleanup. If a template starts emitting it, the dispatcher
    branch needs to come back too. Templates only — JS may
    mention the string in comments / dispatcher branches; the
    contract is about DOM emission."""
    templates = list((REPO / "app" / "web" / "templates").glob("*.html"))
    template_text = "".join(t.read_text() for t in templates)
    assert 'data-act="clear-failure"' not in template_text


# ── L4: unused settingsRefreshBusy ──────────────────────────


def test_settings_refresh_busy_const_removed():
    """The const went unused after v1.13.63 retired the
    settings #refresh-libraries-btn. Audit L4 cleanup."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const settingsRefreshBusy = plexEnumBusy" not in js
    # The v1.14.38 marker explains where the consumer went.
    assert "v1.14.38: dropped unused `settingsRefreshBusy`" in js


def test_settings_refresh_busy_acknowledgment_block_dropped():
    """The matching acknowledgment comment block at ~line 840
    that said "still exists in scope; cheap to leave" is also
    gone — no point claiming "cheap to leave" when we just
    dropped it."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "settingsRefreshBusy variable still exists in scope" not in js


# ── L6: fmt shadow rename ────────────────────────────────────


def test_render_sync_history_no_longer_shadows_module_fmt():
    """The local `fmt` const inside renderSyncHistory was renamed
    to `fmtN` so it doesn't shadow the module-level `fmt` helpers
    object. Pin both: the new name is present + the pre-fix
    shadow is gone."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function renderSyncHistory(payload)")
    body = js[fn_anchor:fn_anchor + 3000]
    # New name.
    assert "const fmtN =" in body
    # Pre-fix shadow gone.
    assert "const fmt = (n) =>" not in body
    # Consumers updated to fmtN.
    assert "fmtN(r.new_count)" in body
    assert "fmtN(r.updated_count)" in body


def test_module_level_fmt_helpers_still_present():
    """Sanity pin: the module-level `fmt` helpers object that the
    L6 fix exists to protect is still defined at the top of the
    file. If `fmt = {...}` ever goes away, the rename is
    pointless."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The first 500 chars should hold the module-level fmt.
    head = js[:500]
    assert "const fmt = {" in head
