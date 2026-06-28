"""v1.15.109 — loadDashboard seq guard + AUDIT_WORKER M4 defensive.

Two unrelated audit-trail fixes bundled.

## loadDashboard seq guard

loadDashboard fans into 4 sequential `await api(...)` calls
(/api/stats → /api/sync/history → /api/sections/coverage →
/api/dashboard/insights → /api/events) with DOM writes between
each. The 30s setInterval poll + sync-watcher's loadDashboard
call (app.js:2062, 2103) + click handlers can fire concurrently.
Each await is a yield point where a fresher loadDashboard could
overtake; without a token check after each await, a stale
response can write to the DOM after the newer one has already
rendered.

Same pattern as v1.13.28 (loadLibrary) + v1.15.108 (loadQueue,
loadLibraries). loadDashboard checks the token after EACH await
because the function isn't a single-write — it fans out across
5 fetches.

## AUDIT_WORKER M4: pending_section_ids.add(sid) in loop

`_do_sync`'s post-sync plex_enum cascade builds
`pending_section_ids` once at the top of the txn and never
updates it as the for-loop INSERTs new cascade jobs. If the
`included` list ever contained the same section_id twice
(schema has section_id PK so unreachable today, but defensive
against a future schema regression), both inserts would fire.

Trivial defensive fix: `pending_section_ids.add(sid)` after each
insert. AUDIT_WORKER.md M4.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
WORKER_PY = REPO / "app" / "core" / "worker.py"


# ── loadDashboard seq guard ─────────────────────────────────

def test_load_dashboard_assigns_seq_token():
    src = APP_JS.read_text()
    fn_start = src.index("async function loadDashboard()")
    # Walk forward to next top-level function for end boundary.
    import re
    m = re.search(r"\n  (?:async )?function [a-zA-Z_]",
                  src[fn_start + 1:])
    fn_body = src[fn_start:fn_start + 1 + m.start()] if m else src[fn_start:]
    assert "loadDashboard._seq" in fn_body
    assert "_myToken = loadDashboard._seq" in fn_body


def test_load_dashboard_checks_token_after_each_await():
    """The function has 4+ awaits before DOM writes. After each
    await, the seq token must be re-checked so a stale fetch
    result doesn't write to the DOM."""
    src = APP_JS.read_text()
    fn_start = src.index("async function loadDashboard()")
    import re
    m = re.search(r"\n  (?:async )?function [a-zA-Z_]",
                  src[fn_start + 1:])
    fn_body = src[fn_start:fn_start + 1 + m.start()] if m else src[fn_start:]
    # Count seq checks. There are 5 sequential awaits (/api/stats,
    # /api/sync/history, /api/sections/coverage,
    # /api/dashboard/insights, /api/events) so at least 5 token
    # checks are expected.
    n_guards = fn_body.count(
        "if (loadDashboard._seq !== _myToken) return;")
    assert n_guards >= 5, (
        f"v1.15.109: expected ≥5 seq-token guards in loadDashboard "
        f"(one after each await), found {n_guards}"
    )


# ── AUDIT_WORKER M4: pending_section_ids.add(sid) ────────────

def test_do_sync_cascade_updates_pending_section_ids():
    """The post-sync plex_enum cascade loop must add each
    inserted section_id to pending_section_ids so a duplicate
    in `included` doesn't fire two cascade jobs."""
    src = WORKER_PY.read_text()
    # Anchor on the cascade loop.
    fn_start = src.index("def _do_sync(")
    fn_end = src.index("\n    def ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The cascade INSERT + the defensive add must both be present
    # inside the loop body. Anchor on the plex_enum cascade
    # marker — VALUES line splits across raw strings.
    cascade_idx = fn_body.index("'plex_enum'")
    after_insert = fn_body[cascade_idx:cascade_idx + 1500]
    assert "pending_section_ids.add(sid)" in after_insert, (
        "v1.15.109: AUDIT_WORKER M4 — `_do_sync`'s cascade loop "
        "must update pending_section_ids with each inserted sid "
        "to prevent duplicate cascade jobs."
    )
