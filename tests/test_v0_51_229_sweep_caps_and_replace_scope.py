"""v0.51.229 — audit wave 2: amplifier-sweep caps + /replace section scope.

Two findings from the v0.51.228 holistic sweep, both data-integrity:

  1. sync.py's two destructive `DELETE ... WHERE NOT EXISTS` sweeps
     (_sweep_orphan_user_overrides, _prune_stale_pending_updates) had NO abort cap and
     run on EVERY sync including no-change ones. This is the v1.18.10 class by name:
     v1.18.0 wiped local_files + placements, then the next sync's orphan sweep found 98
     user_overrides whose presence-EXISTS checks all failed and DELETEd those too. Every
     sibling sweep got a guard (deorphan _MERGE_ABORT_CAP, the v1.18.89 reaper,
     recovery_v55, worker); these two never did. pending_updates is worse — it has NO
     recovery walker at all, so a mass-prune is unrecoverable.

     The guard measures the DELETE's OWN rowcount inside a transaction and rolls back,
     so the check and the delete can never drift onto different predicates.

  2. api_replace_item (PUSH TO PLEX) never DECLARED section_id, so FastAPI silently
     dropped the ?section_id=... app.js has always sent — the worklist was edition-scoped
     but SECTION-WIDE, fanning a force-place across every section holding a local_files
     row. On a row left in LET PLEX SERVE elsewhere that silently re-installed a sidecar.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from app.core import sync
from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-07-27T00:00:00"


def _db_with_orphan_overrides(n: int) -> Path:
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys=OFF")
        for i in range(n):
            c.execute(
                "INSERT INTO user_overrides (media_type, tmdb_id, section_id, "
                " edition_key, youtube_url, set_at) VALUES ('movie',?,'1','',?,?)",
                (i, f"https://x/{i}", NOW))
        c.commit()
    return db


# ── 1: the amplifier caps ─────────────────────────────────────────────────────

def test_routine_cleanup_still_deletes():
    """The cap must not break the sweep's actual job. A handful of genuinely-orphaned
    overrides (the post-PURGE case) is normal cleanup and must still be removed."""
    db = _db_with_orphan_overrides(5)
    assert sync._sweep_orphan_user_overrides(db) == 5
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT COUNT(*) FROM user_overrides").fetchone()[0] == 0


def test_mass_delete_is_refused_and_rolled_back():
    """The v1.18.10 scenario: broken state makes every presence-EXISTS fail, so the sweep
    would take the WHOLE table. It must abort and leave every row intact."""
    db = _db_with_orphan_overrides(400)
    assert sync._sweep_orphan_user_overrides(db) == 0, "must report nothing deleted"
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT COUNT(*) FROM user_overrides").fetchone()[0] == 400, (
            "the transaction must ROLL BACK — a partial mass-delete is the worst outcome")


def test_the_cap_needs_both_absolute_and_proportional_signals():
    """Guard the guard: a big table losing a few rows, and a tiny table losing all of
    them, are both routine. Only large-AND-proportional trips the abort."""
    assert sync._abort_if_amplified("t", 5, 5) is False, "tiny table, all rows = routine"
    assert sync._abort_if_amplified("t", 50, 1000) is False, "under the row floor"
    assert sync._abort_if_amplified("t", 100, 1000) is False, "10% is not a mass-delete"
    assert sync._abort_if_amplified("t", 400, 1000) is True, "40% of 1000 rows = abort"


def test_both_sweeps_are_capped():
    """pending_updates especially — unlike user_overrides (rebuildable from the events
    log by the v1.18.10/v1.18.83 walkers) it has NO recovery path, so a mass-prune loses
    every un-actioned ACCEPT / KEEP CURRENT decision for good."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    for fn in ("_sweep_orphan_user_overrides", "_prune_stale_pending_updates"):
        body = src[src.index(f"def {fn}("):src.index("\ndef ", src.index(f"def {fn}(") + 1)]
        assert "_abort_if_amplified(" in body, f"{fn} has no amplifier cap"
        assert "with transaction(conn):" in body, (
            f"{fn} must delete inside a txn so the cap can roll back")


# ── 2: /replace must honour the section the caller named ──────────────────────

def _replace_body() -> str:
    i = API_PY.index("async def api_replace_item(")
    return API_PY[i:API_PY.index("\n    @app.", i)]


def test_replace_declares_section_id():
    """app.js sends ?section_id=... — an undeclared param is silently DROPPED by FastAPI,
    which is why this went unnoticed (the frontend believed it was scoped)."""
    body = _replace_body()
    assert "section_id: str | None = Query(None)" in body


def test_replace_scopes_its_worklist_and_placement_writes_to_that_section():
    """Three statements decide the blast radius: the section worklist, the
    existing-placements read, and the placements DELETE. All must narrow."""
    body = _replace_body()
    assert body.count("_repl_sec_clause") >= 4, (
        "the section clause must be built AND applied at the worklist SELECT, the "
        "existing_placements SELECT, and the DELETE FROM placements")
    assert '_repl_sec_clause = " AND section_id = ?" if section_id else ""' in body
    # every application must pair the clause with its param, or SQLite binds the wrong slot
    assert body.count("+ _repl_ed_clause + _repl_sec_clause,") == 3
    assert body.count("+ _repl_ed_params + _repl_sec_params,") == 3


def test_replace_stays_section_wide_when_no_section_named():
    """Legacy/unscoped callers (no section_id) must behave exactly as before — the clause
    collapses to empty, so this is a pure narrowing, never a behavior change for them."""
    body = _replace_body()
    assert 'if section_id else ""' in body
    assert "if section_id else ()" in body
