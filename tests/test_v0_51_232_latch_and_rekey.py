"""v0.51.232 — audit wave 5: the ops-drawer latch + the incomplete de-orphan re-key.

  1. ops.js fetchProgress did `return r.json()` inside a try. In an ASYNC function a bare
     `return promise` resolves the outer promise WITH it, so the rejection escapes the
     try/catch entirely. poll() does not guard the await and sets pollInFlight=true +
     clears pollTimer BEFORE it — so one non-JSON 200 (behind NPM + Authentik an expired
     session redirects to an HTML login page, and fetch FOLLOWS redirects so r.ok is true)
     left the latch stuck true with no timer armed: LIVE OPS dead until a page reload.
     poll()'s own v0.51.17 comment asserts "fetchProgress never throws" — `return await`
     makes that true rather than assumed.

  2. deorphan re-keys the theme + 5 FK'd children but never the two NEWER
     (media_type, tmdb_id) tables. They are not FK'd, so nothing cascaded and nothing
     complained — the rows just kept pointing at the retired synthetic id.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
DEORPHAN = (REPO / "app" / "core" / "deorphan.py").read_text()
NOW = "2026-07-27T00:00:00"


def test_fetch_progress_awaits_its_json():
    """Without the await the rejection bypasses the catch and wedges poll()'s latch."""
    i = OPS_JS.index("async function fetchProgress(")
    body = OPS_JS[i:OPS_JS.index("\n  function ", i)]
    assert "return await r.json();" in body
    assert "\n      return r.json();" not in body, (
        "a bare `return promise` in an async fn escapes the enclosing try/catch")


def test_poll_still_relies_on_that_invariant():
    """The latch is set + the timer cleared BEFORE the await, and only reset after it —
    so the no-throw guarantee is load-bearing. If this shape ever changes, the fix above
    needs revisiting (a throw would strand pollInFlight=true with no timer)."""
    i = OPS_JS.index("if (state.pollInFlight) return;")
    block = OPS_JS[i:OPS_JS.index("const data = await fetchProgress();", i)]
    assert "state.pollInFlight = true;" in block
    assert "state.pollTimer = null;" in block


def _tables_carry_the_identity() -> bool:
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    c = sqlite3.connect(db)
    for t in ("notifications", "section_failure_acks"):
        cols = {r[1] for r in c.execute(f"PRAGMA table_info({t})")}
        if not {"media_type", "tmdb_id"} <= cols:
            return False
    return True


def test_the_two_tables_really_are_identity_keyed():
    """Premise for the re-key below — asserted so a schema change fails loudly."""
    assert _tables_carry_the_identity()


def test_promote_path_rekeys_notifications_and_acks():
    """An orphan promoted to its real tmdb must carry the operator's own rows across:
    otherwise the inbox row stays clickable (the drawer gates on media_type && tmdb_id)
    and deep-links to an id with no theme, and a dismissed failure ack stops matching so
    the banner returns."""
    i = DEORPHAN.index("UPDATE themes SET tmdb_id = ? WHERE id = ?")
    block = DEORPHAN[i:DEORPHAN.index("# v0.51.11:", i)]
    assert 'for tbl in ("notifications", "section_failure_acks"):' in block
    assert "UPDATE {tbl} SET tmdb_id = ?" in block


def test_merge_path_repoints_them_before_the_husk_is_dropped():
    """Same rows, other walker. They must be RE-KEYED to the survivor, never deleted —
    they are the operator's notifications and dismissals, not FK-invalid junk."""
    i = DEORPHAN.index("# collision losers left at the old tmdb, then the husk.")
    before = DEORPHAN[max(0, i - 900):i]
    assert 'for tbl in ("notifications", "section_failure_acks"):' in before, (
        "the re-point must happen BEFORE the husk's rows are dropped")
    assert "UPDATE {tbl} SET tmdb_id = ?" in before


def test_they_are_not_added_to_the_predelete_loop():
    """Deliberate asymmetry: the pre-delete clears FK-invalid junk at the TARGET id.
    Deleting the target's notifications/acks there would destroy live inbox rows."""
    i = DEORPHAN.index("# Clear FK-invalid leftovers at the target")
    predelete = DEORPHAN[i:DEORPHAN.index("UPDATE themes SET tmdb_id = ? WHERE id = ?", i)]
    assert "notifications" not in predelete
    assert "section_failure_acks" not in predelete
