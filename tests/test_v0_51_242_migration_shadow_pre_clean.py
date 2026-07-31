"""v0.51.242 — a table-rebuild shadow must not survive an interrupted migration.

The rebuild idiom is CREATE TABLE X_new -> INSERT SELECT -> DROP X -> RENAME.
If the process dies between the CREATE and the RENAME and that shadow COMMITS,
the retry on the next boot dies on "table X_new already exists" — init_db raises,
so motif fails to start on every restart until someone drops the table by hand.

Whether the shadow commits depends entirely on whether the block is wrapped in
BEGIN/COMMIT. Measured (see test_transactional_blocks_roll_back_a_shadow):

  * BEGIN-wrapped   -> the shadow rolls back. All 10 op_progress rebuilds are
                       wrapped, so the audit item "op_progress rebuild
                       shadow-table pre-clean" was a FALSE POSITIVE.
  * not wrapped     -> the shadow survives. Three older blocks
                       (_migrate_v4_to_v5 themes_new + jobs_new,
                       _migrate_v6_to_v7 jobs__new) were in this state.

Those three now pre-clean, mirroring the v1.19.73 pre-clean in
_widen_check_constraint. The lint below keeps every FUTURE rebuild honest
without anyone having to remember the rule.
"""
from __future__ import annotations

import re
import sqlite3
import tempfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
DB_PY = (REPO / "app" / "core" / "db.py").read_text()


# ── the mechanism, measured rather than assumed ──────────────────────────

def _shadow_survives(script: str) -> bool:
    """Run a partial rebuild, then abandon the connection without committing —
    the closest in-process analogue of the container being killed."""
    d = Path(tempfile.mkdtemp()) / "t.db"
    c = sqlite3.connect(d)
    c.execute("CREATE TABLE t (a TEXT)")
    c.commit()
    try:
        c.executescript(script)
    except Exception:
        pass
    c.close()
    c2 = sqlite3.connect(d)
    try:
        return c2.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='t_new'"
        ).fetchone() is not None
    finally:
        c2.close()


def test_transactional_blocks_roll_back_a_shadow():
    """Why the 10 op_progress rebuilds need no pre-clean."""
    assert _shadow_survives("BEGIN;\nCREATE TABLE t_new (a TEXT);\n") is False


def test_non_transactional_blocks_leak_a_shadow():
    """Why the three old blocks did."""
    assert _shadow_survives("CREATE TABLE t_new (a TEXT);\n") is True


def test_a_stale_shadow_really_does_break_init_db():
    """The consequence, end to end: not a cosmetic leftover — init_db RAISES,
    which means the container fails to boot."""
    from app.core.db import init_db
    d = Path(tempfile.mkdtemp()) / "m.db"
    init_db(d)
    with sqlite3.connect(d) as c:
        c.execute("DELETE FROM schema_version")
        c.execute("INSERT INTO schema_version (version, applied_at) "
                  "VALUES (46, datetime('now'))")
        c.execute("CREATE TABLE op_progress_new (op_id TEXT PRIMARY KEY)")
        c.commit()
    with pytest.raises(sqlite3.OperationalError, match="already exists"):
        init_db(d)


# ── the standing lint ────────────────────────────────────────────────────

def _rebuild_sites():
    """(line, shadow_name, enclosing executescript block) for every hand-written
    table rebuild in db.py."""
    out = []
    for m in re.finditer(r"CREATE TABLE (\w+_+new)\s*\(", DB_PY):
        start = DB_PY.rfind('executescript("""', 0, m.start())
        if start == -1:
            continue
        block = DB_PY[start:DB_PY.find('""")', start)]
        line = DB_PY[:m.start()].count("\n") + 1
        out.append((line, m.group(1), block))
    return out


def test_every_rebuild_is_either_transactional_or_pre_cleaned():
    """The durable guard. A new rebuild must either sit inside BEGIN/COMMIT (so
    an interrupted run rolls back) or DROP its shadow first. Doing neither
    reintroduces a boot-blocking crash-loop."""
    sites = _rebuild_sites()
    assert sites, "expected to find rebuild sites — did the idiom change?"
    unsafe = [
        (line, name) for line, name, block in sites
        if "BEGIN;" not in block and f"DROP TABLE IF EXISTS {name};" not in block
    ]
    assert not unsafe, (
        "table rebuilds that neither roll back nor pre-clean their shadow "
        f"(an interrupted migration would block every boot): {unsafe}")


def test_the_three_known_non_transactional_blocks_pre_clean():
    """Explicit, so a refactor that drops the pre-clean is caught by name and
    not just by the general lint."""
    for name in ("themes_new", "jobs_new", "jobs__new"):
        blocks = [b for _, n, b in _rebuild_sites()
                  if n == name and "BEGIN;" not in b]
        for b in blocks:
            assert f"DROP TABLE IF EXISTS {name};" in b, (
                f"{name} rebuild is not transactional and no longer pre-cleans")
