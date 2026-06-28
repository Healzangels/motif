"""v1.22.78 (audit round 2, Batch C #7) — recovery telemetry + orphan
mint atomicity.

(1) recovery_v55's rebuild loops gated their counters on
`conn.total_changes` — CUMULATIVE for the connection's lifetime, so
after the first real insert every OR IGNORE skip also counted. A
re-run on a half-recovered DB reported "5000 local_files rebuilt"
when 1 inserted, and constraint-rejected rows (swallowed by
OR IGNORE) were never distinguishable — wrong operator-facing
telemetry on the flagship data-loss recovery path (the v1.18.5
cold-path-needs-accurate-signal class). Now `cur.rowcount`, like
every other counter in the file.

(2) adopt._create_orphan_theme claimed its synthetic-id mint was
"atomic via single transaction" but ran SELECT-MIN then INSERT as two
independent autocommit statements — a concurrent inline ADOPT +
worker adopt could read the same MIN and the loser 500'd on the
UNIQUE. The mint now happens INSIDE the INSERT (one statement =
atomic even on an autocommit conn); the real-tmdb path's
check→INSERT race now lands on ON CONFLICT DO NOTHING + re-read
instead of an uncaught IntegrityError.
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from app.core.adopt import _create_orphan_theme
from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
RECOVERY_PY = (REPO / "app" / "core" / "recovery_v55.py").read_text()
ADOPT_PY = (REPO / "app" / "core" / "adopt.py").read_text()


# ── (1) recovery counters ────────────────────────────────────


def test_recovery_counters_use_cursor_rowcount():
    assert "conn.total_changes" not in RECOVERY_PY, (
        "v1.22.78: total_changes is cumulative — every OR IGNORE skip "
        "counted as an insert after the first real one"
    )
    assert RECOVERY_PY.count("if cur.rowcount:") >= 2


def test_total_changes_vs_rowcount_demo(tmp_path):
    """Executable proof of the mechanism: total_changes stays truthy
    for OR IGNORE skips once anything inserted; rowcount doesn't."""
    conn = sqlite3.connect(tmp_path / "x.db")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO t VALUES (1)")
    cur = conn.execute("INSERT OR IGNORE INTO t VALUES (1)")
    assert conn.total_changes > 0, "the bug: cumulative count is truthy"
    assert cur.rowcount == 0, "the fix: per-statement count is honest"
    conn.close()


# ── (2) orphan mint atomicity ────────────────────────────────


def _finding(*, tmdb_id=None):
    meta = {"title": "Orphan", "year": "2020"}
    if tmdb_id:
        meta["tmdb_id"] = tmdb_id
    import json
    return {"resolved_metadata": json.dumps(meta),
            "section_type": "movie"}


def test_sequential_mints_descend(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    _, t1 = _create_orphan_theme(db, _finding(), "admin")
    _, t2 = _create_orphan_theme(db, _finding(), "admin")
    assert (t1, t2) == (-1, -2)


def test_concurrent_mints_never_collide(tmp_path):
    """Pre-fix: both threads read MIN before either inserted → same id
    → the loser raised IntegrityError. Single-statement mint can't."""
    db = tmp_path / "m.db"
    init_db(db)
    results: list[int] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def _mint():
        try:
            _, tid = _create_orphan_theme(db, _finding(), "admin")
            with lock:
                results.append(tid)
        except Exception as e:  # noqa: BLE001
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=_mint) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    assert len(set(results)) == 4, results


def test_real_tmdb_check_insert_race_lands_on_conflict_do_nothing(tmp_path):
    """Sequential idempotency + the source pin for the lost-race shape."""
    db = tmp_path / "m.db"
    init_db(db)
    id1, t1 = _create_orphan_theme(db, _finding(tmdb_id=603), "admin")
    id2, t2 = _create_orphan_theme(db, _finding(tmdb_id=603), "admin")
    assert (t1, t2) == (603, 603)
    assert id1 == id2
    i = ADOPT_PY.index("def _create_orphan_theme(")
    body = ADOPT_PY[i:i + 6000]
    assert "ON CONFLICT(media_type, tmdb_id) DO NOTHING" in body
    # The two-statement mint shape must be gone.
    assert "SELECT MIN(tmdb_id) AS lo FROM themes" not in body
