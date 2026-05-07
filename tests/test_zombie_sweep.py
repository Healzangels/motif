"""Regression guard for the v1.12.113 zombie running-job sweep SQL.

v1.12.113 introduced a startup pass that flips jobs.status='running'
rows to 'failed' so a SIGKILL / OOM / container-crash mid-job doesn't
leave a ghost row that the dashboard SYNC button + ops mini-bar
mistake for live activity. The original SQL split the explanation
string across two adjacent literals inside a triple-quoted Python
string:

    last_error = COALESCE(last_error,
      'session_expired: motif process restarted '
      'while this job was running')

Python only concatenates adjacent string literals at the *expression*
level, NOT inside a triple-quoted multi-line string — so the two
quoted segments became two consecutive SQL string literals, which is
a SQLite syntax error. The sweep silently failed via the broad
try/except at every startup, leaving any zombie 'running' rows in
place forever. Symptom: dashboard SYNC button stuck on
'// SYNCING THEMERRDB…' indefinitely because
themerrdb_sync_in_flight never dropped to 0.

This test asserts the sweep query is syntactically valid SQL and
actually flips a 'running' row to 'failed' with the expected
last_error text. v1.12.123.
"""
from __future__ import annotations

import sqlite3


def test_zombie_sweep_sql_is_valid_and_flips_running_rows():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE jobs ("
        "  id INTEGER PRIMARY KEY,"
        "  status TEXT,"
        "  finished_at TEXT,"
        "  last_error TEXT"
        ")"
    )
    conn.execute("INSERT INTO jobs (status, last_error) VALUES ('running', NULL)")
    conn.execute("INSERT INTO jobs (status, last_error) VALUES ('running', 'prior')")
    conn.execute("INSERT INTO jobs (status, last_error) VALUES ('success', NULL)")

    # Mirror the exact statement compiled by app/main.py's startup
    # sweep (v1.12.123). If this raises sqlite3.OperationalError, the
    # production sweep will too — and a stuck SYNC button is the
    # downstream symptom.
    conn.execute(
        "UPDATE jobs SET status = 'failed', "
        "                finished_at = ?, "
        "                last_error = COALESCE(last_error, "
        "  'session_expired: motif process restarted while this job was running') "
        "WHERE status = 'running'",
        ("2026-05-05T00:00:00Z",),
    )
    rows = conn.execute(
        "SELECT status, last_error FROM jobs ORDER BY id"
    ).fetchall()
    # Row 1: was running, no prior error → got the canned explanation.
    assert rows[0] == (
        "failed",
        "session_expired: motif process restarted while this job was running",
    )
    # Row 2: was running, had a prior error → COALESCE preserves it.
    assert rows[1] == ("failed", "prior")
    # Row 3: was 'success' → untouched.
    assert rows[2] == ("success", None)


def test_zombie_sweep_against_production_app_main():
    """Defense in depth: re-extract the SQL from app/main.py and run
    it. Catches the case where someone re-introduces the multi-line
    string-literal pattern but the standalone test above still passes
    against a hand-rolled SQL string."""
    import re
    from pathlib import Path

    src = Path(__file__).resolve().parent.parent / "app" / "main.py"
    text = src.read_text()
    # Locate the UPDATE jobs block and pull out the SQL passed to
    # conn.execute. The sweep is the only `UPDATE jobs SET status = 'failed'`
    # in main.py.
    # Walk every conn.execute(...) called with (now_iso(),) and pick
    # the one whose collapsed SQL is the zombie sweep. Robust against
    # other UPDATE-jobs statements main.py may grow over time.
    sql = None
    for m in re.finditer(
        r'conn\.execute\(\s*(.+?)\s*,\s*\(now_iso\(\),\)',
        text,
        re.DOTALL,
    ):
        raw = m.group(1)
        pieces = re.findall(r'"((?:\\.|[^"\\])*)"', raw)
        candidate = "".join(pieces)
        if (
            "UPDATE jobs" in candidate
            and "status = 'failed'" in candidate
            and "WHERE status = 'running'" in candidate
        ):
            sql = candidate
            break
    assert sql, "couldn't locate the zombie-sweep conn.execute in app/main.py"
    assert "UPDATE jobs SET status = 'failed'" in sql

    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE jobs (id INTEGER PRIMARY KEY, status TEXT, "
        "finished_at TEXT, last_error TEXT)"
    )
    conn.execute("INSERT INTO jobs (status) VALUES ('running')")
    conn.execute(sql, ("2026-05-05T00:00:00Z",))
    row = conn.execute("SELECT status FROM jobs").fetchone()
    assert row == ("failed",)
