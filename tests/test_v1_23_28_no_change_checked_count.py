"""v1.23.28 — every no-change sync notification shows a "checked" count.

the user's report: a scheduled (cron) sync that found nothing showed a bare
"Motif sync — no changes", while a manual / non-empty sync showed "no changes
(5620 checked)". Cause: a git-differential sync with an EMPTY diff processes 0
items, so total_seen=0 fell into the count-less branch; the manual sync had a
non-empty diff so total_seen was non-zero.

Fix: when new+updated are both 0, render "(N checked)" using total_seen, falling
back to the tracked ThemerrDB catalog size (COUNT(*) FROM themes) when the diff
was empty — so the scheduled "no changes" still confirms WHAT was compared
instead of looking like it might not have run.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


def _title_block() -> str:
    # the no-change title branch: from `if new_count == 0` to the dispatch.
    i = WORKER_PY.index("if new_count == 0 and upd_count == 0:")
    return WORKER_PY[i:WORKER_PY.index('event_kind="sync_completed"', i)]


def test_no_change_title_uses_catalog_count():
    block = _title_block()
    # v1.23.79: the displayed count is ALWAYS the catalog size — the v1.23.28
    # `checked = total_seen` (diff size) is gone; that two-scales-under-one-word
    # jitter was the user's 6193-vs-5618 confusion.
    assert "checked = total_seen" not in block
    assert "SELECT COUNT(*) FROM themes" in block
    # v0.50.72: the count now names its unit — "N ThemerrDB themes checked".
    assert "({checked:,} ThemerrDB themes checked)" in block
    # a zero-catalog fresh install (or an errored sync) degrades to the bare
    # message. v1.24.16: "no catalog changes" (scoped to the TDB catalog diff).
    assert 'else "Motif sync — no catalog changes"' in block


def test_empty_diff_branch_no_longer_shows_countless_no_changes():
    # the old `total_seen == 0 ...: "Motif sync — no changes"` standalone
    # branch (count-less) is gone — every no-change path now computes `checked`.
    block = _title_block()
    assert "total_seen == 0 and new_count == 0" not in block


def test_catalog_count_query_returns_themes_total(tmp_path):
    # behavioral: the exact query the title uses returns the catalog size.
    db = tmp_path / "m.db"
    init_db(db)
    now = now_iso()
    with sqlite3.connect(db) as conn:
        for tid in range(37):
            conn.execute(
                "INSERT INTO themes (media_type, tmdb_id, title, title_norm, "
                "  year, upstream_source, last_seen_sync_at, first_seen_sync_at) "
                "VALUES ('movie', ?, 'T', 't', '2020', 'imdb', ?, ?)",
                (tid, now, now))
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM themes").fetchone()[0]
    assert n == 37
