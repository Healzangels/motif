"""v1.23.32 — the opening fetch/connect stage SAYS what it's doing.

the user: kicking off a ThemerrDB sync / Plex refresh, the first step had no % and
"doesn't really indicate anything", then the next step showed detail + %. Cause:
the opening stage (the %-less fetch — git mirror pull / paged Plex enumerate,
stage_total=0 → indeterminate shimmer) carried a label but pushed NO activity-
feed line, so it read as a sparse step before the first detailed one.

Fix: start_progress gains an `activity` param that seeds the first feed line, and
both ops pass an action-oriented label + a working line. The indeterminate
shimmer (existing) still conveys "active".
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from app.core import progress
from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent


def _detail(db, op_id="tdb_sync"):
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT stage_label, detail_json FROM op_progress WHERE op_id=?",
            (op_id,)).fetchone()
    return row[0], json.loads(row[1])


def test_start_progress_seeds_activity_line(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    progress.start_progress(
        db, op_id="tdb_sync", kind="tdb_sync", stage="index",
        stage_label="Fetching latest from ThemerrDB",
        activity="Connecting to ThemerrDB…")
    label, detail = _detail(db)
    assert label == "Fetching latest from ThemerrDB"
    assert detail["activity"] and detail["activity"][0]["msg"] == "Connecting to ThemerrDB…"
    assert detail["activity"][0]["ts"]  # timestamped like every feed line


def test_start_progress_without_activity_is_empty(tmp_path):
    # backward compat: omitting activity leaves the feed empty (prior behavior).
    db = tmp_path / "m.db"
    init_db(db)
    progress.start_progress(db, op_id="plex_enum", kind="plex_enum",
                            stage="enumerate", stage_label="x")
    _, detail = _detail(db, "plex_enum")
    assert detail["activity"] == []


def test_sync_and_enum_seed_an_opening_working_line():
    sync_src = (REPO / "app" / "core" / "sync.py").read_text()
    enum_src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    i = sync_src.index('op_id="tdb_sync"')
    sync_block = sync_src[i:i + 400]
    assert 'stage_label="Fetching latest from ThemerrDB"' in sync_block
    assert "activity=" in sync_block
    j = enum_src.index('op_id="plex_enum"')
    enum_block = enum_src[j:j + 400]
    assert 'stage_label="Fetching libraries from Plex"' in enum_block
    assert "activity=" in enum_block
