"""v0.51.104 — daily sweep of orphaned theme.mp3.motif-tmp placement temps.

_safe_link_or_copy writes theme.mp3.motif-tmp then os.replace()s it to
theme.mp3; a crash/restart in that window orphans the temp. Normally the next
placement to that folder cleans it, but a folder that never gets re-placed
(edition drift, a switch to plex_upload) keeps it forever. sweep_stale_
placement_temps walks distinct plex_items.folder_path media folders and unlinks
any theme.mp3.motif-tmp older than the threshold; a daily scheduler job runs it.
"""
from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from app.core.db import init_db
from app.core.plex_enum import sweep_stale_placement_temps

REPO = Path(__file__).resolve().parent.parent
SCHED_SRC = (REPO / "app" / "core" / "scheduler.py").read_text()
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")


def _db(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    return db


def _plex_item(db, *, rk, section, folder):
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            " guid_tmdb, title, edition_key, has_theme, folder_path, "
            " first_seen_at, last_seen_at) "
            "VALUES (?, ?, 'movie', ?, ?, '', 0, ?, ?, ?)",
            (rk, section, int(rk), f"m{rk}", folder, NOW, NOW),
        )
        c.commit()


def _tmp_in(folder: Path, *, age_secs: float) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    t = folder / "theme.mp3.motif-tmp"
    t.write_bytes(b"x" * 16)
    old = time.time() - age_secs
    os.utime(t, (old, old))
    return t


def test_sweep_removes_stale_keeps_fresh_and_untouched(tmp_path):
    db = _db(tmp_path)
    a = tmp_path / "movies" / "Old {edition-x}"       # stale tmp → delete
    b = tmp_path / "movies" / "Fresh {edition-x}"      # fresh tmp → keep
    c = tmp_path / "movies" / "Themed {edition-x}"     # real theme, no tmp
    stale = _tmp_in(a, age_secs=7200)                  # 2h old > 1h threshold
    fresh = _tmp_in(b, age_secs=60)                    # 1m old < threshold
    c.mkdir(parents=True); (c / "theme.mp3").write_bytes(b"real")
    for i, folder in enumerate((a, b, c), start=1):
        _plex_item(db, rk=str(100 + i), section="1", folder=str(folder))

    removed = sweep_stale_placement_temps(db)
    assert removed == 1
    assert not stale.exists(), "stale tmp must be removed"
    assert fresh.exists(), "a fresh tmp (placement may be mid-flight) must be kept"
    assert (c / "theme.mp3").exists(), "the real theme is untouched"


def test_sweep_scopes_to_given_sections(tmp_path):
    db = _db(tmp_path)
    a = tmp_path / "s1" / "A"
    d = tmp_path / "s2" / "D"
    ta = _tmp_in(a, age_secs=7200)
    td = _tmp_in(d, age_secs=7200)
    _plex_item(db, rk="201", section="1", folder=str(a))
    _plex_item(db, rk="202", section="2", folder=str(d))

    removed = sweep_stale_placement_temps(db, section_ids=["1"])
    assert removed == 1
    assert not ta.exists(), "scoped section's stale tmp removed"
    assert td.exists(), "out-of-scope section's tmp left alone"


def test_sweep_no_folders_is_noop(tmp_path):
    db = _db(tmp_path)
    assert sweep_stale_placement_temps(db) == 0


def test_daily_scheduler_job_registered():
    assert "def _sweep_placement_temps_job(" in SCHED_SRC
    assert "sweep_stale_placement_temps" in SCHED_SRC
    assert 'id="placement_temp_sweep"' in SCHED_SRC
