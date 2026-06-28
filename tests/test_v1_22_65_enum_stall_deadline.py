"""v1.22.65 (audit round 2, Batch B #1) — the enum's per-folder
timeout was dead code; one stalled mount wedged plex_enum forever.

Phase 1 (sidecar stat) used `as_completed(futures, timeout=None)` +
`fut.result(timeout=30)`. as_completed only yields FINISHED futures,
so result() always returned instantly and the `except _FutTimeout`
arm was unreachable — the v1.11.63 "30s timeout per folder"
protection never worked. A stat hung on a dead NFS/stalled SMB mount
was simply never yielded: the loop (and the executor exit) waited
forever, the enum sat 'running' with no log output — the exact
incident v1.11.63 claims to have fixed.

Fix: a wait()-based NO-PROGRESS deadline (_SIDECAR_STALL_TIMEOUT_S of
zero completions → every remaining folder marked indeterminate,
existing local_theme_file flags preserved, enum continues; the hung
threads are abandoned without joining, logged loudly).
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()
TS = "2026-06-11T00:00:00+00:00"


def _item(rk, folder, has_theme=False):
    from app.core import plex_enum
    return plex_enum.PlexLibraryItem(
        rating_key=rk, section_id="1", media_type="movie",
        title=rk, year="2020", guid_imdb=None, guid_tmdb=None,
        guid_tvdb=None, folder_path=folder, has_theme=has_theme,
        plex_theme_uri="")


def test_stalled_mount_no_longer_wedges_phase1(tmp_path, monkeypatch):
    """One hanging stat + three quick ones: the enum must complete
    within seconds (pre-fix: blocked until the hung stat returned —
    potentially forever), quick folders get their sidecar flag, the
    stalled folder stays at its existing value."""
    from app.core import plex_enum
    monkeypatch.setattr(plex_enum, "_SIDECAR_STALL_TIMEOUT_S", 0.3)
    release = threading.Event()

    def fake_stat(folder_path):
        if "stalled" in folder_path:
            release.wait(30)  # far past the test deadline
            return None
        return True

    monkeypatch.setattr(plex_enum, "stat_theme_sidecar", fake_stat)

    db = tmp_path / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (TS, TS))
        conn.commit()

    items = [
        _item("rk-ok-1", "/data/movies/ok1"),
        _item("rk-ok-2", "/data/movies/ok2"),
        _item("rk-ok-3", "/data/movies/ok3"),
        _item("rk-stall", "/data/movies/stalled"),
    ]
    t0 = time.monotonic()
    plex_enum._upsert_items(db, items, section_id="1")
    elapsed = time.monotonic() - t0
    release.set()  # unhang the abandoned thread so pytest exits clean

    assert elapsed < 10, (
        f"v1.22.65: a stalled stat must not wedge the enum — took "
        f"{elapsed:.1f}s (pre-fix: forever)"
    )
    with sqlite3.connect(db) as conn:
        flags = {r[0]: r[1] for r in conn.execute(
            "SELECT rating_key, local_theme_file FROM plex_items")}
    assert flags["rk-ok-1"] == 1 and flags["rk-ok-2"] == 1
    assert flags["rk-stall"] == 0, (
        "the stalled folder keeps the existing/default flag "
        "(indeterminate), it is not guessed"
    )


def test_dead_as_completed_shape_is_gone():
    """Source pin: the unreachable as_completed + result(timeout) shape
    must not return; the wait()-based deadline replaces it."""
    # Code shapes only (the v1.22.65 comment legitimately describes the
    # removed pattern).
    assert "for fut in as_completed(" not in ENUM_PY
    assert "res = fut.result(timeout=30)" not in ENUM_PY
    assert "_fut_wait(" in ENUM_PY
    assert "_SIDECAR_STALL_TIMEOUT_S" in ENUM_PY
    # The stall path must not re-wedge on executor exit.
    assert "ex.shutdown(wait=not stalled, cancel_futures=stalled)" in ENUM_PY
