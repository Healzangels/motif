"""v1.23.26 — prune superseded sidecar placement records.

the user's live-DB repro: 6 rows (Avatar, Borat, Flow, Zootopia 2, Super Mario
Galaxy, 100 Years of Warner Bros.) each carried TWO placement records — a live
plex_upload (API push, serving the theme) AND a stale hardlink whose theme.mp3
was gone (theme_present=0). The v1.18.59 leftover-file cleanup unlinked the old
theme.mp3 but left the hardlink RECORD, so:
  - it read theme_present=0 → a false red PL dot, and
  - it collided with the plex_upload row in the library's two-tier (p_e/p_g)
    JOIN — both match on (section, edition) ignoring media_folder — a
    dual-placement fan-out that rendered the row cyan or red depending on which
    placement the query surfaced, and kept NEEDS WORK from surfacing it.

Fix: a missing sidecar with a LIVE plex_upload sibling is superseded cruft, not
broken. verify_placement_health PRUNES it (instead of stamping theme_present=0);
a missing sidecar with NO plex_upload sibling is genuinely broken and still
stamps 0. The place path (worker) drops the record at the source too. An
abort-safe cap (class-8) never deletes blind.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso
from app.core.plex_enum import verify_placement_health


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


def _seed_theme(conn: sqlite3.Connection, tmdb_id: int) -> None:
    now = now_iso()
    conn.execute(
        "INSERT OR IGNORE INTO themes (media_type, tmdb_id, title, title_norm, "
        "  year, upstream_source, last_seen_sync_at, first_seen_sync_at) "
        "VALUES ('movie', ?, 'T', 't', '2020', 'imdb', ?, ?)",
        (tmdb_id, now, now))


def _add_placement(conn, tmdb_id, kind, media_folder, edition_key=""):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, "
        "  media_folder, placed_at, placement_kind, provenance) "
        "VALUES ('movie', ?, '1', ?, ?, ?, ?, 'auto')",
        (tmdb_id, edition_key, media_folder, now_iso(), kind))


def _present_folder(tmp_path: Path, name: str) -> str:
    d = tmp_path / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "theme.mp3").write_bytes(b"x")
    return str(d)


def _missing_folder(tmp_path: Path, name: str) -> str:
    d = tmp_path / name
    d.mkdir(parents=True, exist_ok=True)  # folder exists, theme.mp3 does not
    return str(d)


def test_missing_sidecar_with_plex_upload_sibling_is_pruned(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, 700)
        # the dual state: stale hardlink (file gone) + live plex_upload.
        _add_placement(conn, 700, "hardlink", _missing_folder(tmp_path, "f700"))
        _add_placement(conn, 700, "plex_upload", "")
        conn.commit()
    res = verify_placement_health(db)
    assert res["pruned"] == 1
    with sqlite3.connect(db) as conn:
        kinds = [r[0] for r in conn.execute(
            "SELECT placement_kind FROM placements WHERE tmdb_id=700")]
    # the stale hardlink record is gone; the live plex_upload survives.
    assert kinds == ["plex_upload"]


def test_missing_sidecar_without_sibling_stamps_broken_not_pruned(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, 701)
        _add_placement(conn, 701, "hardlink", _missing_folder(tmp_path, "f701"))
        conn.commit()
    res = verify_placement_health(db)
    assert res["pruned"] == 0
    assert res["missing"] == 1
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT placement_kind, theme_present FROM placements "
            "WHERE tmdb_id=701").fetchone()
    # genuinely broken (no API-upload alternative) — kept + stamped 0 so it
    # still surfaces under NEEDS WORK.
    assert row == ("hardlink", 0)


def test_present_sidecar_stamped_healthy(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, 702)
        _add_placement(conn, 702, "hardlink", _present_folder(tmp_path, "f702"))
        conn.commit()
    res = verify_placement_health(db)
    assert res["pruned"] == 0 and res["missing"] == 0
    with sqlite3.connect(db) as conn:
        tp = conn.execute(
            "SELECT theme_present FROM placements WHERE tmdb_id=702").fetchone()[0]
    assert tp == 1


def test_present_sidecar_with_sibling_not_pruned(tmp_path):
    # a sidecar whose file is PRESENT is not cruft even with a plex_upload
    # sibling — only a MISSING-file sidecar is pruned.
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, 703)
        _add_placement(conn, 703, "hardlink", _present_folder(tmp_path, "f703"))
        _add_placement(conn, 703, "plex_upload", "")
        conn.commit()
    res = verify_placement_health(db)
    assert res["pruned"] == 0
    with sqlite3.connect(db) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE tmdb_id=703").fetchone()[0]
    assert n == 2


def test_amplifier_sweep_cap_skips_implausible_prune(tmp_path):
    # class-8 guard: if the prune set is implausibly large (a mass stat
    # failure), skip the DELETE entirely rather than wipe real records.
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        # 60 dual rows → 60 prunable, total 120 placements, cap=max(50,30)=50.
        for i in range(60):
            tid = 1000 + i
            _seed_theme(conn, tid)
            _add_placement(conn, tid, "hardlink",
                           _missing_folder(tmp_path, f"d{tid}"))
            _add_placement(conn, tid, "plex_upload", "")
        conn.commit()
    res = verify_placement_health(db)
    assert res["pruned"] == 0, "over-cap prune must be skipped, not run blind"
    with sqlite3.connect(db) as conn:
        n = conn.execute("SELECT COUNT(*) FROM placements").fetchone()[0]
    assert n == 120, "no records deleted when the cap trips"


def test_worker_drops_superseded_record_after_api_upload():
    # regression-lock: the place path (_do_place_collection api branch) must
    # DELETE the superseded sidecar RECORD after an API upload, not just unlink
    # its file — co-located with the v1.18.59 leftover-sidecar file cleanup.
    # v1.23.31: the inline DELETE was extracted into the shared
    # delete_superseded_sidecar_placement helper (predicate-drift fix).
    i = WORKER_PY.index("dropped %d superseded")
    block = WORKER_PY[i - 600:i + 400]
    assert "delete_superseded_sidecar_placement(" in block
    assert "_coll_edition_key" in block
