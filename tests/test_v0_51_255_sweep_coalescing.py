"""v0.51.255 — the two remaining sweeps declare themselves bulk.

Follow-up to v0.51.254, from sweeping the whole class rather than only the
loop the operator happened to hit. Every coalesced dispatch keys `bulk` off
the job payload; of the twelve place/download enqueue sites, five stamp it,
three are genuinely one-action-per-call (UPLOAD MP3, SET URL, PROMOTE — no
bulk button exists for any of them), and the auto_restore sweep coalesces via
a hardcoded branch. That left exactly two, both AUTOMATED — they would flood
without anyone pressing a button:

  _retry_pending_placements   scheduler.py — hourly, `LIMIT 500`. Payload was
                              {"edition_key": …}: no force, no reason, so each
                              landed place fires theme_added on the immediate
                              path. Ceiling: 500 Discord messages from a cron
                              tick.

  reconcile_placement_paths   plex_enum.py — runs every enum, UNCAPPED. Stamps
                              force+reason so each landed place fires
                              theme_pushed. Trigger is a mass folder rename
                              (*arr reorganisation, library restructure, a disk
                              re-add), i.e. exactly the conditions where the
                              batch is large.

Neither can ever be one user action per row, so both stamp bulk
unconditionally — the same reasoning that has always applied to auto_restore,
and NOT the time-window inference v1.23.46 removed (which was wrong because it
collapsed N genuinely separate manual actions; a sweep is one event by
construction).

Confirmed live the day this shipped: the auto-restore sweep coalesced 76
restored themes into 2 messages (the buffer chunks at _COALESCE_MAX_TAIL), so
the machinery works — these two just never opted in.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso

REPO = Path(__file__).resolve().parent.parent
NOW = now_iso()
TMDB = 4242
FOLDER_OLD = "/data/Movies/Old Name (2011)"
FOLDER_NEW = "/data/Movies/New Name (2011)"


def _payloads(db, tmdb=TMDB) -> list[dict]:
    conn = sqlite3.connect(db)
    return [json.loads(r[0]) for r in conn.execute(
        "SELECT payload FROM jobs WHERE job_type='place' AND tmdb_id=?",
        (tmdb,)).fetchall()]


# ── 1. the hourly retry sweep ────────────────────────────────────────────

def _seed_retry(tmp_path, n: int):
    """n downloaded rows with NO placement — the sweep's exact candidate
    shape (local_files LEFT JOIN placements WHERE media_folder IS NULL)."""
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        for i in range(n):
            conn.execute(
                "INSERT INTO themes (media_type, tmdb_id, title, year,"
                " upstream_source, last_seen_sync_at, first_seen_sync_at)"
                " VALUES ('movie',?,?,'2011','imdb',?,?)",
                (TMDB + i, f"Movie {i}", NOW, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',?,'1','',?,?,"
                "'v','auto','themerrdb')",
                (TMDB + i, f"movies/{i}.mp3", NOW))
        conn.commit()
    return db


def test_retry_sweep_marks_its_jobs_bulk(tmp_path):
    """THE fix. Pre-v0.51.255 an hourly cron tick could emit one Discord
    message per re-placed row, up to its 500-row cap."""
    from app.core.scheduler import _retry_pending_placements
    db = _seed_retry(tmp_path, 3)

    _retry_pending_placements(db)

    got = [_payloads(db, TMDB + i) for i in range(3)]
    assert all(p for p in got), f"sweep enqueued nothing: {got}"
    for i, ps in enumerate(got):
        assert all(p.get("bulk") is True for p in ps), (
            f"row {i} unmarked — an unattended sweep must coalesce: {ps}")


def test_retry_sweep_keeps_carrying_edition_key(tmp_path):
    """v1.21.56's edition_key must survive the payload edit — dropping it
    sends the worker back to the '' fallback and it re-places the WRONG
    edition. Adding a key is easy; silently losing its neighbour is the
    risk worth pinning."""
    from app.core.scheduler import _retry_pending_placements
    db = _seed_retry(tmp_path, 1)

    _retry_pending_placements(db)

    ps = _payloads(db)
    assert ps and all("edition_key" in p for p in ps), ps


# ── 2. the folder-move reconcile ─────────────────────────────────────────

def _seed_reloc(tmp_path):
    """A placement whose folder Plex no longer reports, and a plex_items row
    at the NEW path — the relocate candidate shape (mirrors v0.51.250)."""
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES (1,'movie',?,'Renamed','2011','imdb',?,?)",
            (TMDB, NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
            " has_theme, local_theme_file, first_seen_at, last_seen_at)"
            " VALUES ('rk-1','1','movie',1,?,'Renamed','2011','',?,1,1,?,?)",
            (TMDB, FOLDER_NEW, NOW, NOW))
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " edition_key, media_folder, placed_at, placement_kind,"
            " plex_refreshed, provenance) VALUES ('movie',?,'1','',?,?,"
            "'hardlink',1,'auto')", (TMDB, FOLDER_OLD, NOW))
        conn.commit()
    return db


def test_folder_relocate_marks_its_jobs_bulk(tmp_path):
    """A mass rename is one event, not N user actions. Uncapped, so this is
    the larger of the two floods."""
    from app.core.plex_enum import reconcile_placement_paths
    db = _seed_reloc(tmp_path)

    assert reconcile_placement_paths(db) == 1

    ps = _payloads(db)
    assert ps, "no relocate place job enqueued"
    assert all(p.get("bulk") is True for p in ps), ps


def test_folder_relocate_keeps_force_and_reason(tmp_path):
    """`force` + reason='folder_relocated' drive the worker's theme_pushed
    branch AND the relocate semantics. The bulk key rides ALONGSIDE them —
    if either is lost the notification silently changes kind."""
    from app.core.plex_enum import reconcile_placement_paths
    db = _seed_reloc(tmp_path)
    reconcile_placement_paths(db)

    ps = _payloads(db)
    assert ps
    for p in ps:
        assert p.get("force") is True, p
        assert p.get("reason") == "folder_relocated", p
        assert "edition_key" in p, p


# ── the class, stated once ───────────────────────────────────────────────

def test_no_unmarked_sweep_enqueues_remain():
    """The class lint. Every place-job enqueue inside a SWEEP must stamp
    bulk; a new sweep that forgets is the next 500-message cron tick.

    Deliberately narrow: only the two sweep functions. A repo-wide rule
    would be wrong — the single-action endpoints (UPLOAD MP3, SET URL,
    PROMOTE) MUST stay unmarked so they keep their immediate rich preview.
    """
    for mod, fn in (("app/core/scheduler.py", "_retry_pending_placements"),
                    ("app/core/scheduler.py", "_restore_lost_placements"),
                    ("app/core/plex_enum.py", "reconcile_placement_paths")):
        src = (REPO / mod).read_text()
        i = src.index(f"def {fn}(")
        j = src.index("\ndef ", i + 1)
        body = src[i:j]
        if "INSERT INTO jobs" not in body:
            continue
        marked = '"bulk": True' in body or '"reason": "auto_restore"' in body
        assert marked, (
            f"{mod}::{fn} enqueues place jobs without declaring bulk — an "
            "unattended sweep will emit one notification per row")
