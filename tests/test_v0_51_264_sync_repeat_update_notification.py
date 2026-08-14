"""v0.51.264 — an override row stops re-notifying "1 updated" every sync,
and stops having its own !UPD prompt swept away.

The operator's report: "keep receiving notifications for 'Motif sync — 1
updated / Updated: Anime · Vampire in the Garden (2022)' — that row is TDB ∅
in the library." The run log showed the two halves of it in one run:
`updated:1` and `cleared 1 stale pending update(s) — upstream theme removed`.

Two defects, both rooted in the same wrong assumption — that
`themes.youtube_url` is upstream's answer:

  1. v1.22.8's url-less sweep reads `themes.youtube_url IS NULL` as "TDB
     removed its theme". On a row the operator overrode BECAUSE TDB had
     nothing (SRC=U, youtube_url NULL), v0.51.228 deliberately WITHHOLDS the
     write when TDB later publishes one — so the column stays NULL and the
     sweep deleted the !UPD prompt the very same sync run had written. The
     operator had nothing to accept or decline, and the log line blamed
     upstream for motif's own withheld write.
  2. The `stats.updated_count` increment fired on every `url_changed`
     re-detection. Because the withheld branch also leaves
     tdb_content_fingerprint stale (by design — it's the cursor that makes
     the row re-checked), url_changed re-fires forever for the SAME video.
     A KEEP CURRENT didn't help: v1.20.14 keeps the row 'declined', but the
     count never consulted the decision.

Fixes: the sweep asks TDB's own record (raw_json, which the withheld branch
rewrites every sync) for override rows; the count fires only when the UPSERT
below it would actually arm a prompt.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import get_conn, init_db, transaction  # noqa: E402
from app.core.sync import (  # noqa: E402
    SyncStats,
    _clear_url_less_pending_updates,
    _flush_sync_batch,
)

MT, TID = "tv", 900001
USER_VID = "USERpickID1"
TDB_VID = "TDBnewVID01"


def _record(vid: str | None) -> dict:
    rec = {"title": "Vampire Repro", "first_air_date": "2022-05-16"}
    if vid:
        rec["youtube_theme_url"] = f"https://www.youtube.com/watch?v={vid}"
    return rec


def _seed(db, *, override: bool, tdb_url: str | None = None, raw_json: str = "{}"):
    """A title in Plex whose themes row carries NO TDB url (the ∅ pill)."""
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title, year,
                 youtube_url, youtube_video_id, upstream_source, raw_json,
                 last_seen_sync_at, first_seen_sync_at, tdb_content_fingerprint)
               VALUES (?, ?, 'Vampire Repro', '2022', ?, NULL, 'themoviedb', ?,
                       '2026-01-01', '2026-01-01', 'stale-fp')""",
            (MT, TID, tdb_url, raw_json))
        theme_id = conn.execute(
            "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
            (MT, TID)).fetchone()["id"]
        if override:
            conn.execute(
                """INSERT INTO user_overrides (media_type, tmdb_id, theme_id,
                     youtube_url, set_at, set_by)
                   VALUES (?, ?, ?, ?, '2026-02-01', 'admin')""",
                (MT, TID, theme_id,
                 f"https://www.youtube.com/watch?v={USER_VID}"))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, year, guid_tmdb, theme_id, first_seen_at, last_seen_at)
               VALUES ('r900001', '3', 'show', 'Vampire Repro', '2022', ?, ?,
                       '2026-01-01', '2026-01-01')""",
            (TID, theme_id))


def _sync(db, vid: str | None = TDB_VID, *, ts: str = "2026-08-11",
          enqueue: bool = False) -> SyncStats:
    stats = SyncStats()
    _flush_sync_batch(
        db, [(MT, TID, _record(vid), "themoviedb")], sync_ts=ts,
        enqueue_downloads=enqueue, auto_place_override=None,
        auto_download_new_themes=enqueue, stats=stats)
    return stats


def _pendings(db) -> list[dict]:
    with get_conn(db) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT section_id, new_video_id, decision, kind FROM pending_updates "
            "WHERE media_type = ? AND tmdb_id = ?", (MT, TID))]


@pytest.fixture
def db(tmp_path):
    return tmp_path / "t.db"


# ── defect 2: the repeat notification ────────────────────────


def test_override_row_notifies_once_not_every_sync(db):
    """Three identical syncs, one notification. Pre-fix: three."""
    _seed(db, override=True)
    counts = []
    for n in (1, 2, 3):
        s = _sync(db, ts=f"2026-08-1{n}")
        _clear_url_less_pending_updates(db)
        counts.append(s.updated_count)
    assert counts == [1, 0, 0], (
        "v0.51.264: the withheld-write branch re-detects the same upstream "
        f"video every sync — it must notify ONCE, not {counts}")


def test_the_one_notification_carries_the_title(db):
    _seed(db, override=True)
    s = _sync(db)
    assert s.updated_titles == [(MT, TID, "Vampire Repro", "2022")]
    assert _sync(db, ts="2026-08-12").updated_titles == []


def test_a_genuinely_new_upstream_video_re_notifies(db):
    """The gate is 'newly detected', not 'notified once ever' — TDB moving to
    a DIFFERENT video is a fresh decision for the operator."""
    _seed(db, override=True)
    assert _sync(db).updated_count == 1
    assert _sync(db, ts="2026-08-12").updated_count == 0
    s = _sync(db, vid="TDBsecond02", ts="2026-08-13")
    assert s.updated_count == 1, (
        "a different upstream video must re-arm the prompt AND re-notify")


def test_keep_current_is_not_re_nagged(db):
    """v1.20.14 holds a declined row declined while TDB serves the same video.
    Pre-fix the notification ignored that and announced it every sync."""
    _seed(db, override=True)
    _sync(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "UPDATE pending_updates SET decision = 'declined' "
            "WHERE media_type = ? AND tmdb_id = ?", (MT, TID))
    assert _sync(db, ts="2026-08-12").updated_count == 0
    assert [p["decision"] for p in _pendings(db)] == ["declined"], (
        "the decline must also survive the sync (v1.20.14)")


def test_enqueue_path_still_counts(db):
    """The no-override / no-file row that ENQUEUES a download is untouched by
    the newness gate — it writes no pending_update to compare against."""
    _seed(db, override=False)
    assert _sync(db, enqueue=True).updated_count == 1


# ── defect 1: the sweep ate its own prompt ───────────────────


def test_withheld_url_row_keeps_its_pending_prompt(db):
    """The !UPD prompt must survive the same run that created it, or the
    operator gets a notification about something they cannot act on."""
    _seed(db, override=True)
    _sync(db)
    assert _clear_url_less_pending_updates(db) == 0, (
        "v0.51.264: a withheld youtube_url is motif's own doing — not "
        "evidence that upstream removed the theme")
    assert [(p["new_video_id"], p["decision"]) for p in _pendings(db)] == [
        (TDB_VID, "pending")]


def test_sweep_still_clears_when_upstream_really_removed_the_url(db):
    """v1.22.8's actual case, on an override row: TDB's record itself has no
    youtube_theme_url, so the pending points at nothing. Still swept."""
    _seed(db, override=True)
    _sync(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "UPDATE themes SET raw_json = '{\"title\": \"Vampire Repro\"}' "
            "WHERE media_type = ? AND tmdb_id = ?", (MT, TID))
    assert _clear_url_less_pending_updates(db) == 1
    assert _pendings(db) == []


def test_sweep_unchanged_for_rows_without_an_override(db):
    """The v1.22.8 regression lock — no override means themes.youtube_url IS
    upstream's answer, and the sweep reads it directly as before."""
    _seed(db, override=False)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO pending_updates (media_type, tmdb_id, section_id,
                 new_video_id, detected_at, decision, kind)
               VALUES (?, ?, '', ?, '2026-08-11', 'pending', 'upstream_changed')""",
            (MT, TID, TDB_VID))
    assert _clear_url_less_pending_updates(db) == 1


def test_legacy_unparseable_raw_json_is_not_swept(db):
    """json_valid guards the override arm into the conservative answer: a row
    motif cannot read upstream's record for keeps its prompt (v1.18.10 —
    a sweep must never amplify a broken state)."""
    _seed(db, override=True, raw_json="")
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO pending_updates (media_type, tmdb_id, section_id,
                 new_video_id, detected_at, decision, kind)
               VALUES (?, ?, '', ?, '2026-08-11', 'pending', 'upstream_changed')""",
            (MT, TID, TDB_VID))
    assert _clear_url_less_pending_updates(db) == 0


def test_urls_match_still_never_swept(db):
    """Unchanged from v1.22.8 — the 'your U == TDB' convert prompt."""
    _seed(db, override=True, raw_json='{"title": "Vampire Repro"}')
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO pending_updates (media_type, tmdb_id, section_id,
                 new_video_id, detected_at, decision, kind)
               VALUES (?, ?, '', ?, '2026-08-11', 'pending', 'urls_match')""",
            (MT, TID, USER_VID))
    assert _clear_url_less_pending_updates(db) == 0


def test_v0_51_264_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
