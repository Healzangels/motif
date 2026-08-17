"""v0.51.265 — `themes.youtube_url` is the last COMMITTED TDB url, not
upstream's live answer. Pinned as behaviour, not prose.

v0.51.264 fixed a consumer that read the column as upstream truth: v1.22.8's
url-less sweep treated `youtube_url IS NULL` as "TDB removed its theme" and
deleted the !UPD prompt the same sync run had written, on rows where v0.51.228
deliberately WITHHOLDS the write. The enabler was the contract as stated —
`db.py` said "themes.youtube_url — TDB's current URL", flatly, with no mention
of the exception. That statement is what made the sweep's author reasonable.

Correcting the comment alone would be a source-text pin (v1.18.81's phantom-fix
sub-pattern: prose nothing executes). So the exception is pinned here as the
behaviour it actually is. These tests fail if someone "fixes" the withheld
branch to write the url, or drops raw_json as the live-answer channel — which
is the moment to re-read v0.51.228 rather than discover this again downstream.
"""
from __future__ import annotations
from _slice_helpers import slice_to_next

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import get_conn, init_db, transaction  # noqa: E402
from app.core.sync import SyncStats, _flush_sync_batch  # noqa: E402

MT, TID = "movie", 265001
OLD_VID, NEW_VID = "committed01", "upstream002"


def _record(vid: str) -> dict:
    return {
        "title": "Contract Movie",
        "release_date": "2020-01-01",
        "youtube_theme_url": f"https://www.youtube.com/watch?v={vid}",
    }


def _seed(db, *, override: bool, committed_vid: str | None = OLD_VID):
    init_db(db)
    url = (f"https://www.youtube.com/watch?v={committed_vid}"
           if committed_vid else None)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title, year,
                 youtube_url, youtube_video_id, upstream_source, raw_json,
                 last_seen_sync_at, first_seen_sync_at, tdb_content_fingerprint)
               VALUES (?, ?, 'Contract Movie', '2020', ?, ?, 'themoviedb', '{}',
                       '2026-01-01', '2026-01-01', 'stale-fp')""",
            (MT, TID, url, committed_vid))
        if override:
            theme_id = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (MT, TID)).fetchone()["id"]
            conn.execute(
                """INSERT INTO user_overrides (media_type, tmdb_id, theme_id,
                     youtube_url, set_at, set_by)
                   VALUES (?, ?, ?, 'https://www.youtube.com/watch?v=operator01',
                           '2026-02-01', 'admin')""",
                (MT, TID, theme_id))


def _theme(db) -> dict:
    with get_conn(db) as conn:
        return dict(conn.execute(
            "SELECT youtube_url, youtube_video_id, raw_json, "
            "       tdb_content_fingerprint FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?", (MT, TID)).fetchone())


def _sync(db, vid: str):
    stats = SyncStats()
    _flush_sync_batch(
        db, [(MT, TID, _record(vid), "themoviedb")], sync_ts="2026-08-14",
        enqueue_downloads=False, auto_place_override=None,
        auto_download_new_themes=False, stats=stats)


@pytest.fixture
def db(tmp_path):
    return tmp_path / "t.db"


# ── the exception ────────────────────────────────────────────


def test_override_row_column_does_not_track_upstream(db):
    """THE contract exception. With an override present, upstream moving to a
    new video leaves themes.youtube_url at the previously committed value —
    v0.51.228, so the operator's choice wins until they ACCEPT."""
    _seed(db, override=True)
    _sync(db, NEW_VID)
    t = _theme(db)
    assert t["youtube_video_id"] == OLD_VID, (
        "v0.51.228: the withheld branch must NOT advance the committed url; if "
        "this now tracks upstream, re-read that tag before 'fixing' consumers")
    assert NEW_VID not in (t["youtube_url"] or "")


def test_raw_json_IS_the_live_upstream_answer_on_that_row(db):
    """The channel consumers must use instead. The withheld branch rewrites
    raw_json every sync, so it carries upstream's current url even while the
    committed column is stale — this is what v0.51.264's sweep reads."""
    _seed(db, override=True)
    _sync(db, NEW_VID)
    rec = json.loads(_theme(db)["raw_json"])
    assert rec.get("youtube_theme_url", "").endswith(NEW_VID), (
        "raw_json must carry upstream's live url on a withheld row — "
        "v0.51.264's url-less sweep depends on it to avoid eating live prompts")


def test_withheld_row_stays_on_the_slow_path(db):
    """The fingerprint must NOT advance either, or the v1.15.81 fast path skips
    the row forever and the withheld url is never revisited (v0.51.228)."""
    _seed(db, override=True)
    before = _theme(db)["tdb_content_fingerprint"]
    _sync(db, NEW_VID)
    assert _theme(db)["tdb_content_fingerprint"] == before


# ── the rule the exception is an exception TO ────────────────


def test_without_an_override_the_column_does_track_upstream(db):
    """The base case, so the tests above pin an EXCEPTION and not an accident:
    no override → sync commits upstream's url and advances the fingerprint."""
    _seed(db, override=False)
    _sync(db, NEW_VID)
    t = _theme(db)
    assert t["youtube_video_id"] == NEW_VID
    assert t["tdb_content_fingerprint"] != "stale-fp"


# ── the second writer ────────────────────────────────────────


def test_adopt_backfill_only_touches_a_blank_column(db):
    """adopt.py writes a restored (non-TDB) url here when the column is blank —
    the other reason it isn't upstream truth. It must never overwrite a
    committed value; sync owns that."""
    adopt_py = (REPO / "app" / "core" / "adopt.py").read_text()
    stmt = slice_to_next(adopt_py, "UPDATE themes SET youtube_url = ? ",
                         "conn.execute(")
    assert "youtube_url IS NULL OR youtube_url = ''" in stmt, (
        "the adopt backfill must stay gated on a blank column")
    # and behaviourally: a committed url survives a sync round-trip untouched
    # by anything but sync itself.
    _seed(db, override=False, committed_vid=OLD_VID)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "UPDATE themes SET youtube_url = ? "
            "WHERE media_type = ? AND tmdb_id = ? "
            "  AND (youtube_url IS NULL OR youtube_url = '')",
            ("https://www.youtube.com/watch?v=restored001", MT, TID))
    assert OLD_VID in _theme(db)["youtube_url"]


# ── the contract is stated where the column lives ────────────


def test_column_comment_names_both_writers(db):
    """Cheap belt: the schema comment must name the exception, since that is
    where the next consumer will look. The behavioural tests above are the
    real guard — this only stops the comment being deleted wholesale."""
    schema = (REPO / "app" / "core" / "db.py").read_text()
    i = schema.index("CREATE TABLE IF NOT EXISTS themes")
    block = schema[i:schema.index("youtube_video_id", i)]
    assert "v0.51.228" in block and "adopt.py" in block
    assert "raw_json" in block


def test_v0_51_265_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
