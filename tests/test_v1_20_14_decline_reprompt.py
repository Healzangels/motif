"""v1.20.14 — declined / un-actioned prompts re-arm on a genuinely new
TDB video (audit round 3 M1 + M2; the user's "re-prompt on change" policy).

The decline dialog promises "you'll see another prompt only if ThemerrDB
publishes a further URL change." But pre-fix:
  M1: a per-section KEEP CURRENT wrote a per-section 'declined' row that
      shadowed the global '' row forever — sync only re-emitted to '',
      so the declined section never re-prompted on a later change. The
      '' row itself was also sticky-declined regardless of changes.
  M2: new_theme_available is emitted only on is_new, so a SRC=– row whose
      theme later changed kept its ORIGINAL URL forever — an un-actioned
      prompt went stale and a declined section never re-prompted.

Fix (all guarded so re-arm fires ONLY on a genuinely different video —
no prompt-spam on unchanged syncs):
  A: the '' UPSERT keeps a declined row declined only while the video is
     unchanged.
  B: a follow-up UPDATE re-arms per-section 'declined' rows on a new video.
  C: the SRC=– (else) branch refreshes the new_theme row's URL + re-arms
     declined rows on a new video.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
OLD_VID = "OLDvid00000"
NEW_VID = "NEWvid11111"
NEW_URL = f"https://www.youtube.com/watch?v={NEW_VID}"


def _db(tmp_path):
    from app.core.db import init_db
    db = tmp_path / "m.db"
    init_db(db)
    return db


def _seed_theme(db, *, tmdb_id, video_id=OLD_VID):
    """A themes row whose CURRENT video is `video_id` and whose stored
    fingerprint is stale (forces the slow path → url_changed computed)."""
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "  youtube_url, youtube_video_id, tdb_content_fingerprint, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv',?,'X','imdb',?,?, 'STALE-FP', "
            "  '2026-01-01T00:00:00','2026-01-01T00:00:00')",
            (tmdb_id, f"https://yt/{video_id}", video_id))
        c.commit()


def _pending(db, tmdb_id, section_id):
    with sqlite3.connect(db) as c:
        r = c.execute(
            "SELECT decision, new_video_id FROM pending_updates "
            "WHERE media_type='tv' AND tmdb_id=? AND section_id=?",
            (tmdb_id, section_id)).fetchone()
    return (r[0], r[1]) if r else None


def _flush(db, tmdb_id):
    """Run the real sync emission for a record carrying NEW_VID."""
    from app.core.sync import _flush_sync_batch, SyncStats
    record = {
        "imdb_id": f"tt{tmdb_id}", "title": "X",
        "youtube_theme_url": NEW_URL,
        "youtube_theme_edited": "2026-05-29T00:00:00",
        "release_date": "2020-01-01",
    }
    _flush_sync_batch(
        db, [("tv", tmdb_id, record, "imdb")],
        sync_ts="2026-06-01T00:00:00",
        enqueue_downloads=False, auto_place_override=None,
        auto_download_new_themes=False, stats=SyncStats())


# ── M1: declined U/A/M rows re-arm on a genuinely new video ──


def test_declined_section_rearms_on_new_video(tmp_path):
    db = _db(tmp_path)
    _seed_theme(db, tmdb_id=1, video_id=OLD_VID)
    with sqlite3.connect(db) as c:
        # has_override → the already_have/has_override emission branch.
        c.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, section_id, "
            "youtube_url, set_at, set_by) VALUES ('tv',1,'','https://yt/u',"
            "'2026-01-01','admin')")
        # '' + per-section '3' both DECLINED at the OLD video.
        for sec in ("", "3"):
            c.execute(
                "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
                "kind, new_video_id, new_youtube_url, decision, detected_at) "
                "VALUES ('tv',1,?,'upstream_changed',?,?, 'declined',"
                "'2026-01-01')", (sec, OLD_VID, f"https://yt/{OLD_VID}"))
        c.commit()

    _flush(db, 1)

    # Both the global '' row (Change A) and the per-section '3' row
    # (Change B) re-arm to pending at the NEW video.
    assert _pending(db, 1, "") == ("pending", NEW_VID), _pending(db, 1, "")
    assert _pending(db, 1, "3") == ("pending", NEW_VID), _pending(db, 1, "3")


def test_declined_section_stays_declined_on_same_video(tmp_path):
    """The guard: a section that already declined THIS exact video stays
    declined even when the sync fires url_changed (themes rolled)."""
    db = _db(tmp_path)
    # themes currently OLD → url_changed fires (NEW != OLD); but the
    # section already declined NEW_VID, so it must NOT re-nag.
    _seed_theme(db, tmdb_id=2, video_id=OLD_VID)
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, section_id, "
            "youtube_url, set_at, set_by) VALUES ('tv',2,'','https://yt/u',"
            "'2026-01-01','admin')")
        c.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_video_id, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',2,'3','upstream_changed',?,?, 'declined',"
            "'2026-01-01')", (NEW_VID, NEW_URL))
        c.commit()

    _flush(db, 2)
    assert _pending(db, 2, "3") == ("declined", NEW_VID), (
        "a section that already declined this exact video must stay "
        "declined (no re-nag on the same video)"
    )


# ── M2: SRC=– new_theme rows refresh + re-arm on a new video ──


def test_new_theme_url_refreshes_and_rearms_declined(tmp_path):
    db = _db(tmp_path)
    _seed_theme(db, tmdb_id=3, video_id=OLD_VID)
    with sqlite3.connect(db) as c:
        # SRC=– row (no local_files, no override). '' new_theme DECLINED
        # at the OLD video; a per-section pending new_theme at OLD too.
        c.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_video_id, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',3,'','new_theme_available',?,?, 'declined',"
            "'2026-01-01')", (OLD_VID, f"https://yt/{OLD_VID}"))
        c.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_video_id, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',3,'3','new_theme_available',?,?, 'pending',"
            "'2026-01-01')", (OLD_VID, f"https://yt/{OLD_VID}"))
        c.commit()

    _flush(db, 3)

    # Declined '' re-arms to pending at NEW; the URL is refreshed (no
    # longer stale → ACCEPT would download the NEW theme).
    assert _pending(db, 3, "") == ("pending", NEW_VID), _pending(db, 3, "")
    assert _pending(db, 3, "3") == ("pending", NEW_VID), _pending(db, 3, "3")
    with sqlite3.connect(db) as c:
        url = c.execute(
            "SELECT new_youtube_url FROM pending_updates WHERE tmdb_id=3 "
            "AND section_id=''").fetchone()[0]
    assert url == NEW_URL, "v1.20.14: the new_theme URL must refresh (M2)"


def test_accepted_new_theme_not_disturbed(tmp_path):
    """Defensive: the Change-C CASE leaves an 'accepted' new_theme row
    accepted (it should never reach the SRC=– else branch, but the guard
    protects against a re-pend if it does)."""
    db = _db(tmp_path)
    _seed_theme(db, tmdb_id=4, video_id=OLD_VID)
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_video_id, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',4,'','new_theme_available',?,?, 'accepted',"
            "'2026-01-01')", (OLD_VID, f"https://yt/{OLD_VID}"))
        c.commit()
    _flush(db, 4)
    assert _pending(db, 4, "")[0] == "accepted"


# ── source pins ──


def test_changes_present_in_sync():
    assert "re-arm per-section KEEP CURRENT decisions" in SYNC_PY  # B
    assert "refresh the new_theme_available prompt for" in SYNC_PY  # C
    assert "AND new_video_id IS NOT ?" in SYNC_PY  # B guard


def test_v1_20_14_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
