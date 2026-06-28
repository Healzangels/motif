"""v1.24.29 — auto re-PUSH a stale plex_upload (Avenue Q auto-recovery).

Follow-up to v1.24.28 (which made a stale plex_upload read honestly — RP badge,
not-placed). the user chose: also auto re-push. The same Plex delete+re-add that
wipes a sidecar destroys an uploaded theme's rating_key; v1.24.28 stamps such a
placement theme_present=0. The v1.24.26 _restore_lost_placements sweep already
auto-re-places stale SIDECARS; v1.24.29 extends it to stale plex_uploads —
re-pushing to the new live rk (after v1.24.27 re-linked the theme) via the same
place-job + theme_auto_restored path.

Gating (mirrors the sidecar branch + adds plex_upload-specific re-verification):
the old uploaded rk must be genuinely dead (re-checked, not just the stamp), AND
a live plex_items row must be bonded to the theme (a current rk to upload to).
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core import scheduler
from app.core.db import get_conn, init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _settings(db):
    return SimpleNamespace(db_path=db, cfg=SimpleNamespace(notifications=None))


def _seed(
    db,
    *,
    tmdb=-29,
    tid=90,
    section="1",
    edition="",
    upload_rk="dead-rk",
    theme_present=0,
    last_reason=None,
    live_rk="live-rk",
    bond_live=True,
    old_rk_still_live=False,
):
    """A plex_upload placement (uploaded to upload_rk) + a surviving canonical +
    (optionally) a LIVE plex_items row bonded to the theme at live_rk."""
    with get_conn(db) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
            " themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)",
            (section, NOW, NOW))
        c.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, year, "
            " upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES (?, 'movie', ?, 'Avenue Q', '2003', 'plex_orphan', ?, ?)",
            (tid, tmdb, NOW, NOW))
        c.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            " edition_key, file_path, source_kind, source_video_id, "
            " downloaded_at, last_place_attempt_reason) "
            "VALUES ('movie', ?, ?, ?, 'movies/x.mp3', 'url', 'vid', ?, ?)",
            (tmdb, section, edition, NOW, last_reason))
        c.execute(
            "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id, "
            " edition_key, media_folder, placed_at, placement_kind, "
            " plex_rating_key, plex_refreshed, theme_present) "
            "VALUES (?, 'movie', ?, ?, ?, '', ?, 'plex_upload', ?, 1, ?)",
            (tid, tmdb, section, edition, NOW, upload_rk, theme_present))
        if bond_live:
            c.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type, "
                " theme_id, guid_tmdb, title, edition_key, has_theme, "
                " first_seen_at, last_seen_at) "
                "VALUES (?, ?, 'movie', ?, ?, 'Avenue Q', ?, 0, ?, ?)",
                (live_rk, section, tid, tmdb, edition, NOW, NOW))
        if old_rk_still_live:
            # the OLD uploaded rk still exists as a plex_items row → not stale
            c.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type, "
                " theme_id, guid_tmdb, title, edition_key, has_theme, "
                " first_seen_at, last_seen_at) "
                "VALUES (?, ?, 'movie', ?, ?, 'Avenue Q', ?, 1, ?, ?)",
                (upload_rk, section, tid, tmdb, edition, NOW, NOW))
        c.commit()


def _place_jobs(db):
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        return c.execute("SELECT * FROM jobs WHERE job_type='place'").fetchall()


# ── the fix: a stale plex_upload re-pushes ──────────────────────────────

def test_stale_plex_upload_repushes(db, monkeypatch):
    _seed(db)  # upload rk dead, a live bonded plex_items at live-rk
    fired = []
    monkeypatch.setattr("app.core.notify.dispatch",
                        lambda *a, **k: fired.append(k or a))
    scheduler._restore_lost_placements(_settings(db))
    jobs = _place_jobs(db)
    assert len(jobs) == 1
    payload = json.loads(jobs[0]["payload"])
    assert payload["edition_key"] == ""
    # v1.24.38 (review #6): tagged for the worker, no dispatch at enqueue.
    assert payload["reason"] == "auto_restore"
    assert payload["kind"] == "api", "a stale plex_upload re-pushes via the API"
    assert not fired, "no notification at enqueue — the worker fires on landing"


def test_carries_edition_key(db, monkeypatch):
    _seed(db, tmdb=-40, tid=91, edition="extended")
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)
    scheduler._restore_lost_placements(_settings(db))
    jobs = _place_jobs(db)
    assert len(jobs) == 1
    assert json.loads(jobs[0]["payload"])["edition_key"] == "extended"


# ── gating ──────────────────────────────────────────────────────────────

def test_skipped_when_old_rk_still_live(db):
    # the uploaded rk still exists → the upload isn't actually gone → not stale.
    _seed(db, theme_present=0, old_rk_still_live=True)
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_theme_present_not_zero(db):
    _seed(db, theme_present=1)
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_no_live_bonded_plex_item(db):
    # no current rk bonded to the theme → nowhere to re-upload → skip until the
    # v1.24.27 re-link gives it a live item.
    _seed(db, bond_live=False)
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_backup_only(db):
    _seed(db, last_reason="backup_only")
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_place_job_in_flight(db):
    _seed(db, tmdb=-50, tid=92)
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, "
            " payload, status, created_at, next_run_at) "
            "VALUES ('place', 'movie', -50, '1', ?, 'pending', ?, ?)",
            (json.dumps({"edition_key": ""}), NOW, NOW))
        c.commit()
    scheduler._restore_lost_placements(_settings(db))
    assert len(_place_jobs(db)) == 1  # the pre-seeded one, no duplicate


def test_skipped_when_null_rating_key(db):
    # a legacy plex_upload with no stored rk can't be re-verified as stale.
    _seed(db, upload_rk=None, theme_present=0)
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


# ── source pins ─────────────────────────────────────────────────────────

def test_plex_upload_branch_present():
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "scheduler.py").read_text()
    assert "pu_cands" in src
    assert "p.placement_kind = 'plex_upload'" in src
    assert "to_place.extend(pu_cands)" in src
