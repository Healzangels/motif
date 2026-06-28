"""v1.24.26 — auto-restore a motif-owned sidecar whose Plex theme.mp3 went missing.

the user's repro (continuation of the Avenue Q v1.24.25 thread): a movie/show that
motif had PLACED a theme.mp3 sidecar onto gets removed + re-added in Plex. The
new Plex folder is fresh (no theme.mp3), but motif's canonical survives in
/config/themes and its placement row survives. `verify_placement_health` stamps
`placements.theme_present=0` (the sidecar is CONFIRMED gone), yet the v1.18.90
reaper silently skips the loss (a surviving placement/canonical routes it to the
'other_fallback' tier — no notification, no recovery).

`_restore_lost_placements` is an hourly sweep that finds these (theme_present=0,
motif-owned non-plex_upload sidecar, surviving local_files canonical, a LIVE
plex_items folder, no in-flight place job, not a permanent-skip reason),
RE-STATs each `media_folder/theme.mp3` for authority, re-enqueues a `place` job
from the surviving backup, and fires `theme_auto_restored` so the operator knows
motif self-healed.

These tests call the REAL function against a seeded DB + a tmp media folder.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core import scheduler
from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _settings(db: Path):
    # The function only touches settings.db_path + settings.cfg.notifications;
    # notify.dispatch is monkeypatched (or never reached when nothing restores).
    return SimpleNamespace(db_path=db, cfg=SimpleNamespace(notifications=None))


def _seed(
    db: Path,
    *,
    media_folder: str,
    tmdb_id: int = 100,
    section_id: str = "1",
    edition_key: str = "",
    placement_kind: str = "hardlink",
    theme_present: int = 0,
    last_reason: str | None = None,
    with_local_file: bool = True,
    with_plex_item: bool = True,
    plex_media_type: str = "movie",
    placement_media_type: str = "movie",
    title: str = "Avenue Q",
    year: str = "2003",
) -> None:
    """One title's worth of rows: themes + local_files + placements + plex_items,
    wired so the sweep's JOINs + EXISTS gate line up (or deliberately don't)."""
    now = _now()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            " upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES (?, ?, ?, ?, 'imdb', ?, ?)",
            (placement_media_type, tmdb_id, title, year, now, now),
        )
        if with_local_file:
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id, "
                " edition_key, file_path, source_kind, source_video_id, "
                " downloaded_at, last_place_attempt_reason) "
                "VALUES (?, ?, ?, ?, 'theme.mp3', 'themerrdb', 'vid', ?, ?)",
                (placement_media_type, tmdb_id, section_id, edition_key, now,
                 last_reason),
            )
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            " edition_key, media_folder, placed_at, placement_kind, "
            " plex_refreshed, theme_present) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
            (placement_media_type, tmdb_id, section_id, edition_key,
             media_folder, now, placement_kind, theme_present),
        )
        if with_plex_item:
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type, "
                " title, year, folder_path, edition_key, has_theme, "
                " first_seen_at, last_seen_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
                (f"rk-{tmdb_id}", section_id, plex_media_type, title, year,
                 media_folder, edition_key, now, now),
            )
        conn.commit()


def _place_jobs(db: Path) -> list[sqlite3.Row]:
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT * FROM jobs WHERE job_type='place'").fetchall()


# ── the fix: a missing sidecar with a surviving canonical is re-placed ──

def test_restores_when_sidecar_missing_and_canonical_survives(db, tmp_path,
                                                              monkeypatch):
    folder = tmp_path / "movies" / "Avenue Q (2003)"
    folder.mkdir(parents=True)  # folder exists, but NO theme.mp3 (Plex re-add)
    _seed(db, media_folder=str(folder))

    fired = []
    monkeypatch.setattr(
        "app.core.notify.dispatch",
        lambda *a, **k: fired.append(k or a))

    scheduler._restore_lost_placements(_settings(db))

    jobs = _place_jobs(db)
    assert len(jobs) == 1
    assert jobs[0]["status"] == "pending"
    payload = json.loads(jobs[0]["payload"])
    assert payload["edition_key"] == ""
    # v1.24.38 (review #6): the sweep now ENQUEUES tagged with reason=auto_restore
    # and does NOT dispatch at enqueue — the worker fires theme_auto_restored on
    # the place ACTUALLY landing, so a place that later fails can't claim success.
    assert payload["reason"] == "auto_restore"
    assert not fired, "no notification should fire at enqueue time"


def test_restore_carries_the_edition_key(db, tmp_path, monkeypatch):
    folder = tmp_path / "movies" / "LOTR {edition-Extended}"
    folder.mkdir(parents=True)
    _seed(db, media_folder=str(folder), tmdb_id=120, edition_key="extended")
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)

    scheduler._restore_lost_placements(_settings(db))

    jobs = _place_jobs(db)
    assert len(jobs) == 1
    assert json.loads(jobs[0]["payload"])["edition_key"] == "extended"


def test_show_plex_item_bridges_to_tv_placement(db, tmp_path, monkeypatch):
    # plex_items.media_type='show' must bridge to a 'tv' placement via the CASE.
    folder = tmp_path / "tv" / "Some Show"
    folder.mkdir(parents=True)
    _seed(db, media_folder=str(folder), tmdb_id=200, section_id="2",
          placement_media_type="tv", plex_media_type="show")
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)

    scheduler._restore_lost_placements(_settings(db))
    assert len(_place_jobs(db)) == 1


# ── gating: each guard suppresses a re-place ────────────────────────────

def test_skipped_when_theme_present(db, tmp_path):
    folder = tmp_path / "f"
    folder.mkdir()
    _seed(db, media_folder=str(folder), theme_present=1)  # not missing
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_no_canonical_local_file(db, tmp_path):
    # No local_files row → motif has no backup to re-place → JOIN drops it.
    folder = tmp_path / "f"
    folder.mkdir()
    _seed(db, media_folder=str(folder), with_local_file=False)
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_backup_only_intent(db, tmp_path):
    folder = tmp_path / "f"
    folder.mkdir()
    _seed(db, media_folder=str(folder), last_reason="backup_only")
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_plex_has_theme(db, tmp_path):
    folder = tmp_path / "f"
    folder.mkdir()
    _seed(db, media_folder=str(folder), last_reason="plex_has_theme")
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_placement_error(db, tmp_path):
    folder = tmp_path / "f"
    folder.mkdir()
    _seed(db, media_folder=str(folder),
          last_reason="placement_error:[Errno 28] No space left")
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_plex_upload_kind(db, tmp_path):
    # plex_upload placements live in Plex's metadata store (media_folder=''),
    # not on a folder we can re-stat — out of scope for this sidecar sweep.
    _seed(db, media_folder="", placement_kind="plex_upload")
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_no_live_plex_item(db, tmp_path):
    # A placement whose folder no longer maps to any live plex_items row is
    # stale — re-placing could target a wrong/divergent dir. Skip it.
    folder = tmp_path / "f"
    folder.mkdir()
    _seed(db, media_folder=str(folder), with_plex_item=False)
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


def test_skipped_when_place_job_already_in_flight(db, tmp_path):
    folder = tmp_path / "f"
    folder.mkdir()
    _seed(db, media_folder=str(folder), tmdb_id=100, edition_key="")
    now = _now()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, "
            " payload, status, created_at, next_run_at) "
            "VALUES ('place', 'movie', 100, '1', ?, 'pending', ?, ?)",
            (json.dumps({"edition_key": ""}), now, now))
        conn.commit()
    scheduler._restore_lost_placements(_settings(db))
    # still exactly the one we pre-seeded — no duplicate enqueued.
    assert len(_place_jobs(db)) == 1


def test_in_flight_dedup_is_per_edition(db, tmp_path, monkeypatch):
    # A pending place job for edition A must NOT block edition B's restore.
    folder = tmp_path / "f"
    folder.mkdir()
    _seed(db, media_folder=str(folder), tmdb_id=130, edition_key="director")
    now = _now()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, "
            " payload, status, created_at, next_run_at) "
            "VALUES ('place', 'movie', 130, '1', ?, 'pending', ?, ?)",
            (json.dumps({"edition_key": "theatrical"}), now, now))
        conn.commit()
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)
    scheduler._restore_lost_placements(_settings(db))
    # the pre-seeded theatrical job + a new director restore = 2 place jobs.
    eds = sorted(json.loads(j["payload"])["edition_key"] for j in _place_jobs(db))
    assert eds == ["director", "theatrical"]


def test_restat_skips_when_sidecar_actually_present(db, tmp_path):
    # theme_present=0 is a STALE stamp (the file is actually there now). The
    # authoritative re-stat must catch this and NOT re-place.
    folder = tmp_path / "f"
    folder.mkdir()
    (folder / "theme.mp3").write_bytes(b"id3")  # sidecar is present after all
    _seed(db, media_folder=str(folder))
    scheduler._restore_lost_placements(_settings(db))
    assert _place_jobs(db) == []


# ── source pins: wired into the scheduler + the notification registries ──

def test_function_registered_in_start_scheduler():
    src = (REPO / "app" / "core" / "scheduler.py").read_text()
    assert 'id="restore_lost_placements"' in src
    assert "_restore_lost_placements, args=[settings]" in src


def test_event_kind_registered_in_notify_and_config():
    cfg = (REPO / "app" / "core" / "config_file.py").read_text()
    assert '"theme_auto_restored":' in cfg
    levels = (REPO / "app" / "core" / "notify.py").read_text()
    assert '"theme_auto_restored":' in levels


def test_settings_toggle_present():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert "notifications.events.theme_auto_restored" in html
