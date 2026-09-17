"""v0.51.344 PB-062: a restore waits on any queued download that writes its canonical's path, not only its own row's."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import closing
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from app.core import worker as worker_mod
from app.core.canonical import canonical_theme_rel
from app.core.db import get_conn, init_db
from test_v0_51_342_restore_from_plex_job import AUTH, _ago, env  # noqa: F401 — env is the endpoints' fixture
from test_v0_51_342_restore_pool import FakePlex

SIDECAR = bytes(range(256)) * 64
URL = "https://www.youtube.com/watch?v=abcdefghijk"
TITLE, YEAR = "Shared: Title", "2001"


@pytest.fixture(autouse=True)
def fresh_in_flight_paths(monkeypatch):
    """Each test starts with no in-flight paths remembered from another run."""
    monkeypatch.setattr(ch, "_IN_FLIGHT_DOWNLOADS", {}, raising=False)


def _db(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    return db, tmp_path / "themes", tmp_path / "plex"


def _theme(db, tmdb, *, title=TITLE, media_type="movie"):
    now = _ago(days=1)
    with closing(sqlite3.connect(db)) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, "
                  "discovered_at, last_seen_at) VALUES ('1', 'M', 'movie', 0, 0, 'movies', 1, ?, ?) "
                  "ON CONFLICT(section_id) DO NOTHING", (now, now))
        c.execute("INSERT INTO themes (media_type, tmdb_id, title, year, upstream_source, last_seen_sync_at, "
                  "first_seen_sync_at, youtube_url) VALUES (?, ?, ?, ?, 'imdb', ?, ?, ?)",
                  (media_type, tmdb, title, YEAR, now, now, URL))
        c.commit()


def _path(*, title=TITLE, edition="", media_type="movie", subdir="movies"):
    """Where a download of this title writes theme.mp3, relative to themes_dir."""
    return str(Path(canonical_theme_rel(media_type, subdir, title, YEAR, edition)) / "theme.mp3")


def _broken(db, plexdir, tmdb, file_path, *, edition="", store=False, media_type="movie", placement=True,
            section="1"):
    """A broken canonical recorded at file_path; its copy survives in its Plex folder, or with store in Plex's store."""
    now = _ago(days=1)
    folder = plexdir / f"{tmdb}{edition}"
    if placement and not store:
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "theme.mp3").write_bytes(SIDECAR)
    with closing(sqlite3.connect(db)) as c:
        theme_id = c.execute("SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
                             (media_type, tmdb)).fetchone()[0]
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, theme_id, file_path, "
                  "file_size, file_sha256, downloaded_at, source_video_id, provenance, source_kind, canonical_present) "
                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', 'manual', 'upload', 0)",
                  (media_type, tmdb, section, edition, theme_id, file_path, len(SIDECAR),
                   hashlib.sha256(SIDECAR).hexdigest(), now))
        if placement:
            c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind, "
                      "provenance, placed_at, plex_rating_key, edition_key) VALUES (?, ?, ?, ?, ?, 'manual', ?, ?, ?)",
                      (media_type, tmdb, section, "" if store else str(folder), "plex_upload" if store else "hardlink", now,
                       f"9{tmdb}" if store else None, edition))
        c.commit()


def _download(db, tmdb, *, edition="", media_type="movie", status="pending", section="1"):
    with closing(sqlite3.connect(db)) as c:
        cur = c.execute("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, payload, status, created_at) "
                        "VALUES ('download', ?, ?, ?, ?, ?, ?)",
                        (media_type, tmdb, section, json.dumps({"edition_key": edition}), status, _ago(minutes=1)))
        c.commit()
        return cur.lastrowid


def test_an_edition_row_on_the_untagged_folder_waits_on_that_folders_download(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    _theme(db, 2201)
    untagged = _path()
    _broken(db, plexdir, 2201, untagged, edition="extended")
    _download(db, 2201, edition="")
    res = ch.restore_from_plex(db, themes, None)
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(2201, "download_in_flight")]
    assert res["restored"] == 0
    assert not (themes / untagged).exists(), "the untagged folder's theme.mp3 was restored under its queued download"


def test_the_info_card_restore_waits_on_the_untagged_folders_download_too(env):
    client, settings, tmp_path, _events = env
    db = settings.db_path
    _theme(db, 2202)
    untagged = _path()
    _broken(db, tmp_path / "plex", 2202, untagged, edition="extended")
    _download(db, 2202, edition="")
    answer = client.post("/api/items/movie/2202/restore-canonical", headers=AUTH).json()
    assert (answer["restored"], [s["reason"] for s in answer["skipped"]]) == (0, ["download_in_flight"])
    assert not (settings.themes_dir / untagged).exists()


@pytest.mark.parametrize("leg", ["sidecar", "store"])
def test_a_same_title_tmdbs_download_on_this_rows_path_holds_it_and_plex_is_never_asked(tmp_path, leg):
    db, themes, plexdir = _db(tmp_path)
    _theme(db, 2101)
    _theme(db, 2102)
    shared = _path()
    _broken(db, plexdir, 2101, shared, store=leg == "store")
    _download(db, 2102)
    plex = FakePlex()
    res = ch.restore_from_plex(db, themes, None, plex_client_factory=lambda: plex)
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(2101, "download_in_flight")]
    assert not (themes / shared).exists(), "a restore wrote the theme.mp3 another tmdb's queued download writes"
    assert plex.meter.calls == [], "Plex was asked for bytes the publish would refuse"


@pytest.mark.parametrize("leg", ["sidecar", "store"])
def test_a_download_writing_another_path_in_the_section_holds_nothing(tmp_path, leg):
    db, themes, plexdir = _db(tmp_path)
    _theme(db, 2101)
    _theme(db, 2103, title="Another Title")
    mine = _path()
    assert _path(title="Another Title") != mine, "premise: the download writes another path"
    _broken(db, plexdir, 2101, mine, store=leg == "store")
    _download(db, 2103)
    res = ch.restore_from_plex(db, themes, None, plex_client_factory=FakePlex)
    assert (res["restored"], res["skipped"]) == (1, []), "a download on another path held this row"
    assert (themes / mine).is_file()


def test_a_download_writing_this_rows_path_in_another_letter_case_holds_it(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    _theme(db, 2101, title=TITLE.lower())
    _theme(db, 2102)
    mine = _path(title=TITLE.lower())
    assert mine != _path() and mine.casefold() == _path().casefold(), "premise: one path on a case-blind share"
    _broken(db, plexdir, 2101, mine)
    _download(db, 2102)
    res = ch.restore_from_plex(db, themes, None)
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(2101, "download_in_flight")]
    assert not (themes / mine).exists(), "a restore wrote the theme.mp3 a download writes under another letter case"


class _Wrote(Exception):
    """The fake yt-dlp step: the worker has chosen its output folder."""


@pytest.mark.parametrize(("media_type", "edition"), [("movie", ""), ("movie", "extended"), ("collection", "")])
def test_the_path_the_download_worker_writes_is_the_path_the_check_holds(tmp_path, monkeypatch, media_type, edition):
    from app.config import Settings
    from app.core.runtime import set_dry_run
    from app.core.worker import TokenBucket, Worker
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    (tmp_path / "themes").mkdir()
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    settings._cfg.paths.min_free_disk_mb = 0
    set_dry_run(settings.db_path, False, updated_by="test")
    db, themes = settings.db_path, settings.themes_dir
    monkeypatch.setattr(worker_mod, "log_event", lambda *a, **k: None)
    wrote: list[Path] = []

    def download_theme(*, output_dir, **_kw):
        wrote.append(output_dir)
        raise _Wrote()
    monkeypatch.setattr(worker_mod, "download_theme", download_theme)
    _theme(db, 2102, media_type=media_type)
    job_id = _download(db, 2102, edition=edition, media_type=media_type, status="running")
    with closing(sqlite3.connect(db)) as c:
        c.row_factory = sqlite3.Row
        job = c.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    with pytest.raises(_Wrote):
        Worker(settings=settings, stop_event=threading.Event(), bucket=TokenBucket(60, 60))._do_download(job)
    written = str((wrote[0] / "theme.mp3").relative_to(themes))
    # another tmdb of the same title, recorded at the path the real worker writes: only the path can hold it
    _theme(db, 2101, media_type=media_type)
    _broken(db, tmp_path / "plex", 2101, written, media_type=media_type, placement=False)
    with get_conn(db) as conn:
        row = conn.execute("SELECT * FROM local_files WHERE tmdb_id = 2101").fetchone()
        assert ch._download_in_flight(conn, row), f"the worker writes {written}; the check did not hold a row there"


class _QueuesMidFetch:
    """Plex's store: while the bytes are on their way, on_fetch runs."""
    def __init__(self, on_fetch):
        self.on_fetch = on_fetch

    def get_themes(self, *, rating_key):
        return {"ok": True, "http_status": 200, "error": None,
                "body": {"MediaContainer": {"Metadata": [{"ratingKey": "upload://themes/a", "selected": True}]}}}

    def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
        self.on_fetch()
        return {"ok": True, "http_status": 200, "bytes": SIDECAR}

    def close(self):
        return None


def test_a_download_queued_on_this_rows_path_while_plex_sends_its_bytes_holds_the_publish(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    _theme(db, 2101)
    _theme(db, 2102)
    shared = _path()
    _broken(db, plexdir, 2101, shared, store=True)
    queued: list[int] = []
    res = ch.restore_from_plex(db, themes, _QueuesMidFetch(lambda: queued.append(_download(db, 2102))))
    assert len(queued) == 1, "premise: nothing held the row before Plex was asked for its bytes"
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(2101, "download_in_flight")]
    assert not (themes / shared).exists(), "the publish landed on the path a download queued mid-fetch writes"


def test_a_download_that_ends_mid_run_stops_holding_the_rows_on_its_path(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    _theme(db, 2100, title="Alpha Title")
    _theme(db, 2101)
    _theme(db, 2102)
    first, shared = _path(title="Alpha Title"), _path()
    _broken(db, plexdir, 2100, first)
    _broken(db, plexdir, 2101, shared)
    job = _download(db, 2102)
    seen: list[str] = []

    def progress(done, total, counts):
        if done == 1 and not seen:
            with closing(sqlite3.connect(db)) as c:
                seen.append(c.execute("SELECT status FROM jobs WHERE id = ?", (job,)).fetchone()[0])
                # v0.51.344: as every end-writer does — the in-flight memo's key reads the finished_at stamp (R2-F9)
                c.execute("UPDATE jobs SET status = 'failed', finished_at = ? WHERE id = ?", (_ago(), job))
                c.commit()
    res = ch.restore_from_plex(db, themes, None, progress_cb=progress)
    assert seen == ["pending"], "premise: the first row in the section was checked while the download was queued"
    assert (res["restored"], res["skipped"]) == (2, []), "a download that had ended still held a row on its path"
    assert (themes / shared).is_file()


def _vm_steps(root, monkeypatch, rows, backlog):
    """SQLite VM instructions one restore_from_plex runs: `rows` restorable rows, `backlog` other titles' downloads queued."""
    from contextlib import contextmanager

    from app.core import db as db_mod
    root.mkdir()
    db, themes, plexdir = _db(root)
    for i in range(rows):
        _theme(db, 3000 + i, title=f"Row {i:03d}")
        _broken(db, plexdir, 3000 + i, _path(title=f"Row {i:03d}"))
    now = _ago(days=1)
    with closing(sqlite3.connect(db)) as c:
        c.executemany("INSERT INTO themes (media_type, tmdb_id, title, year, upstream_source, last_seen_sync_at, "
                      "first_seen_sync_at, youtube_url) VALUES ('movie', ?, ?, ?, 'imdb', ?, ?, ?)",
                      [(5000 + i, f"Queued {i:04d}", YEAR, now, now, URL) for i in range(backlog)])
        c.executemany("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, payload, status, created_at) "
                      "VALUES ('download', 'movie', ?, '1', '{}', 'pending', ?)",
                      [(5000 + i, now) for i in range(backlog)])
        c.commit()
    steps = [0]

    def step():
        steps[0] += 1
        return 0
    real = db_mod.get_conn

    @contextmanager
    def counted(path):
        with real(path) as conn:
            conn.set_progress_handler(step, 10)
            yield conn
    monkeypatch.setattr(db_mod, "get_conn", counted)
    try:
        res = ch.restore_from_plex(db, themes, None)
    finally:
        monkeypatch.setattr(db_mod, "get_conn", real)
    assert (res["restored"], res["skipped"]) == (rows, []), "premise: every row restores under the unrelated backlog"
    return steps[0]


def test_a_queued_download_backlog_is_read_once_per_section_not_once_per_restored_row(tmp_path, monkeypatch):
    backlog = 150
    extra = {rows: _vm_steps(tmp_path / f"r{rows}-b{backlog}", monkeypatch, rows, backlog)
             - _vm_steps(tmp_path / f"r{rows}-b0", monkeypatch, rows, 0) for rows in (8, 32)}
    assert extra[8] > 0, "premise: the check reads the section's queued downloads"
    assert extra[32] < 2 * extra[8], f"4x the rows re-read the queued downloads about 4x as often: {extra}"


def test_a_later_restore_never_reuses_the_downloads_an_earlier_run_read(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    _theme(db, 2101)
    _theme(db, 2102)
    shared = _path()
    _broken(db, plexdir, 2101, shared)
    job = _download(db, 2102)
    first = ch.restore_from_plex(db, themes, None)
    assert [s["reason"] for s in first["skipped"]] == ["download_in_flight"], "premise: the queued download held the row"
    with closing(sqlite3.connect(db)) as c:
        # v0.51.344: as every end-writer does — the in-flight memo's key reads the finished_at stamp (R2-F9)
        c.execute("UPDATE jobs SET status = 'failed', finished_at = ? WHERE id = ?", (_ago(), job))
        c.commit()
    second = ch.restore_from_plex(db, themes, None)
    assert (second["restored"], second["skipped"]) == (1, []), "a new run answered from the downloads an earlier run read"
    assert (themes / shared).is_file()


def test_downloads_read_for_one_section_never_answer_for_another(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    _theme(db, 2100, title="Alpha Title")
    _theme(db, 2101)
    _theme(db, 2102)
    now = _ago(days=1)
    with closing(sqlite3.connect(db)) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, "
                  "discovered_at, last_seen_at) VALUES ('2', 'M4K', 'movie', 0, 1, 'movies-4k', 1, ?, ?)", (now, now))
        c.commit()
    _broken(db, plexdir, 2100, _path(title="Alpha Title"))
    shared_4k = _path(subdir="movies-4k")
    _broken(db, plexdir, 2101, shared_4k, section="2")
    _download(db, 2102, section="2")
    res = ch.restore_from_plex(db, themes, None)
    assert res["restored"] == 1, "premise: the first section's row restores"
    assert [(s["tmdb_id"], s["section_id"], s["reason"]) for s in res["skipped"]] == [(2101, "2", "download_in_flight")]
    assert not (themes / shared_4k).exists(), "a row was answered from another section's queued downloads"
