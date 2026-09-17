"""v0.51.344 (R2-F8): one spelling for every canonical writer.

The download worker, UPLOAD MP3, DOWNLOAD PLEX BACKUP, ADOPT, the v55 recovery walk and RESTORE FROM PLEX's
in-flight check resolve an item's theme.mp3 to the same relative path — a collection's nests under collections/
for all of them, so a later download never repoints a hand-uploaded collection theme and orphans the file — and a
library-folder rename moves the section's collections/ tree and rows along with its plain tree.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
from contextlib import closing
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.core import canonical_health as ch
from app.core import worker as worker_mod
from app.core.canonical import canonical_theme_rel
from app.core.db import get_conn, init_db
from app.core.downloader import DownloadResult
from app.core.editions import edition_key_for_folder
from app.core.recovery_v55 import _expected_canonical_path
from test_v0_51_339_canonical_health_restore import AUTH, NOW, admin_client  # noqa: F401 — admin_client is a fixture

MP3 = b"ID3" + b"\x00" * 64              # passes _looks_like_audio
CLOUD = b"ID3" + b"cloud-bytes" * 8
FETCHED = b"ID3" + b"yt-dlp-bytes" * 8
SIDECAR = b"ID3" + b"sidecar-bytes" * 8
URL = "https://www.youtube.com/watch?v=abcdefghijk"
SECTIONS = {"movies": ("1", "Movies", "movie", 0), "tv": ("2", "TV Shows", "show", 0), "anime": ("3", "Anime", "show", 1)}
PLEX_MT = {"movie": "movie", "tv": "show", "collection": "collection"}


@pytest.fixture
def env(admin_client, monkeypatch):
    """The real app over a fresh database; the worker may download; history and events are stubbed."""
    client, settings, tmp_path = admin_client
    from app.core.runtime import set_dry_run
    settings._cfg.paths.min_free_disk_mb = 0
    set_dry_run(settings.db_path, False, updated_by="test")
    monkeypatch.setattr("app.core.revisions.capture_revision", lambda *a, **k: None)
    monkeypatch.setattr(worker_mod, "log_event", lambda *a, **k: None)
    monkeypatch.setattr(ch, "_IN_FLIGHT_DOWNLOADS", {}, raising=False)
    return client, settings, tmp_path


def _section(conn, subdir):
    sid, title, type_, anime = SECTIONS[subdir]
    conn.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, "
                 "discovered_at, last_seen_at) VALUES (?, ?, ?, ?, 0, ?, 1, ?, ?) ON CONFLICT(section_id) DO NOTHING",
                 (sid, title, type_, anime, subdir, NOW, NOW))
    return sid


def _theme(conn, media_type, tmdb, title, year):
    cur = conn.execute("INSERT INTO themes (media_type, tmdb_id, title, year, upstream_source, last_seen_sync_at, "
                       "first_seen_sync_at, youtube_url) VALUES (?, ?, ?, ?, 'themoviedb', ?, ?, ?)",
                       (media_type, tmdb, title, year, NOW, NOW, URL))
    return cur.lastrowid


def _plex_item(conn, rk, sid, media_type, theme_id, tmdb, title, year, folder):
    conn.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, year, "
                 "edition_key, folder_path, has_theme, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
                 (rk, sid, PLEX_MT[media_type], theme_id, tmdb, title, year, edition_key_for_folder(folder), folder, NOW, NOW))


def _job(conn, media_type, tmdb, sid, edition, status="running"):
    cur = conn.execute("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, payload, status, created_at) "
                       "VALUES ('download', ?, ?, ?, ?, ?, ?)",
                       (media_type, tmdb, sid, json.dumps({"edition_key": edition}), status, NOW))
    return cur.lastrowid


def _run_download(settings, job_id):
    with closing(sqlite3.connect(settings.db_path)) as c:
        c.row_factory = sqlite3.Row
        job = c.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    worker_mod.Worker(settings=settings, stop_event=threading.Event(),
                      bucket=worker_mod.TokenBucket(60, 60))._do_download(job)


class _Chose(Exception):
    """The fake yt-dlp step: the worker has chosen its output folder."""


def _download_writes(settings, monkeypatch, job_id) -> str:
    """The path the real _do_download hands yt-dlp, relative to themes_dir."""
    wrote: list[Path] = []

    def download_theme(*, output_dir, **_kw):
        wrote.append(output_dir)
        raise _Chose()
    monkeypatch.setattr(worker_mod, "download_theme", download_theme)
    with pytest.raises(_Chose):
        _run_download(settings, job_id)
    return str((wrote[0] / "theme.mp3").relative_to(settings.themes_dir))


def _upload_writes(client, rk) -> str:
    r = client.post(f"/api/plex_items/{rk}/upload-theme", headers=AUTH,
                    files={"file": ("theme.mp3", MP3, "audio/mpeg")})
    assert r.status_code == 200, r.text
    return r.json()["file_path"]


def _cloud_backup_writes(settings, media_type, tmdb, sid, title, year, edition) -> str:
    from app.core.cloud_theme_backup import backup_cloud_theme
    plex = MagicMock()
    plex._rk_path.return_value = f"/library/metadata/rk-{tmdb}/file"
    plex._headers = {}
    resp = MagicMock()
    resp.status_code, resp.content, resp.text, resp.headers = 200, CLOUD, "", {}
    plex._client.get.return_value = resp
    target = {"rating_key": f"rk-{tmdb}", "guid_tmdb": tmdb, "media_type": media_type, "section_id": sid,
              "title": title, "year": year, "edition_key": edition,
              "entry_uri": "metadata://themes/" + "c" * 40, "sha1": "c" * 40}
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    try:
        result = backup_cloud_theme(conn, target, settings.themes_dir, plex)
        conn.commit()
    finally:
        conn.close()
    assert result["ok"] is True and result["file_path"], result
    return result["file_path"]


def _adopt_writes(settings, sid, section_type, theme_id, media_folder: Path) -> str:
    from app.core.adopt import _do_adopt
    src = media_folder / "theme.mp3"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_bytes(SIDECAR)
    finding = {"section_id": sid, "section_type": section_type, "finding_kind": "content_mismatch",
               "theme_id": theme_id, "file_path": str(src), "file_sha256": hashlib.sha256(SIDECAR).hexdigest(),
               "file_size": len(SIDECAR), "media_folder": str(media_folder)}
    out = _do_adopt(settings.db_path, finding, settings, decided_by="testadmin")
    return str(Path(out["canonical_path"]).relative_to(settings.themes_dir))


# ── the finding's scenario ───────────────────────────────────────────

def test_a_collections_uploaded_theme_is_what_its_later_download_replaces_not_a_second_file(env, monkeypatch):
    client, settings, _tmp = env
    db, themes = settings.db_path, settings.themes_dir
    with sqlite3.connect(db) as conn:
        sid = _section(conn, "movies")
        tid = _theme(conn, "collection", 5201, "Wizard Collection", "2001")
        _plex_item(conn, "95201", sid, "collection", tid, 5201, "Wizard Collection", "2001", "")
        job = _job(conn, "collection", 5201, sid, "")
        conn.commit()
    uploaded = _upload_writes(client, "95201")
    assert (themes / uploaded).read_bytes() == MP3, "premise: the upload landed where the route says"

    def download_theme(*, output_dir, video_id, **_kw):
        output_dir.mkdir(parents=True, exist_ok=True)
        p = output_dir / "theme.mp3"
        p.write_bytes(FETCHED)
        return DownloadResult(file_path=p, file_size=len(FETCHED),
                              file_sha256=hashlib.sha256(FETCHED).hexdigest(), video_id=video_id)
    monkeypatch.setattr(worker_mod, "download_theme", download_theme)
    _run_download(settings, job)
    with get_conn(db) as conn:
        rows = conn.execute("SELECT file_path, source_kind FROM local_files WHERE media_type = 'collection' "
                            "AND tmdb_id = 5201").fetchall()
    assert [(r["file_path"], r["source_kind"]) for r in rows] == [(uploaded, "themerrdb")], (
        "the download repointed the collection's row away from the canonical the upload wrote")
    on_disk = sorted(str(p.relative_to(themes)) for p in themes.rglob("theme.mp3"))
    assert on_disk == [uploaded], f"the uploaded file was left on disk with no row referencing it: {on_disk}"
    assert (themes / uploaded).read_bytes() == FETCHED, "the download replaced the canonical in place"


# ── parity across every writer ───────────────────────────────────────

CASES = [
    ("movie", "movies", "Plain: Title", "2001", ""),
    ("tv", "tv", "Show Title", "1999", ""),
    ("tv", "anime", "Anime Title", "2010", ""),
    ("movie", "movies", "LotR", "2001", "extended"),
    ("collection", "movies", "Wizard Collection", "2001", ""),
    ("collection", "anime", "Willy Wonka Collection", None, ""),
]


@pytest.mark.parametrize(("media_type", "subdir", "title", "year", "edition"), CASES,
                         ids=["movie", "tv", "anime", "edition", "collection", "collection-no-year"])
def test_every_canonical_writer_resolves_an_items_theme_to_one_path(env, monkeypatch, media_type, subdir, title,
                                                                    year, edition):
    client, settings, tmp_path = env
    db, themes = settings.db_path, settings.themes_dir
    folder = tmp_path / "plex" / ((f"{title} ({year})" if year else title) + (f" {{edition-{edition}}}" if edition else ""))
    assert edition_key_for_folder(str(folder)) == edition, "premise: the folder's tag parses to the case's edition"
    with sqlite3.connect(db) as conn:
        sid = _section(conn, subdir)
        tid = _theme(conn, media_type, 5101, title, year)
        _theme(conn, media_type, 5102, title, year)   # a same-title tmdb: only the PATH can tie its download to 5101's row
        _plex_item(conn, "95101", sid, media_type, tid, 5101, title, year, str(folder))
        job = _job(conn, media_type, 5101, sid, edition)
        conn.commit()
    resolved = {
        "download": _download_writes(settings, monkeypatch, job),     # first: no file to stash yet
        "upload": _upload_writes(client, "95101"),
        "cloud backup": _cloud_backup_writes(settings, media_type, 5101, sid, title, year, edition),
        "adopt": _adopt_writes(settings, sid, SECTIONS[subdir][2], tid, folder),
    }
    # the v55 walk predates editions: it looks for the untagged canonical (per-edition ones are backfilled later)
    recovery = str(_expected_canonical_path(themes, subdir, title, year, media_type=media_type).relative_to(themes))
    if edition:
        assert recovery == str(Path(canonical_theme_rel(media_type, subdir, title, year, "")) / "theme.mp3")
    else:
        resolved["v55 recovery"] = recovery
    one = resolved["upload"]
    assert set(resolved.values()) == {one}, f"the writers spell this item's canonical differently: {resolved}"
    assert one == str(Path(canonical_theme_rel(media_type, subdir, title, year, edition)) / "theme.mp3"), (
        "a writer's path is not the helper's — RESTORE FROM PLEX's in-flight check reads the helper")
    assert one.startswith("collections/") is (media_type == "collection"), (
        "v1.18.2: a collection nests under collections/; nothing else does")
    # RESTORE FROM PLEX's in-flight check: the same-title tmdb's queued download holds 5101's row by its path alone
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE jobs SET status = 'done', finished_at = ? WHERE id = ?", (NOW, job))
        _job(conn, media_type, 5102, sid, edition, status="pending")
        conn.commit()
    with get_conn(db) as conn:
        row = conn.execute("SELECT * FROM local_files WHERE media_type = ? AND tmdb_id = 5101 AND edition_key = ?",
                           (media_type, edition)).fetchone()
        assert row["file_path"] == one, "premise: the row records the path the writers agreed on"
        assert ch._download_in_flight(conn, row), "the in-flight check does not hold a row on the path the writers use"


# ── a library-folder rename moves the collections/ tree too ──────────

def _renamed_section(tmp_path):
    """A section stored at 'oldmovies' whose library folder now names it 'movies', with a movie and a collection."""
    db, themes = tmp_path / "m.db", tmp_path / "themes"
    init_db(db)
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, included, is_anime, is_4k, themes_subdir, "
                  "location_paths, discovered_at, last_seen_at) VALUES ('s', 'Movies', 'movie', 1, 0, 0, 'oldmovies', "
                  "'[]', ?, ?)", (NOW, NOW))
        for mt, tmdb, title in (("movie", 7001, "Foo"), ("collection", 7002, "Foo Collection")):
            _theme(c, mt, tmdb, title, "2020")
            rel = str(Path(canonical_theme_rel(mt, "oldmovies", title, "2020")) / "theme.mp3")
            (themes / rel).parent.mkdir(parents=True)
            (themes / rel).write_bytes(title.encode())
            c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, source_video_id, "
                      "source_kind, downloaded_at) VALUES (?, ?, 's', ?, 'vid', 'themerrdb', ?)", (mt, tmdb, rel, NOW))
        c.commit()
    return db, themes


def _after(db):
    with get_conn(db) as c:
        rows = {r["media_type"]: r["file_path"] for r in c.execute("SELECT media_type, file_path FROM local_files")}
        subdir = c.execute("SELECT themes_subdir FROM plex_sections WHERE section_id = 's'").fetchone()[0]
    return rows, subdir


def test_a_library_folder_rename_moves_the_sections_collection_tree_and_its_rows_with_it(tmp_path):
    from app.core.sections import migrate_themes_subdirs_inplace
    db, themes = _renamed_section(tmp_path)
    assert migrate_themes_subdirs_inplace(db, themes) == 1
    rows, subdir = _after(db)
    assert subdir != "oldmovies", "premise: the section took its library folder's name"
    for mt, title in (("movie", "Foo"), ("collection", "Foo Collection")):
        expect = str(Path(canonical_theme_rel(mt, subdir, title, "2020")) / "theme.mp3")
        assert rows[mt] == expect, f"{mt}: the row did not follow the section to '{subdir}'"
        assert (themes / expect).read_bytes() == title.encode(), f"{mt}: the file did not move with the section"
    assert not (themes / "collections" / "oldmovies").exists() and not (themes / "oldmovies").exists()


def test_a_collection_tree_that_cannot_move_keeps_its_rows_with_its_files_and_says_so(tmp_path, caplog):
    from app.core.sections import migrate_themes_subdirs_inplace
    db, themes = _renamed_section(tmp_path)
    (themes / "collections" / "movies").mkdir(parents=True)     # the destination is taken
    with caplog.at_level(logging.WARNING, logger="app.core.sections"):
        assert migrate_themes_subdirs_inplace(db, themes) == 1
    rows, subdir = _after(db)
    assert subdir == "movies", "premise: the section itself still migrates"
    moved = str(Path(canonical_theme_rel("movie", "movies", "Foo", "2020")) / "theme.mp3")
    stayed = str(Path(canonical_theme_rel("collection", "oldmovies", "Foo Collection", "2020")) / "theme.mp3")
    assert rows["movie"] == moved and (themes / moved).read_bytes() == b"Foo"
    assert rows["collection"] == stayed and (themes / stayed).read_bytes() == b"Foo Collection", (
        "a collection row was rewritten to a folder its file never moved to")
    assert [r.levelname for r in caplog.records
            if "section s's collection themes stay on the old subdir" in r.getMessage()] == ["WARNING"]
