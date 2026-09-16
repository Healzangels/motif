"""v0.51.342 integration review round 3: canonical health — one row's writers, a download in flight, the page's words, the exit join."""
from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import yaml

from app.core import canonical_health as ch
from app.core import placement
from app.core.db import get_conn, init_db
from test_v0_51_339_canonical_health_restore import _DRIVER, _LEVELLED, _NODE, _NORM_COLS, APP_JS, _app_fn, _report, _row
from test_v0_51_342_restore_from_plex_job import (  # noqa: F401 — env and ssr_running are fixtures
    AUTH, JOB_THREAD, START, HeldRestore, _ago, _finish, _marker, env, ssr_running,
)
from test_v0_51_342_restore_pool import SHARED
from test_v0_51_342_restore_shutdown_cancel import _free_port, _http, _seed_rows, _up

REPO = Path(__file__).resolve().parents[1]
NOW = "2026-09-13T00:00:00"
REPAIR = "/api/admin/canonical-health/repair"
SIDECAR = bytes(range(256)) * 256
URL = "https://www.youtube.com/watch?v=abcdefghijk"


def _item(tmdb):
    return f"/api/items/movie/{tmdb}/restore-canonical"


def _canon(themes, tmdb):
    return themes / "movies" / str(tmdb) / "theme.mp3"


def _seed(db, plexdir, tmdb, *, source_kind="upload", tdb_url=None, plex_item=False, store=False, data=SIDECAR,
          recorded_size=None, extra=None):
    """A broken canonical whose copy survives in its Plex folder (or, with store, in Plex's store)."""
    folder = plexdir / str(tmdb)
    if not store:
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "theme.mp3").write_bytes(data)
    lf = {"media_type": "movie", "tmdb_id": tmdb, "section_id": "1", "edition_key": "", "theme_id": tmdb,
          "file_path": f"movies/{tmdb}/theme.mp3", "file_size": len(data) if recorded_size is None else recorded_size,
          "file_sha256": hashlib.sha256(data).hexdigest(), "downloaded_at": NOW, "source_video_id": "",
          "provenance": "manual", "source_kind": source_kind, "canonical_present": 0, **(extra or {})}
    with closing(sqlite3.connect(db)) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, "
                  "discovered_at, last_seen_at) VALUES ('1', 'M', 'movie', 0, 0, 'movies', 1, ?, ?) "
                  "ON CONFLICT(section_id) DO NOTHING", (NOW, NOW))
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at, "
                  "first_seen_sync_at, youtube_url) VALUES (?, 'movie', ?, ?, ?, ?, ?, ?)",
                  (tmdb, tmdb, f"T{tmdb}", "imdb" if tdb_url else "plex_orphan", NOW, NOW, tdb_url))
        c.execute(f"INSERT INTO local_files ({','.join(lf)}) VALUES ({','.join('?' * len(lf))})", tuple(lf.values()))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind, provenance, "
                  "placed_at, plex_rating_key, edition_key) VALUES ('movie', ?, '1', ?, ?, 'manual', ?, ?, '')",
                  (tmdb, "" if store else str(folder), "plex_upload" if store else "hardlink", NOW,
                   f"9{tmdb}" if store else None))
        if plex_item:
            c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, title, guid_tmdb, folder_path, "
                      "theme_id, first_seen_at, last_seen_at) VALUES (?, '1', 'movie', ?, ?, ?, ?, ?, ?)",
                      (f"7{tmdb}", f"T{tmdb}", tmdb, "" if store else str(folder), tmdb, NOW, NOW))
        c.commit()
    return folder / "theme.mp3"


def _cols(db, tmdb, cols):
    with closing(sqlite3.connect(db)) as c:
        return c.execute(f"SELECT {', '.join(cols)} FROM local_files WHERE tmdb_id = ?", (tmdb,)).fetchone()


def _row_dict(db, tmdb):
    with get_conn(db) as conn:
        lf = conn.execute("SELECT * FROM local_files WHERE tmdb_id = ?", (tmdb,)).fetchone()
        p, _sidecar = ch._placement_for(conn, lf)
    return {**dict(lf), "media_folder": p["media_folder"], "placement_kind": p["placement_kind"]}


def _tmps(themes):
    return sorted(p.name for p in themes.rglob("*.motif-tmp"))


# ── R3-F1: two writers on one row ────────────────────────────────────

class _Contended:
    """The per-path write lock, noting when a second writer has to wait for it."""
    def __init__(self, real, waiting):
        self.real, self.waiting = real, waiting

    def __enter__(self):
        if not self.real.acquire(blocking=False):
            self.waiting.set()
            self.real.acquire()
        return self

    def __exit__(self, *exc):
        self.real.release()


def _hold_the_first_stage(monkeypatch, mode):
    """The first writer stops mid-stage (half its copy written, or its link made). The second writer's
    arrival is its own stage call, or a wait on the path's write lock — whichever the code reaches."""
    lock, calls = threading.Lock(), []
    midway, go, arrived = threading.Event(), threading.Event(), threading.Event()
    real_lock = getattr(ch, "_canonical_write_lock", None)
    if real_lock is not None:
        monkeypatch.setattr(ch, "_canonical_write_lock", lambda p: _Contended(real_lock(p), arrived))

    def first():
        with lock:
            calls.append(1)
            return len(calls) == 1
    if mode == "copy":
        monkeypatch.setattr(placement, "_can_hardlink", lambda src, dst_dir: False)
        real_copy2 = shutil.copy2

        def copy2(src, dst, *a, **k):
            if not first():
                arrived.set()
                return real_copy2(src, dst, *a, **k)
            data = Path(src).read_bytes()
            with open(dst, "wb") as f:
                f.write(data[:len(data) // 2])
                f.flush()
                midway.set()
                assert go.wait(10)
                f.write(data[len(data) // 2:])
            return dst
        monkeypatch.setattr(shutil, "copy2", copy2)
    else:
        real_link = os.link

        def link(src, dst, *a, **k):
            if not first():
                arrived.set()
                return real_link(src, dst, *a, **k)
            real_link(src, dst, *a, **k)
            midway.set()
            assert go.wait(10)
        monkeypatch.setattr(os, "link", link)
    return midway, go, arrived


@pytest.mark.parametrize("mode", ["copy", "hardlink"])
@pytest.mark.parametrize("second", ["restore-from-plex", "another-item-restore"])
def test_a_second_writer_on_a_row_mid_restore_waits_and_finds_it_restored(env, monkeypatch, mode, second):
    client, settings, tmp_path, events = env
    themes, tmdb = tmp_path / "themes", 1801
    sidecar = _seed(settings.db_path, tmp_path / "plex", tmdb, extra=_LEVELLED)
    midway, go, arrived = _hold_the_first_stage(monkeypatch, mode)
    out: dict = {}
    first = threading.Thread(target=lambda: out.update(a=client.post(_item(tmdb), headers=AUTH)), daemon=True)
    first.start()
    other = None
    try:
        assert midway.wait(10), "premise: the INFO card's restore is mid-stage"
        if second == "restore-from-plex":
            assert client.post(START, headers=AUTH).json()["started"] is True
        else:
            other = threading.Thread(target=lambda: out.update(b=client.post(_item(tmdb), headers=AUTH)), daemon=True)
            other.start()
        assert arrived.wait(10), "the second writer never reached the row"
    finally:
        go.set()
    first.join(10)
    if other is not None:
        other.join(10)
        got = out["b"].json()
        b_restored, b_reasons = got["restored"], [s["reason"] for s in got["skipped"]]
    else:
        st = _finish(client)
        b_restored, b_reasons = st["restored"], [s["reason"] for s in st["skipped"]]
    assert out["a"].status_code == 200, out["a"].text
    assert (out["a"].json()["restored"], b_restored, b_reasons) == (1, 0, ["canonical_already_present"]), \
        "the second writer staged the row while the first was mid-restore"
    canonical = _canon(themes, tmdb)
    assert canonical.read_bytes() == SIDECAR
    assert _cols(settings.db_path, tmdb, ("canonical_present", "file_size", "file_sha256")) == (
        1, len(SIDECAR), hashlib.sha256(SIDECAR).hexdigest()), "the stamp is not the bytes on disk"
    assert _cols(settings.db_path, tmdb, _NORM_COLS) == tuple(_LEVELLED[c] for c in _NORM_COLS), \
        "the levelled bytes lost their loudness/norm anchors — // UNDO cannot reverse the gain"
    assert _tmps(themes) == []
    if mode == "hardlink":
        assert canonical.stat().st_ino == sidecar.stat().st_ino


@pytest.mark.parametrize("stage", ["fails", "lands"])
def test_a_restore_removes_only_the_staging_file_it_created(tmp_path, monkeypatch, stage):
    db, themes = tmp_path / "m.db", tmp_path / "themes"
    init_db(db)
    tmdb = 1802
    _seed(db, tmp_path / "plex", tmdb)
    canonical = _canon(themes, tmdb)
    canonical.parent.mkdir(parents=True)
    foreign = canonical.with_name("theme.mp3.motif-tmp")
    foreign.write_bytes(b"another writer's staging")
    monkeypatch.setattr(placement, "_can_hardlink", lambda src, dst_dir: False)
    if stage == "fails":
        def enospc(src, dst, *a, **k):
            Path(dst).write_bytes(Path(src).read_bytes()[:10])
            raise OSError(errno.ENOSPC, "No space left on device")
        monkeypatch.setattr(shutil, "copy2", enospc)
    res = ch.restore_from_placement(db, themes, _row_dict(db, tmdb))
    assert foreign.is_file() and foreign.read_bytes() == b"another writer's staging", \
        "the restore removed a staging file it did not create"
    assert _tmps(themes) == [foreign.name], "the restore left its own staging file behind"
    if stage == "fails":
        assert res["ok"] is False and res["reason"].startswith("link_failed:") and not canonical.exists()
    else:
        assert res == {"ok": True, "kind": "copy"} and canonical.read_bytes() == SIDECAR


def test_one_lock_guards_every_case_spelling_of_a_canonical_path(tmp_path):
    # v0.51.344: N3 — the bulk treats case spellings as one shared path, so the writers' lock must too
    a = ch._canonical_write_lock(tmp_path / "Movies" / "Heat (1995)" / "theme.mp3")
    b = ch._canonical_write_lock(tmp_path / "movies" / "HEAT (1995)" / "theme.mp3")
    other = ch._canonical_write_lock(tmp_path / "tv" / "Heat (1995)" / "theme.mp3")
    assert a is b, "two spellings of one canonical path got two locks — their writers can stage it at once"
    assert a is not other


def test_the_per_item_restore_is_refused_while_restore_from_plex_runs(env, monkeypatch):
    client, settings, tmp_path, events = env
    tmdb = 1803
    _seed(settings.db_path, tmp_path / "plex", tmdb)
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert held.entered.wait(10)
    r = client.post(_item(tmdb), headers=AUTH)
    assert r.status_code == 409 and "RESTORE FROM PLEX is running" in r.json()["detail"], r.text
    assert not _canon(tmp_path / "themes", tmdb).exists() and _cols(settings.db_path, tmdb, ("canonical_present",)) == (0,)
    held.release.set()
    _finish(client)
    r = client.post(_item(tmdb), headers=AUTH)
    assert (r.status_code, r.json()["restored"]) == (200, 1)


def test_a_restore_that_stamps_a_size_other_than_the_recorded_one_is_a_changed_candidate(tmp_path):
    db, themes = tmp_path / "m.db", tmp_path / "themes"
    init_db(db)
    _seed(db, tmp_path / "plex", 1804, recorded_size=len(SIDECAR) * 2)
    _seed(db, tmp_path / "plex", 1805)
    res = ch.restore_from_plex(db, themes, None)
    assert (res["restored_sidecar"], res["skipped"]) == (2, [])
    assert [_cols(db, t, ("canonical_changed_candidate",))[0] for t in (1804, 1805)] == [1, None], \
        "a stamped size that moved must be re-read by CHANGED; the recorded size restored is no candidate"
    with get_conn(db) as conn:
        assert ch.broken_canonical_report(conn, themes)["changed"] == []
    for t in (1804, 1805):
        with _canon(themes, t).open("ab") as f:
            f.write(b"late")  # bytes landing after the stamp — a copy finishing into the published inode
    with get_conn(db) as conn:
        changed = [(r["tmdb_id"], r["recorded"], r["on_disk"]) for r in ch.broken_canonical_report(conn, themes)["changed"]]
    assert changed == [(1804, len(SIDECAR), len(SIDECAR) + 4)]


def test_a_store_publish_meeting_a_sidecar_restore_mid_stage_on_its_path_waits_and_finds_it_restored(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    db, themes, plexdir = settings.db_path, tmp_path / "themes", tmp_path / "plex"
    # the store row sorts first, so the job's shared-path tail publishes it while the INFO card's restore holds its link
    _seed(db, plexdir, 3101, plex_item=True, store=True, extra=_LEVELLED)
    sidecar = _seed(db, plexdir, 3102)
    with closing(sqlite3.connect(db)) as c:
        c.execute("UPDATE local_files SET file_path = ? WHERE tmdb_id = 3101", ("movies/3102/theme.mp3",))
        c.commit()
    recorded = _cols(db, 3101, ("file_size", "file_sha256"))
    midway, go, arrived = _hold_the_first_stage(monkeypatch, "hardlink")
    real_publish = ch._publish_store_bytes

    def publish(*a, **k):
        res = real_publish(*a, **k)
        if not go.is_set():
            arrived.set()  # the publish came back while the sidecar restore was still mid-stage
        return res
    monkeypatch.setattr(ch, "_publish_store_bytes", publish)
    settings._cfg.plex.enabled = True
    settings._cfg.plex.url = "http://plex.test:32400"
    settings._cfg.plex.token = "token-for-test"
    _RecordingPlex.calls = []
    monkeypatch.setattr(api_mod, "PlexClient", _RecordingPlex)
    out: dict = {}
    info = threading.Thread(target=lambda: out.update(a=client.post(_item(3102), headers=AUTH)), daemon=True)
    info.start()
    try:
        assert midway.wait(10), "premise: the INFO card's restore is mid-link"
        assert client.post(START, headers=AUTH).json()["started"] is True
        assert arrived.wait(10), "the job's store publish never reached the path"
    finally:
        go.set()
    info.join(10)
    st = _finish(client)
    assert _RecordingPlex.calls == ["93101"], "premise: the job fetched the store row's bytes to publish them"
    assert (out["a"].status_code, out["a"].json()["restored"]) == (200, 1), out["a"].text
    assert (st["restored_store"], {s["tmdb_id"]: s["reason"] for s in st["skipped"]}) == (
        0, {3101: "canonical_already_present", 3102: "canonical_already_present"}), \
        "the store bytes published under the sidecar restore mid-stage"
    canonical = _canon(themes, 3102)
    disk = canonical.read_bytes()
    assert disk == SIDECAR and canonical.stat().st_ino == sidecar.stat().st_ino
    on_disk = (len(disk), hashlib.sha256(disk).hexdigest())
    assert _cols(db, 3102, ("canonical_present", "file_size", "file_sha256")) == (1, *on_disk)
    assert _cols(db, 3101, ("file_size", "file_sha256")) == recorded == on_disk, \
        "the store row carries a stamp of bytes that are not on disk"
    assert _cols(db, 3101, _NORM_COLS) == tuple(_LEVELLED[c] for c in _NORM_COLS), \
        "the store row lost its loudness/norm anchors to bytes that never stayed"
    assert _tmps(themes) == [] and list(themes.rglob("*.part")) == []


_LIB_DRIVER = r"""
"use strict";
const fs = require("node:fs");
const vm = require("node:vm");
const [, , srcPath, scenarioPath] = process.argv;
const scenario = JSON.parse(fs.readFileSync(scenarioPath, "utf8"));
const queue = scenario.responses.slice();
const calls = [];
let handler = null;
const btn = { textContent: "// RESTORE FROM PLEX (3)", disabled: false };
const ctx = vm.createContext({
  document: { getElementById: (id) => (id === "library-restore-from-plex-btn"
    ? { addEventListener: (type, fn) => { handler = fn; } } : null) },
  libraryState: { selected: new Set(), selectedRows: new Map(), items: scenario.items },
  api: async (method, url) => {
    calls.push(`${method} ${url}`);
    const next = queue.shift();
    if (next && next.__throw) {
      const err = new Error(`${next.__throw.status}: error`);
      err.status = next.__throw.status;
      throw err;
    }
    return next;
  },
  confirm: () => true, alert: (m) => { throw new Error(`alert: ${m}`); }, window: {},
  setTimeout: () => 0, loadLibrary: async () => {}, refreshTopbarStatus: () => {}, libraryRapidPoll: () => {},
});
vm.runInContext(fs.readFileSync(srcPath, "utf8"), ctx);
(async () => {
  await handler({ currentTarget: btn });
  process.stdout.write(JSON.stringify({ text: btn.textContent, calls, left: queue.length }));
})().catch((e) => { console.error(e); process.exit(1); });
"""


def _run_library_loop(work, items, responses):
    """The library page's // RESTORE FROM PLEX click handler, run under node over these rows and answers."""
    work.mkdir(parents=True, exist_ok=True)
    needle = "document.getElementById('library-restore-from-plex-btn')?.addEventListener"
    anchor = APP_JS.index(needle)
    body = APP_JS[anchor:APP_JS.index("')?.addEventListener", anchor + len(needle))]
    (work / "handler.js").write_text(body[:body.rindex("});") + 3])
    (work / "scenario.json").write_text(json.dumps({"items": items, "responses": responses}))
    (work / "driver.js").write_text(_LIB_DRIVER)
    r = subprocess.run([_NODE, str(work / "driver.js"), str(work / "handler.js"), str(work / "scenario.json")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_the_library_restore_loop_counts_what_each_answer_restored(tmp_path):
    items = [{"canonical_missing": True, "file_path": "x", "theme_media_type": "movie", "theme_tmdb": t,
              "plex_title": f"T{t}"} for t in (1, 2, 3, 4, 5)]
    responses = [{"ok": True, "restored": 1, "skipped": [{"section_id": "2", "reason": "canonical_already_present"}]},
                 {"ok": True, "restored": 0, "skipped": [{"section_id": "1", "reason": "link_failed:[Errno 2]"}]},
                 {"__throw": {"status": 409}},
                 # v0.51.344: N6 — an answer whose restored is not a number says nothing was restored
                 {"ok": True, "restored": "1", "skipped": []}, {"ok": True, "skipped": []}]
    out = _run_library_loop(tmp_path, items, responses)
    assert (len(out["calls"]), out["left"]) == (5, 0)
    assert out["text"] == "// 1 RESTORED · 4 FAILED", \
        "a 200 that restored nothing (or no number) was counted as RESTORED, or a section already present as FAILED"


def _seed_two_sections(db, plexdir, tmdb):
    """One title in Movies and 4K Movies: two library rows, two broken canonicals, each with its Plex-folder copy."""
    with closing(sqlite3.connect(db)) as c:
        for sid, sub in (("1", "movies"), ("2", "movies-4k")):
            c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, "
                      "discovered_at, last_seen_at) VALUES (?, ?, 'movie', 0, ?, ?, 1, ?, ?)",
                      (sid, f"M{sid}", 1 if sid == "2" else 0, sub, NOW, NOW))
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at, "
                  "first_seen_sync_at, youtube_url) VALUES (?, 'movie', ?, 'Two Cuts', 'plex_orphan', ?, ?, NULL)",
                  (tmdb, tmdb, NOW, NOW))
        for sid, sub in (("1", "movies"), ("2", "movies-4k")):
            folder = plexdir / sid / str(tmdb)
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "theme.mp3").write_bytes(SIDECAR)
            c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, theme_id, file_path, "
                      "file_size, file_sha256, downloaded_at, source_video_id, provenance, source_kind, "
                      "canonical_present) VALUES ('movie', ?, ?, '', ?, ?, ?, ?, ?, '', 'manual', 'upload', 0)",
                      (tmdb, sid, tmdb, f"{sub}/Two Cuts/theme.mp3", len(SIDECAR),
                       hashlib.sha256(SIDECAR).hexdigest(), NOW))
            c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind, "
                      "provenance, placed_at, plex_rating_key, edition_key) VALUES ('movie', ?, ?, ?, 'hardlink', "
                      "'manual', ?, NULL, '')", (tmdb, sid, str(folder), NOW))
        c.commit()


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_title_listed_in_two_sections_is_restored_by_one_call_and_counted_by_its_canonicals(env):
    client, settings, tmp_path, events = env
    tmdb = 2701
    _seed_two_sections(settings.db_path, tmp_path / "plex", tmdb)
    answers = [client.post(_item(tmdb), headers=AUTH).json() for _ in range(2)]
    present = [{"section_id": s, "reason": "canonical_already_present"} for s in ("1", "2")]
    assert [(a["restored"], a["skipped"]) for a in answers] == [(2, []), (0, present)], \
        "premise: the item's first restore restores every section; a second finds them present"
    with closing(sqlite3.connect(settings.db_path)) as c:
        assert c.execute("SELECT section_id, canonical_present FROM local_files ORDER BY section_id").fetchall() == [
            ("1", 1), ("2", 1)]
    items = [{"canonical_missing": True, "file_path": f"{sub}/Two Cuts/theme.mp3", "theme_media_type": "movie",
              "theme_tmdb": tmdb, "plex_title": "Two Cuts"} for sub in ("movies", "movies-4k")]
    out = _run_library_loop(tmp_path / "library-loop", items, answers)
    assert out["calls"] == [f"POST {_item(tmdb)}"], "each of the title's rows restored the whole title again"
    assert out["text"] == "// 2 RESTORED", "two restored canonicals must read as two, with nothing FAILED"


def test_a_download_for_another_canonical_leaves_this_one_restorable(env):
    # v0.51.344: N1 — a download queued for the 4K cut's canonical must not hold the other cut's restore
    client, settings, tmp_path, events = env
    tmdb = 2702
    _seed_two_sections(settings.db_path, tmp_path / "plex", tmdb)
    with closing(sqlite3.connect(settings.db_path)) as c:
        c.execute("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, payload, status, created_at) "
                  "VALUES ('download', 'movie', ?, '2', ?, 'pending', ?)", (tmdb, json.dumps({"edition_key": ""}), _ago(seconds=1)))
        c.commit()
    got = client.post(_item(tmdb), headers=AUTH).json()
    themes = tmp_path / "themes"
    assert (got["restored"], got["skipped"]) == (1, [{"section_id": "2", "reason": "download_in_flight"}]), got
    assert [(themes / sub / "Two Cuts" / "theme.mp3").exists() for sub in ("movies", "movies-4k")] == [True, False], \
        "the download of one canonical decided whether another canonical was restored"


# ── R3-F3: a download in flight ──────────────────────────────────────

def _worker(settings):
    from app.core.worker import TokenBucket, Worker
    return Worker(settings=settings, stop_event=threading.Event(), bucket=TokenBucket(1.0, 1),
                  job_type_filter=("download",))


def _claim_all(settings):
    w = _worker(settings)
    claimed = []
    while (job := w._claim_next_job()) is not None:
        claimed.append(job["id"])
    return w, claimed


def _download_jobs(db):
    with closing(sqlite3.connect(db)) as c:
        return c.execute("SELECT tmdb_id, status FROM jobs WHERE job_type = 'download' ORDER BY tmdb_id").fetchall()


class _RecordingPlex:
    calls: list = []

    def __init__(self, cfg, *, plus_mode):
        self.closed = False

    def get_themes(self, *, rating_key):
        _RecordingPlex.calls.append(rating_key)
        return {"ok": True, "http_status": 200, "error": None,
                "body": {"MediaContainer": {"Metadata": [{"ratingKey": "upload://themes/a", "selected": True}]}}}

    def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
        return {"ok": True, "http_status": 200, "bytes": b"store-bytes"}

    def close(self):
        self.closed = True


@pytest.mark.parametrize("job_state", ["queued", "running"])
def test_restore_from_plex_skips_every_row_whose_download_is_in_flight(env, monkeypatch, job_state):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    db, themes, plexdir = settings.db_path, tmp_path / "themes", tmp_path / "plex"
    _seed(db, plexdir, 1901, source_kind="themerrdb", tdb_url=URL, plex_item=True)
    _seed(db, plexdir, 1902, source_kind="themerrdb", tdb_url=URL, plex_item=True, store=True)
    _seed(db, plexdir, 1903)
    with closing(sqlite3.connect(db)) as c:
        # another edition's download of 1903 is not this canonical's
        c.execute("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, payload, status, created_at) "
                  "VALUES ('download', 'movie', 1903, '1', ?, 'pending', ?)", (json.dumps({"edition_key": "extended"}), NOW))
        c.commit()
    assert client.post(REPAIR, headers=AUTH).json()["repaired_rows"] == 2, "premise: REPAIR ALL queued both downloads"
    if job_state == "running":
        _claim_all(settings)
    assert ("running" if job_state == "running" else "pending") in {s for t, s in _download_jobs(db) if t == 1901}
    settings._cfg.plex.enabled = True
    settings._cfg.plex.url = "http://plex.test:32400"
    settings._cfg.plex.token = "token-for-test"
    _RecordingPlex.calls = []
    monkeypatch.setattr(api_mod, "PlexClient", _RecordingPlex)
    assert client.post(START, headers=AUTH).json()["started"] is True
    st = _finish(client)
    assert {s["tmdb_id"]: s["reason"] for s in st["skipped"]} == {1901: "download_in_flight", 1902: "download_in_flight"}
    assert (st["restored_sidecar"], st["restored_store"]) == (1, 0), "the row with no download of its own must restore"
    assert _RecordingPlex.calls == [], "Plex was asked for a row whose download is in flight"
    assert not _canon(themes, 1901).exists() and not _canon(themes, 1902).exists()
    assert [_cols(db, t, ("canonical_present",))[0] for t in (1901, 1902, 1903)] == [0, 0, 1]


@pytest.mark.parametrize("job_state", ["queued", "running"])
def test_the_per_item_restore_skips_a_row_whose_download_is_in_flight(env, job_state):
    client, settings, tmp_path, events = env
    db, themes = settings.db_path, tmp_path / "themes"
    _seed(db, tmp_path / "plex", 1904, source_kind="themerrdb", tdb_url=URL, plex_item=True)
    assert client.post(REPAIR, headers=AUTH).json()["repaired_rows"] == 1
    w = _worker(settings)
    job = None
    if job_state == "running":
        job = w._claim_next_job()
        assert job is not None
    r = client.post(_item(1904), headers=AUTH)
    assert (r.status_code, r.json()["restored"], r.json()["skipped"]) == (
        200, 0, [{"section_id": "1", "reason": "download_in_flight"}])
    assert not _canon(themes, 1904).exists() and _cols(db, 1904, ("canonical_present",)) == (0,)
    if job is None:
        job = w._claim_next_job()
    w._mark_done(job["id"])
    r = client.post(_item(1904), headers=AUTH)
    assert (r.status_code, r.json()["restored"]) == (200, 1), "the row stayed refused after its download ended"


def test_a_download_queued_while_the_store_bytes_are_in_flight_keeps_the_publish_off_the_row(tmp_path):
    from app.core.db import transaction
    from app.core.sync import _enqueue_download
    db, themes = tmp_path / "m.db", tmp_path / "themes"
    init_db(db)
    _seed(db, tmp_path / "plex", 1906, source_kind="themerrdb", tdb_url=URL, plex_item=True, store=True)
    queued = []

    class QueuesADownloadMidFetch:
        def get_themes(self, *, rating_key):
            return {"ok": True, "http_status": 200, "error": None,
                    "body": {"MediaContainer": {"Metadata": [{"ratingKey": "upload://themes/a", "selected": True}]}}}

        def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
            with get_conn(db) as conn, transaction(conn):
                queued.append(_enqueue_download(conn, media_type="movie", tmdb_id=1906, reason="manual",
                                                only_section_id="1"))
            return {"ok": True, "http_status": 200, "bytes": b"store-bytes"}

    res = ch.restore_from_plex(db, themes, QueuesADownloadMidFetch())
    assert queued == [1], "premise: a download was queued while the store bytes were in flight"
    assert (res["restored_store"], [(s["tmdb_id"], s["reason"]) for s in res["skipped"]]) == (
        0, [(1906, "download_in_flight")]), "the publish landed under a download that is now in flight"
    assert not _canon(themes, 1906).exists() and _cols(db, 1906, ("canonical_present",)) == (0,)


def test_repair_all_is_refused_while_restore_from_plex_runs(env, monkeypatch):
    client, settings, tmp_path, events = env
    _seed(settings.db_path, tmp_path / "plex", 1905, source_kind="themerrdb", tdb_url=URL, plex_item=True)
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert held.entered.wait(10)
    r = client.post(REPAIR, headers=AUTH)
    assert r.status_code == 409 and "RESTORE FROM PLEX is running" in r.json()["detail"], r.text
    assert _download_jobs(settings.db_path) == [], "a download was queued under the running restore"
    held.release.set()
    _finish(client)
    assert client.post(REPAIR, headers=AUTH).json()["repaired_rows"] == 1
    assert _download_jobs(settings.db_path) == [(1905, "pending")]


# ── R3-F4 / F6 / F7 / F8: the page, under node ───────────────────────

_THROW_OLD = """  if (next && next.__throw) {
    const err = new Error(`${next.__throw.status}: ${next.__throw.detail || "error"}`);
    err.status = next.__throw.status;
    throw err;
  }
"""
# api()'s own shapes: motif's JSON (detail parsed), a proxy's page (a status, no detail), a 200 login page (SyntaxError).
_THROW_NEW = """  if (next && next.__throw) {
    const t = next.__throw;
    if (t.nonjson) throw new SyntaxError("Unexpected token '<', \\"<!doctype \\"... is not valid JSON");
    const body = t.detail != null ? JSON.stringify({ detail: t.detail }) : "<html>proxy page</html>";
    const err = new Error(`${t.status}: ${body}`);
    err.status = t.status;
    err.detail = t.detail != null ? t.detail : null;
    throw err;
  }
"""


def _page(tmp_path, responses, clicks, ssr=None):
    assert _THROW_OLD in _DRIVER
    start = APP_JS.index("  function bindCanonicalHealth() {")
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "bind.js").write_text(_app_fn("fmtRelativePast") + _app_fn("proxyStatusHint")
                                      + _app_fn("gatewayTimeoutNote") + _app_fn("restoreSkipWord") + _app_fn("failWords")
                                      + APP_JS[start:APP_JS.index("\n  function ", start + 1)])
    (tmp_path / "scenario.json").write_text(json.dumps({"responses": responses, "clicks": clicks, "ssr": ssr or {}}))
    (tmp_path / "driver.js").write_text(_DRIVER.replace(_THROW_OLD, _THROW_NEW))
    r = subprocess.run([_NODE, str(tmp_path / "driver.js"), str(tmp_path / "bind.js"), str(tmp_path / "scenario.json")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    out = json.loads(r.stdout)
    assert out["unexpected"] == [] and out["left"] == 0, out
    return out["snaps"]


_PAGE = _report(missing=[_row(2001, "Page Title", "store")], restorable=1)
_BTN, _STATUS, _CHECK_BTN, _REPAIR_BTN = ("canon-restore-plex-btn", "canon-restore-plex-status", "canon-check-btn",
                                         "canon-repair-btn")
_NEVER = "✗ the start never reached motif — nothing ran; press RESTORE FROM PLEX again"
_RUNNING = {"status": "running", "stage": "restoring", "done": 3, "total": 10, "restored_sidecar": 1,
            "restored_store": 1, "skipped_count": 1, "cancelling": False, "elapsed_s": 4.0}
_PROGRESS = "restoring 3 / 10 · 2 restored · 1 skipped"
_WORDS = "✓ restored 9 (5 from Plex folders, 4 from Plex's store)"


def _done(started, finished):
    return {"status": "done", "broken": 10, "restored": 9, "restored_sidecar": 5, "restored_store": 4, "skipped": [],
            "skipped_count": 0, "not_attempted": 0, "cancelled": False, "plex_unreachable": False,
            "started_at": started, "finished_at": finished, "actor": "testadmin"}


def _unlocked(s):
    return (s[_BTN]["disabled"], s[_CHECK_BTN]["disabled"], s[_REPAIR_BTN]["disabled"],
            s["canon-restore-plex-cancel-btn"]["display"]) == (False, False, False, "none")


@pytest.mark.skipif(not _NODE, reason="node not installed")
@pytest.mark.parametrize("seen", ["done", "interrupted", "idle", "never-answered"])
def test_a_502_start_answered_by_an_older_run_says_the_start_never_reached_motif(tmp_path, seen):
    old = {"done": _done(_ago(hours=3, minutes=1), _ago(hours=3)),
           "interrupted": {"status": "interrupted", "started_at": _ago(hours=2), "actor": "testadmin"},
           "idle": {"status": "idle"}, "never-answered": _done(_ago(hours=3, minutes=1), _ago(hours=3))}[seen]
    load = {"__throw": {"status": 502}} if seen == "never-answered" else old
    s0, s1 = _page(tmp_path, [_PAGE, load, {"__throw": {"status": 502}}, old], [_BTN])
    last = {"done": f" · last run 3h ago: {_WORDS}", "never-answered": f" · last run 3h ago: {_WORDS}",
            "interrupted": " · last run started 2h ago: cut off by a motif restart", "idle": ""}[seen]
    status = s1[_STATUS]
    assert (status["text"], status["className"]) == (_NEVER + last, "form-status form-status-fail"), \
        "an older run's answer was presented as this click's result"
    assert _unlocked(s1) and s1["__timers"] == 0


@pytest.mark.skipif(not _NODE, reason="node not installed")
@pytest.mark.parametrize("seen", ["done", "never-answered"])
def test_a_502_start_whose_run_starts_and_finishes_shows_that_runs_result(tmp_path, seen):
    old = _done(_ago(hours=3, minutes=1), _ago(hours=3))
    load = {"__throw": {"status": 502}} if seen == "never-answered" else old
    new_start = _ago(seconds=0)
    _s0, s1, s2 = _page(tmp_path, [_PAGE, load, {"__throw": {"status": 502}}, dict(_RUNNING, started_at=new_start),
                                   _done(new_start, _ago(seconds=0)), _report()], [_BTN, "tick"])
    assert s1[_STATUS]["text"] == _PROGRESS and s1[_BTN]["text"] == "// RESTORING…"
    status = s2[_STATUS]
    assert (status["text"], status["className"]) == (_WORDS, "form-status form-status-ok")
    assert _unlocked(s2)


_GATEWAY = "✗ 502: the reverse proxy timed out, but motif may still be finishing — verify before retrying."
_SIGN_IN = "✗ authentication required — reload the page and sign in again"
_LOGIN_PAGE = ("✗ could not reach motif — a reverse proxy / WAF may have returned a non-motif page (SSO login or a "
               "size/security block), or the network dropped. Reload, sign in, then retry.")


@pytest.mark.skipif(not _NODE, reason="node not installed")
@pytest.mark.parametrize("button, status_id, detail", [
    (_CHECK_BTN, "canon-check-status", "RESTORE FROM PLEX is running — run the check when it finishes"),
    (_REPAIR_BTN, "canon-repair-status", "RESTORE FROM PLEX is running — repair when it finishes"),
], ids=["check-409", "repair-409"])
def test_a_check_or_repair_refused_by_a_running_restore_words_it_and_attaches_to_the_run(tmp_path, button, status_id,
                                                                                         detail):
    # v0.51.344: another tab's run refused the click — these params pinned the page unlocked beside a live run
    _s0, s1 = _page(tmp_path, [_PAGE, {"status": "idle"}, {"__throw": {"status": 409, "detail": detail}}, _RUNNING],
                    [button])
    status = s1[status_id]
    assert (status["text"], status["className"]) == ("✗ " + detail, "form-status form-status-fail")
    assert (s1[_BTN]["text"], s1[_CHECK_BTN]["disabled"], s1[_REPAIR_BTN]["disabled"], s1["__timers"]) == (
        "// RESTORING…", True, True, 1), "the page stayed unlocked beside the run that refused it"
    assert s1[_STATUS]["text"] == _PROGRESS


@pytest.mark.skipif(not _NODE, reason="node not installed")
@pytest.mark.parametrize("button, status_id, thrown, words", [
    (_CHECK_BTN, "canon-check-status", {"status": 401, "detail": "authentication required"}, _SIGN_IN),
    (_CHECK_BTN, "canon-check-status", {"status": 502}, _GATEWAY),
    (_CHECK_BTN, "canon-check-status", {"nonjson": True}, _LOGIN_PAGE),
    (_REPAIR_BTN, "canon-repair-status", {"status": 401, "detail": "authentication required"}, _SIGN_IN),
    (_REPAIR_BTN, "canon-repair-status", {"status": 502}, _GATEWAY),
    (_BTN, _STATUS, {"status": 409, "detail": "themes_dir not configured"}, "✗ themes_dir not configured"),
    (_BTN, _STATUS, {"status": 401, "detail": "authentication required"}, _SIGN_IN),
    (_BTN, _STATUS, {"status": 403}, "✗ 403: a reverse proxy / WAF answered before reaching motif — blocked by a WAF / "
                                     "CrowdSec rule (or an oversized body) — retry on your LAN, or check the proxy/WAF."),
], ids=["check-401", "check-502", "check-login-page", "repair-401", "repair-502",
        "restore-409", "restore-401", "restore-403-proxy"])
def test_the_page_words_each_failure_instead_of_printing_the_json(tmp_path, button, status_id, thrown, words):
    _s0, s1 = _page(tmp_path, [_PAGE, {"status": "idle"}, {"__throw": thrown}], [button])
    status = s1[status_id]
    assert (status["text"], status["className"]) == (words, "form-status form-status-fail")
    assert "{" not in status["text"], "FastAPI's JSON reached the page"
    assert _unlocked(s1)


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_lost_session_mid_run_is_said_on_the_progress_line_and_the_poll_keeps_trying(tmp_path, ssr_running):
    snaps = _page(tmp_path, [_PAGE, _RUNNING, {"__throw": {"status": 401, "detail": "authentication required"}},
                             {"__throw": {"nonjson": True}}, _RUNNING], ["tick", "tick", "tick"], ssr=ssr_running)
    _s0, s1, s2, s3 = snaps
    lost = _PROGRESS + " — lost contact: the session expired, reload and sign in"
    for s in (s1, s2):
        assert s[_STATUS]["text"] == lost, s[_STATUS]["text"]
        assert (s["__timers"], s[_BTN]["text"], s[_CHECK_BTN]["disabled"]) == (1, "// RESTORING…", True)
    assert s3[_STATUS]["text"] == _PROGRESS, "a poll that reached motif again kept the lost-contact note"


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_login_page_as_the_first_failed_poll_mid_run_says_the_session_expired(tmp_path, ssr_running):
    # v0.51.342: an SSO page answering the poll (a non-JSON 200) must say so on its own — a 401 first wrote the note for it.
    _s0, s1, s2 = _page(tmp_path, [_PAGE, _RUNNING, {"__throw": {"nonjson": True}}, _RUNNING], ["tick", "tick"],
                        ssr=ssr_running)
    assert s1[_STATUS]["text"] == _PROGRESS + " — lost contact: the session expired, reload and sign in", s1[_STATUS]
    assert (s1["__timers"], s1[_BTN]["text"], s1[_CHECK_BTN]["disabled"]) == (1, "// RESTORING…", True)
    assert s2[_STATUS]["text"] == _PROGRESS, "a poll that reached motif again kept the lost-contact note"


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_run_of_failed_polls_mid_run_says_since_when_contact_was_lost(tmp_path, ssr_running):
    blip = {"__throw": {"status": 502}}
    snaps = _page(tmp_path, [_PAGE, _RUNNING, blip, blip, blip, _RUNNING], ["tick", "tick", "tick", "tick"],
                  ssr=ssr_running)
    _s0, s1, s2, s3, s4 = snaps
    assert s1[_STATUS]["text"] == s2[_STATUS]["text"] == _PROGRESS, "one proxy blip must not read as lost contact"
    assert re.fullmatch(re.escape(_PROGRESS) + r" — lost contact since \d{2}:\d{2}", s3[_STATUS]["text"]), s3[_STATUS]
    assert s3["__timers"] == 1
    assert s4[_STATUS]["text"] == _PROGRESS


@pytest.mark.skipif(not _NODE, reason="node not installed")
# v0.51.344: N7 (drop && !restoreRunning) is equivalent — the running poll clears cutOffStartedAt first and START/CHECK disable each other
def test_a_successful_check_quiets_the_cut_off_alarm_a_failed_one_leaves_it(tmp_path):
    missing = [_row(2002, "No Copy Title", None)]
    checked = {**_report(missing=missing, restorable=0), "check": {"checked": 5, "missing": 1, "skipped": 0},
               "checked": {"tracked": 5, "never": 0, "oldest": _ago(seconds=1), "newest": _ago(seconds=1)}}
    cut = {"status": "interrupted", "started_at": _ago(hours=2), "actor": "testadmin", "first_report": True}
    s0, s1, s2 = _page(tmp_path, [_report(missing=missing, restorable=0), cut, {"__throw": {"status": 502}}, checked],
                       [_CHECK_BTN, _CHECK_BTN])
    alarm = ("✗ the run started 2h ago was cut off by a motif restart — RUN CHECK, then RESTORE FROM PLEX restores "
             "what is left", "form-status form-status-fail")
    for s in (s0, s1):
        assert (s[_STATUS]["text"], s[_STATUS]["className"]) == alarm
    assert s2["canon-check-status"]["text"] == "✓ check complete"
    assert (s2[_STATUS]["text"], s2[_STATUS]["className"]) == (
        "last run started 2h ago: cut off by a motif restart", "form-status"), "the alarm still asks for RUN CHECK"
    assert (s2[_BTN]["display"], s2["canon-missing-block"]["display"]) == ("none", "")


# ── restore-shutdown-cancel review: the thread a shutdown joins ──────

class _HoldTheJobAtItsFirstLock:
    """Wraps _CANON_RESTORE_LOCK: the job thread's first take waits for `go` before it asks for the lock."""
    def __init__(self, real):
        self.real, self.holding, self.go, self.seen = real, threading.Event(), threading.Event(), False

    def __enter__(self):
        if threading.current_thread().name == JOB_THREAD and not self.seen:
            self.seen = True
            self.holding.set()
            assert self.go.wait(10)
        return self.real.__enter__()

    def __exit__(self, *exc):
        return self.real.__exit__(*exc)


def test_a_shutdown_right_after_start_joins_the_run_that_start_claimed(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_SHUTDOWN", threading.Event())
    previous = threading.Thread(target=lambda: None, name="the-previous-run")
    previous.start()
    previous.join()
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_THREAD", previous)
    hold = _HoldTheJobAtItsFirstLock(api_mod._CANON_RESTORE_LOCK)
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_LOCK", hold)
    monkeypatch.setattr(ch, "restore_from_plex", HeldRestore())
    assert client.post(START, headers=AUTH).json()["started"] is True
    try:
        assert hold.holding.wait(10), "premise: the job thread has not run its first line"
        job = api_mod.canon_restore_shutdown()
    finally:
        hold.go.set()
    assert job is not previous, "the exit path would join the previous run's finished thread"
    assert job is not None and job.name == JOB_THREAD
    job.join(10)
    assert not job.is_alive()
    assert client.get(START + "/status", headers=AUTH).json()["status"] == "interrupted"


# ── restore-shutdown-cancel review: exit inside Docker's stop grace ──

_DOCKER_STOP_GRACE_S = 10.0  # docker stop's default; nothing in the repo sets stop_grace_period

_EXIT_CHILD = r"""
import sys
sys.path.insert(0, sys.argv[1])
import app.core.canonical_health as ch
print("child imports " + ch.__file__, flush=True)
from app.main import main
sys.exit(main())
"""


def _trickling_plex():
    """A Plex whose theme listing never finishes: a byte of body at a time, each inside the client's read timeout."""
    state = {"listings": [], "release": threading.Event()}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = self.path.split("?")[0]
            if path.startswith("/library/metadata/") and path.endswith("/themes"):
                state["listings"].append(path.split("/")[3])
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "100000")
                self.end_headers()
                while not state["release"].wait(0.5):
                    try:
                        self.wfile.write(b" ")
                        self.wfile.flush()
                    except OSError:
                        return
                return
            body = b'{"MediaContainer": {"size": 0}}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            return None
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, state


def _stop_grace_premise(repo: Path) -> tuple[list, list, list]:
    """What the deploy files set that changes docker stop's grace or signal: (compose, Dockerfile, Unraid)."""
    # v0.51.344: parse the deploy files — a comment naming stop_grace_period tripped the old substring scan
    services = (yaml.safe_load((repo / "docker-compose.yml").read_text()) or {}).get("services") or {}
    compose = [f"{name}.{key}" for name, svc in services.items() for key in ("stop_grace_period", "stop_signal")
               if key in (svc or {})]
    dockerfile = [ln.strip() for ln in (repo / "Dockerfile").read_text().splitlines()
                  if re.match(r"\s*STOPSIGNAL\b", ln, re.IGNORECASE)]
    unraid = []
    for xml in sorted((repo / "unraid").rglob("*.xml")):
        # ElementTree refuses motif.xml: a comment there holds "--user"
        text = re.sub(r"<!--.*?-->", "", xml.read_text(), flags=re.DOTALL)
        for tag, args in re.findall(r"<(ExtraParams|PostArgs)>(.*?)</\1>", text, flags=re.DOTALL):
            unraid += [f"{xml.name} {tag}: {flag}" for flag in ("--stop-timeout", "--stop-signal") if flag in args]
    return compose, dockerfile, unraid


def test_nothing_in_the_deploy_files_changes_dockers_stop_grace_or_signal():
    assert _stop_grace_premise(REPO) == ([], [], []), "the exit test's 10 s grace and SIGTERM are not what docker sends"


@pytest.mark.parametrize("edit, caught", [
    ("compose-comment", False), ("compose-key", True), ("unraid-extraparams", True), ("dockerfile-stopsignal", True),
])
def test_the_stop_grace_premise_reads_settings_not_comments(tmp_path, edit, caught):
    (tmp_path / "unraid").mkdir()
    compose = (REPO / "docker-compose.yml").read_text()
    docker = (REPO / "Dockerfile").read_text()
    xml = (REPO / "unraid" / "motif.xml").read_text()
    if edit == "compose-comment":
        compose += "\n# stop_grace_period is left at docker's default on purpose\n"
    elif edit == "compose-key":
        compose = compose.replace("    restart: unless-stopped\n", "    restart: unless-stopped\n    stop_grace_period: 30s\n")
    elif edit == "unraid-extraparams":
        xml = xml.replace("<ExtraParams></ExtraParams>", "<ExtraParams>--stop-timeout 30</ExtraParams>")
    else:
        docker += "\nSTOPSIGNAL SIGINT\n"
    assert (compose, docker, xml) != ((REPO / "docker-compose.yml").read_text(), (REPO / "Dockerfile").read_text(),
                                      (REPO / "unraid" / "motif.xml").read_text()), "premise: the edit applied"
    (tmp_path / "docker-compose.yml").write_text(compose)
    (tmp_path / "Dockerfile").write_text(docker)
    (tmp_path / "unraid" / "motif.xml").write_text(xml)
    found = _stop_grace_premise(tmp_path)
    assert (found != ([], [], [])) is caught, found


def test_a_motif_stopped_while_a_restore_request_hangs_exits_inside_dockers_stop_grace(tmp_path):
    assert _stop_grace_premise(REPO) == ([], [], []), "premise: a deploy file sets its own stop grace or signal"
    from app.core.auth import create_admin, init_auth_schema
    cfg, data, themes = tmp_path / "config", tmp_path / "data", tmp_path / "themes"
    cfg.mkdir()
    themes.mkdir()
    db = cfg / "motif.db"
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    _seed_rows(db, (601, 602), file_path=SHARED)
    srv, plex = _trickling_plex()
    web_port = _free_port()
    env_vars = {k: v for k, v in os.environ.items() if not k.startswith("MOTIF_")}
    env_vars.update(MOTIF_CONFIG_DIR=str(cfg), MOTIF_DATA_DIR=str(data), MOTIF_THEMES_DIR=str(themes),
                    MOTIF_COOKIES_FILE=str(cfg / "cookies.txt"), MOTIF_PLEX_ENABLED="true",
                    MOTIF_PLEX_URL=f"http://127.0.0.1:{srv.server_address[1]}", MOTIF_PLEX_TOKEN="token-for-test",
                    MOTIF_WEB_HOST="127.0.0.1", MOTIF_WEB_PORT=str(web_port), MOTIF_TRUST_FORWARD_AUTH="true",
                    MOTIF_FORWARD_AUTH_ALLOWED_IPS="127.0.0.1")
    log_path = tmp_path / "motif.out"

    def until(pred, what, seconds=60):
        end = time.monotonic() + seconds
        while not pred():
            assert proc.poll() is None, f"motif exited while waiting for {what}:\n{log_path.read_text()[-4000:]}"
            assert time.monotonic() < end, f"timed out waiting for {what}:\n{log_path.read_text()[-4000:]}"
            time.sleep(0.05)

    with log_path.open("w") as out:
        proc = subprocess.Popen([sys.executable, "-c", _EXIT_CHILD, str(REPO)], cwd=str(REPO), env=env_vars,
                                stdout=out, stderr=subprocess.STDOUT)
    try:
        until(lambda: _up(web_port), "motif to come up")
        assert _http(web_port, "POST", START)["started"] is True
        until(lambda: plex["listings"], "the shared-path row's Plex request to hang")
        sent = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=60)
        took = time.monotonic() - sent
    finally:
        plex["release"].set()
        if proc.poll() is None:
            proc.kill()
            proc.wait(10)
        srv.shutdown()
        srv.server_close()
    text = log_path.read_text(errors="replace")
    assert f"child imports {REPO / 'app' / 'core' / 'canonical_health.py'}" in text, "the child ran another tree"
    assert proc.returncode == 0, text[-4000:]
    # v0.51.344: the deadline closes publishing — the restore left waiting on Plex can start no write exit would cut
    assert "RESTORE FROM PLEX was still waiting on Plex" in text and "no new write starts" in text, \
        "exit did not name the restore it left in flight, or did not close its writes"
    assert took < _DOCKER_STOP_GRACE_S, \
        f"exit took {took:.2f} s after SIGTERM — past docker stop's {_DOCKER_STOP_GRACE_S:.0f} s grace, so it is SIGKILLed"
    assert json.loads((cfg / "canonical_health" / "restore_from_plex.json").read_text())["status"] == "running", \
        "the next start must report the run cut off"
