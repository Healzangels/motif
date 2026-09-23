"""v0.51.346: /api/library reads each file flag with one live stat on a bounded shared pool; a hung stat no longer holds exit."""
from __future__ import annotations

import collections
import contextlib
import errno
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db

AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
_REAL_STAT = Path.stat

# canonical state -> canonical_missing; placement state -> placement_missing (what the row's dots paint)
CANON = {"ok": False, "empty": True, "gone": True, "dir": True, "loop": True, "dangling": True, None: False}
PLACE = {"real": False, "empty": False, "gone": True, "dir": True, "upload": False, None: False}
ROWS = [(canon, place) for canon in CANON for place in PLACE]


def _rk(n):
    return f"m{n:03d}"


TRUTH = {_rk(n): (CANON[c], PLACE[p]) for n, (c, p) in enumerate(ROWS, start=1)}


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def lib(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.web import api
    monkeypatch.setattr(api, "log_event", lambda *a, **k: None)
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    app = api.create_app(s)
    return SimpleNamespace(tc=TestClient(app), app=app, api=api, db=s.db_path, themes=tmp_path / "themes",
                           media=tmp_path / "media")


def _section(c, section_id, kind):
    now = _now()
    c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included,"
              " discovered_at, last_seen_at) VALUES (?, ?, ?, 0, 0, ?, 1, ?, ?)",
              (section_id, kind, kind, "movies" if kind == "movie" else "tv", now, now))


def _row(lib, c, n, *, canon, place, mt="movie", section_id="1", sub="movies"):
    now = _now()
    tmdb = 8000 + n
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
              " first_seen_sync_at, youtube_url) VALUES (?, ?, ?, ?, 'imdb', ?, ?, ?)",
              (n, mt, tmdb, f"Title {n:04d}", now, now, f"https://www.youtube.com/watch?v=v{n:010d}"))
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, edition_key,"
              " folder_path, has_theme, local_theme_file, plex_independent_theme, plex_theme_verified_ok,"
              " first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, '', ?, 0, 0, 0, 1, ?, ?)",
              (_rk(n), section_id, "show" if mt == "tv" else mt, n, tmdb, f"Title {n:04d}", f"/nonexistent/{n}",
               now, now))
    if canon:
        rel = f"{sub}/{_rk(n)}.mp3"
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path, downloaded_at,"
                  " source_video_id, provenance, source_kind) VALUES (?, ?, ?, '', ?, ?, ?, 'auto', 'themerrdb')",
                  (mt, tmdb, section_id, rel, now, f"v{n:010d}"))
        path = lib.themes / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if canon in ("ok", "empty"):
            path.write_bytes(b"canonical" if canon == "ok" else b"")
        elif canon == "dir":
            path.mkdir()
        elif canon == "loop":
            os.symlink(path.name, path)
        elif canon == "dangling":
            os.symlink("nowhere.mp3", path)
    if place == "upload":
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder, placed_at,"
                  " placement_kind, plex_rating_key, plex_refreshed, theme_present)"
                  " VALUES (?, ?, ?, '', '', ?, 'plex_upload', ?, 1, 1)", (mt, tmdb, section_id, now, _rk(n)))
    elif place:
        folder = lib.media / _rk(n)
        folder.mkdir(parents=True)
        if place in ("real", "empty"):
            (folder / "theme.mp3").write_bytes(b"placed" if place == "real" else b"")
        elif place == "dir":
            (folder / "theme.mp3").mkdir()
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder, placed_at,"
                  " placement_kind, plex_refreshed, theme_present) VALUES (?, ?, ?, '', ?, ?, 'hardlink', 1, 1)",
                  (mt, tmdb, section_id, str(folder), now))


def _seed(lib):
    with contextlib.closing(sqlite3.connect(lib.db)) as c, c:
        _section(c, "1", "movie")
        for n, (canon, place) in enumerate(ROWS, start=1):
            _row(lib, c, n, canon=canon, place=place)


def _get(tc, **params):
    r = tc.get("/api/library", params=params, headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()


def _walk(lib, per_page, **params):
    first = _get(lib.tc, page=1, per_page=per_page, **params)
    rows = list(first["items"])
    for page in range(2, 1000):
        items = _get(lib.tc, page=page, per_page=per_page, **params)["items"]
        if not items:
            break
        rows += items
    assert first["total"] == len(rows), (params, first["total"], len(rows))
    return rows


@contextlib.contextmanager
def _stats(monkeypatch, lib, hook=None):
    """Every stat of a path under themes/ or media/ while the block runs: (path, thread name), in call order."""
    seen, lock = [], threading.Lock()
    roots = (str(lib.themes), str(lib.media))

    def stat(self, **kw):
        if str(self).startswith(roots):
            with lock:
                seen.append((str(self), threading.current_thread().name))
            if hook is not None:
                hook(self)
        return _REAL_STAT(self, **kw)

    monkeypatch.setattr(Path, "stat", stat)
    try:
        yield seen
    finally:
        monkeypatch.setattr(Path, "stat", _REAL_STAT)


def _dl_on(it):
    return bool(it["file_path"]) and not it["canonical_missing"]


def _pl_on(it):
    return not it["placement_missing"] and (bool(it["media_folder"]) or it["placement_kind"] == "plex_upload")


# (view, the rows it returns out of the unfiltered walk) — the post-stat matchers, read off the row's own flags
POST_STAT_VIEWS = [
    (dict(dl_pills="on"), _dl_on),
    (dict(dl_pills="broken"), lambda it: it["canonical_missing"]),
    (dict(dl_pills="on,off,broken"), lambda it: True),
    (dict(status="dl_missing"), lambda it: it["canonical_missing"] and it["media_folder"] is not None),
    (dict(pl_pills="on"), _pl_on),
    (dict(pl_pills="broken"), lambda it: it["placement_missing"]),
    (dict(attn_pills="broken"), lambda it: it["canonical_missing"]),
    (dict(dl_pills="on", pl_pills="broken"), lambda it: _dl_on(it) and it["placement_missing"]),
    (dict(attn_pills="broken", pl_pills="on"), lambda it: it["canonical_missing"] and _pl_on(it)),
    (dict(status="dl_missing", pl_pills="broken"), lambda it: it["canonical_missing"] and it["placement_missing"]),
    (dict(status="dl_missing", attn_pills="broken"), lambda it: it["canonical_missing"] and it["media_folder"] is not None),
]


@pytest.mark.parametrize("per_page", [1, 7, 50])
def test_every_row_a_post_stat_view_returns_carries_both_live_flags_in_key_order(lib, per_page):
    _seed(lib)
    base = _walk(lib, 50, tab="movies")
    assert {it["rating_key"]: (it["canonical_missing"], it["placement_missing"]) for it in base} == TRUTH
    for view, keep in POST_STAT_VIEWS:
        rows = _walk(lib, per_page, tab="movies", **view)
        want = [it for it in base if keep(it)]
        assert 0 < len(want) < len(base) or view == dict(dl_pills="on,off,broken"), view
        assert rows == want, view
        assert [list(it) for it in rows] == [list(it) for it in want], (view, "key order")


def test_the_flags_follow_the_disk_between_requests(lib):
    _seed(lib)
    n = ROWS.index(("ok", "real")) + 1
    canon, sidecar = lib.themes / "movies" / f"{_rk(n)}.mp3", lib.media / _rk(n) / "theme.mp3"

    def write(p, data=b"canonical"):
        if p.is_dir():
            p.rmdir()
        p.write_bytes(data)

    def to_dir(p):
        p.unlink()
        p.mkdir()

    steps = [
        ("canonical deleted", lambda: canon.unlink(), (True, False)),
        ("canonical restored", lambda: write(canon), (False, False)),
        ("canonical zero-byte", lambda: write(canon, b""), (True, False)),
        ("canonical a directory", lambda: to_dir(canon), (True, False)),
        ("canonical restored again", lambda: write(canon), (False, False)),
        ("sidecar deleted", lambda: sidecar.unlink(), (False, True)),
        ("sidecar a directory", lambda: sidecar.mkdir(), (False, True)),
        ("sidecar restored", lambda: write(sidecar, b"placed"), (False, False)),
        ("both deleted", lambda: (canon.unlink(), sidecar.unlink()), (True, True)),
        ("both restored", lambda: (write(canon), write(sidecar, b"placed")), (False, False)),
    ]
    assert TRUTH[_rk(n)] == (False, False)
    for name, act, flags in steps:
        act()
        # (view, whether the row is in it): every row has a placement; broken,fail holds it only while the canonical is gone
        for view, listed in ((dict(), True), (dict(dl_pills="on,off,broken"), True), (dict(pl_pills="on,broken"), True),
                             (dict(attn_pills="broken,fail"), flags[0])):
            got = [(it["canonical_missing"], it["placement_missing"]) for it in _walk(lib, 50, tab="movies", **view)
                   if it["rating_key"] == _rk(n)]
            assert got == ([flags] if listed else []), (name, view, got)


def test_a_stat_error_reads_missing_with_one_warning_then_debug_breadcrumbs(lib, monkeypatch, caplog):
    _seed(lib)
    monkeypatch.setattr(lib.api, "_CANON_FS_OSERROR_WARNED", False)
    ok = [_rk(n) for n, row in enumerate(ROWS, start=1) if row == ("ok", "real")][0]
    upload = [_rk(n) for n, row in enumerate(ROWS, start=1) if row == ("ok", "upload")][0]
    denied_canon = lib.themes / "movies" / f"{ok}.mp3"
    denied_sidecar = lib.media / ok / "theme.mp3"
    odd = lib.themes / "movies" / f"{upload}.mp3"

    def hook(p):
        if p == denied_canon or p == denied_sidecar:
            raise PermissionError(errno.EACCES, "Permission denied", str(p))
        if p == odd:
            raise ValueError("embedded null byte")

    with caplog.at_level(logging.DEBUG, logger="app.web.api"), _stats(monkeypatch, lib, hook):
        rows = {it["rating_key"]: it for it in _walk(lib, 50, tab="movies")}
    assert (rows[ok]["canonical_missing"], rows[ok]["placement_missing"]) == (True, True)
    assert (rows[upload]["canonical_missing"], rows[upload]["placement_missing"]) == (True, False)
    crumbs = [r for r in caplog.records if r.getMessage().startswith("_annotate_canonical_state: ") and "OSError on" in r.getMessage()]
    assert [(r.levelno, str(denied_canon) in r.getMessage() or str(denied_sidecar) in r.getMessage())
            for r in crumbs] == [(logging.WARNING, True), (logging.DEBUG, True)], [r.getMessage() for r in crumbs]
    assert str(denied_canon) in crumbs[0].getMessage(), "the canonical is read before its row's placement"
    # the absent errnos (gone, loop, dangling) read missing as is_file() did: silently
    assert not any(r.levelno >= logging.WARNING for r in caplog.records if r not in crumbs)


def test_a_post_stat_filter_stats_only_the_flag_it_reads_and_the_page_the_rest(lib, monkeypatch):
    _seed(lib)
    base = _walk(lib, 50, tab="movies")
    canon_paths = {str(lib.themes / it["file_path"]) for it in base if it["file_path"]}
    place_paths = {str(Path(it["media_folder"]) / "theme.mp3") for it in base if it["media_folder"]}

    def stats_for(**params):
        with _stats(monkeypatch, lib) as seen:
            body = _get(lib.tc, tab="movies", **params)
        paths = [p for p, _t in seen]
        assert all(n == 1 for n in collections.Counter(paths).values()), (params, "a path stat'd twice")
        page_c = {str(lib.themes / it["file_path"]) for it in body["items"] if it["file_path"]}
        page_p = {str(Path(it["media_folder"]) / "theme.mp3") for it in body["items"] if it["media_folder"]}
        return set(p for p in paths if p in canon_paths), set(p for p in paths if p in place_paths), page_c, page_p

    c, p, page_c, page_p = stats_for(page=1, per_page=50)
    assert (c, p) == (page_c, page_p) and len(c) + len(p) > 20
    c, p, page_c, page_p = stats_for(page=1, per_page=5)
    assert (c, p) == (page_c, page_p)
    for pills in (dict(dl_pills="on"), dict(dl_pills="broken"), dict(attn_pills="broken,fail")):
        c, p, page_c, page_p = stats_for(page=1, per_page=5, **pills)
        assert c == canon_paths and p == page_p, pills
    # dl_missing's SQL keeps only rows with a placement row: those are its candidates
    placed_canon = {str(lib.themes / it["file_path"]) for it in base if it["file_path"] and it["media_folder"] is not None}
    c, p, page_c, page_p = stats_for(page=1, per_page=5, status="dl_missing")
    assert c == placed_canon < canon_paths and p == page_p
    c, p, page_c, page_p = stats_for(page=1, per_page=5, pl_pills="on")
    assert p == place_paths and c == page_c
    dl_on = {str(Path(it["media_folder"]) / "theme.mp3") for it in base if _dl_on(it) and it["media_folder"]}
    c, p, page_c, page_p = stats_for(page=1, per_page=3, dl_pills="on", pl_pills="broken")
    assert c == canon_paths and p == dl_on


def test_a_requests_stats_run_side_by_side_on_the_library_stat_pool(lib, monkeypatch):
    _seed(lib)
    wide = 16  # the design's per-request fan-out (hotspots check #3)
    barrier = threading.Barrier(wide, timeout=20)
    left = [wide]
    lock = threading.Lock()

    def hook(_p):
        with lock:
            first = left[0] > 0
            left[0] -= 1
        if first:
            barrier.wait()

    try:
        with _stats(monkeypatch, lib, hook) as seen:
            rows = _walk(lib, 50, tab="movies")
    finally:
        barrier.abort()
    assert {it["rating_key"]: (it["canonical_missing"], it["placement_missing"]) for it in rows} == TRUTH
    assert seen and all(t.startswith("library-stat_") for _p, t in seen), collections.Counter(t for _p, t in seen)


def test_results_land_on_their_rows_when_the_stats_finish_out_of_order(lib, monkeypatch):
    _seed(lib)
    base = _walk(lib, 50, tab="movies")
    order = [str(lib.themes / it["file_path"]) if it["file_path"] else None for it in base]
    first = next(p for p in order if p)
    last = str(Path(next(it for it in reversed(base) if it["media_folder"])["media_folder"]) / "theme.mp3")
    last_done = threading.Event()

    def hook(p):
        if str(p) == first:
            assert last_done.wait(20), "the last row's stat never ran while the first row's waited"
        elif str(p) == last:
            last_done.set()

    with _stats(monkeypatch, lib, hook):
        rows = _walk(lib, 50, tab="movies")
    assert last_done.is_set()
    assert rows == base and [list(it) for it in rows] == [list(it) for it in base]


def test_a_light_page_does_not_wait_out_a_heavy_requests_backlog(lib, monkeypatch):
    heavy_rows, light_rows = 1024, 24
    with contextlib.closing(sqlite3.connect(lib.db)) as c, c:
        _section(c, "1", "movie")
        _section(c, "2", "show")
        for n in range(1, heavy_rows + 1):
            _row(lib, c, n, canon="ok", place=None)
        for n in range(heavy_rows + 1, heavy_rows + light_rows + 1):
            _row(lib, c, n, canon="ok", place=None, mt="tv", section_id="2", sub="tv")
    api = lib.api
    gate, held_until, cond = threading.Event(), threading.Event(), threading.Condition()
    counts = {"heavy_at_gate": 0, "light_queued": 0, "widest": 0, "held": 0}
    # per request (keyed by its result queue): batches queued and not yet answered, the most at once, how many queued
    live, peak, sent = collections.Counter(), collections.Counter(), collections.Counter()
    past_bound: set = set()
    jobs = api._LIB_STAT_JOBS

    class Done:
        """A request's result queue: a batch stops counting as in flight when its worker answers."""
        def __init__(self, real):
            self.real = real

        def put(self, msg):
            with cond:
                live[id(self.real)] -= 1
            self.real.put(msg)

    class Jobs:
        """The pool's queue: each request's batches in flight, and the stats a request queued past the bound."""
        def put(self, item):
            fn, batch, done, idx = item
            paths = [str(path) for _c, path in batch]
            with cond:
                if sent[id(done)] >= api._LIB_STAT_WORKERS:
                    past_bound.update(paths)
                counts["light_queued"] += sum("/tv/" in p for p in paths)
                counts["widest"] = max(counts["widest"], len(batch))
                live[id(done)] += 1
                sent[id(done)] += 1
                peak[id(done)] = max(peak[id(done)], live[id(done)])
                cond.notify_all()
            jobs.put((fn, batch, Done(done), idx))

        def get(self):
            return jobs.get()

    def hook(p):
        if "/movies/" in str(p) and not gate.is_set():
            with cond:
                counts["heavy_at_gate"] += 1
                cond.notify_all()
            gate.wait(30)
        # a batch queued past its request's bound holds its worker until the light page is answered
        with cond:
            held = str(p) in past_bound and not held_until.is_set()
            counts["held"] += held
        if held:
            held_until.wait(30)

    out = {}

    def call(name, **params):
        out[name] = TestClient(lib.app).get("/api/library", params=params, headers=AUTH)

    heavy = threading.Thread(target=call, args=("heavy",), kwargs=dict(tab="movies", dl_pills="on", per_page=200))
    light = threading.Thread(target=call, args=("light",), kwargs=dict(tab="tv"))
    monkeypatch.setattr(api, "_LIB_STAT_JOBS", Jobs())
    light_answered = False
    try:
        with _stats(monkeypatch, lib, hook):
            heavy.start()
            with cond:
                assert cond.wait_for(lambda: counts["heavy_at_gate"] >= 1, 30), "the heavy request's stats never began"
            light.start()
            with cond:
                assert cond.wait_for(lambda: counts["light_queued"] == light_rows, 30), \
                    "the light request never queued its stats"
            gate.set()
            light.join(20)
            light_answered = not light.is_alive()
            held_until.set()
            heavy.join(60)
    finally:
        gate.set()
        held_until.set()
        for t in (light, heavy):
            if t.ident is not None:
                t.join(60)
        # the wrapper only forwards to the real queue, so a worker parked in it is parked on the real one
        monkeypatch.setattr(api, "_LIB_STAT_JOBS", jobs)
    # FIFO: the light page's batches queue behind the heavy request's in-flight batches only, never its backlog
    assert light_answered, ("the light page waited on a heavy batch queued past the per-request bound", counts)
    assert out["heavy"].status_code == 200 and out["heavy"].json()["total"] == heavy_rows
    assert out["light"].status_code == 200 and len(out["light"].json()["items"]) == light_rows
    # the per-request bound itself (hotspots check #3): never more than W batches of at most B stats in flight
    assert counts["widest"] <= api._LIB_STAT_BATCH, counts
    assert max(peak.values()) <= api._LIB_STAT_WORKERS, dict(peak)
    assert max(sent.values()) > api._LIB_STAT_WORKERS, ("premise: the heavy request had more batches than the bound",
                                                        dict(sent))
    # the most the bound lets a light page wait (one round, W x B heavy stats) is a small part of the backlog
    assert api._LIB_STAT_WORKERS * api._LIB_STAT_BATCH < heavy_rows // 3


def test_exit_releases_a_library_request_waiting_on_a_stuck_stat(lib, monkeypatch):
    _seed(lib)
    late_n = len(ROWS) + 1
    with contextlib.closing(sqlite3.connect(lib.db)) as c, c:
        _section(c, "2", "show")
        _row(lib, c, late_n, canon="ok", place="real", mt="tv", section_id="2", sub="tv")
    late_paths = {str(lib.themes / "tv" / f"{_rk(late_n)}.mp3"), str(lib.media / _rk(late_n) / "theme.mp3")}

    def tv_page():
        with _stats(monkeypatch, lib) as seen:
            r = TestClient(lib.app, raise_server_exceptions=False).get("/api/library", params=dict(tab="tv"),
                                                                        headers=AUTH)
        return r.status_code, late_paths & {p for p, _t in seen}

    # the release closes the pool for good: this test's own latch, so no later test inherits a closed pool
    monkeypatch.setattr(lib.api, "_LIB_STAT_CLOSED", threading.Event())
    assert tv_page() == (200, late_paths), "premise: the tv page stats its row's canonical and sidecar"
    stuck, entered = threading.Event(), threading.Event()
    target = str(lib.themes / "movies" / f"{_rk(1)}.mp3")

    def hook(p):
        if str(p) == target:
            entered.set()
            stuck.wait(60)

    out = {}

    def call():
        out["r"] = TestClient(lib.app, raise_server_exceptions=False).get(
            "/api/library", params=dict(tab="movies"), headers=AUTH)

    t = threading.Thread(target=call)
    try:
        with _stats(monkeypatch, lib, hook):
            t.start()
            assert entered.wait(30), "the stat never ran"
            assert lib.api.library_stat_release() == 1
            t.join(20)
            assert not t.is_alive(), "the request still waited on the stuck stat after the release"
    finally:
        stuck.set()
        t.join(30)
    assert out["r"].status_code == 500
    # a request that reaches its stats after the release (its SQL outlasted the drain) fails fast and stats nothing
    assert tv_page() == (500, set())
    assert lib.api.library_stat_release() == 0


_CHILD = r"""
import os, sys, threading
sys.path.insert(0, sys.argv[1])
import app.main as motif_main
import app.web.api as api
print("child imports " + motif_main.__file__ + " " + api.__file__, flush=True)
real_stat, never = os.stat, threading.Event()

def stat(p, *a, **k):
    if not isinstance(p, int) and os.fspath(p).endswith(sys.argv[2]):
        print("PROBE stat stuck on " + threading.current_thread().name, flush=True)
        never.wait()
    return real_stat(p, *a, **k)
os.stat = stat
sys.exit(motif_main.main())
"""


def test_a_motif_stopped_while_a_library_stat_never_returns_exits_inside_dockers_stop_grace(tmp_path):
    from test_v0_51_342_canonical_round3 import _DOCKER_STOP_GRACE_S, _stop_grace_premise
    from test_v0_51_342_restore_shutdown_cancel import _free_port, _up
    assert _stop_grace_premise(REPO) == ([], [], []), "premise: a deploy file sets its own stop grace or signal"
    cfg, data = tmp_path / "config", tmp_path / "data"
    cfg.mkdir()
    lib = SimpleNamespace(db=cfg / "motif.db", themes=tmp_path / "themes", media=tmp_path / "media")
    init_db(lib.db)
    init_auth_schema(lib.db)
    create_admin(lib.db, username="testadmin", password="testpassword")
    with contextlib.closing(sqlite3.connect(lib.db)) as c, c:
        _section(c, "1", "movie")
        for n in range(1, 4):
            _row(lib, c, n, canon="ok", place="real")
    port = _free_port()
    env = {k: v for k, v in os.environ.items() if not k.startswith("MOTIF_")}
    env.update(MOTIF_CONFIG_DIR=str(cfg), MOTIF_DATA_DIR=str(data), MOTIF_THEMES_DIR=str(lib.themes),
               MOTIF_COOKIES_FILE=str(cfg / "cookies.txt"), MOTIF_WEB_HOST="127.0.0.1", MOTIF_WEB_PORT=str(port),
               MOTIF_TRUST_FORWARD_AUTH="true", MOTIF_FORWARD_AUTH_ALLOWED_IPS="127.0.0.1")
    log_path = tmp_path / "motif.out"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def text():
        return log_path.read_text(errors="replace")

    def until(pred, what, seconds=60):
        end = time.monotonic() + seconds
        while not pred():
            assert proc.poll() is None, f"motif exited while waiting for {what}:\n{text()[-4000:]}"
            assert time.monotonic() < end, f"timed out waiting for {what}:\n{text()[-4000:]}"
            threading.Event().wait(0.05)

    def request():
        req = urllib.request.Request(f"http://127.0.0.1:{port}/api/library?tab=movies", headers=AUTH)
        try:
            with opener.open(req, timeout=60) as r:
                answered.append(r.status)
        except OSError as e:
            answered.append(getattr(e, "code", type(e).__name__))

    answered: list = []
    with log_path.open("w") as out:
        proc = subprocess.Popen([sys.executable, "-c", _CHILD, str(REPO), f"{_rk(2)}.mp3"], cwd=str(REPO), env=env,
                                stdout=out, stderr=subprocess.STDOUT)
    exited = None
    client = threading.Thread(target=request, daemon=True)
    try:
        until(lambda: _up(port), "motif to come up")
        client.start()
        until(lambda: "PROBE stat stuck on" in text(), "the library request's stat to get stuck")
        sent = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=_DOCKER_STOP_GRACE_S + 5)
            exited = time.monotonic() - sent
        except subprocess.TimeoutExpired:
            exited = None
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(10)
    log = text()
    assert f"child imports {REPO / 'app' / 'main.py'} {REPO / 'app' / 'web' / 'api.py'}" in log, "the child ran another tree"
    assert "PROBE stat stuck on library-stat_" in log, "premise: the stuck stat is on a library stat pool worker"
    assert exited is not None, \
        f"motif did not exit within {_DOCKER_STOP_GRACE_S + 5:.0f} s of SIGTERM: exit waited on the stuck stat\n{log[-3000:]}"
    assert exited < _DOCKER_STOP_GRACE_S, f"exit took {exited:.2f} s after SIGTERM — docker SIGKILLs at the grace"
    assert proc.returncode == 0, log[-4000:]
    assert "still waited on a file stat" in log and log.index("still waited on a file stat") < log.index("motif stopped")
