"""v0.51.346: SELECT ALL FILTERED is one unpaged GET /api/library?selection=true.

The selection is the paged walk's rows projected to the columns the bulk code reads, in page order, for the view the
row list shows; its body is built and rendered off the event loop; the post-stat matchers' own inputs ride the statement
but never the body; and the page code reads no row field outside the selection columns.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import re
import shutil
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlsplit

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db

AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the SELECT ALL page checks would silently not run")

needs_node = pytest.mark.skipif(not _NODE, reason="node not installed")

# canon: ok / empty / missing; place: real (theme.mp3 there) / gone; upload: (plex rating_key, theme_present);
# lps: plex_independent_theme; ek: edition_key; job: (job_type, status); pending: pending_updates kind (title-global).
MOVIES = [
    dict(rk="t1", title="Tie", canon="ok", place="real"),
    dict(rk="t2", title="tie", canon="missing", place="real"),
    dict(rk="t3", title="TIE", canon="ok"),
    dict(rk="m01", canon="ok", place="real", year=1999),
    dict(rk="m02", canon="ok", place="gone", year=2004),
    dict(rk="m03", canon="missing", place="real"),
    dict(rk="m04", canon="ok", lps=1),
    dict(rk="m05", canon="ok", reason="backup_only"),
    dict(rk="m06", canon="ok", reason="plex_rejected:over_ceiling"),
    dict(rk="m07", canon="ok", year=2011),
    dict(rk="m08", canon="ok", upload=("m08", 1)),
    dict(rk="m09", canon="ok", upload=("dead-m09", 0)),
    dict(rk="m10", theme=False, has_theme=1, verified=1),
    dict(rk="m11", canon="ok", place="real", failure="video_removed"),
    dict(rk="m12", canon="ok", place="real", failure="cookies_expired", acked=True),
    dict(rk="m13", canon="ok", place="real", mismatch="pending"),
    dict(rk="m14", canon="ok", place="real", pending="upstream_changed"),
    dict(rk="m15", local=1, has_theme=1, pending="new_theme_available"),
    dict(rk="m16", job=("download", "pending")),
    dict(rk="m17", canon="ok", job=("place", "running")),
    dict(rk="m18", canon="ok", place="real", dropped=True),
    dict(rk="m19", canon="ok", lps=1, ek="extended", tmdb_of="m04"),
    dict(rk="m20", local=1, has_theme=1, lps=1),
    dict(rk="m21", canon="ok", place="real", kind="url", vid="at-21"),
    dict(rk="m22", canon="empty", place="real", year=1987),
    dict(rk="m23", canon="missing", upload=("dead-m23", 0), reason="backup_only", lps=1),
]
FOURK = [dict(rk="k01", canon="ok", place="real", tmdb_of="m01"), dict(rk="k02", canon="missing", place="real")]
COLLECTIONS = [
    dict(rk="c01", canon="ok", upload=("c01", 1)),
    dict(rk="c02", canon="ok", upload=("dead-c02", 0)),
    dict(rk="c03", canon="missing", reason="backup_only", section="2"),
    dict(rk="c04", section="2"),
    dict(rk="c05", canon="ok", upload=("c05", 1), section="2"),
]
NOT_IN_PLEX = [("movie", "Only In TDB A"), ("movie", "only in tdb b"), ("collection", "TDB Collection")]


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _url(tag):
    return f"https://www.youtube.com/watch?v={tag}"


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
    lib = SimpleNamespace(tc=TestClient(api.create_app(s)), db=s.db_path, themes=tmp_path / "themes",
                          media=tmp_path / "media", api=api, tmp=tmp_path)
    _seed(lib)
    return lib


def _seed(lib):
    now = _now()
    with contextlib.closing(sqlite3.connect(lib.db)) as c, c:
        for sid, fourk in (("1", 0), ("2", 1)):
            c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included,"
                      " discovered_at, last_seen_at) VALUES (?, ?, 'movie', 0, ?, ?, 1, ?, ?)",
                      (sid, f"Movies {sid}", fourk, f"movies{sid}", now, now))
        tmdb_of = {}
        rows = ([(r, "movie", "1") for r in MOVIES] + [(r, "movie", "2") for r in FOURK]
                + [(r, "collection", r.get("section", "1")) for r in COLLECTIONS])
        for n, (r, mt, sid) in enumerate(rows, start=1):
            title = r.get("title", f"Title {n:02d}")
            theme = r.get("theme", True)
            if r.get("tmdb_of"):
                tid, tmdb = tmdb_of[r["tmdb_of"]]
            else:
                tid, tmdb = n, 7000 + n
                if theme:
                    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                              " first_seen_sync_at, youtube_url, failure_kind, failure_acked_at, tdb_dropped_at)"
                              " VALUES (?, ?, ?, ?, 'imdb', ?, ?, ?, ?, ?, ?)",
                              (tid, mt, tmdb, title, now, now, _url(f"new{n:08d}"), r.get("failure"),
                               now if r.get("acked") else None, now if r.get("dropped") else None))
            tmdb_of[r["rk"]] = (tid, tmdb)
            ek = r.get("ek", "")
            c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, year,"
                      " edition_key, folder_path, has_theme, local_theme_file, plex_independent_theme,"
                      " plex_theme_verified_ok, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (r["rk"], sid, mt, tid if theme else None, None if mt == "collection" else tmdb, title,
                       r.get("year"), ek, f"/nonexistent/{r['rk']}", r.get("has_theme", 0), r.get("local", 0),
                       r.get("lps", 0), r.get("verified"), now, now))
            if r.get("canon"):
                rel = f"{mt}/{r['rk']}.mp3"
                c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path,"
                          " downloaded_at, source_video_id, provenance, source_kind, mismatch_state,"
                          " last_place_attempt_reason) VALUES (?, ?, ?, ?, ?, ?, ?, 'auto', ?, ?, ?)",
                          (mt, tmdb, sid, ek, rel, now, r.get("vid", f"vid{n:08d}"), r.get("kind", "themerrdb"),
                           r.get("mismatch"), r.get("reason")))
                if r["canon"] != "missing":
                    (lib.themes / mt).mkdir(parents=True, exist_ok=True)
                    (lib.themes / rel).write_bytes(b"canonical" if r["canon"] == "ok" else b"")
            if r.get("place"):
                folder = lib.media / r["place"] / r["rk"]
                if r["place"] == "real":
                    folder.mkdir(parents=True)
                    (folder / "theme.mp3").write_bytes(b"placed")
                c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder,"
                          " placed_at, placement_kind, plex_refreshed, theme_present, provenance)"
                          " VALUES (?, ?, ?, ?, ?, ?, 'hardlink', 1, 1, 'auto')", (mt, tmdb, sid, ek, str(folder), now))
            if r.get("upload"):
                plex_rk, present = r["upload"]
                c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder,"
                          " placed_at, placement_kind, plex_rating_key, plex_refreshed, theme_present, provenance)"
                          " VALUES (?, ?, ?, ?, '', ?, 'plex_upload', ?, 1, ?, 'auto')",
                          (mt, tmdb, sid, ek, now, plex_rk, present))
            if r.get("pending"):
                c.execute("INSERT INTO pending_updates (media_type, tmdb_id, section_id, edition_key, decision,"
                          " detected_at, old_youtube_url, new_youtube_url, kind) VALUES (?, ?, '', '', 'pending', ?, ?, ?, ?)",
                          (mt, tmdb, now, _url(f"old{n:08d}"), _url(f"nxt{n:08d}"), r["pending"]))
            if r.get("job"):
                job_type, status = r["job"]
                c.execute("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, payload, status, created_at)"
                          " VALUES (?, ?, ?, ?, '{}', ?, ?)", (job_type, mt, tmdb, sid, status, now))
        for n, (mt, title) in enumerate(NOT_IN_PLEX, start=900):
            c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                      " first_seen_sync_at, youtube_url) VALUES (?, ?, ?, ?, 'imdb', ?, ?, ?)",
                      (n, mt, 9000 + n, title, now, now, _url(f"nip{n:08d}")))


def _get(lib, **params):
    r = lib.tc.get("/api/library", params=params, headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()


def _walk(lib, per_page, **params):
    # every page until an empty one
    rows = []
    for page in range(1, 500):
        body = _get(lib, page=page, per_page=per_page, **params)
        if not body["items"]:
            return rows
        rows += body["items"]
    raise AssertionError("walk never ended")


def _selection(lib, **params):
    # page 2 of per_page 1 on purpose: a selection ignores both
    return _get(lib, selection="true", page=2, per_page=1, **params)


def _rebuilt(body):
    return [dict(zip(body["columns"], vals)) for vals in body["rows"]]


SHAPES = [
    dict(tab="movies"), dict(tab="movies", sort="year", sort_dir="desc"), dict(tab="movies", sort="attention"),
    dict(tab="movies", status="placed"), dict(tab="movies", status="dl_missing"),
    dict(tab="movies", status="dl_missing", pl_pills="on"), dict(tab="movies", dl_pills="on"),
    dict(tab="movies", dl_pills="broken"), dict(tab="movies", dl_pills="on,broken"), dict(tab="movies", pl_pills="on"),
    dict(tab="movies", pl_pills="await,on"), dict(tab="movies", pl_pills="await,broken"),
    dict(tab="movies", pl_pills="off,broken"), dict(tab="movies", attn_pills="await,broken"),
    dict(tab="movies", attn_pills="broken,update,restore,repush"), dict(tab="movies", attn_pills="fail,cookies,mismatch"),
    dict(tab="movies", src_pills="P,M"), dict(tab="movies", tdb_pills="update"), dict(tab="movies", q="tie"),
    dict(tab="movies", all_res="true"), dict(tab="movies", all_res="true", dl_pills="broken"),
    dict(tab="movies", fourk="true"), dict(tab="collections"), dict(tab="collections", section_id="2"),
    dict(tab="collections", pl_pills="on,broken"), dict(tab="collections", attn_pills="await,repush"),
    dict(tab="movies", status="not_in_plex"), dict(tab="movies", status="not_in_plex", sort="year", sort_dir="desc"),
    dict(tab="collections", status="not_in_plex"),
]


@pytest.mark.parametrize("params", SHAPES, ids=lambda p: "-".join(f"{k}={v}" for k, v in p.items()))
def test_the_selection_is_the_paged_walk_projected_to_its_columns(lib, params):
    walk = _walk(lib, 2, **params)
    assert walk, params
    body = _selection(lib, **params)
    assert set(body) == {"total", "tab", "fourk", "columns", "rows"}, sorted(body)
    assert body["columns"] == [c for c in lib.api._LIB_SELECTION_COLUMNS if c in walk[0]]
    assert _rebuilt(body) == [{c: it[c] for c in body["columns"]} for it in walk], params
    assert body["total"] == len(body["rows"]) == len(walk)


@pytest.mark.parametrize("params", [dict(tab="movies"), dict(tab="movies", dl_pills="on", attn_pills="broken,update"),
                                    dict(tab="movies", status="dl_missing"), dict(tab="movies", status="not_in_plex")],
                         ids=["default", "post-stat", "dl-missing", "not-in-plex"])
def test_a_selection_runs_one_statement_with_no_count_and_no_meta(lib, monkeypatch, params):
    statements = []
    real = lib.api.get_conn

    @contextlib.contextmanager
    def get_conn(db_path):
        with real(db_path) as conn:
            conn.set_trace_callback(statements.append)
            yield conn

    monkeypatch.setattr(lib.api, "get_conn", get_conn)
    assert _selection(lib, **params)["rows"]
    reads = [s for s in statements if s.lstrip().upper().startswith(("SELECT", "WITH"))]
    assert len(reads) == 1, reads
    # the trace carries bound values: a paged statement ends LIMIT <per_page> OFFSET <n>, an unbounded one does not
    assert "sync_runs" not in reads[0] and not re.search(r"\bLIMIT \d+ OFFSET \d+\s*$", reads[0])

    def counts():
        return [s for s in statements if s.lstrip().upper().startswith("SELECT COUNT(*)")]

    assert not counts()
    # the control: a paged request reads the banner scalars, and counts unless its post-stat walk is the count
    statements.clear()
    _get(lib, page=1, per_page=5, **params)
    assert any("sync_runs" in s for s in statements)
    assert any(re.search(r"\bLIMIT \d+ OFFSET \d+\s*$", s) for s in statements) == (params.get("status") == "not_in_plex"
                                                                                  or len(params) == 1)
    assert bool(counts()) == (len(params) == 1 or params.get("status") == "not_in_plex")


def test_the_default_walk_spans_the_edge_states_the_selection_must_keep(lib):
    walk = _walk(lib, 2, tab="movies")
    titles = [it["plex_title"].lower() for it in walk]
    # equal NOCASE titles straddle the first page boundary
    assert titles[1] == titles[2] == "tie"
    by_rk = {it["rating_key"]: it for it in walk}
    assert by_rk["m08"]["media_folder"] == "" and by_rk["m08"]["placement_kind"] == "plex_upload"
    assert by_rk["m09"]["media_folder"] is None and by_rk["m09"]["placement_kind"] is None and by_rk["m09"]["needs_repush"]
    assert by_rk["m03"]["canonical_missing"] and by_rk["t2"]["canonical_missing"]
    assert (by_rk["m16"]["job_in_flight"], by_rk["m17"]["job_in_flight"]) == ("download", "place")
    assert by_rk["m14"]["pending_update"] == 1 and by_rk["m15"]["pending_update_kind"] == "new_theme_available"
    assert by_rk["m13"]["mismatch_state"] == "pending" and by_rk["m12"]["failure_acked_at"] and by_rk["m18"]["tdb_dropped_at"]
    assert by_rk["m19"]["edition_key"] == "extended"
    sel = {it["rating_key"]: it for it in _rebuilt(_selection(lib, tab="movies"))}
    assert sel["m08"]["media_folder"] == "" and sel["m09"]["media_folder"] is None
    # the view's own scope: // ALL adds the 4K rows, a section chip keeps its own collections
    assert {"k01", "k02"} <= {it["rating_key"] for it in _rebuilt(_selection(lib, tab="movies", all_res="true"))}
    assert {it["section_id"] for it in _rebuilt(_selection(lib, tab="collections", section_id="2"))} == {"2"}


@pytest.mark.parametrize("params", [dict(pl_pills="await,on"), dict(pl_pills="await,broken"),
                                    dict(attn_pills="await,broken")], ids=["pl-await-on", "pl-await-broken", "attn-await-broken"])
def test_a_terminal_place_reason_keeps_its_row_out_of_an_await_selection(lib, params):
    rows = {it["rating_key"]: it for it in _walk(lib, 50, tab="movies")}
    # premise: m05 / m06 / m07 differ only in last_place_attempt_reason, a column the selection body never carries
    for rk in ("m05", "m06", "m07"):
        it = rows[rk]
        assert (bool(it["file_path"]), it["media_folder"], it["plex_independent_theme"], it["canonical_missing"]) \
            == (True, None, 0, False), rk
    assert [rows[rk]["last_place_attempt_reason"] for rk in ("m05", "m06", "m07")] \
        == ["backup_only", "plex_rejected:over_ceiling", None]
    body = _selection(lib, tab="movies", **params)
    got = {it["rating_key"] for it in _rebuilt(body)}
    assert "m07" in got and not {"m05", "m06"} & got, (params, sorted(got))
    assert "last_place_attempt_reason" not in body["columns"]


def test_the_selection_body_never_meets_jsonable_encoder_or_the_event_loop(lib, monkeypatch):
    import fastapi.routing
    encoded = []
    real_encoder = fastapi.routing.jsonable_encoder

    def encoder(obj, *a, **k):
        if isinstance(obj, dict) and "columns" in obj:
            raise AssertionError("the selection body reached jsonable_encoder on the event loop")
        encoded.append(sorted(obj) if isinstance(obj, dict) else type(obj).__name__)
        return real_encoder(obj, *a, **k)

    dumped = []
    real_dumps = json.dumps

    def dumps(obj, *a, **k):
        try:
            asyncio.get_running_loop()
            on_loop = True
        except RuntimeError:
            on_loop = False
        kind = ("page" if isinstance(obj, dict) and "items" in obj
                else "head" if isinstance(obj, dict) and "columns" in obj
                else "rows" if isinstance(obj, list) and obj and all(isinstance(r, list) for r in obj) else "other")
        dumped.append((kind, on_loop))
        return real_dumps(obj, *a, **k)

    monkeypatch.setattr(fastapi.routing, "jsonable_encoder", encoder)
    monkeypatch.setattr(json, "dumps", dumps)
    monkeypatch.setattr(lib.api, "_LIB_SELECTION_CHUNK", 2)
    for params in (dict(tab="movies"), dict(tab="movies", status="not_in_plex")):
        dumped.clear()
        n = len(_selection(lib, **params)["rows"])
        assert n >= 2, params
        # the head and every 2-row chunk were rendered in the threadpool; nothing of the body on the loop
        assert dumped.count(("head", False)) == 1 and dumped.count(("rows", False)) == -(-n // 2), (params, dumped)
        assert not [d for d in dumped if d[1] and d[0] != "other"], (params, dumped)
    dumped.clear()
    assert _get(lib, tab="movies", page=1, per_page=5)["items"]
    # premise: both recorders see the loop — the paged dict went through jsonable_encoder and rendered there
    assert any("items" in e for e in encoded if isinstance(e, list))
    assert ("page", True) in dumped


@pytest.mark.parametrize("chunk", [1, 2, 10**6])
def test_the_chunked_selection_body_is_byte_for_byte_what_jsonresponse_renders(lib, monkeypatch, chunk):
    from starlette.responses import JSONResponse
    with contextlib.closing(sqlite3.connect(lib.db)) as c, c:
        c.execute("UPDATE plex_items SET title = ? WHERE rating_key = 'm07'", ("Amélie — 東京 \"q\"",))
        c.execute("UPDATE themes SET title = ? WHERE title = 'Only In TDB A'", ("Ünïcode Ä",))
    monkeypatch.setattr(lib.api, "_LIB_SELECTION_CHUNK", chunk)
    for params in (dict(tab="movies"), dict(tab="movies", status="not_in_plex"), dict(tab="movies", q="no such title")):
        r = lib.tc.get("/api/library", params=dict(params, selection="true"), headers=AUTH)
        assert r.status_code == 200 and r.headers["content-type"] == "application/json", r.text
        body = r.json()
        assert r.content == JSONResponse(body).body, params
        if params.get("q"):
            assert body["rows"] == [] and body["total"] == 0
        else:
            assert len(body["rows"]) >= 2 and ("東京".encode() in r.content or "Ünïcode".encode() in r.content)


@pytest.mark.parametrize("chunk", [1, 2, 3])
def test_a_body_of_many_chunks_carries_every_row_in_page_order(lib, monkeypatch, chunk):
    # v0.51.346: /movies is ~22 chunks in production; the tests above only compare rows inside one chunk
    monkeypatch.setattr(lib.api, "_LIB_SELECTION_CHUNK", chunk)
    for params in (dict(tab="movies"), dict(tab="movies", pl_pills="await,on"), dict(tab="movies", sort="year", sort_dir="desc")):
        walk = _walk(lib, 2, **params)
        body = _selection(lib, **params)
        assert len(walk) > 2 * chunk, (params, chunk, len(walk))
        assert _rebuilt(body) == [{c: it[c] for c in body["columns"]} for it in walk], (params, chunk)


def test_a_slow_selection_logs_no_slow_query_warning(lib, monkeypatch, caplog):
    clock = iter(range(0, 10**6, 5))
    monkeypatch.setattr(time, "monotonic", lambda: next(clock))
    kw = dict(tab="movies", fourk=False, q="", status="all", page=1, per_page=50, themes_dir=lib.themes)
    with caplog.at_level(logging.WARNING, logger=lib.api.log.name):
        lib.api._library_main_query(lib.db, selection=True, **kw)
        assert not [r for r in caplog.records if "slow /api/library query" in r.getMessage()]
        # the control: the same clock over a paged request crosses the threshold and warns
        lib.api._library_main_query(lib.db, **kw)
    assert [r for r in caplog.records if "slow /api/library query" in r.getMessage()]


# The page's own app.js under node: bindLibrary registers the real handlers on fake elements, fetch answers from the
# payload (and records every request), and a selection can be seeded as Proxies that record every property read.
_HARNESS = r"""
"use strict";
const fs = require("fs");
const vm = require("vm");
const input = JSON.parse(fs.readFileSync(0, "utf8"));
const src = fs.readFileSync(input.appjs, "utf8");
const at = src.lastIndexOf("})();");
if (at < 0) throw new Error("app.js no longer ends in its IIFE");
const code = src.slice(0, at)
  + "\nglobalThis.__page = { libraryState, updateLibrarySelectionUi, bindLibrary, libKey, loadLibrary };\n" + src.slice(at);
const log = { requests: [], alerts: [], confirms: [], errors: [] };
const els = new Map();
function classes() {
  const s = new Set();
  return { add: (...c) => c.forEach((x) => s.add(x)), remove: (...c) => c.forEach((x) => s.delete(x)),
    toggle(c, f) { const on = f === undefined ? !s.has(c) : !!f; if (on) s.add(c); else s.delete(c); return on; },
    contains: (c) => s.has(c), forEach: (fn) => s.forEach(fn), get length() { return s.size; }, list: () => [...s].sort() };
}
function el(id, tag = "div") {
  const on = {};
  return {
    id, tagName: tag.toUpperCase(), nodeName: tag.toUpperCase(), nodeType: 1, style: { setProperty() {}, removeProperty() {} },
    dataset: {}, children: [], childNodes: [], classList: classes(), textContent: "", innerHTML: "", innerText: "", value: "",
    disabled: false, checked: false, indeterminate: false, hidden: false, title: "", className: "", open: false,
    offsetWidth: 0, offsetHeight: 0, clientWidth: 0, clientHeight: 0, scrollWidth: 0, scrollHeight: 0, scrollTop: 0,
    parentElement: null, parentNode: null, firstChild: null, lastChild: null, nextSibling: null, previousSibling: null,
    firstElementChild: null, lastElementChild: null, nextElementSibling: null, previousElementSibling: null, on,
    addEventListener(t, fn) { (on[t] = on[t] || []).push(fn); },
    removeEventListener(t, fn) { if (on[t]) on[t] = on[t].filter((f) => f !== fn); },
    dispatchEvent() { return true; }, querySelector() { return null; }, querySelectorAll() { return []; },
    getElementsByTagName() { return []; }, getElementsByClassName() { return []; }, appendChild(c) { return c; },
    append() {}, prepend() {}, insertBefore(c) { return c; }, insertAdjacentHTML() {}, insertAdjacentElement() {},
    removeChild(c) { return c; }, replaceChild() {}, remove() {}, replaceChildren() {}, replaceWith() {}, before() {},
    after() {}, cloneNode() { return el(null, tag); }, setAttribute(k, v) { this["@" + k] = String(v); },
    getAttribute(k) { return this["@" + k] ?? null; }, removeAttribute(k) { delete this["@" + k]; },
    hasAttribute(k) { return ("@" + k) in this; }, toggleAttribute() {}, closest() { return null; },
    contains() { return false; }, matches() { return false; }, getClientRects() { return []; },
    getBoundingClientRect() { return { top: 0, left: 0, right: 0, bottom: 0, width: 0, height: 0, x: 0, y: 0 }; },
    focus() {}, blur() {}, click() {}, scrollIntoView() {}, scrollTo() {}, showModal() {}, show() {}, close() {},
    select() {}, setSelectionRange() {}, animate() { return { cancel() {} }; },
  };
}
const byId = (id) => { if (!els.has(id)) els.set(id, el(id)); return els.get(id); };
const store = () => { const m = new Map(); return { getItem: (k) => (m.has(k) ? m.get(k) : null),
  setItem: (k, v) => m.set(k, String(v)), removeItem: (k) => m.delete(k), clear: () => m.clear(), key: () => null,
  get length() { return m.size; } }; };
const noop = () => {};
const ctx = {
  console: { log: noop, info: noop, debug: noop, warn: noop, error: (...a) => log.errors.push(a.map(String).join(" ")) },
  document: { getElementById: byId, querySelector: () => null, querySelectorAll: () => [], getElementsByClassName: () => [],
    getElementsByTagName: () => [], createElement: (t) => el(null, t), createTextNode: (t) => ({ textContent: t }),
    createDocumentFragment: () => el(null, "fragment"), addEventListener: noop, removeEventListener: noop,
    body: el("body", "body"), documentElement: el("html", "html"), head: el("head", "head"),
    visibilityState: "visible", hidden: false, activeElement: null, cookie: "", title: "", readyState: "complete" },
  location: { search: "", pathname: "/movies", href: "http://motif.test/movies", hash: "", origin: "http://motif.test",
    reload: noop, assign: noop, replace: noop },
  history: { replaceState: noop, pushState: noop, back: noop },
  navigator: { userAgent: "node", clipboard: { writeText: async () => {} }, sendBeacon: () => true },
  localStorage: store(), sessionStorage: store(),
  matchMedia: () => ({ matches: false, addEventListener: noop, removeEventListener: noop, addListener: noop }),
  getComputedStyle: () => ({ getPropertyValue: () => "", paddingRight: "0px", borderRightWidth: "0px" }),
  requestAnimationFrame: () => 0, cancelAnimationFrame: noop, queueMicrotask,
  setTimeout: () => 0, clearTimeout: noop, setInterval: () => 0, clearInterval: noop,
  ResizeObserver: class { observe() {} unobserve() {} disconnect() {} },
  MutationObserver: class { observe() {} disconnect() {} takeRecords() { return []; } },
  IntersectionObserver: class { observe() {} unobserve() {} disconnect() {} },
  Event: class { constructor(t) { this.type = t; } },
  CustomEvent: class { constructor(t, o) { this.type = t; this.detail = o && o.detail; } },
  URL: Object.assign(function (u, b) { return new URL(u, b || "http://motif.test"); }, { createObjectURL: () => "blob:x", revokeObjectURL: noop }),
  URLSearchParams, FormData: class { append() {} }, Blob: class {}, TextEncoder, TextDecoder, AbortController,
  alert: (m) => log.alerts.push(String(m)), confirm: (m) => { log.confirms.push(String(m)); return true; },
  prompt: () => null, addEventListener: noop, removeEventListener: noop, dispatchEvent: () => true, scrollTo: noop,
  innerWidth: 1400, innerHeight: 900, devicePixelRatio: 1,
  fetch: async (url, opts = {}) => {
    url = String(url);
    log.requests.push(input.brief ? null
      : { method: opts.method || "GET", url, body: typeof opts.body === "string" ? JSON.parse(opts.body) : null });
    let payload = { ok: true, enqueued: 0, skipped: 0 };
    if (url.startsWith("/api/library?")) {
      payload = new URLSearchParams(url.split("?")[1]).get("selection") === "true" && input.selection
        ? input.selection : { items: [], total: 0, per_page: 200, columns: [], rows: [] };
    }
    return { ok: true, status: 200, statusText: "OK", json: async () => payload, text: async () => JSON.stringify(payload) };
  },
};
ctx.window = ctx;
ctx.globalThis = ctx;
ctx.self = ctx;
vm.createContext(ctx);
vm.runInContext(code, ctx, { filename: "app.js" });
const P = ctx.__page;
byId("library-tab").value = input.tab || "movies";
P.bindLibrary();
const columns = new Set(input.columns || []);
const outside = {};
const inside = {};
const track = (row) => new Proxy(row, { get(t, k, r) {
  if (typeof k === "string" && /^[a-z][a-z0-9_]*$/.test(k)) { const m = columns.has(k) ? inside : outside; m[k] = (m[k] || 0) + 1; }
  return Reflect.get(t, k, r);
} });
const settle = async () => { for (let i = 0; i < 40; i++) await new Promise((r) => setImmediate(r)); };
const plain = (it) => Object.fromEntries(Object.keys(it).map((k) => [k, it[k]]));
function snapBar() {
  const out = {};
  for (const [id, e] of els) {
    if (!id || !id.startsWith("library-")) continue;
    out[id] = { display: e.style.display ?? null, text: e.textContent, title: e.title, disabled: e.disabled,
      checked: e.checked, indeterminate: e.indeterminate, classes: e.classList.list() };
  }
  return out;
}
(async () => {
  await settle();
  const out = [];
  for (const step of input.steps) {
    const mark = { r: log.requests.length, a: log.alerts.length, c: log.confirms.length };
    const st = P.libraryState;
    const res = { op: step.op, id: step.id };
    if (step.op === "state") {
      if (step.tab) byId("library-tab").value = step.tab;
      for (const [k, v] of Object.entries(step.set || {})) st[k] = Array.isArray(v) ? new Set(v) : v;
    } else if (step.op === "select") {
      st.selected.clear();
      st.selectedRows.clear();
      for (const row of input.rowsets[step.rows]) {
        const it = step.track ? track(Object.assign({}, row)) : Object.assign({}, row);
        const k = P.libKey(it);
        st.selected.add(k);
        st.selectedRows.set(k, it);
      }
    } else if (step.op === "load") {
      await P.loadLibrary();
    } else if (step.op === "ui") {
      P.updateLibrarySelectionUi();
      res.bar = snapBar();
    } else if (step.op === "click") {
      const e = byId(step.id);
      res.listeners = (e.on.click || []).length;
      for (const fn of e.on.click || []) {
        try { await fn({ currentTarget: e, target: e, preventDefault: noop, stopPropagation: noop }); }
        catch (err) { log.errors.push(step.id + ": " + (err && err.stack || err)); }
      }
      await settle();
      res.label = e.textContent;
    } else if (step.op === "snapshot") {
      res.selected = [...st.selected];
      res.rows = [...st.selectedRows.values()].map(plain);
    }
    await settle();
    res.requests = input.brief ? log.requests.length - mark.r : log.requests.slice(mark.r);
    res.alerts = log.alerts.slice(mark.a);
    res.confirms = log.confirms.slice(mark.c);
    out.push(res);
  }
  process.stdout.write(JSON.stringify({ steps: out, outside, inside, errors: log.errors }));
})().catch((e) => { process.stderr.write(String(e && e.stack || e)); process.exit(1); });
"""

BULK = ["library-download-selected-btn", "library-tdb-backup-btn", "library-cloud-backup-btn",
        "library-push-selected-btn", "library-switch-to-api-btn", "library-revert-mismatch-btn",
        "library-restore-from-plex-btn", "library-let-plex-serve-btn", "library-bulk-probe-tdb-btn",
        "library-adopt-and-lps-btn", "library-accept-all-updates-btn", "library-decline-all-updates-btn",
        "library-ack-selected-btn", "library-adopt-selected-btn", "library-export-csv-btn"]


def _drive(tmp_path, tab, steps, rowsets=None, selection=None, columns=None, brief=False):
    harness = tmp_path / "select_all_page.js"
    harness.write_text(_HARNESS)
    payload = {"appjs": str(APP_JS), "tab": tab, "steps": steps, "rowsets": rowsets or {}, "selection": selection,
               "columns": columns or [], "brief": brief}
    r = subprocess.run([_NODE, str(harness)], input=json.dumps(payload), capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stderr[-3000:]
    out = json.loads(r.stdout)
    assert not out["errors"], out["errors"][:5]
    return out


def _library_gets(step):
    return [q["url"] for q in step["requests"] if q["url"].startswith("/api/library?")]


def _query(url):
    return dict(parse_qsl(urlsplit(url).query))


VIEWS = [("movies", {"allRes": True, "fourk": False, "section_id": ""}),
         ("collections", {"allRes": False, "fourk": False, "section_id": "2"}),
         ("movies", {"allRes": False, "fourk": False, "section_id": ""})]


@needs_node
@pytest.mark.parametrize("tab,view", VIEWS, ids=["movies-all", "collections-section-2", "movies-standard"])
def test_select_all_filtered_selects_exactly_the_view_the_rows_list(lib, tmp_path, tab, view):
    steps = [{"op": "state", "tab": tab, "set": dict(view, tab=tab)}, {"op": "load"},
             {"op": "click", "id": "library-select-all-filtered-btn"}]
    loaded, clicked = _drive(tmp_path, tab, steps)["steps"][1:]
    (view_url,), (select_url,) = _library_gets(loaded), _library_gets(clicked)
    view_q, select_q = _query(view_url), _query(select_url)
    assert select_q.get("selection") == "true" and "page" not in select_q and "per_page" not in select_q
    view_q.pop("page")
    view_q.pop("per_page")
    listed = [it["rating_key"] for it in _walk(lib, 2, **view_q)]
    body = _get(lib, **select_q)
    assert [it["rating_key"] for it in _rebuilt(body)] == listed, (view_url, select_url)
    if view.get("allRes"):
        assert {"k01", "k02"} <= set(listed)
    if view.get("section_id"):
        assert listed and {it["section_id"] for it in _walk(lib, 50, **view_q)} == {view["section_id"]}
    # the same click answered by that body: one request, and every row selected under its own key
    steps[-1:] = [{"op": "click", "id": "library-select-all-filtered-btn"}, {"op": "snapshot"},
                  {"op": "click", "id": "library-export-csv-btn"}, {"op": "click", "id": "library-adopt-selected-btn"}]
    out = _drive(tmp_path, tab, steps, selection=body)["steps"]
    assert _library_gets(out[2]) == [select_url]
    assert out[2]["label"] == f"// {len(body['rows'])} SELECTED"
    assert out[3]["rows"] == _rebuilt(body)
    assert len(out[3]["selected"]) == len(body["rows"])
    # EXPORT CSV and the ADOPT scan still page, over the same view's scope, so they find every selected row
    scope = ("tab", "fourk", "all_res", "section_id")
    for walked in out[4:]:
        first = _query(_library_gets(walked)[0])
        assert "selection" not in first and first.get("page") == "1", walked["id"]
        assert {k: first.get(k) for k in scope} == {k: view_q.get(k) for k in scope}, walked["id"]


PARITY = [("movies", {}, {}), ("movies", {"attn_pills": "await,broken"}, {"attnPills": ["await", "broken"]}),
          ("movies", {"all_res": "true"}, {"allRes": True}), ("collections", {}, {})]


@needs_node
@pytest.mark.parametrize("tab,params,state", PARITY, ids=["movies", "movies-attn-await-broken", "movies-all", "collections"])
def test_one_select_all_request_paints_and_acts_as_the_full_paged_rows_did(lib, tmp_path, tab, params, state):
    body = _selection(lib, tab=tab, **params)
    full = _walk(lib, 2, tab=tab, **params)
    assert len(full) >= 5 and _rebuilt(body) == [{c: it[c] for c in body["columns"]} for it in full]
    base = [{"op": "state", "tab": tab, "set": dict(state, tab=tab)}]
    bulk = [s for h in BULK for s in ({"op": "select", "rows": "rows"}, {"op": "click", "id": h})]
    old = _drive(tmp_path, tab, base + [{"op": "select", "rows": "rows"}, {"op": "ui"}] + bulk, rowsets={"rows": full})
    new = _drive(tmp_path, tab, base + [{"op": "click", "id": "library-select-all-filtered-btn"}, {"op": "ui"}] + bulk,
                 rowsets={"rows": _rebuilt(body)}, selection=body)
    assert len(_library_gets(new["steps"][1])) == 1
    select_all = "library-select-all-filtered-btn"
    assert new["steps"][1]["label"] == f"// {len(body['rows'])} SELECTED"
    assert {k: v for k, v in new["steps"][2]["bar"].items() if k != select_all} == \
        {k: v for k, v in old["steps"][2]["bar"].items() if k != select_all}
    acted = 0
    for o, n in zip(old["steps"][3:], new["steps"][3:]):
        if o["op"] == "click":
            assert o["listeners"] == n["listeners"] == 1, o["id"]
            assert (n["requests"], n["alerts"], n["confirms"], n["label"]) == \
                (o["requests"], o["alerts"], o["confirms"], o["label"]), o["id"]
            acted += bool(o["requests"])
    assert acted >= 5


def _synthetic_rows(n):
    rnd = random.Random(346)
    pick = rnd.choice
    ts = _now()  # this run's clock, never a fixed date
    rows = []
    for i in range(n):
        rows.append({
            "rating_key": f"r{i}", "section_id": pick(["1", "2", "18"]), "plex_media_type": pick(["movie", "show", "collection"]),
            "plex_title": f"T{i}", "folder_path": pick([None, f"/media/f{i}"]), "edition_key": pick(["", "", "extended"]),
            "plex_has_theme": pick([0, 1]), "plex_local_theme": pick([0, 1]), "plex_theme_verified_ok": pick([None, 0, 1]),
            "plex_independent_theme": pick([0, 1]), "theme_tmdb": pick([None, 100 + i, -(100 + i)]),
            "theme_media_type": pick([None, "movie", "tv", "collection"]), "youtube_url": pick([None, _url(f"y{i}")]),
            "failure_kind": pick([None, None, "video_removed", "cookies_expired", "video_private"]),
            "failure_acked_at": pick([None, ts]), "upstream_source": pick([None, "imdb", "themoviedb", "plex_orphan"]),
            "tdb_dropped_at": pick([None, None, ts]), "file_path": pick([None, f"m/{i}.mp3"]),
            "source_video_id": pick([None, f"v{i}", f"at-{i}"]),
            "source_kind": pick([None, "themerrdb", "url", "adopt", "upload", "plex_cloud"]),
            "mismatch_state": pick([None, None, "pending"]), "media_folder": pick([None, "", f"/media/f{i}"]),
            "placement_kind": pick([None, "hardlink", "copy", "plex_upload"]),
            "placement_provenance": pick([None, "auto", "manual", "adopt"]),
            "job_in_flight": pick([None, None, "download", "place"]), "pending_update": pick([0, 0, 1]),
            "pending_update_kind": pick([None, "upstream_changed", "urls_match", "new_theme_available"]),
            "canonical_missing": pick([False, False, True]),
        })
    return rows


COVERAGE_STATES = [{}, {"attnPills": ["fail", "update", "mismatch", "await", "broken"]}, {"tdbPills": ["update"]},
                   {"attnPills": ["update"], "status": "updates"}]


@needs_node
def test_page_code_reads_no_row_field_outside_the_selection_columns(lib, tmp_path):
    columns = list(lib.api._LIB_SELECTION_COLUMNS)
    assert set(_synthetic_rows(1)[0]) == set(columns)
    real = _rebuilt(_selection(lib, tab="movies", all_res="true")) + _rebuilt(_selection(lib, tab="collections"))
    rowsets = {"real": real, "synthetic": _synthetic_rows(4000)}
    steps = []
    for state in COVERAGE_STATES:
        for rows in rowsets:
            steps += [{"op": "state", "set": state}, {"op": "select", "rows": rows, "track": True}, {"op": "ui"}]
            steps += [s for h in BULK for s in ({"op": "select", "rows": rows, "track": True}, {"op": "click", "id": h})]
    out = _drive(tmp_path, "movies", steps, rowsets=rowsets, columns=columns, brief=True)
    assert out["outside"] == {}, out["outside"]
    # premise: the drive reached the branches that read the rarer selection fields
    assert {"edition_key", "pending_update_kind", "canonical_missing", "job_in_flight", "mismatch_state",
            "placement_provenance", "source_video_id", "tdb_dropped_at"} <= set(out["inside"]), sorted(out["inside"])
    assert sum(s["requests"] for s in out["steps"] if s["op"] == "click") > 1000
