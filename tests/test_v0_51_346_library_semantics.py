"""v0.51.346: /api/library filters, header totals and the row display agree.

Every post-stat-capable pill pair (DL, PL, ATTN) returns the union of its two pills in page order, the header total
is the number of rows a walk returns, each PL / DL / ATTN filter selects exactly the rows renderLibraryRow paints in
that state, and DOWNLOAD MISSING keeps the scope the dropped missing_count used to mirror.
"""
from __future__ import annotations

import contextlib
import itertools
import json
import os
import re
import shutil
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from _slice_helpers import slice_between
from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db

AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the row-display mirror would silently not run")

AXES = {"dl_pills": ("on", "off", "broken"), "pl_pills": ("on", "await", "off", "broken"),
        "attn_pills": ("fail", "cookies", "update", "mismatch", "await", "broken", "restore", "repush")}

# canon: ok / empty / missing; place: real (theme.mp3 there) / gone; upload: (plex rating_key, theme_present);
# lps: plex_independent_theme; prev: (previous_urls kind, url tag); guid: guid_tmdb override (None = no guid).
MOVIES = [
    dict(rk="m01", canon="ok", place="real", pending=True),
    dict(rk="m02", canon="ok", place="gone"),
    dict(rk="m03", canon="missing", place="real"),
    dict(rk="m04", canon="missing", reason="backup_only", lps=1),
    dict(rk="m05", canon="ok", reason="backup_only", lps=1, failure="cookies_expired"),
    dict(rk="m06", canon="ok", reason="backup_only"),
    dict(rk="m07", canon="ok", lps=1),
    dict(rk="m08", canon="ok"),
    dict(rk="m09", canon="ok", reason="plex_rejected:over_ceiling"),
    dict(rk="m10", canon="ok", upload=("dead-m10", 0)),
    dict(rk="m11", upload=("dead-m11", 0)),
    dict(rk="m12", canon="ok", upload=("m12", 1)),
    dict(rk="m13", theme=False),
    dict(rk="m14", prev=("themerrdb", "old14")),
    dict(rk="m15", canon="ok", prev=("user", "old15"), yt=False),
    dict(rk="m16", canon="ok", place="real", failure="video_removed"),
    dict(rk="m17", canon="ok", place="real", mismatch="pending"),
    dict(rk="m18", canon="empty", place="real"),
    dict(rk="m19", canon="ok", place="real", guid=880019),
    dict(rk="m20", canon="missing", upload=("dead-m20", 0), prev=("themerrdb", "old20")),
    dict(rk="m21", unlinked=True),
    dict(rk="m22", canon="missing", place="real", failure="video_private"),
    dict(rk="m23", canon="missing", place="gone"),
    dict(rk="m24", canon="missing", place="gone", lps=1),
    dict(rk="m25", canon="ok", place="real", guid=880025),
    dict(rk="m26", canon="ok", place="real", prev=("user", "old26")),
    dict(rk="m27", canon="ok", pending=True),
]
COLLECTIONS = [
    dict(rk="c01", canon="ok", upload=("c01", 1), guid=None),
    dict(rk="c02", canon="ok", upload=("dead-c02", 0), guid=None),
    dict(rk="c03", canon="missing", reason="backup_only", guid=None),
    dict(rk="c04", guid=None),
]


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
    return SimpleNamespace(tc=TestClient(api.create_app(s)), db=s.db_path, themes=tmp_path / "themes",
                           media=tmp_path / "media")


def _seed(lib):
    now = _now()
    with contextlib.closing(sqlite3.connect(lib.db)) as c, c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included,"
                  " discovered_at, last_seen_at) VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)", (now, now))
        for n, r in enumerate(MOVIES + COLLECTIONS, start=1):
            mt = "collection" if r["rk"].startswith("c") else "movie"
            tmdb, title = 7000 + n, f"Title {n:02d}"
            has_theme = r.get("theme", True)
            if has_theme:
                c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                          " first_seen_sync_at, youtube_url, failure_kind) VALUES (?, ?, ?, ?, 'imdb', ?, ?, ?, ?)",
                          (n, mt, tmdb, title, now, now, None if r.get("yt") is False else _url(f"new{n:08d}"),
                           r.get("failure")))
            c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title,"
                      " edition_key, folder_path, has_theme, local_theme_file, plex_independent_theme,"
                      " plex_theme_verified_ok, first_seen_at, last_seen_at)"
                      " VALUES (?, '1', ?, ?, ?, ?, '', ?, 0, 0, ?, 1, ?, ?)",
                      (r["rk"], mt, n if has_theme and not r.get("unlinked") else None, r.get("guid", tmdb), title,
                       f"/nonexistent/{r['rk']}", r.get("lps", 0), now, now))
            if r.get("unlinked"):
                # a guid-matched title with no theme_id link: its placement and download render on no row
                r = dict(r, canon="ok", place="real")
            if r.get("canon"):
                rel = f"movies/{r['rk']}.mp3"
                c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path,"
                          " downloaded_at, source_video_id, provenance, source_kind, mismatch_state,"
                          " last_place_attempt_reason) VALUES (?, ?, '1', '', ?, ?, ?, 'auto', 'themerrdb', ?, ?)",
                          (mt, tmdb, rel, now, f"vid{n:08d}", r.get("mismatch"), r.get("reason")))
                if r["canon"] != "missing":
                    (lib.themes / "movies").mkdir(parents=True, exist_ok=True)
                    (lib.themes / rel).write_bytes(b"canonical" if r["canon"] == "ok" else b"")
            if r.get("place"):
                folder = lib.media / r["place"] / r["rk"]
                if r["place"] == "real":
                    folder.mkdir(parents=True)
                    (folder / "theme.mp3").write_bytes(b"placed")
                c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder,"
                          " placed_at, placement_kind, plex_refreshed, theme_present)"
                          " VALUES (?, ?, '1', '', ?, ?, 'hardlink', 1, 1)", (mt, tmdb, str(folder), now))
            if r.get("upload"):
                plex_rk, present = r["upload"]
                c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder,"
                          " placed_at, placement_kind, plex_rating_key, plex_refreshed, theme_present)"
                          " VALUES (?, ?, '1', '', '', ?, 'plex_upload', ?, 1, ?)", (mt, tmdb, now, plex_rk, present))
            if r.get("prev"):
                kind, tag = r["prev"]
                c.execute("INSERT INTO previous_urls (media_type, tmdb_id, section_id, youtube_url, kind, captured_at)"
                          " VALUES (?, ?, '1', ?, ?, ?)", (mt, tmdb, _url(tag), kind, now))
            if r.get("pending"):
                c.execute("INSERT INTO pending_updates (media_type, tmdb_id, section_id, edition_key, decision,"
                          " detected_at, old_youtube_url, new_youtube_url, kind)"
                          " VALUES (?, ?, '', '', 'pending', ?, ?, ?, 'upstream_changed')",
                          (mt, tmdb, now, _url(f"old{n:08d}"), _url(f"new{n:08d}")))


def _get(lib, **params):
    r = lib.tc.get("/api/library", params=params, headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()


def _walk(lib, per_page, **params):
    # Every page until an empty one; the header total must equal what the pages delivered.
    first = _get(lib, page=1, per_page=per_page, **params)
    rows = list(first["items"])
    for page in range(2, 500):
        body = _get(lib, page=page, per_page=per_page, **params)
        assert body["total"] == first["total"], (params, page, body["total"], first["total"])
        if not body["items"]:
            break
        assert len(rows) % per_page == 0, (params, page, len(rows))
        rows += body["items"]
    assert first["total"] == len(rows), (params, first["total"], len(rows))
    return rows


def _key(it):
    return json.dumps(it, sort_keys=True)


def _rks(rows):
    return {it["rating_key"] for it in rows}


@pytest.mark.parametrize("axis", sorted(AXES))
@pytest.mark.parametrize("tab", ["movies", "collections"])
def test_every_post_stat_pill_pair_returns_the_union_of_its_pills_in_page_order(lib, tab, axis):
    _seed(lib)
    base = [_key(it) for it in _walk(lib, 50, tab=tab)]
    single = {p: {_key(it) for it in _walk(lib, 2, tab=tab, **{axis: p})} for p in AXES[axis]}
    if tab == "movies":
        assert all(0 < len(rows) < len(base) for rows in single.values()), {p: len(r) for p, r in single.items()}
    for a, b in itertools.combinations(AXES[axis], 2):
        got = [_key(it) for it in _walk(lib, 2, tab=tab, **{axis: f"{a},{b}"})]
        assert got == [k for k in base if k in single[a] or k in single[b]], (tab, axis, a, b)


COUNT_SHAPES = (
    [dict(status=s) for s in ("has_theme", "themed", "manual", "plex_agent", "untracked", "downloaded", "placed",
                              "unplaced", "failures", "updates")]
    + [dict(tdb="tracked"), dict(tdb="untracked"), dict(tdb_pills="tdb"), dict(tdb_pills="none,update")]
    + [dict(link_pills=p) for p in ("hl", "c", "m", "none", "pu", "rp", "b", "bk", "tb", "ab")]
    + [dict(src_pills=p) for p in ("T", "U", "A", "M", "P", "Pp", "-")]
    + [dict(pl_pills="await"), dict(pl_pills="off"), dict(pl_pills="await,off"), dict(dl_pills="off"),
       dict(ed_pills="has"), dict(ed_pills="none"), dict(loudness_pills="raw"),
       dict(loudness_pills="normalized,outliers"), dict(attn_pills="update,repush"), dict(sort="pl"), dict(q="Title")]
)


@pytest.mark.parametrize("tab", ["movies", "collections"])
def test_the_header_total_is_the_rows_a_walk_returns_on_every_filtered_shape(lib, tab):
    _seed(lib)
    for shape in COUNT_SHAPES:
        _walk(lib, 2, tab=tab, **shape)
    if tab == "collections":
        # the guid-keyed slim count read these theme_id-linked collections as 0
        assert "c01" in _rks(_walk(lib, 1, tab=tab, status="placed"))
        assert "c03" in _rks(_walk(lib, 1, tab=tab, status="downloaded"))
        assert "c01" in _rks(_walk(lib, 1, tab=tab, link_pills="pu"))
    else:
        # m19 / m25 link by theme_id with another guid, m21 by guid alone: a guid-keyed count drifts both ways
        placed = _rks(_walk(lib, 1, tab=tab, status="placed"))
        assert {"m19", "m25"} <= placed and "m21" not in placed


@pytest.mark.parametrize("extra", [dict(), dict(pl_pills="on"), dict(pl_pills="broken"), dict(attn_pills="fail,broken")],
                         ids=["alone", "pl-on", "pl-broken", "attn-fail-broken"])
def test_dl_missing_beside_a_post_stat_pill_slices_its_page_once(lib, extra):
    _seed(lib)
    whole = _get(lib, tab="movies", status="dl_missing", page=1, per_page=200, **extra)
    assert len(whole["items"]) >= 2 and whole["total"] == len(whole["items"])
    rows = _walk(lib, 1, tab="movies", status="dl_missing", **extra)
    assert [_key(it) for it in rows] == [_key(it) for it in whole["items"]]


def test_restore_and_repush_keep_their_rows_beside_broken(lib):
    _seed(lib)
    restore = _rks(_walk(lib, 2, tab="movies", attn_pills="restore"))
    repush = _rks(_walk(lib, 2, tab="movies", attn_pills="repush"))
    broken = _rks(_walk(lib, 2, tab="movies", attn_pills="broken"))
    assert {"m14", "m20"} <= restore and "m15" not in restore
    assert {"m10", "m11", "m20"} <= repush
    # m15's redundancy is NULL in SQL: the row columns read (1, 0), so composing them would wrongly admit it
    m15 = next(it for it in _walk(lib, 50, tab="movies") if it["rating_key"] == "m15")
    assert (m15["has_previous_url"], m15["revert_redundant"]) == (1, 0)
    # m26 differs and is not redundant, but restore is a SRC '-' / M chip and m26 reads T: out on both sides
    assert "m26" not in restore and "m26" not in _rks(_walk(lib, 2, tab="movies", src_pills="-,M"))
    assert _rks(_walk(lib, 2, tab="movies", attn_pills="broken,restore")) == restore | broken
    assert _rks(_walk(lib, 2, tab="movies", attn_pills="broken,repush")) == repush | broken


def test_update_beside_broken_keeps_the_update_chip(lib):
    _seed(lib)
    update = _rks(_walk(lib, 2, tab="movies", attn_pills="update"))
    broken = _rks(_walk(lib, 2, tab="movies", attn_pills="broken"))
    # m27's pending update is actionable, but it is downloaded and unplaced (SRC '-'): the chip leaves it out
    m27 = next(it for it in _walk(lib, 50, tab="movies") if it["rating_key"] == "m27")
    assert m27["actionable_update"] == 1 and not m27["canonical_missing"]
    assert "m27" in _rks(_walk(lib, 2, tab="movies", src_pills="-"))
    assert "m01" in update and "m27" not in update
    assert _rks(_walk(lib, 2, tab="movies", attn_pills="broken,update")) == update | broken
    # both chip columns ride one post-stat projection
    restore = _rks(_walk(lib, 2, tab="movies", attn_pills="restore"))
    assert _rks(_walk(lib, 2, tab="movies", attn_pills="broken,update,restore")) == update | broken | restore


def test_a_stale_plex_upload_awaits_on_the_sql_and_the_post_stat_side(lib):
    _seed(lib)
    for tab, stale, other in (("movies", "m10", "m08"), ("collections", "c02", None)):
        for params in (dict(attn_pills="await"), dict(attn_pills="await,broken"),
                       dict(pl_pills="await"), dict(pl_pills="await,on")):
            got = _rks(_walk(lib, 2, tab=tab, **params))
            assert stale in got, (tab, params)
            assert other is None or other in got, (tab, params)
            assert "m07" not in got and "m09" not in got, (tab, params)


_DISPLAY_CONSTS = ("downloaded", "placed", "dlBroken", "dl", "isPlexUpload", "lpsState", "awaitingApproval",
                   "placementBroken", "pl")


def _row_display_states(rows):
    # renderLibraryRow's own DL / PL derivations and its blue ! update gate, run under node against the API rows.
    body = slice_between(APP_JS, "function renderLibraryRow(it) {", "function renderLibraryRowNotInPlex(it) {")
    stmts = []
    for name in _DISPLAY_CONSTS:
        start = f"const {name} = "
        found, at = [], body.find(start)
        while at != -1:
            found.append(body[at:body.index(";", at) + 1])
            at = body.find(start, at + 1)
        assert found and len(set(found)) == 1, (name, found)
        stmts.append(found[0])
    gate_open = "} else if (it.actionable_update"
    assert body.count(gate_open) == 1
    update_gate = slice_between(body, gate_open, ") {\n")[len("} else if ("):]
    src_letter = slice_between(APP_JS, "  function computeSrcLetter(it) {", "\n  }") + "\n  }"
    script = ("const rows = JSON.parse(require('fs').readFileSync(0, 'utf8'));\n" + src_letter + "\n"
              "function derive(it) {\n" + "\n".join(stmts)
              + f"\nreturn {{dl, pl, awaitingApproval, blueUpdate: !!({update_gate})}};\n}}\n"
              "process.stdout.write(JSON.stringify(rows.map(derive)));\n")
    r = subprocess.run([_NODE, "-e", script], input=json.dumps(rows), capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    return {it["rating_key"]: state for it, state in zip(rows, json.loads(r.stdout))}


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_each_dl_pl_and_attn_filter_selects_the_rows_the_row_paints_in_that_state(lib):
    _seed(lib)
    for tab in ("movies", "collections"):
        rows = _walk(lib, 50, tab=tab)
        assert not any(it["job_in_flight"] for it in rows)
        shown = _row_display_states(rows)
        reason = {it["rating_key"]: it["last_place_attempt_reason"] for it in rows}
        stale = {it["rating_key"] for it in rows if it["needs_repush"]}
        # v0.51.346: no exception left — an over-ceiling row is terminal on both sides, so it paints gray and PL=off lists it
        awaiting = {rk for rk, s in shown.items() if s["awaitingApproval"]}
        expected = {
            ("dl_pills", "on"): {rk for rk, s in shown.items() if s["dl"] == "on"},
            ("dl_pills", "off"): {rk for rk, s in shown.items() if s["dl"] == ""},
            ("dl_pills", "broken"): {rk for rk, s in shown.items() if s["dl"] == "broken"},
            ("pl_pills", "on"): {rk for rk, s in shown.items() if s["pl"] in ("on", "pushed")},
            ("pl_pills", "broken"): {rk for rk, s in shown.items() if s["pl"] == "broken"},
            ("pl_pills", "off"): {rk for rk, s in shown.items() if s["pl"] == ""},
            ("pl_pills", "await"): awaiting,
            ("attn_pills", "await"): awaiting,
            ("attn_pills", "broken"): {rk for rk, s in shown.items() if s["dl"] == "broken"},
            ("attn_pills", "repush"): stale,
            # the blue ! gate: a failure's ⚠ would take that glyph slot first, and the chip still lists the row
            ("attn_pills", "update"): {rk for rk, s in shown.items() if s["blueUpdate"]},
        }
        if tab == "movies":
            assert "m01" in expected[("attn_pills", "update")] and "m27" not in expected[("attn_pills", "update")]
            assert {"m07", "m10", "m11"} & awaiting == {"m10"}
            # m09 is the over-ceiling row: terminal, so gray and in PL=off, never in an await pill (v1.24.46 / v0.51.68)
            assert "m09" not in awaiting and "m09" in expected[("pl_pills", "off")]
            assert {"m04", "m05", "m06", "m07", "m09", "m11"} <= expected[("pl_pills", "off")]
            assert {"m04", "m20", "m24"} <= expected[("dl_pills", "broken")]
        for (axis, pill), want in expected.items():
            assert _rks(_walk(lib, 3, tab=tab, **{axis: pill})) == want, (tab, axis, pill)


def test_the_broken_title_glyph_links_to_a_view_that_lists_its_own_row(lib):
    # v0.51.346: the ↺ glyph pointed at status=dl_missing, which also demands a placement — an LPS or backup-only row
    # with a missing canonical painted the glyph and then opened a view without it.
    _seed(lib)
    m = re.search(r'title-glyph-broken[^>]*href="/\$\{libraryState\.tab\}\?([^"]+)"', APP_JS)
    assert m, "the broken title glyph no longer links anywhere"
    params = dict(p.split("=", 1) for p in m.group(1).split("&"))
    rows = _walk(lib, 50, tab="movies")
    shown_broken = {rk for rk, s in _row_display_states(rows).items() if s["dl"] == "broken"}
    assert shown_broken, "premise: the seed paints the glyph on some row"
    assert shown_broken <= _rks(_walk(lib, 50, tab="movies", **params)), params


def test_the_library_response_and_its_statements_carry_no_missing_count(lib, monkeypatch):
    _seed(lib)
    with contextlib.closing(sqlite3.connect(lib.db)) as c, c:
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                  " first_seen_sync_at, youtube_url) VALUES (900, 'movie', 9900, 'Only in TDB', 'imdb', ?, ?, ?)",
                  (_now(), _now(), _url("tdbonly900")))
    from app.web import api
    statements = []
    real = api.get_conn

    @contextlib.contextmanager
    def get_conn(db_path):
        with real(db_path) as conn:
            conn.set_trace_callback(statements.append)
            yield conn

    monkeypatch.setattr(api, "get_conn", get_conn)
    for params in (dict(), dict(dl_pills="broken"), dict(status="not_in_plex")):
        statements.clear()
        body = _get(lib, tab="movies", page=1, per_page=5, **params)
        assert body["items"] and "missing_count" not in body, params
        assert all(it.get("not_in_plex") == 1 for it in body["items"]) == ("status" in params), params
        # the dropped banner count: distinct Plex rating keys with no download in the row's section and edition
        assert statements and not [s for s in statements if "COUNT(DISTINCT pi.rating_key)" in s], params


@pytest.fixture
def action_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.web import api
    monkeypatch.setattr(api, "log_event", lambda *a, **k: None)
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(api.create_app(s)), s.db_path


def _download_jobs(db):
    with contextlib.closing(sqlite3.connect(db)) as c:
        return {(mt, tmdb, sid, ek or "") for mt, tmdb, sid, ek in c.execute(
            "SELECT media_type, tmdb_id, section_id, json_extract(payload, '$.edition_key') FROM jobs"
            " WHERE job_type = 'download'")}


def test_download_missing_skips_plex_orphan_themes_and_keeps_section_and_edition_scope(action_client):
    tc, db = action_client
    now = _now()
    with contextlib.closing(sqlite3.connect(db)) as c, c:
        for sid, fourk in (("1", 0), ("2", 1)):
            c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included,"
                      " discovered_at, last_seen_at) VALUES (?, ?, 'movie', 0, ?, ?, 1, ?, ?)",
                      (sid, f"S{sid}", fourk, f"sub{sid}", now, now))
        for tid, tmdb, src in ((1, 9101, "imdb"), (2, 9102, "imdb"), (3, -9103, "plex_orphan")):
            c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                      " first_seen_sync_at, youtube_url) VALUES (?, 'movie', ?, ?, ?, ?, ?, ?)",
                      (tid, tmdb, f"T{tid}", src, now, now, _url(f"vid{tid:08d}")))
        for rk, sid, tid, guid, ek in (("r1", "1", 1, 9101, ""), ("r1-4k", "2", 1, 9101, ""),
                                       ("r2", "1", 2, 9102, ""), ("r2-ext", "1", 2, 9102, "extended"),
                                       ("r3", "1", 3, None, "")):
            c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title,"
                      " edition_key, folder_path, has_theme, local_theme_file, first_seen_at, last_seen_at)"
                      " VALUES (?, ?, 'movie', ?, ?, ?, ?, ?, 0, 0, ?, ?)",
                      (rk, sid, tid, guid, rk, ek, f"/nonexistent/{rk}", now, now))
        for tmdb, sid, ek in ((9101, "1", ""), (9102, "1", "")):
            c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path, downloaded_at,"
                      " source_video_id, provenance, source_kind) VALUES ('movie', ?, ?, ?, ?, ?, 'vid', 'auto',"
                      " 'themerrdb')", (tmdb, sid, ek, f"m/{tmdb}-{sid}.mp3", now))
    r = tc.post("/api/library/download-missing", json={"tab": "movies", "fourk": False}, headers=AUTH)
    assert r.status_code == 200, r.text
    # standard tab: only the themeless Extended edition; the downloaded titles and the plex_orphan theme stay out
    assert _download_jobs(db) == {("movie", 9102, "1", "extended")}
    assert r.json()["enqueued"] == 1
    r = tc.post("/api/library/download-missing", json={"tab": "movies", "fourk": True}, headers=AUTH)
    assert r.status_code == 200, r.text
    # 4K tab: the title downloaded only in the standard section is missing in the 4K one
    jobs = _download_jobs(db)
    assert ("movie", 9101, "2", "") in jobs
    assert not {j for j in jobs if j[1] == -9103}
