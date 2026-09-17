"""v0.51.345: every /api/library route (pi_only, full, window total, post-stat) serves the same rows two-phase."""
from __future__ import annotations

import contextlib
import inspect
import re
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db

AUTH = {"X-Authentik-Username": "testadmin"}
ALL_SRC = "T,U,AT,A,M,P,-"
# Filters that match every row, each forcing a different row path through _library_main_query.
ROUTES = {
    "default": {},
    "full-slim-count": {"ed_pills": "has,none"},
    "full-window-total": {"src_pills": ALL_SRC},
    "post-stat": {"dl_pills": "on,off,broken"},
}
# (sort, sort_dir) pairs the mode-agreement walk covers.
SORTS = [("title", "asc"), ("year", "desc"), ("src", "asc"), ("pl", "asc"), ("attention", "asc")]

# Unique titles (case-blind) so every sort is a total order; no placement fan-out, so pi_only serves the default view.
MIXED = [
    dict(rk="m-alpha", title="Alpha", year="1999", canon="ok", place=["real"], loud=-30.0),
    dict(rk="m-bravo", title="bravo", year="2004", canon="missing", place=["gone"], present=0),
    dict(rk="m-charlie", title="Charlie", year="1999", canon="ok", norm="normalized", loud=-18.0),
    dict(rk="m-delta", title="Delta", year="2010", has_theme=1),
    dict(rk="m-echo", title="Echo", year="2004", canon="empty", place=["real"], mismatch="pending"),
    dict(rk="m-fox", title="Foxtrot", year="2021", canon="ok", upload=("", "dead-rk-fox", 0)),
    dict(rk="m-golf", title="Golf", year="2010", canon="ok", upload=("gone", "dead-rk-golf", 0)),
    dict(rk="m-hotel", title="Hotel", year=None, canon="ok", upload=("", "m-hotel", 1)),
    dict(rk="m-india", title="India", year="2015", canon="ok", place=["gone"], failure="video_removed"),
    dict(rk="m-juliet", title="Juliet", year="2015", canon="ok", place=["real"], loud=-5.0, pending=True,
         failure="cookies_expired", acked=True),
    dict(rk="m-kilo", title="Kilo", year="2008", canon="ok", place=["real"], edition="extended"),
    dict(rk="m-lima", title="Lima", year="2012", canon="ok", place=["real"], guid=False),
]
STALE_UPLOADS = {"m-fox", "m-golf"}

# Bravo: an edition with two placement folders (2 join rows); Delta: a standard title with two (2 x 2 = 4 join rows).
FANOUT = [
    dict(rk="f-alpha", title="Alpha", canon="ok", place=["real"]),
    dict(rk="f-bravo", title="Bravo", canon="ok", place=["real", "gone"], edition="extended"),
    dict(rk="f-charlie", title="Charlie", canon="missing", place=["gone"]),
    dict(rk="f-delta", title="Delta", canon="ok", place=["real", "gone"]),
    dict(rk="f-echo", title="Echo", canon="ok", place=["real"]),
    dict(rk="f-fox", title="Foxtrot"),
]
FANOUT_ROWS = ["f-alpha", "f-bravo", "f-bravo", "f-charlie", "f-delta", "f-delta", "f-delta", "f-delta",
               "f-echo", "f-fox"]
# per_page=2 by hand: Bravo straddles pages 1-2, Delta fills pages 3-4.
FANOUT_PAGES = [["f-alpha", "f-bravo"], ["f-bravo", "f-charlie"], ["f-delta", "f-delta"], ["f-delta", "f-delta"],
                ["f-echo", "f-fox"]]


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
    return SimpleNamespace(tc=TestClient(api.create_app(s)), db=s.db_path, api=api,
                           themes=tmp_path / "themes", media=tmp_path / "media")


def _folder(lib, title, mode, i):
    if mode == "":
        return ""
    if mode == "gone":
        return str(lib.media / "gone" / f"{title} {i}")
    path = lib.media / "real" / f"{title} {i}"
    path.mkdir(parents=True)
    (path / "theme.mp3").write_bytes(b"placed-theme")
    return str(path)


def _seed(lib, rows):
    now = _now()
    with contextlib.closing(sqlite3.connect(lib.db)) as c, c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included,"
                  " discovered_at, last_seen_at) VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)", (now, now))
        for n, r in enumerate(rows, start=1):
            tmdb, ek, title = 7000 + n, r.get("edition", ""), r["title"]
            new_url = f"https://www.youtube.com/watch?v=new{n:08d}"
            c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                      " first_seen_sync_at, youtube_url, failure_kind, failure_acked_at)"
                      " VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, ?, ?, ?)",
                      (n, tmdb, title, now, now, new_url, r.get("failure"), now if r.get("acked") else None))
            c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, year,"
                      " edition_key, folder_path, has_theme, local_theme_file, plex_independent_theme,"
                      " plex_theme_verified_ok, first_seen_at, last_seen_at)"
                      " VALUES (?, '1', 'movie', ?, ?, ?, ?, ?, ?, ?, 0, 0, 1, ?, ?)",
                      (r["rk"], n, None if r.get("guid") is False else tmdb, title, r.get("year"), ek,
                       f"/nonexistent/{title}" + (" {edition-Extended}" if ek else ""), r.get("has_theme", 0),
                       now, now))
            if r.get("canon"):
                rel = f"movies/{r['rk']}.mp3"
                c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path,"
                          " downloaded_at, source_video_id, provenance, source_kind, loudness_i, norm_state,"
                          " mismatch_state) VALUES ('movie', ?, '1', ?, ?, ?, ?, 'auto', 'themerrdb', ?, ?, ?)",
                          (tmdb, ek, rel, now, f"vid{n:08d}", r.get("loud"), r.get("norm"), r.get("mismatch")))
                if r["canon"] != "missing":
                    (lib.themes / "movies").mkdir(parents=True, exist_ok=True)
                    (lib.themes / rel).write_bytes(b"canonical" if r["canon"] == "ok" else b"")
            for i, mode in enumerate(r.get("place", [])):
                c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder,"
                          " placed_at, placement_kind, plex_refreshed, theme_present)"
                          " VALUES ('movie', ?, '1', ?, ?, ?, 'hardlink', 1, ?)",
                          (tmdb, ek, _folder(lib, title, mode, i), now, r.get("present", 1)))
            if r.get("upload"):
                mode, plex_rk, present = r["upload"]
                c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder,"
                          " placed_at, placement_kind, plex_rating_key, plex_refreshed, theme_present)"
                          " VALUES ('movie', ?, '1', ?, ?, ?, 'plex_upload', ?, 1, ?)",
                          (tmdb, ek, _folder(lib, title, mode, 0), now, plex_rk, present))
            if r.get("pending"):
                c.execute("INSERT INTO pending_updates (media_type, tmdb_id, section_id, edition_key, decision,"
                          " detected_at, old_youtube_url, new_youtube_url, kind)"
                          " VALUES ('movie', ?, '', '', 'pending', ?, ?, ?, 'upstream_changed')",
                          (tmdb, now, f"https://www.youtube.com/watch?v=old{n:08d}", new_url))


def _get(lib, **params):
    r = lib.tc.get("/api/library", params={"tab": "movies", **params}, headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()


def _walk(lib, per_page, **params):
    first = _get(lib, page=1, per_page=per_page, **params)
    pages = [first["items"]]
    for page in range(2, 200):
        body = _get(lib, page=page, per_page=per_page, **params)
        assert body["total"] == first["total"], (params, page, body["total"], first["total"])
        if not body["items"]:
            break
        pages.append(body["items"])
    else:
        raise AssertionError(f"pagination never ran out of rows: {params}")
    assert all(len(items) == per_page for items in pages[:-1]), (params, [len(items) for items in pages])
    assert len(pages[-1]) <= per_page
    return first["total"], pages


def _flat(pages):
    return [list(it.items()) for items in pages for it in items]


def _keys(pages):
    return [it["rating_key"] for items in pages for it in items]


def test_every_route_serves_identical_rows_and_totals_at_every_page_size(lib):
    _seed(lib, MIXED)
    for sort, sort_dir in SORTS:
        total, pages = _walk(lib, 50, sort=sort, sort_dir=sort_dir)
        baseline = _flat(pages)
        assert total == len(MIXED) and sorted(_keys(pages)) == sorted(r["rk"] for r in MIXED)
        flags = {(it["canonical_missing"], it["placement_missing"]) for items in pages for it in items}
        assert {(True, True), (True, False), (False, False)} <= flags
        for route, extra in ROUTES.items():
            for per_page in (1, 2, 7, 50):
                got_total, got_pages = _walk(lib, per_page, sort=sort, sort_dir=sort_dir, **extra)
                assert got_total == total, (sort, sort_dir, route, per_page)
                assert _flat(got_pages) == baseline, (sort, sort_dir, route, per_page)


def _folders_by_key(pages):
    out = {}
    for items in pages:
        for it in items:
            out.setdefault(it["rating_key"], []).append(it["media_folder"])
    return {rk: sorted(folders) for rk, folders in out.items()}


def test_multi_folder_rows_fan_out_identically_on_every_route(lib):
    _seed(lib, FANOUT)
    gone = str(lib.media / "gone")
    assert [rk for page in FANOUT_PAGES for rk in page] == FANOUT_ROWS
    walks = {}
    for route, extra in ROUTES.items():
        _total, pages = _walk(lib, 2, sort="title", **extra)
        assert [[it["rating_key"] for it in items] for items in pages] == FANOUT_PAGES, route
        folders = _folders_by_key(pages)
        # N folders fan an edition out N times and a standard title N x N times (p_e x p_g): each folder shows 1 or 2 times.
        assert sorted(Counter(folders["f-bravo"]).values()) == [1, 1], (route, folders["f-bravo"])
        assert sorted(Counter(folders["f-delta"]).values()) == [2, 2], (route, folders["f-delta"])
        for items in pages:
            for it in items:
                if it["media_folder"]:
                    assert it["placement_missing"] == it["media_folder"].startswith(gone), (route, it)
        walks[route] = pages
    for route, pages in walks.items():
        assert sorted(map(str, _flat(pages))) == sorted(map(str, _flat(walks["default"]))), route


class _Conn:
    def __init__(self, conn, before_execute):
        self._conn, self._before = conn, before_execute

    def execute(self, sql, *args):
        self._before(sql)
        return self._conn.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _hook_get_conn(monkeypatch, api, *, on_call=None, before_execute=None):
    real = api.get_conn
    calls = []

    def get_conn(db_path):
        if inspect.currentframe().f_back.f_code.co_name == "_library_main_query":
            calls.append(db_path)
            if on_call:
                on_call(len(calls))

        @contextlib.contextmanager
        def wrapped():
            with real(db_path) as conn:
                yield _Conn(conn, before_execute or (lambda sql: None))
        return wrapped()

    monkeypatch.setattr(api, "get_conn", get_conn)
    return calls


def _race_write(db):
    # Deletes f-charlie's item and deletes+re-inserts f-echo's placement unchanged, from another connection.
    with contextlib.closing(sqlite3.connect(db)) as c, c:
        c.row_factory = sqlite3.Row
        c.execute("DELETE FROM plex_items WHERE rating_key = 'f-charlie'")
        echo = [dict(r) for r in c.execute("SELECT p.* FROM placements p JOIN themes t ON t.tmdb_id = p.tmdb_id"
                                           " AND t.media_type = p.media_type WHERE t.title = 'Echo'")]
        assert len(echo) == 1
        c.execute("DELETE FROM placements WHERE media_type = ? AND tmdb_id = ?", (echo[0]["media_type"], echo[0]["tmdb_id"]))
        cols = ", ".join(echo[0])
        c.execute(f"INSERT INTO placements ({cols}) VALUES ({', '.join('?' for _ in echo[0])})", list(echo[0].values()))


def test_post_stat_hydration_after_a_concurrent_write_drops_only_the_deleted_row(lib, monkeypatch):
    _seed(lib, FANOUT)
    params = dict(page=1, per_page=len(FANOUT_ROWS), sort="title", **ROUTES["post-stat"])
    unraced = _get(lib, **params)["items"]
    assert [it["rating_key"] for it in unraced] == FANOUT_ROWS
    fired = []

    def on_call(n):
        if n == 2:
            fired.append(n)
            _race_write(lib.db)

    _hook_get_conn(monkeypatch, lib.api, on_call=on_call)
    raced = _get(lib, **params)["items"]
    assert fired == [2]
    assert len(raced) <= params["per_page"]
    assert [list(it.items()) for it in raced] == [list(it.items()) for it in unraced if it["rating_key"] != "f-charlie"]


def test_paged_rows_are_read_in_one_snapshot_across_a_concurrent_write(lib, monkeypatch):
    _seed(lib, FANOUT)
    params = dict(page=1, per_page=len(FANOUT_ROWS), sort="title")
    unraced = _get(lib, **params)["items"]
    assert [it["rating_key"] for it in unraced] == FANOUT_ROWS
    fired = []

    def before_execute(sql):
        if "json_each(" in sql and not fired:
            fired.append(sql)
            _race_write(lib.db)

    _hook_get_conn(monkeypatch, lib.api, before_execute=before_execute)
    raced = _get(lib, **params)["items"]
    assert len(fired) == 1
    assert [list(it.items()) for it in raced] == [list(it.items()) for it in unraced]
    assert "f-charlie" not in [it["rating_key"] for it in _get(lib, **params)["items"]]


def _attn_match(row, pill):
    upload = row["placement_kind"] == "plex_upload"
    lps = bool(row["file_path"]) and not row["media_folder"] and row["plex_independent_theme"] == 1 and not upload
    return {
        "broken": bool(row["canonical_missing"]),
        "fail": bool(row["failure_kind"]) and not row["failure_acked_at"],
        "update": row["actionable_update"] == 1,
        "mismatch": row["mismatch_state"] == "pending",
        "await": (bool(row["file_path"]) and not row["media_folder"] and not lps and not upload
                  and row["last_place_attempt_reason"] not in ("backup_only", "plex_rejected:over_ceiling")),
    }[pill]


@pytest.mark.parametrize("pill", ["fail", "update", "mismatch", "await"])
def test_post_stat_attention_filters_read_only_projected_columns(lib, pill):
    _seed(lib, MIXED)
    _total, pages = _walk(lib, 50)
    rows = [it for items in pages for it in items]
    expected = [it for it in rows if _attn_match(it, pill) or _attn_match(it, "broken")]
    assert any(_attn_match(it, pill) for it in rows) and any(_attn_match(it, "broken") for it in rows)
    total, got = _walk(lib, 3, attn_pills=f"{pill},broken")
    assert total == len(expected)
    assert _flat(got) == [list(it.items()) for it in expected]


def test_a_post_stat_filter_reading_an_unprojected_column_fails_loudly(lib, monkeypatch):
    _seed(lib, MIXED)
    trimmed = tuple(c for c in lib.api._LIB_POST_STAT_COLUMNS if c != "mismatch_state")
    assert len(trimmed) == len(lib.api._LIB_POST_STAT_COLUMNS) - 1
    monkeypatch.setattr(lib.api, "_LIB_POST_STAT_COLUMNS", trimmed)
    with pytest.raises(KeyError, match="mismatch_state"):
        lib.tc.get("/api/library", params={"tab": "movies", "attn_pills": "mismatch,broken"}, headers=AUTH)


def test_stale_uploads_and_the_upload_sentinel_render_the_same_on_every_path(lib):
    _seed(lib, MIXED)
    for route, extra in [("pi_only", {}), ("full", {"sort": "pl"}), ("post-stat", {"pl_pills": "on,off,await,broken"}),
                         ("post-stat-dl", ROUTES["post-stat"])]:
        _total, pages = _walk(lib, 4, **extra)
        rows = {it["rating_key"]: it for items in pages for it in items}
        assert STALE_UPLOADS | {"m-hotel"} <= set(rows), route
        hotel = rows["m-hotel"]
        assert (hotel["media_folder"], hotel["placement_kind"], hotel["needs_repush"],
                hotel["placement_missing"]) == ("", "plex_upload", 0, False), route
        for rk in STALE_UPLOADS:
            it = rows[rk]
            assert (it["media_folder"], it["placement_kind"], it["needs_repush"], it["placement_missing"]) == (
                None, None, 1, False), (route, rk)
    default = {it["rating_key"]: it for items in _walk(lib, 50)[1] for it in items}
    broken = set(_keys(_walk(lib, 2, pl_pills="broken")[1]))
    assert broken == {rk for rk, it in default.items() if it["placement_missing"]}
    assert not broken & (STALE_UPLOADS | {"m-hotel"})
    placed = set(_keys(_walk(lib, 2, pl_pills="on")[1]))
    assert "m-hotel" in placed and not placed & STALE_UPLOADS
    assert set(_keys(_walk(lib, 2, link_pills="rp")[1])) == STALE_UPLOADS
    assert set(_keys(_walk(lib, 2, link_pills="pu")[1])) == {"m-hotel"}


def test_window_total_matches_the_count_on_every_page_and_zero_when_nothing_matches(lib):
    _seed(lib, MIXED)
    total, pages = _walk(lib, 3, **ROUTES["full-window-total"])
    assert total == len(MIXED) == sum(len(items) for items in pages)
    past_end = _get(lib, page=len(pages) + 3, per_page=3, **ROUTES["full-window-total"])
    assert (past_end["total"], past_end["items"]) == (total, [])
    empty = _get(lib, page=1, per_page=3, q="no title matches this", **ROUTES["full-window-total"])
    assert (empty["total"], empty["items"]) == (0, [])
    for extra in ({"status": "placed"}, {"link_pills": "hl"}, {"tdb_pills": "tdb"}, {"status": "has_theme"}):
        _walk(lib, 1, **extra)


def test_every_sort_key_serves_the_same_rows_whichever_route_answers(lib):
    _seed(lib, MIXED)
    sort_keys = sorted(lib.api._LIBRARY_SORTS_MAIN)
    assert {"title", "year", "src", "pl", "attention"} <= set(sort_keys)
    for sort in sort_keys:
        for sort_dir in ("asc", "desc"):
            total, pages = _walk(lib, 5, sort=sort, sort_dir=sort_dir)
            for route in ("full-window-total", "full-slim-count"):
                got_total, got = _walk(lib, 5, sort=sort, sort_dir=sort_dir, **ROUTES[route])
                assert (got_total, _flat(got)) == (total, _flat(pages)), (sort, sort_dir, route)


def _plan(db, sql, params):
    with contextlib.closing(sqlite3.connect(db)) as c:
        return [row[3] for row in c.execute("EXPLAIN QUERY PLAN " + sql, params)]


def _capture(lib, monkeypatch, **params):
    statements = []
    real = lib.api.get_conn

    def get_conn(db_path):
        @contextlib.contextmanager
        def wrapped():
            with real(db_path) as conn:
                yield _Recorder(conn, statements)
        return wrapped()

    monkeypatch.setattr(lib.api, "get_conn", get_conn)
    _get(lib, page=1, per_page=5, **params)
    monkeypatch.setattr(lib.api, "get_conn", real)
    phase2 = [(s, p) for s, p in statements if "json_each(" in s]
    phase1 = [(s, p) for s, p in statements if re.search(r"\b_rk\b", s) and "json_each(" not in s]
    assert len(phase1) == 1 and len(phase2) == 1, [s[:60] for s, _p in statements]
    return phase1[0], phase2[0]


class _Recorder:
    def __init__(self, conn, statements):
        self._conn, self._statements = conn, statements

    def execute(self, sql, *args):
        self._statements.append((sql, list(args[0]) if args else []))
        return self._conn.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


_HEAVY_ALIAS = re.compile(r"^(SCAN|SEARCH) (t|p_e|p_g|lf_e|lf_g|pv_sec|pv_global|pu_sec|pu_global|pu_dsec|pu_dglobal|sfa)\b")


@pytest.mark.skipif(sqlite3.sqlite_version_info < (3, 38, 0), reason="plan shapes measured on SQLite >= 3.38")
def test_phase_plans_keep_their_shape(lib, monkeypatch):
    _seed(lib, MIXED)
    (p1_sql, p1_params), (p2_sql, p2_params) = _capture(lib, monkeypatch, sort="title")
    p1 = _plan(lib.db, p1_sql, p1_params)
    assert not [line for line in p1 if _HEAVY_ALIAS.match(line) or "CORRELATED" in line], p1
    p2 = _plan(lib.db, p2_sql, p2_params)
    first_table = next(line for line in p2 if line.startswith(("SCAN", "SEARCH")))
    assert first_table.startswith("SCAN json_each"), p2
    assert not [line for line in p2 if re.match(r"SCAN (pi|plex_items)\b", line)], p2

    (ps_sql, ps_params), _ = _capture(lib, monkeypatch, sort="src", **ROUTES["post-stat"])
    ps = _plan(lib.db, ps_sql, ps_params)
    assert not [line for line in ps if "CO-ROUTINE" in line or "MATERIALIZE" in line], ps
    assert any("TEMP B-TREE FOR ORDER BY" in line for line in ps), ps

    (tdb_sql, tdb_params), _ = _capture(lib, monkeypatch, tdb_pills="update")
    tdb = _plan(lib.db, tdb_sql, tdb_params)
    assert not [line for line in tdb if re.match(r"(SCAN|SEARCH) (pu_sec|pu_dsec)\b", line)], tdb
