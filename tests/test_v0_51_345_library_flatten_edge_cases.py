"""v0.51.345: /api/library edge cases the two-phase rows and the predicate grafts must not move."""
from __future__ import annotations

import contextlib
import sqlite3
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db

AUTH = {"X-Authentik-Username": "testadmin"}


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def url(tag):
    return f"https://www.youtube.com/watch?v={tag}"


@pytest.fixture
def client(tmp_path, monkeypatch):
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


def _section(c, sid, typ, anime, fourk, now):
    c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included,"
              " discovered_at, last_seen_at) VALUES (?,?,?,?,?,?,1,?,?)",
              (sid, f"S{sid}", typ, anime, fourk, f"sub{sid}", now, now))


def _theme(c, tid, mt, tmdb, now, *, src="imdb", yt=None, dropped=None):
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
              " first_seen_sync_at, youtube_url, tdb_dropped_at) VALUES (?,?,?,?,?,?,?,?,?)",
              (tid, mt, tmdb, f"T{tid}", src, now, now, yt, dropped))


def _item(c, rk, sid, mt, tid, guid, title, now, *, ek="", folder=None, has_theme=0, ltf=0):
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, edition_key,"
              " folder_path, has_theme, local_theme_file, plex_independent_theme, plex_theme_verified_ok,"
              " first_seen_at, last_seen_at) VALUES (?,?,?,?,?,?,?,?,?,?,0,1,?,?)",
              (rk, sid, mt, tid, guid, title, ek, folder if folder is not None else f"/nonexistent/{rk}",
               has_theme, ltf, now, now))


def _lf(c, mt, tmdb, sid, now, *, ek="", kind="themerrdb", svid="vidvidvid01", prov="auto"):
    c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path, downloaded_at,"
              " source_video_id, provenance, source_kind) VALUES (?,?,?,?,?,?,?,?,?)",
              (mt, tmdb, sid, ek, f"x/{mt}-{tmdb}-{sid}-{ek or 'std'}.mp3", now, svid, prov, kind))


def _pl(c, mt, tmdb, sid, now, *, ek="", kind="hardlink", folder=None, rk=None, present=1, prov="auto"):
    c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder, placed_at,"
              " placement_kind, plex_rating_key, plex_refreshed, provenance, theme_present)"
              " VALUES (?,?,?,?,?,?,?,?,1,?,?)",
              (mt, tmdb, sid, ek, folder if folder is not None else ("" if kind == "plex_upload" else f"/nonexistent/{mt}{tmdb}{ek}"),
               now, kind, rk, prov, present))


def _pu(c, mt, tmdb, sid, now, *, ek="", decision="pending", kind="upstream_changed", old=None, new=None):
    c.execute("INSERT INTO pending_updates (media_type, tmdb_id, section_id, edition_key, decision, detected_at,"
              " old_youtube_url, new_youtube_url, kind) VALUES (?,?,?,?,?,?,?,?,?)",
              (mt, tmdb, sid, ek, decision, now, old, new, kind))


def _uo(c, mt, tmdb, sid, ek, u, now):
    c.execute("INSERT INTO user_overrides (media_type, tmdb_id, section_id, edition_key, youtube_url, intent, set_at,"
              " set_by) VALUES (?,?,?,?,?,'replace',?,'admin')", (mt, tmdb, sid, ek, u, now))


def seed(db):
    now = _now()
    with contextlib.closing(sqlite3.connect(db)) as c, c:
        _section(c, "1", "movie", 0, 0, now)
        _section(c, "2", "movie", 0, 1, now)
        _section(c, "4", "show", 1, 0, now)
        # m101: detection ONLY at the title-global '' tier, real URL diff (T row)
        _theme(c, 1, "movie", 101, now, yt=url("new101"))
        _item(c, "m101", "1", "movie", 1, 101, "Alpha", now)
        _lf(c, "movie", 101, "1", now); _pl(c, "movie", 101, "1", now)
        _pu(c, "movie", 101, "", now, old=url("old101"), new=url("new101"))
        # m101-4k: same title in the 4K section, no motif presence -> no pill there
        _item(c, "m101-4k", "2", "movie", 1, 101, "Alpha", now, has_theme=1)
        # m102: section-tier detection; U row whose override is title-global
        _theme(c, 2, "movie", 102, now, yt=url("new102"))
        _item(c, "m102", "1", "movie", 2, 102, "Bravo", now)
        _lf(c, "movie", 102, "1", now, kind="url", svid="abcdefghijk", prov="manual")
        _pl(c, "movie", 102, "1", now, prov="manual")
        _uo(c, "movie", 102, "", "", url("user102"), now)
        _pu(c, "movie", 102, "1", now, old=url("old102"), new=url("new102"))
        # m103 / m103-ext: edition siblings; declined on Extended only; per-edition + per-section overrides;
        # a download job in flight for Extended only
        _theme(c, 3, "movie", 103, now, yt=url("new103"))
        _item(c, "m103", "1", "movie", 3, 103, "Charlie", now, folder="/nonexistent/Charlie")
        _item(c, "m103-ext", "1", "movie", 3, 103, "Charlie Extended", now, ek="extended",
              folder="/nonexistent/Charlie {edition-Extended}")
        _lf(c, "movie", 103, "1", now); _pl(c, "movie", 103, "1", now)
        _pu(c, "movie", 103, "1", now, old=url("old103"), new=url("new103"))
        _pu(c, "movie", 103, "1", now, ek="extended", decision="declined", old=url("old103"), new=url("new103"))
        _uo(c, "movie", 103, "1", "", url("sec103"), now)
        _uo(c, "movie", 103, "1", "extended", url("ext103"), now)
        c.execute("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, payload, status, created_at)"
                  " VALUES ('download','movie',103,'1','{\"edition_key\": \"extended\"}','pending',?)", (now,))
        # m107: dropped by TDB
        _theme(c, 7, "movie", 107, now, yt=url("new107"), dropped=now)
        _item(c, "m107", "1", "movie", 7, 107, "Delta", now)
        # m110: stale plex_upload (theme_present=0, rk dead) -> RP
        _theme(c, 10, "movie", 110, now, yt=url("new110"))
        _item(c, "m110", "1", "movie", 10, 110, "Echo", now)
        _lf(c, "movie", 110, "1", now, kind="url", svid="abcdefghijz", prov="manual")
        _pl(c, "movie", 110, "1", now, kind="plex_upload", rk="dead-rk", present=0, prov="manual")
        # m111: new_theme_available at '' on an unthemed (SRC '-') row
        _theme(c, 11, "movie", 111, now, yt=url("new111"))
        _item(c, "m111", "1", "movie", 11, 111, "Foxtrot", now)
        _pu(c, "movie", 111, "", now, kind="new_theme_available", new=url("new111"))
        # m112: title-global detection, DECLINED at the '' tier
        _theme(c, 12, "movie", 112, now, yt=url("new112"))
        _item(c, "m112", "1", "movie", 12, 112, "Golf", now)
        _lf(c, "movie", 112, "1", now); _pl(c, "movie", 112, "1", now)
        _pu(c, "movie", 112, "", now, decision="declined", old=url("old112"), new=url("new112"))
        # m120 untracked, m121 healthy T with no pending
        _item(c, "m120", "1", "movie", None, 120, "Hotel", now)
        _theme(c, 21, "movie", 121, now, yt=url("new121"))
        _item(c, "m121", "1", "movie", 21, 121, "India", now)
        _lf(c, "movie", 121, "1", now); _pl(c, "movie", 121, "1", now)
        # c105: collection, plex_upload (media_folder ''), urls_match + override both title-global
        _theme(c, 5, "collection", 105, now, yt=url("new105"))
        _item(c, "c105", "1", "collection", 5, None, "Coll", now, folder="")
        _lf(c, "collection", 105, "1", now, kind="url", svid="new105vid00", prov="manual")
        _pl(c, "collection", 105, "1", now, kind="plex_upload", rk="c105", prov="manual")
        _uo(c, "collection", 105, "", "", url("new105"), now)
        _pu(c, "collection", 105, "", now, kind="urls_match", old=url("new105"), new=url("new105"))
        # a6: anime plex_orphan with a NEGATIVE tmdb_id, AT pick via a title-global override
        _theme(c, 6, "tv", -5001, now, src="plex_orphan")
        _item(c, "a6", "4", "show", 6, None, "Kilo", now)
        _lf(c, "tv", -5001, "4", now, kind="url", svid="at-kilo-op1", prov="manual")
        _pl(c, "tv", -5001, "4", now, prov="manual")
        _uo(c, "tv", -5001, "", "", "https://animethemes.moe/anime/kilo/OP1", now)


def get(cl, **params):
    r = cl.get("/api/library", params=params, headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()


def all_rows(cl, per_page=2, **params):
    # Pages through with a tiny per_page; the header total must equal what the pages deliver.
    first = get(cl, page=1, per_page=per_page, **params)
    total, out, page = first["total"], list(first["items"]), 1
    while page * per_page < total:
        page += 1
        out += get(cl, page=page, per_page=per_page, **params)["items"]
    assert total == len(out), (params, total, len(out))
    return out


def keys(rows):
    return [r["rating_key"] for r in rows]


def by_rk(rows):
    return {r["rating_key"]: r for r in rows}


def test_title_global_detection_lights_every_pending_surface(client):
    cl, db = client
    seed(db)
    rows = by_rk(all_rows(cl, tab="movies"))
    assert (rows["m101"]["pending_update"], rows["m101"]["actionable_update"]) == (1, 1)
    assert rows["m101"]["pending_update_kind"] == "upstream_changed"
    assert (rows["m111"]["pending_update"], rows["m111"]["actionable_update"]) == (1, 1)
    assert (rows["m112"]["pending_update"], rows["m112"]["actionable_update"]) == (1, 0)
    assert (rows["m121"]["pending_update"], rows["m120"]["pending_update"]) == (0, 0)
    # the filters mirror the columns
    upd = set(keys(all_rows(cl, tab="movies", tdb_pills="update")))
    attn = set(keys(all_rows(cl, tab="movies", attn_pills="update")))
    green = set(keys(all_rows(cl, tab="movies", tdb_pills="tdb")))
    assert {"m101", "m102", "m103", "m111", "m112"} <= upd and not upd & {"m121", "m120", "m107"}
    assert attn == {r for r, it in rows.items() if it["actionable_update"] == 1}
    assert not green & {r for r, it in rows.items() if it["pending_update"] == 1}
    assert "m121" in green
    # collections: plex_upload row, urls_match + override both at the title-global tier
    coll = by_rk(all_rows(cl, tab="collections"))
    assert (coll["c105"]["pending_update"], coll["c105"]["actionable_update"]) == (1, 1)
    assert keys(all_rows(cl, tab="collections", attn_pills="update")) == ["c105"]
    assert keys(all_rows(cl, tab="collections", tdb_pills="update")) == ["c105"]


def test_needs_work_ranks_title_global_pending_rows(client):
    cl, db = client
    seed(db)
    rows = all_rows(cl, per_page=200, tab="movies", sort="attention")
    order = keys(rows)
    assert order[0] == "m110", order                       # broken/stale plex_upload bucket first
    actionable = {it["rating_key"] for it in rows if it["actionable_update"] == 1}
    assert set(order[1:1 + len(actionable)]) == actionable, (order, actionable)
    assert keys(all_rows(cl, per_page=3, tab="movies", sort="attention")) == order   # LIMIT/OFFSET agree


def test_per_edition_decision_and_job_scoping(client):
    cl, db = client
    seed(db)
    rows = by_rk(all_rows(cl, tab="movies"))
    std, ext = rows["m103"], rows["m103-ext"]
    assert (std["pending_update"], std["actionable_update"]) == (1, 1)
    assert (ext["pending_update"], ext["actionable_update"]) == (1, 0)
    assert (std["job_in_flight"], ext["job_in_flight"]) == (None, "download")
    assert ext["file_path"] is None and ext["media_folder"] is None   # no '' fallback on a multi-edition title


def test_applied_url_tiers_match_an_independent_resolution(client):
    cl, db = client
    seed(db)
    with contextlib.closing(sqlite3.connect(db)) as c:
        uo = {(mt, tmdb, sid, ek): u for mt, tmdb, sid, ek, u in
              c.execute("SELECT media_type, tmdb_id, section_id, edition_key, youtube_url FROM user_overrides")}
        themes = {tid: (mt, tmdb, yt) for tid, mt, tmdb, yt in c.execute("SELECT id, media_type, tmdb_id, youtube_url FROM themes")}
        items = {rk: (sid, tid, ek) for rk, sid, tid, ek in c.execute("SELECT rating_key, section_id, theme_id, edition_key FROM plex_items")}

    def oracle(rk):
        sid, tid, ek = items[rk]
        if tid is None:
            return None
        mt, tmdb, yt = themes[tid]
        glob = [u for (m, t, s, _e), u in sorted(uo.items()) if (m, t, s) == (mt, tmdb, "")]
        for cand in (uo.get((mt, tmdb, sid, ek)), uo.get((mt, tmdb, sid, "")), glob[0] if glob else None, yt):
            if cand is not None:
                return cand
        return None

    seen = 0
    for tab, extra in (("movies", {}), ("movies", {"fourk": "true"}), ("collections", {}), ("anime", {})):
        for it in all_rows(cl, tab=tab, **extra):
            assert it["applied_youtube_url"] == oracle(it["rating_key"]), it["rating_key"]
            seen += 1
    assert seen == len(items)
    rows = by_rk(all_rows(cl, tab="movies"))
    assert rows["m102"]["applied_youtube_url"] == url("user102")          # title-global override wins over TDB
    assert (rows["m103"]["applied_youtube_url"], rows["m103-ext"]["applied_youtube_url"]) == (url("sec103"), url("ext103"))
    assert by_rk(all_rows(cl, tab="anime"))["a6"]["applied_youtube_url"].startswith("https://animethemes.moe/")


def test_filtered_totals_equal_paged_rows_for_every_count_shape(client):
    cl, db = client
    seed(db)
    shapes = [dict(tab="movies", status="has_theme"), dict(tab="movies", status="untracked"),
              dict(tab="movies", status="placed"), dict(tab="movies", tdb="tracked"),
              dict(tab="movies", src_pills="T"), dict(tab="movies", src_pills="U,-"),
              dict(tab="movies", tdb_pills="dropped"), dict(tab="movies", tdb_pills="tdb,update"),
              dict(tab="movies", link_pills="rp"), dict(tab="movies", link_pills="pu,hl"),
              dict(tab="movies", pl_pills="await"), dict(tab="movies", dl_pills="off"),
              dict(tab="movies", loudness_pills="raw"), dict(tab="movies", attn_pills="update,repush"),
              dict(tab="movies", fourk="true", status="has_theme"),
              dict(tab="anime", src_pills="AT"), dict(tab="anime", tdb_pills="none")]
    for shp in shapes:
        all_rows(cl, per_page=1, **shp)        # asserts total == rows paged
    assert keys(all_rows(cl, tab="movies", link_pills="rp")) == ["m110"]
    rp = by_rk(all_rows(cl, tab="movies"))["m110"]
    assert (rp["needs_repush"], rp["media_folder"], rp["placement_kind"]) == (1, None, None)
    assert keys(all_rows(cl, tab="anime", src_pills="AT")) == ["a6"]
    assert keys(all_rows(cl, tab="movies", fourk="true")) == ["m101-4k"]
    assert by_rk(all_rows(cl, tab="movies", fourk="true"))["m101-4k"]["pending_update"] == 0


@pytest.mark.xfail(strict=True, reason="pre-existing: the slim COUNT joins placements via pi.guid_tmdb; a "
                   "collection links via theme_id with a NULL guid_tmdb, so LINK=PU reads total 0 while its "
                   "row renders. Kept on purpose in v0.51.345; fixing it flips this to XPASS.")
def test_known_slim_count_drift_on_collections(client):
    cl, db = client
    seed(db)
    all_rows(cl, per_page=1, tab="collections", link_pills="pu")


def test_count_wrapper_counts_joined_tuples_not_items(client):
    # Two '' -edition placements in different folders fan a standard row out N*N (p_e x p_g); the header counts those tuples.
    cl, db = client
    seed(db)
    with contextlib.closing(sqlite3.connect(db)) as c, c:
        _pl(c, "movie", 121, "1", _now(), folder="/nonexistent/India-second-folder")
        n = c.execute("SELECT COUNT(*) FROM placements WHERE tmdb_id = 121 AND edition_key = ''").fetchone()[0]
    assert n == 2
    for shp in (dict(tab="movies", status="placed"), dict(tab="movies", tdb_pills="tdb"),
                dict(tab="movies", src_pills="T")):
        got = keys(all_rows(cl, per_page=1, **shp))      # all_rows asserts header total == tuples paged
        assert got.count("m121") == n * n, (shp, got)
