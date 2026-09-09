"""v0.51.314 — AnimeThemes source, tag 1: bridge + client + resolver + harness.

Offline pins for docs/specs/ANIMETHEMES_SPEC.md §3.2/§3.3/§4. The live
numbers live in docs/specs/animethemes_eval/; these tests pin the SHAPES
the live probe proved necessary (two-step lookups, scoped filters, batch
size, pacing, 429 handling, never-cache-errors) and the confidence rules.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import httpx
import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core import animethemes as at  # noqa: E402

# ── fixtures ─────────────────────────────────────────────────

BRIDGE_JSON = [
    {"anidb_id": 23, "tvdb_id": 76885, "themoviedb_id": {"tv": 30991}, "season": {"tvdb": 1}, "type": "TV", "mal_id": 1},
    {"anidb_id": 219, "tvdb_id": 76885, "season": {"tvdb": 0}, "type": "MOVIE"},          # special under the same show
    {"anidb_id": 1, "tvdb_id": 72025, "themoviedb_id": {"tv": 26209}, "season": {"tvdb": 1}, "type": "TV"},
    {"anidb_id": 4, "tvdb_id": 72025, "season": {"tvdb": 2}, "type": "TV"},
    {"anidb_id": 5, "tvdb_id": 72025, "season": {"tvdb": 3}, "type": "TV"},
    {"anidb_id": 9000, "tvdb_id": 99999, "season": {"tvdb": 2}, "type": "TV"},           # only a season-2 entry
    {"mal_id": 42, "tvdb_id": 55555},                                                     # no anidb → skipped
]


def _anime(id_, name, year, themes):
    # real shape: ONE animetheme per slug, several animethemeentries (versions) under it
    by_slug: dict = {}
    for slug, seq, ver, src, size in themes:
        by_slug.setdefault(slug, (seq, []))[1].append(
            {"version": ver, "nsfw": False, "spoiler": False,
             "videos": [{"basename": f"{slug}-v{ver}.webm", "nc": True, "source": src, "resolution": 1080,
                         "audio": {"link": f"https://a.animethemes.moe/{name.replace(' ', '')}-{slug}{'' if ver == 1 else 'v' + str(ver)}.ogg",
                                   "size": size}}]})
    return {"id": id_, "name": name, "year": year, "season": "Spring", "slug": name.lower().replace(" ", "_"),
            "animethemes": [{"slug": slug, "type": slug[:2], "sequence": seq, "animethemeentries": entries}
                            for slug, (seq, entries) in by_slug.items()]}


class FakeAPI:
    """Serves the two-step shapes; records every call; can inject statuses."""

    def __init__(self):
        self.calls: list[httpx.Request] = []
        self.statuses: list[int] = []           # pop-front overrides
        self.retry_after: str | None = None
        self.by_anidb = {23: [(521, "Cowboy Bebop", 1998)], 219: [(522, "Cowboy Bebop Movie", 2001)],
                         1: [(601, "Crest of the Stars", 1999)], 4: [(602, "Banner of the Stars", 2000)],
                         5: [(603, "Banner of the Stars II", 2001)], 9000: [(700, "Bleach TYBW", 2022)]}
        self.themes = {521: [("OP1", None, 1, "BD", 3725835), ("ED1", 1, 1, "BD", 3713958), ("ED2", 2, 1, "BD", 100)],
                       522: [("OP1", None, 1, "BD", 3969108)],
                       601: [("OP1", None, 1, "DVD", 2000000)], 602: [("OP1", None, 1, "BD", 2100000)],
                       603: [], 700: [("OP1", None, 2, "WEB", 500), ("OP1", None, 1, "BD", 900)]}
        self.search_results: list[tuple[int, str, int, list]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.statuses:
            code = self.statuses.pop(0)
            if code != 200:
                headers = {"retry-after": self.retry_after} if (code == 429 and self.retry_after) else {}
                return httpx.Response(code, headers=headers, json={"message": "nope"})
        q = dict(request.url.params.multi_items())
        if request.url.path == "/resource":
            ids = [int(x) for x in q["filter[external_id]"].split(",")]
            return httpx.Response(200, json={"resources": [
                {"id": 1000 + i, "external_id": i, "site": "aniDB",
                 "anime": [{"id": aid, "name": n, "year": y, "season": "Spring", "slug": n.lower()}
                           for aid, n, y in self.by_anidb.get(i, [])]}
                for i in ids if i in self.by_anidb]})
        if request.url.path == "/anime" and "q" in q:
            return httpx.Response(200, json={"anime": [_anime(i, n, y, th) for i, n, y, th in self.search_results]})
        if request.url.path == "/anime":
            ids = [int(x) for x in q["filter[anime][id]"].split(",")]
            names = {aid: (n, y) for lst in self.by_anidb.values() for aid, n, y in lst}
            return httpx.Response(200, json={"anime": [_anime(i, *names[i], self.themes.get(i, [])) for i in ids if i in names]})
        return httpx.Response(404, json={})


@pytest.fixture
def api():
    return FakeAPI()


@pytest.fixture
def client(api):
    hc = httpx.Client(transport=httpx.MockTransport(api.handler), base_url=at.API_BASE)
    sleeps: list[float] = []
    clock = [100.0]

    def sleep(s):
        sleeps.append(s); clock[0] += s

    c = at.AnimeThemesClient(client=hc, min_interval_s=1.0, sleep=sleep, monotonic=lambda: clock[0],
                             now=lambda: 1_000_000.0 + clock[0])
    c._test_sleeps = sleeps  # type: ignore[attr-defined]
    c._test_clock = clock    # type: ignore[attr-defined]
    return c


@pytest.fixture
def bridge():
    return at.Bridge.from_json(BRIDGE_JSON)


# ── bridge ───────────────────────────────────────────────────


def test_bridge_indexes_tvdb_with_season_and_tmdb_tv(bridge):
    assert bridge.entries == 6, "entries without an anidb id are skipped"
    via, ents = bridge.entries_for(72025, None)
    assert via == "tvdb" and [e.season for e in ents] == [1, 2, 3], "season 1 first, then ascending"
    via, ents = bridge.entries_for(76885, None)
    assert [e.anidb for e in ents] == [23, 219], "specials (season 0) sort AFTER season 1"
    via, ents = bridge.entries_for(None, 30991)
    assert via == "tmdb" and ents[0].anidb == 23, "themoviedb_id.tv is the fallback key"
    assert bridge.entries_for(None, None) == (None, [])
    assert bridge.entries_for(123456, 1) == (None, [])


def test_load_bridge_uses_a_fresh_cache_without_fetching(tmp_path):
    (tmp_path / at.BRIDGE_FILENAME).write_text(json.dumps(BRIDGE_JSON))
    calls = []
    hc = httpx.Client(transport=httpx.MockTransport(lambda r: (calls.append(r), httpx.Response(500))[1]))
    b = at.load_bridge(tmp_path, client=hc, now=lambda: (tmp_path / at.BRIDGE_FILENAME).stat().st_mtime + 10)
    assert b.entries == 6 and calls == [], "a cache younger than the TTL must not touch the network"


def test_load_bridge_refreshes_a_stale_cache_and_stores_the_etag(tmp_path):
    p = tmp_path / at.BRIDGE_FILENAME
    p.write_text(json.dumps(BRIDGE_JSON[:1]))
    seen = []

    def h(r):
        seen.append(dict(r.headers))
        return httpx.Response(200, headers={"etag": '"abc"'}, content=json.dumps(BRIDGE_JSON * 200).encode())
    hc = httpx.Client(transport=httpx.MockTransport(h))
    b = at.load_bridge(tmp_path, client=hc, now=lambda: p.stat().st_mtime + at.BRIDGE_TTL_S + 1)
    assert b.entries == 6 * 200 and (tmp_path / (at.BRIDGE_FILENAME + ".etag")).read_text() == '"abc"'
    assert "motif/" in seen[0]["user-agent"] and "github.com/Healzangels/motif" in seen[0]["user-agent"]
    # second refresh sends If-None-Match and a 304 keeps the file
    at._BRIDGE_CACHE.clear()
    hc2 = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(304) if r.headers.get("if-none-match") == '"abc"' else httpx.Response(500)))
    b2 = at.load_bridge(tmp_path, client=hc2, now=lambda: p.stat().st_mtime + at.BRIDGE_TTL_S + 1)
    assert b2.entries == 6 * 200


def test_load_bridge_stale_beats_nothing_and_warns_once(tmp_path, caplog):
    p = tmp_path / at.BRIDGE_FILENAME
    p.write_text(json.dumps(BRIDGE_JSON))
    at._BRIDGE_STALE_WARNED = False
    hc = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    with caplog.at_level(logging.WARNING, logger="motif.animethemes"):
        b = at.load_bridge(tmp_path, client=hc, now=lambda: p.stat().st_mtime + at.BRIDGE_TTL_S + 1)
        at._BRIDGE_CACHE.clear()
        at.load_bridge(tmp_path, client=hc, now=lambda: p.stat().st_mtime + at.BRIDGE_TTL_S + 1)
    assert b.entries == 6
    warns = [r for r in caplog.records if r.levelno == logging.WARNING and "stale cache" in r.getMessage()]
    assert len(warns) == 1, "class-9 hot-path rule: first failure warns, repeats stay quiet"


def test_load_bridge_without_any_cache_raises_and_logs(tmp_path, caplog):
    hc = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    with caplog.at_level(logging.WARNING, logger="motif.animethemes"):
        with pytest.raises(at.BridgeUnavailable):
            at.load_bridge(tmp_path, client=hc)
    assert any("bridge unavailable" in r.getMessage() for r in caplog.records), "never a silent empty bridge"
    assert not (tmp_path / at.BRIDGE_FILENAME).exists()


def test_load_bridge_rejects_a_truncated_payload(tmp_path):
    hc = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, json=BRIDGE_JSON)))
    with pytest.raises(at.BridgeUnavailable):
        at.load_bridge(tmp_path, client=hc)
    assert not (tmp_path / at.BRIDGE_FILENAME).exists(), "a 7-entry 'bridge' must not replace anything"


# ── client: query shapes, batching, pacing, errors ───────────


def test_step1_query_shape_is_flat_include_only(client, api):
    client.lookup_anidb([219, 23])
    r = api.calls[0]
    q = dict(r.url.params.multi_items())
    assert r.url.path == "/resource"
    assert q["filter[site]"] == "aniDB" and q["filter[external_id]"] == "23,219"
    assert q["include"] == "anime", "/resource refuses deep includes (measured 422) — themes come from step 2"
    assert q["page[size]"] == "100"


def test_step2_query_shape_uses_a_scoped_filter_and_the_deep_include(client, api):
    client.themes_for([521, 522])
    q = dict(api.calls[0].url.params.multi_items())
    assert api.calls[0].url.path == "/anime"
    assert q["filter[anime][id]"] == "521,522", (
        "an UNSCOPED filter[id] is applied to the include too and returns anime with EMPTY theme lists")
    assert "filter[id]" not in q
    assert q["include"] == "animethemes.animethemeentries.videos.audio"
    assert q["fields[audio]"] == "link,size"


def test_batches_are_capped_at_fifty_ids(client, api):
    api.by_anidb.update({i: [] for i in range(10_000, 10_101)})
    client.lookup_anidb(range(10_000, 10_101))
    sizes = [len(dict(c.url.params.multi_items())["filter[external_id]"].split(",")) for c in api.calls]
    assert sizes == [50, 50, 1]
    assert all(s <= at.BATCH for s in sizes)


def test_pacing_sleeps_to_the_minimum_interval(client, api):
    api.by_anidb.update({i: [] for i in range(10_000, 10_101)})
    client.lookup_anidb(range(10_000, 10_101))
    sleeps = client._test_sleeps
    assert len(sleeps) == 2 and all(0.99 <= s <= 1.0 for s in sleeps), (
        "three back-to-back batches → two waits of ~1s (60/min, a third under the documented 90/min)")


def test_429_honours_retry_after_then_succeeds(client, api):
    api.statuses = [429]; api.retry_after = "7"
    out = client.lookup_anidb([23])
    assert out[23][0].name == "Cowboy Bebop" and client.requests == 2
    assert 7.0 in client._test_sleeps, "Retry-After is the API's own instruction — sleep exactly that"


def test_retry_after_is_capped(client, api):
    api.statuses = [429]; api.retry_after = "3600"
    client.lookup_anidb([23])
    assert max(client._test_sleeps) == at.MAX_RETRY_AFTER_S


def test_server_errors_raise_and_are_never_cached(client, api):
    api.statuses = [500]
    with pytest.raises(at.AnimeThemesError) as ei:
        client.lookup_anidb([23])
    assert ei.value.status == 500
    assert 23 not in client._anime_by_anidb, "the tmdb.py v1.22.43 lesson: an outage must not poison the cache"
    out = client.lookup_anidb([23])
    assert out[23][0].anime_id == 521 and client.requests == 2


def test_empty_answers_are_cached_but_errors_are_not(client, api):
    out = client.lookup_anidb([424242])
    assert out == {424242: []} and client.requests == 1
    client.lookup_anidb([424242])
    assert client.requests == 1, "a 200 with no resource is an answer ('not on AnimeThemes') — cached"


def test_cache_expires_after_ttl(client, api):
    client.themes_for([521]); client.themes_for([521])
    assert client.requests == 1
    client._test_clock[0] += at.CACHE_TTL_S + 1
    client.themes_for([521])
    assert client.requests == 2


def test_default_client_sends_a_versioned_user_agent(monkeypatch):
    seen = {}

    class _C:
        def __init__(self, *a, **kw):
            seen.update(kw)

        def close(self):
            pass
    monkeypatch.setattr(httpx, "Client", _C)
    at.AnimeThemesClient().close()
    from app import __version__
    assert seen["headers"]["User-Agent"] == f"motif/{__version__} (+https://github.com/Healzangels/motif)"
    assert seen["base_url"] == at.API_BASE and seen["timeout"].read == 20.0


# ── resolver ─────────────────────────────────────────────────


def test_clean_resolution_picks_season_one_op1_first_version_best_source(bridge, client):
    row = {"title": "Cowboy Bebop", "year": "1998", "guid_tvdb": 76885, "guid_tmdb": None}
    res = at.resolve(row, bridge, client, name_search=False)
    assert res.confidence == "clean" and res.via == "tvdb"
    assert [s.season for s in res.seasons] == [1, 0], "the movie special is kept as a later group, never the default"
    sm, theme, audio = res.default
    assert sm.info.name == "Cowboy Bebop" and theme.slug == "OP1" and audio.link.endswith("CowboyBebop-OP1.ogg")
    assert res.has_op and res.has_audio


def test_glance_when_the_bridge_falls_through_to_a_later_season(bridge, client):
    row = {"title": "Bleach", "year": "2004", "guid_tvdb": 99999, "guid_tmdb": None}
    res = at.resolve(row, bridge, client, name_search=False)
    assert res.confidence == "glance" and "season 2" in res.reason
    assert res.default[1].slug == "OP1" and res.default[2].version == 1 and res.default[2].source == "BD", (
        "among two audio versions of one OP: version 1 first, then BD over WEB")


def test_glance_when_the_year_is_off_by_more_than_one(bridge, client):
    row = {"title": "Cowboy Bebop", "year": "2010", "guid_tvdb": 76885, "guid_tmdb": None}
    res = at.resolve(row, bridge, client, name_search=False)
    assert res.confidence == "glance" and "year differs" in res.reason


def test_unknown_year_is_not_evidence_against(bridge, client):
    row = {"title": "Cowboy Bebop", "year": None, "guid_tvdb": 76885, "guid_tmdb": None}
    assert at.resolve(row, bridge, client, name_search=False).confidence == "clean"


def test_multi_season_show_groups_every_season_with_audio(bridge, client):
    row = {"title": "Crest of the Stars", "year": "1999", "guid_tvdb": 72025, "guid_tmdb": None}
    res = at.resolve(row, bridge, client, name_search=False)
    assert res.confidence == "clean"
    assert [(s.season, s.info.name) for s in res.seasons] == [(1, "Crest of the Stars"), (2, "Banner of the Stars")], (
        "season 3 has no audio on AnimeThemes and is dropped; groups keep bridge order")


def test_tmdb_fallback_key(bridge, client):
    row = {"title": "Cowboy Bebop", "year": "1998", "guid_tvdb": None, "guid_tmdb": 30991}
    res = at.resolve(row, bridge, client, name_search=False)
    assert res.via == "tmdb" and res.confidence == "clean"


def test_unmapped_row_without_name_search_is_none_with_a_reason(bridge, client):
    row = {"title": "Arcane", "year": "2021", "guid_tvdb": 1, "guid_tmdb": 2}
    res = at.resolve(row, bridge, client, name_search=False)
    assert res.confidence is None and res.default is None and "no bridge entry" in res.reason
    assert client.requests == 0


def test_name_search_needs_similar_name_year_and_audio(bridge, client, api):
    api.search_results = [(801, "Arcane", 2021, [("OP1", None, 1, "WEB", 1234)]),
                          (802, "Arcane Something Else", 1990, [("OP1", None, 1, "WEB", 1)])]
    row = {"title": "Arcane", "year": "2021", "guid_tvdb": 1, "guid_tmdb": 2}
    res = at.resolve(row, bridge, client)
    assert res.confidence == "name" and res.via == "name" and res.default[0].info.anime_id == 801
    q = dict(api.calls[-1].url.params.multi_items())
    assert q["q"] == "Arcane" and q["page[size]"] == "5"
    api.search_results = [(803, "Totally Different", 2021, [("OP1", None, 1, "WEB", 1)])]
    assert at.resolve(row, bridge, client).confidence is None, "similarity < 0.6 is not a match"


def test_bridged_but_absent_on_animethemes_reports_that(bridge, client, api):
    api.by_anidb.pop(23); api.by_anidb.pop(219)
    row = {"title": "Cowboy Bebop", "year": "1998", "guid_tvdb": 76885, "guid_tmdb": None}
    res = at.resolve(row, bridge, client, name_search=False)
    assert res.confidence is None and res.via == "tvdb" and "no AnimeThemes entry" in res.reason


def test_prefetch_warms_caches_so_resolve_makes_no_further_calls(bridge, client, api):
    rows = [{"title": "Cowboy Bebop", "year": "1998", "guid_tvdb": 76885, "guid_tmdb": None},
            {"title": "Crest of the Stars", "year": "1999", "guid_tvdb": 72025, "guid_tmdb": None},
            {"title": "Bleach", "year": "2004", "guid_tvdb": 99999, "guid_tmdb": None}]
    at.prefetch(rows, bridge, client)
    assert client.requests == 2, "one step-1 batch + one step-2 batch for the whole set"
    for r in rows:
        at.resolve(r, bridge, client, name_search=False)
    assert client.requests == 2


# ── harness + conventions ────────────────────────────────────


def test_harness_buckets_and_row_selection(tmp_path):
    sys.path.insert(0, str(REPO / "tools"))
    import animethemes_eval as ev
    from app.core.db import get_conn, init_db, transaction
    db = tmp_path / "motif.db"; init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute("""INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, discovered_at, last_seen_at)
                        VALUES ('3', 'Anime', 'show', 1, 0, 'anime', 1, 'x', 'x'), ('2', 'TV', 'show', 0, 0, 'tv', 1, 'x', 'x')""")
        tid = conn.execute("""INSERT INTO themes (media_type, tmdb_id, title, upstream_source, last_seen_sync_at, first_seen_sync_at)
                              VALUES ('tv', 30991, 'Cowboy Bebop', 'themoviedb', 'x', 'x')""").lastrowid
        oid = conn.execute("""INSERT INTO themes (media_type, tmdb_id, title, upstream_source, last_seen_sync_at, first_seen_sync_at)
                              VALUES ('tv', -5, 'Orphan', 'plex_orphan', 'x', 'x')""").lastrowid
        for rk, title, has, theme in (("1", "A", 0, None), ("2", "B", 1, tid), ("3", "C", 1, oid), ("4", "D", 1, None)):
            conn.execute("""INSERT INTO plex_items (rating_key, section_id, media_type, title, guid_tvdb, guid_tmdb, theme_id, folder_path, edition_key, has_theme, first_seen_at, last_seen_at)
                            VALUES (?, '3', 'show', ?, 1, 2, ?, '/x', '', ?, 'x', 'x')""", (rk, title, theme, has))
        conn.execute("""INSERT INTO plex_items (rating_key, section_id, media_type, title, folder_path, edition_key, has_theme, first_seen_at, last_seen_at)
                        VALUES ('5', '2', 'show', 'not anime', '/y', '', 0, 'x', 'x')""")
        conn.execute("""INSERT INTO plex_items (rating_key, section_id, media_type, title, folder_path, edition_key, has_theme, first_seen_at, last_seen_at)
                        VALUES ('6', '3', 'collection', 'anime coll', '', '', 0, 'x', 'x')""")
    rows = ev.load_rows(db, None, None)
    assert [r["title"] for r in rows] == ["A", "B", "C", "D"], "anime section, shows only — the /api/library shape"
    assert [ev.bucket_for(r) for r in rows] == ["no_theme", "tdb_linked", "user_set", "plex_served_unlinked"]


def test_ci_lints_the_tools_dir():
    ci = (REPO / ".github" / "workflows" / "ci.yml").read_text()
    assert "ruff check app/ tests/ tools/ --select F" in ci, "tools/ holds product-importing scripts — the blocking lint covers it"


def test_spec_and_baseline_are_in_the_tree():
    assert (REPO / "docs" / "specs" / "ANIMETHEMES_SPEC.md").exists()
    assert (REPO / "docs" / "specs" / "animethemes_eval" / "2026-09-09-baseline.md").exists()


def test_v0_51_314_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.314: " in init_py
