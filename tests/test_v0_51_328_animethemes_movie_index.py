"""v0.51.328 — ANIME THEMES close-out: the bridge's TMDB movie index.

The anime-lists file carries 1,363 entries keyed by a TMDB *movie* id
(`themoviedb_id: {"movie": [128]}` — a list), but the bridge indexed only the
tv key and a Plex movie row looked its movie id up in the tv index — where a
numerically equal tv id would have matched the wrong show. The sweep includes
movie rows (`pi.media_type IN ('show', 'movie')`), so an anime-films section
was exposed. Now: a movie index, and lookups routed by the row's media type —
a movie row never consults the tv index, a show never the movie one.
"""
from __future__ import annotations

import sys
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))
from app.core import animethemes as at  # noqa: E402
from test_v0_51_314_animethemes_resolver import BRIDGE_JSON, FakeAPI  # noqa: E402

CORE = (REPO / "app" / "core" / "animethemes.py").read_text()
HARNESS = (REPO / "tools" / "animethemes_eval.py").read_text()

# a movie whose TMDB movie id (128) is numerically equal to a tv entry's tv id
BRIDGE = BRIDGE_JSON + [
    {"anidb_id": 7, "themoviedb_id": {"movie": [128]}, "type": "MOVIE"},
    {"anidb_id": 8, "themoviedb_id": {"tv": 128}, "season": {"tvdb": 1}, "type": "TV"},
    {"anidb_id": 9, "themoviedb_id": {"movie": 4242}, "type": "MOVIE"},   # scalar form, defensively
]


def test_bridge_indexes_movie_ids_as_their_own_key_space():
    b = at.Bridge.from_json(BRIDGE)
    assert [e.anidb for e in b.by_tmdb_movie[128]] == [7]
    assert [e.anidb for e in b.by_tmdb_tv[128]] == [8]
    assert [e.anidb for e in b.by_tmdb_movie[4242]] == [9], "a scalar movie id still indexes"
    assert "%d tmdb movie keys" in CORE, "the bridge-loaded log line counts them"


def test_lookup_routes_by_media_type_and_never_crosses():
    b = at.Bridge.from_json(BRIDGE)
    via, ents = b.entries_for(None, 128, "movie")
    assert via == "tmdb" and [e.anidb for e in ents] == [7], "a movie row hits the MOVIE index"
    via, ents = b.entries_for(None, 128, "show")
    assert via == "tmdb" and [e.anidb for e in ents] == [8], "a show row hits the TV index"
    assert b.entries_for(None, 4242, "show") == (None, []), "a show never consults the movie index"
    assert b.entries_for(None, 30991, "movie") == (None, []), "a movie never consults the tv index"
    assert b.entries_for(76885, 30991, "movie") == (None, []), "…nor the tvdb one (movies carry no tvdb id)"
    via, ents = b.entries_for(76885, None)
    assert via == "tvdb" and ents[0].anidb == 23, "no media type = the tag-1 show path, unchanged"


def test_resolve_and_sweep_pass_the_row_media_type():
    fake = FakeAPI()
    fake.by_anidb[7] = [(700, "Cowboy Bebop: The Movie", 2001)]
    fake.themes[700] = [("OP1", None, 1, "BD", 4000000)]
    fake.by_anidb[8] = [(800, "Some Show", 1999)]          # the tv entry that shares the number 128
    fake.themes[800] = [("OP1", None, 1, "BD", 3000000)]
    hc = httpx.Client(transport=httpx.MockTransport(fake.handler), base_url=at.API_BASE)
    client = at.AnimeThemesClient(client=hc, min_interval_s=0.0, sleep=lambda s: None)
    bridge = at.Bridge.from_json(BRIDGE)
    movie = {"rating_key": "m1", "title": "Cowboy Bebop: The Movie", "year": "2001",
             "guid_tvdb": None, "guid_tmdb": 128, "media_type": "movie", "section_id": "3"}
    res = at.resolve(movie, bridge, client, name_search=False)
    assert res.confidence == "clean" and res.seasons[0].anidb == 7 and res.default is not None, (
        "a film's entry has no season — for a movie row it IS the clean match (year agrees)")
    assert res.reason == "film entry, year agrees"
    off = at.resolve({**movie, "year": "1990"}, bridge, client, name_search=False)
    assert off.confidence == "glance" and "year differs" in off.reason, "the year rule still applies to films"
    show = {**movie, "rating_key": "s1", "media_type": "show", "title": "Some Show", "year": "1999"}
    res = at.resolve(show, bridge, client, name_search=False)
    assert res.seasons and res.seasons[0].anidb == 8, "the same tmdb number as a show resolves through the tv index"
    out = at.sweep([movie], bridge, client)
    assert out[0]["group"] == "ready" and out[0]["anidb"] == 7
    assert 'bridge.entries_for(r.get("guid_tvdb"), r.get("guid_tmdb"), r.get("media_type"))' in CORE, "prefetch too"
    assert 'bridge.entries_for(r["guid_tvdb"], r["guid_tmdb"], r["media_type"])' in HARNESS


def test_series_close_report_is_banked():
    rep = (REPO / "docs" / "specs" / "animethemes_eval" / "2026-09-11-series-close.md").read_text()
    assert "| all | 1280 | 1210 (94%)" in rep and "| no_theme | 376 |" in rep
    assert "decision 3" in rep
    assert not (REPO / "docs" / "specs" / "animethemes_eval" / "2026-09-11").exists(), "results.json is not committed"


def test_v0_51_328_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.328: ANIME THEMES close-out" in init_py
