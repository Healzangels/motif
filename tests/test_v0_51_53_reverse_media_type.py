"""v0.51.53 — reverse-direction plex_items.media_type -> motif is collection-safe.

The code review of the v0.51.52 diff surfaced that the v0.51.47-49 forward sweep
+ its guard only covered the FORWARD map (motif media_type -> plex_items). Five
REVERSE sites (plex_items -> motif) carried the identical collection->movie
collapse in a spelling the forward grep couldn't see. The LIVE one:
api_upload_theme minted a themeless collection's themes row at
media_type='movie' (a collection has NULL guid_tmdb, so the theme-match branch
is skipped and it hits the mint), orphaning the theme from every
/api/items/collection/{tmdb_id} endpoint (INFO card / REVERT / PURGE) that keys
on media_type='collection'. (The other 4 reverse sites 409 before the ternary
for collections, so they were defensive; upload-theme was the reachable bug.)

v0.51.53 routes all reverse conversions through _motif_media_type (show->tv,
movie/collection identity) and extends the v0.51.48 guard to the reverse map.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_helper_maps_collection_and_show():
    # Exercise the real function: a collection must stay 'collection', never
    # collapse to 'movie'; show->tv; movie identity.
    from app.web.api import _motif_media_type
    assert _motif_media_type("collection") == "collection"
    assert _motif_media_type("show") == "tv"
    assert _motif_media_type("movie") == "movie"


def test_upload_theme_mints_collection_via_helper():
    # api_upload_theme (the live bug) resolves theme_media_type through the
    # collection-safe helper, so a themeless collection's minted themes row is
    # keyed media_type='collection', not 'movie'.
    i = API_PY.index("async def api_upload_theme(")
    body = API_PY[i:API_PY.index("\n    @app.", i + 1)]
    assert 'theme_media_type = _motif_media_type(pi["media_type"])' in body
    # the buggy reverse ternary itself is gone (the comment may still cite it).
    assert '"tv" if pi["media_type"] == "show" else "movie"' not in body


def test_reverse_sites_routed_through_helper():
    # No inline plex->motif ternary maps a collection to 'movie' anywhere; the
    # only raw `"tv" if ... == "show" else ...` is the helper's own body.
    assert API_PY.count('"media_type"] == "show" else "movie"') == 0
    # def + 6 call sites (adopt-sidecar, replace-with-tdb, upload-theme,
    # 2x import-match, api_recovery _row_mt).
    assert API_PY.count("_motif_media_type(") >= 7
