"""v1.18.6 — accept media_type='collection' on per-item API endpoints.

Two related bugs surfaced in the user's collections-tab testing:

  1. **API validation rejected 'collection'**. The `MediaType =
     Literal["movie", "tv"]` alias at the top of api.py drove
     FastAPI's path-parameter validation on ~50 per-item
     endpoints (`/api/items/{media_type}/{tmdb_id}/...`).
     Hitting any of them on a collection row returned
       `422 Input should be 'movie' or 'tv'`.
     the user's screenshot showed exactly this on a SOURCE-menu
     redownload click. The // MOTIF INFO panel echoed the
     server's literal_error payload verbatim.

  2. **manual-url created the orphan with the wrong media_type.**
     `api_manual_url` mapped `pi.media_type` → themes-canonical
     media_type via `theme_media_type = "tv" if pi["media_type"]
     == "show" else "movie"`. The fall-through branch treated
     'collection' as 'movie', so SET URL on a Plex collection
     created an orphan at `movie/-N` (instead of `collection/-N`).
     Downstream consequences:
       - The download job ran as media_type='movie', so the
         place job dispatched to `_do_place` (movie/show path)
         instead of `_do_place_collection` (HTTP upload).
       - `_do_place` searched FolderIndex for a media folder
         that doesn't exist (collections have no folder).
         Result: "Skipped placement: no_match" in the user's logs.
       - The orphan's `pi.theme_id` linkage couldn't bridge the
         media_type mismatch (pi.media_type='collection' vs
         themes.media_type='movie'), so the row stayed in a
         broken state with no recovery path beyond PURGE +
         re-SET URL.

Fixes:

  * `MediaType` literal widened to `Literal["movie", "tv",
    "collection"]`. Every endpoint that takes media_type as a
    path / query param now accepts 'collection' without
    additional change — the type alias drives ~50 routes.

  * `api_manual_url` `theme_media_type` dispatch expanded from
    a binary `show|else` to a tristate: 'show' → 'tv',
    'collection' → 'collection', else → 'movie'. The
    collection branch preserves the value so the orphan, the
    download job, and the placement job all stay in the
    collection namespace.

Existing already-broken rows (like the user's Willy Wonka
Collection orphan at movie/-27): recovery is PURGE + re-SET
URL once v1.18.6 deploys. The fixed manual-url creates the
orphan as `collection/-27` and the worker's collection-place
path takes over.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = REPO / "app" / "web" / "api.py"


# ── MediaType literal accepts 'collection' ─────────────────────


def test_media_type_literal_includes_collection():
    """The top-of-file `MediaType` alias must declare
    Literal["movie", "tv", "collection"] so FastAPI's path-
    parameter validation accepts 'collection' on every endpoint
    that uses it. Pre-fix, every collection-row click that hit
    /api/items/collection/<tmdb>/... returned 422."""
    src = API_PY.read_text()
    # Pin the exact literal value declaration.
    m = re.search(
        r'^MediaType\s*=\s*Literal\[([^\]]+)\]',
        src, re.MULTILINE,
    )
    assert m is not None, (
        "v1.18.6: MediaType literal declaration missing"
    )
    members = [x.strip().strip('"') for x in m.group(1).split(",")]
    assert "movie" in members
    assert "tv" in members
    assert "collection" in members, (
        "v1.18.6: MediaType must include 'collection' so per-item "
        "API endpoints accept the new media_type. Pre-fix the user's "
        "redownload on a collection row returned 422."
    )


def test_media_type_literal_is_single_source_of_truth():
    """Pin that only ONE MediaType literal exists in api.py.
    Multiple definitions would drift — every endpoint signature
    depends on this exact alias for FastAPI's validation."""
    src = API_PY.read_text()
    matches = re.findall(
        r'^MediaType\s*=\s*Literal\[',
        src, re.MULTILINE,
    )
    assert len(matches) == 1, (
        f"v1.18.6: exactly ONE MediaType literal declaration "
        f"expected; found {len(matches)}"
    )


# ── manual-url maps collection correctly ──────────────────────


def test_api_manual_url_handles_collection_media_type():
    """`api_manual_url` must dispatch pi.media_type='collection'
    to theme_media_type='collection' — NOT to 'movie' (the pre-
    fix fall-through behavior). Without this, SET URL on a
    collection row creates an orphan at movie/-N, the worker
    routes through the movie place path, FolderIndex fails to
    find a (nonexistent) folder, and the row ends in a broken
    state requiring PURGE + re-SET URL."""
    src = API_PY.read_text()
    # Find the api_manual_url body.
    fn_start = src.index('async def api_manual_url(')
    fn_end = src.index('\n    @app.', fn_start + 1)
    body = src[fn_start:fn_end]
    # Pin the new tristate dispatch.
    assert 'elif pi["media_type"] == "collection":' in body, (
        "v1.18.6: api_manual_url must explicitly branch on "
        "pi.media_type == 'collection' (not fall-through to "
        "the movie default)"
    )
    assert 'theme_media_type = "collection"' in body, (
        "v1.18.6: the collection branch must set "
        "theme_media_type='collection' — preserving the media "
        "type so the orphan, download, and place stay in the "
        "collection namespace"
    )


def test_api_manual_url_keeps_movie_tv_dispatch_intact():
    """Sanity: the v1.18.6 fix must not regress the existing
    'show' → 'tv' / else → 'movie' dispatch — that's the
    movie/tv flow which is unchanged."""
    src = API_PY.read_text()
    fn_start = src.index('async def api_manual_url(')
    fn_end = src.index('\n    @app.', fn_start + 1)
    body = src[fn_start:fn_end]
    assert 'if pi["media_type"] == "show":' in body
    assert 'theme_media_type = "tv"' in body
    assert 'theme_media_type = "movie"' in body, (
        "v1.18.6: the else-movie branch must remain — pi.media"
        "_type='movie' must still map to themes.media_type='movie'"
    )
