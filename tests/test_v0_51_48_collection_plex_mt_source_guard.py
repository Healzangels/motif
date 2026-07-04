"""v0.51.48 — collection-safe plex_items media_type maps (source guard).

Eight sites in api.py derived a plex_items media_type filter as
`"show" if media_type == "tv" else "movie"`. For a themed COLLECTION
(media_type='collection', placed via plex_upload) the `else "movie"`
branch made every plex_items lookup filter on media_type='movie' — an
empty set — so the collection's rows were silently missed. v0.51.47
fixed the two INFO-card sites (api_item edition-label + api_recovery_
options); v0.51.48 finishes the sweep across the destructive / query
handlers. The fix mirrors api_item's `_info_plex_type` convention:
`else media_type` (collection -> 'collection').

All eight are plex_items TABLE queries, where 'collection' is the correct
media_type value. Plex HTTP-API type args (which use 'movie'/'show' and
have no 'collection' library type) are a different shape and are NOT
swept — none of these eight feed a Plex API call. The `else "movie"`
maps keyed on OTHER variables (bulk-LPS `mt`, section-probe `tab`) are
outside this guard on purpose.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()

BUGGY = '"show" if media_type == "tv" else "movie"'
SAFE = '"show" if media_type == "tv" else media_type'

# The eight handlers whose plex_items media_type filter was swept, keyed
# by their def anchor. Value = the collection reach path (or why not).
SWEPT = {
    "def _trigger_plex_item_refresh(": "post PURGE/DEL plex refresh (live)",
    "def api_unplace_item(": "DEL / unplace (live)",
    "def api_replace_item(": "REPLACE api->file (movie/tv-only in practice)",
    "def api_switch_placement(": "SWITCH api->file (400s on collection)",
    "def api_unmanage_item(": "UNMANAGE restat (live)",
    "def api_forget_item(": "PURGE / forget (live)",
    "def api_delete_item(": "DELETE (live)",
    "def api_clear_failure(": "clear-failure owner-sections (live)",
}


def _body(anchor: str) -> str:
    i = API_PY.index(anchor)
    j = API_PY.index("\n    @app.", i + 1)
    return API_PY[i:j]


def test_no_media_type_hardcoded_movie_map_remains():
    # Global tripwire: the media_type-keyed hardcoded-movie shape must be gone
    # everywhere. Any reappearance re-misses a collection's plex_items rows
    # (media_type='collection' != 'movie'). Use `else media_type` instead.
    assert BUGGY not in API_PY, (
        "a media_type ternary still hardcodes `else \"movie\"` — a themed "
        "collection (media_type='collection') would match no plex_items rows. "
        "Map with `else media_type` (see _info_plex_type)."
    )


def test_each_swept_handler_uses_collection_safe_map():
    for anchor, note in SWEPT.items():
        body = _body(anchor)
        assert SAFE in body, (
            f"{anchor} ({note}) lost its collection-safe plex_items "
            f"media_type map (`else media_type`)."
        )
        assert BUGGY not in body, (
            f"{anchor} ({note}) reintroduced the hardcoded `else \"movie\"` "
            f"plex_items map — collections would be missed."
        )
