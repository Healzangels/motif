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
swept — none of these eight feed a Plex API call.

v0.51.49 caught a 9th site of the same class that the media_type-keyed
enumeration missed: the bulk LET PLEX SERVE helper (_bulk_lps_run) maps
`"show" if mt == "tv" else "movie"` (mt = a theme row's media_type, so it
reaches 'collection') into a plex_items lookup — now `else mt`. That map
is guarded below too. The section-probe `tab` map (api.py ~6926) stays
`else "movie"` on purpose: it filters plex_sections.type, where
'collection' is not a valid library section type.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()

BUGGY = '"show" if media_type == "tv" else "movie"'
SAFE = '"show" if media_type == "tv" else media_type'

# v0.51.49: the mt-keyed bulk-LPS sibling (theme-row media_type, reaches
# 'collection'). Same plex_items-query class, different variable name.
BUGGY_MT = '"show" if mt == "tv" else "movie"'
SAFE_MT = '"show" if mt == "tv" else mt'

# v0.51.53: the REVERSE direction (plex_items.media_type -> motif media_type)
# had the SAME class in a spelling the forward grep couldn't see:
# `"tv" if pi["media_type"] == "show" else "movie"` collapses a collection to
# 'movie' (the upload-theme mint bug). Now routed through _motif_media_type.
BUGGY_REVERSE = '"media_type"] == "show" else "movie"'

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


def test_no_reverse_hardcoded_movie_map_remains():
    # v0.51.53: mirror of the forward tripwire for the plex_items.media_type ->
    # motif conversion. A collection must stay 'collection', never collapse to
    # 'movie' (which mis-keys its theme row and orphans it from the
    # media_type='collection' endpoints — the upload-theme mint bug). Route
    # every reverse conversion through _motif_media_type().
    assert BUGGY_REVERSE not in API_PY, (
        "a reverse plex->motif media_type map still hardcodes `else \"movie\"` "
        "— a collection collapses to 'movie'. Route through _motif_media_type()."
    )


def test_motif_media_type_helper_is_collection_safe():
    # The reverse-direction chokepoint must exist and map collection->collection
    # + show->tv (never collection->movie).
    assert "def _motif_media_type(plex_media_type: str) -> str:" in API_PY
    i = API_PY.index("def _motif_media_type(")
    body = API_PY[i:i + 200]
    assert 'return "tv" if plex_media_type == "show" else plex_media_type' in body


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


def test_bulk_lps_mt_map_is_collection_safe():
    # v0.51.49: the bulk LET PLEX SERVE helper's mt-keyed plex_items map must
    # be `else mt` (mt = a theme row's media_type, reaches 'collection'), not
    # the hardcoded `else "movie"` that missed a collection's plex_items rows.
    assert BUGGY_MT not in API_PY, (
        "an mt-keyed plex_items map still hardcodes `else \"movie\"` — a themed "
        "collection (media_type='collection') would match no plex_items rows. "
        "Map with `else mt`."
    )
    body = _body("def _bulk_lps_run(")
    assert SAFE_MT in body, (
        "_bulk_lps_run lost its collection-safe mt map (`else mt`)."
    )
