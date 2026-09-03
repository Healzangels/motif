"""v0.51.47 — INFO card audit fixes (4-agent read-only sweep).

Three real bugs + one consistency align surfaced by auditing the INFO card:
  1+2. EDITION BLEED: the recovery-options RE-DOWNLOAD + REVERT handlers passed
       only section_id (dropped rating_key), so on a multi-edition-in-one-section
       title they hit the legacy section-wide path instead of the clicked edition.
       Now thread rating_key like the row SOURCE-menu equivalents (v1.21.64/.73).
  3.   COLLECTIONS: api_recovery_options mapped media_type='collection' → plex_mt
       'movie', so every rk-pick query (incl. the clicked-rk one) missed the
       collection's plex_items row → rating_key None → SET URL / UPLOAD MP3
       recovery buttons couldn't wire up. Now `else media_type` like api_item.
  4.   api_item's edition-label folder_path query used the same hardcoded-movie
       map → aligned to _info_plex_type.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ── 1+2. recovery-option RE-DOWNLOAD + REVERT thread rating_key ───────

def test_recovery_redownload_scopes_to_edition():
    i = APP_JS.index("const redlQp = [")
    block = APP_JS[i:i + 320]
    assert "rating_key=${encodeURIComponent(ratingKey)}" in block
    assert "section_id=${encodeURIComponent(sectionId)}" in block
    assert "/redownload${redlQp ? `?${redlQp}` : ''}" in block


def test_recovery_revert_scopes_to_edition():
    i = APP_JS.index("const revertQp = [")
    block = APP_JS[i:i + 320]
    assert "rating_key=${encodeURIComponent(ratingKey)}" in block
    assert "section_id=${encodeURIComponent(sectionId)}" in block
    assert "/revert${revertQp ? `?${revertQp}` : ''}" in block


# ── 3. recovery-options preserves the collection media_type ──────────

def test_recovery_options_maps_collection_correctly():
    i = API_PY.index("def api_recovery_options(")
    body = API_PY[i:API_PY.index("\n    @app.", i + 1)]
    assert 'plex_mt = "show" if media_type == "tv" else media_type' in body
    # the old collection→movie bug must be gone from this endpoint.
    assert 'plex_mt = "show" if media_type == "tv" else "movie"' not in body


# ── 4. api_item edition-label query uses the shared plex-type ─────────

def test_api_item_edition_label_uses_info_plex_type():
    # the folder_path edition-label fallback threads _info_plex_type (preserves
    # 'collection') instead of the hardcoded-movie map.
    import re
    flat = re.sub(r"\s+", " ", API_PY)
    # v0.51.311: the fallback is two-arm now (guid_tmdb OR theme_id) — the
    # invariant is unchanged: it threads _info_plex_type, not the movie map.
    # v0.51.313: the tuple grew ORDER BY params — a PREFIX still pins the
    # invariant (the fallback threads _info_plex_type, not the movie map).
    assert '(section_id, str(tmdb_id), t["id"], _info_plex_type,' in flat
    # no (section_id, str(tmdb_id)) plex_items query still pairs with the hardcoded
    # movie map (the collection→movie edition-label bug is gone).
    assert '(section_id, str(tmdb_id), "show" if media_type == "tv" else "movie")' not in flat
