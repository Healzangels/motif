"""v1.22.39 (holistic audit) — HAMA guid_tmdb-NULL theme_id-linkage misses.

AniDB anime resolve to TVDB GUIDs and carry guid_tmdb=NULL; they link to a
themes row only via plex_items.theme_id. Two sidecar-presence checks keyed on
guid_tmdb ONLY (the v1.22.17 "4 sites" + v1.22.32 still_p class missed them):

- sync.py url_changed `has_sidecar`: returned False for a HAMA M-row on a TDB
  URL change → re-downloaded TDB bytes over the user's manual sidecar every cron
  sync. Now mirrors the is_new twin's LEFT JOIN themes + (guid_tmdb OR theme_id).
- plex_enum.py reaper Tier-2 `sidecar_db`: missed a anime survivor → mis-tiered
  to no_fallback ("theme lost, no recovery"). Now LEFT JOIN themes + theme_id.

Both queries are behaviorally validated by the existing v1.22.17 / v1.22.32
linkage tests for their sibling sites; these source-pins lock the two new sites.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()


def test_sync_url_changed_has_sidecar_uses_theme_id_linkage():
    # v1.23.89: both has_sidecar sites (is_new + url_changed) now route through
    # _plex_title_present(... sidecar_only=True), an index-friendly split-EXISTS
    # that preserves the v1.22.39 theme_id linkage (its 2nd EXISTS matches
    # pi.theme_id). The url_changed twin must carry the same call as is_new.
    assert SYNC_PY.count("has_sidecar = _plex_title_present(") == 2, (
        "v1.22.39: both is_new and url_changed has_sidecar must match via "
        "theme_id linkage (now through the shared helper)")
    helper = SYNC_PY[SYNC_PY.index("def _plex_title_present("):
                     SYNC_PY.index("def _plex_title_present(") + 1600]
    assert "theme_id = ?" in helper and "local_theme_file = 1" in helper
    # Pin the url_changed comment so the intent survives.
    assert "still open for the guid_tmdb-NULL" in SYNC_PY


def test_reaper_tier2_sidecar_db_uses_theme_id_linkage():
    i = PLEX_ENUM_PY.index("sidecar_db = conn.execute(")
    block = PLEX_ENUM_PY[i:i + 700]
    assert "LEFT JOIN themes t ON t.id = pi.theme_id" in block, (
        "v1.22.39: Tier-2 sidecar_db must match a anime survivor via theme_id")
    assert "OR (t.media_type = ? AND t.tmdb_id = ?)" in block


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
