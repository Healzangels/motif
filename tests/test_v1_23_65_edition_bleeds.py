"""v1.23.65 — holistic-audit cluster G: edition-scope bleeds.

Three reads/writes that were section-scoped but edition-BLIND, so on a
multi-edition title sharing one Plex section (the user's LotR / Watchmen — each
edition a distinct rating_key + distinct plex_upload placement) one edition's
state bled onto a sibling:

  #9 (api.py) — the shared _teardown_plex_api_artifacts_for_placements helper
    resolved the Plex rating_key by (theme_id|guid, section_id) with NO edition
    filter, so PURGE/DELETE on one edition called plex.delete_theme on an
    ARBITRARY sibling edition's rk → cleared Plex's serving theme for the wrong
    edition. Now each placement resolves its rk by its own edition_key (the
    placement carries it; both callers now SELECT it).

  #13 (plex_enum.py) — the has_plex_upload lookup that clears the +P
    (plex_independent_theme) observation joined placements WITHOUT p.edition_key.
    The "collection-only, edition-blind OK" note went stale (worker now writes
    plex_upload placements for movie/TV too), so one edition's plex_upload
    cleared SRC=P on a sibling that IS independently Plex-served. Now scoped to
    the enumerated row's own edition.

  #4 (api.py) — DOWNLOAD MISSING LEFT JOINed local_files on (mt, tmdb, section)
    with no edition_key, so a multi-edition title whose standard edition is
    themed but a sibling is themeless was treated as "not missing" and the
    sibling's gap was silently skipped. Now edition-aware end-to-end (DISTINCT +
    join + per-edition enqueue).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()


# ── #13: has_plex_upload +P suppression is edition-scoped ──


def test_has_plex_upload_lookup_is_edition_scoped():
    i = ENUM_PY.index("has_plex_upload = bool(conn.execute(")
    block = ENUM_PY[i:i + 700]
    assert "p.edition_key = pi.edition_key" in block, (
        "v1.23.65: the has_plex_upload placements join must carry edition_key so "
        "one edition's plex_upload doesn't clear a sibling's +P"
    )


# ── #9: teardown helper resolves rk per-edition ──


def test_teardown_helper_rk_lookup_is_edition_scoped():
    i = API_PY.index("def _teardown_plex_api_artifacts_for_placements(")
    block = API_PY[i:i + 5500]
    # both rk lookups (theme_id path + guid fallback) carry the edition clause.
    assert block.count("AND edition_key = ?") >= 2, (
        "v1.23.65: both rk lookups in the teardown helper must filter on "
        "edition_key so PURGE/DELETE on one edition doesn't tear down a sibling"
    )
    assert 'pr["edition_key"]' in block, (
        "v1.23.65: the helper must use each placement's own edition_key"
    )


def test_teardown_callers_select_edition_key():
    # PURGE (both branches) + DELETE now SELECT edition_key into the placements
    # list so the helper can read pr["edition_key"].
    assert API_PY.count(
        '"SELECT media_folder, placement_kind, section_id, edition_key "'
    ) >= 3, (
        "v1.23.65: the two PURGE branches + the DELETE handler must select "
        "edition_key for the teardown helper"
    )


# ── #4: DOWNLOAD MISSING is edition-aware ──


def test_download_missing_query_is_edition_aware():
    i = API_PY.index("async def api_library_download_missing(")
    block = API_PY[i:i + 5500]
    assert "SELECT DISTINCT t.media_type, t.tmdb_id, pi.edition_key" in block, (
        "v1.23.65: the missing-theme query must carry pi.edition_key"
    )
    assert "AND lf.edition_key = pi.edition_key" in block, (
        "v1.23.65: the local_files join must match the row's edition so a themed "
        "standard edition no longer hides a themeless sibling"
    )
    assert 'edition_key=r["edition_key"]' in block, (
        "v1.23.65: the enqueue must stage the specific missing edition"
    )
