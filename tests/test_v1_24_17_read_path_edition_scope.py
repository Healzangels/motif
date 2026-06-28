"""v1.24.17 — edition-sweep read-path bleeds (all read-only display/coverage).

#3 /api/sections/coverage: the per-section AND collections plex_items->placements
   LEFT JOINs joined on (media_type, tmdb_id, section_id) but NOT edition_key, so
   a title with two PLACED editions in one section row-multiplied → non-DISTINCT
   COUNT(pi.rating_key) + the themed SUM double-counted. Added
   `p.edition_key = pi.edition_key`.
#4 api_recovery_options' INFO-card `local` query was section-only (LIMIT 1) → an
   arbitrary sibling edition's source_kind drove "RESOLVED VIA {source}" / the
   resolved flag for the CLICKED edition. Now scopes to the clicked rating_key's
   edition (edition_key IN (?, '') ORDER BY (edition_key = ?) DESC), mirroring the
   audio endpoint (v1.21.90) / api_item (v1.21.68).
#5 /api/coverage/plex `placed` EXISTS (movies/tv/anime/collections) matched any
   sibling's placement → a placed sibling marked EVERY edition placed, hiding a
   genuinely-unthemed edition from the dashboard "ready to add" set. Added
   edition_key to all four EXISTS subqueries.

Source guards (the fixes are read-only SQL scoping; the broader endpoint
behavior is exercised by the coverage tests in the full suite).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ── #3 coverage JOINs + #5 placed EXISTS all carry edition_key ──

def test_edition_scoped_placement_reads_present():
    # 2 coverage JOINs (#3 per-section + collections) + 4 placed EXISTS (#5)
    # = 6 `p.edition_key = pi.edition_key` clauses on placement reads.
    assert API_PY.count("AND p.edition_key = pi.edition_key") >= 6


def test_placed_exists_is_edition_scoped():
    # the EXISTS no longer closes on bare section_id — every one carries the
    # edition predicate before `) AS placed`.
    assert "AND p.section_id = pi.section_id) AS placed" not in API_PY
    assert API_PY.count(
        "AND p.edition_key = pi.edition_key) AS placed") == 4


# ── #4 recovery-options scopes to the clicked edition ──────────

def test_recovery_options_local_query_edition_scoped():
    assert "_rec_edition = (edition_key_for_rating_key(conn, rating_key)" in API_PY
    # the scoped local query: prefer the edition's own row, fall back to ''.
    i = API_PY.index("_rec_edition = (edition_key_for_rating_key")
    body = API_PY[i:i + 800]
    assert "AND edition_key IN (?, '') " in body
    assert "ORDER BY (edition_key = ?) DESC LIMIT 1" in body
