"""v1.22.83 (audit round 2, Batch C #12 — FINAL Batch C item) — the
dashboard pie + UPD-pill stats queries were edition-blind.

_stats_sync's src_rows (the source pie), update_tab_row, and
update_tab_breakdown_rows all joined placements/local_files on the
bare (media_type, tmdb_id, section_id) — but both tables key editions
into their PKs (post-C2 a title legitimately holds '' + 'Extended'
sibling rows in one section). Each pi edition-row matched EVERY
sibling row: COUNT(*) double-counted in renderThemeSourcePie, the
SRC letter classified off an arbitrary sibling, and the UPD cycle
counts inflated vs the (EXISTS-based, correct) badge — the exact
sort-vs-pill mirror-drift class this project keeps re-fixing.

All three now use the v1.21.59 two-tier shape the library query was
rebuilt around: p_e/p_g + lf_e/lf_g (edition-exact preferred, ''
fallback) with _LIB_SRC_LETTER_SQL reading the COALESCE columns —
so the pie/pill counts agree with the row pills by construction.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _query_block(anchor: str) -> str:
    i = API_PY.index(anchor)
    # v1.23.87: widened 5000→6500 — the edition '' fallback gate added ~11 lines
    # per p_g/lf_g join, pushing the asserted COALESCE past the old window.
    return API_PY[i:i + 6500]


def test_all_three_stats_queries_use_two_tier_joins():
    for anchor in ("update_tab_row = conn.execute(",
                   "update_tab_breakdown_rows = conn.execute",
                   "src_rows = conn.execute("):
        block = _query_block(anchor)
        assert "LEFT JOIN placements p_e" in block, anchor
        assert "p_g.edition_key = ''" in block, anchor
        assert "LEFT JOIN local_files lf_e" in block, anchor
        assert "lf_g.edition_key = ''" in block, anchor


def test_src_pie_uses_lib_src_letter():
    block = _query_block("src_rows = conn.execute(")
    assert "{_LIB_SRC_LETTER_SQL} AS letter" in block
    # The edition-blind single-alias joins must be gone from all
    # three stats queries (the only remaining bare `p.`/`lf.` join
    # shape on (mt, tmdb, section) in _stats_sync would re-multiply).
    for anchor in ("update_tab_row = conn.execute(",
                   "update_tab_breakdown_rows = conn.execute",
                   "src_rows = conn.execute("):
        b = _query_block(anchor)
        assert "LEFT JOIN placements p\n" not in b, anchor
        assert "LEFT JOIN local_files lf\n" not in b, anchor


def test_has_something_reads_coalesced_placement():
    for anchor in ("update_tab_row = conn.execute(",
                   "update_tab_breakdown_rows = conn.execute"):
        block = _query_block(anchor)
        assert ("COALESCE(p_e.media_folder, p_g.media_folder) "
                "IS NOT NULL") in block, anchor
