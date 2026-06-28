"""v1.24.7 — api_unplace_item's restore loop crashed on plex_upload LPS/UNPLACE.

Edition audit finding (HIGH, latent crash). The v1.22.76 per-edition local_files
fix added `_pr_edition = pr["edition_key"]` in the LET PLEX SERVE / UNPLACE restore
loop, but the THREE placement worklist SELECTs (section+edition / section / title)
only fetched `media_folder, placement_kind, section_id` — never `edition_key`. With
`conn.row_factory = sqlite3.Row`, `pr["edition_key"]` raises IndexError, so the whole
Plex-API restore loop threw on any plex_upload LPS/UNPLACE with Plex enabled (the
normal prod config). The fix-read was dead on arrival; no test exercised the
plex-enabled restore loop, so it slipped the v1.22.76 source pins (which guarded the
READ but never checked the SELECT supplied the column).

This pins BOTH sides of the contract.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _unplace_body() -> str:
    i = API_PY.index("async def api_unplace_item(")
    return API_PY[i:API_PY.index("\n    @app.", i + 10)]


def test_unplace_worklist_selects_supply_edition_key():
    body = _unplace_body()
    # the restore loop reads pr["edition_key"] (v1.22.76)…
    assert 'pr["edition_key"]' in body, "the per-edition restore read must exist"
    # …so EVERY placement worklist SELECT that feeds it must fetch the column,
    # or it's an IndexError on the sqlite3.Row. (no-comma form = missing column.)
    total = body.count("SELECT media_folder, placement_kind, section_id")
    with_edition = body.count("SELECT media_folder, placement_kind, section_id, ")
    assert total == 3, f"expected 3 unplace worklist SELECTs, found {total}"
    assert with_edition == total, (
        f"all {total} unplace worklist SELECTs must include edition_key "
        f"(only {with_edition} do) — pr['edition_key'] IndexErrors otherwise")
