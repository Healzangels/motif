"""v1.22.81 (audit round 2, Batch C #10) — orphan scan edition-blind
keying.

placements gained edition_key in its PK (v1.21.51) PRECISELY to
disambiguate plex_upload rows, yet the scan enumerated placements
without it and keyed findings on (mt, tmdb, section): a two-edition
plex_upload title produced identical-keyed findings — // PROBE patched
whichever matched first, the per-item endpoint's exists/cache-patch hit
the wrong row, and // RE-PUSH (whose data-rk read was DEAD — nothing
wrote the attribute) fired the legacy un-scoped /replace that re-placed
EVERY edition (the wrong-edition class v1.21.69 fixed for the library
page).

Now: the enumeration carries edition_key; scan_one_placement accepts it
(scopes the rk + local_files lookups, stamps the finding identity); the
per-item endpoint accepts ?edition_key= for exists + cache patch/drop;
orphans.html threads data-ek through every action button + reprobeRow +
matches(), and RE-PUSH finally carries data-rk → ?rating_key=.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

from test_v1_14_59_recovery_options_behavioral import (  # noqa: F401
    _seed_plex_item, _seed_section, _seed_theme)

from app.core.db import init_db
from app.core.orphan_scan import scan_plex_upload_placements

REPO = Path(__file__).resolve().parent.parent
ORPHANS_HTML = (REPO / "app" / "web" / "templates" /
                "orphans.html").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
TS = "2026-06-11T00:00:00+00:00"


def test_two_edition_placements_yield_distinct_findings(tmp_path):
    """Behavioral: each edition's finding carries ITS edition + ITS rk.
    Pre-fix both findings resolved the same arbitrary plex_items row
    and were identical-keyed."""
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=120, title="LotR")
        tid_pk = conn.execute(
            "SELECT id FROM themes WHERE tmdb_id = 120").fetchone()[0]
        for ek, rk in (("", "rk-std"), ("extended", "rk-ext")):
            _seed_plex_item(conn, rating_key=rk, section_id="1",
                            tmdb_id=120)
            conn.execute(
                "UPDATE plex_items SET theme_id = ?, edition_key = ? "
                "WHERE rating_key = ?", (tid_pk, ek, rk))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id,"
                " section_id, media_folder, edition_key, placed_at,"
                " placement_kind)"
                " VALUES ('movie', 120, '1', '', ?, ?, 'plex_upload')",
                (ek, TS))
        conn.commit()
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True,
        "body": {"MediaContainer": {"size": 0, "Metadata": []}},
    }
    findings = scan_plex_upload_placements(db, plex)
    assert len(findings) == 2
    by_ek = {f["edition_key"]: f for f in findings}
    assert set(by_ek) == {"", "extended"}
    assert by_ek[""]["rk"] == "rk-std"
    assert by_ek["extended"]["rk"] == "rk-ext", (
        "v1.22.81: each edition's finding must resolve ITS OWN rk"
    )


def test_per_item_endpoint_accepts_and_matches_edition():
    i = API_PY.index("async def api_admin_orphan_scan_item(")
    body = API_PY[i:API_PY.index("\n    @app.", i)]
    assert "edition_key: str | None = Query(None)" in body
    assert body.count("edition is identity") == 2, (
        "both the cache patch and the placement_gone drop must key on "
        "edition"
    )
    assert "edition_key=edition_key," in body


def test_orphans_page_threads_edition_and_rk():
    assert "await reprobeRow(mt, id, sec, ek)" in ORPHANS_HTML
    assert "const ek = btn.dataset.ek;" in ORPHANS_HTML
    assert 'data-rk="${htmlEscape(f.rk || \'\')}"' in ORPHANS_HTML, (
        "RE-PUSH's data-rk read was dead — the attribute must be written"
    )
    assert "?rating_key=${encodeURIComponent(rk)}" in ORPHANS_HTML, (
        "RE-PUSH must scope /replace to the row's edition (v1.21.69)"
    )
    # matches() compares edition when the caller provides one.
    assert "String(f.edition_key || '') === String(ek || '')" \
        in ORPHANS_HTML
