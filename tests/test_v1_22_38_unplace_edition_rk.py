"""v1.22.38 (holistic audit) — per-row DEL + bulk ADOPT+LPS thread rating_key.

Multi-edition data loss: api_unplace_item scopes to one edition only when the
UI sends rating_key; "absent rk = legacy section-wide fan-out" physically
unlinks EVERY edition's theme.mp3 in the section. The per-row DEL handler
(act==='unplace' → unplaceTheme) and the bulk ADOPT+LET-PLEX-SERVE loop both
sent section_id only, so DEL/bulk-LPS on one edition of a multi-edition title
nuked its siblings' placements. Both now thread rating_key. Same class as the
v1.21.61/.93 LET-PLEX-SERVE fixes — these two sites were missed.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_unplacetheme_takes_rating_key_param():
    assert ("async function unplaceTheme(mediaType, tmdbId, title, sectionId, "
            "isPlexUpload, ratingKey)" in APP_JS), (
        "v1.22.38: unplaceTheme must accept a ratingKey param")
    # The URL build appends rating_key when present.
    i = APP_JS.index("async function unplaceTheme(")
    body = APP_JS[i:i + 2400]
    assert "_uParams.set('rating_key', ratingKey)" in body, (
        "v1.22.38: unplaceTheme must scope the /unplace to the row's edition")


def test_del_action_passes_rk_to_unplacetheme():
    i = APP_JS.index("act === 'unplace'")
    block = APP_JS[i:i + 320]
    assert "btn.dataset.rk || undefined" in block, (
        "v1.22.38: the DEL/unplace action must forward the row's rating_key")


def test_bulk_adopt_lps_unplace_threads_rk():
    i = APP_JS.index("Step 2: unplace")
    block = APP_JS[i:i + 900]
    assert "_bParams.set('rating_key', it.rating_key)" in block, (
        "v1.22.38: bulk ADOPT+LPS unplace must scope to each row's edition")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
