"""v1.22.2 — LET PLEX SERVE must carry the clicked row's rating_key.

THE bug (the user's Watchmen, ~5 tags of mis-diagnosis): LET PLEX SERVE on the
Midnight edition did nothing — every unplace returned placements_total:0. The
backend was correct (an in-container test proved edition_key_for_rating_key(
'417813')='midnight' and the worklist matched the midnight hardlink placement).
The bug was the JS: the `purge-revert-to-plex` SOURCE-menu item was built
WITHOUT `rk: it.rating_key` (its adopt-and-let-plex-serve sibling had it), so
letPlexServeFlow fell back to an ambiguous (mt,id,section) `.find()` that
returned the FIRST of the 4 Watchmen editions — a sibling with no placement →
0 unlinked → silent no-op.

These are source-text guards (the app.js JS-test convention — no node/DOM in
CI; the quickjs parse guard lives in test_v1_21_91). They pin the rating_key
threading end-to-end: menu item → flow → unplace URL.
"""
from __future__ import annotations
from _slice_helpers import slice_to_next

from pathlib import Path


APP_JS = (Path(__file__).resolve().parent.parent
          / "app" / "web" / "static" / "app.js").read_text()


def test_lps_menu_item_carries_rating_key():
    """The LET PLEX SERVE menu item must pass the row's rating_key so the
    unplace scopes to THIS edition (the exact omission that caused the bug)."""
    idx = APP_JS.index("'purge-revert-to-plex', 'LET PLEX SERVE'")
    # The dataObj literal follows within the menuItemHtml(...) call.
    block = APP_JS[idx:idx + 900]
    assert "rk: it.rating_key" in block, (
        "LET PLEX SERVE menu item must carry rk: it.rating_key — without it "
        "the unplace resolves the wrong edition on multi-edition titles")


def test_lps_flow_resolves_by_clicked_rating_key():
    """letPlexServeFlow must take a ratingKey param and resolve the row by it
    (not the ambiguous mt/id/section find that returns the first edition)."""
    sig = APP_JS.index("async function letPlexServeFlow(")
    sig_line = APP_JS[sig:APP_JS.index(")", sig) + 1]
    assert "ratingKey" in sig_line, (
        "letPlexServeFlow must accept the clicked ratingKey; signature was: "
        + sig_line)
    # v0.51.57: widened 2500→3100 — the PU-vs-sidecar removalLine branch (+
    # comment) landed ahead of the unplace-URL rk resolution, pushing it out.
    body = slice_to_next(APP_JS, "async function letPlexServeFlow(",
                        "\n  function ", "\n  async function ")
    # Row resolution prefers an exact rating_key match.
    assert "String(row.rating_key) === String(ratingKey)" in body
    # The unplace URL threads the clicked rk (authoritative).
    assert "ratingKey || (lpsRow && lpsRow.rating_key)" in body


def test_lps_call_site_passes_dataset_rk():
    """The purge-revert-to-plex dispatch must forward btn.dataset.rk."""
    idx = APP_JS.index("letPlexServeFlow(btn.dataset.mt")
    call = APP_JS[idx:idx + 200]
    assert "btn.dataset.rk" in call, (
        "the LET PLEX SERVE dispatcher must forward btn.dataset.rk to the flow")


def test_adopt_lps_unplace_is_edition_scoped():
    """ADOPT + LET PLEX SERVE already holds ratingKey (for adopt-sidecar) — its
    unplace URL must use it too, else the section-only unplace nukes EVERY
    edition's placement (the same class as the LPS rk omission)."""
    fn = APP_JS.index("async function adoptAndLetPlexServeFlow(")
    body = APP_JS[fn:fn + 2200]
    # The unplace URL must include a rating_key param built from ratingKey.
    assert "rating_key=${encodeURIComponent(ratingKey)}" in body, (
        "adopt+LPS unplace must be rating_key-scoped to avoid cross-edition "
        "placement deletion on multi-edition titles")


def test_v1_22_2_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
