"""v0.51.244 — re-opening the card you're already on updates in place.

Reported: "after clicking remeasure in the info card it looks like the info card
reloads."

It did. // RE-MEASURE, // LEVEL, // UNDO and the cut picker all call
openInfoDialog again on the same row — deliberately, because the target stepper
captures loudness_i at render, so patching the text inline would leave its gain
math stale. The re-FETCH is right. Blanking the card to the loader was not:
`article.dlg-body` is the scroll container and `#info-dlg-body` its only child,
so the loader collapses scrollHeight and the browser clamps scrollTop to 0.

MEASURED in a browser against the real app.css, at the 720px drawer:

    clicked with the LOUDNESS block on screen : scrollTop 3423
    after the loader blank + re-render        : ~0, block ~660px below the fold

So you read "now -19.7 LUFS", then the card visibly rebuilt itself and threw you
to the top. Post-fix the same sequence holds 3423 and the block stays visible,
while a different row, a different EDITION of the same row, and a close-then-
reopen all still get the loader.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _open_info_dialog_src() -> str:
    """Slice by function-name anchor, never a fixed byte window (the repo's
    +N-slice treadmill)."""
    i = APP_JS.index("async function openInfoDialog")
    m = re.search(r"\n  (?:async )?function ", APP_JS[i + 10:])
    return APP_JS[i:i + 10 + m.start()]


def test_the_loader_blank_is_conditional():
    """THE fix. An unconditional blank is what collapsed the scroller."""
    src = _open_info_dialog_src()
    assert "if (!_sameCard) body.innerHTML = recordLoaderHtml" in src, (
        "re-opening the card already on screen must not blank it to the loader")
    assert not re.search(r"^\s*body\.innerHTML = recordLoaderHtml", src, re.M), (
        "an unconditional loader write reintroduces the scroll reset")


def test_scroll_is_captured_before_and_restored_after():
    src = _open_info_dialog_src()
    assert "_keepScroll = _sameCard && _scroller ? _scroller.scrollTop : null" in src
    assert "if (_keepScroll !== null && _scroller) _scroller.scrollTop = _keepScroll" in src, (
        "capturing the scroll without restoring it fixes nothing")


def test_same_card_requires_the_dialog_to_be_open():
    """A closed dialog must reload: `dlg.open` is false after closeInfoDialog's
    280ms slide-out, so close-then-reopen is a fresh card, not an in-place one."""
    src = _open_info_dialog_src()
    assert "_sameCard = dlg.open && dlg.dataset.cardKey === _cardKey" in src


def test_the_card_key_is_the_fetch_url_not_a_hand_rolled_tuple():
    """Identity has to include rating_key. Two editions of one title in one
    section share media_type/tmdb_id/section_id/edition_key and differ only by
    rk — a tuple that omitted it would call them the same card and skip the
    loader while the data underneath changed. The fetch URL can't drift from
    what was actually requested, because it IS what was requested."""
    src = _open_info_dialog_src()
    assert "_cardKey = _infoUrl(mediaType, tmdbId, sectionId, ratingKey, editionKey)" in src
    assert "_infoFetch(_infoUrl(mediaType, tmdbId, sectionId, ratingKey, editionKey))" in src, (
        "the key and the fetch must be built from the same call, or they drift")


def test_the_key_is_stamped_only_after_a_successful_render():
    """A failed fetch returns early. Stamping the key before that would leave the
    dialog claiming to BE a card it never rendered, so the next open would skip
    the loader over stale content."""
    src = _open_info_dialog_src()
    stamp = src.index("dlg.dataset.cardKey = _cardKey")
    render = src.index("body.innerHTML = `")
    assert stamp > render, "the key must be stamped after the render, not before"
    for err in re.finditer(r"body\.innerHTML = `<p class=\"accent-red\">", src):
        assert err.start() < stamp, "error paths must return before the key is stamped"


def test_the_scroll_container_is_still_the_element_we_restore():
    """The fix reads .dlg-body, so the template must keep #info-dlg-body nested
    inside it. Flattening them would silently make the restore a no-op."""
    base = (REPO / "app" / "web" / "templates" / "base.html").read_text()
    i = base.index('id="info-dlg"')
    block = base[i:base.index("</dialog>", i)]
    assert '<article class="dlg-body">' in block
    assert block.index('class="dlg-body"') < block.index('id="info-dlg-body"'), (
        "#info-dlg-body must sit INSIDE the .dlg-body scroller")
    assert ".dlg.dlg-drawer-left .dlg-body {" in (
        REPO / "app" / "web" / "static" / "app.css").read_text()


def test_no_nul_bytes_in_app_js():
    """A stray NUL slipped into this very edit (`.join('\\x00')`). node --check
    accepts it inside a string literal, so nothing else would have caught it."""
    raw = (REPO / "app" / "web" / "static" / "app.js").read_bytes()
    assert b"\x00" not in raw, "NUL byte in app.js"
