"""v0.51.326 — page navigation at the bottom of the library table.

The operator, at the bottom of a 50-row anime page: "can we add page navigation
to the bottom of the page so that when we get to the bottom we can go to the
next page". A .block-foot strip under the table carries a second pager that
mirrors the header's markup (one template, one state, two places); the shared
click handler scrolls the results head back into view after a footer click so
the new page is read from its first row.
"""
from __future__ import annotations

from pathlib import Path

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
LIBRARY = (REPO / "app" / "web" / "templates" / "library.html").read_text()


def _rule(sel: str) -> str:
    return slice_between(APP_CSS, sel + " {", "}")


def test_footer_pager_sits_under_the_table_inside_the_results_section():
    i_table_end = LIBRARY.index("</table>\n  </div>{# /.table-scroll #}")
    i_foot = LIBRARY.index('<footer class="block-foot">')
    i_section_end = LIBRARY.index("</section>", i_table_end)
    assert i_table_end < i_foot < i_section_end
    assert '<div class="pager" id="library-pager-foot"></div>' in LIBRARY[i_foot:i_section_end]
    assert LIBRARY.count('id="library-pager"') == 1 and LIBRARY.count('id="library-pager-foot"') == 1


def test_block_foot_is_the_block_head_mirror_on_tokens():
    foot = _rule(".block-foot")
    head = _rule(".block-head")
    assert "border-top: 1px solid var(--line)" in foot
    assert "background: var(--bg-elev-2)" in foot and "background: var(--bg-elev-2)" in head
    assert "padding: 12px 18px" in foot and "padding: 12px 18px" in head, "same strip metrics as the head"
    assert "justify-content: flex-end" in foot, "the pager sits right, where the header's lives"
    assert "#" not in foot.replace("var(--", ""), "tokens only"
    mobile = APP_CSS[APP_CSS.index("  .pager { flex-wrap: wrap; justify-content: center; }"):]
    assert "  .block-foot { justify-content: center; }" in mobile[:200], "centred on a phone like the head's"


def test_footer_mirrors_the_header_pager_markup():
    render = slice_between(APP_JS, "document.getElementById('library-pager').innerHTML = `", "\n  }\n")
    assert "const _pagerFoot = document.getElementById('library-pager-foot');" in render
    assert "_pagerFoot.innerHTML = document.getElementById('library-pager').innerHTML;" in render, (
        "one template, one state, two places — no second copy of the buttons")
    assert APP_JS.count("data-lib-page=\"1\"") == 1, "the button markup exists once"


def test_one_handler_on_both_pagers_and_a_footer_click_scrolls_back_up():
    assert "document.getElementById('library-pager')?.addEventListener('click', _onLibraryPagerClick);" in APP_JS
    assert "document.getElementById('library-pager-foot')?.addEventListener('click', _onLibraryPagerClick);" in APP_JS
    fn = slice_between(APP_JS, "function _onLibraryPagerClick(e) {", "\n    }\n")
    assert "libraryState.page = Number(b.dataset.libPage);" in fn
    assert "const fromFoot = !!b.closest('#library-pager-foot');" in fn
    assert "scrollIntoView({ block: 'start' })" in fn and "if (fromFoot)" in fn
    assert fn.index("loadLibrary().then(") < fn.index("scrollIntoView"), "after the new page has painted"
    assert ".catch(console.error)" in fn


def test_v0_51_326_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.326: page navigation at the bottom of the library table" in init_py
