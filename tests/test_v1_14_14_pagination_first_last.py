"""v1.14.14 — pagination gains FIRST and LAST shortcuts.

the user: "for the results we can a prev and next can we also have
a first and last to easily go between first page and last".

Pre-fix the library pager had only `« prev` and `next »`. On a
207-page library (his 10K-row movies set with `+P` filter active),
jumping to the last page required 200 next-clicks. v1.14.14 wraps
the existing PREV/NEXT pair with FIRST and LAST shortcuts:

    « first | « prev | page N / total | next » | last »

The new buttons reuse the existing `[data-lib-page]` click handler
(no JS plumbing change) — `« first` sets data-lib-page="1",
`last »` sets data-lib-page="${totalPages}". Disabled state mirrors
PREV (« first off on page 1) and NEXT (» last off on the final
page) so the boundary feedback is consistent across the four
buttons.

Tests pin the four buttons + their data-lib-page targets +
disabled states + that the handler routing is unchanged.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Pager renders all four buttons ────────────────────────────


def test_pager_renders_first_button():
    """The pager template must include a `« first` button with
    data-lib-page="1" so clicking jumps to page 1."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Template literal at the pager innerHTML site.
    pager_anchor = js.index("getElementById('library-pager').innerHTML")
    block = js[pager_anchor:pager_anchor + 1000]
    assert '<button data-lib-page="1"' in block
    assert ">« first</button>" in block


def test_pager_renders_last_button():
    """The pager template must include a `last »` button with
    data-lib-page="${totalPages}" so clicking jumps to the
    last page (whatever the current total is)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    pager_anchor = js.index("getElementById('library-pager').innerHTML")
    block = js[pager_anchor:pager_anchor + 1000]
    assert '<button data-lib-page="${totalPages}"' in block
    assert ">last »</button>" in block


def test_pager_keeps_existing_prev_next_buttons():
    """v1.14.14 wraps PREV/NEXT — it doesn't replace them. Pin
    that the existing buttons survive the change."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    pager_anchor = js.index("getElementById('library-pager').innerHTML")
    block = js[pager_anchor:pager_anchor + 1000]
    assert '<button data-lib-page="${libraryState.page - 1}"' in block
    assert ">« prev</button>" in block
    assert '<button data-lib-page="${libraryState.page + 1}"' in block
    assert ">next »</button>" in block


def test_pager_keeps_page_n_of_total_label():
    """The current `page N / total` label must survive — it's
    the orientation cue for which page the user is on."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    pager_anchor = js.index("getElementById('library-pager').innerHTML")
    block = js[pager_anchor:pager_anchor + 1000]
    assert "<span>page ${libraryState.page} / ${totalPages}</span>" in block


# ── Disabled state mirrors PREV/NEXT ──────────────────────────


def test_first_and_prev_disabled_together_on_page_1():
    """When the user is on page 1, both `« first` and `« prev`
    should be disabled — both jump to a page that doesn't
    exist (page 0 / page 1 itself)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The shared boundary check.
    assert "const onFirst = libraryState.page <= 1;" in js
    pager_anchor = js.index("getElementById('library-pager').innerHTML")
    block = js[pager_anchor:pager_anchor + 1000]
    # « first uses onFirst.
    assert '<button data-lib-page="1" ${onFirst ? \'disabled\' : \'\'}' in block
    # « prev also uses onFirst (same condition).
    assert (
        '<button data-lib-page="${libraryState.page - 1}" '
        '${onFirst ? \'disabled\' : \'\'}'
    ) in block


def test_last_and_next_disabled_together_on_final_page():
    """When the user is on the final page, both `next »` and
    `last »` should be disabled. Same condition for both."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const onLast = libraryState.page >= totalPages;" in js
    pager_anchor = js.index("getElementById('library-pager').innerHTML")
    block = js[pager_anchor:pager_anchor + 1000]
    # next » uses onLast.
    assert (
        '<button data-lib-page="${libraryState.page + 1}" '
        '${onLast ? \'disabled\' : \'\'}'
    ) in block
    # last » also uses onLast.
    assert (
        '<button data-lib-page="${totalPages}" '
        '${onLast ? \'disabled\' : \'\'}'
    ) in block


# ── Click handler still works (uses existing data-lib-page route) ──


def test_pager_click_handler_unchanged():
    """The four buttons all use `data-lib-page` — the existing
    delegate handler at app.js:6997 reads `b.dataset.libPage`
    and sets `libraryState.page = Number(...)`. Pin so a
    refactor doesn't accidentally narrow the selector to
    only PREV/NEXT."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-pager')?.addEventListener")
    block = js[handler_anchor:handler_anchor + 500]
    # The selector is still the generic data-lib-page attr.
    assert "button[data-lib-page]" in block
    # Reading the page from dataset — no special-casing for first/last.
    assert "libraryState.page = Number(b.dataset.libPage);" in block


# ── Visual order: first | prev | label | next | last ──────────


def test_pager_button_order_is_first_prev_label_next_last():
    """Read order matters — the user scans left-to-right.
    Layout: « first  « prev  page N / total  next »  last »"""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    pager_anchor = js.index("getElementById('library-pager').innerHTML")
    block = js[pager_anchor:pager_anchor + 1000]
    # Use index-of to verify ordering.
    pos_first = block.index('"« first</button>".replace("\\"","")'.replace('"« first</button>".replace("\\"","")', ">« first</button>"))
    pos_prev = block.index(">« prev</button>")
    pos_label = block.index("<span>page")
    pos_next = block.index(">next »</button>")
    pos_last = block.index(">last »</button>")
    assert pos_first < pos_prev < pos_label < pos_next < pos_last
