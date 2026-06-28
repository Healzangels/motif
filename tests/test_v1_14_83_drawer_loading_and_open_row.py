"""v1.14.83 — drawer "// loading…" stuck fix + OPEN ROW button now works on /queue.

the user: "the live ops drawer is not loading even when idle.
Also attempted the new reprobe, I see the following in the
logs reprobe from the logs seems to work but open row doesn't
do anything."

## Two bugs

### Bug 1: drawer stuck on "// loading…"

v1.14.81's renderDrawerBody refactor used `tmp.firstChild` to
extract the new card element from a parsed HTML string. But
renderCard returns a template literal that starts with a
newline + indentation:

    return `
      <div class="op-card …" data-op-id="…">
        …
      </div>`;

When that string is set as innerHTML, the browser parses it
into a TEXT NODE (the whitespace) followed by the `<div>`.
`tmp.firstChild` returns the text node, not the div. The
next line `newEl.dataset.cardKey = …` throws TypeError
(text nodes don't have a dataset property), the forEach loop
bails, and `body.replaceChildren(frag)` runs with whatever's
in the partial fragment. The "// loading…" template div is
replaced with nothing → drawer appears stuck.

Fix: use `tmp.firstElementChild` (skips text/comment nodes
when picking the card element).

### Bug 2: OPEN ROW button does nothing on /queue

`openInfoDialog` early-returns when `#info-dlg` isn't in the
DOM. The dialog template was defined inside library.html, so
clicking OPEN ROW on /queue (which doesn't extend library.html)
silently no-op'd.

Fix: move the `<dialog id="info-dlg">` from library.html to
base.html so it's available on every page that has the LIVE
OPS drawer.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"


# ── Bug 1: firstElementChild fix in renderDrawerBody ──────────


def test_render_drawer_body_uses_firstElementChild_not_firstChild():
    """The diff loop must use tmp.firstElementChild so leading
    whitespace text nodes (renderCard's template-literal output
    starts with `\\n      <div>`) don't get picked as the card
    element — text nodes have no dataset property and the
    next .dataset.cardKey assignment would TypeError, silently
    bailing the whole forEach."""
    src = OPS_JS.read_text()
    # Anchor on the diff-loop forEach.
    fn_start = src.index("function renderDrawerBody(ops) {")
    fn_body = src[fn_start:fn_start + 7500]
    # The fix uses firstElementChild.
    assert "tmp.firstElementChild" in fn_body, (
        "diff loop must use tmp.firstElementChild — not "
        "tmp.firstChild — so leading whitespace text nodes "
        "from renderCard's template literal don't break the "
        "card extraction."
    )
    # The pre-fix tmp.firstChild form must NOT survive in
    # this function body.
    assert "tmp.firstChild" not in fn_body, (
        "pre-fix tmp.firstChild reference snuck back in — "
        "would re-introduce the silent bail bug the user saw "
        "as a stuck '// loading…' drawer."
    )


def test_v1_14_83_marker_explains_the_firstElementChild_choice():
    """A v1.14.83 marker on the firstElementChild line documents
    why the diff is safer than firstChild — so a future
    'simplify' refactor sees the rationale.

    Slice widened in v1.14.90 — the in-place updater pushed the
    fragment-rebuild path (where the firstElementChild guard
    lives) further down in renderDrawerBody."""
    src = OPS_JS.read_text()
    fn_start = src.index("function renderDrawerBody(ops) {")
    fn_body = src[fn_start:fn_start + 12000]
    assert "v1.14.83: use firstElementChild" in fn_body


# ── Bug 2: info-dlg promoted to base.html ─────────────────────


def test_info_dlg_template_in_base_html():
    """The info-dlg dialog must live in base.html so every page
    (including /queue) can trigger openInfoDialog. The pre-fix
    library.html-local definition meant the OPEN ROW buttons
    on /queue's REPROBE error events silently no-op'd."""
    base = BASE_HTML.read_text()
    assert 'id="info-dlg"' in base
    # Closer button id is what bindInfoDialog wires.
    assert 'id="info-dlg-close"' in base
    # Body is what openInfoDialog populates.
    assert 'id="info-dlg-body"' in base


def test_info_dlg_template_not_in_library_html():
    """The library.html-local definition must be retired —
    otherwise we'd have two #info-dlg elements in the DOM on
    library pages and openInfoDialog's getElementById would
    pick whichever the browser sees first (undefined which)."""
    html = LIBRARY_HTML.read_text()
    assert '<dialog class="dlg" id="info-dlg">' not in html
    # The v1.14.83 marker explaining the move stays in
    # library.html so future readers grepping for info-dlg in
    # the original location see why it's gone.
    assert "v1.14.83: info-dlg promoted to base.html" in html


def test_info_dlg_promotion_marker_in_base_html():
    """The v1.14.83 marker on the base.html dialog block
    explains the promotion rationale + the OPEN ROW dependency."""
    base = BASE_HTML.read_text()
    assert "v1.14.83: info-dlg promoted from library.html" in base


def test_open_info_dialog_function_unchanged():
    """openInfoDialog still finds #info-dlg via getElementById
    — the lookup contract is the same, just the dialog now
    lives in base.html. Sanity check that nothing in the
    function changed."""
    src = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_start = src.index("async function openInfoDialog(")
    fn_body = src[fn_start:fn_start + 500]
    assert "document.getElementById('info-dlg')" in fn_body


def test_bind_info_dialog_unconditional_init():
    """bindInfoDialog must continue running on every page (not
    gated on a library-only check) so the close button click
    handler is attached on /queue too. The function is called
    from the global init block at the bottom of app.js."""
    src = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The init call is unconditional (no path-based guard).
    init_idx = src.index("bindInfoDialog();")
    # Look at the surrounding context — must NOT be inside
    # an if-block that gates on path.
    context = src[max(0, init_idx - 500):init_idx]
    # No "path === '/queue'" or similar gate immediately
    # before the call.
    pattern = re.compile(r"if\s*\(\s*path\s*===\s*['\"]\S+['\"]\s*\)\s*\{[^}]*bindInfoDialog")
    assert not pattern.search(context), (
        "bindInfoDialog must be called unconditionally — "
        "gating on path would break OPEN ROW on /queue."
    )
