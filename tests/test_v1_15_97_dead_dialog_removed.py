"""v1.15.97 — `openItemDialog` + `bindDialog` removed (doubly-dead).

While verifying that v1.15.94's section-id threading on the
CLEAR OVERRIDE button actually activated in production UI, I
discovered the entire dialog flow was unreachable:

* `openItemDialog(mt, tmdb)` early-returned when `$('#item-dlg')`
  returned null. No template (Jinja or otherwise) ever
  declared an element with `id="item-dlg"` or
  `id="item-dlg-body"`. So the function would `return` on
  every invocation before doing any work.

* `bindDialog()` listened for clicks on `[data-clear-override]`
  and `[data-set-override]` form submits. Those data attributes
  were ONLY rendered inside `openItemDialog`'s template — a
  closed loop where the only way to surface a button was to
  already have the dialog rendered. No external code rendered
  those attributes.

So:
1. The DOM never has `#item-dlg`.
2. `openItemDialog` early-returns immediately on every call.
3. The only callers of `openItemDialog` are inside `bindDialog`'s
   click handlers.
4. The click handlers also early-return on missing `#item-dlg`.
5. The data attributes the handlers listen for are only created
   by the function that never executes.

The whole flow is circular dead code. v1.15.97 removes it,
roughly 160 lines.

## What v1.15.94 actually accomplished

The v1.15.94 commit message implied a user-facing fix for
cross-section override deletion. That implication was wrong:
the buggy UI path was unreachable from production. The
server-side API fix (api_clear_override taking an optional
`section_id` query parameter) is still valuable as defensive
API surface — any future caller (CLI, external integration, or
a re-added per-section CLEAR OVERRIDE button) can now
section-scope properly. But no current UI is affected.

The user's `openInfoDialog` (the actual info card surfaced
from the library's ⓘ button) is section-aware by design AND
doesn't expose a CLEAR OVERRIDE button at all. So the bug
class v1.15.94 claimed to fix wasn't user-visible.

## Tests

Static guards that the dead code is gone, plus a tombstone-
comment guard so future "let me re-add an item dialog" PRs
have to confront the v1.15.97 history first.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_open_item_dialog_function_removed():
    """The `openItemDialog` function definition no longer
    exists. A re-add would need to also add `#item-dlg` to a
    template (currently absent)."""
    src = APP_JS.read_text()
    assert "async function openItemDialog" not in src, (
        "v1.15.97 regression: `openItemDialog` returned to app.js. "
        "Pre-v1.15.97 this was doubly-dead (no `#item-dlg` in any "
        "template + no external callers). If it's being re-added as "
        "a real UI feature, also add the `<dialog id=\"item-dlg\">` "
        "element to a template, otherwise the function silently "
        "no-ops on every invocation."
    )


def test_bind_dialog_function_removed():
    """`bindDialog` (the click + submit handler init) is gone.
    Removed alongside `openItemDialog` since its data attributes
    were only rendered by that function."""
    src = APP_JS.read_text()
    assert "function bindDialog" not in src, (
        "v1.15.97 regression: `bindDialog` returned to app.js. "
        "It listened for data attributes (`data-clear-override`, "
        "`data-set-override`) that were only created by "
        "`openItemDialog` — circular dependency. Make sure you "
        "have a non-circular use case before re-adding."
    )


def test_bind_dialog_call_removed_from_init():
    """The `bindDialog()` call inside the init block is gone."""
    src = APP_JS.read_text()
    # Look for an unqualified `bindDialog()` call — the
    # tombstone comment mentioning it is allowed.
    import re
    # Strip line comments.
    no_comments = re.sub(r"//[^\n]*\n", "\n", src)
    assert "bindDialog()" not in no_comments, (
        "v1.15.97 regression: `bindDialog()` is being invoked "
        "in the init block. The function is gone — this call "
        "would be a ReferenceError. Remove."
    )


def test_item_dlg_dom_id_still_not_referenced():
    """Counter-guard against a half-revert. If a future PR re-adds
    `openItemDialog` it should also add the `<dialog id=\"item-dlg\">`
    element to a Jinja template. Until that happens, the dead-flow
    pattern won't return.

    Strip line comments before checking — the v1.15.97 tombstone
    comment block mentions `#item-dlg` as part of the archaeology
    explanation; that's narrative, not a live reference."""
    import re
    src = APP_JS.read_text()
    # Strip `//` line comments + `/* ... */` blocks so the
    # tombstone narrative doesn't trigger a false positive.
    src_no_comments = re.sub(r"//[^\n]*\n", "\n", src)
    src_no_comments = re.sub(r"/\*.*?\*/", "", src_no_comments, flags=re.DOTALL)
    if "#item-dlg" in src_no_comments:
        # Verify the template declares the element.
        templates_dir = REPO / "app" / "web" / "templates"
        found_in_template = False
        for html_file in templates_dir.glob("*.html"):
            if 'id="item-dlg"' in html_file.read_text():
                found_in_template = True
                break
        assert found_in_template, (
            "v1.15.97: JS references `#item-dlg` but no template "
            "declares the element. This is the doubly-dead pattern "
            "v1.15.97 cleaned up — if you're re-adding the dialog "
            "as a real feature, add `<dialog id=\"item-dlg\">` to "
            "a Jinja template too."
        )


def test_v1_15_97_tombstone_comment_present():
    """A future maintainer searching for `openItemDialog` should
    find the v1.15.97 archaeology explaining why it's gone."""
    src = APP_JS.read_text()
    assert "v1.15.97: removed `openItemDialog`" in src, (
        "v1.15.97: the tombstone comment explaining the removal "
        "must remain so future searches surface the history "
        "before re-adding the function."
    )
