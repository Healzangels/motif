"""v1.15.140 — manual URL dialog preview dedup must respect
visibility.

the user on v1.15.139 (manual theme URL dialog):

> closing and reopening the url prevents the thumbnail from
> loading again.

## Root cause

`bindManualUrlDialog()` keeps `previewLastKey` as a closure-
scoped variable initialized at page load. It's used to dedup
`updatePreview()` so typing or pasting the same URL twice
doesn't re-hit oembed:

    if (key === previewLastKey) return;
    previewLastKey = key;

`openManualUrlDialog()` correctly resets the visible state of
the dialog on each open — it clears the input, hides the
preview block, etc. But it can't touch `previewLastKey` (private
to the binding closure), so the dedup variable survives across
opens.

Repro path:

  1. Open SET URL dialog on a row, paste a YouTube/SoundCloud
     URL. `updatePreview` runs, `previewLastKey = 'yt:VID'`,
     preview shows.
  2. Close dialog (× or Esc or Cancel). `previewLastKey` is
     still `'yt:VID'`.
  3. Reopen the dialog. `openManualUrlDialog` hides the preview
     block (`preview.hidden = true`) but `previewLastKey` is
     unchanged.
  4. Paste the SAME URL. `updatePreview` computes the same key,
     hits `key === previewLastKey` → returns early. The preview
     stays hidden.

## Fix — `app/web/static/app.js`

Defensive check: the dedup short-circuit must also require the
preview to be currently visible. If the preview is hidden, the
dedup target (the visible render) doesn't exist, so the next
identical-key paste must re-render.

    if (key === previewLastKey && !preview.hidden) return;

Anything that hides the preview (openManualUrlDialog reset,
urlSource → unknown transition emptying the field, etc.) frees
the next matching-key paste to re-render. The same-URL paste
while the dialog stays open still dedups normally.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _read_js() -> str:
    return APP_JS.read_text()


def _update_preview_body() -> str:
    """Slice out the `updatePreview` function body inside
    `bindManualUrlDialog`. The function is locally scoped, so we
    anchor on its declaration and walk forward to its closing
    brace (matched via a brace-counter)."""
    js = _read_js()
    start = js.index("    async function updatePreview() {")
    # Brace-counter walk.
    i = js.index("{", start)
    depth = 1
    j = i + 1
    while j < len(js) and depth > 0:
        c = js[j]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
        j += 1
    return js[start:j]


def test_dedup_check_requires_preview_visible():
    """The short-circuit must include `!preview.hidden` — otherwise
    a stale previewLastKey from a previous dialog open blocks the
    re-render on reopen."""
    body = _update_preview_body()
    assert "if (key === previewLastKey && !preview.hidden) return;" in body, (
        "v1.15.140: updatePreview's dedup short-circuit must check "
        "`key === previewLastKey && !preview.hidden` so a closed-"
        "and-reopened dialog with the same URL re-renders the "
        "thumbnail instead of getting skipped."
    )


def test_old_unconditional_dedup_is_gone():
    """The pre-fix unconditional `if (key === previewLastKey) return;`
    must not survive — re-adding it would resurrect the bug."""
    body = _update_preview_body()
    # The exact pre-fix line (without the visibility check).
    forbidden = re.compile(
        r"if\s*\(\s*key\s*===\s*previewLastKey\s*\)\s*return\s*;"
    )
    assert not forbidden.search(body), (
        "v1.15.140: pre-fix `if (key === previewLastKey) return;` "
        "without the !preview.hidden guard re-introduces the "
        "closed-and-reopened-dialog dedup-stuck bug."
    )


def test_preview_last_key_still_assigned_after_check():
    """The dedup variable must still be assigned the new key after
    the visibility-aware check passes — otherwise the dedup is
    never useful (would re-render on every input event)."""
    body = _update_preview_body()
    # The assignment must appear AFTER the dedup short-circuit.
    check_idx = body.index(
        "if (key === previewLastKey && !preview.hidden) return;")
    assign_idx = body.index("previewLastKey = key;", check_idx)
    assert assign_idx > check_idx, (
        "v1.15.140: previewLastKey assignment must follow the "
        "short-circuit check (otherwise consecutive same-URL "
        "inputs while the preview is visible would re-render "
        "every keystroke — defeats the dedup)."
    )


def test_open_manual_url_dialog_still_hides_preview():
    """openManualUrlDialog hiding the preview is THE mechanism that
    makes the v1.15.140 fix work — if it stops hiding, a stale
    visible render from the previous open would still satisfy
    `!preview.hidden` and the dedup would re-engage incorrectly."""
    js = _read_js()
    fn_start = js.index("function openManualUrlDialog(")
    fn_end = js.index("function closeManualUrlDialog(", fn_start)
    body = js[fn_start:fn_end]
    assert "preview.hidden = true" in body, (
        "v1.15.140: openManualUrlDialog MUST hide the preview on "
        "open — that's what flips !preview.hidden so the next "
        "matching-key paste re-renders. Without this reset the "
        "v1.12.97 'stale thumbnail flash' bug returns AND the "
        "v1.15.140 fix becomes a no-op."
    )
