"""v1.17.18 — bulk-bar overflow menu.

the user's screenshot showed the bulk-action bar with `// RESTORE
FROM PLEX` cut off mid-word at the right edge of the viewport.
The v1.17.6 contract was "constant height, horizontal scroll on
overflow" — but in practice the scroll was undiscoverable
(no visual cue) and clipped actions silently. the user: "we can't
have the bulk actions spill off like this we need a different
solution."

## Fix

Replace the horizontal-scroll fallback with a `// MORE ▾`
overflow dropdown. When the bar would overflow, the rightmost
visible action buttons move into the dropdown panel until the
bar fits. The bar's constant-height contract (v1.17.6) is
preserved; actions remain discoverable instead of clipped.

Implementation:

* **Template** (`library.html`): trailing `<details class="row-menu"
  id="library-bulk-overflow-menu">` with a `// MORE ▾` summary
  and a panel. Hidden by default; the JS shows it only when
  buttons actually overflow.
* **CSS** (`app.css`): swap `overflow-x: auto` → `overflow: hidden`
  on `#library-bulk-bar` since the JS handles overflow now.
  Style the overflow panel with `min-width: 200px` and a
  `max-height: 50vh` scroll for very-many-actions cases.
* **JS** (`app.js`): `_layoutBulkBar()` measures
  `bar.scrollWidth > bar.clientWidth`, then moves the rightmost
  non-primary visible button into the panel one at a time until
  the bar fits. Primary bookends (SELECT ALL FILTERED, CLEAR)
  never move — they're the always-needed selection controls.
  Re-running the function is idempotent: it first restores
  every panel child back to the bar inline (DOM order
  preserved via `insertBefore(_, overflowMenu)`), then
  re-measures.
* Hooks: called at the end of `updateLibrarySelectionUi`, and
  via a `ResizeObserver` on the bar so width-only changes
  (sidebar collapse, drawer toggle, window resize) re-trigger
  layout.
* Close-on-action: clicking a button inside the overflow panel
  closes the dropdown after firing (mirrors the v1.10.24
  row-menu pattern, but scoped to the bulk bar which lives
  outside `#library-body`).

## Anti-regression pins

* SELECT ALL FILTERED + CLEAR are in the `_BULK_BAR_PRIMARY_IDS`
  set — they never get moved into the overflow.
* The CSS no longer says `overflow-x: auto`.
* The template includes the `<details>` overflow container.
* The layout function exists and is wired into
  `updateLibrarySelectionUi`.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"
APP_INIT = REPO / "app" / "__init__.py"


# ── Template: overflow menu container ─────────────────────────


def test_template_has_overflow_menu_container():
    """library.html must declare the `<details>` overflow
    container with the expected id + a `// MORE ▾` summary."""
    src = LIBRARY_HTML.read_text()
    assert 'id="library-bulk-overflow-menu"' in src, (
        "v1.17.18: library.html must include the overflow menu "
        "container."
    )
    # The summary uses the canonical // prefix per § 3 UI conventions.
    assert "// MORE" in src, (
        "v1.17.18: overflow toggle should read `// MORE ▾` to "
        "match the dashboard convention for menu/toggle labels."
    )
    # Initially hidden — only the JS shows it when buttons overflow.
    overflow_idx = src.index('id="library-bulk-overflow-menu"')
    window = src[overflow_idx:overflow_idx + 300]
    assert 'style="display:none"' in window, (
        "v1.17.18: overflow menu must start hidden (the JS "
        "reveals it only when overflow is actually detected)."
    )


def test_template_uses_row_menu_primitive():
    """The overflow menu reuses the existing .row-menu / .row-
    menu-panel CSS — same primitive as per-row SOURCE / PLACE /
    REMOVE menus. Keeps the visual language consistent."""
    src = LIBRARY_HTML.read_text()
    idx = src.index('id="library-bulk-overflow-menu"')
    # The class= attribute lives on the same <details> tag as the
    # id=, but appears earlier in source order. Anchor a window
    # that includes the preceding tag start.
    window = src[max(0, idx - 60):idx + 500]
    assert 'class="row-menu"' in window, (
        "v1.17.18: reuse the .row-menu primitive for visual "
        "consistency with the per-row menus."
    )
    assert "row-menu-panel" in window, (
        "v1.17.18: overflow content must live in a "
        ".row-menu-panel container."
    )


# ── CSS: scroll retired ───────────────────────────────────────


def test_css_dropped_overflow_x_auto():
    """The v1.17.6 horizontal-scroll declaration is retired
    (the JS now handles overflow via the // MORE menu). Pin
    the absence so a future "let's just bring back scroll"
    edit fails this test."""
    src = APP_CSS.read_text()
    # The #library-bulk-bar rule must not include
    # `overflow-x: auto`.
    idx = src.index("#library-bulk-bar {")
    end = src.index("}", idx)
    rule = src[idx:end]
    assert "overflow-x: auto" not in rule, (
        "v1.17.18: #library-bulk-bar must NOT use "
        "overflow-x: auto — the JS // MORE menu handles "
        "overflow now. See the user's clipped-button screenshot."
    )
    # v1.17.21: overflow flipped hidden → visible because
    # hidden was clipping the absolutely-positioned // MORE ▾
    # dropdown panel (the user's "panel behind the next frame"
    # screenshot). _layoutBulkBar runs synchronously before
    # paint so the original layout-flash worry was theoretical;
    # the clip bug was real.
    assert "overflow: visible" in rule, (
        "v1.17.21: #library-bulk-bar should use overflow: "
        "visible so the // MORE ▾ dropdown panel can render "
        "below the bar without being clipped."
    )


def test_css_constrains_overflow_panel():
    """The overflow panel needs a min-width + max-height so a
    panel full of moved-in buttons stays a tidy popover."""
    src = APP_CSS.read_text()
    assert (
        "#library-bulk-overflow-menu .row-menu-panel"
    ) in src, (
        "v1.17.18: must declare a CSS rule scoped to the "
        "overflow panel so its width / max-height don't fall "
        "back to the row-menu defaults (which are sized for "
        "per-row menus)."
    )


# ── JS: layout function + observer ────────────────────────────


def test_layout_function_exists():
    """`_layoutBulkBar` is the load-bearing piece — it measures
    overflow and moves buttons into the dropdown."""
    src = APP_JS.read_text()
    assert "function _layoutBulkBar()" in src, (
        "v1.17.18: _layoutBulkBar function must exist."
    )
    idx = src.index("function _layoutBulkBar()")
    body = src[idx:idx + 3000]
    # Must detect overflow. v1.20.31 swapped the unreliable
    # scrollWidth<=clientWidth comparison (broken on the overflow:visible
    # flex bar) for the getBoundingClientRect-based _barHasOverflow helper.
    assert "_barHasOverflow(bar)" in body, (
        "v1.17.18/v1.20.31: layout fn must detect overflow (now via "
        "the _barHasOverflow helper)."
    )
    # Must reset state by moving panel children back to the bar
    # before measuring (idempotency contract).
    assert "while (panel.firstChild)" in body, (
        "v1.17.18: layout fn must reset prior overflow state "
        "before re-measuring (idempotent contract)."
    )


def test_primary_buttons_never_moved():
    """SELECT ALL FILTERED + CLEAR are the always-needed
    selection controls — they must never get moved into the
    overflow menu, even when the bar is fully packed."""
    src = APP_JS.read_text()
    assert "_BULK_BAR_PRIMARY_IDS" in src, (
        "v1.17.18: must declare a _BULK_BAR_PRIMARY_IDS Set "
        "containing the never-overflow IDs."
    )
    # Both bookend IDs must be in the set.
    assert "'library-select-all-filtered-btn'" in src, (
        "v1.17.18: SELECT ALL FILTERED must be in "
        "_BULK_BAR_PRIMARY_IDS — it's the user's selection "
        "entry point."
    )
    assert "'library-clear-selection-btn'" in src, (
        "v1.17.18: CLEAR must be in _BULK_BAR_PRIMARY_IDS — "
        "the user's selection exit point."
    )


def test_layout_called_from_selection_ui_update():
    """`updateLibrarySelectionUi` must call `_layoutBulkBar`
    at the end so a filter/selection change re-flows the bar."""
    src = APP_JS.read_text()
    idx = src.index("function updateLibrarySelectionUi()")
    end = src.index("\n  function ", idx + 1)
    body = src[idx:end]
    assert "_layoutBulkBar()" in body, (
        "v1.17.18: updateLibrarySelectionUi must call "
        "_layoutBulkBar() at the end so visibility changes "
        "re-layout the overflow."
    )


def test_resize_observer_installed():
    """The bar's width can change without going through
    updateLibrarySelectionUi (window resize, drawer open).
    ResizeObserver fills that gap."""
    src = APP_JS.read_text()
    assert "ResizeObserver" in src, (
        "v1.17.18: must use a ResizeObserver on the bar to "
        "re-layout on width-only changes."
    )
    assert "_installBulkBarObserver" in src, (
        "v1.17.18: must declare _installBulkBarObserver and "
        "call it during bindLibrary."
    )


def test_overflow_menu_closes_on_action_click():
    """Clicking a button inside the overflow panel must close
    the dropdown after firing. The row-menu close-on-action
    handler at app.js:11355 is scoped to #library-body — the
    bulk overflow menu needs its own hook."""
    src = APP_JS.read_text()
    # Locate the dedicated handler.
    idx = src.index(
        "v1.17.18: close the bulk-bar overflow menu when the user")
    window = src[idx:idx + 1200]
    assert "_bulkOverflowMenu.addEventListener('click'" in window, (
        "v1.17.18: must attach a click listener to the "
        "overflow menu that closes it on action."
    )
    assert "removeAttribute('open')" in window


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_18():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 18), (
        f"v1.17.18: __version__ must be >= 1.17.18 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
