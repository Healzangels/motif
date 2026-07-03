"""v1.18.18 — Drop // ALL chip from Collections tab.

the user's UX ask:

  > Can we get rid of the All section in the Collections tab so
  > we only have Movies, TV Shows, Anime, or any Library that
  > has a collection in it.

The v1.18.1 chip row rendered `// ALL` plus one chip per
managed section that had ≥1 collection enumerated. the user wants
the ALL escape hatch gone so the user is always scoped to a
specific library.

## Implementation

  * `library.html` removes the ALL chip. Adds a Jinja-side
    default-to-first-chip resolution so the first paint already
    shows one chip active.
  * `app.js` hydration picks the first available chip when the
    URL/localStorage section_id is empty or no longer valid.
  * `_library_section_state` (api.py) unchanged — already
    returns only sections with ≥1 collection.

Degenerate case: fresh install with no collections enumerated
anywhere → `show_chips=False` → the chip row stays hidden
(unchanged from v1.18.1) and the resolved section_id is empty
(library shows everything, same as today's degenerate path).
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Template pins ─────────────────────────────────────────────


def test_collections_all_chip_readded_v0_51_21():
    """v0.51.21 (the user, reversing v1.18.18): the collections ALL chip is
    BACK — but as a `data-allres="1"` chip (all_res sentinel), NOT the old
    `data-section-id=""` empty-section shape. The old markup must stay gone
    (it collided with the section-default logic); the new one is opt-in and
    keys off all_res, so the first-chip default is preserved."""
    html = LIBRARY_HTML.read_text()
    # The OLD dropped shape must not come back.
    assert '// ALL</button>' not in html, (
        "v1.18.18: the old // ALL-labelled chip stays removed"
    )
    assert 'data-section-id=""' not in html, (
        "v1.18.18: no chip carries an empty section_id (the ALL sentinel is "
        "data-allres, not an empty section_id)"
    )
    # The NEW ALL chip: a data-allres chip in BOTH the collections and the
    # resolution branches.
    assert 'data-allres="1"' in html, (
        "v0.51.21: the // ALL chip is re-added as a data-allres chip"
    )
    assert html.count('data-allres="1"') >= 2, (
        "v0.51.21: ALL chip exists for the resolution branch AND the "
        "collections branch"
    )


def test_collections_template_defaults_first_chip_active():
    """When the URL has no ?section_id=, the template must mark
    the first chip in the rendered list as active so the first
    paint isn't ambiguous."""
    html = LIBRARY_HTML.read_text()
    # The new resolution variables.
    assert "_default_sec = _sec.sections[0].section_id" in html, (
        "v1.18.18: template must derive a _default_sec from "
        "the first section in the list"
    )
    assert "_resolved_sec = _active_sec if _active_sec else _default_sec" in html, (
        "v1.18.18: template must fall back to _default_sec when "
        "the URL didn't carry a section_id"
    )
    # The chip-active class must check against _resolved_sec.
    assert "_resolved_sec == s.section_id" in html


def test_collections_template_marker_present():
    """v1.18.18 marker comment in the template."""
    html = LIBRARY_HTML.read_text()
    assert "v1.18.18:" in html


# ── JS pins ───────────────────────────────────────────────────


def test_js_hydration_falls_back_to_first_chip():
    """app.js's section_id hydration must fall back to the first
    chip when no specific value is requested OR the requested
    value no longer matches any rendered chip."""
    js = APP_JS.read_text()
    # The validSecIds gating set + the firstChip fallback must co-occur.
    # v0.51.21: re-anchored on validSecIds itself (the old `if (tabKey ===
    # 'collections')` anchor became ambiguous once the ALL-chip logic added
    # the `&& !libraryState.allRes` guard). The firstChip fallback sits a few
    # lines below the set in both the init and hydrate collections branches.
    assert "validSecIds = new Set()" in js
    assert "const firstChip = chipNodes[0]" in js
    idx = js.index("validSecIds = new Set()")
    block = js[idx:idx + 600]
    assert "chipNodes[0]" in block, (
        "v1.18.18/v0.51.21: the firstChip fallback must sit just below the "
        "validSecIds set in the collections hydration"
    )


def test_js_marker_present():
    """v1.18.18 marker comment in the JS."""
    js = APP_JS.read_text()
    assert "v1.18.18:" in js


# ── Section-state SQL unchanged ───────────────────────────────


def test_library_section_state_still_filters_to_collections_only():
    """Regression guard: _library_section_state's per-section
    EXISTS subquery still requires media_type='collection'. The
    chip list = sections that have collections, which is exactly
    the user's 'or any Library that has a collection in it.'"""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = api_py.index("def _library_section_state(")
    body = api_py[fn_idx:fn_idx + 3000]
    assert "pi.media_type = 'collection'" in body, (
        "v1.18.18: section chips must continue to filter on "
        "media_type='collection' — without it the chip set "
        "would include music sections, etc."
    )
