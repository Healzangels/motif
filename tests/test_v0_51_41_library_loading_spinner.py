"""v0.51.41 — the library RESULTS loading state uses the branded record-spinner.

the user: "when there is a loading screen for the results can we have the spinning
logo loading screen we used for our info card loading screen".

The library table showed a bare "loading…" while fetching. Now both loading
placeholders — the JS re-fetch one (loadLibrary, gated on lastHash==null) and the
template first-paint one (#library-body) — render the same vinyl record-spinner
the INFO card uses (recordLoaderHtml / .record-spinner). The spinner SVG is
inlined in the template for the SSR first paint; this test pins it in sync with
the JS helper so the two never drift.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


def _spinner_svg(text: str) -> str:
    """Extract + whitespace-normalize the <svg class="record-spinner">…</svg>."""
    m = re.search(r'<svg class="record-spinner".*?</svg>', text, re.S)
    assert m, "no record-spinner SVG found"
    return re.sub(r"\s+", " ", m.group(0)).strip()


# ── the JS re-fetch placeholder uses the spinner (not a bare string) ──

def test_js_loading_row_uses_record_spinner():
    idx = APP_JS.index("if (tbody.dataset.lastHash == null)")
    branch = APP_JS[idx:idx + 400]
    assert "recordLoaderHtml('loading…')" in branch, (
        "the loadLibrary first-load placeholder must render the shared spinner")
    # colspan spans the full 11-column row so the spinner centres under the table.
    assert 'colspan="11"' in branch


# ── the template first-paint placeholder also shows the spinner ───────

def test_template_first_paint_shows_spinner():
    i = LIB_HTML.index('<tbody id="library-body">')
    block = LIB_HTML[i:i + 1200]
    assert 'class="record-loader"' in block
    assert 'class="record-spinner"' in block
    # the bare-text placeholder is gone.
    assert 'class="muted center">loading…' not in block


# ── drift guard: template spinner == the JS helper's spinner ──────────

def test_template_and_js_spinner_are_identical():
    js_fn = APP_JS[APP_JS.index("function recordLoaderHtml("):]
    assert _spinner_svg(js_fn) == _spinner_svg(LIB_HTML), (
        "the template first-paint spinner drifted from recordLoaderHtml — keep "
        "the two SVGs identical (their .record-spinner CSS is shared, v1.24.95)")
