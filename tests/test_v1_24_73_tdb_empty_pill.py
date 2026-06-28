"""v1.24.73 — distinct TDB pill for a tracked-but-themeless record.

the user: Daredevil: Born Again showed a green "TDB" pill yet offered no REPLACE
TDB — confusing. The green tdb-pill-yes is the fallthrough in the row's TDB-pill
render: it fires whenever upstream_source is set, regardless of whether
ThemerrDB actually has a theme video (themes.youtube_url). v1.24.71 already hid
REPLACE TDB on those rows (keyed on it.youtube_url); this aligns the PILL to the
same signal so the two agree — a tracked-but-themeless row now renders a muted
"TDB ∅" pill instead of the healthy green one.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


# ── render: empty-pill branch precedes the green fallthrough ──────────────────


def test_empty_pill_branch_before_green_fallthrough():
    # the muted TDB ∅ pill exists and is gated on !it.youtube_url …
    empty = APP_JS.index('tdb-pill-empty')
    guard = APP_JS.rindex('if (!it.youtube_url)', 0, empty)
    assert guard != -1
    # … and it sits ABOVE the green tdb-pill-yes fallthrough so a themeless
    # row stops here instead of rendering green.
    green = APP_JS.index('tdb-pill-yes" title="ThemerrDB tracked"')
    assert empty < green, "TDB ∅ branch must precede the green fallthrough"
    # keyed on the SAME signal the REPLACE TDB gate uses (it.youtube_url).
    replace_gate = APP_JS.index("'replace-with-themerrdb', 'REPLACE TDB'")
    gate = APP_JS[APP_JS.rindex('if (isThemerrDb', 0, replace_gate):replace_gate]
    assert 'it.youtube_url' in gate


# ── CSS: the muted pill class exists and is distinct ─────────────────────────


def test_empty_pill_css_rule():
    assert '.tdb-pill-empty {' in APP_CSS
    # muted (not green) + dashed border to distinguish from solid .tdb-pill-dropped.
    rule = APP_CSS[APP_CSS.index('.tdb-pill-empty {'):APP_CSS.index('.tdb-pill-empty {') + 200]
    assert 'var(--fg-mute)' in rule
    assert 'dashed' in rule


# ── legend gloss: both the full + condensed decoders include it ──────────────


def test_legend_glosses_include_empty_pill():
    assert 'tdb-pill-empty">TDB ∅' in BASE_HTML
    assert 'tdb-pill-empty">TDB ∅' in LIBRARY_HTML
