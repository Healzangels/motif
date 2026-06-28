"""v1.20.61 — render an informational panel for urls_match pending updates.

Bug report (the user, Zootopia 2): the INFO card's // PROPOSED CHANGE
thumbnail diff wasn't showing. Root cause: the row is a `urls_match`
pending update — ThemerrDB published the exact YouTube video the user's
user override already used (Xry6B0I3pT8, reconstructed by recovery
v1.18.10), so `extract_video_id(override_url) == yt_vid` and sync wrote
`kind='urls_match'` (sync.py:1205-1209). `renderPendingUpdateDiff` then
short-circuited at `if (pu.kind === 'urls_match') return ''` — by design
since v1.12.56, because CURRENT and PROPOSED would be the IDENTICAL video
(no diff to draw). Not a regression, but the card looked blank for a
state where ACCEPT UPDATE still does something real (U→T reclassify).

Fix: instead of returning '', render a single-tile panel (the matched
theme thumbnail + a U→T reclassify note). No arrow, no second tile —
they'd be identical. The two-tile diff still renders for genuine
`upstream_changed` rows (TDB proposing a DIFFERENT video).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _diff_fn() -> str:
    start = APP_JS.index("function renderPendingUpdateDiff(")
    # The function ends just before the next sibling function.
    end = APP_JS.index("async function hydrateDiffTitles(", start)
    return APP_JS[start:end]


def test_urls_match_no_longer_returns_blank():
    """The bare `return ''` blank-out for urls_match must be gone — that
    was the symptom (empty card)."""
    fn = _diff_fn()
    assert "if (pu.kind === 'urls_match') return '';" not in fn, (
        "the urls_match early-return blanked the card; v1.20.61 renders "
        "an informational single-tile panel instead"
    )


def test_urls_match_branch_renders_single_tile_panel():
    """The urls_match branch renders the // THEMERRDB MATCH panel with
    the single-tile grid + the matched-thumbnail label."""
    fn = _diff_fn()
    # The branch still keys on urls_match...
    assert "if (pu.kind === 'urls_match') {" in fn
    # ...and returns a real panel, not nothing.
    assert "// THEMERRDB MATCH" in fn
    assert 'class="diff-tiles-single"' in fn
    assert "CURRENT = PROPOSED" in fn
    # The reclassify note must explain the U→T no-op-content meaning.
    assert "reclassifies this row" in fn
    assert "no re-download" in fn


def test_urls_match_thumbnail_prefers_override_url():
    """The matched thumbnail sources the override URL first (it's what's
    applied), falling back to the pending-update URLs / lf vid."""
    fn = _diff_fn()
    anchor = fn.index("if (pu.kind === 'urls_match') {")
    branch = fn[anchor:anchor + 2600]
    assert "const matchRaw = (ovr && ovr.youtube_url)" in branch
    assert "pu.new_youtube_url || pu.old_youtube_url" in branch
    # Thumbnail still goes through img.youtube.com like the real diff.
    assert "https://img.youtube.com/vi/" in branch
    # And the oembed title hydration slot is present so the title fills in.
    assert 'data-oembed-slot="current"' in branch


def test_two_tile_diff_still_intact_for_upstream_changed():
    """The genuine diff (CURRENT → PROPOSED two-tile) must still render
    for non-urls_match rows — we only changed the urls_match path."""
    fn = _diff_fn()
    assert "// PROPOSED CHANGE" in fn
    assert 'class="diff-tiles"' in fn  # the 3-col diff grid
    assert 'class="diff-arrow"' in fn
    # The non-YouTube guard for the PROPOSED tile is unchanged.
    assert "if (newSrc === 'unknown') return '';" in fn


def test_diff_tiles_single_css_primitive_exists():
    """The single-tile layout primitive must exist + use tokens (no
    hardcoded gap). v1.22.13: the lone CURRENT=PROPOSED tile is now CENTERED
    (was 1fr 1fr, which parked the only tile in the left column) — a single
    capped track + justify-content."""
    assert ".diff-tiles-single {" in APP_CSS
    start = APP_CSS.index(".diff-tiles-single {")
    rule = APP_CSS[start:start + 360]
    assert "grid-template-columns: minmax(0, 360px);" in rule, (
        "v1.22.13: single tile uses a capped centered track, not 1fr 1fr"
    )
    assert "justify-content: center;" in rule, (
        "v1.22.13: the lone tile must be centered in its container"
    )
    assert "gap: var(--gap-3);" in rule
    # Mobile collapses it to a single full-width column alongside .diff-tiles.
    assert ".diff-tiles, .diff-tiles-single { grid-template-columns: 1fr; }" in APP_CSS


def test_v1_20_61_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
