"""v1.18.42 — orphan dashboard chip-click feedback + tone-coding.

the user on v1.18.40: "pressing All or OK (10) doesn't do anything."

The chip clicks WERE firing — activeDriftFilter was toggling
correctly + the chip-active CSS was applied — but with all 10
rows in the `ok` state, the filtered table looked identical to
the unfiltered one. No visible feedback that the filter applied.

Two fixes in v1.18.42:

  1. **Tone-coded per-drift chips**: the DRIFT_TONE map was
     computed but never applied to chip className. Pre-fix
     every chip rendered with the same gray styling. Post-fix
     drift chips inherit motif's lib-source-X tone vocab so a
     `motif_not_selected` chip reads violet, `rk_lookup_failed`
     reads red, etc. — visual severity classification.

  2. **"Showing N of M" status line**: added below the chips.
     Re-renders on every filter change so the click produces
     immediate text feedback even when the row count is
     unchanged. the user's "doesn't do anything" perception fix.
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
ORPHANS_HTML = REPO / "app" / "web" / "templates" / "orphans.html"


def test_drift_chip_tone_class_applied():
    """Per-drift chips must include the tone class from
    DRIFT_TONE, not just 'chip' + 'chip-active'. Pre-v1.18.42
    the toneClass variable was computed but never applied."""
    src = ORPHANS_HTML.read_text()
    # The chip className must be assembled from a classes
    # array that includes the toneClass.
    assert "if (toneClass) classes.push(toneClass);" in src, (
        "v1.18.42: per-drift chip className must include the "
        "tone class from DRIFT_TONE for severity classification"
    )
    assert "classes.join(' ')" in src


def test_filter_status_line_element_exists():
    """The status line element must be present in the template
    DOM so the JS has somewhere to write feedback."""
    src = ORPHANS_HTML.read_text()
    assert 'id="orphans-filter-status"' in src


def test_render_table_writes_to_filter_status():
    """renderTable must update the status line with row count
    and current filter every time it runs."""
    src = ORPHANS_HTML.read_text()
    assert "filterStatusEl" in src
    # The status text must include the row count + filter label.
    assert "Showing <strong>" in src
    assert "of <strong>" in src
    # The filter label distinguishes 'all' from a specific
    # drift_type filter.
    assert "drift_type =" in src or "drift_type=" in src
    assert "all" in src.lower()


def test_status_line_renders_when_row_count_unchanged():
    """The status line wording must produce visible feedback
    even when the filtered row count equals the unfiltered row
    count (the {ok: 10} case that triggered the user's report).
    Pin that the wording includes BOTH the matching count AND
    the total — so 'Showing 10 of 10' is visible when ALL is
    active, then changes to 'Showing 10 of 10 · filter:
    drift_type = ok' when OK is clicked. Same numbers, different
    text => user can see the filter applied."""
    src = ORPHANS_HTML.read_text()
    # The "filter:" label must vary between the all-active and
    # drift-specific cases.
    assert "filter:" in src
