"""v1.19.25 — pin chevron via wrapper + import UX parity.

Three /queue + /settings fixes from the user's v1.19.24 review.

## Fix 1: chevron pinned to viewport via wrapper div

v1.19.24's chevron pseudos sat on the scroll container itself
(.jobs-scroll-x, .event-stream, .event-stream-tall). Because
those containers have overflow:auto, their absolute-positioned
pseudos position relative to the SCROLL CONTENT — not the
visible viewport. When the user scrolled horizontally, the
chevrons slid along with content and out of view. the user:
"not see < vs > can we make jobs and event stream chevron at
the same point centered in the sections. no seeing the
direction changing."

Fix: queue.html wraps each scroll container in a new
.scroll-chevron-wrap div. The wrapper has position: relative
but no overflow — chevron pseudos pinned to its edges stay at
the visible viewport regardless of inner scroll. Vertical
centering via top: 50% + translateY(-50%) — both wrappers
have similar heights so chevrons land at the same y-band.

## Fix 2: import file input reverted to <label>

v1.15.73 replaced `<label>` with `<div>` after the user flagged
that clicking the heading opened the file dialog as a footgun.
v1.19.25 reverses that: the user on v1.19.24 compared this to
the theme upload modal (which uses a <label>) and said "I
like how the theme upload functions/click area." Re-enables
click-anywhere-on-row → opens picker, matching the modal.

## Fix 3: preview import button reverts to standard color

v1.15.76 made the button violet (lib-source-user) to group
with the U-row palette (CSV imports populate user_overrides).
the user on v1.19.24: "can we also change the preview import
button back to the standard button color." Drops the
modifier — green outlined default.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
QUEUE_HTML = (REPO / "app" / "web" / "templates" / "queue.html").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


# ── Fix 1 (chevron wrapper) — RETIRED v1.22.56 ──────────────
# The chevron / .scroll-chevron-wrap horizontal-scroll affordance
# was removed when the LOGS page became a single full-width panel
# with a JOBS/EVENT STREAM toggle (no side-by-side split, no
# horizontal scroll). Its tests went with it; see
# test_v1_22_56_logs_toggle.py for the toggle's guards. The import
# UX tests (Fix 2 / Fix 3) below are unrelated and remain.


# ── Fix 2: import file input <label> wrapper ─────────────────


def test_settings_import_file_uses_label_wrapper():
    """The CSV FILE row on /settings IMPORT must be wrapped in
    a <label> so clicking the heading activates the file
    picker — matches theme-upload modal UX the user prefers."""
    # Anchor on the actual rendered span (>CSV FILE<), not the
    # Jinja comment text. The comment mentions "CSV FILE" too,
    # so plain `.index("CSV FILE")` lands too early.
    idx = SETTINGS_HTML.index(">CSV FILE<")
    backwards = SETTINGS_HTML[max(0, idx - 600):idx]
    # v1.22.55 settings redesign: the labelled field wrapper is now
    # `<label class="field-row">` (hybrid two-column field layout). The
    # UX intent is unchanged — the input is still a direct child of a
    # <label>, so clicking the heading focuses the picker. Accept either
    # the pre-redesign `form-label` or the new `field-row` wrapper.
    assert ('<label class="field-row"' in backwards
            or '<label class="form-label"' in backwards), (
        "v1.19.25 / v1.22.55: CSV FILE row must be wrapped in a <label> "
        "(field-row or form-label) so clicking the heading opens the picker"
    )
    # v1.19.39: inverted from v1.19.25's assertion. DESIGN_SYSTEM.md
    # § 2 (updated v1.19.36) says no `for=` attribute when the
    # input is a direct child of the label — matches the sibling
    # at library.html:659 upload-dlg. Post-v1.19.39 audit found
    # this site was the lone outlier; doc-update lied about
    # consistency. Pin the corrected shape.
    assert 'for="import-csv-file"' not in backwards, (
        "v1.19.39 (reversal of v1.19.25 assertion): the label "
        "must NOT carry `for=import-csv-file` — DESIGN_SYSTEM.md "
        "§ 2 forbids `for=` when the input is wrapped INSIDE the "
        "label (input as direct child). v1.19.25 pinned the "
        "wrong shape; v1.19.39 inverts it."
    )


def test_settings_import_file_row_still_intact():
    """The CSV FILE field keeps its label/name/input structure after
    the v1.22.55 settings redesign — the row migrated from the old
    `form-label` + `form-label-row` shape to the hybrid `field-row` +
    `field-name` + `field-control` shape. Anchor on the rendered span
    so we test the real markup, not the Jinja comment that still
    mentions the legacy class name (pre-fix this matched the comment,
    a v1.18.81-class phantom pass)."""
    idx = SETTINGS_HTML.index(">CSV FILE<")
    backwards = SETTINGS_HTML[max(0, idx - 600):idx]
    assert '<label class="field-row"' in backwards
    assert 'class="field-name"' in backwards
    assert "import-csv-file" in SETTINGS_HTML


# ── Fix 3: preview import button color ───────────────────────


def test_preview_import_button_uses_standard_btn_class():
    """The // PREVIEW IMPORT button must use only `btn btn-tiny`
    (default green) — NOT `lib-source-user` (the v1.15.76
    violet modifier). the user's v1.19.24 ask."""
    idx = SETTINGS_HTML.index('id="import-preview-btn"')
    backwards = SETTINGS_HTML[max(0, idx - 200):idx + 50]
    assert "lib-source-user" not in backwards, (
        "v1.19.25: drop the violet `lib-source-user` modifier "
        "— the user wants standard green button"
    )
    # Must still have the base btn classes.
    assert 'class="btn btn-tiny"' in backwards
