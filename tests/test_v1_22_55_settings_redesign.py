"""v1.22.55 — ground-up settings redesign (hybrid field layout).

the user: "the settings pages are all over the place" — per-tab spacing
voids, buttons floating in different spots, half-text squished. The
redesign introduces three primitives and retires two old shapes:

  * `.field-row` (label | control two-column) replaces the old
    `form-label > form-label-row > form-label-text` scalar-field stack.
  * `.control-group` / `.control-row` is the canonical action+status
    surface; it retired the bespoke `.dry-run-state` family.
  * `.tab-panel .block-body` switches off the inherited
    `white-space: pre-wrap` (the phantom-vertical-space root cause) and
    becomes a flex column whose gap owns all inter-child spacing.

These are static-structure + CSS-presence guards (no node runtime).
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SETTINGS = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── New primitives exist as real CSS rules ───────────────────

def test_field_row_primitives_defined():
    """The hybrid scalar-field primitives must be real CSS rules —
    not silent-inheritance class names (the v1.15.87 gap class)."""
    for sel in (".field-row", ".field-name", ".field-control"):
        assert re.search(rf"^{re.escape(sel)} ?\{{", CSS, re.M), (
            f"v1.22.55: {sel} must be a real CSS rule"
        )


def test_control_group_primitives_defined():
    """The action+status surface primitives must be real CSS rules."""
    for sel in (".control-group", ".control-row", ".control-actions"):
        assert re.search(rf"^{re.escape(sel)} ?\{{", CSS, re.M), (
            f"v1.22.55: {sel} must be a real CSS rule"
        )


def test_pill_danger_tone_defined():
    """The red pill tone (dry-run refresh-failed state) must exist —
    the JS references `pill pill-danger` and the old
    `.form-status.form-status-fail` rule needs a companion class a
    bare pill doesn't carry."""
    assert re.search(r"^\.pill-danger ?\{", CSS, re.M), (
        "v1.22.55: .pill-danger must be a real CSS rule"
    )
    assert "var(--red)" in CSS[CSS.index(".pill-danger"):CSS.index(".pill-danger") + 160]


# ── block-body de-pre-wrap + flex rhythm ─────────────────────

def test_tab_panel_block_body_kills_pre_wrap():
    """The settings panel rhythm fix: .tab-panel .block-body must
    override the inherited white-space: pre-wrap (the phantom-void
    root cause) and become a flex column."""
    m = re.search(r"\.tab-panel \.block-body \{[^}]*\}", CSS, re.S)
    assert m, "v1.22.55: .tab-panel .block-body rule must exist"
    block = m.group(0)
    assert "white-space: normal" in block
    assert "display: flex" in block
    assert "flex-direction: column" in block


# ── Old shapes retired ───────────────────────────────────────

def test_dry_run_state_family_retired():
    """The bespoke .dry-run-state / .dry-run-actions / .dry-run-current
    classes are gone from markup AND their CSS rules are removed — the
    dry-run panel now rides .control-group + a .pill status."""
    # Live markup only — the v1.22.55 comment legitimately names the
    # retired class while documenting what it replaced.
    assert 'class="dry-run-state"' not in SETTINGS
    assert 'class="dry-run-state-info"' not in SETTINGS
    assert 'class="dry-run-actions"' not in SETTINGS
    # The dry-run toggle still exists, just rebuilt on the shared shape.
    assert 'id="dry-run-on-btn"' in SETTINGS
    assert 'id="dry-run-current"' in SETTINGS
    # CSS rules for the retired family must be gone (no orphan rules).
    assert not re.search(r"^\.dry-run-state ?\{", CSS, re.M), (
        "v1.22.55: retired .dry-run-state CSS rule must be removed"
    )
    assert not re.search(r"^\.dry-run-actions ?\{", CSS, re.M)


def test_dry_run_status_rendered_as_pill():
    """The dry-run status element is a .pill and the JS sets its tone
    via the pill class family, not an inline style.color."""
    idx = SETTINGS.index('id="dry-run-current"')
    tag = SETTINGS[max(0, idx - 60):idx + 20]
    assert 'class="pill"' in tag, "dry-run-current must render as a .pill"
    # JS toggles pill / pill-warn / pill-danger, not style.color values.
    assert "'pill pill-warn'" in JS or '"pill pill-warn"' in JS
    assert "'pill pill-danger'" in JS or '"pill pill-danger"' in JS


def test_no_form_label_row_in_tab_panel_markup():
    """No active <div class="form-label-row"> survives in the settings
    markup — every scalar field migrated to .field-row. (The class
    name may still appear inside Jinja {# #} comments documenting the
    old shape; we only forbid live markup.)"""
    # Strip Jinja comments + HTML comments before scanning.
    stripped = re.sub(r"\{#.*?#\}", "", SETTINGS, flags=re.S)
    stripped = re.sub(r"<!--.*?-->", "", stripped, flags=re.S)
    assert '<div class="form-label-row">' not in stripped, (
        "v1.22.55: form-label-row is retired on tab-panels — use .field-row"
    )


# ── Field count sanity (the redesign touched every tab) ──────

def test_field_row_adoption_is_broad():
    """The redesign converted scalar fields across many tabs, not one.
    A floor guard so a partial revert is caught."""
    assert SETTINGS.count('class="field-row"') >= 20, (
        "v1.22.55: expected the hybrid field-row across all settings tabs"
    )


def test_checkbox_stacks_stay_full_width():
    """The hybrid rule: checkbox toggles keep .form-checkbox (full
    width), NOT .field-row — long-hint toggle stacks shouldn't be
    forced into the two-column scalar layout."""
    # AUTOMATION + EVENTS toggles still use form-checkbox.
    assert SETTINGS.count("form-checkbox") >= 8
    # The AUTO-DOWNLOAD toggle's wrapping <label> (which sits just
    # BEFORE the input) must be a form-checkbox, not a field-row.
    i = SETTINGS.index("auto_download_new_themes_for_unthemed_rows")
    window = SETTINGS[max(0, i - 120):i]
    assert "form-checkbox" in window
    assert 'class="field-row"' not in window
