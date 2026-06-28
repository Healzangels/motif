"""v1.18.26 — Settings page UI for placement.default_method.

Closes the v1.18.23 deferred item: the runtime already honored
the config key (`placement.default_method`) and the env override
(`MOTIF_DEFAULT_PLACEMENT_METHOD`); v1.18.26 surfaces it in the
/settings page so users can flip the default without editing
yaml or the env.

## Implementation

  * New section `// DEFAULT PLACEMENT METHOD` in the PLEX tab,
    rendered as a separate <section data-panel="plex"> block
    so the existing tab-show/hide JS picks it up.
  * <select> input with two options ('file' / 'api') wired via
    data-cfg-field="placement.default_method". The select
    integrates with the existing populateConfigForms +
    collectFieldsForTab paths unchanged — el.value setter on
    a select picks the matching option; el.value getter
    returns the selected option's value.
  * Separate SAVE PLACEMENT button (data-save="placement").
    The data-save handler at app.js:6036 collects fields under
    the matching namespace via _pset; PATCH targets the
    placement section. v1.17.2 multi-SAVE-buttons-per-tab
    pattern.
  * ENV override badge mirrors the existing pattern:
    data-env-badge="placement.default_method", hidden unless
    env_overrides() reports the key.

## Backend (already in place from v1.18.23)

  * PlacementConfig.default_method: str = "file"
  * Env override map: MOTIF_DEFAULT_PLACEMENT_METHOD
  * Settings.default_placement_method property validates to
    {'file', 'api'}, garbage → 'file'.
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Template pins ─────────────────────────────────────────────


def test_settings_template_has_placement_method_section():
    """A new <section data-panel='plex'> block titled
    '// DEFAULT PLACEMENT METHOD' must exist."""
    html = SETTINGS_HTML.read_text()
    assert "// DEFAULT PLACEMENT METHOD" in html
    # The block must live inside the PLEX tab panel.
    idx = html.index("// DEFAULT PLACEMENT METHOD")
    # Walk backwards to find the enclosing section opening tag.
    section_open = html.rfind("<section", 0, idx)
    section_tag = html[section_open:section_open + 200]
    assert 'data-panel="plex"' in section_tag, (
        "v1.18.26: placement-method block must live in the PLEX "
        "tab panel (data-panel='plex')"
    )


def test_settings_template_has_select_with_file_and_api():
    """The <select> input must offer exactly the two backend-
    accepted values: 'file' and 'api'."""
    html = SETTINGS_HTML.read_text()
    assert 'data-cfg-field="placement.default_method"' in html
    idx = html.index('data-cfg-field="placement.default_method"')
    block = html[idx:idx + 1000]
    assert '<option value="file"' in block
    assert '<option value="api"' in block
    # No bogus third option.
    option_count = block.count('<option value=')
    assert option_count == 2, (
        f"v1.18.26: select must have exactly 2 options (file + "
        f"api), found {option_count}"
    )


def test_settings_template_has_separate_save_placement_button():
    """A separate SAVE PLACEMENT button must exist (vs trying
    to piggyback on SAVE PLEX, which only collects plex.*
    fields)."""
    html = SETTINGS_HTML.read_text()
    assert '<button class="btn btn-warn" data-save="placement">// SAVE PLACEMENT</button>' in html
    assert 'data-save-status="placement"' in html


def test_settings_template_env_override_badge_wired():
    """The env-override badge for placement.default_method must
    reference env_overrides() so MOTIF_DEFAULT_PLACEMENT_METHOD
    disables the input."""
    html = SETTINGS_HTML.read_text()
    assert 'data-env-badge="placement.default_method"' in html
    idx = html.index('data-env-badge="placement.default_method"')
    block = html[idx:idx + 300]
    assert '"placement.default_method" not in env_overrides()' in block


def test_settings_template_hint_clarifies_collection_exception():
    """The form-hint must tell users that collections IGNORE
    this setting (they always use API). Without that note, a
    user picking 'file' might expect collections to fall back
    to sidecars, which can't work (no folder)."""
    html = SETTINGS_HTML.read_text()
    idx = html.index('// DEFAULT PLACEMENT METHOD')
    block = html[idx:idx + 2500]
    # Look for the explanatory hint.
    assert "Collections" in block
    assert "ignore" in block.lower() or "always" in block.lower()


def test_settings_template_hint_mentions_per_row_override():
    """The hint must clarify that the PLACE menu's per-row
    (FILE/API) actions override the global default. Users
    should understand the setting only governs auto-place."""
    html = SETTINGS_HTML.read_text()
    idx = html.index('// DEFAULT PLACEMENT METHOD')
    block = html[idx:idx + 2500]
    assert "PUSH TO PLEX (FILE)" in block
    assert "PUSH TO PLEX (API)" in block or "override" in block.lower()


# ── Marker pin ────────────────────────────────────────────────


def test_settings_template_has_v1_18_26_marker():
    """v1.18.26 marker comment must anchor the new block."""
    html = SETTINGS_HTML.read_text()
    assert "v1.18.26:" in html
