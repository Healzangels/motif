"""v1.21.12 — Settings IA: group the automation toggles.

Audit of the Settings page (the user's request) found the three "what
motif does on its own" toggles scattered: auto_download +
auto_enum_after_sync were buried under SYNC TIMING (not a timing
concern), and auto_place had its own PLACEMENT MODE block. Grouped them
into one // AUTOMATION block in the SCHEDULE tab.

The block spans two config sections (sync.* + placement.*), so its SAVE
button uses data-save="sync placement" and collectFieldsForTab() now
accepts space-separated sections, PATCHing both in one request.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _automation_block() -> str:
    start = SETTINGS_HTML.index("// AUTOMATION")
    end = SETTINGS_HTML.index('data-save="sync placement"', start) + 200
    return SETTINGS_HTML[start:end]


def test_automation_block_groups_all_three_toggles():
    b = _automation_block()
    assert 'data-cfg-field="sync.auto_download_new_themes_for_unthemed_rows"' in b
    assert 'data-cfg-field="placement.auto_place"' in b
    assert 'data-cfg-field="sync.auto_enum_after_sync"' in b


def test_automation_save_button_spans_both_sections():
    b = _automation_block()
    assert 'data-save="sync placement"' in b
    assert 'data-save-status="sync placement"' in b


def test_each_toggle_appears_exactly_once():
    """No duplicate after the move (the old blocks are gone)."""
    for field in (
        "sync.auto_download_new_themes_for_unthemed_rows",
        "placement.auto_place",
        "sync.auto_enum_after_sync",
    ):
        assert SETTINGS_HTML.count(f'data-cfg-field="{field}"') == 1


def test_placement_mode_block_removed():
    """The standalone // PLACEMENT MODE block is gone (auto_place folded
    into AUTOMATION). The // DEFAULT PLACEMENT METHOD block in the PLEX
    tab — which legitimately keeps its own data-save="placement" for
    placement.default_method — is untouched."""
    assert '<h2 class="block-title">// PLACEMENT MODE</h2>' not in SETTINGS_HTML
    assert '// SAVE PLACEMENT</button>' in SETTINGS_HTML  # default_method still saved


def test_sync_timing_block_is_just_the_cron():
    """SYNC TIMING keeps the cron but no longer the automation toggles."""
    start = SETTINGS_HTML.index("// SYNC TIMING")
    block = SETTINGS_HTML[start:SETTINGS_HTML.index('data-save="sync"', start)]
    assert 'data-cfg-field="sync.cron"' in block
    assert "auto_download_new_themes_for_unthemed_rows" not in block
    assert "auto_enum_after_sync" not in block


def test_collect_fields_supports_multi_section():
    """collectFieldsForTab must split the tab spec on whitespace so one
    save button can PATCH multiple config sections."""
    idx = APP_JS.index("function collectFieldsForTab(")
    body = APP_JS[idx:idx + 600]
    assert ".split(/\\s+/)" in body
    # It builds an empty `out` then keys each section in.
    assert "out[tab] = out[tab] || {}" in body


def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
