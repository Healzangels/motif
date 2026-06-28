"""v1.23.47 — help feature Phase 2: per-section // ABOUT on /settings.

the user wanted the help layer to reach settings (the Unraid model). Decision
(AskUser): KEEP the always-on per-field hints — a new user setting motif up
needs that guidance visible — and have help mode ADD a one-line "what this
section is for / when you'd touch it" overview at the top of each settings
block. The overview text lives in one JS map (SETTINGS_HELP_ABOUT) and is
injected as a CSS-gated <p class="settings-help-about"> after each .block-head,
hanging off the same body.help-mode toggle as the v1.23.44/45 work.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


def _about_keys():
    block = APP_JS[APP_JS.index("const SETTINGS_HELP_ABOUT = {"):]
    block = block[:block.index("};")]
    return set(re.findall(r"'(// [^']+)':", block))


def _block_titles():
    return set(t.strip() for t in re.findall(
        r'<h2 class="block-title">([^<]+)</h2>', SETTINGS_HTML))


# ── drift guard: every ABOUT key maps to a real settings section ──


def test_every_about_key_matches_a_real_block_title():
    """A typo'd or renamed key silently injects nothing (the lookup misses).
    Pin every SETTINGS_HELP_ABOUT key to an actual settings block-title."""
    keys = _about_keys()
    titles = _block_titles()
    assert keys, "the SETTINGS_HELP_ABOUT map must be populated"
    stale = keys - titles
    assert not stale, f"ABOUT keys with no matching settings section: {stale}"


def test_key_config_sections_are_covered():
    """The sections a new user must understand to set motif up carry an
    overview."""
    keys = _about_keys()
    for section in ("// PATHS", "// PLEX SERVER", "// DOWNLOAD TUNING",
                    "// AUTOMATION", "// SYNC TRANSPORT", "// NOTIFICATIONS",
                    "// EVENTS"):
        assert section in keys, f"missing // ABOUT for {section}"


# ── injector + CSS gate + keep-hints invariant ──


def test_injector_inserts_gated_overview_after_block_head():
    assert "function initSettingsHelp()" in APP_JS
    fn = APP_JS[APP_JS.index("function initSettingsHelp()"):]
    fn = fn[:fn.index("\n  function ")]
    assert ".block-head" in fn
    assert "'settings-help-about'" in fn
    assert "insertAdjacentElement('afterend'" in fn
    # idempotent — guards against double-insert on a re-run.
    assert "settings-help-about" in fn and "return" in fn
    # wired into help-mode init.
    assert "initSettingsHelp();" in APP_JS[APP_JS.index("function initHelpMode()"):]


def test_overview_hidden_until_help_mode_and_hints_stay_visible():
    assert ".settings-help-about {" in APP_CSS
    i = APP_CSS.index(".settings-help-about {")
    assert "display: none" in APP_CSS[i:i + 200]
    assert "body.help-mode .settings-help-about { display: block; }" in APP_CSS
    # the keep-hints invariant: help mode must NOT hide the per-field hints.
    assert "body.help-mode .form-hint" not in APP_CSS
    assert ".form-hint { display: none" not in APP_CSS
