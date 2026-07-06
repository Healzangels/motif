"""v0.51.89 — settings // CLEAR KEY / // CLEAR PROXY buttons actually work.

The clear-secret handler was hard-coded `if (path !== 'plex.token') return;`, so
the // CLEAR KEY (plex.tmdb_api_key) and // CLEAR PROXY (downloads.proxy_url)
buttons silently no-op'd. Combined with the backend keep-on-empty contract (a
blank save = keep existing), there was NO way to wipe those two secrets — while
the proxy hint text literally says "Click CLEAR PROXY to wipe". Generalized the
handler to PATCH {section: {field: null}} for any data-cfg-clear path; the
backend already clears those paths on null (proven by test_apply_partial_config).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


def _clear_handler_block() -> str:
    i = APP_JS.index("querySelectorAll('[data-cfg-clear]')")
    return APP_JS[i:i + 900]


def test_clear_handler_is_not_plex_token_only():
    block = _clear_handler_block()
    assert "if (path !== 'plex.token') return;" not in block, (
        "v0.51.89: the CLEAR handler must not hard-return for non-plex.token "
        "paths — that made // CLEAR KEY + // CLEAR PROXY dead controls")


def test_clear_handler_patches_section_field_null_generically():
    block = _clear_handler_block()
    # splits "section.field" and clears via null (the backend's clear signal).
    assert "path.slice(0, dot)" in block and "path.slice(dot + 1)" in block
    assert "[section]: { [field]: null }" in block


def test_clear_buttons_exist_for_tmdb_and_proxy():
    # the two buttons that were dead must still be present + wired to real paths.
    assert 'data-cfg-clear="plex.tmdb_api_key"' in SETTINGS_HTML
    assert 'data-cfg-clear="downloads.proxy_url"' in SETTINGS_HTML
    assert 'data-cfg-clear="plex.token"' in SETTINGS_HTML  # the one that worked
