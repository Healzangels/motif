"""v0.51.342 — every settings save button reports into a status span.

The save handler writes 'saving…' / '✓ saved' / '✗ <error>' into
document.querySelectorAll(`[data-save-status="${btn.dataset.save}"]`) — an exact
attribute match. // SAVE DOWNLOADS (data-save="downloads loudness") pointed at a
span marked "downloads", so every result of that save, the v0.51.189-.341 400
included, rendered nowhere.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
# Comments are stripped first: the SAVE DOWNLOADS note itself quotes a bare data-save="downloads".
HTML = re.sub(r"\{#.*?#\}|<!--.*?-->", "", (REPO / "app" / "web" / "templates" / "settings.html").read_text(), flags=re.S)
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_every_save_button_has_a_status_span_with_its_exact_value():
    saves = set(re.findall(r'data-save="([^"]+)"', HTML))
    statuses = set(re.findall(r'data-save-status="([^"]+)"', HTML))
    assert saves, "no data-save buttons found — the settings template moved"
    silent = sorted(saves - statuses)
    assert not silent, (
        f"save button(s) {silent} have no data-save-status span with the same value — "
        f"their saving / saved / error text would render nowhere")


def test_the_save_handler_still_matches_status_spans_by_the_buttons_exact_value():
    """The invariant above only holds while the handler selects by the button's own value."""
    assert "const tab = btn.dataset.save;" in APP_JS
    assert '[data-save-status="${tab}"]' in APP_JS
