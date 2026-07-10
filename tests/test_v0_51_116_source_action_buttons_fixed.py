"""v0.51.116 — SOURCE / PLACE action-button colors stay FIXED across themes.

Each `.btn.lib-source-X` menu-item color ENCODES the action's source / target
kind — green = ThemerrDB (T), violet = user URL (U), cyan = adopt / PU, amber =
Plex (P), green = hardlink placement (HL). It mirrors the FIXED SRC/LINK pill of
the state the action lands the row in ("visual at-a-glance link between the
action and its result"). So the button color MUST stay fixed on every theme,
exactly like the pill — otherwise (the user) the same source reads as two colors
(e.g. RE-DOWNLOAD TDB gold on the Plex theme beside a still-green T pill).

Pre-v0.51.116 the themerrdb + place_file tones rode --green* (which v0.51.108
aliased to the themeable --accent); the others were already fixed. This guard
locks the WHOLE family to fixed semantic tokens so a future edit can't re-theme
a source-encoded action button.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _rule(selector: str) -> str:
    i = APP.index(selector)
    return APP[i:APP.index("}", i) + 1]


def test_source_action_buttons_use_fixed_tokens():
    # every SOURCE/PLACE menu action button is colored by its source/target
    # kind and must NOT ride the themeable --green/--accent aliases.
    expected = {
        ".btn.lib-source-themerrdb {": "--src-t",   # T source (green)
        ".btn.lib-source-place_file {": "--ok",      # HL placement (green)
        ".btn.lib-source-user {": "--violet",        # U source
        ".btn.lib-source-adopt {": "--cyan",         # A source
        ".btn.lib-source-plex {": "--amber",         # P source
        ".btn.lib-source-place_api {": "--cyan",      # PU placement
    }
    for selector, tok in expected.items():
        rule = _rule(selector)
        assert f"var({tok}" in rule, f"{selector} should use the fixed {tok} token"
        assert "var(--green" not in rule and "var(--accent" not in rule, (
            f"{selector} rides a themeable alias — its color encodes the action's "
            "SOURCE and must stay fixed across themes, like the pill it mirrors")


def test_themerrdb_button_matches_the_T_pill_token():
    # the RE-DOWNLOAD TDB / // THEMERRDB action must use the EXACT token the
    # SRC=T row pill uses, so the action and its result never diverge in hue.
    btn = _rule(".btn.lib-source-themerrdb {")
    pill = _rule(".link-badge-themerrdb {")
    assert "var(--src-t-bright)" in btn
    assert "var(--src-t-bright)" in pill
