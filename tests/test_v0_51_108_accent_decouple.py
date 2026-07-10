"""v0.51.108 — theme presets Tag 1: accent/semantic-green decouple.

--green* used to be BOTH the brand/chrome accent AND the fixed semantic green
(SRC=T source + healthy/ok status). To let a preset retheme the accent while
pills/chips keep their meaning, the two are split:
  * --accent* — the THEMEABLE chrome accent (hex default = green). Presets
    override it (+ the canvas) pre-paint.
  * --green* — kept as ALIASES of --accent* so the ~125 chrome sites follow the
    accent with no mass-rename.
  * --src-t* / --ok* — FIXED semantic green (literal hex), never themed; the
    SRC=T identity + healthy/present status repoint here so they stay green.

These guards lock the architecture so a future edit can't silently re-couple
(e.g. point a chrome site back at a fixed token, or a pill at the themeable one).
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP = (REPO / "app" / "web" / "static" / "app.css").read_text()
OPS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
ROOT = APP[APP.index(":root"):APP.index("\n}", APP.index(":root"))]


def _rule(css: str, selector: str) -> str:
    i = css.index(selector)
    return css[i:css.index("}", i) + 1]


# ── token layer ──────────────────────────────────────────────


def test_accent_tokens_are_literal_hex_defaults():
    # the themeable accent defaults to the Fallout green literals.
    for tok, val in [("--accent", "#6dffb5"), ("--accent-bright", "#9affc9"),
                     ("--accent-deep", "#2d8c5c")]:
        assert f"{tok}: {val};" in ROOT, f"{tok} must default to {val}"
    assert re.search(r"--accent-rgb:\s*109,\s*255,\s*181;", ROOT)


def test_green_tokens_alias_the_accent():
    # chrome rides --green* which now points at the themeable accent.
    assert "--green: var(--accent);" in ROOT
    assert "--green-bright: var(--accent-bright);" in ROOT
    assert "--green-deep: var(--accent-deep);" in ROOT
    assert "--green-rgb:   var(--accent-rgb);" in ROOT
    # no literal green hex def survives (that would freeze chrome off-theme).
    assert not re.search(r"--green(-bright|-deep)?:\s*#", ROOT)


def test_fixed_semantic_green_tokens_exist_as_literals():
    # src-t (ThemerrDB source) + ok (health) are FIXED — literal, not aliased.
    for tok, val in [("--src-t", "#6dffb5"), ("--src-t-bright", "#9affc9"),
                     ("--src-t-deep", "#2d8c5c"), ("--ok", "#6dffb5"),
                     ("--ok-bright", "#9affc9"), ("--ok-deep", "#2d8c5c")]:
        assert f"{tok}: {val};" in ROOT, f"{tok} must be a fixed {val} literal"
    assert re.search(r"--src-t-rgb:\s*109,\s*255,\s*181;", ROOT)
    assert re.search(r"--ok-rgb:\s*109,\s*255,\s*181;", ROOT)


# ── semantic sites ride the FIXED tokens (stay green in every theme) ─


def test_src_t_identity_uses_fixed_token():
    assert "var(--src-t-bright)" in _rule(APP, ".link-badge-themerrdb ")
    assert "var(--src-t-bright)" in _rule(APP, ".stat-tdb-primary")
    assert "var(--src-t-bright)" in _rule(APP, ".source-pie-T-text")
    assert "--tone-tdb: var(--src-t-bright);" in OPS
    # and NOT the themeable alias.
    assert "var(--green" not in _rule(APP, ".link-badge-themerrdb ")


def test_health_status_uses_fixed_ok_token():
    assert ".state-pill.on { background: var(--ok);" in APP
    assert ".service-dot-ok { background: var(--ok); }" in APP
    assert "var(--ok)" in _rule(APP, ".link-glyph-hardlink {")
    # fidelity: .gd-on background must still equal .state-pill-btn-on color
    # (both now --ok) — the v1.23.50 glossary-mirror contract.
    assert ".gd-on { background: var(--ok); }" in APP
    assert ".state-pill-btn-on   { color: var(--ok);" in APP


# ── chrome still rides the themeable alias (follows the accent) ─────


def test_chrome_still_uses_themeable_green_alias():
    # brand wordmark + base link — must ride --green* (=> --accent), not a fixed
    # token, so they follow the theme.
    assert ".brand-name { color: var(--green-bright); }" in APP
    assert "a { color: var(--green); text-decoration: none; }" in APP


def test_action_buttons_follow_the_accent():
    # v0.51.111: the generic amber action tone (SAVE / TEST / CLEAR / REBUILD)
    # now rides the accent, so it themes instead of staying amber on a
    # Plex/Dracula/Nord canvas. Bolder than a plain .btn (accent-bright vs accent).
    assert ".btn-warn { color: var(--accent-bright); border-color: var(--accent); }" in APP
    assert ".btn-warn:hover { background: rgba(var(--accent-rgb), 0.08)" in APP
    assert "var(--amber)" not in _rule(APP, ".btn-warn ")
    # but genuinely-semantic button tones stay FIXED (meaning, not chrome).
    assert ".btn-danger { color: var(--red); border-color: var(--red); }" in APP
    assert ".btn-plex { color: var(--amber); border-color: var(--amber); }" in APP


def test_brand_equalizer_and_hero_glyph_follow_accent():
    # v0.51.113: the decorative flourishes — the topbar equalizer bars + the
    # hero-title glyph — follow the theme accent (were fixed --amber, which read
    # off-theme / Plex-ish on a non-green canvas).
    i = APP.index(".brand-mark .brand-bar {")
    assert "background: var(--accent);" in APP[i:APP.index("}", i)]
    assert ".title::before { content: '▰ '; color: var(--accent);" in APP
