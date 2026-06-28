"""v1.20.48 — token-discipline nits (CSS audit bundle 3/4).

The 2026-05-30 audit found three off-token values in app.css:

1. `.lib-flag-pill-4k.is-active` used `border-color: #ffb84a` — literally
   var(--amber) — while its own adjacent lines already used
   var(--amber-rgb). Now var(--amber).
2/3. Two glow/shadow sites used raw `rgba(255,212,122, …)` = --amber-bright
   channels, paired with an adjacent `var(--amber-bright)`. v1.15.113
   added --amber-rgb but never the -bright variant, so there was no token
   to use. Added `--amber-bright-rgb` and migrated both sites.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_amber_bright_rgb_token_defined():
    assert "--amber-bright-rgb: 255, 212, 122;" in APP_CSS


def test_no_raw_amber_hex_outside_token_def():
    """The only #ffb84a allowed is the --amber token definition itself.
    Any other occurrence is an un-tokenized literal."""
    # Strip the single sanctioned definition line, then assert clean.
    without_def = APP_CSS.replace("--amber: #ffb84a;", "")
    assert "#ffb84a" not in without_def, (
        "#ffb84a outside the --amber token def must be var(--amber)"
    )


def test_no_raw_amber_bright_rgba_channels():
    """Both glow sites must route through the token, not raw channels."""
    assert "rgba(255,212,122" not in APP_CSS
    assert "rgba(255, 212, 122" not in APP_CSS


def test_glow_sites_use_amber_bright_token():
    assert "rgba(var(--amber-bright-rgb),0.6)" in APP_CSS
    assert "rgba(var(--amber-bright-rgb),0.4)" in APP_CSS


def test_v1_20_48_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
