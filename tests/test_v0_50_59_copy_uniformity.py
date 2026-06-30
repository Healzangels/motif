"""v0.50.59 — hover/confirm copy uniformity (the decisions the user made).

Pins the few normalizations most likely to silently regress: the config-key leak
into confirm copy, the IMDb casing, and the SRC T-chip tooltip (brand sentence-
initial 'Motif' + terminal period). Soft convention overall, so the guard is narrow.
"""
from __future__ import annotations
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_no_config_key_themes_dir_in_user_copy():
    # the user-facing path token is /themes, never the config key themes_dir
    assert "your themes_dir" not in APP_JS


def test_dry_run_disable_confirm_has_no_redundant_immediately():
    assert "placements will resume immediately." not in APP_JS
    assert "placements will resume." in APP_JS


def test_imdb_casing_in_match_tooltip():
    assert "not IMDb — verify before apply." in APP_JS
    assert "not IMDB — verify" not in APP_JS


def test_src_t_tooltip_brand_initial_and_period():
    # sentence-initial brand capitalized + terminal period (the two taste calls)
    assert 'title="Motif manages from ThemerrDB."' in APP_JS
    assert 'title="motif manages from ThemerrDB"' not in APP_JS
