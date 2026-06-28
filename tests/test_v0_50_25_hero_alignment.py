"""v0.50.25 — consistent hero height so content tops align across tabs.

the user: the library search bar / dashboard // RECENTLY ADDED start at different
Y per tab; a .hero min-height floor reserves the same header height everywhere so
the first content block lines up when tabbing.
"""
from pathlib import Path

APP_CSS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.css").read_text()


def test_hero_has_a_min_height_floor():
    i = APP_CSS.index(".hero { margin-bottom")
    rule = APP_CSS[i:APP_CSS.index("}", i)]
    assert "min-height:" in rule
    # min-height, not a fixed height, so a hero can still grow when it needs to.
    assert "height: 150px" not in rule.replace("min-height", "")
