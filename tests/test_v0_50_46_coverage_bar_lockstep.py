"""v0.50.46 — dashboard coverage bar-fill no longer lags the % count-up.

The COVERAGE cards (MOVIES/TV/ANIME/COLLECTIONS THEMED) SSR-bake + count-up the %,
but the bar below was only written by setCov when the async coverage poll resolved
— so it filled visibly late (the user: "the bar shows up late and looks funny").

Fix mirrors the v0.50.6 % pattern:
  - SSR-bake the bar width inline so it paints filled (reduced-motion users keep it
    static — "already displayed");
  - the pre-paint inline script resets it to 0 (transition off) for motion users;
  - dashCountUp drives the bar in lockstep with the % climb (transition off during
    the rAF, restored after) so they fill at the same rate;
  - setCov defers the bar write while data-countup is pending so the live poll
    doesn't snap the bar ahead of the animation.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_ssr_bakes_each_bar_width():
    assert "macro ssr_pct_width(num, denom)" in DASH
    # all four coverage bars carry an SSR-baked inline width
    for key, num, denom in [
        ("movies", "plex_movies_with_theme", "plex_movies_total"),
        ("tv", "plex_tv_with_theme", "plex_tv_total"),
        ("anime", "plex_anime_with_theme", "plex_anime_total"),
        ("collections", "plex_collections_with_theme", "plex_collections_total"),
    ]:
        marker = (f'data-bar-fill="{key}" '
                  f'style="width: {{{{ ssr_pct_width(_ssr_dash.{num}, '
                  f'_ssr_dash.{denom}) }}}}%"')
        assert marker in DASH, key


def test_prepaint_resets_bar_for_motion_users():
    # the count-up reset block also zeroes the bar (transition off) so it climbs
    # from 0 instead of jumping; reduced-motion users return before this.
    assert "[data-bar-fill=\"' + k + '\"]" in DASH
    assert "bar.style.transition = 'none'; bar.style.width = '0%'" in DASH


def test_dashcountup_drives_the_bar_in_lockstep():
    i = APP_JS.index("function dashCountUp()")
    body = APP_JS[i:i + 1500]
    # find the matching bar from the cov-<key>-pct id
    assert "match(/^cov-(.+)-pct$/)" in body
    # transition off during the climb, width tracks the eased value, restored after
    assert "bar.style.transition = 'none'" in body
    assert "bar.style.width = `${target * eased}%`" in body
    assert "bar.style.width = `${target}%`; bar.style.transition = ''" in body


def test_setcov_defers_bar_write_during_countup():
    # the live poll must not snap the bar ahead of the synchronized climb
    assert ("if (bar && !(pctEl && pctEl.dataset.countup)) bar.style.width"
            in APP_JS)
