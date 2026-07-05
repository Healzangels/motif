"""v0.51.77 — canonical_theme_subdir digit-validates `year` (path-traversal DiD).

Security-audit accepted-risk item, landed as cheap defense-in-depth. In
canonical.py the theme staging folder is f"{sanitize(title)} ({year})" — title
and edition_key are sanitized but `year` was interpolated RAW. A malicious Plex
server (the only uncapped `year` source; ThemerrDB caps it to 4 chars) supplying
a crafted year like "2020)/../../etc/(x" would escape themes_dir. v0.51.77 coerces
any non-4-digit year to no-year, so the folder can never leave the sandbox via the
year field. (Not real-world exploitable under the trusted-Plex threat model — the
outcome was a constrained fixed-name write, not RCE — but a cheap one-line close.)
"""
from __future__ import annotations

import pytest

from app.core.canonical import canonical_theme_subdir


@pytest.mark.parametrize("year", [
    "2020)/../../etc/(x",   # the traversal PoC
    "../../evil",
    "2020/2021",
    "20200",               # 5 digits
    "202",                 # 3 digits
    "abcd",
    "2020 ",               # trailing junk (stripped → still must be exactly 4 digits)
    "(2020)",
])
def test_malformed_year_is_dropped(year):
    out = canonical_theme_subdir("Movie", year, "")
    assert ".." not in out and "/" not in out and "\\" not in out
    assert out == "Movie", f"malformed year must be dropped, got {out!r}"


@pytest.mark.parametrize("year,expected", [
    ("2020", "Movie (2020)"),
    (2020, "Movie (2020)"),      # int accepted
    ("1999", "Movie (1999)"),
    (None, "Movie"),
    ("", "Movie"),
])
def test_valid_year_preserved(year, expected):
    assert canonical_theme_subdir("Movie", year, "") == expected


def test_edition_still_appends_with_valid_year():
    out = canonical_theme_subdir("Movie", "2020", "extended")
    assert out == "Movie (2020) {edition-extended}"
