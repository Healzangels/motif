"""v1.13.55: regression guard for the edition-extraction logic.

Pins the regex used by api_item to surface section_context.edition
chips in the info card. The v1.13.51 fallback to plex_items.folder_path
when no placement carries an {edition-X} tag closes the gap where
unplaced rows showed an edition pill on the row but no edition chip
in the info card.

Frontend equivalent: parseEditionFromFolderPath() at app.js:105 reads
the same {edition-X} tag from pi.folder_path. Both sides need to
agree on the regex.
"""
from __future__ import annotations

import re

import pytest


# Same pattern as both api.py:6942 (server-side) and the JS regex
# at app.js:112 (client-side). Pinning here so a regex tweak on
# either side breaks loudly.
EDITION_PATTERN = re.compile(r"\{edition-([^}]+)\}")


@pytest.mark.parametrize("folder, expected", [
    # Standard Plex edition tags.
    ("/data/media/movies/1408 (2007) {edition-Director's Cut}",
     "Director's Cut"),
    ("/data/media/movies/Blade Runner (1982) {edition-Final Cut}",
     "Final Cut"),
    ("/data/media/movies/The Matrix (1999) {edition-IMAX}", "IMAX"),
    # Multi-word with spaces.
    ("/Movies/Lord of the Rings: Fellowship (2001) {edition-Extended Edition}",
     "Extended Edition"),
    # Trailing GUID tag after edition (Plex stacks them).
    ("/Movies/1408 (2007) {edition-Director's Cut} {imdb-tt0450385}",
     "Director's Cut"),
    # Edition AFTER GUID tag — still extracted (regex is greedy
    # within the brace pair, scans the whole string).
    ("/Movies/1408 (2007) {imdb-tt0450385} {edition-Director's Cut}",
     "Director's Cut"),
    # Numeric edition (4K Movies / 4K UHD).
    ("/Movies/Inception (2010) {edition-4K}", "4K"),
])
def test_extracts_edition_from_folder(folder, expected):
    m = EDITION_PATTERN.search(folder)
    assert m is not None
    assert m.group(1) == expected


@pytest.mark.parametrize("folder", [
    # No edition tag.
    "/data/media/movies/Inception (2010)",
    "/data/media/movies/Inception (2010) {imdb-tt1375666}",
    "/Movies/Casablanca (1942)",
    # Empty folder path / None.
    "",
])
def test_no_edition_tag_returns_none(folder):
    m = EDITION_PATTERN.search(folder)
    assert m is None


def test_edition_with_special_chars():
    """Plex's {edition-...} is liberal about contents — apostrophes,
    spaces, dashes. Only the closing brace terminates the match."""
    cases = [
        "{edition-Theatrical Cut}",
        "{edition-Director's Cut: Special}",
        "{edition-3D-Re-release}",
    ]
    expected = [
        "Theatrical Cut",
        "Director's Cut: Special",
        "3D-Re-release",
    ]
    for s, e in zip(cases, expected):
        m = EDITION_PATTERN.search(s)
        assert m is not None
        assert m.group(1) == e


def test_first_match_wins_when_multiple_editions():
    """A folder with two {edition-} tags (rare; would be a Plex
    config error) returns the FIRST match — same semantics as the
    api_item loop's break-on-first."""
    folder = "/Movies/Weird (2024) {edition-A} {edition-B}"
    m = EDITION_PATTERN.search(folder)
    assert m is not None
    assert m.group(1) == "A"
