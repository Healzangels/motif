"""v1.14.94 — `:` in titles becomes ` - ` (space-dash-space) for the
download folder name.

the user: "the folders that are being created in the downloads
locations are formatted as Terminator 2- Judgment Day (1991)
or The Texas Chainsaw Massacre- The Beginning (2006) or
Yu-Gi-Oh!- The Dark Side of Dimensions (2016) while on plex
they are Terminator 2 Judgment Day (1991) or The Texas
Chainsaw Massacre - The Beginning (2006) or Yu-Gi-Oh! - The
Dark Side of Dimensions (2016) ... it looks like it's the :
causing the download folder to have the - right away. Can we
make it so it space - space"

## Root cause

`sanitize_for_filesystem` (v1.10.23) does a per-character
`_FS_BAD` → `-` replacement. For `Title: Subtitle`, the colon
becomes `-` but the space after it survives unchanged and no
space gets added before the dash → `Title- Subtitle`.

The v1.10.23 docstring already states the intent: mirror Plex's
`Mission - Impossible` form. But the per-char approach was too
crude to handle the surrounding-space convention.

## Fix

Pre-process the input by replacing `:` with ` - ` (with
surrounding spaces) BEFORE the per-char loop. The existing
`re.sub(r"\\s+", " ", ...)` collapse step normalizes any
double-spaces produced when the colon was already preceded
or followed by a space. Other `_FS_BAD` chars (slash,
asterisk, etc.) keep the bare `-` replacement — they don't
have the same Plex-folder-convention precedent.
"""
from __future__ import annotations

from app.core.canonical import (
    canonical_theme_subdir,
    sanitize_for_filesystem,
)


# ── The the user repros ──────────────────────────────────────────


def test_terminator_2_judgment_day():
    assert sanitize_for_filesystem("Terminator 2: Judgment Day") \
        == "Terminator 2 - Judgment Day"


def test_texas_chainsaw_massacre_the_beginning():
    assert sanitize_for_filesystem(
        "The Texas Chainsaw Massacre: The Beginning"
    ) == "The Texas Chainsaw Massacre - The Beginning"


def test_yu_gi_oh_dark_side_of_dimensions():
    """Hyphens already in the title (Yu-Gi-Oh!) must be preserved
    — the colon-to-dash replacement only fires on `:`."""
    assert sanitize_for_filesystem(
        "Yu-Gi-Oh!: The Dark Side of Dimensions"
    ) == "Yu-Gi-Oh! - The Dark Side of Dimensions"


def test_canonical_theme_subdir_includes_year_with_space_dash():
    """End-to-end: the public canonical_theme_subdir caller (the
    download/adopt/api code paths) emits the corrected folder
    name with the year suffix."""
    assert canonical_theme_subdir("Terminator 2: Judgment Day", "1991") \
        == "Terminator 2 - Judgment Day (1991)"
    assert canonical_theme_subdir(
        "Yu-Gi-Oh!: The Dark Side of Dimensions", "2016"
    ) == "Yu-Gi-Oh! - The Dark Side of Dimensions (2016)"


# ── Edge cases ──────────────────────────────────────────────────


def test_colon_with_no_following_space_still_gets_space_dash():
    """Source titles without a space after the colon should still
    end up with the canonical ` - ` form — Plex normalizes the
    same way."""
    assert sanitize_for_filesystem("Title:Subtitle") \
        == "Title - Subtitle"


def test_multiple_colons_each_become_space_dash():
    """Each colon converts independently — composition stays
    sane even with subtitle-of-subtitle titles."""
    assert sanitize_for_filesystem("A: B: C") == "A - B - C"


def test_no_colon_unchanged():
    """Titles without colons are unaffected — the v1.10.23 base
    behavior is preserved."""
    assert sanitize_for_filesystem("Plain Title") == "Plain Title"
    assert sanitize_for_filesystem("Title (1999)") == "Title (1999)"


def test_other_fs_bad_chars_still_get_bare_dash():
    """Slash, asterisk, question mark, etc. keep the bare `-`
    replacement — only `:` carries the surrounding-space
    Plex-folder convention. The other chars don't appear in
    well-formed titles routinely; the bare `-` matches the
    pre-fix shape for compatibility with already-downloaded
    folders that may contain them."""
    assert sanitize_for_filesystem("a/b") == "a-b"
    assert sanitize_for_filesystem("a*b") == "a-b"
    assert sanitize_for_filesystem("a?b") == "a-b"
    assert sanitize_for_filesystem('a"b') == "a-b"


def test_collapse_handles_redundant_whitespace_from_colon_replace():
    """When the colon sits between spaces in the source string,
    the replace produces ` -  ` (double-space after the dash);
    the existing `\\s+` collapse normalizes back to single
    spaces."""
    assert sanitize_for_filesystem("Title : Subtitle") \
        == "Title - Subtitle"


def test_empty_string_unchanged_default():
    """The empty-string default ('untitled') still applies."""
    assert sanitize_for_filesystem("") == "untitled"
    assert sanitize_for_filesystem(None) == "untitled"


def test_v1_14_94_marker_explains_the_colon_handling():
    """A v1.14.94 marker on the function explains WHY colons
    are special-cased so a future cleanup pass doesn't
    re-merge into the per-char path."""
    from pathlib import Path
    src = (
        Path(__file__).resolve().parent.parent
        / "app" / "core" / "canonical.py"
    ).read_text()
    assert "v1.14.94" in src
    assert "space-dash-space" in src or "Mission - Impossible" in src
