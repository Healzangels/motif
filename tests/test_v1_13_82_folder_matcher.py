"""v1.13.82 — folder-matcher test coverage (no behavior change).

Per the post-v1.13.80 holistic audit, the placement folder-matching
module (`app/core/placement.py`) was reviewed by a parallel agent
and recommended as "don't refactor — leave it alone, but add 3
tests to lock in that verdict permanently":

  1. `find_target_folder` chain coverage — every branch of the
     three-step matching algorithm.
  2. `plus_mode` round-trip via `titles_equal` — the 'word' and
     'literal' modes are reachable but only 'separator' had test
     coverage. the user's library could opt into either via settings.
  3. `place_theme` with `cached_folder_path` skips the index
     entirely — pins the v1.11.68 bypass that sidesteps the
     edition-mismatch trap on orphan/SET-URL/UPLOAD-MP3 rows.

These tests are pure additions — no production code changes. They
exist so a future refactor (or accidental break of one of the
three documented architectural pivots: per-section cache v1.11.0,
cached-path bypass v1.11.68, three-step chain ordering) gets
caught before it ships.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from app.core.normalize import titles_equal
from app.core.placement import (
    FolderEntry, FolderIndex, MatchResult,
    PlacementOutcome, find_target_folder, place_theme,
)


# ── (1) find_target_folder chain coverage ────────────────────

def _index_with(*entries: FolderEntry) -> FolderIndex:
    """Build a FolderIndex by directly populating its dicts.
    Bypasses _index_root's filesystem walk so the test pins the
    matching algorithm independently from the iterdir step."""
    from app.core.normalize import normalize_edition, normalize_title
    idx = FolderIndex()
    for e in entries:
        idx.by_full_name[e.path.name] = e
        tn = normalize_title(e.title)
        ed = normalize_edition(e.edition_raw)
        if e.year:
            idx.by_title_year_edition[(tn, e.year, ed)] = e
        idx.by_title_edition[(tn, ed)].append(e)
    return idx


def test_find_target_folder_title_year_edition_exact_hit():
    """Step 1 of the chain: (title, year, edition) hits
    by_title_year_edition. Most common path."""
    e = FolderEntry(path=Path("/m/Inception (2010)"),
                    title="Inception", year="2010", edition_raw="")
    idx = _index_with(e)
    result = find_target_folder(idx, title="Inception", year="2010")
    assert result.kind == MatchResult.TITLE_YEAR_EDITION
    assert result.folder == Path("/m/Inception (2010)")


def test_find_target_folder_title_edition_unique_with_year_filter():
    """Step 2 of the chain: (title, edition) with year-filter.
    The folder has no year embedded; the requested theme has a
    year. Result: the folder still matches because the candidate
    list filters out conflicting years (folder.year='' is allowed
    by the filter at placement.py:166-167)."""
    e = FolderEntry(path=Path("/m/Adrift"),
                    title="Adrift", year="", edition_raw="")
    idx = _index_with(e)
    # No (title, '2018', '') key — falls through to step 2.
    result = find_target_folder(idx, title="Adrift", year="2018")
    assert result.kind == MatchResult.TITLE_EDITION_UNIQUE
    assert result.folder == Path("/m/Adrift")


def test_find_target_folder_year_filter_excludes_conflicting():
    """Step 2 chain branch: a candidate with a CONFLICTING year is
    filtered out. Two entries share the same normalized (title,
    edition) but different years — the requested year matches one
    and excludes the other; remaining single candidate becomes
    UNIQUE."""
    e1 = FolderEntry(path=Path("/m/Carrie (1976)"),
                     title="Carrie", year="1976", edition_raw="")
    e2 = FolderEntry(path=Path("/m/Carrie (2013)"),
                     title="Carrie", year="2013", edition_raw="")
    idx = _index_with(e1, e2)
    # No (title, '2002', '') key — falls through to step 2; both
    # candidates fail the year filter — chain returns NONE.
    result = find_target_folder(idx, title="Carrie", year="2002")
    assert result.kind == MatchResult.NONE
    assert result.folder is None


def test_find_target_folder_ambiguous_when_yearless_collision():
    """Step 2 chain branch: yearless request matches MULTIPLE
    yearless folders → AMBIGUOUS. Mirrors the v1.11.68 comment's
    edge case for orphan rows with no year metadata."""
    e1 = FolderEntry(path=Path("/m/Crash"),
                     title="Crash", year="", edition_raw="")
    e2 = FolderEntry(path=Path("/m/Crash (Different)"),
                     title="Crash", year="", edition_raw="")
    idx = _index_with(e1, e2)
    result = find_target_folder(idx, title="Crash", year="")
    assert result.kind == MatchResult.AMBIGUOUS
    assert result.folder is None
    assert result.reason and "ambiguous" in result.reason


def test_find_target_folder_strict_edition_rejects_no_edition_match():
    """Step 3 chain: strict_edition=True (default) refuses to
    fall through to a no-edition folder when the request carries
    an edition. This is the exact trap that v1.11.68's
    cached_folder_path bypass exists to dodge — orphan rows with
    SET URL pointing at an {edition-X} Plex folder."""
    e = FolderEntry(path=Path("/m/Inception (2010)"),
                    title="Inception", year="2010", edition_raw="")
    idx = _index_with(e)
    # Request carries an edition — strict_edition refuses to land
    # on the editionless folder.
    result = find_target_folder(
        idx, title="Inception", year="2010",
        edition_raw="edition-Director's Cut", strict_edition=True,
    )
    assert result.kind == MatchResult.NONE
    assert result.reason and "edition" in result.reason


def test_find_target_folder_no_match_returns_enum_string_reason():
    """v1.11.68 compat: the no_match branch must use the literal
    'no matching folder' reason string that /pending's
    last_place_attempt_reason switch keys on. Pre-fix the wrong
    string made /pending fall through to its generic message
    instead of the specific 'No Plex folder matched' branch."""
    e = FolderEntry(path=Path("/m/Inception (2010)"),
                    title="Inception", year="2010", edition_raw="")
    idx = _index_with(e)
    result = find_target_folder(idx, title="The Matrix", year="1999")
    assert result.kind == MatchResult.NONE
    assert result.reason == "no matching folder"


# ── (2) plus_mode round-trip via titles_equal ────────────────

def test_titles_equal_separator_mode_default():
    """Default plus_mode='separator': '+' becomes whitespace.
    Pin existing behavior."""
    assert titles_equal("Disc + Tape", "Disc Tape")
    # Different titles still don't match.
    assert not titles_equal("Disc + Tape", "Disc Box")


def test_titles_equal_word_mode():
    """plus_mode='word': '+' becomes ' plus '. So 'A+B' matches
    'A plus B' but NOT 'A B'. Pin the discrimination."""
    assert titles_equal("A+B", "A plus B", plus_mode="word")
    assert not titles_equal("A+B", "A B", plus_mode="word")


def test_titles_equal_literal_mode_preserves_plus():
    """plus_mode='literal': '+' is preserved through normalization
    via the __plus__ sentinel. So 'A+B' matches 'a+b' (case
    folding) but NOT 'A B' or 'A plus B'. Locks the literal-mode
    branch."""
    assert titles_equal("A+B", "a+b", plus_mode="literal")
    assert not titles_equal("A+B", "A B", plus_mode="literal")
    assert not titles_equal("A+B", "A plus B", plus_mode="literal")


def test_titles_equal_modes_are_disjoint():
    """The same input can match in one mode and not another —
    that's the whole point of having three modes. Pin the
    discrimination so a normalize.py refactor can't collapse
    them silently."""
    a, b = "Tony+Sons", "Tony plus Sons"
    assert not titles_equal(a, b, plus_mode="separator")  # '+'→' '
    assert titles_equal(a, b, plus_mode="word")            # '+'→' plus '
    assert not titles_equal(a, b, plus_mode="literal")     # '+' kept


# ── (3) place_theme cached_folder_path bypasses the index ────

def test_place_theme_cached_folder_path_skips_find_target_folder(tmp_path):
    """The v1.11.68 fast-path: when the worker pre-resolved the
    target folder via plex_items.folder_path, place_theme uses
    it directly without invoking find_target_folder. Pre-fix,
    orphan/SET-URL/UPLOAD-MP3 rows hit the FolderIndex with
    edition_raw="" and got rejected by strict_edition=True for
    every {edition-X} Plex folder.

    Test: build an index that DOES NOT contain the title (so
    non-bypass would return no_match), then call place_theme
    with cached_folder_path pointing at a real dir. Patch
    find_target_folder to assert it's never called. The
    placement must succeed."""
    # Real source file (the worker's downloaded theme).
    source = tmp_path / "theme.mp3"
    source.write_bytes(b"fake mp3 bytes")

    # Real target dir (the cached_folder_path pre-resolved via
    # plex_items.folder_path). Note: NOT in the FolderIndex.
    target_dir = tmp_path / "Inception (2010) {edition-IMAX}"
    target_dir.mkdir()

    # Empty index — find_target_folder would return no_match if
    # called. The bypass is the whole point.
    idx = _index_with()

    # Patch find_target_folder so we can assert it's not invoked.
    with patch("app.core.placement.find_target_folder") as mock_find:
        outcome = place_theme(
            media_type="movie",
            title="Inception",
            year="2010",
            edition_raw="",  # the trap: empty edition, strict mode
            source_file=source,
            index=idx,
            plex=None,
            cached_folder_path=str(target_dir),
            strict_edition=True,
        )
    # Bypass fired — find_target_folder never called.
    mock_find.assert_not_called()
    # Placement landed at the cached path, not via index lookup.
    assert outcome.placed
    assert outcome.target_folder == target_dir
    assert outcome.reason == "placed"
    # File actually placed on disk.
    assert (target_dir / "theme.mp3").exists()


def test_place_theme_falls_back_to_find_target_folder_when_no_cached(tmp_path):
    """Inverse of the bypass: with no cached_folder_path, the
    place falls through to find_target_folder. Pin the contract
    that the bypass is OPT-IN — the matcher still runs by
    default. Catches a regression that would silently drop the
    fallback path."""
    source = tmp_path / "theme.mp3"
    source.write_bytes(b"fake")
    target_dir = tmp_path / "Test (2020)"
    target_dir.mkdir()

    e = FolderEntry(path=target_dir, title="Test", year="2020",
                    edition_raw="")
    idx = _index_with(e)

    with patch(
        "app.core.placement.find_target_folder",
        return_value=type("R", (), {
            "kind": MatchResult.TITLE_YEAR_EDITION,
            "folder": target_dir,
            "reason": None,
        })(),
    ) as mock_find:
        outcome = place_theme(
            media_type="movie",
            title="Test",
            year="2020",
            edition_raw="",
            source_file=source,
            index=idx,
            plex=None,
            cached_folder_path=None,  # no bypass
        )
    mock_find.assert_called_once()
    assert outcome.placed
    assert outcome.target_folder == target_dir
