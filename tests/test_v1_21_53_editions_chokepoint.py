"""v1.21.53 — per-edition theme isolation, Phase B1 (the chokepoint).

Lands app/core/editions.py: the single module that derives the
`edition_key` discriminator (schema v63) from a folder / rating_key. Dead
code until B2 wires the worker write sites through it — these tests pin the
contract B2 will rely on.

The load-bearing guarantee is PARITY: edition_key_for_basename must equal
placement.py's FolderEntry.edition_norm derivation AND the v62 placements
backfill derivation, so motif's state keys and the placement matcher agree
on what edition a folder is. A drift here is the silent-wrong-edition
class (an Extended URL written onto Theatrical).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.editions import (
    edition_key_for_basename,
    edition_key_for_folder,
    edition_key_for_rating_key,
)
from app.core.normalize import normalize_edition, parse_folder_name


# ── edition_key_for_basename: the canonical derivation ──


def test_basename_standard_is_empty():
    assert edition_key_for_basename("The Matrix (1999)") == ""


def test_basename_empty_input_is_empty():
    assert edition_key_for_basename("") == ""


def test_basename_theatrical_tag():
    assert edition_key_for_basename(
        "The Lord of the Rings (2001) {edition-Theatrical}") == "theatrical"


def test_basename_extended_tag():
    assert edition_key_for_basename(
        "The Lord of the Rings (2001) {edition-Extended}") == "extended"


def test_basename_multiword_edition_normalizes_to_spaces():
    # the user's actual custom edition — normalize lowercases + collapses.
    assert edition_key_for_basename(
        "LotR (2001) {edition-Sam Takes a Step}") == "sam takes a step"


def test_basename_guid_tag_is_not_an_edition():
    # {imdb-...}/{tmdb-...} are scanner GUID hints, NOT editions (v1.10.25).
    assert edition_key_for_basename(
        "The Matrix (1999) {imdb-tt0133093}") == ""


def test_basename_4k_tag():
    assert edition_key_for_basename("Dune (2021) {edition-4K}") == "4k"


# ── edition_key_for_folder: basename-of-path ──


def test_folder_takes_basename_of_path():
    assert edition_key_for_folder(
        "/data/Movies/LotR (2001) {edition-Extended}") == "extended"


def test_folder_none_and_empty_are_empty():
    assert edition_key_for_folder(None) == ""
    assert edition_key_for_folder("") == ""


def test_folder_trailing_slash_handled():
    assert edition_key_for_folder(
        "/data/Movies/Dune (2021) {edition-IMAX}/") == "imax"


def test_folder_standard_path_is_empty():
    assert edition_key_for_folder("/data/Movies/The Matrix (1999)") == ""


# ── edition_key_for_rating_key: rk -> plex_items.folder_path -> key ──


def _seed_item(conn, rk, folder_path):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, title,"
        " folder_path, first_seen_at, last_seen_at)"
        " VALUES (?, '1', 'movie', 'X', ?, 't', 't')", (rk, folder_path))


def test_rating_key_resolves_through_folder_path(tmp_path):
    p = tmp_path / "m.db"
    init_db(p)
    with sqlite3.connect(p) as conn:
        _seed_item(conn, "rk-std", "/data/Movies/LotR (2001)")
        _seed_item(conn, "rk-ext",
                   "/data/Movies/LotR (2001) {edition-Extended}")
        conn.commit()
        assert edition_key_for_rating_key(conn, "rk-std") == ""
        assert edition_key_for_rating_key(conn, "rk-ext") == "extended"


def test_rating_key_unknown_is_empty(tmp_path):
    p = tmp_path / "m.db"
    init_db(p)
    with sqlite3.connect(p) as conn:
        assert edition_key_for_rating_key(conn, "nope") == ""


def test_rating_key_none_or_empty_is_empty(tmp_path):
    p = tmp_path / "m.db"
    init_db(p)
    with sqlite3.connect(p) as conn:
        assert edition_key_for_rating_key(conn, None) == ""
        assert edition_key_for_rating_key(conn, "") == ""


def test_rating_key_collection_empty_folder_is_empty(tmp_path):
    # Collections have folder_path='' (the plex_upload sentinel).
    p = tmp_path / "m.db"
    init_db(p)
    with sqlite3.connect(p) as conn:
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " title, folder_path, first_seen_at, last_seen_at)"
            " VALUES ('rk-coll','1','collection','C','','t','t')")
        conn.commit()
        assert edition_key_for_rating_key(conn, "rk-coll") == ""


# ── PARITY: the chokepoint must equal placement.py + the v62 backfill ──


def test_parity_with_placement_edition_norm():
    """edition_key_for_basename must equal the placement matcher's
    edition_norm (normalize_edition(parse_folder_name(name).editions_raw))
    for every shape — they MUST agree or state + placement disagree."""
    names = [
        "The Matrix (1999)",
        "LotR (2001) {edition-Theatrical}",
        "LotR (2001) {edition-Extended}",
        "Dune (2021) {edition-4K}",
        "Movie (2020) {edition-Director's Cut}",
        "Show (2019) {imdb-tt123}",
        "Weird {edition-A} {edition-B}",
        "No Year Title",
    ]
    for n in names:
        expected = normalize_edition(parse_folder_name(n).editions_raw)
        assert edition_key_for_basename(n) == expected, n


def test_parity_with_v62_placements_backfill():
    """The v62 backfill derived edition_key as
    normalize_edition(parse_folder_name(Path(folder).name).editions_raw).
    edition_key_for_folder must reproduce that exactly."""
    folder = "/data/Movies/LotR (2001) {edition-Extended}"
    backfill = normalize_edition(
        parse_folder_name(Path(folder).name).editions_raw)
    assert edition_key_for_folder(folder) == backfill
