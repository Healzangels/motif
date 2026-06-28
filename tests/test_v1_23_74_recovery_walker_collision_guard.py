"""v1.23.74 — recovery walker rejects cross-media-type title collisions.

the user's prod surfaced a single stuck `placements.theme_present=0` row:
`movie` / tmdb 16007 / section 3 (Anime) pointing at the anime *series*
folder `/data/media/anime/Death Note (2006) {tvdb-79481}` with a now-
deleted theme.mp3 and no live Plex item. Root cause: the v1.18.5 recovery
walk iterates EVERY section and claims the canonical `theme.mp3` in any
section's subdir whose `{title} ({year})` matches the themes row — with no
check that the section actually holds an item with that identity. The
live-action Death Note (2006) MOVIE (tmdb 16007) and the anime SERIES
(tmdb 13916) share a title+year, so the movie theme claimed the show's
`anime/Death Note (2006)/theme.mp3`, minting a cross-section local_files
row that a later adopt-placement walker turned into a broken placement on
the show's folder.

The fix (recovery_v55): compute the placement candidates BEFORE the
local_files append and only attribute the canonical when the section
holds a matching item OR is the theme's home media-type — so a true
orphan still recovers in its own section but a foreign-section same-named
file is left to its real owner.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"

TS = "2026-06-15T21:30:00"


def _mkfile(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"\xff\xfb\x90" + b"\x00" * 4096)


def _seed_section(conn, sid, title, sectype, is_anime, subdir):
    conn.execute(
        "INSERT INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, themes_subdir, "
        "   included, discovered_at, last_seen_at) "
        "VALUES (?, ?, ?, ?, 0, ?, 1, ?, ?)",
        (sid, title, sectype, is_anime, subdir, TS, TS),
    )


def _seed_theme(conn, media_type, tmdb_id, title, title_norm, year, src):
    return conn.execute(
        "INSERT INTO themes "
        "  (media_type, tmdb_id, title, title_norm, year, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (media_type, tmdb_id, title, title_norm, year, src, TS, TS),
    ).lastrowid


def _seed_pi(conn, rk, sid, media_type, title, title_norm, year, tmdb,
             folder, theme_id):
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, title, title_norm, year, "
        "   guid_tmdb, folder_path, local_theme_file, theme_id, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)",
        (rk, sid, media_type, title, title_norm, year, tmdb, folder,
         theme_id, TS, TS),
    )


# ── source pins ───────────────────────────────────────────────


def test_home_type_helper_exists():
    src = RECOVERY_PY.read_text()
    assert "def _section_is_home_type(" in src
    # movie themes home to movie sections, tv to show.
    fn = src[src.index("def _section_is_home_type("):]
    fn = fn[:fn.index("\ndef ")]
    assert 'section_type == "movie"' in fn and 'section_type == "show"' in fn


def test_guard_gates_attribution_before_append():
    """The skip must sit between the candidate build and the
    local_files append — reject foreign-section files before minting
    the row."""
    src = RECOVERY_PY.read_text()
    guard = "if not section_candidates and not _section_is_home_type("
    assert guard in src
    gi = src.index(guard)
    # the local_files append for this theme/section comes AFTER the guard.
    appi = src.index("lf_rows.append((", gi)
    # and the guard sits inside the same walk block as section_candidates.
    assert 0 < src.index("section_candidates = [") < gi < appi


# ── behavioral: the Death Note collision ──────────────────────


@pytest.fixture
def collision_fixture(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("app.core.recovery_v55._SIDECAR_EVIDENCE_FLOOR", 1)
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)

    movie_folder = tmp_path / "movies_plex" / "Death Note (2006)"
    show_folder = tmp_path / "anime_plex" / "Death Note (2006) {tvdb-79481}"
    with sqlite3.connect(db_path) as conn:
        _seed_section(conn, "1", "Movies", "movie", 0, "movies")
        _seed_section(conn, "3", "Anime", "show", 1, "anime")
        movie_tid = _seed_theme(conn, "movie", 16007, "Death Note",
                                "death note", "2006", "themoviedb")
        show_tid = _seed_theme(conn, "tv", 13916, "Death Note",
                               "death note", "2006", "themoviedb")
        _seed_pi(conn, "rk-movie", "1", "movie", "Death Note", "death note",
                 "2006", 16007, str(movie_folder), movie_tid)
        _seed_pi(conn, "rk-show", "3", "show", "Death Note", "death note",
                 "2006", 13916, str(show_folder), show_tid)
        conn.commit()

    # Canonical staging files in BOTH subdirs (the collision precondition:
    # each title+year resolves to a real file under the OTHER's subdir too).
    _mkfile(themes_dir / "movies" / "Death Note (2006)" / "theme.mp3")
    _mkfile(themes_dir / "anime" / "Death Note (2006)" / "theme.mp3")
    # Placed sidecars in the Plex media folders.
    _mkfile(movie_folder / "theme.mp3")
    _mkfile(show_folder / "theme.mp3")
    return db_path, themes_dir


def _rows(db_path, table, **where):
    clause = " AND ".join(f"{k}=?" for k in where)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            f"SELECT * FROM {table} WHERE {clause}", tuple(where.values())
        ).fetchall()


def test_collision_does_not_cross_attribute(collision_fixture):
    db_path, themes_dir = collision_fixture
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    stats = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    assert stats["detected"] is True

    # The movie theme is attributed ONLY to the Movies section…
    assert _rows(db_path, "local_files", media_type="movie", tmdb_id=16007,
                 section_id="1"), "movie keeps its real Movies-section row"
    assert not _rows(db_path, "local_files", media_type="movie",
                     tmdb_id=16007, section_id="3"), (
        "movie theme must NOT claim the show's anime-subdir theme.mp3"
    )
    # …and the show theme ONLY to the Anime section.
    assert _rows(db_path, "local_files", media_type="tv", tmdb_id=13916,
                 section_id="3"), "show keeps its real Anime-section row"
    assert not _rows(db_path, "local_files", media_type="tv", tmdb_id=13916,
                     section_id="1"), (
        "show theme must NOT claim the movie's movies-subdir theme.mp3"
    )

    # Placements land only on the matching folders, never the foreign one.
    mp = _rows(db_path, "placements", media_type="movie", tmdb_id=16007)
    sp = _rows(db_path, "placements", media_type="tv", tmdb_id=13916)
    assert [r["section_id"] for r in mp] == ["1"]
    assert [r["section_id"] for r in sp] == ["3"]
    assert "anime" not in mp[0]["media_folder"]
    assert "{tvdb-79481}" in sp[0]["media_folder"]


# ── behavioral: a true orphan still recovers in its home section ──


def test_orphan_still_recovers_in_home_section(tmp_path, monkeypatch):
    """The guard's home-type fallback: an orphan theme with no matching
    plex_items still gets its local_files row in a section of its own
    media-type — only FOREIGN-type sections are rejected."""
    monkeypatch.setattr("app.core.recovery_v55._SIDECAR_EVIDENCE_FLOOR", 1)
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _seed_section(conn, "1", "Movies", "movie", 0, "movies")
        _seed_section(conn, "3", "Anime", "show", 1, "anime")
        # A non-orphan theme + sidecar-bearing pi so detection fires.
        filler_tid = _seed_theme(conn, "movie", 999, "Filler", "filler",
                                 "2020", "themoviedb")
        ff = tmp_path / "movies_plex" / "Filler (2020)"
        _seed_pi(conn, "rk-filler", "1", "movie", "Filler", "filler",
                 "2020", 999, str(ff), filler_tid)
        # The orphan: a movie theme with NO plex_items anywhere.
        _seed_theme(conn, "movie", -5, "Lonely", "lonely", "2020",
                    "plex_orphan")
        conn.commit()
    _mkfile(ff / "theme.mp3")
    _mkfile(themes_dir / "movies" / "Filler (2020)" / "theme.mp3")
    # Orphan canonical exists only under the movies subdir (its home).
    _mkfile(themes_dir / "movies" / "Lonely (2020)" / "theme.mp3")

    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    maybe_recover_post_v55_data_loss(db_path, themes_dir)

    # Orphan recovered in its home (movie) section despite no pi…
    assert _rows(db_path, "local_files", media_type="movie", tmdb_id=-5,
                 section_id="1"), (
        "orphan must still recover a local_files row in its home section"
    )
    # …but never a placement (no Plex item to place onto).
    assert not _rows(db_path, "placements", media_type="movie", tmdb_id=-5)
