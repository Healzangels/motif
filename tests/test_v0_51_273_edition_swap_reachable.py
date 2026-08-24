"""v0.51.273 — the edition carry-over is reachable for the case it was built for.

The fan-out review's top finding: the tier classifier's `other_fallback` arm
matches the reaped row's OWN local_files and dead placement (edition-scoped
`IN (?, '')`), and tier-3 `continue`d before candidacy — so a motif-downloaded,
PLACED theme whose edition was swapped never reached the v0.51.271 resolver at
all. The carry-over only fired for tier-1 (backup) and tier-2 (sidecar) rows.

It survived two tags of tests because the only wiring test was a source-text
pin (`assert "resolve_edition_swap(" in block`) while every behavioral test
called the resolver directly — verbatim the v1.18.81 phantom-fix sub-pattern.
So the load-bearing test here drives the REAL pipe: `_upsert_items` twice (the
v0.51.128 grace threshold is 2 consecutive misses), from Plex item list through
reap → tier classifier → dispatch loop → resolver, asserting the carry-over on
the other side and that tier-3's historical silence held (no dispatch).

Also in this tag, the review's smaller resolver findings:
  - a malformed local_files.file_path (absolute, or fewer than 3 components) is
    refused before anything is derived from it — pathlib DISCARDS the left side
    when joining an absolute right side, so a corrupted row would have sent
    every exists/mkdir/replace into a foreign tree;
  - the survivor lookup counts LIVE rows only (consecutive_missing == 0),
    mirroring _do_place's v0.51.253 idiom — a mid-grace row neither vetoes as a
    phantom sibling nor gets a theme carried onto it right before its reap;
  - the '' title-global override is left keyed at the lost edition when ANOTHER
    section still carries that edition (the one-row residual of .272's F3).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

MT, TID, SEC = "movie", 273001, "1"
NOW = "2026-08-23T00:00:00+00:00"
OLD_REL = "movies/Twilight (2008) {edition-extended}/theme.mp3"


def _seed_base(db, themes):
    from app.core.db import get_conn, init_db, transaction
    init_db(db)
    (themes / "movies" / "Twilight (2008) {edition-extended}").mkdir(parents=True)
    (themes / OLD_REL).write_bytes(b"ID3theme")
    with get_conn(db) as conn, transaction(conn):
        for sec, sub in (("1", "movies"), ("18", "movies-4k")):
            conn.execute(
                """INSERT INTO plex_sections (section_id, title, type, is_anime,
                     is_4k, themes_subdir, included, discovered_at, last_seen_at)
                   VALUES (?, 'Movies', 'movie', 0, ?, ?, 1, ?, ?)""",
                (sec, int(sec == "18"), sub, NOW, NOW))
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title, year,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES (?, ?, 'Twilight', '2008', 'themoviedb', ?, ?)""",
            (MT, TID, NOW, NOW))
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size, downloaded_at,
                 source_video_id, provenance, source_kind)
               VALUES (?, ?, ?, 'extended', ?, 'sha', 8, ?, '', 'auto', 'themerrdb')""",
            (MT, TID, SEC, OLD_REL, NOW))
        conn.execute(
            """INSERT INTO placements (media_type, tmdb_id, section_id,
                 media_folder, placed_at, placement_kind, edition_key)
               VALUES (?, ?, ?, '/data/movies/Twilight (2008) {edition-Extended}',
                       ?, 'hardlink', 'extended')""",
            (MT, TID, SEC, NOW))
        # the OLD edition's Plex row — Plex was serving its theme. This row will
        # vanish from the enumerated item list and reap after 2 misses.
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type, title,
                 year, guid_tmdb, edition_key, folder_path, has_theme,
                 first_seen_at, last_seen_at)
               VALUES ('rk-old', ?, 'movie', 'Twilight', '2008', ?, 'extended',
                       '/data/movies/Twilight (2008) {edition-Extended}', 1, ?, ?)""",
            (SEC, TID, NOW, NOW))


def _survivor_item():
    from app.core.plex import PlexLibraryItem
    return PlexLibraryItem(
        rating_key="rk-new", section_id=SEC, media_type="movie",
        title="Twilight", year="2008", guid_imdb=None, guid_tmdb=TID,
        guid_tvdb=None, folder_path="/data/movies/Twilight (2008)",
        has_theme=False)


@pytest.fixture
def env(tmp_path, monkeypatch):
    themes = tmp_path / "data" / "themes"
    themes.mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    db = tmp_path / "motif.db"
    _seed_base(db, themes)
    # The dispatch block constructs Settings() itself, and Settings' default
    # config dir is captured from the ENV AT MODULE IMPORT — in the full suite
    # app.config imported long before this fixture, so setenv alone points the
    # dispatch at the wrong config (this test passed alone and failed in-suite,
    # the discriminator that found it). Inject the instance instead.
    import app.config as cfg
    real = cfg.Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    monkeypatch.setattr(cfg, "Settings", lambda *a, **kw: real)
    dispatched = []
    import app.core.notify as notify_mod
    monkeypatch.setattr(notify_mod, "dispatch",
                        lambda *a, **kw: dispatched.append((a, kw)))
    return db, themes, dispatched


def _q(db, sql, *args):
    from app.core.db import get_conn
    with get_conn(db) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(sql, args).fetchall()


# ── THE test: the real pipe, items → reap → tier → resolver ──


def test_placed_edition_swap_carries_through_the_real_reaper(env):
    from app.core.plex_enum import _upsert_items
    db, themes, dispatched = env

    # enum 1: old edition missing (miss #1 — grace defers the reap)
    _upsert_items(db, [_survivor_item()], section_id=SEC)
    lf = _q(db, "SELECT COALESCE(edition_key,'') e FROM local_files "
                "WHERE media_type=? AND tmdb_id=?", MT, TID)
    assert [r["e"] for r in lf] == ["extended"], (
        "one miss is inside the v0.51.128 grace window — nothing may move yet")

    # enum 2: miss #2 → reap → tier-3 candidate → dispatch → resolver
    _upsert_items(db, [_survivor_item()], section_id=SEC)

    assert _q(db, "SELECT 1 FROM plex_items WHERE rating_key='rk-old'") == [], \
        "the old edition's row must have been reaped"
    lf = _q(db, "SELECT COALESCE(edition_key,'') e, file_path FROM local_files "
                "WHERE media_type=? AND tmdb_id=?", MT, TID)
    assert [(r["e"]) for r in lf] == [""], (
        "v0.51.273: the carry-over must now be REACHED through the real "
        "reaper for a motif-placed row — tier-3 candidacy was the gap")
    assert (themes / lf[0]["file_path"]).read_bytes() == b"ID3theme"
    assert _q(db, "SELECT 1 FROM placements WHERE media_type=? AND tmdb_id=? "
                  "AND COALESCE(edition_key,'')='extended'", MT, TID) == [], \
        "the dead placement is deleted (v0.51.272 F1)"
    jobs = _q(db, "SELECT job_type, status, payload FROM jobs "
                  "WHERE media_type=? AND tmdb_id=?", MT, TID)
    assert [(j["job_type"], j["status"]) for j in jobs] == [("place", "pending")]
    assert json.loads(jobs[0]["payload"] or "{}") == {}
    assert dispatched == [], (
        "tier-3 keeps its historical silence — the swap resolved, no alarm")


def test_unresolved_tier3_stays_silent_like_before(env):
    """The behavior-preservation half: tier-3 that can't swap (two survivors →
    ambiguous) must dispatch nothing, exactly as pre-.273."""
    from app.core.db import get_conn, transaction
    from app.core.plex import PlexLibraryItem
    from app.core.plex_enum import _upsert_items
    db, themes, dispatched = env
    second = PlexLibraryItem(
        rating_key="rk-dir", section_id=SEC, media_type="movie",
        title="Twilight", year="2008", guid_imdb=None, guid_tmdb=TID,
        guid_tvdb=None,
        folder_path="/data/movies/Twilight (2008) {edition-Directors Cut}",
        has_theme=False)
    for _ in range(2):
        _upsert_items(db, [_survivor_item(), second], section_id=SEC)
    lf = _q(db, "SELECT COALESCE(edition_key,'') e FROM local_files "
                "WHERE media_type=? AND tmdb_id=?", MT, TID)
    assert [r["e"] for r in lf] == ["extended"], "ambiguous → nothing moves"
    assert dispatched == [], "and tier-3 silence holds when the swap declines"


# ── the malformed-path guard ─────────────────────────────────


@pytest.mark.parametrize("shape", ["absolute", "one_part", "two_parts"])
def test_malformed_file_path_is_refused(tmp_path, shape):
    """The file EXISTS at the malformed location — that is the case the guard
    protects. Without it, an absolute row would have the resolver MOVE a file
    inside a foreign tree (pathlib discards the left side of an absolute join);
    the first draft of this test seeded no file, so `old_abs.exists()`
    short-circuited to the same None and the mutation stayed green."""
    from app.core.db import get_conn, init_db, transaction
    from app.core.edition_swap import resolve_edition_swap
    db = tmp_path / "t.db"; themes = tmp_path / "themes"; themes.mkdir()
    init_db(db)
    if shape == "absolute":
        foreign = tmp_path / "plex-media" / "Twilight (2008)"
        foreign.mkdir(parents=True)
        (foreign / "theme.mp3").write_bytes(b"FOREIGN")
        bad = str(foreign / "theme.mp3")
        watched = foreign / "theme.mp3"
    elif shape == "one_part":
        (themes / "theme.mp3").write_bytes(b"ROOTFILE")
        bad = "theme.mp3"
        watched = themes / "theme.mp3"
    else:
        (themes / "movies").mkdir()
        (themes / "movies" / "theme.mp3").write_bytes(b"TWOPART")
        bad = "movies/theme.mp3"
        watched = themes / "movies" / "theme.mp3"
    before = watched.read_bytes()
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)""",
            (SEC, NOW, NOW))
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title, year,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES (?, ?, 'Twilight', '2008', 'themoviedb', ?, ?)""",
            (MT, TID, NOW, NOW))
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size, downloaded_at,
                 source_video_id, provenance, source_kind)
               VALUES (?, ?, ?, 'extended', ?, 'sha', 8, ?, '', 'auto', 'themerrdb')""",
            (MT, TID, SEC, bad, NOW))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type, title,
                 year, guid_tmdb, edition_key, folder_path, has_theme,
                 first_seen_at, last_seen_at)
               VALUES ('rk-s', ?, 'movie', 'Twilight', '2008', ?, '', '/d', 0, ?, ?)""",
            (SEC, TID, NOW, NOW))
    assert resolve_edition_swap(db, themes, media_type=MT, tmdb_id=TID,
                                section_id=SEC, lost_edition_key="extended") is None
    assert watched.read_bytes() == before, (
        "the file at the malformed path must not be moved or altered")
    row = _q(db, "SELECT file_path, COALESCE(edition_key,'') e FROM local_files "
                 "WHERE tmdb_id=?", TID)[0]
    assert (row["file_path"], row["e"]) == (bad, "extended"), "nothing touched"


# ── the live-row filter ──────────────────────────────────────


def test_a_dying_survivor_is_not_picked(env):
    from app.core.db import get_conn, transaction
    from app.core.edition_swap import resolve_edition_swap
    db, themes, _ = env
    with get_conn(db) as conn, transaction(conn):
        conn.execute("DELETE FROM plex_items WHERE rating_key='rk-old'")
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type, title,
                 year, guid_tmdb, edition_key, folder_path, has_theme,
                 first_seen_at, last_seen_at, consecutive_missing)
               VALUES ('rk-dying', ?, 'movie', 'Twilight', '2008', ?, '',
                       '/data/movies/Twilight (2008)', 0, ?, ?, 1)""",
            (SEC, TID, NOW, NOW))
    assert resolve_edition_swap(db, themes, media_type=MT, tmdb_id=TID,
                                section_id=SEC, lost_edition_key="extended") is None, (
        "a mid-grace row is about to be reaped — carrying a theme onto it "
        "would strand the theme again one enum later")


def test_a_dying_third_sibling_does_not_veto(env):
    from app.core.db import get_conn, transaction
    from app.core.edition_swap import resolve_edition_swap
    db, themes, _ = env
    with get_conn(db) as conn, transaction(conn):
        conn.execute("DELETE FROM plex_items WHERE rating_key='rk-old'")
        for rk, ed, cm in (("rk-live", "", 0), ("rk-ghost", "directors cut", 1)):
            conn.execute(
                """INSERT INTO plex_items (rating_key, section_id, media_type,
                     title, year, guid_tmdb, edition_key, folder_path, has_theme,
                     first_seen_at, last_seen_at, consecutive_missing)
                   VALUES (?, ?, 'movie', 'Twilight', '2008', ?, ?, '/d', 0,
                           ?, ?, ?)""",
                (rk, SEC, TID, ed, NOW, NOW, cm))
    out = resolve_edition_swap(db, themes, media_type=MT, tmdb_id=TID,
                               section_id=SEC, lost_edition_key="extended")
    assert out is not None and out["to_edition"] == "", (
        "a transiently-missing third row must not turn one live survivor "
        "into a phantom ambiguity (the _do_place v0.51.253 idiom)")


# ── the '' override residual ─────────────────────────────────


def test_title_global_override_stays_when_another_section_has_the_edition(env):
    from app.core.db import get_conn, transaction
    from app.core.edition_swap import resolve_edition_swap
    db, themes, _ = env
    with get_conn(db) as conn, transaction(conn):
        conn.execute("DELETE FROM plex_items WHERE rating_key='rk-old'")
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type, title,
                 year, guid_tmdb, edition_key, folder_path, has_theme,
                 first_seen_at, last_seen_at)
               VALUES ('rk-s1', ?, 'movie', 'Twilight', '2008', ?, '', '/d', 0, ?, ?)""",
            (SEC, TID, NOW, NOW))
        # the 4K section STILL has the extended edition
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type, title,
                 year, guid_tmdb, edition_key, folder_path, has_theme,
                 first_seen_at, last_seen_at)
               VALUES ('rk-4k', '18', 'movie', 'Twilight', '2008', ?, 'extended',
                       '/d4k', 0, ?, ?)""",
            (TID, NOW, NOW))
        conn.execute(
            """INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,
                 set_at, set_by, section_id, edition_key)
               VALUES (?, ?, 'https://www.youtube.com/watch?v=globalPick1',
                       ?, 'admin', '', 'extended')""", (MT, TID, NOW))
    out = resolve_edition_swap(db, themes, media_type=MT, tmdb_id=TID,
                               section_id=SEC, lost_edition_key="extended")
    assert out is not None, "the carry itself still proceeds"
    ov = _q(db, "SELECT COALESCE(edition_key,'') e FROM user_overrides "
                "WHERE media_type=? AND tmdb_id=? AND section_id=''", MT, TID)
    assert [r["e"] for r in ov] == ["extended"], (
        "v0.51.273: the '' row is read by every section — while section 18 "
        "still HAS the extended edition, its association must not move")


def test_title_global_override_follows_when_no_other_section_has_it(env):
    from app.core.db import get_conn, transaction
    from app.core.edition_swap import resolve_edition_swap
    db, themes, _ = env
    with get_conn(db) as conn, transaction(conn):
        conn.execute("DELETE FROM plex_items WHERE rating_key='rk-old'")
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type, title,
                 year, guid_tmdb, edition_key, folder_path, has_theme,
                 first_seen_at, last_seen_at)
               VALUES ('rk-s1', ?, 'movie', 'Twilight', '2008', ?, '', '/d', 0, ?, ?)""",
            (SEC, TID, NOW, NOW))
        conn.execute(
            """INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,
                 set_at, set_by, section_id, edition_key)
               VALUES (?, ?, 'https://www.youtube.com/watch?v=globalPick1',
                       ?, 'admin', '', 'extended')""", (MT, TID, NOW))
    out = resolve_edition_swap(db, themes, media_type=MT, tmdb_id=TID,
                               section_id=SEC, lost_edition_key="extended")
    assert out is not None
    ov = _q(db, "SELECT COALESCE(edition_key,'') e FROM user_overrides "
                "WHERE media_type=? AND tmdb_id=? AND section_id=''", MT, TID)
    assert [r["e"] for r in ov] == [""], "no other section holds it → follows"


def test_v0_51_273_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
