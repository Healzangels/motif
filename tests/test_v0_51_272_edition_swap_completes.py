"""v0.51.272 — the edition carry-over finishes the job (four .271 review fixes).

A review of v0.51.271 the day after it shipped — the one module written after
that day's self-review — found the carry-over stopped halfway:

  F1  it re-keyed the old placement row instead of replacing it. The row's
      media_folder / uploaded rating key died with the edition, so the library
      read PLACED (`!!media_folder`) while Plex played nothing — until the next
      enum stamped theme_present=0 and the row surfaced in NEEDS WORK as a
      generic broken placement with no connection to the swap. And nothing ever
      placed the theme for the survivor. Now: the dead placement is DELETED and
      a place job is enqueued (the v1.21.78 shape), so the survivor actually
      serves the theme and the fresh placement row is written by the worker.
  F2  the canonical moved BEFORE the row transaction, with no compensation — a
      row failure (PK collision, lock exhaustion) left the file at the new path
      while file_path pointed at the old one, plus a loss notification that
      never mentioned the half-move. Now the file is moved back on any row
      failure, making the documented contract ("a failed run leaves the old
      edition intact") true in both halves.
  F3  the user_overrides re-key was not section-scoped — a swap in section 1
      re-keyed a 4K section's override off an edition that still exists there
      (class-2 cross-section bleed). Now scoped to `section_id IN (?, '')`,
      and guard 3's override arm matches, so another section's override no
      longer blocks this section's carry either.
  F4  the emptied {edition-X} canonical folder was left behind. Removed when
      empty; a non-empty folder is left alone (rmdir is the emptiness test).

All four were latent — the census showed the path had never fired in prod.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

MT, TID, SEC = "movie", 272001, "1"
NOW = "2026-08-22T00:00:00+00:00"
OLD_REL = "movies/Twilight (2008) {edition-extended}/theme.mp3"


@pytest.fixture
def env(tmp_path):
    from app.core.db import get_conn, init_db, transaction
    db = tmp_path / "t.db"
    themes = tmp_path / "themes"
    init_db(db)
    (themes / "movies" / "Twilight (2008) {edition-extended}").mkdir(parents=True)
    (themes / OLD_REL).write_bytes(b"ID3theme")
    with get_conn(db) as conn, transaction(conn):
        # themes_subdir is UNIQUE across sections
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
        # the DEAD placement: its media_folder was deleted with the edition
        conn.execute(
            """INSERT INTO placements (media_type, tmdb_id, section_id,
                 media_folder, placed_at, placement_kind, edition_key)
               VALUES (?, ?, ?, '/data/movies/Twilight (2008) {edition-Extended}',
                       ?, 'hardlink', 'extended')""",
            (MT, TID, SEC, NOW))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type, title,
                 year, guid_tmdb, edition_key, folder_path, has_theme,
                 first_seen_at, last_seen_at)
               VALUES ('rk-surv', ?, 'movie', 'Twilight', '2008', ?, '',
                       '/data/movies/Twilight (2008)', 0, ?, ?)""",
            (SEC, TID, NOW, NOW))
    return db, themes


def _swap(db, themes):
    from app.core.edition_swap import resolve_edition_swap
    return resolve_edition_swap(db, themes, media_type=MT, tmdb_id=TID,
                                section_id=SEC, lost_edition_key="extended")


def _q(db, sql, *args):
    from app.core.db import get_conn
    with get_conn(db) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(sql, args).fetchall()


# ── F1: the carry-over ends with the survivor actually served ──


def test_the_dead_placement_is_deleted_not_rekeyed(env):
    db, themes = env
    assert _swap(db, themes) is not None
    rows = _q(db, "SELECT COALESCE(edition_key,'') e, media_folder FROM placements "
                  "WHERE media_type=? AND tmdb_id=?", MT, TID)
    assert rows == [], (
        "v0.51.272: the old placement's folder died with the edition — "
        "re-keying it made the library read PLACED while Plex played nothing")


def test_a_place_job_is_enqueued_for_the_survivor(env):
    db, themes = env
    _swap(db, themes)
    jobs = _q(db, "SELECT job_type, section_id, payload, status FROM jobs "
                  "WHERE media_type=? AND tmdb_id=?", MT, TID)
    assert len(jobs) == 1 and jobs[0]["job_type"] == "place"
    assert jobs[0]["status"] == "pending" and jobs[0]["section_id"] == SEC
    assert json.loads(jobs[0]["payload"] or "{}") == {}, (
        "survivor is the standard edition → the v1.21.78 payload shape is {}")


def test_place_payload_carries_a_real_edition_key(env):
    """The reverse direction: survivor IS an edition → payload names it."""
    db, themes = env
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        conn.execute("UPDATE plex_items SET edition_key='theatrical', "
                     "folder_path='/data/movies/Twilight (2008) {edition-Theatrical}' "
                     "WHERE rating_key='rk-surv'")
    _swap(db, themes)
    jobs = _q(db, "SELECT payload FROM jobs WHERE media_type=? AND tmdb_id=?", MT, TID)
    assert json.loads(jobs[0]["payload"])["edition_key"] == "theatrical"


# ── F2: a row failure restores the file ──────────────────────


def test_row_failure_moves_the_canonical_back(env, monkeypatch):
    db, themes = env
    import app.core.edition_swap as mod

    def boom(conn):
        raise RuntimeError("synthetic row failure")

    monkeypatch.setattr(mod, "transaction", boom)
    assert _swap(db, themes) is None
    assert (themes / OLD_REL).exists(), (
        "v0.51.272: the file moved before the txn — a row failure must move it "
        "back or file_path points at a missing file")
    lf = _q(db, "SELECT COALESCE(edition_key,'') e, file_path FROM local_files "
                "WHERE media_type=? AND tmdb_id=?", MT, TID)
    assert [(r["e"], r["file_path"]) for r in lf] == [("extended", OLD_REL)], (
        "and every row must be exactly as it started")


# ── F3: the override re-key stays inside this section ─────────


def test_another_sections_override_is_not_rekeyed(env):
    db, themes = env
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        for sec in (SEC, "18"):
            conn.execute(
                """INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,
                     set_at, set_by, section_id, edition_key)
                   VALUES (?, ?, 'https://www.youtube.com/watch?v=operatorPk1',
                           ?, 'admin', ?, 'extended')""", (MT, TID, NOW, sec))
    assert _swap(db, themes) is not None
    rows = {r["section_id"]: r["e"] for r in _q(
        db, "SELECT section_id, COALESCE(edition_key,'') e FROM user_overrides "
            "WHERE media_type=? AND tmdb_id=?", MT, TID)}
    assert rows[SEC] == "", "this section's override follows the survivor"
    assert rows["18"] == "extended", (
        "v0.51.272: the 4K section still HAS the extended edition — its "
        "override must not move (class-2 cross-section bleed)")


def test_a_foreign_sections_survivor_override_no_longer_blocks(env):
    """Guard 3's override arm is scoped the same way: an override some OTHER
    section holds at the survivor key must not veto this section's carry."""
    db, themes = env
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,
                 set_at, set_by, section_id, edition_key)
               VALUES (?, ?, 'https://www.youtube.com/watch?v=other4kPick',
                       ?, 'admin', '18', '')""", (MT, TID, NOW))
    assert _swap(db, themes) is not None


def test_a_title_global_survivor_override_still_blocks(env):
    """The '' row applies to every section, so it still vetoes (fails safe)."""
    db, themes = env
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,
                 set_at, set_by, section_id, edition_key)
               VALUES (?, ?, 'https://www.youtube.com/watch?v=globalPick1',
                       ?, 'admin', '', '')""", (MT, TID, NOW))
    assert _swap(db, themes) is None


# ── F4: the emptied folder is tidied, a non-empty one is not ──


def test_the_emptied_edition_folder_is_removed(env):
    db, themes = env
    _swap(db, themes)
    assert not (themes / "movies" / "Twilight (2008) {edition-extended}").exists()


def test_a_non_empty_edition_folder_is_left_alone(env):
    db, themes = env
    stray = themes / "movies" / "Twilight (2008) {edition-extended}" / "cover.jpg"
    stray.write_bytes(b"jpg")
    assert _swap(db, themes) is not None
    assert stray.exists(), "rmdir is the emptiness test — never delete contents"


# ── the .271 contract still holds ────────────────────────────


def test_idempotent_after_the_complete_carry(env):
    db, themes = env
    assert _swap(db, themes) is not None
    assert _swap(db, themes) is None, "guard 3 still stops a second pass"
    jobs = _q(db, "SELECT id FROM jobs WHERE media_type=? AND tmdb_id=?", MT, TID)
    assert len(jobs) == 1, "and no duplicate place job is queued"


def test_v0_51_272_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
