"""v0.51.271 — swapping one edition for another is not a lost theme.

The operator: the extended cut of a film is removed and the theatrical cut put
in its place; motif reaps the old edition's row and fires "💔 Theme lost … no
backup configured" for a title whose theme never went anywhere.

Two facts reframe it. The canonical lives in motif's OWN store
(`<themes_dir>/…/Title (Year) {edition-<key>}/theme.mp3`), not the media folder,
so only the placement and the edition-keyed association broke. And edition
separation exists to resolve AMBIGUITY — the edition-sibling bleed class — which
needs two editions. With exactly one survivor the edition key is a distinction
without a difference.

So the theme is carried to the survivor under four guards, each of which this
module holds to a case: exactly one surviving edition, a genuinely different
key, a survivor with no theme of its own, and the replacement PRESENT rather
than merely the old row absent (a positive condition — a transient enumeration
gap shows only the absence, and reading an absence as a removal is the shape
v1.22.8 got wrong).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

MT, TID, SEC = "movie", 271001, "1"
NOW = "2026-08-20T00:00:00+00:00"


@pytest.fixture
def env(tmp_path):
    from app.core.db import get_conn, init_db, transaction
    db = tmp_path / "t.db"
    themes = tmp_path / "themes"
    init_db(db)
    old_dir = themes / "movies" / "Twilight (2008) {edition-extended}"
    old_dir.mkdir(parents=True)
    (old_dir / "theme.mp3").write_bytes(b"ID3theme")
    rel = "movies/Twilight (2008) {edition-extended}/theme.mp3"
    with get_conn(db) as conn, transaction(conn):
        # local_files.section_id is FK'd to plex_sections
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
            (MT, TID, SEC, rel, NOW))
    return db, themes, rel


def _add_plex_row(db, rating_key, edition_key, folder, has_theme=0):
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type, title,
                 year, guid_tmdb, edition_key, folder_path, has_theme,
                 first_seen_at, last_seen_at)
               VALUES (?, ?, 'movie', 'Twilight', '2008', ?, ?, ?, ?, ?, ?)""",
            (rating_key, SEC, TID, edition_key, folder, has_theme, NOW, NOW))


def _swap(db, themes, lost="extended"):
    from app.core.edition_swap import resolve_edition_swap
    return resolve_edition_swap(db, themes, media_type=MT, tmdb_id=TID,
                                section_id=SEC, lost_edition_key=lost)


def _row(db, table, edition):
    from app.core.db import get_conn
    with get_conn(db) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            f"SELECT * FROM {table} WHERE media_type=? AND tmdb_id=? "
            f"  AND COALESCE(edition_key,'')=?", (MT, TID, edition)).fetchone()


# ── the operator's case ──────────────────────────────────────


def test_theme_is_carried_to_the_single_surviving_edition(env):
    db, themes, _ = env
    _add_plex_row(db, "rk-theatrical", "", "/data/movies/Twilight (2008)")
    out = _swap(db, themes)
    assert out is not None, "the swap must be recognised"
    assert out["from_edition"] == "extended" and out["to_edition"] == ""
    assert _row(db, "local_files", "") is not None, "re-keyed onto the survivor"
    assert _row(db, "local_files", "extended") is None, "old key gone"


def test_the_canonical_moves_to_the_survivors_folder(env):
    db, themes, old_rel = env
    _add_plex_row(db, "rk-theatrical", "", "/data/movies/Twilight (2008)")
    out = _swap(db, themes)
    assert not (themes / old_rel).exists(), "the old canonical was moved, not copied"
    assert (themes / out["file_path"]).read_bytes() == b"ID3theme"
    assert "{edition-" not in out["file_path"], (
        "the survivor is the standard edition, so its canonical carries no tag")
    assert _row(db, "local_files", "")["file_path"] == out["file_path"], (
        "file_path must follow the move — a stale path is a broken canonical")


def test_it_works_in_the_other_direction_too(env):
    """standard → {edition-Extended}: the same swap, reversed."""
    db, themes, _ = env
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        conn.execute("UPDATE local_files SET edition_key='' WHERE tmdb_id=?", (TID,))
    _add_plex_row(db, "rk-ext", "extended",
                  "/data/movies/Twilight (2008) {edition-Extended}")
    out = _swap(db, themes, lost="")
    assert out is not None and out["to_edition"] == "extended"
    assert "{edition-extended}" in out["file_path"]


# ── the guards ───────────────────────────────────────────────


def test_two_surviving_editions_are_left_alone(env):
    """Guard 1 — the ambiguous case. This is what edition separation is FOR;
    motif must not guess which cut inherits the theme."""
    db, themes, old_rel = env
    _add_plex_row(db, "rk-theatrical", "", "/data/movies/Twilight (2008)")
    _add_plex_row(db, "rk-director", "directors cut",
                  "/data/movies/Twilight (2008) {edition-Directors Cut}")
    assert _swap(db, themes) is None
    assert (themes / old_rel).exists(), "nothing may move while it is ambiguous"
    assert _row(db, "local_files", "extended") is not None


def test_nothing_surviving_is_a_real_loss(env):
    """Guard 4 — the replacement must be PRESENT, not just the old row absent.
    A transient enumeration gap shows only an absence; a swap shows both."""
    db, themes, old_rel = env
    assert _swap(db, themes) is None, (
        "no surviving row means a genuine removal — keep the loss path")
    assert (themes / old_rel).exists()


def test_a_survivor_with_its_own_theme_is_never_overwritten(env):
    """Guard 3 — an operator's per-edition choice outranks the carry-over."""
    db, themes, _ = env
    _add_plex_row(db, "rk-theatrical", "", "/data/movies/Twilight (2008)")
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,
                 set_at, set_by, edition_key)
               VALUES (?, ?, 'https://www.youtube.com/watch?v=theirPick01',
                       ?, 'admin', '')""", (MT, TID, NOW))
    assert _swap(db, themes) is None


def test_same_edition_key_is_not_a_swap(env):
    """Guard 2 — the row was re-added under the same edition. Nothing swapped,
    so the existing loss/reaper handling applies unchanged."""
    db, themes, _ = env
    _add_plex_row(db, "rk-new", "extended",
                  "/data/movies/Twilight (2008) {edition-Extended}")
    assert _swap(db, themes) is None


def test_a_missing_canonical_is_a_real_loss(env):
    """If the audio is genuinely gone there is nothing to carry."""
    db, themes, old_rel = env
    (themes / old_rel).unlink()
    _add_plex_row(db, "rk-theatrical", "", "/data/movies/Twilight (2008)")
    assert _swap(db, themes) is None


def test_an_existing_file_at_the_destination_is_never_clobbered(env):
    db, themes, old_rel = env
    _add_plex_row(db, "rk-theatrical", "", "/data/movies/Twilight (2008)")
    dest = themes / "movies" / "Twilight (2008)"
    dest.mkdir(parents=True)
    (dest / "theme.mp3").write_bytes(b"SOMEONE ELSE")
    assert _swap(db, themes) is None
    assert (dest / "theme.mp3").read_bytes() == b"SOMEONE ELSE"
    assert (themes / old_rel).exists(), "and the source stays put"


# ── it is idempotent + audited ───────────────────────────────


def test_running_twice_changes_nothing_further(env):
    db, themes, _ = env
    _add_plex_row(db, "rk-theatrical", "", "/data/movies/Twilight (2008)")
    first = _swap(db, themes)
    assert first is not None
    assert _swap(db, themes) is None, (
        "after the carry-over the survivor HAS a theme — guard 3 stops a second "
        "pass, so reconciliation cannot loop")


def test_the_move_is_recorded_in_events(env, monkeypatch):
    """Every automated repair explains itself. Asserted at the emission — the
    events table is written by a background flusher, so reading it straight
    back would be a race, not a check."""
    db, themes, _ = env
    _add_plex_row(db, "rk-theatrical", "", "/data/movies/Twilight (2008)")
    seen = []
    import app.core.edition_swap as mod
    monkeypatch.setattr(mod, "log_event",
                        lambda *a, **kw: seen.append(kw))
    _swap(db, themes)
    assert len(seen) == 1, "exactly one audit line per carry-over"
    ev = seen[0]
    assert "Edition swap" in ev["message"]
    assert ev["detail"]["from_edition"] == "extended"
    assert ev["detail"]["to_edition"] == ""
    assert ev["media_type"] == MT and ev["tmdb_id"] == TID


# ── the reaper consults it before crying loss ────────────────


def test_the_reaper_resolves_swaps_before_dispatching_loss():
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    i = src.index("for cand in lost_theme_candidates:")
    block = src[i:src.index("_ctx = _nc.enrich_item(", i)]
    assert "resolve_edition_swap(" in block, (
        "the swap check must run BEFORE the loss notification is built")
    assert "continue" in block, "a resolved swap must skip the loss dispatch"
    assert "_settings.is_paths_ready()" in block, (
        "no themes_dir means no canonical to carry")


def test_v0_51_271_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
