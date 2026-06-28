"""v1.19.77 — prune sweep must preserve new_theme_available rows.

Opus-4.8 audit HIGH-1. The v1.19.71 feature surfaces NEW ThemerrDB
themes on in-Plex rows via a blue !UPD glyph, written as a
pending_updates row with kind='new_theme_available'. For the two
headline cohorts — genuinely-unthemed rows (SRC=—) and pure-cloud
Plex-Pass rows (SRC=P, metadata://, no sidecar) — the row has NO
local_files, NO user_override, NO placement, and plex_items.
local_theme_file=0 by definition.

The v1.12.49 end-of-sync prune sweep (sync._prune_stale_pending_updates)
deletes pending_updates rows with "nothing to update against": all
five NOT EXISTS branches (local_files / user_overrides / placements
/ plex_items.local_theme_file=1 / in-flight job). Pre-fix, the
new_theme_available row written during the SAME run_sync matched
every branch and got pruned before the page ever loaded — the
entire SRC=— + cloud-P half of the feature was dead at rest, with
zero test coverage exercising the write→prune cycle end-to-end.

v1.19.77 widens the 4th NOT EXISTS so a new_theme_available row
anchors on plex_items PRESENCE (any matching row) instead of
local_theme_file=1, while still pruning the row if the title has
left Plex entirely (orphan-cleanup intent preserved) and leaving
the other kinds' behavior unchanged.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import init_db
from app.core.sync import _prune_stale_pending_updates


TS = "2026-05-28T00:00:00"


def _seed_theme(conn, mt, tmdb):
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
        "last_seen_sync_at, first_seen_sync_at) VALUES (?, ?, ?, 'themoviedb', ?, ?)",
        (mt, tmdb, f"Title {tmdb}", TS, TS),
    )


def _seed_plex_item(conn, rk, plex_mt, tmdb, *, local_theme_file=0, has_theme=0):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, title, "
        "guid_tmdb, has_theme, local_theme_file, first_seen_at, last_seen_at) "
        "VALUES (?, '1', ?, ?, ?, ?, ?, ?, ?)",
        (rk, plex_mt, f"Title {tmdb}", tmdb, has_theme, local_theme_file, TS, TS),
    )


def _seed_pending(conn, mt, tmdb, kind):
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
        "new_youtube_url, detected_at, decision, kind) "
        "VALUES (?, ?, '', 'https://youtu.be/new', ?, 'pending', ?)",
        (mt, tmdb, TS, kind),
    )


def _pending_exists(conn, mt, tmdb) -> bool:
    return conn.execute(
        "SELECT 1 FROM pending_updates WHERE media_type=? AND tmdb_id=?",
        (mt, tmdb),
    ).fetchone() is not None


def _make_db(tmp_path) -> Path:
    db = tmp_path / "motif.db"
    init_db(db)
    return db


# ── The fix: new_theme_available survives on bare Plex presence ──


def test_unthemed_movie_new_theme_survives_prune(tmp_path):
    """SRC=— cohort: movie in Plex, no local theme, no anchors.
    The new_theme_available prompt MUST survive the sweep."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, "movie", 100)
        _seed_plex_item(conn, "rk100", "movie", 100, local_theme_file=0)
        _seed_pending(conn, "movie", 100, "new_theme_available")
        conn.commit()

    _prune_stale_pending_updates(db)

    with sqlite3.connect(db) as conn:
        assert _pending_exists(conn, "movie", 100), (
            "v1.19.77: unthemed in-Plex new_theme_available row was pruned "
            "in the same sweep — the headline SRC=— cohort is dead at rest"
        )


def test_cloud_plex_new_theme_survives_prune(tmp_path):
    """Pure-cloud-P cohort: Plex serves a metadata:// theme (has_theme=1)
    but no sidecar (local_theme_file=0). Same root cause as SRC=— —
    no local anchor. Must survive."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, "movie", 110)
        _seed_plex_item(conn, "rk110", "movie", 110, has_theme=1, local_theme_file=0)
        _seed_pending(conn, "movie", 110, "new_theme_available")
        conn.commit()

    _prune_stale_pending_updates(db)

    with sqlite3.connect(db) as conn:
        assert _pending_exists(conn, "movie", 110)


def test_tv_new_theme_survives_via_show_mapping(tmp_path):
    """The carve-out must honor the tv→show media_type mapping in
    the plex_items anchor (pending_updates.media_type='tv' maps to
    plex_items.media_type='show')."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, "tv", 400)
        _seed_plex_item(conn, "rk400", "show", 400, local_theme_file=0)
        _seed_pending(conn, "tv", 400, "new_theme_available")
        conn.commit()

    _prune_stale_pending_updates(db)

    with sqlite3.connect(db) as conn:
        assert _pending_exists(conn, "tv", 400), (
            "v1.19.77: tv new_theme row pruned — the CASE tv→show "
            "mapping in the carve-out is broken"
        )


# ── Orphan-cleanup intent preserved ─────────────────────────────


def test_new_theme_pruned_when_title_left_plex(tmp_path):
    """A new_theme_available row whose title is no longer in Plex
    (no plex_items row at all) IS a genuine orphan and must still
    be pruned — the carve-out anchors on presence, not on kind alone."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, "movie", 200)
        # no plex_items row for tmdb 200
        _seed_pending(conn, "movie", 200, "new_theme_available")
        conn.commit()

    _prune_stale_pending_updates(db)

    with sqlite3.connect(db) as conn:
        assert not _pending_exists(conn, "movie", 200), (
            "v1.19.77: an orphan new_theme row (title left Plex) must "
            "still prune — otherwise stale rows accumulate forever"
        )


# ── Regression guard: other kinds unchanged ─────────────────────


def test_upstream_changed_still_pruned_without_local_anchor(tmp_path):
    """The carve-out is scoped to new_theme_available ONLY. An
    'upstream_changed' row on an in-Plex item with no local theme
    (local_theme_file=0) and no other anchor must STILL prune —
    that's the original v1.12.49 behavior, untouched."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, "movie", 300)
        _seed_plex_item(conn, "rk300", "movie", 300, local_theme_file=0)
        _seed_pending(conn, "movie", 300, "upstream_changed")
        conn.commit()

    _prune_stale_pending_updates(db)

    with sqlite3.connect(db) as conn:
        assert not _pending_exists(conn, "movie", 300), (
            "v1.19.77: the carve-out leaked to upstream_changed rows — "
            "those must still prune when there's nothing to update against"
        )


def test_upstream_changed_kept_when_local_theme_present(tmp_path):
    """Sanity: an 'upstream_changed' row with a real sidecar
    (local_theme_file=1) survives, exactly as before the fix."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, "movie", 310)
        _seed_plex_item(conn, "rk310", "movie", 310, local_theme_file=1)
        _seed_pending(conn, "movie", 310, "upstream_changed")
        conn.commit()

    _prune_stale_pending_updates(db)

    with sqlite3.connect(db) as conn:
        assert _pending_exists(conn, "movie", 310)


def test_run_sync_calls_extracted_prune_helper():
    """The inline sweep was extracted to _prune_stale_pending_updates
    so this behavior is unit-testable end-to-end; run_sync must call
    it (guards against a future refactor silently dropping the sweep)."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    assert "_prune_stale_pending_updates(db_path)" in src
    assert "def _prune_stale_pending_updates(db_path)" in src
