"""v1.24.34 — ensure-coverage backfill for historical edition_key='' canonicals.

the user's Two Towers 409 ("no local file to replace from"): a pre-edition-aware
theme has its canonical at edition_key='' while the live Plex rows are tagged
(theatrical / extended), so the edition-scoped push + v1.24.28-29 RP auto-recovery
can't find a canonical for the tagged edition. Prod diagnostic: 31 such titles.

the user's call: ensure-coverage — create a per-edition local_files row (sharing
the '' canonical file) for every live non-'' edition that lacks one, so each
edition is push-able + auto-restorable. One-shot via a runtime_settings marker
(so a later UNMANAGE isn't resurrected); idempotent; amplifier-sweep guarded.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import get_conn, init_db
from app.core.recovery_v55 import maybe_backfill_edition_canonicals

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "motif.db"
    init_db(p)
    with get_conn(p) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (10,'movie',121,'The Two Towers','2002','imdb',?,?)",
                  (NOW, NOW))
        c.commit()
    return p


def _empty_canonical(c, *, file_path="movies/tt.mp3", reason="placed"):
    c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
              " edition_key, theme_id, file_path, source_video_id, provenance, "
              " source_kind, downloaded_at, canonical_present, "
              " last_place_attempt_reason) "
              "VALUES ('movie',121,'1','',10,?, 'vid','manual','url',?,1,?)",
              (file_path, NOW, reason))


def _plex_edition(c, rk, ed):
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
              " theme_id, guid_tmdb, title, edition_key, has_theme, "
              " first_seen_at, last_seen_at) "
              "VALUES (?, '1','movie',10,121,'The Two Towers',?,0,?,?)",
              (rk, ed, NOW, NOW))


def _editions_with_canonical(db):
    with sqlite3.connect(db) as c:
        return sorted(r[0] for r in c.execute(
            "SELECT edition_key FROM local_files WHERE tmdb_id=121").fetchall())


def _marker(db):
    with sqlite3.connect(db) as c:
        r = c.execute("SELECT value FROM runtime_settings "
                      "WHERE key='edition_canonical_coverage_done_at'").fetchone()
        return r[0] if r else None


# ── the fix ─────────────────────────────────────────────────────────────

def test_creates_canonical_for_tagged_edition(db):
    with get_conn(db) as c:
        _empty_canonical(c)
        _plex_edition(c, "rk-th", "theatrical")
        c.commit()
    res = maybe_backfill_edition_canonicals(db)
    assert res["created"] == 1
    assert _editions_with_canonical(db) == ["", "theatrical"]
    assert _marker(db) is not None


def test_created_row_shares_file_and_is_placeable(db):
    with get_conn(db) as c:
        _empty_canonical(c, file_path="movies/shared.mp3")
        _plex_edition(c, "rk-th", "theatrical")
        c.commit()
    maybe_backfill_edition_canonicals(db)
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        row = c.execute("SELECT file_path, last_place_attempt_reason, source_kind "
                        "FROM local_files WHERE tmdb_id=121 AND "
                        "edition_key='theatrical'").fetchone()
    assert row["file_path"] == "movies/shared.mp3", "shares the '' canonical file"
    assert row["last_place_attempt_reason"] is None, "NULL → retry sweep places it"
    assert row["source_kind"] == "url"


def test_covers_all_editions_multi(db):
    with get_conn(db) as c:
        _empty_canonical(c)
        _plex_edition(c, "rk-th", "theatrical")
        _plex_edition(c, "rk-ex", "extended edition")
        c.commit()
    res = maybe_backfill_edition_canonicals(db)
    assert res["created"] == 2
    assert _editions_with_canonical(db) == ["", "extended edition", "theatrical"]


# ── safety: idempotency, marker, no-op, the '' row preserved ────────────

def test_skips_edition_that_already_has_canonical(db):
    with get_conn(db) as c:
        _empty_canonical(c)
        _plex_edition(c, "rk-th", "theatrical")
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
                  " edition_key, theme_id, file_path, source_video_id, "
                  " provenance, source_kind, downloaded_at) "
                  "VALUES ('movie',121,'1','theatrical',10,'movies/own.mp3',"
                  "'v','auto','themerrdb',?)", (NOW,))
        c.commit()
    res = maybe_backfill_edition_canonicals(db)
    assert res["created"] == 0  # theatrical already had its own
    with sqlite3.connect(db) as c:
        fp = c.execute("SELECT file_path FROM local_files WHERE tmdb_id=121 AND "
                       "edition_key='theatrical'").fetchone()[0]
    assert fp == "movies/own.mp3", "the existing edition canonical is untouched"


def test_marker_makes_it_one_shot(db):
    with get_conn(db) as c:
        _empty_canonical(c)
        _plex_edition(c, "rk-th", "theatrical")
        c.commit()
    maybe_backfill_edition_canonicals(db)
    # delete the created row to simulate a deliberate UNMANAGE; re-run must NOT
    # resurrect it (the marker is set).
    with get_conn(db) as c:
        c.execute("DELETE FROM local_files WHERE tmdb_id=121 AND "
                  "edition_key='theatrical'")
        c.commit()
    res2 = maybe_backfill_edition_canonicals(db)
    assert res2["skipped_reason"] == "marker_set"
    assert _editions_with_canonical(db) == [""], "UNMANAGE not resurrected"


def test_no_gaps_stamps_marker(db):
    with get_conn(db) as c:
        _empty_canonical(c)  # a '' canonical but NO non-'' editions in Plex
        c.commit()
    res = maybe_backfill_edition_canonicals(db)
    assert res["created"] == 0 and res["gaps"] == 0
    assert _marker(db) is not None  # stamped so it won't re-scan every boot


def test_amplifier_guard_aborts_without_marker(db, monkeypatch):
    # Force a tiny cap so any gap looks implausible → abort, no write, no marker.
    with get_conn(db) as c:
        _empty_canonical(c)
        _plex_edition(c, "rk-th", "theatrical")
        c.commit()
    monkeypatch.setattr("builtins.max", lambda *a, **k: 0)
    res = maybe_backfill_edition_canonicals(db)
    assert res["skipped_reason"] == "implausible_gap_count"
    assert _editions_with_canonical(db) == [""], "no rows written on abort"
    assert _marker(db) is None, "marker NOT stamped → a fixed build retries"


# ── source pins ─────────────────────────────────────────────────────────

def test_wired_into_boot():
    main_src = (REPO / "app" / "main.py").read_text()
    assert "maybe_backfill_edition_canonicals" in main_src
