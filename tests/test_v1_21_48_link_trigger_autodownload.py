"""v1.21.48 — auto-download triggers on the item↔theme LINK event.

The two pre-existing auto-download triggers are proxies that miss a
real case:
  - sync `is_new`     → "a theme was published" (assumes item linked)
  - refresh new-item  → "an item was inserted"  (assumes theme linked)

The actual "row became downloadable" event is `plex_items.theme_id`
going NULL→set (the item↔theme link forming) — which can happen at a
THIRD moment: a PRE-EXISTING Plex item linked late (its guid resolved
this enum, or a theme published after the item was already enumerated).
That row is never in `new_item_rks`, so both proxies miss it forever.
the user's repro: the 2026 movie "The Legend of Aang" sat SRC=— with a
TDB theme available but never auto-downloaded.

Fix (root-cause, not a periodic re-scan): `resolve_theme_ids` now
reports the rows it newly links via `collect_newly_linked`, and the
plex_enum caller fires the theme-available push on
`new_item_rks ∪ newly_linked`. These tests pin that resolve reports
exactly the NULL→set transitions, and that the call site unions them.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db
from app.core import plex_enum


REPO = Path(__file__).resolve().parent.parent
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _seed_theme(db: Path, *, theme_id: int, tmdb: int):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title,"
            " upstream_source, youtube_url, last_seen_sync_at,"
            " first_seen_sync_at) VALUES (?,?,?,?,'imdb',?,?,?)",
            (theme_id, 'movie', tmdb, 'X', 'https://youtu.be/abcdefghijk',
             NOW, NOW))
        conn.commit()


def _seed_plex_item(db: Path, *, rk: str, tmdb: int, theme_id: int | None,
                    section: str = '1'):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type,"
            " is_anime, is_4k, themes_subdir, included, discovered_at,"
            " last_seen_at) VALUES (?,'Movies','movie',0,0,'movies',1,?,?)",
            (section, NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_tmdb, title, title_norm, has_theme,"
            " plex_theme_verified_ok, local_theme_file, folder_path,"
            " first_seen_at, last_seen_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (rk, section, 'movie', theme_id, tmdb, 'X', 'x', 0, 0, 0,
             '/data/movies/x', NOW, NOW))
        conn.commit()


# ── resolve reports exactly the NULL→set transitions ─────────


def test_newly_linked_row_is_reported(db):
    """An UNLINKED plex_item that resolve links (guid_tmdb match) →
    its rk lands in collect_newly_linked. This is the gap case: the
    item pre-exists, the link is what's new."""
    _seed_theme(db, theme_id=50, tmdb=500)
    _seed_plex_item(db, rk='rk1', tmdb=500, theme_id=None)
    acc: list[str] = []
    plex_enum.resolve_theme_ids(db, collect_newly_linked=acc)
    assert acc == ['rk1']
    # And it actually got linked.
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='rk1'"
        ).fetchone()[0] == 50


def test_already_linked_row_not_reported(db):
    """A row already linked (theme_id set) does NOT transition, so it
    must NOT be reported — this is what keeps the existing backlog from
    being re-grabbed."""
    _seed_theme(db, theme_id=50, tmdb=500)
    _seed_plex_item(db, rk='rk1', tmdb=500, theme_id=50)  # already linked
    acc: list[str] = []
    plex_enum.resolve_theme_ids(db, collect_newly_linked=acc)
    assert acc == []


def test_unmatched_row_not_reported(db):
    """An unlinked row with no matching theme stays NULL → not
    reported (no false positive)."""
    _seed_theme(db, theme_id=50, tmdb=500)
    _seed_plex_item(db, rk='rk1', tmdb=999, theme_id=None)  # no match
    acc: list[str] = []
    plex_enum.resolve_theme_ids(db, collect_newly_linked=acc)
    assert acc == []
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='rk1'"
        ).fetchone()[0] is None


def test_mixed_population_reports_only_transitions(db):
    _seed_theme(db, theme_id=50, tmdb=500)
    _seed_theme(db, theme_id=51, tmdb=501)
    _seed_plex_item(db, rk='new', tmdb=500, theme_id=None)   # links → report
    _seed_plex_item(db, rk='old', tmdb=501, theme_id=51)     # already linked
    _seed_plex_item(db, rk='miss', tmdb=999, theme_id=None)  # no match
    acc: list[str] = []
    plex_enum.resolve_theme_ids(db, collect_newly_linked=acc)
    assert acc == ['new']


def test_newly_linked_accumulates_across_chunks(db):
    """Force multiple chunks (chunk_size=1) and confirm the per-chunk
    capture accumulates into the shared list — guards the pagination +
    cross-chunk accumulation, the subtlest part of the mechanism."""
    for i in range(3):
        _seed_theme(db, theme_id=50 + i, tmdb=500 + i)
        _seed_plex_item(db, rk=f'rk{i}', tmdb=500 + i, theme_id=None)
    acc: list[str] = []
    plex_enum.resolve_theme_ids(db, chunk_size=1, collect_newly_linked=acc)
    assert sorted(acc) == ['rk0', 'rk1', 'rk2']


def test_section_scope_limits_reporting(db):
    """A section-scoped resolve only reports links in that section — the
    snapshot reuses the same scope_clause as the UPDATEs."""
    _seed_theme(db, theme_id=50, tmdb=500)
    _seed_theme(db, theme_id=51, tmdb=501)
    _seed_plex_item(db, rk='in1', tmdb=500, theme_id=None, section='1')
    _seed_plex_item(db, rk='in2', tmdb=501, theme_id=None, section='2')
    acc: list[str] = []
    plex_enum.resolve_theme_ids(db, section_id='1', collect_newly_linked=acc)
    assert acc == ['in1']
    # Section 2's row is untouched by a section-1-scoped resolve.
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='in2'"
        ).fetchone()[0] is None


def test_collect_param_default_is_noop(db):
    """Default None → no tracking, no crash; recovery/sync callers that
    don't pass it are unaffected and still get the int return."""
    _seed_theme(db, theme_id=50, tmdb=500)
    _seed_plex_item(db, rk='rk1', tmdb=500, theme_id=None)
    out = plex_enum.resolve_theme_ids(db)  # no collect_newly_linked
    assert isinstance(out, int)


# ── the call site unions new + newly-linked and fires the push ──


def test_call_site_unions_new_and_newly_linked():
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    # resolve is called WITH the collector...
    assert "collect_newly_linked=newly_linked" in src
    # ...and the push fires on the UNION of inserted + newly-linked.
    assert "dict.fromkeys(new_item_rks + newly_linked)" in src
    assert "_maybe_notify_theme_available(db_path, candidate_rks)" in src


def test_band_aid_sweep_is_gone():
    """The hourly periodic sweep (the rejected band-aid) must not be
    re-introduced — the root-cause link trigger replaces it."""
    sched = (REPO / "app" / "core" / "scheduler.py").read_text()
    assert "_auto_download_stranded_unthemed" not in sched
    assert "stranded_unthemed_autodl" not in sched
