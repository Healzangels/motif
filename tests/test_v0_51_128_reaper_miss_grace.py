"""v0.51.128 — the reaper waits for consecutive misses before it reaps + alerts.

The v1.18.90 reaper DELETEs a plex_items row (and fires 💔 Theme lost) the moment
Plex's section listing stops returning it. A transient Plex glitch — a partial
catalog, an API hiccup, a re-add under a new rating_key mid-enum — could drop a
live row for a single enum and trigger a false delete + false alert.

v0.51.128 adds a grace window: plex_items.consecutive_missing counts consecutive
full enums a row is absent, and the reaper only reaps at >= _REAP_MISS_THRESHOLD.
A row that reappears resets to 0. enumerate_section_items already raises on
partial fetches (v1.23.64), so this counter is the last line distinguishing a
genuine removal from a transient blip.

These are BEHAVIORAL tests against the real _upsert_items reaper path.
"""
from __future__ import annotations

import sqlite3

import pytest

from app.core.db import init_db
from app.core import plex_enum
from app.core.plex_enum import _REAP_MISS_THRESHOLD

SEC = "1"
NOW = "2026-07-10T00:00:00+00:00"
STALE_TMDB = 55555  # the themed STALE row's tmdb — lets a real reap dispatch 💔


def _item(rk, *, has_theme=False, guid_tmdb=None, title="Keeper",
          folder="/data/movies/Keeper"):
    from app.core.plex import PlexLibraryItem
    return PlexLibraryItem(
        rating_key=rk, section_id=SEC, media_type="movie",
        title=title, year="2020", guid_imdb=None, guid_tmdb=guid_tmdb,
        guid_tvdb=None, folder_path=folder, has_theme=has_theme,
        plex_theme_uri="",
    )


@pytest.fixture
def seeded(tmp_path, monkeypatch):
    """A KEEPER row Plex keeps returning + a STALE row Plex has stopped
    returning. The keeper's presence keeps seen_rks non-empty so the reaper
    actually runs (the empty-items safety guard is not what's under test)."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    db = tmp_path / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        # STALE is a THEMED, no-local-fallback row (has_theme=1, guid_tmdb set,
        # local_theme_file=0) so that when the reaper DOES reap it, the v1.18.90
        # candidate query captures it and a plex_theme_lost alert dispatches.
        # That makes the "no false alert on a transient miss" assertions
        # load-bearing (v0.51.129 fix — pre-fix STALE was has_theme=0 with no
        # tmdb, so the alert could never fire and those asserts were vacuous).
        for rk, title, folder, ht, tmdb in (
                ("KEEP", "Keeper", "/data/movies/Keeper", 0, None),
                ("STALE", "Gone", "/data/movies/Gone", 1, STALE_TMDB)):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " title, year, has_theme, local_theme_file, guid_tmdb,"
                " folder_path, first_seen_at, last_seen_at) VALUES "
                "(?,'1','movie',?,'2020',?,0,?,?,?,?)",
                (rk, title, ht, tmdb, folder, NOW, NOW))
        conn.commit()
    calls = []
    import app.core.notify as _notify
    monkeypatch.setattr(
        _notify, "dispatch",
        lambda *a, **k: calls.append(k.get("event_kind")))
    # v0.51.288: the reaper dispatches via the coalescer now — capture both so
    # the fired/suppressed assertions below keep seeing the theme-lost kinds.
    monkeypatch.setattr(
        _notify, "dispatch_coalesced",
        lambda *a, **k: calls.append(k.get("event_kind")))
    return db, calls


def _rows(db):
    with sqlite3.connect(db) as conn:
        return {r[0]: r[1] for r in conn.execute(
            "SELECT rating_key, consecutive_missing FROM plex_items "
            "WHERE section_id='1'")}


def _enum_without_stale(db):
    # Plex returns ONLY the keeper — STALE is absent this enum.
    plex_enum._upsert_items(db, [_item("KEEP")], section_id=SEC)


def test_threshold_is_more_than_one():
    # The whole point is a grace window — a single miss must not reap.
    assert _REAP_MISS_THRESHOLD >= 2


def test_single_miss_defers_reap_and_counts(seeded):
    db, calls = seeded
    _enum_without_stale(db)
    rows = _rows(db)
    assert "STALE" in rows, "a single miss must NOT delete the row"
    assert rows["STALE"] == 1, "the miss must be counted"
    assert rows["KEEP"] == 0, "a returned row stays at 0 misses"
    assert "plex_theme_lost" not in calls, (
        "no theme-lost alert may fire on the first (possibly transient) miss")


def test_reaches_threshold_then_reaps(seeded):
    db, calls = seeded
    for _ in range(_REAP_MISS_THRESHOLD):
        _enum_without_stale(db)
    rows = _rows(db)
    assert "STALE" not in rows, (
        f"after {_REAP_MISS_THRESHOLD} consecutive misses the row must be reaped")
    assert rows.get("KEEP") == 0, "the keeper is untouched"
    # The themed row's reap DOES fire 💔 — proving the alert pipe is live, so
    # the "not in calls" suppression assertions elsewhere are meaningful.
    assert "plex_theme_lost" in calls, (
        "a genuine reap of a themed no-fallback row must fire the theme-lost alert")


def test_reappearance_resets_the_counter(seeded):
    db, _calls = seeded
    # Miss once (counter → 1)...
    _enum_without_stale(db)
    assert _rows(db)["STALE"] == 1
    # ...then Plex returns STALE again → counter resets to 0.
    plex_enum._upsert_items(
        db, [_item("KEEP"), _item("STALE", has_theme=True, guid_tmdb=STALE_TMDB,
                                  title="Gone", folder="/data/movies/Gone")],
        section_id=SEC)
    rows = _rows(db)
    assert rows["STALE"] == 0, "a reappearing row must reset its miss-counter"
    # A fresh single miss afterward must again only count, not reap — proving
    # the reset restored the full grace window (no latent off-by-one).
    _enum_without_stale(db)
    rows = _rows(db)
    assert rows.get("STALE") == 1, (
        "after a reset, one miss counts to 1 and does not reap")


def test_flapping_row_never_reaps(seeded):
    db, calls = seeded
    # Miss, return, miss, return — never two misses in a row → never reaped.
    for _ in range(4):
        _enum_without_stale(db)
        plex_enum._upsert_items(
            db, [_item("KEEP"), _item("STALE", has_theme=True,
                                      guid_tmdb=STALE_TMDB, title="Gone",
                                      folder="/data/movies/Gone")],
            section_id=SEC)
    rows = _rows(db)
    assert "STALE" in rows, "a row that keeps reappearing must never be reaped"
    assert rows["STALE"] == 0
    assert "plex_theme_lost" not in calls, (
        "a themed row that never misses twice in a row must never fire 💔")


def test_churning_glitch_does_not_slip_past_mass_guard(tmp_path, monkeypatch):
    """The amplifier-sweep mass-guard (>50 stale AND >20%) must key off the
    INSTANTANEOUS missing set, not the narrowed threshold-crossed set. A churning
    Plex glitch — a large but DIFFERENT subset missing each enum — could otherwise
    leave a sub-50 persistent core that crosses the miss threshold and gets
    reaped while the section-wide miss is huge. The guard must abort the reap
    while > 50 rows are missing this enum, even if only ~40 have crossed the
    threshold."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    db = tmp_path / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        for i in range(200):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " title, year, has_theme, local_theme_file, folder_path,"
                " consecutive_missing, first_seen_at, last_seen_at) VALUES "
                "(?, '1','movie',?, '2020',0,0,?,?,?,?)",
                (f"rk_{i:04d}", f"Movie {i}", f"/data/movies/Movie {i}",
                 # 40 rows already at 1 miss (will cross threshold this enum);
                 # the rest fresh — so the reap set is 40 (< 50) but the
                 # instantaneous stale set is 100 (> 50, 50% of section).
                 1 if i < 40 else 0, NOW, NOW))
        conn.commit()
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)
    # v0.51.288: the reaper now dispatches via the coalescer — null it too so
    # a mass-reap test can't buffer real notifications.
    monkeypatch.setattr("app.core.notify.dispatch_coalesced",
                        lambda *a, **k: None)
    # Plex returns 100 of 200 (rk_0100..rk_0199) — rk_0000..rk_0099 are missing,
    # which includes the 40 pre-aged (→ cross threshold) + 60 fresh (→ miss 1).
    items = [_item(f"rk_{i:04d}", title=f"Movie {i}",
                   folder=f"/data/movies/Movie {i}") for i in range(100, 200)]
    plex_enum._upsert_items(db, items, section_id=SEC)
    with sqlite3.connect(db) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM plex_items WHERE section_id='1'").fetchone()[0]
    assert n == 200, (
        "v0.51.128: a large instantaneous miss must abort the WHOLE reap — the "
        f"40 threshold-crossed rows must NOT be deleted; got {n} rows (expected 200)")


def test_schema_has_consecutive_missing_column(seeded):
    db, _ = seeded
    with sqlite3.connect(db) as conn:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(plex_items)").fetchall()}
    assert "consecutive_missing" in cols

# v0.51.129: dropped the vacuous test_version_pin (`'__version__ = "0.'` matched
# any 0.x) — the exact-version pin lives in test_v1_13_79_link_fixes.py.
