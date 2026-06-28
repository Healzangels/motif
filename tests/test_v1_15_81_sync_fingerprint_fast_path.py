"""v1.15.81 — sync apply-loop fast-path via content fingerprint.

the user's v1.15.79 question: "Also believe it ended up resulting
in no changes so not sure why the longer process willing to
discuss!" His repro: 9733 git-diff'd paths applied over 2+
minutes, result `new=0, updated=0`.

Root cause: LizardByte's HEAD got new commits touching those
9733 paths, but the fields motif reads (URL, video_id, title,
year, etc.) were unchanged in those files. motif's
`_upsert_theme` ran a full UPDATE for each — bumping
last_seen_sync_at + re-writing title/year/raw_json — even
though nothing actually changed.

v1.15.81 adds a content fingerprint (SHA1 over the 10 motif-
relevant fields) stored per-theme. Sync's `_upsert_theme` now
compares the new record's fingerprint against the stored one
and, when they match, runs only a cheap last_seen_sync_at
bump instead of the full multi-column UPDATE. The "noisy
commit, same data" case turns from 5000 multi-col UPDATEs
into 5000 single-col UPDATEs — ~10× faster.

Storage: schema v51 adds `themes.tdb_content_fingerprint TEXT`
(NULL on legacy rows; next sync writes the fingerprint as part
of the slow-path UPDATE so the v51 → v52 fast-path kicks in
after one full cycle).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core.db import CURRENT_SCHEMA_VERSION, get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent
SYNC_PY = REPO / "app" / "core" / "sync.py"
DB_PY = REPO / "app" / "core" / "db.py"


# ── 1. Schema migration ─────────────────────────────────────


def test_schema_version_advanced_to_51():
    """The CURRENT_SCHEMA_VERSION must be 51 — without the bump
    existing v50 databases never apply the new migration."""
    assert CURRENT_SCHEMA_VERSION >= 51, (
        f"v1.15.81: CURRENT_SCHEMA_VERSION must be >= 51; got "
        f"{CURRENT_SCHEMA_VERSION}"
    )


def test_migration_v50_to_v51_defined():
    """The migration function must exist + be wired into the
    chain. Without the wire-up, fresh databases miss the column."""
    src = DB_PY.read_text()
    assert "def _migrate_v50_to_v51(" in src, (
        "v1.15.81: _migrate_v50_to_v51 must be defined"
    )
    assert "_migrate_v50_to_v51(conn)" in src, (
        "v1.15.81: _migrate_v50_to_v51 must be called from the "
        "migration chain"
    )


def test_themes_has_tdb_content_fingerprint_column(tmp_path):
    """Behavioral check: init_db() on a fresh DB creates the
    fingerprint column. Catches SCHEMA-text-vs-migration drift."""
    db = tmp_path / "test.db"
    init_db(db)
    with get_conn(db) as conn:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(themes)"
        ).fetchall()}
    assert "tdb_content_fingerprint" in cols, (
        "v1.15.81: themes table must include tdb_content_fingerprint"
    )


# ── 2. Fingerprint helper stable + sensitive ────────────────


def test_record_fingerprint_is_stable_across_key_order():
    """Hash must be stable across input dict key ordering — JSON
    dumps with sort_keys=True ensures formatting noise doesn't
    cause false drift."""
    from app.core.sync import _record_fingerprint
    a = {"imdb_id": "tt1", "title": "Foo", "youtube_theme_url": "u"}
    b = {"youtube_theme_url": "u", "title": "Foo", "imdb_id": "tt1"}
    assert _record_fingerprint(a) == _record_fingerprint(b)


def test_record_fingerprint_ignores_non_relevant_fields():
    """Adding/removing fields motif doesn't read must NOT change
    the fingerprint. This is the whole point — LizardByte adding
    a new metadata field shouldn't trigger a re-write."""
    from app.core.sync import _record_fingerprint
    base = {"imdb_id": "tt1", "title": "Foo", "year": "2020",
            "youtube_theme_url": "u"}
    noisy = {**base, "popularity": 7.5, "vote_average": 8.2,
             "internal_metadata_field": "anything"}
    assert _record_fingerprint(base) == _record_fingerprint(noisy)


def test_record_fingerprint_changes_when_url_changes():
    """Changing a motif-relevant field MUST change the
    fingerprint — otherwise the fast-path would skip a real
    update."""
    from app.core.sync import _record_fingerprint
    base = {"imdb_id": "tt1", "youtube_theme_url": "https://yt/a"}
    changed = {"imdb_id": "tt1", "youtube_theme_url": "https://yt/b"}
    assert _record_fingerprint(base) != _record_fingerprint(changed)


def test_record_fingerprint_changes_when_title_changes():
    """Title is a motif-read field — a rename must trigger
    re-write so the cached title doesn't go stale."""
    from app.core.sync import _record_fingerprint
    base = {"title": "Foo", "youtube_theme_url": "u"}
    renamed = {"title": "Bar", "youtube_theme_url": "u"}
    assert _record_fingerprint(base) != _record_fingerprint(renamed)


# ── 3. Fast-path skip behavior + slow-path write ────────────


def _seed_theme(db, *, fingerprint=None):
    """Seed a single themes row with the v1.15.81 fingerprint
    column populated."""
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, imdb_id, title, year,
                  youtube_url, youtube_video_id, upstream_source,
                  raw_json, last_seen_sync_at, first_seen_sync_at,
                  tdb_content_fingerprint)
               VALUES ('movie', 81001, 'tt81001', 'FP Movie', 2020,
                       'https://www.youtube.com/watch?v=fpvideo001',
                       'fpvideo001', 'themoviedb', '{}',
                       '2026-01-01', '2026-01-01', ?)""",
            (fingerprint,),
        )


def test_fast_path_skips_upsert_when_fingerprint_matches(tmp_path):
    """End-to-end: when the new record's fingerprint matches the
    stored one, _upsert_theme returns (False, False, None) +
    only bumps last_seen_sync_at. updated_count must NOT tick."""
    from app.core.sync import _record_fingerprint, _upsert_theme
    db = tmp_path / "test.db"
    init_db(db)
    record = {
        "imdb_id": "tt81001",
        "title": "FP Movie",
        "release_date": "2020-01-01",
        "youtube_theme_url": "https://www.youtube.com/watch?v=fpvideo001",
    }
    expected_fp = _record_fingerprint(record)
    _seed_theme(db, fingerprint=expected_fp)
    with get_conn(db) as conn, transaction(conn):
        is_new, url_changed, old_vid, _ = _upsert_theme(
            conn, media_type="movie", tmdb_id=81001,
            record=record, upstream_source="themoviedb",
            sync_ts="2026-06-01",
        )
    assert (is_new, url_changed, old_vid) == (False, False, None), (
        "v1.15.81: matching fingerprint must return fast-path tuple"
    )
    # last_seen_sync_at was bumped though.
    with get_conn(db) as conn:
        row = conn.execute(
            "SELECT last_seen_sync_at, title FROM themes "
            "WHERE tmdb_id = 81001"
        ).fetchone()
        assert row["last_seen_sync_at"] == "2026-06-01", (
            "v1.15.81: fast-path must still bump last_seen_sync_at "
            "(drop detection depends on it)"
        )


def test_fast_path_skipped_when_fingerprint_differs(tmp_path):
    """Counter-test: if the record's fingerprint doesn't match
    stored, the slow-path full UPDATE must run. url_changed must
    be True when youtube_video_id differs."""
    from app.core.sync import _upsert_theme
    db = tmp_path / "test.db"
    init_db(db)
    # Seed with a stale fingerprint that won't match the new record.
    _seed_theme(db, fingerprint="stale-hash-from-prior-sync")
    new_record = {
        "imdb_id": "tt81001",
        "title": "FP Movie Renamed",  # title changed
        "release_date": "2020-01-01",
        "youtube_theme_url": "https://www.youtube.com/watch?v=newvid00001",
    }
    with get_conn(db) as conn, transaction(conn):
        is_new, url_changed, old_vid, _ = _upsert_theme(
            conn, media_type="movie", tmdb_id=81001,
            record=new_record, upstream_source="themoviedb",
            sync_ts="2026-06-01",
        )
    assert not is_new
    assert url_changed, "v1.15.81: changed video_id must set url_changed=True"
    assert old_vid == "fpvideo001"
    # Verify the title was actually updated (slow-path ran).
    with get_conn(db) as conn:
        row = conn.execute(
            "SELECT title, tdb_content_fingerprint FROM themes "
            "WHERE tmdb_id = 81001"
        ).fetchone()
        assert row["title"] == "FP Movie Renamed"
        assert row["tdb_content_fingerprint"] != "stale-hash-from-prior-sync", (
            "v1.15.81: slow-path must write the new fingerprint so the "
            "next sync's fast-path can compare against it"
        )


def test_legacy_null_fingerprint_runs_slow_path_and_backfills(tmp_path):
    """Edge case: legacy rows from pre-v1.15.81 have NULL
    fingerprint. The compare (None == new_fp) is False so the
    slow-path runs + backfills the fingerprint. After one full
    sync cycle every row has a fingerprint and subsequent syncs
    take the fast-path."""
    from app.core.sync import _record_fingerprint, _upsert_theme
    db = tmp_path / "test.db"
    init_db(db)
    _seed_theme(db, fingerprint=None)
    record = {
        "imdb_id": "tt81001",
        "title": "FP Movie",
        "release_date": "2020-01-01",
        "youtube_theme_url": "https://www.youtube.com/watch?v=fpvideo001",
    }
    with get_conn(db) as conn, transaction(conn):
        _upsert_theme(
            conn, media_type="movie", tmdb_id=81001,
            record=record, upstream_source="themoviedb",
            sync_ts="2026-06-01",
        )
    # After one sync, fingerprint is populated.
    with get_conn(db) as conn:
        row = conn.execute(
            "SELECT tdb_content_fingerprint FROM themes "
            "WHERE tmdb_id = 81001"
        ).fetchone()
        assert row["tdb_content_fingerprint"] == _record_fingerprint(record), (
            "v1.15.81: legacy NULL fingerprint must be backfilled on "
            "first sync after the migration"
        )


def test_insert_writes_fingerprint_on_new_theme(tmp_path):
    """is_new path: a freshly INSERTed theme must carry its
    fingerprint so the next sync's fast-path can compare."""
    from app.core.sync import _record_fingerprint, _upsert_theme
    db = tmp_path / "test.db"
    init_db(db)
    record = {
        "imdb_id": "tt81002",
        "title": "Brand New",
        "release_date": "2026-01-01",
        "youtube_theme_url": "https://www.youtube.com/watch?v=newrow00001",
    }
    with get_conn(db) as conn, transaction(conn):
        is_new, _, _, _ = _upsert_theme(
            conn, media_type="movie", tmdb_id=81002,
            record=record, upstream_source="themoviedb",
            sync_ts="2026-06-01",
        )
    assert is_new
    with get_conn(db) as conn:
        row = conn.execute(
            "SELECT tdb_content_fingerprint FROM themes "
            "WHERE tmdb_id = 81002"
        ).fetchone()
        assert row["tdb_content_fingerprint"] == _record_fingerprint(record)
