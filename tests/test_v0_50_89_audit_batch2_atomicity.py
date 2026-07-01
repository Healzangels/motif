"""v0.50.89 — holistic-audit Batch 2: data-loss / transaction-atomicity.

1. cloud_theme_backup.py `backup_cloud_theme` writes the file to disk BEFORE
   its DB-recording transaction; that transaction wasn't wrapped in
   try/except, violating the function's documented "Never raises" contract
   and (per the caller in api.py) aborting the ENTIRE batch on one failure.
2. sync.py `_detect_and_stamp_drops_git` (the default/primary transport)
   ran its tdb_dropped_at stamp + companion pending_updates DELETE in
   autocommit, unlike its v1.24.15-hardened full-walk sibling.
3. scheduler.py `_restore_lost_placements` split its dedup check and its
   place-job INSERT across two separate connections/transactions with a
   filesystem stat loop in between — a competing place job landing in that
   window was invisible to the dedup check.
4. runtime.py `get_runtime_bool`'s seed-on-first-read was a bare INSERT
   with no ON CONFLICT — two concurrent callers racing an unseeded key
   could both attempt it, and the loser raised an uncaught IntegrityError.
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()
CLOUD_PY = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()


# ── 1. cloud_theme_backup.py never raises on a DB-write failure ────────

def test_backup_cloud_theme_db_write_is_try_except_wrapped():
    i = CLOUD_PY.index("def backup_cloud_theme(")
    body = CLOUD_PY[i:]  # last function in the file
    assert "try:\n        with transaction(conn):" in body, (
        "the local_files-recording transaction must be inside a try so a "
        "failure returns an error dict instead of propagating uncaught "
        "(the function's own documented 'Never raises' contract)"
    )
    assert '"error": f"db: {e!r}"' in body


def test_backup_cloud_theme_returns_error_dict_on_db_failure(tmp_path, monkeypatch):
    """Force the transaction to raise and confirm the function returns
    {"ok": False, ...} instead of propagating — the disk write (already
    complete by this point) must not crash the caller's whole batch."""
    from app.core.db import init_db, get_conn
    from app.core.events import now_iso
    import app.core.cloud_theme_backup as ctb

    db = tmp_path / "m.db"
    init_db(db)
    now = now_iso()
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, included,"
            " discovered_at, last_seen_at, themes_subdir)"
            " VALUES ('1','Movies','movie',1,?,?,'movies')", (now, now))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " guid_tmdb, title, year, has_theme, local_theme_file,"
            " folder_path, first_seen_at, last_seen_at)"
            " VALUES ('rk1','1','movie',900,'T',2020,1,0,"
            " '/data/T', ?, ?)", (now, now))

    def _boom(*a, **kw):
        raise sqlite3.OperationalError("database is locked")

    # backup_cloud_theme does `from .db import transaction` INSIDE the
    # function body, so the patch must target app.core.db.transaction (the
    # source it re-imports from on every call), not the cloud_theme_backup
    # module's own namespace.
    import app.core.db as db_module
    monkeypatch.setattr(db_module, "transaction", _boom)
    monkeypatch.setattr(
        ctb, "_section_themes_subdir", lambda conn, section_id: "movies",
    )

    target = {
        "rating_key": "rk1", "media_type": "movie", "guid_tmdb": 900,
        "title": "T", "year": "2020", "section_id": "1",
        "entry_uri": "metadata://themes/deadbeef", "sha1": "deadbeef" * 5,
    }

    class _FakeResponse:
        status_code = 200
        content = b"x" * 1000
        text = ""
        headers = {"content-length": "1000"}

    class _FakeHttpClient:
        def get(self, *a, **kw):
            return _FakeResponse()

    class _FakePlex:
        _client = _FakeHttpClient()
        _headers = {}

        def _rk_path(self, rk, suffix):
            return f"/library/metadata/{rk}{suffix}"

    with get_conn(db) as conn:
        result = ctb.backup_cloud_theme(conn, target, themes_dir, _FakePlex())

    assert result["ok"] is False
    assert "db:" in (result.get("error") or ""), result


# ── 2. sync.py _detect_and_stamp_drops_git is transactional ────────────

def test_git_drop_stamp_clear_is_transactional():
    i = SYNC_PY.index("def _detect_and_stamp_drops_git(")
    body = SYNC_PY[i:i + 10000]
    assert "with get_conn(db_path) as conn, transaction(conn):" in body, (
        "the git-path tdb_dropped_at stamp + its companion pending_updates "
        "DELETE must share one transaction, mirroring the v1.24.15 fix "
        "already applied to _detect_and_stamp_drops_full_walk"
    )
    assert "UPDATE themes SET tdb_dropped_at = ?" in body
    assert "DELETE FROM pending_updates" in body


# ── 3. scheduler.py _restore_lost_placements re-checks dedup in-txn ────

def test_restore_lost_placements_rechecks_dedup_inside_transaction():
    i = SCHED_PY.index("def _restore_lost_placements(")
    j = SCHED_PY.index("\ndef ", i + 10)
    body = SCHED_PY[i:j]
    txn_idx = body.index("with get_conn(db_path) as conn, transaction(conn):")
    insert_idx = body.index("INSERT INTO jobs", txn_idx)
    recheck_idx = body.index("still_clear", txn_idx)
    assert txn_idx < recheck_idx < insert_idx, (
        "the in-flight-job recheck must run INSIDE the transaction and "
        "BEFORE the INSERT, closing the TOCTOU window between the earlier "
        "(separate-connection) dedup SELECT and this INSERT"
    )


def test_restore_lost_placements_skips_row_with_competing_job(tmp_path, monkeypatch):
    """If a competing place job appears for a candidate row between the
    initial SELECT and the transaction, the sweep must skip it instead of
    inserting a duplicate."""
    from app.config import Settings
    from app.core.db import init_db, get_conn
    from app.core.events import now_iso
    import app.core.scheduler as sched

    tmp_path_data = tmp_path / "data"
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path_data)
    db = settings.db_path
    init_db(db)
    now = now_iso()
    themes_dir = tmp_path_data
    folder = themes_dir / "Movies" / "T (2020)"
    folder.mkdir(parents=True)

    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, included,"
            " discovered_at, last_seen_at, themes_subdir)"
            " VALUES ('1','Movies','movie',1,?,?,'movies')", (now, now))
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, youtube_url, last_seen_sync_at,"
            " first_seen_sync_at) VALUES ('movie', 950, 'T', '2020',"
            " 'themoviedb', 'https://example.com/x', ?, ?)", (now, now))
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, file_path, downloaded_at, source_video_id,"
            " canonical_present) VALUES ('movie', 950, '1', '',"
            " 'movies/T (2020)/theme.mp3', ?, 'vid', 1)", (now,))
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " edition_key, media_folder, placement_kind, placed_at,"
            " theme_present) VALUES ('movie', 950, '1', '', ?,"
            " 'hardlink', ?, 0)", (str(folder), now))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " guid_tmdb, title, year, has_theme, local_theme_file,"
            " folder_path, first_seen_at, last_seen_at)"
            " VALUES ('rk1', '1', 'movie', 950, 'T', 2020, 0, 0, ?, ?, ?)",
            (str(folder), now, now))

    # Simulate a competing place job appearing AFTER the initial dedup
    # SELECT runs, by inserting it from inside get_conn right before the
    # transaction block opens — patch get_conn to insert-once on first call
    # after the initial read completes.
    real_get_conn = sched.get_conn
    state = {"reads": 0}

    class _ConnWrapper:
        def __init__(self, real_cm):
            self._real_cm = real_cm

        def __enter__(self):
            conn = self._real_cm.__enter__()
            state["reads"] += 1
            if state["reads"] == 1:
                # First call is the dedup SELECT block — after it returns,
                # simulate a race by inserting the competing job directly
                # via a fresh connection (mimics another process/thread).
                import sqlite3 as _sqlite3
                with _sqlite3.connect(db) as race_conn:
                    race_conn.execute(
                        "INSERT INTO jobs (job_type, media_type, tmdb_id,"
                        " section_id, payload, status, created_at,"
                        " next_run_at) VALUES ('place', 'movie', 950, '1',"
                        " '{\"edition_key\": \"\"}', 'pending', ?, ?)",
                        (now, now))
                    race_conn.commit()
            return conn

        def __exit__(self, *a):
            return self._real_cm.__exit__(*a)

    def fake_get_conn(path):
        return _ConnWrapper(real_get_conn(path))

    monkeypatch.setattr(sched, "get_conn", fake_get_conn)

    sched._restore_lost_placements(settings)

    with get_conn(db) as conn:
        place_jobs = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='place'"
        ).fetchone()[0]
    assert place_jobs == 1, (
        "v0.50.89: the sweep must NOT insert a duplicate place job when a "
        "competing one appeared after the initial dedup read — pre-fix "
        "there was no recheck inside the final transaction"
    )


# ── 4. runtime.py get_runtime_bool seed race ────────────────────────────

def test_get_runtime_bool_concurrent_seed_does_not_raise(tmp_path):
    from app.core.db import init_db
    from app.core.runtime import get_runtime_bool

    db = tmp_path / "m.db"
    init_db(db)
    errors = []
    results = []
    lock = threading.Lock()

    def _worker():
        try:
            v = get_runtime_bool(db, "concurrent_test_key", default=True)
            with lock:
                results.append(v)
        except Exception as e:  # noqa: BLE001
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=_worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, (
        f"v0.50.89: concurrent seed-on-first-read must never raise — "
        f"got: {errors}"
    )
    assert len(results) == 8
    assert all(r == results[0] for r in results), (
        "every concurrent caller must observe the same, single winning "
        "seeded value"
    )
