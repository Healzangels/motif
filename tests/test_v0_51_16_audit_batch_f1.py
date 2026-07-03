"""v0.51.16 — round-4 audit Batch F1: backend security + behavior.

Findings fixed here (all CONFIRMED by the adversarial verify pass):

  #19 (auth.py) — password rotation now revokes every OTHER session.
    Pre-fix a stolen/lingering motif_sess cookie survived the rotation
    for its full 30-day TTL (lookup_session binds only on id + expiry,
    not the password hash), defeating the point of rotating. The
    caller's own session survives via keep_session_id; API tokens are
    deliberately untouched (own lifecycle + revocation UI).

  #2 (worker.py) — the v1.24.45 collection downscale had no media_type
    gate, so an over-ceiling MOVIE/TV API push was silently re-encoded
    to a lower bitrate and uploaded, making the v1.18.69 full-quality
    sidecar fallback unreachable. Movie/TV over-ceiling now skips the
    downscale AND the doomed ~10MB POST (v1.21.99 pattern),
    synthesizing the (False, None) outcome the fallback gate accepts.

  #23 (worker.py) — _do_place created its PlexClient (eager
    httpx.Client) ~160 lines before the only finally that closed it;
    the pre-place cancel checkpoint's return and any DB raise in the
    pi-resolution span leaked the client + sockets. Creation moved to
    just before the place_theme try, inside the finally's coverage.

  #24 (worker.py) — _safe_mark's final attempt re-raised on the stale
    theory that run()'s OperationalError catch would absorb it; that
    catch only wraps _claim_next_job. An escape from _mark_done landed
    in the crash handler and re-pended an ALREADY-COMPLETED job; an
    escape from the except-block marks killed the worker thread — the
    exact v1.11.51 failure. Locked-on-final now logs ERROR and gives
    up (stuck-job sweep reclaims); non-locked still propagates.

  #25 (scheduler.py) — sync hour 0 wraps section_refresh to 23:00 the
    PREVIOUS day; with restricted dom/dow the refresh fired ~23h AFTER
    the sync instead of 1h before. Hour-0 + restricted day fields now
    drop dom/dow (daily at 23:mm).

  #26 (cloud_theme_backup.py) — the force walk mints plex_orphan
    themes rows + theme_id stamps minutes before the download stage;
    any fetch/disk failure or cancel stranded a linked-but-empty
    orphan (skipped forever by theme_id-IS-NULL resolve passes).
    Targets now carry a 'minted' flag and unmint_stale_orphans
    compensates at the run's exits — guarded so it deletes ONLY
    still-plex_orphan synthetic rows with no local_files row.
"""
from __future__ import annotations

import logging
import sqlite3
import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()

G40 = "c" * 40


# ── #19: password rotation revokes other sessions ─────────────


def _auth_db(tmp_path: Path) -> Path:
    from app.core.auth import init_auth_schema, create_admin
    db = tmp_path / "auth.db"
    init_auth_schema(db)
    create_admin(db, username="admin", password="old-password")
    return db


def test_rotation_revokes_other_sessions_keeps_own(tmp_path):
    from app.core.auth import (
        change_admin_password, create_session, lookup_session,
    )
    db = _auth_db(tmp_path)
    keep = create_session(db, username="admin", user_agent="me")
    stolen = create_session(db, username="admin", user_agent="thief")
    assert lookup_session(db, stolen) == "admin"
    ok = change_admin_password(
        db, current_password="old-password", new_password="new-password",
        keep_session_id=keep,
    )
    assert ok is True
    assert lookup_session(db, keep) == "admin", (
        "v0.51.16 #19: the rotating admin's own session must survive")
    assert lookup_session(db, stolen) is None, (
        "v0.51.16 #19: every OTHER session must be revoked on rotation")


def test_rotation_without_keep_revokes_all(tmp_path):
    from app.core.auth import (
        change_admin_password, create_session, lookup_session,
    )
    db = _auth_db(tmp_path)
    s1 = create_session(db, username="admin", user_agent="a")
    s2 = create_session(db, username="admin", user_agent="b")
    assert change_admin_password(
        db, current_password="old-password", new_password="new-password",
    )
    assert lookup_session(db, s1) is None
    assert lookup_session(db, s2) is None


def test_rotation_failure_revokes_nothing(tmp_path):
    from app.core.auth import (
        change_admin_password, create_session, lookup_session,
    )
    db = _auth_db(tmp_path)
    s1 = create_session(db, username="admin", user_agent="a")
    assert change_admin_password(
        db, current_password="WRONG", new_password="new-password",
    ) is False
    assert lookup_session(db, s1) == "admin", (
        "a failed rotation (bad current password) must not touch sessions")


def test_rotation_keeps_api_tokens(tmp_path):
    from app.core.auth import (
        authenticate_token, change_admin_password, create_api_token,
    )
    db = _auth_db(tmp_path)
    _tid, raw = create_api_token(db, name="ci", scope="read")
    assert change_admin_password(
        db, current_password="old-password", new_password="new-password",
    )
    assert authenticate_token(db, raw) is not None, (
        "v0.51.16 #19: API tokens have their own lifecycle — rotation "
        "must NOT revoke them")


def test_change_password_endpoint_passes_own_cookie():
    """The API endpoint must thread the caller's session cookie through
    as keep_session_id so the admin isn't logged out by their own
    rotation."""
    assert "keep_session_id=request.cookies.get(SESSION_COOKIE)" in API_PY


# ── #2: over-ceiling movie/TV skips downscale + doomed POST ───


def test_skip_doomed_upload_gated_on_media_type():
    assert ('len(audio_bytes) >= _ceiling_bytes and '
            'media_type != "collection")') in WORKER_PY, (
        "v0.51.16 #2: the doomed-POST skip must exempt collections "
        "(folderless — the downscale is FOR them)")
    # The downscale gate must exclude the skip cohort.
    assert ('if len(audio_bytes) >= _ceiling_bytes and '
            'not _skip_doomed_upload:') in WORKER_PY


def test_skip_doomed_upload_synthesizes_fallback_shape():
    """The synthesized outcome must be the (False, None) shape the
    v1.18.69 sidecar-fallback gate accepts (http_status in (500, None))."""
    i = WORKER_PY.index("over upload ceiling — POST skipped (v0.51.16)")
    block = WORKER_PY[i - 300:i]
    assert "False, None," in block
    # The fallback gate still accepts a None http_status.
    assert "http_status in (500, None)" in WORKER_PY


# ── #23: place PlexClient created inside the finally's coverage ──


def test_place_client_created_after_cancel_checkpoint():
    """The eager httpx client must be created AFTER the pre-place
    cancel checkpoint (which returns without any close) and adjacent
    to the try/finally that closes it."""
    i_ckpt = WORKER_PY.index("cancelled at pre-place_theme checkpoint")
    assert WORKER_PY.count("plex_client = self._plex_client()") == 1, (
        "v0.51.16 #23: exactly one creation site for _do_place's client")
    i_create = WORKER_PY.index("plex_client = self._plex_client()")
    assert i_create > i_ckpt, (
        "v0.51.16 #23: creation must follow the cancel checkpoint — the "
        "checkpoint's early return leaked the client pre-fix")
    span = WORKER_PY[i_create:i_create + 400]
    assert "outcome = place_theme(" in span, (
        "creation must sit immediately before the place_theme try so the "
        "finally covers the client's whole lifetime")


def test_place_resolution_span_has_no_client_use():
    """Nothing between the old creation point (the _do_place
    _index_for_section line) and the new one may use plex_client."""
    i_index = WORKER_PY.rindex(
        "index = self._index_for_section(section_id)",
        0, WORKER_PY.index("cancelled at pre-place_theme checkpoint"))
    i_create = WORKER_PY.index("plex_client = self._plex_client()")
    assert "plex_client" not in WORKER_PY[
        i_index + 50:i_create].replace(
        "# lines and several early returns/raises before the only finally",
        "").replace("closed it, leaking a client + open sockets", ""), (
        "the resolution span must not touch plex_client (comment "
        "mentions allowed)")


# ── #24: _safe_mark final attempt — locked swallows, loudly ───


def _bare_worker():
    from app.core.worker import Worker
    w = object.__new__(Worker)
    w.stop_event = threading.Event()
    w.stop_event.set()  # retries return instantly
    return w


def test_safe_mark_swallows_locked_on_final_attempt(caplog):
    w = _bare_worker()
    calls = {"n": 0}

    def always_locked():
        calls["n"] += 1
        raise sqlite3.OperationalError("database is locked")

    with caplog.at_level(logging.ERROR, logger="app.core.worker"):
        w._safe_mark(always_locked)  # must NOT raise
    assert calls["n"] == 5, "4 ladder retries + 1 final attempt"
    assert any(
        "giving up" in r.getMessage() for r in caplog.records
    ), "v0.51.16 #24: the final-attempt swallow must log ERROR loudly"


def test_safe_mark_non_locked_still_raises():
    w = _bare_worker()

    def schema_bug():
        raise sqlite3.OperationalError("no such table: jobs")

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        w._safe_mark(schema_bug)


def test_safe_mark_stale_comment_gone():
    """The lie that run()'s OperationalError catch absorbs the re-raise
    must not survive (that catch only wraps _claim_next_job)."""
    assert ("the run() loop's OperationalError catch above will absorb"
            not in WORKER_PY)


# ── #25: section_refresh hour-0 wrap drops day restrictions ───


def test_section_refresh_hour0_drops_day_restrictions():
    assert 'if int(hour) == 0 and (dom != "*" or dow != "*"):' in SCHED_PY
    assert 'section_dom, section_dow = "*", "*"' in SCHED_PY
    # The trigger must consume the (possibly widened) copies, not the
    # raw sync fields.
    assert "day=section_dom" in SCHED_PY
    assert "day_of_week=section_dow" in SCHED_PY


# ── #26: force-walk mint gets a compensating unmint ───────────


def _seed_section(conn):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, themes_subdir, "
        "   included, discovered_at, last_seen_at) "
        "VALUES ('1', 'Collections', 'movie', 0, 0, 'movies', 1, "
        "        '2026-07-02', '2026-07-02')")


def _seed_no_tdb_row(conn, *, rk, title="A24 Films"):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        "  guid_tmdb, theme_id, title, year, has_theme, "
        "  first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'collection', NULL, NULL, ?, NULL, 1, "
        "        '2026-07-02', '2026-07-02')",
        (rk, title))


def _selected_upload_themes(*, rating_key):
    return {"ok": True, "http_status": 200, "error": None,
            "body": {"MediaContainer": {"Metadata": [
                {"ratingKey": "upload://themes/" + G40, "selected": True},
            ]}}}


def _forced_walk(conn):
    from app.core.cloud_theme_backup import identify_c1_rows
    plex = MagicMock()
    plex.get_themes.side_effect = _selected_upload_themes
    return identify_c1_rows(
        conn, plex, inter_call_sleep_s=0, use_cursor=False,
        rks_scope=["rk-a24"], force=True)


def test_force_mint_target_carries_minted_flag(tmp_path):
    from app.core.db import init_db
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn)
        _seed_no_tdb_row(conn, rk="rk-a24")
        conn.commit()
        forced = _forced_walk(conn)
        assert len(forced) == 1
        assert forced[0]["minted"] is True, (
            "v0.51.16 #26: a walk that MINTED must flag the target")
        # A second run resolves via the stamped theme_id — no new mint.
        second = _forced_walk(conn)
        assert second[0]["minted"] is False, (
            "a resolve via existing theme_id is NOT a mint — unmint must "
            "never touch it")
        assert second[0]["guid_tmdb"] == forced[0]["guid_tmdb"]


def test_unmint_removes_linked_but_empty_orphan(tmp_path):
    from app.core.db import init_db
    from app.core.cloud_theme_backup import unmint_stale_orphans
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        _seed_section(conn)
        _seed_no_tdb_row(conn, rk="rk-a24")
        conn.commit()
        forced = _forced_walk(conn)
        synth = forced[0]["guid_tmdb"]
        assert conn.execute(
            "SELECT COUNT(*) FROM themes WHERE tmdb_id = ?",
            (synth,)).fetchone()[0] == 1
        # Simulate "download never landed" — no local_files write.
        removed = unmint_stale_orphans(conn, forced)
        assert removed == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM themes WHERE tmdb_id = ?",
            (synth,)).fetchone()[0] == 0, (
            "v0.51.16 #26: the stranded mint must be deleted")
        pirow = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key = 'rk-a24'"
        ).fetchone()
        assert pirow["theme_id"] is None, (
            "the theme_id stamp must be cleared so theme_id-IS-NULL "
            "resolve passes can re-link the row")
        # Idempotent — a second call finds nothing.
        assert unmint_stale_orphans(conn, forced) == 0


def test_unmint_leaves_completed_backups(tmp_path):
    from app.core.db import init_db
    from app.core.cloud_theme_backup import unmint_stale_orphans
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        _seed_section(conn)
        _seed_no_tdb_row(conn, rk="rk-a24")
        conn.commit()
        forced = _forced_walk(conn)
        synth = forced[0]["guid_tmdb"]
        # Simulate the download stage SUCCEEDING for this target.
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            "  edition_key, file_path, downloaded_at, source_video_id, "
            "  source_kind, last_place_attempt_reason) "
            "VALUES ('collection', ?, '1', '', 'movies/x/theme.mp3', "
            "        '2026-07-02', 'plex:themes/" + G40 + "', "
            "        'plex_cloud', 'backup_only')",
            (synth,))
        conn.commit()
        assert unmint_stale_orphans(conn, forced) == 0, (
            "v0.51.16 #26: a mint whose backup COMPLETED must survive")
        assert conn.execute(
            "SELECT COUNT(*) FROM themes WHERE tmdb_id = ?",
            (synth,)).fetchone()[0] == 1


def test_unmint_ignores_unminted_targets(tmp_path):
    from app.core.db import init_db
    from app.core.cloud_theme_backup import unmint_stale_orphans
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        # A target dict WITHOUT the minted flag (e.g. real-guid row) must
        # be a no-op even if a matching orphan row existed.
        assert unmint_stale_orphans(conn, [
            {"media_type": "movie", "guid_tmdb": -5, "rating_key": "x"},
            {"media_type": "movie", "guid_tmdb": -6, "rating_key": "y",
             "minted": False},
        ]) == 0


def test_backup_run_wires_unmint_at_both_exits():
    """The runner must compensate on BOTH normal exits: the
    cancel-during-walk early return and the post-download-loop path
    (which covers per-target failures + the mid-loop cancel break)."""
    assert API_PY.count("unmint_stale_orphans(conn, targets)") == 2
    assert ("identify_c1_rows, backup_cloud_theme, unmint_stale_orphans"
            in API_PY)
