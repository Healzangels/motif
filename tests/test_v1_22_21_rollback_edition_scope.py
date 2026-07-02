"""v1.22.21 — download-failure rollback is edition-scoped (data-loss).

Edition-audit finding. ACCEPT UPDATE / REVERT delete-then-redownload: the
endpoint eagerly deletes the clicked edition's user_override + flips its
pending decision, stamps a `rollback` recipe, and queues the download. On
TERMINAL download failure the worker's _run_rollback_safe undoes the prep. But
the recipe dropped the edition_key, so the rollback:
  - re-pended pending_updates for the section with NO edition filter → a
    sibling edition's accepted/declined decision got wrongly reset;
  - restored the override via an INSERT that OMITTED the edition_key column →
    it defaulted to '' → a NON-'' edition's user URL was deleted by the
    endpoint then RESTORED ONTO THE STANDARD ('') ROW, clobbering the standard
    edition's own override and losing the real one. Silent data-loss.

Fix: the endpoints (api_accept_update, api_accept_all_updates, api_revert) stamp
rollback['edition_key']; _run_rollback_safe threads it into the 2 WHEREs + the
2 INSERT column lists. Default '' keeps old in-flight jobs at standard scope.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-06-06T00:00:00"


def _settings(tmp_path):
    from app.config import Settings
    from app.core.db import init_db
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    return s


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def _ovr(conn, tmdb, edition, url):
    conn.execute(
        "INSERT INTO user_overrides (media_type, tmdb_id, section_id,"
        " edition_key, youtube_url, intent, set_at, set_by)"
        " VALUES ('tv',?, '3', ?, ?, 'replace', ?, 'admin')",
        (tmdb, edition, url, NOW))


def _pending(conn, tmdb, edition, decision):
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
        " edition_key, kind, new_youtube_url, decision, detected_at)"
        " VALUES ('tv',?, '3', ?, 'upstream_changed', 'https://yt/new', ?, ?)",
        (tmdb, edition, decision, NOW))


def _fake_job(db, *, tmdb, rollback):
    payload = json.dumps({"rollback": rollback})
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at)"
            " VALUES ('download','tv',?, '3', ?, 'failed', ?)",
            (tmdb, payload, NOW))
        conn.commit()
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c.execute("SELECT * FROM jobs WHERE tmdb_id=?", (tmdb,)).fetchone()


def _ovr_url(db, tmdb, edition):
    with sqlite3.connect(db) as conn:
        r = conn.execute(
            "SELECT youtube_url FROM user_overrides WHERE tmdb_id=?"
            " AND section_id='3' AND edition_key=?", (tmdb, edition)).fetchone()
    return r[0] if r else None


def _decision(db, tmdb, edition):
    with sqlite3.connect(db) as conn:
        r = conn.execute(
            "SELECT decision FROM pending_updates WHERE tmdb_id=?"
            " AND section_id='3' AND edition_key=?", (tmdb, edition)).fetchone()
    return r[0] if r else None


# ── ACCEPT rollback restores to the RIGHT edition ────────────


def test_accept_rollback_restores_clicked_edition_not_standard(tmp_path):
    """User ACCEPTed on the Extended edition; its override was deleted; its
    download fails. The rollback must restore Extended's override onto the
    Extended row AND leave the Standard ('') edition's override + decision
    untouched (pre-fix it clobbered '' and re-pended its decision)."""
    settings = _settings(tmp_path)
    db = settings.db_path
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES ('tv',7001,'Multi','imdb',?,?,'https://yt/tdb')",
            (NOW, NOW))
        # Standard edition keeps its own override + a still-'accepted' decision.
        _ovr(conn, 7001, "", "https://yt/STD-OWN")
        _pending(conn, 7001, "", "accepted")
        # Extended's override was already DELETED by the accept; its decision
        # is 'accepted' awaiting the (now-failed) download.
        _pending(conn, 7001, "extended", "accepted")
        conn.commit()

    job = _fake_job(db, tmdb=7001, rollback={
        "kind": "accept_update",
        "edition_key": "extended",
        "replaced_user_url": "https://yt/EXT-OWN",
        "prior_intent": "replace",
    })
    _worker(settings)._run_rollback_safe(job, "download failed")

    assert _ovr_url(db, 7001, "extended") == "https://yt/EXT-OWN", (
        "v1.22.21: Extended's deleted override must be restored to the "
        "Extended row")
    assert _ovr_url(db, 7001, "") == "https://yt/STD-OWN", (
        "v1.22.21: the Standard edition's override must be UNTOUCHED — pre-fix "
        "the misdirected restore clobbered it (data-loss)")
    assert _decision(db, 7001, "extended") == "pending", (
        "Extended's decision re-pends")
    assert _decision(db, 7001, "") == "accepted", (
        "v1.22.21: the Standard edition's decision must NOT be re-pended")


# ── REVERT rollback DELETE is edition-scoped ─────────────────


def test_revert_rollback_delete_spares_sibling_edition(tmp_path):
    """A REVERT-with-no-prior-override rollback (the DELETE branch) must drop
    ONLY the clicked edition's override, not every edition's."""
    settings = _settings(tmp_path)
    db = settings.db_path
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES ('tv',7002,'Multi','imdb',?,?,'https://yt/tdb')",
            (NOW, NOW))
        _ovr(conn, 7002, "", "https://yt/STD-OWN")
        _ovr(conn, 7002, "extended", "https://yt/EXT-OWN")
        conn.commit()

    job = _fake_job(db, tmdb=7002, rollback={
        "kind": "revert",
        "edition_key": "extended",
        "prior_override_url": None,  # → the else DELETE branch
        "prior_previous_url_row": None,
        "prior_intent": "replace",
    })
    _worker(settings)._run_rollback_safe(job, "download failed")

    assert _ovr_url(db, 7002, "extended") is None, (
        "Extended's override is cleared (no prior to restore)")
    assert _ovr_url(db, 7002, "") == "https://yt/STD-OWN", (
        "v1.22.21: the REVERT rollback DELETE must spare the Standard "
        "edition's override (pre-fix the section-wide DELETE wiped it)")


# ── Producer side: endpoints stamp edition_key into the recipe ─


def test_accept_and_revert_rollback_recipes_carry_edition_key():
    # api_accept_update
    i = API_PY.index('rollback = {"kind": "accept_update"}')
    assert 'rollback["edition_key"] = _acc_edition' in API_PY[i:i + 400]
    # api_accept_all_updates (bulk)
    j = API_PY.index('bulk_rollback = {"kind": "accept_update"}')
    assert 'bulk_rollback["edition_key"] = edition' in API_PY[j:j + 300]
    # api_revert
    k = API_PY.index('"kind": "revert",')
    assert '"edition_key": _rev_edition' in API_PY[k:k + 400]


def test_rollback_writes_thread_edition_key_source_pin():
    # v0.51.11: the rollback body moved to module-level apply_job_rollback (so
    # the API cancel paths can share it); the pin follows the body.
    start = WORKER_PY.index("def apply_job_rollback(")
    end = WORKER_PY.index("\nclass ", start + 10)
    body = WORKER_PY[start:end]
    assert '_rb_edition = rb.get("edition_key", "")' in body
    # Both WHEREs carry edition_key; both INSERTs add the column.
    assert body.count("AND edition_key = ?") >= 2, (
        "v1.22.21: the pending re-pend + the revert DELETE must filter "
        "edition_key")
    assert body.count("section_id, edition_key, intent") >= 2, (
        "v1.22.21: both override-restore INSERTs must include the edition_key "
        "column")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
