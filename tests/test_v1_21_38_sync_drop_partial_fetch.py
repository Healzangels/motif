"""v1.21.38 — drop detection is skipped when the sync fetch was incomplete.

Silent-failure audit finding H1: `_detect_and_stamp_drops_full_walk` keys
on `last_seen_sync_at`, and ran after EVERY snapshot/remote walk with no
error-awareness. A transient per-item timeout (counted in stats.errors but
never upserted) or a swallowed index page left real rows with a stale
last_seen_sync_at → the sweep mis-stamped them as TDB-dropped and DELETEd
their pending_updates. The 5% cap doesn't catch a handful of timeouts.

Fix: `_fetch_index` / snapshot.index now report failed pages → folded into
stats.errors; the full-walk sweep is skipped (fail-safe) whenever this
run's fetch had ANY error. The git-diff sweep keys on the changeset, not
last_seen_sync_at, so it stays ungated.
"""
from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from app.core.db import get_conn, init_db


@pytest.fixture
def db_path(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    return db


def _seed_theme(db, *, tmdb_id, last_seen, mt="movie"):
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES (?, ?, ?, 'themoviedb', ?, ?)",
            (mt, tmdb_id, f"T{tmdb_id}", last_seen, last_seen))


def _drop_stamp(db, tmdb_id, mt="movie"):
    with get_conn(db) as conn:
        r = conn.execute(
            "SELECT tdb_dropped_at FROM themes WHERE media_type=? "
            "AND tmdb_id=?", (mt, tmdb_id)).fetchone()
    return r["tdb_dropped_at"] if r else "MISSING"


# ── unit: _fetch_index reports failed pages ──────────────────


def test_fetch_index_counts_failed_pages():
    from app.core.sync import _fetch_index

    def handler(request):
        url = str(request.url)
        if url.endswith("pages.json"):
            return httpx.Response(200, json={"pages": 2})
        if url.endswith("all_page_1.json"):
            return httpx.Response(200, json=[{"id": 1, "imdb_id": None}])
        if url.endswith("all_page_2.json"):
            return httpx.Response(500)  # one page fails
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    items, failed = _fetch_index(client, "http://x", "movies")
    assert len(items) == 1
    assert failed == 1, "a 5xx page must be counted as a failed page"


def test_fetch_index_clean_run_reports_zero_failed():
    from app.core.sync import _fetch_index

    def handler(request):
        url = str(request.url)
        if url.endswith("pages.json"):
            return httpx.Response(200, json={"pages": 1})
        if url.endswith("all_page_1.json"):
            return httpx.Response(200, json=[{"id": 1, "imdb_id": None}])
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    items, failed = _fetch_index(client, "http://x", "movies")
    assert len(items) == 1 and failed == 0


# ── integration: gate via run_sync ───────────────────────────


def _run_remote(db, monkeypatch, *, failed_pages):
    """Drive run_sync(source=remote) with a 1-item movie index reporting
    `failed_pages`. Returns whether the full-walk drop sweep was reached."""
    import app.core.sync as sync
    seen = {"swept": False}
    real = sync._detect_and_stamp_drops_full_walk

    def spy(*a, **k):
        seen["swept"] = True
        return real(*a, **k)

    monkeypatch.setattr(sync, "_detect_and_stamp_drops_full_walk", spy)

    def fake_index(client, base_url, media_path):
        if media_path == "movies":
            return ([{"id": 1000, "imdb_id": None}], failed_pages)
        return ([], 0)

    monkeypatch.setattr(sync, "_fetch_index", fake_index)

    # Return a VALID record so the item upserts cleanly (a None record is
    # itself counted as a fetch error by _do_fetch — which would inflate
    # stats.errors and mask whether the GATE is what's blocking the sweep).
    def fake_item(client, base_url, media_path, *, imdb_id, tmdb_id):
        return {"id": tmdb_id, "imdb_id": "tt99",
                "title": "Item",
                "youtube_theme_url":
                    "https://www.youtube.com/watch?v=item0000001"}

    monkeypatch.setattr(sync, "_fetch_item", fake_item)
    sync.run_sync(db, "http://fake.invalid", source="remote",
                  enqueue_downloads=False)
    return seen["swept"]


def test_drop_sweep_skipped_when_fetch_incomplete(db_path, monkeypatch):
    # A stale theme that WOULD be stamped dropped by the sweep.
    _seed_theme(db_path, tmdb_id=42, last_seen="2020-01-01T00:00:00Z")
    swept = _run_remote(db_path, monkeypatch, failed_pages=1)
    assert swept is False, (
        "drop sweep must be SKIPPED when the run had a fetch error")
    assert _drop_stamp(db_path, 42) is None, (
        "a stale theme must NOT be mis-stamped dropped on a partial fetch")


def test_drop_sweep_runs_on_clean_fetch(db_path, monkeypatch):
    _seed_theme(db_path, tmdb_id=42, last_seen="2020-01-01T00:00:00Z")
    swept = _run_remote(db_path, monkeypatch, failed_pages=0)
    assert swept is True, "drop sweep must run on a clean (error-free) fetch"
    assert _drop_stamp(db_path, 42) is not None, (
        "on a clean fetch the genuinely-stale theme IS stamped dropped")
