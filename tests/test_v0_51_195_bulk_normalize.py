"""v0.51.195 — Phase 2 Tag 2: the bulk_normalize background op.

Levels the eligible library toward the configured target, loudest first, each row
through the SAME _normalize_one_row chokepoint // NORMALIZE uses (v0.51.194). The op
mutates real files + Plex, so it is SERIAL and cancelable at every row.

These tests drive the runner DIRECTLY with a fake chokepoint (the chokepoint has its own
behavioral test), so they pin the parts unique to bulk: the eligible-set selection, the
loudest-first order, max_rows, cancel, and the op_progress lifecycle. Plus the schema v76
migration and the start endpoint's Plex gate.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db, get_conn
from app.core.plex import THEME_UPLOAD_CEILING_BYTES

NOW = "2026-07-17T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


def _seed(db, *, tmdb, loudness_i, file_size=1_000_000, sha="s", measured=None,
          norm_state=None, hardlink=True):
    """One themed row. Eligible ⟺ raw + measured-current + hardlink + under-ceiling."""
    measured = sha if measured is None else measured
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?, 'movie', ?, ?, 'imdb', ?, ?)",
                  (tmdb, tmdb, f"M{tmdb}", NOW, NOW))
        c.execute("INSERT OR IGNORE INTO plex_items (rating_key, media_type, section_id, "
                  " guid_tmdb, edition_key, title, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'movie', '1', ?, '', ?, 1, ?, ?)",
                  (9000 + tmdb, tmdb, f"M{tmdb}", NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, loudness_measured_sha256, downloaded_at, "
                  " source_video_id, loudness_i, loudness_tp, file_size, norm_state) "
                  "VALUES ('movie', ?, '1', '', ?, ?, ?, ?, 'v', ?, -2.0, ?, ?)",
                  (tmdb, f"movies/{tmdb}/theme.mp3", sha, measured, NOW,
                   loudness_i, file_size, norm_state))
        if hardlink:
            c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, "
                      " media_folder, edition_key, placement_kind, placed_at) "
                      "VALUES ('movie', ?, '1', ?, '', 'hardlink', ?)",
                      (tmdb, f"/data/movies/{tmdb}", NOW))
        c.commit()


@pytest.fixture
def bench(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, "
                  " themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    # keep the end-of-run notify hermetic
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)
    return s


def _install_fake_chokepoint(monkeypatch, db):
    """Records the rows it's handed + stamps norm_state so idempotency holds."""
    calls = []

    def _fake(db_, settings_, row, target):
        calls.append(row["tmdb_id"])
        with sqlite3.connect(db) as c:
            c.execute("UPDATE local_files SET norm_state='normalized' WHERE tmdb_id=?",
                      (row["tmdb_id"],))
            c.commit()
        return {"ok": True, "changed": True, "plex_is_serving_it": True}

    monkeypatch.setattr("app.web.api._normalize_one_row", _fake)
    return calls


def _op(db):
    with get_conn(db) as conn:
        return conn.execute("SELECT status, detail_json FROM op_progress "
                            "WHERE op_id='bulk-normalize'").fetchone()


def test_runner_levels_only_eligible_rows_loudest_first(bench, monkeypatch):
    db = bench.db_path
    _seed(db, tmdb=1, loudness_i=-3.0)     # loudest eligible
    _seed(db, tmdb=2, loudness_i=-10.0)    # eligible
    _seed(db, tmdb=3, loudness_i=-20.0)    # quietest eligible
    _seed(db, tmdb=4, loudness_i=-5.0, file_size=THEME_UPLOAD_CEILING_BYTES + 1)  # over-ceiling
    _seed(db, tmdb=5, loudness_i=-6.0, sha="a", measured="b")   # stale measurement
    _seed(db, tmdb=6, loudness_i=-7.0, norm_state="normalized")  # already leveled
    _seed(db, tmdb=7, loudness_i=-8.0, hardlink=False)          # not hardlink-placed
    calls = _install_fake_chokepoint(monkeypatch, db)

    from app.web.api import _bulk_normalize_run
    _bulk_normalize_run(db, bench)

    # only the 3 eligible rows, loudest first
    assert calls == [1, 2, 3]
    st, detail = _op(db)
    assert st == "done"
    ds = {d["l"]: d["v"] for d in json.loads(detail)["done_summary"]}
    assert ds["leveled"] == 3
    # the ineligible rows are untouched
    with get_conn(db) as conn:
        norm = {r["tmdb_id"]: r["norm_state"] for r in
                conn.execute("SELECT tmdb_id, norm_state FROM local_files").fetchall()}
    assert norm[4] is None and norm[5] is None and norm[7] is None
    assert norm[6] == "normalized"  # was already


def test_runner_max_rows_levels_only_the_loudest_n(bench, monkeypatch):
    db = bench.db_path
    for t, i in [(1, -3.0), (2, -10.0), (3, -20.0)]:
        _seed(db, tmdb=t, loudness_i=i)
    calls = _install_fake_chokepoint(monkeypatch, db)
    from app.web.api import _bulk_normalize_run
    _bulk_normalize_run(db, bench, max_rows=1)
    assert calls == [1]   # only the loudest


def test_runner_stops_when_cancelled(bench, monkeypatch):
    db = bench.db_path
    for t, i in [(1, -3.0), (2, -10.0)]:
        _seed(db, tmdb=t, loudness_i=i)
    calls = _install_fake_chokepoint(monkeypatch, db)
    # cancel is checked at the top of every row iteration
    monkeypatch.setattr("app.core.progress.is_cancelled", lambda db_, op_id: True)
    from app.web.api import _bulk_normalize_run
    _bulk_normalize_run(db, bench)
    assert calls == []                       # cancelled before the first row
    assert _op(db)[0] == "cancelled"


def test_runner_skips_a_row_the_chokepoint_refuses(bench, monkeypatch):
    """A row that slips the selector (a race) is SKIPPED, not failed — batch continues."""
    db = bench.db_path
    _seed(db, tmdb=1, loudness_i=-3.0)
    _seed(db, tmdb=2, loudness_i=-10.0)

    def _fake(db_, s_, row, target):
        if row["tmdb_id"] == 1:
            return {"ok": False, "error": "already normalized — undo it first"}
        return {"ok": True, "changed": True, "plex_is_serving_it": True}
    monkeypatch.setattr("app.web.api._normalize_one_row", _fake)

    from app.web.api import _bulk_normalize_run
    _bulk_normalize_run(db, bench)
    ds = {d["l"]: d["v"] for d in json.loads(_op(db)[1])["done_summary"]}
    assert ds["leveled"] == 1 and ds["skipped"] == 1


# ── the start endpoint ───────────────────────────────────────────────────────

@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    # the thread target — no-op so the endpoint test doesn't run real mp3gain/Plex
    monkeypatch.setattr("app.web.api._bulk_normalize_run", lambda *a, **k: None)
    return s, monkeypatch


def _client_with_plex(s, monkeypatch, *, plex):
    from app.config import Settings
    from app.web.api import create_app
    monkeypatch.setattr(Settings, "plex_enabled", property(lambda self: plex))
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://x" if plex else None))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "t" if plex else None))
    return TestClient(create_app(s))


def test_endpoint_refuses_without_plex(client):
    s, mp = client
    c = _client_with_plex(s, mp, plex=False)
    r = c.post("/api/admin/loudness/bulk-normalize", headers=AUTH)
    assert r.status_code == 400 and "Plex is not configured" in r.json()["detail"]


def test_endpoint_starts_and_is_single_flight(client):
    s, mp = client
    c = _client_with_plex(s, mp, plex=True)
    r = c.post("/api/admin/loudness/bulk-normalize", headers=AUTH)
    assert r.status_code == 200 and r.json()["op_id"] == "bulk-normalize"
    # a second concurrent start is refused (try_acquire holds the slot)
    r2 = c.post("/api/admin/loudness/bulk-normalize", headers=AUTH)
    assert r2.status_code == 409
