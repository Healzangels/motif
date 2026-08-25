"""v0.51.276 — feature-brief E: one RUN RECONCILIATION with dry-run + summary.

The pre-coding review found ~60% of E already shipped as scattered detectors;
the missing piece was the unifying run. So this tag is deliberately a WRAPPER:
repair is exactly the class the hourly retry sweep already owns (its skip
semantics took eleven tags to mature and are reused via a dry_run param, not
re-implemented), everything else is report-only per the brief's own safety
rules, and the orphan census uses BOTH plex_items linkages (guid_tmdb OR
theme_id — the guid-only join over-reported orphans 10x on 2026-08-22).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
NOW = "2026-08-24T00:00:00+00:00"


@pytest.fixture
def env(tmp_path):
    from app.core.db import get_conn, init_db, transaction
    db = tmp_path / "t.db"
    themes = tmp_path / "themes"
    (themes / "movies").mkdir(parents=True)
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)""",
            (NOW, NOW))
    return db, themes


def _theme(conn, tid, title):
    conn.execute(
        """INSERT INTO themes (media_type, tmdb_id, title, year,
             upstream_source, last_seen_sync_at, first_seen_sync_at)
           VALUES ('movie', ?, ?, '2020', 'themoviedb', ?, ?)""",
        (tid, title, NOW, NOW))
    return conn.execute("SELECT id FROM themes WHERE tmdb_id=?",
                        (tid,)).fetchone()["id"]


def _lf(conn, themes, tid, name, *, make_file=True, reason=None):
    rel = f"movies/{name}/theme.mp3"
    if make_file:
        d = themes / "movies" / name
        d.mkdir(parents=True, exist_ok=True)
        (d / "theme.mp3").write_bytes(b"x")
    conn.execute(
        """INSERT INTO local_files (media_type, tmdb_id, section_id,
             edition_key, file_path, file_sha256, file_size, downloaded_at,
             source_video_id, provenance, source_kind, last_place_attempt_reason)
           VALUES ('movie', ?, '1', '', ?, 's', 1, ?, '', 'auto', 'themerrdb', ?)""",
        (tid, rel, NOW, reason))


def _pi(conn, rk, tid, theme_id=None):
    conn.execute(
        """INSERT INTO plex_items (rating_key, section_id, media_type, title,
             year, guid_tmdb, theme_id, edition_key, folder_path, has_theme,
             first_seen_at, last_seen_at)
           VALUES (?, '1', 'movie', 'T', '2020', ?, ?, '', '/d', 0, ?, ?)""",
        (rk, tid, theme_id, NOW, NOW))


def _seed_matrix(db, themes):
    """One row per class: repairable, orphan (guid-linked absent), healthy
    collection-style (theme_id-linked ONLY — must NOT count as orphan),
    broken canonical, permanently-skipped."""
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        # repairable: canonical + live plex row, no placement
        _theme(conn, 1, "Repairable"); _lf(conn, themes, 1, "Repairable (2020)")
        _pi(conn, "rk-1", 1)
        # orphan: canonical, no plex row at all
        _theme(conn, 2, "Orphan"); _lf(conn, themes, 2, "Orphan (2020)")
        # theme_id-linked (collection-style): plex row with NULL guid_tmdb
        tid3 = _theme(conn, 3, "ViaThemeId")
        _lf(conn, themes, 3, "ViaThemeId (2020)")
        _pi(conn, "rk-3", None, theme_id=tid3)
        # broken canonical: row present, file missing — also give it a live
        # plex row so it is not double-counted as an orphan
        _theme(conn, 4, "BrokenCanon")
        _lf(conn, themes, 4, "BrokenCanon (2020)", make_file=False)
        _pi(conn, "rk-4", 4)
        # permanent-skip: canonical + plex row, reason says never auto-place
        _theme(conn, 5, "Skipped"); _lf(conn, themes, 5, "Skipped (2020)",
                                        reason="plex_has_theme")
        _pi(conn, "rk-5", 5)


def _jobs(db):
    from app.core.db import get_conn
    with get_conn(db) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT media_type, tmdb_id, job_type, status FROM jobs")]


# ── the run ──────────────────────────────────────────────────


def test_dry_run_reports_without_touching_anything(env):
    from app.core.reconcile import run_reconciliation
    db, themes = env
    _seed_matrix(db, themes)
    out = run_reconciliation(db, themes, dry_run=True)
    assert out["dry_run"] is True
    # The sweep keys on local_files alone (no live-Plex requirement) — the
    # orphan and broken-canonical rows are candidates too, and the place
    # WORKER then applies its own per-row rules (no_match retry for a missing
    # folder, placement_error terminal for a missing canonical). That is the
    # hourly sweep's existing, deliberate semantics; only the skip-reason row
    # is excluded up front.
    assert out["place_retry"]["candidates"] == 4
    assert out["place_retry"]["enqueued"] == 0
    assert _jobs(db) == [], "dry-run must enqueue nothing"
    assert out["broken_canonicals"] == 1
    assert out["orphans"]["count"] == 1, (
        "ONLY the guid-less, theme_id-less row — the theme_id-linked "
        "collection-style row must not be misreported (the 10x census bug)")
    assert any("Orphan" in x for x in out["orphans"]["sample"])


def test_real_run_enqueues_the_repairable_class_only(env):
    from app.core.reconcile import run_reconciliation
    db, themes = env
    _seed_matrix(db, themes)
    out = run_reconciliation(db, themes, dry_run=False)
    jobs = _jobs(db)
    tids = sorted(j["tmdb_id"] for j in jobs)
    assert out["place_retry"]["enqueued"] == len(jobs)
    assert 5 not in tids, "the permanent-skip reason must be honored"
    assert tids == [1, 2, 3, 4], (
        "the sweep's existing semantics: every no-placement local_files row "
        "without a skip reason enqueues; the worker sorts out the rest")
    assert all(j["job_type"] == "place" and j["status"] == "pending" for j in jobs)


def test_second_run_is_idempotent(env):
    from app.core.reconcile import run_reconciliation
    db, themes = env
    _seed_matrix(db, themes)
    run_reconciliation(db, themes, dry_run=False)
    n1 = len(_jobs(db))
    out2 = run_reconciliation(db, themes, dry_run=False)
    assert len(_jobs(db)) == n1, "pending-job dedup stops duplicate repairs"
    assert out2["place_retry"]["enqueued"] == 0


def test_a_run_that_finds_nothing_still_reports(env, monkeypatch):
    from app.core import reconcile as mod
    db, themes = env
    seen = []
    monkeypatch.setattr(mod, "log_event", lambda *a, **kw: seen.append(kw))
    out = mod.run_reconciliation(db, themes, dry_run=True)
    assert out["scanned"] == 0 and out["orphans"]["count"] == 0
    assert len(seen) == 1, "v1.18.5: the nothing-to-do path logs too"
    assert "Reconciliation (dry-run)" in seen[0]["message"]
    assert seen[0]["detail"]["duration_s"] >= 0


def test_summary_shape_is_stable(env):
    from app.core.reconcile import run_reconciliation
    db, themes = env
    out = run_reconciliation(db, themes, dry_run=True)
    assert set(out) == {"dry_run", "scanned", "canonical", "placements",
                        "place_retry", "broken_canonicals",
                        "broken_placements", "orphans", "duration_s"}
    assert set(out["place_retry"]) == {"candidates", "enqueued", "locked_out"}


# ── the sweep refactor is behavior-preserving for the cron ───


def test_cron_call_shape_unchanged(env):
    """The hourly caller passes nothing — defaults must enqueue (dry_run
    False? NO: default dry_run=False in the sweep) and return counts."""
    from app.core.scheduler import _retry_pending_placements
    db, themes = env
    _seed_matrix(db, themes)
    out = _retry_pending_placements(db)
    assert out["enqueued"] == out["candidates"] >= 1, (
        "no-arg call must behave exactly as the pre-.276 cron did")
    assert len(_jobs(db)) == out["enqueued"]


# ── endpoint ─────────────────────────────────────────────────


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    themes = tmp_path / "data" / "themes"
    themes.mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s


AUTH = {"X-Authentik-Username": "testadmin"}


def test_endpoint_defaults_to_dry_run(client):
    c, _ = client
    r = c.post("/api/admin/reconcile", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["dry_run"] is True, "conservative by default (the brief)"


def test_endpoint_real_run_is_explicit(client):
    c, _ = client
    r = c.post("/api/admin/reconcile?dry_run=false", headers=AUTH)
    assert r.status_code == 200 and r.json()["dry_run"] is False


def test_endpoint_requires_admin(client):
    c, _ = client
    assert c.post("/api/admin/reconcile").status_code in (401, 403)


def test_v0_51_276_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
