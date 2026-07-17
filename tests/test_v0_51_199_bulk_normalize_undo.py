"""v0.51.199 — Phase 2 Tag 4: the bulk_normalize_undo background op.

The safety net. Reverses a whole // LEVEL run: undoes the leveling on every leveled
theme, restoring each file AND putting Plex back, each row through the SAME
_undo_one_row chokepoint // UNDO LEVELING uses. SERIAL + cancelable (it mutates real
files + Plex), idempotent (a re-run skips rows already back to raw).

These tests drive the runner DIRECTLY with a fake _undo_one_row (the chokepoint has its
own coverage via the undo-one endpoint), so they pin the parts unique to bulk: the
leveled-set selection, the most-recently-leveled-first order, max_rows, cancel, the
undone/skipped/failed/diverged counting, and the op_progress lifecycle. Plus the start
endpoint's Plex gate + single-flight, the leveled count that powers the button, and the
UI wiring.
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


def _seed(db, *, tmdb, norm_at, norm_state="normalized", loudness_i=-18.0,
          hardlink=True, file_size=1_000_000):
    """One themed row. `norm_state='normalized'` = leveled (undoable); None = raw."""
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
                  " source_video_id, loudness_i, loudness_tp, file_size, norm_state, "
                  " norm_at, norm_orig_sha256, norm_plex_entry_uri) "
                  "VALUES ('movie', ?, '1', '', ?, 's', 's', ?, 'v', ?, -2.0, ?, ?, ?, "
                  " 'orig', 'entry://x')",
                  (tmdb, f"movies/{tmdb}/theme.mp3", NOW, loudness_i, file_size,
                   norm_state, norm_at))
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
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)
    return s


def _install_fake_undo(monkeypatch, db, *, serving=True):
    """Records the tmdb_ids it's handed + flips norm_state→NULL so idempotency holds.

    `serving` controls plex_is_serving_the_restore (drives the diverged count)."""
    calls = []

    def _fake(db_, settings_, row):
        calls.append(row["tmdb_id"])
        with sqlite3.connect(db) as c:
            c.execute("UPDATE local_files SET norm_state=NULL WHERE tmdb_id=?",
                      (row["tmdb_id"],))
            c.commit()
        return {"ok": True, "title": f"M{row['tmdb_id']}",
                "plex_is_serving_the_restore": serving}

    monkeypatch.setattr("app.web.api._undo_one_row", _fake)
    return calls


def _op(db):
    with get_conn(db) as conn:
        return conn.execute("SELECT status, detail_json FROM op_progress "
                            "WHERE op_id='bulk-normalize-undo'").fetchone()


# ── the runner ───────────────────────────────────────────────────────────────


def test_runner_undoes_only_leveled_rows_most_recent_first(bench, monkeypatch):
    db = bench.db_path
    _seed(db, tmdb=1, norm_at="2026-07-17T03:00:00")   # newest leveled
    _seed(db, tmdb=2, norm_at="2026-07-17T02:00:00")   # leveled
    _seed(db, tmdb=3, norm_at="2026-07-17T01:00:00")   # oldest leveled
    _seed(db, tmdb=4, norm_at=None, norm_state=None)   # raw — not undoable
    _seed(db, tmdb=5, norm_at=None, norm_state=None, hardlink=False)  # raw, unplaced
    calls = _install_fake_undo(monkeypatch, db)

    from app.web.api import _bulk_normalize_undo_run
    _bulk_normalize_undo_run(db, bench)

    # only the 3 leveled rows, most-recently-leveled first
    assert calls == [1, 2, 3]
    st, detail = _op(db)
    assert st == "done"
    ds = {d["l"]: d["v"] for d in json.loads(detail)["done_summary"]}
    assert ds["undone"] == 3
    # the raw rows are untouched (still raw), the leveled rows are now raw
    with get_conn(db) as conn:
        norm = {r["tmdb_id"]: r["norm_state"] for r in
                conn.execute("SELECT tmdb_id, norm_state FROM local_files").fetchall()}
    assert all(norm[t] is None for t in (1, 2, 3, 4, 5))


def test_runner_max_rows_undoes_only_the_most_recent_n(bench, monkeypatch):
    db = bench.db_path
    _seed(db, tmdb=1, norm_at="2026-07-17T03:00:00")
    _seed(db, tmdb=2, norm_at="2026-07-17T02:00:00")
    _seed(db, tmdb=3, norm_at="2026-07-17T01:00:00")
    calls = _install_fake_undo(monkeypatch, db)
    from app.web.api import _bulk_normalize_undo_run
    _bulk_normalize_undo_run(db, bench, max_rows=1)
    assert calls == [1]   # only the most recently leveled


def test_runner_stops_when_cancelled(bench, monkeypatch):
    db = bench.db_path
    _seed(db, tmdb=1, norm_at="2026-07-17T02:00:00")
    _seed(db, tmdb=2, norm_at="2026-07-17T01:00:00")
    calls = _install_fake_undo(monkeypatch, db)
    monkeypatch.setattr("app.core.progress.is_cancelled", lambda db_, op_id: True)
    from app.web.api import _bulk_normalize_undo_run
    _bulk_normalize_undo_run(db, bench)
    assert calls == []                       # cancelled before the first row
    assert _op(db)[0] == "cancelled"


def test_runner_counts_skipped_failed_and_diverged(bench, monkeypatch):
    """A row the chokepoint refuses = skipped; one that raises = failed; one restored
    on disk but not on Plex = diverged (still counted undone)."""
    db = bench.db_path
    _seed(db, tmdb=1, norm_at="2026-07-17T04:00:00")   # ok + serving
    _seed(db, tmdb=2, norm_at="2026-07-17T03:00:00")   # ok but diverged
    _seed(db, tmdb=3, norm_at="2026-07-17T02:00:00")   # refused → skipped
    _seed(db, tmdb=4, norm_at="2026-07-17T01:00:00")   # raises → failed

    def _fake(db_, s_, row):
        t = row["tmdb_id"]
        if t == 1:
            return {"ok": True, "plex_is_serving_the_restore": True}
        if t == 2:
            return {"ok": True, "plex_is_serving_the_restore": False}
        if t == 3:
            return {"ok": False, "error": "row is not normalized — nothing to undo"}
        raise RuntimeError("boom")
    monkeypatch.setattr("app.web.api._undo_one_row", _fake)

    from app.web.api import _bulk_normalize_undo_run
    _bulk_normalize_undo_run(db, bench)
    ds = {d["l"]: d["v"] for d in json.loads(_op(db)[1])["done_summary"]}
    assert ds["undone"] == 2          # rows 1 + 2
    assert ds["diverged"] == 1        # row 2
    assert ds["skipped"] == 1         # row 3
    assert ds["failed"] == 1          # row 4


# ── the leveled count (button label) ─────────────────────────────────────────


def test_leveled_count_is_not_gated_by_eligible_predicate(bench):
    """// UNDO ALL LEVELING must offer to undo EVERY leveled row — even one that's
    unplaced or over the ceiling (undo just restores bytes; it doesn't re-level)."""
    db = bench.db_path
    _seed(db, tmdb=1, norm_at="2026-07-17T02:00:00")                       # leveled, placed
    _seed(db, tmdb=2, norm_at="2026-07-17T01:00:00", hardlink=False)       # leveled, unplaced
    _seed(db, tmdb=3, norm_at="2026-07-17T01:00:00",
          file_size=THEME_UPLOAD_CEILING_BYTES + 1)                        # leveled, over-ceiling
    _seed(db, tmdb=4, norm_at=None, norm_state=None)                       # raw
    from app.core.loudness_audit import bulk_normalize_counts
    with get_conn(db) as conn:
        counts = bulk_normalize_counts(
            conn, ceiling_bytes=THEME_UPLOAD_CEILING_BYTES, target=-18.0)
    assert counts["leveled"] == 3     # all three leveled rows, regardless of placement


# ── the start endpoint ───────────────────────────────────────────────────────


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    # the thread target — no-op so the endpoint test doesn't run real undo/Plex
    monkeypatch.setattr("app.web.api._bulk_normalize_undo_run", lambda *a, **k: None)
    return s, monkeypatch


def _client_with_plex(s, monkeypatch, *, plex):
    from app.config import Settings
    from app.web.api import create_app
    monkeypatch.setattr(Settings, "plex_enabled", property(lambda self: plex))
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://x" if plex else None))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "t" if plex else None))
    return TestClient(create_app(s))


def test_endpoint_refuses_without_plex(client):
    """Undo must put Plex back, so it refuses when Plex is unconfigured rather than
    leaving rows diverged (file raw, Plex still leveled)."""
    s, mp = client
    c = _client_with_plex(s, mp, plex=False)
    r = c.post("/api/admin/loudness/bulk-normalize-undo", headers=AUTH)
    assert r.status_code == 400 and "Plex" in r.json()["detail"]


def test_endpoint_starts_and_is_single_flight(client):
    s, mp = client
    c = _client_with_plex(s, mp, plex=True)
    r = c.post("/api/admin/loudness/bulk-normalize-undo", headers=AUTH)
    assert r.status_code == 200 and r.json()["op_id"] == "bulk-normalize-undo"
    r2 = c.post("/api/admin/loudness/bulk-normalize-undo", headers=AUTH)
    assert r2.status_code == 409


# ── UI wiring ────────────────────────────────────────────────────────────────


APP_JS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.js").read_text()
LOUD_HTML = (Path(__file__).resolve().parent.parent / "app" / "web" / "templates"
             / "loudness.html").read_text()


def test_undo_all_button_exists_in_template():
    assert 'id="loud-undo-all"' in LOUD_HTML
    assert "// UNDO ALL LEVELING" in LOUD_HTML


def test_undo_button_posts_to_the_undo_endpoint():
    """The undo handler must POST to bulk-normalize-undo (not bulk-normalize) and gate
    on lastBulk.leveled — a mis-wire here would silently level instead of undo."""
    i = APP_JS.index("undoAllBtn.addEventListener")
    body = APP_JS[i:i + 700]
    assert "/api/admin/loudness/bulk-normalize-undo" in body
    assert "lastBulk.leveled" in body


def test_render_level_shows_undo_when_leveled_present():
    """renderLevel must reveal the undo button off bulk.leveled, independent of
    eligible — the block stays open for undo even when nothing is left to level."""
    i = APP_JS.index("function renderLevel(bulk)")
    body = APP_JS[i:i + 2000]
    assert "undoAllBtn" in body
    # the block must not hide purely on !eligible (would strand the undo action)
    assert "!eligible && !leveled" in body
