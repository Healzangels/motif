"""v0.51.197 — Phase 2 Tag 3: the // LEVEL OUTLIERS / // LEVEL LIBRARY report buttons.

The audit report gains two bulk-level CTAs. They can only label themselves honestly if a
count MATCHES _bulk_normalize_run's eligible predicate — rep.measured / stats.count
over-count (non-placed + already-normalized rows) and rep.loudest is capped at 40. So the
report endpoint now merges bulk_normalize_counts (eligible + outliers) computed against
the SAME predicate + the configured target.

Pinned: the counts match the op's eligible set exactly (so a button never promises a
count the op won't touch), the endpoint surfaces them, and the buttons post to
bulk-normalize with the right body.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db, get_conn
from app.core.loudness_audit import bulk_normalize_counts, _OUTLIER_MARGIN_DB

NOW = "2026-07-17T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LOUD_HTML = (REPO / "app" / "web" / "templates" / "loudness.html").read_text()
CEIL = 10 * 1024 * 1024


def _seed(db, *, tmdb, loudness_i, size=1_000_000, sha="s", measured=None,
          norm=None, hardlink=True):
    measured = sha if measured is None else measured
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, loudness_measured_sha256, downloaded_at, "
                  " source_video_id, loudness_i, loudness_tp, file_size, norm_state) "
                  "VALUES ('movie', ?, '1', '', ?, ?, ?, ?, 'v', ?, -2.0, ?, ?)",
                  (tmdb, f"m/{tmdb}.mp3", sha, measured, NOW, loudness_i, size, norm))
        if hardlink:
            c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, "
                      " media_folder, edition_key, placement_kind, placed_at) "
                      "VALUES ('movie', ?, '1', ?, '', 'hardlink', ?)",
                      (tmdb, f"/d/{tmdb}", NOW))
        c.commit()


def test_counts_match_the_bulk_op_eligible_set(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    # target -18, margin 6 → outlier when loudness_i > -12
    _seed(db, tmdb=1, loudness_i=-5.2)                     # eligible + outlier
    _seed(db, tmdb=2, loudness_i=-9.0)                     # eligible + outlier
    _seed(db, tmdb=3, loudness_i=-14.5)                    # eligible, not outlier
    _seed(db, tmdb=4, loudness_i=-20.0)                    # eligible, not outlier
    _seed(db, tmdb=5, loudness_i=-5.0, size=CEIL + 1)      # over ceiling → excluded
    _seed(db, tmdb=6, loudness_i=-5.0, norm="normalized")  # already leveled → excluded
    _seed(db, tmdb=7, loudness_i=-5.0, hardlink=False)     # not hardlink → excluded
    with get_conn(db) as conn:
        r = bulk_normalize_counts(conn, ceiling_bytes=CEIL, target=-18.0)
    assert r["eligible"] == 4      # rows 1-4
    assert r["outliers"] == 2      # rows 1,2 (louder than target + margin)
    assert r["outlier_margin_db"] == _OUTLIER_MARGIN_DB


def test_outliers_track_the_target(tmp_path):
    """A quieter target makes MORE rows outliers — the count follows the configured target."""
    db = tmp_path / "m.db"
    init_db(db)
    _seed(db, tmdb=1, loudness_i=-5.0)
    _seed(db, tmdb=2, loudness_i=-14.0)
    with get_conn(db) as conn:
        loud = bulk_normalize_counts(conn, ceiling_bytes=CEIL, target=-6.0)   # +6 → -0
        quiet = bulk_normalize_counts(conn, ceiling_bytes=CEIL, target=-22.0)  # +6 → -16
    assert loud["outliers"] == 0     # nothing louder than -0
    assert quiet["outliers"] == 2    # both louder than -16


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    # v0.51.197: pin the target to the config default (-18) — other loudness tests set
    # MOTIF_LOUDNESS_TARGET in os.environ without cleanup, and a leaked value would move
    # the outlier count out from under this test's -18 assumption.
    monkeypatch.delenv("MOTIF_LOUDNESS_TARGET", raising=False)
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    monkeypatch.setattr(Settings, "plex_enabled", property(lambda self: True))
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://x"))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "t"))
    return TestClient(create_app(s)), s.db_path


def test_report_endpoint_surfaces_the_bulk_counts(client):
    c, db = client
    _seed(db, tmdb=1, loudness_i=-5.2)    # eligible + outlier (default target -18)
    _seed(db, tmdb=2, loudness_i=-20.0)   # eligible, not outlier
    body = c.get("/api/admin/loudness-report", headers=AUTH).json()
    assert "bulk" in body
    assert body["bulk"]["eligible"] == 2
    assert body["bulk"]["outliers"] == 1
    assert body["bulk"]["target"] == -18.0
    assert body["bulk"]["plex_ready"] is True


def test_report_bulk_target_is_the_clamped_setting(client, monkeypatch):
    """The count uses settings.loudness_target_lufs — the clamped hover-band value."""
    c, db = client
    from app.config import Settings
    monkeypatch.setattr(Settings, "loudness_target_lufs", property(lambda self: -20.0))
    _seed(db, tmdb=1, loudness_i=-5.2)
    body = c.get("/api/admin/loudness-report", headers=AUTH).json()
    assert body["bulk"]["target"] == -20.0


# ── the buttons are wired ────────────────────────────────────────────────────

def test_level_buttons_exist_and_post_to_the_op():
    for el in ('id="loud-level-block"', 'id="loud-level-outliers"', 'id="loud-level-all"'):
        assert el in LOUD_HTML, el
    # the JS reads the counts, labels the buttons, and posts to the bulk endpoint
    assert "renderLevel(rep.bulk)" in APP_JS
    assert "'/api/admin/loudness/bulk-normalize'" in APP_JS
    assert "{ max_rows: maxRows }" in APP_JS   # outliers passes a cap; library passes {}
    # LEVEL LIBRARY is gated behind a confirm (native confirm(), the codebase idiom)
    assert "if (confirmMsg && !confirm(confirmMsg)) return;" in APP_JS
