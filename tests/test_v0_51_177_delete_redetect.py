"""v0.51.177 — the audition auto-pick must only ever pick a PUSHABLE theme.

The delete + re-detect probe this file was written for is GONE (removed in v0.51.180 after
being measured dead: the DELETE leaves the entry in the collection and only clears the
selection, so Plex never "lacks" the theme and Local Media Assets has nothing to
re-ingest — it stranded rk 261711 with no theme and bought nothing). The full findings
live in CLAUDE.md § 11 so nobody re-probes it.

What survives is the fix that rode along with it, because it guards live code: the
audition picked the LOUDEST measured row, which on the real library was 10.5MB — over
Plex's ~10MB upload ceiling. Since re-upload is the ONLY propagation mechanism, that made
the propagation half of the audition a dead end before it began.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db
from app.core.plex import THEME_UPLOAD_CEILING_BYTES

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()


@pytest.fixture
def client_and_db(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    themes = tmp_path / "themes"
    themes.mkdir(exist_ok=True)
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: themes))
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _seed_row(db, *, tmdb_id, loudness_i, file_size):
    sha = f"sha{tmdb_id}"
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?, 'movie', ?, ?, '1979', 'imdb', ?, ?)",
                  (tmdb_id, tmdb_id, f"Movie{tmdb_id}", NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, loudness_i, "
                  " loudness_tp, loudness_measured_sha256, loudness_measured_at, "
                  " file_size) "
                  "VALUES ('movie', ?, '1', '', ?, ?, ?, 'vid', ?, -2.0, ?, ?, ?)",
                  (tmdb_id, f"movies/{tmdb_id}/theme.mp3", sha, NOW, loudness_i, sha,
                   NOW, file_size))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, "
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie', ?, '1', ?, '', 'hardlink', ?)",
                  (tmdb_id, f"/data/movies/{tmdb_id}", NOW))
        c.commit()


def test_audition_autopick_skips_un_pushable_themes():
    i = API.index('@app.post("/api/admin/loudness/normalize-one")')
    src = API[i:API.index('@app.get("/api/admin/loudness/normalized")', i)]
    assert "lf.file_size IS NOT NULL" in src
    assert "lf.file_size <= " in src
    assert "THEME_UPLOAD_CEILING_BYTES" in src


def test_autopick_skips_over_ceiling_and_unknown_size_rows(client_and_db, monkeypatch):
    """Behavioral — source text alone is a phantom guard (v1.18.81)."""
    c, db = client_and_db
    ceiling = THEME_UPLOAD_CEILING_BYTES
    _seed_row(db, tmdb_id=1, loudness_i=-3.0, file_size=ceiling + 1)   # loudest, too big
    _seed_row(db, tmdb_id=2, loudness_i=-5.0, file_size=None)          # loud, size unknown
    _seed_row(db, tmdb_id=3, loudness_i=-9.0, file_size=1_000_000)     # quietest, pushable
    captured = {}

    def _fake(path, target, measured_i, true_peak, *, expect_sha=None):
        captured.update(measured_i=measured_i)
        return {"ok": True, "changed": False, "steps": 0, "applied_db": 0.0,
                "note": "no change", "old_sha": "sha3", "new_sha": "sha3",
                "old_pcm_sha": "pcm3", "new_i": measured_i, "new_tp": true_peak,
                "new_lra": None}

    monkeypatch.setattr("app.core.loudness_apply.normalize_file", _fake)
    r = c.post("/api/admin/loudness/normalize-one", headers=AUTH)
    assert r.json()["ok"] is True
    # -3.0 is louder, but a 10.5MB theme cannot be pushed, so there'd be nothing to
    # propagate and nothing to recover with. A NULL size is an UNKNOWN, not a small file.
    assert captured["measured_i"] == -9.0
