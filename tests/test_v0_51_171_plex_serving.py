"""v0.51.171 — measure what Plex actually SERVES, instead of judging by ear.

// PROBE MP3GAIN came back fully green on the real library (deep -9-step attenuation
reversible, audio_restored=true on undo) — but the operator normalized -13.5 dB and could
not hear a difference in Plex. A 4-5x loudness drop is not subtle, so the likely answer is
that Plex never played the new bytes.

v0.51.168's audition claimed "hardlink → Plex plays the normalized theme immediately". The
inode reasoning is sound, but it assumes Plex reads theme.mp3 at PLAYBACK. It doesn't:
Local Media Assets INGESTS the sidecar into Plex's own metadata store at scan time (which
is exactly why CLAUDE.md documents metadata://themes/<sha1> entries keyed by CONTENT hash).
Mutating the sidecar then changes nothing Plex plays until a refresh re-runs the agent.
That premise was never verified — this endpoint verifies it by MEASURING.

Cranking the gain to make it audible would reach the same conclusion via a subjective call
and another mutation; fetching Plex's bytes and measuring them answers it outright.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: tmp_path / "themes"))
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://plex.test"))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "tok"))
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
                  " themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('3','Anime','show',1,0,'anime',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _seed_normalized(db, *, canonical_loudness=-18.7):
    """A normalized tv theme with a plex_items row — mirrors the real audition target."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (1,'tv',136840,'Demon Sword Master','2023','imdb',?,?)",
                  (NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, loudness_i, "
                  " loudness_tp, norm_state, norm_gain_db, norm_at) "
                  "VALUES ('tv',136840,'3','','anime/x/theme.mp3','s',?,'vid',?,-10.95,"
                  " 'normalized',-13.54,?)", (NOW, canonical_loudness, NOW))
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
                  " guid_tmdb, title, folder_path, edition_key, has_theme, first_seen_at, "
                  " last_seen_at) "
                  "VALUES ('99','3','show',1,136840,'Demon Sword Master','/d','',1,?,?)",
                  (NOW, NOW))
        c.commit()


def _stub_plex(monkeypatch, *, plex_loudness):
    """Plex returns its SELECTED theme entry's bytes; ffmpeg measures them."""
    import app.web.api as api_mod

    class _FakePlex:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get_themes(self, *, rating_key):
            return {"ok": True, "http_status": 200, "error": None, "body": {
                "MediaContainer": {"Metadata": [
                    {"ratingKey": "metadata://themes/aaa", "selected": False},
                    {"ratingKey": "metadata://themes/bbb", "selected": True},
                ]}}}

        def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
            return {"ok": True, "http_status": 200, "bytes": b"PLEXBYTES", "error": None}

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)
    monkeypatch.setattr("app.core.loudness.measure_loudness",
                        lambda p, *a, **k: {"loudness_i": plex_loudness,
                                            "true_peak": -3.0, "lra": 5.0})


def test_detects_plex_still_serving_the_pre_normalize_theme(client, monkeypatch):
    """THE SUSPECTED CAUSE: canonical is -18.7 (normalized) but Plex still serves its
    ingested -5.15 copy → the operator hears no change, exactly as reported."""
    c, db = client
    _seed_normalized(db, canonical_loudness=-18.7)
    _stub_plex(monkeypatch, plex_loudness=-5.15)

    r = c.post("/api/admin/loudness/plex-serving", headers=AUTH)
    assert r.status_code == 200, r.text
    b = r.json()
    assert b["ok"] is True
    assert b["serving_normalized"] is False
    assert b["plex_loudness_i"] == -5.15
    assert b["canonical_loudness_i"] == -18.7
    assert "PRE-normalize" in b["verdict"]


def test_confirms_plex_serving_the_normalized_theme(client, monkeypatch):
    c, db = client
    _seed_normalized(db, canonical_loudness=-18.7)
    _stub_plex(monkeypatch, plex_loudness=-18.65)   # within half an mp3gain step

    b = c.post("/api/admin/loudness/plex-serving", headers=AUTH).json()
    assert b["ok"] is True
    assert b["serving_normalized"] is True
    assert "NORMALIZED" in b["verdict"]


def test_uses_the_selected_entry(client, monkeypatch):
    """Plex's plural /themes lists every entry; only the `selected` one is what plays
    (CLAUDE.md: singular /theme is the association)."""
    c, db = client
    _seed_normalized(db)
    _stub_plex(monkeypatch, plex_loudness=-18.7)
    b = c.post("/api/admin/loudness/plex-serving", headers=AUTH).json()
    assert b["entry_uri"] == "metadata://themes/bbb"    # the selected one, not the first
    assert b["entries"] == 2


def test_no_normalized_theme_is_a_clean_error(client):
    c, db = client
    r = c.post("/api/admin/loudness/plex-serving", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["ok"] is False
    assert "no normalized theme" in r.json()["error"]


def test_missing_plex_item_is_a_clean_error(client, monkeypatch):
    c, db = client
    with sqlite3.connect(db) as x:
        x.execute("PRAGMA foreign_keys = OFF")
        x.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, downloaded_at, source_video_id, loudness_i, norm_state, "
                  " norm_at) VALUES ('tv',999,'3','','a/t.mp3',?,'v',-18.0,'normalized',?)",
                  (NOW, NOW))
        x.commit()
    b = c.post("/api/admin/loudness/plex-serving", headers=AUTH).json()
    assert b["ok"] is False
    assert "no plex_items row" in b["error"]


def test_full_fetch_uses_no_range_header():
    """probe_theme_entry_bytes caps at 4KB; measuring loudness needs the whole file."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    i = src.index("def fetch_theme_bytes")
    block = src[i:i + 1800]
    # strip the docstring — it *mentions* Range to explain why it isn't used.
    body = block[block.index('"""', block.index('"""') + 3):]
    assert "Range" not in body and "headers={" not in body.replace(
        "headers=self._headers", "")
    assert "/file'" in block
