"""v0.51.173 — the propagation probe: can a Plex REFRESH make it re-read a normalized sidecar?

// WHAT IS PLEX SERVING? settled the diagnosis on the real library:

    canonical_loudness_i: -18.7      (normalized, on disk)
    plex_loudness_i:      -5.15      (what Plex actually plays)
    entry_uri: "metadata://themes/a6458...", entries: 1
    serving_normalized: false

So Plex plays its INGESTED copy, keyed by the hash of the ORIGINAL bytes. Mutating a
hardlinked sidecar changes nothing Plex serves — v0.51.168's "hardlink → Plex plays it
immediately" was false, and the operator's ears caught what three tags of green checks
didn't.

The remaining unknown is HOW to propagate:
  - REFRESH (this probe): re-run Local Media Assets so it re-ingests the changed sidecar.
    Native, keeps the row a sidecar row. UNPROVEN — so measure it, don't assume it.
  - RE-UPLOAD: POST the normalized bytes; Plex content-dedupes by SHA-1 and auto-selects
    (PROVEN, v1.18.35 probe / v1.18.36 production) — but makes it an upload:// entry.

Assuming is exactly what cost the last three tags, so the probe refreshes and then
RE-MEASURES rather than reporting success off the refresh call's return value.
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
    import app.web.api as api_mod
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: tmp_path / "themes"))
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://plex.test"))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "tok"))
    # don't actually sleep through the async-refresh polls
    monkeypatch.setattr(api_mod, "_REREAD_POLL_S", 0)
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
                  " themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('3','Anime','show',1,0,'anime',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _seed(db):
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (1,'tv',136840,'Demon Sword Master','2023','imdb',?,?)",
                  (NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, loudness_i, "
                  " norm_state, norm_gain_db, norm_at) "
                  "VALUES ('tv',136840,'3','','anime/x/theme.mp3','s',?,'vid',-18.7,"
                  " 'normalized',-13.54,?)", (NOW, NOW))
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
                  " guid_tmdb, title, folder_path, edition_key, has_theme, first_seen_at, "
                  " last_seen_at) "
                  "VALUES ('451936','3','show',1,136840,'Demon Sword','/d','',1,?,?)",
                  (NOW, NOW))
        c.commit()


def _stub_plex(monkeypatch, *, loudness_sequence, refresh_ok=True):
    """Plex serves `loudness_sequence` on successive measurements (so a refresh can be
    modelled as eventually flipping — or never flipping)."""
    import app.web.api as api_mod
    calls = {"refresh": 0}
    seq = list(loudness_sequence)

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
                    {"ratingKey": "metadata://themes/aaa", "selected": True}]}}}

        def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
            return {"ok": True, "http_status": 200, "bytes": b"B", "error": None}

        def refresh(self, rating_key):
            calls["refresh"] += 1
            return refresh_ok

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)
    monkeypatch.setattr("app.core.loudness.measure_loudness",
                        lambda p, *a, **k: {"loudness_i": (seq.pop(0) if len(seq) > 1
                                                           else seq[0]),
                                            "true_peak": -3.0, "lra": 5.0})
    return calls


def test_refresh_that_propagates_is_reported_as_the_answer(client, monkeypatch):
    """If Plex flips to the normalized loudness after a refresh, refresh IS the
    propagation step — and the probe says so because it re-MEASURED, not because the
    refresh call returned True."""
    c, db = client
    _seed(db)
    # before: -5.15 (pre-normalize) → after refresh: -18.7 (normalized)
    calls = _stub_plex(monkeypatch, loudness_sequence=[-5.15, -18.7])

    r = c.post("/api/admin/loudness/plex-reread", headers=AUTH)
    assert r.status_code == 200, r.text
    b = r.json()
    assert b["ok"] is True
    assert calls["refresh"] == 1
    assert b["refresh_propagates"] is True
    assert b["before_plex_loudness_i"] == -5.15
    assert b["after"]["plex_loudness_i"] == -18.7
    assert "DID make it re-read" in b["verdict"]


def test_refresh_that_does_not_propagate_is_reported_honestly(client, monkeypatch):
    """THE CASE THAT MATTERS: a refresh that returns True but leaves Plex serving the old
    copy must NOT read as success — that's the fake-success class this whole arc has been
    about. The re-upload path is then the remaining candidate."""
    c, db = client
    _seed(db)
    calls = _stub_plex(monkeypatch, loudness_sequence=[-5.15], refresh_ok=True)

    b = c.post("/api/admin/loudness/plex-reread", headers=AUTH).json()
    assert b["ok"] is True
    assert calls["refresh"] == 1
    assert b["refreshed"] is True          # Plex accepted the refresh...
    assert b["refresh_propagates"] is False  # ...but it changed nothing it serves
    assert "did NOT make it re-read" in b["verdict"]
    assert "re-upload" in b["verdict"]


def test_already_serving_normalized_is_a_no_op(client, monkeypatch):
    """Don't refresh what's already current."""
    c, db = client
    _seed(db)
    calls = _stub_plex(monkeypatch, loudness_sequence=[-18.7])

    b = c.post("/api/admin/loudness/plex-reread", headers=AUTH).json()
    assert b["ok"] is True
    assert b["already_current"] is True
    assert b["refreshed"] is False
    assert calls["refresh"] == 0           # never touched Plex


def test_no_normalized_theme_is_a_clean_error(client):
    c, db = client
    b = c.post("/api/admin/loudness/plex-reread", headers=AUTH).json()
    assert b["ok"] is False
    assert "no normalized theme" in b["error"]


def test_probe_polls_the_measurement_because_refresh_is_async():
    """Plex's refresh is async; a single sleep-and-guess would report false negatives."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "_REREAD_POLLS" in src
    i = src.index('@app.post("/api/admin/loudness/plex-reread")')
    block = src[i:i + 5000]
    assert "for _ in range(_REREAD_POLLS)" in block
    assert "_measure_plex_serving" in block


def test_both_probes_share_one_measurement_helper():
    """v0.51.171's fetch+measure block is shared, not copy-pasted into the reread probe."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert src.count("def _measure_plex_serving(") == 1
    # both endpoints call it
    assert src.count("_measure_plex_serving(settings") >= 3
