"""v0.51.174 — refresh is OUT (measured); re-upload is the remaining propagation path.

// MAKE PLEX RE-READ IT on the real library: refreshed=true, waited 20s, re-checked
minutes later — Plex STILL -5.15 against a -18.7 canonical, SAME metadata://themes/<sha1>
entry, entries=1. And plex.refresh already sends PUT /refresh?force=1 with an /analyze
fallback — the strongest native primitive motif has. Its own docstring quotes Plex: it is
for "Added local media assets". That IS the mechanism: Plex's agent ADDS assets it lacks;
it will not replace a theme entry it already holds. A changed sidecar is a dead letter.

Remaining path is the PROVEN one: POST the bytes to /library/metadata/{rk}/themes. Plex
content-dedupes by SHA-1, so NEW bytes make a new entry and auto-select it (v1.18.35 probe
→ v1.18.36 production). Undo falls out free — re-uploading the ORIGINAL bytes hashes back
to the EXISTING metadata:// entry, so Plex re-selects it rather than accumulating junk.

Guarded here at the level that has burned this arc twice: the probe must report off the
RE-MEASUREMENT, never off the upload's 2xx.
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
    themes = tmp_path / "themes" / "anime" / "x"
    themes.mkdir(parents=True, exist_ok=True)
    (themes / "theme.mp3").write_bytes(b"NORMALIZED-BYTES")
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: tmp_path / "themes"))
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://plex.test"))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "tok"))
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


def _stub_plex(monkeypatch, *, loudness_sequence, upload=(True, 200, "")):
    import app.web.api as api_mod
    seen = {"uploads": [], "bytes": None}
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

        def upload_theme(self, *, rating_key, audio_bytes, content_type="audio/mpeg"):
            seen["uploads"].append(rating_key)
            seen["bytes"] = audio_bytes
            return upload

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)
    monkeypatch.setattr("app.core.loudness.measure_loudness",
                        lambda p, *a, **k: {"loudness_i": (seq.pop(0) if len(seq) > 1
                                                           else seq[0]),
                                            "true_peak": -3.0, "lra": 5.0})
    return seen


def test_upload_that_propagates_is_the_answer(client, monkeypatch):
    """Plex flips to the normalized loudness after the push → re-upload IS the propagation
    step, reported because it was RE-MEASURED."""
    c, db = client
    _seed(db)
    seen = _stub_plex(monkeypatch, loudness_sequence=[-5.15, -18.7])

    r = c.post("/api/admin/loudness/plex-push", headers=AUTH)
    assert r.status_code == 200, r.text
    b = r.json()
    assert b["ok"] is True
    assert b["uploaded"] is True
    assert b["upload_propagates"] is True
    assert b["before_plex_loudness_i"] == -5.15
    assert b["after"]["plex_loudness_i"] == -18.7
    assert "Plex now serves the pushed theme" in b["verdict"]
    # it pushed the NORMALIZED canonical bytes, to the right rk
    assert seen["uploads"] == ["451936"]
    assert seen["bytes"] == b"NORMALIZED-BYTES"


def test_upload_2xx_that_changes_nothing_is_not_success(client, monkeypatch):
    """THE CLASS THIS ARC KEEPS HITTING: a POST that 200s while Plex keeps serving the old
    entry must NOT read as propagation."""
    c, db = client
    _seed(db)
    _stub_plex(monkeypatch, loudness_sequence=[-5.15], upload=(True, 200, ""))

    b = c.post("/api/admin/loudness/plex-push", headers=AUTH).json()
    assert b["ok"] is True
    assert b["uploaded"] is True          # Plex accepted it...
    assert b["upload_propagates"] is False  # ...and served the old copy anyway
    assert "NOT serving it" in b["verdict"]


def test_rejected_upload_is_reported(client, monkeypatch):
    c, db = client
    _seed(db)
    _stub_plex(monkeypatch, loudness_sequence=[-5.15], upload=(False, 413, "too large"))

    b = c.post("/api/admin/loudness/plex-push", headers=AUTH).json()
    assert b["ok"] is False
    assert b["uploaded"] is False
    assert "413" in b["error"]


def test_no_normalized_theme_is_a_clean_error(client):
    c, db = client
    b = c.post("/api/admin/loudness/plex-push", headers=AUTH).json()
    assert b["ok"] is False
    assert "no normalized theme" in b["error"]


def test_missing_canonical_file_is_a_clean_error(client, monkeypatch):
    c, db = client
    _seed(db)
    (Path(str(db)).parent / "themes" / "anime" / "x" / "theme.mp3").unlink()
    b = c.post("/api/admin/loudness/plex-push", headers=AUTH).json()
    assert b["ok"] is False
    assert "canonical file missing" in b["error"]


def test_push_polls_the_measurement_not_the_status_code():
    """v0.51.185: the poll moved into _push_theme_to_plex — ONE chokepoint now shared by
    the push button, normalize and undo. Three copies of "upload then check" is the
    mirror-drift that left the upload ceiling un-guarded at a 4th site (v0.51.175), so the
    invariant is asserted where the code lives rather than duplicated per caller.

    Bound by the next def, not a byte count — a fixed window silently slides out of range
    the moment the function grows, which is a test that quietly stops testing. See
    motif_upload_test_slice_windows."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    i = src.index("def _push_theme_to_plex(")
    block = src[i:src.index("def _measure_plex_serving(", i)]
    assert "for _ in range(_REREAD_POLLS)" in block
    assert "_measure_plex_serving(" in block
    # the verdict keys on the re-measurement, never the 2xx
    assert 'after.get("serving_normalized")' in block


def test_push_endpoint_delegates_rather_than_copying_the_step():
    """The chokepoint only helps if the callers actually route through it."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    i = src.index('@app.post("/api/admin/loudness/plex-push")')
    block = src[i:src.index('@app.post("/api/admin/loudness/undo-one")', i)]
    assert "_push_theme_to_plex(" in block
    assert "upload_theme(" not in block, "the endpoint must not re-implement the upload"
