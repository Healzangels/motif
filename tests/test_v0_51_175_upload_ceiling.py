"""v0.51.175 — the push 500'd on a ceiling motif has guarded since v1.21.99.

The operator's // PUSH NORMALIZED TO PLEX returned:

    {"ok": false, "uploaded": false, "http_status": 500,
     "error": "Plex rejected the upload (HTTP 500): <html>...Internal Server Error..."}

Plex 500s on a theme POST over ~10MB. motif has known this since v1.21.99 — the comment
there names the same symptom ("the user's Watchmen Theatrical Cut: rk=417795 re-upload
500'd, restore 'failed', LPS looked like it did nothing") — and guarded it at THREE sites:
worker._PLEX_THEME_UPLOAD_CEILING_MB, orphan_scan._UPLOAD_CEILING_BYTES, and an inline copy
inside set_active_theme_via_reupload. v0.51.174's loudness push was a FOURTH upload path,
written without the check. That is CLAUDE.md's mirror-drift class verbatim: a rule living at
N sites, and the new site misses it.

Fixed at the altitude rather than by adding a 4th copy: ONE plex.THEME_UPLOAD_CEILING_BYTES,
enforced at the chokepoint (upload_collection_theme) that every caller already shares.

Second failure, just as important: v0.51.174 reported bytes_sent only on SUCCESS, so the
real 500 arrived WITHOUT the one number that diagnoses it. An error path that omits the
deciding measurement is the same class as reporting off a status code instead of a
re-measurement.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core import plex as plex_mod
from app.core.db import init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent


# ── one constant, enforced at the chokepoint ─────────────────────────────────

def test_single_ceiling_constant_exists():
    assert plex_mod.THEME_UPLOAD_CEILING_BYTES == 10 * 1024 * 1024


def test_inline_fourth_copy_is_retired():
    """set_active_theme_via_reupload had its own `_reupload_ceiling = 10 * 1024 * 1024`."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    assert "_reupload_ceiling = 10 * 1024 * 1024" not in src
    assert "THEME_UPLOAD_CEILING_BYTES" in src


def test_upload_chokepoint_refuses_an_over_ceiling_post(monkeypatch):
    """The guard belongs where every caller passes, so the NEXT new upload path can't
    repeat v0.51.174. An over-ceiling POST must never reach Plex."""
    posted = []

    class _Client:
        def post(self, *a, **k):
            posted.append(a)
            raise AssertionError("must not POST an over-ceiling body")

    c = plex_mod.PlexClient.__new__(plex_mod.PlexClient)
    c._client = _Client()
    c._headers = {}
    big = b"x" * (plex_mod.THEME_UPLOAD_CEILING_BYTES + 1)

    ok, status, body = c.upload_collection_theme(rating_key="1", audio_bytes=big)
    assert ok is False
    assert status is None
    assert "over_ceiling" in body
    assert posted == []          # the doomed POST never fired


def test_upload_chokepoint_allows_an_under_ceiling_post(monkeypatch):
    """The guard must not block normal themes."""
    class _Resp:
        status_code = 200
        text = "ok"

    class _Client:
        def post(self, *a, **k):
            return _Resp()

    c = plex_mod.PlexClient.__new__(plex_mod.PlexClient)
    c._client = _Client()
    c._headers = {}
    ok, status, _body = c.upload_collection_theme(rating_key="1", audio_bytes=b"small")
    assert ok is True
    assert status == 200


# ── the probe reports the deciding number on EVERY path ──────────────────────

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
    d = tmp_path / "themes" / "anime" / "x"
    d.mkdir(parents=True, exist_ok=True)
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
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (1,'tv',136840,'Demon Sword','2023','imdb',?,?)", (NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, loudness_i, "
                  " norm_state, norm_gain_db, norm_at) "
                  "VALUES ('tv',136840,'3','','anime/x/theme.mp3','s',?,'v',-18.7,"
                  " 'normalized',-13.54,?)", (NOW, NOW))
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
                  " guid_tmdb, title, folder_path, edition_key, has_theme, first_seen_at, "
                  " last_seen_at) VALUES ('451936','3','show',1,136840,'D','/d','',1,?,?)",
                  (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path, d / "theme.mp3"


def test_over_ceiling_theme_is_named_as_such_not_a_raw_500(client, monkeypatch):
    """THE OPERATOR'S CASE, if the theme is large: say the size and the cap, don't fire the
    POST and surface Plex's HTML 500."""
    c, db, theme = client
    theme.write_bytes(b"x" * (plex_mod.THEME_UPLOAD_CEILING_BYTES + 1))

    b = c.post("/api/admin/loudness/plex-push", headers=AUTH).json()
    assert b["ok"] is False
    assert b["over_ceiling"] is True
    assert b["uploaded"] is False
    assert b["bytes_sent"] == plex_mod.THEME_UPLOAD_CEILING_BYTES + 1
    assert b["ceiling_bytes"] == plex_mod.THEME_UPLOAD_CEILING_BYTES
    assert "over Plex's" in b["error"]
    assert "cannot propagate" in b["error"]


def test_under_ceiling_500_reports_the_size_and_says_it_is_not_the_cap(client, monkeypatch):
    """If Plex 500s on a SMALL theme, the size cap is NOT the cause — the error must say so
    rather than leave the operator guessing (the v1.18.68 lesson, re-learned)."""
    import app.web.api as api_mod
    c, db, theme = client
    theme.write_bytes(b"small-theme")

    class _FakePlex:
        def __init__(self, *a, **k): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def get_themes(self, *, rating_key):
            return {"ok": True, "http_status": 200, "error": None, "body": {
                "MediaContainer": {"Metadata": [
                    {"ratingKey": "metadata://themes/aaa", "selected": True}]}}}
        def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
            return {"ok": True, "http_status": 200, "bytes": b"B", "error": None}
        def upload_theme(self, *, rating_key, audio_bytes, content_type="audio/mpeg"):
            return (False, 500, "<html>Internal Server Error</html>")

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)
    monkeypatch.setattr("app.core.loudness.measure_loudness",
                        lambda p, *a, **k: {"loudness_i": -5.15, "true_peak": -3.0,
                                            "lra": 5.0})
    b = c.post("/api/admin/loudness/plex-push", headers=AUTH).json()
    assert b["ok"] is False
    assert b["over_ceiling"] is False
    assert b["bytes_sent"] == len(b"small-theme")
    assert "UNDER the" in b["error"]      # explicitly rules the ceiling out
    assert "not the size cap" in b["error"]
