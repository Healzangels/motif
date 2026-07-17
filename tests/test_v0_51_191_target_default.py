"""v0.51.191 — one configured level target, not two.

The download-conditioner has always used settings.loudness_target_lufs (worker.py
~2149). normalize-one hardcoded -18.0. Same operation — "level this theme" — answering
two different ways depending on which door you came through, which is the mirror-drift
class this codebase keeps paying for (docs/PROJECT_HISTORY.md).

Set Settings→loudness target to -20 and the divergence was live: a theme auto-leveled
on download landed at -20, the same theme leveled by hand landed at -18. Nothing
errored; the library just quietly held two loudnesses.

The INFO card's stepper made it matter — it has to seed from the real default, and
shipping a third hardcoded -18 in the JS would have widened the drift instead of
closing it. So the card reads api_item's loudness_target_default.
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
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _make_client(tmp_path, monkeypatch, *, target=None):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    if target is not None:
        monkeypatch.setenv("MOTIF_LOUDNESS_TARGET", str(target))
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
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (7, 'movie', 7, 'Loud', '1979', 'imdb', ?, ?)", (NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, file_size, "
                  " loudness_i, loudness_tp, loudness_measured_sha256, loudness_measured_at) "
                  "VALUES ('movie', 7, '1', '', 'movies/7/theme.mp3', 'sha7', ?, 'vid', "
                  " 1000000, -5.2, 2.9, 'sha7', ?)", (NOW, NOW))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, "
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie', 7, '1', '/data/movies/7', '', 'hardlink', ?)", (NOW,))
        c.execute("INSERT OR IGNORE INTO plex_items (rating_key, media_type, section_id, "
                  " title, guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (900007, 'movie', '1', 'Loud', 7, '', 1, ?, ?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s


def _capture_target(monkeypatch):
    """Intercept the ONE place the target reaches real bytes."""
    seen = {}

    def _fake(path, target, measured_i, true_peak, *, expect_sha=None):
        seen["target"] = target
        return {"ok": True, "changed": False, "steps": 0, "applied_db": 0.0,
                "note": "no change", "old_sha": "sha7", "new_sha": "sha7",
                "old_pcm_sha": "pcm7", "new_i": measured_i, "new_tp": true_peak,
                "new_lra": None}

    monkeypatch.setattr("app.core.loudness_apply.normalize_file", _fake)
    return seen


def test_normalize_one_uses_the_configured_target_not_a_hardcoded_18(tmp_path, monkeypatch):
    """The bug: auto-level landed at -20, hand-level at -18, on the same library."""
    c, s = _make_client(tmp_path, monkeypatch, target=-20.0)
    assert s.loudness_target_lufs == -20.0, "fixture failed to configure the target"
    seen = _capture_target(monkeypatch)
    r = c.post("/api/admin/loudness/normalize-one",
               json={"media_type": "movie", "tmdb_id": 7, "section_id": "1",
                     "edition_key": ""}, headers=AUTH)
    assert r.json()["ok"] is True, r.json()
    assert seen["target"] == -20.0, (
        f"normalize-one leveled to {seen['target']} while the configured target is -20 — "
        f"the same operation answering two ways depending on the door")


def test_an_explicit_body_target_still_wins(tmp_path, monkeypatch):
    """The stepper's whole point: the card names a target and it is honored."""
    c, _ = _make_client(tmp_path, monkeypatch, target=-20.0)
    seen = _capture_target(monkeypatch)
    c.post("/api/admin/loudness/normalize-one",
           json={"media_type": "movie", "tmdb_id": 7, "section_id": "1",
                 "edition_key": "", "target": -14.5}, headers=AUTH)
    assert seen["target"] == -14.5


def test_a_junk_body_target_falls_back_to_config_not_to_18(tmp_path, monkeypatch):
    """A malformed target is a bad REQUEST, not a request for a different default."""
    c, _ = _make_client(tmp_path, monkeypatch, target=-20.0)
    seen = _capture_target(monkeypatch)
    c.post("/api/admin/loudness/normalize-one",
           json={"media_type": "movie", "tmdb_id": 7, "section_id": "1",
                 "edition_key": "", "target": "loud"}, headers=AUTH)
    assert seen["target"] == -20.0


def test_out_of_band_target_is_still_clamped(tmp_path, monkeypatch):
    """The clamp must survive the default change — it guards real audio."""
    c, _ = _make_client(tmp_path, monkeypatch, target=-20.0)
    seen = _capture_target(monkeypatch)
    c.post("/api/admin/loudness/normalize-one",
           json={"media_type": "movie", "tmdb_id": 7, "section_id": "1",
                 "edition_key": "", "target": 40.0}, headers=AUTH)
    assert seen["target"] == -6.0, "clamp lost — a +40 LUFS request would destroy a theme"


def test_api_item_publishes_the_configured_target_for_the_stepper(tmp_path, monkeypatch):
    c, _ = _make_client(tmp_path, monkeypatch, target=-20.0)
    body = c.get("/api/items/movie/7", headers=AUTH).json()
    assert body["loudness_target_default"] == -20.0, (
        "the card's stepper seeds from this; a stale value sends the wrong target")


def test_the_stepper_seeds_from_the_server_not_a_third_hardcoded_default():
    """Closing the drift server-side is pointless if the JS re-opens it."""
    assert "data.loudness_target_default" in APP_JS
    # mp3gain's quantum is a FACT about the tool, not a display choice — one press of
    # the stepper must be exactly one step or the gain note lies about what lands.
    assert "1.505" in APP_JS
