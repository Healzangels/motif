"""v1.22.0 — line-close audit fixes (3-agent audit before the v1.22.0 cut).

A) SET URL on a tagged edition must thread edition_key into the download
   payload. v1.21.92 scoped the worker's override resolution to edition_key,
   but api_manual_url wrote the override at the tagged edition while enqueuing
   an edition-LESS download → worker resolved the override at '' → no match →
   ignored the user's URL. The regression the line-close audit caught.

B) worker._do_relink must pull THIS edition's source file (the v1.21.94
   edition_key JOIN) — a copy→hardlink relink on a multi-edition title must not
   cross-link a sibling edition's theme.mp3. Previously source-pinned only.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


NOW = "2026-06-04T00:00:00Z"
AUTH = {"X-Authentik-Username": "testadmin"}


# ── A: SET URL on a tagged edition threads edition_key into the download ──


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s.db_path


def test_set_url_on_tagged_edition_threads_edition_key(client):
    c, db = client
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at) VALUES (90,'movie',900,'W',"
            "'imdb',?,?)", (NOW, NOW))
        for rk, ek, folder in (
            ("rk-std", "", "/d/W (2000)"),
            ("rk-ext", "extended", "/d/W (2000) {edition-Extended}"),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1','movie',"
                "90,900,'W',?,?,0,?,?)", (rk, ek, folder, NOW, NOW))
        conn.commit()

    r = c.post("/api/plex_items/rk-ext/manual-url", headers=AUTH,
               json={"youtube_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"})
    assert r.status_code == 200, r.text
    with sqlite3.connect(db) as conn:
        payloads = [json.loads(p) for (p,) in conn.execute(
            "SELECT payload FROM jobs WHERE job_type='download' AND tmdb_id=900")]
        ovr = conn.execute(
            "SELECT youtube_url FROM user_overrides WHERE tmdb_id=900 AND"
            " edition_key='extended'").fetchone()
    assert len(payloads) == 1
    # The download must carry the edition so the worker resolves the override
    # we just wrote at edition_key='extended' (not the '' tier).
    assert payloads[0].get("edition_key") == "extended", payloads
    assert ovr is not None and ovr[0].endswith("dQw4w9WgXcQ")


# ── B: _do_relink pulls THIS edition's source, not a sibling's ──


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def test_do_relink_does_not_cross_link_sibling_edition(tmp_path):
    from app.config import Settings
    from app.core.db import init_db
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    themes = tmp_path / "themes"
    s._cfg.paths.themes_dir = str(themes)
    media = tmp_path / "media"

    # Two editions, each with its OWN canonical source (distinct bytes) and a
    # copy-kind placement in its own media folder (a stale theme.mp3 to relink).
    eds = {
        "theatrical": (b"THEATRICAL-BYTES", media / "X (2001) {edition-Theatrical}"),
        "extended": (b"EXTENDED-BYTES", media / "X (2001) {edition-Extended}"),
    }
    with sqlite3.connect(s.db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at) VALUES (12,'movie',120,'X',"
            "'imdb',?,?)", (NOW, NOW))
        for ek, (data, folder) in eds.items():
            rel = f"movies/X (2001) {{edition-{ek}}}/theme.mp3"
            src = themes / rel
            src.parent.mkdir(parents=True, exist_ok=True)
            src.write_bytes(data)
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "theme.mp3").write_bytes(b"STALE-COPY")  # to be relinked
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',120,'1',?,?,?,'v',"
                "'auto','themerrdb')", (ek, rel, NOW))
            conn.execute(
                "INSERT INTO placements (theme_id, media_type, tmdb_id,"
                " section_id, edition_key, media_folder, placed_at,"
                " placement_kind, plex_refreshed) VALUES (12,'movie',120,'1',?,"
                "?,?,'copy',1)", (ek, str(folder), NOW))
        conn.commit()

    c = sqlite3.connect(s.db_path)
    c.row_factory = sqlite3.Row
    c.execute("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
              " payload, status, created_at) VALUES ('relink','movie',120,'1',"
              "'{}','running',?)", (NOW,))
    c.commit()
    job = c.execute("SELECT * FROM jobs WHERE job_type='relink'").fetchone()
    _worker(s)._do_relink(job)

    # Each edition's placed theme.mp3 must now match ITS OWN source — never the
    # sibling's. Without the edition_key JOIN the copy-relink cross-products and
    # could hardlink Extended's bytes into the Theatrical folder.
    for ek, (data, folder) in eds.items():
        assert (folder / "theme.mp3").read_bytes() == data, (
            f"{ek} relinked to the wrong edition's source")


def test_v1_22_0_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
