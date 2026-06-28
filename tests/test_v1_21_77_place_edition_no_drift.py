"""v1.21.77 — verify PLACE / PL row has no cross-edition drift.

the user asked, after the v1.21.74 download-dot drift fix: is there a similar
issue on the PLACE side / the PL row? Audit conclusion: no —
  * PL STATE (placed / await / unplaced) reads the edition-aware two-join
    COALESCE(p_e.*, p_g.*), so each edition's placement is its own.
  * the "placing" pulse reads job_in_flight, whose subquery (v1.21.74)
    matches job_type IN ('download','place') scoped by the payload's
    edition_key — and every place-job enqueue site carries edition_key.

This test locks both properties in: a running PLACE job for ONE edition
lights job_in_flight on that edition only, and a placement on one edition
does not make a sibling look placed.
"""
from __future__ import annotations

import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.editions import edition_key_for_folder
from app.core.events import now_iso


NOW = now_iso()
A_RK = "167699"            # Theatrical — PLACED, no active job
B_RK = "167709"            # Extended  — being PLACED right now
A_FOLDER = "/data/Movies/LotR (2001) {edition-Theatrical}"
B_FOLDER = "/data/Movies/LotR (2001) {edition-Extended Edition}"
A_KEY = edition_key_for_folder(A_FOLDER)
B_KEY = edition_key_for_folder(B_FOLDER)


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    return TestClient(app), db


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',120,'LotR','2001','imdb',?,?,'u')",
            (NOW, NOW))
        tid = cur.lastrowid
        for rk, ek, fp in ((A_RK, A_KEY, A_FOLDER), (B_RK, B_KEY, B_FOLDER)):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1',"
                "'movie',?,120,'LotR','2001',?,?,1,?,?)",
                (rk, tid, ek, fp, NOW, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',120,'1',?,?,?,"
                "'v','auto','themerrdb')", (ek, f"movies/{ek}.mp3", NOW))
        # Only Theatrical (A) is PLACED.
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
            " media_folder, placed_at, placement_kind, plex_refreshed,"
            " provenance, edition_key) VALUES ('movie',120,'1',?,?,?,"
            "'hardlink',1,'auto',?)", (tid, A_FOLDER, NOW, A_KEY))
        # A PLACE job is in flight for Extended (B) right now.
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at) VALUES ('place','movie',120,'1',"
            "?,'running',?)", (json.dumps({"edition_key": B_KEY}), NOW))
        conn.commit()


def _rows(client):
    r = client.get("/api/library?tab=movies",
                   headers={"X-Authentik-Username": "testadmin"})
    assert r.status_code == 200, r.text
    return {it["edition_key"]: it for it in r.json()["items"]}


def test_place_pulse_and_pl_state_are_per_edition(app_client):
    client, db = app_client
    _seed(db)
    rows = _rows(client)
    assert set(rows) == {A_KEY, B_KEY}, list(rows)

    # PL STATE: Theatrical is placed (its own folder), Extended is not.
    assert rows[A_KEY]["media_folder"] == A_FOLDER
    assert rows[A_KEY]["placement_kind"] == "hardlink"
    assert not rows[B_KEY]["media_folder"], rows[B_KEY]

    # PLACE PULSE: the in-flight place job lights job_in_flight on Extended
    # ONLY — Theatrical (no active job) stays clear, no cross-edition drift.
    assert rows[B_KEY]["job_in_flight"] == "place", rows[B_KEY]
    assert not rows[A_KEY]["job_in_flight"], (
        f"Theatrical lit the PL pulse from Extended's place job: "
        f"{rows[A_KEY]['job_in_flight']}")
