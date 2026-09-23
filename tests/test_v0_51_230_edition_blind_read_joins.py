"""v0.51.230 — audit wave 3: the edition-blind placements→local_files read joins.

Five sites joined `placements` to `local_files` on (media_type, tmdb_id, section_id) only.
Both tables carry `edition_key` in their PRIMARY KEY (verified by PRAGMA, added by schema
v63), so without it EVERY placement fans across EVERY edition's local_files row — SUM and
COUNT multiply. This re-opens the v1.14.36 cartesian bug on the edition axis; the inline
comment on those lines still documented only the v1.14.36 SECTION-axis fix.

Measured on a 2-edition title with 2 copy placements of 100B each:
    edition-blind join -> 400 bytes / 4 rows      (2x inflated)
    edition-scoped     -> 200 bytes / 2 rows      (truth)

Affected surfaces: the dashboard STORAGE 'copies' KPI, /api/public/stats (the Homepage
widget), /api/storage/copies (whose whole purpose is telling the operator how many bytes
reorganising the share would reclaim — so the number they act on was wrong), plus two more.

Also: sql_missing_count joined local_files title-wide while the DOWNLOAD MISSING action
query it mirrors is section+edition scoped, so a title downloaded in the standard section
reported 0 missing on the 4K tab while the action would enqueue it.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-07-27T00:00:00"


def test_both_tables_really_are_edition_keyed():
    """The premise. If edition_key ever leaves either PK the joins below are moot — and
    a future reader should be told that by a failing test, not by silence."""
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    c = sqlite3.connect(db)
    for t in ("local_files", "placements"):
        pk = [r[1] for r in c.execute(f"PRAGMA table_info({t})") if r[5]]
        assert "edition_key" in pk, f"{t} PK lost edition_key: {pk}"


def test_every_placements_to_local_files_join_is_edition_scoped():
    """A bare (media_type, tmdb_id, section_id) join is one-to-MANY once a title has a
    second edition. Every such join must carry the edition predicate."""
    blind = API_PY.count("AND lf.section_id = p.section_id")
    scoped = API_PY.count("AND lf.edition_key = p.edition_key")
    assert blind == scoped, (
        f"{blind} placements->local_files joins but only {scoped} carry "
        "`AND lf.edition_key = p.edition_key` — the unscoped ones row-multiply")
    assert scoped >= 5, "expected at least the 5 known sites"


def test_the_multiplication_is_actually_gone():
    """Behavioral: reproduce the fan-out on a real DB and assert the scoped join returns
    the truth. A source-pin alone would not prove the predicate is doing anything."""
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    c = sqlite3.connect(db)
    c.execute("PRAGMA foreign_keys=OFF")
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
              " last_seen_sync_at, first_seen_sync_at) "
              "VALUES (1,'movie',120,'Two Cuts','imdb',?,?)", (NOW, NOW))
    for edn in ("theatrical", "extended"):
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
                  " file_path, file_sha256, downloaded_at, source_video_id, file_size) "
                  "VALUES ('movie',120,'1',?,?,?,?,'v',100)",
                  (edn, f"m/{edn}.mp3", f"sha{edn}", NOW))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie',120,'1',?,?,'copy',?)", (f"/data/{edn}", edn, NOW))
    c.commit()

    base = ("SELECT COALESCE(SUM(lf.file_size),0), COUNT(*) FROM placements p "
            "JOIN local_files lf ON lf.media_type=p.media_type "
            "AND lf.tmdb_id=p.tmdb_id AND lf.section_id=p.section_id ")
    blind_sum, blind_rows = c.execute(base + "WHERE p.placement_kind='copy'").fetchone()
    scoped_sum, scoped_rows = c.execute(
        base + "AND lf.edition_key=p.edition_key WHERE p.placement_kind='copy'").fetchone()

    assert (blind_sum, blind_rows) == (400, 4), "the fan-out premise must hold"
    assert (scoped_sum, scoped_rows) == (200, 2), (
        "the edition-scoped join must report the 2 real copies and their 200 real bytes")


def test_download_missing_is_section_scoped(tmp_path, monkeypatch):
    """v0.51.346: sql_missing_count is gone; the DOWNLOAD MISSING action it mirrored holds the
    section scope. A title downloaded only in the standard section is missing on the 4K tab."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from fastapi.testclient import TestClient
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web import api
    monkeypatch.setattr(api, "log_event", lambda *a, **k: None)
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    c = sqlite3.connect(s.db_path)
    for sid, fourk in (("1", 0), ("2", 1)):
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included,"
                  " discovered_at, last_seen_at) VALUES (?, ?, 'movie', 0, ?, ?, 1, ?, ?)",
                  (sid, f"S{sid}", fourk, f"sub{sid}", NOW, NOW))
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
              " first_seen_sync_at, youtube_url) VALUES (1, 'movie', 120, 'Two Cuts', 'imdb', ?, ?,"
              " 'https://www.youtube.com/watch?v=abcdefghijk')", (NOW, NOW))
    for rk, sid in (("std", "1"), ("uhd", "2")):
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title,"
                  " first_seen_at, last_seen_at) VALUES (?, ?, 'movie', 1, 120, 'Two Cuts', ?, ?)", (rk, sid, NOW, NOW))
    c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path, downloaded_at,"
              " source_video_id) VALUES ('movie', 120, '1', '', 'm/std.mp3', ?, 'v')", (NOW,))
    c.commit()
    tc = TestClient(api.create_app(s))
    hdr = {"X-Authentik-Username": "testadmin"}
    r = tc.post("/api/library/download-missing", json={"tab": "movies", "fourk": False}, headers=hdr)
    assert (r.status_code, r.json()["enqueued"]) == (200, 0)
    r = tc.post("/api/library/download-missing", json={"tab": "movies", "fourk": True}, headers=hdr)
    assert r.status_code == 200 and r.json()["enqueued"] >= 1
    sections = {row[0] for row in c.execute("SELECT section_id FROM jobs WHERE job_type = 'download'")}
    c.close()
    assert "2" in sections
