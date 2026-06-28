"""v1.21.49 — library CSV export is TAB-delimited (UTF-16 fix).

The export is UTF-16 LE (v1.14.26, for encoding robustness), but it was
comma-delimited. Excel / Numbers treat UTF-16 as "Unicode Text" — which
is TAB-separated — so a UTF-16 + comma file made every field land in
column A (the user's repro: "headers all lumped in the first column").

Fix: emit TAB-delimited. The importer auto-detects the delimiter off the
header line so a user who re-saved as a comma CSV in Excel still
round-trips. These tests exercise the real import endpoint with a
UTF-16-LE-BOM tab file (mimicking the export) — the discriminator is a
title CONTAINING A COMMA, which would split into the wrong columns under
the old comma parser but parses as one field under tab.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), settings


@pytest.fixture
def auth_headers():
    return {"X-Authentik-Username": "testadmin"}


def _seed_theme(db_path: Path, *, tmdb_id: int, imdb_id: str, title: str,
                year: int):
    with get_conn(db_path) as conn, transaction(conn):
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, imdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie', ?, ?, ?, ?, 'themoviedb',"
            " '2026-01-01T00:00:00', '2026-01-01T00:00:00')",
            (tmdb_id, imdb_id, title, year))


def _utf16_tsv(rows: list[tuple[str, str, str]]) -> bytes:
    """Mimic the v1.21.49 export exactly: UTF-16 LE BOM, TAB-delimited,
    CRLF line endings."""
    lines = ["Title\tIMDB\tYoutube_URL"]
    for title, imdb, yt in rows:
        lines.append(f"{title}\t{imdb}\t{yt}")
    text = "\r\n".join(lines) + "\r\n"
    return ("﻿" + text).encode("utf-16-le")


def _utf8_csv(rows: list[tuple[str, str, str]]) -> bytes:
    """A plain comma CSV (what a user gets if they re-save in Excel) —
    backward-compat path the auto-detect must still handle."""
    lines = ["Title,IMDB,Youtube_URL"]
    for title, imdb, yt in rows:
        lines.append(f"{title},{imdb},{yt}")
    return ("\r\n".join(lines) + "\r\n").encode("utf-8")


def _preview(client, headers, fname, data):
    return client.post(
        "/api/import/preview",
        files={"file": (fname, data, "text/csv")},
        headers=headers,
    )


def test_tab_delimited_utf16_import_parses(app_client, auth_headers):
    client, settings = app_client
    _seed_theme(settings.db_path, tmdb_id=100, imdb_id="tt0000100",
                title="Test Movie", year=1999)
    resp = _preview(client, auth_headers, "motif-export.csv", _utf16_tsv([
        ("Test Movie (1999)", "tt0000100",
         "https://www.youtube.com/watch?v=abcdefghijk"),
    ]))
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["counts"]["clean"] == 1, data["counts"]
    assert data["rows"][0]["theme_tmdb_id"] == 100


def test_comma_in_title_round_trips_under_tab(app_client, auth_headers):
    """THE discriminator: a title with commas. Under the OLD comma
    delimiter this split into extra columns and the IMDB landed in the
    wrong field → no_match. Tab-delimited, the comma is just data."""
    client, settings = app_client
    _seed_theme(settings.db_path, tmdb_id=300, imdb_id="tt0000300",
                title="Lock, Stock and Two Smoking Barrels", year=1998)
    resp = _preview(client, auth_headers, "motif-export.csv", _utf16_tsv([
        ("Lock, Stock and Two Smoking Barrels (1998)", "tt0000300",
         "https://www.youtube.com/watch?v=abcdefghijk"),
    ]))
    assert resp.status_code == 200, resp.text
    data = resp.json()
    # The comma-laden title matched the theme by IMDB → clean, not
    # no_match. Proves the title parsed as a SINGLE field.
    assert data["counts"].get("no_match", 0) == 0, data["counts"]
    assert data["counts"]["clean"] == 1, data["counts"]
    assert data["rows"][0]["theme_tmdb_id"] == 300


def test_comma_csv_still_imports_backward_compat(app_client, auth_headers):
    """A user who re-saved the export as a plain comma CSV in Excel must
    still import — the auto-detect falls back to comma when the header
    has no tab."""
    client, settings = app_client
    _seed_theme(settings.db_path, tmdb_id=200, imdb_id="tt0000200",
                title="Comma Saved", year=2001)
    resp = _preview(client, auth_headers, "resaved.csv", _utf8_csv([
        ("Comma Saved (2001)", "tt0000200",
         "https://www.youtube.com/watch?v=abcdefghijk"),
    ]))
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["counts"]["clean"] == 1, data["counts"]
    assert data["rows"][0]["theme_tmdb_id"] == 200


def test_import_autodetects_delimiter_off_header(app_client, auth_headers):
    """Pin the delimiter-detection logic in the importer source."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert 'delimiter = "\\t" if "\\t" in first_line else ","' in api
    assert "csv.DictReader(io.StringIO(text), delimiter=delimiter)" in api


def test_preview_parse_offloaded_to_threadpool():
    """v1.21.50: the parse + per-row categorization (N DB queries) must
    run OFF the event loop — a large import froze the whole app before.
    v1.21.49 exposed it: tab files finally parse into N rows, running
    the previously-dead blocking loop."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert "run_in_threadpool(_import_preview_work, db, raw)" in api
    assert "def _import_preview_work(" in api
