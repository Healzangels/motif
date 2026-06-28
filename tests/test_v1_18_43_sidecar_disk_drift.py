"""v1.18.43 — Sidecar-disk drift detection.

Pre-v1.18.36 `api_switch_placement` had a bug where SWITCH
TO API (file→api) dropped motif's placement row + queued the
API upload job but never removed the existing sidecar
theme.<ext> at the Plex media folder. Result: legacy installs
have orphan sidecars on plex_upload rows that motif's tracking
doesn't know about.

v1.18.43 adds:

  1. **`find_theme_sidecar_path(folder_path)`** in plex_enum.py:
     like `stat_theme_sidecar` but returns the actual Path
     (used by both the scanner for detection AND the delete
     endpoint to know which file to unlink).

  2. **Orphan scan extension**: for every plex_upload
     placement, walk plex_items.folder_path with the new
     helper. If a theme.<ext> exists, classify as new drift
     type `orphan_sidecar_on_disk` (or surface alongside the
     existing classification for non-ok rows).

  3. **`POST /api/admin/delete-orphan-sidecar`**: admin
     endpoint to unlink the file. Re-resolves folder_path
     from plex_items server-side (defense-in-depth — doesn't
     trust client-supplied paths).

  4. **Dashboard DELETE SIDECAR button**: appears on rows
     classified as `orphan_sidecar_on_disk`. Wires to the new
     endpoint. Path shown in tooltip for transparency before
     the confirm dialog fires.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent
ORPHANS_HTML = REPO / "app" / "web" / "templates" / "orphans.html"


# ── find_theme_sidecar_path helper ───────────────────────────


def test_find_theme_sidecar_returns_path_when_present(tmp_path):
    """Returns the actual Path when a theme.mp3 exists at the
    folder."""
    from app.core.plex_enum import find_theme_sidecar_path
    folder = tmp_path / "Movie (2020)"
    folder.mkdir()
    sidecar = folder / "theme.mp3"
    sidecar.write_bytes(b"audio bytes")
    out = find_theme_sidecar_path(str(folder))
    assert out is not None
    assert out == sidecar


def test_find_theme_sidecar_returns_none_when_absent(tmp_path):
    """Returns None when no theme.<ext> exists."""
    from app.core.plex_enum import find_theme_sidecar_path
    folder = tmp_path / "Movie (2020)"
    folder.mkdir()
    # Empty folder; no theme.* file.
    out = find_theme_sidecar_path(str(folder))
    assert out is None


def test_find_theme_sidecar_returns_none_for_empty_path():
    from app.core.plex_enum import find_theme_sidecar_path
    assert find_theme_sidecar_path("") is None
    assert find_theme_sidecar_path(None) is None


def test_find_theme_sidecar_returns_none_for_nonexistent_folder():
    from app.core.plex_enum import find_theme_sidecar_path
    out = find_theme_sidecar_path("/nonexistent/path/here")
    assert out is None


# ── Orphan scan classifies orphan sidecars ───────────────────


def _seed_plex_upload_with_folder(
    db, *, media_type, tmdb_id, section_id, folder_path,
):
    ts = "2026-05-21T00:00:00+00:00"
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT OR IGNORE INTO themes "
            "(media_type, tmdb_id, title, youtube_url, "
            " upstream_source, last_seen_sync_at, "
            " first_seen_sync_at) "
            "VALUES (?, ?, ?, ?, 'themoviedb', ?, ?)",
            (media_type, tmdb_id, f"Test {tmdb_id}", "", ts, ts),
        )
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections "
            "(section_id, title, type, included, "
            " discovered_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (section_id, "Test Section", "movie", 1, ts, ts),
        )
        conn.execute(
            "INSERT OR IGNORE INTO plex_items "
            "(rating_key, section_id, media_type, title, year, "
            " guid_tmdb, has_theme, folder_path, "
            " first_seen_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (f"rk-{tmdb_id}", section_id, "movie",
             f"Test {tmdb_id}", 2024, str(tmdb_id), 1,
             str(folder_path), ts, ts),
        )
        conn.execute(
            "INSERT OR REPLACE INTO placements "
            "(media_type, tmdb_id, section_id, placement_kind, "
            " media_folder, placed_at) "
            "VALUES (?, ?, ?, 'plex_upload', '', ?)",
            (media_type, tmdb_id, section_id, ts),
        )


def test_scan_classifies_orphan_sidecar_on_disk(tmp_path):
    """plex_upload row + sidecar at the media folder → drift
    type orphan_sidecar_on_disk. The sidecar path is surfaced
    in the finding for the delete-action UI."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    # Set up the Plex media folder + drop an orphan sidecar.
    plex_folder = tmp_path / "movies" / "Test 400 (2020)"
    plex_folder.mkdir(parents=True)
    (plex_folder / "theme.mp3").write_bytes(b"orphan sidecar")
    _seed_plex_upload_with_folder(
        db, media_type="movie", tmdb_id=400,
        section_id="1", folder_path=plex_folder,
    )
    # Plex has motif's upload entry, healthy selection.
    plex = MagicMock()
    # Hash motif's canonical bytes so the entry matches.
    import hashlib
    motif_canonical = tmp_path / "themes" / "movies" / "Test 400 (2020)"
    motif_canonical.mkdir(parents=True)
    (motif_canonical / "theme.mp3").write_bytes(b"motif theme bytes")
    motif_hash = hashlib.sha1(b"motif theme bytes").hexdigest()
    # Register the canonical in local_files.
    ts = "2026-05-21T00:00:00+00:00"
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT OR REPLACE INTO local_files "
            "(media_type, tmdb_id, section_id, file_path, "
            " downloaded_at, source_video_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("movie", 400, "1",
             "movies/Test 400 (2020)/theme.mp3",
             ts, "video_id"),
        )
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 1, "Metadata": [
            {"ratingKey": f"upload://themes/{motif_hash}",
             "selected": True},
        ]}},
    }
    findings = scan_plex_upload_placements(
        db, plex, themes_dir=tmp_path / "themes",
    )
    assert len(findings) == 1
    f = findings[0]
    assert f["drift_type"] == "orphan_sidecar_on_disk", (
        f"expected orphan_sidecar_on_disk; got "
        f"{f['drift_type']}: {f['details']}"
    )
    assert f["orphan_sidecar_path"] == str(
        plex_folder / "theme.mp3"
    )
    assert "pre-v1.18.36 SWITCH" in f["details"]


def test_scan_ok_when_no_orphan_sidecar(tmp_path):
    """plex_upload row + no sidecar at folder → drift type
    stays `ok` (the v1.18.40 baseline)."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    plex_folder = tmp_path / "movies" / "Test 401 (2020)"
    plex_folder.mkdir(parents=True)
    # No theme.mp3 dropped.
    _seed_plex_upload_with_folder(
        db, media_type="movie", tmdb_id=401,
        section_id="1", folder_path=plex_folder,
    )
    motif_canonical = tmp_path / "themes" / "movies" / "Test 401 (2020)"
    motif_canonical.mkdir(parents=True)
    (motif_canonical / "theme.mp3").write_bytes(b"motif theme bytes")
    import hashlib
    motif_hash = hashlib.sha1(b"motif theme bytes").hexdigest()
    ts = "2026-05-21T00:00:00+00:00"
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT OR REPLACE INTO local_files "
            "(media_type, tmdb_id, section_id, file_path, "
            " downloaded_at, source_video_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("movie", 401, "1",
             "movies/Test 401 (2020)/theme.mp3",
             ts, "video_id"),
        )
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 1, "Metadata": [
            {"ratingKey": f"upload://themes/{motif_hash}",
             "selected": True},
        ]}},
    }
    findings = scan_plex_upload_placements(
        db, plex, themes_dir=tmp_path / "themes",
    )
    assert findings[0]["drift_type"] == "ok"
    assert findings[0]["orphan_sidecar_path"] is None


# ── DELETE endpoint ──────────────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_ENABLED", "true")
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://plex.test:32400")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "testtoken")
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    client = TestClient(app)
    return client, settings


AUTH = {"X-Authentik-Username": "testadmin"}


def test_delete_endpoint_requires_auth(admin_client):
    client, _ = admin_client
    r = client.post(
        "/api/admin/delete-orphan-sidecar"
        "?media_type=movie&tmdb_id=400&section_id=1",
    )
    assert r.status_code in (401, 403)


def test_delete_endpoint_unlinks_sidecar(admin_client, tmp_path):
    client, settings = admin_client
    plex_folder = tmp_path / "media" / "Test 500 (2020)"
    plex_folder.mkdir(parents=True)
    sidecar = plex_folder / "theme.mp3"
    sidecar.write_bytes(b"orphan to delete")
    assert sidecar.is_file()
    _seed_plex_upload_with_folder(
        settings.db_path, media_type="movie", tmdb_id=500,
        section_id="1", folder_path=plex_folder,
    )
    r = client.post(
        "/api/admin/delete-orphan-sidecar"
        "?media_type=movie&tmdb_id=500&section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] is True
    assert data["deleted"] is True
    assert data["path"] == str(sidecar)
    # File should actually be gone.
    assert not sidecar.is_file()


def test_delete_endpoint_returns_ok_false_when_no_sidecar(
    admin_client, tmp_path,
):
    """No sidecar to delete → ok=True, deleted=False (idempotent
    — clicking the button twice doesn't error)."""
    client, settings = admin_client
    plex_folder = tmp_path / "media" / "Test 501 (2020)"
    plex_folder.mkdir(parents=True)
    # No sidecar dropped.
    _seed_plex_upload_with_folder(
        settings.db_path, media_type="movie", tmdb_id=501,
        section_id="1", folder_path=plex_folder,
    )
    r = client.post(
        "/api/admin/delete-orphan-sidecar"
        "?media_type=movie&tmdb_id=501&section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["deleted"] is False
    assert "no sidecar" in data["reason"].lower()


def test_delete_endpoint_404_when_no_plex_items_row(admin_client):
    """No matching plex_items row → 404 (defense-in-depth — we
    don't trust client-supplied paths)."""
    client, _ = admin_client
    r = client.post(
        "/api/admin/delete-orphan-sidecar"
        "?media_type=movie&tmdb_id=999999&section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 404


# ── Dashboard DELETE SIDECAR button ──────────────────────────


def test_dashboard_has_delete_sidecar_action():
    """The orphans dashboard template must render a DELETE
    SIDECAR button for orphan_sidecar_on_disk rows."""
    src = ORPHANS_HTML.read_text()
    assert "DELETE SIDECAR" in src
    assert "data-act=\"delete-sidecar\"" in src
    # Action handler wires to the new endpoint.
    assert "/api/admin/delete-orphan-sidecar" in src


def test_dashboard_delete_sidecar_requires_confirm():
    """Destructive action — must show a confirm dialog first."""
    src = ORPHANS_HTML.read_text()
    # The handler block for delete-sidecar must include confirm.
    idx = src.index("act === 'delete-sidecar'")
    block = src[idx:idx + 1000]
    assert "confirm(" in block


def test_dashboard_drift_tone_includes_orphan_sidecar():
    """DRIFT_TONE map must include the new drift type so the
    chip renders with the right tone class."""
    src = ORPHANS_HTML.read_text()
    assert "'orphan_sidecar_on_disk':" in src
