"""v1.18.37 — Plex-side drift scanner + motif_hash diagnostic.

Two additions from the user's v1.18.36 live-install verification:

1. The 12 Monkeys LPS test surfaced `motif_hash=None` in the
   docker log. The plex_upload branch in api_unplace_item
   couldn't compute motif's SHA-1 for the canonical file (likely
   a section-scope mismatch in the local_files lookup). The
   fallback heuristic correctly picked the themerr-plex entry
   anyway, but only because Plex enumerated it first. Fragile.

2. With v1.18.0+ collection deletes silently failing for ~2
   months and v1.18.23+ SWITCH operations leaving orphans, the
   user's Plex theme store probably has drift between motif's
   tracking and Plex's reality. Need a read-only scanner to
   surface what's drifted.

## v1.18.37 ships

### A. motif_hash diagnostic + fallback

`api_unplace_item` plex_upload branch:
  - If section-scoped local_files lookup returns no row, fall
    back to unscoped lookup
  - Log the specific failure mode when motif_hash ends up None
    (no row / empty path / file missing / IO error)

### B. orphan_scan.scan_plex_upload_placements

New module `app/core/orphan_scan.py`. Walks every
placement_kind='plex_upload' row, queries Plex's /themes,
classifies drift per row. Drift types:
  - ok / rk_lookup_failed / plex_fetch_failed
  - no_plex_entries / motif_hash_unknown
  - motif_entry_missing / motif_not_selected / nothing_selected

### C. Admin endpoint

GET /api/admin/orphan-scan?rk_only=<optional_rk>
Returns JSON with summary stats + per-row findings.

Read-only — no fixes applied. Findings guide manual follow-up
via motif's existing UI actions.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── motif_hash diagnostic + fallback ─────────────────────────


def test_unplace_motif_hash_uses_unscoped_fallback():
    """The plex_upload branch must fall back to an unscoped
    local_files lookup when the section-scoped one returns
    nothing. v1.18.37 adds this because the user's 12 Monkeys
    LPS test showed the section-scoped lookup can miss rows
    that legitimately exist."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 24000]
    # The fallback unscoped lookup must be present.
    assert "if lf_row is None:" in body
    # And it must query local_files without section_id.
    fb_idx = body.find("if lf_row is None:")
    fallback_block = body[fb_idx:fb_idx + 800]
    assert (
        "SELECT file_path FROM local_files "
        in fallback_block
    )
    assert "LIMIT 1" in fallback_block


def test_unplace_motif_hash_logs_failure_reason():
    """When motif_hash ends up None, the branch must log WHY
    (no row / empty file_path / file missing / io error) so
    operators can debug future LPS drift cases."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 24000]
    assert "hash_reason" in body
    assert '"no local_files row"' in body
    assert '"empty file_path"' in body
    assert "file missing:" in body
    # The summary warning must mention the reason. The log
    # string is split across two adjacent literals in source
    # ("motif_hash " "unavailable for ...") so we check each
    # half rather than the concatenated form.
    assert '"unplace[plex_upload]: motif_hash "' in body
    assert "unavailable for" in body


# ── orphan_scan module ───────────────────────────────────────


def test_scan_empty_db_returns_empty_findings(tmp_path):
    """No plex_upload placements → empty findings list."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    plex = MagicMock()
    findings = scan_plex_upload_placements(db, plex)
    assert findings == []
    plex.get_themes.assert_not_called()


def _seed_placement(
    db, *, media_type, tmdb_id, section_id,
    placement_kind="plex_upload", media_folder="",
):
    """Helper: insert a placements row + parent themes / plex_items
    / plex_sections rows. Fills all NOT NULL columns the
    schema requires."""
    ts = "2026-05-21T00:00:00+00:00"
    with get_conn(db) as conn, transaction(conn):
        # themes row (NOT NULL: upstream_source, last_seen_sync_at,
        # first_seen_sync_at).
        conn.execute(
            "INSERT OR IGNORE INTO themes "
            "(media_type, tmdb_id, title, youtube_url, "
            " upstream_source, last_seen_sync_at, "
            " first_seen_sync_at) "
            "VALUES (?, ?, ?, ?, 'themoviedb', ?, ?)",
            (media_type, tmdb_id, f"Test {tmdb_id}", "", ts, ts),
        )
        # plex_sections row (NOT NULL: title, type, discovered_at,
        # last_seen_at).
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections "
            "(section_id, title, type, included, discovered_at, "
            " last_seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (section_id, "Test Section", "movie", 1, ts, ts),
        )
        # plex_items row.
        plex_mt = (
            "show" if media_type == "tv"
            else ("collection" if media_type == "collection" else "movie")
        )
        conn.execute(
            "INSERT OR IGNORE INTO plex_items "
            "(rating_key, section_id, media_type, title, year, "
            " guid_tmdb, has_theme, first_seen_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (f"rk-{tmdb_id}", section_id, plex_mt,
             f"Test {tmdb_id}", 2024, str(tmdb_id), 1, ts, ts),
        )
        # placements row — placed_at is NOT NULL.
        conn.execute(
            "INSERT OR REPLACE INTO placements "
            "(media_type, tmdb_id, section_id, placement_kind, "
            " media_folder, placed_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (media_type, tmdb_id, section_id, placement_kind,
             media_folder, ts),
        )


def _seed_local_file(db, *, media_type, tmdb_id, section_id, file_path):
    ts = "2026-05-21T00:00:00+00:00"
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT OR REPLACE INTO local_files "
            "(media_type, tmdb_id, section_id, file_path, "
            " downloaded_at, source_video_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (media_type, tmdb_id, section_id, str(file_path),
             ts, "test_video_id"),
        )


def test_scan_rk_lookup_failed_when_no_plex_items(tmp_path):
    """Placement exists but plex_items row was wiped → drift
    classification rk_lookup_failed."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_placement(
        db, media_type="movie", tmdb_id=99, section_id="1",
    )
    # Wipe the plex_items row.
    with get_conn(db) as conn, transaction(conn):
        conn.execute("DELETE FROM plex_items WHERE rating_key = ?",
                     ("rk-99",))
    plex = MagicMock()
    findings = scan_plex_upload_placements(db, plex)
    assert len(findings) == 1
    assert findings[0]["drift_type"] == "rk_lookup_failed"
    assert findings[0]["rk"] is None
    plex.get_themes.assert_not_called()


def test_scan_no_plex_entries(tmp_path):
    """Plex returns 0 entries → no_plex_entries drift."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_placement(
        db, media_type="movie", tmdb_id=100, section_id="1",
    )
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 0, "Metadata": []}},
    }
    findings = scan_plex_upload_placements(db, plex)
    assert len(findings) == 1
    assert findings[0]["drift_type"] == "no_plex_entries"
    assert findings[0]["entries_count"] == 0


def test_scan_motif_hash_unknown_when_no_local_file(tmp_path):
    """No local_files row → motif_hash is None → drift type
    motif_hash_unknown (when Plex has entries)."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_placement(
        db, media_type="movie", tmdb_id=101, section_id="1",
    )
    # No local_files row inserted.
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 1, "Metadata": [
            {"ratingKey": "upload://themes/abc123",
             "selected": True},
        ]}},
    }
    findings = scan_plex_upload_placements(db, plex)
    assert findings[0]["drift_type"] == "motif_hash_unknown"
    assert findings[0]["motif_hash"] is None


def test_scan_ok_when_motif_entry_selected(tmp_path):
    """Motif's hash matches a selected entry → drift type ok."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_placement(
        db, media_type="movie", tmdb_id=102, section_id="1",
    )
    # Write a small file + record its path so motif_hash matches.
    audio = tmp_path / "theme.mp3"
    audio.write_bytes(b"\x00\x01\x02" * 1000)
    _seed_local_file(
        db, media_type="movie", tmdb_id=102, section_id="1",
        file_path=audio,
    )
    # Compute the actual hash so the mock returns the right rk.
    import hashlib
    expected = hashlib.sha1(audio.read_bytes()).hexdigest()
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 1, "Metadata": [
            {"ratingKey": f"upload://themes/{expected}",
             "selected": True},
        ]}},
    }
    findings = scan_plex_upload_placements(db, plex)
    assert findings[0]["drift_type"] == "ok"
    assert findings[0]["motif_entry_present"] is True
    assert findings[0]["motif_entry_selected"] is True


def test_scan_motif_not_selected(tmp_path):
    """Motif's entry present but Plex selected something else
    → drift type motif_not_selected."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_placement(
        db, media_type="movie", tmdb_id=103, section_id="1",
    )
    audio = tmp_path / "theme103.mp3"
    audio.write_bytes(b"motif theme bytes")
    _seed_local_file(
        db, media_type="movie", tmdb_id=103, section_id="1",
        file_path=audio,
    )
    import hashlib
    motif_h = hashlib.sha1(audio.read_bytes()).hexdigest()
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 2, "Metadata": [
            {"ratingKey": f"upload://themes/{motif_h}",
             "selected": False},
            {"ratingKey": "upload://themes/abc999",
             "selected": True},
        ]}},
    }
    findings = scan_plex_upload_placements(db, plex)
    assert findings[0]["drift_type"] == "motif_not_selected"


# v1.21.29: test_scan_rk_only_scopes_to_one_item removed alongside the
# dead rk_only param — no route passed it after the v1.21.24 background-op
# conversion (the scan always walks the full DB now).


def test_summarize_groups_by_drift_type():
    from app.core.orphan_scan import summarize
    findings = [
        {"drift_type": "ok"},
        {"drift_type": "ok"},
        {"drift_type": "motif_not_selected"},
        {"drift_type": "no_plex_entries"},
    ]
    s = summarize(findings)
    assert s == {"ok": 2, "motif_not_selected": 1,
                 "no_plex_entries": 1}


# ── Admin endpoint ───────────────────────────────────────────


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
    return client


AUTH = {"X-Authentik-Username": "testadmin"}


def test_endpoint_requires_auth(admin_client):
    r = admin_client.get("/api/admin/orphan-scan")
    assert r.status_code in (401, 403)


def test_endpoint_503_when_plex_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_ENABLED", "false")
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
    # v1.21.24: scan is now a background op started via POST /start.
    r = client.post("/api/admin/orphan-scan/start", headers=AUTH)
    assert r.status_code == 503


def test_endpoint_returns_summary_and_findings(
    admin_client, monkeypatch,
):
    """Happy path: returns ok=True with summary + findings list."""
    # Stub scan to return canned findings. v1.18.39 added a
    # themes_dir kwarg; accept-and-ignore here.
    # v1.21.24 added progress_cb; accept-and-ignore here.
    def fake_scan(db, plex, rk_only=None, themes_dir=None, progress_cb=None):
        return [
            {"media_type": "movie", "tmdb_id": 1,
             "section_id": "1", "rk": "rk-1",
             "drift_type": "ok",
             "details": "motif's entry selected",
             "motif_hash": "abc", "entries_count": 1,
             "motif_entry_present": True,
             "motif_entry_selected": True,
             "any_selected": True},
        ]

    import app.core.orphan_scan as os_mod
    monkeypatch.setattr(
        os_mod, "scan_plex_upload_placements", fake_scan,
    )
    # Stub PlexClient context manager so the endpoint doesn't
    # try real httpx.
    import app.core.plex as plex_mod
    monkeypatch.setattr(
        plex_mod, "PlexClient",
        lambda *a, **kw: MagicMock(
            __enter__=lambda self: self,
            __exit__=lambda self, *exc: None,
        ),
    )
    # v1.21.24: start the background scan, then poll /status until done.
    import time
    r = admin_client.post("/api/admin/orphan-scan/start", headers=AUTH)
    assert r.status_code == 200, r.text
    data = None
    for _ in range(100):
        s = admin_client.get(
            "/api/admin/orphan-scan/status", headers=AUTH,
        ).json()
        if s["status"] == "done":
            data = s
            break
        time.sleep(0.03)
    assert data is not None, "scan did not reach done"
    assert data["total"] == 1
    assert data["summary"] == {"ok": 1}
    assert len(data["findings"]) == 1
    assert data["findings"][0]["drift_type"] == "ok"
