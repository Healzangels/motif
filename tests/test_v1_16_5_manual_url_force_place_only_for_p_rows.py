"""v1.16.5 — manual-url force_place gate respects global auto_place
on non-P rows.

the user on v1.16.4:

> this will still respect the the setting for autoplace on or off
> in settings right? If autoplace is unchecked I don't want this
> to force it unintentionally this is only for the P rows we want
> to get into a PS link state via a url or upload

## What v1.16.4 got wrong

The non-download_only branch always set `force_place=True`. The
worker's gate (worker.py:1451-1453) treats force_place=True as
"flip auto_place to True regardless of global setting." That
overrode the user's global auto_place preference on every SET
URL action, even for non-P rows where the force isn't needed
(there's no Plex theme to override on a "-" / T / U / A / M row).

## What v1.16.5 fixes

Three branches now:

  download_only=True       → payload["auto_place"] = False
    (explicit opt-out; honored regardless of P-row state)

  P-row + !download_only   → payload["force_place"] = True
    (canonical "replace Plex's theme" intent — needs force to
    bypass plex_has_theme skip + this IS an intentional global-
    setting override since the user is taking the explicit
    replace action)

  non-P row + !download_only → neutral payload
    (worker honors settings.auto_place_default — if globally
    off, no auto-place; if on, place runs normally without
    the force flag)

P-row classification: `pi.has_theme = 1` in plex_items. Server-
side check, no client trust required.

## Tests
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    app = create_app(settings)
    c = TestClient(app)
    c.headers["X-Authentik-Username"] = "testadmin"
    c.db = db  # type: ignore[attr-defined]
    yield c


def _seed_row(conn: sqlite3.Connection, *,
              rk: str, has_theme: int, tmdb_id: int = 99999) -> None:
    """Set up a plex_items row at given has_theme + linked
    themes record. has_theme=1 means P row; =0 means non-P."""
    now = now_iso()
    # Idempotent section + themes seeding for multi-call usage.
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES ('3', 'Anime', 'show', 1, 1, 0, 'anime', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, "
        "                              title, title_norm, year, "
        "                              upstream_source, youtube_url, "
        "                              first_seen_sync_at, "
        "                              last_seen_sync_at) "
        "VALUES (?, 'tv', ?, 'A Show', 'a show', 2023, "
        "        'themoviedb', "
        "        'https://youtube.com/watch?v=tdb12345xyz', ?, ?)",
        (tmdb_id, tmdb_id, now, now),
    )
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, title_norm, year, "
        "  has_theme, local_theme_file, folder_path, "
        "  theme_id, first_seen_at, last_seen_at) "
        "VALUES (?, '3', 'show', ?, 'A Show', "
        "        'a show', '2023', ?, 0, '/data/anime/A Show', "
        "        ?, ?, ?)",
        (rk, tmdb_id, has_theme, tmdb_id, now, now),
    )
    conn.commit()


# ── non-P row + !download_only → neutral payload (the regression) ─

def test_non_p_row_no_download_only_uses_neutral_payload(client):
    """The regression target: pre-v1.16.5, this would have set
    force_place=True and OVERRIDDEN the global auto_place
    setting. With the fix, the payload stays neutral and the
    worker honors the global preference."""
    conn = sqlite3.connect(client.db)
    _seed_row(conn, rk="rk-nonp", has_theme=0, tmdb_id=11111)
    conn.close()
    r = client.post(
        "/api/plex_items/rk-nonp/manual-url",
        json={
            "youtube_url": "https://www.youtube.com/watch?v=user11111111",
        },
    )
    assert r.status_code == 200, r.text
    conn = sqlite3.connect(client.db)
    conn.row_factory = sqlite3.Row
    job = conn.execute(
        "SELECT payload FROM jobs "
        "WHERE media_type='tv' AND tmdb_id=11111 "
        "  AND job_type='download'"
    ).fetchone()
    payload = json.loads(job["payload"])
    conn.close()
    assert "force_place" not in payload, (
        "v1.16.5: non-P row SET URL must NOT set force_place. "
        "Pre-fix this overrode the user's global auto_place "
        "preference unconditionally. There's no Plex theme to "
        "override on a non-P row, so the force is redundant + "
        "the override is unintentional."
    )
    assert "auto_place" not in payload, (
        "v1.16.5: non-P row + !download_only must leave the "
        "payload neutral. Setting auto_place=False would block "
        "even users who DO have it globally enabled; setting "
        "auto_place=True would override users who have it off. "
        "Neutral = worker reads settings.auto_place_default."
    )


# ── P row + !download_only → force_place=True (canonical replace) ─

def test_p_row_no_download_only_still_sets_force_place_true(client):
    """The canonical SET URL on a P row IS the user explicitly
    saying 'replace Plex's theme with my URL.' That intent DOES
    warrant overriding global auto_place (force_place=True flips
    auto_place=True in the worker's gate). Pin this behavior."""
    conn = sqlite3.connect(client.db)
    _seed_row(conn, rk="rk-p", has_theme=1, tmdb_id=22222)
    conn.close()
    r = client.post(
        "/api/plex_items/rk-p/manual-url",
        json={
            "youtube_url": "https://www.youtube.com/watch?v=user22222222",
        },
    )
    assert r.status_code == 200, r.text
    conn = sqlite3.connect(client.db)
    conn.row_factory = sqlite3.Row
    job = conn.execute(
        "SELECT payload FROM jobs "
        "WHERE media_type='tv' AND tmdb_id=22222 "
        "  AND job_type='download'"
    ).fetchone()
    payload = json.loads(job["payload"])
    conn.close()
    assert payload.get("force_place") is True, (
        "v1.16.5: P row + !download_only must STILL set "
        "force_place=True. The user is explicitly replacing "
        "Plex's theme; without force, the place job would skip "
        "on plex_has_theme and the URL change would have no "
        "visible effect."
    )


# ── P row + download_only → auto_place=False (v1.16.4 contract) ───

def test_p_row_download_only_sets_auto_place_false(client):
    """The v1.16.4 contract — preserved unchanged in v1.16.5."""
    conn = sqlite3.connect(client.db)
    _seed_row(conn, rk="rk-p-do", has_theme=1, tmdb_id=33333)
    conn.close()
    r = client.post(
        "/api/plex_items/rk-p-do/manual-url",
        json={
            "youtube_url": "https://www.youtube.com/watch?v=user33333333",
            "download_only": True,
        },
    )
    assert r.status_code == 200
    conn = sqlite3.connect(client.db)
    conn.row_factory = sqlite3.Row
    job = conn.execute(
        "SELECT payload FROM jobs "
        "WHERE media_type='tv' AND tmdb_id=33333 "
        "  AND job_type='download'"
    ).fetchone()
    payload = json.loads(job["payload"])
    conn.close()
    assert payload.get("auto_place") is False
    assert "force_place" not in payload


# ── non-P row + download_only=True → auto_place=False (edge case) ─

def test_non_p_row_explicit_download_only_still_honored(client):
    """The UI doesn't expose the download_only checkbox on non-P
    rows (it's gated to srcLetter==='P'), but if a client sends
    download_only=True via direct API call on a non-P row, honor
    it — set auto_place=False. Defensible interpretation: the
    user explicitly asked to skip placement."""
    conn = sqlite3.connect(client.db)
    _seed_row(conn, rk="rk-nonp-do", has_theme=0, tmdb_id=44444)
    conn.close()
    r = client.post(
        "/api/plex_items/rk-nonp-do/manual-url",
        json={
            "youtube_url": "https://www.youtube.com/watch?v=user44444444",
            "download_only": True,
        },
    )
    assert r.status_code == 200
    conn = sqlite3.connect(client.db)
    conn.row_factory = sqlite3.Row
    job = conn.execute(
        "SELECT payload FROM jobs "
        "WHERE media_type='tv' AND tmdb_id=44444 "
        "  AND job_type='download'"
    ).fetchone()
    payload = json.loads(job["payload"])
    conn.close()
    assert payload.get("auto_place") is False, (
        "v1.16.5: download_only=True must be honored regardless "
        "of P-row state. UI doesn't expose this on non-P rows, "
        "but if a direct API call sends it, the explicit opt-"
        "out should win."
    )


# ── code-presence guard ──────────────────────────────────────────

def test_api_py_has_v1_16_5_marker():
    """Pin the explanatory marker so a future refactor that
    drops the is_p_row gate surfaces the rationale."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = api.index("async def api_manual_url")
    next_fn_idx = api.index("\n    @app.", fn_idx + 1)
    block = api[fn_idx:next_fn_idx]
    assert "v1.16.5" in block
    assert "is_p_row" in block
    assert 'pi["has_theme"]' in block, (
        "v1.16.5: the P-row classification must read pi.has_"
        "theme from the existing plex_items row in scope — no "
        "extra DB query, no client trust."
    )
