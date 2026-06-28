"""v1.16.4 — manual-url with download_only=True must not enqueue
a place job.

the user on v1.16.3:

> // DOWNLOAD ONLY (Plex keeps serving) when checking this and
> allowing it to download only we see in the status bar it
> mention placing as well as the amber PL blinking. it doesn't
> place it but it animates as it were going to

## Root cause

Manual-url endpoint sent `payload = {"reason": "manual_url",
"force_place": not download_only}`. When download_only=True
this became `force_place=False`.

But the worker's post-download gate (worker.py:1445-1467)
treats `force_place` and `auto_place` as INDEPENDENT signals:

    auto_place = self.settings.auto_place_default  # typically True
    if "auto_place" in payload:
        auto_place = bool(payload["auto_place"])
    if payload.get("force_place"):
        force_place = True
        auto_place = True
    if auto_place:
        # enqueue place job

With download_only=True, payload had `force_place=False` (falsy
to .get) → no flip. `auto_place` stayed True from the global
default → a non-forcing place job was enqueued. The job then
ran, saw `plex_has_theme=1`, and skipped (log: "Skipped
placement: plex_has_theme") — but during its pending/running
window, the row's `job_in_flight='place'` painted the amber
PL pulse, misleading the user.

## Fix — `app/web/api.py:api_manual_url`

Switched the download_only branch from `force_place=False` to
`auto_place=False`. The worker's gate reads `auto_place=False`
from payload → no place job enqueued at all → no transient
place pulse on the row.

The download_only=False branch keeps `force_place=True`
(unchanged behavior).

## Tests
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso
from app.web.api import create_app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
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


def _seed_p_row(conn: sqlite3.Connection) -> str:
    """Set up a P-row: included anime section, a TDB-tracked
    themes record, a plex_items row with has_theme=1 (Plex
    serves its own theme) and a known rating_key."""
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES ('3', 'Anime', 'show', 1, 1, 0, 'anime', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, "
        "                    title_norm, year, "
        "                    upstream_source, youtube_url, "
        "                    first_seen_sync_at, last_seen_sync_at) "
        "VALUES (1, 'tv', 99999, 'A Show', 'a show', 2023, "
        "        'themoviedb', "
        "        'https://youtube.com/watch?v=tdb12345xyz', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, title_norm, year, "
        "  has_theme, local_theme_file, folder_path, "
        "  theme_id, first_seen_at, last_seen_at) "
        "VALUES ('rk-pshow', '3', 'show', 99999, 'A Show', "
        "        'a show', '2023', 1, 0, '/data/anime/A Show', "
        "        1, ?, ?)",
        (now, now),
    )
    conn.commit()
    return "rk-pshow"


# ── download_only=True: payload must encode auto_place=False ──────

def test_download_only_true_enqueues_download_with_auto_place_false(client):
    rk = _seed_p_row(sqlite3.connect(client.db))
    r = client.post(
        f"/api/plex_items/{rk}/manual-url",
        json={
            "youtube_url": "https://www.youtube.com/watch?v=newuser1234",
            "download_only": True,
        },
    )
    assert r.status_code == 200, r.text
    # Verify the download job's payload encodes the fix.
    conn = sqlite3.connect(client.db)
    conn.row_factory = sqlite3.Row
    job = conn.execute(
        "SELECT job_type, payload FROM jobs "
        "WHERE media_type='tv' AND tmdb_id=99999 "
        "  AND job_type='download'"
    ).fetchone()
    assert job is not None, (
        "v1.16.4: manual-url must still enqueue a download job "
        "(download_only suppresses placement, not downloading)."
    )
    payload = json.loads(job["payload"])
    assert payload.get("auto_place") is False, (
        "v1.16.4: download_only=True must set auto_place=False in "
        "the payload — that's what the worker reads to skip the "
        "place-enqueue branch (worker.py:1445-1467). Pre-fix the "
        "payload only had force_place=False which the worker's "
        "gate doesn't honor."
    )
    # No place job should exist yet (worker hasn't run; even if
    # it did, auto_place=False would block the enqueue).
    place_job = conn.execute(
        "SELECT id FROM jobs "
        "WHERE media_type='tv' AND tmdb_id=99999 "
        "  AND job_type='place'"
    ).fetchone()
    assert place_job is None
    conn.close()


def test_download_only_true_payload_does_not_set_force_place(client):
    """Belt-and-suspenders: confirm force_place is NOT set when
    download_only=True. The worker treats force_place=True as a
    flip to auto_place=True, which would defeat the auto_place=
    False guard."""
    rk = _seed_p_row(sqlite3.connect(client.db))
    r = client.post(
        f"/api/plex_items/{rk}/manual-url",
        json={
            "youtube_url": "https://www.youtube.com/watch?v=newuser1234",
            "download_only": True,
        },
    )
    assert r.status_code == 200
    conn = sqlite3.connect(client.db)
    conn.row_factory = sqlite3.Row
    job = conn.execute(
        "SELECT payload FROM jobs "
        "WHERE media_type='tv' AND tmdb_id=99999 "
        "  AND job_type='download'"
    ).fetchone()
    payload = json.loads(job["payload"])
    assert "force_place" not in payload, (
        "v1.16.4: when download_only=True, force_place MUST NOT "
        "be present in the payload — its presence with True would "
        "flip auto_place back to True via the worker's gate."
    )
    conn.close()


# ── download_only=False: legacy force_place=True path preserved ───

def test_download_only_false_keeps_force_place_true(client):
    """Non-download_only path is the canonical SET URL flow:
    "replace whatever's there with my URL." Worker still needs
    force_place=True so the place job bypasses skip_if_plex_has_
    theme. v1.16.4 must not regress this."""
    rk = _seed_p_row(sqlite3.connect(client.db))
    r = client.post(
        f"/api/plex_items/{rk}/manual-url",
        json={
            "youtube_url": "https://www.youtube.com/watch?v=newuser1234",
            # download_only omitted → defaults to False
        },
    )
    assert r.status_code == 200
    conn = sqlite3.connect(client.db)
    conn.row_factory = sqlite3.Row
    job = conn.execute(
        "SELECT payload FROM jobs "
        "WHERE media_type='tv' AND tmdb_id=99999 "
        "  AND job_type='download'"
    ).fetchone()
    payload = json.loads(job["payload"])
    assert payload.get("force_place") is True, (
        "v1.16.4: download_only=False (default) must still set "
        "force_place=True — that's the canonical SET URL contract "
        "since v1.15.75."
    )
    # auto_place should NOT be set on the false path — force_place
    # True flips auto_place to True via the worker's gate.
    assert "auto_place" not in payload
    conn.close()


def test_download_only_explicit_false_keeps_force_place_true(client):
    """Same contract when the client sends {download_only: false}
    explicitly (the JS dialog's checkbox unchecked path)."""
    rk = _seed_p_row(sqlite3.connect(client.db))
    r = client.post(
        f"/api/plex_items/{rk}/manual-url",
        json={
            "youtube_url": "https://www.youtube.com/watch?v=newuser1234",
            "download_only": False,
        },
    )
    assert r.status_code == 200
    conn = sqlite3.connect(client.db)
    conn.row_factory = sqlite3.Row
    job = conn.execute(
        "SELECT payload FROM jobs "
        "WHERE media_type='tv' AND tmdb_id=99999 "
        "  AND job_type='download'"
    ).fetchone()
    payload = json.loads(job["payload"])
    assert payload.get("force_place") is True
    conn.close()


# ── reads the api.py block — pin the comment ANCHOR -----

def test_api_py_has_v1_16_4_marker_explaining_fix():
    """Pin the explanatory v1.16.4 marker in api_manual_url so a
    future refactor that drops force_place vs auto_place
    semantics surfaces the rationale to the next reader."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    # api_manual_url is large (URL canonicalization, plex_items
    # lookup, prior-URL capture, override upsert, then the
    # download enqueue at the bottom). Just anchor on the next
    # function definition to bound the search rather than guess
    # at a char-window size.
    fn_idx = api.index("async def api_manual_url")
    next_fn_idx = api.index("\n    @app.", fn_idx + 1)
    block = api[fn_idx:next_fn_idx]
    assert "v1.16.4" in block
    assert "auto_place" in block
    assert "download_only" in block
