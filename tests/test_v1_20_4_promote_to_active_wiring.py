"""v1.20.4 — PROMOTE TO ACTIVE / MARK AS BACKUP click handlers wired
to the in-scope element (fix the undeclared-`body` ReferenceError).

the user's repro: Being John Malkovich (movie/492), an adopted-sidecar
row in BK state (source_kind=adopt, last_place_attempt_reason=
'backup_only', no placement, Plex serving its own theme). The INFO
card rendered "✓ BACKUP READY — DEFERRING TO PLEX" + // PROMOTE TO
ACTIVE, but clicking did nothing — no request in the docker logs.

Root cause: `_wireIntentFlip` (inside `hydrateRecoveryOptions`)
referenced `body` — but `body` is NOT in that function's scope. It's
a local of the *sibling* `openInfoDialog`. app.js runs under
`'use strict'`, so reading the undeclared `body` throws a
ReferenceError the instant `_wireIntentFlip(...)` is called at wire
time. That:
  - never bound the PROMOTE / MARK click handlers, AND
  - aborted hydrateRecoveryOptions before the recovery-option-btn
    dispatcher (REDOWNLOAD / ADOPT / LET PLEX SERVE) below it could
    bind.
The card looked live because its HTML was set via innerHTML BEFORE
the throw.

v1.20.4 fix:
  - `_wireIntentFlip` looks up the button via `section` (the in-scope
    recovery container that holds the buttons) instead of `body`.
  - `_postIntent` scopes the POST with the `sectionId` PARAMETER —
    the exact value the recovery-options GET used — instead of the
    never-set `body.dataset.sectionId`, so the PROMOTE POST hits the
    same section the BK state was detected in (an empty section_id
    409'd 'nothing to promote').

The server endpoint itself was always correct (behaviorally covered
by test_v1_19_35_promote_to_active_on_bk_rows.py) — the request just
never left the browser.
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

from fastapi.testclient import TestClient

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _hydrate_recovery_fn() -> str:
    """Extract the full hydrateRecoveryOptions function text (from its
    declaration to the next top-level indent-2 function)."""
    start = APP_JS.index("async function hydrateRecoveryOptions(")
    m = re.search(r"\n  (?:async )?function ", APP_JS[start + 1:])
    end = start + 1 + m.start() if m else len(APP_JS)
    return APP_JS[start:end]


# ── scope-hygiene lint: no out-of-scope `body` in this function ──


def test_no_undeclared_body_member_access_in_hydrate_recovery():
    """`body` is undeclared in hydrateRecoveryOptions' scope — any
    `body.<member>` access throws under strict mode. Guard against
    re-introduction (the v1.20.4 regression class)."""
    fn = _hydrate_recovery_fn()
    # Strip `//` line-comments first so doc mentions of the old
    # `body.querySelector` / `body.dataset.sectionId` (including this
    # fix's own breadcrumbs) don't false-positive. Then assert no
    # `body.` member access survives in executable code. (The function
    # declares no local `body`; openInfoDialog's is a sibling.)
    # `tbody` is excluded by the negative-lookbehind.
    code = "\n".join(line.split("//", 1)[0] for line in fn.splitlines())
    hits = re.findall(r"(?<![A-Za-z0-9_.])body\.", code)
    assert not hits, (
        f"v1.20.4: hydrateRecoveryOptions references an out-of-scope "
        f"`body.` ({len(hits)} hit(s)) — strict-mode ReferenceError. "
        f"Use `section` / the `sectionId` parameter instead."
    )


def test_wire_intent_flip_uses_section_lookup():
    fn = _hydrate_recovery_fn()
    assert "section.querySelector(selector)?.addEventListener" in fn, (
        "v1.20.4: _wireIntentFlip must bind via the in-scope `section` "
        "container, not the undeclared `body`"
    )


def test_post_intent_scopes_with_section_parameter():
    fn = _hydrate_recovery_fn()
    assert "const sec = sectionId || ''" in fn, (
        "v1.20.4: _postIntent must scope the POST with the sectionId "
        "PARAMETER (parity with the recovery-options GET), not the "
        "never-set body.dataset.sectionId"
    )


# ── behavioral: PROMOTE on an ADOPT-source BK row (the user's case) ─


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_adopt_bk_row(conn, *, tmdb_id, section_id="1"):
    """Mirror Being John Malkovich: an adopted-sidecar row in BK state
    — source_kind='adopt', last_place_attempt_reason='backup_only',
    no placement, Plex serving its own theme."""
    now = "2026-05-29T00:00:00"
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
        "  is_anime, is_4k, themes_subdir, included, discovered_at, "
        "  last_seen_at) VALUES (?,'Movies','movie',0,0,'movies',1,?,?)",
        (section_id, now, now))
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, "
        "  upstream_source, last_seen_sync_at, first_seen_sync_at, "
        "  youtube_url) VALUES (?,'movie',?,'Being John Malkovich','imdb',"
        "  ?,?,'https://yt/orig')",
        (tmdb_id, tmdb_id, now, now))
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        "  theme_id, guid_imdb, guid_tmdb, title, year, has_theme, "
        "  local_theme_file, folder_path, plex_independent_theme, "
        "  plex_theme_verified_ok, first_seen_at, last_seen_at) "
        "VALUES (?,?,'movie',?,'tt0120601',?,'Being John Malkovich',1999,"
        "  1,0,'/data/movies/BJM',0,1,?,?)",
        (f"rk{tmdb_id}", section_id, tmdb_id, tmdb_id, now, now))
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "  file_path, source_kind, source_video_id, downloaded_at, "
        "  provenance, last_place_attempt_reason) "
        "VALUES ('movie',?,?,'movies/BJM/theme.mp3','adopt','vid', "
        "  ?,'manual','backup_only')",
        (tmdb_id, section_id, now))


def test_recovery_card_renders_promote_for_adopt_bk_row(admin_client):
    """The card must surface backup_state + a synthesized
    intent='backup' override for an adopt-source BK row so the JS
    renders PROMOTE TO ACTIVE."""
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _seed_adopt_bk_row(conn, tmdb_id=492)
        conn.commit()
    r = client.get("/api/items/movie/492/recovery-options?section_id=1",
                   headers=AUTH)
    assert r.status_code == 200, r.text
    d = r.json()
    assert d.get("backup_state") is True
    assert (d.get("override") or {}).get("intent") == "backup"
    assert (d.get("override") or {}).get("source_kind") == "adopt"


def test_promote_on_adopt_bk_row_queues_force_place(admin_client):
    """The exact action that 'did nothing' in the UI: PROMOTE TO
    ACTIVE → POST /intent {replace} with the row's section. The
    endpoint must enqueue a force-place job (proving the request,
    once the client actually sends it, works end-to-end)."""
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _seed_adopt_bk_row(conn, tmdb_id=492)
        conn.commit()
    r = client.post("/api/items/movie/492/intent?section_id=1",
                    headers=AUTH, json={"intent": "replace"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("ok") is True and body.get("bk_no_override") is True
    with sqlite3.connect(db) as conn:
        jobs = conn.execute(
            "SELECT payload FROM jobs WHERE job_type='place' "
            "AND media_type='movie' AND tmdb_id=492").fetchall()
    assert len(jobs) == 1, f"expected one force-place job, got {jobs}"
    payload = json.loads(jobs[0][0])
    assert payload.get("force_place") is True
    assert payload.get("reason") == "promote_bk_no_override"


def test_promote_with_empty_section_409s_proving_parity_matters(admin_client):
    """Why the sectionId-parameter fix matters: the BK row lives at
    section '1'. A PROMOTE POST with NO section_id (the pre-fix
    behavior — body.dataset.sectionId was never set) looks for the
    backup_only row at section '' and 409s 'nothing to promote'.
    This is the silent failure the user hit."""
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _seed_adopt_bk_row(conn, tmdb_id=492)
        conn.commit()
    r = client.post("/api/items/movie/492/intent",  # no section_id
                    headers=AUTH, json={"intent": "replace"})
    assert r.status_code == 409, (
        f"empty section_id must 409 (the pre-fix silent failure); "
        f"got {r.status_code} {r.text}"
    )


def test_v1_20_4_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
