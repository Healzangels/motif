"""v1.18.81 — the v1.18.77/.78 phantom-fix root cause.

v1.18.77 added `user_overrides.intent` and the frontend branches
that key on `data.override.intent === 'backup'` to render the
"✓ BACKUP READY — DEFERRING TO PLEX" section title + the
// PROMOTE TO ACTIVE button. v1.18.78 fixed the branch ordering
when the conditional itself was wrong. But neither tag's tests
ever exercised the actual API → frontend data flow — they only
asserted the JS source-text shape of the conditional.

The real bug: `api_recovery_options` (the endpoint that powers
the section the banner + button render in) never returned
`override` in its response payload. The frontend's
`data.override.intent` access was always reading an undefined
field. The BACKUP banner + button silently never rendered on
real installs despite v1.18.77's DB writes being correct.

Class-9 contract-drift sub-pattern: a test that pins source-
text shape without exercising the data pipeline through that
shape is a phantom guard. the user's repro on the 1941 row
(intent='backup' confirmed in user_overrides via direct DB
query; API response showed `override: null`) closed the loop.

## What's pinned

- `api_recovery_options` returns the per-section override with
  the `intent` field surfaced.
- Section scoping mirrors api_item: per-section first, '' global
  fallback, any-section LIMIT 1 only when section_id is omitted.
- SET URL audit details record `old_intent` + `new_intent` so
  future provenance shows the intent transition that the SET URL
  requested (pre-v1.18.81 this was unrecoverable post-fact).
- CSS .btn has `flex-shrink: 0` + `white-space: nowrap` so long
  status text in `.form-actions` can't squeeze button labels
  into wrapping (the user's "buttons increase a very large size"
  flash on the // MANUAL THEME URL dialog success state).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent


# ── Fixtures ─────────────────────────────────────────────────


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """Same shape as v1.14.59's app_client fixture — fresh DB,
    forward-auth enabled, admin user seeded."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))

    from app.config import Settings
    settings = Settings(config_dir=tmp_path,
                        data_dir=tmp_path / "data")

    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")

    from app.web.api import create_app
    app = create_app(settings)
    client = TestClient(app)
    return client, db


AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_section(conn, section_id="1"):
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES (?, 'Movies', 'movie', 1, 0, 0, ?, ?, ?)",
        (section_id, f"movies-{section_id}", now, now),
    )


def _seed_theme(conn, tmdb_id=11519, failure_kind="video_removed"):
    now = now_iso()
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, youtube_url, "
        "                    failure_kind, failure_message, "
        "                    failure_at, "
        "                    first_seen_sync_at, last_seen_sync_at) "
        "VALUES ('movie', ?, '1941', 1979, 'imdb', "
        "        'https://www.youtube.com/watch?v=PFaoGxJl7Xo', "
        "        ?, 'Track or video was removed', ?, ?, ?)",
        (tmdb_id, failure_kind, now, now, now),
    )


def _seed_plex_item(conn, rating_key="461824", section_id="1",
                    tmdb_id=11519, has_theme=1):
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, year, has_theme, "
        "  local_theme_file, folder_path, plex_independent_theme, "
        "  first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'movie', ?, '1941', 1979, ?, "
        "  0, '/data/movies/1941 (1979)', 0, ?, ?)",
        (rating_key, section_id, str(tmdb_id), has_theme, now, now),
    )


def _seed_override(conn, *, intent="backup", section_id="1",
                   tmdb_id=11519, url="https://www.youtube.com/watch?v=GHHfCIMEjOA"):
    now = now_iso()
    conn.execute(
        "INSERT INTO user_overrides (media_type, tmdb_id, "
        "  youtube_url, set_at, set_by, note, "
        "  section_id, intent) "
        "VALUES ('movie', ?, ?, ?, 'admin', '', ?, ?)",
        (tmdb_id, url, now, section_id, intent),
    )


# ── 1. recovery-options surfaces override.intent ─────────────


def test_recovery_options_returns_override_with_intent_backup(app_client):
    """v1.18.81: this is the test v1.18.77 should have shipped.
    Confirms api_recovery_options returns the section-scoped
    override row INCLUDING the intent field so the frontend's
    `data.override.intent === 'backup'` check can fire.

    Pre-v1.18.81 the response never contained `override`; the
    frontend was reading `undefined.intent` (silently coerced
    to null), so the BACKUP banner + // PROMOTE TO ACTIVE
    button never rendered."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn)
        _seed_plex_item(conn)
        _seed_override(conn, intent="backup")
        conn.commit()

    r = client.get(
        "/api/items/movie/11519/recovery-options?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "override" in body, (
        "v1.18.81: recovery-options response must include "
        "`override` key (None when no override, dict otherwise). "
        "Pre-fix this key was absent → data.override on the "
        "frontend was always undefined."
    )
    assert body["override"] is not None, (
        "v1.18.81: override exists in DB; endpoint must surface it"
    )
    assert body["override"]["intent"] == "backup"


def test_recovery_options_returns_override_with_intent_replace(app_client):
    """Mirror test — intent='replace' must also surface so the
    frontend can render // MARK AS BACKUP for the
    `overrideIntent === 'replace'` branch."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn)
        _seed_plex_item(conn)
        _seed_override(conn, intent="replace")
        conn.commit()

    r = client.get(
        "/api/items/movie/11519/recovery-options?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["override"] is not None
    assert body["override"]["intent"] == "replace"


def test_recovery_options_returns_null_override_when_none_exists(app_client):
    """Sanity: no user_overrides row → override is null (not
    missing). The frontend's `(data.override && data.override.
    intent) || null` chain handles both shapes but the API
    contract should be explicit."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn)
        _seed_plex_item(conn)
        conn.commit()

    r = client.get(
        "/api/items/movie/11519/recovery-options?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200
    body = r.json()
    assert "override" in body
    assert body["override"] is None


# ── 2. Section-scoped lookup mirrors api_item ────────────────


def test_recovery_options_prefers_section_scoped_override(app_client):
    """Two overrides — one per-section, one global '' — must
    return the per-section row when section_id matches.
    Mirrors api_item:11772-11796 scoping. Pre-v1.18.77 cross-
    section bleed would have leaked a sibling section's intent
    into this row's INFO card."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn)
        _seed_plex_item(conn, section_id="1")
        # Global '' row with replace intent
        _seed_override(conn, intent="replace", section_id="")
        # Section-1 row with backup intent — should win
        _seed_override(conn, intent="backup", section_id="1")
        conn.commit()

    r = client.get(
        "/api/items/movie/11519/recovery-options?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["override"]["intent"] == "backup", (
        "v1.18.81: section-scoped lookup must prefer the matching "
        "section's row over the '' global"
    )
    assert body["override"]["section_id"] == "1"


def test_recovery_options_falls_back_to_global_override(app_client):
    """When no per-section override exists, the '' global is
    surfaced — same fallback as api_item."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn)
        _seed_plex_item(conn, section_id="1")
        # Only a global '' row — no section-1 row
        _seed_override(conn, intent="backup", section_id="")
        conn.commit()

    r = client.get(
        "/api/items/movie/11519/recovery-options?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["override"] is not None
    assert body["override"]["intent"] == "backup"
    assert body["override"]["section_id"] == ""


# ── 3. SET URL audit records intent ──────────────────────────


def test_set_url_audit_records_new_intent(app_client):
    """v1.18.81: SET URL with download_only=true must record
    new_intent='backup' in the audit_log details. Pre-fix the
    audit only carried old_url/new_url/rating_key — there was
    NO way post-fact to confirm what intent a SET URL had
    requested. That provenance gap is what made the v1.18.81
    root-cause analysis hard."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn)
        _seed_plex_item(conn)
        conn.commit()

    r = client.post(
        "/api/plex_items/461824/manual-url",
        headers=AUTH,
        json={
            "youtube_url": "https://www.youtube.com/watch?v=GHHfCIMEjOA",
            "download_only": True,
        },
    )
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        audit_rows = conn.execute(
            "SELECT * FROM audit_events "
            "WHERE action = 'set_url' AND tmdb_id = 11519 "
            "ORDER BY id DESC LIMIT 1"
        ).fetchall()
    assert len(audit_rows) == 1, (
        "v1.18.81: SET URL must record an audit_log row"
    )
    details = json.loads(audit_rows[0]["details"])
    assert "new_intent" in details, (
        "v1.18.81: audit details must include new_intent so "
        "provenance shows what KEEP AS BACKUP requested"
    )
    assert details["new_intent"] == "backup"
    # First SET URL — no prior intent
    assert details.get("old_intent") is None


def test_set_url_audit_records_intent_transition(app_client):
    """A second SET URL on the same row must record the
    transition (old_intent → new_intent) so a future audit
    sweep can see e.g. "the user flipped replace → backup at
    8:09 PM" from the audit log alone."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn)
        _seed_plex_item(conn)
        conn.commit()

    # First SET URL — intent='replace' (no checkbox).
    r1 = client.post(
        "/api/plex_items/461824/manual-url",
        headers=AUTH,
        json={
            "youtube_url": "https://www.youtube.com/watch?v=GHHfCIMEjOA",
        },
    )
    assert r1.status_code == 200, r1.text
    # Second SET URL — same URL but flipped to backup.
    r2 = client.post(
        "/api/plex_items/461824/manual-url",
        headers=AUTH,
        json={
            "youtube_url": "https://www.youtube.com/watch?v=GHHfCIMEjOA",
            "download_only": True,
        },
    )
    assert r2.status_code == 200, r2.text

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM audit_events "
            "WHERE action = 'set_url' AND tmdb_id = 11519 "
            "ORDER BY id DESC LIMIT 1"
        ).fetchall()
    assert len(rows) == 1
    details = json.loads(rows[0]["details"])
    assert details["old_intent"] == "replace"
    assert details["new_intent"] == "backup"


# ── 4. Button flash CSS guard ────────────────────────────────


def test_btn_has_flex_shrink_zero_for_form_actions_layout():
    """v1.18.81: .btn must keep its natural width inside flex
    containers — otherwise long sibling status text squeezes
    button labels into wrapping ("// SAVE & DOWNLOAD" became
    two-line for the 700ms before the // MANUAL THEME URL
    dialog closed). Both `flex-shrink: 0` and `white-space:
    nowrap` are needed: shrink keeps the box, nowrap keeps the
    text on one line if the parent is too narrow."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    # Find the canonical `.btn {` rule (not .btn-tiny / .btn-warn).
    idx = css.index(".btn {")
    # The opening rule body extends until the next `}`.
    end = css.index("}", idx)
    rule = css[idx:end]
    assert "flex-shrink: 0" in rule, (
        "v1.18.81: .btn must have flex-shrink: 0 so long status "
        "text can't squeeze button labels"
    )
    assert "white-space: nowrap" in rule, (
        "v1.18.81: .btn must have white-space: nowrap so squeezed "
        "button text doesn't wrap mid-label"
    )
