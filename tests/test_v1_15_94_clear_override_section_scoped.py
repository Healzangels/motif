"""v1.15.94 — silent bug pass #2: clear_override section scoping + outcome stamp log.

Two unrelated silent-bug findings shipped together:

## #1 — api_clear_override unscoped (cross-section data loss)

`/api/items/{mt}/{id}/override` DELETE was the only override-touching
endpoint without a `section_id` query parameter. Pre-fix it deleted
EVERY section's override row for the title:

  DELETE FROM user_overrides WHERE media_type=? AND tmdb_id=?

On multi-section rows (standard + 4K editions with different
per-section overrides), clicking CLEAR URL on the 4K info card
would silently delete the standard section's override too. The
fix adds an optional `section_id` Query parameter that scopes
both the SELECT prior-state read + the DELETE. JS caller updated
to pass section_id when the dialog has section context. Title-
global fallback preserved for legacy callers without section
context.

Mirrors v1.12.72 / v1.13.54 scoping pattern used by every other
override-touching endpoint (api_set_override, api_unmanage_item,
api_accept_update, api_replace_with_themerrdb, etc.). This
endpoint was the lone outlier.

## #3 — place outcome stamp log.debug → log.warning

worker.py line ~1769 wrapped the place-outcome bookkeeping in
`try ... except Exception as e: log.debug(...)`. If the bookkeeping
UPDATE failed (DB lock, schema mismatch, etc.), it logged only at
DEBUG — invisible in default-INFO production logs.

The place itself isn't at stake; the failure means motif's
plex_items state stayed inconsistent with the place outcome.
On Unraid setups where plex_enum can't reach the folder, the
stale flag persists until manual intervention.

Bumped to `log.warning` so the failure surfaces in production
logs. The except stays — place correctness doesn't depend on
the stamp.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def app_client(tmp_path, monkeypatch):
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


def _seed_section(conn, section_id, title="Movies"):
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES (?, ?, 'movie', 1, 0, ?, ?, ?, ?)",
        (section_id, title, 1 if title.endswith("4K") else 0,
         f"movies-{section_id}", now, now),
    )


def _seed_theme(conn, tmdb_id):
    now = now_iso()
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "  upstream_source, youtube_url, "
        "  first_seen_sync_at, last_seen_sync_at) "
        "VALUES ('movie', ?, 'TestMovie', 2020, 'imdb', "
        "  'https://youtube.com/watch?v=tdb11111111', ?, ?)",
        (tmdb_id, now, now),
    )


def _seed_override(conn, tmdb_id, section_id, url):
    now = now_iso()
    conn.execute(
        "INSERT INTO user_overrides "
        "  (media_type, tmdb_id, youtube_url, set_at, set_by, "
        "   section_id) "
        "VALUES ('movie', ?, ?, ?, 'testadmin', ?)",
        (tmdb_id, url, now, section_id),
    )


# ── #1 — clear_override is now section-scoped ───────────────


def test_clear_override_scoped_deletes_only_target_section(app_client):
    """Multi-section row with overrides in both 'standard' and '4K'.
    DELETE with section_id='4K' must leave the standard override
    intact. Pre-v1.15.94 this would have deleted both."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1", "Movies")
        _seed_section(conn, "2", "Movies 4K")
        _seed_theme(conn, tmdb_id=100)
        _seed_override(conn, 100, "1",
                       "https://youtube.com/watch?v=standard111")
        _seed_override(conn, 100, "2",
                       "https://youtube.com/watch?v=fourk111111")
        conn.commit()

    r = client.delete(
        "/api/items/movie/100/override?section_id=2",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        remaining = conn.execute(
            "SELECT section_id, youtube_url FROM user_overrides "
            "WHERE tmdb_id=100"
        ).fetchall()
    assert len(remaining) == 1, (
        f"v1.15.94 regression: per-section DELETE wiped a sibling "
        f"section's override. Remaining rows: {remaining}"
    )
    assert remaining[0][0] == "1", (
        f"v1.15.94: the standard section's override (section_id='1') "
        f"must survive a section_id='2' DELETE. Got: {remaining}"
    )


def test_clear_override_unscoped_still_clears_all(app_client):
    """Legacy fan-out semantics: when no section_id is passed,
    every section's override gets deleted. Preserved for callers
    without per-section context."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_section(conn, "2")
        _seed_theme(conn, tmdb_id=200)
        _seed_override(conn, 200, "1", "https://yt.com/w?v=aaa11111111")
        _seed_override(conn, 200, "2", "https://yt.com/w?v=bbb11111111")
        conn.commit()

    r = client.delete(
        "/api/items/movie/200/override",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM user_overrides WHERE tmdb_id=200"
        ).fetchone()[0]
    assert count == 0, (
        f"v1.15.94: unscoped DELETE must still clear all sections "
        f"(legacy fan-out). Found {count} surviving rows."
    )


def test_clear_override_endpoint_signature_has_section_id_param():
    """Counter-guard: the section_id Query parameter must remain
    on the api_clear_override signature. A refactor that drops it
    re-introduces the cross-section data-loss bug."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_start = src.index("async def api_clear_override(")
    # Pull the signature until the closing paren of the function def.
    sig_end = src.index("):", fn_start) + 2
    signature = src[fn_start:sig_end]
    assert "section_id" in signature, (
        "v1.15.94: api_clear_override must accept a section_id "
        "parameter. Without it the DELETE wipes every section's "
        "override silently — the v1.15.94 bug class."
    )


# test_clear_override_js_caller_threads_section_id retired in
# v1.15.97. The JS path it pinned (`data-clear-override` button
# inside `openItemDialog`'s template) was discovered to be
# DOUBLY-DEAD: no template ever declared `<dialog id="item-dlg">`
# AND no external code rendered the `data-clear-override`
# attribute. v1.15.97 removed `openItemDialog` + `bindDialog`
# (~160 lines). The server-side fix above (api_clear_override
# taking optional section_id) remains as defensive API surface —
# any future CLI / external integration / re-added per-section
# CLEAR OVERRIDE UI can use it. See v1.15.97 commit + test
# file for the archaeology.


# ── #3 — place outcome stamp logs at warning ────────────────


def test_place_outcome_stamp_logs_at_warning_not_debug():
    """The except wrapper around the place-outcome bookkeeping
    UPDATE must log at warning level. log.debug is invisible in
    production (default INFO) — a swallowed stamp failure leaves
    motif's plex_items state stale until manual intervention."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # Anchor to the v1.15.91 stamp block (introduced w/ the
    # natural-key WHERE) then look at the except handler.
    anchor = src.index("# v1.11.24: stamp the place outcome")
    end = src.index('"place outcome stamp failed', anchor)
    span = src[anchor:end + 200]
    # The log call after "place outcome stamp failed:" must be
    # log.warning (or log.error), not log.debug.
    assert "log.warning(\"place outcome stamp failed:" in span, (
        "v1.15.94: place outcome stamp's except handler must log "
        "at warning level. log.debug hides production stamp "
        "failures and Unraid setups won't see plex_enum catch-up."
    )
    # Counter-guard: log.debug for this message must be gone.
    assert 'log.debug("place outcome stamp failed:' not in span, (
        "v1.15.94 regression: log.debug returned for the stamp "
        "exception. Switch back to log.warning."
    )
