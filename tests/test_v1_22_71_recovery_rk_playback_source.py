"""v1.22.71 (audit round 2, Batch B #7) — recovery-card rk threading +
INFO playback-source dead fields.

(1) api_recovery_options resolved the "representative rating_key" (the
rk SET URL / UPLOAD MP3 act through) via an UNSCOPED LIMIT 1 over
plex_items — on a multi-section title the card could hand the buttons
a sibling section's (or edition's) rk; and the JS never sent the
clicked row's rk even though openInfoDialog had it in hand. Now the
endpoint accepts ?rating_key= (validated against the title) with a
section-scoped fallback, then the legacy unscoped pick; openInfoDialog
threads ratingKey → hydrateRecoveryOptions (as rowRk — NOT ratingKey,
whose name the body derives from data.rating_key; the v1.21.88
duplicate-param class).

(2) The INFO card's "playback source" line read
data.theme.plex_independent_theme (a plex_items column — never on the
themes dict) and data.is_plex_agent (never returned by api_item): both
always undefined, so every pure-P row showed "(none — row has no theme
staged)" instead of "Plex serves its own theme". api_item now returns
a top-level plex_independent_theme (rk → section → title resolution)
and the JS reads that.

Behavioral per the v1.18.81 phantom-fix rule: the backend→frontend
data flow is exercised at the endpoints, not just pinned in JS source.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from test_v1_14_59_recovery_options_behavioral import (  # noqa: F401
    app_client, _seed_section, _seed_theme, _seed_plex_item)

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


# ── (2) api_item: top-level plex_independent_theme ───────────


def test_api_item_returns_plex_independent_theme_rk_first(app_client):
    """Two rks on one title: each rk's card reads ITS OWN flag."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=100)
        _seed_plex_item(conn, rating_key="rk-p", tmdb_id=100,
                        plex_independent_theme=1)
        _seed_plex_item(conn, rating_key="rk-np", tmdb_id=100,
                        plex_independent_theme=0)
        conn.commit()
    r = client.get("/api/items/movie/100?section_id=1&rating_key=rk-p",
                   headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["plex_independent_theme"] == 1
    r = client.get("/api/items/movie/100?section_id=1&rating_key=rk-np",
                   headers=AUTH)
    assert r.json()["plex_independent_theme"] == 0


def test_api_item_title_fallback_without_rk(app_client):
    """Legacy callers (no rk, no section) still get the title-global
    answer instead of nothing."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=100)
        _seed_plex_item(conn, rating_key="rk-p", tmdb_id=100,
                        plex_independent_theme=1)
        conn.commit()
    r = client.get("/api/items/movie/100", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["plex_independent_theme"] == 1


# ── (1) recovery-options rk threading ────────────────────────


def _seed_two_sections_two_rks(db):
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_section(conn, "2", title="Movies 4K")
        _seed_theme(conn, tmdb_id=100)
        _seed_plex_item(conn, rating_key="rk-std", section_id="1",
                        tmdb_id=100)
        _seed_plex_item(conn, rating_key="rk-4k", section_id="2",
                        tmdb_id=100)
        conn.commit()


def test_recovery_options_honors_clicked_rk(app_client):
    client, db = app_client
    _seed_two_sections_two_rks(db)
    r = client.get("/api/items/movie/100/recovery-options"
                   "?section_id=1&rating_key=rk-4k", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["rating_key"] == "rk-4k"


def test_recovery_options_section_scoped_without_rk(app_client):
    """No rk sent (legacy caller): the pick must at least scope to the
    requested section — pre-fix the unscoped LIMIT 1 returned the
    insertion-order row (rk-std) for a section-2 card."""
    client, db = app_client
    _seed_two_sections_two_rks(db)
    r = client.get("/api/items/movie/100/recovery-options?section_id=2",
                   headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["rating_key"] == "rk-4k"


def test_recovery_options_foreign_rk_falls_back(app_client):
    """An rk that doesn't belong to this title must not be trusted —
    fall back to the section-scoped pick."""
    client, db = app_client
    _seed_two_sections_two_rks(db)
    with sqlite3.connect(db) as conn:
        _seed_theme(conn, tmdb_id=999, title="Other")
        _seed_plex_item(conn, rating_key="rk-other", section_id="1",
                        tmdb_id=999)
        conn.commit()
    r = client.get("/api/items/movie/100/recovery-options"
                   "?section_id=2&rating_key=rk-other", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["rating_key"] == "rk-4k"


# ── source pins (JS side of the pipe) ────────────────────────


def test_js_playback_source_reads_top_level_field():
    i = APP_JS.index("function _derivePlaybackSourceLabel")
    region = APP_JS[i:i + 2500]
    assert "data.plex_independent_theme === 1" in region
    assert "data.is_plex_agent" not in APP_JS, (
        "is_plex_agent is not returned by any endpoint — dead read"
    )


def test_js_threads_row_rk_to_recovery_options():
    sig_i = APP_JS.index("async function hydrateRecoveryOptions(")
    sig = APP_JS[sig_i:APP_JS.index(")", sig_i) + 1]
    assert "rowRk" in sig
    body = APP_JS[sig_i:sig_i + 2500]
    assert "params.set('rating_key', rowRk)" in body
    # openInfoDialog passes its clicked-row ratingKey through.
    call_i = APP_JS.index("hydrateRecoveryOptions(body,")
    assert "ratingKey" in APP_JS[call_i:call_i + 200]
