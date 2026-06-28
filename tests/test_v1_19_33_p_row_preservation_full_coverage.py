"""v1.19.33 — P-row preservation completed across all three callsites.

v1.19.32 fixed `api_accept_update` so P-rows survive ACCEPT UPDATE
without losing Plex's serving slot. The session audit found the
fix was incomplete:

  1. REVERT (`api_revert_to_themerrdb`) still force-placed → undid
     the v1.19.32 protection on the very next click. the user reverts
     his P+backup accept, gets the new URL force-placed via
     plex_upload, row flips P → U.
  2. Bulk ACCEPT ALL (`api_accept_all_updates`) iterates per-section
     with force_place=True — any P-row in the sweep silently flips
     to T. the user's Bleach scenario reproduces for users who click
     // ACCEPT ALL.
  3. v1.19.32's inline `is_p_row` predicate JOINed `themes t ON
     t.id = pi.theme_id` — silently missed fresh / orphan rows that
     hadn't been through resolve_theme_ids yet. The check returned
     False → fell back to force_place → exact same Bleach symptom
     at lower probability.
  4. Worker's backup_only stamp whitelist (v1.19.21) only covered
     `bulk_backup` + v1.19.32's new reason. Two other writers
     (`manual_url` = SET URL + KEEP AS BACKUP; `manual_backup` =
     per-row DOWNLOAD TDB BACKUP) wrote backup-intent local_files
     without the stamp → no BK badge + retry sweep re-enqueued
     place jobs → defeats user intent. This was a v1.19.21 gap
     the v1.19.32 work surfaced.

## Fix

`_is_p_row_for_section(conn, media_type, tmdb_id, section_id)`
extracted to a module-level helper near `_not_p_row_sql`. Mirrors
the v1.19.4 predicate exactly:

  - pi.has_theme = 1
  - COALESCE(pi.plex_theme_verified_ok, 1) = 1
  - NOT EXISTS (placements for this mt/tmdb/section)

Match against plex_items via EITHER `theme_id` linkage OR
`guid_tmdb` fallback so fresh / orphan / anime / pre-resolution
rows are all covered. v1.19.32's narrower JOIN-only-via-theme_id
form silently missed half the rows it should have protected.

Three callsites updated:

  - `api_accept_update` — refactored from inline SQL to helper.
  - `api_accept_all_updates` — applies per-section inside the
    bulk loop. P-row branch uses the v1.19.32 reason.
  - `api_revert_to_themerrdb` — new branch uses reason
    `revert_to_{prev_kind}_url_p_backup` (distinct so the worker
    whitelist can include it).

Worker `_record_local_file` whitelist extended to six reasons:
`bulk_backup`, `manual_backup`, `manual_url`,
`upstream_update_accepted_p_backup`, `revert_to_user_url_p_backup`,
`revert_to_themerrdb_url_p_backup`. Every backup-intent writer
now reaches the stamp.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient

API_PY = (REPO / "app" / "web" / "api.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


# ── Helper extracted + has the wider JOIN ────────────────────


def test_helper_defined_at_module_scope():
    """`_is_p_row_for_section` must be a top-level function so all
    three callsites import the same predicate."""
    assert "def _is_p_row_for_section(" in API_PY, (
        "v1.19.33: helper must be extracted to module scope so "
        "ACCEPT UPDATE, REVERT, and bulk ACCEPT ALL share it"
    )


def test_helper_matches_via_theme_id_OR_guid_tmdb():
    """The v1.19.32 inline form JOINed only `themes t ON t.id =
    pi.theme_id`. v1.19.33 widened the predicate to also match via
    `pi.guid_tmdb` so rows that haven't been through
    resolve_theme_ids yet (fresh / orphan / anime-pre-stamp) are
    covered. Pin both branches present in the helper body."""
    fn_start = API_PY.index("def _is_p_row_for_section(")
    fn_end = API_PY.index("\n\n", fn_start)
    body = API_PY[fn_start:fn_end]
    # Both join paths must appear.
    assert "pi.theme_id" in body, (
        "v1.19.33: helper must check theme_id linkage (preferred path)"
    )
    assert "pi.guid_tmdb" in body, (
        "v1.19.33: helper must ALSO check guid_tmdb so rows pre-"
        "resolve_theme_ids are covered — the v1.19.32 gap"
    )
    # The predicate body itself.
    assert "pi.has_theme = 1" in body
    assert "COALESCE(pi.plex_theme_verified_ok, 1) = 1" in body
    assert "NOT EXISTS" in body


# ── ACCEPT UPDATE single-row uses the helper ─────────────────


def test_accept_update_uses_helper():
    """`api_accept_update` must call the helper (not inline SQL)
    so a future change to the predicate flows everywhere."""
    fn_start = API_PY.index("async def api_accept_update(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    assert "_is_p_row_for_section(" in body, (
        "v1.19.33: api_accept_update must invoke the helper"
    )
    # The inline `JOIN themes t ON t.id = pi.theme_id` shape from
    # v1.19.32 must be gone (replaced by the helper call).
    assert "JOIN themes t ON t.id = pi.theme_id" not in body, (
        "v1.19.33: legacy v1.19.32 inline JOIN must be removed — "
        "helper call replaces it"
    )


# ── Bulk ACCEPT ALL gets the P-row branch ────────────────────


def test_accept_all_invokes_helper_per_section():
    """The bulk endpoint iterates per-section and passes
    `only_section_id`. It must call `_is_p_row_for_section` for
    each section so any P-row in the sweep doesn't get
    force-placed."""
    fn_start = API_PY.index("async def api_accept_all_updates(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    assert "_is_p_row_for_section(" in body, (
        "v1.19.33: bulk ACCEPT ALL must invoke the helper per "
        "section — pre-fix it force-placed every row including P"
    )
    # The legacy reason must remain for the non-P branch.
    assert '"bulk_update_accepted"' in body
    # The P-row branch must use the v1.19.32 reason so the worker
    # whitelist stamps backup_only.
    assert '"upstream_update_accepted_p_backup"' in body, (
        "v1.19.33: bulk P-row branch must use the v1.19.32 reason "
        "so worker.py's backup_only stamp fires"
    )


# ── REVERT gets the P-row branch ─────────────────────────────


def test_revert_invokes_helper_when_section_provided():
    """REVERT's force_place was unconditional pre-v1.19.33. With
    a P-row at the moment of REVERT, it stole Plex's serving slot."""
    fn_start = API_PY.index("async def api_revert_to_themerrdb(")
    fn_end = API_PY.index("@app.get", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    assert "_is_p_row_for_section(" in body, (
        "v1.19.33: REVERT must invoke the helper so a P-row revert "
        "doesn't force-place over Plex's serving slot"
    )
    # The new revert reasons must be present.
    assert "revert_to_{prev_kind}_url_p_backup" in body, (
        "v1.19.33: REVERT P-row branch must use the new reason "
        "string so worker whitelist matches"
    )
    # The audit details now carry the branch flag.
    assert '"p_row_backup":' in body, (
        "v1.19.33: REVERT audit row must record which branch was "
        "taken for post-incident debugging"
    )


# ── Worker whitelist extended ────────────────────────────────


def test_worker_whitelist_covers_all_six_reasons():
    """`_record_local_file`'s auto_place=False branch must stamp
    backup_only for every backup-intent writer."""
    fn_start = WORKER_PY.index("def _record_local_file(")
    next_def = WORKER_PY.find("\n    def ", fn_start + 1)
    body = WORKER_PY[fn_start:next_def if next_def > 0 else len(WORKER_PY)]
    for reason in (
        "bulk_backup",                          # v1.19.21
        "manual_backup",                        # v1.14.45 (v1.19.21 gap)
        "manual_url",                           # v1.16.4 (v1.19.21 gap)
        "upstream_update_accepted_p_backup",    # v1.19.32
        "revert_to_user_url_p_backup",          # v1.19.33
        "revert_to_themerrdb_url_p_backup",     # v1.19.33
    ):
        assert reason in body, (
            f"v1.19.33: worker whitelist missing reason '{reason}' — "
            f"every backup-intent writer must reach the stamp"
        )


# ── End-to-end behavioral ────────────────────────────────────


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
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    ).db_path


def _seed_section(conn, section_id="1"):
    conn.execute(
        "INSERT INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, "
        "   themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES (?, 'Movies', 'movie', 0, 0, "
        "        'movies', 1, '2026-05-26T00:00:00', "
        "        '2026-05-26T00:00:00')",
        (section_id,),
    )


def _seed_theme(conn, theme_id, tmdb_id,
                youtube_url="https://www.youtube.com/watch?v=ORIG"):
    now = "2026-05-26T00:00:00"
    conn.execute(
        "INSERT INTO themes "
        "  (id, media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
        "VALUES (?, 'movie', ?, 'Test Movie', 'imdb', ?, ?, ?)",
        (theme_id, tmdb_id, now, now, youtube_url),
    )


def _seed_plex_item(conn, *, rk, theme_id, tmdb_id, section_id="1",
                    has_theme=1, verified=1):
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, theme_id, "
        "   guid_imdb, guid_tmdb, title, year, has_theme, "
        "   local_theme_file, folder_path, "
        "   plex_independent_theme, plex_theme_verified_ok, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'movie', ?, "
        "        'tt100', ?, 'Test', 2020, ?, 0, "
        "        '/data/movies/Test', 0, ?, "
        "        '2026-05-26T00:00:00', "
        "        '2026-05-26T00:00:00')",
        (rk, section_id, theme_id, tmdb_id, has_theme, verified),
    )


def _seed_pending_update(conn, tmdb_id, section_id="1",
                         kind="upstream_changed",
                         new_url="https://www.youtube.com/watch?v=NEW",
                         decision="pending"):
    conn.execute(
        "INSERT INTO pending_updates "
        "  (media_type, tmdb_id, section_id, "
        "   new_video_id, new_youtube_url, old_youtube_url, "
        "   detected_at, decision, kind) "
        # v1.22.62: old_youtube_url seeded so the pending is a REAL
        # old→new diff — the shape the UI shows + the (now restored)
        # accept-all actionable gate matches. Pre-fix the url-less
        # shape only passed bulk accept because the gate was missing.
        "VALUES ('movie', ?, ?, 'NEW', ?, 'https://yt/orig', "
        "        '2026-05-26T00:00:00', ?, ?)",
        (tmdb_id, section_id, new_url, decision, kind),
    )


def _get_download_jobs(db, tmdb_id):
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT job_type, payload, status "
            "  FROM jobs "
            " WHERE media_type='movie' AND tmdb_id=?",
            (tmdb_id,),
        ).fetchall()
    return rows


# ── Wider helper covers theme_id-NULL rows (v1.19.32 gap) ────


def test_helper_returns_true_when_theme_id_null_but_guid_tmdb_matches(
    admin_client, tmp_path,
):
    """The v1.19.32 inline form failed for rows where pi.theme_id
    is NULL (fresh import, pre-resolve_theme_ids). v1.19.33's wider
    helper must still detect P state via guid_tmdb."""
    from app.web.api import _is_p_row_for_section
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn)
        _seed_theme(conn, theme_id=1, tmdb_id=100)
        # plex_item with theme_id=NULL — simulates pre-resolve.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, theme_id, "
            "   guid_imdb, guid_tmdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, plex_theme_verified_ok, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-fresh', '1', 'movie', NULL, "
            "        'tt100', 100, 'Test', 2020, 1, 0, "
            "        '/data/movies/Test', 0, 1, "
            "        '2026-05-26T00:00:00', "
            "        '2026-05-26T00:00:00')"
        )
        conn.commit()
        result = _is_p_row_for_section(
            conn, media_type="movie", tmdb_id=100, section_id="1",
        )
    assert result is True, (
        "v1.19.33: helper must return True for theme_id-NULL rows "
        "when guid_tmdb matches — the v1.19.32 gap that would have "
        "force-placed the user's Bleach repro on fresh-import rows"
    )


def test_helper_handles_tv_show_alias(admin_client, tmp_path):
    """media_type='tv' must alias to pi.media_type='show' inside
    the helper — the user's Bleach repro is a TV row."""
    from app.web.api import _is_p_row_for_section
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn)
        # TV theme.
        conn.execute(
            "INSERT INTO themes "
            "  (id, media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES (1, 'tv', 30984, 'Bleach', 'imdb', "
            "        '2026-05-26T00:00:00', '2026-05-26T00:00:00', "
            "        'https://www.youtube.com/watch?v=ORIG')"
        )
        # plex_items.media_type='show' (the Plex side).
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, theme_id, "
            "   guid_imdb, guid_tmdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, plex_theme_verified_ok, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-bleach', '1', 'show', 1, "
            "        'tt0434665', 30984, 'Bleach', 2004, 1, 0, "
            "        '/data/tv/Bleach', 0, 1, "
            "        '2026-05-26T00:00:00', "
            "        '2026-05-26T00:00:00')"
        )
        conn.commit()
        result = _is_p_row_for_section(
            conn, media_type="tv", tmdb_id=30984, section_id="1",
        )
    assert result is True, (
        "v1.19.33: helper must alias 'tv' → 'show' for pi.media_type — "
        "without the swap, Bleach's exact repro returns False"
    )


# ── REVERT P-row preserves Plex serving ──────────────────────


def test_revert_on_p_row_does_not_force_place(
    admin_client, tmp_path,
):
    """REVERT on a row that's currently SRC=P must NOT enqueue a
    force-placing download. Pin payload shape + the new reason."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn, theme_id=1, tmdb_id=100)
        _seed_plex_item(
            conn, rk="rk-p", theme_id=1, tmdb_id=100,
            has_theme=1, verified=1,
        )
        # Seed a previous_urls snapshot so REVERT has something to
        # restore. Kind='user' means it'll re-INSERT user_overrides.
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES ('movie', 100, '1', "
            "        'https://www.youtube.com/watch?v=PREV', "
            "        'user', '2026-05-26T00:00:00')"
        )
        conn.commit()

    r = admin_client.post(
        "/api/items/movie/100/revert?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text

    jobs = _get_download_jobs(db, 100)
    download_jobs = [j for j in jobs if j[0] == "download"]
    assert len(download_jobs) == 1
    payload = json.loads(download_jobs[0][1])
    assert payload.get("auto_place") is False, (
        f"v1.19.33: P-row REVERT must NOT auto-place; got payload={payload}"
    )
    assert payload.get("force_place") is not True, (
        f"v1.19.33: P-row REVERT must NOT force-place; got payload={payload}"
    )
    assert payload.get("reason") == "revert_to_user_url_p_backup", (
        f"v1.19.33: P-row REVERT reason must use the _p_backup "
        f"suffix so worker whitelist matches; got "
        f"reason={payload.get('reason')}"
    )
    place_jobs = [j for j in jobs if j[0] == "place"]
    assert place_jobs == [], (
        f"v1.19.33: P-row REVERT must NOT enqueue a place job; "
        f"got {place_jobs}"
    )


def test_revert_on_non_p_row_keeps_legacy_force_place(
    admin_client, tmp_path,
):
    """Counter-guard: REVERT on a non-P row keeps the v1.12.47
    force_place=True semantics so the U→T direction works."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn, theme_id=2, tmdb_id=200)
        _seed_plex_item(
            conn, rk="rk-u", theme_id=2, tmdb_id=200,
            has_theme=0, verified=0,
        )
        # Placement exists → not a P-row.
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, "
            "   media_folder, placed_at, placement_kind, "
            "   plex_rating_key, plex_refreshed, provenance) "
            "VALUES ('movie', 200, '1', "
            "        '/data/movies/U Row', "
            "        '2026-05-26T00:00:00', 'hardlink', "
            "        'rk-u', 1, 'manual')"
        )
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES ('movie', 200, '1', "
            "        'https://www.youtube.com/watch?v=PREV', "
            "        'themerrdb', '2026-05-26T00:00:00')"
        )
        conn.commit()

    r = admin_client.post(
        "/api/items/movie/200/revert?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text

    jobs = _get_download_jobs(db, 200)
    download_jobs = [j for j in jobs if j[0] == "download"]
    assert len(download_jobs) == 1
    payload = json.loads(download_jobs[0][1])
    assert payload.get("force_place") is True, (
        f"v1.19.33: non-P REVERT must preserve v1.12.47 "
        f"force_place=True; got payload={payload}"
    )
    assert payload.get("reason") == "revert_to_themerrdb_url", (
        f"v1.19.33: non-P REVERT reason must remain the legacy "
        f"string; got reason={payload.get('reason')}"
    )


# ── Bulk ACCEPT ALL preserves P state ────────────────────────


def test_bulk_accept_all_protects_p_rows(admin_client, tmp_path):
    """Bulk ACCEPT ALL must apply the P-row branch per section.
    Two rows seeded — one P, one not. The P-row gets the backup
    reason; the placed row gets force_place=True."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        # P-row + U-backup (the user's Bleach shape — P+U has a
        # user_overrides row, satisfying the bulk gate's presence
        # check below).
        _seed_theme(conn, theme_id=1, tmdb_id=100)
        _seed_plex_item(
            conn, rk="rk-p", theme_id=1, tmdb_id=100,
            has_theme=1, verified=1,
        )
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   set_at, set_by) "
            "VALUES ('movie', 100, '1', "
            "        'https://www.youtube.com/watch?v=USR1', "
            "        '2026-05-26T00:00:00', 'admin')"
        )
        _seed_pending_update(
            conn, tmdb_id=100,
            new_url="https://www.youtube.com/watch?v=NEW1",
        )
        # Non-P row (placement exists)
        _seed_theme(conn, theme_id=2, tmdb_id=200)
        _seed_plex_item(
            conn, rk="rk-placed", theme_id=2, tmdb_id=200,
            has_theme=0, verified=0,
        )
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, "
            "   media_folder, placed_at, placement_kind, "
            "   plex_rating_key, plex_refreshed, provenance) "
            "VALUES ('movie', 200, '1', "
            "        '/data/movies/Placed', "
            "        '2026-05-26T00:00:00', 'hardlink', "
            "        'rk-placed', 1, 'auto')"
        )
        _seed_pending_update(
            conn, tmdb_id=200,
            new_url="https://www.youtube.com/watch?v=NEW2",
        )
        conn.commit()

    r = admin_client.post("/api/updates/accept-all", headers=AUTH)
    assert r.status_code == 200, r.text

    # P-row job: backup reason, no force_place.
    p_jobs = [
        json.loads(j[1])
        for j in _get_download_jobs(db, 100) if j[0] == "download"
    ]
    assert len(p_jobs) == 1
    assert p_jobs[0].get("auto_place") is False, (
        f"v1.19.33: P-row in bulk ACCEPT ALL must auto_place=False; "
        f"got {p_jobs[0]}"
    )
    assert p_jobs[0].get("reason") == "upstream_update_accepted_p_backup"

    # Non-P-row job: force_place preserved.
    np_jobs = [
        json.loads(j[1])
        for j in _get_download_jobs(db, 200) if j[0] == "download"
    ]
    assert len(np_jobs) == 1
    assert np_jobs[0].get("force_place") is True, (
        f"v1.19.33: non-P row in bulk ACCEPT ALL must keep "
        f"force_place=True; got {np_jobs[0]}"
    )
    assert np_jobs[0].get("reason") == "bulk_update_accepted"
