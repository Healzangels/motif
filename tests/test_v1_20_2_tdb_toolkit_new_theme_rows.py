"""v1.20.2 — TDB toolkit (REPLACE TDB / DOWNLOAD TDB BACKUP) on
net-new (new_theme_available) rows + clear-the-pill-on-backup.

the user's ANIME-page repro (the net-new TDB rows from sync, all
kind='new_theme_available' per v1.19.71):

  ① The blue !UPD/TDB↑ pill row had NO source-menu TDB action —
     REPLACE TDB + DOWNLOAD TDB BACKUP were both suppressed by the
     bare `!it.pending_update` gate ("ACCEPT UPDATE covers it"). On a
     net-new P-row that's wrong: the user wants the explicit TDB
     toolkit (download a backup, or replace the Plex agent theme).
  ② "if a TDB backup is done that should also clear the blue pill
     TDB since then it's not an update and has been added to the
     motif collection." → the download-backup + replace-with-themerrdb
     endpoints resolve the pending new_theme_available update so the
     pill clears.

v1.20.2 fix:
  - JS: shared `tdbActionPendingOk` flag (= !pending_update ||
    kind==='new_theme_available') relaxes BOTH gates so net-new rows
    keep the toolkit. Genuine upstream-change updates still suppress
    it (they're not satisfied by a backup).
  - api.py: `_resolve_new_theme_pending_update` helper, wired into
    api_download_backup + api_replace_with_themerrdb. ONLY
    new_theme_available is resolved this way.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


# ── ① JS: both TDB gates relaxed for new_theme_available ─────


def test_shared_tdb_action_pending_flag_exists():
    assert "const tdbActionPendingOk = !it.pending_update" in APP_JS, (
        "v1.20.2: the shared TDB-action pending gate must exist"
    )
    idx = APP_JS.index("const tdbActionPendingOk = !it.pending_update")
    block = APP_JS[idx:idx + 160]
    assert "it.pending_update_kind === 'new_theme_available'" in block, (
        "v1.20.2: the flag must exempt new_theme_available rows so "
        "net-new TDB rows keep the toolkit while genuine updates "
        "still suppress it"
    )


def test_replace_tdb_gate_uses_shared_flag():
    idx = APP_JS.index("'replace-with-themerrdb', 'REPLACE TDB'")
    # The gate is above the menu push (the multi-line replaceTip sits
    # between them) — slice back far enough to capture the gate.
    gate = APP_JS[idx - 900:idx]
    assert "tdbActionPendingOk" in gate, (
        "v1.20.2: REPLACE TDB gate must go through tdbActionPendingOk, "
        "not the bare !it.pending_update"
    )


def test_download_tdb_backup_gate_uses_shared_flag():
    idx = APP_JS.index("'download-tdb-backup', 'DOWNLOAD TDB BACKUP'")
    # The long tdbBackupTip branch sits between the gate and the push —
    # slice back generously to reach the gate condition.
    gate = APP_JS[idx - 1200:idx]
    assert "tdbActionPendingOk" in gate, (
        "v1.20.2: DOWNLOAD TDB BACKUP gate must go through "
        "tdbActionPendingOk"
    )


# ── server: resolver only acts on new_theme_available ────────


def test_resolver_scoped_to_new_theme_kind():
    # v1.20.10: the resolver moved to core.sync (so the worker can call
    # it). It writes inline, scoped to new_theme_available — acts only
    # when the acted section's EFFECTIVE pending update IS the pending
    # new_theme one (v1.20.9 Finding-1 guard).
    idx = SYNC_PY.index("def resolve_new_theme_pending_update(")
    # v1.21.94: 2400 → 2800 — an `edition-blind OK` marker added a few
    # lines before the resolver's INSERT (structural-comment drift).
    block = SYNC_PY[idx:idx + 2800]
    assert 'eff["kind"] != "new_theme_available"' in block, (
        "v1.20.9: the resolver must gate on the effective row's kind "
        "being new_theme_available — a genuine upstream change isn't "
        "satisfied by a backup"
    )
    assert '"new_theme_available"' in block and "'accepted'" in block


def test_resolver_moved_to_worker_not_endpoints():
    # v1.20.10: the endpoints no longer resolve synchronously at click —
    # the worker resolves on download SUCCESS (so a failed backup leaves
    # the pill up). See test_v1_20_10_resolve_on_download_success.py.
    assert "_resolve_new_theme_pending_update(" not in API_PY, (
        "v1.20.10: api.py must not resolve the pill synchronously"
    )
    assert "resolve_new_theme_pending_update(" in WORKER_PY, (
        "v1.20.10: the worker (_record_local_file) resolves on download "
        "success"
    )


# ── behavioral: download-backup clears the new_theme pill ────


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


def _seed_new_theme_row(db, *, tmdb_id, title, kind="new_theme_available",
                        with_plex_item=False):
    """A TDB-tracked row with a pending update of the given kind whose
    new URL equals themes.youtube_url (the net-new shape).

    with_plex_item=True links an in-Plex item via theme_id (the linkage
    _enqueue_download matches on) so DOWNLOAD TDB BACKUP actually
    enqueues n>=1 — required since v1.20.9 gated the pill-clear on a
    real enqueue."""
    now = "2026-05-29T00:00:00"
    url = f"https://www.youtube.com/watch?v=NEW{tmdb_id}"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
            "  is_anime, is_4k, themes_subdir, included, discovered_at, "
            "  last_seen_at) VALUES ('3','Anime','show',1,0,'anime',1,?,?)",
            (now, now))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "  last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES ('tv',?,?,'imdb',?,?,?)",
            (tmdb_id, title, now, now, url))
        theme_id = cur.lastrowid
        if with_plex_item:
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type, "
                "  theme_id, guid_tmdb, title, year, has_theme, first_seen_at, "
                "  last_seen_at) VALUES (?,'3','show',?,?,?,2024,0,?,?)",
                (f"rk{tmdb_id}", theme_id, tmdb_id, title, now, now))
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "  kind, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',?,'3',?,?,'pending',?)",
            (tmdb_id, kind, url, now))
        conn.commit()


def _pending_decision(db, tmdb_id, section_id="3"):
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT decision FROM pending_updates WHERE media_type='tv' "
            "AND tmdb_id=? AND section_id=?",
            (tmdb_id, section_id)).fetchone()
    return row[0] if row else None


def test_download_backup_enqueues_without_clearing_pill_at_click(admin_client):
    client, db = admin_client
    _seed_new_theme_row(db, tmdb_id=6001, title="Made in Abyss",
                        with_plex_item=True)

    r = client.post(
        "/api/items/tv/6001/download-backup?section_id=3", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json().get("enqueued_sections", 0) >= 1, r.text
    # v1.20.10: the click only ENQUEUES — the !UPD pill clears on the
    # download's SUCCESS (worker), not synchronously here, so a failed
    # backup can't leave a falsely-cleared pill.
    assert _pending_decision(db, 6001) == "pending", (
        "v1.20.10: DOWNLOAD TDB BACKUP must not clear the pill at click — "
        "the worker resolves it on download success"
    )


def test_download_backup_leaves_genuine_update_pending(admin_client):
    """A genuine upstream change (kind != new_theme_available) is NOT
    satisfied by a backup — its pill must survive until the user
    explicitly accepts/declines."""
    client, db = admin_client
    _seed_new_theme_row(db, tmdb_id=6002, title="Vinland Saga",
                        kind="upstream_changed", with_plex_item=True)

    r = client.post(
        "/api/items/tv/6002/download-backup?section_id=3", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json().get("enqueued_sections", 0) >= 1, r.text
    assert _pending_decision(db, 6002) == "pending", (
        "v1.20.2: a non-new_theme pending update must stay pending after "
        "a backup — the resolver is scoped to new_theme_available only"
    )


def test_replace_with_themerrdb_does_not_clear_pill_at_click(
        admin_client, monkeypatch):
    """v1.20.10: REPLACE TDB enqueues a fresh TDB download; the worker
    resolves the pill on that download's SUCCESS, not synchronously in
    the endpoint. adopt's replace_with_themerrdb is mocked (it needs a
    real Plex folder) so no real download runs here → pill stays pending."""
    client, db = admin_client
    now = "2026-05-29T00:00:00"
    _seed_new_theme_row(db, tmdb_id=6003, title="Frieren")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  guid_tmdb, title, year, has_theme, first_seen_at, last_seen_at) "
            "VALUES ('rk6003','3','show',6003,'Frieren',2024,1,?,?)",
            (now, now))
        conn.commit()

    import app.core.adopt as adopt_mod
    monkeypatch.setattr(
        adopt_mod, "replace_with_themerrdb",
        lambda db, **kw: {"ok": True, "enqueued": 1}, raising=True)

    r = client.post(
        "/api/plex_items/rk6003/replace-with-themerrdb", headers=AUTH)
    assert r.status_code == 200, r.text
    assert _pending_decision(db, 6003) == "pending", (
        "v1.20.10: REPLACE TDB must not clear the pill synchronously — the "
        "worker resolves it when the enqueued TDB download succeeds"
    )


def test_v1_20_2_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
