"""v1.22.16 — action-layer audit fixes (#2 data-loss, #5 silent no-op, #6 contract).

#2 (data-loss): bulk ACCEPT ALL deleted user_overrides SECTION-WIDE — a single
   accept of one edition wiped EVERY edition's override for the title. The
   per-row api_accept_update was edition-scoped in v1.21.87; the bulk path was
   missed. Now the override fetch/DELETE + the enqueues are edition-scoped.

#5 (silent failure): api_accept_update discarded _enqueue_download's return. A
   0-enqueue (no included Plex section owns the item) left the row "accepted"
   with the override deleted, no download, and no rollback — behind HTTP 200.
   Now it captures the count, logs a warning, and returns enqueued_sections.

#6 (contract break): backup_cloud_theme promises "never raises," but the
   section-subdir resolve only caught RuntimeError — a sqlite3.OperationalError
   ('database is locked') escaped and aborted the whole bulk-backup batch. Now
   any resolve error lands in the return dict.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-06-06T00:00:00"


# ── #2 source pins ───────────────────────────────────────────


def _accept_all_body() -> str:
    i = API_PY.index('@app.post("/api/updates/accept-all")')
    j = API_PY.index("\n    @app.", i + 10)  # next route after accept-all
    return API_PY[i:j]


def test_bulk_accept_all_override_delete_is_edition_scoped():
    body = _accept_all_body()
    # The override DELETE must carry an edition_key predicate.
    assert "DELETE FROM user_overrides" in body
    didx = body.index("DELETE FROM user_overrides")
    dblock = body[didx:didx + 220]
    assert "edition_key = ?" in dblock, (
        "v1.22.16: bulk ACCEPT ALL override DELETE must scope by edition_key "
        "(was section-wide → wiped sibling editions, data-loss)"
    )
    # Both enqueues thread edition_key.
    assert body.count("edition_key=edition") >= 2, (
        "v1.22.16: both bulk enqueues must pass edition_key=edition"
    )


# ── #2 behavioral: sibling edition's override survives ────────


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


def _db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def test_bulk_accept_all_does_not_wipe_sibling_edition_override(admin_client, tmp_path):
    """Multi-edition title, one section. The standard edition ('') has a pending
    update + its own override; the Extended edition has an override but NO pending
    (so it's NOT in the accept-all set). Pre-fix the standard accept's section-
    wide DELETE wiped BOTH overrides; post-fix Extended's survives."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES (1,'movie',500,'Multi','imdb',?,?,'https://y/watch?v=TDB')",
            (NOW, NOW))
        for rk, ed in (("rk-std", ""), ("rk-ext", "extended")):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_imdb, guid_tmdb, title, year, has_theme,"
                " local_theme_file, folder_path, plex_independent_theme,"
                " plex_theme_verified_ok, edition_key, first_seen_at, last_seen_at)"
                " VALUES (?, '1','movie',1,'tt500',500,'Multi',2012,0,0,?,0,1,?,?,?)",
                (rk, f"/data/m/Multi {ed}", ed, NOW, NOW))
        # Standard-edition override + a pending update at the global '' detection
        # row → standard edition is in the accept-all set.
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, section_id,"
            " edition_key, youtube_url, intent, set_at, set_by)"
            " VALUES ('movie',500,'1','','https://y/watch?v=STD','replace',?, 'admin')",
            (NOW,))
        # Extended-edition override — must SURVIVE (not in the accept set).
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, section_id,"
            " edition_key, youtube_url, intent, set_at, set_by)"
            " VALUES ('movie',500,'1','extended','https://y/watch?v=EXT','replace',?, 'admin')",
            (NOW,))
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
            " edition_key, new_video_id, new_youtube_url, old_video_id,"
            " old_youtube_url, detected_at, decision, kind)"
            " VALUES ('movie',500,'','','NEW','https://y/watch?v=NEW','OLD',"
            " 'https://y/watch?v=OLD',?, 'pending','upstream_changed')", (NOW,))
        # Extended edition has its own DECLINED decision → it's EXCLUDED from
        # accept-all (kept its theme). Pre-fix the standard accept's section-wide
        # DELETE still wiped its override anyway (the data-loss this isolates).
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
            " edition_key, new_video_id, new_youtube_url, old_video_id,"
            " old_youtube_url, detected_at, decision, kind)"
            " VALUES ('movie',500,'1','extended','NEW','https://y/watch?v=NEW',"
            " 'OLD','https://y/watch?v=OLD',?, 'declined','upstream_changed')",
            (NOW,))
        conn.commit()

    r = admin_client.post("/api/updates/accept-all", headers=AUTH)
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        ext = conn.execute(
            "SELECT youtube_url FROM user_overrides WHERE media_type='movie'"
            " AND tmdb_id=500 AND section_id='1' AND edition_key='extended'"
        ).fetchone()
        std = conn.execute(
            "SELECT youtube_url FROM user_overrides WHERE media_type='movie'"
            " AND tmdb_id=500 AND section_id='1' AND edition_key=''"
        ).fetchone()
    assert ext is not None and ext[0] == "https://y/watch?v=EXT", (
        "v1.22.16: the Extended edition's override must SURVIVE bulk ACCEPT ALL "
        "of the standard edition (pre-fix the section-wide DELETE wiped it)"
    )
    assert std is None, "the accepted standard edition's override is deleted"


# ── #5 source pins (silent no-op surfaced) ───────────────────


def test_accept_update_surfaces_zero_enqueue():
    i = API_PY.index("async def api_accept_update(")
    j = API_PY.index("async def api_decline_update(", i)
    body = API_PY[i:j]
    assert "_n_enq = _enqueue_download(" in body, (
        "v1.22.16: api_accept_update must CAPTURE the enqueue count"
    )
    assert '"enqueued_sections": _n_enq' in body, (
        "v1.22.16: the response must surface the enqueue count"
    )
    assert "queued NO download" in body, (
        "v1.22.16: a 0-enqueue must log a warning breadcrumb"
    )


# ── #6 contract: backup_cloud_theme never raises ─────────────


def test_accept_update_zero_enqueue_warning_is_warn_level():
    i = API_PY.index("async def api_accept_update(")
    j = API_PY.index("async def api_decline_update(", i)
    body = API_PY[i:j]
    widx = body.index("queued NO download")
    assert "log.warning" in body[widx - 120:widx]


def test_backup_cloud_theme_never_raises_on_db_locked():
    """The 'never raises' contract: a sqlite OperationalError resolving the
    section subdir must land in the return dict, not propagate (audit #6)."""
    from app.core.cloud_theme_backup import backup_cloud_theme

    class _RaisingConn:
        def execute(self, *a, **k):
            raise sqlite3.OperationalError("database is locked")

    target = {
        "rating_key": 1, "entry_uri": "upload://themes/abc", "sha1": "abc",
        "media_type": "movie", "guid_tmdb": 500, "section_id": "1",
        "title": "X", "year": 2012, "edition_key": "",
    }
    res = backup_cloud_theme(
        conn=_RaisingConn(), target=target,
        themes_dir=Path("/tmp/nope"), plex_client=None)
    assert res["ok"] is False, (
        "v1.22.16: a DB-locked section-subdir resolve must return ok=False, "
        "not raise (would abort the bulk-backup batch)"
    )
    assert res.get("error")


def test_backup_section_subdir_except_widened():
    src = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()
    i = src.index("section_slug = _section_themes_subdir(")
    block = src[i:i + 220]
    assert "except Exception" in block, (
        "v1.22.16: the section-subdir resolve must catch any error (was "
        "RuntimeError-only → sqlite errors escaped the never-raises contract)"
    )


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
