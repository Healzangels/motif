"""v1.22.49 — imdb→tmdb de-orphan walker.

The v1.22.48 diagnostic confirmed ~547 plex_orphan themes are real titles whose
imdb resolves to a real tmdb via TMDB. This walker re-keys the synthetic negative
tmdb_id to the real one across the theme + its FK'd children (mirroring
sync._upsert_theme's promotion, FK-deferred), SKIPPING collisions and KEEPING
upstream_source='plex_orphan' (the theme is still a manual/adopted theme, not
from ThemerrDB — flipping upstream would falsely imply a TDB url).

Heavy testing: re-key correctness across every child table + FK integrity,
dry-run no-mutation, collision skip (real theme untouched), no-match/type-
mismatch skip, idempotency, media_type bridge, + the admin endpoint (dry-run
default, execute, no-key, admin gate).
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core import deorphan
from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db
from app.web.api import create_app

_NOW = "2026-06-09T09:00:00"

_MAP = {
    "tt_resolve": {"tmdb_id": 414419, "kind": "movie"},
    "tt_collide": {"tmdb_id": 777, "kind": "movie"},
    "tt_nomatch": None,
    "tt_wrongtype": {"tmdb_id": 999, "kind": "tv"},   # for a movie orphan
    "tt_tv": {"tmdb_id": 52896, "kind": "tv"},
}


class _FakeTMDB:
    enabled = True

    def lookup_by_imdb(self, imdb_id):
        return _MAP.get(imdb_id)


def _db():
    d = Path(tempfile.mkdtemp()) / "motif.db"
    init_db(d)
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('s','Movies','movie',1,0,0,'m',?,?)", (_NOW, _NOW))
        c.commit()
    return d


def _orphan(c, tmdb, imdb, title="T", mt="movie"):
    c.execute(
        "INSERT INTO themes (media_type,tmdb_id,imdb_id,title,year,"
        " upstream_source,first_seen_sync_at,last_seen_sync_at) "
        "VALUES (?,?,?,?,'2020','plex_orphan',?,?)",
        (mt, tmdb, imdb, title, _NOW, _NOW))


def _children(c, tmdb, mt="movie"):
    c.execute(
        "INSERT INTO local_files (media_type,tmdb_id,section_id,file_path,"
        " downloaded_at,source_video_id,source_kind) "
        "VALUES (?,?,'s','p/theme.mp3',?, 'vid','url')", (mt, tmdb, _NOW))
    c.execute(
        "INSERT INTO user_overrides (media_type,tmdb_id,section_id,youtube_url,"
        " set_at) VALUES (?,?,'s','https://u',?)", (mt, tmdb, _NOW))
    c.execute(
        "INSERT INTO placements (media_type,tmdb_id,section_id,media_folder,"
        " placed_at,placement_kind) VALUES (?,?,'s','',?, 'plex_upload')",
        (mt, tmdb, _NOW))
    c.execute(
        "INSERT INTO pending_updates (media_type,tmdb_id,detected_at) "
        "VALUES (?,?,?)", (mt, tmdb, _NOW))


def _tmdbs(d, table="themes"):
    with get_conn(d) as c:
        return sorted(r[0] for r in c.execute(
            f"SELECT tmdb_id FROM {table}").fetchall())


# ── re-key correctness ────────────────────────────────────────


def test_rekey_moves_theme_and_all_children_fk_clean():
    d = _db()
    with get_conn(d) as c:
        _orphan(c, -2, "tt_resolve", "Kill Bill")
        _children(c, -2)
        c.commit()
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert rep.rekeyed == 1
    assert _tmdbs(d, "themes") == [414419]
    for tbl in ("local_files", "user_overrides", "placements",
                "pending_updates"):
        assert _tmdbs(d, tbl) == [414419], f"{tbl} did not follow the re-key"
    with get_conn(d) as c:
        # upstream_source deliberately preserved
        assert c.execute(
            "SELECT upstream_source FROM themes WHERE tmdb_id=414419"
        ).fetchone()[0] == "plex_orphan"
        assert len(c.execute("PRAGMA foreign_key_check").fetchall()) == 0


def test_dry_run_mutates_nothing():
    d = _db()
    with get_conn(d) as c:
        _orphan(c, -2, "tt_resolve")
        _children(c, -2)
        c.commit()
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=True)
    assert rep.rekeyed == 1  # would-rekey count
    assert _tmdbs(d, "themes") == [-2]  # unchanged
    assert _tmdbs(d, "local_files") == [-2]


def test_collision_skips_and_leaves_real_theme_untouched():
    d = _db()
    with get_conn(d) as c:
        _orphan(c, 777, "tt_real", "RealMovie")  # a REAL theme at the target
        c.execute("UPDATE themes SET upstream_source='themoviedb' "
                  "WHERE tmdb_id=777")
        _orphan(c, -3, "tt_collide", "Collider")
        c.commit()
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert rep.skipped_collision == 1
    assert rep.rekeyed == 0
    assert _tmdbs(d, "themes") == [-3, 777]  # orphan stays, real untouched
    with get_conn(d) as c:
        assert c.execute("SELECT title FROM themes WHERE tmdb_id=777"
                         ).fetchone()[0] == "RealMovie"


def test_no_tmdb_match_skips():
    d = _db()
    with get_conn(d) as c:
        _orphan(c, -4, "tt_nomatch", "Niche")
        c.commit()
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert rep.no_tmdb_match == 1
    assert rep.rekeyed == 0
    assert _tmdbs(d, "themes") == [-4]


def test_media_type_mismatch_skips():
    d = _db()
    with get_conn(d) as c:
        _orphan(c, -5, "tt_wrongtype", "MovieButTmdbSaysTv")  # movie orphan
        c.commit()
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert rep.no_tmdb_match == 1
    assert _tmdbs(d, "themes") == [-5]


def test_tv_orphan_rekeys_via_tv_resolution():
    d = _db()
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('tv','TV','show',1,0,0,'t',?,?)", (_NOW, _NOW))
        _orphan(c, -6, "tt_tv", "A Show", mt="tv")
        c.commit()
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert rep.rekeyed == 1
    assert _tmdbs(d, "themes") == [52896]


def test_idempotent_rerun_is_noop():
    d = _db()
    with get_conn(d) as c:
        _orphan(c, -2, "tt_resolve")
        _children(c, -2)
        c.commit()
    deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    rep2 = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert rep2.scanned == 0   # the re-keyed row has a positive tmdb → not selected
    assert rep2.rekeyed == 0


def test_only_negative_tmdb_orphans_selected():
    # a plex_orphan that already has a real positive tmdb (the Matilda shape) is
    # left alone — its identity is already correct.
    d = _db()
    with get_conn(d) as c:
        _orphan(c, 10830, "tt_resolve", "Matilda")  # positive, already real id
        c.commit()
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert rep.scanned == 0
    assert _tmdbs(d, "themes") == [10830]


# ── v1.22.50 hardening: pre-delete stale children + error surfacing ──


def test_pre_delete_stale_target_children_lets_rekey_succeed():
    """A FK-invalid leftover child row at the TARGET tmdb (no theme there) used
    to UNIQUE-collide and error the re-key. The walker now clears such junk
    first (clash2 confirmed no real theme is there), so the re-key succeeds."""
    d = _db()
    with get_conn(d) as c:
        _orphan(c, -2, "tt_resolve", "Hokum")
        c.execute(
            "INSERT INTO placements (media_type,tmdb_id,section_id,media_folder,"
            " placed_at,placement_kind) VALUES ('movie',-2,'s','',?, "
            "'plex_upload')", (_NOW,))
        c.commit()
    # inject a STRAY placement at the resolved target (414419) with no theme
    raw = sqlite3.connect(d)
    raw.execute("PRAGMA foreign_keys=OFF")
    raw.execute(
        "INSERT INTO placements (media_type,tmdb_id,section_id,media_folder,"
        " placed_at,placement_kind) VALUES ('movie',414419,'s','',?, "
        "'plex_upload')", (_NOW,))
    raw.commit()
    raw.close()
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert rep.rekeyed == 1 and rep.errors == 0
    assert _tmdbs(d, "themes") == [414419]
    assert _tmdbs(d, "placements") == [414419]  # one row, no dup
    with get_conn(d) as c:
        assert len(c.execute("PRAGMA foreign_key_check").fetchall()) == 0


def test_rekey_moves_previous_urls_fk_child():
    """v1.22.51 (the user's 'Hokum'): previous_urls is ALSO FK'd to
    themes(media_type,tmdb_id). An orphan carrying a REVERT/url-change history
    row used to fail the re-key with FOREIGN KEY constraint failed (the row was
    left pointing at the old synthetic id). It must now move with the re-key."""
    d = _db()
    with get_conn(d) as c:
        _orphan(c, -33, "tt_resolve", "Hokum")
        c.execute(
            "INSERT INTO previous_urls (media_type,tmdb_id,youtube_url,kind,"
            " captured_at) VALUES ('movie',-33,'https://old','user',?)", (_NOW,))
        c.commit()
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert rep.rekeyed == 1 and rep.errors == 0
    assert _tmdbs(d, "themes") == [414419]
    assert _tmdbs(d, "previous_urls") == [414419]
    with get_conn(d) as c:
        assert len(c.execute("PRAGMA foreign_key_check").fetchall()) == 0


def test_error_surfaces_in_sample_with_reason(monkeypatch):
    """A re-key that throws is counted as an error AND surfaced in the sample as
    action='error' with the reason — pre-fix the sample optimistically said
    'rekeyed' before the txn even ran."""
    d = _db()
    with get_conn(d) as c:
        _orphan(c, -2, "tt_resolve", "Boom")
        c.commit()

    def _boom(_conn):
        raise sqlite3.IntegrityError("forced failure")

    monkeypatch.setattr("app.core.deorphan.transaction", _boom)
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert rep.errors == 1 and rep.rekeyed == 0
    assert _tmdbs(d, "themes") == [-2]  # untouched
    err_samples = [s for s in rep.samples if s["action"] == "error"]
    assert len(err_samples) == 1
    assert "forced failure" in err_samples[0]["error"]


def test_sync_promotion_also_moves_previous_urls():
    """v1.22.51: the SAME previous_urls FK gap existed in sync._upsert_theme's
    orphan promotion (higher-traffic path) — a real TDB theme arriving for an
    orphan that carried a previous_urls row would fail promotion. It must now
    move that row too."""
    from app.core.sync import SyncStats, _flush_sync_batch
    d = _db()
    with get_conn(d) as c:
        _orphan(c, -33, "tt_h", "Hokum")
        c.execute(
            "INSERT INTO previous_urls (media_type,tmdb_id,youtube_url,kind,"
            " captured_at) VALUES ('movie',-33,'https://old','user',?)", (_NOW,))
        c.commit()
    record = {"title": "Hokum", "imdb_id": "tt_h",
              "youtube_theme_url": "https://youtu.be/x",
              "release_date": "2026-01-01"}
    _flush_sync_batch(
        d, [("movie", 1430077, record, "themoviedb")], sync_ts=_NOW,
        enqueue_downloads=False, auto_place_override=None,
        auto_download_new_themes=False, stats=SyncStats())
    with get_conn(d) as c:
        # the orphan was promoted to the real tmdb; previous_urls moved with it
        assert _tmdbs(d, "themes") == [1430077]
        assert _tmdbs(d, "previous_urls") == [1430077]
        assert len(c.execute("PRAGMA foreign_key_check").fetchall()) == 0


def test_rekeyed_sample_label_reflects_real_outcome():
    d = _db()
    with get_conn(d) as c:
        _orphan(c, -2, "tt_resolve", "Real")
        c.commit()
    rep = deorphan.deorphan_imdb_resolvable(d, _FakeTMDB(), dry_run=False)
    assert [s["action"] for s in rep.samples] == ["rekeyed"]


# ── admin endpoint ────────────────────────────────────────────


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_TMDB_API_KEY", "fakekey")
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    with get_conn(db) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('s','Movies','movie',1,0,0,'m',?,?)", (_NOW, _NOW))
        _orphan(c, -2, "tt_resolve", "Kill Bill")
        _children(c, -2)
        c.commit()
    app = create_app(settings)
    c = TestClient(app)
    c.headers["X-Authentik-Username"] = "testadmin"
    c.db = db  # type: ignore[attr-defined]
    yield c


def test_endpoint_dry_run_default_does_not_mutate(client, monkeypatch):
    monkeypatch.setattr(
        "app.core.tmdb.TMDBClient.lookup_by_imdb", _FakeTMDB.lookup_by_imdb)
    r = client.post("/api/admin/deorphan-imdb")  # no dry_run → defaults True
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is True and j["dry_run"] is True and j["rekeyed"] == 1
    assert _tmdbs(client.db, "themes") == [-2]  # untouched


def test_endpoint_execute_rekeys(client, monkeypatch):
    monkeypatch.setattr(
        "app.core.tmdb.TMDBClient.lookup_by_imdb", _FakeTMDB.lookup_by_imdb)
    r = client.post("/api/admin/deorphan-imdb?dry_run=false")
    assert r.status_code == 200
    j = r.json()
    assert j["dry_run"] is False and j["rekeyed"] == 1
    assert _tmdbs(client.db, "themes") == [414419]


def test_endpoint_requires_admin(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    app = create_app(settings)
    c = TestClient(app)  # no admin header
    r = c.post("/api/admin/deorphan-imdb")
    if r.status_code == 200:
        assert "application/json" not in r.headers.get("content-type", "")
        assert '"rekeyed"' not in r.text
    else:
        assert r.status_code in (401, 403, 302, 307)
