"""v0.51.337: backup bundle, tag 3 — the themes check (feature D), realized on
the v0.51.167 // CANONICAL HEALTH page.

Spec docs/specs/BACKUP_BUNDLE_SPEC.md § 3. The page already stats every
canonical and splits the broken rows into re-downloadable vs canonical-
missing; this tag adds the other way a canonical comes back without a
source URL — from Plex: the sidecar still in its Plex folder, else Plex's
own store for a plex_upload placement — as one bulk, plus a CHANGED bucket
for files present but not the recorded size.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import canonical_health as ch
from app.core.db import get_conn, init_db

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
PAGE = (REPO / "app" / "web" / "templates" / "canonical_health.html").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
NOW = "2026-09-12T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


# ── seed helpers (the v0.51.167 shapes) ───────────────────────────────

def _section(conn, section_id="1"):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, 'M', 'movie', 0, 0, 'movies', 1, ?, ?)"
        " ON CONFLICT(section_id) DO NOTHING", (section_id, NOW, NOW))


def _theme(conn, *, tid, tmdb, upstream="plex_orphan"):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, ?, ?, ?, ?, NULL)",
        (tid, tmdb, f"T{tmdb}", upstream, NOW, NOW))


def _lf(conn, *, tid, tmdb, source_kind="upload", canonical_present=0, file_size=None):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id,"
        " file_path, file_size, downloaded_at, source_video_id, provenance, source_kind,"
        " canonical_present, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, ?, ?, '', 'manual', ?, ?, '')",
        (tmdb, tid, f"movies/{tmdb}/theme.mp3", file_size, NOW, source_kind, canonical_present))


def _placement(conn, *, tmdb, media_folder, kind="hardlink", rk=None, theme_present=1):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind,"
        " provenance, placed_at, plex_rating_key, theme_present, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, 'manual', ?, ?, ?, '')",
        (tmdb, media_folder, kind, NOW, rk, theme_present))


def _seed(tmp_path: Path):
    db = tmp_path / "m.db"
    init_db(db)
    themes = tmp_path / "themes"
    plexdir = tmp_path / "plex"
    with sqlite3.connect(db) as conn:
        _section(conn)
        # 101: broken, sidecar survives in the Plex folder → restorable from the folder
        _theme(conn, tid=1, tmdb=101)
        _lf(conn, tid=1, tmdb=101)
        (plexdir / "101").mkdir(parents=True)
        (plexdir / "101" / "theme.mp3").write_bytes(b"sidecar-bytes-101")
        # theme_present is NULL on purpose: a database restored onto a new box
        # carries no fresh stamps — the report must find the sidecar by stat.
        _placement(conn, tmdb=101, media_folder=str(plexdir / "101"), theme_present=None)
        # 102: broken, a plex_upload placement → restorable from Plex's store
        _theme(conn, tid=2, tmdb=102)
        _lf(conn, tid=2, tmdb=102)
        _placement(conn, tmdb=102, media_folder="", kind="plex_upload", rk="9002")
        # 103: broken, nothing anywhere
        _theme(conn, tid=3, tmdb=103)
        _lf(conn, tid=3, tmdb=103)
        # 104: present but the wrong size → CHANGED
        _theme(conn, tid=4, tmdb=104)
        _lf(conn, tid=4, tmdb=104, canonical_present=1, file_size=10)
        (themes / "movies" / "104").mkdir(parents=True)
        (themes / "movies" / "104" / "theme.mp3").write_bytes(b"x" * 25)
        # 105: present and the right size → nothing to report
        _theme(conn, tid=5, tmdb=105)
        _lf(conn, tid=5, tmdb=105, canonical_present=1, file_size=3)
        (themes / "movies" / "105").mkdir(parents=True)
        (themes / "movies" / "105" / "theme.mp3").write_bytes(b"abc")
        conn.commit()
    return db, themes, plexdir


class FakePlex:
    """The two calls the store path makes, as a plex_client stand-in."""
    def __init__(self, entries=None, body=b"store-bytes-102", ok=True):
        self.entries = entries if entries is not None else [{"ratingKey": "upload://themes/abc", "selected": True}]
        self.body = body
        self.ok = ok
        self.calls: list[tuple] = []

    def get_themes(self, *, rating_key):
        self.calls.append(("themes", rating_key))
        return {"ok": self.ok, "http_status": 200 if self.ok else 500, "error": None,
                "body": {"MediaContainer": {"Metadata": self.entries}}}

    def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
        self.calls.append(("fetch", item_rating_key, entry_uri))
        return {"ok": True, "http_status": 200, "bytes": self.body}


# ── the module ────────────────────────────────────────────────────────

def test_restore_from_placement_hardlinks_the_surviving_sidecar_and_stamps(tmp_path):
    db, themes, plexdir = _seed(tmp_path)
    with get_conn(db) as conn:
        rows = ch._broken_rows_with_placement(conn)
    r = next(x for x in rows if x["tmdb_id"] == 101)
    res = ch.restore_from_placement(db, themes, r)
    assert res["ok"] and res["kind"] in ("hardlink", "copy")
    assert (themes / "movies" / "101" / "theme.mp3").read_bytes() == b"sidecar-bytes-101"
    with sqlite3.connect(db) as conn:
        size, sha, present = conn.execute(
            "SELECT file_size, file_sha256, canonical_present FROM local_files WHERE tmdb_id = 101").fetchone()
    assert size == len(b"sidecar-bytes-101") and len(sha) == 64 and present == 1
    # a second call is a no-op: the canonical is present now
    assert ch.restore_from_placement(db, themes, r) == {"ok": False, "reason": "canonical_already_present"}


def test_restore_from_placement_reasons(tmp_path):
    db, themes, plexdir = _seed(tmp_path)
    with get_conn(db) as conn:
        rows = {x["tmdb_id"]: x for x in ch._broken_rows_with_placement(conn)}
    assert ch.restore_from_placement(db, themes, rows[103]) == {"ok": False, "reason": "no_placement"}
    (plexdir / "101" / "theme.mp3").unlink()
    assert ch.restore_from_placement(db, themes, rows[101]) == {"ok": False, "reason": "placement_file_missing"}


def test_refetch_from_plex_store_writes_the_selected_entry(tmp_path):
    db, themes, plexdir = _seed(tmp_path)
    with get_conn(db) as conn:
        r = next(x for x in ch._broken_rows_with_placement(conn) if x["tmdb_id"] == 102)
    plex = FakePlex()
    res = ch.refetch_from_plex_store(db, themes, plex, r)
    assert res == {"ok": True, "bytes": len(b"store-bytes-102"), "entry_uri": "upload://themes/abc"}
    assert plex.calls == [("themes", "9002"), ("fetch", "9002", "upload://themes/abc")]
    assert (themes / "movies" / "102" / "theme.mp3").read_bytes() == b"store-bytes-102"
    assert not list((themes / "movies" / "102").glob("*.part"))
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT canonical_present FROM local_files WHERE tmdb_id = 102").fetchone()[0] == 1


def test_refetch_from_plex_store_reasons(tmp_path):
    db, themes, plexdir = _seed(tmp_path)
    with get_conn(db) as conn:
        rows = {x["tmdb_id"]: x for x in ch._broken_rows_with_placement(conn)}
    assert ch.refetch_from_plex_store(db, themes, None, rows[102]) == {"ok": False, "reason": "plex_unavailable"}
    assert ch.refetch_from_plex_store(db, themes, FakePlex(), rows[103]) == {"ok": False, "reason": "no_rating_key"}
    two = FakePlex(entries=[{"ratingKey": "upload://themes/a"}, {"ratingKey": "metadata://themes/b"}])
    assert ch.refetch_from_plex_store(db, themes, two, rows[102]) == {"ok": False, "reason": "no_theme_entry"}, "two entries, none selected"
    bad = FakePlex(ok=False)
    assert ch.refetch_from_plex_store(db, themes, bad, rows[102])["reason"].startswith("plex_themes:")


def test_selected_entry_rules():
    body = lambda entries: {"MediaContainer": {"Metadata": entries}}  # noqa: E731
    assert ch._selected_entry_uri(body([{"ratingKey": "upload://x"}])) == "upload://x"
    assert ch._selected_entry_uri(body([{"ratingKey": "a"}, {"ratingKey": "b", "selected": True}])) == "b"
    assert ch._selected_entry_uri(body([{"ratingKey": "a"}, {"ratingKey": "b"}])) is None
    assert ch._selected_entry_uri(body([])) is None
    assert ch._selected_entry_uri("junk") is None


def test_bulk_restores_sidecar_then_store_and_skips_the_rest(tmp_path):
    db, themes, plexdir = _seed(tmp_path)
    plex = FakePlex()
    res = ch.restore_from_plex(db, themes, plex)
    assert (res["broken"], res["restored_sidecar"], res["restored_store"], res["restored"]) == (3, 1, 1, 2)
    assert [s["tmdb_id"] for s in res["skipped"]] == [103] and res["skipped"][0]["reason"] == "no_plex_copy"
    assert plex.calls[0] == ("themes", "9002"), "only the store row touched Plex"
    with get_conn(db) as conn:
        rep = ch.broken_canonical_report(conn, themes)
    assert rep["counts"]["broken"] == 1 and rep["counts"]["restorable_from_plex"] == 0


def test_bulk_without_plex_still_restores_sidecars(tmp_path):
    db, themes, plexdir = _seed(tmp_path)
    res = ch.restore_from_plex(db, themes, None)
    assert res["restored_sidecar"] == 1 and res["restored_store"] == 0
    reasons = {s["tmdb_id"]: s["reason"] for s in res["skipped"]}
    assert reasons == {102: "plex_unavailable", 103: "no_plex_copy"}


def test_report_carries_plex_copy_restorable_count_and_changed(tmp_path):
    db, themes, plexdir = _seed(tmp_path)
    with get_conn(db) as conn:
        rep = ch.broken_canonical_report(conn, themes, plex_available=True)
        # v0.51.339: the store copy is only promised when the bulk can reach Plex.
        off = ch.broken_canonical_report(conn, themes)
    copies = {r["tmdb_id"]: r["plex_copy"] for r in rep["canonical_missing"]}
    assert copies == {101: "sidecar", 102: "store", 103: None}, "101 by stat (no theme_present stamp), 102 by kind"
    assert {r["tmdb_id"]: r["plex_copy"] for r in off["canonical_missing"]}[102] is None
    (plexdir / "101" / "theme.mp3").unlink()
    with get_conn(db) as conn:
        rep2 = ch.broken_canonical_report(conn, themes, plex_available=True)
    assert {r["tmdb_id"]: r["plex_copy"] for r in rep2["canonical_missing"]}[101] is None, "the sidecar gone → no copy"
    assert rep["counts"]["restorable_from_plex"] == 2
    assert [c["tmdb_id"] for c in rep["changed"]] == [104]
    assert rep["changed"][0]["recorded"] == 10 and rep["changed"][0]["on_disk"] == 25
    assert rep["counts"]["changed"] == 1
    with get_conn(db) as conn:
        assert ch.broken_canonical_report(conn)["changed"] == [], "no themes_dir → no stats"


# ── the API + the page ────────────────────────────────────────────────

@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings, tmp_path


def _seed_into(settings, tmp_path):
    db, themes, plexdir = _seed(tmp_path / "seed")
    # move the seed's rows into the app's db
    src = sqlite3.connect(db)
    dst = sqlite3.connect(settings.db_path)
    for t in ("plex_sections", "themes", "local_files", "placements"):
        cols = [r[1] for r in src.execute(f"PRAGMA table_info({t})")]
        rows = src.execute(f"SELECT * FROM {t}").fetchall()
        dst.executemany(f"INSERT OR REPLACE INTO {t} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})", rows)
    dst.commit(); src.close(); dst.close()
    # the app's themes_dir is tmp_path/themes — mirror the present files there
    import shutil
    shutil.copytree(themes, tmp_path / "themes", dirs_exist_ok=True)
    return plexdir


def test_endpoint_restores_from_folders_without_plex_and_reports(admin_client):
    client, settings, tmp_path = admin_client
    _seed_into(settings, tmp_path)
    r = client.get("/api/admin/canonical-health/report", headers=AUTH)
    assert r.status_code == 200
    j = r.json()
    # v0.51.339: was 2 — Plex is off here, so 102's store copy is not restorable (the bulk skips it plex_unavailable).
    assert j["counts"]["restorable_from_plex"] == 1 and j["counts"]["changed"] == 1
    r = client.post("/api/admin/canonical-health/restore-from-plex", headers=AUTH)
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["ok"] and j["restored_sidecar"] == 1 and j["restored_store"] == 0
    assert {s["tmdb_id"]: s["reason"] for s in j["skipped"]} == {102: "plex_unavailable", 103: "no_plex_copy"}
    assert (tmp_path / "themes" / "movies" / "101" / "theme.mp3").read_bytes() == b"sidecar-bytes-101"
    r = client.get("/api/admin/canonical-health/report", headers=AUTH)
    assert r.json()["counts"]["broken"] == 2


def test_endpoint_requires_admin_and_themes_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path); init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    client = TestClient(create_app(settings))
    assert client.post("/api/admin/canonical-health/restore-from-plex").status_code in (401, 403)
    r = client.post("/api/admin/canonical-health/restore-from-plex", headers=AUTH)
    assert r.status_code == 409, "no themes_dir configured"


def test_page_markup_and_binder():
    assert 'id="canon-restore-plex-btn"' in PAGE and 'id="canon-changed-block"' in PAGE
    assert 'id="canon-changed-tbody"' in PAGE and "// RESTORE FROM PLEX" in PAGE
    i = APP_JS.index("function bindCanonicalHealth() {")
    blk = APP_JS[i:APP_JS.index("\n  function ", i + 40)]
    assert "'/api/admin/canonical-health/restore-from-plex'" in blk
    assert "c.restorable_from_plex" in blk and "rep.changed" in blk
    assert "r.plex_copy === 'sidecar'" in blk
    assert "restorable from Plex" in blk


def test_restore_card_links_to_canonical_health():
    assert 'href="/admin/canonical-health">// CANONICAL HEALTH</a>' in SETTINGS_HTML


def test_v0_51_337_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.337: backup bundle, tag 3" in init_py
