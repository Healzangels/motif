"""v0.51.336: backup bundle, tag 2 — restore from a bundle (feature D).

Spec docs/specs/BACKUP_BUNDLE_SPEC.md § 3 / § 5. A bundle restores through a
preview (manifest line, DB check, the config keys that differ with secrets
masked, the cookies verdict), then stages the DB as a snapshot does and the
config + cookies as <name>.restore-pending; the boot hook swaps them after
.prerestore-<stamp> copies, BEFORE get_settings() reads the YAML. The
operator may keep the live config.
"""
from __future__ import annotations

import json
import sqlite3
import tarfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.db import CURRENT_SCHEMA_VERSION, init_db

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
MAIN_PY = (REPO / "app" / "main.py").read_text()
NOW = "20260912-040000"


def _make_bundle(root: Path, *, stamp: str = NOW, token: str = "OLD-TOKEN",
                 themes: str = "/data/media/themes", cookies: bool = True) -> Path:
    """A real bundle built by tag 1's create_bundle from a seeded DB."""
    src = root / "src"
    src.mkdir(parents=True, exist_ok=True)
    db = src / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, file_size, "
                     "downloaded_at, source_video_id, source_kind) "
                     "VALUES ('tv', 777, '3', 'tv/x/theme.mp3', 10, 'x', 'v', 'url')")
        conn.commit()
    cfg = src / "motif.yaml"
    cfg.write_text(f"plex:\n  url: http://plex:32400\n  token: {token}\npaths:\n  themes_dir: {themes}\n")
    ck = src / "cookies.txt"
    if cookies:
        ck.write_text("# cookies\n")
    bf = bundle.create_bundle(db, src, config_file=cfg, cookies_file=ck if cookies else None,
                              themes_dir=Path(themes), now_stamp=stamp, motif_version="0.51.336",
                              schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


def _live(root: Path, *, token: str = "LIVE-TOKEN") -> tuple[Path, Path]:
    cd = root / "live"
    cd.mkdir(parents=True, exist_ok=True)
    db = cd / "motif.db"
    init_db(db)
    (cd / "motif.yaml").write_text(f"plex:\n  url: http://plex.old:32400\n  token: {token}\n"
                                   "paths:\n  themes_dir: /mnt/user/data/media/themes\n")
    (cd / "cookies.txt").write_text("# live cookies\n")
    return db, cd


# ── the diff ─────────────────────────────────────────────────────────

def test_config_diff_lists_only_differing_keys_and_masks_secrets():
    live = "plex:\n  url: http://a\n  token: A\nnotifications:\n  apprise_urls: [discord://x]\nruntime:\n  log_level: INFO\n"
    other = "plex:\n  url: http://b\n  token: B\nnotifications:\n  apprise_urls: [discord://y]\nruntime:\n  log_level: INFO\nnew:\n  key: 1\n"
    d = {r["key"]: r for r in bundle.config_diff(live, other)}
    assert set(d) == {"plex.url", "plex.token", "notifications.apprise_urls", "new.key"}
    # v0.51.341: reversed secret False→True — plex.url joined the userinfo mask; a credential-free URL still shows whole
    assert d["plex.url"] == {"key": "plex.url", "secret": True, "live": "http://a", "bundle": "http://b"}
    assert d["plex.token"]["secret"] and d["plex.token"]["live"] == bundle.MASK and d["plex.token"]["bundle"] == bundle.MASK
    # v0.51.339: apprise URLs mask as GET /api/config masks them — the scheme shows, the token never does
    ap = d["notifications.apprise_urls"]
    assert ap["secret"] and ap["live"] == ap["bundle"] == '["discord://***"]', "webhook URLs carry tokens"
    assert d["new.key"]["live"] == "(unset)" and d["new.key"]["bundle"] == "1"
    assert "B" not in json.dumps(d) and "discord://x" not in json.dumps(d) and "discord://y" not in json.dumps(d)


def test_config_diff_is_empty_when_a_side_does_not_parse():
    # v0.51.339: reversed — this pinned a false diff ("a" unset → 1) for YAML that does not parse
    assert bundle.config_diff("a: [unclosed", "a: 1") == []
    assert bundle.config_diff("a: 1", "a: [unclosed") == []


# ── inspect ───────────────────────────────────────────────────────────

def test_inspect_accepts_a_real_bundle(tmp_path):
    b = _make_bundle(tmp_path)
    c = bundle.inspect_bundle(b)
    assert c.ok and c.has_config and c.has_cookies and c.db.ok and c.manifest["format"] == 1


def _rewrite(b: Path, out: Path, mutate):
    """Re-pack a bundle after mutating its members dict {name: bytes}."""
    members = {}
    with tarfile.open(b, "r:gz") as tar:
        for m in tar.getmembers():
            members[m.name] = tar.extractfile(m).read()
    mutate(members)
    import io
    with tarfile.open(out, "w:gz") as tar:
        for name, data in members.items():
            ti = tarfile.TarInfo(name)
            ti.size = len(data)
            tar.addfile(ti, io.BytesIO(data))


def test_inspect_refuses_newer_format_extra_member_bad_checksum_and_junk(tmp_path):
    b = _make_bundle(tmp_path)

    def newer(m):
        j = json.loads(m["manifest.json"]); j["format"] = bundle.BUNDLE_FORMAT + 1
        m["manifest.json"] = json.dumps(j).encode()
    _rewrite(b, tmp_path / "newer.tar.gz", newer)
    c = bundle.inspect_bundle(tmp_path / "newer.tar.gz")
    assert not c.ok and "newer than this build" in c.error

    def extra(m):
        m["../evil.sh"] = b"boom"
    _rewrite(b, tmp_path / "extra.tar.gz", extra)
    c = bundle.inspect_bundle(tmp_path / "extra.tar.gz")
    assert not c.ok and "unexpected member" in c.error

    def tamper(m):
        m["motif.yaml"] = b"plex:\n  token: TAMPERED\n"
    _rewrite(b, tmp_path / "tamper.tar.gz", tamper)
    c = bundle.inspect_bundle(tmp_path / "tamper.tar.gz")
    assert not c.ok and "checksum" in c.error

    junk = tmp_path / "junk.tar.gz"
    junk.write_bytes(b"\x1f\x8b\x08\x00garbage")
    c = bundle.inspect_bundle(junk)
    assert not c.ok and "not a motif bundle" in c.error
    assert not list(tmp_path.glob(".bundle-inspect-*")), "inspect cleans its temp dir"


# ── preview ───────────────────────────────────────────────────────────

def test_preview_reports_manifest_db_diff_and_cookies(tmp_path):
    b = _make_bundle(tmp_path)
    _, cd = _live(tmp_path)
    p = bundle.preview(b, cd / "motif.yaml")
    assert p["name"] == b.name and p["manifest"]["census_rows"] == 1
    assert p["manifest"]["schema_version"] == CURRENT_SCHEMA_VERSION and p["manifest"]["created_at"] == "2026-09-12T04:00:00+00:00"
    assert p["db"] == {"ok": True, "schema_version": CURRENT_SCHEMA_VERSION}
    assert p["config_in_bundle"] and p["cookies"] == "in bundle"
    keys = {r["key"]: r for r in p["config_diff"]}
    assert set(keys) == {"plex.url", "plex.token", "paths.themes_dir"}
    assert keys["plex.token"]["live"] == bundle.MASK and "OLD-TOKEN" not in json.dumps(p) and "LIVE-TOKEN" not in json.dumps(p)
    assert keys["paths.themes_dir"] == {"key": "paths.themes_dir", "secret": False,
                                        "live": "/mnt/user/data/media/themes", "bundle": "/data/media/themes"}


# ── staging, pending, cancel, apply ───────────────────────────────────

def test_stage_stages_db_config_and_cookies(tmp_path):
    b = _make_bundle(tmp_path)
    db, cd = _live(tmp_path)
    c = bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert c.staged == ["database", "config", "cookies"]
    assert db_backup.restore_pending_path(db).exists()
    assert (cd / bundle.CONFIG_PENDING).read_text().startswith("plex:")
    assert (cd / bundle.COOKIES_PENDING).read_text() == "# cookies\n"
    assert bundle.pending_members(db, cd) == ["database", "config", "cookies"]
    assert (cd / "motif.yaml").read_text().find("LIVE-TOKEN") > 0, "nothing live changed"
    assert not list(cd.glob(".bundle-stage-*"))


def test_keep_config_stages_the_db_only_and_clears_stale_config_pendings(tmp_path):
    b = _make_bundle(tmp_path)
    db, cd = _live(tmp_path)
    (cd / bundle.CONFIG_PENDING).write_text("stale")
    c = bundle.stage_bundle_restore(db, cd, b, keep_config=True)
    assert c.staged == ["database"]
    assert bundle.pending_members(db, cd) == ["database"]


def test_cancel_drops_everything(tmp_path):
    b = _make_bundle(tmp_path)
    db, cd = _live(tmp_path)
    bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert bundle.cancel_pending(db, cd) is True
    assert bundle.pending_members(db, cd) == []
    assert bundle.cancel_pending(db, cd) is False


def test_apply_pending_config_swaps_after_prerestore_copies(tmp_path):
    b = _make_bundle(tmp_path)
    db, cd = _live(tmp_path)
    bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    # v0.51.339: retargeted — the cookies leg is apply_pending_cookies onto settings.cookies_file, after get_settings()
    res = bundle.apply_pending_config(cd, now_stamp="20260913-010203")
    assert res["applied"] == ["motif.yaml"] and res["errors"] == {}
    assert res["safety"] == {"motif.yaml": "motif.yaml.prerestore-20260913-010203"}
    ck = bundle.apply_pending_cookies(cd, cd / "cookies.txt", now_stamp="20260913-010203")
    assert ck["applied"] == [str((cd / "cookies.txt").resolve())] and ck["errors"] == {}
    assert ck["safety"] == {str((cd / "cookies.txt").resolve()): "cookies.txt.prerestore-20260913-010203"}
    assert "OLD-TOKEN" in (cd / "motif.yaml").read_text(), "the bundle's config is live"
    assert "LIVE-TOKEN" in (cd / "motif.yaml.prerestore-20260913-010203").read_text(), "the undo copy"
    assert (cd / "cookies.txt").read_text() == "# cookies\n"
    assert (cd / "cookies.txt.prerestore-20260913-010203").read_text() == "# live cookies\n"
    assert not (cd / bundle.CONFIG_PENDING).exists() and not (cd / bundle.COOKIES_PENDING).exists()
    # the DB member still waits for db_backup's own boot hook
    assert db_backup.restore_pending_path(db).exists()
    assert bundle.apply_pending_config(cd, now_stamp="20260913-010204") is None
    assert bundle.apply_pending_cookies(cd, cd / "cookies.txt", now_stamp="20260913-010204") is None


def test_stage_refuses_a_bad_bundle_and_touches_nothing(tmp_path):
    db, cd = _live(tmp_path)
    junk = tmp_path / "junk.tar.gz"
    junk.write_bytes(b"\x1f\x8b\x08\x00garbage")
    with pytest.raises(ValueError):
        bundle.stage_bundle_restore(db, cd, junk, keep_config=False)
    assert bundle.pending_members(db, cd) == []


# ── the API ───────────────────────────────────────────────────────────

@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    (tmp_path / "motif.yaml").write_text("plex:\n  url: http://plex.old:32400\n  token: LIVE-TOKEN\n")
    from app.web.api import create_app
    return TestClient(create_app(settings)), tmp_path


_H = {"X-Authentik-Username": "testadmin"}


def test_upload_saves_the_bundle_and_returns_the_preview_without_staging(app_client, tmp_path):
    client, cd = app_client
    b = _make_bundle(tmp_path / "mk")
    r = client.post("/api/admin/database-restore/upload", headers=_H,
                    files={"file": (b.name, b.read_bytes(), "application/gzip")})
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["ok"] and j["preview"]["name"] == b.name and j["staged"] is False
    assert {x["key"] for x in j["preview"]["config_diff"]} >= {"plex.url", "plex.token"}
    assert "LIVE-TOKEN" not in r.text and "OLD-TOKEN" not in r.text
    assert (cd / "backups" / b.name).exists(), "an uploaded bundle joins the list"
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["pending"] is False
    # the same bytes again: fine (already there); different bytes under the same stamp: refused
    r = client.post("/api/admin/database-restore/upload", headers=_H,
                    files={"file": (b.name, b.read_bytes(), "application/gzip")})
    assert r.status_code == 200
    other = _make_bundle(tmp_path / "mk2", token="ANOTHER")
    r = client.post("/api/admin/database-restore/upload", headers=_H,
                    files={"file": (other.name, other.read_bytes(), "application/gzip")})
    assert r.status_code == 409


def test_restore_by_name_previews_then_confirms_then_cancels(app_client, tmp_path):
    client, cd = app_client
    b = _make_bundle(tmp_path / "mk")
    (cd / "backups").mkdir(exist_ok=True)
    (cd / "backups" / b.name).write_bytes(b.read_bytes())
    r = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H)
    assert r.status_code == 200 and r.json()["staged"] is False and r.json()["preview"]["cookies"] == "in bundle"
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["members"] == []
    r = client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True, "keep_config": False}, headers=_H)
    assert r.status_code == 200 and r.json()["restart_required"] and r.json()["members"] == ["database", "config", "cookies"]
    p = client.get("/api/admin/database-restore/pending", headers=_H).json()
    assert p["pending"] is True and p["members"] == ["database", "config", "cookies"]
    r = client.post("/api/admin/database-restore/cancel", headers=_H)
    assert r.json()["cancelled"] is True
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["members"] == []
    r = client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True, "keep_config": True}, headers=_H)
    assert r.json()["members"] == ["database"]


def test_restore_by_name_of_a_snapshot_still_stages_at_once(app_client):
    client, cd = app_client
    r = client.post("/api/admin/database-backup", headers=_H)
    name = r.json()["backup"]["name"]
    r = client.post("/api/admin/database-restore", json={"name": name}, headers=_H)
    assert r.status_code == 200 and r.json()["restart_required"] and "preview" not in r.json()
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["members"] == ["database"]


def test_upload_refuses_a_bundle_of_a_newer_format(app_client, tmp_path):
    client, _ = app_client
    b = _make_bundle(tmp_path / "mk")

    def newer(m):
        j = json.loads(m["manifest.json"]); j["format"] = bundle.BUNDLE_FORMAT + 1
        m["manifest.json"] = json.dumps(j).encode()
    _rewrite(b, tmp_path / "newer.tar.gz", newer)
    r = client.post("/api/admin/database-restore/upload", headers=_H,
                    files={"file": ("motif-bundle-20260912-040000.tar.gz", (tmp_path / "newer.tar.gz").read_bytes(), "application/gzip")})
    assert r.status_code == 422 and "newer" in r.json()["detail"]


# ── boot order + the surface ──────────────────────────────────────────

def test_main_applies_the_config_swap_before_settings_are_read():
    i = MAIN_PY.index("apply_pending_config(")
    j = MAIN_PY.index("settings = get_settings()")
    assert i < j, "motif.yaml must be swapped before get_settings() reads it"


def test_settings_markup_has_the_preview_card_and_accepts_bundles():
    assert 'accept=".db,.sqlite,.sqlite3,.tar.gz,.tgz"' in SETTINGS_HTML
    for i in ('id="database-restore-preview"', 'id="restore-preview-manifest"', 'id="restore-preview-diff"',
              'id="database-restore-keep-config"', 'id="database-restore-stage-btn"', 'id="database-restore-pending-members"'):
        assert i in SETTINGS_HTML, i
    assert "KEEP MY CURRENT CONFIG" in SETTINGS_HTML


def test_js_previews_bundles_confirms_by_name_and_shows_pending_members():
    i = APP_JS.index("function bindDatabaseBackup() {")
    blk = APP_JS[i:APP_JS.index("\n  function ", i + 40)]  # to the next top-level function
    assert "function showBundlePreview(" in blk
    assert "confirm: true, keep_config:" in blk
    assert "/\\.tar\\.gz$/" in blk or ".tar.gz" in blk
    assert "database-restore-pending-members" in blk
    assert "kind !== 'bundle'" not in blk, "bundle rows carry RESTORE now"


def test_preview_css_exists():
    for cls in (".restore-preview {", ".restore-diff {", ".restore-diff-del", ".restore-diff-add"):
        assert cls in APP_CSS, cls


def test_v0_51_336_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.336: backup bundle, tag 2" in init_py
