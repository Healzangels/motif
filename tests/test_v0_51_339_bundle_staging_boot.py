"""v0.51.339: review follow-ups for the backup bundle — staging and the boot order.

  1. A snapshot staging drops a bundle's config/cookies pendings; a bundle stages exactly its own members.
  2. At boot the staged database applies FIRST; the config follows only a database that applied.
  3. Restored cookies land on settings.cookies_file (any path), after a pre-restore copy of that file.
  4. The live motif.yaml / cookies file are 0600 after a boot swap, whatever mode the pending had.
  5. The scheduled backup reads settings.db_backup_bundle directly (no getattr shim).
"""
from __future__ import annotations

import logging
import os
import sqlite3
import stat
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.db import CURRENT_SCHEMA_VERSION, init_db

NOW = "20260912-040000"
_H = {"X-Authentik-Username": "testadmin"}
LIVE_YAML = "plex:\n  url: http://plex.old:32400\n  token: LIVE-TOKEN\n"


def _bundle(root: Path, *, token: str = "BUNDLE-TOKEN", cookies: bool = True) -> Path:
    """A real bundle from create_bundle; its DB carries one marker local_files row (tmdb 777)."""
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
    cfg.write_text(f"plex:\n  url: http://plex:32400\n  token: {token}\n")
    ck = src / "cookies.txt"
    ck.write_text("# bundle cookies\n")
    bf = bundle.create_bundle(db, src, config_file=cfg, cookies_file=ck if cookies else None,
                              themes_dir=None, now_stamp=NOW, motif_version="0.51.339",
                              schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


def _live(root: Path) -> tuple[Path, Path]:
    cd = root / "live"
    cd.mkdir(parents=True, exist_ok=True)
    db = cd / "motif.db"
    init_db(db)
    (cd / "motif.yaml").write_text(LIVE_YAML)
    (cd / "cookies.txt").write_text("# live cookies\n")
    return db, cd


def _marker_rows(db: Path) -> int:
    with sqlite3.connect(db) as conn:
        return conn.execute("SELECT COUNT(*) FROM local_files WHERE tmdb_id = 777").fetchone()[0]


# ── 1. staging stages exactly one restore's members ──────────────────

def test_a_bundle_without_cookies_drops_an_earlier_bundles_cookies(tmp_path):
    db, cd = _live(tmp_path)
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "a"), keep_config=False)
    assert bundle.pending_members(db, cd) == ["database", "config", "cookies"]
    second = _bundle(tmp_path / "b", token="SECOND-TOKEN", cookies=False)
    assert bundle.preview(second, cd / "motif.yaml")["cookies"] == "not in bundle"
    c = bundle.stage_bundle_restore(db, cd, second, keep_config=False)
    assert c.staged == ["database", "config"]
    assert bundle.pending_members(db, cd) == ["database", "config"], "the preview said cookies: not in bundle"
    assert "SECOND-TOKEN" in (cd / bundle.CONFIG_PENDING).read_text()


def test_a_refused_bundle_leaves_the_earlier_staging_whole(tmp_path):
    db, cd = _live(tmp_path)
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "a"), keep_config=False)
    junk = tmp_path / "junk.tar.gz"
    junk.write_bytes(b"\x1f\x8b\x08\x00garbage")
    with pytest.raises(ValueError):
        bundle.stage_bundle_restore(db, cd, junk, keep_config=False)
    assert bundle.pending_members(db, cd) == ["database", "config", "cookies"], \
        "a refusal must not split the earlier staging into a DB without its config"
    unparseable = _bundle(tmp_path / "bad", token="[unclosed")  # refused AFTER extraction, not at inspection
    with pytest.raises(ValueError, match="does not parse"):
        bundle.stage_bundle_restore(db, cd, unparseable, keep_config=False)
    assert bundle.pending_members(db, cd) == ["database", "config", "cookies"], \
        "a YAML refusal must not split the earlier staging into a DB without its config"
    assert "BUNDLE-TOKEN" in (cd / bundle.CONFIG_PENDING).read_text()


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "log_event", lambda *a, **k: None)
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    (tmp_path / "motif.yaml").write_text(LIVE_YAML)
    return TestClient(api_mod.create_app(settings)), tmp_path


def _stage_bundle_by_name(client, cd: Path, root: Path) -> None:
    b = _bundle(root)
    (cd / "backups").mkdir(exist_ok=True)
    (cd / "backups" / b.name).write_bytes(b.read_bytes())
    r = client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True, "keep_config": False}, headers=_H)
    assert r.status_code == 200 and r.json()["members"] == ["database", "config", "cookies"], r.text


def _pending(client) -> list[str]:
    return client.get("/api/admin/database-restore/pending", headers=_H).json()["members"]


def test_a_listed_snapshot_after_a_bundle_stages_the_database_only(app_client, tmp_path):
    client, cd = app_client
    _stage_bundle_by_name(client, cd, tmp_path / "mk")
    (cd / "backups" / "motif-20260101-000000.db").write_bytes(b"not a sqlite database at all")
    r = client.post("/api/admin/database-restore", json={"name": "motif-20260101-000000.db"}, headers=_H)
    assert r.status_code == 422, r.text
    assert _pending(client) == ["database", "config", "cookies"], \
        "a refused snapshot must not split the bundle's staging into a DB without its config"
    name = client.post("/api/admin/database-backup", headers=_H).json()["backup"]["name"]
    r = client.post("/api/admin/database-restore", json={"name": name}, headers=_H)
    assert r.status_code == 200 and r.json()["restart_required"], r.text
    assert _pending(client) == ["database"], "the bundle's config/cookies would apply beside the snapshot's DB"
    assert not (cd / bundle.CONFIG_PENDING).exists() and not (cd / bundle.COOKIES_PENDING).exists()


def test_an_uploaded_snapshot_after_a_bundle_stages_the_database_only(app_client, tmp_path):
    client, cd = app_client
    _stage_bundle_by_name(client, cd, tmp_path / "mk")
    r = client.post("/api/admin/database-restore/upload", headers=_H,
                    files={"file": ("snap.db", b"not a sqlite database at all", "application/octet-stream")})
    assert r.status_code == 422, r.text
    assert _pending(client) == ["database", "config", "cookies"], \
        "a refused upload must not split the bundle's staging into a DB without its config"
    fresh = tmp_path / "fresh.db"
    init_db(fresh)
    snap = tmp_path / "snap.db"
    db_backup.vacuum_into(fresh, snap)  # a real snapshot: init_db's own file keeps its tables in the WAL
    r = client.post("/api/admin/database-restore/upload", headers=_H,
                    files={"file": ("snap.db", snap.read_bytes(), "application/octet-stream")})
    assert r.status_code == 200 and r.json()["restart_required"], r.text
    assert _pending(client) == ["database"]


# ── 2 + 3. the boot order, through main() ────────────────────────────

class _Booted(Exception):
    """Raised from the stubbed init_db — main() got past the restore section."""


def _boot(monkeypatch, cd: Path, seen: list[str] | None = None) -> Path:
    """Run main() up to init_db with cd as the env's config dir; returns the db path init_db got."""
    from app import config as config_mod
    from app import main as main_mod
    monkeypatch.setattr(config_mod, "_DEFAULT_CONFIG_DIR", cd)

    def get_settings():
        if seen is not None:
            seen.append((cd / "motif.yaml").read_text())
        return config_mod.Settings(data_dir=cd / "data")

    def init_db_stub(db_path):
        raise _Booted(db_path)
    monkeypatch.setattr(main_mod, "get_settings", get_settings)
    monkeypatch.setattr(main_mod, "configure_logging", lambda *a, **k: None)
    monkeypatch.setattr(main_mod, "init_db", init_db_stub)
    with pytest.raises(_Booted) as ei:
        main_mod.main()
    return ei.value.args[0]


def test_boot_applies_database_then_config_before_settings_then_cookies(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    db, cd = _live(tmp_path)
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))  # the Dockerfile's pin; the YAML default is a literal /config path
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk"), keep_config=False)
    seen: list[str] = []
    assert _boot(monkeypatch, cd, seen) == db, "the DB applied at the path Settings.db_path names"
    assert _marker_rows(db) == 1, "the bundle's database is live"
    assert "BUNDLE-TOKEN" in seen[0], "motif.yaml was swapped before get_settings() read it"
    assert (cd / "cookies.txt").read_text() == "# bundle cookies\n"
    assert bundle.pending_members(db, cd) == []
    assert "Database restored at boot" in caplog.text and "Config restored at boot" in caplog.text


def test_boot_keeps_the_config_staged_while_its_database_cannot_apply(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    db, cd = _live(tmp_path)
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))  # the Dockerfile's pin; the YAML default is a literal /config path
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk"), keep_config=False)
    real_create_backup = db_backup.create_backup

    def disk_full(*a, **k):
        raise OSError(28, "No space left on device")
    monkeypatch.setattr(db_backup, "create_backup", disk_full)
    seen: list[str] = []
    _boot(monkeypatch, cd, seen)
    assert _marker_rows(db) == 0, "the pre-restore copy failed, so the DB swap aborted"
    assert "LIVE-TOKEN" in seen[0] and "LIVE-TOKEN" in (cd / "motif.yaml").read_text(), \
        "the other box's config must not go live beside this box's database"
    assert (cd / "cookies.txt").read_text() == "# live cookies\n"
    assert bundle.pending_members(db, cd) == ["database", "config", "cookies"], "all three wait for a retry"
    waits = [r for r in caplog.records if "waits for its database" in r.getMessage()]
    assert waits and waits[0].levelno == logging.WARNING and "config, cookies" in waits[0].getMessage()
    # the fault cleared: the next boot applies all three together
    monkeypatch.setattr(db_backup, "create_backup", real_create_backup)
    seen.clear()
    _boot(monkeypatch, cd, seen)
    assert _marker_rows(db) == 1 and "BUNDLE-TOKEN" in seen[0]
    assert (cd / "cookies.txt").read_text() == "# bundle cookies\n"
    assert bundle.pending_members(db, cd) == []


def test_boot_drops_the_config_of_a_database_rejected_at_boot(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    db, cd = _live(tmp_path)
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))  # the Dockerfile's pin; the YAML default is a literal /config path
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk"), keep_config=False)
    db_backup.restore_pending_path(db).write_bytes(b"corrupted after staging")
    seen: list[str] = []
    _boot(monkeypatch, cd, seen)
    assert "LIVE-TOKEN" in seen[0] and (cd / "cookies.txt").read_text() == "# live cookies\n"
    assert bundle.pending_members(db, cd) == [], "db_backup discarded the bad DB; its config goes with it"
    dropped = [r for r in caplog.records if "Staged config restore dropped" in r.getMessage()]
    assert dropped and dropped[0].levelno == logging.ERROR
    seen.clear()
    _boot(monkeypatch, cd, seen)
    assert "LIVE-TOKEN" in seen[0], "a later boot never swaps in a config whose database was rejected"


def test_boot_with_nothing_staged_says_so_at_info(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    _, cd = _live(tmp_path)
    _boot(monkeypatch, cd)
    info = {r.getMessage() for r in caplog.records if r.levelno == logging.INFO}
    assert {"no staged database restore pending", "no staged config restore pending",
            "no staged cookies restore pending"} <= info


def test_boot_restores_cookies_to_the_configured_cookies_file(tmp_path, monkeypatch):
    db, cd = _live(tmp_path)
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    target = secrets / "yt-cookies.txt"
    target.write_text("# the cookies yt-dlp reads\n")
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(target))
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk"), keep_config=False)
    _boot(monkeypatch, cd)
    assert target.read_text() == "# bundle cookies\n", "restored where settings.cookies_file points"
    keeps = list(secrets.glob("yt-cookies.txt.prerestore-*"))
    assert len(keeps) == 1 and keeps[0].read_text() == "# the cookies yt-dlp reads\n", "the undo copy is of the REAL file"
    assert (cd / "cookies.txt").read_text() == "# live cookies\n", "nothing reads config_dir/cookies.txt here"
    assert not (cd / bundle.COOKIES_PENDING).exists()
    assert not list(secrets.glob("*.restore-tmp"))


def test_apply_pending_cookies_writes_through_a_symlinked_cookies_file(tmp_path):
    _, cd = _live(tmp_path)
    real = tmp_path / "real"
    real.mkdir()
    (real / "cookies.txt").write_text("# linked live\n")
    link = cd / "cookies.txt"
    link.unlink()
    link.symlink_to(real / "cookies.txt")
    (cd / bundle.COOKIES_PENDING).write_text("# bundle cookies\n")
    res = bundle.apply_pending_cookies(cd, link, now_stamp="20260913-010203")
    assert res["errors"] == {} and res["applied"] == [os.path.realpath(real / "cookies.txt")]
    assert link.is_symlink(), "the operator's link survives the restore"
    assert (real / "cookies.txt").read_text() == "# bundle cookies\n"
    assert (real / "cookies.txt.prerestore-20260913-010203").read_text() == "# linked live\n"


def test_apply_pending_cookies_failure_keeps_live_and_pending(tmp_path, caplog):
    _, cd = _live(tmp_path)
    (cd / bundle.COOKIES_PENDING).write_text("# bundle cookies\n")
    missing = tmp_path / "not-mounted" / "cookies.txt"
    res = bundle.apply_pending_cookies(cd, missing, now_stamp="20260913-010203")
    assert res["applied"] == [] and list(res["errors"]) == [os.path.realpath(missing)]
    assert (cd / bundle.COOKIES_PENDING).exists(), "kept for a retry"
    assert any(r.levelno == logging.ERROR and "not restored" in r.getMessage() for r in caplog.records)


# ── 4. modes after the boot swap ─────────────────────────────────────

def test_boot_swaps_leave_config_and_cookies_0600_even_from_a_0644_pending(tmp_path):
    _, cd = _live(tmp_path)
    old = os.umask(0o022)
    try:
        for name, text in ((bundle.CONFIG_PENDING, "plex:\n  token: OLD-BUILD\n"),
                           (bundle.COOKIES_PENDING, "# staged by an older build\n")):
            (cd / name).write_text(text)
            (cd / name).chmod(0o644)  # what .336-.338 staged
        assert bundle.apply_pending_config(cd, now_stamp="20260913-010203")["applied"] == ["motif.yaml"]
        target = tmp_path / "elsewhere" / "cookies.txt"
        target.parent.mkdir()
        assert bundle.apply_pending_cookies(cd, target, now_stamp="20260913-010203")["errors"] == {}
        assert stat.S_IMODE((cd / "motif.yaml").stat().st_mode) == 0o600
        assert stat.S_IMODE(target.stat().st_mode) == 0o600
    finally:
        os.umask(old)


# ── 5. the scheduler reads the toggle, no shim ───────────────────────

def test_scheduled_backup_without_the_bundle_toggle_fails_loudly(tmp_path, monkeypatch):
    from app.core import scheduler as sched
    cd = tmp_path / "cfg"
    cd.mkdir()
    db = cd / "motif.db"
    init_db(db)

    class S:  # a settings surface that lost the property: never silently a bare snapshot
        db_path = db
        config_dir = cd
        cookies_file = cd / "cookies.txt"
        themes_dir = None
        db_backup_enabled = True
        db_backup_retention = 0

    logged: list[tuple[str, str]] = []
    monkeypatch.setattr(sched, "log_event", lambda *a, **k: logged.append((k.get("level"), k.get("message"))))
    sched._scheduled_database_backup(S())
    assert not (cd / "backups").exists() or not any((cd / "backups").iterdir())
    assert logged and logged[0][0] == "WARNING" and "failed" in logged[0][1], logged


# ── merge: a share that refuses chmod keeps the bundle and the restore ──

def _refuse(monkeypatch, target, name="chmod"):
    def refuse(*a, **k):
        raise PermissionError(1, "Operation not permitted")
    monkeypatch.setattr(target, name, refuse)


def test_an_extraction_chmod_refusal_never_refuses_the_bundle(tmp_path, monkeypatch, caplog):
    b = _bundle(tmp_path)
    db, cd = _live(tmp_path)
    _refuse(monkeypatch, Path)
    with caplog.at_level(logging.WARNING, logger=bundle.log.name):
        assert bundle.inspect_bundle(b).ok, "the 0600 mode is a belt — a share that refuses it keeps the bundle"
        c = bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert c.staged == ["database", "config", "cookies"]
    assert any("could not chmod 0600" in r.getMessage() for r in caplog.records)


def test_a_chmod_refusing_share_still_restores_cookies(tmp_path, monkeypatch):
    _, cd = _live(tmp_path)
    target = tmp_path / "share" / "cookies.txt"
    target.parent.mkdir()
    target.write_text("# live\n")
    (cd / bundle.COOKIES_PENDING).write_text("# bundle cookies\n")
    _refuse(monkeypatch, os)  # os.chmod: what Path.chmod and shutil.copystat both call
    res = bundle.apply_pending_cookies(cd, target, now_stamp="20260913-010203")
    assert res["errors"] == {} and target.read_text() == "# bundle cookies\n"
    assert (target.parent / "cookies.txt.prerestore-20260913-010203").read_text() == "# live\n"
    assert not (cd / bundle.COOKIES_PENDING).exists()


def test_apply_pending_cookies_never_raises_on_a_path_with_no_file_name(tmp_path):
    _, cd = _live(tmp_path)
    (cd / bundle.COOKIES_PENDING).write_text("# bundle cookies\n")
    res = bundle.apply_pending_cookies(cd, Path("/"), now_stamp="20260913-010203")
    assert res["applied"] == [] and res["errors"]
    assert (cd / bundle.COOKIES_PENDING).exists(), "kept for a retry"
