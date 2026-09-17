"""v0.51.335: backup bundle, tag 1 — create + schedule (feature D).

Spec: docs/specs/BACKUP_BUNDLE_SPEC.md. A bundle is one tar.gz next to the
snapshots: a VACUUM INTO copy of motif.db, motif.yaml (secrets as-is),
cookies.txt when present, and a manifest with versions, counts, member
checksums and a themes census. Bundles share the snapshot list, name gate,
retention and the four endpoints; the scheduled job writes one instead of a
bare snapshot when database_backup.bundle is on.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import tarfile
from datetime import datetime, timezone
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
NOW = "20260912-040000"


def _seed(db: Path) -> None:
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO themes (media_type, tmdb_id, title, upstream_source, last_seen_sync_at, first_seen_sync_at) "
                     "VALUES ('tv', 777, 'Cowboy Bebop', 'imdb', 'x', 'x')")
        conn.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, file_size, file_sha256, "
                     "downloaded_at, source_video_id, provenance, source_kind) "
                     "VALUES ('tv', 777, '3', 'tv/Cowboy Bebop (1998)/theme.mp3', 4587123, 'abc', 'x', 'at-CowboyBebop-OP1', 'manual', 'url')")
        conn.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, file_size, downloaded_at, source_video_id, source_kind) "
                     "VALUES ('movie', 42, '1', 'movies/Akira (1988)/theme.mp3', 999, 'x', '', 'adopt')")
        conn.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind, provenance, placed_at) "
                     "VALUES ('tv', 777, '3', '/x', 'hardlink', 'manual', 'x')")
        conn.commit()


def _prep(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    db = tmp_path / "motif.db"
    _seed(db)
    cfg = tmp_path / "motif.yaml"
    cfg.write_text("plex:\n  token: SECRET-TOKEN\n")
    cookies = tmp_path / "cookies.txt"
    cookies.write_text("# Netscape HTTP Cookie File\n")
    (tmp_path / ".session_key").write_text("nope")
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "motif.log").write_text("log")
    return db, cfg, cookies, tmp_path


def _create(tmp_path: Path, **over):
    db, cfg, cookies, cd = _prep(tmp_path)
    kw = dict(config_file=cfg, cookies_file=cookies, themes_dir=tmp_path / "data" / "themes",
              now_stamp=NOW, motif_version="0.51.335", schema_version=CURRENT_SCHEMA_VERSION)
    kw.update(over)
    return bundle.create_bundle(db, cd, **kw), cd


# ── the module ────────────────────────────────────────────────────────

def test_bundle_holds_exactly_the_four_members_and_nothing_else(tmp_path):
    bf, cd = _create(tmp_path)
    assert bf.name == f"motif-bundle-{NOW}.tar.gz" and bf.kind == "bundle"
    with tarfile.open(cd / "backups" / bf.name, "r:gz") as tar:
        names = sorted(tar.getnames())
    assert names == sorted(bundle.MEMBERS), names
    assert not any(n.startswith(".") or "/" in n for n in names), "member names only, no archived paths"
    assert not list((cd / "backups").glob(".bundle-*")), "the temp dir is cleaned"
    assert not list((cd / "backups").glob("*.part"))


def test_manifest_checksums_match_the_members(tmp_path):
    bf, cd = _create(tmp_path)
    with tarfile.open(cd / "backups" / bf.name, "r:gz") as tar:
        m = json.loads(tar.extractfile(bundle.MEMBER_MANIFEST).read())
        for name, meta in m["members"].items():
            data = tar.extractfile(name).read()
            assert meta["size"] == len(data), name
            assert meta["sha256"] == hashlib.sha256(data).hexdigest(), name
    assert m["kind"] == "motif-bundle" and m["format"] == bundle.BUNDLE_FORMAT
    assert m["motif_version"] == "0.51.335" and m["schema_version"] == CURRENT_SCHEMA_VERSION
    assert m["created_at"] == "2026-09-12T04:00:00+00:00"
    assert m["members"][bundle.MEMBER_CONFIG]["secrets"] == "as-is"
    assert m["paths"]["config_dir"] == str(cd) and m["paths"]["themes_dir"].endswith("themes")


def test_snapshot_inside_the_bundle_is_a_consistent_copy(tmp_path):
    bf, cd = _create(tmp_path)
    with tarfile.open(cd / "backups" / bf.name, "r:gz") as tar:
        raw = tar.extractfile(bundle.MEMBER_DB).read()
    out = tmp_path / "out.db"
    out.write_bytes(raw)
    with sqlite3.connect(out) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT COUNT(*) FROM local_files").fetchone()[0] == 2


def test_census_and_counts_mirror_local_files(tmp_path):
    bf, cd = _create(tmp_path)
    c = bundle.inspect_bundle(cd / "backups" / bf.name)
    assert c.ok, c.error
    m = c.manifest
    assert m["counts"]["local_files"] == 2 and m["counts"]["placements"] == 1 and m["counts"]["themes"] == 1
    census = {c["path"]: c for c in m["themes_census"]}
    bebop = census["tv/Cowboy Bebop (1998)/theme.mp3"]
    assert bebop == {
        "media_type": "tv", "tmdb_id": 777, "section_id": "3", "edition_key": "",
        "path": "tv/Cowboy Bebop (1998)/theme.mp3", "size": 4587123, "sha256": "abc",
        "source_kind": "url", "source_video_id": "at-CowboyBebop-OP1",
        "provenance": "manual", "placement_kind": "hardlink",
    }
    akira = census["movies/Akira (1988)/theme.mp3"]
    assert akira["source_kind"] == "adopt" and akira["sha256"] is None and akira["placement_kind"] is None


def test_config_secrets_travel_as_is_and_cookies_are_optional(tmp_path):
    bf, cd = _create(tmp_path)
    with tarfile.open(cd / "backups" / bf.name, "r:gz") as tar:
        assert b"SECRET-TOKEN" in tar.extractfile(bundle.MEMBER_CONFIG).read()
    # a second bundle with no cookies file: the member and its manifest entry are absent
    bf2, _ = _create(tmp_path / "two", cookies_file=tmp_path / "two" / "missing.txt", now_stamp="20260912-050000")
    with tarfile.open(tmp_path / "two" / "backups" / bf2.name, "r:gz") as tar:
        assert bundle.MEMBER_COOKIES not in tar.getnames()
    c = bundle.inspect_bundle(tmp_path / "two" / "backups" / bf2.name)
    assert c.ok, c.error
    assert bundle.MEMBER_COOKIES not in c.manifest["members"]


def test_same_second_never_clobbers_and_bad_stamp_refused(tmp_path):
    _create(tmp_path)
    db, cfg, cookies, cd = tmp_path / "motif.db", tmp_path / "motif.yaml", tmp_path / "cookies.txt", tmp_path
    with pytest.raises(FileExistsError):
        bundle.create_bundle(db, cd, config_file=cfg, cookies_file=cookies, themes_dir=None,
                             now_stamp=NOW, motif_version="x", schema_version=1)
    with pytest.raises(ValueError):
        bundle.create_bundle(db, cd, config_file=cfg, cookies_file=cookies, themes_dir=None,
                             now_stamp="../evil", motif_version="x", schema_version=1)
    assert len(list((cd / "backups").iterdir())) == 1


def test_mid_write_failure_leaves_nothing_behind(tmp_path, monkeypatch):
    """A tar that fails half-way must leave no bundle, no .part and no temp
    dir — otherwise list_backups would count a corpse toward retention."""
    db, cfg, cookies, cd = _prep(tmp_path)
    real_open = bundle.tarfile.open

    def boom(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(bundle.tarfile, "open", boom)
    with pytest.raises(OSError):
        bundle.create_bundle(db, cd, config_file=cfg, cookies_file=cookies, themes_dir=None,
                             now_stamp=NOW, motif_version="x", schema_version=1)
    monkeypatch.setattr(bundle.tarfile, "open", real_open)
    assert sorted(p.name for p in (cd / "backups").iterdir()) == []


def test_bundle_is_renamed_into_place_never_copied(tmp_path, monkeypatch):
    """The final step is an atomic rename of the finished .part; a copy could
    leave a half-written bundle under the listed name."""
    import shutil
    db, cfg, cookies, cd = _prep(tmp_path)

    def no_copy(*a, **k):
        raise AssertionError("create_bundle must rename the finished bundle into place, never copy it")
    # v0.51.339: behavioural — the old whole-module "shutil.copyfile(" ban also forbade the cookies restore's temp copy.
    for name in ("copy", "copy2", "copyfile", "move"):
        monkeypatch.setattr(shutil, name, no_copy)
    renames = []
    real_replace = Path.replace

    def replace(self, target):
        renames.append((self.name, Path(target).name))
        return real_replace(self, target)
    monkeypatch.setattr(Path, "replace", replace)
    bf = bundle.create_bundle(db, cd, config_file=cfg, cookies_file=cookies,
                              themes_dir=tmp_path / "data" / "themes", now_stamp=NOW,
                              motif_version="0.51.335", schema_version=CURRENT_SCHEMA_VERSION)
    assert (f"{bf.name}.part", bf.name) in renames


# ── the snapshot machinery admits bundles ────────────────────────────

def test_name_gate_and_kinds():
    assert db_backup.is_backup_name(f"motif-bundle-{NOW}.tar.gz")
    assert db_backup.kind_of(f"motif-bundle-{NOW}.tar.gz") == "bundle"
    assert db_backup.kind_of(f"motif-{NOW}.db") == "snapshot"
    assert db_backup.kind_of(f"motif-prerestore-{NOW}.db") == "prerestore"
    for bad in (f"../motif-bundle-{NOW}.tar.gz", f"motif-bundle-{NOW}.tar", "motif-bundle-x.tar.gz",
                f"motif-bundle-{NOW}.tar.gz/../motif.db", "manifest.json"):
        assert not db_backup.is_backup_name(bad), bad
        assert db_backup.kind_of(bad) is None, bad


def test_list_sorts_both_kinds_by_stamp_with_kind(tmp_path):
    bf, cd = _create(tmp_path)
    db_backup.create_backup(tmp_path / "motif.db", cd, now_stamp="20260912-050000")
    db_backup.create_backup(tmp_path / "motif.db", cd, now_stamp="20260911-040000", prerestore=True)
    rows = db_backup.list_backups(cd)
    assert [(r.name, r.kind) for r in rows] == [
        ("motif-20260912-050000.db", "snapshot"),
        (f"motif-bundle-{NOW}.tar.gz", "bundle"),
        ("motif-prerestore-20260911-040000.db", "prerestore"),
    ]


def test_prune_counts_bundles_and_snapshots_together_never_prerestore(tmp_path):
    bf, cd = _create(tmp_path)  # 04:00 bundle
    db = tmp_path / "motif.db"
    db_backup.create_backup(db, cd, now_stamp="20260912-050000")
    db_backup.create_backup(db, cd, now_stamp="20260912-060000")
    db_backup.create_backup(db, cd, now_stamp="20260901-000000", prerestore=True)
    removed = db_backup.prune_backups(cd, retention=2, now_stamp=datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"))  # v0.51.344: prune sets aside a stamp after now
    assert removed == [f"motif-bundle-{NOW}.tar.gz"], "the oldest routine file — a bundle — goes; the pre-restore copy stays"
    assert sorted(p.name for p in (cd / "backups").iterdir()) == [
        "motif-20260912-050000.db", "motif-20260912-060000.db", "motif-prerestore-20260901-000000.db",
    ]


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
    from app.web.api import create_app
    return TestClient(create_app(settings)), tmp_path


_H = {"X-Authentik-Username": "testadmin"}


def test_create_bundle_endpoint_lists_downloads_and_deletes(app_client):
    client, tmp = app_client
    r = client.post("/api/admin/database-backup?kind=bundle", headers=_H)
    assert r.status_code == 200, r.text
    b = r.json()["backup"]
    assert b["kind"] == "bundle" and re.fullmatch(r"motif-bundle-\d{8}-\d{6}\.tar\.gz", b["name"])
    r = client.get("/api/admin/database-backups", headers=_H)
    rows = r.json()["backups"]
    assert [x["kind"] for x in rows] == ["bundle"]
    r = client.get(f"/api/admin/database-backup/download/{b['name']}", headers=_H)
    assert r.status_code == 200 and r.headers["content-type"].startswith("application/gzip")
    assert r.content[:2] == b"\x1f\x8b", "a gzip stream"
    r = client.post("/api/admin/database-backup/delete", json={"name": b["name"]}, headers=_H)
    assert r.status_code == 200
    assert client.get("/api/admin/database-backups", headers=_H).json()["backups"] == []


def test_create_endpoint_default_kind_is_still_a_snapshot(app_client):
    client, _ = app_client
    r = client.post("/api/admin/database-backup", headers=_H)
    assert r.status_code == 200 and r.json()["backup"]["kind"] == "snapshot"
    r = client.post("/api/admin/database-backup?kind=zip", headers=_H)
    assert r.status_code == 400


# ── the schedule ──────────────────────────────────────────────────────

def test_scheduled_job_writes_a_bundle_when_the_toggle_is_on(tmp_path, monkeypatch):
    from app.core import scheduler as sched
    from app.core import events
    db, cfg, cookies, cd = _prep(tmp_path)

    class S:  # the settings surface the job reads
        db_path = db
        config_dir = cd
        config_file = type("CF", (), {"path": cfg})  # v0.51.344: create_bundle_for reads the loaded config file's path
        cookies_file = cookies
        themes_dir = cd / "data" / "themes"
        db_backup_enabled = True
        db_backup_retention = 0
        db_backup_bundle = True

    logged: list[str] = []
    monkeypatch.setattr(sched, "log_event", lambda *a, **k: logged.append(k.get("message") or (a[3] if len(a) > 3 else "")))
    sched._scheduled_database_backup(S())
    # v0.51.344: retargeted from "exactly one file in the dir" — R1-F4's one-time naming check leaves its marker there; the invariant is one listed bundle and no temp
    names = [b.name for b in db_backup.list_backups(cd)]
    assert len(names) == 1 and names[0].startswith("motif-bundle-") and names[0].endswith(".tar.gz")
    assert not [p.name for p in (cd / "backups").iterdir() if p.name.startswith(".bundle-")], "no temp directory left behind"
    assert any("bundle" in (m or "") for m in logged), logged
    # the toggle off → a bare snapshot, unchanged behaviour
    S.db_backup_bundle = False
    sched._scheduled_database_backup(S())
    names = sorted(p.name for p in (cd / "backups").iterdir())
    assert any(n.endswith(".db") for n in names)


def test_config_field_default_off_and_env_mirror():
    from app.core.config_file import ENV_BINDINGS, DatabaseBackupConfig
    assert DatabaseBackupConfig().bundle is False
    assert ("MOTIF_DB_BACKUP_BUNDLE", "database_backup.bundle") in [(e[0], e[1]) for e in ENV_BINDINGS]


# ── the settings surface ──────────────────────────────────────────────

def test_settings_markup_has_the_button_and_the_toggle():
    assert 'id="database-bundle-create-btn">// CREATE BUNDLE NOW</button>' in SETTINGS_HTML
    assert 'data-cfg-field="database_backup.bundle"' in SETTINGS_HTML
    assert "WRITE A BUNDLE" in SETTINGS_HTML
    assert "as sensitive as" in SETTINGS_HTML, "the secrets warning"


def test_list_rows_carry_a_kind_chip_and_a_restore():
    i = APP_JS.index("function bindDatabaseBackup() {")
    blk = APP_JS[i:APP_JS.index("async function refreshPending()", i)]
    assert "tier-badge tier-badge-${kind}" in blk
    assert "data-backup-restore=" in blk  # v0.51.336: bundles restore too (through a preview)
    assert "'/api/admin/database-backup?kind=bundle'" in APP_JS
    assert "// BUNDLING…" in APP_JS


def test_kind_chip_tones_exist_on_the_tier_badge_family():
    for cls in (".tier-badge-bundle", ".tier-badge-snapshot", ".tier-badge-prerestore"):
        assert f"\n{cls} {{" in APP_CSS, cls
    assert APP_CSS.index("\n.tier-badge {") < APP_CSS.index("\n.tier-badge-bundle {")


def test_v0_51_335_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.335: backup bundle, tag 1" in init_py
