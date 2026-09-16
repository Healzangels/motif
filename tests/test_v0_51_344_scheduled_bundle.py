"""v0.51.344: the scheduler and the API write a live bundle through one spelling; a member over its cap is refused before the VACUUM where it can be, and the nightly then takes a plain snapshot."""
from __future__ import annotations

import sqlite3
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from app import __version__
from app.core import bundle, db_backup, events
from app.core.config_file import ConfigFile
from app.core.db import CURRENT_SCHEMA_VERSION, init_db
from tests.test_v0_51_339_bundle_staging_boot import _H, NOW
from tests.test_v0_51_343_backup_upload_retention import api  # noqa: F401 — the (client, cd, settings) fixture

MOUNTED = "plex:\n  url: http://plex:32400\n  token: MOUNTED-TOKEN\n"


def _mounted(tmp_path: Path) -> Path:
    p = tmp_path / "mounted" / "motif.yaml"
    p.parent.mkdir(parents=True)
    p.write_text(MOUNTED)
    return p


def _config_member(p: Path) -> bytes:
    with tarfile.open(p, "r:gz") as t:
        return t.extractfile(bundle.MEMBER_CONFIG).read()


def _cfg(tmp_path: Path) -> Path:
    cd = tmp_path / "cfg"
    cd.mkdir()
    init_db(cd / "motif.db")
    (cd / "motif.yaml").write_text("plex:\n  token: NOT-THE-LOADED-FILE\n")
    return cd


def _job(cd: Path, monkeypatch, **over):
    from app.core import scheduler as sched
    seen: list[tuple[str, str]] = []
    monkeypatch.setattr(sched, "log_event", lambda *a, **k: seen.append((k.get("level"), k.get("message") or "")))
    monkeypatch.setattr(events, "log_event", lambda *a, **k: None)
    settings = SimpleNamespace(**{"db_path": cd / "motif.db", "config_dir": cd, "config_file": SimpleNamespace(path=cd / "motif.yaml"),
                                  "cookies_file": None, "themes_dir": None, "db_backup_enabled": True,
                                  "db_backup_retention": 0, "db_backup_bundle": True, **over})
    return lambda: sched._scheduled_database_backup(settings), seen


# ── PB-002: one spelling ─────────────────────────────────────────────

def test_the_scheduled_bundle_holds_the_loaded_config_file_and_this_builds_versions(tmp_path, monkeypatch):
    cd = _cfg(tmp_path)
    run, _ = _job(cd, monkeypatch, config_file=SimpleNamespace(path=_mounted(tmp_path)))
    run()
    [made] = db_backup.list_backups(cd)
    assert _config_member(cd / "backups" / made.name) == MOUNTED.encode(), "settings.config_file.path, never config_dir/motif.yaml"
    c = bundle.inspect_bundle(cd / "backups" / made.name)
    assert c.ok, c.error
    assert (c.manifest["motif_version"], c.manifest["schema_version"]) == (__version__, CURRENT_SCHEMA_VERSION)


def test_create_bundle_now_holds_the_loaded_config_file_and_this_builds_versions(api, tmp_path, monkeypatch):
    client, cd, settings = api
    monkeypatch.setattr(settings, "_config_file", ConfigFile(_mounted(tmp_path)))
    r = client.post("/api/admin/database-backup?kind=bundle", headers=_H)
    assert r.status_code == 200, r.text
    name = r.json()["backup"]["name"]
    assert _config_member(cd / "backups" / name) == MOUNTED.encode(), "settings.config_file.path, never config_dir/motif.yaml"
    c = bundle.inspect_bundle(cd / "backups" / name)
    assert c.ok, c.error
    assert (c.manifest["motif_version"], c.manifest["schema_version"]) == (__version__, CURRENT_SCHEMA_VERSION)


# ── PB-052: refused before the VACUUM, and the nightly's snapshot ────

def _estimate(db: Path) -> int:
    conn = sqlite3.connect(db)
    try:
        pages, free, size = (conn.execute(f"PRAGMA {p}").fetchone()[0] for p in ("page_count", "freelist_count", "page_size"))
    finally:
        conn.close()
    return (pages - free) * size


def _vacuums(monkeypatch) -> list:
    calls, real = [], db_backup.vacuum_into
    monkeypatch.setattr(db_backup, "vacuum_into", lambda *a: calls.append(a) or real(*a))
    return calls


def _create(db: Path, cd: Path) -> db_backup.BackupFile:
    return bundle.create_bundle(db, cd, config_file=None, cookies_file=None, themes_dir=None, now_stamp=NOW,
                                motif_version="t", schema_version=CURRENT_SCHEMA_VERSION)


def test_a_database_clearly_over_the_cap_is_refused_before_its_vacuum(tmp_path, monkeypatch):
    db = tmp_path / "motif.db"
    init_db(db)
    calls = _vacuums(monkeypatch)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_DB: 1024})
    with pytest.raises(bundle.BundleOverCap) as refused:
        _create(db, tmp_path)
    assert calls == [], "no VACUUM paid for a refusal the estimate already makes"
    assert isinstance(refused.value, ValueError) and "no bundle was written; take a plain snapshot instead" in str(refused.value)
    assert list((tmp_path / "backups").iterdir()) == []


@pytest.mark.parametrize("cap_at", ["just under the estimate", "the snapshot's own size"])
def test_an_estimate_within_the_margin_leaves_the_decision_to_the_vacuum(tmp_path, monkeypatch, cap_at):
    db = tmp_path / "motif.db"
    init_db(db)
    about = _estimate(db)
    snap = tmp_path / "snap.db"
    db_backup.vacuum_into(db, snap)
    size = snap.stat().st_size
    cap = size if cap_at == "the snapshot's own size" else -(-about * 20 // 21)
    assert cap <= about <= cap * 21 // 20, (cap, about, size)
    calls = _vacuums(monkeypatch)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_DB: cap})
    if cap_at == "the snapshot's own size":
        assert _create(db, tmp_path).kind == "bundle" and len(calls) == 1
    else:
        assert size > cap, "the premise: the snapshot itself is over this cap"
        with pytest.raises(bundle.BundleOverCap, match="the database snapshot is "):
            _create(db, tmp_path)
        assert len(calls) == 1 and [p.name for p in (tmp_path / "backups").iterdir()] == []


def test_the_free_pages_a_delete_leaves_behind_never_count_against_the_cap(tmp_path, monkeypatch):
    db = tmp_path / "motif.db"
    init_db(db)
    conn = sqlite3.connect(db)
    try:
        conn.execute("CREATE TABLE fill (b BLOB)")
        conn.executemany("INSERT INTO fill VALUES (?)", [(b"x" * 3000,) for _ in range(600)])
        conn.commit()
        conn.execute("DELETE FROM fill")
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        pages, size = (conn.execute(f"PRAGMA {p}").fetchone()[0] for p in ("page_count", "page_size"))
    finally:
        conn.close()
    snap = tmp_path / "snap.db"
    db_backup.vacuum_into(db, snap)
    cap = snap.stat().st_size
    assert pages * size > cap * 21 // 20, "the premise: counting every page would refuse a database whose snapshot fits"
    calls = _vacuums(monkeypatch)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_DB: cap})
    made = _create(db, tmp_path)
    assert made.kind == "bundle" and len(calls) == 1
    assert [p.name for p in (tmp_path / "backups").iterdir()] == [made.name]


def test_an_estimate_just_past_the_margin_is_refused_before_its_vacuum(tmp_path, monkeypatch):
    db = tmp_path / "motif.db"
    init_db(db)
    cap = _estimate(db) * 20 // 21 - 1
    calls = _vacuums(monkeypatch)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_DB: cap})
    with pytest.raises(bundle.BundleOverCap, match="the database is about "):
        _create(db, tmp_path)
    assert calls == [] and list((tmp_path / "backups").iterdir()) == []


@pytest.mark.parametrize("member", [bundle.MEMBER_DB, bundle.MEMBER_MANIFEST])
def test_a_nightly_bundle_refused_over_a_cap_takes_a_plain_snapshot_instead(tmp_path, monkeypatch, member):
    cd = _cfg(tmp_path)
    run, seen = _job(cd, monkeypatch, db_backup_retention=1)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, member: 16})
    run()
    assert [level for level, _ in seen] == ["WARNING", "INFO"], seen
    said = seen[0][1]
    assert said.startswith("Scheduled backup bundle not written") and said.endswith("a plain database snapshot was taken instead"), said
    assert ("manifest" in said) is (member == bundle.MEMBER_MANIFEST), said
    made = db_backup.list_backups(cd)
    assert [b.kind for b in made] == ["snapshot"] and made[0].name in seen[1][1], (made, seen)
    assert db_backup.inspect_restore_source(cd / "backups" / made[0].name).ok
    assert not list((cd / "backups").glob(".bundle-*"))


@pytest.mark.parametrize("fault", [OSError(28, "No space left on device"), FileExistsError("backup already exists")],
                         ids=["the disk refuses it", "a snapshot holds that second"])
def test_a_fallback_snapshot_that_is_not_written_is_never_said_to_be_taken(tmp_path, monkeypatch, fault):
    cd = _cfg(tmp_path)
    run, seen = _job(cd, monkeypatch, db_backup_retention=1)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_DB: 16})

    def refused(*a, **k):
        raise fault
    monkeypatch.setattr(db_backup, "create_backup", refused)
    run()
    assert db_backup.list_backups(cd) == [] and not list((cd / "backups").glob(".bundle-*"))
    assert not [m for _, m in seen if "taken instead" in m or "created" in m], seen
    if isinstance(fault, FileExistsError):
        return
    assert [level for level, _ in seen] == ["WARNING"] and str(fault) in seen[0][1], seen
