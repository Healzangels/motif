"""v0.51.341: restore staging and the boot hooks, hardened.

  1. A share that refuses chmod still stages and restores (copyfile + a tolerant chmod); the DB pending keeps its source's mode where allowed.
  2. A single-file bind mount takes the restored bytes in place; one pre-restore copy per staged pending, however many boots retry.
  3. A config pending that cannot be removed never gets a new database staged beside it; boot logs and continues.
  4. Every stage and cancel runs whole under one lock.
  5. What the restore hooks log before configure_logging reaches its handlers (stdout + motif.log), once.
  6. A failed config swap is an ERROR and the cookies wait for it; one outcome line per member.
"""
from __future__ import annotations

import contextlib
import errno
import hashlib
import logging
import os
import stat
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from tests.test_v0_51_339_bundle_staging_boot import LIVE_YAML, _H, _boot, _bundle, _live, _marker_rows

STAMP = "20260913-010203"
LISTED_SNAPSHOT = "motif-20260101-000000.db"


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _snapshot(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    fresh = root / "fresh.db"
    init_db(fresh)
    snap = root / "snap.db"
    db_backup.vacuum_into(fresh, snap)  # a real snapshot: init_db's own file keeps its tables in the WAL
    return snap


def _refuse_chmod(monkeypatch) -> None:
    def refuse(*a, **k):
        raise PermissionError(errno.EPERM, "Operation not permitted")
    monkeypatch.setattr(os, "chmod", refuse)  # what Path.chmod and shutil.copystat both call


def _replace_refused_onto(monkeypatch, name: str, err: int) -> None:
    real = os.replace

    def replace(src, dst, *a, **k):
        if Path(dst).name == name:
            raise OSError(err, os.strerror(err))
        return real(src, dst, *a, **k)
    monkeypatch.setattr(os, "replace", replace)


def _unlink_refused_for(monkeypatch, *names: str) -> None:
    real = Path.unlink

    def unlink(self, *a, **k):
        if self.name in names:
            raise PermissionError(errno.EACCES, "Permission denied")
        return real(self, *a, **k)
    monkeypatch.setattr(Path, "unlink", unlink)


# ── 1. a share that refuses chmod ────────────────────────────────────

def test_a_chmod_refusing_share_stages_a_bundle_then_a_snapshot(tmp_path, monkeypatch, caplog):
    db, cd = _live(tmp_path)
    b = _bundle(tmp_path / "mk")
    snap = _snapshot(tmp_path / "src")
    _refuse_chmod(monkeypatch)
    with caplog.at_level(logging.WARNING):
        assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config", "cookies"]
        assert "BUNDLE-TOKEN" in (cd / bundle.CONFIG_PENDING).read_text()
        assert bundle.stage_snapshot_restore(db, cd, snap).ok
    assert bundle.pending_members(db, cd) == ["database"]
    assert _sha(db_backup.restore_pending_path(db)) == _sha(snap)
    assert any("could not copy the mode" in r.getMessage() for r in caplog.records), "the refusal is named, not silent"


def test_the_staged_database_keeps_its_sources_mode_where_the_share_allows(tmp_path):
    db, _ = _live(tmp_path)
    snap = _snapshot(tmp_path / "src")
    snap.chmod(0o600)  # an uploaded snapshot's mkstemp file
    old = os.umask(0o022)
    try:
        db_backup.stage_restore(db, snap)
    finally:
        os.umask(old)
    assert stat.S_IMODE(db_backup.restore_pending_path(db).stat().st_mode) == 0o600, "copy2's mode still rides the pending"


def test_a_chmod_refusing_share_still_swaps_the_config_in_at_boot(tmp_path, monkeypatch):
    _, cd = _live(tmp_path)
    (cd / bundle.CONFIG_PENDING).write_text("plex:\n  token: BUNDLE-TOKEN\n")
    _refuse_chmod(monkeypatch)
    res = bundle.apply_pending_config(cd, now_stamp=STAMP)
    assert res["applied"] == ["motif.yaml"] and res["errors"] == {}
    assert "BUNDLE-TOKEN" in (cd / "motif.yaml").read_text()
    assert (cd / f"motif.yaml.prerestore-{STAMP}").read_text() == LIVE_YAML


def test_the_pre_restore_copies_of_the_replaced_secrets_are_owner_only(tmp_path):
    _, cd = _live(tmp_path)
    (cd / bundle.CONFIG_PENDING).write_text("plex:\n  token: BUNDLE-TOKEN\n")
    (cd / bundle.COOKIES_PENDING).write_text("# bundle cookies\n")
    old = os.umask(0o022)
    try:
        for live in (cd / "motif.yaml", cd / "cookies.txt"):
            live.chmod(0o644)  # group/other-readable live files: a copy that inherits their mode would not pass
        assert bundle.apply_pending_config(cd, now_stamp=STAMP)["applied"] == ["motif.yaml"]
        assert bundle.apply_pending_cookies(cd, cd / "cookies.txt", now_stamp=STAMP)["errors"] == {}
    finally:
        os.umask(old)
    copies = sorted(cd.glob("*.prerestore-*"))
    assert [p.name for p in copies] == [f"cookies.txt.prerestore-{STAMP}", f"motif.yaml.prerestore-{STAMP}"]
    for p in copies:  # the old Plex token and the old cookies
        assert stat.S_IMODE(p.stat().st_mode) == 0o600, f"{p.name} is {oct(stat.S_IMODE(p.stat().st_mode))}"


# ── 2. a single-file bind mount ──────────────────────────────────────

def test_a_bind_mounted_cookies_file_is_restored_in_place_with_one_undo_copy(tmp_path, monkeypatch):
    _, cd = _live(tmp_path)
    target = cd / "cookies.txt"
    (cd / bundle.COOKIES_PENDING).write_text("# bundle cookies\n")
    _replace_refused_onto(monkeypatch, "cookies.txt", errno.EBUSY)  # what rename(2) answers onto a mount point
    target.chmod(0o400)  # boot 1: the in-place write is refused as well — the pending waits
    try:
        assert bundle.apply_pending_cookies(cd, target, now_stamp="20260913-010203")["applied"] == []
        assert (cd / bundle.COOKIES_PENDING).exists() and target.read_text() == "# live cookies\n"
    finally:
        target.chmod(0o644)  # boot 2: the mounted file is group/other-readable — written in place, it keeps that mode
    res = bundle.apply_pending_cookies(cd, target, now_stamp="20260914-010203")  # boot 2
    assert res["errors"] == {} and target.read_text() == "# bundle cookies\n", "written into the mounted file"
    assert not (cd / bundle.COOKIES_PENDING).exists() and not list(cd.glob("*.restore-tmp"))
    assert [p.read_text() for p in cd.glob("cookies.txt.prerestore-*")] == ["# live cookies\n"], \
        "one undo copy across both boots"
    assert stat.S_IMODE(target.stat().st_mode) == 0o600, "restored credentials never stay group/other-readable"
    (cd / bundle.COOKIES_PENDING).write_text("# bundle cookies\n")  # boot 3: a pending whose removal failed applies again
    assert bundle.apply_pending_cookies(cd, target, now_stamp="20260915-010203")["errors"] == {}
    assert len(list(cd.glob("cookies.txt.prerestore-*"))) == 1, "nothing was replaced, so no copy of the restored file"


def test_a_bind_mounted_motif_yaml_is_restored_in_place_with_one_undo_copy(tmp_path, monkeypatch):
    _, cd = _live(tmp_path)
    live = cd / "motif.yaml"
    (cd / bundle.CONFIG_PENDING).write_text("plex:\n  token: BUNDLE-TOKEN\n")
    _replace_refused_onto(monkeypatch, "motif.yaml", errno.EBUSY)
    live.chmod(0o400)
    try:
        assert bundle.apply_pending_config(cd, now_stamp="20260913-010203")["applied"] == []
        assert (cd / bundle.CONFIG_PENDING).exists()
    finally:
        live.chmod(0o644)  # boot 2: the mounted file is group/other-readable — written in place, it keeps that mode
    res = bundle.apply_pending_config(cd, now_stamp="20260914-010203")
    assert res["applied"] == ["motif.yaml"] and "BUNDLE-TOKEN" in live.read_text()
    assert stat.S_IMODE(live.stat().st_mode) == 0o600, "the restored Plex token never stays group/other-readable"
    assert not (cd / bundle.CONFIG_PENDING).exists()
    assert [p.read_text() for p in cd.glob("motif.yaml.prerestore-*")] == [LIVE_YAML], "one undo copy across both boots"


def test_boot_restores_bind_mounted_config_and_cookies_in_place(tmp_path, monkeypatch):
    db, cd = _live(tmp_path)
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk"), keep_config=False)
    _replace_refused_onto(monkeypatch, "motif.yaml", errno.EBUSY)
    _replace_refused_onto(monkeypatch, "cookies.txt", errno.EBUSY)
    seen: list[str] = []
    _boot(monkeypatch, cd, seen)
    assert _marker_rows(db) == 1 and "BUNDLE-TOKEN" in seen[0]
    assert (cd / "cookies.txt").read_text() == "# bundle cookies\n"
    assert bundle.pending_members(db, cd) == []
    assert len(list(cd.glob("motif.yaml.prerestore-*"))) == 1 and len(list(cd.glob("cookies.txt.prerestore-*"))) == 1


# ── 3. a config pending that will not go ─────────────────────────────

def test_clear_pending_config_never_raises_and_names_what_stays(tmp_path, monkeypatch, caplog):
    _, cd = _live(tmp_path)
    (cd / bundle.CONFIG_PENDING).write_text("plex: {}\n")
    (cd / bundle.COOKIES_PENDING).write_text("# c\n")
    _unlink_refused_for(monkeypatch, bundle.COOKIES_PENDING)
    removed, failed = bundle.clear_pending_config(cd)
    assert removed == [bundle.CONFIG_PENDING] and list(failed) == [bundle.COOKIES_PENDING]
    assert (cd / bundle.COOKIES_PENDING).exists()
    assert any(r.levelno == logging.ERROR and bundle.COOKIES_PENDING in r.getMessage() for r in caplog.records)


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    cd = tmp_path / "cfg"
    cd.mkdir()
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(cd))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(cd / "data"))
    from app.config import Settings
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "log_event", lambda *a, **k: None)
    settings = Settings(config_dir=cd, data_dir=cd / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    (cd / "motif.yaml").write_text(LIVE_YAML)
    (cd / "backups").mkdir()
    client = TestClient(api_mod.create_app(settings), raise_server_exceptions=False)
    assert client.get("/api/admin/database-restore/pending", headers=_H).status_code == 200  # warm
    return client, cd


def _list_bundle(cd: Path, root: Path, name: str, **kw) -> str:
    (cd / "backups" / name).write_bytes(_bundle(root, **kw).read_bytes())
    return name


def _pending(client) -> list[str]:
    return client.get("/api/admin/database-restore/pending", headers=_H).json()["members"]


@pytest.mark.parametrize("how", ["listed snapshot", "uploaded snapshot", "second bundle"])
def test_a_config_pending_that_will_not_go_never_gets_a_new_database_beside_it(app_client, tmp_path, monkeypatch, how):
    client, cd = app_client
    first = _list_bundle(cd, tmp_path / "mk", "motif-bundle-20260912-040000.tar.gz")
    r = client.post("/api/admin/database-restore", json={"name": first, "confirm": True, "keep_config": False}, headers=_H)
    assert r.status_code == 200, r.text
    staged_db = db_backup.restore_pending_path(cd / "motif.db")
    before = _sha(staged_db)
    snap = _snapshot(tmp_path / "src")
    (cd / "backups" / LISTED_SNAPSHOT).write_bytes(snap.read_bytes())
    second = _list_bundle(cd, tmp_path / "mk2", "motif-bundle-20260912-050000.tar.gz", token="SECOND-TOKEN", cookies=False)
    _unlink_refused_for(monkeypatch, bundle.CONFIG_PENDING)
    if how == "listed snapshot":
        r = client.post("/api/admin/database-restore", json={"name": LISTED_SNAPSHOT}, headers=_H)
    elif how == "uploaded snapshot":
        r = client.post("/api/admin/database-restore/upload", headers=_H,
                        files={"file": ("snap.db", snap.read_bytes(), "application/octet-stream")})
    else:
        r = client.post("/api/admin/database-restore", json={"name": second, "confirm": True, "keep_config": False},
                        headers=_H)
    assert r.status_code == 500, r.text
    detail = r.json()["detail"]
    assert "not staged" in detail and bundle.CONFIG_PENDING in detail, detail
    assert _sha(staged_db) == before, "the earlier staging's database is still the one staged"
    assert "BUNDLE-TOKEN" in (cd / bundle.CONFIG_PENDING).read_text()
    assert not staged_db.with_name(staged_db.name + ".tmp").exists()


def test_a_cancel_that_cannot_drop_the_config_keeps_the_database_and_says_so(app_client, tmp_path, monkeypatch):
    client, cd = app_client
    name = _list_bundle(cd, tmp_path / "mk", "motif-bundle-20260912-040000.tar.gz")
    assert client.post("/api/admin/database-restore", json={"name": name, "confirm": True, "keep_config": False},
                       headers=_H).status_code == 200
    _unlink_refused_for(monkeypatch, bundle.CONFIG_PENDING)
    r = client.post("/api/admin/database-restore/cancel", headers=_H)
    assert r.status_code == 500 and "not cancelled" in r.json()["detail"], r.text
    assert bundle.CONFIG_PENDING in r.json()["detail"]
    assert _pending(client) == ["database", "config"], "never a stale config left staged without its database"


_LIVE_WORD = {bundle.CONFIG_PENDING: "motif.yaml", bundle.COOKIES_PENDING: "cookies"}


@pytest.mark.parametrize("how", ["listed snapshot", "uploaded snapshot", "second bundle", "cancel"])
@pytest.mark.parametrize("stays", [bundle.COOKIES_PENDING, bundle.CONFIG_PENDING],
                         ids=["the cookies stay", "the config stays"])
def test_a_partial_drop_names_what_went_and_what_a_restart_now_applies(app_client, tmp_path, monkeypatch, how, stays):
    client, cd = app_client
    first = _list_bundle(cd, tmp_path / "mk", "motif-bundle-20260912-040000.tar.gz")
    assert client.post("/api/admin/database-restore", json={"name": first, "confirm": True, "keep_config": False},
                       headers=_H).status_code == 200
    snap = _snapshot(tmp_path / "src")
    (cd / "backups" / LISTED_SNAPSHOT).write_bytes(snap.read_bytes())
    second = _list_bundle(cd, tmp_path / "mk2", "motif-bundle-20260912-050000.tar.gz", token="SECOND-TOKEN", cookies=False)
    went = ({bundle.CONFIG_PENDING, bundle.COOKIES_PENDING} - {stays}).pop()
    _unlink_refused_for(monkeypatch, stays)
    calls = {
        "listed snapshot": lambda: client.post("/api/admin/database-restore", json={"name": LISTED_SNAPSHOT}, headers=_H),
        "uploaded snapshot": lambda: client.post("/api/admin/database-restore/upload", headers=_H,
                                                 files={"file": ("snap.db", snap.read_bytes(), "application/octet-stream")}),
        "second bundle": lambda: client.post("/api/admin/database-restore",
                                             json={"name": second, "confirm": True, "keep_config": False}, headers=_H),
        "cancel": lambda: client.post("/api/admin/database-restore/cancel", headers=_H),
    }
    r = calls[how]()
    assert r.status_code == 500, r.text
    detail = r.json()["detail"]
    left = _pending(client)
    assert left[0] == "database" and len(left) == 2, left
    assert stays in detail and went in detail, detail
    assert f"staged {' + '.join(left)} with your live {_LIVE_WORD[went]}" in detail, \
        "what a restart applies now that one member went"
    assert "nothing applies without it" not in detail, "a restart now applies the database without the member that went"


@pytest.mark.parametrize("op", ["stage", "cancel"])
def test_a_partial_drop_with_no_database_staged_never_claims_one(tmp_path, monkeypatch, op):
    db, cd = _live(tmp_path)
    (cd / bundle.CONFIG_PENDING).write_text("plex:\n  token: BUNDLE-TOKEN\n")  # a boot whose config swap failed after its database applied
    (cd / bundle.COOKIES_PENDING).write_text("# bundle cookies\n")
    snap = _snapshot(tmp_path / "src")
    _unlink_refused_for(monkeypatch, bundle.CONFIG_PENDING)
    with pytest.raises(bundle.StagingError) as ei:
        if op == "stage":
            bundle.stage_snapshot_restore(db, cd, snap)
        else:
            bundle.cancel_pending(db, cd)
    msg = str(ei.value)
    assert bundle.pending_members(db, cd) == ["config"]
    assert bundle.COOKIES_PENDING in msg and "staged config with your live cookies" in msg, msg
    assert "staged database" not in msg, msg


def _main_lines_naming(caplog, needle: str) -> list[str]:
    """motif.main's boot lines that name one member (lower-case needle) — item 6 wants exactly one per member."""
    return [r.getMessage() for r in caplog.records if r.name == "motif.main" and needle in r.getMessage().lower()]


def test_boot_logs_and_continues_when_a_rejected_databases_config_will_not_go(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    db, cd = _live(tmp_path)
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk"), keep_config=False)
    db_backup.restore_pending_path(db).write_bytes(b"corrupted after staging")
    _unlink_refused_for(monkeypatch, bundle.CONFIG_PENDING, bundle.COOKIES_PENDING)
    assert _boot(monkeypatch, cd) == db, "boot reached init_db"
    errors = [r.getMessage() for r in caplog.records if r.name == "motif.main" and r.levelno == logging.ERROR]
    assert any("could not be dropped" in m and bundle.CONFIG_PENDING in m for m in errors), errors
    assert len(_main_lines_naming(caplog, "cookies")) == 1, _main_lines_naming(caplog, "cookies")
    assert (cd / "cookies.txt").read_text() == "# live cookies\n"


@pytest.mark.parametrize("refused", [(), (bundle.CONFIG_PENDING,), (bundle.COOKIES_PENDING,)],
                         ids=["both go", "the config stays", "the cookies stay"])
def test_boot_names_each_member_of_a_rejected_databases_staging_once(tmp_path, monkeypatch, caplog, refused):
    caplog.set_level(logging.INFO)
    db, cd = _live(tmp_path)
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk"), keep_config=False)
    db_backup.restore_pending_path(db).write_bytes(b"corrupted after staging")
    _unlink_refused_for(monkeypatch, *refused)
    _boot(monkeypatch, cd)
    assert bundle.pending_members(db, cd) == [m for p, m in ((bundle.CONFIG_PENDING, "config"),
                                                            (bundle.COOKIES_PENDING, "cookies")) if p in refused]
    for needle in (bundle.CONFIG_PENDING, "cookies"):
        assert len(_main_lines_naming(caplog, needle)) == 1, (needle, _main_lines_naming(caplog, needle))


# ── 4. one lock over every stage and cancel ──────────────────────────

def _slow_config_staging(monkeypatch) -> threading.Event:
    started = threading.Event()
    real = bundle._stage_file

    def slow(src, pending):
        started.set()
        time.sleep(0.5)
        return real(src, pending)
    monkeypatch.setattr(bundle, "_stage_file", slow)
    return started


def test_a_snapshot_staged_mid_bundle_staging_waits_for_it(tmp_path, monkeypatch):
    db, cd = _live(tmp_path)
    b = _bundle(tmp_path / "mk")
    snap = _snapshot(tmp_path / "src")
    started = _slow_config_staging(monkeypatch)
    t = threading.Thread(target=bundle.stage_bundle_restore, args=(db, cd, b), kwargs={"keep_config": False})
    t.start()
    assert started.wait(30)
    bundle.stage_snapshot_restore(db, cd, snap)
    t.join(30)
    assert bundle.pending_members(db, cd) == ["database"], "the snapshot's database never sits beside the bundle's config"
    assert _sha(db_backup.restore_pending_path(db)) == _sha(snap)


def test_a_cancel_mid_bundle_staging_waits_and_drops_it_whole(tmp_path, monkeypatch):
    db, cd = _live(tmp_path)
    b = _bundle(tmp_path / "mk")
    started = _slow_config_staging(monkeypatch)
    t = threading.Thread(target=bundle.stage_bundle_restore, args=(db, cd, b), kwargs={"keep_config": False})
    t.start()
    assert started.wait(30)
    assert bundle.cancel_pending(db, cd) is True
    t.join(30)
    assert bundle.pending_members(db, cd) == [], "a config staged after the cancel would go live alone"


@pytest.mark.parametrize("how", ["listed snapshot", "uploaded snapshot", "bundle", "cancel"])
def test_every_staging_endpoint_waits_for_the_staging_lock(app_client, tmp_path, how):
    client, cd = app_client
    snap = _snapshot(tmp_path / "src")
    (cd / "backups" / LISTED_SNAPSHOT).write_bytes(snap.read_bytes())
    name = _list_bundle(cd, tmp_path / "mk", "motif-bundle-20260912-040000.tar.gz")
    if how == "cancel":
        db_backup.stage_restore(cd / "motif.db", snap)
    calls = {
        "listed snapshot": lambda: client.post("/api/admin/database-restore", json={"name": LISTED_SNAPSHOT}, headers=_H),
        "uploaded snapshot": lambda: client.post("/api/admin/database-restore/upload", headers=_H,
                                                 files={"file": ("snap.db", snap.read_bytes(), "application/octet-stream")}),
        "bundle": lambda: client.post("/api/admin/database-restore",
                                      json={"name": name, "confirm": True, "keep_config": False}, headers=_H),
        "cancel": lambda: client.post("/api/admin/database-restore/cancel", headers=_H),
    }
    out: dict = {}
    with bundle.STAGING_LOCK:
        t = threading.Thread(target=lambda: out.setdefault("r", calls[how]()))
        t.start()
        t.join(1.0)
        assert t.is_alive(), f"{how} ran while another staging held the lock"
    t.join(60)
    assert out["r"].status_code == 200, out["r"].text


# ── 5. boot lines logged before configure_logging ────────────────────

class _Stop(Exception):
    pass


@contextlib.contextmanager
def _unconfigured_root_logger():
    """The root logger as `python -m app.main` starts: no handlers, WARNING."""
    root = logging.getLogger()
    saved, level = root.handlers[:], root.level
    root.handlers = []
    root.setLevel(logging.WARNING)
    try:
        yield root
    finally:
        for h in root.handlers:
            if h not in saved:
                h.close()
        root.handlers = saved
        root.setLevel(level)


def _real_logging_boot(monkeypatch, cd: Path, get_settings=None):
    from app import config as config_mod
    from app import main as main_mod
    monkeypatch.setattr(config_mod, "_DEFAULT_CONFIG_DIR", cd)
    monkeypatch.setattr(main_mod, "get_settings",
                        get_settings or (lambda: config_mod.Settings(config_dir=cd, data_dir=cd / "data")))

    def init_db_stub(db_path):
        raise _Stop(db_path)
    monkeypatch.setattr(main_mod, "init_db", init_db_stub)
    return main_mod


def test_boot_lines_logged_before_configure_logging_reach_stdout_and_motif_log_once(tmp_path, monkeypatch, capsys):
    db, cd = _live(tmp_path)
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))
    db_backup.stage_restore(db, _snapshot(tmp_path / "src"))
    main_mod = _real_logging_boot(monkeypatch, cd)
    with _unconfigured_root_logger() as root:
        with pytest.raises(_Stop):
            main_mod.main()
        assert not any(isinstance(h, main_mod._BootLogBuffer) for h in root.handlers)
    out, err = capsys.readouterr()
    log_text = (cd / "logs" / "motif.log").read_text()
    # apply_pending_restore's own lines: the safety copy (INFO) and the swap (WARNING)
    for line in ("database backup written: motif-prerestore-", "DATABASE RESTORED from staged snapshot"):
        assert line in log_text, f"{line!r} never reached motif.log"
        assert out.count(line) == 1, f"{line!r} reached stdout {out.count(line)} times"
        assert line not in err, f"{line!r} went raw to stderr through logging.lastResort"


def test_a_boot_that_dies_before_configure_logging_still_prints_its_restore_warning(tmp_path, monkeypatch, capsys):
    db, cd = _live(tmp_path)
    db_backup.stage_restore(db, _snapshot(tmp_path / "src"))

    def broken_settings():
        raise RuntimeError("the restored motif.yaml does not load")
    main_mod = _real_logging_boot(monkeypatch, cd, get_settings=broken_settings)
    with _unconfigured_root_logger() as root:
        with pytest.raises(RuntimeError):
            main_mod.main()
        assert root.handlers == [] and root.level == logging.WARNING, "the buffer is gone and the root level restored"
    _, err = capsys.readouterr()
    assert "DATABASE RESTORED from staged snapshot" in err, "the one line saying what this boot changed was swallowed"


# ── 6. boot outcome lines ────────────────────────────────────────────

def test_boot_names_a_failed_config_swap_as_an_error_and_holds_the_cookies(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    db, cd = _live(tmp_path)
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk"), keep_config=False)
    real_replace = os.replace
    _replace_refused_onto(monkeypatch, "motif.yaml", errno.EACCES)
    seen: list[str] = []
    _boot(monkeypatch, cd, seen)
    assert _marker_rows(db) == 1, "the database applied"
    assert "LIVE-TOKEN" in seen[0]
    assert (cd / "cookies.txt").read_text() == "# live cookies\n", "cookies wait for the config they came with"
    assert bundle.pending_members(db, cd) == ["config", "cookies"]
    lines = [(r.levelno, r.getMessage()) for r in caplog.records if r.name == "motif.main"]
    assert not any("Config restored at boot" in m for _, m in lines), lines
    assert any(lv == logging.ERROR and "FAILED" in m and os.strerror(errno.EACCES) in m for lv, m in lines), lines
    cookie_lines = [(lv, m) for lv, m in lines if "cookies" in m.lower()]
    assert len(cookie_lines) == 1 and cookie_lines[0][0] == logging.WARNING and "waits for its config" in cookie_lines[0][1]
    # the fault cleared: the next boot applies the config, then the cookies
    monkeypatch.setattr(os, "replace", real_replace)
    seen.clear()
    _boot(monkeypatch, cd, seen)
    assert "BUNDLE-TOKEN" in seen[0] and (cd / "cookies.txt").read_text() == "# bundle cookies\n"
    assert bundle.pending_members(db, cd) == []


@pytest.mark.parametrize("cookies, cookies_line", [(False, "no staged cookies restore pending"),
                                                   (True, "waits for its database")],
                         ids=["no cookies staged", "cookies staged"])
def test_boot_gives_every_member_an_outcome_line_while_the_config_waits(tmp_path, monkeypatch, caplog,
                                                                        cookies, cookies_line):
    caplog.set_level(logging.INFO)
    db, cd = _live(tmp_path)
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))
    bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk", cookies=cookies), keep_config=False)

    def disk_full(*a, **k):
        raise OSError(errno.ENOSPC, "No space left on device")
    monkeypatch.setattr(db_backup, "create_backup", disk_full)
    _boot(monkeypatch, cd)
    lines = [r.getMessage() for r in caplog.records if r.name == "motif.main"]
    assert sum("restore not applied" in m for m in lines) == 1, lines
    assert sum("waits for its database" in m for m in lines) == 1, lines
    cookie_lines = _main_lines_naming(caplog, "cookies")
    assert len(cookie_lines) == 1 and cookies_line in cookie_lines[0], lines
    assert (cd / "cookies.txt").read_text() == "# live cookies\n"
