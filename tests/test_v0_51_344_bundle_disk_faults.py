"""v0.51.344: disk faults around a bundle's extraction answer in the disk's own words, and never mask the archive's.

  1. A write that makes no progress raises in words — never a loop spinning under STAGING_LOCK or at boot.
  2. A fault making the extraction directory, or saving an upload, answers 507 / 500 in words, not a wordless 500.
  3. A file motif cannot read is refused in errno words, never its absolute path.
  4. A close fault never replaces a read fault in flight: a corrupt bundle stays a refusal.
"""
from __future__ import annotations

import errno
import logging
import os
import shutil
import tempfile
import zlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from tests.test_v0_51_339_bundle_staging_boot import _H, LIVE_YAML, _bundle, _live
from tests.test_v0_51_341_staging_boot_hardening import _snapshot
from tests.test_v0_51_342_bundle_tail_bound import _within

STAMP = "20260914-010203"
SNAP_NAME = "motif-20260912-040000.db"
ERRNOS = pytest.mark.parametrize("err, status", [(errno.ENOSPC, 507), (errno.EDQUOT, 507), (errno.EACCES, 500), (errno.EROFS, 500)],
                                 ids=["ENOSPC", "EDQUOT", "EACCES", "EROFS"])
needs_non_root = pytest.mark.skipif(os.geteuid() == 0, reason="root reads a chmod-000 file")


@pytest.fixture
def api(tmp_path, monkeypatch):
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
    return TestClient(api_mod.create_app(settings), raise_server_exceptions=False), cd


def _no_paths(words: str, tmp_path: Path) -> bool:
    return str(tmp_path) not in words and os.path.realpath(tmp_path) not in words


def _track_extraction_fds(monkeypatch) -> set[int]:
    real_open, real_close = os.open, os.close
    fds: set[int] = set()

    def open_(path, flags, *a, **k):
        fd = real_open(path, flags, *a, **k)
        p = Path(path)
        if p.name == bundle.MEMBER_DB and p.parent.name.startswith(".bundle-"):
            fds.add(fd)
        return fd

    def close(fd):
        fds.discard(fd)
        real_close(fd)
    monkeypatch.setattr(os, "open", open_)
    monkeypatch.setattr(os, "close", close)
    return fds


# ── 1. a write that makes no progress ────────────────────────────────

def test_an_extraction_write_that_makes_no_progress_is_refused_in_words(tmp_path, monkeypatch):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    fds = _track_extraction_fds(monkeypatch)
    real_write = os.write
    monkeypatch.setattr(os, "write", lambda fd, data: 0 if fd in fds else real_write(fd, data))
    with _within(10), pytest.raises(bundle.ExtractionWriteError) as inspected:
        bundle.inspect_bundle(b)
    with _within(10), pytest.raises(bundle.ExtractionWriteError) as staged:
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    for e in (inspected.value, staged.value):
        assert "(the write made no progress)" in str(e) and not e.out_of_space and _no_paths(str(e), tmp_path), str(e)
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()
    assert not list(cd.glob(".bundle-*")) and not list(b.parent.glob(".bundle-*"))


@pytest.mark.parametrize("which", ["motif.yaml", "cookies.txt"])
def test_an_in_place_write_that_makes_no_progress_writes_the_original_back_at_boot(tmp_path, monkeypatch, caplog, which):
    _, cd = _live(tmp_path)
    live = cd / which
    pending = cd / (bundle.CONFIG_PENDING if which == "motif.yaml" else bundle.COOKIES_PENDING)
    original, new = live.read_bytes(), b"# restored bytes\n" * 64
    pending.write_bytes(new)
    real_replace, real_open, real_write = os.replace, os.open, os.write
    stalled: set[int] = set()

    def replace(src, dst, *a, **k):
        if Path(dst).name == which:
            raise OSError(errno.EBUSY, os.strerror(errno.EBUSY))
        return real_replace(src, dst, *a, **k)

    def open_(path, flags, *a, **k):
        fd = real_open(path, flags, *a, **k)
        if Path(path).name == which and flags & os.O_RDWR:
            stalled.add(fd)
        return fd

    def write(fd, data):
        if fd in stalled and bytes(data) == new:
            return 0  # the restored bytes never advance; the write-back of the original lands
        return real_write(fd, data)
    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(os, "open", open_)
    monkeypatch.setattr(os, "write", write)
    with caplog.at_level(logging.ERROR, logger=bundle.log.name), _within(10):
        res = (bundle.apply_pending_config(cd, now_stamp=STAMP) if which == "motif.yaml"
               else bundle.apply_pending_cookies(cd, live, now_stamp=STAMP))
    assert res["applied"] == [] and "the write made no progress" in next(iter(res["errors"].values())), res
    assert live.read_bytes() == original and pending.exists(), "the live file whole, the pending kept for a retry"
    assert any("original bytes were written back" in r.getMessage() for r in caplog.records)


# ── 2. faults before the extraction ──────────────────────────────────

def _mkdtemp_refused(monkeypatch, err: int) -> None:
    real = tempfile.mkdtemp

    def mkdtemp(suffix=None, prefix=None, dir=None):
        if prefix in (".bundle-inspect-", ".bundle-stage-"):
            raise OSError(err, os.strerror(err), str(dir))
        return real(suffix, prefix, dir)
    monkeypatch.setattr(tempfile, "mkdtemp", mkdtemp)


@ERRNOS
def test_a_fault_making_the_extraction_directory_is_answered_in_words(api, tmp_path, monkeypatch, caplog, err, status):
    client, cd = api
    b = _bundle(tmp_path / "mk")
    shutil.copyfile(b, cd / "backups" / b.name)
    _mkdtemp_refused(monkeypatch, err)
    calls = {
        "preview": lambda: client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H),
        "stage": lambda: client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True}, headers=_H),
        "upload": lambda: client.post("/api/admin/database-restore/upload", headers=_H,
                                      files={"file": ("off-box.tar.gz", b.read_bytes(), "application/gzip")}),
    }
    with caplog.at_level(logging.ERROR, logger=bundle.log.name):
        for flow, call in calls.items():
            r = call()
            assert r.status_code == status, f"{flow}: {r.text}"
            detail = r.json()["detail"]
            assert detail.startswith("could not write the extraction beside") and os.strerror(err) in detail, f"{flow}: {detail}"
            assert "not a motif bundle" not in detail and _no_paths(detail, tmp_path), f"{flow}: {detail}"
    assert any("could not make the extraction directory" in r.getMessage() for r in caplog.records), "the log keeps the full error"
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["members"] == []
    assert not bundle.STAGING_LOCK.locked()
    listed = [row["name"] for row in client.get("/api/admin/database-backups", headers=_H).json()["backups"]]
    assert listed == [b.name], "the refused upload is not listed"
    assert not list((cd / "backups").glob(".restore-upload.*"))


class _RefusingFile:
    def __init__(self, fd: int, err: int):
        self._fd, self._err = fd, err

    def write(self, data):
        raise OSError(self._err, os.strerror(self._err))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        os.close(self._fd)
        return False


def _upload_save_refused(monkeypatch, err: int, at: str) -> None:
    real_mkstemp, real_fdopen, real_open, real_replace = tempfile.mkstemp, os.fdopen, os.open, os.replace
    fds: set[int] = set()

    def mkstemp(suffix=None, prefix=None, dir=None, text=False):
        if prefix == ".restore-upload.":
            if at == "mkstemp":
                raise OSError(err, os.strerror(err), str(dir))
            fd, name = real_mkstemp(suffix, prefix, dir, text)
            fds.add(fd)
            return fd, name
        return real_mkstemp(suffix, prefix, dir, text)

    def fdopen(fd, *a, **k):
        if fd in fds and at == "write":
            fds.discard(fd)
            return _RefusingFile(fd, err)
        return real_fdopen(fd, *a, **k)

    def open_(path, flags, *a, **k):  # v0.51.344: R1-F10 — the O_EXCL claim of the upload's listed name
        if at == "claim" and flags & os.O_EXCL and Path(path).name.startswith("motif-bundle-upload-"):
            raise OSError(err, os.strerror(err), str(path))
        return real_open(path, flags, *a, **k)

    def replace(src, dst, *a, **k):  # v0.51.344: R1-F10 — the rename onto the claimed name
        if at == "replace" and Path(dst).name.startswith("motif-bundle-upload-"):
            raise OSError(err, os.strerror(err), str(dst))
        return real_replace(src, dst, *a, **k)
    monkeypatch.setattr(tempfile, "mkstemp", mkstemp)
    monkeypatch.setattr(os, "fdopen", fdopen)
    monkeypatch.setattr(os, "open", open_)
    monkeypatch.setattr(os, "replace", replace)


# v0.51.344: R1-F10 — the claim and the rename are the bundle upload's own steps
_UPLOAD_STEPS = [("bundle", "mkstemp"), ("bundle", "write"), ("bundle", "claim"), ("bundle", "replace"),
                 ("snapshot", "mkstemp"), ("snapshot", "write")]


@ERRNOS
@pytest.mark.parametrize("kind, at", _UPLOAD_STEPS, ids=[f"{k}-{a}" for k, a in _UPLOAD_STEPS])
def test_a_fault_saving_an_upload_is_answered_in_words_and_keeps_nothing(api, tmp_path, monkeypatch, caplog, err, status, at, kind):
    client, cd = api
    if kind == "bundle":
        name, data = "off-box.tar.gz", _bundle(tmp_path / "mk").read_bytes()
    else:
        name, data = "off-box.db", _snapshot(tmp_path / "src").read_bytes()
    _upload_save_refused(monkeypatch, err, at)
    with caplog.at_level(logging.ERROR):
        r = client.post("/api/admin/database-restore/upload", headers=_H, files={"file": (name, data, "application/octet-stream")})
    assert r.status_code == status, r.text
    detail = r.json()["detail"]
    assert detail.startswith("could not save the upload") and os.strerror(err) in detail, detail
    assert _no_paths(detail, tmp_path), detail
    assert any("could not save the upload" in rec.getMessage() for rec in caplog.records), "the log keeps the full error"
    assert not list((cd / "backups").glob(".restore-upload.*")) and not list(cd.glob(".restore-upload.*"))
    assert client.get("/api/admin/database-backups", headers=_H).json()["backups"] == []
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["members"] == []


# ── 3. a file motif cannot read ──────────────────────────────────────

@needs_non_root
def test_a_bundle_motif_cannot_read_is_refused_in_errno_words_never_its_path(api, tmp_path, caplog):
    client, cd = api
    b = cd / "backups" / "motif-bundle-20260912-040000.tar.gz"
    shutil.copyfile(_bundle(tmp_path / "mk"), b)
    b.chmod(0)
    try:
        with caplog.at_level(logging.WARNING, logger=bundle.log.name):
            c = bundle.inspect_bundle(b)
        r = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H)
    finally:
        b.chmod(0o600)
    assert not c.ok and c.error == f"not a motif bundle: PermissionError: {os.strerror(errno.EACCES)}", c.error
    assert r.status_code == 422 and r.json()["detail"] == c.error, r.text
    assert any(b.name in rec.getMessage() and str(cd) in rec.getMessage() for rec in caplog.records), "the log keeps the path"


@needs_non_root
def test_a_snapshot_motif_cannot_read_is_refused_in_errno_words_never_its_path(api, tmp_path, caplog):
    client, cd = api
    snap = cd / "backups" / SNAP_NAME
    shutil.copyfile(_snapshot(tmp_path / "src"), snap)
    snap.chmod(0)
    try:
        with caplog.at_level(logging.WARNING, logger=db_backup.log.name):
            check = db_backup.inspect_restore_source(snap)
        r = client.post("/api/admin/database-restore", json={"name": SNAP_NAME}, headers=_H)
    finally:
        snap.chmod(0o600)
    assert not check.ok and check.error == f"unreadable: {os.strerror(errno.EACCES)}", check.error
    assert r.status_code == 422 and r.json()["detail"] == check.error, r.text
    assert any(SNAP_NAME in rec.getMessage() and str(cd) in rec.getMessage() for rec in caplog.records), "the log keeps the path"


def test_a_file_that_is_not_gzip_keeps_its_words(tmp_path):
    p = tmp_path / "motif-bundle-20260912-040000.tar.gz"
    p.write_bytes(b"PK\x03\x04" + bytes(64))
    c = bundle.inspect_bundle(p)
    assert not c.ok and c.error.startswith("not a motif bundle: Not a gzipped file"), c.error


# ── 4. a close fault never masks a read fault ────────────────────────

def _close_refused_for_the_extraction(monkeypatch) -> None:
    real_open, real_close = os.open, os.close
    fds: set[int] = set()

    def open_(path, flags, *a, **k):
        fd = real_open(path, flags, *a, **k)
        p = Path(path)
        if p.name == bundle.MEMBER_DB and p.parent.name.startswith(".bundle-"):
            fds.add(fd)
        return fd

    def close(fd):
        real_close(fd)  # really closed first — the fault leaks nothing
        if fd in fds:
            fds.discard(fd)
            raise OSError(errno.EIO, os.strerror(errno.EIO))
    monkeypatch.setattr(os, "open", open_)
    monkeypatch.setattr(os, "close", close)


def test_a_close_fault_never_replaces_a_read_fault_in_flight(tmp_path, monkeypatch, caplog):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)

    def read(self, n=-1):
        raise zlib.error("Error -3 while decompressing data: invalid distance too far back")
    monkeypatch.setattr(bundle._HashingReader, "read", read)
    _close_refused_for_the_extraction(monkeypatch)
    with caplog.at_level(logging.WARNING, logger=bundle.log.name):
        c = bundle.inspect_bundle(b)
        with pytest.raises(ValueError, match="invalid distance too far back"):
            bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert not c.ok and c.error.startswith("not a motif bundle:") and "invalid distance too far back" in c.error, c.error
    said = [r.getMessage() for r in caplog.records if r.name == bundle.log.name]
    assert sum("could not close the extraction" in m for m in said) == 2, said
    assert not any("the disk refused it" in m for m in said), said
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()
    assert not list(cd.glob(".bundle-*")) and not list(b.parent.glob(".bundle-*"))


def test_a_close_fault_after_a_whole_extraction_is_still_the_disks(tmp_path, monkeypatch):
    b = _bundle(tmp_path / "mk")
    _close_refused_for_the_extraction(monkeypatch)
    with pytest.raises(bundle.ExtractionWriteError) as refused:
        bundle.inspect_bundle(b)
    assert str(refused.value).startswith(f"could not write the extraction beside the bundle ({os.strerror(errno.EIO)})"), refused.value
    assert not refused.value.out_of_space
