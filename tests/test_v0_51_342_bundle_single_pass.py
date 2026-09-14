"""v0.51.342: every bundle flow reads the archive once and checks its database once.

  1. inspect / preview / stage: one gzip open, the archive inflated once, one integrity_check; upload + stage by name: two.
  2. The member gate holds on the stream: known names, regular files only, no repeats, size caps, the next header where
     the capped size puts it, the gzip trailer, and a corrupt deflate stream anywhere is a refusal (a 422), never a raise.
  3. A check vouches only for the bytes it read: stage_restore moves the checked file and re-hashes it; a changed file never stages.
  4. Staged config/cookies are the verified bytes, born 0600; an extraction left behind is logged.
"""
from __future__ import annotations

import contextlib
import errno
import gzip
import hashlib
import io
import json
import logging
import os
import random
import shutil
import signal
import sqlite3
import stat
import tarfile
import zlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.db import CURRENT_SCHEMA_VERSION, init_db
from tests.test_v0_51_339_bundle_staging_boot import _H, LIVE_YAML, _bundle, _live

NAME = "motif-bundle-20260912-040000.tar.gz"
MiB = 1 << 20


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


@pytest.fixture
def seen(monkeypatch):
    c = {"opens": 0, "checks": 0, "inflated": 0}
    real_init, real_read, real_check = gzip.GzipFile.__init__, gzip._GzipReader.read, db_backup.inspect_restore_source

    def init(self, *a, **k):
        c["opens"] += 1
        return real_init(self, *a, **k)

    def read(self, size=-1):
        data = real_read(self, size)
        if size is not None and size >= 0:
            c["inflated"] += len(data)
        return data

    def check(path):
        c["checks"] += 1
        return real_check(path)
    monkeypatch.setattr(gzip.GzipFile, "__init__", init)
    monkeypatch.setattr(gzip._GzipReader, "read", read)
    monkeypatch.setattr(db_backup, "inspect_restore_source", check)
    return c


# ── 1. one pass, one check ───────────────────────────────────────────

@pytest.mark.parametrize("flow", ["inspect", "preview", "stage"])
def test_each_flow_reads_the_archive_once_and_checks_the_database_once(tmp_path, seen, flow):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    tar = len(zlib.decompress(b.read_bytes(), 31))
    seen.update(opens=0, checks=0, inflated=0)
    if flow == "inspect":
        assert bundle.inspect_bundle(b).ok
    elif flow == "preview":
        assert bundle.preview(b, cd / "motif.yaml")["config_in_bundle"]
    else:
        assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config", "cookies"]
    assert (seen["opens"], seen["checks"]) == (1, 1), seen
    assert tar <= seen["inflated"] < 1.5 * tar, f"{seen['inflated']} bytes inflated for a {tar}-byte tar — a rewind re-inflates it"


def test_upload_then_stage_by_name_is_two_passes_and_two_checks(api, tmp_path, seen, monkeypatch):
    client, _ = api
    data = _bundle(tmp_path / "mk").read_bytes()
    hashed: list = []
    real_sha = bundle._sha256_file
    monkeypatch.setattr(bundle, "_sha256_file", lambda p: hashed.append(p) or real_sha(p))
    seen.update(opens=0, checks=0, inflated=0)
    r = client.post("/api/admin/database-restore/upload", headers=_H, files={"file": (NAME, data, "application/gzip")})
    assert r.status_code == 200 and r.json()["preview"]["config_in_bundle"] is True, r.text
    assert (seen["opens"], seen["checks"]) == (1, 1) and seen["inflated"] > 0, seen
    r = client.post("/api/admin/database-restore", json={"name": NAME, "confirm": True, "keep_config": False}, headers=_H)
    assert r.status_code == 200 and r.json()["members"] == ["database", "config", "cookies"], r.text
    assert (seen["opens"], seen["checks"]) == (2, 2), seen
    seen.update(opens=0, checks=0)
    r = client.post("/api/admin/database-restore/upload", headers=_H, files={"file": (NAME, data, "application/gzip")})
    assert r.status_code == 200 and (seen["opens"], seen["checks"]) == (1, 1), (r.text, seen)
    assert hashed == [], "the same-stamp check compares the upload's bytes — no sha256 pass over either file"


def test_a_different_bundle_under_the_same_name_is_refused_even_at_the_same_size(api, tmp_path):
    client, cd = api
    data = _bundle(tmp_path / "mk").read_bytes()
    squatter = bytes(len(data))
    (cd / "backups" / NAME).write_bytes(squatter)
    r = client.post("/api/admin/database-restore/upload", headers=_H, files={"file": (NAME, data, "application/gzip")})
    assert r.status_code == 409, r.text
    assert (cd / "backups" / NAME).read_bytes() == squatter


# ── 2. the member gate, on the stream ────────────────────────────────

def _entries(b: Path) -> list[tuple[tarfile.TarInfo, bytes]]:
    out = []
    with tarfile.open(b, "r:gz") as t:
        for m in t.getmembers():
            data = t.extractfile(m).read()
            ti = tarfile.TarInfo(m.name)
            ti.size = len(data)
            out.append((ti, data))
    return out


def _named(entry, name):
    ti = tarfile.TarInfo(name)
    ti.size = len(entry[1])
    return ti, entry[1]


def _odd(name, type_, **kw):
    ti = tarfile.TarInfo(name)
    ti.type = type_
    for k, v in kw.items():
        setattr(ti, k, v)
    return ti, b""


def _swap(entries, name, repl):
    return [repl if ti.name == name else (ti, d) for ti, d in entries]


def _pack(out: Path, entries) -> Path:
    with tarfile.open(out, "w:gz", format=tarfile.PAX_FORMAT) as t:
        for ti, data in entries:
            t.addfile(ti, io.BytesIO(data) if ti.size else None)
    return out


def _conttype(entry):
    ti, data = _named(entry, entry[0].name)
    ti.type = tarfile.CONTTYPE
    return ti, data


def _pax_path(entry):
    ti, data = _named(entry, entry[0].name)
    ti.pax_headers = {"path": "../../" + entry[0].name}
    return ti, data


JUNK_DB = b"SQLite format 3\x00" + bytes(4080)


def _crc_flipped(gz: bytes) -> bytes:
    return gz[:-8] + bytes([gz[-8] ^ 0xFF]) + gz[-7:]


def _long_tail(raw: bytes) -> bytes:
    return gzip.compress(zlib.decompress(raw, 31) + bytes(2 << 20))  # 2 MiB of tar padding: the stream stops reading long before the trailer


def _skip_lie(e, out: Path, *, key: str, scope: str) -> Path:
    """The config first, sized to its real length by a pax `key` (on it, or global), while its ustar size field says 2**62: capped as one, skipped as the other."""
    cfg = next(x for x in e if x[0].name == "motif.yaml")
    pax = {key: str(len(cfg[1]))}
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT, pax_headers=pax if scope == "global" else None) as t:
        for ti, data in [cfg] + [x for x in e if x is not cfg]:
            if ti is cfg[0] and scope == "member":
                ti.pax_headers = pax
            t.addfile(ti, io.BytesIO(data))
    tar = bytearray(buf.getvalue())
    at = next(o for o in range(0, len(tar), tarfile.BLOCKSIZE) if tar[o:o + 11] == b"motif.yaml\0" and tar[o + 156:o + 157] == b"0")
    end = at + tarfile.BLOCKSIZE
    hdr = tar[at:end]
    hdr[124:136] = b"\x80" + (1 << 62).to_bytes(11, "big")  # base-256: what tarfile skips to reach the next header
    hdr[148:156] = b" " * 8
    hdr[148:156] = b"%06o\0 " % sum(hdr)
    tar[at:end] = hdr
    out.write_bytes(gzip.compress(bytes(tar)))
    return out


def _sparse_map(entry):
    ti, data = _named(entry, entry[0].name)
    ti.pax_headers = {"GNU.sparse.map": f"0,{len(data)}", "GNU.sparse.size": str(len(data))}  # one region, the whole file: the same bytes read back
    return ti, data


class _StillReading(BaseException):
    pass


@contextlib.contextmanager
def _within(seconds: float):
    def alarm(signum, frame):
        raise _StillReading
    prev = signal.signal(signal.SIGALRM, alarm)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    except _StillReading:
        pytest.fail(f"still reading the archive after {seconds} s — a header sent the stream seeking far past its end")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, prev)


HOSTILE = {
    "a repeated config": lambda e, raw, out: _pack(out, [_named((None, b"plex: {}\n"), "motif.yaml")] + e),
    "a flipped CRC after a long tail": lambda e, raw, out: out.write_bytes(_crc_flipped(_long_tail(raw))) and out,
    "a path outside": lambda e, raw, out: _pack(out, e + [_named(e[0], "../motif.db")]),
    "an absolute name": lambda e, raw, out: _pack(out, _swap(e, "motif.db", _named(e[0], "/motif.db"))),
    "a ./ name": lambda e, raw, out: _pack(out, _swap(e, "motif.db", _named(e[0], "./motif.db"))),
    "a symlink": lambda e, raw, out: _pack(out, _swap(e, "motif.yaml", _odd("motif.yaml", tarfile.SYMTYPE, linkname="/etc/passwd"))),
    "a hardlink": lambda e, raw, out: _pack(out, _swap(e, "motif.yaml", _odd("motif.yaml", tarfile.LNKTYPE, linkname="motif.db"))),
    "a directory": lambda e, raw, out: _pack(out, _swap(e, "motif.db", _odd("motif.db", tarfile.DIRTYPE))),
    "a fifo": lambda e, raw, out: _pack(out, _swap(e, "cookies.txt", _odd("cookies.txt", tarfile.FIFOTYPE))),
    "a contiguous file": lambda e, raw, out: _pack(out, _swap(e, "motif.yaml", _conttype(next(x for x in e if x[0].name == "motif.yaml")))),
    "a pax path override": lambda e, raw, out: _pack(out, _swap(e, "motif.yaml", _pax_path(next(x for x in e if x[0].name == "motif.yaml")))),
    "a repeated name": lambda e, raw, out: _pack(out, [_named((None, JUNK_DB), "motif.db")] + e),
    "a flipped gzip CRC": lambda e, raw, out: out.write_bytes(_crc_flipped(raw)) and out,
    "a flipped gzip length": lambda e, raw, out: out.write_bytes(raw[:-1] + bytes([raw[-1] ^ 0x01])) and out,
    "trailing garbage": lambda e, raw, out: out.write_bytes(raw + b"trailing garbage") and out,
    "a truncated archive": lambda e, raw, out: out.write_bytes(raw[: len(raw) * 6 // 10]) and out,
    "a pax sparse realsize over a 2**62 skip": lambda e, raw, out: _skip_lie(e, out, key="GNU.sparse.realsize", scope="member"),
    "a global pax size over a 2**62 skip": lambda e, raw, out: _skip_lie(e, out, key="size", scope="global"),
    "a pax sparse map": lambda e, raw, out: _pack(out, _swap(e, "motif.yaml", _sparse_map(next(x for x in e if x[0].name == "motif.yaml")))),
}


@pytest.mark.parametrize("how", list(HOSTILE))
def test_a_hostile_bundle_is_refused_by_inspect_and_by_stage_and_leaves_nothing(tmp_path, how):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    bad = cd / "backups" / NAME
    bad.parent.mkdir()
    HOSTILE[how](_entries(b), b.read_bytes(), bad)
    with _within(10):  # a seek past the stream's end spins for months: a regression fails here instead of hanging the suite
        c = bundle.inspect_bundle(bad)
        assert not c.ok and c.error and c.error.count("not a motif bundle:") <= 1, c.error
        with pytest.raises(ValueError):
            bundle.stage_bundle_restore(db, cd, bad, keep_config=False)
    assert bundle.pending_members(db, cd) == []
    assert not list(cd.glob(".bundle-*")) and not list(bad.parent.glob(".bundle-*")) and not list(cd.glob("*.tmp"))


@pytest.mark.parametrize("how", ["zero padding after the gzip member", "the manifest first", "a long zero tail inside the tar"])
def test_a_legitimate_layout_still_restores(tmp_path, how):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    ok = tmp_path / "ok.tar.gz"
    if how == "the manifest first":
        e = _entries(b)
        _pack(ok, [x for x in e if x[0].name == "manifest.json"] + [x for x in e if x[0].name != "manifest.json"])
    elif how == "a long zero tail inside the tar":
        ok.write_bytes(_long_tail(b.read_bytes()))
    else:
        ok.write_bytes(b.read_bytes() + bytes(1024))
    assert bundle.inspect_bundle(ok).ok
    assert bundle.stage_bundle_restore(db, cd, ok, keep_config=False).staged == ["database", "config", "cookies"]


@pytest.mark.parametrize("member", bundle.MEMBERS)
def test_a_member_over_its_cap_is_refused_before_its_bytes_are_written(tmp_path, monkeypatch, member):
    b = _bundle(tmp_path / "mk")
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, member: 8})
    wd = tmp_path / "wd"
    wd.mkdir()
    c = bundle._inspect_into(b, wd)
    assert not c.ok and f"{member} is" in c.error and "cap" in c.error, c.error
    if member == bundle.MEMBER_DB:
        assert list(wd.iterdir()) == []


@pytest.mark.parametrize("shape", ["plain", "long tail"])
def test_a_flipped_byte_anywhere_is_never_an_exception_and_never_different_bytes(tmp_path, shape):
    b = _bundle(tmp_path / "mk")
    raw = b.read_bytes() if shape == "plain" else _long_tail(b.read_bytes())
    want = bundle.inspect_bundle(b).manifest
    refused = 0
    for i, off in enumerate(range(0, len(raw), max(1, len(raw) // 160))):
        p = tmp_path / f"c{i}.tar.gz"
        p.write_bytes(raw[:off] + bytes([raw[off] ^ 0x5A]) + raw[off + 1:])
        c = bundle.inspect_bundle(p)  # the contract: never raises
        assert c.ok is False or c.manifest == want, off  # a flip the CRC cannot see (gzip mtime/name, unused Huffman lengths) reads the same bytes
        refused += not c.ok
    assert refused > 100, refused


def _padded_bundle(root: Path) -> Path:
    """A real bundle whose database carries ~12 MiB of deterministic hex: deflate Huffman-codes it, so a flipped byte deep in it breaks the stream."""
    src = root / "src"
    src.mkdir(parents=True)
    db = src / "motif.db"
    init_db(db)
    rng = random.Random(342)
    with sqlite3.connect(db) as conn:
        conn.executemany("INSERT INTO events (ts, level, component, message) VALUES ('x', 'info', 'pad', ?)",
                         ((f"{rng.getrandbits(2048):0512x}",) for _ in range(24000)))
        conn.commit()
    (src / "motif.yaml").write_text("plex:\n  url: http://plex:32400\n  token: BUNDLE-TOKEN\n")
    bf = bundle.create_bundle(db, src, config_file=src / "motif.yaml", cookies_file=None, themes_dir=None,
                              now_stamp="20260912-040000", motif_version="0.51.342",
                              schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


def _deep_deflate_faults(raw: bytes, want: int, *, past: int) -> list[int]:
    """Offsets (into the gzip file) whose flipped byte makes the raw deflate stream raise zlib.error only after `past` bytes inflated."""
    hdr = 10 + raw[10:].index(b"\0") + 1 if raw[3] & 8 else 10
    body, ch = raw[hdr:], 1 << 16
    base, base_off, base_pos = zlib.decompressobj(-15), 0, 0
    found: list[int] = []
    for off in range(len(body) // 4, len(body) * 9 // 10, SCAN_STEP):
        c = off // ch * ch
        while base_off < c:
            base_pos += len(base.decompress(body[base_off:base_off + ch]))
            base_off += ch
        d, pos = base.copy(), base_pos
        tail = body[c:off] + bytes([body[off] ^ SCAN_MASK]) + body[off + 1:c + SCAN_TAIL * ch]
        try:
            for i in range(0, len(tail), ch):
                pos += len(d.decompress(tail[i:i + ch]))
        except zlib.error:
            if pos > past:
                found.append(hdr + off)
        if len(found) == want:
            break
    return found


SCAN_STEP, SCAN_MASK, SCAN_TAIL = 131, 0xFF, 2


def test_a_deflate_fault_deep_in_the_database_is_a_refusal_never_a_raise(api, tmp_path, caplog):
    client, cd = api
    b = _padded_bundle(tmp_path / "mk")
    raw = b.read_bytes()
    assert len(zlib.decompress(raw, 31)) >= 8 * MiB
    cases = _deep_deflate_faults(raw, 5, past=2 * MiB)
    assert len(cases) == 5, cases
    db, live = _live(tmp_path)
    for i, off in enumerate(cases):
        bad = live / "backups" / f"motif-bundle-20260912-04000{i}.tar.gz"
        bad.parent.mkdir(exist_ok=True)
        bad.write_bytes(raw[:off] + bytes([raw[off] ^ SCAN_MASK]) + raw[off + 1:])
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=bundle.log.name):
            c = bundle.inspect_bundle(bad)  # was a raw zlib.error: tarfile wraps it only while reading a header
        assert c.ok is False and c.error.startswith("not a motif bundle: ") and c.error.count("not a motif bundle:") == 1, (off, c.error)
        assert any(bad.name in r.getMessage() and "refused" in r.getMessage() for r in caplog.records), "a 500 left a traceback; its 422 leaves a line"
        with pytest.raises(ValueError):
            bundle.stage_bundle_restore(db, live, bad, keep_config=False)
        assert bundle.pending_members(db, live) == []
        assert not [p.name for d in (live, bad.parent) for p in d.iterdir() if p.name.startswith(".bundle-") or p.name.endswith(".tmp")]
    listed = cd / "backups" / NAME
    listed.write_bytes(raw[:cases[0]] + bytes([raw[cases[0]] ^ SCAN_MASK]) + raw[cases[0] + 1:])
    for body in ({"name": NAME}, {"name": NAME, "confirm": True, "keep_config": False}):
        r = client.post("/api/admin/database-restore", json=body, headers=_H)
        assert r.status_code == 422 and "not a motif bundle" in r.json()["detail"], (body, r.status_code, r.text)
    listed.unlink()
    r = client.post("/api/admin/database-restore/upload", headers=_H,
                    files={"file": (NAME, raw[:cases[0]] + bytes([raw[cases[0]] ^ SCAN_MASK]) + raw[cases[0] + 1:], "application/gzip")})
    assert r.status_code == 422 and "not a motif bundle" in r.json()["detail"], (r.status_code, r.text)
    assert not listed.exists()


# ── 3. a check vouches only for the bytes it read ────────────────────

def _checked(tmp_path):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    wd = cd / ".bundle-stage-test"
    wd.mkdir()
    c = bundle._inspect_into(b, wd)
    assert c.ok and c.db_source.path == wd / "motif.db"
    return db, cd, c.db_source


def test_a_database_changed_after_its_check_is_never_staged(tmp_path, caplog):
    db, cd, tok = _checked(tmp_path)
    data = bytearray(tok.path.read_bytes())
    data[-1] ^= 0xFF
    tok.path.write_bytes(bytes(data))
    with caplog.at_level(logging.ERROR, logger=db_backup.log.name), pytest.raises(ValueError, match="changed"):
        db_backup.stage_restore(db, tok.path, verified=tok)
    assert not db_backup.restore_pending_path(db).exists() and not list(cd.glob("*.tmp"))
    assert any("changed between its check" in r.getMessage() for r in caplog.records)


def test_a_link_swapped_in_after_the_check_is_never_staged(tmp_path):
    db, cd, tok = _checked(tmp_path)
    same = tmp_path / "same-bytes.db"
    os.replace(tok.path, same)
    tok.path.symlink_to(same)
    with pytest.raises(ValueError, match="changed"):
        db_backup.stage_restore(db, tok.path, verified=tok)
    assert not db_backup.restore_pending_path(db).exists() and not list(cd.glob("*.tmp"))


def test_a_token_vouches_only_for_its_own_file_and_only_for_a_passing_check(tmp_path):
    db, _, tok = _checked(tmp_path)
    other = tmp_path / "other.db"
    shutil.copyfile(tok.path, other)
    with pytest.raises(ValueError):
        db_backup.stage_restore(db, other, verified=tok)
    assert other.exists() and tok.path.exists() and not db_backup.restore_pending_path(db).exists()
    failed = db_backup.VerifiedSource(tok.path, tok.size, tok.sha256,
                                      db_backup.RestoreCheck(False, None, "integrity check failed: page 2"))
    with pytest.raises(ValueError, match="integrity"):
        db_backup.stage_restore(db, tok.path, verified=failed)
    assert tok.path.exists() and not db_backup.restore_pending_path(db).exists()


def test_a_verified_staging_moves_without_a_recheck_and_the_boot_still_checks(tmp_path, seen):
    db, cd, tok = _checked(tmp_path)
    want = hashlib.sha256(tok.path.read_bytes()).hexdigest()
    seen.update(checks=0)
    db_backup.stage_restore(db, tok.path, verified=tok)
    assert seen["checks"] == 0 and not tok.path.exists(), "moved, not copied, and not checked a second time"
    assert hashlib.sha256(db_backup.restore_pending_path(db).read_bytes()).hexdigest() == want
    assert db_backup.apply_pending_restore(db, cd, now_stamp="20260913-010203")["applied"] is True
    assert seen["checks"] == 1, "the boot swap still validates the pending file"


def test_the_checked_database_moves_from_beside_the_pending_file(tmp_path, monkeypatch):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    assert b.parent != db.parent, "a listed bundle's own directory is not where the pending file lives"
    moves: list[tuple[Path, Path]] = []
    real = db_backup.os.replace

    def replace(src, dst, *a, **k):
        moves.append((Path(src), Path(dst)))
        return real(src, dst, *a, **k)
    monkeypatch.setattr(db_backup.os, "replace", replace)
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config", "cookies"]
    pending = db_backup.restore_pending_path(db)
    into_tmp = [s for s, d in moves if d.name == pending.name + ".tmp"]
    assert len(into_tmp) == 1 and into_tmp[0].name == "motif.db", moves
    assert into_tmp[0].parent.parent == pending.parent, "extracted beside the pending file, so the move never crosses a mount"


def test_a_bundle_staging_never_copies_the_database(tmp_path, monkeypatch):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)

    def refuse(*a, **k):
        raise AssertionError("the database was copied")
    for name in ("copyfile", "copy2", "copy"):
        monkeypatch.setattr(shutil, name, refuse)
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config", "cookies"]
    assert db_backup.restore_pending_path(db).stat().st_size > 0


def test_a_database_with_a_wal_header_leaves_no_sidecars_behind(tmp_path):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    src = tmp_path / "wal.db"
    init_db(src)
    conn = sqlite3.connect(src)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    wal_db = src.read_bytes()
    assert wal_db[18:20] == b"\x02\x02", "a WAL header: a read-only check of it creates -wal/-shm beside it"
    e = _swap(_entries(b), "motif.db", _named((None, wal_db), "motif.db"))
    man = json.loads(next(d for ti, d in e if ti.name == "manifest.json"))
    man["members"]["motif.db"] = {"size": len(wal_db), "sha256": hashlib.sha256(wal_db).hexdigest()}
    e = _swap(e, "manifest.json", _named((None, json.dumps(man).encode()), "manifest.json"))
    listed = cd / "backups" / NAME
    listed.parent.mkdir()
    _pack(listed, e)
    assert bundle.inspect_bundle(listed).ok
    assert bundle.stage_bundle_restore(db, cd, listed, keep_config=False).staged == ["database", "config", "cookies"]
    live_sidecars = {"motif.db-wal", "motif.db-shm"}
    litter = [p.name for d in (cd, listed.parent) for p in d.iterdir()
              if p.name.startswith(".bundle-") or (p.name.endswith(("-wal", "-shm")) and p.name not in live_sidecars)]
    assert litter == [], litter


def test_no_check_hands_out_a_dead_token_or_a_repr_with_the_config(tmp_path):
    b = _bundle(tmp_path / "mk")
    c = bundle.inspect_bundle(b)
    assert c.ok and c.db_source is None and b"BUNDLE-TOKEN" in c.config_bytes
    assert "BUNDLE-TOKEN" not in repr(c)
    db, cd = _live(tmp_path)
    s = bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert (s.db_source, s.config_bytes, s.cookies_bytes) == (None, None, None)


# ── 4. staged bytes, modes, leftovers ────────────────────────────────

def test_config_and_cookies_pendings_are_born_0600_even_where_chmod_is_refused(tmp_path, monkeypatch):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)

    def refuse(*a, **k):
        raise PermissionError(errno.EPERM, "Operation not permitted")
    monkeypatch.setattr(os, "chmod", refuse)
    old = os.umask(0o002)
    try:
        assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config", "cookies"]
    finally:
        os.umask(old)
    for pending in (bundle.CONFIG_PENDING, bundle.COOKIES_PENDING):
        assert stat.S_IMODE((cd / pending).stat().st_mode) == 0o600, pending
    assert b"BUNDLE-TOKEN" in (cd / bundle.CONFIG_PENDING).read_bytes()


def test_a_config_staging_that_fails_leaves_no_tmp_behind(tmp_path, monkeypatch):
    pending = tmp_path / bundle.CONFIG_PENDING
    pending.write_bytes(b"an earlier staging\n")

    def refuse(*a, **k):
        raise OSError(errno.EXDEV, "Invalid cross-device link")
    monkeypatch.setattr(bundle._os, "replace", refuse)
    with pytest.raises(OSError):
        bundle._stage_file(b"plex:\n  token: BUNDLE-TOKEN\n", pending)
    monkeypatch.undo()
    assert [p.name for p in tmp_path.iterdir()] == [bundle.CONFIG_PENDING], "the half-staged tmp is removed"
    assert pending.read_bytes() == b"an earlier staging\n"


def test_an_extraction_that_cannot_be_removed_is_logged(tmp_path, monkeypatch, caplog):
    b = _bundle(tmp_path / "mk")
    real = shutil.rmtree

    def rmtree(path, *a, onexc=None, **k):
        onexc(os.rmdir, str(path), PermissionError(errno.EACCES, "Permission denied"))
        return real(path)
    monkeypatch.setattr(shutil, "rmtree", rmtree)
    with caplog.at_level(logging.WARNING, logger=bundle.log.name):
        assert bundle.inspect_bundle(b).ok
    assert any("could not remove" in r.getMessage() for r in caplog.records)
