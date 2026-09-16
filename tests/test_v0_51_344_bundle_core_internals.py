"""v0.51.344: bundle internals from the part-B follow-ups.

  1. The restore preview's cookies target comes from ConfigFile.load()'s own pipeline, never a hand-kept copy of it.
  2. Extension headers share one budget per archive and a 'g' or GNU sparse header, or a pax GNU sparse 1.0 member, is refused before tarfile holds a payload or a map; real pax headers still inspect.
  3. A stale staging tmp never holds the bundle token at its old mode.
  4. A database pending that cannot be unstaged keeps the config it was staged with; following the refusal's words leaves nothing to apply alone.
  5. A mount that cannot fsync still takes an in-place restore; a real sync fault does not.
"""
from __future__ import annotations

import errno
import gzip
import io
import logging
import os
import re
import stat
import tarfile
import tracemalloc
from pathlib import Path

import pytest

from app.core import bundle, config_file, db_backup
from tests.test_v0_51_339_bundle_staging_boot import _bundle, _live
from tests.test_v0_51_341_config_secrets_preview import _bundle as _bundle_with
from tests.test_v0_51_342_bundle_tail_bound import _checksum

STAMP = "20260914-010203"


@pytest.fixture
def no_env(monkeypatch):
    for env_name, _dotted, _conv in config_file.ENV_BINDINGS:
        monkeypatch.delenv(env_name, raising=False)


# ── 1. one loader pipeline ───────────────────────────────────────────

def test_a_step_the_loader_gains_reaches_the_previewed_cookies_target(tmp_path, monkeypatch, no_env):
    target = tmp_path / "bundle-mount" / "yt-cookies.txt"
    text = f"paths:\n  cookies_file: {target}\n"
    b = _bundle_with(tmp_path / "mk", text)
    _, cd = _live(tmp_path)
    real = config_file.load_config_text

    def gained(t):
        cfg = real(t)
        cfg.paths.cookies_file += ".gained"
        return cfg
    monkeypatch.setattr(config_file, "load_config_text", gained)
    boot = tmp_path / "boot.yaml"
    boot.write_text(text)
    booted = config_file.ConfigFile(boot).load().paths.cookies_file
    assert booted == f"{target}.gained", "the premise: load() runs the gained step"
    assert bundle.preview(b, cd / "motif.yaml", cookies_target=tmp_path / "live.txt")["cookies_target"] == booted


def test_load_keeps_its_refusals_and_a_missing_file_loads_the_defaults(tmp_path, no_env):
    p = tmp_path / "motif.yaml"
    assert config_file.ConfigFile(p).load() == config_file.MotifConfig()
    for text, words in (("plex: [unclosed\n", "motif.yaml is not valid YAML"), ("- a list\n", "motif.yaml top-level must be a mapping")):
        p.write_text(text)
        with pytest.raises(config_file.ConfigValidationError) as refused:
            config_file.ConfigFile(p).load()
        assert any(e.startswith(words) for e in refused.value.errors), refused.value.errors


# ── 2. extension headers ─────────────────────────────────────────────

EXT_TYPES = {"L": tarfile.GNUTYPE_LONGNAME, "K": tarfile.GNUTYPE_LONGLINK, "x": tarfile.XHDTYPE, "g": tarfile.XGLTYPE, "X": tarfile.SOLARIS_XHDTYPE}
BUDGETED = [k for k in EXT_TYPES if k != "g"]


def _refusal_words(kind: str) -> str:
    return "a global pax header, which motif never writes" if kind == "g" else "bytes of extension headers"


def _ext_header_archive(path: Path, kind: str, size: int) -> None:
    h = tarfile.TarInfo("././@LongLink" if kind in "LK" else "././@PaxHeader")
    h.type = EXT_TYPES[kind]
    h.size = size
    with gzip.open(path, "wb") as gz:
        gz.write(h.tobuf(format=tarfile.GNU_FORMAT))
        left = -(-size // tarfile.BLOCKSIZE) * tarfile.BLOCKSIZE
        block = bytes(1 << 20)
        while left:
            gz.write(block[:min(left, len(block))])
            left -= min(left, len(block))
        gz.write(tarfile.TarInfo(bundle.MEMBER_DB).tobuf(format=tarfile.GNU_FORMAT))
        gz.write(bytes(2 * tarfile.BLOCKSIZE))


def _spy_fetches(monkeypatch) -> list[int]:
    fetched: list[int] = []
    real = bundle._TarFeed.read

    def read(self, size=-1):
        data = real(self, size)
        fetched.append(len(data))
        return data
    monkeypatch.setattr(bundle._TarFeed, "read", read)
    return fetched


@pytest.mark.parametrize("kind", list(EXT_TYPES))
def test_an_extension_header_over_its_cap_is_refused_before_its_payload_is_inflated(tmp_path, monkeypatch, kind):
    size = 64 << 20
    p = tmp_path / "hostile.tar.gz"
    _ext_header_archive(p, kind, size)
    fetched = _spy_fetches(monkeypatch)
    c = bundle.inspect_bundle(p)
    assert not c.ok and _refusal_words(kind) in c.error and c.error.count("not a motif bundle:") == 1, c.error
    assert sum(fetched) <= 2 * bundle._STREAM_BUF, f"{sum(fetched)} bytes inflated for a refused header"
    db, cd = _live(tmp_path)
    with pytest.raises(ValueError, match=_refusal_words(kind)):
        bundle.stage_bundle_restore(db, cd, p, keep_config=False)
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()


def test_a_gnu_sparse_header_is_refused_before_its_extended_blocks_are_read(tmp_path, monkeypatch):
    hdr = bytearray(tarfile.TarInfo(bundle.MEMBER_DB).tobuf(format=tarfile.GNU_FORMAT))
    hdr[156:157], hdr[482] = b"S", 1  # v0.51.344: isextended — tarfile reads extended blocks until one clears the flag
    ext = bytearray(tarfile.BLOCKSIZE)
    for i in range(21):
        ext[i * 24:i * 24 + 24] = b"%011o\0" % (i + 1) * 2
    ext[504] = 1
    p = tmp_path / "sparse.tar.gz"
    p.write_bytes(gzip.compress(bytes(_checksum(hdr)) + bytes(ext) * ((8 << 20) // tarfile.BLOCKSIZE) + bytes(2 * tarfile.BLOCKSIZE)))
    fetched = _spy_fetches(monkeypatch)
    c = bundle.inspect_bundle(p)
    assert not c.ok and "a GNU sparse header, which motif never writes" in c.error, c.error
    assert sum(fetched) <= 2 * bundle._STREAM_BUF, f"{sum(fetched)} bytes inflated for a refused header"
    db, cd = _live(tmp_path)
    with pytest.raises(ValueError, match="GNU sparse header"):
        bundle.stage_bundle_restore(db, cd, p, keep_config=False)
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()


@pytest.mark.parametrize("kind", ["x", "X"])
def test_a_pax_gnu_sparse_1_0_member_is_refused_before_its_map_is_read_from_the_data(tmp_path, monkeypatch, kind):
    data = b"1000000000000\n" + b"1000\n" * ((8 << 20) // 5)  # v0.51.344: a map that claims a trillion entries — tarfile reads the data block by block looking for them
    assert len(data) > 4 * bundle._STREAM_BUF, "the premise: the map runs far past the fetches a refused header may take"
    m = tarfile.TarInfo(bundle.MEMBER_DB)
    m.size, m.pax_headers = len(data), {"GNU.sparse.major": "1", "GNU.sparse.minor": "0"}
    hdr = bytearray(m.tobuf(format=tarfile.PAX_FORMAT))
    assert hdr[156:157] == tarfile.XHDTYPE, "the premise: the member rides its own pax header"
    hdr[156:157] = EXT_TYPES[kind]
    hdr[:tarfile.BLOCKSIZE] = _checksum(hdr[:tarfile.BLOCKSIZE])
    p = tmp_path / "sparse10.tar.gz"
    p.write_bytes(gzip.compress(bytes(hdr) + data + bytes(-len(data) % tarfile.BLOCKSIZE + 2 * tarfile.BLOCKSIZE), compresslevel=1))
    fetched = _spy_fetches(monkeypatch)
    c = bundle.inspect_bundle(p)
    assert not c.ok and "a GNU sparse member, which motif never writes" in c.error and c.error.count("not a motif bundle:") == 1, c.error
    assert sum(fetched) <= 2 * bundle._STREAM_BUF, f"{sum(fetched)} bytes inflated for a refused header"
    db, cd = _live(tmp_path)
    fetched.clear()
    with pytest.raises(ValueError, match="a GNU sparse member, which motif never writes"):
        bundle.stage_bundle_restore(db, cd, p, keep_config=False)
    assert 0 < sum(fetched) <= 2 * bundle._STREAM_BUF, f"{sum(fetched)} bytes inflated under STAGING_LOCK for a refused header"
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()


@pytest.mark.parametrize("kind", BUDGETED)
def test_extension_headers_filling_the_budget_are_judged_and_one_block_more_is_refused(tmp_path, kind):
    fits = bundle._EXT_HEADER_CAP - tarfile.BLOCKSIZE  # v0.51.344: the boundary, header block included — the chain test below pins the budget's size
    for size, refused in ((fits, False), (fits + 1, True)):
        p = tmp_path / f"edge-{size}.tar.gz"
        _ext_header_archive(p, kind, size)
        c = bundle.inspect_bundle(p)
        assert not c.ok and ("bytes of extension headers" in c.error) is refused, (size, c.error)


def _header_chain(kind: str, n: int) -> bytes:
    out = io.BytesIO()
    key = 0
    for _ in range(n):
        body = bytearray()
        while len(body) < 60 << 10:  # v0.51.344: distinct keys under a 'g', so its dict grows; repeated 2-byte keys under the rest
            rec = (b"13 k%07d=\n" % key) if kind == "g" else b"6 ab=\n"
            body += rec
            key += 1
        h = tarfile.TarInfo("././@LongLink" if kind in "LK" else "././@PaxHeader")
        h.type, h.size = EXT_TYPES[kind], len(body)
        out.write(h.tobuf(format=tarfile.USTAR_FORMAT) + bytes(body) + bytes(-len(body) % tarfile.BLOCKSIZE))
    m = tarfile.TarInfo(bundle.MEMBER_MANIFEST)
    m.size = 2
    out.write(m.tobuf(format=tarfile.USTAR_FORMAT) + b"{}" + bytes(tarfile.BLOCKSIZE - 2) + bytes(2 * tarfile.BLOCKSIZE))
    return out.getvalue()


def _peak_inspecting(path: Path):
    tracing = tracemalloc.is_tracing()
    if not tracing:
        tracemalloc.start()
    tracemalloc.reset_peak()
    held = tracemalloc.get_traced_memory()[0]
    try:
        c = bundle.inspect_bundle(path)
        return c, tracemalloc.get_traced_memory()[1] - held
    finally:
        if not tracing:
            tracemalloc.stop()


@pytest.mark.parametrize("kind", list(EXT_TYPES))
def test_splitting_a_payload_over_many_headers_under_the_cap_holds_no_more_than_one(tmp_path, kind):
    many = _header_chain(kind, 15)  # v0.51.344: ~900 KiB of headers inside tarfile's first fetch — a per-header cap alone held every one
    one = _header_chain(kind, 1)
    (p_many := tmp_path / "many.tar.gz").write_bytes(gzip.compress(many))
    (p_one := tmp_path / "one.tar.gz").write_bytes(gzip.compress(one + bytes(len(many) - len(one))))  # v0.51.344: the same fetch size, so only the headers differ
    _, alone = _peak_inspecting(p_one)
    c, split = _peak_inspecting(p_many)
    assert not c.ok and _refusal_words(kind) in c.error, c.error
    assert split - alone < bundle._STREAM_BUF // 2, f"{split - alone} more bytes held for 15 headers than for one"


def test_a_re_packed_bundle_whose_members_carry_pax_headers_still_inspects_and_stages(tmp_path):
    b = _bundle(tmp_path / "mk")
    out = tmp_path / "repacked.tar.gz"
    with tarfile.open(b, "r:gz") as t, tarfile.open(out, "w:gz", format=tarfile.PAX_FORMAT) as w:
        for m in t:
            data = t.extractfile(m).read()
            m.pax_headers = {"comment": "re-packed " + "z" * 4000}
            w.addfile(m, io.BytesIO(data))
    with tarfile.open(out, "r:gz") as t:
        assert all(len(m.pax_headers.get("comment", "")) > 4000 for m in t), "the premise: every member rides a 4 KiB pax record"
    assert bundle.inspect_bundle(out).ok
    db, cd = _live(tmp_path)
    assert bundle.stage_bundle_restore(db, cd, out, keep_config=False).staged == ["database", "config", "cookies"]


# ── 3. a stale staging tmp ───────────────────────────────────────────

@pytest.mark.parametrize("pending", [bundle.CONFIG_PENDING, bundle.COOKIES_PENDING])
def test_a_stale_tmp_never_holds_the_new_bytes_at_its_old_mode(tmp_path, monkeypatch, pending):
    db, cd = _live(tmp_path)
    b = _bundle(tmp_path / "mk")
    stale = cd / (pending + ".tmp")
    stale.write_bytes(b"left by a crashed .339-.341 staging\n")
    stale.chmod(0o644)
    modes: list[int] = []
    real = Path.chmod

    def refuse(self, mode, *a, **k):
        if self.name == stale.name:
            modes.append(stat.S_IMODE(os.stat(self).st_mode))
            raise PermissionError(errno.EPERM, "Operation not permitted")
        return real(self, mode, *a, **k)
    monkeypatch.setattr(Path, "chmod", refuse)
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config", "cookies"]
    staged = cd / pending
    assert modes == [0o600], f"the mode the bundle's bytes sat under: {modes}"
    assert stat.S_IMODE(staged.stat().st_mode) == 0o600 and b"left by a crashed" not in staged.read_bytes()
    assert not stale.exists()


@pytest.mark.parametrize("pending", [bundle.CONFIG_PENDING, bundle.COOKIES_PENDING])
def test_a_directory_at_the_tmp_path_is_refused_as_a_directory_and_stages_nothing(tmp_path, pending):
    db, cd = _live(tmp_path)
    b = _bundle(tmp_path / "mk")
    (cd / (pending + ".tmp")).mkdir()
    with pytest.raises(bundle.StagingError) as refused:
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert "Is a directory" in str(refused.value) and str(tmp_path) not in str(refused.value), refused.value  # v0.51.344: unlinking it first said EPERM on macOS
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()


# ── 4. the database pending that will not go ─────────────────────────

def _boot_as_main(db: Path, cd: Path) -> dict | None:
    restore = db_backup.apply_pending_restore(db, cd, now_stamp=STAMP)
    if restore is None or restore.get("applied"):
        bundle.apply_pending_config(cd, now_stamp=STAMP)
    elif not db_backup.restore_pending_path(db).exists():
        bundle.clear_pending_config(cd)
    return restore


@pytest.mark.parametrize("follow", [False, True], ids=["restart-as-is", "follow-the-words-then-restart"])
@pytest.mark.parametrize("fails", [bundle.CONFIG_PENDING, bundle.COOKIES_PENDING])
def test_a_database_that_cannot_be_unstaged_keeps_the_config_it_was_staged_with(tmp_path, monkeypatch, fails, follow):
    db, cd = _live(tmp_path)
    live_yaml = (cd / "motif.yaml").read_bytes()
    b = _bundle(tmp_path / "mk")
    dbp = db_backup.restore_pending_path(db)
    real_stage, real_unlink = bundle._stage_file, Path.unlink

    def stage(data, pending):
        if pending.name == fails:
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
        return real_stage(data, pending)

    def unlink(self, *a, **k):
        if self == dbp:
            raise PermissionError(errno.EACCES, os.strerror(errno.EACCES))
        return real_unlink(self, *a, **k)
    monkeypatch.setattr(bundle, "_stage_file", stage)
    monkeypatch.setattr(Path, "unlink", unlink)
    with pytest.raises(bundle.StagingError) as exc:
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    monkeypatch.undo()
    msg = str(exc.value)
    config_failed = fails == bundle.CONFIG_PENDING
    assert bundle.pending_members(db, cd) == (["database"] if config_failed else ["database", "config"]), msg
    beside = "your live motif.yaml" if config_failed else "the staged config and your live cookies file"
    assert f"{dbp.name} could not be removed" in msg and f"a restart applies it with {beside}" in msg, msg
    assert "nothing from this bundle is staged" not in msg and str(tmp_path) not in msg, msg
    if not follow:
        assert _boot_as_main(db, cd)["applied"]
        assert (b"BUNDLE-TOKEN" in (cd / "motif.yaml").read_bytes()) is not config_failed, "the database boots with the config it was staged with"
        return
    remedy = re.search(r"; remove (.+?) by hand, then stage again", msg)
    assert remedy, msg
    named = remedy.group(1).split(" and ")
    for name in named:
        assert (cd / name).exists(), f"the words name {name}, which is not there: {msg}"
        (cd / name).unlink()
    assert _boot_as_main(db, cd) is None and bundle.pending_members(db, cd) == [], f"removed {named}: {msg}"
    assert (cd / "motif.yaml").read_bytes() == live_yaml, f"following the words ({named}) left a config to apply beside the live database"
    assert ("alone lets the staged config apply by itself" in msg) is not config_failed, msg


# ── 5. a mount that cannot fsync ─────────────────────────────────────

def _in_place_with_fsync_refused(tmp_path, monkeypatch, err: int):
    cd = tmp_path / "cfg"
    cd.mkdir()
    live, pending = cd / "motif.yaml", cd / bundle.CONFIG_PENDING
    live.write_bytes(b"plex:\n  token: LIVE-TOKEN\n")
    new = b"plex:\n  token: BUNDLE-TOKEN\n"
    pending.write_bytes(new)
    real_replace = os.replace

    def replace(s, d, *a, **k):
        if Path(d) == live:
            raise OSError(errno.EBUSY, os.strerror(errno.EBUSY))
        return real_replace(s, d, *a, **k)

    def fsync(fd):
        raise OSError(err, os.strerror(err))
    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(os, "fsync", fsync)
    return cd, live, pending, new


@pytest.mark.parametrize("err", [errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP], ids=["EINVAL", "ENOTSUP", "EOPNOTSUPP"])
def test_a_mount_that_cannot_fsync_still_takes_the_in_place_restore(tmp_path, monkeypatch, caplog, err):
    cd, live, pending, new = _in_place_with_fsync_refused(tmp_path, monkeypatch, err)
    with caplog.at_level(logging.WARNING, logger=bundle.log.name):
        res = bundle.apply_pending_config(cd, now_stamp=STAMP)
    assert res["applied"] == ["motif.yaml"] and res["errors"] == {} and live.read_bytes() == new and not pending.exists(), res
    said = [r.getMessage() for r in caplog.records]
    assert any("written, not synced" in m and str(len(new)) in m for m in said), said
    assert not any("partly written" in m for m in said), said


def test_a_sync_fault_on_the_in_place_write_is_still_a_failure(tmp_path, monkeypatch, caplog):
    cd, live, pending, new = _in_place_with_fsync_refused(tmp_path, monkeypatch, errno.EIO)
    with caplog.at_level(logging.WARNING, logger=bundle.log.name):
        res = bundle.apply_pending_config(cd, now_stamp=STAMP)
    assert res["applied"] == [] and res["errors"] and pending.exists(), "durability unknown — the pending file stays for a retry"
    assert not any("written, not synced" in r.getMessage() for r in caplog.records)
