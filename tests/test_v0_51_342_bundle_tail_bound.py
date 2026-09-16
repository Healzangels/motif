"""v0.51.342: _inspect_into bounds what it reads after the archive, and refuses the parser faults that used to 500.

  1. A one-pass GzipFile + tarfile "r|" reader drains to the gzip trailer. Data appended past end-of-archive —
     32 MiB of zero padding, 200k/400k empty gzip members — used to be read in full (seconds, holding STAGING_LOCK).
     Two budgets refuse it fast: a raw allowance per 1 MiB tarfile fetches, which the drain shares with the last
     fetch (the empty-member flood, the trailing-zero scan, however inflated data splits them), and a drain
     inflate cap (zeros hidden inside the member past end-of-archive). A tail under budget still restores, and a
     database that compresses to many allowances still restores.
  2. A GNU sparse 'S' extended header at end of stream (IndexError) and a ~400-deep chain of 'g'/'x'/'L'
     extension headers (RecursionError) crash tarfile's parser before any gate. Both are refused now, not raised,
     through inspect_bundle AND stage_bundle_restore, leaving no staging behind and STAGING_LOCK free.
"""
from __future__ import annotations

import gzip
import logging
import os
import signal
import sqlite3
import tarfile
import zlib
from contextlib import contextmanager
from pathlib import Path

import pytest

from app.core import bundle
from app.core.db import CURRENT_SCHEMA_VERSION, init_db
from tests.test_v0_51_339_bundle_staging_boot import NOW, _bundle, _live

MiB = 1 << 20
NAME = "motif-bundle-20260912-040000.tar.gz"
# v0.51.342: a refusal parses one 2 MiB raw allowance of 20-byte empty gzip members — 2.4-3.4 s measured on an M1 Pro; 8 s left a slow CI runner a false red
TAIL_BOUND = 30


class _StillReading(BaseException):
    pass


@contextmanager
def _within(seconds: float):
    def alarm(signum, frame):
        raise _StillReading
    prev = signal.signal(signal.SIGALRM, alarm)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    except _StillReading:
        pytest.fail(f"still reading the archive after {seconds} s — the tail budget did not bite")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, prev)


def _tar_bytes(b: Path) -> bytes:
    return zlib.decompress(b.read_bytes(), 31)


def _place(cd: Path, name: str, data: bytes) -> Path:
    p = cd / "backups" / name
    p.parent.mkdir(exist_ok=True)
    p.write_bytes(data)
    return p


# ── 1. the archive's tail is bounded ─────────────────────────────────

_EMPTY_MEMBER = gzip.compress(b"", mtime=0)


def _shapes(b: Path) -> dict[str, bytes]:
    raw = b.read_bytes()
    return {
        # spec's three measured shapes: 32 MiB zero padding in the gzip member, and 200k / 400k empty members
        "32 MiB of trailing zero padding": raw + bytes(32 * MiB),
        "200k empty gzip members": raw + _EMPTY_MEMBER * 200_000,
        "400k empty gzip members": raw + _EMPTY_MEMBER * 400_000,
    }


@pytest.mark.parametrize("how", ["32 MiB of trailing zero padding", "200k empty gzip members", "400k empty gzip members"])
def test_a_measured_tail_shape_is_refused_fast_by_the_raw_budget(tmp_path, how):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    bad = _place(cd, NAME, _shapes(b)[how])
    with _within(TAIL_BOUND):  # a hang guard only — each refusal costs about 3 s; the refusal reasons below are the signal
        c = bundle.inspect_bundle(bad)
    # v0.51.342: removing _TAIL_RAW_BUDGET makes each of these a VALID gzip stream again → ok flips True (the mutation signal)
    assert c.ok is False and c.error.startswith("not a motif bundle:") and c.error.count("not a motif bundle:") == 1, c.error
    assert "padding or empty gzip members" in c.error and str(bundle._TAIL_RAW_BUDGET) in c.error, c.error  # v0.51.344: the raw budget's words, true inside the archive too


def test_zeros_inflated_past_end_of_archive_are_refused_by_the_inflate_budget(tmp_path, caplog):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    over = bundle._TAIL_INFLATE_BUDGET + 4 * MiB  # compresses to a few KB of raw, so only the inflate budget can catch it
    bad = _place(cd, NAME, gzip.compress(_tar_bytes(b) + bytes(over)))
    with _within(TAIL_BOUND), caplog.at_level(logging.WARNING, logger=bundle.log.name):
        c = bundle.inspect_bundle(bad)
    # v0.51.342: removing _TAIL_INFLATE_BUDGET makes this a VALID gzip stream again → ok flips True (the mutation signal)
    assert c.ok is False and c.error.startswith("not a motif bundle:") and c.error.count("not a motif bundle:") == 1, c.error
    assert "after the archive's end" in c.error and str(bundle._TAIL_INFLATE_BUDGET) in c.error, c.error
    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert [m for m in warned if bad.name in m and str(bundle._TAIL_INFLATE_BUDGET) in m], f"v0.51.344: the refusal leaves a breadcrumb: {warned}"


def test_a_refused_tail_shape_stages_nothing_and_frees_the_lock(tmp_path):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    bad = _place(cd, NAME, _shapes(b)["400k empty gzip members"])
    with _within(TAIL_BOUND):
        with pytest.raises(ValueError):
            bundle.stage_bundle_restore(db, cd, bad, keep_config=False)
    assert bundle.pending_members(db, cd) == []
    assert not bundle.STAGING_LOCK.locked()
    assert not list(cd.glob(".bundle-*")) and not list(cd.glob("*.tmp"))


def _inflate_pad(tmp_path: Path, monkeypatch, b: Path) -> int:
    tar, fed, real = _tar_bytes(b), [], bundle._TarFeed.read

    def read(self, size=-1):
        data = real(self, size)
        fed.append(len(data))
        return data
    monkeypatch.setattr(bundle._TarFeed, "read", read)
    sizing = tmp_path / "sizing.tar.gz"
    sizing.write_bytes(gzip.compress(tar + bytes(bundle._TAIL_INFLATE_BUDGET)))
    assert bundle.inspect_bundle(sizing).ok, "the premise: the sizing copy drains under the budget"
    monkeypatch.setattr(bundle._TarFeed, "read", real)
    return bundle._TAIL_INFLATE_BUDGET + sum(fed) - len(tar)  # v0.51.344: what tarfile fetched is not drained — this pad drains exactly the budget; the old tail sat 1.59 MiB under it


@pytest.mark.parametrize("how", ["a raw tail just under the raw budget", "an inflated tail exactly at the inflate budget"])
def test_a_tail_just_under_budget_still_restores(tmp_path, monkeypatch, how):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    if how == "a raw tail just under the raw budget":
        # trailing zero padding a real writer can leave, read inside the one raw allowance the archive's last fetch and the drain share (measured 44 KiB from its boundary, under one 128 KiB gzip raw read)
        data = b.read_bytes() + bytes(bundle._TAIL_RAW_BUDGET - 64 * 1024)
    else:
        data = gzip.compress(_tar_bytes(b) + bytes(_inflate_pad(tmp_path, monkeypatch, b)))
    ok = _place(cd, NAME, data)
    assert bundle.inspect_bundle(ok).ok, how
    assert bundle.stage_bundle_restore(db, cd, ok, keep_config=False).staged == ["database", "config", "cookies"]


def test_an_inflated_tail_one_byte_over_the_inflate_budget_is_refused(tmp_path, monkeypatch):
    b = _bundle(tmp_path / "mk")
    _, cd = _live(tmp_path)
    bad = _place(cd, NAME, gzip.compress(_tar_bytes(b) + bytes(_inflate_pad(tmp_path, monkeypatch, b) + 1)))
    c = bundle.inspect_bundle(bad)
    assert c.ok is False and c.error.count("not a motif bundle:") == 1 and str(bundle._TAIL_INFLATE_BUDGET) in c.error, c.error


def test_a_real_bundle_still_passes_and_stages(tmp_path):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    assert bundle.inspect_bundle(b).ok
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config", "cookies"]


def _split_tails(b: Path) -> dict[str, bytes]:
    raw, zeros, budget = b.read_bytes(), gzip.compress(bytes(MiB), mtime=0), bundle._TAIL_RAW_BUDGET
    def empties(n: int) -> bytes:
        return _EMPTY_MEMBER * (n // len(_EMPTY_MEMBER))
    return {
        # empty members with 1 MiB of inflated zeros between them: each drain read stayed under a per-read raw cap
        "empty members then 1 MiB of zeros, 8 times": raw + (empties(budget * 37 // 40) + zeros) * 8,
        "1 MiB of zeros then 4/5 of a budget of empty members, 8 times": raw + (zeros + empties(budget * 4 // 5)) * 8,
        "1 MiB of zeros then 4/5 of a budget of empty members, 3 times": raw + (zeros + empties(budget * 4 // 5)) * 3,
        # over one allowance of empty members in all, split across the archive's last fetch and the drain
        "empty members split around 1 MiB of zeros": raw + empties(budget * 3 // 4) + zeros + empties(budget * 2 // 5),
    }


@pytest.mark.parametrize("how", ["empty members then 1 MiB of zeros, 8 times",
                                 "1 MiB of zeros then 4/5 of a budget of empty members, 8 times",
                                 "1 MiB of zeros then 4/5 of a budget of empty members, 3 times",
                                 "empty members split around 1 MiB of zeros"])
def test_a_tail_split_by_inflated_data_is_refused_by_the_raw_budget_and_stages_nothing(tmp_path, monkeypatch, how):
    monkeypatch.setattr(bundle, "_TAIL_RAW_BUDGET", bundle._STREAM_BUF)  # v0.51.344: one fetch's worth of raw budget refuses these in a third of the time and still kills a per-read or pre-drain reset
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    bad = _place(cd, NAME, _split_tails(b)[how])
    with _within(TAIL_BOUND):
        c = bundle.inspect_bundle(bad)
    # v0.51.342: a fresh raw allowance per drain read ran these to the inflate budget, or restored them, after seconds of parsing
    assert c.ok is False and c.error.count("not a motif bundle:") == 1, c.error
    assert "padding or empty gzip members" in c.error and str(bundle._TAIL_RAW_BUDGET) in c.error, c.error
    with _within(TAIL_BOUND), pytest.raises(ValueError) as refused:
        bundle.stage_bundle_restore(db, cd, bad, keep_config=False)
    assert str(bundle._TAIL_RAW_BUDGET) in str(refused.value), refused.value
    assert bundle.pending_members(db, cd) == []
    assert not bundle.STAGING_LOCK.locked()
    assert not list(cd.glob(".bundle-*")) and not list((cd / "backups").glob(".bundle-*")) and not list(cd.glob("*.tmp"))


def test_a_database_that_compresses_past_many_raw_allowances_still_passes_and_stages(tmp_path):
    src = tmp_path / "mk"
    src.mkdir()
    db = src / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE pad (b BLOB)")
        conn.execute("INSERT INTO pad VALUES (?)", (os.urandom(3 * bundle._TAIL_RAW_BUDGET),))  # incompressible: every inflated byte costs a raw byte
        conn.commit()
    (src / "motif.yaml").write_text("plex:\n  url: http://plex:32400\n")
    (src / "cookies.txt").write_text("# bundle cookies\n")
    bf = bundle.create_bundle(db, src, config_file=src / "motif.yaml", cookies_file=src / "cookies.txt", themes_dir=None,
                              now_stamp=NOW, motif_version="0.51.342", schema_version=CURRENT_SCHEMA_VERSION)
    b = src / "backups" / bf.name
    assert b.stat().st_size > 3 * bundle._TAIL_RAW_BUDGET, "the premise: no single raw allowance covers this database"
    live_db, cd = _live(tmp_path)
    # v0.51.342: each 1 MiB tarfile fetches starts a fresh allowance — one allowance for the whole stream refused every database over ~2 MiB compressed
    c = bundle.inspect_bundle(b)
    assert c.ok, c.error
    assert bundle.stage_bundle_restore(live_db, cd, b, keep_config=False).staged == ["database", "config", "cookies"]


# ── 2. parser faults are refused, not raised ─────────────────────────

def _checksum(hdr: bytearray) -> bytearray:
    hdr[148:156] = b" " * 8
    hdr[148:156] = b"%06o\0 " % sum(hdr)
    return hdr


def _sparse_S_at_end(b: Path) -> bytes:
    """A GNU sparse 'S' header whose isextended flag is set, with no extended block after it: _proc_sparse reads
       an empty block and indexes it → IndexError, before _member_refusal ever sees the type."""
    tar = bytearray(_tar_bytes(b))
    while len(tar) >= tarfile.BLOCKSIZE and tar[-tarfile.BLOCKSIZE:] == bytes(tarfile.BLOCKSIZE):
        del tar[-tarfile.BLOCKSIZE:]  # drop the end-of-archive zero blocks so the sparse header is the last thing read
    ti = tarfile.TarInfo("motif.db")
    ti.size = 0
    hdr = bytearray(ti.tobuf(format=tarfile.GNU_FORMAT))
    hdr[156:157] = b"S"     # GNUTYPE_SPARSE
    hdr[482] = 1            # isextended
    return gzip.compress(bytes(tar) + bytes(_checksum(hdr)))


def _extension_header_chain(b: Path, kind: bytes, n: int) -> bytes:
    """n back-to-back 'x'/'g'/'L' extension headers: each _proc_pax/_proc_gnulong recurses into the next → RecursionError."""
    h = tarfile.TarInfo("././@Header")
    h.size = 0
    hb = bytearray(h.tobuf(format=tarfile.USTAR_FORMAT))
    hb[156:157] = kind
    block = bytes(_checksum(hb))
    return gzip.compress(block * n + _tar_bytes(b))


def _parser_faults(b: Path) -> dict[str, bytes]:
    return {
        "a GNU sparse 'S' extended header at end of stream": _sparse_S_at_end(b),
        "a chain of 'x' extension headers": _extension_header_chain(b, b"x", 600),
        "a chain of 'g' extension headers": _extension_header_chain(b, b"g", 600),
        "a chain of 'L' extension headers": _extension_header_chain(b, b"L", 600),
    }


@pytest.mark.parametrize("how", ["a GNU sparse 'S' extended header at end of stream",
                                 "a chain of 'x' extension headers",
                                 "a chain of 'g' extension headers",
                                 "a chain of 'L' extension headers"])
def test_a_parser_fault_is_refused_by_inspect_and_by_stage_and_leaves_nothing(tmp_path, how):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    bad = _place(cd, NAME, _parser_faults(b)[how])
    # v0.51.342: without IndexError/RecursionError in the refusal tuple, inspect_bundle RAISES here instead of refusing
    c = bundle.inspect_bundle(bad)
    assert c.ok is False and c.error.startswith("not a motif bundle:") and c.error.count("not a motif bundle:") == 1, c.error
    with pytest.raises(ValueError):
        bundle.stage_bundle_restore(db, cd, bad, keep_config=False)
    assert bundle.pending_members(db, cd) == []
    assert not bundle.STAGING_LOCK.locked()
    assert not [p.name for p in cd.iterdir() if p.name.startswith(".bundle-")]
    assert not list((cd / "backups").glob(".bundle-*")) and not list(cd.glob("*.tmp"))
