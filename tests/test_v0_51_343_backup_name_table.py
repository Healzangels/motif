"""v0.51.343: the backup name gate, kind, stamp and retention read one table (db_backup._classify)."""
from __future__ import annotations

import dataclasses
import itertools
import re
from pathlib import Path

import pytest

from app.core import db_backup

NOW = "20260912-040000"


# ── frozen copy of the pre-v0.51.343 functions: the equivalence oracle, never edit ──

_OLD_BACKUP_RE = re.compile(r"^motif-(\d{8}-\d{6})\.db$")
_OLD_PRERESTORE_RE = re.compile(r"^motif-prerestore-(\d{8}-\d{6})\.db$")
_OLD_BUNDLE_RE = re.compile(r"^motif-bundle-(\d{8}-\d{6})\.tar\.gz$")


def _old_is_backup_name(name):
    if "/" in name or "\\" in name or name in (".", ".."):
        return False
    return bool(_OLD_BACKUP_RE.match(name) or _OLD_PRERESTORE_RE.match(name)
                or _OLD_BUNDLE_RE.match(name))


def _old_kind_of(name):
    if not _old_is_backup_name(name):
        return None
    if _OLD_BUNDLE_RE.match(name):
        return "bundle"
    if _OLD_PRERESTORE_RE.match(name):
        return "prerestore"
    return "snapshot"


def _old_stamp_of(name):
    m = _OLD_BACKUP_RE.match(name) or _OLD_PRERESTORE_RE.match(name) or _OLD_BUNDLE_RE.match(name)
    return m.group(1) if m else None


def _old_list_backups(config_dir):
    bdir = config_dir / "backups"
    if not bdir.exists():
        return []
    out = []
    for p in bdir.iterdir():
        if not p.is_file():
            continue
        stamp = _old_stamp_of(p.name)
        if stamp is None:
            continue
        try:
            size = p.stat().st_size
        except OSError:
            continue
        out.append(db_backup.BackupFile(name=p.name, size=size,
                                        created_at=db_backup._iso_from_stamp(stamp),
                                        kind=_old_kind_of(p.name) or "snapshot"))
    out.sort(key=lambda b: (_old_stamp_of(b.name) or "", b.name), reverse=True)
    return out


def _old_prune_backups(config_dir, retention):
    if retention <= 0:
        return []
    routine = [b for b in _old_list_backups(config_dir)
               if _OLD_BACKUP_RE.match(b.name) or _OLD_BUNDLE_RE.match(b.name)]
    removed = []
    for b in routine[retention:]:
        (config_dir / "backups" / b.name).unlink()
        removed.append(b.name)
    return removed


# ── v0.51.343: what the tightened table must answer ──────────────────

def _admissible(name):
    # v0.51.343: printable ASCII — the old `\d` admitted full-width digits and its `$` a trailing newline, both now refused
    return name.isascii() and name.isprintable()


def _upload_stamp(name):
    # v0.51.343: the uploaded-bundle shape spelled without a regex — motif-bundle-upload-<8 ASCII digits>-<6>.tar.gz
    pre, suf = "motif-bundle-upload-", ".tar.gz"
    mid = name[len(pre):-len(suf)] if name.startswith(pre) and name.endswith(suf) else ""
    ok = len(mid) == 15 and mid[8] == "-" and all(c in "0123456789" for c in mid[:8] + mid[9:])
    return mid if ok else None


def _want(name):
    """(is_backup_name, kind_of, _stamp_of, retained): the frozen copy on printable ASCII, uploads outside retention, quirks refused."""
    stamp = _upload_stamp(name)
    if stamp is not None:
        return True, "bundle", stamp, False
    kind = _old_kind_of(name) if _admissible(name) else None
    if kind is None:
        return False, None, None, None
    return True, kind, _old_stamp_of(name), bool(_OLD_BACKUP_RE.match(name) or _OLD_BUNDLE_RE.match(name))


# ── the generated name corpus ────────────────────────────────────────

_STAMPS = (NOW, "20260101-000000", "00000000-000000", "99999999-999999",
           "２０２６０９１２-０４００００", "٢٠٢٦٠٩١٢-٠٤٠٠٠٠",
           "2026091-040000", "202609120-040000", "20260912_040000", "20260912-04000",
           "20260912-0400000", "20260912040000", "2026O912-040000", "x", "")
_PREFIXES = ("motif-", "motif-prerestore-", "motif-bundle-", "Motif-", "MOTIF-", "motif-Bundle-",
             "motif-PRERESTORE-", "motif_", "motif-bundle_", "motif-prerestore", "motif--",
             "motif-snapshot-", "xmotif-", " motif-", "", "./motif-", "../motif-",
             "backups/motif-", "..\\motif-", "/motif-",
             "motif-bundle-upload-", "motif-bundle-upload_", "motif-bundle-upload", "motif-Bundle-Upload-",
             "motif-upload-", "motif-prerestore-upload-", "motif-bundle-upload-upload-", "backups/motif-bundle-upload-")
_SUFFIXES = (".db", ".tar.gz", ".tar", ".gz", ".DB", ".TAR.GZ", ".db.part", ".tar.gz.part",
             ".db\n", ".tar.gz\n", ".db\n\n", ".db ", " .db", ".db/", ".db\\", "", ".db/..",
             ".tar.gz/../motif.db", ".db-wal", ".dbx", ".tar.gz.db", ".db.tar.gz")
_EXTRAS = ("", ".", "..", "/", "\\", "manifest.json", "motif.db", "motif-2026.db",
           "motif-bundle-x.tar.gz", "../../etc/passwd", f"motif-{NOW}.db\x00",
           f"\nmotif-{NOW}.db", f"motif-{NOW}.db\r", f"motif-{NOW}.db\n.db")
CORPUS = tuple(dict.fromkeys(
    [p + s + x for p, s, x in itertools.product(_PREFIXES, _STAMPS, _SUFFIXES)] + list(_EXTRAS)))


def test_the_corpus_reaches_every_old_outcome():
    assert {_old_kind_of(n) for n in CORPUS} == {"snapshot", "prerestore", "bundle", None}
    assert any("\n" in n and _old_is_backup_name(n) for n in CORPUS), "a trailing-newline name the old `$` admitted"
    assert any(not n.isascii() and _old_is_backup_name(n) for n in CORPUS), "a non-ASCII digit stamp the old \\d admitted"
    assert any(("/" in n or "\\" in n) and _old_is_backup_name(re.split(r"[/\\]", n)[-1]) for n in CORPUS), \
        "a traversal attempt wrapped around a valid name"
    # v0.51.343: the upload shape, and its near misses the old table and the new one both refuse
    assert any(_upload_stamp(n) for n in CORPUS) and any(n.startswith("motif-bundle-upload") and not _want(n)[0] for n in CORPUS)


def test_name_functions_match_the_frozen_copy_on_printable_ascii_and_refuse_the_quirks():
    # v0.51.343: retargeted from "equal to the frozen copy everywhere" — that pinned the `\d` / `$` admissions
    diffs = []
    for n in CORPUS:
        want = _want(n)[:3]
        got = (db_backup.is_backup_name(n), db_backup.kind_of(n), db_backup._stamp_of(n))
        if got != want:
            diffs.append((n, want, got))
    assert not diffs, diffs[:10]
    quirks = [n for n in CORPUS if _old_is_backup_name(n) and not _admissible(n)]
    assert {"\n" in n for n in quirks} == {True, False}, "both quirks: a trailing newline and a non-ASCII digit stamp"
    assert [n for n in quirks if db_backup.is_backup_name(n) or db_backup.kind_of(n) or db_backup._stamp_of(n)] == []


def test_classify_is_the_one_answer_for_gate_kind_stamp_and_retention():
    assert db_backup._classify(f"motif-{NOW}.db") == ("snapshot", NOW, True)
    assert db_backup._classify(f"motif-bundle-{NOW}.tar.gz") == ("bundle", NOW, True)
    assert db_backup._classify(f"motif-prerestore-{NOW}.db") == ("prerestore", NOW, False)
    assert db_backup._classify(f"motif-bundle-upload-{NOW}.tar.gz") == ("bundle", NOW, False)  # v0.51.343
    for n in CORPUS:
        ok, kind, stamp, retained = _want(n)
        assert db_backup._classify(n) == ((kind, stamp, retained) if ok else None), n


# ── the same comparison on a real backups dir ────────────────────────

_FS_NEAR_MISSES = (f"motif-bundle-{NOW}.tar", "motif-bundle-x.tar.gz", f"motif-{NOW}.db.part",
                   f"motif_{NOW}.db", f"motif-bundle-{NOW}.tar.gz.part", f"motif-{NOW}.db ",
                   f" motif-{NOW}.db", f"motif-{NOW}.dbx", f"motif-prerestore{NOW}.db",
                   f"motif-{NOW}.db\n\n", "manifest.json", "motif.db", "notes.txt",
                   f"motif-bundle-upload-{NOW}.tar", f"motif-bundle-upload-{NOW}.tar.gz.part")


def _fs_names():
    names, seen = [], set()
    for n in [n for n in CORPUS if _old_is_backup_name(n) or _upload_stamp(n)] + list(_FS_NEAR_MISSES):
        if n.casefold() not in seen:  # a case-insensitive filesystem would merge the two
            seen.add(n.casefold())
            names.append(n)
    return names


def _populate(cd: Path, names: list[str]) -> list[str]:
    bdir = cd / "backups"
    bdir.mkdir(parents=True)
    for i, n in enumerate(names):
        (bdir / n).write_bytes(b"x" * i)
    (bdir / "motif-20250101-000000.db").mkdir()
    assert len(list(bdir.iterdir())) == len(names) + 1
    return names


def _frozen_reach(n):
    # v0.51.343: the names the frozen copy is still the oracle for — printable ASCII, not an upload
    return _admissible(n) and not _upload_stamp(n)


def test_list_backups_matches_the_frozen_copy_on_disk(tmp_path):
    names = _populate(tmp_path, _fs_names())
    rows = db_backup.list_backups(tmp_path)
    # v0.51.343: retargeted — quirk names no longer list, uploads list as retained-False bundles, retained rides every row
    old = [dataclasses.replace(b, retained=_want(b.name)[3]) for b in _old_list_backups(tmp_path) if _frozen_reach(b.name)]
    assert [r for r in rows if _frozen_reach(r.name)] == old
    assert len(rows) == sum(1 for n in names if _want(n)[0]) and all(_admissible(r.name) for r in rows)
    ups = [r for r in rows if _upload_stamp(r.name)]
    assert sorted(r.name for r in ups) == sorted(n for n in names if _upload_stamp(n)) and ups
    assert all((r.kind, r.retained, r.created_at) == ("bundle", False, db_backup._iso_from_stamp(_upload_stamp(r.name)))
               for r in ups)
    assert rows == sorted(rows, key=lambda r: (_want(r.name)[2], r.name), reverse=True), "newest first across every shape"
    assert {(r.kind, r.retained) for r in rows} == {("snapshot", True), ("prerestore", False), ("bundle", True), ("bundle", False)}
    assert db_backup.list_backups(tmp_path / "absent") == _old_list_backups(tmp_path / "absent") == []


@pytest.mark.parametrize("retention", [-1, 0, 1, 2, 7, 1000])
def test_prune_backups_matches_the_frozen_copy_on_disk(tmp_path, retention):
    names = _fs_names()
    # v0.51.343: retargeted — the frozen copy prunes the printable-ASCII names; quirk names and uploads sit beside them untouched
    _populate(tmp_path / "old", [n for n in names if _frozen_reach(n)])
    _populate(tmp_path / "new", names)
    removed = db_backup.prune_backups(tmp_path / "new", retention)
    assert removed == _old_prune_backups(tmp_path / "old", retention)
    assert (sorted(p.name for p in (tmp_path / "new" / "backups").iterdir())
            == sorted([p.name for p in (tmp_path / "old" / "backups").iterdir()] + [n for n in names if not _frozen_reach(n)]))
    if retention in (2, 7):
        assert removed and not any("prerestore" in n for n in removed)
