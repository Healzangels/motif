"""v0.51.344 (integration fixes 2, R2-F1): UPLOAD MP3, adopt and the edition swap are canonical writers like the others.

  1. A RESTORE FROM PLEX mid-stage on a row's canonical holds the path's lock: an UPLOAD MP3 arriving then waits,
     lands whole in a fresh inode, and the restored bytes stay in the Plex folder's inode.
  2. Two uploads on one row serialise on the same lock — neither is torn and no staging file is left behind.
  3. A sidecar adopt and ADOPT FROM PLEX wait out a restore mid-stage on their canonical and land consistent.
  4. An edition swap arriving while a restore stages the survivor's canonical waits and never clobbers it.
  5. Both writers stage: adopt's cross-filesystem copy and the upload's bytes never sit half-written at the canonical
     path, and a failed write leaves the canonical as it was with no staging file behind.
  6. Both adopts' "already this link" short-circuit is device AND inode: a canonical on the config volume whose inode
     NUMBER coincides with the Plex file's on the media volume is not that link — the copy is staged, not skipped.
"""
from __future__ import annotations

import errno
import hashlib
import os
import shutil
import sqlite3
import threading
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from app.core import placement
from app.core.canonical import canonical_theme_subdir
from app.core.db import get_conn
from test_v0_51_271_edition_swap import TID, _add_plex_row, _row, _swap, env  # noqa: F401 — env is a fixture
from test_v0_51_339_canonical_health_restore import (  # noqa: F401 — admin_client is a fixture
    AUTH, NOW, _lf, _lf_cols, _placement, _section, _theme, admin_client,
)
from test_v0_51_341_canonical_writers import _plex_item

PLEX = b"ID3" + b"plex-folder-bytes" * 8
USER = b"ID3" + b"user-upload-bytes" * 8
OTHER = b"ID3" + b"a-second-upload" * 8
SIDECAR = b"ID3" + b"sidecar-finding-bytes" * 8
REJECTED = b"ID3" + b"the-rejected-upload" * 8


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class _Contended:
    """The per-path write lock, counting every writer that has to wait for it."""
    def __init__(self, real, waiting):
        self.real, self.waiting = real, waiting

    def acquire(self, blocking=True, timeout=-1):
        if self.real.acquire(blocking=False):
            return True
        self.waiting.release()
        return self.real.acquire(blocking, timeout)

    def release(self):
        self.real.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()


def _contended_locks(monkeypatch):
    """Every canonical writer's lock notes a wait; returns the semaphore counting the waiters."""
    waiting = threading.Semaphore(0)
    real = ch._canonical_write_lock
    monkeypatch.setattr(ch, "_canonical_write_lock", lambda p: _Contended(real(p), waiting))
    return waiting


def _hold_the_restore_stage(monkeypatch):
    """The restore stops inside its stage — under the path's lock, before its os.replace — until `go`."""
    midway, go = threading.Event(), threading.Event()
    real = placement._stage_link_or_copy  # v0.51.344: the restore stages through the link-or-copy half (R2-F7 hashes the staged inode before its move)

    def held(src, dst_tmp):
        midway.set()
        assert go.wait(10)
        return real(src, dst_tmp)
    monkeypatch.setattr(placement, "_stage_link_or_copy", held)
    return midway, go


def _run(out: dict, key: str, fn):
    try:
        out[key] = fn()
    except Exception as e:  # noqa: BLE001 — the assertions surface it
        out[key] = e


def _row_dict(db, tmdb):
    with get_conn(db) as conn:
        lf = conn.execute("SELECT * FROM local_files WHERE tmdb_id = ?", (tmdb,)).fetchone()
        p, _sidecar = ch._placement_for(conn, lf)
    return {**dict(lf), "media_folder": p["media_folder"], "placement_kind": p["placement_kind"]}


def _restore_thread(db, themes, tmdb, out):
    return threading.Thread(
        target=lambda: _run(out, "restore", lambda: ch.restore_from_placement(db, themes, _row_dict(db, tmdb))),
        daemon=True)


def _staging(themes: Path) -> list[str]:
    return sorted(p.name for pat in ("*.motif-tmp", "*.adopt.tmp", "*.part") for p in themes.rglob(pat))


def _plex_folder(tmp_path, name, data):
    folder = tmp_path / "plex" / name
    folder.mkdir(parents=True)
    (folder / "theme.mp3").write_bytes(data)
    return folder


def _seed_upload_row(settings, tmp_path, tmdb, rk):
    """A broken canonical whose copy survives in its Plex folder: a restore target and, with its placement, an upload mismatch."""
    folder = _plex_folder(tmp_path, f"T{tmdb} (2001)", PLEX)
    rel = f"movies/{canonical_theme_subdir(f'T{tmdb}', '2001', '')}/theme.mp3"
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, tmdb)
        _plex_item(conn, rk, tmdb, str(folder))
        _lf(conn, tmdb, canonical_present=0, file_size=len(PLEX), file_sha256=_sha(PLEX), extra={"file_path": rel})
        _placement(conn, tmdb, str(folder))
        conn.commit()
    return Path(settings.themes_dir) / rel, folder / "theme.mp3"


def _upload(client, rk, body):
    return client.post(f"/api/plex_items/{rk}/upload-theme", headers=AUTH,
                       files={"file": ("theme.mp3", body, "audio/mpeg")})


# ── 1: an upload meeting a restore mid-stage ─────────────────────────

def test_an_upload_arriving_while_a_restore_stages_its_canonical_waits_and_lands_in_a_fresh_inode(admin_client, monkeypatch):
    client, settings, tmp_path = admin_client
    monkeypatch.setattr("app.core.revisions.capture_revision", lambda *a, **k: None)
    tmdb, rk = 4401, "94401"
    canonical, plex_file = _seed_upload_row(settings, tmp_path, tmdb, rk)
    waiting = _contended_locks(monkeypatch)
    midway, go = _hold_the_restore_stage(monkeypatch)
    out: dict = {}
    restore = _restore_thread(settings.db_path, Path(settings.themes_dir), tmdb, out)
    upload = threading.Thread(target=lambda: _run(out, "upload", lambda: _upload(client, rk, USER)), daemon=True)
    restore.start()
    try:
        assert midway.wait(10), "premise: the restore is mid-stage under the path's lock"
        upload.start()
        assert waiting.acquire(timeout=10), "the upload did not wait for the path's lock"
        assert not canonical.exists(), "a writer landed the canonical while the restore was mid-stage"
    finally:
        go.set()
    restore.join(10)
    upload.join(10)
    assert out["restore"] == {"ok": True, "kind": "hardlink"}
    assert out["upload"].status_code == 200, out["upload"].text
    assert canonical.read_bytes() == USER
    assert plex_file.read_bytes() == PLEX, \
        "the upload wrote through the inode the restore had just re-linked into the Plex folder"
    assert canonical.stat().st_ino != plex_file.stat().st_ino
    assert _lf_cols(settings.db_path, tmdb, ("canonical_present", "file_size", "file_sha256", "mismatch_state")) == (
        1, len(USER), _sha(USER), "pending")
    assert _staging(Path(settings.themes_dir)) == []


# ── 2: two uploads on one row ────────────────────────────────────────

def test_two_uploads_on_one_row_serialise_on_the_paths_lock_and_neither_is_torn(admin_client, monkeypatch):
    client, settings, tmp_path = admin_client
    monkeypatch.setattr("app.core.revisions.capture_revision", lambda *a, **k: None)
    tmdb, rk = 4402, "94402"
    canonical, plex_file = _seed_upload_row(settings, tmp_path, tmdb, rk)
    held = ch._canonical_write_lock(canonical)  # the real lock, taken before the wrapper counts waiters
    waiting = _contended_locks(monkeypatch)
    out: dict = {}
    uploads = [threading.Thread(target=lambda k=k, b=b: _run(out, k, lambda: _upload(client, rk, b)), daemon=True)
               for k, b in (("a", USER), ("b", OTHER))]
    held.acquire()
    try:
        for t in uploads:
            t.start()
        assert waiting.acquire(timeout=10) and waiting.acquire(timeout=10), \
            "both uploads did not wait for the path's lock"
        assert not canonical.exists(), "an upload landed its canonical while another writer held the path"
    finally:
        held.release()
    for t in uploads:
        t.join(10)
    assert {out["a"].status_code, out["b"].status_code} == {200}, (out["a"].text, out["b"].text)
    disk = canonical.read_bytes()
    assert disk in (USER, OTHER), "the canonical is torn between the two uploads"
    assert plex_file.read_bytes() == PLEX and canonical.stat().st_ino != plex_file.stat().st_ino
    present, sha = _lf_cols(settings.db_path, tmdb, ("canonical_present", "file_sha256"))
    assert present == 1 and sha in (_sha(USER), _sha(OTHER))
    assert _staging(Path(settings.themes_dir)) == []


# ── 3: adopt's two writers meeting a restore mid-stage ───────────────

def test_a_sidecar_adopt_arriving_while_a_restore_stages_its_canonical_waits_and_lands_its_own_bytes(admin_client, monkeypatch):
    _client, settings, tmp_path = admin_client
    import app.core.adopt as adopt_mod
    monkeypatch.setattr(adopt_mod, "log_event", lambda *a, **k: None)
    tmdb = 4403
    sidecar = _plex_folder(tmp_path, "sidecar/LotR (2001)", SIDECAR) / "theme.mp3"
    plex_file = _plex_folder(tmp_path, "placed/LotR (2001)", PLEX) / "theme.mp3"
    rel = f"movies/{canonical_theme_subdir('LotR', '2001', '')}/theme.mp3"
    canonical = Path(settings.themes_dir) / rel
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        conn.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, upstream_source, last_seen_sync_at,"
                     " first_seen_sync_at) VALUES (?, 'movie', ?, 'LotR', '2001', 'imdb', ?, ?)", (tmdb, tmdb, NOW, NOW))
        _lf(conn, tmdb, canonical_present=0, file_size=len(PLEX), file_sha256=_sha(PLEX), extra={"file_path": rel})
        _placement(conn, tmdb, str(plex_file.parent))
        conn.commit()
    finding = {"section_id": "1", "section_type": "movie", "finding_kind": "content_mismatch", "theme_id": tmdb,
               "file_path": str(sidecar), "file_sha256": _sha(SIDECAR), "file_size": len(SIDECAR),
               "media_folder": str(sidecar.parent)}
    waiting = _contended_locks(monkeypatch)
    midway, go = _hold_the_restore_stage(monkeypatch)
    out: dict = {}
    restore = _restore_thread(settings.db_path, Path(settings.themes_dir), tmdb, out)
    adopt = threading.Thread(
        target=lambda: _run(out, "adopt", lambda: adopt_mod._do_adopt(settings.db_path, finding, settings, "t")),
        daemon=True)
    restore.start()
    try:
        assert midway.wait(10), "premise: the restore is mid-stage under the path's lock"
        adopt.start()
        assert waiting.acquire(timeout=10), "the adopt did not wait for the path's lock"
        assert not canonical.exists(), "a writer landed the canonical while the restore was mid-stage"
    finally:
        go.set()
    restore.join(10)
    adopt.join(10)
    assert out["restore"] == {"ok": True, "kind": "hardlink"}
    assert isinstance(out["adopt"], dict) and out["adopt"]["placement_kind"] == "hardlink", out["adopt"]
    assert canonical.read_bytes() == SIDECAR and canonical.stat().st_ino == sidecar.stat().st_ino
    assert plex_file.read_bytes() == PLEX
    assert _lf_cols(settings.db_path, tmdb, ("canonical_present", "file_sha256")) == (1, _sha(SIDECAR)), \
        "the stamp is not the bytes on disk"
    assert _staging(Path(settings.themes_dir)) == []


def test_adopt_from_plex_arriving_while_a_restore_stages_its_canonical_waits_and_lands_consistent(admin_client, monkeypatch):
    client, settings, tmp_path = admin_client
    tmdb = 4404
    plex_file = _plex_folder(tmp_path, f"T{tmdb} (2001)", PLEX) / "theme.mp3"
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, tmdb)
        _lf(conn, tmdb, canonical_present=0, file_size=len(PLEX), file_sha256=_sha(PLEX),
            extra={"mismatch_state": "pending"})
        _placement(conn, tmdb, str(plex_file.parent))
        conn.commit()
    canonical = Path(settings.themes_dir) / _lf_cols(settings.db_path, tmdb, ("file_path",))[0]
    waiting = _contended_locks(monkeypatch)
    midway, go = _hold_the_restore_stage(monkeypatch)
    out: dict = {}
    restore = _restore_thread(settings.db_path, Path(settings.themes_dir), tmdb, out)
    adopt = threading.Thread(
        target=lambda: _run(out, "adopt", lambda: client.post(f"/api/items/movie/{tmdb}/adopt-from-plex", headers=AUTH)),
        daemon=True)
    restore.start()
    try:
        assert midway.wait(10), "premise: the restore is mid-stage under the path's lock"
        adopt.start()
        assert waiting.acquire(timeout=10), "ADOPT FROM PLEX did not wait for the path's lock"
        assert not canonical.exists(), "a writer landed the canonical while the restore was mid-stage"
    finally:
        go.set()
    restore.join(10)
    adopt.join(10)
    assert out["restore"] == {"ok": True, "kind": "hardlink"}
    assert (out["adopt"].status_code, out["adopt"].json()["sections_adopted"]) == (200, 1), out["adopt"].text
    assert canonical.read_bytes() == PLEX and canonical.stat().st_ino == plex_file.stat().st_ino
    assert _lf_cols(settings.db_path, tmdb, ("canonical_present", "file_sha256", "mismatch_state")) == (
        1, _sha(PLEX), None)
    assert _staging(Path(settings.themes_dir)) == []


# ── 4: an edition swap meeting a restore on the survivor's path ──────

def test_an_edition_swap_arriving_while_a_restore_stages_the_survivors_canonical_waits_and_keeps_it(env, monkeypatch):
    db, themes, old_rel = env
    monkeypatch.setattr("app.core.edition_swap.log_event", lambda *a, **k: None)
    _add_plex_row(db, "rk-theatrical", "", "/data/movies/Twilight (2008)")
    # the survivor's canonical path is a restore target of another row keyed to the untagged folder
    other = TID + 1
    new_rel = f"movies/{canonical_theme_subdir('Twilight', '2008', '')}/theme.mp3"
    plex_file = _plex_folder(themes.parent, "Twilight (2008)", PLEX) / "theme.mp3"
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, upstream_source, last_seen_sync_at,"
                     " first_seen_sync_at) VALUES (?, 'movie', ?, 'Twilight', '2008', 'plex_orphan', ?, ?)",
                     (other, other, NOW, NOW))
        _lf(conn, other, canonical_present=0, file_size=len(PLEX), file_sha256=_sha(PLEX), extra={"file_path": new_rel})
        _placement(conn, other, str(plex_file.parent))
        conn.commit()
    old_abs, new_abs = themes / old_rel, themes / new_rel
    waiting = _contended_locks(monkeypatch)
    midway, go = _hold_the_restore_stage(monkeypatch)
    out: dict = {}
    restore = _restore_thread(db, themes, other, out)
    swap = threading.Thread(target=lambda: _run(out, "swap", lambda: _swap(db, themes)), daemon=True)
    restore.start()
    try:
        assert midway.wait(10), "premise: the restore is mid-stage under the survivor path's lock"
        swap.start()
        assert waiting.acquire(timeout=10), "the swap did not wait for the survivor path's lock"
        assert not new_abs.exists(), "a writer landed the survivor's canonical while the restore was mid-stage"
    finally:
        go.set()
    restore.join(10)
    swap.join(10)
    assert out["restore"] == {"ok": True, "kind": "hardlink"}
    assert out["swap"] is None, "the swap moved the lost edition's canonical over the one the restore had just landed"
    assert new_abs.read_bytes() == PLEX and new_abs.stat().st_ino == plex_file.stat().st_ino
    assert old_abs.read_bytes() == b"ID3theme", "the lost edition's canonical is gone"
    assert _row(db, "local_files", "extended")["file_path"] == old_rel
    assert _lf_cols(db, other, ("canonical_present", "file_sha256")) == (1, _sha(PLEX))
    assert _staging(themes) == []


# ── 5: both writers stage ────────────────────────────────────────────

@pytest.mark.parametrize("copy", ["lands", "fails"])
def test_adopts_cross_fs_copy_never_sits_half_written_at_the_canonical_path(admin_client, monkeypatch, copy):
    _client, settings, tmp_path = admin_client
    import app.core.adopt as adopt_mod
    monkeypatch.setattr(adopt_mod, "log_event", lambda *a, **k: None)
    tmdb = 4405
    sidecar = _plex_folder(tmp_path, "sidecar/LotR (2001)", SIDECAR) / "theme.mp3"
    canonical = Path(settings.themes_dir) / f"movies/{canonical_theme_subdir('LotR', '2001', '')}/theme.mp3"
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        conn.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, upstream_source, last_seen_sync_at,"
                     " first_seen_sync_at) VALUES (?, 'movie', ?, 'LotR', '2001', 'imdb', ?, ?)", (tmdb, tmdb, NOW, NOW))
        conn.commit()
    finding = {"section_id": "1", "section_type": "movie", "finding_kind": "content_mismatch", "theme_id": tmdb,
               "file_path": str(sidecar), "file_sha256": _sha(SIDECAR), "file_size": len(SIDECAR),
               "media_folder": str(sidecar.parent)}

    def exdev(*a, **k):
        raise OSError(errno.EXDEV, "Cross-device link")
    monkeypatch.setattr(os, "link", exdev)
    if copy == "fails":
        def enospc(src, dst, *a, **k):
            Path(dst).write_bytes(Path(src).read_bytes()[:10])
            raise OSError(errno.ENOSPC, "No space left on device")
        monkeypatch.setattr(shutil, "copy2", enospc)
        with pytest.raises(adopt_mod.AdoptError):
            adopt_mod._do_adopt(settings.db_path, finding, settings, "t")
        assert not canonical.exists(), "a half-written copy sits at the canonical path"
    else:
        out = adopt_mod._do_adopt(settings.db_path, finding, settings, "t")
        assert out["placement_kind"] == "copy" and canonical.read_bytes() == SIDECAR
        assert _lf_cols(settings.db_path, tmdb, ("canonical_present", "file_sha256")) == (1, _sha(SIDECAR))
    assert _staging(Path(settings.themes_dir)) == []


def test_an_upload_whose_write_fails_leaves_the_canonical_as_it_was_and_no_staging_file(admin_client, monkeypatch):
    client, settings, tmp_path = admin_client
    monkeypatch.setattr("app.core.revisions.capture_revision", lambda *a, **k: None)
    tmdb, rk = 4406, "94406"
    old = b"ID3" + b"the-canonical-before" * 8
    canonical = Path(settings.themes_dir) / f"movies/{canonical_theme_subdir(f'T{tmdb}', '2001', '')}/theme.mp3"
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(old)
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, tmdb)
        _plex_item(conn, rk, tmdb, str(tmp_path / "plex" / f"T{tmdb} (2001)"))
        conn.commit()
    real_write = Path.write_bytes

    def enospc(self, data):
        if self.name.endswith(".motif-tmp"):
            real_write(self, data[:10])
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_write(self, data)
    monkeypatch.setattr(Path, "write_bytes", enospc)
    with pytest.raises(OSError):
        _upload(client, rk, USER)
    assert canonical.read_bytes() == old, "a failed upload destroyed the canonical it was replacing"
    assert _staging(Path(settings.themes_dir)) == []
    assert _lf_cols(settings.db_path, tmdb, ("canonical_present",)) is None, "a failed upload stamped a row"


# ── 6: the "already this link" short-circuit is device AND inode ────

def _second_volume(monkeypatch, canonical: Path, other: Path):
    """The canonical reports OTHER's inode number on another device, and a link across raises EXDEV: a config volume beside a media volume whose small inode numbers overlap."""
    real_stat, seen = os.stat, other.stat()

    class _OnTheConfigVolume:
        st_ino, st_dev = seen.st_ino, seen.st_dev + 1

        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            return getattr(self._real, name)

    def stat(path, *a, **k):
        st = real_stat(path, *a, **k)
        return _OnTheConfigVolume(st) if not isinstance(path, int) and os.fspath(path) == os.fspath(canonical) else st

    def exdev(*a, **k):
        raise OSError(errno.EXDEV, "Cross-device link")
    monkeypatch.setattr(os, "stat", stat)
    monkeypatch.setattr(os, "link", exdev)


def test_adopt_from_plex_stages_the_copy_when_the_canonical_on_another_volume_shares_an_inode_number(admin_client, monkeypatch):
    client, settings, tmp_path = admin_client
    monkeypatch.setattr("app.web.api.log_event", lambda *a, **k: None)
    tmdb = 4407
    plex_file = _plex_folder(tmp_path, f"T{tmdb} (2001)", PLEX) / "theme.mp3"
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, tmdb)
        _lf(conn, tmdb, canonical_present=1, file_size=len(REJECTED), file_sha256=_sha(REJECTED),
            extra={"mismatch_state": "pending"})
        _placement(conn, tmdb, str(plex_file.parent), kind="copy")
        conn.commit()
    canonical = Path(settings.themes_dir) / _lf_cols(settings.db_path, tmdb, ("file_path",))[0]
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(REJECTED)
    _second_volume(monkeypatch, canonical, plex_file)
    r = client.post(f"/api/items/movie/{tmdb}/adopt-from-plex", headers=AUTH)
    assert (r.status_code, r.json()["sections_adopted"]) == (200, 1), r.text
    assert canonical.read_bytes() == PLEX, \
        "ADOPT FROM PLEX took a coinciding inode number for its link and kept the rejected upload's bytes"
    assert plex_file.read_bytes() == PLEX
    assert _lf_cols(settings.db_path, tmdb, ("file_sha256", "file_size", "mismatch_state")) == (_sha(PLEX), len(PLEX), None)
    with sqlite3.connect(settings.db_path) as conn:
        assert conn.execute("SELECT placement_kind FROM placements WHERE tmdb_id = ?", (tmdb,)).fetchone() == ("copy",)
    assert _staging(Path(settings.themes_dir)) == []


def test_a_sidecar_adopt_stages_the_copy_when_the_canonical_on_another_volume_shares_an_inode_number(admin_client, monkeypatch):
    _client, settings, tmp_path = admin_client
    import app.core.adopt as adopt_mod
    monkeypatch.setattr(adopt_mod, "log_event", lambda *a, **k: None)
    tmdb = 4408
    sidecar = _plex_folder(tmp_path, "sidecar/LotR (2001)", SIDECAR) / "theme.mp3"
    canonical = Path(settings.themes_dir) / f"movies/{canonical_theme_subdir('LotR', '2001', '')}/theme.mp3"
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(REJECTED)
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        conn.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, upstream_source, last_seen_sync_at,"
                     " first_seen_sync_at) VALUES (?, 'movie', ?, 'LotR', '2001', 'imdb', ?, ?)", (tmdb, tmdb, NOW, NOW))
        conn.commit()
    finding = {"section_id": "1", "section_type": "movie", "finding_kind": "content_mismatch", "theme_id": tmdb,
               "file_path": str(sidecar), "file_sha256": _sha(SIDECAR), "file_size": len(SIDECAR),
               "media_folder": str(sidecar.parent)}
    _second_volume(monkeypatch, canonical, sidecar)
    out = adopt_mod._do_adopt(settings.db_path, finding, settings, "t")
    assert out["placement_kind"] == "copy", out
    assert canonical.read_bytes() == SIDECAR, \
        "the adopt took a coinciding inode number for its link and stamped the sidecar's sha over the old bytes"
    assert sidecar.read_bytes() == SIDECAR
    assert _lf_cols(settings.db_path, tmdb, ("canonical_present", "file_sha256")) == (1, _sha(SIDECAR))
    assert _staging(Path(settings.themes_dir)) == []
