"""v0.51.344 PB-048 (R2-F4): a publish torn between its replace and its stamp leaves a row naming the bytes on disk."""
from __future__ import annotations

import errno
import hashlib
import os
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from app.core import plex_enum
from app.core.db import init_db
from test_v0_51_339_canonical_health_restore import _LEVELLED, _NORM_COLS
from test_v0_51_342_restore_from_plex_job import _ago

RECORDED = b"M" * 4096
SAME_SIZE = b"P" * 4096
OTHER_SIZE = b"P" * 4000
TMDB = 301
COLS = ("canonical_present", "file_size", "file_sha256") + _NORM_COLS


def _sha(data):
    return hashlib.sha256(data).hexdigest()


class Killed(BaseException):
    """motif killed between the replace and the stamp."""


class Plex:
    """Serves one body for the item; on_fetch runs while the bytes are on their way."""
    def __init__(self, body, on_fetch=None):
        self.body, self.on_fetch = body, on_fetch

    def get_themes(self, *, rating_key):
        return {"ok": True, "http_status": 200, "error": None,
                "body": {"MediaContainer": {"Metadata": [{"ratingKey": "upload://themes/new", "selected": True}]}}}

    def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
        if self.on_fetch is not None:
            self.on_fetch()
        return {"ok": True, "http_status": 200, "bytes": self.body}

    def close(self):
        return None


def _seed(tmp_path, *, leg, copy):
    """A BROKEN row levelled against RECORDED; Plex holds `copy` in the item's folder (sidecar) or its store."""
    db = tmp_path / "m.db"
    init_db(db)
    themes, folder, now = tmp_path / "themes", tmp_path / "plex" / str(TMDB), _ago(days=2)
    if leg == "sidecar":
        folder.mkdir(parents=True)
        (folder / "theme.mp3").write_bytes(copy)
    anchors = {**_LEVELLED, "loudness_measured_at": now, "norm_at": now, "loudness_measured_sha256": _sha(RECORDED)}
    lf = {"media_type": "movie", "tmdb_id": TMDB, "section_id": "1", "edition_key": "", "theme_id": TMDB,
          "file_path": f"movies/{TMDB}/theme.mp3", "file_size": len(RECORDED), "file_sha256": _sha(RECORDED),
          "downloaded_at": now, "source_video_id": "", "provenance": "manual", "source_kind": "upload",
          "canonical_present": 0, **anchors}
    with closing(sqlite3.connect(db)) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, "
                  "discovered_at, last_seen_at) VALUES ('1', 'M', 'movie', 0, 0, 'movies', 1, ?, ?)", (now, now))
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at, "
                  "first_seen_sync_at, youtube_url) VALUES (?, 'movie', ?, 'T', 'plex_orphan', ?, ?, NULL)",
                  (TMDB, TMDB, now, now))
        c.execute(f"INSERT INTO local_files ({','.join(lf)}) VALUES ({','.join('?' * len(lf))})", tuple(lf.values()))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind, provenance, "
                  "placed_at, plex_rating_key, edition_key) VALUES ('movie', ?, '1', ?, ?, 'manual', ?, ?, '')",
                  (TMDB, str(folder) if leg == "sidecar" else "", "hardlink" if leg == "sidecar" else "plex_upload",
                   now, None if leg == "sidecar" else f"9{TMDB}"))
        c.commit()
    return db, themes, themes / lf["file_path"], anchors


def _row(db):
    with closing(sqlite3.connect(db)) as c:
        return dict(zip(COLS, c.execute(f"SELECT {', '.join(COLS)} FROM local_files WHERE tmdb_id = ?",
                                        (TMDB,)).fetchone()))


def _on_the_move(monkeypatch, canonical, *, after=None, instead=None):
    """Both legs land the canonical with one os.replace: `instead` raises in its place, `after` raises once it moved."""
    real = os.replace

    def replace(src, dst, *a, **k):
        if Path(dst) != canonical:
            return real(src, dst, *a, **k)
        if instead is not None:
            instead()
        real(src, dst, *a, **k)
        if after is not None:
            after()
    monkeypatch.setattr(os, "replace", replace)


def _torn_restore(monkeypatch, db, themes, canonical, plex, kill):
    def killed(*a, **k):
        raise Killed()
    with monkeypatch.context() as m:
        if kill == "after-the-move":
            _on_the_move(m, canonical, after=killed)
        else:
            m.setattr(ch, "_stamp_restored", killed)
        with pytest.raises(Killed):
            ch.restore_from_plex(db, themes, plex)


KILLS = pytest.mark.parametrize("kill", ["after-the-move", "at-the-stamp"])


@KILLS
@pytest.mark.parametrize("copy", [SAME_SIZE, OTHER_SIZE], ids=["same-size", "other-size"])
@pytest.mark.parametrize("leg", ["store", "sidecar"])
def test_a_torn_publish_is_checked_present_naming_the_bytes_on_disk_with_no_anchor_left(tmp_path, monkeypatch,
                                                                                      leg, copy, kill):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=copy)
    _torn_restore(monkeypatch, db, themes, canonical, Plex(copy), kill)
    assert canonical.read_bytes() == copy and _row(db)["canonical_present"] == 0, \
        "premise: the bytes moved and the stamp never landed"
    plex_enum.verify_canonical_health(db, themes)
    on_disk, row = canonical.read_bytes(), _row(db)
    assert (row["canonical_present"], row["file_size"], row["file_sha256"]) == (1, len(on_disk), _sha(on_disk)), \
        "the check stamped the torn row present under the sha of bytes no longer on disk"
    assert {k: row[k] for k in _NORM_COLS} == dict.fromkeys(_NORM_COLS), \
        "loudness/norm anchors measured on the old bytes survived onto the new ones"


@KILLS
@pytest.mark.parametrize("leg", ["store", "sidecar"])
def test_a_torn_publish_of_the_recorded_bytes_keeps_the_anchors_that_measured_them(tmp_path, monkeypatch, leg, kill):
    db, themes, canonical, anchors = _seed(tmp_path, leg=leg, copy=RECORDED)
    _torn_restore(monkeypatch, db, themes, canonical, Plex(RECORDED), kill)
    assert canonical.read_bytes() == RECORDED and _row(db)["canonical_present"] == 0, \
        "premise: the bytes moved and the stamp never landed"
    plex_enum.verify_canonical_health(db, themes)
    row = _row(db)
    assert (row["canonical_present"], row["file_size"], row["file_sha256"]) == (1, len(RECORDED), _sha(RECORDED))
    assert {k: row[k] for k in _NORM_COLS} == {k: anchors[k] for k in _NORM_COLS}, \
        "identical bytes lost the loudness/norm anchors that still describe them"


@pytest.mark.parametrize("leg", ["store", "sidecar"])
def test_a_download_in_flight_refuses_the_publish_before_anything_is_recorded(tmp_path, leg):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=OTHER_SIZE)
    before = _row(db)

    def queue_download():
        with closing(sqlite3.connect(db)) as c:
            c.execute("INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, payload, status, created_at) "
                      "VALUES ('download', 'movie', ?, '1', '{}', 'pending', ?)", (TMDB, _ago(seconds=1)))
            c.commit()
    if leg == "sidecar":
        queue_download()
    # the store leg's download is queued while Plex sends the bytes, so the publish itself is the one to refuse
    res = ch.restore_from_plex(db, themes, Plex(OTHER_SIZE, on_fetch=queue_download if leg == "store" else None))
    assert [s["reason"] for s in res["skipped"]] == ["download_in_flight"]
    assert not canonical.exists()
    assert _row(db) == before, "a refused publish still recorded its bytes or voided the anchors"


FAILED = {"store": "write_failed:", "sidecar": "link_failed:"}


@pytest.mark.parametrize("leg", ["store", "sidecar"])
def test_a_move_that_fails_leaves_the_row_as_it_was_and_the_next_restore_lands_the_bytes(tmp_path, monkeypatch, leg):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=SAME_SIZE)
    before, full = _row(db), OSError(errno.ENOSPC, "No space left on device")

    def no_space():
        raise full
    with monkeypatch.context() as m:
        _on_the_move(m, canonical, instead=no_space)
        res = ch.restore_from_plex(db, themes, Plex(SAME_SIZE))
    # v0.51.344 (R1-F16): the reason words the errno; the path stays in the log
    assert [s["reason"] for s in res["skipped"]] == [f"{FAILED[leg]}{full.strerror}"]
    assert not canonical.exists()
    assert _row(db) == before, "a move that moved nothing still recorded the incoming bytes or voided the anchors"
    assert ch.restore_from_plex(db, themes, Plex(SAME_SIZE))["restored"] == 1
    plex_enum.verify_canonical_health(db, themes)
    row = _row(db)
    assert (row["canonical_present"], row["file_size"], row["file_sha256"]) == (1, len(SAME_SIZE), _sha(SAME_SIZE))
    assert {k: row[k] for k in _NORM_COLS} == dict.fromkeys(_NORM_COLS)


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root ignores a 000 folder")
@pytest.mark.parametrize("leg", ["store", "sidecar"])
def test_a_canonical_its_folder_hid_from_the_restore_is_checked_present_under_its_own_sha_and_anchors(tmp_path, leg):
    db, themes, canonical, anchors = _seed(tmp_path, leg=leg, copy=SAME_SIZE)
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(RECORDED)
    canonical.parent.chmod(0)
    try:
        res = ch.restore_from_plex(db, themes, Plex(SAME_SIZE))
    finally:
        canonical.parent.chmod(0o755)
    assert [s["reason"].startswith(FAILED[leg]) for s in res["skipped"]] == [True], \
        f"premise: the unreadable folder refused the move: {res['skipped']}"
    assert canonical.read_bytes() == RECORDED, "premise: the recorded bytes never left the canonical"
    plex_enum.verify_canonical_health(db, themes)
    row = _row(db)
    assert (row["canonical_present"], row["file_size"], row["file_sha256"]) == (1, len(RECORDED), _sha(RECORDED)), \
        "the row names Plex's copy while the recorded bytes are what is on disk"
    assert {k: row[k] for k in _NORM_COLS} == {k: anchors[k] for k in _NORM_COLS}, \
        "a restore that moved nothing voided the anchors of the bytes still on disk (UNDO's norm_orig_sha256)"


@pytest.mark.parametrize("leg", ["store", "sidecar"])
def test_a_move_that_fails_never_undoes_a_stamp_another_writer_landed_meanwhile(tmp_path, monkeypatch, leg):
    db, themes, canonical, _anchors = _seed(tmp_path, leg=leg, copy=SAME_SIZE)
    uploaded = b"U" * 3000

    def upload_lands_then_no_space():
        canonical.write_bytes(uploaded)
        with closing(sqlite3.connect(db)) as c:
            c.execute(f"UPDATE local_files SET canonical_present = 1, file_size = ?, file_sha256 = ?, "
                      f"{', '.join(f'{k} = NULL' for k in _NORM_COLS)} WHERE tmdb_id = ?",
                      (len(uploaded), _sha(uploaded), TMDB))
            c.commit()
        raise OSError(errno.ENOSPC, "No space left on device")
    with monkeypatch.context() as m:
        _on_the_move(m, canonical, instead=upload_lands_then_no_space)
        res = ch.restore_from_plex(db, themes, Plex(SAME_SIZE))
    assert [s["reason"].startswith(FAILED[leg]) for s in res["skipped"]] == [True], "premise: the move failed"
    plex_enum.verify_canonical_health(db, themes)
    row = _row(db)
    assert (row["canonical_present"], row["file_size"], row["file_sha256"]) == (1, len(uploaded), _sha(uploaded)), \
        "the failed restore wrote its old sha back over the upload that landed on disk"


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root reads a 000 file")
def test_a_plex_folder_copy_motif_cannot_read_is_never_linked_in_unrecorded(tmp_path):
    db, themes, canonical, _anchors = _seed(tmp_path, leg="sidecar", copy=SAME_SIZE)
    before, src = _row(db), tmp_path / "plex" / str(TMDB) / "theme.mp3"
    src.chmod(0)
    try:
        res = ch.restore_from_plex(db, themes, None)
    finally:
        src.chmod(0o644)
    assert (res["restored"], [s["reason"].startswith(FAILED["sidecar"]) for s in res["skipped"]]) == (0, [True]), \
        f"a copy whose sha could not be read was linked in: {res}"
    assert not canonical.exists()
    assert _row(db) == before
