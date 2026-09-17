"""v0.51.344: integration fixes, round 2 — the canonical temp sweep's shapes, clocks, lock wait and walk.

  R2-F3: _publish_store_bytes stages to theme.mp3.<16hex>.part, a name only it writes, and the sweep's shape takes
         motif's two unique writer names — never yt-dlp's own theme.mp3.part beside a queued or stalled download.
  R2-F4: a hardlink temp's ctime is its inode's, moved by every later link to the same file, so its age is read
         from its mtime; ctime still gates a copy2 temp, whose inode is its own.
  R3-F2: a writer that never releases a canonical's lock no longer holds the interpreter at exit — the sweep's wait
         on a pool thread is bounded, and the temp is left for the next run.
  R3-F3: the canonical half walks each section's root (and collections/<root>) one level down, so a temp is swept
         by its name shape when no local_files row names its folder any more; the walk is counted.
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from app.core.db import init_db
from app.core.plex_enum import sweep_stale_placement_temps

REPO = Path(__file__).resolve().parent.parent
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
REL = "movies/M1/theme.mp3"
PART = ".0123456789abcdef.part"
STAGED = re.compile(r"theme\.mp3\.([0-9a-f]{16})\.part")


def _db(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    init_db(path)
    return path


def _lf(db, tmdb, rel, *, section="1"):
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id, file_path, file_size, "
                  " file_sha256, downloaded_at, source_video_id, provenance, source_kind, canonical_present, "
                  " edition_key) VALUES ('movie', ?, ?, NULL, ?, 10, ?, ?, '', 'manual', 'upload', 0, '')",
                  (tmdb, section, rel, "0" * 64, NOW))


def _plex_item(db, *, rk, section, folder):
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, guid_tmdb, title, edition_key, "
                  " has_theme, folder_path, first_seen_at, last_seen_at) VALUES (?, ?, 'movie', ?, ?, '', 0, ?, ?, ?)",
                  (rk, section, int(rk), f"m{rk}", folder, NOW, NOW))


def _section(db, section_id, subdir):
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, themes_subdir, included, discovered_at, "
                  " last_seen_at) VALUES (?, ?, 'movie', ?, 1, ?, ?)", (section_id, f"S{section_id}", subdir, NOW, NOW))


def _forward(monkeypatch, secs):
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + secs)


def _stranded(path: Path, data: bytes = b"stranded") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


# ── R2-F3: the store publish's staging name is its own; a download's theme.mp3.part is never the sweep's ──

def test_the_store_publish_stages_to_a_name_only_it_writes_and_the_sweep_takes_that_shape(tmp_path, monkeypatch):
    db = _db(tmp_path / "m.db")
    themes = tmp_path / "themes"
    staged: list[str] = []
    real_replace = os.replace

    def spy(src, dst):
        staged.append(Path(src).name)
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", spy)
    rows = []
    for tmdb in (1, 2):
        rel = f"movies/M{tmdb}/theme.mp3"
        _lf(db, tmdb, rel)
        rows.append(dict(media_type="movie", tmdb_id=tmdb, section_id="1", edition_key="", file_path=rel,
                         file_size=10, file_sha256="0" * 64))
    for r in rows:
        assert ch._publish_store_bytes(db, themes, r, b"store-bytes", "upload://themes/x")["ok"]
    tokens = [m.group(1) for m in map(STAGED.fullmatch, staged) if m]
    assert len(tokens) == 2 and len(set(tokens)) == 2, f"two publishes, two names of the writer's own shape: {staged}"
    assert "theme.mp3.part" not in staged, "the fixed name is yt-dlp's in-flight file for an mp3-ext source"
    assert all((themes / r["file_path"]).read_bytes() == b"store-bytes" for r in rows)
    assert all(sorted(p.name for p in (themes / r["file_path"]).parent.iterdir()) == ["theme.mp3"] for r in rows)
    left = _stranded(themes / f"movies/M1/theme.mp3.{tokens[0]}.part")  # the name a kill mid-publish strands
    _forward(monkeypatch, 7200)
    assert sweep_stale_placement_temps(db, themes_dir=themes) == 1 and not left.exists()


def test_a_downloads_own_in_flight_files_beside_the_canonical_are_never_the_sweeps(tmp_path, monkeypatch):
    db = _db(tmp_path / "m.db")
    themes = tmp_path / "themes"
    _lf(db, 1, REL)
    # yt-dlp's names for an mp3-ext source: <canonical>.part and its fragments, then its rename onto theme.mp3
    ytdlp = [_stranded(themes / (REL + ".part"), b"in flight"), _stranded(themes / (REL + ".part-Frag1"), b"frag")]
    ours = _stranded(themes / (REL + PART))
    _forward(monkeypatch, 7200)
    assert sweep_stale_placement_temps(db, themes_dir=themes) == 1
    assert all(p.exists() for p in ytdlp) and not ours.exists(), "an hour-old download staging file is a resume's"


# ── R2-F4: a hardlink temp is aged by its mtime — its inode's ctime moves with every later link ──

@pytest.mark.parametrize("root", ["media", "canonical"])
def test_a_hardlink_temp_whose_inode_a_later_link_touched_is_still_swept(tmp_path, monkeypatch, root):
    db = _db(tmp_path / "m.db")
    themes = tmp_path / "themes"
    src = tmp_path / "old" / "theme.mp3"  # the canonical (media root) or the Plex folder's copy (canonical root)
    src.parent.mkdir()
    src.write_bytes(b"s" * 64)
    old = time.time() - 7200
    os.utime(src, (old, old))
    if root == "media":
        folder = tmp_path / "media" / "M1"
        folder.mkdir(parents=True)
        _plex_item(db, rk="1", section="1", folder=str(folder))
        orphan = folder / "theme.mp3.motif-tmp"
    else:
        (themes / REL).parent.mkdir(parents=True)
        _lf(db, 1, REL)
        orphan = themes / "movies/M1/theme.mp3.0123456789abcdef.motif-tmp"
    os.link(src, orphan)  # a placement / a sidecar restore stages a link, then dies before its os.replace
    os.link(src, tmp_path / "old" / "elsewhere.mp3")  # a later placement of the same file: the shared inode's ctime moves
    st = orphan.stat()
    assert st.st_nlink == 3 and st.st_mtime < old + 1 and st.st_ctime > time.time() - 60, \
        "premise: the temp's mtime is two hours old and its ctime was just moved by a link elsewhere"
    monkeypatch.setattr(time, "time", lambda: st.st_ctime + 60)  # a sweep a minute after the touch, inside the 1 h gate
    assert sweep_stale_placement_temps(db, themes_dir=themes) == 1
    assert not orphan.exists() and src.exists()


# ── R3-F2: a wedged writer no longer holds the interpreter at exit ──

_CHILD_EXIT = """
import sys, threading, time
sys.path.insert(0, sys.argv[1])
from pathlib import Path
import app.core.plex_enum as pe
import app.core.canonical_health as ch
assert pe.__file__.startswith(sys.argv[1]), pe.__file__
db, themes, rel = Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4]
pe._CANON_SWEEP_LOCK_WAIT_S = 0.25
real_time = time.time
time.time = lambda: real_time() + 7200
held = ch._canonical_write_lock(themes / rel)  # a restore wedged mid-write on a daemon thread — never released
held.acquire()
asked = threading.Event()
real_lock = ch._canonical_write_lock
def spy(p):
    lock = real_lock(p)
    asked.set()
    return lock
ch._canonical_write_lock = spy
threading.Thread(target=lambda: pe.sweep_stale_placement_temps(db, themes_dir=themes), daemon=True).start()
assert asked.wait(30), "the sweep's pool thread never reached the canonical's lock"
# main() returns here with the pool thread on the lock; the interpreter joins that thread before it can exit
"""


def test_a_writer_that_never_releases_the_lock_does_not_hold_the_interpreter_at_exit(tmp_path):
    db = _db(tmp_path / "m.db")
    themes = tmp_path / "themes"
    _lf(db, 1, REL)
    part = _stranded(themes / (REL + PART))
    r = subprocess.run([sys.executable, "-c", _CHILD_EXIT, str(REPO), str(db), str(themes), REL],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    assert part.exists(), "the sweep unlinked a temp while its writer held the canonical's lock"


# ── R3-F3: a folder no local_files row names any more is still walked from its section's root ──

def test_temps_in_folders_no_row_names_are_swept_from_the_section_roots_and_the_walk_is_counted(tmp_path, monkeypatch,
                                                                                                caplog):
    caplog.set_level(logging.INFO, logger="app.core.plex_enum")
    db = _db(tmp_path / "m.db")
    themes = tmp_path / "themes"
    _section(db, "1", "movies")
    _lf(db, 1, REL)
    named = _stranded(themes / (REL + PART))
    gone = "movies/Gone (2001)"  # downloaded after the backup the operator restored: its row is gone, its folder is not
    rowless = [_stranded(themes / gone / "theme.mp3.0123456789abcdef.motif-tmp"),
               _stranded(themes / gone / ("theme.mp3" + PART)),
               _stranded(themes / "collections/movies/Set (2010)" / ("theme.mp3" + PART))]
    kept = [_stranded(themes / gone / "theme.mp3", b"the canonical"),  # finished bytes are never the sweep's
            _stranded(themes / gone / "theme.mp3.part", b"yt-dlp's"),
            _stranded(themes / "movies/Deep/Nested" / ("theme.mp3" + PART)),  # the walk is one level: bounded
            _stranded(themes / "movies" / ("theme.mp3" + PART))]  # a section root is not a canonical's folder
    _forward(monkeypatch, 7200)
    assert sweep_stale_placement_temps(db, themes_dir=themes) == 4
    assert not any(p.exists() for p in [named, *rowless]) and all(p.exists() for p in kept)
    walked = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO and "walked" in r.getMessage()]
    assert len(walked) == 1 and "walked 4 canonical folder(s) under 1 section root(s)" in walked[0] and \
        "3 named by no local_files row" in walked[0], walked


def test_section_ids_scope_the_root_walk_as_they_scope_the_rows(tmp_path, monkeypatch):
    db = _db(tmp_path / "m.db")
    themes = tmp_path / "themes"
    temps = {}
    for section, subdir in (("1", "movies"), ("2", "tv")):
        _section(db, section, subdir)
        temps[section] = _stranded(themes / subdir / "Rowless (2000)" / ("theme.mp3" + PART))
    _forward(monkeypatch, 7200)
    assert sweep_stale_placement_temps(db, section_ids=["1"], themes_dir=themes) == 1
    assert not temps["1"].exists() and temps["2"].exists()


def test_a_section_root_nothing_was_downloaded_for_is_skipped_without_a_warning(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="app.core.plex_enum")
    db = _db(tmp_path / "m.db")
    themes = tmp_path / "themes"
    themes.mkdir()
    _section(db, "1", "movies")
    _forward(monkeypatch, 7200)
    assert sweep_stale_placement_temps(db, themes_dir=themes) == 0
    assert not [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
