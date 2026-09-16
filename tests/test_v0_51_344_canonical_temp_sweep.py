"""v0.51.344: the daily temp sweep also clears the staging temps a killed canonical restore strands in themes_dir.

  1. A real SIGKILL mid-publish strands theme.mp3.part and theme.mp3.<hex>.motif-tmp; the sweep removes those two
     shapes and nothing else beside the canonical.
  2. The age gate reads ctime: link() and copy2() keep the source's mtime, so a temp staged a moment ago stays.
  3. The daily job passes the configured themes_dir; section_ids scope the canonical walk; a writer's lock is waited on.
"""
from __future__ import annotations

import logging
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from app.core import canonical_health as ch
from app.core import scheduler
from app.core.db import init_db
from app.core.plex_enum import sweep_stale_placement_temps

REPO = Path(__file__).resolve().parent.parent
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
REL = "movies/M1/theme.mp3"


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


def _forward(monkeypatch, secs):
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + secs)


_CHILD_LINK = """
import os, signal, sys
sys.path.insert(0, sys.argv[1])
from pathlib import Path
import app.core.placement as p
assert p.__file__.startswith(sys.argv[1]), p.__file__
os.replace = lambda a, b: os.kill(os.getpid(), signal.SIGKILL)
p._safe_link_or_copy(Path(sys.argv[2]), Path(sys.argv[3]), unique_tmp=True)
"""

_CHILD_PUBLISH = """
import os, signal, sys
sys.path.insert(0, sys.argv[1])
from pathlib import Path
import app.core.canonical_health as ch
assert ch.__file__.startswith(sys.argv[1]), ch.__file__
os.replace = lambda a, b: os.kill(os.getpid(), signal.SIGKILL)
r = dict(media_type="movie", tmdb_id=1, section_id="1", edition_key="", file_path=sys.argv[4], file_size=10,
         file_sha256="0" * 64)
ch._publish_store_bytes(Path(sys.argv[2]), Path(sys.argv[3]), r, b"store-bytes", "upload://themes/x")
"""


def test_a_killed_restores_two_temps_are_swept_and_nothing_else_beside_the_canonical(tmp_path, monkeypatch):
    db = _db(tmp_path / "m.db")
    themes = tmp_path / "themes"
    canonical = themes / REL
    canonical.parent.mkdir(parents=True)
    _lf(db, 1, REL)
    src = tmp_path / "media" / "theme.mp3"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"s" * 4096)
    for child, args in ((_CHILD_LINK, [str(src), str(canonical)]), (_CHILD_PUBLISH, [str(db), str(themes), REL])):
        r = subprocess.run([sys.executable, "-c", child, str(REPO), *args], capture_output=True, text=True, timeout=120)
        assert r.returncode == -signal.SIGKILL, r.stderr[-2000:]
    stranded = sorted(p.name for p in canonical.parent.iterdir())
    assert len(stranded) == 2 and "theme.mp3.part" in stranded, stranded
    canonical.write_bytes(b"the canonical")
    # a placement's fixed name, a sibling link's temp, an operator's copy that only starts with a writer's shape
    foreign = ["theme.mp3.motif-tmp", "theme.mp3.sib.tmp", "theme.mp3.part.keep"]
    for name in foreign:
        (canonical.parent / name).write_bytes(b"not a restore's")
    _forward(monkeypatch, 7200)
    assert sweep_stale_placement_temps(db, themes_dir=themes) == 2
    assert sorted(p.name for p in canonical.parent.iterdir()) == sorted(["theme.mp3", *foreign])


def test_a_temp_staged_a_moment_ago_from_an_old_file_is_kept_in_both_roots(tmp_path):
    db = _db(tmp_path / "m.db")
    src = tmp_path / "old.mp3"
    src.write_bytes(b"s" * 64)
    old = time.time() - 7200
    os.utime(src, (old, old))
    folder = tmp_path / "media" / "M1"
    folder.mkdir(parents=True)
    _plex_item(db, rk="1", section="1", folder=str(folder))
    themes = tmp_path / "themes"
    (themes / REL).parent.mkdir(parents=True)
    _lf(db, 1, REL)
    linked = [folder / "theme.mp3.motif-tmp", themes / "movies/M1/theme.mp3.0123456789abcdef.motif-tmp"]
    for p in linked:
        os.link(src, p)
    copied = themes / "movies/M1/theme.mp3.part"
    shutil.copy2(src, copied)
    staged = [*linked, copied]
    assert all(time.time() - p.stat().st_mtime > 3600 for p in staged), "premise: mtime says two hours old"
    assert sweep_stale_placement_temps(db, themes_dir=themes) == 0
    assert all(p.exists() for p in staged), "a restore or placement may still be about to os.replace these"


def test_the_daily_job_sweeps_the_configured_themes_dir_and_the_media_folders_without_it(tmp_path, monkeypatch):
    from app.config import Settings
    events: list[str] = []
    monkeypatch.setattr(scheduler, "log_event", lambda *a, **k: events.append(k.get("message")))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    _db(s.db_path)
    themes = tmp_path / "themes"
    part = themes / "movies/M1/theme.mp3.part"
    part.parent.mkdir(parents=True)
    part.write_bytes(b"stranded")
    _lf(s.db_path, 1, REL)
    folder = tmp_path / "media" / "M2"
    folder.mkdir(parents=True)
    media_tmp = folder / "theme.mp3.motif-tmp"
    media_tmp.write_bytes(b"stranded")
    _plex_item(s.db_path, rk="2", section="1", folder=str(folder))
    _forward(monkeypatch, 7200)
    assert s.themes_dir is None
    scheduler._sweep_placement_temps_job(s)
    assert not media_tmp.exists() and part.exists(), "no themes_dir: the media folders are still swept"
    s._cfg.paths.themes_dir = str(themes)
    scheduler._sweep_placement_temps_job(s)
    assert not part.exists()
    assert len(events) == 2


def test_the_job_start_scheduler_registers_sweeps_the_themes_dir_with_the_args_it_was_given(tmp_path, monkeypatch):
    from app.config import Settings
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(scheduler, "_check_release_update", lambda *a, **k: None)
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    _db(s.db_path)
    themes = tmp_path / "themes"
    part = themes / (REL + ".part")
    part.parent.mkdir(parents=True)
    part.write_bytes(b"stranded")
    _lf(s.db_path, 1, REL)
    s._cfg.paths.themes_dir = str(themes)
    sched = scheduler.start_scheduler(s)
    try:
        job = sched.get_job("placement_temp_sweep")
    finally:
        sched.shutdown(wait=False)
    events: list[str] = []
    monkeypatch.setattr(scheduler, "log_event", lambda *a, **k: events.append(k.get("message")))
    _forward(monkeypatch, 7200)
    job.func(*job.args, **job.kwargs)
    assert not part.exists(), "the daily 03:20 job must reach the canonical sweep through its registered args"
    assert len(events) == 1


def test_a_dead_themes_root_is_named_once_and_the_media_folders_are_still_swept(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    db = _db(tmp_path / "m.db")
    _lf(db, 1, REL)
    folder = tmp_path / "media" / "M2"
    folder.mkdir(parents=True)
    media_tmp = folder / "theme.mp3.motif-tmp"
    media_tmp.write_bytes(b"stranded")
    _plex_item(db, rk="2", section="1", folder=str(folder))
    _forward(monkeypatch, 7200)
    dead = tmp_path / "unmounted"
    assert sweep_stale_placement_temps(db, themes_dir=dead) == 1
    assert not media_tmp.exists()
    named = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and str(dead) in r.getMessage()]
    assert len(named) == 1, named


def test_section_ids_scope_the_canonical_walk(tmp_path, monkeypatch):
    db = _db(tmp_path / "m.db")
    themes = tmp_path / "themes"
    parts = {}
    for tmdb, section in ((1, "1"), (2, "2")):
        rel = f"movies/S{section}/theme.mp3"
        _lf(db, tmdb, rel, section=section)
        parts[section] = themes / (rel + ".part")
        parts[section].parent.mkdir(parents=True)
        parts[section].write_bytes(b"stranded")
    _forward(monkeypatch, 7200)
    assert sweep_stale_placement_temps(db, section_ids=["1"], themes_dir=themes) == 1
    assert not parts["1"].exists() and parts["2"].exists()


def test_the_sweep_waits_for_the_writer_holding_the_canonicals_lock(tmp_path, monkeypatch):
    db = _db(tmp_path / "m.db")
    themes = tmp_path / "themes"
    _lf(db, 1, REL)
    part = themes / (REL + ".part")
    part.parent.mkdir(parents=True)
    part.write_bytes(b"stranded")
    _forward(monkeypatch, 7200)
    out: list[int] = []
    t = threading.Thread(target=lambda: out.append(sweep_stale_placement_temps(db, themes_dir=themes)))
    lock = ch._canonical_write_lock(themes / REL)
    try:
        with lock:
            t.start()
            t.join(timeout=1.0)
            held = (t.is_alive(), part.exists())
    finally:
        t.join(timeout=30)
    assert held == (True, True), "the sweep unlinked a temp while its writer held the canonical's lock"
    assert out == [1] and not part.exists()
