"""v0.51.342 integration review (R1-F1, F5, F6, F9): verify's compare-and-set stamp, the re-hash candidate, the boot counts, the cancel note."""
from __future__ import annotations

import errno
import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from app.core import db_backup, plex_enum, reconcile, scheduler
from app.core.db import get_conn, init_db
from test_v0_51_339_canonical_health_restore import (  # noqa: F401 — admin_client is a fixture
    _DRIVER, _NODE, APP_JS, _app_fn, _report, _row, admin_client,
)
from test_v0_51_342_canonical_health_changed import _boot, _main_lines

NOW = "2026-09-13T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}
HEALTHY = (1, 2, 3)
RESTORABLE = (11, 12, 13, 14, 15)
UNRESTORABLE = 21
_DAILY_LINE = "Daily health: {} canonical theme.mp3 file(s) missing from motif storage (table-wide)"


# ── seed helpers ──────────────────────────────────────────────────────

def _rel(t):
    return f"movies/{t}/theme.mp3"


def _canon(themes, t):
    return themes / _rel(t)


def _bytes(t):
    return b"h" * (100 + t)


def _seed(db: Path, themes: Path, plexdir: Path):
    init_db(db)
    themes.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db)) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, "
                  "discovered_at, last_seen_at) VALUES ('1', 'M', 'movie', 0, 0, 'movies', 1, ?, ?)", (NOW, NOW))
        for t in HEALTHY + RESTORABLE + (UNRESTORABLE,):
            data = _bytes(t)
            c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at, "
                      "first_seen_sync_at) VALUES (?, 'movie', ?, ?, 'plex_orphan', ?, ?)", (t, t, f"T{t}", NOW, NOW))
            c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, theme_id, file_path, "
                      "file_size, file_sha256, downloaded_at, source_video_id, provenance, source_kind, "
                      "canonical_present) VALUES ('movie', ?, '1', '', ?, ?, ?, ?, ?, '', 'manual', 'upload', ?)",
                      (t, t, _rel(t), len(data), hashlib.sha256(data).hexdigest(), NOW, 1 if t in HEALTHY else 0))
            if t in HEALTHY:
                _canon(themes, t).parent.mkdir(parents=True, exist_ok=True)
                _canon(themes, t).write_bytes(data)
            if t in RESTORABLE:
                _placement(c, plexdir, t, data)
        c.commit()


def _placement(c, plexdir, t, data):
    folder = plexdir / str(t)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "theme.mp3").write_bytes(data)
    c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind, provenance, "
              "placed_at, edition_key) VALUES ('movie', ?, '1', ?, 'hardlink', 'manual', ?, '')", (t, str(folder), NOW))
    return folder


@pytest.fixture
def world(tmp_path, monkeypatch):
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    themes, plexdir = tmp_path / "themes", tmp_path / "plex"
    s._cfg.paths.themes_dir = str(themes)
    _seed(s.db_path, themes, plexdir)
    events: list[str] = []

    def record(_db, **kw):
        events.append(kw.get("message", ""))
    for mod in (scheduler, reconcile, plex_enum):
        monkeypatch.setattr(mod, "log_event", record)
    return s, s.db_path, themes, plexdir, events


def _cols(db, t, cols):
    with closing(sqlite3.connect(db)) as c:
        return c.execute(f"SELECT {', '.join(cols)} FROM local_files WHERE tmdb_id = ?", (t,)).fetchone()


def _present(db):
    with closing(sqlite3.connect(db)) as c:
        return dict(c.execute("SELECT tmdb_id, canonical_present FROM local_files"))


def _report_of(db, themes):
    with get_conn(db) as conn:
        return ch.broken_canonical_report(conn, themes)


def _between_stat_and_stamp(monkeypatch, writer):
    """Runs writer when verify opens its stamp connection — the read was its first, every stat is done by the second."""
    real = plex_enum.get_conn
    seen: list = []

    def hooked(db_path):
        if sys._getframe(1).f_code is plex_enum.verify_canonical_health.__code__:
            seen.append(db_path)
            if len(seen) == 2:
                writer()
        return real(db_path)
    monkeypatch.setattr(plex_enum, "get_conn", hooked)
    return seen


def _restore_job(db, themes, out):
    def run():
        assert not any(_canon(themes, t).exists() for t in RESTORABLE), "the canonicals were there when verify statted"
        out.append(ch.restore_from_plex(db, themes, None))
    return run


def _assert_restored_rows_stand(db, themes):
    assert all(_canon(themes, t).is_file() for t in RESTORABLE)
    flags = _present(db)
    assert [flags[t] for t in RESTORABLE] == [1] * 5, "verify re-stamped the rows the restore just brought back"
    assert flags[UNRESTORABLE] == 0 and [flags[t] for t in HEALTHY] == [1, 1, 1]
    assert _report_of(db, themes)["counts"]["broken"] == 1


# ── F1: a restore between verify's stat and its stamp keeps its answer ──

def test_verify_keeps_the_rows_a_restore_stamped_after_its_stat_and_does_not_count_them(world, monkeypatch):
    _s, db, themes, _plexdir, _events = world
    out: list = []
    seen = _between_stat_and_stamp(monkeypatch, _restore_job(db, themes, out))
    res = plex_enum.verify_canonical_health(db, themes)
    assert len(seen) == 2 and out[0]["restored_sidecar"] == 5
    _assert_restored_rows_stand(db, themes)
    assert res == {"checked": 4, "missing": 1, "skipped": 0}, "a stamp the restore won was counted"


def test_the_daily_pass_keeps_them_and_its_event_counts_only_the_rows_it_stamped(world, monkeypatch):
    s, db, themes, _plexdir, events = world
    out: list = []
    _between_stat_and_stamp(monkeypatch, _restore_job(db, themes, out))
    scheduler._daily_health_passes_job(s)
    assert out and out[0]["restored_sidecar"] == 5, "the restore never ran inside the daily pass's verify"
    _assert_restored_rows_stand(db, themes)
    assert [m for m in events if "canonical" in m] == [_DAILY_LINE.format(1)]


def test_a_reconciliation_run_keeps_them_and_reports_only_the_rows_it_stamped(world, monkeypatch):
    _s, db, themes, _plexdir, _events = world
    out: list = []
    _between_stat_and_stamp(monkeypatch, _restore_job(db, themes, out))
    summary = reconcile.run_reconciliation(db, themes, dry_run=True)
    assert out[0]["restored_sidecar"] == 5
    _assert_restored_rows_stand(db, themes)
    assert summary["canonical"] == {"checked": 4, "missing": 1, "skipped": 0}
    assert summary["broken_canonicals"] == 1


@pytest.mark.parametrize("caller", ["verify", "daily", "reconcile"])
def test_with_no_writer_in_between_every_caller_stamps_and_counts_every_row(world, monkeypatch, caller):
    s, db, themes, _plexdir, events = world
    seen = _between_stat_and_stamp(monkeypatch, lambda: None)
    if caller == "verify":
        assert plex_enum.verify_canonical_health(db, themes) == {"checked": 9, "missing": 6, "skipped": 0}
    elif caller == "daily":
        scheduler._daily_health_passes_job(s)
        assert [m for m in events if "canonical" in m] == [_DAILY_LINE.format(6)]
    else:
        assert reconcile.run_reconciliation(db, themes, dry_run=True)["canonical"] == {
            "checked": 9, "missing": 6, "skipped": 0}
    assert len(seen) == 2
    assert _report_of(db, themes)["counts"]["broken"] == 6
    assert ch.restore_from_plex(db, themes, None)["restored_sidecar"] == 5
    assert plex_enum.verify_canonical_health(db, themes) == {"checked": 9, "missing": 1, "skipped": 0}, \
        "the compare-and-set refused a stamp no writer raced"
    _assert_restored_rows_stand(db, themes)


def test_a_present_stamp_that_leaves_the_download_time_alone_still_wins(world, monkeypatch):
    _s, db, themes, _plexdir, _events = world
    out: list = []

    def files_back_then_restore():
        for t in RESTORABLE:  # put back by hand — the job finds each one there and stamps present only
            _canon(themes, t).parent.mkdir(parents=True, exist_ok=True)
            _canon(themes, t).write_bytes(b"by-hand")
        out.append(ch.restore_from_plex(db, themes, None))
    _between_stat_and_stamp(monkeypatch, files_back_then_restore)
    res = plex_enum.verify_canonical_health(db, themes)
    assert {s["tmdb_id"]: s["reason"] for s in out[0]["skipped"] if s["tmdb_id"] in RESTORABLE} == dict.fromkeys(
        RESTORABLE, "canonical_already_present")
    assert [_cols(db, t, ("canonical_present", "downloaded_at")) for t in RESTORABLE] == [(1, NOW)] * 5, \
        "only canonical_present moved"
    assert res == {"checked": 4, "missing": 1, "skipped": 0}
    assert sorted(r["tmdb_id"] for r in _report_of(db, themes)["changed"]) == list(RESTORABLE)


def test_a_download_time_stamp_on_a_row_already_recorded_present_still_wins(world, monkeypatch):
    _s, db, themes, plexdir, _events = world
    gone = HEALTHY[0]
    with closing(sqlite3.connect(db)) as c:
        folder = _placement(c, plexdir, gone, _bytes(gone))
        c.commit()
    _canon(themes, gone).unlink()  # recorded present (1), deleted before the check
    row = {"media_type": "movie", "tmdb_id": gone, "section_id": "1", "edition_key": "", "file_path": _rel(gone),
           "file_size": len(_bytes(gone)), "file_sha256": hashlib.sha256(_bytes(gone)).hexdigest(),
           "media_folder": str(folder), "placement_kind": "hardlink"}
    out: list = []
    _between_stat_and_stamp(monkeypatch, lambda: out.append(ch.restore_from_placement(db, themes, row)))
    res = plex_enum.verify_canonical_health(db, themes)
    assert out[0]["ok"] is True
    present, downloaded = _cols(db, gone, ("canonical_present", "downloaded_at"))
    assert (present, _canon(themes, gone).is_file()) == (1, True), "the INFO card's restore was stamped broken"
    assert downloaded != NOW, "only downloaded_at moved"
    assert res == {"checked": 8, "missing": 6, "skipped": 0}


# ── F5: a failed re-hash makes the row a CHANGED candidate ───────────

def test_a_restore_whose_rehash_fails_lists_the_row_as_changed(world, monkeypatch):
    _s, db, themes, plexdir, _events = world
    for t, n in ((11, 40), (12, 50)):
        (plexdir / str(t) / "theme.mp3").write_bytes(b"s" * n)
    real_open = Path.open
    target = _canon(themes, 11)

    def open_eio(self, *a, **kw):
        if self == target:
            raise OSError(errno.EIO, os.strerror(errno.EIO), str(self))
        return real_open(self, *a, **kw)
    monkeypatch.setattr(Path, "open", open_eio)
    res = ch.restore_from_plex(db, themes, None)
    monkeypatch.setattr(Path, "open", real_open)
    assert res["restored_sidecar"] == 5 and target.stat().st_size == 40
    changed = [(r["tmdb_id"], r["recorded"], r["on_disk"]) for r in _report_of(db, themes)["changed"]]
    assert changed == [(11, 111, 40)], "the kept size is not these bytes' — CHANGED must re-read it"
    assert _cols(db, 11, ("canonical_present", "canonical_changed_candidate")) == (1, 1)
    # v0.51.342: reversed — a stamp whose size moved (112 → 50) is a candidate too; CHANGED re-reads it and lists nothing.
    assert _cols(db, 12, ("file_size", "canonical_changed_candidate")) == (50, 1)


# ── F6: the boot lines agree on what a restore carried ──────────────

def _v79_shape(db):
    with closing(sqlite3.connect(db)) as c:
        c.execute("ALTER TABLE local_files DROP COLUMN canonical_changed_candidate")
        c.execute("ALTER TABLE local_files DROP COLUMN canonical_hash_miss_sig")
        c.execute("DELETE FROM schema_version")
        c.execute("INSERT INTO schema_version (version, applied_at) VALUES (79, ?)", (NOW,))
        c.commit()


def _db_lines(caplog, needle):
    return [r.getMessage() for r in caplog.records if r.name == "app.core.db" and needle in r.getMessage()]


@pytest.mark.parametrize("schema", [79, 80])
def test_a_restore_at_boot_logs_counts_that_agree(tmp_path, monkeypatch, caplog, schema):
    caplog.set_level(logging.INFO)
    cd = tmp_path / "cfg"
    cd.mkdir()
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(cd / "cookies.txt"))
    src = tmp_path / "src.db"
    _seed(src, tmp_path / "themes", tmp_path / "plex")
    with closing(sqlite3.connect(src)) as c:
        c.execute("UPDATE local_files SET file_path = '' WHERE tmdb_id = ?", (UNRESTORABLE,))  # tracked by nobody
        if schema == 79:  # 3 stamped, the other 6 never checked
            c.execute("UPDATE local_files SET canonical_health_checked_at = ? WHERE tmdb_id IN (1, 2, 11)", (NOW,))
        c.commit()
    if schema == 79:
        _v79_shape(src)
    snap = tmp_path / "snap.db"
    db_backup.vacuum_into(src, snap)
    live = cd / "motif.db"
    init_db(live)
    db_backup.stage_restore(live, snap)
    _boot(monkeypatch, cd)
    with closing(sqlite3.connect(live)) as c:
        assert c.execute("SELECT COUNT(*), COUNT(canonical_health_checked_at) FROM local_files").fetchone() == (9, 0)
    assert not _main_lines(caplog, logging.WARNING, "Canonical health"), "nothing was set aside at the hook"
    if schema == 79:
        assert [m.split(" that ")[0] for m in _db_lines(caplog, "check stamp(s)")] == ["v80: cleared 3 check stamp(s)"]
        assert len(_main_lines(caplog, logging.INFO, "the v80 migration already set aside the check results the "
                                                      "restored database (schema v79) carried")) == 1
    else:
        assert _db_lines(caplog, "check stamp(s)") == []
        assert len(_main_lines(caplog, logging.INFO, "the restored database carried no check results to set aside")) == 1


# ── the page names every pass that re-reads the files ────────────────

def test_the_page_names_every_pass_that_re_reads_the_files(admin_client):
    client, _settings, _tmp = admin_client
    html = client.get("/admin/canonical-health", headers=AUTH).text
    intro = re.search(r'<p class="form-hint block-intro">(.*?)</p>', html, re.S)
    sentence = " ".join(re.sub(r"<[^>]+>", "", intro.group(1)).split())
    sentence = sentence[sentence.index("Opening this page"):]
    assert sentence == ("Opening this page shows what the last check found — only // RUN CHECK, the daily pass at "
                        "03:25 UTC, a Plex refresh of a section, or a reconciliation run (POST /api/admin/reconcile) "
                        "re-reads the files."), sentence


# ── F9: the cancel note, under node ──────────────────────────────────

_CANCEL_BTN = "canon-restore-plex-cancel-btn"
_RUNNING = {"status": "running", "stage": "restoring", "done": 3, "total": 10, "restored_sidecar": 1,
            "restored_store": 1, "skipped_count": 1, "cancelling": False, "elapsed_s": 4.0}
_SSR_RUNNING = {"canon-restore-plex-btn": {"disabled": True, "display": "", "text": "// RESTORING…"},
                _CANCEL_BTN: {"display": ""},
                "canon-restore-plex-status": {"text": "restoring…", "dataset": {"running": "1"}}}
_PROGRESS = "restoring 3 / 10 · 2 restored · 1 skipped"


def _finished():
    return {"status": "done", "broken": 10, "restored": 9, "restored_sidecar": 5, "restored_store": 4,
            "skipped": [], "skipped_count": 0, "not_attempted": 0, "cancelled": False, "plex_unreachable": False,
            "finished_at": (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(timespec="seconds")}


def _run(tmp_path, responses, clicks, ssr=None):
    """_run_page with the thrown error's FastAPI detail and the page's console (stderr) kept."""
    driver = _DRIVER.replace("    err.status = next.__throw.status;\n",
                             "    err.status = next.__throw.status;\n    err.detail = next.__throw.detail || null;\n")
    assert driver != _DRIVER
    start = APP_JS.index("  function bindCanonicalHealth() {")
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "bind.js").write_text(_app_fn("fmtRelativePast") + _app_fn("proxyStatusHint") + _app_fn("gatewayTimeoutNote")
                                      + APP_JS[start:APP_JS.index("\n  function ", start + 1)])
    (tmp_path / "scenario.json").write_text(json.dumps({"responses": responses, "clicks": clicks, "ssr": ssr or {}}))
    (tmp_path / "driver.js").write_text(driver)
    r = subprocess.run([_NODE, str(tmp_path / "driver.js"), str(tmp_path / "bind.js"), str(tmp_path / "scenario.json")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    out = json.loads(r.stdout)
    assert out["unexpected"] == [] and out["left"] == 0, out
    return out["snaps"], r.stderr


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_cancel_the_server_never_received_says_so_until_a_poll_reports_cancelling(tmp_path):
    page = _report(missing=[_row(1701, "Cancel Title", "store")], restorable=1)
    snaps, err = _run(tmp_path, [page, _RUNNING, {"__throw": {"status": 502}}, _RUNNING,
                                 {"ok": True, "cancelling": True}, dict(_RUNNING, cancelling=True)],
                      [_CANCEL_BTN, "tick", _CANCEL_BTN, "tick"])
    _s0, s1, s2, s3, s4 = snaps
    note = ("✗ cancel not sent — 502: the reverse proxy timed out, but motif may still be finishing — verify before "
            "retrying. — press // CANCEL again")
    for s in (s1, s2):
        status = s["canon-restore-plex-status"]
        assert (status["text"], status["className"]) == (note, "form-status form-status-fail"), status
        assert s[_CANCEL_BTN]["disabled"] is False, "// CANCEL must stay pressable after a cancel that was not sent"
    assert s1["__timers"] == 1, "only the poll is armed — nothing re-enables a button that was never disabled"
    assert "canonical health restore cancel failed" in err, err
    assert s3[_CANCEL_BTN]["disabled"] is True
    status = s4["canon-restore-plex-status"]
    assert (status["text"], status["className"]) == (_PROGRESS + " — cancelling…", "form-status")


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_run_that_ends_under_the_note_clears_it_and_the_next_run_shows_its_progress(tmp_path):
    page = _report(missing=[_row(1702, "Ending Title", "store")], restorable=1)
    snaps, _err = _run(tmp_path, [page, _RUNNING, {"__throw": {"status": 403, "detail": "Not authenticated"}},
                                  _finished(), page, {"ok": True, "started": True}, _RUNNING],
                       [_CANCEL_BTN, "tick", "canon-restore-plex-btn"])
    _s0, s1, s2, s3 = snaps
    assert s1["canon-restore-plex-status"]["text"] == "✗ cancel not sent — Not authenticated — press // CANCEL again"
    status = s2["canon-restore-plex-status"]
    assert status["text"].startswith("✓ restored 9 ") and status["className"] == "form-status form-status-ok"
    status = s3["canon-restore-plex-status"]
    assert (status["text"], status["className"]) == (_PROGRESS, "form-status"), "a finished run's cancel note came back"


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_an_idle_pages_failed_status_poll_reaches_the_console_and_a_watched_run_retries_quietly(tmp_path):
    (s0,), err = _run(tmp_path / "idle", [_report(), {"__throw": {"status": 502}}], [])
    assert s0["__timers"] == 0
    assert "canonical health restore status failed" in err, err
    snaps, err = _run(tmp_path / "running", [_report(), {"__throw": {"status": 502}}, _RUNNING], ["tick"],
                      ssr=_SSR_RUNNING)
    assert snaps[0]["__timers"] == 1 and snaps[1]["canon-restore-plex-status"]["text"] == _PROGRESS
    assert "restore status failed" not in err, err
