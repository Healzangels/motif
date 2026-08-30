"""v0.51.302 — holistic round 2, wave 11: sixteen low-severity closures.

Boot: init_db before the first log_event (fresh installs lost the
fail-closed forward-auth WARNING); the coalescer flush is joined by an
events-flusher drain; an unwritable /config logs the WRITABILITY guidance
instead of crashing boot; the zombie sweep owns its get_conn import.
Core: the queue-burst bookkeeping is locked; two lex-compare timestamps
moved to julianday; throughput anchors to the last sample; tmdb drops
datetime.utcnow(); reconcile reports an indeterminate census instead of a
false mass-orphan when the enum never ran; the notify attachment fetch is
gated on an embedded sink; the scrubber coerces non-JSON-native objects to
redacted strings; PlexConfig keeps the token out of its repr; the
re-upload path aborts on an empty 200 body; the write-probe unlink is
race-proof. UI: the deferred INFO re-opens bail unless the dialog is
still open on the same card.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
MAIN = (REPO / "app" / "main.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── behavioral ───────────────────────────────────────────────


def test_reconcile_reports_indeterminate_when_enum_never_ran(tmp_path):
    from app.core.db import get_conn, init_db, transaction
    from app.core.reconcile import run_reconciliation
    db = tmp_path / "motif.db"
    themes = tmp_path / "themes"
    themes.mkdir()
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, 'x', 'x')""")
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('movie', 302001, 'T', 'imdb', 'x', 'x')""")
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size,
                 downloaded_at, source_video_id, provenance, source_kind)
               VALUES ('movie', 302001, '1', '', 'p.mp3', 's', 1, 'x', 'v',
                       'auto', 'themerrdb')""")
    out = run_reconciliation(db, themes, dry_run=True)
    assert out["orphans"]["count"] is None, (
        "canonicals + empty plex_items = the enum never ran — reporting "
        "every canonical as orphaned is the v1.18.10 amplifier shape")
    assert "skipped" in out["orphans"]


def test_flush_events_drains_the_queue(tmp_path):
    # the flusher is process-global bound to the FIRST db_path (documented),
    # so in-suite the row lands in another test's db — the contract
    # flush_events owns is that the QUEUE is drained before return.
    from app.core import events as ev
    from app.core.db import init_db
    db = tmp_path / "motif.db"
    init_db(db)
    ev.log_event(db, level="INFO", component="t", message="drain-me")
    ev.flush_events(timeout=5.0)
    assert ev._EVENT_QUEUE.empty(), (
        "the shutdown drain must empty the queue — a daemon-thread kill "
        "mid-queue silently drops the final events")


def test_scrubber_coerces_unknown_objects(tmp_path):
    from app.core.events import _scrub_value
    class _UrlLike:
        def __str__(self):
            return "https://user:hunter2@plex.host/x"
    out = _scrub_value(_UrlLike())
    assert isinstance(out, str) and "hunter2" not in out, (
        "a non-JSON-native object serialized via repr bypassed every "
        "redaction pre-fix")


def test_plex_config_repr_hides_the_token():
    from app.core.plex import PlexConfig
    r = repr(PlexConfig(url="http://p:32400", token="SECRETTOKEN",
                        movie_section="1", tv_section="2"))
    assert "SECRETTOKEN" not in r


def test_probe_unlink_is_race_proof(tmp_path):
    from app.config import probe_dir_writable
    assert probe_dir_writable(tmp_path) is None
    src = (REPO / "app" / "config.py").read_text()
    assert "probe.unlink(missing_ok=True)" in src, (
        "a concurrent probe deleting the shared filename made the loser "
        "report an unwritable dir that is fine")


def test_get_conn_burst_lock_serializes():
    from app.core import progress
    import threading
    assert isinstance(progress._QUEUE_BURST_LOCK, type(threading.Lock()))
    src = (REPO / "app" / "core" / "progress.py").read_text()
    i = src.index("def _synthesize_queue_ops(")
    blk = src[i:src.index("def _synthesize_queue_ops_locked(")]
    assert "with _QUEUE_BURST_LOCK:" in blk


# ── wiring pins ──────────────────────────────────────────────


def test_boot_creates_schema_before_the_first_log_event():
    assert MAIN.index("init_db(settings.db_path)") < MAIN.index(
        "forward_auth on with empty IP allowlist"), (
        "the fail-closed WARNING wrote to a table that did not exist on the "
        "fresh install where it mattered most")


def test_shutdown_drains_events_after_the_coalescer():
    i = MAIN.index("flush_all_coalesced()")
    j = MAIN.index("flush_events(timeout=5.0)")
    assert i < j, "coalescer tails first, then the events queue"


def test_bootstrap_save_is_guarded():
    i = MAIN.index("def _bootstrap_config_file")
    blk = MAIN[i:MAIN.index("\ndef ", i + 10)]
    assert "except OSError" in blk and "WRITABILITY" in blk, (
        "an unwritable /config crashed boot before the diagnostic that "
        "explains the uid/owner fix could print")


def test_zombie_sweep_owns_its_get_conn_import():
    i = MAIN.index("v0.51.302: own binding")
    assert "from .core.db import get_conn" in MAIN[i - 100:i + 100]


def test_no_utcnow_left_in_app():
    import subprocess
    r = subprocess.run(
        ["grep", "-rn", "datetime.utcnow()", str(REPO / "app")],
        capture_output=True, text=True)
    live = [l for l in r.stdout.splitlines()
            if l and not l.split(":", 2)[2].lstrip().startswith(("#", "#   "))]
    assert live == [], f"deprecated CALL sites remain:\n{live}"


def test_progress_timestamp_compares_use_julianday():
    src = (REPO / "app" / "core" / "progress.py").read_text()
    assert "AND finished_at > datetime('now'" not in src
    assert "AND finished_at < datetime('now'" not in src
    assert src.count("julianday(finished_at)") >= 2


def test_throughput_anchors_to_the_last_sample():
    src = (REPO / "app" / "core" / "progress.py").read_text()
    i = src.index('throughput_buf[-1]["ts"] if throughput_buf')
    assert "updated_at" in src[i:src.index(")", i)], (
        "graceful first-sample fallback")


def test_attachment_fetch_gated_on_embedded_sink():
    src = (REPO / "app" / "core" / "notify.py").read_text()
    assert "if (attach_url and urls) else None" in src


def test_reupload_aborts_on_empty_body():
    # v0.51.307: anchor moved — the one-off "empty_fetch" reason key was
    # reshaped to the sibling step_failed/fetch contract; the invariant here
    # is unchanged (an empty 200 body aborts before the POST).
    src = (REPO / "app" / "core" / "plex.py").read_text()
    i = src.index("aborting the re-select")
    assert "if not audio_bytes:" in src[i - 600:i]


def test_deferred_reopens_check_the_card():
    assert APP_JS.count("_d.dataset.cardKey ===") == 2, (
        "both deferred re-opens (LEVEL/UNDO +900ms, RE-MEASURE +700ms) must "
        "bail when the dialog closed or moved to another row")


def test_v0_51_302_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.302: " in init_py
