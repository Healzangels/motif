"""v1.23.42 — holistic-audit follow-ups (C1 / B3 / B2).

Three contained fixes from the v1.23.39-era holistic audit, parked pending
the user's go-ahead:

C1 (class-12 event-loop block): `api_refresh_libraries` ran the PlexClient
   section-discovery round-trip + `refresh_sections` directly in its async
   body → froze the single event loop (every concurrent request, UI polling
   included, hung for the whole Plex discovery). Now offloaded to
   run_in_threadpool, and `refresh_sections` is registered with the v1.22.58
   async-lint so a future direct call is caught.

B3 (classifier ordering, transient→dead): `classify_yt_dlp_error` matched the
   broad "video unavailable" / "is unavailable" / "no longer available" DEAD
   patterns before the network check, so transient YouTube anti-bot / temporary
   / timeout messages that ALSO carry an "unavailable" substring were red-
   pilled (needs_manual_override → dropped from "ready to add", no retry). A
   transient-signal guard now wins first (mirrors the v1.15.12 rate-limit fix).

B2 (mount-fault guards): verify_canonical_health / verify_placement_health
   bounded only the False-read (missing) count; a partial mount fault is a MIX
   of False-reads + OSError-skips, so a mostly-ESTALE outage whose few False-
   reads stayed under the cap still stamped rows false-broken. The skipped
   count now folds into the suspect total. verify_canonical_health also probes
   themes_dir liveness up front — a dead canonical root skips the whole run.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core import plex_enum
from app.core.db import init_db
from app.core.downloader import classify_yt_dlp_error, FailureKind


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()
NOW = "2026-06-14T00:00:00"


# ── C1: refresh_libraries off the event loop ────────────────────


def test_refresh_libraries_offloads_to_threadpool():
    """The api_refresh_libraries body wraps the blocking PlexClient +
    refresh_sections work in a nested _run() awaited via run_in_threadpool."""
    i = API_PY.index("async def api_refresh_libraries(")
    body = API_PY[i:i + 5000]
    assert "def _run():" in body
    assert "return await run_in_threadpool(_run)" in body
    # the blocking calls live INSIDE the offload, not the async body.
    run_idx = body.index("def _run():")
    assert body.index("refresh_sections(") > run_idx
    assert body.index("with PlexClient(") > run_idx


def test_refresh_sections_registered_with_async_lint():
    """refresh_sections is a module function driving a Plex round-trip — it must
    be in the v1.22.58 lint's BLOCKING_FUNCS so a future async handler calling
    it directly fails the standing guard (it evaded detection until v1.23.42)."""
    lint = (REPO / "tests" / "test_v1_22_58_async_no_blocking_calls.py").read_text()
    assert '"refresh_sections"' in lint


# ── B3: transient yt-dlp errors are not classified dead ─────────


def test_transient_unavailable_classifies_network_not_removed():
    """Messages carrying a transient signal (try-again-later / temporary /
    timeout / connection / 503) must be NETWORK_ERROR — preserved + retried —
    even when they also contain an "unavailable" substring the dead patterns
    would otherwise catch."""
    transient = [
        "Video unavailable. This content isn't available, try again later.",
        "ERROR: This video is temporarily unavailable.",
        "HTTP Error 503: Service Unavailable",
        "Read timed out.",
        "Connection reset by peer",
    ]
    for msg in transient:
        kind = classify_yt_dlp_error(msg)
        assert kind == FailureKind.NETWORK_ERROR, (msg, kind)
        # transient → must NOT red-pill (needs_manual_override drops the row
        # from "ready to add" + the recovery card demands a new URL).
        assert not kind.needs_manual_override, msg


def test_genuinely_removed_still_classified_dead():
    """Regression guard: a real removal with no transient token still maps to
    VIDEO_REMOVED (the transient guard must not swallow genuine death)."""
    dead = [
        "Video unavailable. The uploader has not made this video available",
        "This video has been removed by the user",
        "HTTP Error 410: Gone. This video has been removed",
    ]
    for msg in dead:
        assert classify_yt_dlp_error(msg) == FailureKind.VIDEO_REMOVED, msg


def test_transient_guard_precedes_dead_patterns_in_source():
    """The transient-signal block must sit BEFORE the 'video unavailable' dead
    pattern in classify_yt_dlp_error — ordering is the whole fix."""
    src = (REPO / "app" / "core" / "downloader.py").read_text()
    fn = src[src.index("def classify_yt_dlp_error("):]
    fn = fn[:fn.index("\n\nclass DownloadError")]
    # compare the CODE lines (not the v1.15.12 comment prose, which line-wraps
    # "try again\nlater"): the transient condition must precede the dead check.
    assert fn.index('"try again later" in m') < fn.index('if "video unavailable" in m')


# ── B2: mount-fault guards fold skipped + probe root liveness ───


def _seed_lf(conn, *, tid, tmdb, file_path):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','M','movie',0,0,'movies',1,?,?)"
        " ON CONFLICT(section_id) DO NOTHING", (NOW, NOW))
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, 'X', 'imdb', ?, ?, 'u')", (tid, tmdb, NOW, NOW))
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind,"
        " edition_key) VALUES ('movie', ?, '1', ?, ?, ?, 'V', 'auto',"
        " 'themerrdb', '')", (tmdb, tid, file_path, NOW))


def test_canonical_health_dead_root_skips_run(tmp_path):
    """A dead themes_dir (mount dropped) must skip the run wholesale —
    every prior canonical_present preserved, nothing stamped false-broken."""
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        for i in range(5):
            _seed_lf(conn, tid=i + 1, tmdb=100 + i,
                     file_path=f"movies/{100 + i}/theme.mp3")
        conn.commit()
    dead_root = tmp_path / "no-such-mount"   # never created
    res = plex_enum.verify_canonical_health(db, dead_root)
    assert res == {"checked": 0, "missing": 0, "skipped": 5}
    zero = sqlite3.connect(db).execute(
        "SELECT COUNT(*) FROM local_files WHERE canonical_present = 0"
    ).fetchone()[0]
    assert zero == 0, "dead root must not stamp any row broken"


def test_canonical_health_caps_with_live_root(tmp_path):
    """With a LIVE root but every file missing (beyond the cap), the missing-
    stamps are still skipped — this exercises the cap path the liveness gate
    would otherwise shadow."""
    db = tmp_path / "m.db"
    init_db(db)
    root = tmp_path / "themes"
    root.mkdir()   # root is alive; the per-file theme.mp3s are not
    with sqlite3.connect(db) as conn:
        for i in range(60):   # 60 > cap max(50, 60//4)=50
            _seed_lf(conn, tid=i + 1, tmdb=1000 + i,
                     file_path=f"movies/{1000 + i}/theme.mp3")
        conn.commit()
    res = plex_enum.verify_canonical_health(db, root)
    assert res["missing"] == 0
    zero = sqlite3.connect(db).execute(
        "SELECT COUNT(*) FROM local_files WHERE canonical_present = 0"
    ).fetchone()[0]
    assert zero == 0


def test_verify_caps_fold_skipped_into_suspect_total():
    """Both verify functions fold the OSError-skipped count into the cap's
    suspect total (a partial mount fault is missing + skipped, not just
    missing)."""
    # placement health: suspect includes skipped.
    ph = PLEX_ENUM_PY[PLEX_ENUM_PY.index("def verify_placement_health("):]
    ph = ph[:ph.index("def verify_canonical_health(")]
    assert "len(missing_updates) + len(prune) + skipped" in ph
    # canonical health: cap check includes skipped.
    ch = PLEX_ENUM_PY[PLEX_ENUM_PY.index("def verify_canonical_health("):]
    assert "len(missing_updates) + skipped > cap" in ch
    # and the liveness gate exists.
    assert "themes_dir.is_dir()" in ch
