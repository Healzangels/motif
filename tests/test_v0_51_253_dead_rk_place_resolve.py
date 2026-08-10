"""v0.51.253 — the place resolve must target the LIVE rating key.

INCIDENT 2026-08-09. A disk dropped off the array; Plex rescanned, DELETED the
items it could no longer see, and on reconnect RE-ADDED them as brand-new items
with brand-new rating keys. The operator refreshed Plex in motif, saw ~80 movies
now sourceless-but-backed-up, selected all and bulk-PUSHed. Every upload died:

    upload_collection_theme: rk=137499 multipart HTTP 404 in 0.0s
    RuntimeError: Plex rejected upload (rk=137499, HTTP 404, size=8.8MB)

motif was uploading to the rating keys Plex had already deleted.

WHY the stale key survived the refresh: the v0.51.128 reaper deliberately holds
a missing row for `_REAP_MISS_THRESHOLD` (2) consecutive enums before deleting
it, so one transient Plex glitch can't false-delete a library and fire a storm
of 💔 alerts. One REFRESH PLEX = miss #1. So plex_items legitimately held BOTH
rows — and that is a NORMAL state, not corruption.

WHY the DEAD one won — measured on the operator's DB, and NOT what I first
guessed. I assumed the new row lacked `theme_id` and so lost the JOIN. It has
it; `resolve_theme_ids` had already linked both:

    {'rating_key': '137499', 'theme_id': 2474, 'has_theme': 1,
     'consecutive_missing': 1, 'last_seen_at': '...T13:01:51+00:00'}   # DEAD
    {'rating_key': '738854', 'theme_id': 2474, 'has_theme': 0,
     'consecutive_missing': 0, 'last_seen_at': '...T20:44:54+00:00'}   # LIVE

Both matched the JOIN, and a bare `LIMIT 1` with no ORDER BY returned whichever
SQLite yielded first — the older, dead one, every single time. Hence the log's
contradiction: `cached_has_theme=True` (read off the dead row) on titles the
library rendered with no SRC (read off the live row).

The fix is ordering, on all three resolve branches: `consecutive_missing ASC`
(rows Plex returned THIS enum first), `last_seen_at DESC` to break ties. That
also makes the reaper irrelevant to this path — the next dropout self-corrects
on the first enum instead of needing two refreshes and a manual re-push.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso

# The operator's actual numbers — the incident, reproduced.
TMDB = 68726                       # Pacific Rim
DEAD_RK = "137499"                 # pre-disconnect, Plex deleted it
LIVE_RK = "738854"                 # post-reconnect re-add
OLDER = "2026-08-09T13:01:51+00:00"
NEWER = "2026-08-09T20:44:54+00:00"
FOLDER = "/data/Movies/Pacific Rim (2013)"


def _settings(tmp_path, *, dry):
    from app.config import Settings
    from app.core.runtime import set_dry_run
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    set_dry_run(s.db_path, dry, updated_by="test")
    return s


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def _seed(db, *, rows, with_local=False, themes_dir=None):
    """rows = [(rating_key, consecutive_missing, has_theme, last_seen_at)].

    The DEAD row is inserted FIRST (lower rowid) — that is the discriminator.
    Pre-fix, `LIMIT 1` with no ORDER BY returns it; post-fix, ordering by
    liveness must skip past it to the live row regardless of insert order.
    Every row gets the SAME theme_id, which is what the operator's DB actually
    showed and what makes a theme_id-based tiebreak useless here.
    """
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)",
            (OLDER, NEWER))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',?,'Pacific Rim','2013','imdb',?,?,"
            "'https://www.youtube.com/watch?v=tdb00068726')",
            (TMDB, OLDER, OLDER))
        tid = cur.lastrowid
        for rk, missing, has_theme, seen in rows:
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, local_theme_file, consecutive_missing,"
                " first_seen_at, last_seen_at)"
                " VALUES (?,'1','movie',?,?,'Pacific Rim','2013','',?,?,0,?,?,?)",
                (rk, tid, TMDB, FOLDER, has_theme, missing, OLDER, seen))
        if with_local:
            rel = "movies/pacific-rim.mp3"
            p = Path(themes_dir) / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b"ID3themebytes")
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',?,'1','',?,?,"
                "'v','auto','themerrdb')", (TMDB, rel, OLDER))
        conn.commit()
    return tid


def _place_job(db, *, kind="api", edition_key=""):
    payload = json.dumps({"kind": kind, "edition_key": edition_key})
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at) VALUES ('place','movie',?,'1',"
            "?,'running',?)", (TMDB, payload, OLDER))
        conn.commit()
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c.execute("SELECT * FROM jobs WHERE tmdb_id=?", (TMDB,)).fetchone()


def _theme_row(db, tid):
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c.execute("SELECT * FROM themes WHERE id=?", (tid,)).fetchone()


def _resolved_rk(s, tid, caplog):
    job = _place_job(s.db_path)
    theme = _theme_row(s.db_path, tid)
    with caplog.at_level(logging.INFO, logger="app.core.worker"):
        _worker(s)._do_place_collection(job=job, theme=theme, local=None)
    line = next((r.getMessage() for r in caplog.records
                 if "_do_place_collection: job=" in r.getMessage()), None)
    assert line is not None, "expected the _do_place_collection state log"
    return line


# ── THE incident ─────────────────────────────────────────────────────────

def test_dead_and_live_row_resolves_the_live_rk(tmp_path, caplog):
    """Both rows carry theme_id (measured); only consecutive_missing tells them
    apart. Pre-fix this resolved DEAD_RK and every upload 404'd."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, rows=[
        (DEAD_RK, 1, 1, OLDER),     # inserted first — what LIMIT 1 used to pick
        (LIVE_RK, 0, 0, NEWER),
    ])

    line = _resolved_rk(s, tid, caplog)

    assert f"cached_rk={LIVE_RK}" in line, (
        f"v0.51.253: must upload to the LIVE rating key — got {line!r}. "
        "Resolving the dead rk is the 2026-08-09 incident: HTTP 404 on every "
        "push until the reaper eventually pruned the row.")
    assert f"cached_rk={DEAD_RK}" not in line


def test_has_theme_is_read_off_the_live_row(tmp_path, caplog):
    """The incident's tell: cached_has_theme=True came off the DEAD row while
    the library rendered the LIVE row as sourceless. Pin that they now agree —
    otherwise the skip-vs-upload decision is made on the wrong row's state."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, rows=[
        (DEAD_RK, 1, 1, OLDER),
        (LIVE_RK, 0, 0, NEWER),
    ])

    line = _resolved_rk(s, tid, caplog)

    assert "cached_has_theme=False" in line, (
        f"read has_theme off the dead row — got {line!r}")


def test_insert_order_does_not_decide(tmp_path, caplog):
    """Same pair, LIVE inserted first. A test that only passed because of row
    order would be a mirror, not a guard — this pins the ordering itself."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, rows=[
        (LIVE_RK, 0, 0, NEWER),
        (DEAD_RK, 1, 1, OLDER),
    ])

    assert f"cached_rk={LIVE_RK}" in _resolved_rk(s, tid, caplog)


def test_two_dead_rows_pick_the_most_recently_seen(tmp_path, caplog):
    """Mid-incident state: BOTH rows missing this enum (Plex down, or the
    re-add hasn't been enumerated yet). Nothing is live, so the tiebreak is
    last_seen_at — never silently the oldest."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, rows=[
        (DEAD_RK, 1, 1, OLDER),
        (LIVE_RK, 1, 0, NEWER),
    ])

    assert f"cached_rk={LIVE_RK}" in _resolved_rk(s, tid, caplog)


def test_single_healthy_row_is_unchanged(tmp_path, caplog):
    """The 99.9% case. The ordering must not perturb an ordinary one-row
    title — if this breaks, the fix is worse than the bug."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, rows=[(LIVE_RK, 0, 1, NEWER)])

    line = _resolved_rk(s, tid, caplog)
    assert f"cached_rk={LIVE_RK}" in line
    assert "cached_has_theme=True" in line


# ── the guid_tmdb fallback branch carries the same ordering ──────────────

def test_guid_tmdb_fallback_also_prefers_live(tmp_path, caplog):
    """Branch 2 (theme_id not yet resolved — a freshly-downloaded row). Same
    dead/live pair, so the same ordering has to be on this query too: the
    v0.51.246-252 review's standing finding is a fix landing on some sites and
    not its siblings."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, rows=[
        (DEAD_RK, 1, 1, OLDER),
        (LIVE_RK, 0, 0, NEWER),
    ])
    # Break the theme_id linkage so branch 1 misses and branch 2 answers.
    with sqlite3.connect(s.db_path) as conn:
        conn.execute("UPDATE plex_items SET theme_id = NULL")
        conn.commit()

    assert f"cached_rk={LIVE_RK}" in _resolved_rk(s, tid, caplog)


# ── the v1.24.24 ambiguity guard, re-scoped to live rows ─────────────────

def test_dead_plus_live_is_not_treated_as_ambiguous(tmp_path, caplog):
    """Branch 3. The v1.24.24 guard refuses to guess across 2+ candidates —
    aimed at multi-EDITION siblings. A re-added title also presents 2 rows and
    was wrongly refused, turning a recoverable place into a no-match. One LIVE
    candidate is not ambiguous."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, rows=[
        (DEAD_RK, 1, 1, OLDER),
        (LIVE_RK, 0, 0, NEWER),
    ])
    # Force branch 3: no theme_id link, and no '' edition_key row to match on.
    with sqlite3.connect(s.db_path) as conn:
        conn.execute("UPDATE plex_items SET theme_id = NULL,"
                     " edition_key = 'directors cut'")
        conn.commit()

    assert f"cached_rk={LIVE_RK}" in _resolved_rk(s, tid, caplog)


def test_two_live_editions_still_refuse_to_guess(tmp_path, caplog):
    """The v1.24.24 protection itself, preserved. Two LIVE tagged siblings and
    no '' row is genuinely ambiguous — picking one hardlinks the theme into the
    wrong folder and refreshes the wrong rk. Must resolve nothing."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, rows=[
        ("9101", 0, 0, NEWER),
        ("9102", 0, 0, NEWER),
    ])
    with sqlite3.connect(s.db_path) as conn:
        conn.execute("UPDATE plex_items SET theme_id = NULL")
        conn.execute("UPDATE plex_items SET edition_key = 'directors cut'"
                     " WHERE rating_key = '9101'")
        conn.execute("UPDATE plex_items SET edition_key = 'extended'"
                     " WHERE rating_key = '9102'")
        conn.commit()

    line = _resolved_rk(s, tid, caplog)
    assert "cached_rk=None" in line, (
        f"v1.24.24 ambiguity guard must still refuse — got {line!r}")


def test_all_dead_single_candidate_still_falls_back(tmp_path, caplog):
    """The pre-v0.51.253 count is kept when NOTHING is live — a section-wide
    Plex glitch must not newly strand a place that used to resolve. One
    candidate, dead, no '' row: still falls back to it."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, rows=[(DEAD_RK, 1, 1, OLDER)])
    with sqlite3.connect(s.db_path) as conn:
        conn.execute("UPDATE plex_items SET theme_id = NULL,"
                     " edition_key = 'directors cut'")
        conn.commit()

    assert f"cached_rk={DEAD_RK}" in _resolved_rk(s, tid, caplog)


# ── the FILE path is the sibling site, and must match ────────────────────

class _Outcome:
    placed = False
    reason = "no_match"
    kind = "hardlink"
    target_folder = None
    plex_rating_key = None
    plex_refreshed = False


def test_file_path_resolve_also_prefers_live(tmp_path, monkeypatch):
    """`_do_place` (hardlink) has its OWN resolve, and `kind='api'` RETURNS
    before reaching it — so patching one does not patch the other. That split
    is exactly the review's standing meta-pattern (a fix landing on some sites
    and not its siblings), and it bit while writing this tag: the first patch
    went to the FILE resolve while the incident ran through the API one. Both
    are pinned now. Observed via the cached_rk handed to place_theme."""
    import app.core.worker as worker_mod

    s = _settings(tmp_path, dry=False)
    _seed(s.db_path, rows=[
        (DEAD_RK, 1, 1, OLDER),
        (LIVE_RK, 0, 0, NEWER),
    ], with_local=True, themes_dir=s.themes_dir)
    job = _place_job(s.db_path, kind="file", edition_key="")

    captured: dict = {}

    def _fake_place_theme(**kwargs):
        captured.update(kwargs)
        return _Outcome()

    monkeypatch.setattr(worker_mod, "place_theme", _fake_place_theme)
    _worker(s)._do_place(job)

    assert captured, "place_theme was never reached"
    assert captured.get("cached_rk") == LIVE_RK, (
        f"the FILE path resolved {captured.get('cached_rk')!r} — the dead-rk "
        "ordering must be on both resolves, not just the API one")


# ── the mislabeled log field ─────────────────────────────────────────────

def test_state_log_labels_the_job_id_as_job(tmp_path, caplog):
    """`rk=7621` next to `cached_rk=137499` read as two rating keys during the
    incident; 7621 was the JOB id. Two numbers, two different meanings, one
    label."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, rows=[(LIVE_RK, 0, 0, NEWER)])
    job = _place_job(s.db_path)
    theme = _theme_row(s.db_path, tid)

    with caplog.at_level(logging.INFO, logger="app.core.worker"):
        _worker(s)._do_place_collection(job=job, theme=theme, local=None)

    line = next((r.getMessage() for r in caplog.records
                 if "_do_place_collection:" in r.getMessage()), None)
    assert line is not None
    assert f"job={job['id']}" in line, line
    assert f"rk={job['id']} " not in line, (
        f"the job id is still labeled rk= — {line!r}")
