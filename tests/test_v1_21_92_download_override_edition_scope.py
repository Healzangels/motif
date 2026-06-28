"""v1.21.92 — _do_download override resolution must be edition-scoped.

the user's anime repro (real library, Yu-Gi-Oh! Duel Monsters tv/902):
REPLACE TDB on the "Uncut" edition — which has NO override of its own —
downloaded the "Odex Dub" edition's USER-URL override instead of Uncut's
ThemerrDB URL. Odex Dub is the un-tagged folder (edition_key='') carrying
a manual override; Uncut is the {edition-uncut} folder (edition_key=
'uncut'). The worker's _do_download resolved the override by (media_type,
tmdb_id, section_id) with NO edition filter, so the Uncut download matched
the '' override and fetched the wrong video into the Uncut folder — Uncut
ended up stuck SRC=U playing the sibling's song.

The download DESTINATION (out_dir / canonical_theme_subdir) was already
edition-aware (the file correctly landed in {edition-uncut}/); only the
URL RESOLUTION leaked across editions. This pins it by running the real
_do_download in dry-run mode (no network) and asserting the resolved URL
in the DRY-RUN event detail.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading

from app.core.db import init_db
from app.core.events import now_iso


NOW = now_iso()
TDB_URL = "https://www.youtube.com/watch?v=MjdNz071O4E"   # themes.youtube_url
USER_URL = "https://www.youtube.com/watch?v=nAAL759bNjU"  # Odex Dub override
ODEX_RK = "579200"   # un-tagged folder → edition_key=''
UNCUT_RK = "579145"  # {edition-uncut}  → edition_key='uncut'


def _settings(tmp_path):
    from app.config import Settings
    from app.core.runtime import set_dry_run
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    (tmp_path / "themes").mkdir(parents=True, exist_ok=True)
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    set_dry_run(s.db_path, True, updated_by="test")
    return s


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('3','Anime','show',1,0,'anime',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('tv',902,'Yu-Gi-Oh','2000','themoviedb',"
            "?,?,?)", (NOW, NOW, TDB_URL))
        tid = cur.lastrowid
        for rk, ek, fp in (
            (ODEX_RK, "", "/d/Anime/Yu-Gi-Oh! Duel Monsters (2000)"),
            (UNCUT_RK, "uncut",
             "/d/Anime/Yu-Gi-Oh! Duel Monsters (2000) {edition-uncut}"),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'3','show',"
                "?,902,'Yu-Gi-Oh','2000',?,?,1,?,?)", (rk, tid, ek, fp, NOW, NOW))
        # The Odex Dub (edition '') manual override — the URL that bled.
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,"
            " set_at, set_by, section_id, edition_key) VALUES ('tv',902,?,?,"
            "'admin','3','')", (USER_URL, NOW))
        conn.commit()
    return tid


def _run_download(s, caplog, *, edition_key):
    """Run the real _do_download in dry-run mode and return the DRY-RUN log
    line. log_event writes the events table asynchronously (background
    flusher) but logs the message synchronously to getLogger('dryrun') —
    the dry-run message embeds the resolved yt_url + the would_save_to path
    (appended as the JSON detail), so caplog captures both with no race."""
    payload = json.dumps({"edition_key": edition_key}) if edition_key else "{}"
    with sqlite3.connect(s.db_path) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at) VALUES ('download','tv',902,'3',"
            "?,'running',?)", (payload, NOW))
        conn.commit()
    c = sqlite3.connect(s.db_path)
    c.row_factory = sqlite3.Row
    job = c.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 1").fetchone()
    with caplog.at_level(logging.INFO, logger="dryrun"):
        _worker(s)._do_download(job)
    line = next((r.getMessage() for r in caplog.records
                 if "DRY-RUN: would download" in r.getMessage()), None)
    assert line is not None, "expected a DRY-RUN download log line"
    return line


def test_uncut_replace_tdb_resolves_tdb_url_not_sibling_override(tmp_path, caplog):
    """THE repro: a download for edition 'uncut' (no override of its own)
    must resolve the ThemerrDB URL, NOT the '' edition's user override."""
    s = _settings(tmp_path)
    _seed(s.db_path)
    line = _run_download(s, caplog, edition_key="uncut")
    assert TDB_URL in line, (
        "Uncut (no override) must download its ThemerrDB URL, not the "
        "Odex Dub '' edition's user override", line)
    assert USER_URL not in line
    assert "{edition-uncut}" in line


def test_standard_edition_still_uses_its_own_override(tmp_path, caplog):
    """The '' (Odex Dub) edition keeps resolving its own override — the fix
    scopes per-edition without regressing the standard edition."""
    s = _settings(tmp_path)
    _seed(s.db_path)
    line = _run_download(s, caplog, edition_key="")
    assert USER_URL in line, (
        "the '' edition's own override must still apply", line)
    assert "{edition-" not in line


def test_v1_21_92_version_pin():
    from pathlib import Path
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
