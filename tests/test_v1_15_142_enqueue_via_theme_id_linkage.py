"""v1.15.142 — `_enqueue_download` matches via pi.theme_id linkage.

the user reproduction (anime, v1.15.141):

> attempted a download TDB backup on Dark Gathering, but nothing
> happened. Expected to see a green DL and copy there and a
> link PS.

Log line:

    TDB backup download requested by admin
    (0 sections, scoped to 3)

## Root cause — `app/core/sync.py:_enqueue_download`

The section-lookup query strictly required
`pi.guid_tmdb = tmdb_id`:

    SELECT DISTINCT pi.section_id
    FROM plex_items pi
    JOIN plex_sections ps ON ...
    WHERE pi.guid_tmdb = ?  -- ← STRICT
      AND pi.media_type = ?
      AND pi.section_id = ?
      AND ps.included = 1

Plex rows matched via the anime agent (most anime — Dark
Gathering's case) typically have `guid_tmdb = NULL` because the anime agent
resolves to TVDB/AniDB GUIDs, not TMDB.

`plex_enum.resolve_theme_ids()` (plex_enum.py:1058-1199) sets
`pi.theme_id` via THREE fallback paths:
  1. `pi.guid_tmdb = t.tmdb_id` (preferred)
  2. `pi.guid_imdb = t.imdb_id` (orphan match)
  3. `pi.title_norm = t.title_norm AND pi.year = t.year`
     (last-resort title fallback)

The library list query joins on `pi.theme_id` (api.py:1913), so
anime-agent-matched rows DO appear in the UI with TDB-tracked state.
But `_enqueue_download`'s strict `guid_tmdb` filter missed them
— every action endpoint that calls into it (download-backup,
redownload, accept_update, override, etc.) silently no-opped
on anime-agent-matched rows.

## Fix

Match by themes-row identity (the actual semantic target)
through the `pi.theme_id` linkage:

    LEFT JOIN themes t ON t.id = pi.theme_id
    WHERE t.tmdb_id = ?
      AND t.media_type = ?  -- motif format ('tv'/'movie')
      AND pi.media_type = ? -- plex format ('show'/'movie')
      ...

If `pi.theme_id` is NULL (resolve hasn't run yet, or all three
fallback paths failed), the LEFT JOIN gives NULL `t.*` and
`t.tmdb_id = ?` fails → row not matched. Same behavior as
before for unresolved rows.

## Defensive UX — `app/web/static/app.js`

Click handler now alerts the user when `enqueued_sections === 0`
comes back. Pre-fix the response was silently treated as success
(loadLibrary, libraryRapidPoll) and the user saw nothing happen.
Future edge cases that still hit 0 (e.g. section excluded
mid-click) get a clear message instead of silent failure.

## Tests
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import init_db
from app.core.sync import _enqueue_download
from app.core.events import now_iso


def _seed_section_and_themes(conn: sqlite3.Connection) -> None:
    """Common fixture: one included anime section + one themes
    row with a TDB URL. Each test seeds a different plex_items
    matching pattern on top."""
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES ('3', 'Anime', 'show', 1, 1, 0, 'anime', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, "
        "                    title_norm, year, "
        "                    upstream_source, youtube_url, "
        "                    first_seen_sync_at, last_seen_sync_at) "
        "VALUES (1, 'tv', 999888, 'Dark Gathering', "
        "        'dark gathering', 2023, 'themoviedb', "
        "        'https://youtube.com/watch?v=abc11111111', ?, ?)",
        (now, now),
    )


# ── primary regression: anime-agent-matched row (guid_tmdb=NULL) ─────────

def test_enqueue_finds_section_when_guid_tmdb_null_but_theme_id_set(tmp_path):
    """The Dark Gathering case: anime matched by the anime agent → no
    guid_tmdb on the plex_items row, but plex_enum's title-
    fallback resolve_theme_ids stamped `theme_id=1` pointing at
    the themes row. _enqueue_download must find the section."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_section_and_themes(conn)
    now = now_iso()
    # plex_items row with guid_tmdb=NULL (anime-agent-matched) and
    # theme_id=1 set (title-fallback linked).
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
        "  year, has_theme, local_theme_file, folder_path, "
        "  theme_id, plex_independent_theme, "
        "  first_seen_at, last_seen_at) "
        "VALUES ('rk-dg', '3', 'show', NULL, 456789, "
        "        'Dark Gathering', 'dark gathering', '2023', "
        "        1, 0, '/data/anime/Dark Gathering', "
        "        1, 1, ?, ?)",
        (now, now),
    )
    conn.commit()
    n = _enqueue_download(
        conn, media_type="tv", tmdb_id=999888,
        reason="manual_backup",
        auto_place=False,
        force_place=False,
        only_section_id="3",
    )
    conn.commit()
    assert n == 1, (
        f"v1.15.142: expected 1 enqueued job for anime-agent-matched "
        f"row (guid_tmdb=NULL, theme_id=1), got {n}. The pre-fix "
        f"strict pi.guid_tmdb filter silently skipped this row "
        f"even though the library list shows it as TDB-tracked."
    )
    job = conn.execute(
        "SELECT job_type, section_id, payload FROM jobs"
    ).fetchone()
    assert job is not None
    assert job["job_type"] == "download"
    assert job["section_id"] == "3"
    payload = json.loads(job["payload"])
    assert payload.get("auto_place") is False
    conn.close()


# ── orphan-IMDB-match path also works ─────────────────────────────

def test_enqueue_finds_section_when_matched_via_imdb_id(tmp_path):
    """Orphan rows: pi.guid_tmdb=NULL but pi.guid_imdb set and
    theme_id linked via the resolve_theme_ids imdb path
    (plex_enum.py:1158-1177). Should also enqueue correctly."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_section_and_themes(conn)
    # Update the themes row to have an imdb_id and plex_orphan source.
    conn.execute(
        "UPDATE themes SET imdb_id = 'tt9999999', "
        "  upstream_source = 'plex_orphan' WHERE id = 1"
    )
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, guid_imdb, title, title_norm, "
        "  year, has_theme, local_theme_file, folder_path, "
        "  theme_id, first_seen_at, last_seen_at) "
        "VALUES ('rk-imdb', '3', 'show', NULL, 'tt9999999', "
        "        'IMDB Match Show', 'imdb match show', '2020', "
        "        1, 0, '/data/anime/Foo', "
        "        1, ?, ?)",
        (now, now),
    )
    conn.commit()
    n = _enqueue_download(
        conn, media_type="tv", tmdb_id=999888,
        reason="manual_backup",
        only_section_id="3",
    )
    conn.commit()
    assert n == 1, (
        f"v1.15.142: imdb-matched orphan row (theme_id set via "
        f"plex_enum's imdb-fallback path) should also enqueue. "
        f"Got {n}."
    )
    conn.close()


# ── direct guid_tmdb match still works (regression guard) ─────────

def test_enqueue_still_works_for_guid_tmdb_matched_row(tmp_path):
    """The v1.14.45 happy path (movie row with guid_tmdb directly
    matching) must NOT regress. Mirrors test_v1_14_45's
    test_download_backup_enqueues_with_auto_place_false to ensure
    the v1.15.142 query change is a strict superset."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES ('1', 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, "
        "                    upstream_source, youtube_url, "
        "                    first_seen_sync_at, last_seen_sync_at) "
        "VALUES (10, 'movie', 555, 'Pure-P Movie', "
        "        'themoviedb', "
        "        'https://youtube.com/watch?v=abc11111111', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, year, has_theme, "
        "  local_theme_file, folder_path, theme_id, "
        "  plex_independent_theme, first_seen_at, last_seen_at) "
        "VALUES ('555', '1', 'movie', 555, 'Pure-P Movie', "
        "        '2020', 1, 0, '/data/movies/Pure-P', "
        "        10, 1, ?, ?)",
        (now, now),
    )
    conn.commit()
    n = _enqueue_download(
        conn, media_type="movie", tmdb_id=555,
        reason="manual_backup",
        auto_place=False,
        only_section_id="1",
    )
    conn.commit()
    assert n == 1, (
        f"v1.15.142: direct guid_tmdb match path must still "
        f"work (was working since v1.14.45). Got {n}."
    )
    conn.close()


# ── pi.theme_id NULL → no match (correct exclusion) ───────────────

def test_enqueue_skips_when_theme_id_unresolved(tmp_path):
    """If plex_enum hasn't resolved theme_id yet (or all three
    fallbacks failed — e.g. brand-new plex_items row with no
    matching themes record), the row must NOT be enqueued. The
    LEFT JOIN gives NULL t.*, and t.tmdb_id=? fails. Same
    behavior as the pre-fix query (which also wouldn't match
    because guid_tmdb was likely also NULL)."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_section_and_themes(conn)
    now = now_iso()
    # Note: theme_id explicitly NULL — resolve hasn't linked it.
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, title_norm, year, "
        "  has_theme, local_theme_file, folder_path, theme_id, "
        "  first_seen_at, last_seen_at) "
        "VALUES ('rk-unresolved', '3', 'show', NULL, "
        "        'Unresolved Show', 'unresolved show', '2024', "
        "        1, 0, '/data/anime/Unresolved', NULL, ?, ?)",
        (now, now),
    )
    conn.commit()
    n = _enqueue_download(
        conn, media_type="tv", tmdb_id=999888,
        reason="manual_backup",
        only_section_id="3",
    )
    conn.commit()
    assert n == 0, (
        f"v1.15.142: rows with theme_id=NULL must not be "
        f"enqueued — no link to a themes record means we don't "
        f"know what to download. Got {n}."
    )
    conn.close()


# ── excluded section → no enqueue ─────────────────────────────────

def test_enqueue_skips_excluded_section(tmp_path):
    """plex_sections.included = 0 sections must not be enqueued
    even when the plex_items row + theme_id are otherwise valid.
    Same exclusion logic as before (ps.included = 1 in WHERE)."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_section_and_themes(conn)
    # Flip the section to excluded.
    conn.execute("UPDATE plex_sections SET included = 0 WHERE section_id = '3'")
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, title_norm, year, "
        "  has_theme, local_theme_file, folder_path, theme_id, "
        "  first_seen_at, last_seen_at) "
        "VALUES ('rk-x', '3', 'show', NULL, "
        "        'Dark Gathering', 'dark gathering', '2023', "
        "        1, 0, '/data/anime/DG', 1, ?, ?)",
        (now, now),
    )
    conn.commit()
    n = _enqueue_download(
        conn, media_type="tv", tmdb_id=999888,
        reason="manual_backup",
        only_section_id="3",
    )
    conn.commit()
    assert n == 0, (
        f"v1.15.142: excluded section (ps.included=0) must still "
        f"be filtered out — the user opted out of that library. "
        f"Got {n}."
    )
    conn.close()


# ── non-section-scoped variant works too (fan-out path) ───────────

def test_enqueue_fan_out_without_section_scope(tmp_path):
    """When `only_section_id=None`, _enqueue_download fans out
    across every included section that owns the title (sync's
    multi-section enqueue path). The anime-agent-matched row must be
    visible to this path too."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_section_and_themes(conn)
    # Second section, also included.
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES ('7', 'Anime 4K', 'show', 1, 1, 1, 'anime-4k', ?, ?)",
        (now, now),
    )
    # Two plex_items rows — one in each section, both anime-agent-matched.
    for rk, sec in [("rk-3", "3"), ("rk-7", "7")]:
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "  media_type, guid_tmdb, title, title_norm, year, "
            "  has_theme, local_theme_file, folder_path, theme_id, "
            "  first_seen_at, last_seen_at) "
            "VALUES (?, ?, 'show', NULL, "
            "        'Dark Gathering', 'dark gathering', '2023', "
            "        1, 0, ?, 1, ?, ?)",
            (rk, sec, f"/data/sec-{sec}/DG", now, now),
        )
    conn.commit()
    n = _enqueue_download(
        conn, media_type="tv", tmdb_id=999888,
        reason="sync_auto",
        # No only_section_id → fan-out across all included sections
    )
    conn.commit()
    assert n == 2, (
        f"v1.15.142: fan-out path must also see anime-agent-matched "
        f"rows via theme_id linkage — got {n}, expected 2."
    )
    conn.close()


# ── JS surfaces the 0-enqueued case ───────────────────────────────

def test_js_alerts_on_zero_enqueued_sections():
    """The JS click handler must surface the 0-enqueued case to
    the user instead of silently treating it as success."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the download-tdb-backup click handler.
    handler_idx = js.index("act === 'download-tdb-backup'")
    handler = js[handler_idx:handler_idx + 4000]
    assert "res.enqueued_sections === 0" in handler, (
        "v1.15.142: JS click handler must check "
        "res.enqueued_sections === 0 and alert. Otherwise any "
        "future edge case that still hits 0 (e.g. section just "
        "excluded mid-click) silently no-ops as before."
    )
    assert "enqueued nothing" in handler, (
        "v1.15.142: JS alert must convey the 'nothing enqueued' "
        "meaning to the user clearly."
    )
