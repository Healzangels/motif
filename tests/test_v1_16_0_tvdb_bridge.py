"""v1.16.0 — TVDB bridge: TVDB→TMDB linkage backfill.

the user's v1.15.143 diagnostic surfaced 2054 stranded rows in his
library — Plex-themed TV shows + movies where motif's themes table
has no theme_id link because Plex's TV Series agent (and HAMA for
anime) gave only TVDB GUIDs while motif's TDB sync produces
TMDB-keyed records.

v1.16.0 closes that gap via the TMDB `/find/{tvdb_id}?external_
source=tvdb_id` endpoint. For each stranded plex_items row,
lookup the TMDB ID via TVDB, then link to motif's existing themes
record if one exists.

## Components

  1. `app/core/tmdb.py:lookup_by_tvdb()` — new method
  2. `app/core/plex_enum.py:bridge_tvdb_to_tmdb()` — backfill loop
  3. `POST /api/admin/tvdb-bridge/rebuild` — endpoint + background
     runner with op_progress lifecycle
  4. // TVDB BRIDGE UI in settings.html + JS wiring
  5. Auto-incremental bridge in `resolve_theme_ids` (gated on
     `last_tvdb_bridge_at` timestamp, bounded by max_rows=100)

## Tests
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import init_db
from app.core.events import now_iso
from app.core.plex_enum import bridge_tvdb_to_tmdb
from app.core.tmdb import TMDBClient


# ── 1. tmdb.lookup_by_tvdb ────────────────────────────────────────

def test_lookup_by_tvdb_disabled_returns_none(tmp_path):
    """No API key → return None without hitting the network."""
    db = tmp_path / "motif.db"
    init_db(db)
    client = TMDBClient(api_key=None, db_path=db)
    assert client.lookup_by_tvdb(12345, "tv") is None
    assert client.lookup_by_tvdb(12345, "movie") is None


def test_lookup_by_tvdb_invalid_media_type_returns_none(tmp_path):
    """media_type must be 'tv' or 'movie'. Anything else short-
    circuits without an API call — TMDB /find has movie_results +
    tv_results, no support for arbitrary kinds."""
    db = tmp_path / "motif.db"
    init_db(db)
    client = TMDBClient(api_key="testkey", db_path=db)
    assert client.lookup_by_tvdb(12345, "anime") is None
    assert client.lookup_by_tvdb(12345, "") is None


def test_lookup_by_tvdb_tv_extracts_tmdb_id(tmp_path):
    """Happy path: TMDB returns a tv_results array with the
    matching show, we extract its tmdb_id."""
    db = tmp_path / "motif.db"
    init_db(db)
    client = TMDBClient(api_key="testkey", db_path=db)

    fake_response = MagicMock(status_code=200)
    fake_response.json.return_value = {
        "movie_results": [],
        "tv_results": [{
            "id": 999888,
            "name": "Dark Gathering",
            "first_air_date": "2023-07-09",
        }],
        "person_results": [], "tv_episode_results": [],
        "tv_season_results": [],
    }
    with patch("app.core.tmdb.httpx.get",
               return_value=fake_response) as mock_get:
        result = client.lookup_by_tvdb(456789, "tv")

    assert result is not None
    assert result["tmdb_id"] == 999888
    assert result["tvdb_id"] == 456789
    assert result["kind"] == "tv"
    # The /find call must use external_source=tvdb_id (not imdb_id).
    call_args = mock_get.call_args
    assert "find/456789" in call_args[0][0]
    assert call_args[1]["params"]["external_source"] == "tvdb_id"


def test_lookup_by_tvdb_movie_filters_by_media_type(tmp_path):
    """If TMDB /find returns both movie + tv results for a TVDB
    ID, the lookup must pick the requested kind. A movie request
    must NOT return a tv result."""
    db = tmp_path / "motif.db"
    init_db(db)
    client = TMDBClient(api_key="testkey", db_path=db)
    fake_response = MagicMock(status_code=200)
    fake_response.json.return_value = {
        "movie_results": [{
            "id": 12345,
            "title": "Some Movie",
            "release_date": "2020-01-01",
        }],
        "tv_results": [{  # also present, should be ignored
            "id": 67890,
            "name": "Same Title TV Show",
            "first_air_date": "2019-01-01",
        }],
        "person_results": [], "tv_episode_results": [],
        "tv_season_results": [],
    }
    with patch("app.core.tmdb.httpx.get",
               return_value=fake_response):
        result = client.lookup_by_tvdb(999, "movie")
    assert result["tmdb_id"] == 12345
    assert result["kind"] == "movie"


def test_lookup_by_tvdb_empty_results_returns_none(tmp_path):
    """TMDB has no mapping for this TVDB ID → return None.
    Caller treats this as 'unmappable' and skips."""
    db = tmp_path / "motif.db"
    init_db(db)
    client = TMDBClient(api_key="testkey", db_path=db)
    fake_response = MagicMock(status_code=200)
    fake_response.json.return_value = {
        "movie_results": [], "tv_results": [],
        "person_results": [], "tv_episode_results": [],
        "tv_season_results": [],
    }
    with patch("app.core.tmdb.httpx.get",
               return_value=fake_response):
        assert client.lookup_by_tvdb(99999, "tv") is None


def test_lookup_by_tvdb_caches_negative_results(tmp_path):
    """Second call for the same TVDB ID must hit the cache, not
    the network. Negative results are cached too — 7-day TTL —
    so impossible-lookup rows don't repeatedly hammer the API."""
    db = tmp_path / "motif.db"
    init_db(db)
    client = TMDBClient(api_key="testkey", db_path=db)
    fake_response = MagicMock(status_code=200)
    fake_response.json.return_value = {
        "movie_results": [], "tv_results": [],
        "person_results": [], "tv_episode_results": [],
        "tv_season_results": [],
    }
    with patch("app.core.tmdb.httpx.get",
               return_value=fake_response) as mock_get:
        client.lookup_by_tvdb(11111, "tv")  # miss → fetch
        client.lookup_by_tvdb(11111, "tv")  # hit → cached
    assert mock_get.call_count == 1, (
        "v1.16.0: negative results must be cached. Second call "
        f"should hit the cache; got {mock_get.call_count} API "
        "requests."
    )


def test_lookup_by_tvdb_cache_key_distinguishes_media_type(tmp_path):
    """Same TVDB ID + different media_type must be cached
    separately. A movie cached as 'no match' shouldn't suppress
    a tv lookup for the same numeric ID (TVDB namespaces tv/
    movie IDs separately but the cache key needs to reflect
    that)."""
    db = tmp_path / "motif.db"
    init_db(db)
    client = TMDBClient(api_key="testkey", db_path=db)
    fake_response = MagicMock(status_code=200)
    fake_response.json.return_value = {
        "movie_results": [], "tv_results": [],
        "person_results": [], "tv_episode_results": [],
        "tv_season_results": [],
    }
    with patch("app.core.tmdb.httpx.get",
               return_value=fake_response) as mock_get:
        client.lookup_by_tvdb(12345, "tv")
        client.lookup_by_tvdb(12345, "movie")
    assert mock_get.call_count == 2, (
        "v1.16.0: cache key must include media_type so tv + "
        "movie lookups don't collide."
    )


# ── 2. bridge_tvdb_to_tmdb ────────────────────────────────────────

def _seed_baseline_for_bridge(conn: sqlite3.Connection) -> None:
    """Common fixture: included section, themes row with id=1,
    plex_items row stranded (theme_id NULL, guid_tvdb set)."""
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
        "        'https://youtube.com/watch?v=a1234567890', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
        "  year, has_theme, local_theme_file, folder_path, "
        "  theme_id, first_seen_at, last_seen_at) "
        "VALUES ('rk-strand', '3', 'show', NULL, 456789, "
        "        'Dark Gathering', 'dark gathering', '2023', "
        "        1, 0, '/data/anime/DG', NULL, ?, ?)",
        (now, now),
    )


def test_bridge_links_row_when_tmdb_returns_known_themes(tmp_path):
    """Happy path: TMDB lookup resolves TVDB→TMDB, motif's themes
    has a row for that TMDB ID, plex_items.theme_id gets stamped."""
    db = tmp_path / "motif.db"
    init_db(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    _seed_baseline_for_bridge(conn)
    conn.commit()
    conn.close()

    mock_tmdb = MagicMock()
    mock_tmdb.enabled = True
    mock_tmdb.lookup_by_tvdb.return_value = {
        "tmdb_id": 999888, "tvdb_id": 456789, "kind": "tv",
    }
    result = bridge_tvdb_to_tmdb(db, mock_tmdb)

    assert result["processed"] == 1
    assert result["linked"] == 1
    assert result["unmappable"] == 0
    assert result["no_themes_record"] == 0
    assert result["errors"] == 0
    # Verify the linkage actually wrote to the DB.
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT theme_id FROM plex_items WHERE rating_key='rk-strand'"
    ).fetchone()
    assert row["theme_id"] == 1, (
        "v1.16.0: bridge must UPDATE plex_items SET theme_id when "
        "TMDB returns a tmdb_id matching an existing themes row."
    )


def test_bridge_skips_when_tmdb_returns_none(tmp_path):
    """TMDB has no mapping → unmappable++ → no UPDATE."""
    db = tmp_path / "motif.db"
    init_db(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    _seed_baseline_for_bridge(conn)
    conn.commit()
    conn.close()

    mock_tmdb = MagicMock()
    mock_tmdb.enabled = True
    mock_tmdb.lookup_by_tvdb.return_value = None  # no mapping
    result = bridge_tvdb_to_tmdb(db, mock_tmdb)

    assert result["processed"] == 1
    assert result["linked"] == 0
    assert result["unmappable"] == 1
    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT theme_id FROM plex_items WHERE rating_key='rk-strand'"
    ).fetchone()
    assert row[0] is None, "must not link when TMDB has no mapping"


def test_bridge_no_themes_record_increments_when_tmdb_match_but_themes_empty(tmp_path):
    """TMDB resolves to a tmdb_id, but motif's themes table
    doesn't have that record (TDB doesn't track this title).
    no_themes_record++; no UPDATE."""
    db = tmp_path / "motif.db"
    init_db(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    _seed_baseline_for_bridge(conn)
    conn.commit()
    conn.close()

    mock_tmdb = MagicMock()
    mock_tmdb.enabled = True
    # TMDB resolves to a tmdb_id that motif's themes table
    # doesn't have a record for (themes has only 999888).
    mock_tmdb.lookup_by_tvdb.return_value = {
        "tmdb_id": 7777777, "tvdb_id": 456789, "kind": "tv",
    }
    result = bridge_tvdb_to_tmdb(db, mock_tmdb)

    assert result["processed"] == 1
    assert result["linked"] == 0
    assert result["unmappable"] == 0
    assert result["no_themes_record"] == 1
    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT theme_id FROM plex_items WHERE rating_key='rk-strand'"
    ).fetchone()
    assert row[0] is None


def test_bridge_disabled_client_returns_zero_counts(tmp_path):
    """No TMDB key → no work, no exception, return all zeros."""
    db = tmp_path / "motif.db"
    init_db(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    _seed_baseline_for_bridge(conn)
    conn.commit()
    conn.close()

    mock_tmdb = MagicMock()
    mock_tmdb.enabled = False
    result = bridge_tvdb_to_tmdb(db, mock_tmdb)
    assert result == {
        "processed": 0, "linked": 0, "unmappable": 0,
        "no_themes_record": 0, "errors": 0,
    }
    # The lookup must NOT have been called.
    mock_tmdb.lookup_by_tvdb.assert_not_called()


def test_bridge_skips_excluded_sections(tmp_path):
    """Rows in excluded sections (ps.included=0) must not be
    bridged — user opted out of that library."""
    db = tmp_path / "motif.db"
    init_db(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    _seed_baseline_for_bridge(conn)
    conn.execute(
        "UPDATE plex_sections SET included=0 WHERE section_id='3'")
    conn.commit()
    conn.close()

    mock_tmdb = MagicMock()
    mock_tmdb.enabled = True
    mock_tmdb.lookup_by_tvdb.return_value = {"tmdb_id": 999888}
    result = bridge_tvdb_to_tmdb(db, mock_tmdb)
    assert result["processed"] == 0
    mock_tmdb.lookup_by_tvdb.assert_not_called()


def test_bridge_skips_rows_without_has_theme(tmp_path):
    """has_theme=0 rows aren't 'stranded' from motif's POV —
    Plex doesn't claim a theme. Skip them."""
    db = tmp_path / "motif.db"
    init_db(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    _seed_baseline_for_bridge(conn)
    conn.execute(
        "UPDATE plex_items SET has_theme=0 WHERE rating_key='rk-strand'")
    conn.commit()
    conn.close()

    mock_tmdb = MagicMock()
    mock_tmdb.enabled = True
    result = bridge_tvdb_to_tmdb(db, mock_tmdb)
    assert result["processed"] == 0
    mock_tmdb.lookup_by_tvdb.assert_not_called()


def test_bridge_respects_since_iso_for_incremental(tmp_path):
    """The auto-incremental path passes since_iso. Rows whose
    first_seen_at predates the timestamp must be filtered out."""
    db = tmp_path / "motif.db"
    init_db(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    _seed_baseline_for_bridge(conn)
    conn.commit()
    conn.close()

    mock_tmdb = MagicMock()
    mock_tmdb.enabled = True
    # since_iso in the future → row first_seen_at < since_iso →
    # filtered out.
    result = bridge_tvdb_to_tmdb(
        db, mock_tmdb, since_iso="9999-01-01T00:00:00")
    assert result["processed"] == 0
    mock_tmdb.lookup_by_tvdb.assert_not_called()


def test_bridge_respects_max_rows(tmp_path):
    """The auto-incremental path caps max_rows. With 3 stranded
    rows + max_rows=2, only 2 are processed."""
    db = tmp_path / "motif.db"
    init_db(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    _seed_baseline_for_bridge(conn)
    now = now_iso()
    for i in range(2):
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
            "  year, has_theme, local_theme_file, folder_path, "
            "  theme_id, first_seen_at, last_seen_at) "
            "VALUES (?, '3', 'show', NULL, ?, ?, ?, '2024', "
            "        1, 0, '/data/anime/X', NULL, ?, ?)",
            (f"rk-extra-{i}", 10000 + i,
             f"Extra {i}", f"extra {i}", now, now),
        )
    conn.commit()
    conn.close()

    mock_tmdb = MagicMock()
    mock_tmdb.enabled = True
    mock_tmdb.lookup_by_tvdb.return_value = None  # unmappable
    result = bridge_tvdb_to_tmdb(db, mock_tmdb, max_rows=2)
    assert result["processed"] == 2, (
        "v1.16.0: max_rows must cap the candidate set BEFORE the "
        "lookup loop. With 3 candidates + max_rows=2, expect 2."
    )


def test_bridge_movie_media_type_conversion(tmp_path):
    """plex_items.media_type='movie' must call lookup_by_tvdb
    with motif format 'movie' (not 'show')."""
    db = tmp_path / "motif.db"
    init_db(db)
    conn = sqlite3.connect(db)
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
        "                    title_norm, year, upstream_source, "
        "                    youtube_url, first_seen_sync_at, "
        "                    last_seen_sync_at) "
        "VALUES (5, 'movie', 555, 'Some Movie', 'some movie', "
        "        2020, 'themoviedb', "
        "        'https://youtube.com/watch?v=a1234567890', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
        "  year, has_theme, local_theme_file, folder_path, "
        "  theme_id, first_seen_at, last_seen_at) "
        "VALUES ('rk-mov', '1', 'movie', NULL, 7777, "
        "        'Some Movie', 'some movie', '2020', "
        "        1, 0, '/data/movies/X', NULL, ?, ?)",
        (now, now),
    )
    conn.commit()
    conn.close()

    mock_tmdb = MagicMock()
    mock_tmdb.enabled = True
    mock_tmdb.lookup_by_tvdb.return_value = {"tmdb_id": 555}
    bridge_tvdb_to_tmdb(db, mock_tmdb)
    # Verify the call used 'movie', not 'show'.
    mock_tmdb.lookup_by_tvdb.assert_called_once_with(7777, "movie")


# ── 3. Endpoint structure ─────────────────────────────────────────

def test_rebuild_endpoint_defined_in_api_py():
    """The /api/admin/tvdb-bridge/rebuild endpoint must exist."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert '/api/admin/tvdb-bridge/rebuild' in api, (
        "v1.16.0: TVDB-bridge rebuild endpoint missing."
    )
    assert "_tvdb_bridge_run" in api, (
        "v1.16.0: background runner function missing."
    )


def test_rebuild_endpoint_uses_try_acquire():
    """The endpoint must atomically claim the tvdb-bridge op_id
    so double-clicks can't spawn two concurrent passes."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    rebuild_idx = api.index('"/api/admin/tvdb-bridge/rebuild"')
    handler = api[rebuild_idx:rebuild_idx + 2500]
    assert "try_acquire" in handler
    assert "tvdb-bridge" in handler
    # v1.16.2: user-facing error message renamed HAMA → TVDB.
    assert "TVDB bridge already running" in handler


def test_rebuild_endpoint_rejects_when_no_tmdb_key():
    """Without a TMDB key, the endpoint must 409 with a clear
    message pointing at the settings field. Otherwise the user
    clicks the button and gets a silent failure."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    rebuild_idx = api.index('"/api/admin/tvdb-bridge/rebuild"')
    handler = api[rebuild_idx:rebuild_idx + 2500]
    assert "settings.tmdb_api_key" in handler
    assert "status_code=409" in handler


def test_diagnostic_endpoint_surfaces_last_run_and_config_state():
    """The v1.15.143 diagnostic now exposes last_bridge_run_at +
    tmdb_configured so the UI can render 'last rebuild: X ago'
    and disable the button when no API key is set."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    diag_idx = api.index('"/api/admin/diagnostics/tvdb-gap"')
    # The endpoint body is large (3 buckets × 2 media types +
    # verdict tier composition + v1.16.0 last-run/tmdb_configured
    # extension). 10000 chars covers it.
    block = api[diag_idx:diag_idx + 10000]
    assert "last_tvdb_bridge_at" in block
    assert "tmdb_configured" in block


def test_run_records_last_bridge_run_timestamp():
    """The background runner must stamp last_tvdb_bridge_at in
    runtime_settings on completion — that's the gate for the
    auto-incremental path."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert "set_runtime_text" in api
    runner_idx = api.index("def _tvdb_bridge_run")
    block = api[runner_idx:runner_idx + 3500]
    assert "last_tvdb_bridge_at" in block
    assert "set_runtime_text" in block


# ── 4. Settings UI ────────────────────────────────────────────────

def test_settings_html_has_tvdb_bridge_section():
    """v1.16.2: HTML ids + data-attrs renamed from 'tvdb-bridge'
    to 'tvdb-bridge' because the user-facing label changed. The
    op_id ("tvdb-bridge") + schema kind ("tvdb_bridge") stay as
    internal identifiers — those would require a migration."""
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'id="tvdb-bridge-section"' in html
    assert '// TVDB BRIDGE' in html
    assert 'id="tvdb-bridge-rebuild-btn"' in html
    assert 'data-tvdb-stranded' in html
    assert 'data-tvdb-last-run' in html


def test_app_js_wires_tvdb_bridge_button():
    """v1.16.2: JS function names renamed Tvdb*, but the op_id
    string the watcher filters for is still 'tvdb-bridge' (the
    internal identifier wasn't renamed)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "hydrateTvdbBridgeStats" in js
    assert "watchTvdbBridgeCompletion" in js
    assert "/api/admin/tvdb-bridge/rebuild" in js, (
        "v1.16.2: endpoint URL stays /api/admin/tvdb-bridge/"
        "rebuild for stability — only user-facing labels were "
        "renamed."
    )
    watcher_idx = js.index("async function watchTvdbBridgeCompletion(")
    watcher = js[watcher_idx:watcher_idx + 3500]
    assert "'tvdb-bridge'" in watcher, (
        "v1.16.0 op_id stays 'tvdb-bridge' — watcher must filter "
        "/api/progress rows by that key. Internal ID stable, UI "
        "label changed."
    )
    assert "sawRunning" in watcher, (
        "v1.16.0: same gate as bulk-probe-tdb watcher — avoid "
        "latching onto a stale finished-row from a prior run."
    )


def test_app_js_summary_format_includes_linked_and_unmappable():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_idx = js.index("function formatTvdbBridgeSummary(")
    body = js[fn_idx:fn_idx + 1500]
    assert "LINKED" in body
    assert "UNMAPPABLE" in body
    assert "NO TDB" in body
    assert "BRIDGE FAILED" in body
    assert "BRIDGE CANCELLED" in body


def test_app_js_parse_activity_matches_runner_string_format():
    """Cross-language contract: the JS parser regex must match
    the exact terminal activity string the Python runner emits.
    Tests the bridge between _tvdb_bridge_run's activity-string
    template and parseTvdbBridgeActivity's regex."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    # The runner composes: f"linked={...}, unmappable={...},
    # no_record={...}, errors={...}"
    assert (
        'f"linked={result[\'linked\']}, "'
        in api or "linked={result['linked']}" in api
    ), "runner must format the terminal activity in the agreed shape"
    # And the JS parser must read it back.
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    parse_idx = js.index("function parseTvdbBridgeActivity(")
    parse_body = js[parse_idx:parse_idx + 800]
    assert "linked=" in parse_body
    assert "unmappable=" in parse_body
    assert "no_record=" in parse_body
    assert "errors=" in parse_body


def test_ops_js_kind_label_renders_tvdb_bridge():
    """v1.16.2: the LIVE OPS drawer card was rendering the raw
    op kind ('// tvdb_bridge') because KIND_LABEL had no entry.
    Pin that the LABEL says TVDB BRIDGE (matching the settings
    page heading) — without it the user sees inconsistent
    branding between /settings and the drawer."""
    ops_js = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    label_idx = ops_js.index("const KIND_LABEL")
    label_block = ops_js[label_idx:label_idx + 2000]
    assert "tvdb_bridge:" in label_block
    assert "'TVDB BRIDGE'" in label_block, (
        "v1.16.2: ops.js KIND_LABEL must map tvdb_bridge → "
        "'TVDB BRIDGE' so the drawer card matches the settings "
        "page heading."
    )


# ── 5. Auto-incremental gate ──────────────────────────────────────

def test_resolve_theme_ids_skips_auto_bridge_without_last_run_timestamp():
    """The auto-incremental path is gated on `last_tvdb_bridge_at`
    being set in runtime_settings. Fresh installs with TMDB key
    set but never manually rebuilt should NOT trigger 2000+ API
    calls in the middle of plex_enum.

    Structural test — verify the gate exists in the resolve_
    theme_ids source. Logical test would require a heavy
    integration setup; the gate is a single boolean check on the
    timestamp."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    # Find the resolve_theme_ids_impl function body.
    impl_start = src.index("def _resolve_theme_ids_impl(")
    # v1.18.16: widen the slice — sql_tmdb's COALESCE rationale
    # comment pushed the gate past the prior 12000 boundary.
    impl_block = src[impl_start:impl_start + 24000]
    assert "last_tvdb_bridge_at" in impl_block, (
        "v1.16.0: auto-incremental bridge gate is missing — "
        "without checking last_tvdb_bridge_at, plex_enum would "
        "auto-trigger a 2000+ row backfill on every pass."
    )
    # The gate must guard against an empty/missing timestamp.
    # Look for `if last_bridge_run_at:` pattern.
    gate_idx = impl_block.index("last_tvdb_bridge_at")
    gate_block = impl_block[gate_idx:gate_idx + 800]
    assert "if last_bridge_run_at:" in gate_block, (
        "v1.16.0: gate must skip the auto-incremental bridge "
        "when no manual rebuild has run."
    )


def test_resolve_theme_ids_auto_bridge_uses_max_rows_cap():
    """The auto-incremental path must pass `max_rows` to keep
    plex_enum's wall-clock bounded. 100 is the chosen cap."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    impl_start = src.index("def _resolve_theme_ids_impl(")
    # v1.18.16: widen the slice — sql_tmdb's COALESCE rationale
    # comment pushed the gate past the prior 12000 boundary.
    impl_block = src[impl_start:impl_start + 24000]
    assert "max_rows=100" in impl_block, (
        "v1.16.0: auto-incremental call must pass max_rows=100 "
        "so plex_enum's per-pass cost stays bounded."
    )


def test_resolve_theme_ids_auto_bridge_swallows_failures():
    """If TMDB API is unreachable / key rotated / rate-limited
    out, the auto-incremental bridge must NOT take down
    plex_enum. The whole block is wrapped in try/except."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    impl_start = src.index("def _resolve_theme_ids_impl(")
    # v1.18.2 widened the window: a fourth collection-specific
    # SQL pass (`sql_collection_title`) was inserted before the
    # auto-bridge block, pushing the bridge past the prior 12000-char
    # ceiling.
    # v1.22.47: 16000→18000 — the new sql_imdb_real pass (real-theme imdb
    # match) added another SQL block ahead of the bridge.
    # v1.24.25: 18000→20000 — the sql_title_orphan re-bond pass added another
    # SQL block ahead of the bridge.
    # v1.24.27: 20000→22000 — sql_title_orphan_yearless (the year-less orphan
    # fallback) added one more SQL block ahead of the bridge.
    # v1.24.30: 22000→24000 — the title_norm backfill pre-pass landed at the
    # top of the impl, ahead of every SQL block + the bridge.
    impl_block = src[impl_start:impl_start + 24000]
    # The auto-bridge block ends with except handling.
    # v1.18.55: log line renamed HAMA → TVDB so docker logs
    # match the UI's // TVDB BRIDGE label. The internal kind
    # stays 'tvdb_bridge' for schema continuity; only the
    # user-visible message text changed.
    assert "auto-incremental TVDB bridge failed" in impl_block, (
        "v1.16.0 (text refreshed v1.18.55): best-effort logging "
        "on bridge failure inside resolve_theme_ids — plex_enum "
        "must still complete."
    )
