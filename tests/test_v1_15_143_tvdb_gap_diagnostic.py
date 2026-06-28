"""v1.15.143 — TVDB-gap diagnostic endpoint.

the user's question after v1.15.142:

> Are we cutting any corners by doing it this way or should we
> try and integrate with anime? open to discuss.

The discussion outcome: v1.15.142 fixed an internal-consistency
bug (the enqueue path's strict guid_tmdb filter disagreed with
every read path's theme_id linkage). native anime-agent integration is a
separate question about COVERAGE — how many themed-by-Plex rows
sit unlinked to motif because Plex's anime agent gives only
TVDB/AniDB GUIDs and motif's themes table only knows IMDB/TMDB.

Before committing engineering effort to native anime-agent integration, count
the stranded rows. This endpoint exposes the numbers.

## Endpoint — `GET /api/admin/diagnostics/tvdb-gap`

Returns three buckets per media_type:

  - `fallback_matched` — guid_tmdb=NULL but theme_id set
    (v1.15.142's recovered rows: linked via plex_enum's title/
    imdb fallback)
  - `unmatched_with_tvdb` — themed-by-Plex, no theme_id, but
    guid_tvdb present (Option A — themes.tvdb_id backfill
    via TMDB API — would catch these)
  - `unmatched_no_tvdb` — themed-by-Plex, no theme_id, no
    guid_tvdb (Option B — AniDB→TMDB mapping — or Option C
    — manual SET URL — territory)

Each bucket includes a sample of up to 10 titles so the operator
can sanity-check the counts against actual library content.

Summary at the top gives a quick read-at-a-glance verdict:
  - 0 stranded → defer indefinitely
  - <10 → manual override per row is cheaper than building
  - <50 → borderline; pick A or B based on bucket dominance
  - 50+ → significant; worth a v1.16.x project

Admin-gated via `_require_admin(request)` — read-only but
sensitive (exposes library titles).

## Tests
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso
from app.web.api import create_app


def _seed_baseline(conn: sqlite3.Connection) -> None:
    """One included anime section, one included movie section,
    and one TDB-tracked themes row per media_type. Each test
    layers per-bucket plex_items rows on top."""
    now = now_iso()
    for sid, title, plex_type, anime in (
        ("3", "Anime", "show", 1),
        ("1", "Movies", "movie", 0),
    ):
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  included, is_anime, is_4k, themes_subdir, "
            "  discovered_at, last_seen_at) "
            "VALUES (?, ?, ?, 1, ?, 0, ?, ?, ?)",
            (sid, title, plex_type, anime,
             title.lower(), now, now),
        )
    # Two themes rows so fallback_matched + direct match can
    # coexist in the same fixture without collision.
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, "
        "                    title_norm, year, "
        "                    upstream_source, youtube_url, "
        "                    first_seen_sync_at, last_seen_sync_at) "
        "VALUES "
        " (1, 'tv', 100, 'Anime A', 'anime a', 2023, 'themoviedb', "
        "  'https://youtube.com/watch?v=a1234567890', ?, ?), "
        " (2, 'movie', 200, 'Movie X', 'movie x', 2020, 'themoviedb', "
        "  'https://youtube.com/watch?v=m1234567890', ?, ?)",
        (now, now, now, now),
    )


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Build a fresh app + DB, set up admin auth, return a
    TestClient pre-armed with the forward-auth header so
    _require_admin passes."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    app = create_app(settings)
    c = TestClient(app)
    c.headers["X-Authentik-Username"] = "testadmin"
    c.db = db  # type: ignore[attr-defined]
    yield c


def _conn(db: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    return conn


# ── endpoint exists + auth-gated ──────────────────────────────────

def test_endpoint_exists_and_requires_admin(tmp_path, monkeypatch):
    """Without the forward-auth header, _require_admin should
    reject the request."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    app = create_app(settings)
    c = TestClient(app)
    r = c.get("/api/admin/diagnostics/tvdb-gap")
    # The auth middleware redirects unauthenticated browser-style
    # GETs to the login page rather than returning 401, so check
    # the response body — it must NOT be the diagnostic JSON.
    # A logged-out caller could get either a 401/403 OR an HTML
    # login page back; what matters is they don't see the
    # library data.
    content_type = r.headers.get("content-type", "")
    if r.status_code == 200:
        assert "application/json" not in content_type, (
            "v1.15.143: diagnostic returned JSON to an "
            "unauthenticated caller — admin gate is broken."
        )
        # HTML login page is acceptable for browser flow.
        assert "summary" not in r.text or "library" not in r.text, (
            "v1.15.143: response body contains diagnostic data "
            "without auth — admin gate is broken."
        )
    else:
        assert r.status_code in (401, 403, 302, 307), (
            f"Expected auth-rejection status, got {r.status_code}"
        )


# ── empty library → zeros + 'defer indefinitely' verdict ──────────

def test_empty_library_returns_zero_counts_and_defer_verdict(client):
    r = client.get("/api/admin/diagnostics/tvdb-gap")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["summary"]["total_stranded"] == 0
    assert body["summary"]["total_v1_15_142_recovered"] == 0
    assert "defer indefinitely" in body["summary"]["verdict"].lower()


# ── primary buckets are computed correctly ────────────────────────

def test_fallback_matched_bucket_counts_v1_15_142_beneficiaries(client):
    """anime-agent-style row: guid_tmdb=NULL but theme_id IS set via
    title fallback. This is the bucket v1.15.142 fixed."""
    conn = _conn(client.db)
    _seed_baseline(conn)
    now = now_iso()
    # One TV row with guid_tmdb=NULL, guid_tvdb=12345, theme_id=1
    # — represents Dark Gathering's exact shape.
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
        "  year, has_theme, local_theme_file, folder_path, "
        "  theme_id, first_seen_at, last_seen_at) "
        "VALUES ('rk-anime', '3', 'show', NULL, 12345, "
        "        'Anime A', 'anime a', '2023', "
        "        1, 0, '/data/anime/A', 1, ?, ?)",
        (now, now),
    )
    conn.commit()
    conn.close()
    r = client.get("/api/admin/diagnostics/tvdb-gap")
    body = r.json()
    assert body["tv"]["fallback_matched"]["count"] == 1
    assert body["tv"]["unmatched_with_tvdb"]["count"] == 0
    assert body["tv"]["unmatched_no_tvdb"]["count"] == 0
    sample = body["tv"]["fallback_matched"]["sample"]
    assert len(sample) == 1
    assert sample[0]["title"] == "Anime A"


def test_unmatched_with_tvdb_bucket_counts_option_a_candidates(client):
    """Themed-by-Plex row with guid_tvdb set but no theme_id —
    these would be caught by a future themes.tvdb_id backfill."""
    conn = _conn(client.db)
    _seed_baseline(conn)
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
        "  year, has_theme, local_theme_file, folder_path, "
        "  theme_id, first_seen_at, last_seen_at) "
        "VALUES ('rk-stranded', '3', 'show', NULL, 67890, "
        "        'Some Unmatched Anime', 'some unmatched anime', "
        "        '2022', 1, 0, '/data/anime/Stranded', NULL, ?, ?)",
        (now, now),
    )
    conn.commit()
    conn.close()
    r = client.get("/api/admin/diagnostics/tvdb-gap")
    body = r.json()
    assert body["tv"]["unmatched_with_tvdb"]["count"] == 1
    assert body["tv"]["unmatched_no_tvdb"]["count"] == 0
    assert body["tv"]["fallback_matched"]["count"] == 0


def test_unmatched_no_tvdb_bucket_counts_pure_anidb_rows(client):
    """Themed row, no theme_id, no guid_tvdb either. Pure AniDB
    case — only Option B (anime-lists mapping) or Option C
    (manual override) would help."""
    conn = _conn(client.db)
    _seed_baseline(conn)
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
        "  year, has_theme, local_theme_file, folder_path, "
        "  theme_id, first_seen_at, last_seen_at) "
        "VALUES ('rk-anidb', '3', 'show', NULL, NULL, "
        "        'AniDB-Only Anime', 'anidb-only anime', "
        "        '2021', 1, 0, '/data/anime/AniDB', NULL, ?, ?)",
        (now, now),
    )
    conn.commit()
    conn.close()
    r = client.get("/api/admin/diagnostics/tvdb-gap")
    body = r.json()
    assert body["tv"]["unmatched_no_tvdb"]["count"] == 1
    assert body["tv"]["unmatched_with_tvdb"]["count"] == 0


# ── exclusion logic ───────────────────────────────────────────────

def test_excluded_section_rows_dont_count(client):
    """plex_sections.included=0 rows must NOT appear in any
    bucket — user opted out of that library."""
    conn = _conn(client.db)
    _seed_baseline(conn)
    # Flip the anime section to excluded.
    conn.execute("UPDATE plex_sections SET included=0 WHERE section_id='3'")
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
        "  year, has_theme, local_theme_file, folder_path, "
        "  theme_id, first_seen_at, last_seen_at) "
        "VALUES ('rk-x', '3', 'show', NULL, 99999, "
        "        'Excluded Show', 'excluded show', '2024', "
        "        1, 0, '/data/anime/X', NULL, ?, ?)",
        (now, now),
    )
    conn.commit()
    conn.close()
    r = client.get("/api/admin/diagnostics/tvdb-gap")
    body = r.json()
    assert body["tv"]["fallback_matched"]["count"] == 0
    assert body["tv"]["unmatched_with_tvdb"]["count"] == 0
    assert body["tv"]["unmatched_no_tvdb"]["count"] == 0


def test_unthemed_rows_dont_count_as_stranded(client):
    """A plex_items row with has_theme=0 isn't 'stranded' — Plex
    doesn't claim a theme for it, so motif missing a linkage
    isn't a coverage gap."""
    conn = _conn(client.db)
    _seed_baseline(conn)
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
        "  year, has_theme, local_theme_file, folder_path, "
        "  theme_id, first_seen_at, last_seen_at) "
        "VALUES ('rk-unthemed', '3', 'show', NULL, 11111, "
        "        'Unthemed', 'unthemed', '2024', "
        "        0, 0, '/data/anime/U', NULL, ?, ?)",  # has_theme=0
        (now, now),
    )
    conn.commit()
    conn.close()
    r = client.get("/api/admin/diagnostics/tvdb-gap")
    body = r.json()
    assert body["tv"]["unmatched_with_tvdb"]["count"] == 0
    assert body["tv"]["unmatched_no_tvdb"]["count"] == 0


# ── verdict tiers fire correctly ──────────────────────────────────

def test_verdict_tier_changes_with_stranded_count(client):
    """Single stranded row → 'manual override is cheaper'.
    More than ~50 → 'worth a v1.16.x project'. Tier boundaries
    are the v1.15.143 contract."""
    conn = _conn(client.db)
    _seed_baseline(conn)
    now = now_iso()
    # Seed 60 stranded TV rows (well above the 50-row threshold)
    # to exercise the 'significant' verdict tier.
    for i in range(60):
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
            "  year, has_theme, local_theme_file, folder_path, "
            "  theme_id, first_seen_at, last_seen_at) "
            "VALUES (?, '3', 'show', NULL, ?, "
            "        ?, ?, '2024', "
            "        1, 0, '/data/anime/many', NULL, ?, ?)",
            (f"rk-many-{i}", 10000 + i,
             f"Stranded {i:02d}", f"stranded {i:02d}",
             now, now),
        )
    conn.commit()
    conn.close()
    r = client.get("/api/admin/diagnostics/tvdb-gap")
    body = r.json()
    assert body["summary"]["total_stranded"] >= 50
    assert "significant" in body["summary"]["verdict"].lower(), (
        "60 stranded rows should trigger the 'significant — "
        "worth a v1.16.x project' tier."
    )


# ── sample lists are bounded ──────────────────────────────────────

def test_samples_capped_at_ten_per_bucket(client):
    """Even with 60 stranded rows, the sample list per bucket
    must cap at 10 so the JSON response stays bounded."""
    conn = _conn(client.db)
    _seed_baseline(conn)
    now = now_iso()
    for i in range(60):
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
            "  year, has_theme, local_theme_file, folder_path, "
            "  theme_id, first_seen_at, last_seen_at) "
            "VALUES (?, '3', 'show', NULL, ?, ?, ?, '2024', "
            "        1, 0, '/data/anime/many', NULL, ?, ?)",
            (f"rk-many-{i}", 10000 + i,
             f"Cap Sample {i:02d}", f"cap sample {i:02d}",
             now, now),
        )
    conn.commit()
    conn.close()
    r = client.get("/api/admin/diagnostics/tvdb-gap")
    body = r.json()
    assert len(body["tv"]["unmatched_with_tvdb"]["sample"]) == 10


# ── summary includes the v1.15.142 recovery count ─────────────────

def test_summary_total_v1_15_142_recovered_reflects_fallback_bucket(client):
    """The summary's recovered count rolls up the fallback_matched
    buckets across both media types so the user gets a
    one-number answer to 'how many rows did v1.15.142 unblock?'."""
    conn = _conn(client.db)
    _seed_baseline(conn)
    now = now_iso()
    # 3 TV rows + 2 movie rows in the fallback_matched bucket.
    for i in range(3):
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
            "  year, has_theme, local_theme_file, folder_path, "
            "  theme_id, first_seen_at, last_seen_at) "
            "VALUES (?, '3', 'show', NULL, ?, ?, ?, '2023', "
            "        1, 0, '/data/anime/F', 1, ?, ?)",
            (f"rk-tv-{i}", 20000 + i,
             f"TV Fb {i}", f"tv fb {i}", now, now),
        )
    for i in range(2):
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "  media_type, guid_tmdb, guid_tvdb, title, title_norm, "
            "  year, has_theme, local_theme_file, folder_path, "
            "  theme_id, first_seen_at, last_seen_at) "
            "VALUES (?, '1', 'movie', NULL, ?, ?, ?, '2020', "
            "        1, 0, '/data/movies/F', 2, ?, ?)",
            (f"rk-mov-{i}", 30000 + i,
             f"Mov Fb {i}", f"mov fb {i}", now, now),
        )
    conn.commit()
    conn.close()
    r = client.get("/api/admin/diagnostics/tvdb-gap")
    body = r.json()
    assert body["summary"]["total_v1_15_142_recovered"] == 5
    assert body["tv"]["fallback_matched"]["count"] == 3
    assert body["movie"]["fallback_matched"]["count"] == 2
