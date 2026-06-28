"""v1.22.45 — sync_completed notification groups New/Updated by Plex library.

the user's ask: a sync listed a flat mix of movies + TV + anime ("Cheers, Tires,
Darkwing Duck, Space Dandy, Ben-To, 9-1-1") with no way to tell which library
each belongs to. Now the New + Updated lists render under Plex-section
sub-headers (Movies / 4K Movies / TV Shows / Anime / Anime Movies …):

    🎵 New:
    **Anime**
    * Space Dandy (2014)
    **TV Shows**
    * Cheers (1982)
    * Tires (2024)

Grouping key is the Plex section title (the user's own section names). A title
in two sections (Movies + 4K Movies) appears under both — that IS the answer to
"which library." The New-list EXISTS gate was also widened to pi.theme_id so
anime new themes (guid_tmdb NULL) are listed, not just counted.
"""
from __future__ import annotations

from pathlib import Path

from app.core.db import init_db
from app.core.notify_content import format_section_grouped_lines
from app.core.worker import _build_sync_section_buckets, _sections_for_theme

REPO = Path(__file__).resolve().parent.parent


# ── pure formatter ────────────────────────────────────────────


def test_formatter_renders_section_headers_and_bullets():
    buckets = {
        "TV Shows": ["Cheers (1982)", "Tires (2024)"],
        "Anime": ["Space Dandy (2014)"],
    }
    lines = format_section_grouped_lines("🎵 New:", buckets, total_count=3,
                                         shown_count=3)
    assert lines[0] == "🎵 New:"
    # Sections alphabetical: Anime before TV Shows; markdown bold headers.
    assert "**Anime**" in lines
    assert "**TV Shows**" in lines
    assert lines.index("**Anime**") < lines.index("**TV Shows**")
    assert "* Cheers (1982)" in lines
    assert "* Space Dandy (2014)" in lines
    assert not any("more" in ln for ln in lines)  # no overflow


def test_formatter_overflow_line():
    buckets = {"Movies": ["A (2001)", "B (2002)"]}
    lines = format_section_grouped_lines("🎵 New:", buckets, total_count=7,
                                         shown_count=2)
    assert lines[-1] == "…and 5 more"


def test_formatter_no_overflow_when_shown_ge_total():
    buckets = {"Movies": ["A (2001)"]}
    lines = format_section_grouped_lines("🔄 Updated:", buckets, total_count=1,
                                         shown_count=1)
    assert not any("more" in ln for ln in lines)


# ── DB grouping (behavioral) ──────────────────────────────────


def _seed(conn):
    now = "2026-06-09T09:00:00"
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        " is_anime, is_4k, themes_subdir, discovered_at, last_seen_at) "
        "VALUES ('s_tv','TV Shows','show',1,0,0,'tv',?,?)", (now, now))
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        " is_anime, is_4k, themes_subdir, discovered_at, last_seen_at) "
        "VALUES ('s_an','Anime','show',1,1,0,'anime',?,?)", (now, now))
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        " is_anime, is_4k, themes_subdir, discovered_at, last_seen_at) "
        "VALUES ('s_4k','4K Movies','movie',1,0,1,'movies-4k',?,?)", (now, now))
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        " is_anime, is_4k, themes_subdir, discovered_at, last_seen_at) "
        "VALUES ('s_mv','Movies','movie',1,0,0,'movies',?,?)", (now, now))
    # A TV theme matched by guid_tmdb.
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        " guid_tmdb, title, year, first_seen_at, last_seen_at) "
        "VALUES ('rk1','s_tv','show','247522','Tires','2024',?,?)", (now, now))
    # A anime theme: guid_tmdb NULL, linked only via theme_id.
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, upstream_source, "
        " first_seen_sync_at, last_seen_sync_at) "
        "VALUES ('tv', 999, 'Space Dandy', '2014', 'themoviedb', ?, ?)",
        (now, now))
    theme_id = conn.execute(
        "SELECT id FROM themes WHERE tmdb_id = 999").fetchone()[0]
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        " guid_tmdb, theme_id, title, year, first_seen_at, last_seen_at) "
        "VALUES ('rk2','s_an','show',NULL,?, 'Space Dandy','2014',?,?)",
        (theme_id, now, now))
    # A movie in BOTH Movies + 4K Movies (same tmdb, two sections).
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        " guid_tmdb, title, year, first_seen_at, last_seen_at) "
        "VALUES ('rk3','s_mv','movie','500','Movie','2001',?,?)", (now, now))
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        " guid_tmdb, title, year, first_seen_at, last_seen_at) "
        "VALUES ('rk4','s_4k','movie','500','Movie','2001',?,?)", (now, now))
    conn.commit()


def test_sections_for_theme_guid_and_theme_id_paths(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    from app.core.db import get_conn
    with get_conn(db) as conn:
        _seed(conn)
        # guid_tmdb path
        assert _sections_for_theme(conn, "tv", 247522) == ["TV Shows"]
        # anime theme_id path (guid_tmdb NULL) — the v1.22.17/.39 class
        assert _sections_for_theme(conn, "tv", 999) == ["Anime"]
        # multi-section movie → both, sorted
        assert _sections_for_theme(conn, "movie", 500) == ["4K Movies", "Movies"]


def test_build_buckets_groups_by_section_and_counts_distinct(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    from app.core.db import get_conn
    with get_conn(db) as conn:
        _seed(conn)
        items = [
            ("tv", 247522, "Tires", "2024"),
            ("tv", 999, "Space Dandy", "2014"),
            ("movie", 500, "Movie", "2001"),
        ]
        buckets, shown = _build_sync_section_buckets(conn, items)
        assert shown == 3  # three DISTINCT items
        assert buckets["TV Shows"] == ["Tires (2024)"]
        assert buckets["Anime"] == ["Space Dandy (2014)"]
        # the multi-section movie lands under BOTH headers
        assert buckets["Movies"] == ["Movie (2001)"]
        assert buckets["4K Movies"] == ["Movie (2001)"]


def test_new_query_widened_to_theme_id_for_anime():
    # Source-pin: the New-list EXISTS gate must include the theme_id path so
    # anime (guid_tmdb NULL) appears in the list, not just the count.
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # Scope to the New-list query block specifically.
    start = src.index("SELECT media_type, tmdb_id, title, year")
    block = src[start:start + 900]
    assert "OR pi.theme_id = themes.id" in block, (
        "v1.22.45: New-list EXISTS must match theme_id linkage (anime cohort)")
