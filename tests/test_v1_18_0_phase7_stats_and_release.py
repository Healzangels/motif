"""v1.18.0 Phase 7 — Final release wiring.

Three surfaces:

  1. `/api/stats` response gains a `collections` bucket parallel
     to `movies` / `tv`. The frontend's
     `window.__motif_themes_have.collection` (Phase 6) reads from
     this bucket's `tdb_total`; before Phase 7 it falls through to
     0 (defensive `stats.collections && ...` chain).

  2. Schema, docs, and version surfaces:
       * `app/__init__.py` `__version__` = "1.18.0"
       * `CLAUDE.md` schema marker updated v54 → v55
       * `CLAUDE.md` SRC letter axis section mentions /collections

  3. `notify_content.enrich_item` is verified media_type-agnostic
     (no collection-specific branch needed — the existing queries
     against themes / user_overrides / local_files / plex_sections
     work uniformly for media_type='collection' rows).
"""

from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = REPO / "app" / "web" / "api.py"
CLAUDE_MD = REPO / "CLAUDE.md"
APP_INIT = REPO / "app" / "__init__.py"


# ── /api/stats collections bucket ─────────────────────────────


def test_stats_sql_selects_collections_total():
    """The /api/stats SELECT must include a collections_total
    column so the response can carry the row count."""
    src = API_PY.read_text()
    assert "AS collections_total" in src, (
        "v1.18.0: /api/stats must include collections_total"
    )


def test_stats_sql_selects_collections_tdb_total():
    """collections_tdb_total is the TDB-coverage signal the
    frontend's window.__motif_themes_have.collection reads from.
    Must filter on upstream_source IN ('imdb','themoviedb') to
    match the movies/tv shape.

    v1.18.20: tolerate both spaced ('imdb', 'themoviedb') and
    no-space ('imdb','themoviedb') forms. The v1.18.20 SSR helper
    added a second `AS collections_tdb_total` aggregate with the
    no-space form (matching the surrounding SSR convention);
    the existing /api/stats query uses the spaced form. Both
    are semantically identical and both must be accepted."""
    src = API_PY.read_text()
    assert "AS collections_tdb_total" in src
    # Pin the upstream_source filter on EVERY occurrence of the
    # alias — both the SSR and /api/stats aggregates must filter
    # TDB-only.
    cursor = 0
    found_blocks = []
    while True:
        idx = src.find("AS collections_tdb_total", cursor)
        if idx < 0:
            break
        block = src[max(0, idx - 500):idx + 50]
        found_blocks.append(block)
        cursor = idx + 1
    assert found_blocks, "no AS collections_tdb_total in src"
    for block in found_blocks:
        assert "media_type = 'collection'" in block
        # Whitespace-tolerant: collapse spaces inside the IN clause.
        flat = " ".join(block.split())
        assert (
            "upstream_source IN ('imdb','themoviedb')" in flat
            or "upstream_source IN ('imdb', 'themoviedb')" in flat
        ), (
            "collections_tdb_total aggregate must filter on TDB "
            "upstream sources"
        )


def test_stats_sql_selects_collections_dl_and_placed():
    """The same per-bucket aggregates (downloaded, placed) the
    movies/tv buckets carry — parity matters for the dashboard
    cards that render all three buckets uniformly."""
    src = API_PY.read_text()
    assert "AS collections_dl" in src
    assert "AS collections_placed" in src


def test_stats_response_includes_collections_object():
    """The JSON response dict must include a `collections` key
    with total / downloaded / placed / tdb_total fields shaped the
    same as `movies` / `tv`."""
    src = API_PY.read_text()
    assert '"collections": {' in src
    # The object must include the four parallel fields.
    coll_idx = src.index('"collections": {')
    block = src[coll_idx:coll_idx + 400]
    assert "row[\"collections_total\"]" in block
    assert "row[\"collections_dl\"]" in block
    assert "row[\"collections_placed\"]" in block
    assert "row[\"collections_tdb_total\"]" in block


# ── Version + docs ────────────────────────────────────────────


def test_version_bumped_to_1_18_0():
    """`app/__init__.py` __version__ must be ≥ 1.18.0 so the
    topbar brand reads correctly and the release-check comparison
    works against the deployed image's tag. Floor-only pin
    (vs literal "1.18.0") so v1.18.1+ patch tags don't break
    this guard."""
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m is not None
    assert tuple(int(x) for x in m.groups()) >= (0, 18, 0), (
        "v1.18.0+: app/__init__.py __version__ must be ≥ 1.18.0"
    )


def test_claude_md_schema_marker_updated_to_v55():
    """CLAUDE.md's stack-table schema marker must read the CURRENT
    schema version so a future Claude session reading it before
    debugging schema issues sees the right baseline.

    v1.22.29: was pinned to a frozen literal ('v55') and went stale
    every migration — the docs-currency pass bumped CLAUDE.md to v64
    and this test (correctly) caught the drift, but a frozen literal
    means EVERY future bump trips it for no signal. Now it derives
    the expected marker from db.CURRENT_SCHEMA_VERSION so the doc and
    the code can never silently diverge, and the test never goes stale."""
    from app.core.db import CURRENT_SCHEMA_VERSION
    src = CLAUDE_MD.read_text()
    # Whitespace-tolerant single-line check.
    flat = " ".join(src.split())
    expected = f"current schema **v{CURRENT_SCHEMA_VERSION}**"
    assert expected in flat, (
        f"CLAUDE.md stack table must reference the live schema version "
        f"({expected!r}) — bump the marker when CURRENT_SCHEMA_VERSION changes"
    )


def test_claude_md_src_letter_section_mentions_collections():
    """The SRC letter axis section heading must include
    /collections so a reader sees collections are a first-class
    tab + the axis applies to them."""
    src = CLAUDE_MD.read_text()
    flat = " ".join(src.split())
    assert "/movies, /tv, /anime, /collections" in flat


def test_claude_md_documents_collection_src_letter_behavior():
    """A short note explaining that the SAME _SRC_LETTER_SQL /
    computeSrcLetter handle collections (T/U/P/– only, no A/M),
    with placement_kind='plex_upload' as the distinguishing
    placement signal."""
    src = CLAUDE_MD.read_text()
    flat = " ".join(src.split())
    # Pin the key claim — same definitions, no variant.
    assert "no collection-specific variant" in flat


# ── enrich_item works for collections (no source edit needed) ─


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db)
    return db


def test_enrich_item_returns_display_title_for_collection(fresh_db: Path):
    """`notify_content.enrich_item` must produce a usable
    display_title for a media_type='collection' row — no
    media-type-specific branch needed, the existing themes
    lookup handles it via (media_type, tmdb_id) regardless of
    what the media_type value is."""
    # Seed a collection themes row.
    with sqlite3.connect(fresh_db) as conn:
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, year, "
            "   youtube_url, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("collection", 1241, "Harry Potter Collection", None,
             "https://www.youtube.com/watch?v=yB-c85V8Zsg",
             "themoviedb",
             "2026-05-19T12:00:00", "2026-05-19T12:00:00"),
        )
        conn.commit()
    from app.core.notify_content import enrich_item
    ctx = enrich_item(fresh_db, media_type="collection", tmdb_id=1241)
    assert ctx["display_title"] == "Harry Potter Collection", (
        "v1.18.0: enrich_item must build display_title from the "
        "collection's title (no year suffix since collections "
        "aren't dated)."
    )
    assert ctx["theme_url"] == "https://www.youtube.com/watch?v=yB-c85V8Zsg"
    assert ctx["provenance"] == "themerrdb"


def test_enrich_item_handles_collection_with_no_themes_row(fresh_db: Path):
    """Missing-row case for collections: enrich_item returns a
    fallback display_title shape ('collection/<tmdb>') without
    raising. Class-9 hygiene — best-effort lookup."""
    from app.core.notify_content import enrich_item
    ctx = enrich_item(fresh_db, media_type="collection", tmdb_id=9999)
    # Falls back to the bare-id shape.
    assert ctx["display_title"] == "collection/9999"
    assert ctx["provenance"] == "unknown"
