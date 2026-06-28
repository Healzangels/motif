"""Regression guards for `_library_main_query` SQL composition.

The v1.12.99 → v1.12.100 fix was for a 500 that fired when a SRC pill
was selected: the count query's slim FROM clause didn't include the
`themes` JOIN, but the v1.12.96 _SRC_LETTER_SQL referenced
`t.upstream_source`. Catching that earlier required exercising every
pill combination through the actual query path. This test does that
— if a future SQL edit references a column that's not in the slim
count path's JOIN set, one of the asserts here will 500 at compile
time instead of in production.

Coverage:
  - Each SRC pill (T, U, A, M, P, '-') individually.
  - All 15 pairs of two SRC pills.
  - All 6 SRC pills together.
  - SRC pill × every status filter.
  - SRC pill × tdb axis.
  - SRC pill × sort=src.
  - SRC pill × every other pill axis (DL, PL, LINK, TDB).

Each fixture row is constructed to match exactly one SRC class so
single-pill counts equal 1, all-six-selected count equals 6, and
shared-axis filters can be sanity-checked.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from itertools import combinations
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def seeded_db():
    """Build a tiny library DB with one row per SRC class.

    Class encoding (tmdb_id):
      101 → T  (placed, source_kind=themerrdb)
      102 → U  (placed, source_kind=url, manual)
      103 → A  (placed, source_kind=adopt, manual, plex_orphan)
      104 → M  (sidecar-only)
      105 → P  (Plex agent — has_theme=1, no placement)
      106 → -  (no theme anywhere)
    """
    from app.core.db import init_db  # noqa: WPS433 — local import

    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db_path = Path(path)
    init_db(db_path)

    now = "2026-01-01T00:00:00Z"
    # v1.12.112: P is verified-server-truth, not media-type-derived.
    # The fixture set covers:
    #   - 105 P-tv (verified): TV show with Plex serving its theme
    #   - 107 P-movie (verified): movie with themerr-plex-style embed,
    #         Plex serves the theme. Same SRC=P; different media type.
    #   - 108 stale-movie ('-'): movie with has_theme=1 but Plex's
    #         claim is 404 (verified_ok=0) — must classify as '-'
    #         even though Plex's metadata still says theme=true.
    # All other letters keep their movie fixtures.
    # Main fixture: exactly one row per SRC class. P is intentionally
    # a movie (themerr-plex embed pattern: has_theme=1, verified_ok=1)
    # to enforce that P is NOT media-type-gated. A separate
    # test_stale_plex_cache_movie_classifies_as_dash test below
    # covers the (has_theme=1, verified_ok=0) → '-' path without
    # disturbing this fixture's per-class invariant.
    fixtures = [
        # (tmdb, label, mt, sec, upstream, lf_data, p_data,
        #  has_theme, verified_ok)
        (101, "T", "movie", "1", "imdb",
            ("themerrdb", "abc12345678", "auto"),
            ("/m/T", "auto", "hardlink"),
            0, None),
        (102, "U", "movie", "1", "imdb",
            ("url", "def12345678", "manual"),
            ("/m/U", "manual", "hardlink"),
            0, None),
        (103, "A", "movie", "1", "plex_orphan",
            ("adopt", "sha-prefix-not-yt", "manual"),
            ("/m/A", "manual", "hardlink"),
            0, None),
        (104, "M", "movie", "1", "plex_orphan", None, None,
            0, None),
        (105, "P", "movie", "1", "plex_orphan", None, None,
            1, 1),  # themerr-plex embed: Plex's claim verified live
        (106, "-", "movie", "1", "plex_orphan", None, None,
            0, None),
    ]
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "discovered_at, last_seen_at) VALUES ('1', 'Movies', 'movie', 1, ?, ?)",
        (now, now),
    )
    for tmdb, label, mt, sec, upstream, lf_data, p_data, has_theme, verified_ok in fixtures:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                mt, tmdb, label, upstream, now, now,
                "https://yt/x" if upstream != "plex_orphan" else None,
            ),
        )
        theme_id = conn.execute(
            "SELECT id FROM themes WHERE tmdb_id = ?", (tmdb,),
        ).fetchone()[0]
        plex_mt = "show" if mt == "tv" else mt
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "title, year, guid_tmdb, theme_id, local_theme_file, has_theme, "
            "plex_theme_verified_ok, first_seen_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, '2024', ?, ?, ?, ?, ?, ?, ?)",
            (
                f"rk{tmdb}", sec, plex_mt, label, tmdb, theme_id,
                1 if label == "M" else 0,
                has_theme,
                verified_ok,
                now, now,
            ),
        )
        if lf_data is not None:
            sk, vid, prov = lf_data
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id, "
                "file_path, source_video_id, downloaded_at, source_kind, "
                "provenance) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    mt, tmdb, sec, f"{mt}/{label}/theme.mp3", vid, now, sk, prov,
                ),
            )
        if p_data is not None:
            folder, prov, kind = p_data
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id, "
                "media_folder, placement_kind, provenance, placed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (mt, tmdb, sec, folder, kind, prov, now),
            )
    conn.commit()
    conn.close()
    yield db_path
    db_path.unlink(missing_ok=True)


def _tab_for(letter: str) -> str:
    """v1.12.112: all 6 SRC fixtures live in the movies section
    (P now lives there too, mirroring themerr-plex's embed-on-movies
    pattern). Helper kept for backward compatibility with tests that
    parametrize across letters and want a tab arg."""
    return "movies"


def _run(db_path: Path, **kwargs):
    from app.web.api import _library_main_query  # local import — heavy

    return _library_main_query(
        db_path,
        tab=kwargs.pop("tab", "movies"),
        fourk=kwargs.pop("fourk", False),
        q=kwargs.pop("q", ""),
        status=kwargs.pop("status", "all"),
        page=1, per_page=200,
        sort=kwargs.pop("sort", "title"),
        sort_dir=kwargs.pop("sort_dir", "asc"),
        tdb=kwargs.pop("tdb", "any"),
        **kwargs,
    )


# ── Single SRC pill ──────────────────────────────────────────────


@pytest.mark.parametrize("letter", ["T", "U", "A", "M", "P", "-"])
def test_src_single_pill(seeded_db, letter):
    """Each SRC pill matches exactly the one fixture row of that class."""
    result = _run(seeded_db, tab=_tab_for(letter), src_pills={letter})
    assert result["total"] == 1, (
        f"src={letter} expected 1 match (one fixture per class), "
        f"got {result['total']}"
    )


# ── SRC pill pairs ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "combo",
    list(combinations(["T", "U", "A", "M", "P", "-"], 2)),
)
def test_src_pair_pills(seeded_db, combo):
    """OR-within-axis: any 2 pills selected → exactly 2 rows match."""
    result = _run(seeded_db, src_pills=set(combo))
    assert result["total"] == 2, (
        f"src={','.join(combo)} expected 2 matches, got {result['total']}"
    )


def test_src_all_pills(seeded_db):
    """Selecting every pill should return every row (6 fixtures)."""
    result = _run(seeded_db, src_pills={"T", "U", "A", "M", "P", "-"})
    assert result["total"] == 6


def _insert_synthetic(seeded_db, tmdb, label, has_theme, verified_ok):
    """Helper: add a synthetic plex_items+themes row for a focused test.
    Caller must call _delete_synthetic to clean up before fixture
    pollution affects other tests sharing the module-scoped seeded_db."""
    import sqlite3
    conn = sqlite3.connect(str(seeded_db))
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
        "last_seen_sync_at, first_seen_sync_at, youtube_url) "
        "VALUES ('movie', ?, ?, 'plex_orphan', "
        "'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', NULL)",
        (tmdb, label),
    )
    theme_id = conn.execute(
        "SELECT id FROM themes WHERE tmdb_id = ?", (tmdb,),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        "title, year, guid_tmdb, theme_id, local_theme_file, has_theme, "
        "plex_theme_verified_ok, first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', ?, '2024', ?, ?, 0, ?, ?, "
        "'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        (f"rk{tmdb}", label, tmdb, theme_id, has_theme, verified_ok),
    )
    conn.commit()
    conn.close()


def _delete_synthetic(seeded_db, tmdb):
    import sqlite3
    conn = sqlite3.connect(str(seeded_db))
    conn.execute("DELETE FROM plex_items WHERE rating_key = ?", (f"rk{tmdb}",))
    conn.execute("DELETE FROM themes WHERE tmdb_id = ?", (tmdb,))
    conn.commit()
    conn.close()


def test_stale_plex_cache_movie_classifies_as_dash(seeded_db):
    """v1.12.112: a movie row with has_theme=1 but
    plex_theme_verified_ok=0 (HEAD against Plex returned 404 — stale
    metadata cache pointing at a deleted file) classifies as '-' even
    though Plex's API still claims a theme."""
    _insert_synthetic(seeded_db, 999, "STALE",
                      has_theme=1, verified_ok=0)
    try:
        p = _run(seeded_db, src_pills={"P"}, q="STALE")
        dash = _run(seeded_db, src_pills={"-"}, q="STALE")
        assert p["total"] == 0, "stale-cache row should NOT match src=P"
        assert dash["total"] == 1, "stale-cache row should match src='-'"
    finally:
        _delete_synthetic(seeded_db, 999)


def test_unverified_plex_theme_optimistic_p(seeded_db):
    """v1.12.112: has_theme=1 + verified_ok=NULL (untested) trusts
    Plex's claim optimistically — a fresh install or transient
    network failure shouldn't cause a regression in P classification."""
    _insert_synthetic(seeded_db, 998, "UNTESTED",
                      has_theme=1, verified_ok=None)
    try:
        p = _run(seeded_db, src_pills={"P"}, q="UNTESTED")
        assert p["total"] == 1, "untested has_theme=1 row should match src=P (optimistic)"
    finally:
        _delete_synthetic(seeded_db, 998)


# ── SRC × status (catches the v1.12.99 missing-JOIN regression) ──


@pytest.mark.parametrize("status", [
    "themed", "untracked", "has_theme", "manual", "placed",
    "unplaced", "downloaded", "failures", "updates",
])
def test_src_pill_with_each_status(seeded_db, status):
    """SRC pill compositions with every status filter must compile.

    This is the test that would have caught v1.12.99: when the count
    query's slim FROM was missing the themes JOIN that
    _SRC_LETTER_SQL needs, every status here would 500.
    """
    result = _run(seeded_db, status=status, src_pills={"T"})
    assert "total" in result


# ── SRC × tdb axis ───────────────────────────────────────────────


@pytest.mark.parametrize("tdb", ["tracked", "untracked"])
def test_src_pill_with_tdb_axis(seeded_db, tdb):
    result = _run(seeded_db, tdb=tdb, src_pills={"U"})
    assert "total" in result


# ── SRC × sort=src ───────────────────────────────────────────────


@pytest.mark.parametrize("letter", ["T", "U", "A", "M", "P", "-"])
def test_src_pill_with_sort_src(seeded_db, letter):
    """sort=src reuses _SRC_LETTER_SQL as an ORDER BY expression."""
    result = _run(seeded_db, tab=_tab_for(letter), sort="src", src_pills={letter})
    assert result["total"] == 1


# ── SRC × other-pill axes ────────────────────────────────────────


def test_src_with_dl_pill(seeded_db):
    """SRC=T row has a downloaded canonical → DL=on matches."""
    result = _run(seeded_db, src_pills={"T"}, dl_pills={"on"})
    assert result["total"] == 1


def test_src_with_dl_broken(seeded_db):
    """No fixture rows have canonical_missing=True, so DL=broken=0."""
    result = _run(seeded_db, src_pills={"T"}, dl_pills={"broken"})
    assert result["total"] == 0


def test_src_with_pl_pill(seeded_db):
    """SRC=M is sidecar-only → no placement → PL=off matches."""
    result = _run(seeded_db, src_pills={"M"}, pl_pills={"off"})
    assert result["total"] == 1


def test_src_with_link_pill(seeded_db):
    """SRC=A row has a hardlink placement."""
    result = _run(seeded_db, src_pills={"A"}, link_pills={"hl"})
    assert result["total"] == 1


def test_src_with_tdb_pill(seeded_db):
    """SRC=T fixture: upstream_source='imdb' → tdb pill matches."""
    result = _run(seeded_db, src_pills={"T"}, tdb_pills={"tdb"})
    assert result["total"] == 1


def test_src_orphan_with_tdb_none(seeded_db):
    """SRC=- is a plex_orphan → tdb_pills=none matches.
    v1.12.111: P fixture moved to tv tab so movies tab has 1 plex_orphan
    row with no upstream URL (the '-' fixture). The M fixture is also a
    plex_orphan but has src='M' (different SRC class).
    """
    result = _run(seeded_db, src_pills={"-"}, tdb_pills={"none"})
    assert result["total"] == 1
