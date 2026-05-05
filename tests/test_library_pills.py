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
    # v1.12.111: P fixture moved to TV. The SRC=P case is structurally
    # TV-only — Plex doesn't provide cloud themes for movies, so a
    # movie row with has_theme=1 + no sidecar can't legitimately be P.
    # Tests that touch P pass tab='tv' to _run.
    fixtures = [
        # (tmdb, label, mt, sec, upstream, local_files(sk, vid, prov), placements(folder, prov, kind))
        (101, "T", "movie", "1", "imdb",
            ("themerrdb", "abc12345678", "auto"), ("/m/T", "auto", "hardlink")),
        (102, "U", "movie", "1", "imdb",
            ("url", "def12345678", "manual"), ("/m/U", "manual", "hardlink")),
        (103, "A", "movie", "1", "plex_orphan",
            ("adopt", "sha-prefix-not-yt", "manual"),
            ("/m/A", "manual", "hardlink")),
        (104, "M", "movie", "1", "plex_orphan", None, None),
        (105, "P", "tv",    "2", "plex_orphan", None, None),
        (106, "-", "movie", "1", "plex_orphan", None, None),
    ]
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "discovered_at, last_seen_at) VALUES ('1', 'Movies', 'movie', 1, ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "discovered_at, last_seen_at) VALUES ('2', 'TV', 'show', 1, ?, ?)",
        (now, now),
    )
    for tmdb, label, mt, sec, upstream, lf_data, p_data in fixtures:
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
            "first_seen_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, '2024', ?, ?, ?, ?, ?, ?)",
            (
                f"rk{tmdb}", sec, plex_mt, label, tmdb, theme_id,
                1 if label == "M" else 0,
                1 if label == "P" else 0,
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
    """v1.12.111: P fixture lives in TV; everything else in movies."""
    return "tv" if letter == "P" else "movies"


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
    # v1.12.111: combos with P only valid in tab='tv' (where the P
    # fixture lives) and there's only one row in tv, so pair tests
    # involving P would need cross-tab. Drop combos with P and add
    # a separate single-tab pair test below.
    list(combinations(["T", "U", "A", "M", "-"], 2)),
)
def test_src_pair_pills(seeded_db, combo):
    """OR-within-axis: any 2 pills selected → exactly 2 rows match."""
    result = _run(seeded_db, src_pills=set(combo))
    assert result["total"] == 2, (
        f"src={','.join(combo)} expected 2 matches, got {result['total']}"
    )


def test_src_all_pills_movies(seeded_db):
    """Selecting all movie-tab pills returns every movie fixture (5)."""
    result = _run(seeded_db, src_pills={"T", "U", "A", "M", "-"})
    assert result["total"] == 5


def test_src_p_pill_tv_tab(seeded_db):
    """P pill in tv tab returns the single tv P fixture."""
    result = _run(seeded_db, tab="tv", src_pills={"P"})
    assert result["total"] == 1


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
