"""v1.18.22 — Pushed (plex_upload) link state + collections +P fix.

the user's audit on v1.18.19 collections deploy:

  > the SRC using our libraries logic really is P since plex does
  > have a theme which its serving though in the case of
  > Collections that is a bit unhelpful since they would all be
  > src P since we're uploading via API. … I think they would
  > want link PS to indicate they have been linked to plex.

The full architectural pass split across multiple tags:

  * **v1.18.22 (this tag)** — foundation:
    - plex_enum guards `plex_independent_theme = 1` against
      plex_upload placements. The discriminator ("no sidecar +
      has_theme → Plex serves its own theme") misfires when
      motif uploaded via API — there's no sidecar by design,
      but Plex's theme IS motif's, so it's not independent.
    - One-shot startup backfill clears `plex_independent_theme=1`
      from existing plex_upload rows.
    - New LINK chip **PU** (Pushed) renders for plex_upload
      placements. Cyan tone. Distinct from PS (Plex serves its
      OWN theme — Let-Plex-Serve / cloud / embed).
    - LINK filter row gets a PU pill.
    - SQL link_pills branch for `pu` selection.

  * v1.18.23 (next) — opt-in API-push for movies/TV via the
    PLACE menu. Will follow this foundation.

End state: SRC always reflects origin (T/U/A/M/P/–). The
LINK chip tells you HOW the placement is realized (HL/C/PU/PS).
Collections render with their true source (T or U) and LINK=PU
instead of the misleading SRC=P / +P composite.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"
RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
API_PY = REPO / "app" / "web" / "api.py"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"
MAIN_PY = REPO / "app" / "main.py"


# ── Backend: plex_enum gates indep_observation ────────────────


def test_plex_enum_suppresses_indep_for_plex_upload():
    """The indep_observation computation must check for an
    existing plex_upload placement first; if found, set to 0
    (Plex is NOT serving its own theme — motif's upload IS
    the theme)."""
    src = PLEX_ENUM_PY.read_text()
    assert "has_plex_upload = bool(conn.execute(" in src
    assert "p.placement_kind = 'plex_upload'" in src
    # The gate must come BEFORE the sidecar/has_theme branches.
    idx_check = src.index("has_plex_upload")
    idx_sidecar = src.index("if sidecar == 1:")
    assert idx_check < idx_sidecar, (
        "v1.18.22: plex_upload check must precede the sidecar/"
        "has_theme discriminator"
    )


# ── Backend: backfill helper ──────────────────────────────────


def test_clear_indep_helper_exists():
    """`maybe_clear_plex_upload_indep_flag(db_path)` must be
    importable."""
    src = RECOVERY_PY.read_text()
    assert "def maybe_clear_plex_upload_indep_flag(" in src


def test_clear_indep_uses_independent_marker():
    """Independent marker so the backfill runs on installs
    where earlier recovery markers are already stamped."""
    src = RECOVERY_PY.read_text()
    assert "recovery_plex_upload_indep_clear_done_at_v1_18_22" in src


@pytest.fixture
def plex_upload_with_indep_flag_fixture(tmp_path: Path):
    """Seed the user's pre-v1.18.22 state: collection plex_items
    row with plex_independent_theme=1 (stamped by the
    pre-v1.18.22 plex_enum bug) + a plex_upload placement."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T20:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        themes_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 100, 'Wonka Coll', "
            "        'wonka coll', NULL, 'themoviedb', ?, ?)",
            (ts, ts),
        ).lastrowid
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, placed_at, plex_refreshed) "
            "VALUES ('collection', 100, '1', '', "
            "        'plex_upload', ?, 1)",
            (ts,),
        )
        # Crucially: plex_independent_theme=1 stamped (the false
        # positive we're clearing).
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, guid_tmdb, "
            "   has_theme, plex_independent_theme, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('500', '1', 'collection', 'Wonka Coll', "
            "        'wonka coll', NULL, '', '100', "
            "        1, 1, ?, ?, ?)",
            (themes_id, ts, ts),
        )
        conn.commit()
    return db_path


def test_clear_indep_flag_e2e(plex_upload_with_indep_flag_fixture):
    """End-to-end: backfill clears plex_independent_theme=1 on
    the plex_upload row."""
    from app.core.recovery_v55 import (
        maybe_clear_plex_upload_indep_flag,
    )
    db_path = plex_upload_with_indep_flag_fixture
    stats = maybe_clear_plex_upload_indep_flag(db_path)
    assert stats["rows_cleared"] >= 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT plex_independent_theme FROM plex_items "
            "WHERE rating_key = '500'"
        ).fetchone()
    assert row["plex_independent_theme"] == 0, (
        "v1.18.22: indep flag must be cleared on plex_upload row"
    )


def test_clear_indep_skips_non_plex_upload(tmp_path: Path):
    """Backfill must leave the indep flag alone on rows whose
    placement is NOT plex_upload (legitimate +P signal)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    from app.core.recovery_v55 import (
        maybe_clear_plex_upload_indep_flag,
    )
    init_db(db_path)
    ts = "2026-05-20T20:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        themes_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 200, 'Cloud Movie', "
            "        'cloud movie', '2020', 'themoviedb', ?, ?)",
            (ts, ts),
        ).lastrowid
        # Legitimate +P: hardlink sidecar + Plex Pass cloud theme
        # (plex_independent_theme=1 should stick).
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, placed_at, plex_refreshed) "
            "VALUES ('movie', 200, '1', "
            "        '/data/media/movies/Cloud Movie (2020)', "
            "        'hardlink', ?, 1)",
            (ts,),
        )
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, guid_tmdb, "
            "   has_theme, local_theme_file, "
            "   plex_independent_theme, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('600', '1', 'movie', 'Cloud Movie', "
            "        'cloud movie', '2020', "
            "        '/data/media/movies/Cloud Movie (2020)', "
            "        '200', 1, 1, 1, ?, ?, ?)",
            (themes_id, ts, ts),
        )
        conn.commit()
    stats = maybe_clear_plex_upload_indep_flag(db_path)
    # No plex_upload placement → row should not be touched.
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT plex_independent_theme FROM plex_items "
            "WHERE rating_key = '600'"
        ).fetchone()
    assert row["plex_independent_theme"] == 1, (
        "v1.18.22: legitimate +P flag on a hardlinked sidecar "
        "row must NOT be cleared"
    )
    assert stats["rows_cleared"] == 0


def test_clear_indep_is_idempotent(plex_upload_with_indep_flag_fixture):
    """Second invocation short-circuits on the marker."""
    from app.core.recovery_v55 import (
        maybe_clear_plex_upload_indep_flag,
    )
    db_path = plex_upload_with_indep_flag_fixture
    maybe_clear_plex_upload_indep_flag(db_path)
    stats2 = maybe_clear_plex_upload_indep_flag(db_path)
    assert stats2["skipped_reason"] == "marker_set"
    assert stats2["rows_cleared"] == 0


# ── Frontend: LINK chip PU branch ─────────────────────────────


def test_js_renders_pu_chip_for_plex_upload():
    """The JS linkCell render must include a branch for
    placement_kind='plex_upload' that emits the PU glyph with
    the cyan tone + 'Pushed' tooltip."""
    js = APP_JS.read_text()
    assert "it.placement_kind === 'plex_upload'" in js
    assert "link-glyph-pu" in js
    assert ">PU<" in js, (
        "v1.18.22: PU label must render inside the link-glyph span"
    )


def test_css_defines_link_glyph_pu_cyan_tone():
    """`.link-glyph-pu` must use the --cyan palette so PU reads
    as a distinct chip from PS (amber)."""
    css = APP_CSS.read_text()
    assert ".link-glyph-pu" in css
    idx = css.index(".link-glyph-pu")
    block = css[idx:idx + 400]
    assert "var(--cyan)" in block


def test_link_filter_pill_includes_pu():
    """The LINK filter row in library.html must include a PU
    chip alongside HL / C / M / PS so users can filter to just
    the API-pushed placements."""
    html = LIBRARY_HTML.read_text()
    assert 'data-link-pill="pu"' in html
    assert "link-glyph-pu" in html


def test_sql_link_pills_branch_for_pu():
    """The link_pills SQL handler must include the `pu` branch
    selecting placement_kind='plex_upload'."""
    src = API_PY.read_text()
    assert 'elif p == "pu":' in src
    idx = src.index('elif p == "pu":')
    # v0.51.238: was a fixed +1500 slice, which asserts the byte length of the
    # branch rather than its content — motif requires a `# vX.Y.Z:` rationale on
    # load-bearing lines, so the branch only grows and eventually walks the SQL
    # off the end (it did). Anchored on the next branch instead.
    block = src[idx:src.index('elif p == "rp":', idx)]
    assert "COALESCE(p_e.placement_kind, p_g.placement_kind) = 'plex_upload'" in block


def test_api_library_link_set_whitelist_includes_pu():
    """The api_library `_pset(link_pills, {...})` whitelist must
    accept 'pu'. v1.19.43 widened the set to also include 'b'
    for the cloud-themes-backup filter chip — pin the substring
    rather than the absolute set so future widenings (one more
    token added) don't break this guard."""
    src = API_PY.read_text()
    assert '"hl", "c", "m", "none", "ps", "pu"' in src


# ── End-to-end: link_pills=pu returns plex_upload rows ────────


def test_link_pills_pu_matches_plex_upload_e2e(
    plex_upload_with_indep_flag_fixture,
):
    """When the user clicks the new PU filter chip, the SQL
    must narrow to placement_kind='plex_upload' rows."""
    db_path = plex_upload_with_indep_flag_fixture
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # Direct SQL inspection of the branch — narrowed query
        # that mirrors the link_pills='pu' filter logic.
        rows = conn.execute("""
            SELECT pi.rating_key
            FROM plex_items pi
            INNER JOIN themes t ON t.id = pi.theme_id
            LEFT JOIN placements p
              ON p.media_type = t.media_type
             AND p.tmdb_id = t.tmdb_id
             AND p.section_id = pi.section_id
            WHERE p.placement_kind = 'plex_upload'
        """).fetchall()
    assert len(rows) == 1
    assert rows[0]["rating_key"] == "500"
