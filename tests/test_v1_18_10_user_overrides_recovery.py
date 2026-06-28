"""v1.18.10 — recover user_overrides + restore collection skip semantic.

the user reported on the v1.18.7 deploy:
  * Most rows recovered (T attribution restored)
  * Some orphan rows still showed as M instead of U — even though
    the provenance log clearly showed SET URL events for them.
  * Collections were defaulting to force_overwrite=True (v1.18.9)
    which the user explicitly didn't want: "treated same as movies
    as if plex has a theme that's best case."

## Why U-source orphans showed as M after v1.18.7

The v1.18.0 FK cascade nuked local_files + placements but NOT
user_overrides (user_overrides has no FK to themes). HOWEVER —
the very next sync ran the v1.12.60 "orphan user_overrides
sweep" against the broken DB state:

  DELETE FROM user_overrides WHERE
    NOT EXISTS (lf row) AND NOT EXISTS (placements row)
    AND NOT EXISTS (plex_items where guid_tmdb=tmdb_id AND
                    local_theme_file=1)
    AND NOT EXISTS (job)

For orphan rows the plex_items presence check ALWAYS fails:
plex_items.guid_tmdb holds the REAL Plex TMDB id (or NULL), NOT
the orphan's synthetic negative tmdb_id. So with local_files +
placements wiped, every orphan user_overrides looked "stale" →
got DELETEd.

the user's logs after v1.18.0:
  "Pruned 98 orphan user_overrides rows (no theme presence)"

The v1.18.5 walker recovered local_files + placements but
`_infer_source_kind` saw `upstream='plex_orphan' +
has_user_override=False` → returned ('manual', 'adopt') →
row classified as A. the user's M*A*S*H 30th Anniversary Reunion
example: orphan tmdb=-22 with user URL set, lost user_overrides
in the sync sweep, recovery walker classified as 'adopt'.

## v1.18.10 fix

Three coordinated changes:

  1. `maybe_recover_lost_user_overrides(db_path)` — new
     recovery walker that scans the `events` table for
     historical "Manual URL set by admin" entries, extracts
     (rating_key, URL) pairs, resolves rating_key → plex_items
     → themes, and re-INSERTs user_overrides for orphan rows
     that lost them. Also reclassifies local_files.source_kind
     from 'adopt' → 'url' for the recovered rows so SRC renders
     U not A. Idempotent via INSERT OR IGNORE + its own marker
     `recovery_user_overrides_done_at` (independent of the
     v1.18.5 marker so it can run on installs where the primary
     walker already finished).

  2. v1.18.9's default `force_overwrite=True` for collections
     reverted. the user's explicit preference: collections
     respect Plex-has-theme the same way movies/shows do.

  3. The collection skip path now stamps
     `plex_items.plex_independent_theme=1` on the matched
     rating_key — same +P capture the v1.13.55 movie/show LPS
     path uses. With it, the row renders as PS (Plex Serving)
     state in the library UI: green DL dot, PS link badge, no
     nagging !P attention glyph. Pre-fix the row stalled at
     PL=await (amber) with the !P attention glyph asking the
     user to push something motif intentionally chose not to
     push.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"
MAIN_PY = REPO / "app" / "main.py"
WORKER_PY = REPO / "app" / "core" / "worker.py"


# ── Source-level pins for the new walker ──────────────────────


def test_user_overrides_recovery_function_exists():
    """`maybe_recover_lost_user_overrides(db_path)` must be
    importable from app.core.recovery_v55."""
    src = RECOVERY_PY.read_text()
    assert "def maybe_recover_lost_user_overrides(" in src


def test_user_overrides_recovery_scans_events_table():
    """The walker must scan the events table for historical
    'Manual URL set by admin' entries — the only place the
    user-set URL survives after the sync sweep nuked
    user_overrides."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_user_overrides(")
    fn_end = len(src)
    body = src[fn_start:fn_end]
    assert "FROM events" in body
    assert "Manual URL set by admin" in body
    assert "component = 'api'" in body


def test_user_overrides_recovery_reclassifies_local_files():
    """When the walker INSERTs a user_overrides row, it must
    also UPDATE local_files.source_kind from 'adopt' → 'url'
    so SRC renders U not A on the recovered row."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_user_overrides(")
    body = src[fn_start:]
    assert "UPDATE local_files" in body
    assert "SET source_kind = 'url'" in body
    assert "source_kind = 'adopt'" in body, (
        "v1.18.10: the reclassification must scope to rows that "
        "are currently 'adopt' so we don't clobber other source "
        "kinds"
    )


def test_user_overrides_recovery_has_independent_marker():
    """The user_overrides recovery must use its own marker
    (`recovery_user_overrides_done_at`) — NOT the v1.18.5
    primary marker — so it can run on installs where the
    primary walker already completed."""
    src = RECOVERY_PY.read_text()
    assert "recovery_user_overrides_done_at" in src


def test_user_overrides_recovery_is_idempotent():
    """INSERT OR IGNORE on user_overrides — second runs after
    the marker is stamped will short-circuit, but defensive
    OR IGNORE handles parallel-boot races."""
    src = RECOVERY_PY.read_text()
    assert "INSERT OR IGNORE INTO user_overrides" in src


# ── Worker: force_overwrite default + plex_independent_theme ──


def test_do_place_collection_skip_stamps_plex_independent_theme():
    """The collection skip path (when Plex has its own theme +
    not force_overwrite) must stamp
    `plex_items.plex_independent_theme = 1` on the matched
    rating_key. Without this stamp the row renders as
    'downloaded but not placed' (PL=await amber) instead of
    the LPS (Plex Serving) state the user wants."""
    src = WORKER_PY.read_text()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    # The body has TWO `if cached_has_theme and not force_overwrite:`
    # branches — the dry-run preview and the actual skip path.
    # Anchor on the second.
    first = body.index("if cached_has_theme and not force_overwrite:")
    second = body.index(
        "if cached_has_theme and not force_overwrite:",
        first + 1,
    )
    # v1.19.61: widened from 2500→3500 to absorb the new
    # PS→BK unification comment block in the skip path.
    skip_block = body[second:second + 3500]
    assert "UPDATE plex_items" in skip_block
    assert "plex_independent_theme = 1" in skip_block, (
        "v1.18.10: skip path must stamp +P capture so row "
        "renders as LPS (Plex Serving) not nagging !P await"
    )
    # Must be idempotent: only stamp when the column is NULL
    # so a legitimate post-PURGE reset stays preserved.
    assert "plex_independent_theme IS NULL" in skip_block


# ── End-to-end: walker reclassifies a recovered orphan ────────


@pytest.fixture
def lost_override_fixture(tmp_path: Path):
    """Reproduce the user's M*A*S*H scenario: orphan themes row,
    plex_items row pointing at it, local_files row that
    recovery walker classified as ('manual', 'adopt'), an
    events table entry showing SET URL — but NO user_overrides
    row (nuked by the sync sweep)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-05T09:46:00"
    with sqlite3.connect(db_path) as conn:
        # Seed plex_sections + plex_items + themes (orphan).
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
            "VALUES ('movie', -22, "
            "        \"'M*A*S*H': 30th Anniversary Reunion\", "
            "        \"m a s h 30th anniversary reunion\", "
            "        '2002', 'plex_orphan', ?, ?)",
            (ts, ts),
        ).lastrowid
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, "
            "   local_theme_file, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('628269', '1', 'movie', "
            "        \"'M*A*S*H': 30th Anniversary Reunion\", "
            "        \"m a s h 30th anniversary reunion\", "
            "        '2002', "
            "        \"/data/media/movies/M-A-S-H - 30th Anniversary Reunion (2002)\", "
            "        1, ?, ?, ?)",
            (themes_id, ts, ts),
        )
        # The recovery walker (v1.18.5) inserted local_files with
        # source_kind='adopt' since no user_overrides was found.
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES ('movie', -22, '1', "
            "        'movies/M-A-S-H - 30th Anniversary Reunion (2002)/theme.mp3', "
            "        ?, 'LJuJNw6oayg', 'manual', 'adopt')",
            (ts,),
        )
        # The events table preserves the historical SET URL.
        conn.execute(
            "INSERT INTO events (level, component, message, "
            "                    detail, ts) "
            "VALUES ('INFO', 'api', "
            "        'Manual URL set by admin: "
            "https://www.youtube.com/watch?v=LJuJNw6oayg', "
            "        ?, ?)",
            (
                '{"rating_key": "628269", '
                '"title": "M*A*S*H 30th Anniversary"}',
                ts,
            ),
        )
        conn.commit()
    return db_path


def test_walker_recovers_lost_user_override_from_events(
    lost_override_fixture,
):
    """End-to-end: the walker reads the SET URL event, finds
    the matching orphan row, INSERTs user_overrides, and
    reclassifies local_files.source_kind from 'adopt' → 'url'."""
    db_path = lost_override_fixture
    from app.core.recovery_v55 import maybe_recover_lost_user_overrides
    stats = maybe_recover_lost_user_overrides(db_path)
    assert stats["user_overrides_inserted"] >= 1, (
        "v1.18.10: walker must INSERT user_overrides for the "
        "orphan whose SET URL event survived in the events "
        "table"
    )
    assert stats["local_files_reclassified"] >= 1, (
        "v1.18.10: walker must reclassify local_files."
        "source_kind from 'adopt' → 'url' on the recovered row"
    )
    # Verify the data state.
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        uo = conn.execute(
            "SELECT youtube_url, section_id FROM user_overrides "
            "WHERE media_type='movie' AND tmdb_id=-22"
        ).fetchone()
    assert uo is not None
    assert uo["youtube_url"] == (
        "https://www.youtube.com/watch?v=LJuJNw6oayg"
    )
    assert uo["section_id"] == "1"
    # local_files reclassified.
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lf = conn.execute(
            "SELECT source_kind FROM local_files "
            "WHERE media_type='movie' AND tmdb_id=-22 "
            "  AND section_id='1'"
        ).fetchone()
    assert lf["source_kind"] == "url", (
        f"v1.18.10: local_files.source_kind must be 'url' "
        f"post-recovery; got {lf['source_kind']}"
    )


def test_walker_self_gates_on_second_run(lost_override_fixture):
    """The walker stamps `recovery_user_overrides_done_at` so
    the second invocation is a no-op (zero new inserts)."""
    db_path = lost_override_fixture
    from app.core.recovery_v55 import maybe_recover_lost_user_overrides
    stats1 = maybe_recover_lost_user_overrides(db_path)
    assert stats1["detected"] is True
    stats2 = maybe_recover_lost_user_overrides(db_path)
    assert stats2["user_overrides_inserted"] == 0, (
        "v1.18.10: second run must short-circuit via marker"
    )


def test_walker_does_not_clobber_existing_user_override(
    lost_override_fixture,
):
    """If a user_overrides row ALREADY exists for an orphan
    (e.g., user re-SET URL post-bug, or the walker re-runs
    after the marker is cleared), the walker must NOT overwrite
    it with the older URL from events."""
    db_path = lost_override_fixture
    # Seed an existing user_overrides with a DIFFERENT URL
    # (a later SET URL the user did manually).
    ts = "2026-05-19T23:52:03"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, youtube_url, "
            "   set_at, set_by, section_id) "
            "VALUES ('movie', -22, "
            "        'https://www.youtube.com/watch?v=NEW_URL', "
            "        ?, 'admin', '1')",
            (ts,),
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_recover_lost_user_overrides
    maybe_recover_lost_user_overrides(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        uo = conn.execute(
            "SELECT youtube_url FROM user_overrides "
            "WHERE media_type='movie' AND tmdb_id=-22 "
            "  AND section_id='1'"
        ).fetchone()
    assert uo["youtube_url"] == (
        "https://www.youtube.com/watch?v=NEW_URL"
    ), (
        "v1.18.10: existing user_overrides must NOT be "
        "overwritten by the historical URL from events — the "
        "user's later choice wins"
    )
