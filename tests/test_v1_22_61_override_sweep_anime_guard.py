"""v1.22.61 (audit round 2, Batch A #2) — the orphan user_overrides
sweep deleted anime overrides.

run_sync's post-sync sweep (v1.12.60) deletes user_overrides rows with
no theme presence. Its Plex-sidecar presence check matched
`pi.guid_tmdb = tmdb_id` ONLY — the LAST surviving guid-only site of
the v1.22.17 class (six sibling sites were widened with the theme_id
arm back then; this one was missed). anime rows link to themes
via `pi.theme_id` with `guid_tmdb` NULL, so their sidecars were
invisible to the guard: an anime M-row whose SET-URL download failed
or was cancelled (no local_files, no placements, job no longer
in-flight) had its override DELETED on the next sync — the v1.18.10
amplifier-sweep class (that incident ate 98 overrides).

Fix: the sweep is extracted to `_sweep_orphan_user_overrides` (the
`_prune_stale_pending_updates` pattern — directly testable) and the
sidecar check widened with `LEFT JOIN themes t ON t.id = pi.theme_id`
+ the OR theme_id arm, mirroring v1.22.17 exactly.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.sync import _sweep_orphan_user_overrides

from test_v1_22_17_anime_theme_id_linkage import (  # noqa: F401
    _anime_item, _section, _theme,
)

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
TS = "2026-06-11T00:00:00"


def _make_db(tmp_path) -> Path:
    db = tmp_path / "motif.db"
    init_db(db)
    return db


def _override(conn, *, mt, tmdb, sid="1"):
    conn.execute(
        "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url,"
        " set_at, section_id, edition_key)"
        " VALUES (?, ?, 'https://y/watch?v=USER', ?, ?, '')",
        (mt, tmdb, TS, sid))


def _override_exists(conn, mt, tmdb) -> bool:
    return conn.execute(
        "SELECT 1 FROM user_overrides WHERE media_type=? AND tmdb_id=?",
        (mt, tmdb)).fetchone() is not None


def test_sweep_preserves_theme_id_linked_anime_sidecar_override(tmp_path):
    """The repro: HAMA M-row (guid_tmdb NULL, theme_id linked,
    local_theme_file=1) + an override, NO local_files/placements/jobs.
    Pre-fix the sweep deleted the override; the sidecar must protect it."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, mt="tv", tmdb=900, title="Kino")
        _anime_item(conn, rk="rk-anime-m", theme_id=1, has_theme=1,
                   local_theme_file=1)
        _override(conn, mt="tv", tmdb=900)
        conn.commit()

    pruned = _sweep_orphan_user_overrides(db)

    with sqlite3.connect(db) as conn:
        assert _override_exists(conn, "tv", 900), (
            "v1.22.61: a theme_id-linked HAMA sidecar must protect the "
            "override from the orphan sweep (pre-fix it was DELETED)"
        )
    assert pruned == 0


def test_sweep_still_protects_guid_tmdb_sidecar_override(tmp_path):
    """Regression lock: the original guid_tmdb-matched sidecar
    protection keeps working after the widen."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, mt="tv", tmdb=901, title="Guid")
        _anime_item(conn, rk="rk-guid", theme_id=None, tmdb_guid=901,
                   has_theme=1, local_theme_file=1)
        _override(conn, mt="tv", tmdb=901)
        conn.commit()
    assert _sweep_orphan_user_overrides(db) == 0
    with sqlite3.connect(db) as conn:
        assert _override_exists(conn, "tv", 901)


def test_sweep_still_reaps_truly_orphan_override(tmp_path):
    """Cleanup intent preserved: an override with NO presence anywhere
    (no plex_items row at all) still prunes."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, mt="tv", tmdb=902, title="Gone")
        # no plex_items, no local_files, no placements, no jobs
        _override(conn, mt="tv", tmdb=902)
        conn.commit()
    assert _sweep_orphan_user_overrides(db) == 1
    with sqlite3.connect(db) as conn:
        assert not _override_exists(conn, "tv", 902)


def test_sweep_skips_row_without_sidecar_flag(tmp_path):
    """A theme_id-linked plex_items row with local_theme_file=0 does
    NOT protect — the guard is about sidecar presence, and the widen
    must not turn it into mere plex-presence."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, mt="tv", tmdb=903, title="NoSidecar")
        _anime_item(conn, rk="rk-nosc", theme_id=1, has_theme=1,
                   local_theme_file=0)
        _override(conn, mt="tv", tmdb=903)
        conn.commit()
    assert _sweep_orphan_user_overrides(db) == 1


def test_run_sync_calls_the_extracted_sweep():
    """The inline DELETE is gone from run_sync — the call site routes
    through the extracted (tested) function."""
    assert "stale_overrides = _sweep_orphan_user_overrides(db_path)" in SYNC_PY
    # The widened arm is present in the function.
    i = SYNC_PY.index("def _sweep_orphan_user_overrides")
    block = SYNC_PY[i:SYNC_PY.index("\ndef ", i + 10)]
    assert "LEFT JOIN themes t ON t.id = pi.theme_id" in block
    assert "pi.local_theme_file = 1" in block
