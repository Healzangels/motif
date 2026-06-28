"""v1.24.37 — the title_norm backfill probe rides an index, not a full scan.

The v1.24.30 backfill pre-pass runs at the END of every enum AND every sync. Its
probe `SELECT id, title FROM themes WHERE title_norm IS NULL ...` full-scanned
~50K themes each time (no index serves IS NULL — idx_themes_title_norm is partial
`WHERE title_norm IS NOT NULL`, so it EXCLUDES the NULL rows). The whole
NULL-title_norm cohort is plex_orphan (real themes get title_norm from the sync
importer), so scoping the probe to `upstream_source='plex_orphan'` rides the
existing idx_themes_orphan partial index: EXPLAIN goes SCAN → SEARCH. The
future-NULL self-heal is unchanged (a fresh orphan NULL still backfills).
"""
from __future__ import annotations

from pathlib import Path

from app.core import plex_enum
from app.core.db import get_conn, init_db
from app.core.normalize import normalize_title
from app.core.plex_enum import now_iso

NOW = now_iso()
PROBE = ("SELECT id, title FROM themes "
         "WHERE upstream_source = 'plex_orphan' AND title_norm IS NULL "
         "  AND title IS NOT NULL AND title != ''")


def _db(tmp_path):
    p = tmp_path / "m.db"
    init_db(p)
    return p


def _theme(c, *, tmdb, title, src, title_norm):
    tn = "NULL" if title_norm is None else "?"
    params = ["movie", tmdb, title, src, NOW, NOW]
    if title_norm is not None:
        params.append(title_norm)
    c.execute(
        f"INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
        f" last_seen_sync_at, first_seen_sync_at, title_norm) "
        f"VALUES (?, ?, ?, ?, ?, ?, {tn})", params)


def test_probe_uses_orphan_index_not_full_scan(tmp_path):
    d = _db(tmp_path)
    with get_conn(d) as c:
        for i in range(8):
            _theme(c, tmdb=-(i + 1), title=f"Orphan {i}", src="plex_orphan",
                   title_norm=None)
        for i in range(8):
            _theme(c, tmdb=100 + i, title=f"Real {i}", src="imdb",
                   title_norm="real")
        c.commit()
        plan = " ".join(r[-1] for r in c.execute("EXPLAIN QUERY PLAN " + PROBE))
    assert "SCAN themes" not in plan, f"probe should not full-scan: {plan}"
    assert "idx_themes_orphan" in plan, f"probe should ride the orphan index: {plan}"


def test_future_orphan_null_still_self_heals(tmp_path):
    # The self-heal must survive the scoping change: a fresh plex_orphan with a
    # NULL title_norm still gets stamped on the next resolve.
    d = _db(tmp_path)
    with get_conn(d) as c:
        _theme(c, tmdb=-55, title="Spamalot", src="plex_orphan", title_norm=None)
        c.commit()
    plex_enum.resolve_theme_ids(d)
    with get_conn(d) as c:
        tn = c.execute("SELECT title_norm FROM themes WHERE tmdb_id=-55").fetchone()[0]
    assert tn == normalize_title("Spamalot")


def test_probe_scoped_in_source():
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "plex_enum.py").read_text()
    # the probe must carry the orphan scope (so it rides the index)
    assert "WHERE upstream_source = 'plex_orphan' AND title_norm IS NULL" in src
