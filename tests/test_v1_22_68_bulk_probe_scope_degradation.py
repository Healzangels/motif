"""v1.22.68 (audit round 2, Batch B #4) — a >499-row selection turned
PROBE TDB into a cooldown-bypassed full-library sweep.

_bulk_probe_tdb_run caps the IN-tuples scope at 499 pairs (SQLite
param budget); a larger selection "falls through to a global probe"
(scope_clause stays ''). But the cooldown branch keyed on the
ORIGINAL scope_items list — so the fallthrough ALSO bypassed the 24h
cooldown: select-all + PROBE hammered every themed row in the library
back-to-back (the "previous probe gone bad → 1842 red rows" incident
class), behind an innocuous log.info. The v1.15.1 comment ("the 24h
cooldown filter is preserved either way") had been false since
v1.15.20 added the bypass.

Fix: the bypass keys on scope_CLAUSE (selection actually applied);
the degraded >499 path keeps the cooldown, logs at WARNING, and the
activity note names the degradation.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
TS_FRESH = "2026-06-11T00:00:00+00:00"  # "now" — inside any cooldown


def _seed_two_probed_themes(db):
    with sqlite3.connect(db) as conn:
        for tid in (1, 2):
            conn.execute(
                "INSERT INTO themes (media_type, tmdb_id, title,"
                " upstream_source, last_seen_sync_at, first_seen_sync_at,"
                " youtube_url, last_probed_at)"
                " VALUES ('movie', ?, 'X', 'themoviedb', ?, ?,"
                " 'https://yt/x', datetime('now'))", (tid, TS_FRESH, TS_FRESH))
        conn.commit()


def _settings(db):
    return SimpleNamespace(cookies_file=None, db_path=db)


def test_oversize_selection_honors_cooldown(tmp_path):
    """500 scope pairs (over the 499 cap) against freshly-probed rows:
    the degraded global probe must find 0 rows (cooldown applied) and
    never touch yt-dlp. Pre-fix: cooldown bypassed → both rows probed."""
    from app.web import api as apimod
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_two_probed_themes(db)
    calls = []
    with patch("app.core.downloader.probe_youtube_url",
               side_effect=lambda *a, **k: calls.append(1)):
        apimod._bulk_probe_tdb_run(
            db, _settings(db),
            scope_items=[("movie", i) for i in range(500)],
        )
    assert calls == [], (
        "v1.22.68: the degraded >499 path must honor the 24h cooldown — "
        "freshly-probed rows must not be re-probed"
    )
    with sqlite3.connect(db) as conn:
        st = conn.execute(
            "SELECT status FROM op_progress WHERE op_id='bulk-probe-tdb'"
        ).fetchone()
    assert st and st[0] == "done"


def test_small_selection_still_bypasses_cooldown(tmp_path):
    """Regression lock for v1.15.20: a ≤499 explicit selection still
    bypasses the cooldown (the deliberate-user-choice semantics)."""
    from app.web import api as apimod
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_two_probed_themes(db)
    probed = []
    with patch.object(apimod, "probe_youtube_url", create=True), \
         patch("app.core.downloader.probe_youtube_url",
               side_effect=lambda url, **k: probed.append(url) or None):
        apimod._bulk_probe_tdb_run(
            db, _settings(db),
            scope_items=[("movie", 1), ("movie", 2)],
        )
    assert len(probed) == 2, (
        "an explicit small selection must still re-probe fresh rows "
        f"(v1.15.20 bypass) — probed {probed}"
    )


def test_bypass_gate_keys_on_scope_clause():
    """Source pin: the cooldown-bypass branch keys on scope_clause
    (selection APPLIED), with a separate degraded branch for oversize
    selections that keeps the cooldown + names the degradation."""
    i = API_PY.index("def _bulk_probe_tdb_run(")
    body = API_PY[i:API_PY.index("\ndef ", i + 1)]
    assert "elif scope_clause:" in body
    assert "degraded to a GLOBAL probe" in body
