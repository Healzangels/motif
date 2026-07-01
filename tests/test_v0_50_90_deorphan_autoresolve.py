"""v0.50.90 — auto-resolve imdb-bearing orphans when a TMDB key is present.

The de-orphan re-key walker (v1.22.49) existed only as a manual admin POST
(default dry_run), so orphans minted before a TMDB key was configured stayed
"tmdb: orphan" forever even after a valid key was added. This wires the walker
to fire in the background on:
  - boot (when a key is present),
  - a config save that sets/changes the TMDB key,
  - a TEST KEY that validates,
so orphans self-heal. Single-flight + no-op-fast when there's nothing to do.
Only the NON-destructive re-key runs automatically; the destructive collision
merge stays manual.
"""
from __future__ import annotations

import tempfile
import time
from pathlib import Path

from app.core import deorphan
from app.core.db import get_conn, init_db

REPO = Path(__file__).resolve().parent.parent
_NOW = "2026-07-01T09:00:00"


class _FakeTMDB:
    """Resolves tt900001 → a real movie tmdb; everything else unresolved."""
    def __init__(self, api_key, db_path):
        self.api_key = api_key

    def lookup_by_imdb(self, imdb_id):
        if imdb_id == "tt900001":
            return {"tmdb_id": 1295026, "kind": "movie"}
        return None


def _db():
    d = Path(tempfile.mkdtemp()) / "motif.db"
    init_db(d)
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('s','Movies','movie',1,0,0,'m',?,?)", (_NOW, _NOW))
    return d


def _orphan(d, tmdb, imdb, title="100 METERS"):
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,imdb_id,title,year,"
            " upstream_source,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('movie',?,?,?,'2025','plex_orphan',?,?)",
            (tmdb, imdb, title, _NOW, _NOW))


def _tmdb_of(d, imdb):
    with get_conn(d) as c:
        row = c.execute(
            "SELECT tmdb_id FROM themes WHERE imdb_id = ?", (imdb,)).fetchone()
    return row["tmdb_id"] if row else None


# ── the background helper ───────────────────────────────────────────────


def test_skips_without_api_key():
    d = _db()
    _orphan(d, -1, "tt900001")
    assert deorphan.resolve_orphans_in_background(
        d, api_key="", trigger="test") is False


def test_skips_when_no_resolvable_orphans():
    d = _db()
    # no orphan rows at all
    assert deorphan.resolve_orphans_in_background(
        d, api_key="k", trigger="test") is False


def test_skips_when_orphan_has_no_imdb():
    d = _db()
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,imdb_id,title,year,"
            " upstream_source,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('movie',-1,NULL,'X','2025','plex_orphan',?,?)",
            (_NOW, _NOW))
    assert deorphan.resolve_orphans_in_background(
        d, api_key="k", trigger="test") is False


def test_rekeys_orphan_in_background(monkeypatch):
    d = _db()
    _orphan(d, -1, "tt900001")
    monkeypatch.setattr("app.core.tmdb.TMDBClient", _FakeTMDB)

    started = deorphan.resolve_orphans_in_background(
        d, api_key="k", trigger="test")
    assert started is True

    # daemon thread — poll for the re-key to land
    deadline = time.time() + 5
    while time.time() < deadline:
        if _tmdb_of(d, "tt900001") == 1295026:
            break
        time.sleep(0.05)
    assert _tmdb_of(d, "tt900001") == 1295026, (
        "v0.50.90: the background pass must re-key the imdb-resolvable orphan "
        "to its real tmdb_id"
    )


def test_single_flight_skips_a_second_trigger(monkeypatch):
    d = _db()
    _orphan(d, -1, "tt900001")
    # hold the lock as if a run were already in flight
    assert deorphan._RESOLVE_LOCK.acquire(blocking=False)
    try:
        assert deorphan.resolve_orphans_in_background(
            d, api_key="k", trigger="test") is False
    finally:
        deorphan._RESOLVE_LOCK.release()


# ── wiring sites ────────────────────────────────────────────────────────


def test_boot_triggers_resolution():
    src = (REPO / "app" / "main.py").read_text()
    assert "resolve_orphans_in_background" in src
    assert 'trigger="boot"' in src


def test_tmdb_test_triggers_resolution_on_valid_key():
    src = (REPO / "app" / "web" / "api.py").read_text()
    i = src.index("async def api_tmdb_test(")
    body = src[i:i + 2400]
    assert 'trigger="tmdb_test"' in body
    # must be gated on a valid result
    assert "if ok:" in body


def test_config_save_triggers_resolution_on_tmdb_key_change():
    src = (REPO / "app" / "web" / "api.py").read_text()
    i = src.index("async def api_patch_config(")
    body = src[i:i + 6000]
    assert 'trigger="config_save"' in body
    assert '"tmdb_api_key" in body["plex"]' in body


def test_only_rekey_walker_auto_runs_not_the_destructive_merge():
    """The auto-path must call the non-destructive re-key walker, never the
    destructive collision-merge (which stays manual)."""
    src = (REPO / "app" / "core" / "deorphan.py").read_text()
    i = src.index("def resolve_orphans_in_background(")
    j = src.index("\ndef ", i + 10)
    body = src[i:j]
    assert "deorphan_imdb_resolvable(" in body
    assert "merge_orphan_collisions" not in body
