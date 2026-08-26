"""v1.22.72 (audit round 2, Batch C #1) — plex_enum/plex MED cluster.

(1) Reaper survivor-match collections translation: the v1.22.32
guid-direct branch translated mt with a two-way map (`'show' if tv
else 'movie'`) that didn't know collections — a lost collection's
surviving sibling (multi-section collections are the user's common
layout) never matched `pi.media_type='movie'` → false theme_lost; and
a movie sharing the numeric id could false-suppress a real loss. The
sidecar_db query nearby always had the CASE right.

(2) Failed collections fetch stamped enum success: the catch set
`collections = []` without counting the error, then the
contentChangedAt stamp armed the delta gate — so "will retry next
enum" was false and stale collection rows persisted ~24h behind one
WARN with the op finishing done/0 errors (the v1.15.34 class,
reopened on the collections walk).

(3) Reaper Tier-2 sidecar check hardcoded theme.mp3 while every other
probe accepts SIDECAR_AUDIO_EXTS — a manual theme.flac mis-tiered a
loss to no_fallback ("no recovery") with the recoverable sidecar on
disk. Now routes through find_theme_sidecar_path (also keeps the
v1.22.15 host→container translation; pin updated in
test_v1_22_15_path_translation_fixes.py).

(4) plex.py get_item_paths_bulk fanned _fetch_batch over 4 threads
sharing self._client — the one site violating the class-12
one-client-per-thread doctrine (api.py pools everywhere else). Now a
per-worker PlexClient via the pool initializer, closed on drain.
"""
from __future__ import annotations

import re
import sqlite3
import threading
from pathlib import Path
from unittest.mock import patch

from test_v1_14_74_content_changed_at_delta_gate import (  # noqa: F401
    _FakeClient, _seed_db)

from app.core import plex_enum
from app.core.plex import PlexClient, PlexConfig

REPO = Path(__file__).resolve().parent.parent
ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()


# ── (1) reaper plex_mt translation ───────────────────────────


def _extract_still_p_sql() -> str:
    anchor = ENUM_PY.index("still_p = conn.execute(")
    chunk = ENUM_PY[anchor:ENUM_PY.index(").fetchone()", anchor)]
    return "".join(re.findall(r'"([^"]*)"', chunk))


def test_reaper_plex_mt_knows_collections():
    i = ENUM_PY.index("plex_mt = (\"show\" if mt == \"tv\"")
    block = ENUM_PY[i:i + 200]
    assert '"collection" if mt == "collection"' in block, (
        "v1.22.72: the survivor-match translation must map "
        "collection→collection, not collection→movie"
    )


def test_surviving_collection_sibling_matches_extracted_sql(tmp_path):
    """Behavioral on the REAL SQL: a surviving unlinked collection row
    (has_theme=1, theme_id NULL) in an INCLUDED section must match under the
    collection translation — and demonstrably did NOT under the old 'movie'.
    v1.23.64: the SQL now also joins plex_sections AND gates on included=1, so a
    survivor in a DISABLED section must NOT match (it can't mask a real loss)."""
    db = tmp_path / "m.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE plex_items (rating_key TEXT PRIMARY KEY,"
            " section_id TEXT, media_type TEXT, guid_tmdb TEXT,"
            " has_theme INTEGER, theme_id INTEGER,"
            # v0.51.295: the still_p SQL gained the phantom-P qualifier.
            " plex_theme_verified_ok INTEGER)")
        conn.execute(
            "CREATE TABLE themes (id INTEGER PRIMARY KEY,"
            " media_type TEXT, tmdb_id INTEGER)")
        # v1.23.64: the still_p join needs plex_sections(included).
        conn.execute(
            "CREATE TABLE plex_sections (section_id TEXT PRIMARY KEY,"
            " included INTEGER)")
        conn.execute("INSERT INTO plex_sections VALUES ('9', 1)")
        conn.execute(
            "INSERT INTO plex_items VALUES"
            " ('c-4k', '9', 'collection', '123', 1, NULL, NULL)")
        conn.commit()
        sql = _extract_still_p_sql()
        # New behavior: collection→collection finds the survivor.
        hit = conn.execute(
            sql, ("collection", 123, "collection", 123)).fetchone()
        assert hit is not None
        # The pre-fix translation ('movie') missed it → false
        # theme_lost. Executable demo of the bug mechanism.
        miss = conn.execute(
            sql, ("collection", 123, "movie", 123)).fetchone()
        assert miss is None
        # v0.51.295: a HEAD-verified-dead survivor must not suppress either —
        # the phantom-P qualifier, demonstrated on the real SQL.
        conn.execute("UPDATE plex_items SET plex_theme_verified_ok = 0 "
                     "WHERE rating_key = 'c-4k'")
        conn.commit()
        assert conn.execute(
            sql, ("collection", 123, "collection", 123)).fetchone() is None
        conn.execute("UPDATE plex_items SET plex_theme_verified_ok = NULL "
                     "WHERE rating_key = 'c-4k'")
        conn.commit()
        # v1.23.64: disable the section — the survivor must no longer suppress
        # (a stale has_theme=1 row in a disabled section can't mask a real loss).
        conn.execute(
            "UPDATE plex_sections SET included = 0 WHERE section_id = '9'")
        disabled = conn.execute(
            sql, ("collection", 123, "collection", 123)).fetchone()
        assert disabled is None, (
            "v1.23.64: a survivor in a DISABLED section must not match")


# ── (2) failed collections fetch must not stamp ──────────────


class _CollectionsFailClient(_FakeClient):
    def enumerate_collections_for_section(self, **kwargs):
        raise RuntimeError("plex transport blew up")


def test_collections_failure_blocks_stamp_and_counts_error(
        tmp_path, monkeypatch):
    db_file = _seed_db(tmp_path, stored_cca=None)
    fake = _CollectionsFailClient(live_cca="1700000005")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    stats = plex_enum.run_plex_enum(db_file, cfg)
    assert stats["errors"] >= 1, (
        "v1.22.72: a failed collections fetch must surface in "
        "stats['errors'], not finish done/0"
    )
    with sqlite3.connect(db_file) as conn:
        row = conn.execute(
            "SELECT last_enum_content_changed_at FROM plex_sections "
            "WHERE section_id = '1'").fetchone()
    assert row[0] is None, (
        "v1.22.72: the stamp arms the skip gate — a failed collections "
        "fetch must leave it unstamped so the next enum retries"
    )


def test_successful_collections_pass_still_stamps(tmp_path, monkeypatch):
    """Mirror lock: the happy path keeps stamping (the v1.14.74 gate)."""
    db_file = _seed_db(tmp_path, stored_cca=None)
    fake = _FakeClient(live_cca="1700000005")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    plex_enum.run_plex_enum(db_file, cfg)
    with sqlite3.connect(db_file) as conn:
        row = conn.execute(
            "SELECT last_enum_content_changed_at FROM plex_sections "
            "WHERE section_id = '1'").fetchone()
    assert row[0] == "1700000005"


# ── (4) per-thread clients in get_item_paths_bulk ─────────────


class _EmptyResp:
    status_code = 200
    text = '{"MediaContainer": {"Metadata": []}}'


def test_get_item_paths_bulk_uses_per_thread_clients():
    calls: list[tuple[int, int]] = []
    lock = threading.Lock()

    def _capture_get(self, path, **kwargs):
        with lock:
            calls.append((id(self), threading.get_ident()))
        return _EmptyResp()

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", _capture_get):
        outer = PlexClient(cfg)
        rks = [str(i) for i in range(200)]  # 4 batches of 50
        out = outer.get_item_paths_bulk(rks, max_concurrent_batches=4)
        outer_id = id(outer)
    assert out == {}
    assert len(calls) == 4
    assert all(cid != outer_id for cid, _ in calls), (
        "v1.22.72: threaded batches must not share the outer client's "
        "httpx pool (class-12 one-client-per-thread doctrine)"
    )
    # Each worker thread sticks to ITS one client.
    by_thread: dict[int, set[int]] = {}
    for cid, tid in calls:
        by_thread.setdefault(tid, set()).add(cid)
    assert all(len(s) == 1 for s in by_thread.values())


def test_get_item_paths_bulk_serial_path_uses_self():
    """The serial branch (1 batch or concurrency 1) keeps using the
    caller's client — no spawn cost for the common small case."""
    calls: list[int] = []

    def _capture_get(self, path, **kwargs):
        calls.append(id(self))
        return _EmptyResp()

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", _capture_get):
        outer = PlexClient(cfg)
        outer.get_item_paths_bulk(
            [str(i) for i in range(120)], max_concurrent_batches=1)
        outer_id = id(outer)
    assert calls and all(c == outer_id for c in calls)
