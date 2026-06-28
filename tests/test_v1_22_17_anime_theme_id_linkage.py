"""v1.22.17 — guid_tmdb → theme_id linkage at 4 sync.py detection sites.

AniDB anime (the bulk of the user's library) resolve to TVDB/AniDB GUIDs, so
`plex_items.guid_tmdb` stays NULL while `resolve_theme_ids` links
`plex_items.theme_id` (the v1.15.142 `_enqueue_download` pattern: guid_tmdb →
guid_imdb → title+year fallbacks). Four sync DETECTION sites still keyed strictly
on guid_tmdb, so they silently excluded every theme_id-only (anime cohort) row:

  - `in_plex`           — new_count + the new_theme_available prompt gate
  - `has_sidecar`       — M-row "has existing content" detection
  - `_plex_supplies_theme` — the v1.22.12 pure-P SUPPRESSION precondition
  - `_prune_stale_pending_updates` plex-presence — don't reap a live anime-agent prompt

Each now ALSO matches via theme_id linkage as an OR (superset: the guid_tmdb path
is preserved, so rows linked only that way never regress). When theme_id is
linked — the post-`resolve_theme_ids` steady state (resolve runs at sync.py:3751,
BEFORE the prune at :3820), orphan-promotion, or a re-detection — anime/legacy-agent rows are
finally recognized:
  - an M/U/A row with content gets its new_theme_available prompt;
  - a pure SRC=P row stays SUPPRESSED (v1.22.12 preserved, now for HAMA too);
  - a live HAMA new_theme_available prompt survives the end-of-sync prune.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import init_db
from app.core.sync import (
    SyncStats,
    _flush_sync_batch,
    _plex_supplies_theme,
    _prune_stale_pending_updates,
)

SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
TS = "2026-06-06T00:00:00"


# ── Helpers ──────────────────────────────────────────────────


def _make_db(tmp_path) -> Path:
    db = tmp_path / "motif.db"
    init_db(db)
    return db


def _section(conn, sid="1"):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, 'Anime', 'show', 1, 0, 'anime', 1, ?, ?)", (sid, TS, TS))


def _theme(conn, *, tid, mt, tmdb, title, url="https://y/watch?v=NEW"):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, ?, ?, ?, 'themoviedb', ?, ?, ?)",
        (tid, mt, tmdb, title, TS, TS, url))


def _anime_item(conn, *, rk, theme_id, plex_mt="show", tmdb_guid=None,
               has_theme=1, local_theme_file=0, verified=1, sid="1",
               title="Kino", year=2017):
    """A anime-agent-style plex_items row: guid_tmdb NULL by default, linked to a
    theme only via theme_id (the v1.15.142 resolve fallback)."""
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, local_theme_file,"
        " folder_path, plex_independent_theme, plex_theme_verified_ok,"
        " edition_key, first_seen_at, last_seen_at)"
        " VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, 0, ?, '', ?, ?)",
        (rk, sid, plex_mt, theme_id, tmdb_guid, title, year, has_theme,
         local_theme_file, f"/data/anime/{title}", verified, TS, TS))


def _pending(conn, *, mt, tmdb, kind="new_theme_available"):
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
        " edition_key, new_youtube_url, detected_at, decision, kind)"
        " VALUES (?, ?, '', '', 'https://y/watch?v=NEW', ?, 'pending', ?)",
        (mt, tmdb, TS, kind))


def _pending_exists(conn, mt, tmdb) -> bool:
    return conn.execute(
        "SELECT 1 FROM pending_updates WHERE media_type=? AND tmdb_id=?"
        "  AND kind='new_theme_available'", (mt, tmdb),
    ).fetchone() is not None


# ── Source pins: all 4 sites match theme_id linkage ──────────


def _slice(start_anchor: str, end_anchor: str) -> str:
    i = SYNC_PY.index(start_anchor)
    j = SYNC_PY.index(end_anchor, i)
    return SYNC_PY[i:j]


def test_in_plex_gate_matches_theme_id_linkage():
    # v1.23.89: in_plex now routes through _plex_title_present (index-friendly
    # split-EXISTS) instead of an inline OR-across-themes-JOIN, but the v1.22.17
    # theme_id-linkage semantics are preserved — the helper's 2nd EXISTS matches
    # pi.theme_id (= the just-upserted themes.id, resolved once per item).
    assert "in_plex = _plex_title_present(conn, _plex_mt, tmdb_id, _theme_row_id)" in SYNC_PY
    # theme_id is pre-resolved from the upserted themes row.
    assert "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?" in SYNC_PY
    # The v1.22.17 rationale marker survives.
    assert "v1.22.17: match by pi.theme_id linkage" in SYNC_PY
    # The helper itself matches BOTH the guid_tmdb path and the theme_id path.
    helper = _slice("def _plex_title_present(", "\n\n\ndef ")
    assert "guid_tmdb = ?" in helper
    assert "theme_id = ?" in helper


def test_has_sidecar_gate_matches_theme_id_linkage():
    # v1.23.89: BOTH has_sidecar sites (is_new + url_changed) route through the
    # helper with sidecar_only=True, preserving the theme_id-linkage match.
    assert SYNC_PY.count("has_sidecar = _plex_title_present(") == 2
    assert SYNC_PY.count("sidecar_only=True") >= 2
    helper = _slice("def _plex_title_present(", "\n\n\ndef ")
    # the sidecar variant gates the EXISTS on local_theme_file = 1.
    assert "local_theme_file = 1" in helper
    assert "guid_tmdb = ?" in helper
    assert "theme_id = ?" in helper


def test_plex_supplies_helper_matches_theme_id_linkage():
    block = _slice("def _plex_supplies_theme(", "\n\n\ndef ")
    assert "LEFT JOIN themes t ON t.id = pi.theme_id" in block
    assert "pi.guid_tmdb = ?" in block
    assert "t.tmdb_id = ?" in block and "t.media_type = ?" in block


def test_prune_plex_presence_matches_theme_id_linkage():
    block = _slice("def _prune_stale_pending_updates(", "if pruned:")
    assert "LEFT JOIN themes t ON t.id = pi.theme_id" in block
    assert "pi.guid_tmdb = pending_updates.tmdb_id" in block
    assert "t.tmdb_id = pending_updates.tmdb_id" in block
    assert "t.media_type = pending_updates.media_type" in block


# ── _plex_supplies_theme: the v1.22.12 suppression precondition ──


def test_plex_supplies_true_for_theme_id_linked_anime_p_row(tmp_path):
    """A pure SRC=P HAMA row (guid_tmdb NULL, theme_id linked, has_theme=1,
    no motif content) must be recognized as Plex-supplied — pre-fix the
    guid_tmdb-only query returned False, so v1.22.12 never suppressed it."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, mt="tv", tmdb=900, title="Kino")
        _anime_item(conn, rk="rk-anime-p", theme_id=1, has_theme=1,
                   local_theme_file=0)
        conn.commit()
        assert _plex_supplies_theme(conn, "tv", 900) is True, (
            "v1.22.17: a theme_id-linked HAMA P-row must read as Plex-supplied "
            "(guid_tmdb is NULL) so v1.22.12 can suppress its new_theme prompt"
        )


def test_plex_supplies_still_true_for_guid_tmdb_only_row(tmp_path):
    """Regression guard: a classic guid_tmdb-linked P-row (theme_id NULL)
    still reads Plex-supplied — the OR keeps the legacy path."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_tmdb, title, year, has_theme, local_theme_file,"
            " folder_path, plex_independent_theme, plex_theme_verified_ok,"
            " edition_key, first_seen_at, last_seen_at)"
            " VALUES ('rk-g','1','show',NULL,900,'Kino',2017,1,0,"
            " '/data/anime/Kino',0,1,'',?,?)", (TS, TS))
        conn.commit()
        assert _plex_supplies_theme(conn, "tv", 900) is True


def test_plex_supplies_false_when_motif_owns_content(tmp_path):
    """A HAMA row with a local_files entry is NOT pure-P — motif owns it,
    so the helper must stay False even though theme_id is linked."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, mt="tv", tmdb=900, title="Kino")
        _anime_item(conn, rk="rk-anime-a", theme_id=1, has_theme=1,
                   local_theme_file=0)
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, file_path, source_kind, source_video_id,"
            " downloaded_at)"
            " VALUES ('tv',900,'1','','tv/900/theme.mp3','adopt','NEW',?)",
            (TS,))
        conn.commit()
        assert _plex_supplies_theme(conn, "tv", 900) is False


# ── Prune: a live anime-agent prompt survives via theme_id linkage ──


def test_prune_preserves_theme_id_linked_anime_new_theme(tmp_path):
    """The end-of-sync prune (runs AFTER resolve_theme_ids links theme_id)
    must NOT reap a HAMA new_theme_available row whose only plex_items link
    is theme_id. Pre-fix the guid_tmdb-only presence check failed → the
    fresh prompt was reaped on the same sweep."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, mt="tv", tmdb=900, title="Kino")
        _anime_item(conn, rk="rk-anime-p", theme_id=1, has_theme=1,
                   local_theme_file=0)
        _pending(conn, mt="tv", tmdb=900)
        conn.commit()

    _prune_stale_pending_updates(db)

    with sqlite3.connect(db) as conn:
        assert _pending_exists(conn, "tv", 900), (
            "v1.22.17: a theme_id-linked HAMA new_theme_available row must "
            "survive the prune (pre-fix guid_tmdb-only presence reaped it)"
        )


def test_prune_still_reaps_orphan_new_theme(tmp_path):
    """Orphan-cleanup intent preserved: a new_theme_available row whose title
    is in NO plex_items (neither guid_tmdb nor theme_id) still prunes."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, mt="tv", tmdb=901, title="Gone")
        # no plex_items row for tmdb 901
        _pending(conn, mt="tv", tmdb=901)
        conn.commit()

    _prune_stale_pending_updates(db)

    with sqlite3.connect(db) as conn:
        assert not _pending_exists(conn, "tv", 901), (
            "v1.22.17: an orphan HAMA new_theme row (title left Plex) must "
            "still prune — the OR adds theme_id, it doesn't disable cleanup"
        )


# ── End-to-end: is_new branch via _flush_sync_batch ──────────
#
# To exercise in_plex + has_sidecar live, theme_id must already point at the
# (re-)appearing themes row at is_new time — the orphan-promotion / re-link
# state. A fresh sqlite connection defaults foreign_keys=OFF, so we seed the
# plex_items row pointing at themes id=1 BEFORE _flush_sync_batch inserts that
# themes row (first INSERT into an empty themes table → rowid 1); the dangling
# ref resolves the instant _upsert_theme creates the row.


def _drive_is_new(db, *, batch):
    _flush_sync_batch(
        db, batch, sync_ts=TS, enqueue_downloads=False,
        auto_place_override=None, auto_download_new_themes=False,
        stats=SyncStats())


def test_is_new_anime_m_row_gets_new_theme_prompt(tmp_path):
    """A HAMA M-row (sidecar on disk, guid_tmdb NULL, theme_id linked) hits
    the is_new branch: in_plex True (theme_id) + has_sidecar True (theme_id)
    → has_content True → the manual-prompt path writes new_theme_available."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:  # FKs OFF by default on a bare connect
        _section(conn)
        _anime_item(conn, rk="rk-m", theme_id=1, has_theme=1,
                   local_theme_file=1)  # M-row: sidecar present
        conn.commit()

    _drive_is_new(db, batch=[(
        "tv", 900,
        {"name": "Kino", "first_air_date": "2017-10-01",
         "youtube_theme_url": "https://y/watch?v=NEW",
         "youtube_theme_edited": "2026-06-01"},
        "themoviedb",
    )])

    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT id FROM themes WHERE media_type='tv' AND tmdb_id=900"
        ).fetchone()[0] == 1, "fresh themes INSERT should take rowid 1"
        assert _pending_exists(conn, "tv", 900), (
            "v1.22.17: a theme_id-linked HAMA M-row must surface a "
            "new_theme_available prompt (pre-fix in_plex/has_sidecar were "
            "guid_tmdb-only → the whole is_new branch was skipped)"
        )


def test_is_new_anime_pure_p_row_stays_suppressed(tmp_path):
    """v1.22.12 preserved for HAMA: a pure SRC=P row (has_theme=1, no sidecar,
    no motif content, theme_id linked) is recognized as Plex-supplied and the
    `elif plex_supplies and not has_content: pass` SUPPRESSES the prompt — no
    new_theme_available row is written."""
    db = _make_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _anime_item(conn, rk="rk-p", theme_id=1, has_theme=1,
                   local_theme_file=0)  # pure P: no sidecar, no content
        conn.commit()

    _drive_is_new(db, batch=[(
        "tv", 900,
        {"name": "Kino", "first_air_date": "2017-10-01",
         "youtube_theme_url": "https://y/watch?v=NEW",
         "youtube_theme_edited": "2026-06-01"},
        "themoviedb",
    )])

    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT id FROM themes WHERE media_type='tv' AND tmdb_id=900"
        ).fetchone()[0] == 1, "fresh themes INSERT should take rowid 1"
        assert not _pending_exists(conn, "tv", 900), (
            "v1.22.17: a HAMA pure-P row must STAY suppressed (v1.22.12) — now "
            "that _plex_supplies_theme recognizes it via theme_id, the pass "
            "branch fires instead of the manual-prompt write"
        )


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
