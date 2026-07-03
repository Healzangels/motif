"""v1.21.32 — DOWNLOAD PLEX BACKUP works for no-TMDB-id rows (collections).

the user's repro: DOWNLOAD PLEX BACKUP on the A24 Films collection (no
ThemerrDB match → plex_items.guid_tmdb IS NULL) dead-ended with a
"nothing captured" message even though his probe proved Plex WAS
serving a selected (upload://) theme. Root cause: identify_c1_rows'
candidate query gates on `pi.guid_tmdb IS NOT NULL`, dropping the row
before classification — and even past that, the writer needs a tmdb_id
to key a backup to.

Fix (force path ONLY): drop the guid gate, and resolve-or-MINT a
synthetic negative orphan tmdb_id per row — the same plex_orphan
pattern the writer + UPLOAD MP3 already use. The bulk/strict walk keeps
the gate so we never mass-mint thousands of orphan ids.

Idempotency comes for free: minting stamps plex_items.theme_id, so a
SECOND force run resolves via theme_id back to the SAME synth id rather
than minting a duplicate.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock


REPO = Path(__file__).resolve().parent.parent
CTB_PY = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()

G40 = "c" * 40


def _seed_section(conn, *, section_id="1", type_="movie", themes_subdir="movies"):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, themes_subdir, "
        "   included, discovered_at, last_seen_at) "
        "VALUES (?, 'Collections', ?, 0, 0, ?, 1, "
        "        '2026-06-02', '2026-06-02')",
        (section_id, type_, themes_subdir))


def _seed_no_tdb_row(conn, *, rk, media_type="collection", section_id="1",
                     theme_id=None, title="A24 Films", year=None):
    """A P-row with NO TMDB id — guid_tmdb IS NULL, has_theme=1.

    `year` defaults to NULL; pass '' to mimic how Plex stores a
    collection with no release year (plex.py: year='' if missing)."""
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        "  guid_tmdb, theme_id, title, year, has_theme, "
        "  first_seen_at, last_seen_at) "
        "VALUES (?, ?, ?, NULL, ?, ?, ?, 1, "
        "        '2026-06-02', '2026-06-02')",
        (rk, section_id, media_type, theme_id, title, year))


def _selected_upload_themes(*, rating_key):
    return {"ok": True, "http_status": 200, "error": None,
            "body": {"MediaContainer": {"Metadata": [
                {"ratingKey": "upload://themes/" + G40, "selected": True},
            ]}}}


# ── source pin: the gate is force-conditional ────────────────


def test_guid_gate_is_force_conditional():
    assert 'if not force:\n        sql_parts.append("   AND pi.guid_tmdb IS NOT NULL ")' in CTB_PY
    # the candidate query must SELECT theme_id (the resolve path reads it)
    assert "pi.guid_tmdb, pi.theme_id, pi.media_type" in CTB_PY
    # the resolve-or-mint helper exists. v0.51.16 (audit #26): signature
    # went multi-line when the minted_out compensation param landed.
    assert "def _resolve_or_mint_tmdb_id(" in CTB_PY
    assert "conn, r, motif_mt: str, *, mint: bool," in CTB_PY


# ── force on a NULL-guid row mints + keys the target ─────────


def test_force_mints_tmdb_for_null_guid_row(tmp_path):
    from app.core.db import init_db
    from app.core.cloud_theme_backup import identify_c1_rows
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn)
        _seed_no_tdb_row(conn, rk="rk-a24")
        conn.commit()
        plex = MagicMock()
        plex.get_themes.side_effect = _selected_upload_themes

        # strict walk drops the row entirely (guid gate)
        strict = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
            rks_scope=["rk-a24"])
        assert strict == [], "non-force walk must exclude the NULL-guid row"
        assert conn.execute(
            "SELECT COUNT(*) FROM themes").fetchone()[0] == 0, (
            "strict walk must NOT mint anything")

        # force walk captures it + mints a synthetic id
        forced = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
            rks_scope=["rk-a24"], force=True)
        assert len(forced) == 1
        t = forced[0]
        assert t["entry_uri"] == "upload://themes/" + G40
        assert t["media_type"] == "collection"
        synth = t["guid_tmdb"]
        assert isinstance(synth, int) and synth < 0, (
            "the minted tmdb_id must be a negative synthetic orphan id")

        # a plex_orphan themes row was created with that id
        trow = conn.execute(
            "SELECT id, media_type, tmdb_id, title, upstream_source "
            "FROM themes WHERE tmdb_id = ?", (synth,)).fetchone()
        assert trow is not None
        assert trow["media_type"] == "collection"
        assert trow["upstream_source"] == "plex_orphan"
        assert trow["title"] == "A24 Films"

        # plex_items.theme_id was stamped to the new themes row
        pirow = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key = 'rk-a24'"
        ).fetchone()
        assert pirow["theme_id"] == trow["id"]


# ── v1.21.33: minted orphan stores year as NULL, never '' ────


def test_minted_orphan_year_normalized_to_null(tmp_path):
    """A collection carries year='' from Plex; the minted orphan row must
    store NULL, not '' — else sync.py's year-keyed orphan-promotion
    (WHERE year = ?) silently misses it ('' != NULL)."""
    from app.core.db import init_db
    from app.core.cloud_theme_backup import identify_c1_rows
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn)
        _seed_no_tdb_row(conn, rk="rk-a24", year="")  # Plex empty-string year
        conn.commit()
        plex = MagicMock()
        plex.get_themes.side_effect = _selected_upload_themes
        forced = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
            rks_scope=["rk-a24"], force=True)
        synth = forced[0]["guid_tmdb"]
        yr = conn.execute(
            "SELECT year FROM themes WHERE tmdb_id = ?", (synth,)).fetchone()
        assert yr["year"] is None, "empty-string year must normalize to NULL"


# ── idempotency: second force run resolves, doesn't re-mint ──


def test_second_force_run_resolves_via_theme_id(tmp_path):
    from app.core.db import init_db
    from app.core.cloud_theme_backup import identify_c1_rows
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn)
        _seed_no_tdb_row(conn, rk="rk-a24")
        conn.commit()
        plex = MagicMock()
        plex.get_themes.side_effect = _selected_upload_themes

        first = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
            rks_scope=["rk-a24"], force=True)
        synth1 = first[0]["guid_tmdb"]
        # re-read the row so the candidate query now sees the stamped
        # theme_id (mimics a fresh walk over the same row).
        second = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
            rks_scope=["rk-a24"], force=True)
        synth2 = second[0]["guid_tmdb"]
        assert synth1 == synth2, (
            "the second run must resolve the same synth id via theme_id")
        assert conn.execute(
            "SELECT COUNT(*) FROM themes WHERE upstream_source='plex_orphan'"
        ).fetchone()[0] == 1, "must NOT mint a second orphan row"


# ── mint allocates below the existing MIN ────────────────────


def test_mint_allocates_below_existing_min(tmp_path):
    from app.core.db import init_db
    from app.core.cloud_theme_backup import identify_c1_rows
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn)
        # an existing orphan at -3 for the same media_type
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', -3, 'Other Orphan', 'plex_orphan', "
            "        '2026-06-02', '2026-06-02')")
        _seed_no_tdb_row(conn, rk="rk-a24")
        conn.commit()
        plex = MagicMock()
        plex.get_themes.side_effect = _selected_upload_themes
        forced = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
            rks_scope=["rk-a24"], force=True)
        assert forced[0]["guid_tmdb"] == -4, "must allocate MIN(tmdb_id)-1"


# ── existing theme_id linkage is reused (no mint at all) ─────


def test_existing_theme_id_reused_without_minting(tmp_path):
    from app.core.db import init_db
    from app.core.cloud_theme_backup import identify_c1_rows
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn)
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', -9, 'Pre-linked', 'plex_orphan', "
            "        '2026-06-02', '2026-06-02')")
        pre_id = cur.lastrowid
        _seed_no_tdb_row(conn, rk="rk-a24", theme_id=pre_id)
        conn.commit()
        plex = MagicMock()
        plex.get_themes.side_effect = _selected_upload_themes
        forced = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
            rks_scope=["rk-a24"], force=True)
        assert forced[0]["guid_tmdb"] == -9, (
            "an already-linked row must resolve via theme_id")
        assert conn.execute(
            "SELECT COUNT(*) FROM themes").fetchone()[0] == 1, (
            "no new orphan row when theme_id already resolves")


# ── end-to-end: identify(force) → writer stages plex_cloud ───


def test_end_to_end_writes_plex_cloud_local_file(tmp_path):
    from app.core.db import init_db
    from app.core.cloud_theme_backup import (
        identify_c1_rows, backup_cloud_theme,
    )
    db = tmp_path / "t.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    init_db(db)
    served = b"\x49\x44\x33a24-served-theme-bytes"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_section(conn)
        _seed_no_tdb_row(conn, rk="rk-a24")
        conn.commit()
        plex = MagicMock()
        plex.get_themes.side_effect = _selected_upload_themes
        forced = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
            rks_scope=["rk-a24"], force=True)
        target = forced[0]
        synth = target["guid_tmdb"]

        plex._rk_path.return_value = "/library/metadata/rk/file"
        plex._headers = {}
        resp = MagicMock()
        resp.status_code = 200
        resp.content = served
        resp.text = ""
        resp.headers = {}
        plex._client.get.return_value = resp

        result = backup_cloud_theme(conn, target, themes_dir, plex)
        assert result["ok"] is True

        lf = conn.execute(
            "SELECT source_kind, last_place_attempt_reason, file_sha256 "
            "FROM local_files WHERE media_type='collection' AND tmdb_id=?",
            (synth,)).fetchone()
        assert lf is not None, "a local_files row keyed to the synth id"
        assert lf["source_kind"] == "plex_cloud"
        assert lf["last_place_attempt_reason"] == "backup_only"
        assert lf["file_sha256"] == hashlib.sha256(served).hexdigest()


def test_v1_21_32_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
