"""v1.22.25 — edition-correctness hardening for two re-triggerable recovery walkers.

Both are one-shot, runtime_settings-marker-gated (an operator can clear the
marker to re-run). Their placements joins were section-only, so on a multi-
edition title they bled across sibling editions. Unreachable on a normal install
(the marker is already set) — hardened anyway so a manual re-run on a multi-
edition library is correct.

#7 maybe_clear_plex_upload_indep_flag: cleared plex_independent_theme (+P) on
   EVERY edition's row in a section when ANY edition had a plex_upload. Now the
   placements join carries `p.edition_key = pi.edition_key`.
#5 maybe_cleanup_duplicate_placements: a hardlink/copy placement whose file is
   missing was deleted if a plex_upload existed in the same SECTION — even a
   different edition's. Now the self-join carries `pu.edition_key = p.edition_key`.

(#6 maybe_recover_post_v55_data_loss is intentionally NOT hardened: it's gated by
_detect_loss_pattern, which only the now-impossible v1.18.0 cascade produces, so
it cannot fire even via a manual re-run — and its rebuild edition-derivation is a
multi-column change, disproportionate for provably-dead code.)
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.recovery_v55 import (
    maybe_cleanup_duplicate_placements,
    maybe_clear_plex_upload_indep_flag,
)

REPO = Path(__file__).resolve().parent.parent
RECOVERY_PY = (REPO / "app" / "core" / "recovery_v55.py").read_text()
NOW = "2026-06-06T00:00:00"


def _theme(conn, tid=1, tmdb=500):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at) VALUES (?, 'movie', ?, 'M',"
        " 'imdb', ?, ?)", (tid, tmdb, NOW, NOW))


def _placement(conn, *, edition, folder, kind, placed_at=NOW, tmdb=500):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, edition_key,"
        " media_folder, placed_at, placement_kind, plex_rating_key,"
        " plex_refreshed, provenance) VALUES ('movie',?, '1',?,?,?,?, NULL, 1,"
        " 'auto')", (tmdb, edition, folder, placed_at, kind))


# ── #7: +P clear is edition-scoped ───────────────────────────


def test_indep_flag_clear_spares_sibling_edition(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _theme(conn)
        for rk, ed in (("rk-std", ""), ("rk-ext", "extended")):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, has_theme, local_theme_file,"
                " folder_path, plex_independent_theme, edition_key,"
                " first_seen_at, last_seen_at) VALUES (?, '1','movie',1,500,'M',"
                " 2012,1,0,?,1,?,?,?)",
                (rk, f"/data/m/M {ed}", ed, NOW, NOW))
        # Only the STANDARD edition has a plex_upload placement.
        _placement(conn, edition="", folder="", kind="plex_upload")
        conn.commit()

    maybe_clear_plex_upload_indep_flag(db)

    with sqlite3.connect(db) as conn:
        std = conn.execute("SELECT plex_independent_theme FROM plex_items"
                           " WHERE rating_key='rk-std'").fetchone()[0]
        ext = conn.execute("SELECT plex_independent_theme FROM plex_items"
                           " WHERE rating_key='rk-ext'").fetchone()[0]
    assert std == 0, "the standard edition's +P clears (it has the plex_upload)"
    assert ext == 1, (
        "v1.22.25 #7: the Extended edition's +P must be spared — it has no "
        "plex_upload of its own (pre-fix the section-only join cleared it)")


# ── #5: duplicate-placement cleanup is edition-scoped ────────


def test_dup_cleanup_spares_other_edition_hardlink(tmp_path):
    """An Extended hardlink whose file is missing must NOT be deleted just
    because the STANDARD edition has a plex_upload."""
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _theme(conn)
        # Extended hardlink — the FOLDER exists, only theme.mp3 is missing.
        # v0.51.241: it must be a real dir; an unreachable folder is now skipped
        # as "can't tell removed-movie from unmounted /data", which would make
        # this test pass without ever exercising the edition guard it exists for.
        _ext = tmp_path / "M {edition-Extended}"
        _ext.mkdir()
        _placement(conn, edition="extended", folder=str(_ext), kind="hardlink")
        # Standard edition's plex_upload, placed later.
        _placement(conn, edition="", folder="", kind="plex_upload",
                   placed_at="2026-06-07T00:00:00")
        conn.commit()

    maybe_cleanup_duplicate_placements(db)

    with sqlite3.connect(db) as conn:
        survived = conn.execute(
            "SELECT 1 FROM placements WHERE edition_key='extended'"
            " AND placement_kind='hardlink'").fetchone()
    assert survived is not None, (
        "v1.22.25 #5: the Extended hardlink must survive — its file is missing "
        "but the plex_upload that would 'supersede' it is a DIFFERENT edition")


def test_dup_cleanup_still_deletes_same_edition_dup(tmp_path):
    """Control: a same-edition hardlink (file missing) + plex_upload pair is
    still cleaned up — the edition guard didn't disable the walker."""
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _theme(conn)
        # v0.51.241: real dir — see the note in the sibling test above.
        _ext = tmp_path / "M {edition-Extended}"
        _ext.mkdir()
        _placement(conn, edition="extended", folder=str(_ext), kind="hardlink")
        _placement(conn, edition="extended", folder="", kind="plex_upload",
                   placed_at="2026-06-07T00:00:00")
        conn.commit()

    maybe_cleanup_duplicate_placements(db)

    with sqlite3.connect(db) as conn:
        survived = conn.execute(
            "SELECT 1 FROM placements WHERE edition_key='extended'"
            " AND placement_kind='hardlink'").fetchone()
    assert survived is None, (
        "v1.22.25 #5: a SAME-edition missing-file hardlink + plex_upload dup "
        "must still be cleaned up (the walker still works)")


# ── source pins ──────────────────────────────────────────────


def test_both_joins_carry_edition_key():
    assert "AND p.edition_key = pi.edition_key" in RECOVERY_PY, (
        "v1.22.25 #7: the indep-clear join must match edition")
    assert "AND pu.edition_key = p.edition_key" in RECOVERY_PY, (
        "v1.22.25 #5: the dup-cleanup self-join must match edition")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
