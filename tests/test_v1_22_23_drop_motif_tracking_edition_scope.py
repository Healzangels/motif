"""v1.22.23 — _drop_motif_tracking spares sibling editions (edition audit LOW #9).

PURGE/FORGET/UNMANAGE clear motif's "what to do with this title" decisions
(user_overrides + pending_updates) via _drop_motif_tracking when they reach the
last file-bearing section. That helper deleted TITLE-GLOBALLY (no edition), so
on a multi-edition title, purging the last file-bearing edition also wiped a
sibling edition's OWN per-edition override / pending decision (v1.21.81) — e.g. a
pure-P "Director's Cut" edition with a manually-set URL.

Fix: when called from an edition-scoped action (rating_key present), scope the
delete to `edition_key = '' OR edition_key = <acted>` — still clearing the
title-global '' decisions (the v1.12.57 clean-slate intent: a stale '' override
re-applies on the next download) + the acted edition's, but sparing OTHER
editions. acted_edition=None (DELETE, which nukes the whole title; the legacy
global UNMANAGE/PURGE branches) keeps the full title-wide wipe.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-06-06T00:00:00"


def _seed(db):
    from app.core.db import init_db
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at) VALUES"
            " ('movie',500,'Multi','imdb',?,?)", (NOW, NOW))
        for ed in ("", "extended", "directors"):
            conn.execute(
                "INSERT INTO user_overrides (media_type, tmdb_id, section_id,"
                " edition_key, youtube_url, intent, set_at, set_by)"
                " VALUES ('movie',500,'1',?,?, 'replace', ?, 'admin')",
                (ed, f"https://y/{ed or 'std'}", NOW))
            conn.execute(
                "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
                " edition_key, kind, new_youtube_url, decision, detected_at)"
                " VALUES ('movie',500,'1',?,'upstream_changed','https://y/n',"
                " 'declined',?)", (ed, NOW))
        conn.commit()


def _overrides(db):
    with sqlite3.connect(db) as conn:
        return {r[0] for r in conn.execute(
            "SELECT edition_key FROM user_overrides WHERE tmdb_id=500")}


def _pendings(db):
    with sqlite3.connect(db) as conn:
        return {r[0] for r in conn.execute(
            "SELECT edition_key FROM pending_updates WHERE tmdb_id=500")}


def test_acted_edition_spares_other_editions(tmp_path):
    """Purging the 'extended' edition clears '' + 'extended' but spares
    'directors' (a sibling edition's own override/pending)."""
    from app.web.api import _drop_motif_tracking
    db = tmp_path / "m.db"
    _seed(db)
    with sqlite3.connect(db) as conn:
        _drop_motif_tracking(conn, "movie", 500, acted_edition="extended")
        conn.commit()
    assert _overrides(db) == {"directors"}, (
        "v1.22.23: only the sibling 'directors' override must survive — '' "
        "(clean-slate) + 'extended' (acted) cleared")
    assert _pendings(db) == {"directors"}, (
        "v1.22.23: the sibling 'directors' pending decision must survive")


def test_standard_edition_acted_still_clears_global(tmp_path):
    """Acting on the standard ('') edition clears '' and spares non-'' siblings
    (proves the v1.12.57 '' clean-slate still fires)."""
    from app.web.api import _drop_motif_tracking
    db = tmp_path / "m.db"
    _seed(db)
    with sqlite3.connect(db) as conn:
        _drop_motif_tracking(conn, "movie", 500, acted_edition="")
        conn.commit()
    assert _overrides(db) == {"extended", "directors"}, (
        "v1.22.23: '' cleared, both non-'' editions spared")


def test_none_wipes_title_wide_legacy(tmp_path):
    """acted_edition=None (DELETE / legacy global branches) keeps the full
    title-wide wipe — clean slate for a whole-title nuke."""
    from app.web.api import _drop_motif_tracking
    db = tmp_path / "m.db"
    _seed(db)
    with sqlite3.connect(db) as conn:
        _drop_motif_tracking(conn, "movie", 500)
        conn.commit()
    assert _overrides(db) == set() and _pendings(db) == set(), (
        "v1.22.23: the default (None) must wipe every edition, unchanged")


# ── caller wiring source pins ────────────────────────────────


def test_unmanage_and_forget_pass_acted_edition():
    # UNMANAGE last_section
    assert "acted_edition=_unm_edition if rating_key else None" in API_PY, (
        "v1.22.23: UNMANAGE last_section must pass its resolved edition")
    # FORGET last_section
    assert "acted_edition=_forget_edition if rating_key else None" in API_PY, (
        "v1.22.23: FORGET last_section must pass its resolved edition")


def test_delete_and_legacy_branches_stay_title_global():
    # DELETE (whole-title nuke) + the two legacy global branches keep the bare
    # call (no acted_edition) → title-wide wipe.
    assert API_PY.count("_drop_motif_tracking(conn, media_type, tmdb_id)") == 3, (
        "v1.22.23: exactly the 3 title-global callers (DELETE + 2 legacy "
        "global branches) keep the un-scoped wipe")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
