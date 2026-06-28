"""v1.24.8 — reaper theme-lost fallback classifier was edition-blind.

Edition-audit finding (MED). The lost-theme candidate set is per-edition (DISTINCT
folder_path) and the notification names the specific lost edition, but the four-way
tier classifier (backup_signal / sidecar_db / other_fallback) matched a fallback by
(media_type, tmdb_id) with NO edition filter. On a multi-edition title that fully
lost its Plex theme, a SIBLING edition's backup/sidecar got advertised as the lost
edition's → the operator was told to PROMOTE/ADOPT the wrong edition's file. Now each
classifier is scoped to `edition_key IN (this_edition, '')` (prefer this edition,
allow the shared '' standard, EXCLUDE other named siblings). The dedupe key also
gained the edition so two editions losing their theme in one window each notify.

still_p (the "is the title still themed anywhere" suppressor) stays title-wide by
design (v1.22.32).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()
SEC = "1"
TID = 100
NOW = "2026-06-21T00:00:00+00:00"
THEAT = "/data/Movies/Troy (2004) {edition-theatrical}"
DCUT = "/data/Movies/Troy (2004) {edition-directors cut}"


def _item(rk, *, has_theme, folder):
    from app.core.plex import PlexLibraryItem
    return PlexLibraryItem(
        rating_key=rk, section_id=SEC, media_type="movie",
        title="Troy", year="2004", guid_imdb=None, guid_tmdb=TID,
        guid_tvdb=None, folder_path=folder, has_theme=has_theme,
        plex_theme_uri="",
    )


def _seed(tmp_path, monkeypatch, *, backup_edition):
    """Theatrical (S1, themed) is the reap candidate; a sibling S2 survives
    UNthemed so the title is fully lost (still_p None → classifier runs). A
    user_overrides backup sits at `backup_edition`."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    db = tmp_path / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        # need a themes row so the backup user_override FKs/keys resolve cleanly.
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at) VALUES (70,'movie',?,'Troy',"
            "'imdb',?,?)", (TID, NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
            " guid_tmdb, title, year, has_theme, local_theme_file, folder_path,"
            " edition_key, first_seen_at, last_seen_at) VALUES ('S1','1','movie',"
            "70,?,'Troy',2004,1,0,?,'theatrical',?,?)", (TID, THEAT, NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
            " guid_tmdb, title, year, has_theme, local_theme_file, folder_path,"
            " edition_key, first_seen_at, last_seen_at) VALUES ('S2','1','movie',"
            "70,?,'Troy',2004,1,0,?,'directors cut',?,?)", (TID, DCUT, NOW, NOW))
        # the ONLY fallback is a KEEP-AS-BACKUP override at `backup_edition`.
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, set_at,"
            " set_by, note, section_id, edition_key, intent)"
            " VALUES ('movie',?,'https://y',?, 'u','n','1',?,'backup')",
            (TID, NOW, backup_edition))
        conn.commit()
    calls = []
    import app.core.notify as _notify
    monkeypatch.setattr(
        _notify, "dispatch",
        lambda *a, **k: calls.append(k.get("event_kind")))
    return db, calls


def test_sibling_only_backup_does_not_make_lost_edition_backup_ready(
        tmp_path, monkeypatch):
    # backup lives on the Director's Cut; Theatrical (the reaped edition) has none.
    db, calls = _seed(tmp_path, monkeypatch, backup_edition="directors cut")
    from app.core import plex_enum
    # Plex returns only S2, now UNthemed → S1 reaped, title fully lost.
    plex_enum._upsert_items(
        db, [_item("S2", has_theme=False, folder=DCUT)], section_id=SEC)
    assert "plex_theme_lost" in calls, (
        "Theatrical lost + only a SIBLING (DC) backup → no_fallback for "
        "Theatrical, not a backup_ready advertising the wrong edition's file")
    assert "theme_lost_backup_ready" not in calls, (
        "v1.24.8: a sibling edition's backup must NOT be advertised as the "
        "lost edition's")


def test_own_edition_backup_still_classifies_backup_ready(tmp_path, monkeypatch):
    # control: the backup is at the Theatrical edition itself → backup_ready.
    db, calls = _seed(tmp_path, monkeypatch, backup_edition="theatrical")
    from app.core import plex_enum
    plex_enum._upsert_items(
        db, [_item("S2", has_theme=False, folder=DCUT)], section_id=SEC)
    assert "theme_lost_backup_ready" in calls, (
        "the lost edition's OWN backup must still surface backup_ready "
        "(no over-correction)")
    assert "plex_theme_lost" not in calls


def test_shared_standard_backup_still_recovers(tmp_path, monkeypatch):
    # a '' (standard/shared) backup must still cover a named-edition loss —
    # the IN(this, '') fallback, so we don't create false 'no recovery' alerts.
    db, calls = _seed(tmp_path, monkeypatch, backup_edition="")
    from app.core import plex_enum
    plex_enum._upsert_items(
        db, [_item("S2", has_theme=False, folder=DCUT)], section_id=SEC)
    assert "theme_lost_backup_ready" in calls
    assert "plex_theme_lost" not in calls


def test_classifiers_and_dedupe_edition_scoped_source():
    # backup_signal (3 subqueries) + other_fallback (3) = 6 edition scopes…
    assert PLEX_ENUM_PY.count("COALESCE(edition_key, '') IN (?, '')") >= 6
    # …plus sidecar_db's plex_items scope.
    assert "COALESCE(pi.edition_key, '') IN (?, '')" in PLEX_ENUM_PY
    assert "_cand_edition = edition_key_for_folder(" in PLEX_ENUM_PY
    assert 'f"plex_theme_lost:{tier}:{mt}:{tid}:{_disp_edition}"' in PLEX_ENUM_PY
