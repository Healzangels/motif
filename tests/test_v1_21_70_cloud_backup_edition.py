"""v1.21.70 — cloud/Plex backup edition-aware candidacy + writer.

the user: "I tried to do a Plex backup of the Extended Edition edition and it
failed" — the walker logged `identify_c1_rows: walked 0 rows, found 0 C1
targets`. Root cause: the C1 candidate query's NOT EXISTS placements /
local_files subqueries matched by (tmdb_id, section_id) but NOT edition_key,
so a SIBLING edition's placement (Theatrical/Sam were placed) disqualified
the whole title — the Extended P-row never became a candidate.

identify_c1_rows now scopes each NOT EXISTS by pi.edition_key and carries
edition_key into the target; backup_cloud_theme stages the file in the
edition's own {edition-<key>} folder and keys the local_files row to it.
"""
from __future__ import annotations

from types import SimpleNamespace

from app.core.db import get_conn, init_db
from app.core.cloud_theme_backup import identify_c1_rows
from app.core.editions import edition_key_for_folder
from app.core.events import now_iso


NOW = now_iso()
THEAT_RK = "167699"
EXT_RK = "777001"
THEAT_FOLDER = "/data/Movies/LotR (2001) {edition-Theatrical}"
EXT_FOLDER = "/data/Movies/LotR (2001) {edition-Extended Edition}"
THEAT_KEY = edition_key_for_folder(THEAT_FOLDER)   # 'theatrical'
EXT_KEY = edition_key_for_folder(EXT_FOLDER)        # 'extended edition'


def _settings(tmp_path):
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    return s


def _seed(db):
    """Theatrical is motif-placed; Extended is a clean P-row (Plex serves a
    cloud theme, motif has no local_files / placement of its own)."""
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',120,'LotR','2001','imdb',?,?,'u')",
            (NOW, NOW))
        tid = cur.lastrowid
        for rk, ek, fp in (
            (THEAT_RK, THEAT_KEY, THEAT_FOLDER),
            (EXT_RK, EXT_KEY, EXT_FOLDER),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1',"
                "'movie',?,120,'LotR','2001',?,?,1,?,?)",
                (rk, tid, ek, fp, NOW, NOW))
        # Only Theatrical is placed.
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " theme_id, media_folder, placed_at, placement_kind,"
            " plex_refreshed, provenance, edition_key) VALUES ('movie',"
            "120,'1',?,?,?, 'hardlink',1,'auto',?)",
            (tid, THEAT_FOLDER, NOW, THEAT_KEY))
        conn.commit()


_SHA1 = "deadbeef" * 5  # a valid 40-char hex sha1


def _plex_serving_cloud():
    return SimpleNamespace(get_themes=lambda rating_key: {
        "ok": True,
        "body": {"MediaContainer": {"Metadata": [
            {"ratingKey": f"metadata://themes/{_SHA1}", "selected": True},
        ]}},
    })


def _run(db, rk):
    with get_conn(db) as conn:
        return identify_c1_rows(
            conn, _plex_serving_cloud(),
            rks_scope=[rk], allow_existing_local=True,
            use_cursor=False, inter_call_sleep_s=0.0)


def test_extended_is_candidate_despite_placed_sibling(tmp_path):
    s = _settings(tmp_path)
    _seed(s.db_path)
    targets = _run(s.db_path, EXT_RK)
    assert len(targets) == 1, targets
    assert targets[0]["rating_key"] == EXT_RK
    assert targets[0]["edition_key"] == EXT_KEY, targets[0]


def test_placed_edition_is_not_a_candidate(tmp_path):
    """Counter-guard: the Theatrical edition IS placed, so its own scope
    still excludes it (the edition scoping cuts both ways)."""
    s = _settings(tmp_path)
    _seed(s.db_path)
    assert _run(s.db_path, THEAT_RK) == []
