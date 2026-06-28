"""v1.22.54 — collision merge (the user: "my override wins").

The v1.22.53 diagnostic showed all 8 collisions hold real tracking (no empty
husks): 6 SPLIT TRACKING + 2 row-still-on-duplicate. merge_orphan_collisions
consolidates each duplicate into its real-tmdb record:

  - user_overrides + local_files: ORPHAN WINS on same-slot collision (the
    user's deliberate choice is authoritative — restores the U-row state).
  - placements: LATEST placed_at wins (on-disk reality).
  - pending_updates + previous_urls: TARGET WINS (TDB-side state).
  - plex_items + scan_findings re-pointed BEFORE the husk delete (both FKs
    are ON DELETE SET NULL); moved rows get theme_id re-pointed too.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core import deorphan
from app.core.db import get_conn, init_db

_NOW = "2026-06-10T09:00:00"
_OLD = "2026-06-01T09:00:00"

_MAP = {"tt_a": {"tmdb_id": 1000, "kind": "movie"}}


class _FakeTMDB:
    def lookup_by_imdb(self, imdb_id):
        return _MAP.get(imdb_id)


def _db():
    d = Path(tempfile.mkdtemp()) / "motif.db"
    init_db(d)
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('s','Movies','movie',1,0,0,'m',?,?)", (_NOW, _NOW))
        c.commit()
    return d


def _theme(c, tmdb, *, imdb=None, upstream="themoviedb", title="T"):
    c.execute(
        "INSERT INTO themes (media_type,tmdb_id,imdb_id,title,upstream_source,"
        " first_seen_sync_at,last_seen_sync_at) VALUES ('movie',?,?,?,?,?,?)",
        (tmdb, imdb, title, upstream, _NOW, _NOW))
    return c.execute("SELECT id FROM themes WHERE media_type='movie' "
                     "AND tmdb_id=?", (tmdb,)).fetchone()[0]


def _seed_split_collision(d):
    """The Shape-A shape: real TDB theme at 1000 (own lf+pl, row linked) +
    orphan at -5 holding the manual theme (lf+pl+override) in the SAME
    section/edition slot — every table collides."""
    with get_conn(d) as c:
        target_id = _theme(c, 1000, title="Real")
        orphan_id = _theme(c, -5, imdb="tt_a", upstream="plex_orphan",
                           title="Dup")
        for tmdb, tid, path, vid, when in (
                (1000, target_id, "real/theme.mp3", "vid_tdb", _OLD),
                (-5, orphan_id, "dup/theme.mp3", "vid_manual", _NOW)):
            c.execute(
                "INSERT INTO local_files (media_type,tmdb_id,theme_id,"
                " section_id,edition_key,file_path,downloaded_at,"
                " source_video_id,source_kind) "
                "VALUES ('movie',?,?,'s','',?,?,?,'url')",
                (tmdb, tid, path, when, vid))
            c.execute(
                "INSERT INTO placements (media_type,tmdb_id,theme_id,"
                " section_id,edition_key,media_folder,placed_at,"
                " placement_kind) VALUES ('movie',?,?,'s','','/mf',?,"
                " 'hardlink')", (tmdb, tid, when))
        c.execute(
            "INSERT INTO user_overrides (media_type,tmdb_id,theme_id,"
            " section_id,edition_key,youtube_url,set_at) "
            "VALUES ('movie',-5,?,'s','','https://mychoice',?)",
            (orphan_id, _NOW))
        c.execute(
            "INSERT INTO pending_updates (media_type,tmdb_id,theme_id,"
            " section_id,edition_key,detected_at) "
            "VALUES ('movie',1000,?,'s','',?)", (target_id, _NOW))
        c.execute(
            "INSERT INTO plex_items (rating_key,section_id,media_type,"
            " guid_tmdb,theme_id,title,year,first_seen_at,last_seen_at) "
            "VALUES ('rk1','s','movie','1000',?,'T','2024',?,?)",
            (target_id, _NOW, _NOW))
        c.commit()
    return orphan_id, target_id


def test_split_collision_merges_with_override_winning():
    d = _db()
    orphan_id, target_id = _seed_split_collision(d)
    rep = deorphan.merge_orphan_collisions(d, _FakeTMDB(), dry_run=False)
    assert rep.merged == 1 and rep.errors == 0
    with get_conn(d) as c:
        # husk gone; only the real record remains
        assert [r[0] for r in c.execute(
            "SELECT tmdb_id FROM themes").fetchall()] == [1000]
        # override moved + re-pointed (orphan wins — it was the only one)
        uo = c.execute("SELECT tmdb_id, theme_id, youtube_url "
                       "FROM user_overrides").fetchall()
        assert [(r["tmdb_id"], r["theme_id"], r["youtube_url"])
                for r in uo] == [(1000, target_id, "https://mychoice")]
        # local_files collision: orphan's manual file beat the TDB one
        lf = c.execute("SELECT tmdb_id, theme_id, file_path, "
                       "source_video_id FROM local_files").fetchall()
        assert len(lf) == 1
        assert (lf[0]["tmdb_id"], lf[0]["theme_id"]) == (1000, target_id)
        assert lf[0]["source_video_id"] == "vid_manual"
        # placements collision: orphan's is NEWER → it won the slot
        pl = c.execute("SELECT tmdb_id, theme_id, placed_at "
                       "FROM placements").fetchall()
        assert len(pl) == 1
        assert (pl[0]["tmdb_id"], pl[0]["placed_at"]) == (1000, _NOW)
        # target's pending_update preserved (target wins)
        assert c.execute("SELECT COUNT(*) FROM pending_updates "
                         "WHERE tmdb_id=1000").fetchone()[0] == 1
        # library row still linked to the surviving record
        assert c.execute("SELECT theme_id FROM plex_items WHERE "
                         "rating_key='rk1'").fetchone()[0] == target_id
        assert len(c.execute("PRAGMA foreign_key_check").fetchall()) == 0


def test_row_on_duplicate_gets_repointed():
    """The Shape-B/C shape: library row follows the DUPLICATE — after the
    merge it must follow the surviving real record (no SET-NULL casualty)."""
    d = _db()
    with get_conn(d) as c:
        target_id = _theme(c, 1000, title="Real")
        orphan_id = _theme(c, -5, imdb="tt_a", upstream="plex_orphan")
        c.execute(
            "INSERT INTO plex_items (rating_key,section_id,media_type,"
            " guid_tmdb,theme_id,title,year,first_seen_at,last_seen_at) "
            "VALUES ('rk2','s','movie',NULL,?,'T','2024',?,?)",
            (orphan_id, _NOW, _NOW))
        c.commit()
    rep = deorphan.merge_orphan_collisions(d, _FakeTMDB(), dry_run=False)
    assert rep.merged == 1
    with get_conn(d) as c:
        assert c.execute("SELECT theme_id FROM plex_items WHERE "
                         "rating_key='rk2'").fetchone()[0] == target_id


def test_non_colliding_children_move_intact():
    """Different sections → no slot collision → everything moves."""
    d = _db()
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('s2','4K','movie',1,0,1,'m4k',?,?)", (_NOW, _NOW))
        target_id = _theme(c, 1000, title="Real")
        orphan_id = _theme(c, -5, imdb="tt_a", upstream="plex_orphan")
        c.execute(
            "INSERT INTO local_files (media_type,tmdb_id,theme_id,section_id,"
            " edition_key,file_path,downloaded_at,source_video_id,source_kind)"
            " VALUES ('movie',1000,?,'s','','a/t.mp3',?,'v1','url')",
            (target_id, _NOW))
        c.execute(
            "INSERT INTO local_files (media_type,tmdb_id,theme_id,section_id,"
            " edition_key,file_path,downloaded_at,source_video_id,source_kind)"
            " VALUES ('movie',-5,?,'s2','','b/t.mp3',?,'v2','url')",
            (orphan_id, _NOW))
        c.commit()
    rep = deorphan.merge_orphan_collisions(d, _FakeTMDB(), dry_run=False)
    assert rep.merged == 1
    with get_conn(d) as c:
        lf = c.execute("SELECT section_id, theme_id FROM local_files "
                       "WHERE tmdb_id=1000 ORDER BY section_id").fetchall()
        assert [(r["section_id"], r["theme_id"]) for r in lf] == [
            ("s", target_id), ("s2", target_id)]  # both, re-pointed


def test_older_orphan_placement_loses_slot():
    d = _db()
    with get_conn(d) as c:
        target_id = _theme(c, 1000, title="Real")
        orphan_id = _theme(c, -5, imdb="tt_a", upstream="plex_orphan")
        # target placement NEWER than the orphan's at the same slot
        c.execute(
            "INSERT INTO placements (media_type,tmdb_id,theme_id,section_id,"
            " edition_key,media_folder,placed_at,placement_kind) "
            "VALUES ('movie',1000,?,'s','','/mf',?,'hardlink')",
            (target_id, _NOW))
        c.execute(
            "INSERT INTO placements (media_type,tmdb_id,theme_id,section_id,"
            " edition_key,media_folder,placed_at,placement_kind) "
            "VALUES ('movie',-5,?,'s','','/mf',?,'hardlink')",
            (orphan_id, _OLD))
        c.commit()
    deorphan.merge_orphan_collisions(d, _FakeTMDB(), dry_run=False)
    with get_conn(d) as c:
        pl = c.execute("SELECT placed_at FROM placements").fetchall()
        assert [r["placed_at"] for r in pl] == [_NOW]  # newer target kept


def test_dry_run_mutates_nothing():
    d = _db()
    _seed_split_collision(d)
    rep = deorphan.merge_orphan_collisions(d, _FakeTMDB(), dry_run=True)
    assert rep.merged == 1  # would-merge count
    with get_conn(d) as c:
        assert sorted(r[0] for r in c.execute(
            "SELECT tmdb_id FROM themes").fetchall()) == [-5, 1000]
        assert c.execute("SELECT COUNT(*) FROM user_overrides "
                         "WHERE tmdb_id=-5").fetchone()[0] == 1


def test_idempotent_rerun_is_noop():
    d = _db()
    _seed_split_collision(d)
    deorphan.merge_orphan_collisions(d, _FakeTMDB(), dry_run=False)
    rep2 = deorphan.merge_orphan_collisions(d, _FakeTMDB(), dry_run=False)
    assert rep2.scanned == 0 and rep2.merged == 0


def test_rekeyable_orphan_left_for_rekey_walker():
    """No theme at the resolved target → not a collision; the merge must NOT
    touch it (the re-key walker owns that case)."""
    d = _db()
    with get_conn(d) as c:
        _theme(c, -5, imdb="tt_a", upstream="plex_orphan")
        c.commit()
    rep = deorphan.merge_orphan_collisions(d, _FakeTMDB(), dry_run=False)
    assert rep.scanned == 0 and rep.merged == 0
    with get_conn(d) as c:
        assert c.execute("SELECT COUNT(*) FROM themes "
                         "WHERE tmdb_id=-5").fetchone()[0] == 1


# ── endpoint ──────────────────────────────────────────────────


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_TMDB_API_KEY", "fakekey")
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    with get_conn(db) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('s','Movies','movie',1,0,0,'m',?,?)", (_NOW, _NOW))
        _theme(c, 1000, title="Real")
        _theme(c, -5, imdb="tt_a", upstream="plex_orphan")
        c.commit()
    app = create_app(settings)
    c = TestClient(app)
    c.headers["X-Authentik-Username"] = "testadmin"
    c.db = db  # type: ignore[attr-defined]
    yield c


def test_endpoint_dry_run_default(client, monkeypatch):
    monkeypatch.setattr("app.core.tmdb.TMDBClient.lookup_by_imdb",
                        _FakeTMDB.lookup_by_imdb)
    r = client.post("/api/admin/deorphan-merge-collisions")
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is True and j["dry_run"] is True and j["merged"] == 1
    with get_conn(client.db) as c:
        assert c.execute("SELECT COUNT(*) FROM themes").fetchone()[0] == 2


def test_endpoint_execute(client, monkeypatch):
    monkeypatch.setattr("app.core.tmdb.TMDBClient.lookup_by_imdb",
                        _FakeTMDB.lookup_by_imdb)
    r = client.post("/api/admin/deorphan-merge-collisions?dry_run=false")
    assert r.status_code == 200 and r.json()["merged"] == 1
    with get_conn(client.db) as c:
        assert [row[0] for row in c.execute(
            "SELECT tmdb_id FROM themes").fetchall()] == [1000]
