"""v0.51.355 — bulk ACCEPT ALL lands the url write the per-row ACCEPT always did.

the user, on v0.51.354, still looking at "5 UPD" on /tv that will not clear:

    Bulk-accepted 5 pending updates by admin (4 eager-flipped, 5 downloads queued, 1 as P-row backup in tv)
    Job 8611 permanently failed: no YouTube URL configured for this theme — SET URL or UPLOAD MP3 ...
    rollback: re-pended accept-update failure on tv/241609/2 + restored override

While a user override exists, sync deliberately WITHHOLDS the themes.youtube_url write — "the user's override
wins until they ACCEPT" (v1.14.55, v0.51.228) — so for an override row the url ThemerrDB is offering lives only
on the pending. ACCEPT is where that write is supposed to land, and the per-row endpoint has always done it
(v1.12.37). Bulk ACCEPT ALL never did: it deleted the override and queued a download that resolves override →
themes.youtube_url, found neither, failed permanently, and the rollback restored the override and re-pended the
row. Five rows, five failures, five rollbacks, for ever. The same mirror-drift class as v1.19.38 — two paths for
one action, one of them missing a step.

Second, smaller defect in the same loop: a urls_match row is accepted by flipping the local file's provenance,
and the confirm dialog says so ("For URL-match rows ... this is instant — no download"), but the code queued a
download anyway. Now it only does so when there is something to fetch: an unplaced row, or a P row taking a
backup.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
AUTH = {"X-Authentik-Username": "testadmin"}
TV = "?tab=tv&fourk=0"
MINE = "https://www.youtube.com/watch?v=mine00000001"
TDB_OTHER = "https://www.youtube.com/watch?v=tdb000000002"
TMDB = 241554


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings.db_path


def _seed(db, *, theme_url, new_url, kind="urls_match", override=MINE, placed=True, p_row=False):
    # NOTE: has_theme=1 with no placement IS a P row to motif (_not_p_row_sql), so an unplaced motif-owned
    # row must say Plex has no theme — otherwise the seed quietly tests the P branch instead.
    has_theme = 1 if (placed or p_row) else 0
    """A themed TV row carrying the operator's own url, with a pending update."""
    now = _now()
    with sqlite3.connect(db) as c:
        c.execute("INSERT OR IGNORE INTO plex_sections (section_id,title,type,is_anime,is_4k,themes_subdir,"
                  "included,discovered_at,last_seen_at) VALUES ('2','TV','show',0,0,'tv',1,?,?)", (now, now))
        tid = c.execute("INSERT INTO themes (media_type,tmdb_id,title,upstream_source,last_seen_sync_at,"
                        "first_seen_sync_at,youtube_url) VALUES ('tv',?,'Murderbot','imdb',?,?,?)",
                        (TMDB, now, now, theme_url)).lastrowid
        c.execute("INSERT INTO plex_items (rating_key,section_id,media_type,theme_id,guid_tmdb,title,year,"
                  "edition_key,folder_path,has_theme,local_theme_file,plex_independent_theme,"
                  "plex_theme_verified_ok,first_seen_at,last_seen_at)"
                  " VALUES ('rk1','2','show',?,?,'Murderbot',2025,'','/media/1',?,0,?,1,?,?)",
                  (tid, TMDB, has_theme, 1 if p_row else 0, now, now))
        if not p_row:
            c.execute("INSERT INTO local_files (media_type,tmdb_id,section_id,edition_key,file_path,"
                      "downloaded_at,source_video_id,provenance,source_kind)"
                      " VALUES ('tv',?,'2','','tv/1.mp3',?,'mine00000001','manual','url')", (TMDB, now))
            if placed:
                c.execute("INSERT INTO placements (media_type,tmdb_id,section_id,edition_key,media_folder,"
                          "placed_at,placement_kind,plex_refreshed,theme_present)"
                          " VALUES ('tv',?,'2','','/media/1',?,'hardlink',1,1)", (TMDB, now))
        if override:
            c.execute("INSERT INTO user_overrides (media_type,tmdb_id,section_id,youtube_url,set_by,set_at)"
                      " VALUES ('tv',?,'2',?,'testadmin',?)", (TMDB, override, now))
        c.execute("INSERT INTO pending_updates (media_type,tmdb_id,section_id,edition_key,kind,old_youtube_url,"
                  "new_youtube_url,decision,detected_at) VALUES ('tv',?,'2','',?,?,?,'pending',?)",
                  (TMDB, kind, MINE, new_url, now))
        c.commit()


def _state(db):
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        ovr = c.execute("SELECT youtube_url FROM user_overrides WHERE tmdb_id = ?", (TMDB,)).fetchone()
        pu = c.execute("SELECT decision FROM pending_updates WHERE tmdb_id = ?", (TMDB,)).fetchone()
        lf = c.execute("SELECT source_kind, provenance FROM local_files WHERE tmdb_id = ?", (TMDB,)).fetchone()
        jobs = c.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'download'").fetchone()[0]
    return {"override": ovr["youtube_url"] if ovr else None,
            "decision": pu["decision"] if pu else None,
            "local": (lf["source_kind"], lf["provenance"]) if lf else None,
            "downloads": jobs}


def _worker_would_resolve(db):
    """What worker.py reads when the job runs: the override if any, else the theme url (worker.py:~1736)."""
    with sqlite3.connect(db) as c:
        row = c.execute(
            "SELECT COALESCE((SELECT youtube_url FROM user_overrides WHERE media_type='tv' AND tmdb_id=?),"
            "                (SELECT youtube_url FROM themes WHERE media_type='tv' AND tmdb_id=?))", (TMDB, TMDB)
        ).fetchone()
    return row[0] if row else None


def _theme_url(db):
    with sqlite3.connect(db) as c:
        return c.execute("SELECT youtube_url FROM themes WHERE tmdb_id = ?", (TMDB,)).fetchone()[0]


# ── the operator's five ──────────────────────────────────────


def test_the_row_is_still_offered_because_thermerrdb_does_have_a_url(admin_client):
    """Sanity on the premise: themes.youtube_url is empty ONLY because sync withheld it behind the override.
    The pending carries the url, so the pill is right to offer the row — the accept was the broken half."""
    client, db = admin_client
    _seed(db, theme_url="", new_url=MINE)
    assert client.get(f"/api/updates/count{TV}", headers=AUTH).json()["pending"] == 1


def test_bulk_accept_lands_the_withheld_url_write(admin_client):
    client, db = admin_client
    _seed(db, theme_url="", new_url=MINE)
    client.post(f"/api/updates/accept-all{TV}", headers=AUTH)
    assert _theme_url(db) == MINE, "accepting IS the moment sync's withheld write lands"


def test_bulk_accept_never_leaves_a_download_with_no_source(admin_client):
    """The invariant the operator's log violated: whatever the accept queues, the worker must be able to
    resolve a url for it — override if one survives, else the theme url."""
    client, db = admin_client
    _seed(db, theme_url="", new_url=TDB_OTHER, kind="upstream_changed")
    body = client.post(f"/api/updates/accept-all{TV}", headers=AUTH).json()
    assert body["downloads_queued"] == 1, body
    assert _state(db)["downloads"] == 1
    assert _worker_would_resolve(db) == TDB_OTHER, "this is the read that answered nothing and failed the job"


def test_the_operators_url_match_rows_flip_and_queue_nothing(admin_client):
    """Their four eager-flipped rows: the file on disk came from that very url and is already placed, so the
    flip is the whole accept. The download that used to follow is what failed and rolled the flip back."""
    client, db = admin_client
    _seed(db, theme_url="", new_url=MINE)
    body = client.post(f"/api/updates/accept-all{TV}", headers=AUTH).json()
    assert (body["accepted"], body["eager_flipped"], body["downloads_queued"]) == (1, 1, 0), body
    after = _state(db)
    assert after["local"] == ("themerrdb", "auto"), "the row is TDB-managed now"
    assert (after["override"], after["downloads"]) == (None, 0), after
    assert after["decision"] != "pending", after
    assert _theme_url(db) == MINE


def test_both_accept_paths_leave_the_same_row_behind(admin_client, tmp_path_factory):
    """The drift guard: the per-row endpoint wrote the url (v1.12.37) and bulk did not. Same seed, both paths,
    same resulting row — so the next one to move has to move both."""
    client, db = admin_client
    _seed(db, theme_url="", new_url=TDB_OTHER, kind="upstream_changed")
    client.post(f"/api/updates/accept-all{TV}", headers=AUTH)
    bulk = (_theme_url(db), _state(db)["override"], _state(db)["downloads"])

    # a second, identical instance accepted through the per-row endpoint
    import sqlite3 as _s
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    d2 = tmp_path_factory.mktemp("perrow")
    st = Settings(config_dir=d2, data_dir=d2 / "data")
    st._cfg.paths.themes_dir = str(d2 / "themes")
    init_db(st.db_path); init_auth_schema(st.db_path)
    create_admin(st.db_path, username="testadmin", password="testpassword")
    _seed(st.db_path, theme_url="", new_url=TDB_OTHER, kind="upstream_changed")
    c2 = TestClient(create_app(st))
    assert c2.post(f"/api/updates/tv/{TMDB}/accept?section_id=2&rating_key=rk1", headers=AUTH).status_code == 200
    with _s.connect(st.db_path) as c:
        per_row = (c.execute("SELECT youtube_url FROM themes WHERE tmdb_id=?", (TMDB,)).fetchone()[0],
                   (c.execute("SELECT youtube_url FROM user_overrides WHERE tmdb_id=?", (TMDB,)).fetchone()
                    or [None])[0],
                   c.execute("SELECT COUNT(*) FROM jobs WHERE job_type='download'").fetchone()[0])
    assert bulk == per_row, f"bulk {bulk} vs per-row {per_row}"


# ── the branches that still need their download ──────────────


def test_an_unplaced_url_match_row_still_downloads(admin_client):
    """The skip is only safe when the file is already in Plex's folder — otherwise the download is what puts
    it there, and skipping would leave the row accepted with nothing playing."""
    client, db = admin_client
    _seed(db, theme_url="", new_url=MINE, placed=False)
    body = client.post(f"/api/updates/accept-all{TV}", headers=AUTH).json()
    assert (body["eager_flipped"], body["downloads_queued"]) == (1, 1), body
    assert _worker_would_resolve(db) == MINE


def test_a_p_row_still_takes_its_backup_download(admin_client):
    """SRC=P: Plex serves its own theme and motif holds no file, so the download IS the accept (v1.19.33)."""
    client, db = admin_client
    _seed(db, theme_url="", new_url=TDB_OTHER, kind="upstream_changed", p_row=True)
    body = client.post(f"/api/updates/accept-all{TV}", headers=AUTH).json()
    assert body["p_backup"] == 1, body
    assert _state(db)["downloads"] == 1
    assert _worker_would_resolve(db) == TDB_OTHER, "the backup needs the url too"


def test_a_row_with_no_override_is_unchanged(admin_client):
    """Without an override sync writes themes.youtube_url itself, so this path never had the bug — pin that
    the new write doesn't disturb it."""
    client, db = admin_client
    _seed(db, theme_url=TDB_OTHER, new_url=TDB_OTHER, kind="upstream_changed", override=None)
    body = client.post(f"/api/updates/accept-all{TV}", headers=AUTH).json()
    assert (body["accepted"], body["eager_flipped"], body["downloads_queued"]) == (1, 0, 1), body
    assert _theme_url(db) == TDB_OTHER


def test_a_pending_with_no_url_anywhere_is_still_not_offered(admin_client):
    """v0.51.352 stands: with nothing on either side there is nothing to apply, and no write would help."""
    client, db = admin_client
    _seed(db, theme_url=None, new_url=None, kind="new_theme_available")
    assert client.get(f"/api/updates/count{TV}", headers=AUTH).json()["pending"] == 0
    assert client.post(f"/api/updates/accept-all{TV}", headers=AUTH).json()["accepted"] == 0
    assert _state(db)["override"] == MINE
