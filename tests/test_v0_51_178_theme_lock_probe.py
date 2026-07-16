"""v0.51.178 — read the theme field's lock flag instead of inferring it from a 200.

The open question this settles. v0.51.173 measured a Plex refresh failing to re-read a
changed theme sidecar. v0.51.177 concluded the theme field's LOCK was not the cause — but
based that entirely on an unlock PUT returning 200, and a 200 is not evidence the flag
moved. The unlock uses the metadata-endpoint shape while Plex's field-lock API
conventionally goes through the SECTION endpoint with type= + id=, so it may have been a
silent no-op the whole time. If it was, the lock explains the dead refresh, and
unlock+refresh is an untested ceiling-free path to all 2,821 themes (vs re-upload's 2,739
and ~2.7GB of POSTs).

So: read the flag. The CONTROL row — never normalized, never uploaded to — carries the
NATURAL lock state of a sidecar-ingested theme, which is what was true when v0.51.173 ran.
That flag alone decides the branch, which is why an unread flag (None) must never be
allowed to read as "unlocked" (class-9).

These drive the real endpoint against a fake Plex rather than pinning source text — the
phantom-guard lesson (v1.18.81), and the reason this file's centrepiece is
`test_a_200_that_does_not_move_the_flag_is_not_success`: that exact confusion is the bug
being fixed, so it gets an executable guard, not a comment.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: tmp_path / "themes"))
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://plex.test"))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "tok"))
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _seed(db, *, tmdb_id, rating_key, norm_state=None):
    """A hardlink-placed movie with a plex_items row."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?, 'movie', ?, ?, '1979', 'imdb', ?, ?)",
                  (tmdb_id, tmdb_id, f"Movie{tmdb_id}", NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, loudness_i, "
                  " loudness_tp, file_size, norm_state, norm_at) "
                  "VALUES ('movie', ?, '1', '', ?, ?, ?, 'vid', -5.0, -2.0, 900000, ?, ?)",
                  (tmdb_id, f"movies/{tmdb_id}/theme.mp3", f"sha{tmdb_id}", NOW,
                   norm_state, NOW if norm_state else None))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, "
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie', ?, '1', ?, '', 'hardlink', ?)",
                  (tmdb_id, f"/data/movies/{tmdb_id}", NOW))
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, title, "
                  " guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'movie', '1', ?, ?, '', 1, ?, ?)",
                  (rating_key, f"Movie{tmdb_id}", tmdb_id, NOW, NOW))
        c.commit()


def _stub_plex(monkeypatch, *, control_locked, audition_locked=False,
               shape_that_works="section", read_fails=False):
    """A fake Plex whose lock flag only moves for `shape_that_works` — every shape still
    returns HTTP 200, which is the whole point: the status code carries no information."""
    from app.web import api as api_mod
    state = {"locked": {"261711": audition_locked, "999": control_locked},
             "calls": []}

    class _FakePlex:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get_field_locks(self, *, rating_key):
            if read_fails:
                return {"ok": False, "http_status": None, "error": "transport: boom",
                        "locked_fields": None, "theme_locked": None}
            locked = state["locked"].get(str(rating_key), False)
            return {"ok": True, "http_status": 200, "error": None,
                    "locked_fields": (["theme"] if locked else []),
                    "theme_locked": locked}

        def set_theme_field_lock(self, *, rating_key, locked, shape="metadata",
                                 section_id=None, plex_type=None):
            state["calls"].append({"shape": shape, "locked": locked,
                                   "section_id": section_id, "plex_type": plex_type})
            if shape == shape_that_works:
                state["locked"][str(rating_key)] = locked
            return 200      # <- both shapes "succeed". Only the re-read tells the truth.

        def get_themes(self, *, rating_key):
            return {"ok": True, "http_status": 200, "error": None, "body": {
                "MediaContainer": {"Metadata": [
                    {"ratingKey": "metadata://themes/aaa", "selected": True},
                ]}}}

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)
    return state


# ── the centrepiece: a 200 is not evidence ───────────────────────────────

def test_a_200_that_does_not_move_the_flag_is_not_success(client, monkeypatch):
    """THE BUG BEING FIXED. v0.51.177's unlock returned 200 and was believed. Here BOTH
    shapes return 200 and only `section` actually moves the flag — the probe must name
    `section` off the re-read, never off the status."""
    c, db = client
    _seed(db, tmdb_id=1, rating_key="999")                       # control
    _seed(db, tmdb_id=2, rating_key="261711", norm_state="normalized")
    _stub_plex(monkeypatch, control_locked=True, audition_locked=True,
               shape_that_works="section")

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    st = b["shape_test"]
    assert st["working_shape"] == "section"
    tried = {t["shape"]: t for t in st["tried"]}
    # the metadata shape 200s and changes nothing — exactly v0.51.177's silent no-op.
    assert tried["metadata"]["http_status"] == 200
    assert tried["metadata"]["flag_moved"] is False


def test_when_no_shape_moves_the_flag_the_probe_says_so(client, monkeypatch):
    """If neither shape works, v0.51.177's unlock was a no-op and its 200 meant nothing.
    That has to be stated, not left as a silent absence (class-9)."""
    c, db = client
    _seed(db, tmdb_id=1, rating_key="999")
    _seed(db, tmdb_id=2, rating_key="261711", norm_state="normalized")
    _stub_plex(monkeypatch, control_locked=True, audition_locked=True,
               shape_that_works="nothing-works")

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["shape_test"]["working_shape"] is None
    assert all(t["flag_moved"] is False for t in b["shape_test"]["tried"])
    assert all(t["http_status"] == 200 for t in b["shape_test"]["tried"])
    assert "Fix the unlock first" in b["verdict"]


# ── the control row decides the branch ───────────────────────────────────

def test_locked_control_keeps_unlock_plus_refresh_alive(client, monkeypatch):
    """If sidecar rows are locked by default, the lock explains v0.51.173's dead refresh
    and unlock+refresh is the next thing to try — ceiling-free, all 2,821 themes."""
    c, db = client
    _seed(db, tmdb_id=1, rating_key="999")
    _seed(db, tmdb_id=2, rating_key="261711", norm_state="normalized")
    _stub_plex(monkeypatch, control_locked=True)

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["control"]["theme_locked"] is True
    # v0.51.179: the control samples rows, so per-row detail lives under rows[]
    assert b["control"]["rows"][0]["locked_fields"] == ["theme"]
    assert "UNLOCK + REFRESH is untested" in b["verdict"]


def test_unlocked_control_settles_it_on_reupload(client, monkeypatch):
    """If sidecar rows are NOT locked, the field was already unlocked when v0.51.173's
    refresh ran and it still did nothing → the lock was never the blocker."""
    c, db = client
    _seed(db, tmdb_id=1, rating_key="999")
    _seed(db, tmdb_id=2, rating_key="261711", norm_state="normalized")
    _stub_plex(monkeypatch, control_locked=False)

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["control"]["theme_locked"] is False
    assert "RE-UPLOAD is the propagation mechanism" in b["verdict"]
    assert "was never the blocker" in b["verdict"]


def test_an_unread_flag_is_never_a_verdict(client, monkeypatch):
    """A failed read returns theme_locked=None. Folding that into "unlocked" would let a
    measurement GAP decide the design — the exact class-9 shape."""
    c, db = client
    _seed(db, tmdb_id=1, rating_key="999")
    _seed(db, tmdb_id=2, rating_key="261711", norm_state="normalized")
    _stub_plex(monkeypatch, control_locked=True, read_fails=True)

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["control"]["theme_locked"] is None
    assert "NO verdict" in b["verdict"]
    assert "do not read this as a pass" in b["verdict"]


def test_control_is_a_row_motif_never_normalized(client, monkeypatch):
    """The control's whole value is being UNtouched — picking the normalized row would
    measure v0.51.177's own contamination and answer nothing."""
    c, db = client
    _seed(db, tmdb_id=1, rating_key="999")
    _seed(db, tmdb_id=2, rating_key="261711", norm_state="normalized")
    _stub_plex(monkeypatch, control_locked=False)

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert [r["rating_key"] for r in b["control"]["rows"]] == ["999"]
    assert b["audition"]["rating_key"] == "261711"
    # a metadata:// entry confirms it really is sidecar-ingested, not already uploaded to
    assert b["control"]["rows"][0]["selected_entry"].startswith("metadata://")
    assert b["control"]["rows"][0]["is_sidecar_entry"] is True


# ── self-restoring: the probe leaves the field as it found it ────────────

def test_the_flag_is_restored_with_the_shape_that_works(client, monkeypatch):
    """Leaving the operator's theme field flipped is a side effect nobody asked for. The
    restore must use the WORKING shape — restoring via the no-op shape would silently
    leave it flipped while reporting a clean restore."""
    c, db = client
    _seed(db, tmdb_id=1, rating_key="999")
    _seed(db, tmdb_id=2, rating_key="261711", norm_state="normalized")
    state = _stub_plex(monkeypatch, control_locked=True, audition_locked=True,
                       shape_that_works="section")

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    st = b["shape_test"]
    assert st["original_locked"] is True
    assert st["restored_locked"] is True, "the field must end as it started"
    assert st["restored_ok"] is True
    assert state["locked"]["261711"] is True
    # last write is the restore, and it uses the shape that actually works
    assert state["calls"][-1] == {"shape": "section", "locked": True,
                                  "section_id": "1", "plex_type": 1}


def test_section_shape_gets_the_plex_type_not_the_motif_media_type(client, monkeypatch):
    """Plex's section endpoint keys the lock by item TYPE (movie=1, show=2), not by
    motif's media_type string — the same translation class as _PLEX_MT_MAP."""
    c, db = client
    _seed(db, tmdb_id=1, rating_key="999")
    _seed(db, tmdb_id=2, rating_key="261711", norm_state="normalized")
    state = _stub_plex(monkeypatch, control_locked=True, audition_locked=True,
                       shape_that_works="section")

    c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH)
    section_calls = [x for x in state["calls"] if x["shape"] == "section"]
    assert section_calls, "the section shape must actually be tried"
    assert all(x["plex_type"] == 1 for x in section_calls)      # movie → 1


# ── the probe cannot strand an item ──────────────────────────────────────

def test_probe_never_deletes_or_uploads(client, monkeypatch):
    """Unlike delete+re-detect (which stranded rk=261711 with NO theme), this touches
    neither the selection nor the bytes — so it needs no recovery and no ceiling guard."""
    from app.web import api as api_mod
    i = (REPO / "app" / "web" / "api.py").read_text().index(
        '@app.post("/api/admin/loudness/theme-lock-probe")')
    src = (REPO / "app" / "web" / "api.py").read_text()
    body = src[i:src.index('@app.post("/api/admin/loudness/plex-redetect")', i)]
    for mutating in ("delete_theme(", "delete_collection_theme(",
                     "upload_theme(", "upload_collection_theme("):
        assert mutating not in body, f"the lock probe must not call {mutating}"
    assert "def _run():" in body and "run_in_threadpool(_run)" in body   # class-12


def test_missing_normalized_row_still_reports_the_control(client, monkeypatch):
    """The control answers the real question, so it must survive having no audition row
    to run the shape test against."""
    c, db = client
    _seed(db, tmdb_id=1, rating_key="999")
    _stub_plex(monkeypatch, control_locked=True)

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["ok"] is True
    assert b["control"]["theme_locked"] is True
    assert "error" in b["audition"]
    assert "UNLOCK + REFRESH is untested" in b["verdict"]


# ── the client method reads the flag out of Plex's Field list ────────────

def test_get_field_locks_parses_the_field_list():
    from app.core.plex import PlexClient, PlexConfig

    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return {"MediaContainer": {"Metadata": [{"Field": [
                {"name": "theme", "locked": True},
                {"name": "title", "locked": True},
                {"name": "summary"},
            ]}]}}

    p = PlexClient.__new__(PlexClient)
    p._headers = {}
    p._client = type("C", (), {"get": staticmethod(lambda *a, **k: _Resp())})()
    got = p.get_field_locks(rating_key="1")
    assert got["theme_locked"] is True
    assert got["locked_fields"] == ["theme", "title"]     # unlocked 'summary' excluded


def test_get_field_locks_returns_none_not_false_when_it_cannot_read():
    """Tristate. A read failure returning False would be indistinguishable from a genuine
    "not locked" and would silently decide the design."""
    from app.core.plex import PlexClient

    def _boom(*a, **k):
        raise RuntimeError("plex down")

    p = PlexClient.__new__(PlexClient)
    p._headers = {}
    p._client = type("C", (), {"get": staticmethod(_boom)})()
    got = p.get_field_locks(rating_key="1")
    assert got["theme_locked"] is None
    assert got["ok"] is False


def test_section_shape_refuses_without_the_params_it_needs():
    """Silently PUTting a section URL with type=None would 200 against the wrong thing and
    look like it worked."""
    from app.core.plex import PlexClient
    p = PlexClient.__new__(PlexClient)
    calls = []
    p._put = lambda path, params=None: calls.append(path) or 200
    assert p.set_theme_field_lock(rating_key="1", locked=False, shape="section") is None
    assert calls == []


# ── UI ───────────────────────────────────────────────────────────────────

def test_button_and_bind_exist():
    assert 'id="loud-theme-lock-btn"' in HTML
    assert "// IS THE THEME FIELD LOCKED?" in HTML
    assert "'/api/admin/loudness/theme-lock-probe'" in APP_JS


def test_bind_treats_an_unread_control_flag_as_a_failure():
    i = APP_JS.index("const lockBtn = document.getElementById('loud-theme-lock-btn')")
    src = APP_JS[i:APP_JS.index("// v0.51.177: the ceiling-free propagation candidate", i)]
    assert "theme_locked !== null" in src, (
        "an unread flag must not render as a green ✓ — it decides nothing")
