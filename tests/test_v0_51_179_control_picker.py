"""v0.51.179 — the control picker could never have worked on a real library.

v0.51.178 asked the right question (what is the NATURAL theme-lock state of a
sidecar-ingested row — i.e. what was true when v0.51.173's refresh failed?) and then
picked its control row like this:

    ... WHERE lf.norm_state IS NULL AND p.placement_kind='hardlink'
    ORDER BY lf.tmdb_id LIMIT 1

then looked up plex_items for that ONE row and gave up if it found nothing. On the
operator's library it returned "no untouched hardlink-placed row with a plex_items
rating_key" — and that was guaranteed, not unlucky: synthetic orphan ids are NEGATIVE
(adopt.py mints `MIN(tmdb_id) - 1`), so the lowest tmdb_id row is ALWAYS an orphan, and
an orphan by definition has no plex_items row. One shot, at the one row that could not
possibly resolve.

Three fixes, guarded here:
  1. Resolve the rating_key IN the JOIN — never pick-then-hope.
  2. Exclude synthetic orphans (tmdb_id > 0).
  3. SAMPLE several rows: one row cannot answer a library-wide question, and the sample
     also exposes contaminated rows (an upload:// entry means motif already pushed to it,
     so its lock says nothing about the natural state).

Plus: v0.51.178's single error string covered two different causes and so could not say
which had happened — the same "a gap that reads as an answer" shape this arc keeps hitting.
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


def _row(db, *, tmdb_id, rating_key=None, norm_state=None, kind="hardlink"):
    """rating_key=None ⇒ no plex_items row (an orphan, or simply unresolved)."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, loudness_i, "
                  " loudness_tp, file_size, norm_state, norm_at) "
                  "VALUES ('movie', ?, '1', '', ?, ?, ?, 'vid', -5.0, -2.0, 900000, ?, ?)",
                  (tmdb_id, f"movies/{tmdb_id}/theme.mp3", f"sha{tmdb_id}", NOW,
                   norm_state, NOW if norm_state else None))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, "
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie', ?, '1', ?, '', ?, ?)",
                  (tmdb_id, f"/data/movies/{tmdb_id}", kind, NOW))
        if rating_key is not None:
            c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, title, "
                      " guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
                      "VALUES (?, 'movie', '1', ?, ?, '', 1, ?, ?)",
                      (rating_key, f"Movie{tmdb_id}", tmdb_id, NOW, NOW))
        c.commit()


def _stub(monkeypatch, *, locks=None, entries=None):
    from app.web import api as api_mod
    locks = locks or {}
    entries = entries or {}

    class _FakePlex:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get_field_locks(self, *, rating_key):
            v = locks.get(str(rating_key), False)
            if v is None:
                return {"ok": False, "http_status": None, "error": "transport: boom",
                        "locked_fields": None, "theme_locked": None}
            return {"ok": True, "http_status": 200, "error": None,
                    "locked_fields": (["theme"] if v else []), "theme_locked": v}

        def set_theme_field_lock(self, *, rating_key, locked, shape="metadata",
                                 section_id=None, plex_type=None):
            if shape == "metadata":
                locks[str(rating_key)] = locked
            return 200

        def get_themes(self, *, rating_key):
            uri = entries.get(str(rating_key), "metadata://themes/aaa")
            return {"ok": True, "http_status": 200, "error": None, "body": {
                "MediaContainer": {"Metadata": [{"ratingKey": uri, "selected": True}]}}}

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)


# ── the bug: the orphan that ate the control ─────────────────────────────

def test_negative_tmdb_orphan_does_not_kill_the_control(client, monkeypatch):
    """THE v0.51.178 FAILURE, reproduced. A synthetic orphan (tmdb_id=-1) sorts FIRST and
    has no plex_items row. v0.51.178 picked it, found no rating_key, and reported "no
    untouched hardlink-placed row" while thousands of perfectly good rows sat behind it."""
    c, db = client
    _row(db, tmdb_id=-1, rating_key=None)          # the orphan that broke v0.51.178
    _row(db, tmdb_id=500, rating_key="500")        # a real, resolvable sidecar row
    _stub(monkeypatch, locks={"500": False})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert "error" not in b["control"], b["control"].get("error")
    assert [r["rating_key"] for r in b["control"]["rows"]] == ["500"]
    assert b["control"]["theme_locked"] is False


def test_a_row_without_a_plex_items_match_is_skipped_not_fatal(client, monkeypatch):
    """Resolve in the JOIN. Pick-then-hope means one unresolvable row ends the probe."""
    c, db = client
    _row(db, tmdb_id=10, rating_key=None)          # positive id, still no Plex row
    _row(db, tmdb_id=20, rating_key="20")
    _stub(monkeypatch, locks={"20": False})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert [r["rating_key"] for r in b["control"]["rows"]] == ["20"]


# ── sampling ─────────────────────────────────────────────────────────────

def test_samples_several_rows_not_one(client, monkeypatch):
    c, db = client
    for i in range(1, 9):
        _row(db, tmdb_id=i, rating_key=str(i))
    _stub(monkeypatch, locks={str(i): False for i in range(1, 9)})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    from app.web.api import _CONTROL_SAMPLE
    assert b["control"]["sampled"] == _CONTROL_SAMPLE
    assert b["control"]["candidate_rows"] == 8
    assert b["control"]["sidecar_rows_read"] == _CONTROL_SAMPLE


def test_upload_entries_are_excluded_from_the_natural_state(client, monkeypatch):
    """An upload:// entry means motif already pushed to that row — and a push LOCKS the
    theme field (measured on rk 261711). Counting it would let motif's own contamination
    masquerade as Plex's natural behaviour."""
    c, db = client
    _row(db, tmdb_id=1, rating_key="1")
    _row(db, tmdb_id=2, rating_key="2")
    _stub(monkeypatch,
          locks={"1": True, "2": False},
          entries={"1": "upload://themes/ccc", "2": "metadata://themes/aaa"})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    rows = {r["rating_key"]: r for r in b["control"]["rows"]}
    assert rows["1"]["is_sidecar_entry"] is False      # pushed-to, contaminated
    assert rows["2"]["is_sidecar_entry"] is True
    assert b["control"]["sidecar_rows_read"] == 1      # only the real sidecar counts
    assert b["control"]["theme_locked"] is False       # the locked upload:// row ignored


def test_any_locked_sidecar_row_keeps_the_lock_alive(client, monkeypatch):
    """Conservative on purpose: closing the door early is how this arc got into trouble."""
    c, db = client
    _row(db, tmdb_id=1, rating_key="1")
    _row(db, tmdb_id=2, rating_key="2")
    _stub(monkeypatch, locks={"1": False, "2": True})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["control"]["sidecar_rows_locked"] == 1
    assert b["control"]["theme_locked"] is True
    assert "UNLOCK + REFRESH is untested" in b["verdict"]


def test_unreadable_rows_leave_no_verdict(client, monkeypatch):
    c, db = client
    _row(db, tmdb_id=1, rating_key="1")
    _stub(monkeypatch, locks={"1": None})             # read fails

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["control"]["sidecar_rows_read"] == 0
    assert b["control"]["theme_locked"] is None
    assert "NO verdict" in b["verdict"]


# ── the error says WHICH thing went wrong ────────────────────────────────

def test_error_distinguishes_no_rows_from_none_resolving(client, monkeypatch):
    """v0.51.178 used ONE string for both, so the operator's failure could not be read."""
    c, db = client
    _row(db, tmdb_id=10, rating_key=None)             # exists, doesn't resolve
    _stub(monkeypatch)

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["control"]["candidate_rows"] == 1
    assert "did not resolve" in b["control"]["error"] or \
           "resolved to a plex_items" in b["control"]["error"]


def test_error_when_there_are_no_candidate_rows_at_all(client, monkeypatch):
    c, db = client
    _stub(monkeypatch)
    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["control"]["candidate_rows"] == 0
    assert "exist at all" in b["control"]["error"]


def test_normalized_rows_are_never_the_control(client, monkeypatch):
    """The control's whole value is being untouched."""
    c, db = client
    _row(db, tmdb_id=1, rating_key="1", norm_state="normalized")
    _row(db, tmdb_id=2, rating_key="2")
    _stub(monkeypatch, locks={"1": True, "2": False})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert [r["rating_key"] for r in b["control"]["rows"]] == ["2"]
