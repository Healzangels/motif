"""v0.51.182 — settle the lock lead on the case that actually matters.

Everything measured so far danced around the real question. LET PLEX SERVE deletes motif's
theme and expects PLEX'S AGENT to supply one; the delete LOCKS the theme field; a locked
field is what stops an agent writing. Every witness to date was unusable:

  - rk 497736: locked and already SERVING — "did a theme appear?" is unanswerable when one
    was already there. v0.51.181's endpoint accepted it anyway and printed
    gained_a_theme:false, which is trivially true and says nothing. Guarded here.
  - rk 3487: locked and themeless, but it's a broken canonical (motif had no bytes to
    push) and one of four Star Wars editions.
  - the 6 motif-placed-but-themeless rows: all UNLOCKED, so the lock isn't what ails THEM
    — but that says nothing about a row the delete just locked.

The subject that CAN answer it is a row Plex's own agent is currently serving
(metadata://themes/tv.plex.agents.*): proof Plex HAS a theme to give for that title, which
is what every previous witness lacked. Delete it and watch, holding everything constant:

  refresh while LOCKED   → restored ⇒ the lock doesn't block the agent. LEAD DEAD.
  refresh once UNLOCKED  → restored ⇒ the lock blocks it. LEAD CONFIRMED, LPS degraded.
  neither                → Plex never re-supplies after a delete; LPS rests on something
                           else. Its own finding.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db
from app.core.plex import THEME_UPLOAD_CEILING_BYTES

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()
AGENT = "metadata://themes/tv.plex.agents.series_abc"


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
    monkeypatch.setattr("time.sleep", lambda *_a: None)
    return TestClient(create_app(s)), s.db_path


def _plex_row(db, *, rating_key, tmdb_id=2, placed=None):
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, title, "
                  " guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'show', '1', 'Wildboyz', ?, '', 1, ?, ?)",
                  (rating_key, tmdb_id, NOW, NOW))
        if placed:
            c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, "
                      " media_folder, edition_key, placement_kind, placed_at) "
                      "VALUES ('tv', ?, '1', '/data/tv/x', '', ?, ?)",
                      (tmdb_id, placed, NOW))
        c.commit()


def _stub(monkeypatch, *, entry=AGENT, locked=False, restores_when_locked=False,
          restores_when_unlocked=False, blob=b"THEMEBYTES", capture_ok=True,
          upload_ok=True):
    """Models Plex: the agent may or may not re-supply, and may or may not care about the
    lock. `restores_when_*` are the two hypotheses under test."""
    from app.web import api as api_mod
    st = {"entry": entry, "locked": locked, "entries": 1, "uploaded": None}

    class _FakePlex:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get_field_locks(self, *, rating_key):
            return {"ok": True, "http_status": 200, "error": None,
                    "locked_fields": (["theme"] if st["locked"] else []),
                    "theme_locked": st["locked"]}

        def set_theme_field_lock(self, *, rating_key, locked, shape="metadata",
                                 section_id=None, plex_type=None):
            st["locked"] = locked
            return 200

        def get_themes(self, *, rating_key):
            if st["entry"] is None:
                return {"ok": True, "http_status": 200, "error": None,
                        "body": {"MediaContainer": {"Metadata": []}}}
            return {"ok": True, "http_status": 200, "error": None, "body": {
                "MediaContainer": {"Metadata": [
                    {"ratingKey": st["entry"], "selected": True}]}}}

        def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
            if not capture_ok:
                return {"ok": False, "http_status": 500, "bytes": None,
                        "error": "boom"}
            return {"ok": True, "http_status": 200, "bytes": blob, "error": None}

        def delete_theme(self, *, rating_key):
            st["entry"] = None
            st["locked"] = True        # Plex's DELETE locks the field (documented)
            return True

        def refresh(self, rating_key):
            if st["locked"] and restores_when_locked:
                st["entry"] = AGENT
            elif not st["locked"] and restores_when_unlocked:
                st["entry"] = AGENT
            return True

        def upload_theme(self, *, rating_key, audio_bytes, content_type="audio/mpeg"):
            if not upload_ok:
                return (False, 500, "nope")
            st["uploaded"] = audio_bytes
            st["entry"] = "upload://themes/recovered"
            return (True, 200, "")

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)
    return st


# ── the three outcomes ───────────────────────────────────────────────────

def test_agent_restores_while_locked_kills_the_lead(client, monkeypatch):
    c, db = client
    _plex_row(db, rating_key="302080")
    _stub(monkeypatch, restores_when_locked=True)

    b = c.post("/api/admin/plex/lps-lock-experiment",
               headers=AUTH, json={"rating_key": "302080"}).json()
    assert b["agent_restored_while_locked"] is True
    assert "LEAD DEAD" in b["verdict"]
    # it restored on its own, so nothing should have been unlocked or re-uploaded
    assert b["agent_restored_once_unlocked"] is False
    assert b["recovered_by_reupload"] is None
    assert b["row_has_a_theme_now"] is True


def test_agent_restores_only_once_unlocked_confirms_the_lead(client, monkeypatch):
    """The finding that would matter: the delete's own lock is what stops recovery, so
    LET PLEX SERVE has been degraded on every row motif ever deleted from."""
    c, db = client
    _plex_row(db, rating_key="302080")
    _stub(monkeypatch, restores_when_locked=False, restores_when_unlocked=True)

    b = c.post("/api/admin/plex/lps-lock-experiment",
               headers=AUTH, json={"rating_key": "302080"}).json()
    assert b["agent_restored_while_locked"] is False
    assert b["agent_restored_once_unlocked"] is True
    assert "LEAD CONFIRMED" in b["verdict"]
    assert "unlock after every delete" in b["verdict"]
    assert b["row_has_a_theme_now"] is True


def test_neither_is_its_own_finding_not_a_shrug(client, monkeypatch):
    c, db = client
    _plex_row(db, rating_key="302080")
    _stub(monkeypatch, restores_when_locked=False, restores_when_unlocked=False)

    b = c.post("/api/admin/plex/lps-lock-experiment",
               headers=AUTH, json={"rating_key": "302080"}).json()
    assert "NEITHER" in b["verdict"]
    assert "does not re-supply a deleted theme" in b["verdict"]
    # and the row must not be left bare
    assert b["recovered_by_reupload"] is True
    assert b["row_has_a_theme_now"] is True


# ── the safety net ───────────────────────────────────────────────────────

def test_refuses_when_the_bytes_cannot_be_captured(client, monkeypatch):
    """No way back ⇒ don't delete. The delete probe stranded rk 261711 precisely because
    nothing was captured first."""
    c, db = client
    _plex_row(db, rating_key="302080")
    st = _stub(monkeypatch, capture_ok=False)

    b = c.post("/api/admin/plex/lps-lock-experiment",
               headers=AUTH, json={"rating_key": "302080"}).json()
    assert b["ok"] is False
    assert "not worth running" in b["error"]
    assert st["entry"] == AGENT, "nothing may be deleted once the net is known missing"


def test_refuses_an_over_ceiling_theme(client, monkeypatch):
    c, db = client
    _plex_row(db, rating_key="302080")
    st = _stub(monkeypatch, blob=b"x" * (THEME_UPLOAD_CEILING_BYTES + 1))

    b = c.post("/api/admin/plex/lps-lock-experiment",
               headers=AUTH, json={"rating_key": "302080"}).json()
    assert b["ok"] is False
    assert "over" in b["error"].lower()
    assert st["entry"] == AGENT


def test_a_failed_recovery_is_reported_loudly(client, monkeypatch):
    c, db = client
    _plex_row(db, rating_key="302080")
    _stub(monkeypatch, upload_ok=False)

    b = c.post("/api/admin/plex/lps-lock-experiment",
               headers=AUTH, json={"rating_key": "302080"}).json()
    assert b["recovered_by_reupload"] is False
    assert b["row_has_a_theme_now"] is False
    assert "WARNING" in b["verdict"] and "manual restore" in b["verdict"]


def test_restores_the_original_lock_state(client, monkeypatch):
    c, db = client
    _plex_row(db, rating_key="302080")
    st = _stub(monkeypatch, locked=False, restores_when_locked=True)

    c.post("/api/admin/plex/lps-lock-experiment",
           headers=AUTH, json={"rating_key": "302080"})
    assert st["locked"] is False, "the flag must end as it started"


# ── subject guards: only rows that can actually answer ───────────────────

def test_refuses_a_row_motif_owns(client, monkeypatch):
    """Deleting motif's own theme is not the LPS case — it's destroying real work."""
    c, db = client
    _plex_row(db, rating_key="302080", placed="hardlink")
    st = _stub(monkeypatch)

    b = c.post("/api/admin/plex/lps-lock-experiment",
               headers=AUTH, json={"rating_key": "302080"}).json()
    assert b["ok"] is False
    assert "destroy real work" in b["error"]
    assert st["entry"] == AGENT


def test_refuses_an_upload_entry(client, monkeypatch):
    """An upload:// theme was never the agent's to restore, so the test couldn't answer."""
    c, db = client
    _plex_row(db, rating_key="302080")
    st = _stub(monkeypatch, entry="upload://themes/abc")

    b = c.post("/api/admin/plex/lps-lock-experiment",
               headers=AUTH, json={"rating_key": "302080"}).json()
    assert b["ok"] is False
    assert "not one Plex's agent supplied" in b["error"]
    assert st["entry"] == "upload://themes/abc"


def test_refuses_a_row_with_no_theme(client, monkeypatch):
    c, db = client
    _plex_row(db, rating_key="302080")
    _stub(monkeypatch, entry=None)

    b = c.post("/api/admin/plex/lps-lock-experiment",
               headers=AUTH, json={"rating_key": "302080"}).json()
    assert b["ok"] is False
    assert "nothing to delete" in b["error"]


def test_requires_a_named_row(client):
    c, _ = client
    b = c.post("/api/admin/plex/lps-lock-experiment", headers=AUTH, json={}).json()
    assert b["ok"] is False
    assert "name a rating_key" in b["error"]


def test_confirms_the_delete_actually_locks_by_re_reading(client, monkeypatch):
    """The premise of the whole lead. Read it rather than trust Plex's docs."""
    c, db = client
    _plex_row(db, rating_key="302080")
    _stub(monkeypatch, locked=False, restores_when_locked=True)

    b = c.post("/api/admin/plex/lps-lock-experiment",
               headers=AUTH, json={"rating_key": "302080"}).json()
    deleted_step = next(s for s in b["steps"] if s["step"] == "deleted")
    assert deleted_step["theme_locked_after_delete"] is True


# ── the v0.51.181 bug this tag fixes ─────────────────────────────────────

def test_unlock_experiment_refuses_a_row_that_already_has_a_theme(client, monkeypatch):
    """v0.51.181 accepted rk 497736 — locked and already SERVING — and answered "did a
    theme appear?" with a meaningless false. A probe that reports a verdict where nothing
    could be measured is the gap-reads-as-a-result shape all over again."""
    c, db = client
    _plex_row(db, rating_key="497736")
    _stub(monkeypatch, entry="upload://themes/3b4afe", locked=True)

    b = c.post("/api/admin/plex/theme-unlock-experiment",
               headers=AUTH, json={"rating_key": "497736"}).json()
    assert b["ok"] is False
    assert "ALREADY has a theme" in b["error"]
    assert "gained_a_theme" not in b


def test_experiment_is_threadpooled():
    i = API.index('@app.post("/api/admin/plex/lps-lock-experiment")')
    body = API[i:API.index('@app.post("/api/admin/plex/theme-unlock-experiment")', i)]
    assert "def _run():" in body and "run_in_threadpool(_run)" in body   # class-12
