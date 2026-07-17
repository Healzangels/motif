"""v0.51.183 — the last thread off the lock arc: does an UNSELECTED entry play?

v0.51.182 settled the lock (dead: the agent restored nothing whether the field was locked
or unlocked) but turned up something bigger. Plex's agent never re-selects or re-adds a
theme entry after a delete — at all. So LET PLEX SERVE cannot work the way its name
implies, and exactly one of these is true:

  - Plex plays whatever is in the /themes collection regardless of `selected` ⇒ LPS has
    always worked, just not via the agent; or
  - it plays nothing ⇒ every LPS delete silently strands the item. A real bug in a
    shipped feature.

Nothing measured so far can tell those apart, because everything read the `selected`
FLAG. That is not the same question: _measure_plex_serving even falls back to meta[0]
when nothing is selected, so it would report a loudness for an item Plex may play nothing
for. verify_theme_claim reads the SERVING ASSOCIATION itself (HEAD singular /theme —
200 = Plex really delivers, 404 = it does not, None = transient).

The subject needs no mutation: a row where Plex reports no theme but motif placed one, and
whose collection still holds entries, IS the post-LPS-delete state, sitting there already.
A row with an empty collection cannot answer and must be reported as such, never counted
as a pass — the gap-reads-as-an-answer shape this arc kept hitting.
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
API = (REPO / "app" / "web" / "api.py").read_text()
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
    return TestClient(create_app(s)), s.db_path


def _themeless_placed(db, *, rating_key, tmdb_id, placed=True):
    """motif placed a theme; Plex reports none — the post-LPS-delete shape."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        if placed:
            c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, "
                      " media_folder, edition_key, placement_kind, placed_at) "
                      "VALUES ('movie', ?, '1', '/data/m', '', 'hardlink', ?)",
                      (tmdb_id, NOW))
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, title, "
                  " guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'movie', '1', ?, ?, '', 0, ?, ?)",
                  (rating_key, f"Movie{tmdb_id}", tmdb_id, NOW, NOW))
        c.commit()


def _stub(monkeypatch, *, entries_by_rk, serves_by_rk, alive_by_rk=None):
    """alive_by_rk: rk -> can the unselected entry's bytes be fetched? Defaults to alive.
    A DEAD entry serves nothing regardless of the flag, so it cannot answer."""
    from app.web import api as api_mod
    alive_by_rk = alive_by_rk or {}

    class _FakePlex:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get_themes(self, *, rating_key):
            meta = entries_by_rk.get(str(rating_key), [])
            return {"ok": True, "http_status": 200, "error": None,
                    "body": {"MediaContainer": {"Metadata": meta}}}

        def verify_theme_claim(self, rating_key):
            return serves_by_rk.get(str(rating_key), False)

        def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
            if alive_by_rk.get(str(item_rating_key), True):
                return {"ok": True, "http_status": 200, "bytes": b"AUDIO",
                        "error": None}
            return {"ok": False, "http_status": 404, "bytes": None, "error": "gone"}

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)


UNSELECTED = [{"ratingKey": "metadata://themes/aaa", "selected": False}]


# ── the two answers ──────────────────────────────────────────────────────

def test_unselected_entry_that_plays_means_lps_was_always_fine(client, monkeypatch):
    c, db = client
    _themeless_placed(db, rating_key="1", tmdb_id=10)
    _stub(monkeypatch, entries_by_rk={"1": UNSELECTED}, serves_by_rk={"1": True})

    b = c.post("/api/admin/plex/unselected-serves-probe", headers=AUTH).json()
    assert b["unselected_and_served"] == 1
    assert b["unselected_and_bare"] == 0
    assert "LET PLEX SERVE has always worked" in b["verdict"]
    assert "Nothing to fix" in b["verdict"]


def test_unselected_entry_that_plays_nothing_is_a_real_bug(client, monkeypatch):
    """The entries surviving the delete would be cold comfort: Plex plays none of them."""
    c, db = client
    _themeless_placed(db, rating_key="1", tmdb_id=10)
    _stub(monkeypatch, entries_by_rk={"1": UNSELECTED}, serves_by_rk={"1": False})

    b = c.post("/api/admin/plex/unselected-serves-probe", headers=AUTH).json()
    assert b["unselected_and_bare"] == 1
    assert b["rows"][0]["unselected_entry_is_alive"] is True
    assert "LIVE theme entry" in b["verdict"]
    assert "LPS strands the item" in b["verdict"]
    # v0.51.184: n=1 must be named as n=1, not dressed up
    assert "rests on a single case" in b["verdict"]


def test_a_dead_entry_cannot_answer(client, monkeypatch):
    """v0.51.184's discriminator, and the reason v0.51.183 shouldn't have said REAL BUG.
    An entry pointing at bytes Plex no longer has serves nothing no matter what the flag
    says — identical symptom, innocent cause. The operator's Wildboyz diag showed the
    check: fetching an entry with selected:false returned 4096 bytes, proving it live."""
    c, db = client
    _themeless_placed(db, rating_key="1", tmdb_id=10)
    _stub(monkeypatch, entries_by_rk={"1": UNSELECTED}, serves_by_rk={"1": False},
          alive_by_rk={"1": False})

    b = c.post("/api/admin/plex/unselected-serves-probe", headers=AUTH).json()
    assert b["rows"][0]["unselected_entry_is_alive"] is False
    assert b["rows"][0]["answers_the_question"] is False
    assert b["unselected_and_bare"] == 0
    assert "INCONCLUSIVE" in b["verdict"]


def test_mixed_is_reported_as_mixed(client, monkeypatch):
    c, db = client
    _themeless_placed(db, rating_key="1", tmdb_id=10)
    _themeless_placed(db, rating_key="2", tmdb_id=20)
    _stub(monkeypatch, entries_by_rk={"1": UNSELECTED, "2": UNSELECTED},
          serves_by_rk={"1": True, "2": False})

    b = c.post("/api/admin/plex/unselected-serves-probe", headers=AUTH).json()
    assert "MIXED" in b["verdict"]


# ── the gap must not read as an answer ───────────────────────────────────

def test_a_row_with_no_entries_cannot_answer_and_is_not_counted(client, monkeypatch):
    """An empty collection means Plex has nothing to serve, selected or not — that says
    nothing about the flag. (It IS its own problem on a motif-placed row: motif put a
    sidecar there and Plex never ingested it. 5 of the operator's first 6 looked like
    this, which is why only n=1 could answer.)"""
    c, db = client
    _themeless_placed(db, rating_key="1", tmdb_id=10)
    _stub(monkeypatch, entries_by_rk={"1": []}, serves_by_rk={"1": False})

    b = c.post("/api/admin/plex/unselected-serves-probe", headers=AUTH).json()
    assert b["rows"][0]["answers_the_question"] is False
    assert b["rows_that_answer"] == 0
    assert "INCONCLUSIVE, not a pass" in b["verdict"]


def test_a_transient_read_cannot_answer(client, monkeypatch):
    """verify_theme_claim returns None on transient errors — a tristate. Folding None into
    'serves nothing' would invent a bug out of a network blip."""
    c, db = client
    _themeless_placed(db, rating_key="1", tmdb_id=10)
    _stub(monkeypatch, entries_by_rk={"1": UNSELECTED}, serves_by_rk={"1": None})

    b = c.post("/api/admin/plex/unselected-serves-probe", headers=AUTH).json()
    assert b["rows"][0]["answers_the_question"] is False
    assert b["unselected_and_bare"] == 0
    assert "INCONCLUSIVE" in b["verdict"]


def test_a_row_with_a_selection_cannot_answer(client, monkeypatch):
    """Wildboyz post-recovery: 2 entries, one selected. It plays because of the flag, so
    it says nothing about playing WITHOUT one."""
    c, db = client
    _themeless_placed(db, rating_key="1", tmdb_id=10)
    _stub(monkeypatch,
          entries_by_rk={"1": [{"ratingKey": "upload://themes/x", "selected": True}]},
          serves_by_rk={"1": True})

    b = c.post("/api/admin/plex/unselected-serves-probe", headers=AUTH).json()
    assert b["rows"][0]["answers_the_question"] is False


def test_reads_the_serving_association_not_the_selected_flag():
    """The whole point. _measure_plex_serving falls back to meta[0] when nothing is
    selected, so it cannot tell 'plays entry 0' from 'plays nothing'."""
    i = API.index('@app.post("/api/admin/plex/unselected-serves-probe")')
    body = API[i:API.index("        return await run_in_threadpool(_run)", i)]
    assert "verify_theme_claim(" in body
    # forbid the CALL, not the name — the docstring names it to explain why it's wrong
    assert "_measure_plex_serving(" not in body


def test_probe_is_read_only():
    i = API.index('@app.post("/api/admin/plex/unselected-serves-probe")')
    body = API[i:API.index("        return await run_in_threadpool(_run)", i)]
    for mutating in ("delete_theme(", "upload_theme(", "upload_collection_theme(",
                     ".refresh("):
        assert mutating not in body, f"the probe must not call {mutating}"
    assert "def _run():" in body                      # class-12


# ── the lock apparatus is retired, not deprecated ────────────────────────

def test_the_lock_probes_are_gone(client):
    """Their questions are answered and recorded in CLAUDE.md § 11. Leaving answered
    probes around is what v0.51.180 removed two dead buttons for."""
    c, _ = client
    for route in ("/api/admin/loudness/theme-lock-probe",
                  "/api/admin/plex/theme-unlock-experiment",
                  "/api/admin/plex/lps-lock-experiment"):
        assert route not in API
        assert c.post(route, headers=AUTH).status_code == 404
    for dead in ("get_field_locks", "set_theme_field_lock"):
        assert dead not in (REPO / "app" / "core" / "plex.py").read_text()
        assert dead not in API


def test_claude_md_records_the_closed_lock_arc():
    md = " ".join((REPO / "CLAUDE.md").read_text().split())
    assert "The `theme` field lock: measured, and NOT a problem" in md
    assert "Do not re-open this" in md
    # the finding that outlived the lead
    assert "agent never re-selects or re-adds a theme entry after a delete" in md
    assert "Read the association, not the `selected` flag" in md


def test_button_and_bind_exist():
    assert 'id="loud-unselected-btn"' in HTML
    assert "// DOES AN UNSELECTED ENTRY PLAY?" in HTML
    assert "'/api/admin/plex/unselected-serves-probe'" in APP_JS
