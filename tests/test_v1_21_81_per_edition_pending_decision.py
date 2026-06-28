"""v1.21.81 — per-edition pending-update DECISION (ACCEPT / DECLINE).

the user's 3-edition LotR repro: ACCEPT UPDATE / KEEP CURRENT on one edition
cleared the blue !UPD pill on EVERY edition of the title. Root cause: the
pending decision was keyed only by (media_type, tmdb_id, section_id) — all
editions of a title share that tuple, so one decision drove them all.

The fix keys the DECISION by edition too:
  - detection (kind/url) stays at edition_key='' (sync writes it edition-blind);
    every detection read is pinned to '' so per-edition decision-copy rows can't
    make the scalar subqueries non-deterministic.
  - the decision read is edition-filtered (edition_key=pi.edition_key) and
    defaults to 'pending' when this edition has no decision row — so a fresh
    detection lights EVERY edition, and accepting/declining one edition only
    suppresses THAT edition's pill (the detection-existence is enforced by the
    separate url-diff / new-theme-kind gate, not the decision column).

Behavioral tests drive the real endpoints (the v1.18.81 phantom-guard rule);
the source-walking lint guards every pending_updates subquery so a future
edit can't silently drop the edition predicate (mirror-drift class 9).
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
NOW = now_iso()
AUTH = {"X-Authentik-Username": "testadmin"}

TMDB = 120
STD_RK = "111"          # untagged folder -> edition_key '' (standard)
EXT_RK = "222"          # {edition-Extended} -> edition_key 'extended'
STD_FOLDER = "/data/Movies/LotR (2001)"
EXT_FOLDER = "/data/Movies/LotR (2001) {edition-Extended}"
OLD_URL = "https://www.youtube.com/watch?v=oldoldold01"
NEW_URL = "https://www.youtube.com/watch?v=newnewnew01"


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), db


def _seed(db):
    """Two editions of one title in one section: standard ('' ) + Extended.
    A single DETECTION row at edition_key='' (how sync writes it)."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',?,'LotR','2001','imdb',?,?,?)",
            (TMDB, NOW, NOW, OLD_URL))
        tid = cur.lastrowid
        for rk, ek, fp in (
            (STD_RK, "", STD_FOLDER),
            (EXT_RK, "extended", EXT_FOLDER),
        ):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, first_seen_at, last_seen_at) VALUES (?,'1',"
                "'movie',?,?,'LotR','2001',?,?,1,?,?)",
                (rk, tid, TMDB, ek, fp, NOW, NOW))
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',?,'1',?,?,?,"
                "'v','auto','themerrdb')", (TMDB, ek, f"movies/{ek or 'std'}.mp3", NOW))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " theme_id, media_folder, placed_at, placement_kind,"
                " plex_refreshed, provenance, edition_key) VALUES ('movie',"
                "?,'1',?,?,?, 'hardlink',1,'auto',?)", (TMDB, tid, fp, NOW, ek))
        # DETECTION at edition_key='' — a real url change (old != new).
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
            " decision, detected_at, old_youtube_url, new_youtube_url, kind)"
            " VALUES ('movie',?,'1','pending',?,?,?,'upstream_changed')",
            (TMDB, NOW, OLD_URL, NEW_URL))
        conn.commit()
    return tid


def _rows(client):
    r = client.get("/api/library?tab=movies&fourk=false", headers=AUTH)
    assert r.status_code == 200, r.text
    return {str(row["rating_key"]): row for row in r.json()["items"]}


def _pending(client):
    """Map rating_key -> bool(pending_update pill lit)."""
    return {rk: bool(row.get("pending_update")) for rk, row in _rows(client).items()}


def _actionable(client):
    """Map rating_key -> bool(actionable_update — the ACCEPT-prompt gate,
    which (unlike the blue pill) DOES clear on KEEP CURRENT/decline)."""
    return {rk: bool(row.get("actionable_update")) for rk, row in _rows(client).items()}


# ── Behavioral: the discriminator ───────────────────────────────────


def test_fresh_detection_lights_every_edition(app_client):
    """A single detection (edition_key='') must light the blue !UPD pill on
    BOTH editions — the non-standard edition inherits the open detection via
    the default-to-'pending' decision read."""
    client, db = app_client
    _seed(db)
    pend = _pending(client)
    assert pend.get(STD_RK) is True, pend
    assert pend.get(EXT_RK) is True, pend


def test_accept_standard_leaves_extended_pending(app_client):
    """THE bug: accepting the standard ('' ) edition must NOT clear the
    Extended edition's pill."""
    client, db = app_client
    _seed(db)
    r = client.post(
        f"/api/updates/movie/{TMDB}/accept?section_id=1&rating_key={STD_RK}",
        headers=AUTH)
    assert r.status_code == 200, r.text
    pend = _pending(client)
    assert pend.get(STD_RK) is False, ("standard's own pill should clear", pend)
    assert pend.get(EXT_RK) is True, ("Extended must STAY pending", pend)


def test_decline_extended_leaves_standard_actionable(app_client):
    """Declining (KEEP CURRENT) the Extended edition clears ONLY Extended's
    ACCEPT-prompt (actionable_update) — the standard edition stays actionable.
    (The blue pill itself stays on a declined row by v1.12.5 design; it's
    actionable_update that drops, so this is what we assert per edition.)"""
    client, db = app_client
    _seed(db)
    r = client.post(
        f"/api/updates/movie/{TMDB}/decline?section_id=1&rating_key={EXT_RK}",
        headers=AUTH)
    assert r.status_code == 200, r.text
    act = _actionable(client)
    assert act.get(EXT_RK) is False, ("Extended should no longer be actionable", act)
    assert act.get(STD_RK) is True, ("standard must STAY actionable", act)


def test_topbar_count_is_per_edition(app_client):
    """/api/updates/count must count per-edition pending instances: 2 fresh,
    1 after accepting one edition."""
    client, db = app_client
    _seed(db)
    n0 = client.get("/api/updates/count", headers=AUTH).json()["pending"]
    assert n0 == 2, n0
    client.post(
        f"/api/updates/movie/{TMDB}/accept?section_id=1&rating_key={EXT_RK}",
        headers=AUTH)
    n1 = client.get("/api/updates/count", headers=AUTH).json()["pending"]
    assert n1 == 1, n1


def test_decision_row_written_at_clicked_edition(app_client):
    """ACCEPT on Extended writes the decision at (section, 'extended'),
    leaving the '' detection row untouched (still 'pending')."""
    client, db = app_client
    _seed(db)
    client.post(
        f"/api/updates/movie/{TMDB}/accept?section_id=1&rating_key={EXT_RK}",
        headers=AUTH)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = {
            (r["section_id"], r["edition_key"]): r["decision"]
            for r in conn.execute(
                "SELECT section_id, edition_key, decision FROM pending_updates"
                " WHERE media_type='movie' AND tmdb_id=?", (TMDB,))
        }
    assert rows[("1", "")] == "pending", ("detection row untouched", rows)
    assert rows[("1", "extended")] == "accepted", ("edition decision written", rows)


# ── Source-walking lint: mirror-drift guard (class 9) ───────────────


def _subqueries(col_pattern):
    """Yield the window after each `SELECT pu.<col> FROM pending_updates pu`
    up to its closing predicate so we can assert what edition scope it carries.
    Wide enough to reach the section_id+edition_key tail even at the most
    deeply-indented call sites."""
    src = API_PY.read_text()
    for m in re.finditer(rf"\(SELECT pu\.{col_pattern} FROM pending_updates pu",
                          src):
        yield src[m.start():m.start() + 360]


def test_every_decision_subquery_is_edition_filtered():
    """Every `SELECT pu.decision FROM pending_updates` scalar subquery must
    filter to a row's edition_key — never read a sibling edition's decision."""
    found = list(_subqueries("decision"))
    assert found, "no decision subqueries found — pattern drifted"
    for block in found:
        assert "edition_key = pi" in block, (
            "a pu.decision subquery is NOT edition-filtered — accepting one "
            f"edition could clear a sibling:\n{block}")


def test_every_detection_subquery_is_pinned_to_standard():
    """Every kind / url (detection) subquery must pin edition_key='' so a
    per-edition decision-copy row can't make it non-deterministic."""
    for col in ("kind", "old_youtube_url", "new_youtube_url"):
        for block in _subqueries(col):
            head = block[:block.index(")") + 1] if ")" in block else block
            assert "edition_key = ''" in block, (
                f"a pu.{col} detection subquery is not pinned to "
                f"edition_key='':\n{block}")


def test_library_decision_joins_are_edition_filtered():
    """The library SELECT keeps pu_sec/pu_global (detection, kind, pinned to
    '') and adds pu_dsec/pu_dglobal (decision, edition-filtered)."""
    src = API_PY.read_text()
    assert "pu_dsec.edition_key = pi.edition_key" in src
    assert "pu_dglobal.edition_key = pi.edition_key" in src
    assert "AND pu_sec.edition_key = ''" in src
    assert "AND pu_global.edition_key = ''" in src
    # the decision reads use the edition-filtered aliases.
    assert "COALESCE(pu_dsec.decision, pu_dglobal.decision" in src
    assert "COALESCE(pu_sec.decision" not in src, (
        "a decision read still uses the detection JOIN alias")


def test_v1_21_81_version_pin():
    # Prefix-only — the canonical exact-version guard is test_v1_13_79; a
    # per-tag test must not re-pin the literal version or every later bump
    # breaks it (v1.21.82 caught exactly that).
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
