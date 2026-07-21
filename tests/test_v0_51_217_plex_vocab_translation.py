"""v0.51.217 — plex_items' media_type vocabulary must be translated before it's compared.

Two vocabularies exist for one concept:

  plex_items.media_type   {'movie', 'show',  'collection'}   <- Plex's own strings
  everything else         {'movie', 'tv',    'collection'}   <- themes, local_files,
                                                                placements, /api/items

_maybe_notify_arrived_themed used plex_items' value raw in three places, which produced
two live bugs on a real library (user-reported 2026-07-21):

  1. `lf.media_type = pi.media_type` compares 'tv' to 'show' for every TV row — always
     false — so the NOT EXISTS meant to suppress the FYI when motif ALREADY OWNS a theme
     for the title was inert for TV. The "arrived already themed, nothing to do" notice
     fired for shows motif manages.
  2. The same raw value rode into the notification's item_ctx, so the inbox click-through
     emitted info_mt=show and GET /api/items/show/<id> answered 422 literal_error.

The behavioural tests below seed the exact shape and assert on the endpoint + the recorded
notification. The lint pins the CLASS: every comparison of a plex_items media_type against
a motif-vocabulary table must go through the CASE translation.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

from app.core.db import get_conn, init_db

NOW = "2026-07-21T00:00:00"
REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    p = tmp_path / "motif.db"
    init_db(p)
    with sqlite3.connect(p) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, "
                  " themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('3','TV','show',0,0,'tv',1,?,?)", (NOW, NOW))
        c.commit()
    return p


def _seed_show(db, *, rk, tmdb, title, motif_owns_theme):
    """A TV show Plex serves a theme for. plex_items stores Plex's 'show'; if motif owns a
    theme, local_files/placements store motif's 'tv' for the SAME title."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, guid_tmdb, "
                  " edition_key, title, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'show', '3', ?, '', ?, 1, ?, ?)", (rk, tmdb, title, NOW, NOW))
        if motif_owns_theme:
            c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
                      " edition_key, file_path, downloaded_at, source_video_id) "
                      "VALUES ('tv', ?, '3', '', ?, ?, 'v')",
                      (tmdb, f"tv/{tmdb}.mp3", NOW))
        c.commit()


def _fire(db, rks):
    from app.core import plex_enum
    plex_enum._maybe_notify_arrived_themed(db, rks)
    with get_conn(db) as c:
        return c.execute(
            "SELECT media_type, tmdb_id, title FROM notifications "
            " WHERE event_kind = 'plex_item_arrived_themed' ORDER BY id").fetchall()


def test_suppressed_when_motif_already_owns_a_theme_for_the_show(db):
    """The regression that made this notification wrong. Before the fix the subquery
    compared 'tv' to 'show', never matched, and the FYI fired for a show motif manages."""
    _seed_show(db, rk="10797", tmdb=10797, title="Managed Show", motif_owns_theme=True)
    assert _fire(db, ["10797"]) == [], (
        "motif owns a theme for this title — 'arrived already themed, nothing to do' is "
        "false and must be suppressed")


def test_still_fires_for_a_genuinely_unmanaged_show(db):
    """The complement — the fix must not silence the real signal."""
    _seed_show(db, rk="20001", tmdb=20001, title="Unmanaged Show", motif_owns_theme=False)
    rows = _fire(db, ["20001"])
    assert len(rows) == 1 and rows[0]["tmdb_id"] == 20001


def test_recorded_media_type_is_motif_vocabulary(db):
    """The 422: the notification's media_type becomes info_mt in the click-through URL and
    is fed straight to /api/items/{media_type}/{tmdb}, which accepts only movie|tv|
    collection. Recording Plex's 'show' made every TV click-through a hard 422."""
    _seed_show(db, rk="20002", tmdb=20002, title="Clickable Show", motif_owns_theme=False)
    rows = _fire(db, ["20002"])
    assert rows[0]["media_type"] == "tv", (
        "must record motif's vocabulary — 'show' 422s the info-card fetch")


def test_the_recorded_media_type_is_accepted_by_the_items_endpoint(db, tmp_path,
                                                                   monkeypatch):
    """End-to-end on the actual contract rather than a string compare: whatever the
    notification stores must be a value /api/items will accept."""
    _seed_show(db, rk="20003", tmdb=20003, title="Endpoint Show", motif_owns_theme=False)
    mt = _fire(db, ["20003"])[0]["media_type"]
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    from starlette.testclient import TestClient
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    r = TestClient(create_app(s)).get(f"/api/items/{mt}/20003",
                                      headers={"X-Authentik-Username": "testadmin"})
    assert r.status_code != 422, (
        f"/api/items/{mt}/... rejects the recorded media_type: {r.text[:200]}")


# ── the class lint ───────────────────────────────────────────────────────────

def test_no_raw_comparison_of_plex_vocabulary_against_motif_tables():
    """The drift class, not just this instance. plex_items' media_type may only be
    compared to another table's media_type through the CASE translation — every other
    call site in the codebase already does this (cloud_theme_backup, scheduler, worker,
    api, and the sibling notify function in this same file), which is exactly why the two
    raw ones read as correct at a glance.

    recovery_v55 is exempt at one site: it spells the mapping out longhand as
    `(lf.media_type = 'tv' AND pi.media_type = 'show') OR pi.media_type = lf.media_type`,
    which is correct — the second disjunct only runs for movie/collection."""
    raw = re.compile(r"(?<!')\b([a-z_]+)\.media_type\s*=\s*(pi|plex_items)\.media_type"
                     r"|\b(pi|plex_items)\.media_type\s*=\s*([a-z_]+)\.media_type")
    offenders = []
    for path in sorted((REPO / "app").rglob("*.py")):
        for n, line in enumerate(path.read_text().splitlines(), 1):
            # prose ABOUT the bug is not the bug — this lint's first run flagged the
            # v0.51.217 comment explaining the very comparison it was written to catch.
            if line.lstrip().startswith("#"):
                continue
            code = line.split(" # ", 1)[0]
            if not raw.search(code):
                continue
            if path.name == "recovery_v55.py":
                continue          # the longhand form above — verified correct
            offenders.append(f"{path.relative_to(REPO)}:{n}: {line.strip()}")
    assert not offenders, (
        "plex_items.media_type ('show') compared raw to a motif-vocabulary table ('tv'). "
        "Wrap it: CASE pi.media_type WHEN 'show' THEN 'tv' ELSE pi.media_type END\n"
        + "\n".join(offenders))


def test_the_two_vocabularies_really_are_different():
    """Premise of everything above: if the schemas ever converge, this whole class of bug
    disappears and these guards should be revisited rather than cargo-culted."""
    db_py = (REPO / "app" / "core" / "db.py").read_text()
    assert "CHECK (media_type IN ('movie', 'show', 'collection'))" in db_py
    assert "CHECK (media_type IN ('movie', 'tv', 'collection'))" in db_py
