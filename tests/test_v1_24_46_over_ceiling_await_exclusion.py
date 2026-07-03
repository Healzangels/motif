"""v1.24.46 — code-review follow-ups on the v1.24.43-45 over-ceiling arc.

#1 (behavioral): an over-ceiling collection theme terminal-fails with
   last_place_attempt_reason='plex_rejected:over_ceiling' and NO placement
   (collections have no sidecar fallback). That row satisfied the v1.24.43
   _LIB_AWAIT_SQL (canonical present + no placement + not-LPS), so the amber
   AWAIT badge counted it as "downloaded — click to PLACE" while v1.24.45's row
   glyph said ⊘ "too large, can't place" — two contradictory surfaces, and a
   click re-enqueued the doomed upload. _LIB_AWAIT_SQL now excludes over_ceiling
   (NULL-safe IS NOT), so the count/filter agree with the glyph. A genuinely-
   retriable transient reason (plex_rejected:HTTP_500) STAYS in AWAIT.

#2 (source): _do_place_collection accepted a re-encode that was smaller than the
   ORIGINAL but still over the ceiling, then uploaded it for a guaranteed 500.
   The gate now requires the re-encode to FIT the ceiling; a measured still-over
   re-encode short-circuits to over_ceiling without the doomed POST.

#6 (source): _downscale_audio_to_fit's multi-line docstring → # comments per
   CLAUDE.md ("Single-line comments only ... no docstring-style narration").
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import get_conn, init_db

REPO = Path(__file__).resolve().parent.parent
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _theme(c, tid, tmdb, title):
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
              " upstream_source, last_seen_sync_at, first_seen_sync_at) "
              "VALUES (?, 'movie', ?, ?, '2001', 'imdb', ?, ?)",
              (tid, tmdb, title, NOW, NOW))


def _canonical(c, tid, tmdb, reason=None):
    # canonical theme on disk; `reason` stamps last_place_attempt_reason so we can
    # reproduce an over-ceiling (or transient-rejected) row that never placed.
    c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
              " edition_key, theme_id, file_path, source_kind, source_video_id, "
              " downloaded_at, canonical_present, last_place_attempt_reason) "
              "VALUES ('movie', ?, '1', '', ?, 'f.mp3','themerrdb','v',?,1,?)",
              (tmdb, tid, NOW, reason))


def _plex_item(c, *, tid, tmdb, rk, title, lps=0):
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
              " theme_id, guid_tmdb, title, edition_key, plex_independent_theme, "
              " has_theme, first_seen_at, last_seen_at) "
              "VALUES (?, '1','movie',?,?,?,'',?,0,?,?)",
              (rk, tid, tmdb, title, lps, NOW, NOW))


def _filter_rks(client):
    c, _ = client
    r = c.get("/api/library?tab=movies&attn_pills=await", headers=AUTH)
    assert r.status_code == 200, r.text
    return {it["rating_key"] for it in r.json()["items"]}


# ── #1: over_ceiling is excluded from AWAIT, transient rejections stay ────────

def test_over_ceiling_row_excluded_from_filter(client):
    c, db = client
    with get_conn(db) as conn:
        # genuine await: canonical + no placement + no rejection
        _theme(conn, 10, -1, "Awaiting"); _canonical(conn, 10, -1)
        _plex_item(conn, tid=10, tmdb=-1, rk="rk-await", title="Awaiting")
        # over-ceiling: canonical + no placement BUT terminal-failed → ⊘ glyph,
        # so it must NOT also show as AWAIT (the contradiction this tag fixes).
        _theme(conn, 11, -2, "TooBig")
        _canonical(conn, 11, -2, reason="plex_rejected:over_ceiling")
        _plex_item(conn, tid=11, tmdb=-2, rk="rk-toobig", title="TooBig")
        conn.commit()
    assert _filter_rks(client) == {"rk-await"}, (
        "attn_pills=await must not render the over_ceiling row")


def test_transient_rejection_stays_in_await(client):
    # A non-size 500 (plex_rejected:HTTP_500) is genuinely retriable — the user
    # can still re-push — so it STAYS actionable in AWAIT. Only over_ceiling
    # (unfixable without shortening the theme) is excluded.
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 20, -7, "Transient")
        _canonical(conn, 20, -7, reason="plex_rejected:HTTP_500")
        _plex_item(conn, tid=20, tmdb=-7, rk="rk-transient", title="Transient")
        conn.commit()
    assert _filter_rks(client) == {"rk-transient"}, (
        "a transient rejection is still awaiting placement")


def test_filter_empty_when_only_over_ceiling(client):
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 30, -9, "OnlyTooBig")
        _canonical(conn, 30, -9, reason="plex_rejected:over_ceiling")
        _plex_item(conn, tid=30, tmdb=-9, rk="rk-only", title="OnlyTooBig")
        conn.commit()
    assert _filter_rks(client) == set(), (
        "nothing actionable — the over_ceiling row is excluded from the filter")


def test_predicate_excludes_over_ceiling_clause():
    src = (REPO / "app" / "web" / "api.py").read_text()
    # the exclusion lives inside the SHARED predicate so count==filter is preserved
    i = src.index("_LIB_AWAIT_SQL = (")
    block = src[i:i + 600]
    assert "IS NOT 'plex_rejected:over_ceiling'" in block
    assert "last_place_attempt_reason" in block


# ── #2: downscale gate must require the re-encode to FIT the ceiling ──────────

def test_collection_downscale_gate_requires_ceiling_fit():
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # v0.51.16 (audit #2): the gate grew a media_type guard — over-ceiling
    # movie/TV skips the downscale entirely (sidecar fallback instead).
    i = src.index(
        "if len(audio_bytes) >= _ceiling_bytes and not _skip_doomed_upload:")
    block = src[i:i + 3400]
    # accept only a re-encode that fits the ceiling (was: < len(audio_bytes))
    assert "len(_small) < _ceiling_bytes" in block
    assert "len(_small) < len(audio_bytes)" not in block, (
        "the v1.24.45 original-only gate must be gone (it uploaded still-over "
        "re-encodes for a guaranteed 500)")
    # a measured still-over re-encode short-circuits without the doomed upload
    assert "elif _small:" in block
    assert "plex_rejected:over_ceiling" in block
    assert "_JobPermanentFailure" in block


# ── #6: no docstring-style narration in the helper (CLAUDE.md) ────────────────

def test_downscale_helper_uses_line_comments_not_docstring():
    src = (REPO / "app" / "core" / "worker.py").read_text()
    i = src.index("def _downscale_audio_to_fit(")
    after_sig = src[i:].split("\n", 1)[1]
    first_body_line = after_sig.lstrip().split("\n", 1)[0]
    assert first_body_line.startswith("#"), (
        "CLAUDE.md: single-line comments only, no docstring-style narration — "
        f"helper body starts with: {first_body_line!r}")
