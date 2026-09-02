"""v0.51.311 — code review of .309/.310: the behavior fixes, driven.

  1. The art proxy classifies a miss: Plex 404 / no plex_url / an EMPTY
     image body → "no_art" (204 + short cache); transport errors, non-404
     statuses and non-image bodies → "failed" (204 + no-store, warn-once).
     The .310 single None cached a Plex restart as "no art" for 5 minutes.
  2. Art responses carry Vary: Cookie (the .jpg spelling sits in the
     reverse proxy's asset-cache regex class).
  3. The two-arm (guid_tmdb OR theme_id) match now covers the sibling
     lookups in api_item — a guid-NULL row's has_theme tier resolves.
  4. The poster resolver prefers a guid match over a theme_id match whose
     theme_id may be stale after a Plex Fix Match, deterministically.
  5. canonical-health + loudness producers carry is_anime and route it
     before movie/tv.
  6. The keydown delegate drops key repeats (a HELD Enter navigated).
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}
NOW = "2026-08-31T12:00:00+00:00"


class _Resp:
    def __init__(self, status, ctype=None, content=b""):
        self.status_code = status
        self.headers = {"content-type": ctype} if ctype else {}
        self.content = content


class _FakeClient:
    def __init__(self, resp=None, exc=None):
        self._resp, self._exc = resp, exc

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get(self, *a, **k):
        if self._exc:
            raise self._exc
        return self._resp


def _make_app(tmp_path, monkeypatch, *, plex=True):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir(exist_ok=True)
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    if plex:
        monkeypatch.setattr(Settings, "plex_url",
                            property(lambda self: "http://plex.test"))
        monkeypatch.setattr(Settings, "plex_token",
                            property(lambda self: "tok-abc"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s


def _plex_answers(monkeypatch, resp=None, exc=None):
    import httpx
    monkeypatch.setattr(httpx, "Client", lambda **kw: _FakeClient(resp, exc))


# ── 1 + 2: the art proxy classifies its misses ───────────────


def test_transport_failure_is_uncacheable_and_warns_once(tmp_path, monkeypatch, caplog):
    import httpx
    from app.web import api as api_mod
    c, _ = _make_app(tmp_path, monkeypatch)
    monkeypatch.setattr(api_mod, "_PLEX_ART_FETCH_WARNED", False)
    _plex_answers(monkeypatch, exc=httpx.ConnectError("boom"))
    with caplog.at_level(logging.WARNING):
        r = c.get("/api/plex/art/123.jpg", headers=AUTH)
    assert r.status_code == 204
    assert r.headers["Cache-Control"] == "no-store", (
        "a Plex restart mid-render must NOT be browser-cached as 'no art' — "
        ".310 pinned blank posters for five minutes after recovery")
    assert any("FAILED" in rec.message for rec in caplog.records), (
        "class 9: a dead Plex must be operator-visible (warn-once)")
    # second failure: debug only (hot-path sub-pattern)
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        c.get("/api/plex/art/124.jpg", headers=AUTH)
    assert not any("FAILED" in rec.message for rec in caplog.records)


@pytest.mark.parametrize("resp,cache", [
    (_Resp(404), "private, max-age=300"),                      # genuine no art
    (_Resp(200, "image/jpeg", b""), "private, max-age=300"),   # empty body = no art
    (_Resp(503), "no-store"),                                  # Plex trouble
    (_Resp(401), "no-store"),                                  # rotated token
    (_Resp(200, "text/html", b"<html>"), "no-store"),          # not an image
])
def test_miss_classification(tmp_path, monkeypatch, resp, cache):
    c, _ = _make_app(tmp_path, monkeypatch)
    _plex_answers(monkeypatch, resp=resp)
    r = c.get("/api/plex/art/123.jpg", headers=AUTH)
    assert r.status_code == 204 and r.content == b""
    assert r.headers["Cache-Control"] == cache


def test_art_response_varies_on_cookie(tmp_path, monkeypatch):
    c, _ = _make_app(tmp_path, monkeypatch)
    _plex_answers(monkeypatch, resp=_Resp(200, "image/jpeg", b"\xff\xd8jpg"))
    r = c.get("/api/plex/art/123.jpg", headers=AUTH)
    assert r.status_code == 200 and r.content == b"\xff\xd8jpg"
    assert r.headers.get("Vary") == "Cookie", (
        "the .jpg spelling sits in the reverse proxy's asset-cache regex "
        "class — a shared cache must not hand one session's image to another")
    assert "max-age=86400" in r.headers["Cache-Control"]


# ── 3 + 4: api_item's guid-NULL siblings + guid precedence ───


def _seed_card(s, *, stale_first=False):
    from app.core.db import get_conn, transaction
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('1', 'Anime', 'show', 1, 0, 'anime', 1, ?, ?)""",
            (NOW, NOW))
        x = conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('tv', 311001, 'X', 'imdb', ?, ?)""", (NOW, NOW)).lastrowid
        # the AniDB shape: guid_tmdb NULL, theme_id bonded, Plex serving
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, theme_id, folder_path, edition_key,
                 has_theme, plex_independent_theme, first_seen_at,
                 last_seen_at)
               VALUES ('5555', '1', 'show', 'X', NULL, ?, '/data/anime/X',
                       '', 1, 1, ?, ?)""", (x, NOW, NOW))
        y = conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('tv', 311002, 'Y', 'imdb', ?, ?)""", (NOW, NOW)).lastrowid
        rows = [
            # STALE: Fix-Matched to Y (guid rewritten) but theme_id still X
            ("9001", 311002, x),
            # CORRECT: X's own guid row
            ("9002", 311001, x),
        ]
        if not stale_first:
            rows.reverse()
        for rk, guid, tid in rows:
            conn.execute(
                """INSERT INTO plex_items (rating_key, section_id, media_type,
                     title, guid_tmdb, theme_id, folder_path, edition_key,
                     has_theme, first_seen_at, last_seen_at)
                   VALUES (?, '2', 'show', 'X', ?, ?, '/data/tv/X', '', 0,
                           ?, ?)""", (rk, guid, tid, NOW, NOW))
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('2', 'TV', 'show', 0, 0, 'tv', 1, ?, ?)""", (NOW, NOW))
        return x, y


def test_guidless_row_resolves_its_theme_tiers(tmp_path, monkeypatch):
    c, s = _make_app(tmp_path, monkeypatch, plex=False)
    _seed_card(s)
    r = c.get("/api/items/tv/311001?section_id=1", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["plex_rating_key"] == "5555"
    assert body["plex_has_theme"] == 1 and body["plex_independent_theme"] == 1, (
        "the .309 two-arm widening reached only the poster resolver — the "
        "theme tiers still matched guid_tmdb alone, so the card painted a "
        "poster over 'no theme staged' for a theme Plex is serving")


def test_section_tier_does_not_borrow_a_siblings_theme(tmp_path, monkeypatch):
    # discriminates the SECTION-scoped tier query from the global fallback:
    # section 1 holds the guid-NULL row (unthemed), section 2 a guid row that
    # IS themed. Guid-only in the section query → NULLs → global MAX borrows
    # section 2's theme and the section-1 card lies.
    from app.core.db import get_conn, transaction
    c, s = _make_app(tmp_path, monkeypatch, plex=False)
    with get_conn(s.db_path) as conn, transaction(conn):
        for sid, anime in (("1", 1), ("2", 0)):
            conn.execute(
                """INSERT INTO plex_sections (section_id, title, type, is_anime,
                     is_4k, themes_subdir, included, discovered_at, last_seen_at)
                   VALUES (?, 'S', 'show', ?, 0, ?, 1, ?, ?)""",
                (sid, anime, f"s{sid}", NOW, NOW))
        x = conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('tv', 311003, 'Z', 'imdb', ?, ?)""", (NOW, NOW)).lastrowid
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, theme_id, folder_path, edition_key,
                 has_theme, plex_independent_theme, first_seen_at,
                 last_seen_at)
               VALUES ('7001', '1', 'show', 'Z', NULL, ?, '/a/Z', '', 0, 0,
                       ?, ?)""", (x, NOW, NOW))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, theme_id, folder_path, edition_key,
                 has_theme, plex_independent_theme, first_seen_at,
                 last_seen_at)
               VALUES ('7002', '2', 'show', 'Z', 311003, ?, '/t/Z', '', 1, 1,
                       ?, ?)""", (x, NOW, NOW))
    body = c.get("/api/items/tv/311003?section_id=1", headers=AUTH).json()
    assert body["plex_has_theme"] == 0 and body["plex_independent_theme"] == 0, (
        "the section-scoped tier must resolve the guid-NULL row ITSELF — "
        "falling through to the global MAX borrows a sibling section's theme")


@pytest.mark.parametrize("stale_first", [True, False])
def test_guid_match_beats_a_stale_theme_id_row(tmp_path, monkeypatch, stale_first):
    c, s = _make_app(tmp_path, monkeypatch, plex=False)
    _seed_card(s, stale_first=stale_first)
    r = c.get("/api/items/tv/311001?section_id=2", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["plex_rating_key"] == "9002", (
        "a row Fix-Matched away keeps its stale theme_id (enum rewrites the "
        "guid, never theme_id) — the theme_id arm must lose to the guid "
        "match regardless of insertion / index-scan order")


# ── 5: the two remaining deep-link producers carry is_anime ──


def test_canonical_health_and_loudness_rows_carry_is_anime(tmp_path):
    from app.core.canonical_health import _broken_rows, _entry
    from app.core.db import get_conn, init_db, transaction
    from app.core.loudness_audit import build_report
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('9', 'Anime', 'show', 1, 0, 'anime', 1, ?, ?)""",
            (NOW, NOW))
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('tv', 311009, 'A', 'imdb', ?, ?)""", (NOW, NOW))
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size,
                 downloaded_at, source_video_id, provenance, source_kind,
                 canonical_present, loudness_i, loudness_tp)
               VALUES ('tv', 311009, '9', '', 'p.mp3', 's', 1, ?, 'v',
                       'auto', 'themerrdb', 0, -14.0, -1.0)""", (NOW,))
    with get_conn(db) as conn:
        rows = _broken_rows(conn)
        assert rows and _entry(rows[0])["is_anime"] is True, (
            "the canonical-health OPEN link routed anime rows to /tv")
        rep = build_report(conn)
        assert rep["loudest"][0]["is_anime"] is True, (
            "the loudness-outlier link routed anime rows to /tv")


def test_both_remaining_producers_route_anime_first():
    for fn in ("function link(r) {", "function outRow(r) {"):
        i = APP_JS.index(fn)
        blk = APP_JS[i:APP_JS.index("new URLSearchParams()", i)]
        assert "r.is_anime ? '/anime'" in blk, fn
        assert blk.index("r.is_anime") < blk.index("=== 'movie' ? '/movies'"), fn
        assert "=== 'movie' ? '/movies' : '/tv';" in blk, fn


# ── 6: key repeats are dropped ───────────────────────────────


def test_keydown_drops_key_repeats():
    i = APP_JS.index("listEl.addEventListener('keydown'")
    blk = APP_JS[i:APP_JS.index("openNotifRow(row);", i)]
    assert "if (e.repeat)" in blk, (
        "a HELD Enter auto-repeats past the 400ms absorb onto the parked "
        ".notif-main and navigates — repeats are never a deliberate activation")
    assert blk.index("if (e.repeat)") < blk.index("closest('.notif-row')")


# ── docs + markers ───────────────────────────────────────────


def test_no_stale_404_narration_remains():
    assert "on 404 / non-art" not in APP_JS
    assert "404 on no-art so the carousel" not in API_PY
    assert APP_JS.count("// v0.51.310: .jpg spelling") == 3, (
        "each art emitter carries its marker (CLAUDE.md: markers on "
        "load-bearing lines)")


def test_readme_says_why_cache_assets_stays_off():
    readme = (REPO / "README.md").read_text()
    i = readme.index("**Cache Assets**")
    j = readme.find("\n* ", i + 1)          # the bullet's own extent (structural)
    bullet = readme[i:j if j != -1 else len(readme)]
    assert "must stay off" in bullet
    assert "auth_request" in bullet


def test_v0_51_311_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.311: " in init_py
