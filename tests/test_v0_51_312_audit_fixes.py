"""v0.51.312 — audit of the .311 review batch: the fixes, driven.

  1. The five widened lookups (and the resolver) take the theme_id arm for
     guid-NULL rows ONLY. A row Fix-Matched away keeps a stale theme_id
     (the relink re-points it only when the NEW guid has a ThemerrDB
     row), and MAX()/COUNT()/EXISTS cannot prefer — so it was borrowing
     its has_theme / edition onto another title's card.
  2. An empty 200 image body is a FAILURE (uncacheable + warn-once), not
     designed no-art. Any unknown sentinel string is a failure too.
  3. Vary: Cookie dropped — it keyed the browser cache on the cookie value.
  4. The .311 pins the audit proved escapable: the global tier fallback,
     the single-edition gate and the presence check get behavioral pins.
  5. The .261 census: one walk per file caching only the small window
     lists (the tree cache held ~190 MB); `max(a - N, 0)` counted too.
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
AUTH = {"X-Authentik-Username": "testadmin"}
NOW = "2026-09-02T12:00:00+00:00"


@pytest.fixture
def app(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s


def _sections(conn, *ids):
    for sid, anime in ids:
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES (?, 'S', 'show', ?, 0, ?, 1, ?, ?)""",
            (sid, anime, f"s{sid}", NOW, NOW))


def _theme(conn, tmdb, title):
    return conn.execute(
        """INSERT INTO themes (media_type, tmdb_id, title, upstream_source,
             last_seen_sync_at, first_seen_sync_at)
           VALUES ('tv', ?, ?, 'imdb', ?, ?)""", (tmdb, title, NOW, NOW)).lastrowid


def _item(conn, rk, sec, guid, tid, *, edition="", has=0, indep=0, folder="/x"):
    conn.execute(
        """INSERT INTO plex_items (rating_key, section_id, media_type, title,
             guid_tmdb, theme_id, folder_path, edition_key, has_theme,
             plex_independent_theme, first_seen_at, last_seen_at)
           VALUES (?, ?, 'show', 'T', ?, ?, ?, ?, ?, ?, ?, ?)""",
        (rk, sec, guid, tid, folder, edition, has, indep, NOW, NOW))


# ── 1. the stale theme_id row is EXCLUDED, not merely out-ranked ──


def test_fix_matched_away_row_cannot_lend_its_theme(app):
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("2", 0))
        x = _theme(conn, 312001, "X")
        # X's own row: unthemed, untagged
        _item(conn, "9002", "2", 312001, x, has=0, indep=0)
        # Fix-Matched X→Y where Y is NOT in ThemerrDB: guid rewritten, theme_id
        # stale, Plex serving Y's agent theme, and a tagged edition folder.
        _item(conn, "9001", "2", 312002, x, edition="ext", has=1, indep=1,
              folder="/t/Y {edition-ext}")
    body = c.get("/api/items/tv/312001?section_id=2", headers=AUTH).json()
    assert body["plex_rating_key"] == "9002"
    assert body["plex_has_theme"] == 0 and body["plex_independent_theme"] == 0, (
        "MAX() over a two-arm match borrowed the Fix-Matched-away row's "
        "has_theme onto X's card — the arm must apply to guid-NULL rows only")
    assert body.get("section_context", {}).get("edition") in (None, ""), (
        "the stale row's {edition-ext} folder must not label X's card")


# ── 4. the .311 pins the audit proved escapable ───────────────


def test_global_tier_resolves_a_guidless_row_without_a_section(app):
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("1", 1))
        x = _theme(conn, 312002, "A")
        _item(conn, "5555", "1", None, x, has=1, indep=1)
    body = c.get("/api/items/tv/312002", headers=AUTH).json()   # no section
    assert body["plex_has_theme"] == 1 and body["plex_independent_theme"] == 1, (
        "the section-less deep link falls to the GLOBAL tier — guid-only "
        "there misses the AniDB row and paints 'no theme staged'")


def test_single_edition_gate_counts_a_guidless_tagged_row(app):
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("1", 1))
        x = _theme(conn, 312003, "B")
        _item(conn, "6001", "1", None, x, edition="ext", folder="/a/B {edition-ext}")
        # the SHARED '' local_files row the single-edition read-fallback reaches
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size, downloaded_at,
                 source_video_id, provenance, source_kind)
               VALUES ('tv', 312003, '1', '', 'b.mp3', 's', 1, ?, 'v', 'auto',
                       'themerrdb')""", (NOW,))
    body = c.get("/api/items/tv/312003?section_id=1&edition_key=ext",
                 headers=AUTH).json()
    assert body["local_file"] is not None, (
        "a guid-NULL tagged edition must count as the section's ONE edition — "
        "a guid-only COUNT saw zero, withheld the '' fallback, and the card "
        "read 'not downloaded' for a downloaded theme")


def test_presence_check_sees_a_guidless_row(app):
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("1", 1))
        x = _theme(conn, 312004, "C")
        _item(conn, "6002", "1", None, x)
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size, downloaded_at,
                 source_video_id, provenance, source_kind)
               VALUES ('tv', 312004, '1', '', 'c.mp3', 's', 1, ?, 'v', 'auto',
                       'themerrdb')""", (NOW,))
        conn.execute(
            """INSERT INTO pending_updates (media_type, tmdb_id, section_id,
                 edition_key, old_video_id, new_video_id, old_youtube_url,
                 new_youtube_url, upstream_edited_at, detected_at, decision,
                 kind)
               VALUES ('tv', 312004, '1', '', 'old', 'new',
                       'https://youtu.be/old', 'https://youtu.be/new',
                       ?, ?, NULL, 'upstream_changed')""", (NOW, NOW))
    body = c.get("/api/items/tv/312004?section_id=1&edition_key=",
                 headers=AUTH).json()
    assert body["pending_update"] is not None, (
        "has_presence matched guid_tmdb only — a theme_id-only row never "
        "surfaced its pending update on the card while /api/library lit UPD")


# ── 2. unknown sentinel → failure ─────────────────────────────


def test_unknown_sentinel_is_treated_as_failure(app, monkeypatch):
    from app.web import api as api_mod
    c, _ = app
    real = api_mod.run_in_threadpool
    async def _bogus(fn, *a, **k):
        # only the art fetch — the auth middleware offloads bcrypt through
        # the same helper and must keep working.
        if getattr(fn, "__name__", "") == "_fetch_plex_art_bytes":
            return "err"      # 3 chars: would unpack as a 1-byte "image"
        return await real(fn, *a, **k)
    monkeypatch.setattr(api_mod, "run_in_threadpool", _bogus)
    r = c.get("/api/plex/art/123.jpg", headers=AUTH)
    assert r.status_code == 204 and r.headers["Cache-Control"] == "no-store"


# ── 5. the census helpers ────────────────────────────────────


def test_backward_detector_accepts_either_max_arg_order(tmp_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "ratchet", REPO / "tests" / "test_v0_51_261_no_new_fixed_window_guards.py")
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    p = tmp_path / "sample.py"
    p.write_text("def f(src, i):\n"
                 "    a = src[max(i - 3000, 0):i]\n"
                 "    b = src[max(0, i - 400):i + 100]\n"
                 "    return a, b\n")
    assert m._backward_windows(p) == [(2, 3000), (3, 500)], (
        "`max(i - N, 0)` is the other common spelling — unseen, it banks "
        "nothing and trips nothing")
    assert not hasattr(m, "_tree"), (
        "the census must cache the small window lists, not parsed ASTs")


def test_v0_51_312_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.312: " in init_py
