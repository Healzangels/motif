"""v0.51.312 → v0.51.313 — the INFO card's guid-first row model, driven.

.312 narrowed the theme_id arm to guid-NULL rows; the .313 audit measured
that this broke motif's own bonds — SET URL / UPLOAD orphans and the
imdb/title-year relinks all stamp theme_id on rows carrying a REAL guid
ThemerrDB does not know — and those are indistinguishable in SQL from a
Fix-Matched-away stale link. The model that handles both:

  guid-matched rows are authoritative; theme-linked rows count only
  when the title has NO guid-matched row in scope.

Aggregates (theme tiers, single-edition gate, presence) express it as
`theme_id = ? AND NOT EXISTS (guid row in scope)`; single-row picks (the
poster resolver, the edition-folder lookup) as a guid-first ORDER BY.
Both write gates (unplace, delete-sidecar) mirror the read gate. Residual,
accepted: a title with NO guid row at all still admits a stale link (the
schema records no bond provenance).
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}
NOW = "2026-09-02T12:00:00+00:00"


def _make_app(tmp_path, monkeypatch, *, plex=False):
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


@pytest.fixture
def app(tmp_path, monkeypatch):
    return _make_app(tmp_path, monkeypatch)


def _sections(conn, *ids):
    for sid, anime in ids:
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES (?, 'S', 'show', ?, 0, ?, 1, ?, ?)""",
            (sid, anime, f"s{sid}", NOW, NOW))


def _theme(conn, tmdb, title, *, upstream="imdb"):
    return conn.execute(
        """INSERT INTO themes (media_type, tmdb_id, title, upstream_source,
             last_seen_sync_at, first_seen_sync_at)
           VALUES ('tv', ?, ?, ?, ?, ?)""",
        (tmdb, title, upstream, NOW, NOW)).lastrowid


def _item(conn, rk, sec, guid, tid, *, edition="", has=0, indep=0, folder="/x"):
    conn.execute(
        """INSERT INTO plex_items (rating_key, section_id, media_type, title,
             guid_tmdb, theme_id, folder_path, edition_key, has_theme,
             plex_independent_theme, first_seen_at, last_seen_at)
           VALUES (?, ?, 'show', 'T', ?, ?, ?, ?, ?, ?, ?, ?)""",
        (rk, sec, guid, tid, folder, edition, has, indep, NOW, NOW))


def _local(conn, tmdb, sec, edition=""):
    conn.execute(
        """INSERT INTO local_files (media_type, tmdb_id, section_id,
             edition_key, file_path, file_sha256, file_size, downloaded_at,
             source_video_id, provenance, source_kind)
           VALUES ('tv', ?, ?, ?, 'f.mp3', 's', 1, ?, 'v', 'auto',
                   'themerrdb')""", (tmdb, sec, edition, NOW))


# ── the stale row loses to the title's own guid row ──────────


@pytest.mark.parametrize("stale_first", [True, False])
def test_fix_matched_away_row_cannot_lend_its_theme(app, stale_first):
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("2", 0))
        x = _theme(conn, 312001, "X")
        rows = [
            # Fix-Matched X→Y (Y ABSENT from ThemerrDB, so the relink never
            # re-points): guid rewritten, theme_id stale, Plex serving Y's
            # theme, a tagged edition folder.
            lambda: _item(conn, "9001", "2", 312002, x, edition="ext", has=1,
                          indep=1, folder="/t/Y {edition-ext}"),
            # X's own row: unthemed, untagged
            lambda: _item(conn, "9002", "2", 312001, x, has=0, indep=0),
        ]
        if not stale_first:
            rows.reverse()
        for r in rows:
            r()
    body = c.get("/api/items/tv/312001?section_id=2", headers=AUTH).json()
    assert body["plex_rating_key"] == "9002"
    assert body["plex_has_theme"] == 0 and body["plex_independent_theme"] == 0, (
        "with X's own guid row present, the Fix-Matched-away row must not "
        "lend its has_theme — regardless of insertion / index-scan order")
    assert body["section_context"]["edition"] in (None, ""), (
        "the stale row's {edition-ext} folder must not label X's card")


def test_single_edition_gate_ignores_the_stale_edition(app):
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("2", 0))
        x = _theme(conn, 312005, "X")
        _item(conn, "9101", "2", 312006, x, edition="ext", has=1,
              folder="/t/Y {edition-ext}")
        _item(conn, "9102", "2", 312005, x)
        _local(conn, 312005, "2", "")
    body = c.get("/api/items/tv/312005?section_id=2&edition_key=ext",
                 headers=AUTH).json()
    assert body["local_file"] is not None, (
        "X has ONE edition (its own '' row); the stale row's 'ext' must not "
        "inflate the count and withhold the '' read-fallback")


def test_global_tier_ignores_the_stale_row(app):
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("2", 0))
        x = _theme(conn, 312007, "X")
        _item(conn, "9201", "2", 312008, x, has=1, indep=1)
        _item(conn, "9202", "2", 312007, x, has=0, indep=0)
    body = c.get("/api/items/tv/312007", headers=AUTH).json()   # no section
    assert body["plex_has_theme"] == 0 and body["plex_independent_theme"] == 0


# ── motif's OWN bonds are honoured (the .312 regression) ─────


def test_orphan_bonded_row_is_its_own_card(app):
    # SET URL / UPLOAD MP3 on a TDB-less title: a plex_orphan theme with a
    # NEGATIVE tmdb_id stamped onto a row that KEEPS Plex's real guid.
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("1", 0))
        o = _theme(conn, -5, "Orphan", upstream="plex_orphan")
        _item(conn, "8001", "1", 999001, o, edition="ext", has=1, indep=1,
              folder="/m/O {edition-ext}")
        _local(conn, -5, "1", "")
    body = c.get("/api/items/tv/-5?section_id=1&edition_key=ext",
                 headers=AUTH).json()
    assert body["plex_rating_key"] == "8001"
    assert body["plex_has_theme"] == 1 and body["plex_independent_theme"] == 1
    assert body["local_file"] is not None, (
        ".312's guid-NULL-only arm dropped every SET URL / UPLOAD theme on a "
        "TDB-less title from its own card: no poster, 'no theme staged', "
        "'not downloaded' — a real guid with no TDB entry is a legitimate "
        "bond, not evidence of a stale Fix Match")


def test_section_tier_reads_the_sections_own_orphan_row(app):
    # discriminates the SECTION tier from the global fallback for a bonded
    # row with a real guid: the same orphan theme sits in two sections with
    # DIFFERENT has_theme; the section tier must answer for its own section
    # (the global MAX would say 1 for both).
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("1", 0), ("2", 0))
        o = _theme(conn, -6, "Orphan2", upstream="plex_orphan")
        _item(conn, "8101", "1", 999002, o, has=1, indep=1)
        _item(conn, "8102", "2", 999002, o, has=0, indep=0)
    body = c.get("/api/items/tv/-6?section_id=2", headers=AUTH).json()
    assert body["plex_has_theme"] == 0 and body["plex_independent_theme"] == 0, (
        "the section tier must include the section's own bonded row itself — "
        "falling through to the global MAX borrows section 1's theme")


def test_cross_keyed_bond_surfaces_state_and_pending_update(app):
    # the imdb / title+year relink: a real TDB theme keyed under a DIFFERENT
    # tmdb_id than Plex's guid.
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("1", 0))
        t = _theme(conn, 777, "Real")
        _item(conn, "8002", "1", 778, t, has=1, indep=1)
        _local(conn, 777, "1", "")
        conn.execute(
            """INSERT INTO pending_updates (media_type, tmdb_id, section_id,
                 edition_key, old_video_id, new_video_id, old_youtube_url,
                 new_youtube_url, upstream_edited_at, detected_at, decision,
                 kind)
               VALUES ('tv', 777, '1', '', 'old', 'new',
                       'https://youtu.be/old', 'https://youtu.be/new',
                       ?, ?, NULL, 'upstream_changed')""", (NOW, NOW))
    body = c.get("/api/items/tv/777?section_id=1&edition_key=",
                 headers=AUTH).json()
    assert body["plex_rating_key"] == "8002" and body["plex_has_theme"] == 1
    assert body["pending_update"] is not None, (
        "the library lit UPD for this row; the card must surface the same "
        "pending update (v1.12.117 mirror)")


# ── guid-NULL rows (AniDB / collections) at every site ───────


def test_global_tier_resolves_a_guidless_row_without_a_section(app):
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("1", 1))
        x = _theme(conn, 312002, "A")
        _item(conn, "5555", "1", None, x, has=1, indep=1)
    body = c.get("/api/items/tv/312002", headers=AUTH).json()
    assert body["plex_has_theme"] == 1 and body["plex_independent_theme"] == 1


def test_single_edition_gate_counts_a_guidless_tagged_row(app):
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("1", 1))
        x = _theme(conn, 312003, "B")
        _item(conn, "6001", "1", None, x, edition="ext", folder="/a/B {edition-ext}")
        _local(conn, 312003, "1", "")
    body = c.get("/api/items/tv/312003?section_id=1&edition_key=ext",
                 headers=AUTH).json()
    assert body["local_file"] is not None


def test_presence_check_sees_a_guidless_row(app):
    from app.core.db import get_conn, transaction
    c, s = app
    with get_conn(s.db_path) as conn, transaction(conn):
        _sections(conn, ("1", 1))
        x = _theme(conn, 312004, "C")
        _item(conn, "6002", "1", None, x)
        _local(conn, 312004, "1", "")
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
    assert body["pending_update"] is not None


def test_write_gates_mirror_the_read_gate():
    sites = [m.start() for m in re.finditer(
        r"SELECT COUNT\(DISTINCT edition_key\) FROM plex_items", API_PY)]
    assert len(sites) == 3, "read gate + unplace gate + delete-sidecar gate"
    for i in sites:
        seg = API_PY[i:API_PY.index(".fetchone()", i)]
        assert "NOT EXISTS (" in seg and "theme_id" in seg, (
            "every edition-count gate must use the same guid-first predicate "
            "— a guid-only WRITE gate withholds the '' fallback the READ gate "
            "granted ('card says placed, LPS does 0/0')")


# ── the art proxy handler: any non-tuple is a failure ─────────


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


@pytest.mark.parametrize("bogus", ["err", None, b"err", ("a", "b")])
def test_unknown_fetch_result_is_a_failure(tmp_path, monkeypatch, caplog, bogus):
    from app.web import api as api_mod
    c, _ = _make_app(tmp_path, monkeypatch)
    real = api_mod.run_in_threadpool
    async def _bogus(fn, *a, **k):
        if getattr(fn, "__name__", "") == "_fetch_plex_art_bytes":
            return bogus
        return await real(fn, *a, **k)
    monkeypatch.setattr(api_mod, "run_in_threadpool", _bogus)
    with caplog.at_level(logging.WARNING):
        r = c.get("/api/plex/art/123.jpg", headers=AUTH)
    assert r.status_code == 204 and r.headers["Cache-Control"] == "no-store", (
        "a str-only guard let None / bytes / a short tuple reach the unpack "
        "and 500 per tile")
    assert any("unknown fetch result" in rec.message for rec in caplog.records), (
        "class 9: an unrecognised value must leave a breadcrumb")


def test_empty_body_warns_once_on_its_own_flag(tmp_path, monkeypatch, caplog):
    import httpx
    from app.web import api as api_mod
    c, _ = _make_app(tmp_path, monkeypatch, plex=True)
    monkeypatch.setattr(api_mod, "_PLEX_ART_EMPTY_WARNED", False)
    monkeypatch.setattr(api_mod, "_PLEX_ART_FETCH_WARNED", False)
    monkeypatch.setattr(httpx, "Client",
                        lambda **kw: _FakeClient(_Resp(200, "image/jpeg", b"")))
    with caplog.at_level(logging.DEBUG):
        c.get("/api/plex/art/1.jpg", headers=AUTH)
        c.get("/api/plex/art/2.jpg", headers=AUTH)
    warns = [r for r in caplog.records
             if r.levelno >= logging.WARNING and r.name.startswith("app.")]
    assert len(warns) == 1 and "EMPTY" in warns[0].message, (
        "one WARNING for a persistent empty poster, then debug")
    # ...and it must NOT have consumed the transport-failure warn-once.
    caplog.clear()
    monkeypatch.setattr(httpx, "Client",
                        lambda **kw: _FakeClient(exc=httpx.ConnectError("boom")))
    with caplog.at_level(logging.WARNING):
        c.get("/api/plex/art/3.jpg", headers=AUTH)
    assert any("FAILED" in r.message for r in caplog.records), (
        "a per-ITEM empty poster must not burn the process-wide 'Plex is "
        "dead' warning a later real outage needs")


# ── the census helpers ────────────────────────────────────────


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
    assert m._backward_windows(p) == [(2, 3000), (3, 500)]
    # the memory invariant (not a name): the module's ONLY cache holds the
    # small (lineno, width) tuples, never parsed trees.
    caches = [n for n, v in vars(m).items() if hasattr(v, "cache_info")]
    assert caches == ["_windows"], caches
    val = m._windows(p)
    assert isinstance(val, tuple) and len(val) == 2
    assert all(isinstance(e, tuple) and len(e) == 2
               and all(isinstance(x, int) for x in e)
               for part in val for e in part)


def test_v0_51_312_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.312: " in init_py


def test_v0_51_313_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.313: " in init_py
