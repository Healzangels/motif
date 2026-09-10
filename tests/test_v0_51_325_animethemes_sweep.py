"""v0.51.325 — ANIME THEMES tag 4: the review page (spec §3.6).

RUN SWEEP resolves every row of the included anime sections that has no motif
file and no user override, with one prefetch + cache-only resolves (no name
search), as a page-scoped background job — the loudness-audit / orphan-scan
shape, not an op kind (spec §6 decision 5). The report is a file; the read
side re-checks eligibility so applied rows drop out. Three buckets: READY TO
APPLY (clean + audio default → checkbox + APPLY SELECTED through each row's
own manual-url, Plex-served rows as backups), NEEDS A LOOK (glance → the
picker), NOT FOUND.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx
import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))
from app.core import animethemes as at  # noqa: E402
from test_v0_51_314_animethemes_resolver import BRIDGE_JSON, FakeAPI  # noqa: E402

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
TEMPLATE = (REPO / "app" / "web" / "templates" / "anime_themes.html").read_text()
SETTINGS = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
LIBRARY = (REPO / "app" / "web" / "templates" / "library.html").read_text()
SPEC = (REPO / "docs" / "specs" / "ANIMETHEMES_SPEC.md").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}
NOW = "2026-09-10T12:00:00+00:00"


def _client(fake: FakeAPI) -> at.AnimeThemesClient:
    hc = httpx.Client(transport=httpx.MockTransport(fake.handler), base_url=at.API_BASE)
    return at.AnimeThemesClient(client=hc, min_interval_s=0.0, sleep=lambda s: None)


# ── 1. the sweep, offline on the tag-1 fake ───────────────────


def _rows():
    return [
        {"rating_key": "1", "title": "Cowboy Bebop", "year": "1998", "guid_tvdb": 76885, "guid_tmdb": 30991,
         "media_type": "show", "section_id": "3", "section_title": "Anime", "has_theme": 0},
        # 72025 has seasons 1..3 in the bridge; season 1 → anidb 1 → Crest (1999) but Plex says 2003 → glance
        {"rating_key": "2", "title": "Crest of the Stars", "year": "2003", "guid_tvdb": 72025, "guid_tmdb": None,
         "media_type": "show", "section_id": "3", "section_title": "Anime", "has_theme": 1},
        {"rating_key": "3", "title": "Nothing Bridged", "year": "2010", "guid_tvdb": 1, "guid_tmdb": 2,
         "media_type": "show", "section_id": "3", "section_title": "Anime", "has_theme": 0},
    ]


def test_sweep_buckets_rows_and_prefetches_once():
    fake = FakeAPI()
    fake.search_results = [(999, "Nothing Bridged", 2010, [("OP1", None, 1, "BD", 100)])]
    client = _client(fake)
    bridge = at.Bridge.from_json(BRIDGE_JSON)
    seen = []
    out = at.sweep(_rows(), bridge, client, progress_cb=lambda d, t: seen.append((d, t)))
    by = {r["rating_key"]: r for r in out}
    assert by["1"]["group"] == "ready" and by["1"]["confidence"] == "clean"
    assert by["1"]["default"]["link"] and by["1"]["default"]["type"] == "OP" and by["1"]["default"]["sequence"] in (None, 1)
    assert by["1"]["name"] == "Cowboy Bebop" and by["1"]["anidb"] == 23 and by["1"]["plex_has_theme"] == 0
    assert by["2"]["group"] == "review" and by["2"]["confidence"] == "glance" and by["2"]["plex_has_theme"] == 1
    assert "year differs" in by["2"]["reason"]
    assert by["3"]["group"] == "unresolved" and by["3"]["confidence"] is None
    assert seen == [(1, 3), (2, 3), (3, 3)]
    paths = [r.url.path for r in fake.calls]
    assert "q" not in {k for r in fake.calls for k, _ in r.url.params.multi_items()}, (
        "no name search in the sweep — the picker does that on a click (spec §3.6)")
    assert paths.count("/resource") == 1 and paths.count("/anime") == 1, (
        "one prefetch warms the cache; the per-row loop is cache-only")


def test_sweep_stops_between_rows_on_cancel():
    fake = FakeAPI()
    calls = {"n": 0}

    def cancel():
        calls["n"] += 1
        return calls["n"] >= 2  # first row resolves, the second check cancels
    out = at.sweep(_rows(), at.Bridge.from_json(BRIDGE_JSON), _client(fake), cancel_check=cancel)
    assert [r["rating_key"] for r in out] == ["1"]


def test_sweep_row_shape_is_the_page_contract():
    fake = FakeAPI()
    out = at.sweep(_rows()[:1], at.Bridge.from_json(BRIDGE_JSON), _client(fake))
    r = out[0]
    for k in ("rating_key", "title", "year", "media_type", "section_id", "section_title", "guid_tmdb",
              "plex_has_theme", "confidence", "via", "reason", "group", "name", "at_year", "anidb",
              "seasons", "default"):
        assert k in r, k
    assert isinstance(r["rating_key"], str)
    assert set(r["default"]) >= {"link", "size", "theme", "song", "artists", "name", "year", "type", "sequence"}


# ── 2. eligibility: no motif file, no user override, anime only ─


@pytest.fixture
def app_env(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import get_conn, init_db, transaction
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path); init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                     "VALUES ('3','Anime','show',1,0,'anime',1,?,?)", (NOW, NOW))
        conn.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                     "VALUES ('4','TV','show',0,0,'tv',1,?,?)", (NOW, NOW))
        conn.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                     "VALUES ('5','Anime 4K','show',1,1,'anime4k',0,?,?)", (NOW, NOW))
        ins = ("INSERT INTO plex_items (rating_key, section_id, media_type, title, year, guid_tvdb, guid_tmdb, folder_path, edition_key, has_theme, theme_id, first_seen_at, last_seen_at) "
               "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
        conn.execute(ins, ("777", "3", "show", "Cowboy Bebop", "1998", 76885, 30991, "/data/anime/CB", "", 0, None, NOW, NOW))
        conn.execute(ins, ("778", "3", "show", "Bleach", "2004", 74796, 30984, "/data/anime/BL", "", 1, None, NOW, NOW))   # P row: in
        conn.execute(ins, ("779", "3", "show", "Themed Already", "2010", 1, 1001, "/data/anime/TA", "", 1, None, NOW, NOW))  # local file: out
        conn.execute(ins, ("780", "3", "show", "Overridden", "2011", 2, 1002, "/data/anime/OV", "", 0, None, NOW, NOW))       # override: out
        conn.execute(ins, ("781", "4", "show", "Not Anime", "2012", 3, 1003, "/data/tv/NA", "", 0, None, NOW, NOW))             # non-anime: out
        conn.execute(ins, ("782", "5", "show", "Excluded Section", "2013", 4, 1004, "/data/a4k/ES", "", 0, None, NOW, NOW))  # not included: out
        conn.execute(ins, ("783", "3", "collection", "Some Collection", "", None, None, "", "", 0, None, NOW, NOW))            # collection: out
        # an orphan row applied earlier: ThemerrDB doesn't track it, so manual-url minted a
        # plex_orphan theme (synthetic tmdb -5) and keyed the override by THAT — guid never matches
        conn.execute("INSERT INTO themes (media_type, tmdb_id, title, year, upstream_source, last_seen_sync_at, first_seen_sync_at, title_norm) "
                     "VALUES ('tv', -5, 'Orphan Applied', '2014', 'plex_orphan', ?, ?, 'orphan applied')", (NOW, NOW))
        oid = conn.execute("SELECT id FROM themes WHERE tmdb_id = -5").fetchone()[0]
        conn.execute(ins, ("784", "3", "show", "Orphan Applied", "2014", None, 1005, "/data/anime/OA", "", 1, oid, NOW, NOW))     # orphan override: out
        conn.execute("INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, set_at, set_by, section_id, intent) "
                     "VALUES ('tv', -5, 'https://a.animethemes.moe/Orphan-OP1.ogg', ?, 'testadmin', '3', 'backup')", (NOW,))
        # an orphan row whose download already LANDED: the local_files row is keyed by the
        # orphan theme's synthetic tmdb (-6) with theme_id NULL (the worker's writer) — out
        conn.execute("INSERT INTO themes (media_type, tmdb_id, title, year, upstream_source, last_seen_sync_at, first_seen_sync_at, title_norm) "
                     "VALUES ('tv', -6, 'Orphan Landed', '2015', 'plex_orphan', ?, ?, 'orphan landed')", (NOW, NOW))
        lid = conn.execute("SELECT id FROM themes WHERE tmdb_id = -6").fetchone()[0]
        conn.execute(ins, ("785", "3", "show", "Orphan Landed", "2015", None, 1006, "/data/anime/OL", "", 0, lid, NOW, NOW))       # orphan local file: out
        conn.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id, file_path, file_size, downloaded_at, source_video_id, provenance, source_kind) "
                     "VALUES ('tv', -6, '3', NULL, 'anime/Orphan Landed (2015)/theme.mp3', 10, ?, 'at-Orphan-OP1', 'manual', 'url')", (NOW,))
        conn.execute("INSERT INTO themes (media_type, tmdb_id, title, year, upstream_source, last_seen_sync_at, first_seen_sync_at, title_norm) "
                     "VALUES ('tv', 1001, 'Themed Already', '2010', 'themoviedb', ?, ?, 'themed already')", (NOW, NOW))
        tid = conn.execute("SELECT id FROM themes WHERE tmdb_id = 1001").fetchone()[0]
        conn.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id, file_path, file_size, downloaded_at, source_video_id, provenance, source_kind) "
                     "VALUES ('tv', 1001, '3', ?, 'anime/Themed Already (2010)/theme.mp3', 10, ?, 'abc', 'auto', 'themerrdb')", (tid, NOW))
        conn.execute("INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, set_at, set_by, section_id, intent) "
                     "VALUES ('tv', 1002, 'https://www.youtube.com/watch?v=zzzzzzzzzzz', ?, 'testadmin', '3', 'replace')", (NOW,))
    return TestClient(create_app(s)), s


def test_eligible_rows_are_anime_rows_with_no_motif_theme_and_no_override(app_env):
    from app.core.db import get_conn
    from app.web.api import _animethemes_eligible_rows
    _, s = app_env
    with get_conn(s.db_path) as conn:
        rows = _animethemes_eligible_rows(conn)
    assert [r["rating_key"] for r in rows] == ["778", "777"], (
        "ordered by section then title; only the two open anime rows — the orphan-keyed override (784) "
        "and the orphan-keyed local file (785) are out too")
    assert {r["section_title"] for r in rows} == {"Anime"}
    assert next(r for r in rows if r["rating_key"] == "778")["has_theme"] == 1, "Plex-served rows stay in (backup default)"


# ── 3. endpoints + page ───────────────────────────────────────


def _reset_state():
    from app.web import api as api_mod
    with api_mod._AT_SWEEP_LOCK:
        api_mod._AT_SWEEP_STATE.clear()
        api_mod._AT_SWEEP_STATE["status"] = "idle"


def test_start_runs_the_sweep_and_writes_the_report(app_env, monkeypatch):
    client, s = app_env
    _reset_state()
    fake = FakeAPI()
    monkeypatch.setattr(at, "load_bridge", lambda cache_dir, **kw: at.Bridge.from_json(BRIDGE_JSON))
    real = _client(fake)  # built BEFORE the class is patched
    monkeypatch.setattr(at, "AnimeThemesClient", lambda *a, **kw: real)
    assert client.get("/api/admin/animethemes-sweep", headers=AUTH).json() == {"status": "none"}
    r = client.post("/api/admin/animethemes-sweep/start", headers=AUTH)
    assert r.status_code == 200 and r.json()["started"] is True
    for _ in range(200):
        st = client.get("/api/admin/animethemes-sweep/status", headers=AUTH).json()
        if st["status"] in ("done", "failed", "cancelled"):
            break
        time.sleep(0.02)
    assert st["status"] == "done", st
    assert st["counts"] == {"ready": 1, "review": 0, "unresolved": 1, "total": 2}
    assert "cancel" not in st
    rep = json.loads((s.config_dir / "animethemes" / "sweep.json").read_text())
    assert rep["eligible"] == 2 and rep["sections"] == [{"section_id": "3", "title": "Anime"}]
    assert rep["api_calls"] >= 1 and rep["cancelled"] is False
    got = client.get("/api/admin/animethemes-sweep", headers=AUTH).json()
    assert got["status"] == "ok" and got["applied"] == 0
    assert {r["rating_key"]: r["group"] for r in got["rows"]} == {"777": "ready", "778": "unresolved"}


def test_report_flags_rows_applied_since_the_sweep(app_env):
    client, s = app_env
    d = s.config_dir / "animethemes"; d.mkdir(parents=True, exist_ok=True)
    (d / "sweep.json").write_text(json.dumps({
        "scanned_at": NOW, "cancelled": False, "eligible": 3, "api_calls": 2, "sections": [],
        "counts": {"ready": 3, "review": 0, "unresolved": 0, "total": 3},
        "rows": [{"rating_key": "777", "group": "ready"}, {"rating_key": "778", "group": "ready"},
                 {"rating_key": "780", "group": "ready"}]}))
    got = client.get("/api/admin/animethemes-sweep", headers=AUTH).json()
    assert got["status"] == "ok"
    assert {r["rating_key"]: r["applied"] for r in got["rows"]} == {"777": False, "778": False, "780": True}, (
        "780 carries a user override now → applied; the page drops it without a re-sweep")
    assert got["applied"] == 1


def test_endpoints_and_page_need_admin(app_env):
    client, _ = app_env
    for path, meth in (("/api/admin/animethemes-sweep/start", "post"), ("/api/admin/animethemes-sweep/cancel", "post"),
                       ("/api/admin/animethemes-sweep/status", "get"), ("/api/admin/animethemes-sweep", "get")):
        assert getattr(client, meth)(path).status_code in (401, 403), path
    # the page behaves exactly like its sibling admin pages when unauthenticated
    page = client.get("/admin/anime-themes")
    sibling = client.get("/admin/orphans")
    assert page.status_code == sibling.status_code and "at-sweep-root" not in page.text


def test_page_renders_the_three_buckets_and_ssr_locks_while_running(app_env):
    client, _ = app_env
    _reset_state()
    html = client.get("/admin/anime-themes", headers=AUTH).text
    for i in ("at-sweep-root", "at-sweep-btn", "at-ready-block", "at-apply-btn", "at-review-block",
              "at-unresolved-block", "at-clear-block", "at-select-page"):
        assert f'id="{i}"' in html, i
    assert "// RUN SWEEP" in html and "disabled" not in html.split('id="at-sweep-btn"')[1][:80]
    from app.web import api as api_mod
    with api_mod._AT_SWEEP_LOCK:
        api_mod._AT_SWEEP_STATE["status"] = "running"
    try:
        html = client.get("/admin/anime-themes", headers=AUTH).text
        assert "// SWEEPING…" in html and 'id="at-sweep-btn" class="btn lib-source-themerrdb" disabled' in html
    finally:
        _reset_state()


def test_cancel_sets_the_flag_only_while_running(app_env):
    client, _ = app_env
    _reset_state()
    assert client.post("/api/admin/animethemes-sweep/cancel", headers=AUTH).json() == {"ok": True, "cancelling": False}
    from app.web import api as api_mod
    with api_mod._AT_SWEEP_LOCK:
        api_mod._AT_SWEEP_STATE["status"] = "running"
    try:
        assert client.post("/api/admin/animethemes-sweep/cancel", headers=AUTH).json()["cancelling"] is True
        assert api_mod._AT_SWEEP_STATE["cancel"] is True
    finally:
        _reset_state()


# ── 4. the page binder (source pins) ──────────────────────────


def _binder() -> str:
    return slice_between(APP_JS, "  function bindAnimeThemesSweep() {", "  function bindLoudnessAudit() {")


def test_binder_is_bound_and_gated_on_the_page_root():
    b = _binder()
    assert "document.getElementById('at-sweep-root')" in b and "if (!root) return;" in b
    assert "    bindAnimeThemesSweep();" in APP_JS


def test_apply_walks_each_rows_manual_url_with_the_pickers_origin():
    b = _binder()
    apply = slice_between(b, "applyBtn.addEventListener('click'", "\n    });")
    assert "/api/plex_items/${encodeURIComponent(r.rating_key)}/manual-url" in apply
    assert "source: 'animethemes'" in apply and "anidb: r.anidb" in apply
    assert "if (r.plex_has_theme) body.download_only = true;" in apply, "Plex-served rows land as backups (decision 2)"
    assert "r.group === 'ready'" in apply, "only the clean bucket is bulk-appliable (spec §3.7)"
    assert "failed += 1" in apply and "for (const r of targets)" in apply, "sequential, fail-and-continue"


def test_every_bucket_offers_the_picker_and_it_carries_the_plex_state():
    b = _binder()
    assert 'data-act="at-picker"' in b
    assert "openAnimeThemesDialog({ ratingKey: b.dataset.rk" in b
    assert "srcLetter: b.dataset.hasTheme ? 'P' : ''" in b, "download_only pre-ticks for Plex-served rows"
    assert b.count("pickerBtn(r)") == 3


def test_binder_uses_the_shared_formatters_not_shadows():
    b = _binder()
    assert "const fmt =" not in b and "fmt.bytes(" in b and "fmt.timeAuto(" in b
    assert "_atThemeLabel({ type: d.type, sequence: d.sequence, slug: d.theme })" in b


def test_poll_drives_the_run_button_and_reloads_on_completion():
    b = _binder()
    poll = slice_between(b, "async function poll() {", "\n    }\n")
    assert "/api/admin/animethemes-sweep/status" in poll
    assert "setTimeout(poll, 1500)" in poll
    assert "await load();" in poll and "'cancelled'" in poll


# ── 5. entry points, spec, version ────────────────────────────


def test_entry_points_and_template_reuse_primitives():
    assert 'href="/admin/anime-themes"' in SETTINGS and "// OPEN ANIME THEMES ▸" in SETTINGS
    lib = slice_between(LIBRARY, 'id="library-refresh-btn"', "</div>")
    # rendered on every library tab (the v0.51.12 in-place switch forbids tab gates
    # in the shared markup); SSR display by expression, JS keeps it in sync
    assert 'href="/admin/anime-themes" data-tab-only="anime"' in lib and "{% if tab" not in lib
    assert "style=\"{{ '' if tab == 'anime' else 'display:none' }}\"" in lib
    assert "function _syncTabOnlyEls(tab) {" in APP_JS
    hyd = slice_between(APP_JS, "function hydrateLibraryStateForTab(tab, sp) {", "\n  }")
    assert "_syncTabOnlyEls(tab);" in hyd
    assert APP_JS.count("_syncTabOnlyEls(") >= 3, "helper + the two tab-set sites"
    assert TEMPLATE.count('class="table-scroll"') == 3 and 'class="table table-compact"' in TEMPLATE
    assert 'class="btn lib-source-themerrdb"' in TEMPLATE and 'class="btn btn-tiny btn-warn"' in TEMPLATE


def test_spec_records_the_page_scoped_decision():
    assert "5. ~~Sweep as an `op_progress` kind" in SPEC and "DECIDED 2026-09-10 (tag 4): page-scoped" in SPEC
    assert "shipped v0.51.325" in SPEC
    assert "_AT_SWEEP_STATE" in API_PY and "'animethemes_sweep'" not in (REPO / "app" / "core" / "db.py").read_text(), (
        "no op kind, no schema bump")


def test_v0_51_325_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.325: ANIME THEMES tag 4" in init_py
