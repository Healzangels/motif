"""v1.13.88 — UPD breakdown re-alignment + topbar badge cache + customize move.

Three fixes the user surfaced after pulling v1.13.87:

1. **UPD cycle landing on wrong tab**: v1.13.86 tightened the
   library filter for `attn_pills=update` (added has-something +
   URL-diff predicates) but the breakdown SQL was left at the
   v1.13.84 (then-permissive) shape. They drifted again — same
   class as the v1.13.84/85/86 chain. the user's repro: badge
   "1 UPD" → click → /movies?fourk=true&attn_pills=update → 0
   matches. Breakdown said 4k movies had it; library filter
   said 0.

2. **Pills disappear after click**: badge HTML defaults to
   `hidden`; JS unhides only after the first /api/stats
   response. During heavy plex_enum (writer-lock contention on
   /api/stats) that response can be seconds slow. Pre-fix:
   pill invisible until the response lands. Fix: cache
   topbar counts in localStorage on every successful tick;
   pre-populate badges on page load before the first stats
   response.

3. **Customize layout button position**: moved from the hero
   description line to the bottom of the dashboard. Hero is
   high-attention; layout customization is low-frequency.

## v1.20.62 cleanup

The original breakdown tests here ran against `_upd_breakdown`,
a HAND-COPY of the production SQL. Over time it drifted from the
real query (it kept the title-wide `EXISTS` decision check, a
`guid_tmdb` join instead of `theme_id`, and a new-vs-applied
URL-diff instead of the v1.19.60 old-vs-new `real_diff` helper) —
so the tests passed while asserting a FICTION. That's exactly the
phantom-guard the v1.18.81 lesson warns about, and it's why these
tests did NOT catch the v1.20.62 section-scope drift. Rewritten to
hit the real `/api/stats` endpoint (`updates.tabs` / `pending`),
so they exercise the production query directly.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent

NOW = "2026-05-31T00:00:00+00:00"
TDB_URL = "https://www.youtube.com/watch?v=tdb"
NEW_URL = "https://www.youtube.com/watch?v=new"
AUTH = {"X-Authentik-Username": "testadmin"}


# ── Behavioral harness (real /api/stats, not a hand-copy) ───────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def _upd(client):
    r = client.get("/api/stats", headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()["updates"]


# ── Seeding (writes to the settings DB the app actually reads;
#    plex_items carry the theme_id linkage the production breakdown
#    JOINs on — the hand-copy's missing piece) ─────────────────────


def _seed_section(conn, *, section_id, is_4k=0, is_anime=0):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, ?, 'movie', ?, ?, ?, 1, ?, ?)",
        (section_id, f"sec{section_id}", is_anime, is_4k,
         f"sub{section_id}", NOW, NOW),
    )


def _seed_theme(conn, *, theme_id, tmdb_id):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " youtube_url, youtube_video_id, last_seen_sync_at, first_seen_sync_at)"
        " VALUES (?, 'movie', ?, ?, 'imdb', ?, 'tdb', ?, ?)",
        (theme_id, tmdb_id, f"x{tmdb_id}", TDB_URL, NOW, NOW),
    )


def _seed_plex_item(conn, *, tmdb_id, section_id, theme_id, rk=None):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type,"
        " theme_id, guid_tmdb, title, year, has_theme, local_theme_file,"
        " folder_path, plex_independent_theme, first_seen_at, last_seen_at)"
        " VALUES (?, ?, 'movie', ?, ?, ?, 2020, 0, 0, '/data/x', 0, ?, ?)",
        (rk or f"rk{tmdb_id}-{section_id}", section_id, theme_id, tmdb_id,
         f"x{tmdb_id}", NOW, NOW),
    )


def _seed_override(conn, *, tmdb_id, section_id, url):
    conn.execute(
        "INSERT INTO user_overrides (media_type, tmdb_id, section_id,"
        " youtube_url, set_at, set_by)"
        " VALUES ('movie', ?, ?, ?, ?, 'admin')",
        (tmdb_id, section_id, url, NOW),
    )


def _seed_local_and_placement(conn, *, tmdb_id, section_id):
    """themerrdb local file + placement → SRC=T."""
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, file_path,"
        " source_video_id, downloaded_at, source_kind)"
        " VALUES ('movie', ?, ?, 'x.mp3', 'tdb', ?, 'themerrdb')",
        (tmdb_id, section_id, NOW),
    )
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
        " placed_at, placement_kind, plex_refreshed)"
        " VALUES ('movie', ?, ?, '/data/x', ?, 'hardlink', 0)",
        (tmdb_id, section_id, NOW),
    )


def _seed_pure_p(conn, *, tmdb_id, section_id):
    """Plex serves a theme, motif owns nothing (no local_files /
    placement). SRC=P → ACCEPT is a no-op → must NOT surface."""
    conn.execute(
        "UPDATE plex_items SET has_theme = 1, plex_theme_verified_ok = 1,"
        " local_theme_file = 0 WHERE rating_key = ?",
        (f"rk{tmdb_id}-{section_id}",),
    )


def _seed_pending(conn, *, tmdb_id, new_url=NEW_URL, old_url=TDB_URL,
                  section_id="", kind="upstream_changed"):
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
        " decision, detected_at, old_youtube_url, new_youtube_url, kind)"
        " VALUES ('movie', ?, ?, 'pending', ?, ?, ?, ?)",
        (tmdb_id, section_id, NOW, old_url, new_url, kind),
    )


def _movies(tabs):
    return [t for t in tabs if t.get("tab") == "movies"]


# ── UPD breakdown re-alignment with the strict library filter ───


def test_upd_breakdown_excludes_pure_p_row(admin_client, tmp_path):
    """A tmdb has a plex_item + pending_update but src='P' (Plex
    serves, motif owns nothing) → library filter excludes (ACCEPT is
    a no-op) → the cycle breakdown must too."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, theme_id=10, tmdb_id=1)
        _seed_plex_item(conn, tmdb_id=1, section_id="1", theme_id=10)
        _seed_pure_p(conn, tmdb_id=1, section_id="1")
        _seed_pending(conn, tmdb_id=1)
        conn.commit()
    upd = _upd(admin_client)
    assert upd["tabs"] == [], f"pure-P must not surface; got {upd['tabs']}"
    assert upd["pending"] == 0


def test_upd_breakdown_excludes_no_op_url_match(admin_client, tmp_path):
    """A pending update whose new URL equals the old (no real diff),
    on a plain themerrdb T row → no actionable change → excluded by
    the v1.19.60 real_diff predicate."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, theme_id=10, tmdb_id=1)
        _seed_plex_item(conn, tmdb_id=1, section_id="1", theme_id=10)
        _seed_local_and_placement(conn, tmdb_id=1, section_id="1")
        # old == new → real_diff is FALSE.
        _seed_pending(conn, tmdb_id=1, old_url=TDB_URL, new_url=TDB_URL)
        conn.commit()
    upd = _upd(admin_client)
    assert upd["tabs"] == [], f"no-op diff must not surface; got {upd['tabs']}"
    assert upd["pending"] == 0


def test_upd_breakdown_includes_actionable_T_row(admin_client, tmp_path):
    """Real actionable case: src='T' + a genuine old→new URL diff →
    both the library filter and the cycle breakdown include it."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, theme_id=10, tmdb_id=1)
        _seed_plex_item(conn, tmdb_id=1, section_id="1", theme_id=10)
        _seed_local_and_placement(conn, tmdb_id=1, section_id="1")
        _seed_pending(conn, tmdb_id=1, old_url=TDB_URL, new_url=NEW_URL)
        conn.commit()
    upd = _upd(admin_client)
    movies = _movies(upd["tabs"])
    assert movies and movies[0]["fourk"] is False and movies[0]["count"] == 1, (
        f"actionable T row must surface as standard movies; got {upd['tabs']}"
    )
    assert upd["pending"] == 1


def test_upd_breakdown_partitions_correctly_with_mixed_state(admin_client, tmp_path):
    """the user's original repro shape: tmdb=1 has plex_items in BOTH
    standard movies (theme placed → src=T, actionable) and 4K movies
    (pure-P). A title-global pending. The breakdown must return ONLY
    standard movies — NOT 4K (where the section-scoped filter shows 0)."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1", is_4k=0)
        _seed_section(conn, section_id="2", is_4k=1)
        _seed_theme(conn, theme_id=10, tmdb_id=1)
        _seed_plex_item(conn, tmdb_id=1, section_id="1", theme_id=10)
        _seed_plex_item(conn, tmdb_id=1, section_id="2", theme_id=10)
        _seed_local_and_placement(conn, tmdb_id=1, section_id="1")  # std → T
        _seed_pure_p(conn, tmdb_id=1, section_id="2")               # 4k → P
        _seed_pending(conn, tmdb_id=1)                              # global ''
        conn.commit()
    upd = _upd(admin_client)
    tabs = upd["tabs"]
    std = [t for t in tabs if t.get("tab") == "movies" and not t["fourk"]]
    fourk = [t for t in tabs if t.get("tab") == "movies" and t["fourk"]]
    assert std and std[0]["count"] == 1, f"std movies must surface; got {tabs}"
    assert not fourk, f"4K (pure-P) must NOT surface; got {tabs}"
    # The global '' pending applies to both sections via the COALESCE
    # fallback, but only the std section is actionable → count == 1.
    assert upd["pending"] == 1, upd


def test_upd_breakdown_sql_in_production_has_url_diff_check():
    """Static guard: the production SQL must contain the URL-diff
    check. v1.13.86 added it to the library filter; v1.13.88 added
    it here. A regression that drops it would resurrect the user's
    bug — pure-P rows in the cycle, 0 matches in the library."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    anchor = src.index("update_tab_breakdown_rows = conn.execute")
    block = src[anchor:anchor + 6000]
    assert "lf2.section_id = pi.section_id" in block, (
        "has-something check (lf2 join) missing — would resurrect "
        "the pure-P-in-breakdown bug"
    )
    # v1.22.10: the urls_match short-circuit + URL-diff branch are now inside
    # the consolidated _pending_update_actionable_sql helper (was ~6 inline
    # lines). The breakdown SQL invokes it — so the gate can't drift from the
    # pill columns / filters / NEEDS WORK sort.
    assert "_pending_update_actionable_sql" in block, (
        "v1.22.10: breakdown SQL must invoke the actionable-gate helper "
        "(carries the urls_match + URL-diff branches)"
    )
    # v1.20.62: the decision check must be section-scoped (mirror of
    # the library filter + count), not the old title-wide EXISTS.
    # v1.21.81: ALSO edition-scoped — the decision read filters to the
    # row's edition so accepting one edition can't clear a sibling.
    assert "AND pu.section_id = pi.section_id AND pu.edition_key = pi.edition_key)" in block, (
        "v1.20.62/v1.21.81: breakdown decision check must be "
        "section + edition scoped"
    )


# ── Pills disappear: localStorage cache + prepopulate ───────────


def test_app_js_caches_topbar_counts_to_localstorage():
    """v1.13.88 stores the latest topbar counts in localStorage
    on every refreshTopbarStatus tick so a subsequent page nav
    can pre-populate badges before the first /api/stats response.
    Pre-fix: badges defaulted to `hidden` and stayed invisible
    until the response landed (seconds during heavy plex_enum)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "localStorage.setItem('motif:topbar_counts'" in js
    # The cached payload must include all three count keys so the
    # prepopulate call has data for each badge.
    cache_block_start = js.index("'motif:topbar_counts'")
    cache_block = js[cache_block_start - 500:cache_block_start + 200]
    assert "stats.updates" in cache_block
    assert "stats.failures" in cache_block
    assert "stats.drops" in cache_block


def test_app_js_prepopulates_badges_on_dom_ready():
    """The pre-populate function must exist and be called from
    the DOMContentLoaded handler BEFORE refreshTopbarStatus, so
    badges show their cached values during the gap before the
    first stats response."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "function prepopulateBadgesFromCache()" in js
    # Must be invoked from the init handler.
    init_anchor = js.index(
        "document.addEventListener('DOMContentLoaded', () => {"
    )
    init_block = js[init_anchor:init_anchor + 1000]
    assert "prepopulateBadgesFromCache();" in init_block, (
        "Pre-populate must be called from the init handler so it "
        "runs before the first refreshTopbarStatus()"
    )
    # And must be called BEFORE refreshTopbarStatus — otherwise
    # there's no window where the cache fills the gap.
    pre_idx = init_block.index("prepopulateBadgesFromCache();")
    # v1.15.108: refresh call gained a .catch(() => {}) suffix so
    # an unawaited rejection doesn't become an unhandled promise.
    # Anchor on the function-name + call form, not the trailing
    # punctuation.
    refresh_idx = init_block.index("refreshTopbarStatus()")
    assert pre_idx < refresh_idx, (
        "Order matters: prepopulate must run before refresh so "
        "cached counts paint first, then real values arrive"
    )


# ── Customize layout button moved to bottom ─────────────────────


def test_dashboard_customize_button_at_bottom_not_hero():
    """v1.13.88 moved the // customize layout link from the hero
    description line to the bottom of the dashboard. The hero
    paragraph must NOT contain dash-customize-btn anymore; the
    bottom paragraph must."""
    html = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
    # Find the hero description block (not the small sync line). v0.50.32 added the
    # shared .hero-sub class to every page's hero description.
    hero_anchor = html.index('<p class="muted hero-sub">')
    hero_end = html.index("</p>", hero_anchor)
    hero_block = html[hero_anchor:hero_end]
    assert 'id="dash-customize-btn"' not in hero_block, (
        "Customize button must not be in the hero description "
        "block — moved to bottom of dashboard in v1.13.88"
    )
    # Must be elsewhere in the file (the new bottom location).
    assert 'id="dash-customize-btn"' in html
    # And specifically AFTER the closing of #dash-sections.
    sections_close = html.index("</div>{# /#dash-sections #}")
    assert html.index('id="dash-customize-btn"') > sections_close, (
        "Customize button must be AFTER #dash-sections so it "
        "renders at the page bottom, not inside the sortable area"
    )
