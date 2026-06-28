"""v1.22.9 — urls_match blue TDB↑ pill honors a GLOBAL ('' section) override.

the user's anime repro (live DB `motif (2).db`): Berserk: The Golden Age Arc
(tv, tmdb 211057) and Fate/Strange Fake (229858) sat as GREEN TDB pills while
SRC read U and NEEDS WORK ranked them — a sort/pill drift. Their shape:

  - plex_items:    section '3', media_type 'show', has_theme 1, verified
  - user_overrides: section ''  (GLOBAL), intent 'replace', URL == TDB's
  - pending_updates: section '' (GLOBAL), kind 'urls_match', decision pending
  - local_files / placements: section '3' (url / plex_upload)

`_has_user_override_sql` gated the urls_match no-op branch on a STRICT section
match (uo.section_id = pi.section_id). A global override at '' missed it → the
urls_match branch failed → pending_update=0 → GREEN. But SRC=U already honors
the global override via its own COALESCE(per-section, '') fallback, so the row
showed U + NEEDS WORK while the pill said "clean TDB". the user: "if the user
uploaded theme is the same as a possible themerrdb theme you could be using
then it should be a blue pill tdb".

The fix adds the `OR uo_chk.section_id = ''` fallback to the shared helper, so
all 11 urls_match sites (the `pending_update` row column + the tdb_pills=update
/ =tdb filters) surface the blue ↑ "you could swap U→T" accurately — mirroring
SRC=U's section-or-global scope.

The real-DB gate flip this reproduces (verified against `motif (2).db`):
  strict  EXISTS(uo @ section '3')          → 0  (pre-fix: GREEN)
  fallback EXISTS(uo @ section '3' OR '')    → 1  (post-fix: BLUE ↑)
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"

NEW = "https://www.youtube.com/watch?v=NEW"


# ── Source pin: the '' fallback the whole fix hinges on ──────────


def test_helper_renders_global_section_fallback():
    """The shared override helper must include the global-section ('')
    fallback in its rendered SQL — that single OR is the entire v1.22.9
    behavior change. If a future patch 'optimizes' it back to the strict
    `uo.section_id = pi.section_id` form, the Berserk/Fate green-pill
    drift returns."""
    from app.web.api import _has_user_override_sql
    sql = _has_user_override_sql("t", "pi")
    assert "uo_chk.section_id = pi.section_id" in sql, (
        "v1.22.9: per-section match must remain (the common case)"
    )
    assert "OR uo_chk.section_id = ''" in sql, (
        "v1.22.9: global ('' section) override fallback must be present — "
        "mirrors SRC=U's COALESCE(per-section, '') scope"
    )


def test_helper_marker_explains_why():
    """The helper's docstring must carry the v1.22.9 marker + the SRC=U
    mirror rationale so a future reader doesn't strip the fallback."""
    src = API_PY.read_text()
    idx = src.index("def _has_user_override_sql(")
    block = src[idx:idx + 1200]
    assert "v1.22.9" in block
    assert "SRC=U" in block or "global" in block.lower()


# ── Mirror-drift guard: one helper, always paired ───────────────


def test_override_gate_is_single_chokepoint():
    """v1.22.10 consolidated the (then 11) inline urls_match actionable-gates
    into ONE helper, `_pending_update_actionable_sql`. The override check + its
    `_not_p_row_sql` pairing now live there exactly once — the strongest form of
    the v1.22.9 mirror-drift lock: there's a single place the '' fallback can be
    (mis)edited, and every pill/filter/sort surface routes through it.

    Pin: `_has_user_override_sql` and `_not_p_row_sql` are each called exactly
    once with bare identifier args (inside the helper), the helper pairs them on
    the urls_match branch, and it's invoked at ≥11 surfaces."""
    src = API_PY.read_text()
    # No literal-alias inline call sites remain (all routed via the helper).
    inline = re.findall(
        r"_has_user_override_sql\((['\"]\w+['\"]),\s*(['\"]\w+['\"])\)", src)
    assert inline == [], (
        f"v1.22.10: override gate must be inline NOWHERE — route via "
        f"_pending_update_actionable_sql; found {len(inline)} inline sites"
    )
    # The single chokepoint pairs the override check with the P-row exclusion
    # on the urls_match branch.
    idx = src.index("def _pending_update_actionable_sql(")
    # v1.22.14: anchor on the NEXT def so the window can't overshoot.
    body = src[idx:src.index("def _pending_update_detected_sql(", idx)]
    assert "= 'urls_match'" in body
    assert "_not_p_row_sql(t, pi)" in body
    assert "_has_user_override_sql(t, pi)" in body
    # And it's the gate for every pill/filter/sort surface (≥11 call sites).
    assert src.count("_pending_update_actionable_sql('") >= 11, (
        "v1.22.10: actionable helper must gate every pending-update surface"
    )


# ── Behavioral: the Berserk/Fate repro end-to-end ───────────────


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


AUTH = {"X-Authentik-Username": "testadmin"}


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def _seed_section(conn, section_id="3"):
    conn.execute(
        "INSERT INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, themes_subdir, "
        "   included, discovered_at, last_seen_at) "
        "VALUES (?, 'TV', 'show', 0, 0, 'tv', 1, "
        "        '2026-06-06T00:00:00', '2026-06-06T00:00:00')",
        (section_id,),
    )


def _seed_theme(conn, theme_id, tmdb_id, youtube_url=NEW):
    now = "2026-06-06T00:00:00"
    conn.execute(
        "INSERT INTO themes "
        "  (id, media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
        "VALUES (?, 'tv', ?, 'Berserk: The Golden Age Arc', 'imdb', ?, ?, ?)",
        (theme_id, tmdb_id, now, now, youtube_url),
    )


def _seed_plex_item(conn, *, rk, theme_id, tmdb_id, section_id="3"):
    """Mirror Berserk: media_type 'show', has_theme 1, verified, the
    motif placement landing via plex_upload (so SRC=U, not pure-P)."""
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, theme_id, "
        "   guid_imdb, guid_tmdb, title, year, has_theme, "
        "   local_theme_file, folder_path, plex_independent_theme, "
        "   plex_theme_verified_ok, first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'show', ?, 'tt1', ?, "
        "        'Berserk: The Golden Age Arc', 2012, 1, 0, "
        "        '/data/tv/Berserk', 0, 1, "
        "        '2026-06-06T00:00:00', '2026-06-06T00:00:00')",
        (rk, section_id, theme_id, tmdb_id),
    )


def _seed_override(conn, tmdb_id, section_id, url=NEW):
    conn.execute(
        "INSERT INTO user_overrides "
        "  (media_type, tmdb_id, section_id, youtube_url, intent, "
        "   set_at, set_by) "
        "VALUES ('tv', ?, ?, ?, 'replace', '2026-06-06T00:00:00', 'admin')",
        (tmdb_id, section_id, url),
    )


def _seed_pending(conn, tmdb_id, section_id, kind="urls_match", new_url=NEW):
    conn.execute(
        "INSERT INTO pending_updates "
        "  (media_type, tmdb_id, section_id, edition_key, "
        "   new_video_id, new_youtube_url, old_video_id, old_youtube_url, "
        "   detected_at, decision, kind) "
        "VALUES ('tv', ?, ?, '', 'NEW', ?, 'NEW', ?, "
        "        '2026-06-06T00:00:00', 'pending', ?)",
        (tmdb_id, section_id, new_url, new_url, kind),
    )


def _seed_local_and_placement(conn, tmdb_id, rk, section_id="3"):
    conn.execute(
        "INSERT INTO local_files "
        "  (media_type, tmdb_id, section_id, edition_key, file_path, "
        "   source_kind, source_video_id, downloaded_at) "
        "VALUES ('tv', ?, ?, '', 'berserk.mp3', 'url', 'NEW', "
        "        '2026-06-06T00:00:00')",
        (tmdb_id, section_id),
    )
    conn.execute(
        "INSERT INTO placements "
        "  (media_type, tmdb_id, section_id, edition_key, media_folder, "
        "   placed_at, placement_kind, plex_rating_key, plex_refreshed, "
        "   provenance) "
        "VALUES ('tv', ?, ?, '', '', '2026-06-06T00:00:00', "
        "        'plex_upload', ?, 1, 'manual')",
        (tmdb_id, section_id, rk),
    )


def _row(client, rk):
    r = client.get("/api/library?tab=tv", headers=AUTH)
    assert r.status_code == 200, r.text
    matching = [it for it in r.json().get("items", [])
                if it.get("rating_key") == rk]
    assert matching, (
        f"seeded row {rk} missing from /api/library: "
        f"{[it.get('rating_key') for it in r.json().get('items', [])]}"
    )
    return matching[0]


def test_global_override_urls_match_flags_pending_update(admin_client, tmp_path):
    """The Berserk repro: a global ('' section) override + a urls_match
    pending at '' must now flag pending_update=1 (blue ↑), even though the
    plex_items/local/placement rows live at section '3'."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "3")
        _seed_theme(conn, theme_id=1, tmdb_id=211057)
        _seed_plex_item(conn, rk="rk-berserk", theme_id=1, tmdb_id=211057)
        _seed_override(conn, 211057, section_id="")        # GLOBAL
        _seed_pending(conn, 211057, section_id="")          # GLOBAL urls_match
        _seed_local_and_placement(conn, 211057, "rk-berserk")
        conn.commit()

    row = _row(admin_client, "rk-berserk")
    assert row.get("pending_update") == 1, (
        f"v1.22.9: global-'' override urls_match row must flag the blue ↑ "
        f"pill (pending_update=1); got {row.get('pending_update')}"
    )
    assert row.get("pending_update_kind") == "urls_match"


def test_global_override_row_in_update_filter_not_green(admin_client, tmp_path):
    """The same row must land in the blue tdb_pills=update set and be
    EXCLUDED from the green tdb_pills=tdb set — the two filters mirror the
    row pill, so a green-filter leak would be the same drift."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "3")
        _seed_theme(conn, theme_id=1, tmdb_id=211057)
        _seed_plex_item(conn, rk="rk-berserk", theme_id=1, tmdb_id=211057)
        _seed_override(conn, 211057, section_id="")
        _seed_pending(conn, 211057, section_id="")
        _seed_local_and_placement(conn, 211057, "rk-berserk")
        conn.commit()

    upd = admin_client.get("/api/library?tab=tv&tdb_pills=update", headers=AUTH)
    upd_rks = [it.get("rating_key") for it in upd.json().get("items", [])]
    assert "rk-berserk" in upd_rks, (
        "v1.22.9: global-override urls_match row must appear in the blue "
        "tdb_pills=update filter (matches its row pill)"
    )

    green = admin_client.get("/api/library?tab=tv&tdb_pills=tdb", headers=AUTH)
    green_rks = [it.get("rating_key") for it in green.json().get("items", [])]
    assert "rk-berserk" not in green_rks, (
        "v1.22.9: the row must NOT show in the green tdb_pills=tdb filter — "
        "it has an actionable pending update, not a clean TDB theme"
    )


def test_per_section_override_still_flags(admin_client, tmp_path):
    """Additive-not-replacing regression lock: a STRICT per-section
    override (section '3', matching plex_items) must still flag — the
    fix widens the match, it doesn't move it."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "3")
        _seed_theme(conn, theme_id=2, tmdb_id=222222)
        _seed_plex_item(conn, rk="rk-sec", theme_id=2, tmdb_id=222222)
        _seed_override(conn, 222222, section_id="3")        # per-section
        _seed_pending(conn, 222222, section_id="3")          # per-section
        _seed_local_and_placement(conn, 222222, "rk-sec")
        conn.commit()

    row = _row(admin_client, "rk-sec")
    assert row.get("pending_update") == 1, (
        f"v1.22.9: per-section override urls_match must still flag; "
        f"got {row.get('pending_update')}"
    )


def test_no_override_anywhere_no_pending(admin_client, tmp_path):
    """Control: a urls_match pending with NO user override at any section
    must NOT flag — the v1.19.12 orphan gate still holds, the '' fallback
    only widens WHICH override sections count, it doesn't drop the gate."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "3")
        _seed_theme(conn, theme_id=3, tmdb_id=333333)
        _seed_plex_item(conn, rk="rk-orphan", theme_id=3, tmdb_id=333333)
        # NO _seed_override — the urls_match pending is an orphan.
        _seed_pending(conn, 333333, section_id="")
        _seed_local_and_placement(conn, 333333, "rk-orphan")
        conn.commit()

    row = _row(admin_client, "rk-orphan")
    assert row.get("pending_update") in (0, None, False), (
        f"v1.22.9: orphan urls_match (no override anywhere) must NOT flag; "
        f"got {row.get('pending_update')}"
    )


def test_v1_22_9_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
