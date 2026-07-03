"""v0.51.23 — amber 4K tier badge next to the title (row + INFO card).

the user, after // ALL became the default (v0.51.22): "Since we made an all
section if a movie exists in both 4k and standard its impossible to tell them
apart at a glance, can we make a amber 4k symbol similar to in the setting
section chip that sits next to the title of the 4k library version and in the
info card, we can also make this display in the 4k section."

  * /api/library now returns `section_is_4k` per row (ps.is_4k).
  * renderLibraryRow draws `<span class="tier-badge tier-badge-4k">4K</span>`
    after the title when section_is_4k — visible in the combined ALL view AND
    the dedicated 4K section.
  * the INFO card draws the same badge in its title (gated on the
    section_context.is_4k it already returned).
  * .tier-badge-4k reuses the settings .lib-flag-pill-4k amber palette but is
    non-interactive.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from starlette.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import init_db  # noqa: E402

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


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
        for sid, title, is4k in (("1", "Movies", 0), ("5", "4K Movies", 1)):
            c.execute(
                "INSERT INTO plex_sections (section_id, title, type, is_anime,"
                " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                "VALUES (?,?,'movie',0,?,?,1,?,?)",
                (sid, title, is4k, title.lower().replace(" ", "-"), NOW, NOW))
        for rk, sid in (("rk-std", "1"), ("rk-4k", "5")):
            c.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " title, year, edition_key, has_theme, first_seen_at,"
                " last_seen_at) VALUES (?,?,'movie','Dune','2021','',0,?,?)",
                (rk, sid, NOW, NOW))
        c.commit()
    return TestClient(create_app(s))


def _row(resp, rk):
    return next(it for it in resp.json()["items"] if it["rating_key"] == rk)


# ── /api/library returns section_is_4k per row ────────────────


def test_library_row_carries_section_is_4k(client):
    resp = client.get("/api/library?tab=movies&all_res=true", headers=AUTH)
    assert resp.status_code == 200
    assert _row(resp, "rk-4k")["section_is_4k"] == 1, (
        "v0.51.23: the 4K-section row must carry section_is_4k=1")
    assert _row(resp, "rk-std")["section_is_4k"] == 0, (
        "the standard-section row must carry section_is_4k=0")


def test_section_is_4k_present_in_4k_section_too(client):
    # the user: "we can also make this display in the 4k section" — the flag is
    # per-row from the section, so it's set regardless of the view.
    resp = client.get("/api/library?tab=movies&fourk=true", headers=AUTH)
    assert _row(resp, "rk-4k")["section_is_4k"] == 1


# ── the badge render sites ────────────────────────────────────


def test_row_render_draws_4k_badge_gated_on_section_is_4k():
    assert ("it.section_is_4k ? '<span class=\"tier-badge tier-badge-4k\""
            in APP_JS), (
        "v0.51.23: renderLibraryRow must draw the 4K badge only when "
        "section_is_4k is set")
    # placed after the truncatable title name (a flex-shrink:0 sibling).
    i = APP_JS.index('<span class="title-cell-name">')
    seg = APP_JS[i:i + 400]
    assert "tier-badge-4k" in seg, (
        "the 4K badge must sit next to the title name in the title-cell")


def test_full_info_card_draws_4k_badge_from_section_context():
    # the themed-row card fetches api_item → section_context.is_4k.
    i = APP_JS.index('${sc && sc.is_4k ?')
    h3 = APP_JS[i:i + 200]
    assert "tier-badge-4k" in h3, (
        "v0.51.23: the full INFO card title must draw the 4K badge from "
        "section_context.is_4k")


def test_bare_info_card_draws_4k_badge_from_cached_row():
    # the no-theme card renders client-side from the cached /api/library row,
    # which now carries section_is_4k — so a 4K row with no theme still shows
    # the badge in its INFO card.
    i = APP_JS.index('<h3 class="info-title">${title}${yr}')
    h3 = APP_JS[i:i + 200]
    assert "it.section_is_4k ?" in h3 and "tier-badge-4k" in h3, (
        "v0.51.23: the bare INFO card must draw the 4K badge from the cached "
        "row's section_is_4k")


def test_tier_badge_css_is_amber_and_noninteractive():
    i = APP_CSS.index(".tier-badge-4k {")
    block = APP_CSS[i:i + 200]
    assert "var(--amber-bright)" in block
    assert "var(--amber)" in block
    assert "amber-rgb" in block
    # base .tier-badge must not be a button (no cursor:pointer / hover).
    j = APP_CSS.index(".tier-badge {")
    base = APP_CSS[j:APP_CSS.index("}", j)]
    assert "cursor: pointer" not in base
