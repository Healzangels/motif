"""v1.22.14 — NEEDS WORK sort presence gate carries the new_theme escape.

Code-review finding on the v1.22.10 work: v1.22.10 unified the ACTIONABLE
sub-gate onto the attention sort's priority-2 branch, but the SIBLING PRESENCE
gate there was left missing the `OR _pending_update_new_theme_kind_sql` escape
that the pill `pending_update`/`actionable_update` columns, PENDING_EXISTS, and
the attn_pills=update filter all carry. So a SRC=— row that sync just discovered
a brand-new TDB theme for (no local_files / override / placement / sidecar) —
the row v1.19.71 created the feature for, and one v1.22.12 KEEPS surfacing
(SRC=— passes _not_p_row) — rendered the blue ↑ pill + UPD count + matched both
update filters, but FAILED the sort's presence gate → fell to ELSE 7 and sank to
the bottom of NEEDS WORK instead of priority-2. The exact sort-vs-pill drift
class v1.22.10 set out to eliminate, surviving in the presence half.

v1.22.14 adds the escape to the sort's presence gate.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-06-06T00:00:00"


# ── Source pin ───────────────────────────────────────────────


def test_attention_sort_presence_gate_has_new_theme_escape():
    """The attention sort's priority-2 presence gate must include the
    new_theme escape, mirroring the four sibling surfaces."""
    start = API_PY.index('"attention": (')
    # v1.23.25: window widened (2600→3200) — the broken-placement branch added
    # a higher-priority WHEN at the top of the CASE, shifting the priority-2
    # presence gate down. v1.23.30: 3200→3700 — the broken branch gained the
    # plex_independent_theme=0 gate (#3 review fix), shifting it further. Still
    # asserts the escape lives in the attention CASE.
    # v1.24.41: 3700→4400 — the bucket-0 broken branch gained the rk-liveness
    # gate (a re-linked live-rk plex_upload must not top NEEDS WORK), shifting
    # the priority-2 presence gate further down.
    block = API_PY[start:start + 4400]
    # The presence gate ends at ` ) ` right before the actionable AND. The
    # new_theme escape must be present in the priority-2 region.
    assert "_pending_update_new_theme_kind_sql('t', 'pi')" in block, (
        "v1.22.14: attention sort priority-2 presence gate must carry the "
        "new_theme_available escape (it sank SRC=— new-theme rows to ELSE 7)"
    )


# ── Behavioral ───────────────────────────────────────────────


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


def _db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def _section(conn, sid="1"):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)", (sid, NOW, NOW))


def _theme(conn, tid, tmdb, title):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, 'https://y/watch?v=NEW')",
        (tid, tmdb, title, NOW, NOW))


def _item(conn, *, rk, tid, tmdb, title, has_theme, sid="1"):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, local_theme_file,"
        " folder_path, plex_independent_theme, plex_theme_verified_ok,"
        " first_seen_at, last_seen_at)"
        " VALUES (?, ?, 'movie', ?, ?, ?, ?, 2012, ?, 0, ?, 0, 1, ?, ?)",
        (rk, sid, tid, f"tt{tmdb}", tmdb, title, has_theme,
         f"/data/m/{title}", NOW, NOW))


def _new_theme_pending(conn, tmdb, sid=""):
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
        " edition_key, new_video_id, new_youtube_url, detected_at, decision, kind)"
        " VALUES ('movie', ?, ?, '', 'NEW', 'https://y/watch?v=NEW', ?,"
        " 'pending', 'new_theme_available')", (tmdb, sid, NOW))


def test_unthemed_new_theme_row_ranks_priority2_in_needs_work(admin_client, tmp_path):
    """Two rows, sort=attention asc:
      A — SRC=— (has_theme=0, no content) + new_theme_available → priority 2
      B — plain clean themed row, no pending → priority 7
    Titles chosen so the ONLY way A precedes B is by reaching priority 2 (B's
    title sorts first, so a priority-7 tie would put B ahead). Pre-fix A failed
    the sort's presence gate → ELSE 7 → B ahead; post-fix A is priority-2 → A
    ahead. Discriminator: index(A) < index(B)."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        # A — SRC=— new_theme. Title sorts LAST alphabetically.
        _theme(conn, 1, 100, "ZZZ Unthemed NewTheme")
        _item(conn, rk="rk-A", tid=1, tmdb=100, title="ZZZ Unthemed NewTheme",
              has_theme=0)
        _new_theme_pending(conn, 100)
        # B — plain clean themed row, no pending. Title sorts FIRST.
        _theme(conn, 2, 200, "AAA Clean")
        _item(conn, rk="rk-B", tid=2, tmdb=200, title="AAA Clean", has_theme=1)
        conn.commit()

    r = admin_client.get("/api/library?tab=movies&sort=attention&sort_dir=asc",
                         headers=AUTH)
    assert r.status_code == 200, r.text
    order = [it.get("rating_key") for it in r.json().get("items", [])]
    assert "rk-A" in order and "rk-B" in order, order
    # A also shows the blue pill (sanity — it was already firing there).
    by_rk = {it.get("rating_key"): it for it in r.json().get("items", [])}
    assert by_rk["rk-A"].get("pending_update") == 1
    # THE FIX: A reaches priority-2 and outranks the priority-7 clean row,
    # despite its title sorting after B.
    assert order.index("rk-A") < order.index("rk-B"), (
        f"v1.22.14: SRC=— new-theme row must rank priority-2 in NEEDS WORK "
        f"(ahead of a plain clean priority-7 row); order={order}"
    )


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
