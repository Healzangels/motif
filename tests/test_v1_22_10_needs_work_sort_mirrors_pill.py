"""v1.22.10 — NEEDS WORK (attention) sort mirrors the pill's actionable-gate.

the user's v1.22.9 follow-up (DB `motif (3).db`): The Beginning After the End
(tmdb 274671) and Witch Hat Atelier (196950) sat at the TOP of NEEDS WORK while
showing a GREEN TDB pill. Their pending is a no-op — an `upstream_changed` whose
TDB theme lost its youtube_url (real_diff false, old_youtube_url NULL), so the
pill column correctly suppressed the blue ↑ (green). But the attention SORT's
priority-2 gate only checked decision=pending + a-pending-exists + presence —
NONE of the pill's actionability gate — so it ranked the green-pill row NEEDS
WORK anyway. Same sort-vs-pill drift class as v1.19.86 (priority-3 await).

v1.22.10 extracts the pill's gate into `_pending_update_actionable_sql` and
points the pill columns, tdb_pills=update/=tdb filters, attn_pills=update
filter, the count subqueries, AND the attention sort at it — one helper, so the
sort can't drift from the pill again.

This test proves the SORT change behaviorally:
  - a green-pill url-less-pending row no longer outranks a plain clean row;
  - a genuinely-actionable pending (real old→new URL diff) still ranks first.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── Source pin: the sort references the shared helper ────────────


def test_attention_sort_priority2_uses_actionable_helper():
    """The priority-2 (pending-update) branch of the `attention` sort must
    AND the shared actionable helper — without it the green-pill drift
    returns."""
    src = API_PY.read_text()
    sort_start = src.index('"attention": (')
    # v1.22.14: anchor on the sort's ELSE 7 close (not a fixed char window) so
    # it can't overshoot/undershoot as the priority-2 block grows.
    sort_block = src[sort_start:src.index("ELSE 7 END", sort_start)]
    assert "_pending_update_actionable_sql('t', 'pi')" in sort_block, (
        "v1.22.10: the NEEDS WORK sort priority-2 must gate on "
        "_pending_update_actionable_sql so it mirrors the pill"
    )


# ── Behavioral ──────────────────────────────────────────────────


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
NOW = "2026-06-06T00:00:00"


def _db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def _section(conn, sid="3"):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, 'TV', 'show', 0, 0, 'tv', 1, ?, ?)", (sid, NOW, NOW))


def _theme(conn, tid, tmdb, title, url):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'tv', ?, ?, 'imdb', ?, ?, ?)", (tid, tmdb, title, NOW, NOW, url))


def _item(conn, *, rk, tid, tmdb, title, sid="3"):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, local_theme_file,"
        " folder_path, plex_independent_theme, plex_theme_verified_ok,"
        " first_seen_at, last_seen_at)"
        " VALUES (?, ?, 'show', ?, ?, ?, ?, 2012, 1, 0, ?, 0, 1, ?, ?)",
        (rk, sid, tid, f"tt{tmdb}", tmdb, title, f"/data/tv/{title}", NOW, NOW))


def _override(conn, tmdb, url, sid="3"):
    conn.execute(
        "INSERT INTO user_overrides (media_type, tmdb_id, section_id,"
        " youtube_url, intent, set_at, set_by)"
        " VALUES ('tv', ?, ?, ?, 'replace', ?, 'admin')", (tmdb, sid, url, NOW))


def _pending(conn, tmdb, *, old, new, sid="3"):
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
        " edition_key, new_video_id, new_youtube_url, old_video_id,"
        " old_youtube_url, detected_at, decision, kind)"
        " VALUES ('tv', ?, ?, '', 'NEW', ?, ?, ?, ?, 'pending', 'upstream_changed')",
        (tmdb, sid, new, "OLD" if old else None, old, NOW))


def _local_and_placement(conn, tmdb, rk, *, source_kind="url", sid="3"):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
        " file_path, source_kind, source_video_id, downloaded_at)"
        " VALUES ('tv', ?, ?, '', ?, ?, 'NEW', ?)",
        (tmdb, sid, f"{tmdb}.mp3", source_kind, NOW))
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, edition_key,"
        " media_folder, placed_at, placement_kind, plex_rating_key,"
        " plex_refreshed, provenance)"
        " VALUES ('tv', ?, ?, '', '', ?, 'plex_upload', ?, 1, 'manual')",
        (tmdb, sid, NOW, rk))


def _attention_order(client):
    r = client.get("/api/library?tab=tv&sort=attention&sort_dir=asc", headers=AUTH)
    assert r.status_code == 200, r.text
    return [it.get("rating_key") for it in r.json().get("items", [])], \
           {it.get("rating_key"): it for it in r.json().get("items", [])}


def test_green_pill_urlless_row_drops_out_of_needs_work(admin_client, tmp_path):
    """Three rows, one query, sort=attention:
      C (real old→new diff)  → actionable → priority 2 → ranks FIRST
      B (clean TDB, no pend) → priority 7
      A (url-less pending)   → GREEN pill → priority 7 (was 2 pre-fix)
    Titles chosen so the priority-7 tie breaks B before A. The discriminator:
    A must NOT precede B (proves A left priority-2)."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        # A — the Beginning/Witch Hat repro: TDB url gone, stale upstream_changed
        # with old_youtube_url NULL → real_diff false → GREEN pill.
        _theme(conn, 1, 274671, "ZZZ Url-less Pending", None)
        _item(conn, rk="rk-A", tid=1, tmdb=274671, title="ZZZ Url-less Pending")
        _override(conn, 274671, "https://y/watch?v=U")
        _pending(conn, 274671, old=None, new="https://y/watch?v=NEW")
        _local_and_placement(conn, 274671, "rk-A")
        # B — plain clean themed row, no pending. priority 7.
        _theme(conn, 2, 222000, "AAA Clean", "https://y/watch?v=CLEAN")
        _item(conn, rk="rk-B", tid=2, tmdb=222000, title="AAA Clean")
        # C — genuinely actionable: old≠new real diff. priority 2.
        _theme(conn, 3, 333000, "MMM Real Update", "https://y/watch?v=OLD")
        _item(conn, rk="rk-C", tid=3, tmdb=333000, title="MMM Real Update")
        _pending(conn, 333000, old="https://y/watch?v=OLD", new="https://y/watch?v=NEW")
        _local_and_placement(conn, 333000, "rk-C", source_kind="themerrdb")
        conn.commit()

    order, by_rk = _attention_order(admin_client)
    for rk in ("rk-A", "rk-B", "rk-C"):
        assert rk in order, f"{rk} missing from library: {order}"

    # Pill correctness: A green (0), C blue (1).
    assert by_rk["rk-A"].get("pending_update") in (0, None, False), (
        "v1.22.10: url-less upstream_changed row must show GREEN (pill 0)"
    )
    assert by_rk["rk-C"].get("pending_update") == 1, (
        "v1.22.10: real old→new diff row must show BLUE ↑ (pill 1)"
    )

    iA, iB, iC = order.index("rk-A"), order.index("rk-B"), order.index("rk-C")
    # C is genuinely actionable → ranks first.
    assert iC < iA and iC < iB, (
        f"v1.22.10: real-diff row (C) must rank above both 7-tier rows; "
        f"order={order}"
    )
    # THE FIX: A (green pill) must NOT outrank B (plain clean). Pre-fix A was
    # priority-2 and led; post-fix both are priority-7 and the B<A title tie
    # puts B first.
    assert iB < iA, (
        f"v1.22.10: green-pill url-less row (A) must NOT rank into NEEDS WORK "
        f"ahead of a plain clean row (B) — it left priority-2; order={order}"
    )


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
