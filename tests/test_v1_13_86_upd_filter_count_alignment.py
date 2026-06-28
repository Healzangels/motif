"""v1.13.86 — favicon local serve + UPD library filter aligned with count.

Two bugs the user surfaced from his v1.13.82 deployment:

1. Favicon 404. base.html hardcoded
   `https://raw.githubusercontent.com/healzangels/motif/main/unraid/icon.png`
   but the `main` branch was deleted in v1.13.74-prep when the
   branch model was reset to release/nightly. Browser shows the
   default favicon. Right fix: serve locally from /static/icon.png.

2. UPD count vs library filter drift (the inverse of the v1.13.84
   breakdown drift). The badge count uses a STRICT predicate
   (api.py:2869-2929):
     - effective pu.decision='pending' (per-section + '' fallback)
     - has-something check (lf OR uo OR p OR sidecar)
     - URL-diff check (kind='urls_match' OR new_url != applied)

   The pre-v1.13.86 library filter for `attn_pills=update` used a
   PERMISSIVE predicate (api.py:1154-1167):
     - EXISTS pending_updates with decision='pending'
     - SRC != '-'

   v1.13.68 explicitly chose the permissive form ("sort/filter
   tolerates a few extra 'pending but already same-URL' rows
   much better than the strict gate does"). But on the user's
   library this produced "1 UPD badge → 28 matches" — the 27
   extras were Plex-only rows where ACCEPT is a no-op (motif
   owns nothing to update) plus stale urls_match entries.

   v1.13.86 tightens the filter to mirror the count exactly so
   click-1-UPD lands on exactly 1 row.

Tests pin both fixes by direct SQL exercise + static guards.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── Fix 1: favicon served locally ────────────────────────────

def test_favicon_template_uses_local_static_path():
    """base.html must point at /static/icon.png (local), NOT the
    githubusercontent.com cross-origin URL that 404s after the
    main-branch deletion in v1.13.74-prep."""
    html = (REPO / "app" / "web" / "templates" / "base.html").read_text()
    # New: local path with cache-bust appears as an href= attr.
    assert 'href="/static/icon.png?v=' in html
    # Old: the actual <link rel="icon" href=...github...> attribute
    # form must NOT appear. (Allow the URL substring elsewhere —
    # e.g. comments documenting the rename.)
    assert 'href="https://raw.githubusercontent.com' not in html


def test_favicon_file_exists_in_static_dir():
    """The icon must actually be on disk where the template
    references it. Pre-fix the template was the only reference;
    no local copy existed (the unraid/ template path was github-
    only)."""
    icon = REPO / "app" / "web" / "static" / "icon.png"
    assert icon.exists()
    assert icon.stat().st_size > 0
    # PNG magic bytes — sanity check it's actually a PNG.
    with icon.open("rb") as f:
        header = f.read(8)
    assert header[:8] == b"\x89PNG\r\n\x1a\n"


# ── Fix 2: UPD library filter = badge count ──────────────────


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _seed_section(conn, *, section_id: str, type_: str = "movie",
                  is_4k: int = 0):
    now = _now_iso()
    conn.execute(
        "INSERT INTO plex_sections ("
        "  section_id, title, type, included, discovered_at,"
        "  last_seen_at, is_4k, is_anime"
        ") VALUES (?, ?, ?, 1, ?, ?, ?, 0)",
        (section_id, f"sec-{section_id}", type_, now, now, is_4k),
    )


def _seed_themed(conn, *, tmdb_id: int, applied_url: str | None = None):
    """Theme + plex_items pair. applied_url defaults to the row's
    own youtube_url (the TDB URL); pass a different value to
    simulate a user_overrides entry that diverges."""
    now = _now_iso()
    yt = "https://www.youtube.com/watch?v=tdb"
    conn.execute(
        "INSERT INTO themes ("
        "  media_type, tmdb_id, title, upstream_source,"
        "  youtube_url, youtube_video_id,"
        "  last_seen_sync_at, first_seen_sync_at"
        ") VALUES ('movie', ?, ?, 'imdb', ?, 'tdb', ?, ?)",
        (tmdb_id, f"x{tmdb_id}", yt, now, now),
    )
    conn.execute(
        "INSERT INTO plex_items ("
        "  rating_key, section_id, media_type, title, guid_tmdb,"
        "  first_seen_at, last_seen_at"
        ") VALUES (?, '1', 'movie', ?, ?, ?, ?)",
        (f"rk{tmdb_id}", f"x{tmdb_id}", tmdb_id, now, now),
    )
    if applied_url:
        conn.execute(
            "INSERT INTO user_overrides ("
            "  media_type, tmdb_id, section_id, youtube_url,"
            "  set_at, set_by"
            ") VALUES ('movie', ?, '1', ?, ?, 'admin')",
            (tmdb_id, applied_url, now),
        )


def _seed_pending(conn, *, tmdb_id: int, new_url: str,
                  kind: str = "upstream_changed"):
    conn.execute(
        "INSERT INTO pending_updates ("
        "  media_type, tmdb_id, decision, detected_at,"
        "  new_youtube_url, kind"
        ") VALUES ('movie', ?, 'pending', ?, ?, ?)",
        (tmdb_id, _now_iso(), new_url, kind),
    )


def _seed_local_file_and_placement(conn, *, tmdb_id: int):
    """Gives the row src='T' (themerrdb-source placed). Required
    for the strict UPD filter post-v1.13.86 — without theme
    presence, ACCEPT is a no-op."""
    now = _now_iso()
    conn.execute(
        "INSERT INTO local_files ("
        "  media_type, tmdb_id, section_id, file_path,"
        "  source_video_id, downloaded_at, source_kind"
        ") VALUES ('movie', ?, '1', 'x.mp3', 'tdb', ?, 'themerrdb')",
        (tmdb_id, now),
    )
    conn.execute(
        "INSERT INTO placements ("
        "  media_type, tmdb_id, section_id, media_folder,"
        "  placed_at, placement_kind, plex_refreshed"
        ") VALUES ('movie', ?, '1', '/data/x', ?, 'hardlink', 0)",
        (tmdb_id, now),
    )


def _seed_pure_p_row(conn, *, tmdb_id: int):
    """Pure-P state: pi.has_theme=1 + pi.plex_theme_verified_ok=1
    BUT no local_files / user_overrides / placements / sidecar.
    SRC computes to 'P'. the user's 27 noise rows look like this."""
    conn.execute(
        "UPDATE plex_items SET has_theme = 1,"
        "                       plex_theme_verified_ok = 1,"
        "                       local_theme_file = 0 "
        "WHERE rating_key = ?",
        (f"rk{tmdb_id}",),
    )


# Build the v1.13.86 filter SQL the same way the production code
# does. This pins the SQL by reconstructing it — any change to
# the production snippet must be mirrored here for the test to
# stay green.
def _filter_count(db: Path) -> int:
    from app.web.api import _SRC_LETTER_SQL
    sql = f"""
        SELECT COUNT(*)
        FROM plex_items pi
        INNER JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
        INNER JOIN themes t
          ON t.tmdb_id = pi.guid_tmdb
         AND t.media_type = (CASE pi.media_type
                              WHEN 'show' THEN 'tv'
                              ELSE pi.media_type END)
        LEFT JOIN placements p
          ON p.media_type = t.media_type
         AND p.tmdb_id = t.tmdb_id
         AND p.section_id = pi.section_id
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type
         AND lf.tmdb_id = t.tmdb_id
         AND lf.section_id = pi.section_id
        WHERE EXISTS (
            SELECT 1 FROM pending_updates pu
            WHERE pu.media_type = t.media_type
              AND pu.tmdb_id = t.tmdb_id
              AND pu.decision = 'pending'
          )
          AND ({_SRC_LETTER_SQL}) != '-'
          AND (
            EXISTS (SELECT 1 FROM local_files lf2
                     WHERE lf2.media_type = t.media_type
                       AND lf2.tmdb_id = t.tmdb_id
                       AND lf2.section_id = pi.section_id)
            OR EXISTS (SELECT 1 FROM user_overrides uo2
                        WHERE uo2.media_type = t.media_type
                          AND uo2.tmdb_id = t.tmdb_id
                          AND uo2.section_id = pi.section_id)
            OR p.media_folder IS NOT NULL
            OR pi.local_theme_file = 1
          )
          AND (
            COALESCE(
              (SELECT pu.kind FROM pending_updates pu
                WHERE pu.media_type = t.media_type
                  AND pu.tmdb_id = t.tmdb_id
                  AND pu.section_id = pi.section_id),
              (SELECT pu.kind FROM pending_updates pu
                WHERE pu.media_type = t.media_type
                  AND pu.tmdb_id = t.tmdb_id
                  AND pu.section_id = '')
            ) = 'urls_match'
            OR COALESCE(
              (SELECT pu.new_youtube_url FROM pending_updates pu
                WHERE pu.media_type = t.media_type
                  AND pu.tmdb_id = t.tmdb_id
                  AND pu.section_id = pi.section_id),
              (SELECT pu.new_youtube_url FROM pending_updates pu
                WHERE pu.media_type = t.media_type
                  AND pu.tmdb_id = t.tmdb_id
                  AND pu.section_id = '')
            ) != COALESCE(
              (SELECT youtube_url FROM user_overrides uo
                WHERE uo.media_type = t.media_type
                  AND uo.tmdb_id = t.tmdb_id
                  AND uo.section_id = pi.section_id),
              (SELECT youtube_url FROM user_overrides uo
                WHERE uo.media_type = t.media_type
                  AND uo.tmdb_id = t.tmdb_id
                  AND uo.section_id = ''),
              t.youtube_url
            )
          )
    """
    with sqlite3.connect(db) as conn:
        return conn.execute(sql).fetchone()[0]


def test_filter_excludes_pure_p_row(db):
    """the user's 27-row noise: pure-P rows where Plex serves the
    theme but motif tracks nothing. ACCEPT would be a no-op.
    v1.13.86 hides them from the library filter."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_themed(conn, tmdb_id=1)
        _seed_pure_p_row(conn, tmdb_id=1)
        _seed_pending(conn, tmdb_id=1,
                      new_url="https://www.youtube.com/watch?v=new")
    # Pure-P fails the has-something check → excluded.
    assert _filter_count(db) == 0


def test_filter_includes_T_row_with_real_diff(db):
    """The actionable case: src='T' (motif placed from TDB) +
    pending update with new URL != applied URL. v1.13.86 keeps
    these visible."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_themed(conn, tmdb_id=1)
        _seed_local_file_and_placement(conn, tmdb_id=1)  # → src='T'
        _seed_pending(conn, tmdb_id=1,
                      new_url="https://www.youtube.com/watch?v=new")
    assert _filter_count(db) == 1


def test_filter_excludes_no_op_url_match(db):
    """A pending_update where the new URL ALREADY matches the
    currently-applied URL is a no-op. v1.13.86 hides these."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        # User override already points at the same URL as the
        # pending_update's new_youtube_url.
        _seed_themed(conn, tmdb_id=1,
                     applied_url="https://www.youtube.com/watch?v=same")
        _seed_local_file_and_placement(conn, tmdb_id=1)
        _seed_pending(conn, tmdb_id=1,
                      new_url="https://www.youtube.com/watch?v=same")
    # new_url == applied → filter excludes → 0 matches.
    assert _filter_count(db) == 0


def test_filter_includes_urls_match_kind(db):
    """The 'urls_match' kind is its own actionable case (the
    convert-U-to-T affordance). Even with no URL diff, this kind
    surfaces. Pin the COALESCE branch that handles it."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_themed(conn, tmdb_id=1)
        _seed_local_file_and_placement(conn, tmdb_id=1)
        _seed_pending(conn, tmdb_id=1,
                      new_url="https://www.youtube.com/watch?v=tdb",
                      kind="urls_match")
    # kind='urls_match' → filter includes → 1 match.
    assert _filter_count(db) == 1


def test_filter_excludes_decided_pending(db):
    """Sanity: decision='accepted' rows must NOT match the filter
    (carried over from v1.13.84)."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_themed(conn, tmdb_id=1)
        _seed_local_file_and_placement(conn, tmdb_id=1)
        _seed_pending(conn, tmdb_id=1,
                      new_url="https://www.youtube.com/watch?v=new")
        conn.execute(
            "UPDATE pending_updates SET decision='accepted' "
            "WHERE tmdb_id=1",
        )
    assert _filter_count(db) == 0


def test_filter_partitions_mixed_population_correctly(db):
    """the user's repro shape scaled down: 1 actionable T row + 3
    pure-P noise rows. v1.13.86 returns 1 (the actionable),
    not 4 (the v1.13.68 permissive count)."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        # Actionable T row.
        _seed_themed(conn, tmdb_id=1)
        _seed_local_file_and_placement(conn, tmdb_id=1)
        _seed_pending(conn, tmdb_id=1,
                      new_url="https://www.youtube.com/watch?v=new")
        # 3 pure-P noise rows.
        for tid in (2, 3, 4):
            _seed_themed(conn, tmdb_id=tid)
            _seed_pure_p_row(conn, tmdb_id=tid)
            _seed_pending(conn, tmdb_id=tid,
                          new_url="https://www.youtube.com/watch?v=new")
    assert _filter_count(db) == 1


# ── static guard: production filter has the v1.13.86 predicates ─

def test_production_attn_update_filter_has_strict_predicates():
    """Pin the v1.13.86 predicate additions to the
    attn_pills=update SQL branch. The filter must have BOTH
    the has-something check AND the URL-diff check alongside
    the EXISTS pending + SRC != '-'.

    v1.15.39 added a Python `_row_matches_attn` helper with its
    own `elif p == "update":` branch (handles the broken-mixed
    post-stat case). Anchor on the SQL-branch-specific context
    (`attn_branches.append(`) so this test still finds the SQL
    branch, not the Python matcher."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    # Find the SQL-side attn `update` branch via its
    # `attn_branches.append(` follow-up — that's unique to the
    # SQL branch (the Python matcher uses `return True`).
    branches = [
        i for i in range(len(api_py))
        if api_py.startswith('elif p == "update":', i)
        # Distinguisher: the SQL attn branch's body starts with
        # `attn_branches.append(` within the immediate next ~50
        # chars (after the comment block walked back to indent).
        # The tdb_pills branch uses `branches.append`. The
        # v1.15.39 Python matcher returns True. Look ahead 4000
        # chars to allow for the multi-line SQL inside the
        # append call.
        and 'attn_branches.append(' in api_py[i:i + 4000]
    ]
    assert branches, "attn_pills=update SQL branch missing"
    anchor = branches[-1]
    # 8000 chars covers the multi-line SQL string we added.
    block = api_py[anchor:anchor + 8000]
    # New v1.13.86 predicates must all be present:
    assert "lf2.section_id = pi.section_id" in block, (
        "has-something check (lf2 join) must be present"
    )
    assert "pi.local_theme_file = 1" in block, (
        "has-something check (sidecar) must be present"
    )
    # v1.22.10: the urls_match short-circuit + URL-diff check were CONSOLIDATED
    # into the single _pending_update_actionable_sql helper (v1.19.60 extracted
    # the components; v1.22.10 composed them into one gate). The attn update
    # branch invokes it — so its actionability gate can't drift from the pill
    # columns / tdb filters / NEEDS WORK sort.
    assert "_pending_update_actionable_sql" in block, (
        "v1.22.10: attn_pills=update branch must invoke the actionable-gate "
        "helper (carries the urls_match + URL-diff branches)"
    )
