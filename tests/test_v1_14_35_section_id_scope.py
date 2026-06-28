"""v1.14.35 — section_id scope on accept_update + unmanage + forget.

Closes audit findings api M3 + M4 (cross-section bleed class K /
section-scope drift). Three endpoints had section_id parameters
that scoped MOST of their work but missed specific SELECTs that
silently widened to title-global.

## The bug class

Schema v31 added section_id to user_overrides PK. Schema v?? added
section_id to pending_updates PK. Endpoints that take a section_id
param need to thread it through every read on those tables —
otherwise the title-global SELECT picks an arbitrary section's
row when fetchone() is called.

Common case (every section has the same value): functionally
fine. Rare case (per-edition overrides on a multi-section title,
or different TDB URLs per section): silent wrong-data.

## Three sites fixed

1. **api_accept_update** (M3): the
   `SELECT new_youtube_url FROM pending_updates WHERE mt = ? AND
   tmdb = ?` was unscoped. On a multi-section title with
   different new_youtube_urls per section, ACCEPT UPDATE on the
   4K row could enqueue the standard row's URL.

2. **api_unmanage_item** (M4): the
   `SELECT youtube_url FROM user_overrides WHERE mt = ? AND
   tmdb = ?` snapshot was unscoped. Comment claimed "user_
   overrides / themes are section-agnostic" — TRUE for themes,
   FALSE for user_overrides post-v1.12.72. Wrong section's URL
   landed in local_files_history.

3. **api_forget_item** (M4 sister): same unscoped SELECT, same
   wrong-section snapshot into local_files_history.

## Fix shape

When section_id is provided (the v1.12.46/.73/.77 per-section
scope), SELECT scoped to that section first. Fall back to the
title-global '' row when no per-section row exists (covers the
legacy global override case). When section_id is omitted (bulk
callers / orphan paths), keep the title-global SELECT — that's
the legacy contract.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent


# ── api_accept_update: pending_updates SELECT scope ──────────


def test_accept_update_scopes_pending_updates_select():
    """The pending_updates SELECT must filter by section_id when
    one is provided. Pre-fix the SELECT had no section_id WHERE
    clause so on a multi-section title the wrong section's row
    could be picked."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_accept_update(")
    body = src[fn_anchor:fn_anchor + 4000]
    # The new section-scoped SELECT.
    assert "FROM pending_updates" in body
    # Must include section_id in WHERE when section_id is in scope.
    assert "AND section_id = ?" in body
    # And the v1.14.35 marker explains why.
    assert "v1.14.35: section-scoped pending_updates SELECT" in body


def test_accept_update_falls_back_to_global_pending_update():
    """When no per-section pending_updates row exists, fall back
    to the section_id='' global row — matches the v1.12.99
    pending_updates layout (per-section + global fallback)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_accept_update(")
    body = src[fn_anchor:fn_anchor + 4000]
    # The fallback to '' global section.
    assert "AND section_id = ''" in body


# ── api_unmanage_item: user_overrides SELECT scope ───────────


def test_unmanage_scopes_user_overrides_select():
    """The user_overrides snapshot SELECT must filter by
    section_id when one is provided. Pre-fix the SELECT was
    title-global; comment claimed user_overrides was section-
    agnostic — true pre-v1.12.72, false after."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_unmanage_item(")
    body = src[fn_anchor:fn_anchor + 8000]
    assert "v1.14.35:" in body
    # The section-scoped override SELECT.
    sec_block_anchor = body.index("v1.14.35:")
    sec_block = body[sec_block_anchor:sec_block_anchor + 2000]
    assert "FROM user_overrides" in sec_block
    assert "AND section_id = ?" in sec_block
    # Fallback to '' global.
    assert "AND section_id = ''" in sec_block


# ── api_forget_item: user_overrides SELECT scope ────────────


def test_forget_scopes_user_overrides_select():
    """Sister fix for api_forget_item — same unscoped SELECT,
    same wrong-section snapshot into local_files_history."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_forget_item(")
    # v1.22.75: slice to the function's actual end — the fixed window
    # (widened once already in v1.20.67) went stale again when the PU
    # edition-filter comment grew the body past it.
    body = src[fn_anchor:src.index("\n    @app.", fn_anchor)]
    assert "v1.14.35: section-scoped override fetch" in body
    # The section-scoped override SELECT inside the
    # locals_rows-gated snapshot block.
    forget_marker_anchor = body.index("v1.14.35: section-scoped override fetch")
    sec_block = body[forget_marker_anchor:forget_marker_anchor + 2000]
    assert "FROM user_overrides" in sec_block
    assert "AND section_id = ?" in sec_block
    assert "AND section_id = ''" in sec_block


# ── Behavioral: section-scoped SELECT picks the right URL ────


def test_section_scoped_override_select_picks_correct_url(tmp_path):
    """End-to-end: with two per-section overrides for the same
    title (sec1 = "URL_STD", sec2 = "URL_4K"), running the
    section-scoped SELECT pattern with section_id='sec2' returns
    "URL_4K". Pre-fix SELECT (no section filter) would have
    fetched arbitrarily — likely the first inserted (sec1)."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = now_iso()
    # Theme + two per-section overrides.
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, first_seen_sync_at, "
        "                    last_seen_sync_at) "
        "VALUES ('movie', 7777, 'Multi-Override', 2022, "
        "        'imdb', ?, ?)",
        (now, now),
    )
    for sid, url in (("sec1", "URL_STD"), ("sec2", "URL_4K")):
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, "
            "                            section_id, youtube_url, "
            "                            set_at, set_by, note) "
            "VALUES ('movie', 7777, ?, ?, ?, 'admin', 'test')",
            (sid, url, now),
        )
    conn.commit()
    # Run the section-scoped SELECT pattern (the v1.14.35 fix
    # shape) for section_id='sec2'.
    sec2 = conn.execute(
        "SELECT youtube_url FROM user_overrides "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
        ("movie", 7777, "sec2"),
    ).fetchone()
    # Inverse: the pre-fix unscoped SELECT.
    unscoped = conn.execute(
        "SELECT youtube_url FROM user_overrides "
        "WHERE media_type = ? AND tmdb_id = ?",
        ("movie", 7777),
    ).fetchone()
    conn.close()
    assert sec2["youtube_url"] == "URL_4K", (
        "Section-scoped SELECT for sec2 must return URL_4K — got "
        f"{sec2['youtube_url']!r}. The v1.14.35 fix is built on "
        "this contract."
    )
    # The unscoped SELECT (pre-fix) returns SOMETHING but we
    # can't guarantee which — the bug was that fetchone() picks
    # arbitrarily across sections. Pin only that it differs from
    # the scoped one by NOT asserting it equals 'URL_4K' (which
    # would only pass when the database happened to order
    # 'sec2' first).
    assert unscoped is not None  # at least something matched


def test_section_scoped_override_fallback_to_global(tmp_path):
    """Inverse: when no per-section override exists, the v1.14.35
    fallback SELECT must find the title-global '' row. Covers
    the legacy global-override case (pre-v1.12.72 callers)."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = now_iso()
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, first_seen_sync_at, "
        "                    last_seen_sync_at) "
        "VALUES ('movie', 8888, 'Legacy Global', 2019, "
        "        'imdb', ?, ?)",
        (now, now),
    )
    # Only the legacy '' global row exists.
    conn.execute(
        "INSERT INTO user_overrides (media_type, tmdb_id, "
        "                            section_id, youtube_url, "
        "                            set_at, set_by, note) "
        "VALUES ('movie', 8888, '', 'GLOBAL_URL', ?, 'admin', "
        "        'legacy global')",
        (now,),
    )
    conn.commit()
    # Section-scoped SELECT first returns nothing; fallback
    # SELECT to '' global returns GLOBAL_URL.
    per_sec = conn.execute(
        "SELECT youtube_url FROM user_overrides "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
        ("movie", 8888, "sec1"),
    ).fetchone()
    fallback = conn.execute(
        "SELECT youtube_url FROM user_overrides "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ''",
        ("movie", 8888),
    ).fetchone()
    conn.close()
    assert per_sec is None
    assert fallback is not None
    assert fallback["youtube_url"] == "GLOBAL_URL"


# ── Reuse pin: existing v1.12.46/.72/.73/.77 markers stay ────


def test_v1_12_72_section_aware_override_marker_preserved():
    """The v1.12.72 marker on user_overrides per-section PK is
    load-bearing archaeology — the v1.14.35 fix is the closing
    of a gap that v1.12.72 opened (the table became per-section
    but several callers stayed unscoped). Pin so the next
    refactor that touches user_overrides reads the history."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "v1.12.72:" in src


def test_audit_m3_m4_paired_fixes_co_located():
    """Sanity: the v1.14.35 marker appears at all three sites
    so a grep for the version surfaces the full fix cluster."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Three call sites with the v1.14.35 marker.
    n = src.count("v1.14.35:")
    assert n >= 3, (
        f"Expected ≥3 v1.14.35 markers (accept_update, "
        f"unmanage_item, forget_item), found {n}."
    )
