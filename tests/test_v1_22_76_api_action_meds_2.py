"""v1.22.76 (audit round 2, Batch C #5) — api.py action MEDs, part 2.

(1) UNMANAGE never called _cancel_jobs_for_row, and FORGET's legacy
global branch (no section_id) didn't either — the v1.18.73
check-then-act class: a pending download/place completing after the
destructive deletes re-INSERTed local_files/placements (ghost
tracking reborn). Both now cancel first, like UNPLACE/FORGET-section/
DELETE always did.

(2) PUSH (api_replace_item) and SWITCH PLACEMENT cancelled place jobs
TITLE-WIDE while their re-enqueues only re-cover the named edition's
sections — a sibling edition's pending place died silently (PL chip
stuck until the hourly sweep). Both cancels now use the v1.21.82
payload-edition match when an edition was named; legacy no-rk callers
keep title-wide semantics.

(3) KEEP MISMATCH with a stale rating_key (the v1.18.90 reaper
re-minted it) silently WIDENED to a title-wide ack — fail-open scope
widening with no breadcrumb. Now a 409 ("stale rating_key").

(4) The unplace/LPS restore-loop's motif-hash source lookup was
edition-blind on a multi-edition section — it could hash a SIBLING
edition's file, making the fallback picker re-select motif's own
upload instead of restoring Plex's theme. Now prefers THIS
placement's edition row (the standard IN (?, '') fallback shape).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from test_v1_14_59_recovery_options_behavioral import (  # noqa: F401
    app_client, _seed_section, _seed_theme)

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


# ── (1) check-then-act coverage ──────────────────────────────


def test_cancel_jobs_for_row_called_from_unmanage_and_global_forget():
    # def + unplace + forget-section + forget-global + unmanage + delete.
    assert API_PY.count("_cancel_jobs_for_row(") == 6, (
        "v1.22.76: UNMANAGE + FORGET's global branch must cancel "
        "in-flight jobs before their destructive sweeps"
    )
    i = API_PY.index("async def api_unmanage_item(")
    body = API_PY[i:API_PY.index("\n    @app.", i)]
    assert "_cancel_jobs_for_row(" in body
    j = API_PY.index("# Legacy global PURGE path")
    assert "_cancel_jobs_for_row(" in API_PY[j:j + 600]


# ── (2) PUSH/SWITCH cancels edition-scoped ───────────────────


def test_push_and_switch_cancels_edition_scoped():
    for var in ("_repl_edition", "_sw_edition"):
        i = API_PY.index(f"if {var} is not None:")
        block = API_PY[i:i + 900]
        assert "json_extract(payload, '$.edition_key')" in block, var
        assert "UPDATE jobs SET status = 'cancelled'" in block, var


# ── (3) KEEP MISMATCH stale rk → 409, not title-wide ─────────


def _seed_pending_mismatch(db, *, sections=("1",)):
    with sqlite3.connect(db) as conn:
        for s in sections:
            _seed_section(conn, s, title=f"Movies {s}")
        _seed_theme(conn, tmdb_id=100)
        for s in sections:
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind, mismatch_state)"
                " VALUES ('movie', 100, ?, '', 'm/t.mp3',"
                " '2026-06-11T00:00:00+00:00', '', 'auto', 'themerrdb',"
                " 'pending')", (s,))
        conn.commit()


def test_keep_mismatch_stale_rk_409s(app_client):
    client, db = app_client
    _seed_pending_mismatch(db, sections=("1", "2"))
    r = client.post(
        "/api/items/movie/100/keep-mismatch?rating_key=gone-rk",
        headers=AUTH)
    assert r.status_code == 409, r.text
    assert "stale rating_key" in r.json()["detail"]
    with sqlite3.connect(db) as conn:
        states = [x[0] for x in conn.execute(
            "SELECT mismatch_state FROM local_files WHERE tmdb_id=100"
            " ORDER BY section_id").fetchall()]
    assert states == ["pending", "pending"], (
        "v1.22.76: a stale rk must not widen to a title-wide ack"
    )


def test_keep_mismatch_without_rk_keeps_legacy_title_wide(app_client):
    client, db = app_client
    _seed_pending_mismatch(db, sections=("1", "2"))
    r = client.post("/api/items/movie/100/keep-mismatch", headers=AUTH)
    assert r.status_code == 200, r.text
    with sqlite3.connect(db) as conn:
        states = {x[0] for x in conn.execute(
            "SELECT mismatch_state FROM local_files WHERE tmdb_id=100"
        ).fetchall()}
    assert states == {"acked"}


# ── (4) restore-loop hash lookup prefers the placement edition ─


def test_unplace_hash_lookup_prefers_placement_edition():
    i = API_PY.index('_pr_edition = pr["edition_key"] or ""')
    block = API_PY[i:i + 700]
    assert "edition_key IN (?, '')" in block
    assert "ORDER BY (edition_key = ?) DESC LIMIT 1" in block
