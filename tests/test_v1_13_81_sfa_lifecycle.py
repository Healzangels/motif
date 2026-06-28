"""v1.13.81 — section_failure_acks lifecycle fix.

The post-v1.13.80 audit caught a P0 data-correctness bug: every
code path that clears or changes themes.failure_kind /
failure_acked_at left orphan rows in `section_failure_acks` (sfa).

Symptom: a row was previously per-section-acked under failure_kind=A.
Then either (a) a successful re-download cleared themes.failure_*,
or (b) a new failure landed with kind=B. In both cases the
title-global flags reset correctly so the user gets re-alerted —
but the per-section sfa rows survived. Because library SQL +
FAIL pill tab breakdown filter on `sfa.acked_at IS NULL`, the
stale ack invisibly suppressed the FAIL signal for the sections
that were previously per-section-acked.

Three call sites needed the matching DELETE:

  1. worker._record_local_file (TDB success)         — Audit P0 #1
  2. worker download error handler (kind change)     — Audit P0 #2
  3. /api/items/.../clear-failure (bulk no-section)  — Audit P1 #3

Same-kind re-failures and first-time failures intentionally LEAVE
sfa rows alone — those preserve the user's prior per-section
"I dismissed this kind" decision (matches the existing CASE in
the UPDATE that preserves themes.failure_acked_at on same-kind).

These tests pin all three call sites by direct SQL exercise.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _seed_themed(conn, *, tmdb_id: int, failure_kind: str | None,
                 youtube_video_id: str = "vid"):
    """Seed a themes row + matching plex_items + a sfa row that
    previously dismissed the failure on section '1'."""
    now = _now_iso()
    conn.execute(
        "INSERT INTO themes ("
        "  media_type, tmdb_id, title, upstream_source,"
        "  youtube_url, youtube_video_id,"
        "  last_seen_sync_at, first_seen_sync_at,"
        "  failure_kind, failure_message, failure_at, failure_acked_at"
        ") VALUES ('movie', ?, 'x', 'imdb', ?, ?, ?, ?, ?, ?, ?, ?)",
        (tmdb_id,
         f"https://www.youtube.com/watch?v={youtube_video_id}",
         youtube_video_id, now, now,
         failure_kind, "msg" if failure_kind else None,
         now if failure_kind else None,
         now if failure_kind else None),
    )


def _seed_sfa(conn, *, tmdb_id: int, section_id: str = "1"):
    """Per-section ack row — what gets orphaned by the bug."""
    conn.execute(
        "INSERT INTO section_failure_acks ("
        "  media_type, tmdb_id, section_id, acked_at, acked_by"
        ") VALUES ('movie', ?, ?, ?, 'admin')",
        (tmdb_id, section_id, _now_iso()),
    )


def _sfa_count(db: Path, *, tmdb_id: int) -> int:
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM section_failure_acks "
            "WHERE media_type = 'movie' AND tmdb_id = ?",
            (tmdb_id,),
        ).fetchone()
    return row[0]


# ── Fix #1: TDB success drops sfa ────────────────────────────

def _apply_record_local_file_clear(db: Path, *, tmdb_id: int,
                                    source_kind: str):
    """Mirror the v1.13.81 SQL pattern from worker._record_local_file.
    The clear + DELETE only fires when source_kind='themerrdb' (the
    v1.13.74 scope guard) — this helper preserves both branches."""
    with sqlite3.connect(db) as conn:
        if source_kind == "themerrdb":
            conn.execute(
                "UPDATE themes SET failure_kind = NULL,"
                "                  failure_message = NULL,"
                "                  failure_at = NULL,"
                "                  failure_acked_at = NULL "
                "WHERE media_type = 'movie' AND tmdb_id = ? "
                "  AND failure_kind IS NOT NULL",
                (tmdb_id,),
            )
            conn.execute(
                "DELETE FROM section_failure_acks "
                "WHERE media_type = 'movie' AND tmdb_id = ?",
                (tmdb_id,),
            )


def test_tdb_success_drops_sfa_row(db):
    """The reported bug: row with failure_kind + a per-section sfa
    has the failure cleared by a successful TDB download. Pre-fix
    the sfa row survived — invisible suppression on next failure."""
    with sqlite3.connect(db) as conn:
        _seed_themed(conn, tmdb_id=1, failure_kind="video_removed")
        _seed_sfa(conn, tmdb_id=1)
    assert _sfa_count(db, tmdb_id=1) == 1
    _apply_record_local_file_clear(db, tmdb_id=1, source_kind="themerrdb")
    assert _sfa_count(db, tmdb_id=1) == 0


def test_user_url_success_preserves_sfa_row(db):
    """The v1.13.74 scope guard: source_kind != 'themerrdb' must
    NOT clear themes.failure_* (TDB still broken). The sfa row
    must follow the same scope — preserve the per-section ack
    when no themes-level clear happens."""
    with sqlite3.connect(db) as conn:
        _seed_themed(conn, tmdb_id=1, failure_kind="video_removed")
        _seed_sfa(conn, tmdb_id=1)
    _apply_record_local_file_clear(db, tmdb_id=1, source_kind="url")
    assert _sfa_count(db, tmdb_id=1) == 1


# ── Fix #2: failure_kind CHANGE drops sfa ───────────────────

def _apply_failure_write(db: Path, *, tmdb_id: int, new_kind: str):
    """Mirror the v1.13.81 SQL pattern from worker's download error
    handler: snapshot prior kind, UPDATE themes (CASE preserves
    failure_acked_at on same-kind), DELETE sfa only on kind change."""
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        prior = conn.execute(
            "SELECT failure_kind FROM themes "
            "WHERE media_type = 'movie' AND tmdb_id = ?",
            (tmdb_id,),
        ).fetchone()
        prior_kind = prior["failure_kind"] if prior else None
        conn.execute(
            "UPDATE themes SET failure_kind = ?, failure_message = ?,"
            "                  failure_at = ?,"
            "                  failure_acked_at = CASE"
            "                      WHEN failure_kind = ?"
            "                      THEN failure_acked_at"
            "                      ELSE NULL"
            "                  END "
            "WHERE media_type = 'movie' AND tmdb_id = ?",
            (new_kind, "msg", _now_iso(), new_kind, tmdb_id),
        )
        if prior_kind is not None and prior_kind != new_kind:
            conn.execute(
                "DELETE FROM section_failure_acks "
                "WHERE media_type = 'movie' AND tmdb_id = ?",
                (tmdb_id,),
            )


def test_kind_change_drops_sfa_rows(db):
    """A row was acked under video_removed; new failure lands as
    geo_blocked. failure_acked_at on themes correctly clears (CASE
    in UPDATE) — the per-section sfa rows must follow."""
    with sqlite3.connect(db) as conn:
        _seed_themed(conn, tmdb_id=1, failure_kind="video_removed")
        _seed_sfa(conn, tmdb_id=1)
    _apply_failure_write(db, tmdb_id=1, new_kind="geo_blocked")
    assert _sfa_count(db, tmdb_id=1) == 0


def test_same_kind_refailure_preserves_sfa_rows(db):
    """Re-failure with the SAME kind is the no-spam path — themes-
    level failure_acked_at is preserved by the CASE; sfa rows
    must be preserved too. The test pins the no-DELETE branch."""
    with sqlite3.connect(db) as conn:
        _seed_themed(conn, tmdb_id=1, failure_kind="video_removed")
        _seed_sfa(conn, tmdb_id=1)
    _apply_failure_write(db, tmdb_id=1, new_kind="video_removed")
    assert _sfa_count(db, tmdb_id=1) == 1


def test_first_failure_no_sfa_to_drop(db):
    """A row with no prior failure_kind getting its first failure —
    DELETE is skipped (prior_kind is None). The sfa table is empty
    here anyway, but pin the no-op."""
    with sqlite3.connect(db) as conn:
        _seed_themed(conn, tmdb_id=1, failure_kind=None)
    assert _sfa_count(db, tmdb_id=1) == 0
    _apply_failure_write(db, tmdb_id=1, new_kind="video_removed")
    assert _sfa_count(db, tmdb_id=1) == 0


# ── Fix #3: bulk ACK FAILURE drops sfa ──────────────────────

def _apply_bulk_clear_failure(db: Path, *, tmdb_id: int):
    """Mirror the v1.13.81 SQL pattern from /api/items/.../clear-failure
    bulk path (no section_id): set themes.failure_acked_at + DELETE
    every sfa row for the title."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE themes SET failure_acked_at = ? "
            "WHERE media_type = 'movie' AND tmdb_id = ? "
            "  AND failure_acked_at IS NULL",
            (_now_iso(), tmdb_id),
        )
        conn.execute(
            "DELETE FROM section_failure_acks "
            "WHERE media_type = 'movie' AND tmdb_id = ?",
            (tmdb_id,),
        )


def test_bulk_ack_drops_orphan_sfa_rows(db):
    """User had previously per-section-acked sections A and B; now
    triggers a bulk ack (no section_id). Bulk path supersedes
    per-section state, so the sfa rows go."""
    with sqlite3.connect(db) as conn:
        _seed_themed(conn, tmdb_id=1, failure_kind="video_removed")
        _seed_sfa(conn, tmdb_id=1, section_id="1")
        _seed_sfa(conn, tmdb_id=1, section_id="2")
    assert _sfa_count(db, tmdb_id=1) == 2
    _apply_bulk_clear_failure(db, tmdb_id=1)
    assert _sfa_count(db, tmdb_id=1) == 0


def test_bulk_ack_only_affects_target_title(db):
    """Cross-row safety: a bulk ack on tmdb_id=1 must not touch
    sfa rows belonging to tmdb_id=2."""
    with sqlite3.connect(db) as conn:
        _seed_themed(conn, tmdb_id=1, failure_kind="video_removed")
        _seed_themed(conn, tmdb_id=2, failure_kind="video_removed")
        _seed_sfa(conn, tmdb_id=1)
        _seed_sfa(conn, tmdb_id=2)
    _apply_bulk_clear_failure(db, tmdb_id=1)
    assert _sfa_count(db, tmdb_id=1) == 0
    assert _sfa_count(db, tmdb_id=2) == 1


# ── static guards: the DELETE is actually present in code ────

def test_worker_record_local_file_has_sfa_delete():
    """Pin worker.py:_record_local_file's sfa DELETE alongside the
    themes clear. Static guard against a regression that removes
    only the DELETE while keeping the themes clear (resurrects
    the orphan-sfa bug)."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "worker.py").read_text()
    # Find the source_kind=='themerrdb' branch.
    branch_start = src.index('if source_kind == "themerrdb":')
    branch = src[branch_start:branch_start + 2000]
    assert "DELETE FROM section_failure_acks" in branch


def test_worker_failure_handler_has_conditional_sfa_delete():
    """Pin the kind-change DELETE in the download error handler.
    The DELETE must be guarded by the prior_kind != kind.value
    check — same-kind re-failures preserve sfa rows."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "worker.py").read_text()
    # Anchor on the prior-snapshot SELECT we added.
    anchor = src.index('"SELECT failure_kind FROM themes "')
    block = src[anchor:anchor + 2500]
    assert "prior_kind is not None and prior_kind != kind.value" in block
    assert "DELETE FROM section_failure_acks" in block


def test_api_clear_failure_bulk_path_has_sfa_delete():
    """Pin the bulk-path DELETE in /api/items/.../clear-failure."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "web" / "api.py").read_text()
    # The bulk path is the `else:` branch after the `if section_id:`.
    # Anchor on the comment we added.
    assert "v1.13.81: bulk ack supersedes any earlier per-section" in src
    # Check the DELETE follows the anchor within ~500 chars.
    anchor = src.index("v1.13.81: bulk ack supersedes")
    nearby = src[anchor:anchor + 500]
    assert "DELETE FROM section_failure_acks" in nearby
