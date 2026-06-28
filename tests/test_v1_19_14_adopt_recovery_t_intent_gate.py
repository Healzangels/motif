"""v1.19.14 — refine adopt walker to honor post-adopt T-intent.

## Why v1.19.14

v1.19.13 reclassified 224 rows themerrdb→adopt by computing the
on-disk sha and matching it to the adopt event sha. the user's
diagnostic showed 17 of those (~5%) were OVER-FLIPPED: the user
had adopted, then later clicked REPLACE-WITH-THEMERRDB (explicit
A→T intent shift). The sha-match check still fired because the
TDB URL happens to serve byte-identical audio to the original
adopted sidecar (common when the user adopted a file that itself
came from the same YouTube video).

Net effect: walker overrode a USER INTENT with a STALE SIGNAL.

## The fix

v1.19.14 adds a post-adopt T-intent gate BEFORE the sha check.
Query events for any of these messages POSTDATING the latest
adopt event for the (mt, tmdb, section):

  * `component='download' AND message LIKE 'Downloaded theme%'`
  * `component='adopt' AND message LIKE 'Replace-with-ThemerrDB%'`

If found, the row's most-recent expressed intent was T:
  - Current source_kind='adopt' → flip BACK to 'themerrdb'
    (undoes v1.19.13's over-flip).
  - Current source_kind='themerrdb' → leave alone (already
    matches intent).
  - Other → leave alone.

Without a post-adopt T-intent event, fall through to the
original v1.19.13 sha-match logic.

## Marker key bump

`recovery_lost_adopts_done_at` (v1.19.13) →
`recovery_lost_adopts_done_at_v1_19_14`. Mirrors v1.18.11's
`recovery_v55_done_at` → `recovery_v55_done_at_v1_18_11`
bump pattern — installs that already ran v1.19.13 re-run
with the refined logic instead of staying locked at the
stale outcome.

## What's pinned

- New marker key string in code + boot wiring comment.
- Walker queries events for post-adopt T-intent BEFORE the
  sha check, not after.
- Reversal path: adopt→themerrdb when post-adopt T-intent
  fires and the row is currently 'adopt'.
- Stats surface BOTH `reclassified` (the v1.19.13 outcome,
  preserved) AND `reverted_v1_19_13_overflips` (new).
- Stats also surface `skipped_post_adopt_t_intent` for rows
  that were already themerrdb and stayed that way thanks to
  the gate (the "saved future installs from over-flipping"
  count).
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"
MAIN_PY = REPO / "app" / "main.py"


# ── Source-level pins ────────────────────────────────────────


def test_marker_key_bumped_to_v1_19_14():
    """Marker key must change so v1.19.13-already-completed
    installs re-run with the refined logic. Pattern mirrors
    v1.18.11's bump from `recovery_v55_done_at` →
    `recovery_v55_done_at_v1_18_11`."""
    src = RECOVERY_PY.read_text()
    assert "recovery_lost_adopts_done_at_v1_19_14" in src, (
        "v1.19.14: marker key must include the v1_19_14 suffix "
        "so existing installs re-run the refined walker"
    )


def test_walker_queries_post_adopt_t_intent():
    """The walker must query events for T-intent entries that
    POSTDATE each candidate's adopt event. Two messages count
    as T-intent: 'Downloaded theme%' and 'Replace-with-
    ThemerrDB%'."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_adopts(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "Downloaded theme%" in body, (
        "v1.19.14: download-success message must count as "
        "post-adopt T-intent"
    )
    assert "Replace-with-ThemerrDB%" in body, (
        "v1.19.14: REPLACE-W-TDB enqueue message must count "
        "as post-adopt T-intent"
    )
    # The query must filter on ts > adopt event ts.
    assert "ts > ?" in body or "ts > info" in body, (
        "v1.19.14: T-intent query must be scoped to events "
        "POSTDATING the adopt event"
    )


def test_walker_stats_expose_v1_19_14_counters():
    """The walker's stats dict must surface counters specific
    to v1.19.14 so the operator can see what changed vs. the
    v1.19.13 outcome."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_adopts(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "reverted_v1_19_13_overflips" in body, (
        "v1.19.14: stats must count reversals so the operator "
        "sees how many v1.19.13 over-flips were corrected"
    )
    assert "skipped_post_adopt_t_intent" in body, (
        "v1.19.14: stats must count rows that the T-intent "
        "gate protected (already themerrdb, would've been "
        "over-flipped by v1.19.13)"
    )


# ── End-to-end behavioral tests ──────────────────────────────


def _seed_adopt_row(
    db_path: Path,
    themes_dir: Path,
    *,
    media_type: str = "tv",
    tmdb_id: int = 229610,
    section_id: str = "3",
    section_subdir: str = "anime",
    title_subdir: str = "Test Anime (2024)",
    file_bytes: bytes = b"adopted-content",
    current_source_kind: str = "themerrdb",
    adopt_ts: str = "2026-05-06T23:38:32+00:00",
) -> tuple[str, Path, str]:
    """Seed a row with the v1.18.5 walker signature (current
    source_kind='themerrdb') and the inline-adopt event. Returns
    (adopt_sha256, on_disk_path, adopt_ts)."""
    from app.core.db import init_db
    init_db(db_path)
    on_disk_sha = hashlib.sha256(file_bytes).hexdigest()
    rel_path = f"{section_subdir}/{title_subdir}/theme.mp3"
    disk_path = themes_dir / rel_path
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(file_bytes)
    detail = json.dumps({
        "folder_path": f"/data/media/{section_subdir}/{title_subdir}",
        "sha256": on_disk_sha,
        "kind": "orphan_resolvable",
        "section_id": section_id,
        "decided_by": "admin",
    })
    msg = f"Inline adopt of sidecar at /data/media/{section_subdir}/{title_subdir}"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events (ts, level, component, media_type, "
            "                    tmdb_id, message, detail, section_id) "
            "VALUES (?, 'INFO', 'adopt', ?, ?, ?, ?, ?)",
            (adopt_ts, media_type, tmdb_id, msg, detail, section_id),
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES (?, ?, ?, ?, ?, 'e9athtafL7w', 'auto', ?)",
            (media_type, tmdb_id, section_id, rel_path,
             "2026-05-20T01:11:20+00:00", current_source_kind),
        )
        conn.commit()
    return on_disk_sha, disk_path, adopt_ts


def _add_event(
    db_path: Path, *,
    ts: str, component: str, message: str,
    media_type: str = "tv", tmdb_id: int = 229610,
    section_id: str = "3",
):
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events (ts, level, component, media_type, "
            "                    tmdb_id, message, detail, section_id) "
            "VALUES (?, 'INFO', ?, ?, ?, ?, '{}', ?)",
            (ts, component, media_type, tmdb_id, message, section_id),
        )
        conn.commit()


def test_walker_reverts_v1_19_13_overflip_via_download_event(tmp_path):
    """the user's repro: row currently 'adopt' (v1.19.13 flipped
    it), but events show a "Downloaded theme" entry AFTER the
    latest adopt event. v1.19.14 must REVERT to 'themerrdb'."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _seed_adopt_row(
        db_path, themes_dir,
        current_source_kind="adopt",  # v1.19.13 already flipped
        adopt_ts="2026-05-06T23:38:32+00:00",
    )
    _add_event(
        db_path,
        ts="2026-05-09T01:12:16+00:00",
        component="download",
        message="Downloaded theme for Test Anime",
    )
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["reverted_v1_19_13_overflips"] == 1, (
        "v1.19.14: post-adopt Downloaded-theme event must "
        "trigger reversal of v1.19.13's over-flip"
    )
    assert stats["reclassified"] == 0
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lf = conn.execute(
            "SELECT source_kind FROM local_files WHERE tmdb_id=229610"
        ).fetchone()
    assert lf["source_kind"] == "themerrdb", (
        "v1.19.14: reversal must produce themerrdb in DB"
    )


def test_walker_reverts_v1_19_13_overflip_via_replace_event(tmp_path):
    """Same as above but the post-adopt event is the
    "Replace-with-ThemerrDB enqueued" message instead of the
    download-success message. Both must count as T-intent."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _seed_adopt_row(
        db_path, themes_dir, current_source_kind="adopt",
    )
    _add_event(
        db_path,
        ts="2026-05-09T01:12:15+00:00",
        component="adopt",
        message="Replace-with-ThemerrDB enqueued",
    )
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["reverted_v1_19_13_overflips"] == 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lf = conn.execute(
            "SELECT source_kind FROM local_files WHERE tmdb_id=229610"
        ).fetchone()
    assert lf["source_kind"] == "themerrdb"


def test_walker_gates_first_time_install_against_overflip(tmp_path):
    """Future install (never ran v1.19.13) with the same pattern:
    adopt event + post-adopt T-intent + sha matches. v1.19.14
    must NOT flip the row — it stays themerrdb. Tests the gate's
    forward protection, not just the reversal."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _seed_adopt_row(
        db_path, themes_dir,
        current_source_kind="themerrdb",  # never flipped by v1.19.13
    )
    _add_event(
        db_path,
        ts="2026-05-09T01:12:16+00:00",
        component="download",
        message="Downloaded theme for Test Anime",
    )
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["reclassified"] == 0, (
        "v1.19.14: gate must block reclassification when "
        "post-adopt T-intent exists, regardless of sha match"
    )
    assert stats["skipped_post_adopt_t_intent"] == 1, (
        "v1.19.14: stats must count the gate-protected rows"
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lf = conn.execute(
            "SELECT source_kind FROM local_files WHERE tmdb_id=229610"
        ).fetchone()
    assert lf["source_kind"] == "themerrdb", (
        "v1.19.14: gate-protected row stays themerrdb"
    )


def test_walker_still_reclassifies_no_t_intent_case(tmp_path):
    """Original v1.19.13 behavior preserved: no post-adopt
    T-intent + sha matches → reclassify to 'adopt'. This is the
    the user's 50-States-of-Fright happy path."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    sha, _, _ = _seed_adopt_row(
        db_path, themes_dir, current_source_kind="themerrdb",
    )
    # No post-adopt events seeded.
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["reclassified"] == 1
    assert stats["reverted_v1_19_13_overflips"] == 0
    assert stats["skipped_post_adopt_t_intent"] == 0
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lf = conn.execute(
            "SELECT source_kind, file_sha256 "
            "FROM local_files WHERE tmdb_id=229610"
        ).fetchone()
    assert lf["source_kind"] == "adopt"
    assert lf["file_sha256"] == sha


def test_walker_ignores_pre_adopt_t_intent(tmp_path):
    """If a "Downloaded theme" event exists BEFORE the adopt
    event (the original failed-TDB-then-user-adopted-sidecar
    sequence), the gate must NOT fire — the adopt is the most
    recent intent."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _seed_adopt_row(
        db_path, themes_dir, current_source_kind="themerrdb",
        adopt_ts="2026-05-06T23:38:32+00:00",
    )
    # Earlier download event — pre-adopt.
    _add_event(
        db_path,
        ts="2026-05-01T00:00:00+00:00",  # well before adopt
        component="download",
        message="Downloaded theme for Test Anime",
    )
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["reclassified"] == 1, (
        "v1.19.14: pre-adopt T-intent events must NOT trigger "
        "the gate — the adopt event is newer and wins"
    )
    assert stats["skipped_post_adopt_t_intent"] == 0


def test_walker_marker_prevents_re_run_with_new_key(tmp_path):
    """Second invocation after marker stamp must be a no-op.
    Uses the v1.19.14 marker key, not the v1.19.13 one."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _seed_adopt_row(db_path, themes_dir)
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats1 = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats1["reclassified"] == 1
    stats2 = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats2["adopt_events_scanned"] == 0, (
        "v1.19.14: marker stamp must short-circuit re-runs"
    )
    # Verify the marker key used.
    with sqlite3.connect(db_path) as conn:
        markers = [r[0] for r in conn.execute(
            "SELECT key FROM runtime_settings WHERE key LIKE 'recovery_%'"
        ).fetchall()]
    assert "recovery_lost_adopts_done_at_v1_19_14" in markers


def test_walker_v1_19_13_marker_does_not_block_v1_19_14(tmp_path):
    """Installs that ran v1.19.13 (and have the OLD marker
    `recovery_lost_adopts_done_at`) must NOT skip the v1.19.14
    walker. The new walker checks ONLY the new marker key."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _seed_adopt_row(
        db_path, themes_dir, current_source_kind="adopt",
    )
    _add_event(
        db_path,
        ts="2026-05-09T01:12:16+00:00",
        component="download",
        message="Downloaded theme for Test Anime",
    )
    # Pre-seed the OLD v1.19.13 marker.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO runtime_settings (key, value, updated_at) "
            "VALUES ('recovery_lost_adopts_done_at', "
            "        '2026-05-20T05:11:20+00:00', "
            "        '2026-05-20T05:11:20+00:00')"
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["reverted_v1_19_13_overflips"] == 1, (
        "v1.19.14: old v1.19.13 marker must NOT prevent the "
        "refined walker from running — that's the whole point "
        "of bumping the marker key"
    )


def test_walker_mixed_scenario_three_rows(tmp_path):
    """Realistic mixed scenario:
      - Row A: adopt + no post-T-intent + sha matches →
        reclassified to 'adopt' (v1.19.13 happy path).
      - Row B: adopt + post-Downloaded-theme + currently
        'adopt' (v1.19.13 over-flipped) → reverted to
        'themerrdb'.
      - Row C: adopt + post-Replace-w-TDB + currently
        'themerrdb' (v1.19.13 skipped — sha would've matched
        but it ran before this refinement) → gate keeps as
        'themerrdb', counted as skipped_post_adopt_t_intent.
    """
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _seed_adopt_row(
        db_path, themes_dir,
        tmdb_id=1, title_subdir="Row A",
        file_bytes=b"row-a-content",
        current_source_kind="themerrdb",
        adopt_ts="2026-05-05T00:00:00+00:00",
    )
    _seed_adopt_row(
        db_path, themes_dir,
        tmdb_id=2, title_subdir="Row B",
        file_bytes=b"row-b-content",
        current_source_kind="adopt",  # over-flipped
        adopt_ts="2026-05-05T00:00:00+00:00",
    )
    _add_event(
        db_path, tmdb_id=2,
        ts="2026-05-09T00:00:00+00:00",
        component="download",
        message="Downloaded theme for Row B",
    )
    _seed_adopt_row(
        db_path, themes_dir,
        tmdb_id=3, title_subdir="Row C",
        file_bytes=b"row-c-content",
        current_source_kind="themerrdb",
        adopt_ts="2026-05-05T00:00:00+00:00",
    )
    _add_event(
        db_path, tmdb_id=3,
        ts="2026-05-09T00:00:00+00:00",
        component="adopt",
        message="Replace-with-ThemerrDB enqueued",
    )
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["candidates"] == 3
    assert stats["reclassified"] == 1, "Row A reclassified"
    assert stats["reverted_v1_19_13_overflips"] == 1, (
        "Row B reverted"
    )
    assert stats["skipped_post_adopt_t_intent"] == 1, (
        "Row C protected by gate"
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = {r["tmdb_id"]: r["source_kind"] for r in conn.execute(
            "SELECT tmdb_id, source_kind FROM local_files "
            "ORDER BY tmdb_id"
        ).fetchall()}
    assert rows == {1: "adopt", 2: "themerrdb", 3: "themerrdb"}
