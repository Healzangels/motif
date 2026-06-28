"""v1.19.13 — recover inline-ADOPT attribution lost in the v1.18.0 cascade.

Third sibling to v1.18.10 (per-row SET URL recovery from events)
and v1.18.83 (bulk-import recovery from audit_events).

## The bug class

v1.18.0's FK cascade wiped local_files + placements. v1.18.5's
recovery walker rebuilt missing rows from on-disk evidence, but
`_infer_source_kind` only knows three states:

  * TDB-tracked + no override → ('auto', 'themerrdb') → T
  * TDB-tracked + override    → ('manual', 'url')      → U
  * plex_orphan               → ('manual', 'adopt')    → A

For a TDB-tracked row that was originally ADOPTED (user hand-picked
a sidecar that happened to share a YouTube ID with the TDB record),
the walker had no signal to know the 'adopt' provenance — it landed
at 'themerrdb' (T). Invisible until the TDB URL goes dead.

the user's 50 States of Fright (tmdb=94251) repro on 2026-05-24:
  - Inline-adopted on 5/7 → source_kind='adopt'
  - v1.18.0 cascade nuked local_files between 5/7 and 5/20
  - v1.18.5 walker ran 5/20 05:11:20, rebuilt as 'themerrdb'
  - TDB URL went dead 5/25 (video_removed)
  - INFO panel renders SRC=T with broken URL even though the
    file on disk is the user's hand-picked theme.

Direct DB count at v1.19.13 cut: 228 misclassified rows out of
340 historical adopt events on the user's install (~67%).

## What's pinned

- `maybe_recover_lost_adopts(db_path, themes_dir)` exists in
  app.core.recovery_v55.
- Scans `events` (not audit_events) WHERE component='adopt' AND
  message LIKE 'Inline adopt of sidecar at%' — the canonical
  phrase from adopt.py:adopt_folder since v1.10.9.
- Verifies on-disk sha256 matches the adopt-time sha256 BEFORE
  flipping source_kind. Sha drift → leave as 'themerrdb' (file
  was genuinely re-downloaded post-adopt).
- Only touches rows currently classified as 'themerrdb' — never
  clobbers 'url'/'upload'/'adopt'.
- Backfills file_sha256 alongside the source_kind flip (v1.18.5
  walker left it NULL).
- Has independent marker `recovery_lost_adopts_done_at` so it
  runs on installs where prior walkers already completed.
- main.py wires the walker into boot after the v1.18.83 block,
  gated on `settings.is_paths_ready()` (walker needs themes_dir
  to read files).
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


def test_adopt_recovery_function_exists():
    """`maybe_recover_lost_adopts(db_path, themes_dir)` must be
    importable from app.core.recovery_v55."""
    src = RECOVERY_PY.read_text()
    assert "def maybe_recover_lost_adopts(" in src
    # Two-argument signature (themes_dir is required — walker
    # streams files to compute sha256).
    assert "themes_dir" in src[src.index("def maybe_recover_lost_adopts("):]


def test_adopt_recovery_scans_events_not_audit_events():
    """v1.19.13 distinction: adopt events go to `events` (via
    log_event from adopt.py:adopt_folder), NOT to audit_events
    (adopt_folder doesn't call _record_audit). Walker must read
    from `events`."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_adopts(")
    # Walker body ends at the next top-level def or EOF.
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "FROM events" in body, (
        "v1.19.13: walker must read from `events` — that's where "
        "adopt actions log. Reading audit_events would miss them."
    )
    assert "component = 'adopt'" in body
    assert "Inline adopt of sidecar at" in body, (
        "v1.19.13: must match the canonical adopt log message "
        "pinned in adopt.py:adopt_folder since v1.10.9"
    )


def test_adopt_recovery_has_independent_marker():
    """Independent marker so the walker runs even on installs
    where v1.18.10/v1.18.83 markers are already set."""
    src = RECOVERY_PY.read_text()
    assert "recovery_lost_adopts_done_at_v1_19_14" in src


def test_adopt_recovery_verifies_sha_before_reclassifying():
    """Hash verification is the load-bearing safety. Pre-fix,
    a row classified as 'themerrdb' could be EITHER:
      a) originally-adopted file that survived (reclassify!)
      b) post-adopt re-download (file is genuinely TDB now —
         leave alone, 'themerrdb' reflects reality)
    The walker MUST compute on-disk sha256 and compare with the
    adopt event's sha256 before flipping."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_adopts(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "hashlib" in body, (
        "v1.19.13: walker must compute sha256 from disk"
    )
    assert "sha256()" in body
    # Mismatch branch leaves classification alone.
    assert "sha_mismatch" in body, (
        "v1.19.13: stats must surface sha drift count so the "
        "operator can see how many rows were left as themerrdb "
        "due to legitimate post-adopt re-download"
    )


def test_adopt_recovery_only_touches_themerrdb_rows():
    """Only act on source_kind='themerrdb' — never clobber
    'url' (bulk-import recovered by v1.18.83) or 'upload'
    (user uploaded mp3) or 'adopt' (already correct)."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_adopts(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "source_kind = 'themerrdb'" in body, (
        "v1.19.13: UPDATE must scope to source_kind='themerrdb' "
        "in the WHERE clause as a belt-and-suspenders guard"
    )
    assert "SET source_kind = 'adopt'" in body


def test_adopt_recovery_backfills_file_sha256():
    """Free correctness win: v1.18.5 walker left file_sha256
    NULL. Since this walker computes the sha to verify, write
    it back alongside the source_kind flip."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_adopts(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "file_sha256 = ?" in body


# ── End-to-end behavioral tests ──────────────────────────────


def _seed_misclassified_row(
    db_path: Path,
    themes_dir: Path,
    *,
    media_type: str = "tv",
    tmdb_id: int = 94251,
    section_id: str = "2",
    section_subdir: str = "tv",
    title_subdir: str = "50 States of Fright (2020)",
    file_bytes: bytes = b"original-adopted-mp3-content",
    log_sha_override: str | None = None,
    current_source_kind: str = "themerrdb",
) -> tuple[str, Path]:
    """Materialize a row in the same shape as the user's 50 States
    repro: events table has the inline-adopt log, local_files
    has the misclassified themerrdb row, on-disk file matches
    the adopt-time sha unless log_sha_override is set.

    Returns (adopt_time_sha256, on_disk_path).
    """
    from app.core.db import init_db
    init_db(db_path)
    on_disk_sha = hashlib.sha256(file_bytes).hexdigest()
    log_sha = log_sha_override or on_disk_sha
    rel_path = f"{section_subdir}/{title_subdir}/theme.mp3"
    disk_path = themes_dir / rel_path
    disk_path.parent.mkdir(parents=True, exist_ok=True)
    disk_path.write_bytes(file_bytes)
    ts_adopt = "2026-05-07T13:27:24+00:00"
    ts_log = ts_adopt
    detail = json.dumps({
        "folder_path": f"/data/media/{section_subdir}/{title_subdir}",
        "sha256": log_sha,
        "kind": "orphan_resolvable",
        "section_id": section_id,
        "decided_by": "admin",
    })
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events (ts, level, component, media_type, "
            "                    tmdb_id, message, detail, section_id) "
            "VALUES (?, 'INFO', 'adopt', ?, ?, "
            "        'Inline adopt of sidecar at /data/media/" +
            section_subdir + "/" + title_subdir + "', ?, ?)",
            (ts_log, media_type, tmdb_id, detail, section_id),
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES (?, ?, ?, ?, ?, 'X8czRq2IFCY', 'auto', ?)",
            (media_type, tmdb_id, section_id, rel_path,
             "2026-05-20T05:11:20+00:00", current_source_kind),
        )
        conn.commit()
    return on_disk_sha, disk_path


def test_walker_reclassifies_sha_matching_row(tmp_path):
    """the user's 50 States scenario: adopt event sha matches the
    on-disk file → walker flips source_kind from 'themerrdb' to
    'adopt' and backfills file_sha256."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    sha, _ = _seed_misclassified_row(db_path, themes_dir)
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["adopt_events_scanned"] == 1
    assert stats["candidates"] == 1
    assert stats["reclassified"] == 1
    assert stats["sha_mismatch"] == 0
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lf = conn.execute(
            "SELECT source_kind, file_sha256 "
            "FROM local_files "
            "WHERE tmdb_id = 94251"
        ).fetchone()
    assert lf["source_kind"] == "adopt"
    assert lf["file_sha256"] == sha, (
        "v1.19.13: file_sha256 must be backfilled — v1.18.5 left "
        "it NULL and this walker has the hash in hand anyway"
    )


def test_walker_leaves_sha_drift_row_as_themerrdb(tmp_path):
    """If on-disk sha differs from the adopt-time sha, the file
    was re-downloaded after adopt (e.g., ACCEPT UPDATE before
    the v1.18.0 cascade). 'themerrdb' classification is now
    correct — walker must NOT flip it."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    # Pin a fabricated adopt-time sha that DOESN'T match disk.
    _seed_misclassified_row(
        db_path, themes_dir,
        log_sha_override="00" * 32,  # all-zeros, won't match disk
    )
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["candidates"] == 1
    assert stats["reclassified"] == 0
    assert stats["sha_mismatch"] == 1, (
        "v1.19.13: sha drift must increment the dedicated counter"
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        lf = conn.execute(
            "SELECT source_kind FROM local_files "
            "WHERE tmdb_id = 94251"
        ).fetchone()
    assert lf["source_kind"] == "themerrdb", (
        "v1.19.13: sha-drift row must KEEP 'themerrdb' — flipping "
        "it would falsely tag a TDB-downloaded file as adopted"
    )


def test_walker_skips_already_adopt_row(tmp_path):
    """The lucky 109 rows (per the user's count) whose adopt
    classification survived the cascade. Walker must not touch
    them — INSERT OR IGNORE-like idempotency."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _seed_misclassified_row(
        db_path, themes_dir, current_source_kind="adopt",
    )
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["reclassified"] == 0
    assert stats["skipped_already_adopt"] == 1


def test_walker_skips_url_row(tmp_path):
    """A row whose source_kind is 'url' was recovered by v1.18.10
    or v1.18.83 — has user_overrides → user owns the URL. Walker
    must not clobber that with 'adopt' even if an old adopt
    event exists in the log."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _seed_misclassified_row(
        db_path, themes_dir, current_source_kind="url",
    )
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["reclassified"] == 0
    assert stats["skipped_other_source_kind"] == 1


def test_walker_skips_missing_file(tmp_path):
    """File deleted off disk post-recovery → walker can't verify
    sha → must leave the row alone with a log breadcrumb."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _, disk_path = _seed_misclassified_row(db_path, themes_dir)
    disk_path.unlink()
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["reclassified"] == 0
    assert stats["file_missing"] == 1


def test_walker_marker_prevents_re_run(tmp_path):
    """Second invocation after marker stamp must be a no-op."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    _seed_misclassified_row(db_path, themes_dir)
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats1 = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats1["reclassified"] == 1
    stats2 = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats2["adopt_events_scanned"] == 0, (
        "v1.19.13: marker must short-circuit before scanning"
    )
    assert stats2["reclassified"] == 0


def test_walker_no_op_when_no_adopt_events(tmp_path):
    """Fresh install with no adopt events — walker stamps NO
    marker so future adopts + cascade can still recover."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["adopt_events_scanned"] == 0
    assert stats["reclassified"] == 0
    with sqlite3.connect(db_path) as conn:
        marker = conn.execute(
            "SELECT 1 FROM runtime_settings "
            "WHERE key = 'recovery_lost_adopts_done_at'"
        ).fetchone()
    assert marker is None, (
        "v1.19.13: walker must NOT stamp marker when no adopt "
        "events — otherwise a future adopt+cascade would be "
        "locked out from recovery"
    )


def test_walker_latest_event_wins_dedup(tmp_path):
    """If the same (mt, tmdb, section) has multiple adopt events
    (re-adopt over time), the LATEST event's sha is the one we
    verify against. the user's count showed tmdb 9805 appearing 3x
    in the misclass list — multiple historical adopts."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    # First adopt: write a row with a stale sha that won't match.
    sha_live, disk_path = _seed_misclassified_row(
        db_path, themes_dir,
        file_bytes=b"current-disk-content",
    )
    # Add an OLDER adopt event with a non-matching sha. The
    # LATEST event (seeded by _seed) already has matching sha.
    older_ts = "2026-01-01T00:00:00+00:00"
    older_detail = json.dumps({
        "folder_path": "/data/media/tv/50 States of Fright (2020)",
        "sha256": "ff" * 32,  # would NOT match disk
        "kind": "orphan_resolvable",
        "section_id": "2",
        "decided_by": "admin",
    })
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events (ts, level, component, media_type, "
            "                    tmdb_id, message, detail, section_id) "
            "VALUES (?, 'INFO', 'adopt', 'tv', 94251, "
            "        'Inline adopt of sidecar at /data/media/tv/50 States', "
            "        ?, '2')",
            (older_ts, older_detail),
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["adopt_events_scanned"] == 2
    assert stats["candidates"] == 1, (
        "v1.19.13: 2 events same target → dedup to 1 candidate"
    )
    assert stats["reclassified"] == 1, (
        "v1.19.13: latest event sha matches disk → reclassify. "
        "If dedup picked the older event, sha drift would block."
    )


def test_walker_handles_no_local_files_row(tmp_path):
    """Adopt event exists but local_files row was never recreated
    (e.g., file missing from disk during v1.18.5 walker pass).
    Walker must skip gracefully and log."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    detail = json.dumps({
        "folder_path": "/data/media/tv/orphan",
        "sha256": "ab" * 32,
        "kind": "orphan_resolvable",
        "section_id": "2",
        "decided_by": "admin",
    })
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events (ts, level, component, media_type, "
            "                    tmdb_id, message, detail, section_id) "
            "VALUES ('2026-05-07T13:27:24+00:00', 'INFO', 'adopt', "
            "        'tv', 99999, "
            "        'Inline adopt of sidecar at /data/media/tv/orphan', "
            "        ?, '2')",
            (detail,),
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_recover_lost_adopts
    stats = maybe_recover_lost_adopts(db_path, themes_dir)
    assert stats["reclassified"] == 0
    assert stats["skipped_no_local_files"] == 1
