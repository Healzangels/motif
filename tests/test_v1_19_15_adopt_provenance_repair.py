"""v1.19.15 — fix walker provenance write + backfill 207 broken rows.

## The regression

v1.19.13's `maybe_recover_lost_adopts` UPDATE statement set only
`source_kind`, not `provenance`:

    UPDATE local_files
       SET source_kind = 'adopt',
           file_sha256 = ?
     WHERE ...

After v1.19.13/.14, 207 rows on the user's instance ended up at:
    source_kind = 'adopt'   (correctly classified)
    provenance  = 'auto'    (stale — inherited from v1.18.5
                             walker's themerrdb default)

The CLAUDE.md contract (and `_infer_source_kind` in this very
file) requires the pair to be in sync:

  | source_kind | provenance |
  |---|---|
  | themerrdb | auto   |
  | adopt     | manual |
  | url       | manual |
  | upload    | manual |

## The fix

Two parts:

  1. **Walker source fix**: v1.19.15 SET clauses include
     `provenance = 'manual'` (reclassify path) and
     `provenance = 'auto'` (revert_overflip path).
  2. **One-shot repair**: `maybe_repair_adopt_provenance` on
     boot, scans all 4 source_kind values, fixes any row whose
     provenance doesn't match the contract. Idempotent via
     `recovery_adopt_provenance_repair_done_at` marker.

## What's pinned

- Walker UPDATEs include the provenance column in both write
  paths (forward + revert).
- One-shot repair function exists, scans all 4 source_kind
  values.
- Repair function uses the dedicated marker key.
- main.py wires the repair on boot wrapped in try/except.
- Behavioral end-to-end: misclassified rows get fixed; correct
  rows untouched; marker prevents re-run.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"
MAIN_PY = REPO / "app" / "main.py"


# ── Source-level pins ────────────────────────────────────────


def test_walker_reclassify_path_writes_provenance():
    """The reclassify UPDATE (themerrdb → adopt) must SET both
    source_kind AND provenance. v1.19.13/.14 forgot the second.
    Anchor on the apply-site marker (`if op == "reclassify":`)
    rather than the actions-append site so we look at the UPDATE
    statement itself."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_adopts(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    rec_start = body.index('if op == "reclassify":')
    rec_chunk = body[rec_start:rec_start + 2000]
    assert "SET source_kind = 'adopt'" in rec_chunk
    assert "provenance = 'manual'" in rec_chunk, (
        "v1.19.15: reclassify path must write provenance='manual' "
        "alongside source_kind='adopt' to satisfy the contract"
    )


def test_walker_revert_path_writes_provenance():
    """The revert UPDATE (adopt → themerrdb) must SET both
    source_kind AND provenance back to 'auto'. Anchor on the
    apply-site marker (`elif op == "revert_overflip":`)."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_adopts(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    rev_start = body.index('elif op == "revert_overflip":')
    rev_chunk = body[rev_start:rev_start + 2000]
    assert "SET source_kind = 'themerrdb'" in rev_chunk
    assert "provenance = 'auto'" in rev_chunk, (
        "v1.19.15: revert path must write provenance='auto' "
        "alongside source_kind='themerrdb' to satisfy the contract"
    )


def test_repair_function_exists():
    """`maybe_repair_adopt_provenance(db_path)` must be
    importable from app.core.recovery_v55."""
    src = RECOVERY_PY.read_text()
    assert "def maybe_repair_adopt_provenance(" in src


def test_repair_has_independent_marker():
    """Independent marker so the repair runs even on installs
    where prior recovery markers are already set."""
    src = RECOVERY_PY.read_text()
    assert "recovery_adopt_provenance_repair_done_at" in src


def test_repair_covers_all_four_source_kinds():
    """The repair must address all four source_kind values in
    the contract — adopt, themerrdb, url, upload — even if some
    are currently 0 on a given install. Defensive pin for future
    drift from other paths."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_repair_adopt_provenance(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    # Each source_kind must appear in a WHERE clause + the
    # correct target provenance.
    for sk, expected in [
        ("adopt", "manual"),
        ("themerrdb", "auto"),
        ("url", "manual"),
        ("upload", "manual"),
    ]:
        assert f"source_kind = '{sk}'" in body, (
            f"v1.19.15: repair must scan source_kind='{sk}'"
        )
        # Each SK must have a corresponding target provenance.
        assert f"provenance = '{expected}'" in body, (
            f"v1.19.15: target provenance for '{sk}' must be "
            f"'{expected}' per the contract"
        )


# ── End-to-end behavioral tests ──────────────────────────────


def _seed_drift(
    db_path: Path,
    *,
    media_type: str = "movie",
    tmdb_id: int = 1,
    section_id: str = "1",
    source_kind: str = "adopt",
    provenance: str = "auto",
):
    from app.core.db import init_db
    if not db_path.exists():
        init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES (?, ?, ?, ?, '2026-05-20T00:00:00', 'vid', "
            "        ?, ?)",
            (media_type, tmdb_id, section_id,
             f"path/{tmdb_id}.mp3", provenance, source_kind),
        )
        conn.commit()


def test_repair_fixes_adopt_with_auto_provenance(tmp_path):
    """the user's 207-row case: source_kind='adopt' + provenance=
    'auto' (the v1.19.13/.14 regression). Repair must fix it."""
    db_path = tmp_path / "motif.db"
    _seed_drift(db_path, tmdb_id=1, source_kind="adopt",
                provenance="auto")
    from app.core.recovery_v55 import maybe_repair_adopt_provenance
    stats = maybe_repair_adopt_provenance(db_path)
    assert stats["adopt_provenance_fixed"] == 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        r = conn.execute(
            "SELECT source_kind, provenance FROM local_files "
            "WHERE tmdb_id=1"
        ).fetchone()
    assert r["source_kind"] == "adopt"
    assert r["provenance"] == "manual"


def test_repair_fixes_themerrdb_with_manual_provenance(tmp_path):
    """The inverse drift: source_kind='themerrdb' + provenance=
    'manual'. Could happen via v1.19.14 revert path before the
    v1.19.15 walker fix."""
    db_path = tmp_path / "motif.db"
    _seed_drift(db_path, tmdb_id=2, source_kind="themerrdb",
                provenance="manual")
    from app.core.recovery_v55 import maybe_repair_adopt_provenance
    stats = maybe_repair_adopt_provenance(db_path)
    assert stats["themerrdb_provenance_fixed"] == 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        r = conn.execute(
            "SELECT source_kind, provenance FROM local_files "
            "WHERE tmdb_id=2"
        ).fetchone()
    assert r["source_kind"] == "themerrdb"
    assert r["provenance"] == "auto"


def test_repair_leaves_correct_rows_alone(tmp_path):
    """Rows already in the correct (source_kind, provenance)
    pair must not be touched. Repair stats reflect 0 rowcount."""
    db_path = tmp_path / "motif.db"
    _seed_drift(db_path, tmdb_id=10, source_kind="themerrdb",
                provenance="auto")
    _seed_drift(db_path, tmdb_id=11, source_kind="adopt",
                provenance="manual")
    _seed_drift(db_path, tmdb_id=12, source_kind="url",
                provenance="manual")
    _seed_drift(db_path, tmdb_id=13, source_kind="upload",
                provenance="manual")
    from app.core.recovery_v55 import maybe_repair_adopt_provenance
    stats = maybe_repair_adopt_provenance(db_path)
    assert stats["adopt_provenance_fixed"] == 0
    assert stats["themerrdb_provenance_fixed"] == 0
    assert stats["url_provenance_fixed"] == 0
    assert stats["upload_provenance_fixed"] == 0
    assert stats["detected"] is False


def test_repair_handles_mixed_install(tmp_path):
    """Realistic mix: 3 broken adopt rows, 1 broken themerrdb row,
    2 correct rows. Repair must fix exactly 4 + leave 2 alone."""
    db_path = tmp_path / "motif.db"
    _seed_drift(db_path, tmdb_id=20, source_kind="adopt",
                provenance="auto")
    _seed_drift(db_path, tmdb_id=21, source_kind="adopt",
                provenance="auto")
    _seed_drift(db_path, tmdb_id=22, source_kind="adopt",
                provenance="auto")
    _seed_drift(db_path, tmdb_id=23, source_kind="themerrdb",
                provenance="manual")
    _seed_drift(db_path, tmdb_id=24, source_kind="adopt",
                provenance="manual")  # correct
    _seed_drift(db_path, tmdb_id=25, source_kind="themerrdb",
                provenance="auto")  # correct
    from app.core.recovery_v55 import maybe_repair_adopt_provenance
    stats = maybe_repair_adopt_provenance(db_path)
    assert stats["adopt_provenance_fixed"] == 3
    assert stats["themerrdb_provenance_fixed"] == 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = {r["tmdb_id"]: (r["source_kind"], r["provenance"])
                for r in conn.execute(
                    "SELECT tmdb_id, source_kind, provenance "
                    "FROM local_files ORDER BY tmdb_id"
                ).fetchall()}
    assert rows[20] == ("adopt", "manual")
    assert rows[21] == ("adopt", "manual")
    assert rows[22] == ("adopt", "manual")
    assert rows[23] == ("themerrdb", "auto")
    assert rows[24] == ("adopt", "manual")
    assert rows[25] == ("themerrdb", "auto")


def test_repair_marker_prevents_re_run(tmp_path):
    """After the marker is stamped, second invocation must be
    a no-op even if new drift is seeded between runs (operator
    can manually clear the marker key to re-trigger)."""
    db_path = tmp_path / "motif.db"
    _seed_drift(db_path, tmdb_id=30, source_kind="adopt",
                provenance="auto")
    from app.core.recovery_v55 import maybe_repair_adopt_provenance
    stats1 = maybe_repair_adopt_provenance(db_path)
    assert stats1["adopt_provenance_fixed"] == 1
    # Seed new drift to ensure the marker — not the empty WHERE —
    # is what stops the second run.
    _seed_drift(db_path, tmdb_id=31, source_kind="adopt",
                provenance="auto")
    stats2 = maybe_repair_adopt_provenance(db_path)
    assert stats2["adopt_provenance_fixed"] == 0, (
        "v1.19.15: marker must short-circuit re-runs"
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        r = conn.execute(
            "SELECT provenance FROM local_files WHERE tmdb_id=31"
        ).fetchone()
    assert r["provenance"] == "auto", (
        "v1.19.15: marker must prevent re-fixing — new drift "
        "after the marker stamp is operator's responsibility"
    )


def test_repair_no_op_on_empty_db(tmp_path):
    """Fresh install with no local_files rows — repair runs,
    stamps marker, exits cleanly."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    from app.core.recovery_v55 import maybe_repair_adopt_provenance
    stats = maybe_repair_adopt_provenance(db_path)
    assert stats["adopt_provenance_fixed"] == 0
    assert stats["themerrdb_provenance_fixed"] == 0
    assert stats["url_provenance_fixed"] == 0
    assert stats["upload_provenance_fixed"] == 0
    assert stats["detected"] is False
    # Marker IS stamped (unlike maybe_recover_lost_adopts which
    # doesn't stamp on empty-events case — this repair is
    # idempotent regardless of population).
    with sqlite3.connect(db_path) as conn:
        marker = conn.execute(
            "SELECT 1 FROM runtime_settings "
            "WHERE key = 'recovery_adopt_provenance_repair_done_at'"
        ).fetchone()
    assert marker is not None
