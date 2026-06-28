"""v1.19.18 — backfill file_sha256 for local_files rows where
the v1.18.5 recovery walker left it NULL.

## Why this exists

v1.18.5's recovery walker INSERT (recovery_v55.py line ~410):

    INSERT OR IGNORE INTO local_files
      (..., file_size, downloaded_at, source_video_id,
       provenance, source_kind)
    VALUES (...)

Note the missing file_sha256 column — it was never populated,
defaulting to NULL. The bug-fix walkers that followed
(v1.19.13 + v1.19.14) only backfilled sha for rows they
ACTIVELY reclassified — 224 rows total. The other 578 rows
on the user's instance kept their NULL sha.

Consequences:
  * Future verification walkers can't operate on these rows
    without re-computing sha each time.
  * Drift detection (file content changed unexpectedly) can't
    distinguish "file intact" from "we never knew the sha".

## What's pinned

- `maybe_backfill_file_sha256(db_path, themes_dir)` exists.
- Walks local_files WHERE file_sha256 IS NULL OR ''.
- Streaming hash (1MB chunks) keeps RAM bounded.
- Skips rows where canonical is missing on disk.
- Marker `recovery_file_sha256_backfill_done_at_v1_19_18`.
- main.py wires walker on boot, gated on is_paths_ready.
- Behavioral: backfills NULL rows, leaves populated rows alone,
  skips missing files, marker prevents re-run.
"""
from __future__ import annotations

import hashlib
import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"
MAIN_PY = REPO / "app" / "main.py"


# ── Source-level pins ────────────────────────────────────────


def test_walker_function_exists():
    src = RECOVERY_PY.read_text()
    assert "def maybe_backfill_file_sha256(" in src
    fn = src[src.index("def maybe_backfill_file_sha256("):]
    assert "themes_dir" in fn[:200]


def test_walker_queries_null_sha_rows():
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_backfill_file_sha256(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "file_sha256 IS NULL OR file_sha256 = ''" in body


def test_walker_uses_streaming_hash():
    """Streaming hash (1MB chunks) keeps RAM bounded for large
    libraries — never load whole files at once."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_backfill_file_sha256(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "hashlib" in body
    assert "1024 * 1024" in body, (
        "v1.19.18: streaming hash with 1MB chunks per the "
        "v1.19.13 walker pattern"
    )


def test_walker_update_scopes_to_null_to_avoid_race():
    """The UPDATE must include file_sha256 IS NULL OR '' in WHERE
    so we don't overwrite a value written between the scan and
    the apply (e.g., worker download landed in interim)."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_backfill_file_sha256(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    upd_start = body.index("UPDATE local_files")
    upd_chunk = body[upd_start:upd_start + 600]
    assert "SET file_sha256 = ?" in upd_chunk
    assert "file_sha256 IS NULL OR file_sha256 = ''" in upd_chunk, (
        "v1.19.18: UPDATE WHERE must include NULL-guard so "
        "concurrent writes aren't clobbered"
    )


def test_walker_has_independent_marker():
    src = RECOVERY_PY.read_text()
    assert "recovery_file_sha256_backfill_done_at_v1_19_18" in src


# ── End-to-end behavioral tests ──────────────────────────────


def _seed(tmp_path: Path):
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T05:11:20+00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        conn.commit()
    return db_path, themes_dir


def _seed_row(
    db_path: Path, themes_dir: Path,
    *, tmdb_id: int = 1000, file_bytes: bytes = b"theme-content",
    file_sha256: str | None = None,
    file_present: bool = True,
):
    rel = f"movies/Movie {tmdb_id} (2020)/theme.mp3"
    if file_present:
        canonical = themes_dir / rel
        canonical.parent.mkdir(parents=True, exist_ok=True)
        canonical.write_bytes(file_bytes)
    ts = "2026-05-20T05:11:20+00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_sha256, downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES ('movie', ?, '1', ?, ?, ?, 'vid', "
            "        'auto', 'themerrdb')",
            (tmdb_id, rel, file_sha256, ts),
        )
        conn.commit()


def test_walker_backfills_null_sha_row(tmp_path):
    db_path, themes_dir = _seed(tmp_path)
    payload = b"my-theme-bytes"
    expected_sha = hashlib.sha256(payload).hexdigest()
    _seed_row(db_path, themes_dir,
              tmdb_id=1000, file_bytes=payload, file_sha256=None)
    from app.core.recovery_v55 import maybe_backfill_file_sha256
    stats = maybe_backfill_file_sha256(db_path, themes_dir)
    assert stats["candidates"] == 1
    assert stats["backfilled"] == 1
    assert stats["file_missing"] == 0
    with sqlite3.connect(db_path) as conn:
        sha = conn.execute(
            "SELECT file_sha256 FROM local_files WHERE tmdb_id=1000"
        ).fetchone()[0]
    assert sha == expected_sha


def test_walker_backfills_empty_sha_row(tmp_path):
    """Empty-string sha (not just NULL) must also be backfilled."""
    db_path, themes_dir = _seed(tmp_path)
    payload = b"another-theme"
    expected_sha = hashlib.sha256(payload).hexdigest()
    _seed_row(db_path, themes_dir,
              tmdb_id=2000, file_bytes=payload, file_sha256="")
    from app.core.recovery_v55 import maybe_backfill_file_sha256
    stats = maybe_backfill_file_sha256(db_path, themes_dir)
    assert stats["backfilled"] == 1
    with sqlite3.connect(db_path) as conn:
        sha = conn.execute(
            "SELECT file_sha256 FROM local_files WHERE tmdb_id=2000"
        ).fetchone()[0]
    assert sha == expected_sha


def test_walker_leaves_populated_sha_alone(tmp_path):
    """Rows with sha already set must not be touched."""
    db_path, themes_dir = _seed(tmp_path)
    existing_sha = "deadbeef" * 8
    _seed_row(db_path, themes_dir,
              tmdb_id=3000, file_sha256=existing_sha)
    from app.core.recovery_v55 import maybe_backfill_file_sha256
    stats = maybe_backfill_file_sha256(db_path, themes_dir)
    assert stats["candidates"] == 0
    assert stats["backfilled"] == 0
    with sqlite3.connect(db_path) as conn:
        sha = conn.execute(
            "SELECT file_sha256 FROM local_files WHERE tmdb_id=3000"
        ).fetchone()[0]
    assert sha == existing_sha


def test_walker_skips_missing_file(tmp_path):
    """File missing on disk → can't hash → skip + log."""
    db_path, themes_dir = _seed(tmp_path)
    _seed_row(db_path, themes_dir,
              tmdb_id=4000, file_sha256=None, file_present=False)
    from app.core.recovery_v55 import maybe_backfill_file_sha256
    stats = maybe_backfill_file_sha256(db_path, themes_dir)
    assert stats["candidates"] == 1
    assert stats["backfilled"] == 0
    assert stats["file_missing"] == 1
    with sqlite3.connect(db_path) as conn:
        sha = conn.execute(
            "SELECT file_sha256 FROM local_files WHERE tmdb_id=4000"
        ).fetchone()[0]
    assert sha is None


def test_walker_marker_prevents_re_run(tmp_path):
    db_path, themes_dir = _seed(tmp_path)
    _seed_row(db_path, themes_dir, tmdb_id=5000)
    from app.core.recovery_v55 import maybe_backfill_file_sha256
    stats1 = maybe_backfill_file_sha256(db_path, themes_dir)
    assert stats1["backfilled"] == 1
    # Seed new NULL row — should not be processed.
    _seed_row(db_path, themes_dir, tmdb_id=5001)
    stats2 = maybe_backfill_file_sha256(db_path, themes_dir)
    assert stats2["candidates"] == 0
    with sqlite3.connect(db_path) as conn:
        sha = conn.execute(
            "SELECT file_sha256 FROM local_files WHERE tmdb_id=5001"
        ).fetchone()[0]
    assert sha is None, (
        "v1.19.18: marker must short-circuit re-runs — new "
        "NULL rows after marker stamp are operator's job"
    )


def test_walker_no_op_when_no_null_rows(tmp_path):
    """No NULL rows → no marker stamp (so future drift can be
    caught)."""
    db_path, themes_dir = _seed(tmp_path)
    _seed_row(db_path, themes_dir,
              tmdb_id=6000, file_sha256="abc" * 21 + "x")
    from app.core.recovery_v55 import maybe_backfill_file_sha256
    stats = maybe_backfill_file_sha256(db_path, themes_dir)
    assert stats["candidates"] == 0
    with sqlite3.connect(db_path) as conn:
        marker = conn.execute(
            "SELECT 1 FROM runtime_settings "
            "WHERE key = "
            "'recovery_file_sha256_backfill_done_at_v1_19_18'"
        ).fetchone()
    assert marker is None


def test_walker_mixed_install(tmp_path):
    """Realistic mix: 3 NULL + 1 populated + 1 missing-file +
    1 empty-string. Walker handles all correctly."""
    db_path, themes_dir = _seed(tmp_path)
    _seed_row(db_path, themes_dir, tmdb_id=7000,
              file_bytes=b"a", file_sha256=None)
    _seed_row(db_path, themes_dir, tmdb_id=7001,
              file_bytes=b"b", file_sha256=None)
    _seed_row(db_path, themes_dir, tmdb_id=7002,
              file_bytes=b"c", file_sha256=None)
    _seed_row(db_path, themes_dir, tmdb_id=7003,
              file_sha256="x" * 64)  # already populated
    _seed_row(db_path, themes_dir, tmdb_id=7004,
              file_sha256=None, file_present=False)
    _seed_row(db_path, themes_dir, tmdb_id=7005,
              file_bytes=b"d", file_sha256="")
    from app.core.recovery_v55 import maybe_backfill_file_sha256
    stats = maybe_backfill_file_sha256(db_path, themes_dir)
    # 7003 not in candidates (populated). Others all in.
    assert stats["candidates"] == 5
    # 7000-7002 + 7005 hash successfully. 7004 missing.
    assert stats["backfilled"] == 4
    assert stats["file_missing"] == 1
    expected = {
        7000: hashlib.sha256(b"a").hexdigest(),
        7001: hashlib.sha256(b"b").hexdigest(),
        7002: hashlib.sha256(b"c").hexdigest(),
        7003: "x" * 64,
        7004: None,
        7005: hashlib.sha256(b"d").hexdigest(),
    }
    with sqlite3.connect(db_path) as conn:
        for tid, want in expected.items():
            got = conn.execute(
                "SELECT file_sha256 FROM local_files WHERE tmdb_id=?",
                (tid,),
            ).fetchone()[0]
            assert got == want, f"tmdb={tid}: expected {want}, got {got}"
