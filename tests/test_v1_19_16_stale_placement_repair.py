"""v1.19.16 — repair stale placements via enqueued place jobs.

Two classes of damage targeted:

## Class A: Plex doesn't know about the theme

plex_items.has_theme=0 but motif's placements row has
placed_at stamped. The v1.18.5 / v1.18.11 recovery walkers
INSERTed placements with plex_refreshed=1 hardcoded WITHOUT
actually calling Plex's `/library/metadata/{rk}/refresh` API.
Plex's metadata cache never re-enumerated → the `/theme`
serving endpoint returns empty even though the sidecar is on
disk.

the user's instance: 8 rows in this class.

## Class B: file missing from Plex folder

placement_kind in ('hardlink','copy') with media_folder set,
but media_folder/theme.mp3 doesn't exist on disk. Manual
cleanup or Plex-side rename removed the file. Canonical at
themes_dir still exists.

the user's instance: 1 row (tv/-44 "100 Years of Warner Bros.").

## The fix

Both classes converge on `INSERT INTO jobs (job_type='place',
force=true, ...)`. The worker handles both:
  - Class A: hardlink already in place → no-op for file IO,
    calls Plex refresh API → next plex_enum picks up has_theme=1.
  - Class B: file missing → re-hardlinks canonical to
    media_folder, calls Plex refresh.

Idempotent via marker `recovery_stale_placements_done_at_v1_19_17`.

## What's pinned

- `maybe_repair_stale_placements(db_path, themes_dir)` exists.
- Class A query joins plex_items, filters has_theme=0.
- Class B query stats media_folder/theme.mp3.
- Skips rows with in-flight place jobs (no duplicates).
- Skips rows where canonical is missing (out of scope).
- Dedupes by (mt, tmdb, sec) — one fix covers both classes.
- main.py wires the walker on boot, gated on is_paths_ready.
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


def test_walker_function_exists():
    """`maybe_repair_stale_placements(db_path, themes_dir)` must
    be importable from app.core.recovery_v55."""
    src = RECOVERY_PY.read_text()
    assert "def maybe_repair_stale_placements(" in src
    assert "themes_dir" in src[src.index(
        "def maybe_repair_stale_placements("
    ):]


def test_walker_queries_both_damage_classes():
    """Walker must surface both class A (plex.has_theme=0) and
    class B (file missing from media_folder)."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_repair_stale_placements(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "pi.has_theme = 0" in body, (
        "v1.19.16: Class A query must filter plex_items.has_theme=0"
    )
    assert 'theme.mp3' in body, (
        "v1.19.16: Class B check must stat media_folder/theme.mp3"
    )
    assert "is_file()" in body


def test_walker_skips_in_flight_jobs():
    """Don't pile up duplicate place jobs — must check for
    pending/running place job for each (mt, tmdb, sec) before
    enqueuing."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_repair_stale_placements(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "job_type = 'place'" in body
    assert "status IN ('pending', 'running')" in body


def test_walker_skips_canonical_missing():
    """If canonical is missing from themes_dir, the place job
    would fail (no source to link from). Walker must skip those
    rows — they need a different fix (download/adopt)."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_repair_stale_placements(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "skipped_canonical_missing" in body
    assert "themes_dir" in body


def test_walker_enqueues_force_place_jobs():
    """The enqueued place job must include force=true so the
    worker re-links even if the target file already exists
    (Class A scenario — file IS there, Plex just doesn't know).
    Without force, the worker's idempotent short-circuit might
    skip the Plex refresh.

    v1.19.17: also asserts kind=file is in the payload — without
    it, the worker uses the global default which (for installs
    with default_placement_method='api') routes through Plex API
    upload + deletes sidecars + creates duplicate placement rows.
    Repair operations should never CONVERT placement_kind, only
    re-establish the existing route. file/sidecar is always safe
    (idempotent re-hardlink) and works for orphans (no rk needed).
    """
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_repair_stale_placements(")
    next_def = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:next_def if next_def > 0 else len(src)]
    assert "INSERT INTO jobs" in body
    assert '"force":true' in body or "'force':true" in body
    assert '"kind":"file"' in body or "'kind':'file'" in body, (
        "v1.19.17: walker must pass kind=file in payload — "
        "otherwise global default (api) breaks orphans + "
        "creates duplicate placement rows on api-default installs"
    )
    assert "v1.19.17" in body, (
        "v1.19.17: job payload reason must include the current "
        "version so future debugging traces the source"
    )


def test_walker_has_independent_marker():
    """Marker key must include v1_19_16 suffix per the v1.18.11
    pattern. Independent so it runs on installs that have
    prior recovery markers."""
    src = RECOVERY_PY.read_text()
    assert "recovery_stale_placements_done_at_v1_19_17" in src


# ── End-to-end behavioral tests ──────────────────────────────


def _seed_install(tmp_path: Path):
    """Build a fresh DB + themes_dir layout."""
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    plex_root = tmp_path / "plex"
    plex_root.mkdir()
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
    return db_path, themes_dir, plex_root


def _seed_class_a_row(
    db_path: Path, themes_dir: Path, plex_root: Path,
    *, tmdb_id: int = 1000,
):
    """Class A: motif placed (hardlink exists at Plex folder),
    plex_items.has_theme=0 (Plex doesn't know)."""
    title_subdir = f"Movie {tmdb_id} (2020)"
    rel = f"movies/{title_subdir}/theme.mp3"
    canonical = themes_dir / rel
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.write_bytes(b"class-a-content")
    plex_folder = plex_root / "movies" / title_subdir
    plex_folder.mkdir(parents=True, exist_ok=True)
    plex_theme = plex_folder / "theme.mp3"
    plex_theme.touch()  # file exists (link not actually hardlinked in test)
    ts = "2026-05-20T05:11:20+00:00"
    rk = f"rk-{tmdb_id}"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "                    upstream_source, last_seen_sync_at, "
            "                    first_seen_sync_at) "
            "VALUES ('movie', ?, ?, '2020', 'themoviedb', ?, ?)",
            (tmdb_id, f"Movie {tmdb_id}", ts, ts),
        )
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "                        media_type, guid_tmdb, title, "
            "                        folder_path, has_theme, "
            "                        local_theme_file, "
            "                        first_seen_at, last_seen_at) "
            "VALUES (?, '1', 'movie', ?, ?, ?, "
            "        0, 1, ?, ?)",
            (rk, tmdb_id, f"Movie {tmdb_id}", str(plex_folder),
             ts, ts),
        )
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, "
            "                         section_id, file_path, "
            "                         downloaded_at, source_video_id, "
            "                         provenance, source_kind) "
            "VALUES ('movie', ?, '1', ?, ?, 'vid', "
            "        'auto', 'themerrdb')",
            (tmdb_id, rel, ts),
        )
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            "                        media_folder, placed_at, "
            "                        placement_kind, plex_rating_key, "
            "                        plex_refreshed, provenance) "
            "VALUES ('movie', ?, '1', ?, ?, 'hardlink', ?, 1, 'auto')",
            (tmdb_id, str(plex_folder), ts, rk),
        )
        conn.commit()
    return rk


def _seed_class_b_row(
    db_path: Path, themes_dir: Path, plex_root: Path,
    *, tmdb_id: int = 2000,
):
    """Class B: placement claims hardlink but file is missing
    from Plex folder. Canonical IS present at themes_dir."""
    title_subdir = f"Movie {tmdb_id} (2021)"
    rel = f"movies/{title_subdir}/theme.mp3"
    canonical = themes_dir / rel
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.write_bytes(b"class-b-content")
    plex_folder = plex_root / "movies" / title_subdir
    plex_folder.mkdir(parents=True, exist_ok=True)
    # NO theme.mp3 in plex_folder — this is the damage.
    ts = "2026-05-20T05:11:20+00:00"
    rk = f"rk-{tmdb_id}"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "                    upstream_source, last_seen_sync_at, "
            "                    first_seen_sync_at) "
            "VALUES ('movie', ?, ?, '2021', 'themoviedb', ?, ?)",
            (tmdb_id, f"Movie {tmdb_id}", ts, ts),
        )
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "                        media_type, guid_tmdb, title, "
            "                        folder_path, has_theme, "
            "                        local_theme_file, "
            "                        first_seen_at, last_seen_at) "
            "VALUES (?, '1', 'movie', ?, ?, ?, "
            "        1, 0, ?, ?)",  # has_theme=1 (stale)
            (rk, tmdb_id, f"Movie {tmdb_id}", str(plex_folder),
             ts, ts),
        )
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, "
            "                         section_id, file_path, "
            "                         downloaded_at, source_video_id, "
            "                         provenance, source_kind) "
            "VALUES ('movie', ?, '1', ?, ?, 'vid', "
            "        'manual', 'adopt')",
            (tmdb_id, rel, ts),
        )
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            "                        media_folder, placed_at, "
            "                        placement_kind, plex_rating_key, "
            "                        plex_refreshed, provenance) "
            "VALUES ('movie', ?, '1', ?, ?, 'hardlink', ?, 1, 'manual')",
            (tmdb_id, str(plex_folder), ts, rk),
        )
        conn.commit()
    return rk


def test_walker_enqueues_for_class_a_row(tmp_path):
    """Class A: Plex doesn't know about the theme → walker
    enqueues a force-place job."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    _seed_class_a_row(db_path, themes_dir, plex_root, tmdb_id=1000)
    from app.core.recovery_v55 import maybe_repair_stale_placements
    stats = maybe_repair_stale_placements(db_path, themes_dir)
    assert stats["class_a_candidates"] == 1
    assert stats["enqueued"] == 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        job = conn.execute(
            "SELECT job_type, media_type, tmdb_id, section_id, "
            "       payload, status "
            "FROM jobs WHERE tmdb_id = 1000"
        ).fetchone()
    assert job["job_type"] == "place"
    assert job["status"] == "pending"
    assert '"force":true' in job["payload"]
    assert '"kind":"file"' in job["payload"], (
        "v1.19.17: enqueued payload must force file/sidecar route"
    )
    assert "v1.19.17" in job["payload"]


def test_walker_enqueues_for_class_b_row(tmp_path):
    """Class B: file missing from Plex folder → walker enqueues
    a force-place job (worker will re-hardlink)."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    _seed_class_b_row(db_path, themes_dir, plex_root, tmdb_id=2000)
    from app.core.recovery_v55 import maybe_repair_stale_placements
    stats = maybe_repair_stale_placements(db_path, themes_dir)
    assert stats["class_b_candidates"] == 1
    assert stats["enqueued"] == 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        job = conn.execute(
            "SELECT job_type, status, payload "
            "FROM jobs WHERE tmdb_id = 2000"
        ).fetchone()
    assert job["job_type"] == "place"
    assert job["status"] == "pending"


def test_walker_dedupes_row_hitting_both_classes(tmp_path):
    """A row could (in pathological cases) be both Class A AND
    Class B. Walker must enqueue ONE job, not two."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    # Seed Class B (file missing) but ALSO set has_theme=0
    # (Class A).
    _seed_class_b_row(db_path, themes_dir, plex_root, tmdb_id=3000)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE plex_items SET has_theme = 0 WHERE guid_tmdb = 3000"
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_repair_stale_placements
    stats = maybe_repair_stale_placements(db_path, themes_dir)
    assert stats["class_a_candidates"] == 1
    assert stats["class_b_candidates"] == 1
    assert stats["skipped_duplicate_target"] == 1
    assert stats["enqueued"] == 1, (
        "v1.19.16: dedup must collapse same-target into one job"
    )


def test_walker_skips_in_flight_place_job(tmp_path):
    """If a place job already exists (pending/running), don't
    enqueue another."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    _seed_class_a_row(db_path, themes_dir, plex_root, tmdb_id=4000)
    # Pre-seed an in-flight place job.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, "
            "                  section_id, payload, status, "
            "                  created_at, next_run_at) "
            "VALUES ('place', 'movie', 4000, '1', '{}', 'pending', "
            "        '2026-05-25T00:00:00', '2026-05-25T00:00:00')"
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_repair_stale_placements
    stats = maybe_repair_stale_placements(db_path, themes_dir)
    assert stats["class_a_candidates"] == 1
    assert stats["enqueued"] == 0, (
        "v1.19.16: in-flight job must block dup enqueue"
    )
    assert stats["skipped_job_in_flight"] == 1
    with sqlite3.connect(db_path) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE tmdb_id=4000"
        ).fetchone()[0]
    assert n == 1, "exactly one job for this row, not two"


def test_walker_skips_missing_canonical(tmp_path):
    """If canonical is missing from themes_dir, the place job
    would fail. Walker must skip those rows (different damage
    class — needs download/adopt to recover)."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    _seed_class_a_row(db_path, themes_dir, plex_root, tmdb_id=5000)
    # Delete the canonical file.
    (themes_dir / "movies" / "Movie 5000 (2020)" / "theme.mp3").unlink()
    from app.core.recovery_v55 import maybe_repair_stale_placements
    stats = maybe_repair_stale_placements(db_path, themes_dir)
    assert stats["class_a_candidates"] == 1
    assert stats["enqueued"] == 0
    assert stats["skipped_canonical_missing"] == 1


def test_walker_marker_prevents_re_run(tmp_path):
    """Second invocation after marker stamp must be a no-op
    even if new stale rows appear between runs."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    _seed_class_a_row(db_path, themes_dir, plex_root, tmdb_id=6000)
    from app.core.recovery_v55 import maybe_repair_stale_placements
    stats1 = maybe_repair_stale_placements(db_path, themes_dir)
    assert stats1["enqueued"] == 1
    # Seed new stale row — marker should prevent it from being
    # processed.
    _seed_class_a_row(db_path, themes_dir, plex_root, tmdb_id=6001)
    stats2 = maybe_repair_stale_placements(db_path, themes_dir)
    assert stats2["class_a_candidates"] == 0, (
        "v1.19.16: marker must short-circuit re-runs"
    )
    assert stats2["enqueued"] == 0


def test_walker_no_op_when_no_stale_placements(tmp_path):
    """Healthy install — no stale placements. Walker must NOT
    stamp marker so future drift can be caught."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    # Seed a healthy row (has_theme=1, file present).
    rk = _seed_class_a_row(db_path, themes_dir, plex_root, tmdb_id=7000)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE plex_items SET has_theme = 1 WHERE rating_key = ?",
            (rk,),
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_repair_stale_placements
    stats = maybe_repair_stale_placements(db_path, themes_dir)
    assert stats["enqueued"] == 0
    assert stats["class_a_candidates"] == 0
    with sqlite3.connect(db_path) as conn:
        marker = conn.execute(
            "SELECT 1 FROM runtime_settings "
            "WHERE key = 'recovery_stale_placements_done_at_v1_19_17'"
        ).fetchone()
    assert marker is None, (
        "v1.19.16: marker must NOT be stamped when no work was "
        "needed — future drift should re-trigger the walker"
    )


def test_walker_excludes_plex_upload_placements(tmp_path):
    """plex_upload placements use media_folder='' (no on-disk
    file in a Plex folder). Class B check must exclude them."""
    db_path, themes_dir, plex_root = _seed_install(tmp_path)
    ts = "2026-05-20T05:11:20+00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "                    upstream_source, last_seen_sync_at, "
            "                    first_seen_sync_at) "
            "VALUES ('collection', 8000, 'X', '2020', 'themoviedb', ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "                        media_type, guid_tmdb, title, "
            "                        folder_path, has_theme, "
            "                        local_theme_file, "
            "                        first_seen_at, last_seen_at) "
            "VALUES ('rk-8000', '1', 'collection', 8000, 'X', '', "
            "        1, 0, ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            "                        media_folder, placed_at, "
            "                        placement_kind, plex_rating_key, "
            "                        plex_refreshed, provenance) "
            "VALUES ('collection', 8000, '1', '', ?, "
            "        'plex_upload', 'rk-8000', 1, 'auto')",
            (ts,),
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_repair_stale_placements
    stats = maybe_repair_stale_placements(db_path, themes_dir)
    # plex_upload row has has_theme=1 + media_folder='' so neither
    # Class A nor Class B fires.
    assert stats["class_a_candidates"] == 0
    assert stats["class_b_candidates"] == 0
