"""v0.51.233 — audit wave 6: placement-path churn + the loudness/UNDO wipe.

  1. reconcile_placement_paths built its "Plex still reports this folder" skip set from
     raw plex_items.folder_path (HOST paths) but compares it against
     placements.media_folder, which place_theme resolved through _candidate_local_paths to
     the CONTAINER path. On any install needing a translation the skip could never fire, so
     every placement looked "moved" on every enum: cancel in-flight place → rewrite
     media_folder to the host path → re-enqueue a forced place → which resolves back to the
     container path and INSERTs a SECOND placements row (media_folder is in the PK) → next
     enum deletes it and re-enqueues. Unbounded churn (v1.18.49 class).

  2. _record_local_file's ON CONFLICT unconditionally overwrote all 11 loudness/normalize
     columns, justified by "a re-download REPLACES the bytes". True for a real download,
     FALSE for the two paths that return without replacing anything (download_theme
     short-circuits when the expected mp3 exists; the sibling-hardlink branch passes no
     `conditioned`). A leveled row then reported raw while the file on disk was still
     gained and still carried mp3gain's APEv2 undo tag — and // UNDO refuses unless
     norm_state == 'normalized', so the original audio was no longer restorable.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from app.core.db import init_db
from app.core.plex_enum import _candidate_local_paths

REPO = Path(__file__).resolve().parent.parent
ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
NOW = "2026-07-27T00:00:00"


# ── 1: the skip set must speak both path dialects ────────────────────────────

def test_skip_set_indexes_container_translations():
    i = ENUM_PY.index("plex_paths_by_item: dict[tuple, set[str]] = {}")
    block = ENUM_PY[i:ENUM_PY.index("for r in rows:", i)]
    assert "_candidate_local_paths(" in block, (
        "the skip set must carry host->container translations or it can never match "
        "placements.media_folder, which is a CONTAINER path")


def test_translation_actually_bridges_the_two_dialects():
    """Behavioral: the exact Unraid shape motif hardcodes."""
    host = "/mnt/user/data/media/movies/Dune (2021)"
    container = "/data/media/movies/Dune (2021)"
    cands = {str(c) for c in _candidate_local_paths(host)}
    assert host != container, "premise: the two dialects differ"
    assert container in cands, "the container form must be reachable from the host form"


def test_untranslated_installs_are_unaffected():
    """A path needing no translation must still match itself — the fix only ADDS skips,
    so it can never create a false move where there wasn't one."""
    p = "/data/media/movies/Dune (2021)"
    assert p in {str(c) for c in _candidate_local_paths(p)}


# ── 2: identical bytes must keep their loudness + UNDO anchors ───────────────

def _upsert_tail() -> str:
    f = WORKER_PY.index("def _record_local_file(")
    i = WORKER_PY.index(
        "ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET", f)
    return WORKER_PY[i:WORKER_PY.index('"""', i)]


def test_norm_columns_are_gated_on_the_bytes_actually_changing():
    tail = _upsert_tail()
    assert "local_files.file_sha256 IS NOT excluded.file_sha256" in tail
    for col in ("norm_state", "norm_gain_db", "norm_orig_pcm_sha256", "loudness_i"):
        assert f"{col} = CASE WHEN" in tail, f"{col} must be conditional"


def test_no_op_redownload_preserves_leveled_state_but_real_one_clears_it():
    """The discriminator is the sha. Same bytes -> nothing was re-encoded or re-gained, so
    the existing state still describes this exact file; new bytes -> the v0.51.188 reason
    for clearing still applies (stale 'normalized' on fresh raw bytes would tell // UNDO to
    un-gain a file that was never gained)."""
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    c = sqlite3.connect(db)
    c.execute("PRAGMA foreign_keys=OFF")
    c.execute("INSERT INTO themes (id,media_type,tmdb_id,title,upstream_source,"
              " last_seen_sync_at,first_seen_sync_at) "
              "VALUES (1,'movie',120,'X','imdb',?,?)", (NOW, NOW))
    c.execute("INSERT INTO local_files (media_type,tmdb_id,section_id,edition_key,file_path,"
              " file_sha256,downloaded_at,source_video_id,norm_state,norm_gain_db,"
              " norm_orig_pcm_sha256,loudness_i) "
              "VALUES ('movie',120,'1','','m/t.mp3','SHA_A',?,'vid','normalized',-3.0,"
              " 'PCM_A',-18.5)", (NOW,))
    c.commit()
    tail = _upsert_tail()

    def replay(sha):
        c.execute(f"""INSERT INTO local_files (media_type,tmdb_id,section_id,edition_key,
                  file_path,file_sha256,file_size,downloaded_at,source_video_id,provenance,
                  source_kind,mismatch_state,loudness_i,loudness_tp,loudness_lra,
                  loudness_measured_at,loudness_measured_sha256,norm_state,norm_gain_db,
                  norm_target,norm_at,norm_orig_sha256,norm_orig_pcm_sha256)
                  VALUES ('movie',120,'1','','m/t.mp3',?,1,?,'vid','auto','themerrdb',NULL,
                  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) {tail}""",
                  (sha, NOW))
        c.commit()
        return c.execute("SELECT norm_state, norm_gain_db, norm_orig_pcm_sha256 "
                         "FROM local_files").fetchone()

    same = replay("SHA_A")
    assert same == ("normalized", -3.0, "PCM_A"), (
        "identical bytes must keep norm_state and the mp3gain UNDO anchors — else the "
        "file stays gained on disk while // UNDO refuses to restore it")
    changed = replay("SHA_B")
    assert changed[0] is None, "a genuine re-download must still clear stale norm state"
