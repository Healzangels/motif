"""v1.22.73 (audit round 2, Batch C #2) — worker MED cluster.

(1) Sibling-hardlink short-circuit in _do_download unlinked the
existing staging theme.mp3 BEFORE a fallible link/copy (the v1.22.40
destroy-before-fail class, straggler): a copy fallback failing
(ENOSPC) after the unlink left local_files pointing at a missing file,
and the fall-through download's should_unlink stash never engaged.
Now: link to theme.mp3.sib.tmp, then atomic os.replace.

(2) The download-failure handler cancelled pending place jobs
section-wide with no edition discrimination — a failed Extended
download also cancelled the standard edition's place whose own
download succeeded (theme downloaded, never placed until the hourly
sweep). Now scoped via the v1.21.82 payload-JSON convention.

(3) _do_place/_do_place_collection READ the source local_files row
with an `edition_key IN (?, '')` fallback but WROTE every post-outcome
stamp keyed to the requested/landed edition — when the shared '' row
served, the stamps updated 0 rows: stale mismatch_state +
last_place_attempt_reason, and the hourly sweep's
`p.edition_key = lf.edition_key` join re-enqueued place jobs
indefinitely. Now all five local_files stamps key on the SOURCE row's
edition (_lf_edition / _coll_lf_edition); the placements INSERT keeps
the physical-folder key (the two writes intentionally diverge — see
test_v1_22_18's repointed pins).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


# ── (1) sibling link: no destroy-before-fail ─────────────────


def test_sibling_link_stages_via_temp_then_replace():
    i = WORKER_PY.index('"theme.mp3.sib.tmp"')
    block = WORKER_PY[i - 1200:i + 400]
    assert "os.replace(_sib_tmp, target_mp3)" in block
    # The pre-fix unlink-then-link ordering must be gone from the
    # sibling short-circuit (the idempotence stat-compare remains).
    assert "target_mp3.unlink()\n                                _safe_link(" \
        not in WORKER_PY
    assert "target_mp3.unlink()\n                            _safe_link(" \
        not in WORKER_PY


# ── (2) download-failure place-cancel edition scope ──────────


def _extract_cancel_sql() -> str:
    i = WORKER_PY.index("cancel any pending follow-up place job")
    j = WORKER_PY.index("UPDATE jobs SET status = 'cancelled'", i)
    k = WORKER_PY.index('"""', j)
    return WORKER_PY[j:k]


def test_cancel_sql_is_edition_scoped(tmp_path):
    """Extracted-verbatim SQL: a failed Extended download cancels ONLY
    the Extended place job — the '' and NULL-payload siblings (whose
    downloads succeeded) keep their place jobs."""
    sql = _extract_cancel_sql()
    assert "json_extract(payload, '$.edition_key')" in sql
    db = tmp_path / "m.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE jobs (id INTEGER PRIMARY KEY, job_type TEXT,"
            " media_type TEXT, tmdb_id INTEGER, section_id TEXT,"
            " status TEXT, payload TEXT, finished_at TEXT)")
        for payload in ('{"edition_key": "extended"}',
                        '{"edition_key": ""}', None):
            conn.execute(
                "INSERT INTO jobs (job_type, media_type, tmdb_id,"
                " section_id, status, payload) "
                "VALUES ('place','movie',100,'1','pending',?)", (payload,))
        conn.execute(sql, ("2026-06-11T00:00:00+00:00",
                           "movie", 100, "1", "extended"))
        rows = [r[0] for r in conn.execute(
            "SELECT status FROM jobs ORDER BY id").fetchall()]
    assert rows == ["cancelled", "pending", "pending"], rows


def test_cancel_sql_empty_edition_covers_null_payload(tmp_path):
    """A ''-edition failure cancels the '' AND legacy NULL-payload place
    jobs (COALESCE maps both to '') but not the tagged sibling."""
    sql = _extract_cancel_sql()
    db = tmp_path / "m.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE jobs (id INTEGER PRIMARY KEY, job_type TEXT,"
            " media_type TEXT, tmdb_id INTEGER, section_id TEXT,"
            " status TEXT, payload TEXT, finished_at TEXT)")
        for payload in ('{"edition_key": "extended"}',
                        '{"edition_key": ""}', None):
            conn.execute(
                "INSERT INTO jobs (job_type, media_type, tmdb_id,"
                " section_id, status, payload) "
                "VALUES ('place','movie',100,'1','pending',?)", (payload,))
        conn.execute(sql, ("2026-06-11T00:00:00+00:00",
                           "movie", 100, "1", ""))
        rows = [r[0] for r in conn.execute(
            "SELECT status FROM jobs ORDER BY id").fetchall()]
    assert rows == ["pending", "cancelled", "cancelled"], rows


# ── (3) stamps key on the SOURCE row's edition ───────────────


def test_lf_edition_derived_from_the_fallback_read():
    assert '_lf_edition = (local["edition_key"] if local is not None' \
        in WORKER_PY
    assert '_coll_lf_edition = (local["edition_key"] if local is not None' \
        in WORKER_PY


def test_collection_stamps_use_coll_lf_edition():
    """All four _do_place_collection local_files stamps (skip/backup,
    plex_rejected post-500, v1.24.46 over_ceiling pre-upload short-circuit,
    placed) key on _coll_lf_edition; the payload key survives only for
    non-local_files uses (placements fallback key, notification label)."""
    # def + 4 stamp uses (v1.24.46 added the over_ceiling short-circuit stamp).
    assert WORKER_PY.count("_coll_lf_edition") == 5
    i = WORKER_PY.index("def _do_place_collection")
    body = WORKER_PY[i:]
    for anchor in ("last_place_attempt_reason = 'backup_only'",
                   "last_place_attempt_reason = ? ",
                   "last_place_attempt_reason = 'plex_rejected:over_ceiling'",
                   "last_place_attempt_reason = 'placed'"):
        j = body.index(anchor)
        params = body[j:j + 500]
        assert "_coll_lf_edition" in params, anchor
