"""v0.51.167: CANONICAL HEALTH — find + repair local_files rows whose canonical
theme.mp3 is gone (or 0-byte) from motif's storage, and decide how each is fixed.

The loudness audit (loudness_audit.py) surfaced a real cohort: ~14 library items
whose canonical file_path pointed at a missing file — ffmpeg measure returned
rc=254 "No such file or directory". A vanished / corrupt canonical is separate
from the loudness feature; it's a storage-health problem (an external sweep,
a failed download that left a 0-byte stub, a moved file, a lost mount that never
came back). motif already STAMPS the condition — `verify_canonical_health`
(plex_enum.py) writes `local_files.canonical_present = 0` for a row whose
`themes_dir/file_path` is missing/0-byte, and the library paints a red DL dot +
`dl_pills=broken` filter for it. What was missing is the OPERATOR-facing repair:
an aggregated list of the broken rows, split by how each can be fixed.

Repair decision (per the download worker's own URL resolution, worker.py:1672 —
`override.youtube_url or theme.youtube_url`):

  • RE-DOWNLOADABLE — the row's recorded source IS a re-fetchable URL, so a fresh
    download restores it byte-for-byte:
      - source_kind='url'  → a user_overrides URL is the source (always present +
        NOT NULL for a U row).
      - source_kind='themerrdb' / NULL(legacy) → the ThemerrDB `youtube_url` is the
        source; re-downloadable iff it's non-empty AND the item is genuinely
        TDB-tracked (upstream_source != 'plex_orphan').

  • CANONICAL MISSING — no re-fetchable URL; the operator must re-place manually
    (SET URL / UPLOAD MP3 / RESTORE FROM PLEX from the INFO card):
      - source_kind IN ('upload','adopt','plex_cloud') → the bytes came from a
        direct upload / adopted sidecar / Plex-cloud backup, NOT a URL motif can
        re-fetch. Re-downloading TDB over these would REPLACE the operator's chosen
        content with something different — so they are never auto-repaired here.
      - any re-download-eligible row whose resolved URL is empty.

Everything is edition-scoped: local_files' PK is
(media_type, tmdb_id, section_id, edition_key) and every query + the re-download
enqueue carry edition_key, so one edition's broken canonical never re-downloads
into a sibling edition's folder (v1.21.x edition-isolation rule).
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
import weakref
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import nullcontext
from pathlib import Path

log = logging.getLogger("motif.canonical_health")

# The download pipeline records no re-fetchable URL for these — their bytes are a
# direct upload / adopted sidecar / Plex-cloud backup. A broken canonical for one
# of these is surfaced for manual re-place, never auto-re-downloaded (a TDB URL on
# the shared themes row is NOT this row's source — re-downloading would swap the
# operator's content). Mirrors the api.py canonical-danger grouping.
_NO_URL_SOURCE_KINDS = ("upload", "adopt", "plex_cloud")


def classify_repair(r) -> str:
    """'redownload' if the row's recorded source is a re-fetchable URL, else
    'canonical_missing' (surface for manual re-place). See the module docstring
    for the full rule; keyed off source_kind + the worker's URL resolution
    (r["override_url"], resolved in _broken_rows) so it agrees with what a
    re-download would actually do."""
    if r["source_kind"] in _NO_URL_SOURCE_KINDS:
        return "canonical_missing"
    # source_kind is 'url', 'themerrdb', or NULL(legacy). A live override URL is
    # always re-fetchable (a U row); otherwise fall back to the TDB URL, but only
    # for a genuinely TDB-tracked item (a plex_orphan carries no meaningful TDB URL).
    if r["override_url"]:
        return "redownload"
    if r["upstream_source"] != "plex_orphan" and (r["tdb_url"] or "").strip():
        return "redownload"
    return "canonical_missing"


def _broken_rows(conn) -> list:
    """local_files rows CONFIRMED broken (canonical_present = 0, stamped by
    verify_canonical_health). NULL (never-verified) rows are excluded — we only
    surface a confirmed-missing canonical, never an unverified one. Carries the
    themes fields the classifier needs + a has_live_placement hint (a surviving
    Plex-folder copy → RESTORE FROM PLEX is available for a manual row)."""
    return conn.execute(
        "SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key, "
        "       lf.file_path, lf.source_kind, lf.file_size, lf.file_sha256, "
        # v0.51.342: the worker's section-then-global override (worker.py:1646) in the row — was 2 SELECTs a row.
        "       COALESCE((SELECT uo.youtube_url FROM user_overrides uo "
        "                 WHERE uo.media_type = lf.media_type AND uo.tmdb_id = lf.tmdb_id "
        "                   AND uo.section_id = lf.section_id AND uo.edition_key = lf.edition_key), "
        "                (SELECT uo.youtube_url FROM user_overrides uo "
        "                 WHERE uo.media_type = lf.media_type AND uo.tmdb_id = lf.tmdb_id "
        "                   AND uo.section_id = '' AND uo.edition_key = lf.edition_key)) AS override_url, "
        "       t.title, t.year, t.youtube_url AS tdb_url, t.upstream_source, "
        "       EXISTS(SELECT 1 FROM placements p "
        "              WHERE p.media_type = lf.media_type AND p.tmdb_id = lf.tmdb_id "
        "                AND p.section_id = lf.section_id "
        "                AND p.edition_key = lf.edition_key "
        "                AND p.theme_present = 1) AS has_live_placement, "
        "       COALESCE(ps.is_anime, 0) AS is_anime "
        "FROM local_files lf "
        "LEFT JOIN themes t "
        "  ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id "
        "LEFT JOIN plex_sections ps ON ps.section_id = lf.section_id "
        "WHERE lf.canonical_present = 0 "
        "ORDER BY lf.media_type, t.title, lf.tmdb_id, lf.section_id, lf.edition_key"
    ).fetchall()


def _entry(r) -> dict:
    """The report payload for one broken row — identity + the INFO-card deep-link
    fields (media_type/tmdb_id/section_id) the operator clicks to re-place."""
    return {
        "media_type": r["media_type"],
        "tmdb_id": r["tmdb_id"],
        "section_id": r["section_id"],
        "edition_key": r["edition_key"],
        "title": r["title"] or f'{r["media_type"]}/{r["tmdb_id"]}',
        "year": r["year"],
        "source_kind": r["source_kind"] or "",
        "file_path": r["file_path"],
        "is_anime": bool(r["is_anime"]),
        # v0.51.311 (review): anime rows deep-link to /anime, not /tv — the
        # .308/.309 routing fix reached the inbox + /queue producers only.
    }


def _sidecar_survives(media_folder) -> bool:
    """Stat, not the stored flag: a database restored onto a new box carries the
    OLD box's theme_present stamps, and a fresh install has none — the bulk
    restores from what is actually there, so the report must count the same."""
    if not media_folder:
        return False
    from .plex_enum import _candidate_local_paths
    for cand in _candidate_local_paths(media_folder):
        try:
            p = cand / "theme.mp3"
            if p.is_file() and p.stat().st_size > 0:
                return True
        except OSError:
            continue
    return False


def broken_canonical_report(conn, themes_dir: "Path | None" = None, *,
                            plex_available: bool = False) -> dict:
    """Read-only: every confirmed-broken canonical, split into the re-downloadable
    set (REPAIR ALL can fix these) and the canonical-missing set (manual re-place).
    Writes nothing — the /admin/canonical-health page renders this.
    v0.51.337: every broken row also says whether Plex still holds a copy
    (restorable_from_plex — a surviving sidecar or a plex_upload placement), and
    with a themes_dir the report adds the CHANGED rows (present, wrong size)."""
    rows = _broken_rows(conn)
    # v0.51.342: every broken row's placements in one SELECT — the report was 2.3 SELECTs per broken row.
    placements = _broken_placements(conn)
    redownloadable: list[dict] = []
    canonical_missing: list[dict] = []
    restorable = 0
    for r in rows:
        entry = _entry(r)
        p, sidecar = _pick_placement(placements.get(_row_key(r), []))
        entry["has_live_placement"] = bool(r["has_live_placement"])
        # v0.51.339: restore_from_plex's own gates — the stored flag and a bare kind promised rows the bulk skips.
        store = bool(plex_available and p is not None and str(p["plex_rating_key"] or "").isdigit()
                     and (p["placement_kind"] == "plex_upload" or not p["media_folder"]))
        entry["plex_copy"] = "sidecar" if sidecar else "store" if store else None
        if entry["plex_copy"]:
            restorable += 1
        if classify_repair(r) == "redownload":
            redownloadable.append(entry)
        else:
            canonical_missing.append(entry)
    changed = changed_canonicals(conn, themes_dir) if themes_dir else []
    # v0.51.342: BROKEN and CHANGED are the last check's findings — the page says how old they are.
    tracked, never, oldest, newest = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(canonical_health_checked_at IS NULL), 0), "
        "       MIN(canonical_health_checked_at), MAX(canonical_health_checked_at) "
        "FROM local_files WHERE file_path IS NOT NULL AND file_path != ''").fetchone()
    return {
        "checked": {"tracked": tracked, "never": never, "oldest": oldest, "newest": newest},
        "broken": len(rows),
        "redownloadable": redownloadable,
        "canonical_missing": canonical_missing,
        "changed": changed,
        "counts": {
            "broken": len(rows),
            "redownloadable": len(redownloadable),
            "canonical_missing": len(canonical_missing),
            "restorable_from_plex": restorable,
            "changed": len(changed),
        },
    }


def enqueue_canonical_repairs(conn) -> dict:
    """Re-download every re-downloadable broken canonical (auto-place + force so
    the fresh bytes overwrite whatever remains — same shape as the /redownload
    endpoint). Edition-scoped: each row re-downloads only into its own section +
    edition. Canonical-missing (no-URL) rows are counted as `surfaced`, never
    touched. Returns a summary. Caller wraps this in a transaction."""
    from .sync import _enqueue_download

    rows = _broken_rows(conn)
    repaired_rows = 0
    enqueued_sections = 0
    surfaced = 0
    no_op = 0
    for r in rows:
        if classify_repair(r) != "redownload":
            surfaced += 1
            continue
        n = _enqueue_download(
            conn,
            media_type=r["media_type"],
            tmdb_id=r["tmdb_id"],
            reason="manual",
            auto_place=True,
            force_place=True,
            only_section_id=r["section_id"],
            edition_key=r["edition_key"],
        )
        if n:
            repaired_rows += 1
            enqueued_sections += n
        else:
            # _enqueue_download returned 0 — item isn't in an included Plex section
            # yet, or an identical download is already queued (dedup). Neither is an
            # error; the operator just sees it stay broken until Plex/enum catches up.
            no_op += 1
    log.info(
        "canonical repair: %d re-downloaded (%d sections), %d surfaced (no URL), "
        "%d no-op of %d broken",
        repaired_rows, enqueued_sections, surfaced, no_op, len(rows),
    )
    return {
        "broken": len(rows),
        "repaired_rows": repaired_rows,
        "enqueued_sections": enqueued_sections,
        "surfaced": surfaced,
        "no_op": no_op,
    }


# ── v0.51.337: RESTORE FROM PLEX + CHANGED (feature D tag 3, the themes check) ──
# docs/specs/BACKUP_BUNDLE_SPEC.md § 3: after a restore (or any time), the
# themes directory is checked against what the database expects. The missing
# rows already split into re-downloadable vs canonical-missing above; this
# adds the other way a canonical can come back without a source URL — from
# Plex: the sidecar still in the Plex folder (a hardlink/copy placement
# survives motif's copy being unlinked), or Plex's own store for a
# plex_upload placement (the v0.51.171 fetch_theme_bytes path). And the
# third state a bundle's census implies: a file that is present but no
# longer the size the database recorded.

_PLACEMENT_ORDER = (
    # v0.51.341: DESC sorts NULL last — a verified-missing (0) row outranked an unverified one.
    "CASE WHEN theme_present = 1 THEN 0 "
    "WHEN theme_present IS NULL THEN 1 ELSE 2 END, placed_at DESC, "
    # v0.51.342: a final tie-break so the batched and the per-row picks agree.
    "media_folder"
)

_PLACEMENT_SQL = (
    "SELECT media_folder, placement_kind, plex_rating_key, theme_present "
    "FROM placements WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
    "AND edition_key = ? ORDER BY " + _PLACEMENT_ORDER
)

# v0.51.342: every broken row's placements in one SELECT, each row's in _PLACEMENT_SQL's own order.
_BROKEN_PLACEMENTS_SQL = (
    "SELECT media_type, tmdb_id, section_id, edition_key, "
    "       media_folder, placement_kind, plex_rating_key, theme_present "
    "FROM placements p WHERE EXISTS (SELECT 1 FROM local_files lf "
    "  WHERE lf.canonical_present = 0 AND lf.media_type = p.media_type "
    "    AND lf.tmdb_id = p.tmdb_id AND lf.section_id = p.section_id "
    "    AND lf.edition_key = p.edition_key) "
    "ORDER BY media_type, tmdb_id, section_id, edition_key, " + _PLACEMENT_ORDER
)


def _row_key(r) -> tuple:
    return (r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"])


def _broken_placements(conn) -> dict:
    """{row key: that broken row's placements, in pick order}."""
    by_row: dict[tuple, list] = {}
    for p in conn.execute(_BROKEN_PLACEMENTS_SQL).fetchall():
        by_row.setdefault(_row_key(p), []).append(p)
    return by_row


def _placement_for(conn, r):
    """(placement, sidecar_survives) for one row — see _pick_placement."""
    return _pick_placement(conn.execute(
        _PLACEMENT_SQL, (r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"]),
    ).fetchall())


def _pick_placement(rows):
    """(placement, sidecar_survives): the first placement whose sidecar is still
    on disk, else the first Plex's store can serve, else the first by theme_present
    then recency; (None, False) if none."""
    # v0.51.339: the PK includes media_folder — fetchone() took the dead folder while a sibling held the sidecar.
    for p in rows:
        if _sidecar_survives(p["media_folder"]):
            return p, True
    # v0.51.339: plex_upload inserts leave theme_present NULL (sorts last) — a dead folder row hid the store.
    store = next((p for p in rows if (p["placement_kind"] == "plex_upload" or not p["media_folder"])
                  and str(p["plex_rating_key"] or "").isdigit()), None)
    return (store or (rows[0] if rows else None)), False


# v0.51.342: whole-file GETs of up to ~10 MB — kept below PROBE_MAX_WORKERS (6), which fetches 2 KB ranges.
RESTORE_PLEX_WORKERS = 4
# v0.51.342: reasons that mean Plex did not answer — 401/403/404/500 and no_theme_entry are answers.
_PLEX_NO_ANSWER = ("plex_themes:transport", "plex_fetch:transport", "plex_themes:502", "plex_themes:503",
                   "plex_themes:504", "plex_fetch:502", "plex_fetch:503", "plex_fetch:504")
_PLEX_BACKOFF_BASE_S = 0.5  # v0.51.342: first backoff after a no-answer
_PLEX_BACKOFF_CAP_S = 8.0  # v0.51.342: the backoff never sleeps longer than this
_PLEX_TRIP_AFTER = 8  # v0.51.342: consecutive no-answers before the run may stop asking
_PLEX_TRIP_WINDOW_S = 60.0  # v0.51.342: …and only this long after the first — a Plex restart must not end the run
_PLEX_CANCEL_POLL_S = 0.25  # v0.51.342: how often a backoff asks whether the run was cancelled


def _run_conn(db_path: Path, conn):
    """The caller's connection when it passed one, else a fresh one for this write."""
    from .db import get_conn
    return nullcontext(conn) if conn is not None else get_conn(db_path)


# v0.51.342: every canonical writer holds its path's lock from guard to stamp — two restores of one row raced its tmp.
_CANON_WRITE_LOCKS: "weakref.WeakValueDictionary[str, threading.Lock]" = weakref.WeakValueDictionary()
_CANON_WRITE_LOCKS_GUARD = threading.Lock()


def _canonical_write_lock(canonical: Path) -> threading.Lock:
    """The one lock for this canonical path — casefolded, as the bulk's shared paths are."""
    key = str(canonical).casefold()
    with _CANON_WRITE_LOCKS_GUARD:
        lock = _CANON_WRITE_LOCKS.get(key)
        if lock is None:
            lock = _CANON_WRITE_LOCKS[key] = threading.Lock()
        return lock


# v0.51.344: exit's deadline closes publishing — a fetch answering after it started a write the interpreter froze
_PUBLISH_LOCK = threading.Lock()
_PUBLISH_CLOSED = threading.Event()


def close_publishing(timeout: float) -> bool:
    """No canonical write (store or sidecar) starts after this; True once none is mid-write, False if one still was after `timeout` s."""
    _PUBLISH_CLOSED.set()
    if not _PUBLISH_LOCK.acquire(timeout=max(0.0, timeout)):
        return False
    _PUBLISH_LOCK.release()
    return True


# v0.51.344: section_id -> (backlog key, keys, paths) of its queued downloads — the bulk re-read the backlog per row
_IN_FLIGHT_DOWNLOADS: dict = {}


def _download_in_flight(conn, r) -> bool:
    """A queued or running download that writes this row's canonical path — its own, or another row's on that path."""
    from .canonical import canonical_theme_rel
    # v0.51.344: the backlog's own key, a seek each — a job queued moves MAX(id), one ended its finished_at stamp (every
    # end-writer sets it; none re-pends an ended job), one started or ended the running count. data_version was blind
    # to this connection's queueing and moved on every unrelated commit; a COUNT of the queue is a read per row (R2-F9)
    # v0.51.344: and the database itself, no connection pinned — two databases on one backlog shape shared a memo
    key = tuple(conn.execute(
        "SELECT (SELECT file FROM pragma_database_list WHERE name = 'main'), "
        "       (SELECT MAX(id) FROM jobs), "
        "       (SELECT MAX(finished_at) FROM jobs WHERE job_type = 'download' AND status IN ('done', 'failed', 'cancelled')), "
        "       (SELECT COUNT(*) FROM jobs WHERE job_type = 'download' AND status = 'running')").fetchone())
    memo = _IN_FLIGHT_DOWNLOADS.get(r["section_id"])
    if memo is None or memo[0] != key:
        keys, paths = set(), set()
        for j in conn.execute(
            "SELECT j.media_type, j.tmdb_id, t.title, t.year, ps.themes_subdir, "
            "COALESCE(CASE WHEN json_valid(j.payload) THEN json_extract(j.payload, '$.edition_key') END, '') AS edition_key "
            "FROM jobs j LEFT JOIN themes t ON t.media_type = j.media_type AND t.tmdb_id = j.tmdb_id "
            "LEFT JOIN plex_sections ps ON ps.section_id = j.section_id "
            "WHERE j.job_type = 'download' AND j.status IN ('pending', 'running') AND j.section_id = ?",
            (r["section_id"],),
        ).fetchall():
            keys.add((j["media_type"], j["tmdb_id"], j["edition_key"]))
            if j["title"] is not None and j["themes_subdir"]:
                # v0.51.344: the path the download writes — an edition row on the untagged folder and a same-title tmdb share one theme.mp3
                paths.add(str(Path(canonical_theme_rel(j["media_type"], j["themes_subdir"], j["title"], j["year"],
                                                       j["edition_key"])) / "theme.mp3").casefold())
        memo = _IN_FLIGHT_DOWNLOADS[r["section_id"]] = (key, keys, paths)  # v0.51.344: no connection kept — none to pin
    return (r["media_type"], r["tmdb_id"], r["edition_key"]) in memo[1] or str(r["file_path"]).casefold() in memo[2]


def _stamp_restored(db_path: Path, r, canonical: Path, *, prior_size, prior_sha,
                    placement_kind: str | None, conn=None, known: tuple | None = None, reread: bool = True) -> int:
    """After bytes landed at the canonical path: re-hash, stamp size / sha /
    downloaded_at / canonical_present=1, and record the placement kind when the
    restore changed it (a hardlink that had to fall back to a copy)."""
    from .canonical import hash_file
    from .db import transaction
    from .events import now_iso
    from .worker import _cond_columns
    rehash_failed = False  # v0.51.338: unknown bytes were written — clear the anchors rather than keep them
    if not reread:
        size, sha = known  # v0.51.344: the caller hashed the inode that landed — reading it again was the R2-F7 cost
    else:
        try:
            size = canonical.stat().st_size
            sha, _ = hash_file(canonical)  # v0.51.344: the shared streaming hash; size stays the stat's, as before
        except OSError as e:
            log.warning("restore: re-hash failed for %s/%s section=%s: %s — %s", r["media_type"], r["tmdb_id"],
                        r["section_id"], e, "stamping the published size/sha" if known else "keeping prior size/sha")
            # v0.51.344: `known` is the length and sha of the bytes just published — the prior values wrote the pre-restore stamp back over the incoming one
            size, sha = known if known else (prior_size, prior_sha)
            rehash_failed = True
    # v0.51.338: new bytes void the loudness/norm anchors (revisions.py rule) — else // UNDO un-gains raw bytes.
    new_bytes = rehash_failed or sha != prior_sha
    # v0.51.342: the bulk passes its one connection — a connect + 3 PRAGMAs per restored row was the P3 cost.
    with _run_conn(db_path, conn) as c, transaction(c):
        # v0.51.344: the rows reached — an edition swap mid-run re-keyed the row, and a stamp by the old key is no restore (R2-F10)
        n = c.execute(
            "UPDATE local_files SET file_size = ?, file_sha256 = ?, downloaded_at = ?, "
            "canonical_present = 1"
            # v0.51.342: a kept size, or a stamped size that moved, may not be the bytes on disk — CHANGED re-reads it
            + (", canonical_changed_candidate = 1" if (rehash_failed and not known) or size != prior_size else "")
            + (", loudness_i=?, loudness_tp=?, loudness_lra=?, loudness_measured_at=?, "
               "loudness_measured_sha256=?, norm_state=?, norm_gain_db=?, norm_target=?, "
               "norm_at=?, norm_orig_sha256=?, norm_orig_pcm_sha256=?, norm_plex_entry_uri = NULL"
               if new_bytes else "")
            + " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? AND edition_key = ?",
            (size, sha, now_iso(), *(_cond_columns(None, sha) if new_bytes else ()),
             r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"]),
        ).rowcount
        if placement_kind is not None:
            # v0.51.339: only the folder the bytes came from — edition-wide, a plex_upload sibling ('') became 'hardlink'.
            c.execute(
                "UPDATE placements SET placement_kind = ? WHERE media_type = ? AND tmdb_id = ? "
                "AND section_id = ? AND edition_key = ? AND media_folder = ?",
                (placement_kind, r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"],
                 r["media_folder"]),
            )
    return n


# v0.51.344: what the incoming stamp overwrites — size, sha, then the loudness/norm anchors it voids
_INCOMING_COLS = ("file_size", "file_sha256", "loudness_i", "loudness_tp", "loudness_lra", "loudness_measured_at",
                  "loudness_measured_sha256", "norm_state", "norm_gain_db", "norm_target", "norm_at",
                  "norm_orig_sha256", "norm_orig_pcm_sha256", "norm_plex_entry_uri")
_ROW_WHERE = " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? AND edition_key = ?"


def _stamp_incoming(db_path: Path, r, *, size: int, sha: str, conn=None) -> tuple | None:
    """Before other bytes move onto a BROKEN row's canonical: stamp their size and sha, void the anchors; return what it replaced."""
    from .db import transaction
    # v0.51.344: transaction()'s lock ladder, as the sibling writers — a bare UPDATE gave up after one busy wait and ended the run
    with _run_conn(db_path, conn) as c, transaction(c):
        prior = c.execute(f"SELECT {', '.join(_INCOMING_COLS)} FROM local_files" + _ROW_WHERE, _row_key(r)).fetchone()
        # v0.51.344: the incoming sha lands before the bytes — a kill or lock between replace and stamp kept the old sha and anchors
        c.execute("UPDATE local_files SET " + ", ".join(f"{col} = ?" for col in _INCOMING_COLS) + _ROW_WHERE,
                  (size, sha, *(None,) * (len(_INCOMING_COLS) - 2), *_row_key(r)))
    return None if prior is None else tuple(prior)


def _unstamp_incoming(db_path: Path, r, prior: tuple | None, *, sha: str, conn=None) -> None:
    """A move that failed moved nothing: write back what _stamp_incoming replaced, unless another writer stamped since."""
    from .db import transaction
    if prior is None:
        return
    with _run_conn(db_path, conn) as c, transaction(c):  # v0.51.344: the lock ladder — see _stamp_incoming
        c.execute("UPDATE local_files SET " + ", ".join(f"{col} = ?" for col in _INCOMING_COLS) + _ROW_WHERE
                  + " AND file_sha256 = ?", (*prior, *_row_key(r), sha))


def _stamp_present(db_path: Path, r, *, conn=None) -> None:
    """The restore guard found a non-empty canonical: stamp canonical_present = 1
    only — size/sha stay as recorded, so CHANGED still reports a mismatch."""
    from .db import transaction
    with _run_conn(db_path, conn) as c, transaction(c):
        c.execute(
            # v0.51.342: bytes motif did not write — CHANGED re-reads their size
            "UPDATE local_files SET canonical_present = 1, canonical_changed_candidate = 1 "
            "WHERE media_type = ? AND tmdb_id = ? "
            "AND section_id = ? AND edition_key = ?",
            (r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"]),
        )


def restore_from_placement(db_path: Path, themes_dir: Path, r, *, conn=None) -> dict:
    """Re-create ONE row's canonical from the theme.mp3 still in its Plex folder
    (hardlink first, copy across filesystems). `r` carries media_type, tmdb_id,
    section_id, edition_key, file_path, file_size, file_sha256, media_folder,
    placement_kind. Returns {ok, kind} or {ok: False, reason} with the reasons
    the per-item endpoint has always reported."""
    import os
    import secrets
    from .canonical import hash_file
    from .placement import _stage_link_or_copy
    from .plex_enum import _candidate_local_paths
    canonical = themes_dir / r["file_path"]
    # v0.51.342: guard, stage, replace, stamp under the path's lock — the job and an INFO restore staged one row at once
    # v0.51.344: the path's lock, then publishing's gate — a writer waiting for the path held the gate exit polls (R3-F7)
    with _canonical_write_lock(canonical), _PUBLISH_LOCK:
        if _PUBLISH_CLOSED.is_set():
            return {"ok": False, "reason": "motif_exiting"}  # v0.51.344: exit's gate covers this writer too (R2-F2)
        try:
            if canonical.is_file() and canonical.stat().st_size > 0:
                return {"ok": False, "reason": "canonical_already_present"}
        except OSError as e:
            # v0.51.338: was a bare pass (class 9) — only EACCES/EIO reach here, so say so before trying.
            log.warning("restore-canonical: could not stat %s (%s) — attempting the restore anyway", canonical, e)
        with _run_conn(db_path, conn) as c:
            in_flight = _download_in_flight(c, r)
            # v0.51.344: the row's path now — an edition swap mid-run re-keyed it, and a stamp by the old key writes nothing (R2-F10)
            now_at = c.execute("SELECT file_path FROM local_files" + _ROW_WHERE, _row_key(r)).fetchone()
        if in_flight:
            # v0.51.342: the download's ffmpeg -y re-opens theme.mp3 with O_TRUNC — a link here truncated Plex's copy.
            return {"ok": False, "reason": "download_in_flight"}
        if now_at is None or now_at[0] != r["file_path"]:
            return {"ok": False, "reason": "row_moved"}
        if not r["media_folder"]:
            return {"ok": False, "reason": "no_placement"}
        src: Path | None = None
        for cand in _candidate_local_paths(r["media_folder"]):
            p = cand / "theme.mp3"
            try:
                if p.is_file() and p.stat().st_size > 0:
                    src = p
                    break
            except OSError:
                continue
        if src is None:
            return {"ok": False, "reason": "placement_file_missing"}
        # v0.51.342: a tmp only this call removes; v0.51.344: staged, hashed, stamped, then moved — the sha never runs
        # ahead of bytes on disk (R2-F11) and the staged inode is the one read, whichever kind lands (R2-F7)
        tmp = canonical.with_name(f"{canonical.name}.{secrets.token_hex(8)}.motif-tmp")
        prior = sha = None
        try:
            canonical.parent.mkdir(parents=True, exist_ok=True)
            # v0.51.338: placement's link; any OSError (SMB/FUSE EPERM) copies via a tmp — a dead copy leaves no partial
            kind = _stage_link_or_copy(src, tmp)
            sha, size = hash_file(tmp)
            if sha != r["file_sha256"]:
                prior = _stamp_incoming(db_path, r, size=size, sha=sha, conn=conn)
                if prior is None:
                    return {"ok": False, "reason": "row_moved"}  # v0.51.344: the row left after the re-read — nothing moves
            os.replace(tmp, canonical)
        except OSError as e:
            log.warning("restore-canonical: %s/%s section=%s failed: %s — source %s",
                        r["media_type"], r["tmdb_id"], r["section_id"], e, src)
            # v0.51.344: a move that raised moved nothing — a canonical an EACCES stat hid is still the recorded one; a
            # raise while staging or hashing comes before the stamp, so there is nothing to write back (R2-F2)
            try:
                _unstamp_incoming(db_path, r, prior, sha=sha, conn=conn)
            except sqlite3.Error as e2:
                # v0.51.344: a lock the ladder lost must not end the run — the skip stands; the next restore lands the sha kept
                log.warning("restore-canonical: %s/%s section=%s could not write back the prior stamp (%s) — incoming "
                            "stamp kept", r["media_type"], r["tmdb_id"], r["section_id"], e2)
            # v0.51.344: the errno's words only — str(e) named the absolute media path in the reason the status serves
            return {"ok": False, "reason": f"link_failed:{e.strerror or type(e).__name__}"}
        finally:
            try:
                tmp.unlink(missing_ok=True)  # v0.51.342: only the staging file this call made; gone already after the move
            except OSError as ue:
                log.warning("restore-canonical: could not remove staged %s: %s", tmp, ue)
        if not _stamp_restored(db_path, r, canonical, prior_size=r["file_size"], prior_sha=r["file_sha256"],
                               placement_kind=kind if r["placement_kind"] != kind else None, conn=conn,
                               known=(size, sha), reread=False):
            return {"ok": False, "reason": "row_moved"}  # v0.51.344: the bytes landed, but not for the row that asked
    return {"ok": True, "kind": kind}


def _selected_entry_uri(themes_body) -> str | None:
    """The entry Plex serves for an item: the one flagged selected, else the only
    entry. None when the store holds nothing usable."""
    if not isinstance(themes_body, dict):
        return None
    metadata = (themes_body.get("MediaContainer") or {}).get("Metadata") or []
    entries = [e for e in metadata if isinstance(e, dict) and isinstance(e.get("ratingKey"), str)]
    if not entries:
        return None
    for e in entries:
        if e.get("selected") is True:
            return e["ratingKey"]
    return entries[0]["ratingKey"] if len(entries) == 1 else None


def _store_guard(themes_dir: Path, r) -> dict | None:
    """The store path's refusal when a non-empty canonical is on disk, else None."""
    canonical = themes_dir / r["file_path"]
    try:
        # v0.51.338: a REPAIR ALL download lands before verify re-stamps canonical_present — never replace it.
        if canonical.is_file() and canonical.stat().st_size > 0:
            return {"ok": False, "reason": "canonical_already_present"}
    except OSError as e:
        log.warning("restore-from-plex-store: could not stat %s (%s) — attempting the refetch anyway", canonical, e)
    return None


def _fetch_from_plex_store(plex_client, r) -> dict:
    """The bytes Plex serves for one row: {ok, data, entry_uri} or {ok: False, reason}. No disk, no database."""
    rk = str(r["plex_rating_key"])
    try:
        themes = plex_client.get_themes(rating_key=rk)
        if not themes.get("ok"):
            return {"ok": False, "reason": f"plex_themes:{themes.get('http_status') or themes.get('error')}"}
        uri = _selected_entry_uri(themes.get("body"))
        if not uri:
            return {"ok": False, "reason": "no_theme_entry"}
        got = plex_client.fetch_theme_bytes(item_rating_key=rk, entry_uri=uri)
        data = got.get("bytes") if got.get("ok") else None
        if not data:
            return {"ok": False, "reason": f"plex_fetch:{got.get('http_status') or got.get('error')}"}
    except Exception as e:  # noqa: BLE001 — v0.51.344: one raising fetch re-raised in f.result() and dropped its batch's bytes
        log.warning("restore from plex: %s/%s section=%s — reading Plex's answer raised %s: %s; row skipped",
                    r["media_type"], r["tmdb_id"], r["section_id"], type(e).__name__, e)
        return {"ok": False, "reason": f"plex_error:{type(e).__name__}"}
    return {"ok": True, "data": data, "entry_uri": uri}


def _publish_store_bytes(db_path: Path, themes_dir: Path, r, data: bytes, uri: str, *, conn=None) -> dict:
    """Write fetched store bytes to the canonical and stamp them: {ok, bytes, entry_uri} or {ok: False, reason}."""
    import hashlib
    import os
    import secrets
    canonical = themes_dir / r["file_path"]
    # v0.51.342: under the path's lock, as restore_from_placement — the guard, the write and the stamp are one step.
    # v0.51.344: one lock order — the path's lock, then publishing's gate: a wait for the path held the gate exit polls (R3-F7)
    with _canonical_write_lock(canonical), _PUBLISH_LOCK:
        if _PUBLISH_CLOSED.is_set():
            return {"ok": False, "reason": "motif_exiting"}
        # v0.51.342: a download that landed while the bytes were in flight wins.
        guard = _store_guard(themes_dir, r)
        if guard is not None:
            return guard
        with _run_conn(db_path, conn) as c:
            in_flight = _download_in_flight(c, r)
            # v0.51.344: the row's path now — an edition swap mid-run re-keyed it, and a stamp by the old key writes nothing (R2-F10)
            now_at = c.execute("SELECT file_path FROM local_files" + _ROW_WHERE, _row_key(r)).fetchone()
        if in_flight:
            # v0.51.342: its ffmpeg -y truncates whatever lands on theme.mp3 before it finishes.
            return {"ok": False, "reason": "download_in_flight"}
        if now_at is None or now_at[0] != r["file_path"]:
            return {"ok": False, "reason": "row_moved"}
        sha = hashlib.sha256(data).hexdigest()
        # v0.51.344: a name only this call writes — yt-dlp stages its own download to theme.mp3.part in this folder (R2-F3)
        tmp = canonical.with_name(f"{canonical.name}.{secrets.token_hex(8)}.part")
        prior = None
        try:
            canonical.parent.mkdir(parents=True, exist_ok=True)
            with tmp.open("wb") as f:
                f.write(data)
            # v0.51.344: the stamp follows the staged write — the sha never runs ahead of bytes on disk (R2-F11)
            if sha != r["file_sha256"]:
                prior = _stamp_incoming(db_path, r, size=len(data), sha=sha, conn=conn)
                if prior is None:
                    return {"ok": False, "reason": "row_moved"}  # v0.51.344: the row left after the re-read — nothing moves
            os.replace(tmp, canonical)
        except OSError as e:
            log.warning("restore-from-plex-store: %s/%s section=%s write failed: %s",
                        r["media_type"], r["tmdb_id"], r["section_id"], e)
            # v0.51.344: the write goes to .part and os.replace is atomic — a failure moved nothing onto the canonical, and
            # a raise while writing the .part comes before the stamp, so there is nothing to write back (R2-F2)
            try:
                _unstamp_incoming(db_path, r, prior, sha=sha, conn=conn)
            except sqlite3.Error as e2:
                # v0.51.344: a lock the ladder lost must not end the run — the skip stands; the next restore lands the sha kept
                log.warning("restore-from-plex-store: %s/%s section=%s could not write back the prior stamp (%s) — "
                            "incoming stamp kept", r["media_type"], r["tmdb_id"], r["section_id"], e2)
            # v0.51.344: the errno's words only — str(e) named the absolute canonical path in the reason the status serves
            return {"ok": False, "reason": f"write_failed:{e.strerror or type(e).__name__}"}
        finally:
            try:
                tmp.unlink(missing_ok=True)  # v0.51.344: a .part the move never took is this call's to remove
            except OSError as ue:
                log.warning("restore-from-plex-store: could not remove staged %s: %s", tmp, ue)
        if not _stamp_restored(db_path, r, canonical, prior_size=r["file_size"], prior_sha=r["file_sha256"],
                               placement_kind=None, conn=conn, known=(len(data), sha)):
            return {"ok": False, "reason": "row_moved"}  # v0.51.344: the bytes landed, but not for the row that asked
    return {"ok": True, "bytes": len(data), "entry_uri": uri}


def refetch_from_plex_store(db_path: Path, themes_dir: Path, plex_client, r, *, conn=None) -> dict:
    """Re-create ONE row's canonical from the bytes Plex itself serves for the
    item (a plex_upload placement lives in Plex's metadata store, not a folder).
    Returns {ok, bytes, entry_uri} or {ok: False, reason}."""
    guard = _store_guard(themes_dir, r)
    if guard is not None:
        return guard
    rk = r["plex_rating_key"]
    if not rk or not str(rk).isdigit():
        return {"ok": False, "reason": "no_rating_key"}
    if plex_client is None:
        return {"ok": False, "reason": "plex_unavailable"}
    got = _fetch_from_plex_store(plex_client, r)
    if not got["ok"]:
        return got
    return _publish_store_bytes(db_path, themes_dir, r, got["data"], got["entry_uri"], conn=conn)


def _broken_rows_with_placement(conn) -> list[dict]:
    """The broken rows as dicts carrying the fields both restore paths need."""
    out = []
    rows = _broken_rows(conn)
    # v0.51.342: two SELECTs for the whole list — file_size/file_sha256 ride _broken_rows, placements come batched.
    placements = _broken_placements(conn)
    for r in rows:
        p, _sidecar = _pick_placement(placements.get(_row_key(r), []))
        d = {k: r[k] for k in r.keys()}
        d["media_folder"] = p["media_folder"] if p else None
        d["placement_kind"] = p["placement_kind"] if p else None
        d["plex_rating_key"] = p["plex_rating_key"] if p else None
        out.append(d)
    return out


class _PlexGate:
    """The store fetches' stop rule: back off while Plex gives no answer, stop once it has given none for a while."""

    def __init__(self, clock=time.monotonic, sleep=None):
        self._lock = threading.Lock()
        self._n = 0
        self._first_at = None
        self._alive_at = None  # v0.51.344: when Plex last answered — a no-answer sent before it says nothing about Plex now
        self.tripped = False
        self.cancelled = threading.Event()
        # v0.51.342: the serial tail's only — the pool's loop sees the cancel and wakes its backoffs; workers never ask.
        self.cancel_check = None
        # v0.51.342: the default backoff is a wait the cancel ends — a cancelled run never sleeps one out.
        self._clock, self._sleep = clock, sleep if sleep is not None else self._wait

    def _wait(self, s) -> None:
        if self.cancel_check is None:
            self.cancelled.wait(s)
            return
        end = time.monotonic() + s
        # v0.51.342: the shared-path tail backs off on the thread polling the cancel — its wait asks or nothing wakes it
        while not self.cancelled.wait(max(0.0, min(end - time.monotonic(), _PLEX_CANCEL_POLL_S))):
            if self.cancel_check():
                self.cancelled.set()
            elif time.monotonic() >= end:
                return

    def before(self) -> bool:
        with self._lock:
            if self.tripped:
                return False
            n = self._n
        if n > 0:
            self._sleep(min(_PLEX_BACKOFF_BASE_S * 2 ** (n - 1), _PLEX_BACKOFF_CAP_S))
        with self._lock:
            return not self.tripped

    def after(self, reason, started) -> None:
        with self._lock:
            if reason is not None and str(reason).startswith("plex_error:"):
                return  # v0.51.344: a raise reading Plex's answer is neither an answer nor a no-answer — the tally stands (R2-F5)
            if reason is None or not str(reason).startswith(_PLEX_NO_ANSWER):
                self._n, self._first_at, self._alive_at = 0, None, self._clock()
                return
            if self._alive_at is not None and started < self._alive_at:
                return  # v0.51.344: sent before Plex last answered — it says nothing about Plex now
            self._n += 1
            if self._first_at is None:
                self._first_at = started  # v0.51.344: both ends are request starts — a refusal then a 30 s hang read as a 60 s outage
            # v0.51.342: count AND time — four workers make eight no-answers in ~8 s, shorter than a Plex restart.
            if not self.tripped and self._n >= _PLEX_TRIP_AFTER and started - self._first_at >= _PLEX_TRIP_WINDOW_S:
                self.tripped = True
                log.warning("restore from plex: Plex gave no answer to %d requests over %.0f s — the remaining "
                            "store rows are skipped as plex_unreachable", self._n, started - self._first_at)


def restore_from_plex(db_path: Path, themes_dir: Path, plex_client, *, plex_client_factory=None,
                      workers: int = RESTORE_PLEX_WORKERS, progress_cb=None, cancel_check=None) -> dict:
    """The bulk: every broken canonical that Plex still holds a copy of — the
    sidecar in its Plex folder first (no network), else Plex's own store for a
    plex_upload placement. Rows with neither are skipped with a reason; nothing
    is re-downloaded here (that is REPAIR ALL). Never overwrites a present file."""
    from .db import get_conn
    gate = _PlexGate()
    plex_on = plex_client is not None or plex_client_factory is not None
    n_workers = max(1, workers) if plex_client_factory is not None else 1
    counts = {"restored_sidecar": 0, "restored_store": 0, "skipped_count": 0}
    reasons: dict[int, str] = {}
    cancelled = False
    done = 0
    # v0.51.342: one connection for the run — the pool only talks to Plex; every disk write and stamp is this thread's.
    with get_conn(db_path) as conn:
        rows = _broken_rows_with_placement(conn)
        total = len(rows)
        # v0.51.342: rows on one canonical path run serially after the pool, in report order — the serial run's winner.
        shared = {p for p, n in Counter(str(r["file_path"]).casefold() for r in rows).items() if n > 1}
        log.info("restore from plex: %d broken rows (%d on a shared canonical path), %d Plex worker(s), Plex %s",
                 total, sum(1 for r in rows if str(r["file_path"]).casefold() in shared), n_workers,
                 "configured" if plex_on else "not configured")

        def tick():
            if progress_cb is not None:
                progress_cb(done, total, dict(counts))

        def land(i, kind, reason=None):
            nonlocal done
            if kind == "skip":
                reasons[i] = reason
                counts["skipped_count"] += 1
            else:
                counts["restored_" + kind] += 1
            done += 1

        def land_skip(i, r, reason):
            if reason == "canonical_already_present":
                # v0.51.339: verify's own rule (non-empty = present) — else the row stays listed until the daily verify.
                _stamp_present(db_path, r, conn=conn)
            land(i, "skip", reason)

        def stopped() -> bool:
            # v0.51.344: exit's closed gate stops the run as a cancel does — every later row would only be refused (R2-F2)
            return (cancel_check is not None and cancel_check()) or _PUBLISH_CLOSED.is_set()

        def store_refusal(r):
            refusal = _store_guard(themes_dir, r)
            if refusal is None and _download_in_flight(conn, r):
                # v0.51.342: the publish would refuse these bytes — never ask Plex for them.
                refusal = {"ok": False, "reason": "download_in_flight"}
            if refusal is None and (not r["plex_rating_key"] or not str(r["plex_rating_key"]).isdigit()):
                refusal = {"ok": False, "reason": "no_rating_key"}
            if refusal is None and not plex_on:
                refusal = {"ok": False, "reason": "plex_unavailable"}
            return refusal

        def row_rules(i, r, store_step):
            res = None
            if r["media_folder"]:
                res = restore_from_placement(db_path, themes_dir, r, conn=conn)
                if res["ok"]:
                    land(i, "sidecar")
                    return
            # v0.51.338: a present canonical ends the row — falling through let the store overwrite it.
            present = res is not None and res.get("reason") == "canonical_already_present"
            if not present and (r["placement_kind"] == "plex_upload"
                                or (r["media_folder"] in (None, "") and r["plex_rating_key"])):
                res2 = store_refusal(r)
                if res2 is None:
                    res2 = store_step(i, r)
                    if res2 is None:
                        return
                if res2["ok"]:
                    land(i, "store")
                    return
                res = res2
            land_skip(i, r, (res or {}).get("reason") or "no_plex_copy")

        queued: list[int] = []
        later: list[int] = []
        tick()
        for i, r in enumerate(rows):
            if stopped():
                cancelled = True
                break
            if str(r["file_path"]).casefold() in shared:
                later.append(i)
                continue
            before = done
            row_rules(i, r, lambda j, _row: queued.append(j))
            if done != before:
                tick()

        if queued and not cancelled:
            local = threading.local()
            clients: list = []
            clients_lock = threading.Lock()

            def pool_client():
                if plex_client_factory is None:
                    return plex_client
                c = getattr(local, "client", None)
                if c is None:
                    c = local.client = plex_client_factory()
                    with clients_lock:
                        clients.append(c)
                return c

            def work(i):
                if not gate.before():
                    return i, {"ok": False, "reason": "plex_unreachable"}
                if gate.cancelled.is_set():
                    return i, None  # v0.51.342: woke from a backoff after the cancel — never ask Plex; not tried.
                started = gate._clock()
                got = _fetch_from_plex_store(pool_client(), rows[i])
                gate.after(None if got["ok"] else got["reason"], started)
                return i, got

            todo = iter(queued)
            pending: set = set()
            pool = ThreadPoolExecutor(n_workers, thread_name_prefix="restore-from-plex")
            try:
                refilling = True
                while True:
                    # v0.51.342: a finished fetch holds its bytes until published — at most 2 × workers bodies wait.
                    while refilling and len(pending) < 2 * n_workers:
                        nxt = next(todo, None)
                        if nxt is None:
                            break
                        pending.add(pool.submit(work, nxt))
                    if not pending:
                        break
                    finished, rest = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                    pending = set(rest)
                    for i, got in sorted((f.result() for f in finished), key=lambda t: t[0]):
                        if got is None:
                            continue
                        r = rows[i]
                        if got["ok"]:
                            got = _publish_store_bytes(db_path, themes_dir, r, got["data"], got["entry_uri"],
                                                       conn=conn)
                            if got["ok"]:
                                land(i, "store")
                                continue
                        land_skip(i, r, got["reason"])
                    if finished:
                        tick()
                    if refilling and stopped():
                        refilling, cancelled = False, True
                        gate.cancelled.set()
                        # v0.51.342: a queued fetch asked a hung Plex after the cancel — drop it; running ones publish.
                        pending = {f for f in pending if not f.cancel()}
            except BaseException:
                gate.cancelled.set()  # v0.51.344: a publish that raised left the backoffs to sleep out, then ask Plex for bytes nobody writes
                raise
            finally:
                pool.shutdown(wait=True, cancel_futures=True)
                for c in clients:
                    try:
                        c.close()
                    except Exception as e:  # noqa: BLE001 — a close error must not relabel a finished run
                        log.debug("restore from plex: closing a Plex client failed: %s", e)

        if later and not cancelled:
            tail: list = []

            def tail_client():
                if plex_client is not None or plex_client_factory is None:
                    return plex_client
                if not tail:
                    tail.append(plex_client_factory())
                return tail[0]

            def fetch_now(i, r):
                if not gate.before():
                    return {"ok": False, "reason": "plex_unreachable"}
                if gate.cancelled.is_set():
                    return None  # v0.51.342: as the pool's work() — woke from a backoff after the cancel; not tried.
                started = gate._clock()
                got = refetch_from_plex_store(db_path, themes_dir, tail_client(), r, conn=conn)
                gate.after(None if got["ok"] else got["reason"], started)
                return got

            # v0.51.342: nothing else polls the cancel here, so a tail row's backoff must ask it — the pool's never do.
            gate.cancel_check = cancel_check
            try:
                for i in later:
                    if stopped():
                        cancelled = True
                        break
                    row_rules(i, rows[i], fetch_now)
                    tick()
            finally:
                for c in tail:
                    try:
                        c.close()
                    except Exception as e:  # noqa: BLE001 — a close error must not relabel a finished run
                        log.debug("restore from plex: closing a Plex client failed: %s", e)
            # v0.51.342: a cancel that woke the LAST row's backoff left no row to see it — the run still read done.
            cancelled = cancelled or gate.cancelled.is_set()

    skipped = [{"title": rows[i]["title"] or f'{rows[i]["media_type"]}/{rows[i]["tmdb_id"]}',
                "media_type": rows[i]["media_type"], "tmdb_id": rows[i]["tmdb_id"],
                "section_id": rows[i]["section_id"], "reason": reasons[i]} for i in sorted(reasons)]
    restored_sidecar, restored_store = counts["restored_sidecar"], counts["restored_store"]
    log.info("restore from plex: %d from sidecars, %d from Plex's store, %d skipped of %d broken%s%s",
             restored_sidecar, restored_store, len(skipped), total,
             f" — cancelled, {total - done} not tried" if cancelled else "",
             " — Plex gave no answer" if gate.tripped else "")
    return {"broken": total, "restored_sidecar": restored_sidecar,
            "restored_store": restored_store, "restored": restored_sidecar + restored_store,
            "skipped": skipped, "cancelled": cancelled, "not_attempted": total - done,
            "plex_unreachable": gate.tripped}


def changed_canonicals(conn, themes_dir: Path) -> list[dict]:
    """Rows whose canonical IS on disk but no longer the size the database
    recorded — a truncated write, an external edit, a file swapped underneath
    motif. A size the database never recorded (NULL / 0) is judged too: bytes
    with no stamp to match are CHANGED. Read-only: stats, never stamps.
    v0.51.342: the last check's candidates, re-read now — a page open stats these, not the tree."""
    rows = conn.execute(
        "SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key, lf.file_path, "
        "       lf.file_size, lf.source_kind, t.title, t.year, COALESCE(ps.is_anime, 0) AS is_anime "
        "FROM local_files lf "
        "LEFT JOIN themes t ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id "
        "LEFT JOIN plex_sections ps ON ps.section_id = lf.section_id "
        # v0.51.342: only verify's candidates — the live stat below decides, so a writer's restamp clears a row at once.
        # v0.51.344: a size never recorded (NULL / 0) is a candidate too — it hid a restored row a backup rolled back (R3-F6)
        "WHERE lf.canonical_changed_candidate = 1 AND COALESCE(lf.canonical_present, 1) = 1 "
        "ORDER BY lf.media_type, t.title, lf.tmdb_id, lf.section_id, lf.edition_key"
    ).fetchall()
    out = []
    for r in rows:
        try:
            st = (themes_dir / r["file_path"]).stat()
        except OSError:
            continue  # absent rows are the broken set's business
        if st.st_size != (r["file_size"] or 0) and st.st_size > 0:
            out.append({**_entry(r), "recorded": r["file_size"], "on_disk": st.st_size})
    return out


def forget_canonical_checks(db_path: Path) -> int:
    """After a staged database restore: the rows' check results describe the disk
    when that backup was taken. Clears them (canonical_present stays, so BROKEN and
    the library DL sort keep the restored stamps); returns the rows touched."""
    from .db import get_conn, transaction
    with get_conn(db_path) as conn, transaction(conn):
        return conn.execute(
            "UPDATE local_files SET canonical_health_checked_at = NULL, canonical_changed_candidate = NULL, "
            "    canonical_hash_miss_sig = NULL "
            "WHERE canonical_health_checked_at IS NOT NULL OR canonical_changed_candidate IS NOT NULL "
            "   OR canonical_hash_miss_sig IS NOT NULL").rowcount


def forget_unmarked_checks(db_path: Path) -> tuple[int, str | None]:
    """At boot: clears the check results stamped after the last check's mark; returns (rows cleared, the mark)."""
    from .db import get_conn, transaction
    from .plex_enum import CANONICAL_CHECK_MARK_KEY
    with get_conn(db_path) as conn, transaction(conn):
        row = conn.execute("SELECT value FROM runtime_settings WHERE key = ?", (CANONICAL_CHECK_MARK_KEY,)).fetchone()
        mark = row[0] if row else None
        # v0.51.344: no mark = stamps no v80-aware check vouched for (a .341 rollback or a pre-.344 build) — all go.
        n = conn.execute("UPDATE local_files SET canonical_health_checked_at = NULL, "
                         # v0.51.344: the whole result, as forget_canonical_checks — 'Not checked yet' says CHANGED is empty.
                         "    canonical_changed_candidate = NULL, canonical_hash_miss_sig = NULL "
                         "WHERE canonical_health_checked_at > ?", (mark or "",)).rowcount
    return n, mark
