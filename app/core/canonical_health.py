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

log = logging.getLogger("motif.canonical_health")

# The download pipeline records no re-fetchable URL for these — their bytes are a
# direct upload / adopted sidecar / Plex-cloud backup. A broken canonical for one
# of these is surfaced for manual re-place, never auto-re-downloaded (a TDB URL on
# the shared themes row is NOT this row's source — re-downloading would swap the
# operator's content). Mirrors the api.py canonical-danger grouping.
_NO_URL_SOURCE_KINDS = ("upload", "adopt", "plex_cloud")


def _override_url(conn, r) -> str | None:
    """The user_overrides URL the download worker would resolve for this row —
    section-scoped first, then the '' global fallback (mirrors worker.py:1646)."""
    row = conn.execute(
        "SELECT youtube_url FROM user_overrides "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? AND edition_key = ?",
        (r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"]),
    ).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT youtube_url FROM user_overrides "
            "WHERE media_type = ? AND tmdb_id = ? AND section_id = '' AND edition_key = ?",
            (r["media_type"], r["tmdb_id"], r["edition_key"]),
        ).fetchone()
    return row["youtube_url"] if row else None


def classify_repair(conn, r) -> str:
    """'redownload' if the row's recorded source is a re-fetchable URL, else
    'canonical_missing' (surface for manual re-place). See the module docstring
    for the full rule; keyed off source_kind + the worker's URL resolution so it
    agrees with what a re-download would actually do."""
    if r["source_kind"] in _NO_URL_SOURCE_KINDS:
        return "canonical_missing"
    # source_kind is 'url', 'themerrdb', or NULL(legacy). A live override URL is
    # always re-fetchable (a U row); otherwise fall back to the TDB URL, but only
    # for a genuinely TDB-tracked item (a plex_orphan carries no meaningful TDB URL).
    if _override_url(conn, r):
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
        "       lf.file_path, lf.source_kind, "
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


def broken_canonical_report(conn) -> dict:
    """Read-only: every confirmed-broken canonical, split into the re-downloadable
    set (REPAIR ALL can fix these) and the canonical-missing set (manual re-place).
    Writes nothing — the /admin/canonical-health page renders this."""
    rows = _broken_rows(conn)
    redownloadable: list[dict] = []
    canonical_missing: list[dict] = []
    for r in rows:
        entry = _entry(r)
        if classify_repair(conn, r) == "redownload":
            redownloadable.append(entry)
        else:
            entry["has_live_placement"] = bool(r["has_live_placement"])
            canonical_missing.append(entry)
    return {
        "broken": len(rows),
        "redownloadable": redownloadable,
        "canonical_missing": canonical_missing,
        "counts": {
            "broken": len(rows),
            "redownloadable": len(redownloadable),
            "canonical_missing": len(canonical_missing),
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
        if classify_repair(conn, r) != "redownload":
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
