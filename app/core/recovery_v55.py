"""v1.18.5 — Data recovery for installs hit by the v55 migration bug.

v1.18.0 shipped `_widen_check_constraint` using
`PRAGMA defer_foreign_keys = ON` under the assumption it would
suppress FK cascades during the DROP TABLE step of each
table-rebuild. That was wrong: `defer_foreign_keys` only defers
violation CHECKS to COMMIT time — it does NOT defer cascading
actions. When `DROP TABLE themes` ran, the `ON DELETE CASCADE`
on `local_files` and `placements` fired immediately, wiping every
row in both tables, and the `ON DELETE SET NULL` on
`plex_items.theme_id` nulled the linkage column for every row.

The migration fix in `_widen_check_constraint` (use
`PRAGMA foreign_keys = OFF`) prevents the issue for new
upgrades. This module handles RECOVERY for installs that
already ran the buggy migration and lost their tracking data.

## What survived the bug

  * `themes` table — rebuilt FIRST, so its own data is intact
    (the INSERT-SELECT preserved title, year, youtube_url,
    youtube_video_id, upstream_source, imdb_id, etc.).
  * On-disk theme.mp3 files — `themes_dir/<section_subdir>/
    <canonical_subdir>/theme.mp3` (motif's canonicals)
    AND `<plex_media_folder>/theme.mp3` (Plex's placements,
    hardlinked from canonical at place time). The DB rebuild
    didn't touch the filesystem.
  * `plex_items.local_theme_file = 1` — Plex's
    local-media-assets agent already saw the placement files;
    the column was set during the prior plex_enum and was NOT
    nulled by the migration (plex_items was rebuilt second,
    after themes, with column values preserved by the
    INSERT-SELECT).

## What was destroyed

  * `local_files` — every row deleted via ON DELETE CASCADE.
  * `placements` — every row deleted via ON DELETE CASCADE.
  * `plex_items.theme_id` — nulled via ON DELETE SET NULL.

## Recovery strategy

For each `themes` row T (canonical TDB or user URL):

  1. For each managed plex_sections row S, derive the expected
     canonical path:
        themes_dir / S.themes_subdir / canonical_theme_subdir(
            T.title, T.year) / theme.mp3
  2. If the file exists → INSERT into local_files with
     provenance/source_kind inferred from T.upstream_source
     ('themoviedb'/'imdb' → 'themerrdb', 'plex_orphan' → 'adopt'
     unless a user_overrides row exists for (T, S) in which case
     'url').
  3. For each plex_items row pi in section S where
     pi.local_theme_file = 1 AND (
        pi.guid_tmdb = T.tmdb_id
        OR (pi.title_norm = T.title_norm AND pi.year = T.year)
     ), INSERT placements with the section_id + pi.folder_path.
  4. After all walks, run `resolve_theme_ids` to repopulate
     plex_items.theme_id from the rebuilt themes.

Recovery is idempotent — re-runs skip rows that already have
local_files / placements rows. Stamped in `runtime_settings`
under `recovery_v55_done_at` so subsequent boots skip the walk.

## Why this isn't a full schema migration

The schema (v55) is fine — the CHECK widenings landed
correctly, the migration just nuked data through FK cascades.
A v56 schema bump would force every install (including fresh
ones not affected by the bug) to run the walk, which is
wasteful. The runtime_settings marker keeps the walk
self-gating.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

log = logging.getLogger(__name__)

# v1.18.7: minimum sidecar count to consider an install as
# "has on-disk evidence to recover from." Avoids triggering on
# fresh installs that haven't enumerated yet OR installs where
# Plex genuinely has very few theme files. Exposed as a module-
# level constant so tests can patch it to lower values without
# seeding hundreds of fixture rows. Real-world libraries have
# thousands of sidecars on a populated install; 50 is well
# below any realistic floor.
_SIDECAR_EVIDENCE_FLOOR = 50


def _expected_canonical_path(
    themes_dir: Path,
    themes_subdir: str,
    title: str,
    year: str | None,
) -> Path:
    """Mirror worker._do_download's path construction so the
    recovery finds the exact file motif wrote pre-bug."""
    from .canonical import canonical_theme_subdir
    return (
        themes_dir
        / themes_subdir
        / canonical_theme_subdir(title or "", year)
        / "theme.mp3"
    )


def _infer_source_kind(upstream_source: str | None,
                       has_user_override: bool) -> tuple[str, str]:
    """Infer (provenance, source_kind) for the local_files row
    based on the themes record + user_overrides state.

    The pre-bug local_files rows carried provenance + source_kind
    so the SRC letter SQL could distinguish T / A / U / M. We
    can't reconstruct provenance perfectly (the original
    source_kind value isn't stored anywhere we can read), but
    the heuristic below covers the common cases:

      * TDB-tracked + no user override → ('auto', 'themerrdb') → T
      * TDB-tracked + user override     → ('manual', 'url')      → U
      * plex_orphan                     → ('manual', 'adopt')    → A
        (the user adopted a sidecar — no TDB attribution)
    """
    if upstream_source == "plex_orphan":
        return ("manual", "adopt")
    if has_user_override:
        return ("manual", "url")
    return ("auto", "themerrdb")


def _section_is_home_type(section_type: str, theme_media_type: str) -> bool:
    """v1.23.74: True when a Plex section is the natural home for a
    theme's media type — a movie theme belongs in a movie section, a tv
    theme in a show section. plex_sections.type is Plex's 'movie'/'show';
    themes.media_type is motif's 'movie'/'tv'/'collection'. The recovery
    walk uses this to let a true orphan (no matching plex_items) still
    recover in its own section while rejecting a same-named file claimed
    from a foreign media type's subdir (the Death Note 2006 live-action
    MOVIE vs anime SERIES title collision). Collections never reach the
    walk's sidecar path (plex_upload, no on-disk theme.mp3) → permissive."""
    if theme_media_type == "movie":
        return section_type == "movie"
    if theme_media_type == "tv":
        return section_type == "show"
    return True


def _detect_loss_pattern(conn: sqlite3.Connection) -> bool:
    """True iff the DB looks like a post-v55-bug install:
    themes populated, on-disk evidence (plex_items with
    local_theme_file=1) plentiful, but local_files coverage is
    dramatically below what those sidecars imply.

    v1.18.7: ratio-based heuristic. The v1.18.5 original required
    `local_files == 0 AND placements == 0` for "loss" — that
    short-circuits to False when the user touches a single row
    post-bug (e.g., the user's SET URL on Willy Wonka Collection
    inserted ONE local_files row, which silently disqualified
    his entire library from auto-recovery). Now: detect when
    `local_files` count is < 50% of plex_items.local_theme_file=1
    count + an absolute floor (50 sidecars) to dodge fresh-install
    false positives. Idempotent INSERT OR IGNORE in the walker
    protects against double-inserts on the already-tracked
    minority.

    All branches emit log lines so the operator can see WHY a
    boot skipped the walk (the v1.18.5 silent-skip made
    debugging "why isn't recovery firing" a black box)."""
    themes_n = conn.execute(
        "SELECT COUNT(*) FROM themes WHERE upstream_source != 'plex_orphan'"
    ).fetchone()[0]
    if themes_n == 0:
        log.info(
            "v1.18.5 recovery: no TDB themes (themes_n=0) — "
            "skipping. Fresh install or pre-sync state.",
        )
        return False
    pi_sidecar_n = conn.execute(
        "SELECT COUNT(*) FROM plex_items WHERE local_theme_file = 1"
    ).fetchone()[0]
    if pi_sidecar_n < _SIDECAR_EVIDENCE_FLOOR:
        log.info(
            "v1.18.5 recovery: insufficient on-disk evidence "
            "(plex_items with sidecar=%d, floor=%d) — skipping. "
            "Either Plex hasn't enumerated yet or the install "
            "genuinely has no theme files placed.",
            pi_sidecar_n, _SIDECAR_EVIDENCE_FLOOR,
        )
        return False
    lf_n = conn.execute("SELECT COUNT(*) FROM local_files").fetchone()[0]
    pl_n = conn.execute("SELECT COUNT(*) FROM placements").fetchone()[0]
    # Healthy coverage: at least half of the sidecar-bearing
    # plex_items have a matching local_files row. Pre-bug installs
    # land at ~100% coverage; the bug crashed it to ~0%.
    if lf_n >= pi_sidecar_n * 0.5:
        log.info(
            "v1.18.5 recovery: local_files coverage looks healthy "
            "(lf=%d, sidecars=%d, placements=%d) — skipping.",
            lf_n, pi_sidecar_n, pl_n,
        )
        return False
    log.warning(
        "v1.18.5 recovery: LOSS PATTERN DETECTED — themes=%d, "
        "plex_items_with_sidecar=%d, local_files=%d, "
        "placements=%d. local_files coverage is < 50%% of "
        "sidecar evidence; walking themes_dir to rebuild.",
        themes_n, pi_sidecar_n, lf_n, pl_n,
    )
    return True


def _already_recovered(conn: sqlite3.Connection) -> bool:
    """v1.18.5 stamps a runtime_settings marker once the walk
    completes so we don't re-walk on every boot.

    v1.18.11 bumped the marker key from `recovery_v55_done_at`
    to `recovery_v55_done_at_v1_18_11` because v1.18.11 improved
    the placement-scan algorithm (uses pi.theme_id linkage AND
    scans the Plex folder directly instead of trusting
    pi.local_theme_file). Existing v1.18.5-recovered installs
    re-run the (now better) walker once. Walker is idempotent
    via INSERT OR IGNORE so re-runs are safe."""
    row = conn.execute(
        "SELECT value FROM runtime_settings "
        "WHERE key = 'recovery_v55_done_at_v1_18_11'"
    ).fetchone()
    return row is not None


def maybe_recover_post_v55_data_loss(
    db_path: Path,
    themes_dir: Path | None,
) -> dict:
    """Entry point — call once at startup after init_db. Detects
    the v55-bug pattern and runs the recovery walk if all
    preconditions hold.

    No-ops (returns zero-stats) when:
      * themes_dir isn't configured (first-run install)
      * runtime_settings already carries the recovery marker
      * the loss pattern isn't detected (fresh install OR
        already-tracked install)

    Returns {'detected', 'local_files_inserted',
             'placements_inserted', 'theme_ids_relinked'}.
    """
    from .db import get_conn, transaction
    from .canonical import canonical_theme_subdir

    stats = {
        "detected": False,
        "local_files_inserted": 0,
        "placements_inserted": 0,
        "theme_ids_relinked": 0,
    }
    if themes_dir is None or not themes_dir.exists():
        log.info(
            "v1.18.5 recovery: themes_dir=%r not configured / "
            "missing — skipping",
            themes_dir,
        )
        return stats
    with get_conn(db_path) as conn:
        if _already_recovered(conn):
            log.info(
                "v1.18.5 recovery: runtime_settings marker already "
                "set — skipping. Remove the "
                "`recovery_v55_done_at_v1_18_11` key to re-trigger."
            )
            return stats
        if not _detect_loss_pattern(conn):
            return stats
    stats["detected"] = True
    log.warning(
        "v1.18.5 recovery: detected post-v1.18.0 migration data loss "
        "pattern (themes populated, local_files+placements empty, "
        "plex_items have local_theme_file=1). Walking themes_dir + "
        "Plex folders to rebuild tracking from on-disk state."
    )

    # Snapshot the inputs the walk needs.
    with get_conn(db_path) as conn:
        sections = [
            dict(r) for r in conn.execute(
                "SELECT section_id, themes_subdir, type, is_anime "
                "FROM plex_sections WHERE included = 1 "
                "  AND themes_subdir != ''"
            ).fetchall()
        ]
        themes_rows = [
            dict(r) for r in conn.execute(
                "SELECT id, media_type, tmdb_id, title, year, "
                "       title_norm, upstream_source, youtube_video_id "
                "FROM themes"
            ).fetchall()
        ]
        # Index overrides for the source_kind inference. Each
        # (mt, tmdb) → set of section_ids with an override.
        override_index: dict[tuple, set[str]] = {}
        for r in conn.execute(
            "SELECT media_type, tmdb_id, section_id "
            "FROM user_overrides WHERE youtube_url IS NOT NULL "
            "  AND youtube_url != ''"
        ).fetchall():
            key = (r["media_type"], int(r["tmdb_id"]))
            override_index.setdefault(key, set()).add(r["section_id"])
        # v1.18.11: index ALL plex_items with a folder_path, not
        # just `local_theme_file=1` — the user's repro showed pi
        # rows where local_theme_file was 0 at walker time but
        # the on-disk file definitely existed (the canonical was
        # hardlinked into pi.folder_path/theme.mp3 from the
        # original v1.18.0-era place). Trusting pi.local_theme
        # _file as gospel meant skipping placement inserts for
        # rows whose Plex enumeration was stale. The walker now
        # checks the on-disk file directly per-candidate.
        # Two indexes: by (media_type, section_id) for the
        # title/tmdb fallback path, AND by theme_id for the
        # primary path (orphan resolution by pi.theme_id linkage
        # — repopulated by the v1.18.5 walker's terminal
        # resolve_theme_ids call).
        plex_by_section: dict[tuple, list[dict]] = {}
        plex_by_theme_id: dict[int, list[dict]] = {}
        for r in conn.execute(
            "SELECT rating_key, section_id, media_type, guid_tmdb, "
            "       title_norm, year, folder_path, theme_id "
            "FROM plex_items WHERE folder_path != ''"
        ).fetchall():
            # Translate Plex's media_type ('show') to motif's ('tv').
            mt = "tv" if r["media_type"] == "show" else r["media_type"]
            row = dict(r)
            plex_by_section.setdefault((mt, r["section_id"]), []).append(row)
            if r["theme_id"] is not None:
                plex_by_theme_id.setdefault(
                    int(r["theme_id"]), [],
                ).append(row)

    # v1.18.12: pre-compute disk-check ONCE per pi.folder_path.
    # The v1.18.11 walker called `Path(pi.folder_path)/"theme.mp3"
    # .is_file()` PER (themes_row × candidate) pair which on
    # the user's install was 5,334 themes × thousands of candidates
    # = millions of stat() calls. With NAS-backed I/O at 10-100ms
    # per call, the walk hung past 7 minutes. Caching the disk-
    # check turns it into one-stat-per-pi (max ~16K stats total),
    # bounded regardless of themes count.
    log.info(
        "v1.18.5 recovery: pre-computing disk-state for plex_items "
        "with folder_path != '' (cache build pass)",
    )
    pi_on_disk: dict[str, bool | None] = {}  # rk → exists (None=indeterminate)
    all_pis: list[dict] = []
    for pis in plex_by_section.values():
        all_pis.extend(pis)
    # De-dupe by rating_key (a pi shows up once per index).
    seen_rks: set[str] = set()
    import time as _t
    t0 = _t.monotonic()
    for pi in all_pis:
        rk = pi["rating_key"]
        if rk in seen_rks:
            continue
        seen_rks.add(rk)
        plex_path = Path(pi["folder_path"]) / "theme.mp3"
        try:
            pi_on_disk[rk] = plex_path.is_file()
        except OSError as e:
            # v0.51.15 (audit #21): indeterminate, NOT False — an OSError here
            # (stalled mount, permissions) doesn't mean "file absent"; the old
            # coercion was the exact indeterminate-vs-False class v1.21.42 M2
            # fixed in stat_theme_sidecar. None keeps the consumer conservative
            # (the .get(rk, False) gate skips it) while the breadcrumb makes the
            # one-shot walker's gaps auditable (cold-path rule, v1.18.7).
            pi_on_disk[rk] = None
            log.info(
                "v1.18.5 recovery: sidecar stat indeterminate for rk=%s "
                "(%s): %s — row skipped, not treated as absent",
                rk, plex_path, e,
            )
    log.info(
        "v1.18.5 recovery: disk-state cache built — %d pi rows "
        "checked, %d with on-disk sidecar, %d indeterminate, in %.1fs",
        len(seen_rks),
        sum(1 for v in pi_on_disk.values() if v),
        sum(1 for v in pi_on_disk.values() if v is None),
        _t.monotonic() - t0,
    )

    now_iso = _now_iso()
    lf_rows: list[tuple] = []
    pl_rows: list[tuple] = []

    # v1.18.12: progress logging every PROGRESS_EVERY themes so
    # the user's "7 minutes no logs" black-box experience doesn't
    # repeat. Walk time depends on disk speed; logging every N
    # gives a heartbeat without spamming.
    PROGRESS_EVERY = 250
    walk_t0 = _t.monotonic()

    for t_idx, t in enumerate(themes_rows):
        if t_idx > 0 and t_idx % PROGRESS_EVERY == 0:
            elapsed = _t.monotonic() - walk_t0
            log.info(
                "v1.18.5 recovery: walk progress %d/%d themes "
                "(%.0f%%, %.1fs, %d local_files + %d placements "
                "queued so far)",
                t_idx, len(themes_rows),
                100.0 * t_idx / max(len(themes_rows), 1),
                elapsed, len(lf_rows), len(pl_rows),
            )
        for s in sections:
            # Motif/Plex media_type alignment: themes uses
            # 'movie' / 'tv' / 'collection'; plex_sections.type
            # uses Plex's 'movie' / 'show'. The match below is
            # conservative — anime sections can hold either type
            # but get their own themes_subdir, so the path-walk
            # filter is sufficient.
            canonical_path = _expected_canonical_path(
                themes_dir, s["themes_subdir"],
                t["title"] or "", t["year"],
            )
            if not canonical_path.is_file():
                continue
            try:
                stat = canonical_path.stat()
            except OSError as e:
                # v0.51.15 (audit #20): breadcrumb the skip — the file IS
                # present (is_file above) but unstatable (permissions, mount
                # fault), and this one-shot walker never revisits, so a bare
                # continue left a permanent silent gap (cold-path rule,
                # v1.18.7: every recovery early-return logs WHY).
                log.info(
                    "v1.18.5 recovery: canonical present but stat failed "
                    "for %s/%s (%s): %s — pair skipped",
                    t["media_type"], t["tmdb_id"], canonical_path, e,
                )
                continue
            rel_path = str(canonical_path.relative_to(themes_dir))
            has_override = s["section_id"] in override_index.get(
                (t["media_type"], int(t["tmdb_id"])), set(),
            )
            provenance, source_kind = _infer_source_kind(
                t["upstream_source"], has_override,
            )
            video_id = t["youtube_video_id"] or "recovered"
            # v1.18.11: three-tier placement lookup, fallback
            # cascade. The v1.18.5 walker relied solely on
            # title/tmdb matching + trusted pi.local_theme_file=1
            # as the "Plex sees the sidecar" signal. the user's
            # repro showed orphans (07-Ghost, M*A*S*H) where the
            # original placement had hardlinked a file into
            # pi.folder_path/theme.mp3 but pi.local_theme_file
            # was 0 at walker time (stale plex_enum state), AND
            # title/year match failed because Plex's title_norm
            # didn't exactly match motif's (special chars,
            # year-vs-no-year). Walker now:
            #   1. PRIMARY: walk pi rows linked via theme_id
            #      (set by v1.18.5's terminal resolve_theme_ids
            #      call on this same themes row). For orphans
            #      this catches imdb-id matched rows even when
            #      tmdb/title fail.
            #   2. FALLBACK: title/tmdb in-section scan (the old
            #      v1.18.5 path) for rows where theme_id is
            #      still NULL.
            #   3. DISK CHECK: instead of trusting
            #      pi.local_theme_file, stat the file directly
            #      at pi.folder_path/theme.mp3. Cross-mount
            #      latency is acceptable for a one-shot
            #      recovery walk — accuracy beats speed here.
            # v1.23.74: candidates computed BEFORE the local_files
            # append (moved up) so a foreign-section same-named
            # canonical is rejected before it's attributed. A
            # title+year collision ACROSS media types — the Death
            # Note (2006) live-action MOVIE (tmdb 16007) vs the
            # anime SERIES (tmdb 13916) — let the movie theme claim
            # the show's anime-subdir theme.mp3, minting a cross-
            # section local_files row + (downstream) a broken
            # placement on the show's folder.
            candidates: list[dict] = []
            # Primary: theme_id linkage.
            candidates.extend(plex_by_theme_id.get(int(t["id"]), []))
            # Fallback: title/tmdb match in the same section.
            for pi in plex_by_section.get(
                (t["media_type"], s["section_id"]), [],
            ):
                if pi in candidates:
                    continue
                matches_tmdb = (
                    pi["guid_tmdb"] is not None
                    and int(pi["guid_tmdb"]) == int(t["tmdb_id"])
                )
                matches_title = (
                    pi["title_norm"] is not None
                    and pi["title_norm"] == t["title_norm"]
                    and (pi["year"] or "") == (t["year"] or "")
                )
                if matches_tmdb or matches_title:
                    candidates.append(pi)
            section_candidates = [
                pi for pi in candidates
                if pi["section_id"] == s["section_id"]
            ]
            # v1.23.74: only attribute this canonical to the theme
            # when the section actually holds an item with this
            # theme's identity, OR the section is the theme's home
            # media-type (so a true orphan still recovers in its own
            # section). A movie theme hitting a show-section's same-
            # named subdir has no section candidate and isn't home-
            # type → skip, leaving the file to its real owner.
            if not section_candidates and not _section_is_home_type(
                s["type"], t["media_type"],
            ):
                continue
            lf_rows.append((
                t["media_type"], int(t["tmdb_id"]), s["section_id"],
                rel_path, stat.st_size, now_iso, video_id,
                provenance, source_kind,
            ))
            # v1.18.12: use the pre-built pi_on_disk cache
            # instead of per-candidate stat() calls. Cache lookup
            # is O(1) per pi; the v1.18.11 implementation was
            # O(stat × candidates × themes) which on the user's
            # install (5,334 themes × thousands of candidates)
            # made the walk hang past 7 minutes.
            placed_in_section = False
            for pi in section_candidates:
                if not pi_on_disk.get(pi["rating_key"], False):
                    continue
                pl_rows.append((
                    t["media_type"], int(t["tmdb_id"]),
                    s["section_id"], pi["folder_path"],
                    now_iso, "hardlink", pi["rating_key"],
                    1, provenance,
                ))
                placed_in_section = True
                break  # one placement per (item, section)
            # v1.18.12: dropped the v1.18.11 last-resort "scan by
            # file size match" pass. That pass was O(N²) — for
            # each themes row that hit canonical-on-disk, it
            # walked the entire section's plex_by_section list
            # and stat()'d each candidate. On a populated install
            # (the user's Movies section: 10,381 items) this meant
            # hundreds of millions of stat()s and a 7+ minute
            # hang with no progress signal.
            #
            # The theme_id (primary) and title/tmdb (fallback)
            # tiers above catch the common case. Edge case the
            # last-resort tier was meant to cover — orphans
            # whose Plex title was user-renamed past
            # title_norm match — is exceptionally rare and not
            # worth the perf cost. Users can SET URL manually
            # for those.

    log.warning(
        "v1.18.5 recovery: walk complete — %d local_files rows + "
        "%d placements rows to insert (idempotent: skipped any rows "
        "that already exist post-startup races).",
        len(lf_rows), len(pl_rows),
    )

    # Insert. Use OR IGNORE to be idempotent in case the recovery
    # is re-run or a parallel boot races.
    with get_conn(db_path) as conn, transaction(conn):
        for r in lf_rows:
            try:
                # v1.22.78: per-statement rowcount — total_changes is
                # CUMULATIVE for the connection, so after the first real
                # insert every OR IGNORE skip also counted ("5000 rebuilt"
                # on a half-recovered re-run). cur.rowcount, like the rest
                # of this file.
                cur = conn.execute(
                    "INSERT OR IGNORE INTO local_files "
                    "  (media_type, tmdb_id, section_id, file_path, "
                    "   file_size, downloaded_at, source_video_id, "
                    "   provenance, source_kind) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    r,
                )
                if cur.rowcount:
                    stats["local_files_inserted"] += 1
            except sqlite3.Error as e:
                log.warning("recovery: local_files INSERT failed for "
                            "%s/%s sec=%s: %s",
                            r[0], r[1], r[2], e)
        for r in pl_rows:
            try:
                # v1.22.78: cur.rowcount (see local_files above).
                cur = conn.execute(
                    "INSERT OR IGNORE INTO placements "
                    "  (media_type, tmdb_id, section_id, media_folder, "
                    "   placed_at, placement_kind, plex_rating_key, "
                    "   plex_refreshed, provenance) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    r,
                )
                if cur.rowcount:
                    stats["placements_inserted"] += 1
            except sqlite3.Error as e:
                log.warning("recovery: placements INSERT failed for "
                            "%s/%s sec=%s: %s",
                            r[0], r[1], r[2], e)
        # Stamp the recovery marker so subsequent boots skip.
        # runtime_settings.updated_at is NOT NULL — supply now_iso
        # for both columns so the INSERT doesn't trip the
        # constraint.
        # v1.18.11: bumped marker key from `recovery_v55_done_at`
        # to `recovery_v55_done_at_v1_18_11` so the v1.18.5
        # walker's already-recovered installs re-run the
        # (now improved) placement-scan logic. Existing v1.18.5
        # markers stay in runtime_settings as a historical
        # audit trail.
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES ('recovery_v55_done_at_v1_18_11', ?, ?)",
            (now_iso, now_iso),
        )

    # Re-link plex_items.theme_id from the rebuilt themes.
    try:
        from .plex_enum import resolve_theme_ids
        # resolve_theme_ids returns the count of rows touched;
        # use it as the relinked stat.
        stats["theme_ids_relinked"] = resolve_theme_ids(db_path)
    except Exception as e:  # noqa: BLE001
        log.warning("v1.18.5 recovery: resolve_theme_ids "
                    "failed (%s) — caller can re-run via plex_enum",
                    e)

    log.warning(
        "v1.18.5 recovery: DONE — %d local_files + %d placements "
        "rebuilt; %d plex_items.theme_id re-linked.",
        stats["local_files_inserted"],
        stats["placements_inserted"],
        stats["theme_ids_relinked"],
    )
    return stats


def _now_iso() -> str:
    """Local helper to avoid the events.now_iso import-cycle risk
    (recovery_v55 is imported by main.py at startup; events imports
    db which imports recovery_v55 via lazy use)."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── v1.18.10: user_overrides recovery from events table ───────


def maybe_recover_lost_user_overrides(db_path: Path) -> dict:
    """v1.18.10: secondary recovery for user_overrides rows
    nuked by the post-v1.18.0 sync's "orphan user_overrides
    sweep" running on the v1.18.0-broken DB state.

    ## What happened

    The v1.18.0 FK cascade nuked local_files + placements but
    NOT user_overrides (user_overrides has no FK to themes —
    it's a flat table). However, the next sync ran the v1.12.60
    sweep that DELETEs user_overrides whose "theme presence"
    check fails:

      DELETE FROM user_overrides WHERE
        NOT EXISTS (lf row) AND NOT EXISTS (placements row)
        AND NOT EXISTS (plex_items where guid_tmdb=tmdb_id AND
                        local_theme_file=1) AND NOT EXISTS (job)

    For orphan rows the plex_items presence check ALWAYS fails:
    plex_items.guid_tmdb holds the REAL Plex TMDB id (or NULL),
    not the orphan's synthetic negative tmdb_id (-22, -27, ...).
    So with local_files+placements wiped by the v1.18.0 bug,
    every orphan user_overrides looked stale → deleted.

    the user's sync logs after the v1.18.0 deploy:
      "Pruned 98 orphan user_overrides rows (no theme presence)"

    Result: M*A*S*H 30th Anniversary Reunion (orphan tmdb=-22)
    lost its user_overrides row. The v1.18.5 recovery walker
    correctly recreated local_files + placements but
    `_infer_source_kind` saw upstream='plex_orphan' + no
    user_overrides → classified as ('manual', 'adopt') → row
    renders as M instead of U.

    ## Recovery strategy

    The user-set URL is recoverable from the `events` table:
    every SET URL action logs an event with
    `component='api'`, `message='Manual URL set by admin: <url>'`,
    and `detail={'rating_key': X, 'title': Y}`. Walk events:

      1. For each "Manual URL set by admin" event, extract
         rating_key + URL from message and detail.
      2. Resolve rating_key → plex_items → themes orphan row.
      3. If no user_overrides exists for (themes.media_type,
         themes.tmdb_id, section_id), INSERT one with the URL.
      4. Also UPDATE local_files.source_kind='url' for that row
         so the SRC letter SQL classifies as U (not A).

    Idempotent via INSERT OR IGNORE + a runtime_settings marker
    (`recovery_user_overrides_done_at`).
    """
    from .db import get_conn, transaction
    import re

    stats = {
        "detected": False,
        "user_overrides_inserted": 0,
        "local_files_reclassified": 0,
    }
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_user_overrides_done_at'"
        ).fetchone()
        if row is not None:
            log.info(
                "v1.18.10 user_overrides recovery: marker already "
                "set — skipping. Remove the "
                "`recovery_user_overrides_done_at` key to "
                "re-trigger."
            )
            return stats
        # Pull all "Manual URL set by admin" events. Component
        # 'api' filter narrows the scan; the message-prefix
        # match is the canonical pin (api.py emits exactly this
        # phrase since v1.7.x). Order by id ASC so newer SET URL
        # events overwrite older ones — captures the last URL
        # the user actually set.
        # NB: events column is `detail` (TEXT JSON), not
        # `detail_json` — schema name preserved from v1.0.x.
        events = conn.execute(
            "SELECT id, message, detail, ts "
            "FROM events "
            "WHERE component = 'api' "
            "  AND message LIKE 'Manual URL set by admin:%' "
            "ORDER BY id ASC"
        ).fetchall()
    if not events:
        log.info(
            "v1.18.10 user_overrides recovery: no historical "
            "SET URL events — skipping. Either no overrides "
            "were ever set or events have been pruned."
        )
        return stats
    log.info(
        "v1.18.10 user_overrides recovery: scanning %d "
        "historical SET URL events",
        len(events),
    )
    # Extract (rating_key, url) per event. URL lives in the
    # message after the colon; rating_key in detail_json.
    url_re = re.compile(
        r"Manual URL set by admin:\s*(\S+)"
    )
    # Map rating_key → last-known URL + section_id. The "last"
    # ordering (events ascending) means later events overwrite
    # earlier ones — captures the most recent SET URL the user
    # made for each row.
    rk_to_url: dict[str, dict] = {}
    for ev in events:
        m = url_re.search(ev["message"] or "")
        if not m:
            continue
        url = m.group(1).strip()
        if not url:
            continue
        try:
            detail = json.loads(ev["detail"] or "{}")
        except (TypeError, ValueError) as e:
            # v1.20.63 (class-9 cold-path): a wiped user_override whose
            # reconstruction event has unparseable detail JSON is silently
            # un-recoverable — log so "why didn't recovery restore it" has
            # signal. Boot-time walker, low volume → plain warning.
            log.warning("recovery (lost user_overrides): skipping event — "
                        "bad detail JSON (%s): %.80s", e, ev["detail"] or "")
            continue
        rk = detail.get("rating_key")
        if not rk:
            continue
        rk_to_url[str(rk)] = {"url": url, "ts": ev["ts"]}
    if not rk_to_url:
        log.info(
            "v1.18.10 user_overrides recovery: no extractable "
            "rating_key + URL pairs in events — skipping"
        )
        return stats
    log.info(
        "v1.18.10 user_overrides recovery: extracted %d "
        "(rating_key → URL) mappings from events",
        len(rk_to_url),
    )
    # Resolve each rating_key → plex_items → themes. The themes
    # row's (media_type, tmdb_id) is what user_overrides keys on.
    now_iso = _now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        for rk, info in rk_to_url.items():
            pi = conn.execute(
                "SELECT pi.section_id, pi.media_type, t.media_type "
                "    AS theme_media_type, t.tmdb_id, t.id "
                "    AS theme_id "
                "FROM plex_items pi "
                "INNER JOIN themes t ON t.id = pi.theme_id "
                "WHERE pi.rating_key = ?",
                (rk,),
            ).fetchone()
            if pi is None:
                continue
            section_id = pi["section_id"]
            t_media_type = pi["theme_media_type"]
            t_tmdb_id = int(pi["tmdb_id"])
            # Check if user_overrides already exists. If yes,
            # skip — don't overwrite the user's later choice.
            existing = conn.execute(
                "SELECT 1 FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? "
                "  AND section_id = ?",
                (t_media_type, t_tmdb_id, section_id),
            ).fetchone()
            if existing is not None:
                continue
            # INSERT OR IGNORE — idempotent.
            cur = conn.execute(
                "INSERT OR IGNORE INTO user_overrides "
                "  (media_type, tmdb_id, theme_id, "
                "   youtube_url, set_at, set_by, section_id) "
                "VALUES (?, ?, ?, ?, ?, 'recovery_v1.18.10', ?)",
                (t_media_type, t_tmdb_id, pi["theme_id"],
                 info["url"], info["ts"] or now_iso,
                 section_id),
            )
            if cur.rowcount:
                stats["user_overrides_inserted"] += 1
                # Reclassify the recovered local_files row from
                # source_kind='adopt' → 'url' so SRC renders U
                # not A. The provenance stays 'manual' (no
                # change needed there).
                cur2 = conn.execute(
                    "UPDATE local_files "
                    "   SET source_kind = 'url' "
                    " WHERE media_type = ? AND tmdb_id = ? "
                    "   AND section_id = ? "
                    "   AND source_kind = 'adopt'",
                    (t_media_type, t_tmdb_id, section_id),
                )
                if cur2.rowcount:
                    stats["local_files_reclassified"] += 1
                # v1.18.13: also overwrite the 'recovered'
                # sentinel video id with the real one parsed
                # from the override URL — otherwise the info-
                # card thumbnail builds a 404'ing
                # img.youtube.com/vi/recovered/* URL. Going-
                # forward correctness for installs where this
                # function runs fresh; existing installs (marker
                # already set) get covered by the v1.18.13
                # backfill below.
                from .sync import extract_video_id
                real_vid = extract_video_id(info["url"])
                if real_vid:
                    conn.execute(
                        "UPDATE local_files "
                        "   SET source_video_id = ? "
                        " WHERE media_type = ? AND tmdb_id = ? "
                        "   AND section_id = ? "
                        "   AND source_video_id = 'recovered'",
                        (real_vid, t_media_type, t_tmdb_id,
                         section_id),
                    )
        # Stamp the marker so subsequent boots skip.
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES ('recovery_user_overrides_done_at', ?, ?)",
            (now_iso, now_iso),
        )
    if stats["user_overrides_inserted"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.18.10 user_overrides recovery: DONE — %d "
        "user_overrides re-inserted, %d local_files "
        "reclassified to source_kind='url' (orphans that lost "
        "their U-source attribution in the post-v1.18.0 sync "
        "sweep).",
        stats["user_overrides_inserted"],
        stats["local_files_reclassified"],
    )
    return stats


def maybe_backfill_recovered_video_ids(db_path: Path) -> dict:
    """v1.18.13 — Backfill real source_video_id values for rows
    the v1.18.5 walker stamped with the 'recovered' sentinel.

    The v1.18.5 walker writes `source_video_id = themes.youtube_video_id
    or 'recovered'`. For orphans (no themes.youtube_video_id) the
    placeholder string lands in local_files. The SRC letter SQL
    handles 'recovered' correctly (source_kind='url' takes priority
    over the length check), but the info-card thumbnail builds
    `https://img.youtube.com/vi/{source_video_id}/hqdefault.jpg`
    and 'recovered' yields a 404 → broken-image icon in the UI.

    This backfill walks local_files rows with source_video_id =
    'recovered', joins to user_overrides for the same (media_type,
    tmdb_id, section_id), parses the real YouTube video id (or SC
    'sc-' sentinel) from user_overrides.youtube_url via
    extract_video_id, and updates local_files. Idempotent +
    one-shot via runtime_settings marker
    `recovery_video_ids_done_at`.

    Rows with no matching user_overrides row (orphans the user
    adopted via PROBE/sidecar without ever setting a URL) keep
    their 'recovered' sentinel — there's no source to parse from
    and the SRC letter for those is correctly 'A' (adopt) already."""
    from .db import get_conn, transaction
    from .sync import extract_video_id
    stats = {
        "detected": False,
        "skipped_reason": None,
        "scanned": 0,
        "updated": 0,
    }
    with get_conn(db_path) as conn:
        marker = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_video_ids_done_at'"
        ).fetchone()
        if marker is not None:
            stats["skipped_reason"] = "marker_set"
            log.info(
                "v1.18.13 video_id backfill: marker already set "
                "— skipping. Remove the "
                "`recovery_video_ids_done_at` key to re-trigger."
            )
            return stats
        rows = conn.execute(
            "SELECT lf.media_type, lf.tmdb_id, lf.section_id, "
            "       uo.youtube_url "
            "FROM local_files lf "
            "INNER JOIN user_overrides uo "
            "  ON uo.media_type = lf.media_type "
            " AND uo.tmdb_id   = lf.tmdb_id "
            " AND uo.section_id = lf.section_id "
            "WHERE lf.source_video_id = 'recovered' "
            "  AND uo.youtube_url IS NOT NULL "
            "  AND uo.youtube_url != ''"
        ).fetchall()
    stats["scanned"] = len(rows)
    if not rows:
        # No rows to fix — stamp marker so we don't re-scan every
        # boot, then return. Fresh installs hit this path.
        now_iso = _now_iso()
        with get_conn(db_path) as conn, transaction(conn):
            conn.execute(
                "INSERT OR REPLACE INTO runtime_settings "
                "  (key, value, updated_at) "
                "VALUES ('recovery_video_ids_done_at', ?, ?)",
                (now_iso, now_iso),
            )
        log.info(
            "v1.18.13 video_id backfill: no 'recovered' rows "
            "with a matching user_overrides URL — nothing to "
            "backfill. Marker stamped."
        )
        return stats
    log.warning(
        "v1.18.13 video_id backfill: scanning %d local_files "
        "rows stamped 'recovered' for real video ids extractable "
        "from user_overrides.youtube_url",
        len(rows),
    )
    now_iso = _now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        for r in rows:
            vid = extract_video_id(r["youtube_url"])
            if not vid:
                continue
            cur = conn.execute(
                "UPDATE local_files "
                "   SET source_video_id = ? "
                " WHERE media_type = ? AND tmdb_id = ? "
                "   AND section_id = ? "
                "   AND source_video_id = 'recovered'",
                (vid, r["media_type"], int(r["tmdb_id"]),
                 r["section_id"]),
            )
            if cur.rowcount:
                stats["updated"] += 1
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES ('recovery_video_ids_done_at', ?, ?)",
            (now_iso, now_iso),
        )
    if stats["updated"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.18.13 video_id backfill: DONE — %d/%d local_files "
        "rows updated with real video ids parsed from "
        "user_overrides.youtube_url (info-card thumbnails will "
        "now load instead of 404'ing).",
        stats["updated"], stats["scanned"],
    )
    return stats


def maybe_backfill_edition_canonicals(db_path: Path) -> dict:
    """v1.24.34 — ensure-coverage for historical edition_key='' canonicals.

    the user's Two Towers 409 ("no local file to replace from"): a pre-edition-
    awareness theme landed its canonical/local_files at edition_key='' while the
    live plex_items rows are tagged ({edition-Theatrical} etc.). The edition-
    scoped push / replace / v1.24.29 auto re-push then can't find the canonical
    for the tagged edition → 409, and RP auto-recovery silently skips it. The
    v1.21.96 prod diagnostic found 31 such titles (LotR, the Star Wars fan-edits,
    Watchmen, plus single-edition director's cuts).

    the user's call (AskUserQuestion): ensure-coverage — for each '' canonical
    whose theme has a non-'' live edition LACKING its own local_files, create a
    per-edition local_files row sharing the same canonical file (the movie's
    theme is identical across editions, so no wrong guess). The normal retry
    sweep then places each edition (last_place_attempt_reason=NULL → eligible);
    over-ceiling editions fall back to a sidecar as usual.

    One-shot via the runtime_settings marker `edition_canonical_coverage_done_at`
    so a later deliberate UNMANAGE of an edition isn't resurrected on the next
    boot. Idempotent within the run (NOT EXISTS gate). Amplifier-sweep guarded:
    an implausibly large gap set (a broken state) aborts without writing."""
    from .db import get_conn, transaction
    stats = {"detected": False, "skipped_reason": None,
             "gaps": 0, "created": 0}
    with get_conn(db_path) as conn:
        marker = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'edition_canonical_coverage_done_at'").fetchone()
        if marker is not None:
            stats["skipped_reason"] = "marker_set"
            log.info("v1.24.34 edition-coverage: marker already set — skipping. "
                     "Remove `edition_canonical_coverage_done_at` to re-trigger.")
            return stats
        gaps = conn.execute(
            """
            SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.theme_id,
                   lf.file_path, lf.file_sha256, lf.file_size, lf.source_video_id,
                   lf.provenance, lf.source_kind, lf.canonical_present,
                   COALESCE(t.title, '') AS title, pi.edition_key AS need_edition
            FROM local_files lf
            JOIN themes t ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id
            JOIN plex_items pi ON pi.theme_id = t.id
                 AND pi.section_id = lf.section_id AND pi.edition_key != ''
            WHERE lf.edition_key = ''
              AND NOT EXISTS (
                  SELECT 1 FROM local_files lf2
                  WHERE lf2.media_type = lf.media_type
                    AND lf2.tmdb_id = lf.tmdb_id
                    AND lf2.section_id = lf.section_id
                    AND lf2.edition_key = pi.edition_key
              )
            GROUP BY lf.media_type, lf.tmdb_id, lf.section_id, pi.edition_key
            """).fetchall()
        total_lf = conn.execute("SELECT COUNT(*) FROM local_files").fetchone()[0]
    stats["gaps"] = len(gaps)
    now = _now_iso()
    if not gaps:
        with get_conn(db_path) as conn, transaction(conn):
            conn.execute("INSERT OR REPLACE INTO runtime_settings "
                         "(key, value, updated_at) "
                         "VALUES ('edition_canonical_coverage_done_at', ?, ?)",
                         (now, now))
        log.info("v1.24.34 edition-coverage: no '' canonicals missing an edition "
                 "sibling — nothing to backfill. Marker stamped.")
        return stats
    # v1.18.10 amplifier-sweep guard: a sane library has a handful of these;
    # a flood means broken state (a mass-NULL'd edition_key) — abort + don't
    # stamp, so a fixed build can retry.
    cap = max(200, total_lf // 4)
    if len(gaps) > cap:
        stats["skipped_reason"] = "implausible_gap_count"
        log.warning("v1.24.34 edition-coverage: %d gaps exceed cap %d (suspected "
                    "broken state) — aborting WITHOUT writing or stamping.",
                    len(gaps), cap)
        return stats
    log.warning("v1.24.34 edition-coverage: backfilling %d per-edition canonical(s) "
                "from '' siblings (ensure-coverage)", len(gaps))
    with get_conn(db_path) as conn, transaction(conn):
        for g in gaps:
            cur = conn.execute(
                """INSERT OR IGNORE INTO local_files
                     (media_type, tmdb_id, section_id, edition_key, theme_id,
                      file_path, file_sha256, file_size, source_video_id,
                      provenance, source_kind, downloaded_at, canonical_present,
                      last_place_attempt_reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
                (g["media_type"], g["tmdb_id"], g["section_id"], g["need_edition"],
                 g["theme_id"], g["file_path"], g["file_sha256"], g["file_size"],
                 g["source_video_id"], g["provenance"], g["source_kind"], now,
                 g["canonical_present"]))
            if cur.rowcount:
                stats["created"] += 1
            log.info("v1.24.34 edition-coverage: '%s' canonical for '%s' "
                     "(%s/%s section=%s) shares the '' theme — normal place will "
                     "deploy it", g["need_edition"], g["title"], g["media_type"],
                     g["tmdb_id"], g["section_id"])
        conn.execute("INSERT OR REPLACE INTO runtime_settings "
                     "(key, value, updated_at) "
                     "VALUES ('edition_canonical_coverage_done_at', ?, ?)",
                     (now, now))
    stats["detected"] = stats["created"] > 0
    log.warning("v1.24.34 edition-coverage: DONE — created %d/%d per-edition "
                "canonical(s); each edition is now push-able + auto-restorable.",
                stats["created"], stats["gaps"])
    return stats


def maybe_relink_orphan_theme_ids(db_path: Path) -> dict:
    """v1.18.16 — Re-link plex_items.theme_id for rows whose
    linkage was nuked by the resolve_theme_ids NULL-overwrite bug.

    Pre-fix the sql_tmdb pass in plex_enum.resolve_theme_ids wrote
    the subquery result UNCONDITIONALLY. For orphan-promoted rows
    that had pi.theme_id stamped by /manual-url (the row's first
    SET URL) AND a real pi.guid_tmdb (Plex provided the TMDB id),
    the subquery filtered `upstream_source != 'plex_orphan'` and
    returned NULL — the UPDATE then wrote NULL to theme_id,
    destroying the orphan→synthetic-themes-row linkage.

    The library JOIN then chained NULL through pi.theme_id →
    t.* NULL → p.* NULL → lf.* NULL, so even rows with valid
    placements + canonical files rendered as SRC=P + PL=amber
    (the bottom-of-CASE fallback in _SRC_LETTER_SQL).

    The fix in plex_enum.py wraps each resolve pass in COALESCE
    so a no-match preserves existing linkage. This backfill
    repairs already-broken installs by scanning every orphan
    themes row + finding plex_items rows that should point to it
    but have NULL theme_id. Matched via title_norm + media_type
    + section overlap (collections are section-scoped; orphan
    movies/shows are too).

    Idempotent + one-shot via marker
    `recovery_orphan_theme_id_relink_done_at`.
    """
    from .db import get_conn, transaction
    stats = {
        "detected": False,
        "skipped_reason": None,
        "scanned": 0,
        "relinked": 0,
    }
    with get_conn(db_path) as conn:
        marker = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_orphan_theme_id_relink_done_at'"
        ).fetchone()
        if marker is not None:
            stats["skipped_reason"] = "marker_set"
            log.info(
                "v1.18.16 orphan theme_id relink: marker already "
                "set — skipping. Remove the "
                "`recovery_orphan_theme_id_relink_done_at` key "
                "to re-trigger."
            )
            return stats
        # Find orphan themes rows that have placement/local_files
        # rows tracked under them but no linked plex_items rows.
        # The placement/local_files presence confirms a user
        # interacted with the synthetic theme — the JOIN chain
        # broke afterward.
        candidates = conn.execute(
            "SELECT t.id, t.media_type, t.tmdb_id, t.title_norm "
            "  FROM themes t "
            " WHERE t.upstream_source = 'plex_orphan' "
            "   AND EXISTS ("
            "     SELECT 1 FROM placements p "
            "      WHERE p.media_type = t.media_type "
            "        AND p.tmdb_id = t.tmdb_id) "
            "   AND NOT EXISTS ("
            "     SELECT 1 FROM plex_items pi "
            "      WHERE pi.theme_id = t.id)"
        ).fetchall()
    stats["scanned"] = len(candidates)
    if not candidates:
        now_iso = _now_iso()
        with get_conn(db_path) as conn, transaction(conn):
            conn.execute(
                "INSERT OR REPLACE INTO runtime_settings "
                "  (key, value, updated_at) "
                "VALUES ('recovery_orphan_theme_id_relink_done_at', ?, ?)",
                (now_iso, now_iso),
            )
        log.info(
            "v1.18.16 orphan theme_id relink: no orphan themes "
            "with broken plex_items linkage found — nothing to "
            "relink. Marker stamped."
        )
        return stats
    log.warning(
        "v1.18.16 orphan theme_id relink: %d orphan themes rows "
        "have placements + local_files but no linked plex_items "
        "— attempting re-link via title_norm + media_type",
        len(candidates),
    )
    now_iso = _now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        for theme in candidates:
            # plex_items.media_type uses 'show'/'movie'/'collection'.
            # themes.media_type uses 'tv'/'movie'/'collection'.
            # Map t.media_type → pi.media_type.
            if theme["media_type"] == "tv":
                pi_media_type = "show"
            else:
                pi_media_type = theme["media_type"]
            # Match on title_norm. Collections aren't year-bound;
            # for movies/shows the t.year vs pi.year disagreement
            # is rare for orphans (orphan was created from this
            # plex_items row originally), so title_norm alone is a
            # safe match. Narrowed to rows with NULL theme_id so
            # we don't clobber valid linkages.
            cur = conn.execute(
                "UPDATE plex_items SET theme_id = ? "
                " WHERE media_type = ? "
                "   AND title_norm = ? "
                "   AND theme_id IS NULL",
                (theme["id"], pi_media_type, theme["title_norm"]),
            )
            if cur.rowcount:
                stats["relinked"] += cur.rowcount
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES ('recovery_orphan_theme_id_relink_done_at', ?, ?)",
            (now_iso, now_iso),
        )
    if stats["relinked"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.18.16 orphan theme_id relink: DONE — %d plex_items "
        "rows relinked across %d candidate themes (collections "
        "with broken JOIN should now render SRC=U/T + PL=on "
        "after this boot's plex_enum cycle).",
        stats["relinked"], stats["scanned"],
    )
    return stats


def maybe_repair_collection_linkage(db_path: Path) -> dict:
    """v1.18.19 — Two-step repair for collections (and other rows)
    whose pi.theme_id linkage is broken after the pre-v1.18.16
    sql_tmdb NULL-overwrite bug.

    Step 1: backfill themes.title_norm for orphan themes rows
    that were created via /manual-url before the v1.18.19 fix.
    The pre-fix INSERT omitted title_norm, so resolve_theme_ids'
    sql_collection_title pass (which JOINs on title_norm) could
    never match them.

    Step 2: invoke resolve_theme_ids(db_path) once. With the
    v1.18.16 COALESCE in place, the four passes (sql_tmdb,
    sql_imdb, sql_title, sql_collection_title) re-link every
    pi.theme_id=NULL row by tmdb / imdb / title — preserving any
    valid linkage that already exists. This unbreaks the user's
    Asterix (real TDB row whose pre-v1.18.16 sql_tmdb passed
    NULL through) AND, post step 1, his Wonka + 101 Dalmatians
    orphans.

    Idempotent + one-shot via marker
    `recovery_collection_linkage_done_at_v1_18_19`.
    """
    from .db import get_conn, transaction
    stats = {
        "detected": False,
        "skipped_reason": None,
        "themes_title_norm_backfilled": 0,
        "resolve_called": False,
    }
    with get_conn(db_path) as conn:
        marker = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_collection_linkage_done_at_v1_18_19'"
        ).fetchone()
        if marker is not None:
            stats["skipped_reason"] = "marker_set"
            log.info(
                "v1.18.19 collection linkage repair: marker "
                "already set — skipping. Remove the "
                "`recovery_collection_linkage_done_at_v1_18_19` "
                "key to re-trigger."
            )
            return stats
    # Step 1: backfill themes.title_norm where it's missing.
    try:
        from .normalize import normalize_title
    except Exception as e:  # noqa: BLE001
        log.warning(
            "v1.18.19 collection linkage repair: normalize_title "
            "unavailable (%s) — skipping title_norm backfill.", e,
        )
        normalize_title = None  # type: ignore
    backfilled = 0
    if normalize_title is not None:
        with get_conn(db_path) as conn, transaction(conn):
            rows = conn.execute(
                "SELECT id, title FROM themes "
                "WHERE (title_norm IS NULL OR title_norm = '') "
                "  AND title IS NOT NULL AND title != ''"
            ).fetchall()
            normalize_fallbacks = 0
            for r in rows:
                # v1.18.21: log the per-row fallback so a future
                # debug session can spot when title_norm got
                # populated via the lossy `.lower()` path. Bulk-
                # aggregate at end of loop to avoid log spam on
                # large libraries.
                try:
                    tn = normalize_title(r["title"] or "")
                except Exception:
                    tn = (r["title"] or "").lower()
                    normalize_fallbacks += 1
                conn.execute(
                    "UPDATE themes SET title_norm = ? WHERE id = ?",
                    (tn, r["id"]),
                )
                backfilled += 1
            if normalize_fallbacks:
                log.warning(
                    "v1.18.19 collection linkage repair: %d/%d "
                    "rows fell back to .lower() because "
                    "normalize_title raised — those rows may "
                    "miss the sql_collection_title JOIN.",
                    normalize_fallbacks, backfilled,
                )
    stats["themes_title_norm_backfilled"] = backfilled
    if backfilled:
        log.warning(
            "v1.18.19 collection linkage repair: backfilled "
            "title_norm on %d themes rows (orphan rows created "
            "by /manual-url pre-v1.18.19 missed this column)",
            backfilled,
        )
    # Step 2: invoke resolve_theme_ids to re-link every
    # pi.theme_id=NULL row via the four-pass cascade. The
    # v1.18.16 COALESCE preserves existing valid linkages.
    try:
        from .plex_enum import resolve_theme_ids
        n_updated = resolve_theme_ids(db_path)
        stats["resolve_called"] = True
        stats["resolve_updated"] = n_updated
        if n_updated:
            stats["detected"] = True
        log.warning(
            "v1.18.19 collection linkage repair: "
            "resolve_theme_ids → %d plex_items rows touched",
            n_updated,
        )
    except Exception as e:  # noqa: BLE001
        log.warning(
            "v1.18.19 collection linkage repair: "
            "resolve_theme_ids raised (%s) — startup continues; "
            "linkage may remain broken until the next manual "
            "REFRESH triggers plex_enum.", e,
        )
    # v1.18.21: only stamp the marker on a successful resolve.
    # Pre-fix the marker was stamped UNCONDITIONALLY — if
    # resolve_theme_ids raised (DB lock, permission error, etc.)
    # the repair was effectively abandoned but the marker blocked
    # any re-run. Operator had to manually delete the runtime
    # setting to retry. New behavior: on resolve failure the
    # marker stays absent so the next boot retries.
    if stats["resolve_called"]:
        now_iso = _now_iso()
        with get_conn(db_path) as conn, transaction(conn):
            conn.execute(
                "INSERT OR REPLACE INTO runtime_settings "
                "  (key, value, updated_at) "
                "VALUES ('recovery_collection_linkage_done_at_v1_18_19', "
                "        ?, ?)",
                (now_iso, now_iso),
            )
    else:
        log.warning(
            "v1.18.19 collection linkage repair: marker NOT "
            "stamped because resolve_theme_ids failed — next "
            "boot will retry the repair."
        )
    return stats


def maybe_clear_plex_upload_indep_flag(db_path: Path) -> dict:
    """v1.18.22 — Backfill: clear plex_independent_theme=1 on rows
    whose placement is plex_upload. the user's collections post-
    v1.18.16 hit the false +P stamp because plex_enum's
    discriminator (no sidecar AND has_theme → "Plex serves its
    own theme") misfires for plex_upload placements — the audio
    Plex serves IS motif's upload, not Plex's own. The v1.18.22
    plex_enum.py fix prevents NEW occurrences; this one-shot
    backfill clears the flag on already-stamped rows.

    Idempotent + one-shot via marker
    `recovery_plex_upload_indep_clear_done_at_v1_18_22`.
    """
    from .db import get_conn, transaction
    stats = {
        "detected": False,
        "skipped_reason": None,
        "rows_cleared": 0,
    }
    with get_conn(db_path) as conn:
        marker = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_plex_upload_indep_clear_done_at_v1_18_22'"
        ).fetchone()
        if marker is not None:
            stats["skipped_reason"] = "marker_set"
            log.info(
                "v1.18.22 plex_upload indep-flag clear: marker "
                "already set — skipping. Remove the runtime "
                "setting key to re-trigger."
            )
            return stats
    now_iso = _now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        # Clear plex_independent_theme on every plex_items row
        # whose linked themes record has a plex_upload placement
        # in the same section. Single UPDATE — no per-row JOIN
        # subquery loop.
        cur = conn.execute(
            "UPDATE plex_items "
            "   SET plex_independent_theme = 0 "
            " WHERE plex_independent_theme = 1 "
            "   AND rating_key IN ( "
            "     SELECT pi.rating_key FROM plex_items pi "
            "     INNER JOIN themes t ON t.id = pi.theme_id "
            "     INNER JOIN placements p "
            "       ON p.media_type = t.media_type "
            "      AND p.tmdb_id = t.tmdb_id "
            "      AND p.section_id = pi.section_id "
            # v1.22.25 (edition audit hardening): match the plex_upload to
            # THIS plex_items row's edition — pre-fix the join was section-
            # only, so a plex_upload on one edition cleared the +P
            # plex_independent_theme flag on every sibling edition's row in
            # the section. Unreachable today (marker-gated one-shot) but
            # correct if an operator clears the marker to re-run on a multi-
            # edition library.
            "      AND p.edition_key = pi.edition_key "
            "     WHERE p.placement_kind = 'plex_upload')"
        )
        stats["rows_cleared"] = cur.rowcount
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES ('recovery_plex_upload_indep_clear_done_at_v1_18_22', "
            "        ?, ?)",
            (now_iso, now_iso),
        )
    if stats["rows_cleared"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.18.22 plex_upload indep-flag clear: %d plex_items "
        "rows had plex_independent_theme=1 cleared (false +P "
        "stamps from pre-v1.18.22 plex_enum on plex_upload "
        "placements). Rows now render without the +P composite.",
        stats["rows_cleared"],
    )
    return stats


# ── v1.18.83: bulk-import recovery from audit_events ────────────


def maybe_recover_lost_bulk_imports(db_path: Path) -> dict:
    """v1.18.83 — sibling to v1.18.10's user_overrides walker.

    ## Why a second walker

    v1.18.10's `maybe_recover_lost_user_overrides` only scans the
    `events` table for messages matching
    `'Manual URL set by admin:%'`. The per-row SET URL endpoint
    emits exactly that string via `log_event`, so v1.18.10's
    walker successfully recovers per-row overrides.

    But the **bulk CSV-import** endpoint (`/api/import/apply`)
    NEVER writes to the `events` table — only to `audit_events`,
    with details `{"source":"import", ...}`. So when v1.18.0's
    cascade + v1.12.60's orphan sweep wiped user_overrides,
    bulk-imported URLs were left unrecoverable by v1.18.10.

    the user's repro: bulk-imported a CSV of anime URLs on 5/17,
    saw the rows land as U. After the v1.18.0 cascade hit, the
    v1.18.5 walker rebuilt local_files as `source_kind='adopt'`
    + `source_video_id='recovered'` (the orphan-with-no-override
    pattern). v1.18.10 ran, found no matching `events` rows,
    skipped → bulk-imported rows stayed as A (adopted) instead
    of U. Direct DB query 2026-05-22 confirmed: 17 import
    audit_events present, 0 user_overrides for the orphan IDs.

    ## Recovery strategy

    Walk `audit_events` WHERE action='set_url' AND details
    contains `"source":"import"`. For each:
      1. Build (media_type, tmdb_id) → latest URL map (later
         imports overwrite earlier — captures most recent intent).
      2. Skip if a user_overrides row already exists for that
         (mt, tmdb_id, section_id='') — don't overwrite a later
         choice the user made by hand.
      3. INSERT user_overrides at section_id='' (bulk import's
         canonical write location).
      4. Reclassify local_files.source_kind='adopt' → 'url' for
         the matching (mt, tmdb_id) rows that ALSO have
         source_video_id='recovered' (v1.18.5 walker signature
         — narrow filter avoids touching legitimately-adopted
         rows).
      5. Backfill source_video_id from the URL — mirrors v1.18.10
         to fix the info-card thumbnail 404 (CLAUDE.md class 9
         FK cascade sub-pattern).

    Idempotent via marker `recovery_bulk_imports_done_at`. Safe
    to re-run on installs that never had the bug.
    """
    from .db import get_conn, transaction
    import json

    stats = {
        "detected": False,
        "audits_scanned": 0,
        "user_overrides_inserted": 0,
        "local_files_reclassified": 0,
        "video_ids_backfilled": 0,
    }
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_bulk_imports_done_at'"
        ).fetchone()
        if row is not None:
            log.info(
                "v1.18.83 bulk-import recovery: marker already "
                "set — skipping. Remove the "
                "`recovery_bulk_imports_done_at` key to "
                "re-trigger."
            )
            return stats
        # Pull all bulk-import audits. The details JSON contains
        # `"source":"import"` (api.py:api_import_apply); the
        # LIKE prefilter narrows the scan before JSON parse.
        # Order by id ASC so later imports overwrite earlier
        # ones in the map.
        audits = conn.execute(
            "SELECT id, occurred_at, media_type, tmdb_id, "
            "       section_id, details "
            "FROM audit_events "
            "WHERE action = 'set_url' "
            "  AND details LIKE '%\"source\":\"import\"%' "
            "ORDER BY id ASC"
        ).fetchall()
    stats["audits_scanned"] = len(audits)
    if not audits:
        log.info(
            "v1.18.83 bulk-import recovery: no import audits "
            "to replay — skipping marker stamp so a future "
            "bulk import + cascade can still recover."
        )
        return stats
    log.info(
        "v1.18.83 bulk-import recovery: replaying %d import "
        "audit_events into user_overrides",
        len(audits),
    )
    # Build (mt, tmdb_id) → latest url. Later wins.
    target_to_url: dict[tuple, str] = {}
    for a in audits:
        mt = a["media_type"]
        tid = a["tmdb_id"]
        if mt is None or tid is None:
            continue
        try:
            details = json.loads(a["details"] or "{}")
        except (TypeError, ValueError) as e:
            # v1.20.63 (class-9 cold-path): bulk-import audit row with
            # unparseable details JSON → that imported URL is silently
            # un-recoverable. Boot-time walker → plain warning.
            log.warning("recovery (lost bulk imports): skipping audit row — "
                        "bad details JSON (%s): %.80s", e, a["details"] or "")
            continue
        url = (details.get("new_url") or "").strip()
        if not url:
            continue
        target_to_url[(mt, int(tid))] = url
    now_iso = _now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        for (mt, tid), url in target_to_url.items():
            # Don't overwrite an existing override — could be a
            # later manual SET URL the user explicitly made.
            existing = conn.execute(
                "SELECT 1 FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? "
                "  AND section_id = ''",
                (mt, tid),
            ).fetchone()
            if existing is not None:
                continue
            # Resolve theme_id (orphan or real) so the override
            # FK lands cleanly.
            theme_row = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? "
                "  AND tmdb_id = ?",
                (mt, tid),
            ).fetchone()
            theme_id = theme_row["id"] if theme_row else None
            cur = conn.execute(
                "INSERT OR IGNORE INTO user_overrides "
                "  (media_type, tmdb_id, theme_id, "
                "   youtube_url, set_at, set_by, "
                "   section_id, note) "
                "VALUES (?, ?, ?, ?, ?, "
                "        'recovery_v1.18.83', '', "
                "        'bulk-import recovery v1.18.83')",
                (mt, tid, theme_id, url, now_iso),
            )
            if cur.rowcount:
                stats["user_overrides_inserted"] += 1
                # Reclassify local_files from adopt → url. Narrow
                # the WHERE to BOTH source_kind='adopt' AND
                # source_video_id='recovered' — that combo is the
                # v1.18.5 walker's distinct signature, so legit
                # adopt rows (without the recovered sentinel) are
                # left alone.
                cur2 = conn.execute(
                    "UPDATE local_files "
                    "   SET source_kind = 'url' "
                    " WHERE media_type = ? AND tmdb_id = ? "
                    "   AND source_kind = 'adopt' "
                    "   AND source_video_id = 'recovered'",
                    (mt, tid),
                )
                if cur2.rowcount:
                    stats["local_files_reclassified"] += cur2.rowcount
                # Backfill source_video_id with the real video id
                # parsed from the URL (fix info-card thumbnail
                # 404 — mirror v1.18.10's v1.18.13-era handling).
                from .sync import extract_video_id
                real_vid = extract_video_id(url)
                if real_vid:
                    cur3 = conn.execute(
                        "UPDATE local_files "
                        "   SET source_video_id = ? "
                        " WHERE media_type = ? AND tmdb_id = ? "
                        "   AND source_video_id = 'recovered'",
                        (real_vid, mt, tid),
                    )
                    if cur3.rowcount:
                        stats["video_ids_backfilled"] += cur3.rowcount
        # Stamp marker last so partial failures retry.
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES ('recovery_bulk_imports_done_at', ?, ?)",
            (now_iso, now_iso),
        )
    if stats["user_overrides_inserted"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.18.83 bulk-import recovery: DONE — scanned %d "
        "import audits, re-inserted %d user_overrides, "
        "reclassified %d local_files adopt→url, backfilled "
        "%d source_video_ids (orphans whose bulk-import "
        "attribution was lost in the v1.18.0 cascade + "
        "missed by v1.18.10's events-only walker).",
        stats["audits_scanned"],
        stats["user_overrides_inserted"],
        stats["local_files_reclassified"],
        stats["video_ids_backfilled"],
    )
    return stats


# ── v1.19.13 + v1.19.14: adopt-attribution recovery ───────────


# v1.19.14: marker key bumped (was `recovery_lost_adopts_done_at`)
# so installs that already ran v1.19.13 re-run with the refined
# post-adopt T-intent check. Mirrors v1.18.11's marker-key bump
# pattern when a walker's logic improves between tags.
_ADOPT_RECOVERY_MARKER_KEY = "recovery_lost_adopts_done_at_v1_19_14"


def maybe_recover_lost_adopts(
    db_path: Path, themes_dir: Path,
) -> dict:
    """v1.19.13 + v1.19.14 — third sibling to v1.18.10 +
    v1.18.83 walkers.

    ## Why a third walker

    v1.18.10 recovers per-row SET URL overrides (events table,
    "Manual URL set by admin:%"). v1.18.83 recovers bulk CSV
    imports (audit_events table, source=import). Both target
    user_overrides rows wiped by the post-v1.18.0 orphan sweep.

    The THIRD class of casualty: inline ADOPT actions. The v1.18.0
    FK cascade wiped local_files; v1.18.5's recovery walker
    rebuilt missing rows via `_infer_source_kind`, which only
    knows three states:
      * TDB-tracked + no override → ('auto', 'themerrdb') → T
      * TDB-tracked + override    → ('manual', 'url')      → U
      * plex_orphan               → ('manual', 'adopt')    → A

    For a TDB-tracked row that was originally ADOPTED (user
    hand-picked a sidecar that happened to share a YouTube ID
    with the TDB record), the walker had no signal to know the
    'adopt' provenance — it landed at 'themerrdb' (T). Until
    that row's TDB URL goes dead, the misclass is invisible.

    the user's instance count when v1.19.13 first landed: **228 rows
    misclassified** out of 340 historical adopt events (~67%).

    ## v1.19.14 refinement: post-adopt T-intent gate

    v1.19.13 reclassified 224 rows. Diagnostic showed 17 of those
    (~5%) were over-flipped: user adopted, then later clicked
    REPLACE-WITH-THEMERRDB (an explicit A→T intent shift). The
    sha-match check still fired because the TDB URL happens to
    serve byte-identical audio to the original adopted sidecar
    (common when user adopted a file that itself came from the
    same YouTube video). Net effect: walker overrode a user
    intent with a stale signal.

    Fix: BEFORE the sha check, query events for any T-intent
    entry POSTDATING the latest adopt event for the (mt, tmdb,
    section):
      * component='download', message LIKE 'Downloaded theme%'
      * component='adopt', message LIKE 'Replace-with-ThemerrDB%'

    If found, leave the row classified as 'themerrdb' (or flip
    back to 'themerrdb' if v1.19.13 already over-flipped it to
    'adopt'). Without a post-adopt T-intent event, fall through
    to the original v1.19.13 sha-match logic.

    ## Recovery strategy

    Walk `events` WHERE component='adopt' AND message LIKE
    'Inline adopt of sidecar at%'. Each event's detail JSON
    carries the sha256 of the file at adopt time. For each
    (media_type, tmdb_id, section_id):

      1. Pick the LATEST adopt event (handles re-adopts).
      2. **v1.19.14 gate**: query for post-adopt T-intent
         events. If found:
           - Current source_kind='adopt' → flip BACK to
             'themerrdb' (undoes v1.19.13 over-flip).
           - Current source_kind='themerrdb' → leave alone.
           - Other → leave alone.
      3. No post-adopt T-intent → proceed with v1.19.13 logic:
         only act on source_kind='themerrdb', compute on-disk
         sha, reclassify to 'adopt' on match.

    Idempotent via marker `recovery_lost_adopts_done_at_v1_19_14`
    (bumped from v1.19.13's `recovery_lost_adopts_done_at` so
    installs that ran v1.19.13 re-run with the refined logic).

    Cost: ~228 SHA-256 hashes of 1-10MB files = 1-2GB streamed,
    1-2 min on typical install. T-intent query is per-candidate
    but bounded — covered by the (media_type, tmdb_id) index on
    events.
    """
    from .db import get_conn, transaction
    import hashlib

    stats = {
        "detected": False,
        "adopt_events_scanned": 0,
        "candidates": 0,
        "reclassified": 0,
        "reverted_v1_19_13_overflips": 0,
        "skipped_post_adopt_t_intent": 0,
        "sha_mismatch": 0,
        "file_missing": 0,
        "skipped_already_adopt": 0,
        "skipped_other_source_kind": 0,
        "skipped_no_local_files": 0,
        "skipped_no_sha": 0,
    }
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings WHERE key = ?",
            (_ADOPT_RECOVERY_MARKER_KEY,),
        ).fetchone()
        if row is not None:
            log.info(
                "v1.19.14 adopt recovery: marker already set — "
                "skipping. Remove the `%s` key to re-trigger.",
                _ADOPT_RECOVERY_MARKER_KEY,
            )
            return stats
        # Pull all inline-adopt events. The canonical message
        # "Inline adopt of sidecar at <path>" is pinned in
        # adopt.py:adopt_folder since v1.10.9. Order by ts ASC
        # so the LATEST event for each (mt, tmdb, section) wins
        # the dedup map.
        events = conn.execute(
            "SELECT id, ts, media_type, tmdb_id, detail "
            "FROM events "
            "WHERE component = 'adopt' "
            "  AND message LIKE 'Inline adopt of sidecar at%' "
            "ORDER BY ts ASC, id ASC"
        ).fetchall()
    stats["adopt_events_scanned"] = len(events)
    if not events:
        log.info(
            "v1.19.14 adopt recovery: no inline-adopt events in "
            "log — skipping marker stamp so a future adopt+cascade "
            "can still recover."
        )
        return stats
    log.info(
        "v1.19.14 adopt recovery: scanning %d historical "
        "inline-adopt events",
        len(events),
    )
    # Dedup map keyed by (media_type, tmdb_id, section_id) →
    # most-recent event's (sha256, ts). ASC ordering means the
    # last write wins, capturing the user's latest intent.
    candidates: dict[tuple, dict] = {}
    for ev in events:
        mt = ev["media_type"]
        tid = ev["tmdb_id"]
        if mt is None or tid is None:
            continue
        try:
            detail = json.loads(ev["detail"] or "{}")
        except (TypeError, ValueError) as e:
            # v1.20.63 (class-9 cold-path): adopt event with unparseable
            # detail JSON → that adopt is silently un-recoverable. Boot-time
            # walker → plain warning.
            log.warning("recovery (lost adopts): skipping event — "
                        "bad detail JSON (%s): %.80s", e, ev["detail"] or "")
            continue
        sha = (detail.get("sha256") or "").strip()
        sec = detail.get("section_id")
        if not sha or not sec:
            continue
        candidates[(mt, int(tid), str(sec))] = {
            "sha256": sha, "ts": ev["ts"],
        }
    stats["candidates"] = len(candidates)
    if not candidates:
        log.info(
            "v1.19.14 adopt recovery: no usable (mt, tmdb, "
            "section, sha256) tuples in adopt events — skipping."
        )
        return stats
    log.info(
        "v1.19.14 adopt recovery: %d unique adopt targets to "
        "verify against on-disk state",
        len(candidates),
    )
    now_iso = _now_iso()
    # Hash files OUTSIDE the write txn so a slow disk doesn't
    # hold a write lock for the whole walk. Build the action
    # list first, then apply in a single transaction.
    # actions: list of (op, mt, tid, sec, sha) where op ∈
    # {'reclassify', 'revert_overflip'}.
    actions: list[tuple] = []
    with get_conn(db_path) as conn:
        for (mt, tid, sec), info in candidates.items():
            # v0.50.89 (edition audit): these historical adopt events predate
            # edition_key entirely, so the row they describe can only ever be
            # the standard ('') edition — scoping here (and on both UPDATEs
            # below) the same way the v1.22.25 sibling walkers did, so a
            # title that's since split into multiple editions doesn't bleed
            # this stale signal onto a genuinely different edition's row.
            lf = conn.execute(
                "SELECT source_kind, file_path "
                "FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ? "
                "  AND section_id = ? AND edition_key = ''",
                (mt, tid, sec),
            ).fetchone()
            if lf is None:
                stats["skipped_no_local_files"] += 1
                continue
            # v1.19.14 gate: check for post-adopt T-intent events
            # BEFORE the sha-match check. The user's most-recent
            # action wins over a stale adopt-time signal.
            t_intent = conn.execute(
                "SELECT MAX(ts) AS latest_ts FROM events "
                "WHERE media_type = ? AND tmdb_id = ? "
                "  AND ts > ? "
                "  AND ((component = 'download' AND message LIKE "
                "         'Downloaded theme%') "
                "    OR (component = 'adopt' AND message LIKE "
                "         'Replace-with-ThemerrDB%'))",
                (mt, tid, info["ts"]),
            ).fetchone()
            post_adopt_t_intent = (
                t_intent is not None
                and t_intent["latest_ts"] is not None
            )
            if post_adopt_t_intent:
                # User clicked REPLACE-W-TDB or a TDB download
                # succeeded after the adopt — the row's last
                # expressed intent was T. Honor it.
                if lf["source_kind"] == "adopt":
                    # v1.19.13 over-flipped this row. Reverse.
                    actions.append(
                        ("revert_overflip", mt, tid, sec, None),
                    )
                    log.info(
                        "v1.19.14 adopt recovery: reverting "
                        "v1.19.13 over-flip for %s/%s sec=%s — "
                        "post-adopt T-intent event at %s "
                        "supersedes adopt at %s",
                        mt, tid, sec, t_intent["latest_ts"],
                        info["ts"],
                    )
                else:
                    stats["skipped_post_adopt_t_intent"] += 1
                continue
            if lf["source_kind"] == "adopt":
                stats["skipped_already_adopt"] += 1
                continue
            if lf["source_kind"] != "themerrdb":
                stats["skipped_other_source_kind"] += 1
                continue
            if not lf["file_path"]:
                stats["skipped_no_sha"] += 1
                continue
            disk_path = themes_dir / lf["file_path"]
            if not disk_path.is_file():
                stats["file_missing"] += 1
                log.info(
                    "v1.19.14 adopt recovery: file missing for "
                    "%s/%s sec=%s at %s — leaving as themerrdb",
                    mt, tid, sec, disk_path,
                )
                continue
            try:
                h = hashlib.sha256()
                with disk_path.open("rb") as f:
                    for chunk in iter(lambda: f.read(1024 * 1024), b""):
                        h.update(chunk)
                on_disk_sha = h.hexdigest()
            except OSError as e:
                stats["file_missing"] += 1
                log.warning(
                    "v1.19.14 adopt recovery: hash failed for "
                    "%s/%s sec=%s: %s",
                    mt, tid, sec, e,
                )
                continue
            if on_disk_sha != info["sha256"]:
                stats["sha_mismatch"] += 1
                log.info(
                    "v1.19.14 adopt recovery: sha drift for "
                    "%s/%s sec=%s (disk=%s..., adopt=%s...) — "
                    "leaving as themerrdb (file was re-downloaded "
                    "between adopt and cascade)",
                    mt, tid, sec,
                    on_disk_sha[:8], info["sha256"][:8],
                )
                continue
            actions.append(
                ("reclassify", mt, tid, sec, on_disk_sha),
            )
    n_reclassify = sum(1 for a in actions if a[0] == "reclassify")
    n_revert = sum(1 for a in actions if a[0] == "revert_overflip")
    log.info(
        "v1.19.14 adopt recovery: verification complete — "
        "%d rows queued for themerrdb→adopt, %d v1.19.13 "
        "over-flips queued for reversal, %d sha drift, %d "
        "post-adopt T-intent (skipped), %d file missing, %d "
        "skipped (no lf / wrong sk).",
        n_reclassify, n_revert,
        stats["sha_mismatch"],
        stats["skipped_post_adopt_t_intent"],
        stats["file_missing"],
        stats["skipped_no_local_files"]
        + stats["skipped_other_source_kind"]
        + stats["skipped_already_adopt"],
    )
    with get_conn(db_path) as conn, transaction(conn):
        for op, mt, tid, sec, sha in actions:
            if op == "reclassify":
                # Backfill file_sha256 alongside the source_kind
                # flip — the v1.18.5 walker left it NULL, so
                # this is a free correctness win at the same
                # write site.
                # v1.19.15: also set provenance='manual'. v1.19.13
                # / v1.19.14 forgot this column — left 207 rows on
                # the user's instance with source_kind='adopt' but
                # provenance='auto' (inherited from v1.18.5's
                # themerrdb default). The CLAUDE.md contract is
                # adopt=manual, themerrdb=auto, url=manual,
                # upload=manual. Audit guard at section C in the
                # state-audit script catches violations.
                cur = conn.execute(
                    "UPDATE local_files "
                    "   SET source_kind = 'adopt', "
                    "       provenance = 'manual', "
                    "       file_sha256 = ? "
                    " WHERE media_type = ? AND tmdb_id = ? "
                    "   AND section_id = ? AND edition_key = '' "
                    "   AND source_kind = 'themerrdb'",
                    (sha, mt, tid, sec),
                )
                if cur.rowcount:
                    stats["reclassified"] += cur.rowcount
            elif op == "revert_overflip":
                # Flip adopt → themerrdb. Scoped to
                # source_kind='adopt' so we don't accidentally
                # touch a row that was reclassified to
                # something else between scan and apply.
                # v1.19.15: also set provenance='auto' to match
                # the source_kind/provenance contract. Without
                # this, reverts left rows at source_kind='themerrdb'
                # + provenance='manual' (inherited from the
                # v1.19.13 over-flip's adopt classification).
                cur = conn.execute(
                    "UPDATE local_files "
                    "   SET source_kind = 'themerrdb', "
                    "       provenance = 'auto' "
                    " WHERE media_type = ? AND tmdb_id = ? "
                    "   AND section_id = ? AND edition_key = '' "
                    "   AND source_kind = 'adopt'",
                    (mt, tid, sec),
                )
                if cur.rowcount:
                    stats["reverted_v1_19_13_overflips"] += cur.rowcount
        # Stamp marker last so partial failures retry.
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES (?, ?, ?)",
            (_ADOPT_RECOVERY_MARKER_KEY, now_iso, now_iso),
        )
    if stats["reclassified"] > 0 or stats["reverted_v1_19_13_overflips"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.19.14 adopt recovery: DONE — scanned %d adopt "
        "events, verified %d unique targets. RECLASSIFIED %d "
        "rows themerrdb→adopt (lost A attribution from v1.18.0 "
        "cascade). REVERTED %d v1.19.13 over-flips back to "
        "themerrdb (post-adopt T-intent supersedes stale adopt "
        "event). Skipped: %d sha drift, %d post-adopt T-intent "
        "(already themerrdb), %d file missing, %d already adopt "
        "with no T-intent, %d other source_kind, %d no local_files.",
        stats["adopt_events_scanned"],
        stats["candidates"],
        stats["reclassified"],
        stats["reverted_v1_19_13_overflips"],
        stats["sha_mismatch"],
        stats["skipped_post_adopt_t_intent"],
        stats["file_missing"],
        stats["skipped_already_adopt"],
        stats["skipped_other_source_kind"],
        stats["skipped_no_local_files"],
    )
    return stats


# ── v1.19.15: source_kind ⇄ provenance contract repair ─────────


def maybe_repair_adopt_provenance(db_path: Path) -> dict:
    """v1.19.15 — one-shot repair for rows whose source_kind /
    provenance pair drifted from the CLAUDE.md contract.

    ## The contract

    | source_kind | provenance |
    |---|---|
    | themerrdb | auto   |
    | adopt     | manual |
    | url       | manual |
    | upload    | manual |

    The SRC letter SQL (`_SRC_LETTER_SQL` in api.py) reads
    source_kind directly, so the provenance drift didn't break
    SRC pill rendering. It DOES affect any downstream SQL that
    branches on provenance — search "provenance = 'manual'" or
    "provenance = 'auto'" in api.py / worker.py for sites that
    expect the pair to be in sync.

    ## What broke it

    v1.19.13's walker UPDATE statement set only source_kind, not
    provenance. 224 rows got source_kind='adopt' while keeping
    provenance='auto' from the v1.18.5 walker's themerrdb default.
    v1.19.14 reverted 17 of those (source_kind='themerrdb' but
    provenance='manual' — the opposite drift). Net on the user's
    instance: 207 rows with source_kind='adopt' + provenance='auto'.

    The walker source is now fixed (v1.19.15 SET clauses include
    provenance) so future runs write the correct pair. This
    function repairs the historical damage on existing installs.

    ## Repair logic

    Single UPDATE per source_kind value, scoped to rows where
    provenance is wrong. Idempotent — re-running on a clean
    install is a no-op (0 rowcount). Marker prevents
    unnecessary work on every boot.
    """
    from .db import get_conn, transaction

    stats = {
        "detected": False,
        "adopt_provenance_fixed": 0,
        "themerrdb_provenance_fixed": 0,
        "url_provenance_fixed": 0,
        "upload_provenance_fixed": 0,
    }
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_adopt_provenance_repair_done_at'"
        ).fetchone()
        if row is not None:
            log.info(
                "v1.19.15 provenance repair: marker already set "
                "— skipping. Remove the "
                "`recovery_adopt_provenance_repair_done_at` "
                "key to re-trigger."
            )
            return stats
    now_iso = _now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        # adopt → manual. The v1.19.13/.14 regression target.
        cur = conn.execute(
            "UPDATE local_files SET provenance = 'manual' "
            "WHERE source_kind = 'adopt' "
            "  AND (provenance IS NULL OR provenance != 'manual')"
        )
        stats["adopt_provenance_fixed"] = cur.rowcount
        # themerrdb → auto. Catches the inverse drift from any
        # path that wrote source_kind='themerrdb' without updating
        # provenance (e.g., the v1.19.13 over-flip → v1.19.14
        # revert path on rows that were already classified as
        # adopt+manual before v1.19.13).
        cur = conn.execute(
            "UPDATE local_files SET provenance = 'auto' "
            "WHERE source_kind = 'themerrdb' "
            "  AND (provenance IS NULL OR provenance != 'auto')"
        )
        stats["themerrdb_provenance_fixed"] = cur.rowcount
        # url / upload → manual. Defensive — the user's audit showed
        # 0 of these, but pin the contract for future installs.
        cur = conn.execute(
            "UPDATE local_files SET provenance = 'manual' "
            "WHERE source_kind = 'url' "
            "  AND (provenance IS NULL OR provenance != 'manual')"
        )
        stats["url_provenance_fixed"] = cur.rowcount
        cur = conn.execute(
            "UPDATE local_files SET provenance = 'manual' "
            "WHERE source_kind = 'upload' "
            "  AND (provenance IS NULL OR provenance != 'manual')"
        )
        stats["upload_provenance_fixed"] = cur.rowcount
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES ('recovery_adopt_provenance_repair_done_at', "
            "        ?, ?)",
            (now_iso, now_iso),
        )
    total = (stats["adopt_provenance_fixed"]
             + stats["themerrdb_provenance_fixed"]
             + stats["url_provenance_fixed"]
             + stats["upload_provenance_fixed"])
    if total > 0:
        stats["detected"] = True
    log.warning(
        "v1.19.15 provenance repair: DONE — fixed %d rows "
        "(adopt→manual: %d, themerrdb→auto: %d, url→manual: %d, "
        "upload→manual: %d). Repairs drift from v1.19.13/.14 "
        "walker that wrote source_kind without provenance.",
        total,
        stats["adopt_provenance_fixed"],
        stats["themerrdb_provenance_fixed"],
        stats["url_provenance_fixed"],
        stats["upload_provenance_fixed"],
    )
    return stats


# ── v1.19.16 / v1.19.17: stale-placement repair via place jobs ─


def maybe_repair_stale_placements(
    db_path: Path, themes_dir: Path,
) -> dict:
    """v1.19.16 + v1.19.17 — repair placements where motif's record says
    "placed" but Plex's serving state disagrees, in two classes:

    ## Class A: Plex doesn't know about the theme

    plex_items.has_theme=0 even though `placements.placed_at`
    is stamped. The v1.18.5 / v1.18.11 recovery walkers
    INSERTed placements rows with `plex_refreshed=1` hardcoded
    WITHOUT actually calling Plex's `/library/metadata/{rk}/
    refresh` API. Plex's metadata cache never re-enumerated,
    so the `/theme` serving endpoint returns empty even though
    the sidecar is on disk and `local_theme_file=1` shows Plex
    sees the file in the XML.

    the user's instance at v1.19.16: 8 rows in this class.

    ## Class B: file missing from Plex folder

    `placements.placement_kind in ('hardlink','copy')` with
    `media_folder` set, but `media_folder/theme.mp3` doesn't
    exist on disk anymore. Manual cleanup, fs reorg, or a
    Plex-side rename removed the file. The canonical at
    themes_dir/file_path is still there.

    the user's instance at v1.19.16: 1 row in this class
    (tv/-44 "100 Years of Warner Bros.").

    ## Fix shape

    Both classes converge on enqueueing a `place` job with
    `force_place=true`. The worker:
      * Class A: finds the hardlink already in place →
        no-op for file IO, calls Plex `/refresh` API →
        Plex re-enumerates → `has_theme` flips to 1 on
        next plex_enum.
      * Class B: finds the file missing → re-hardlinks
        canonical to media_folder → calls Plex refresh.

    One fix, both classes.

    ## Safety

    Skip rows that already have a pending/running place job
    (don't pile up duplicates). Skip rows where the canonical
    is missing from themes_dir (different damage class — out
    of scope, would need a download or adopt to recover).

    Idempotent via marker `recovery_stale_placements_done_at_v1_19_16`.
    """
    from .db import get_conn, transaction

    stats = {
        "detected": False,
        "class_a_candidates": 0,
        "class_b_candidates": 0,
        "enqueued": 0,
        "skipped_canonical_missing": 0,
        "skipped_job_in_flight": 0,
        "skipped_duplicate_target": 0,
    }
    with get_conn(db_path) as conn:
        # v1.19.17: marker key bumped (was
        # recovery_stale_placements_done_at_v1_19_16) per the
        # v1.18.11 pattern. The v1.19.16 walker enqueued place
        # jobs WITHOUT setting kind in payload, so the worker
        # used the user's global default (api), which routed to
        # _do_place_collection — uploaded files via Plex API +
        # deleted Plex-folder sidecars + created plex_upload
        # placement rows without removing the old hardlink rows
        # (upsert keyed on media_folder mismatch). v1.19.17
        # forces kind=file in payload (sidecar route) which is
        # safe for orphans AND idempotent for already-linked
        # rows. Re-run picks up tv/-44 (orphan that failed under
        # the collection path).
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_stale_placements_done_at_v1_19_17'"
        ).fetchone()
        if row is not None:
            log.info(
                "v1.19.17 stale placement repair: marker already "
                "set — skipping. Remove "
                "`recovery_stale_placements_done_at_v1_19_17` key "
                "to re-trigger."
            )
            return stats
        # Class A — Plex says no theme but motif placed one.
        # v1.19.42: plex_cloud-backup local_files rows (source_kind
        # ='plex_cloud', last_place_attempt_reason='backup_only')
        # are out-of-scope for this walker — they never get
        # `placements` rows (backup-only by definition: not
        # deployed until the user clicks PROMOTE TO ACTIVE via the
        # v1.19.35 BK-no-override branch). The `JOIN placements p`
        # below naturally excludes them. Comment is preserved for
        # future drift protection: if this walker ever widens to
        # local_files-rows-without-placements, plex_cloud must be
        # explicitly excluded (`AND lf.source_kind != 'plex_cloud'`).
        class_a = conn.execute("""
            SELECT p.media_type, p.tmdb_id, p.section_id,
                   p.plex_rating_key, p.media_folder,
                   p.placement_kind, lf.file_path
              FROM placements p
              JOIN plex_items pi
                ON pi.rating_key = p.plex_rating_key
         LEFT JOIN local_files lf
                ON lf.media_type = p.media_type
               AND lf.tmdb_id = p.tmdb_id
               AND lf.section_id = p.section_id
             WHERE pi.has_theme = 0
               AND p.placed_at IS NOT NULL
        """).fetchall()
        # Class B — file missing from Plex folder. Stat each
        # hardlink/copy placement's expected file. plex_upload
        # placements use media_folder='' and have no on-disk
        # file in a Plex folder, so they're excluded.
        all_sidecar_placements = conn.execute("""
            SELECT p.media_type, p.tmdb_id, p.section_id,
                   p.plex_rating_key, p.media_folder,
                   p.placement_kind, lf.file_path
              FROM placements p
         LEFT JOIN local_files lf
                ON lf.media_type = p.media_type
               AND lf.tmdb_id = p.tmdb_id
               AND lf.section_id = p.section_id
             WHERE p.media_folder != ''
               AND p.placement_kind IN ('hardlink', 'copy')
        """).fetchall()
    class_b = []
    for r in all_sidecar_placements:
        sidecar_path = Path(r["media_folder"]) / "theme.mp3"
        if not sidecar_path.is_file():
            class_b.append(r)
    stats["class_a_candidates"] = len(class_a)
    stats["class_b_candidates"] = len(class_b)
    # Dedupe by (mt, tmdb, sec) — a row could in theory hit both
    # classes (Plex.has_theme=0 AND file missing). One job covers
    # both.
    candidates: dict[tuple, dict] = {}
    for r in class_a + class_b:
        key = (r["media_type"], int(r["tmdb_id"]), r["section_id"])
        if key in candidates:
            stats["skipped_duplicate_target"] += 1
            continue
        candidates[key] = r
    if not candidates:
        log.info(
            "v1.19.16 stale placement repair: no stale placements "
            "found — skipping marker stamp so future drift can be "
            "caught."
        )
        return stats
    log.info(
        "v1.19.16 stale placement repair: %d unique candidates "
        "(class_a=%d, class_b=%d, dedup_overlap=%d)",
        len(candidates),
        stats["class_a_candidates"],
        stats["class_b_candidates"],
        stats["skipped_duplicate_target"],
    )
    now_iso = _now_iso()
    enqueued_pairs: list[tuple] = []
    with get_conn(db_path) as conn:
        for (mt, tid, sec), r in candidates.items():
            # Skip if a place job for this row is already pending
            # or running — don't pile up duplicates.
            existing = conn.execute("""
                SELECT 1 FROM jobs
                 WHERE job_type = 'place'
                   AND media_type = ? AND tmdb_id = ?
                   AND section_id = ?
                   AND status IN ('pending', 'running')
            """, (mt, tid, sec)).fetchone()
            if existing:
                stats["skipped_job_in_flight"] += 1
                continue
            # Skip if canonical is missing — different damage
            # class (would need download or adopt to recover).
            if r["file_path"]:
                canonical = themes_dir / r["file_path"]
                if not canonical.is_file():
                    stats["skipped_canonical_missing"] += 1
                    log.info(
                        "v1.19.16 stale placement: canonical "
                        "missing for %s/%s sec=%s at %s — "
                        "skipping (out of scope, would need "
                        "download/adopt to recover)",
                        mt, tid, sec, canonical,
                    )
                    continue
            enqueued_pairs.append((mt, tid, sec))
    log.info(
        "v1.19.16 stale placement repair: %d rows queued for "
        "place job enqueue, %d skipped (canonical missing), %d "
        "skipped (job already in flight).",
        len(enqueued_pairs),
        stats["skipped_canonical_missing"],
        stats["skipped_job_in_flight"],
    )
    with get_conn(db_path) as conn, transaction(conn):
        for mt, tid, sec in enqueued_pairs:
            conn.execute("""
                INSERT INTO jobs
                  (job_type, media_type, tmdb_id, section_id,
                   payload, status, created_at, next_run_at)
                VALUES ('place', ?, ?, ?, ?, 'pending', ?, ?)
            """, (
                mt, tid, sec,
                # v1.19.17: kind=file forces sidecar route per
                # _do_place dispatch (worker.py:1853-1857). Repair
                # operations should never CONVERT placement_kind,
                # only re-establish the existing route. Without
                # this, the user's global default (the user: api)
                # converted hardlink → plex_upload + created
                # duplicate placement rows.
                '{"kind":"file","force":true,'
                '"reason":"v1.19.17_stale_placement_repair"}',
                now_iso, now_iso,
            ))
            stats["enqueued"] += 1
        # Stamp marker last so partial failures retry.
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES "
            "  ('recovery_stale_placements_done_at_v1_19_17', "
            "   ?, ?)",
            (now_iso, now_iso),
        )
    if stats["enqueued"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.19.16 stale placement repair: DONE — found %d unique "
        "candidates (class_A_plex_unaware=%d, class_B_file_missing"
        "=%d). ENQUEUED %d place jobs. Skipped: %d canonical "
        "missing, %d in-flight job already exists. Worker will "
        "re-hardlink (idempotent) + call Plex refresh.",
        len(candidates),
        stats["class_a_candidates"],
        stats["class_b_candidates"],
        stats["enqueued"],
        stats["skipped_canonical_missing"],
        stats["skipped_job_in_flight"],
    )
    return stats


# ── v1.19.17: cleanup duplicate placement rows from v1.19.16 ───


def maybe_cleanup_duplicate_placements(db_path: Path) -> dict:
    """v1.19.17 — delete stale hardlink/copy placement rows that
    have a newer plex_upload counterpart for the same item.

    ## Why this exists

    v1.19.16's walker enqueued place jobs WITHOUT a `kind` in
    payload, so the worker (worker.py:1853-1857) fell back to
    `settings.default_placement_method`. For installs with the
    default set to "api" (the user's case), the worker dispatched
    to `_do_place_collection` which:

      1. Uploads the file to Plex via `/library/metadata/{rk}/
         themes` (multipart POST).
      2. Removes the Plex-folder sidecar as cleanup (worker.py
         line ~2730).
      3. UPSERTs a NEW placements row with media_folder='' +
         placement_kind='plex_upload'.

    The UPSERT's ON CONFLICT clause is keyed on
    `(media_type, tmdb_id, section_id, media_folder)` — but
    media_folder went from '/data/.../...' to '' — so the
    constraint doesn't match. Result: a fresh row inserts
    instead of updating the old one. The OLD hardlink row
    persists with media_folder pointing at a file that no
    longer exists on disk.

    the user's instance at v1.19.17 cut: 10 duplicate rows.

    ## Cleanup logic

    Delete `placements` rows WHERE:
      * placement_kind IN ('hardlink', 'copy')
      * AND a sibling row exists for the same
        (media_type, tmdb_id, section_id) with placement_kind=
        'plex_upload' AND a fresher placed_at
      * AND the hardlink row's media_folder/theme.mp3 doesn't
        exist on disk (sanity check — don't delete rows whose
        files are still in place, even if there's a duplicate)

    Idempotent via marker
    `recovery_duplicate_placements_cleanup_done_at_v1_19_17`.
    Walks DB-only EXCEPT for the per-row file-existence stat.
    """
    from .db import get_conn, transaction

    stats = {
        "detected": False,
        "stale_hardlink_rows_found": 0,
        "deleted": 0,
        "skipped_file_present": 0,
    }
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = "
            "'recovery_duplicate_placements_cleanup_done_at_v1_19_17'"
        ).fetchone()
        if row is not None:
            log.info(
                "v1.19.17 duplicate placement cleanup: marker "
                "already set — skipping. Remove "
                "`recovery_duplicate_placements_cleanup_done_at"
                "_v1_19_17` key to re-trigger."
            )
            return stats
        # Find candidate stale rows — hardlink/copy placements
        # whose item ALSO has a plex_upload row with newer
        # placed_at. Pull rowid for safe targeted DELETE.
        candidates = conn.execute("""
            SELECT p.rowid AS rid, p.media_type, p.tmdb_id,
                   p.section_id, p.media_folder, p.placement_kind,
                   p.placed_at AS hl_placed,
                   pu.placed_at AS pu_placed
              FROM placements p
              JOIN placements pu
                ON pu.media_type = p.media_type
               AND pu.tmdb_id = p.tmdb_id
               AND pu.section_id = p.section_id
               -- v1.22.25 (edition audit hardening): only pair a hardlink/copy
               -- against a plex_upload of the SAME edition. Pre-fix the join
               -- was section-only, so an Extended hardlink (file missing) could
               -- be deleted because the STANDARD edition had a plex_upload.
               -- Unreachable today (marker-gated one-shot) but correct if the
               -- marker is cleared to re-run on a multi-edition library.
               AND pu.edition_key = p.edition_key
               AND pu.placement_kind = 'plex_upload'
               AND pu.placed_at >= p.placed_at
             WHERE p.placement_kind IN ('hardlink', 'copy')
               AND p.media_folder != ''
        """).fetchall()
    stats["stale_hardlink_rows_found"] = len(candidates)
    if not candidates:
        log.info(
            "v1.19.17 duplicate placement cleanup: no stale "
            "hardlink-vs-plex_upload duplicates — skipping "
            "marker stamp so future drift can be caught."
        )
        return stats
    log.info(
        "v1.19.17 duplicate placement cleanup: %d candidate "
        "stale hardlink rows to verify",
        len(candidates),
    )
    # Sanity check: only delete rows whose file is actually
    # missing. If the file is still there (e.g., user has both
    # a sidecar AND uploaded via API for some reason), leave
    # the hardlink row alone — that's a legit dual state.
    to_delete: list[int] = []
    for c in candidates:
        sidecar = Path(c["media_folder"]) / "theme.mp3"
        if sidecar.is_file():
            stats["skipped_file_present"] += 1
            log.info(
                "v1.19.17 cleanup: %s/%s sec=%s has both "
                "hardlink (file present) AND plex_upload — "
                "leaving hardlink row alone (legitimate dual)",
                c["media_type"], c["tmdb_id"], c["section_id"],
            )
            continue
        to_delete.append(c["rid"])
    now_iso = _now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        for rid in to_delete:
            cur = conn.execute(
                "DELETE FROM placements WHERE rowid = ?",
                (rid,),
            )
            if cur.rowcount:
                stats["deleted"] += cur.rowcount
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES "
            "  ('recovery_duplicate_placements_cleanup_done_at"
            "_v1_19_17', ?, ?)",
            (now_iso, now_iso),
        )
    if stats["deleted"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.19.17 duplicate placement cleanup: DONE — found %d "
        "candidates, DELETED %d stale hardlink rows (superseded "
        "by plex_upload sibling), skipped %d where the sidecar "
        "file is still present (legit dual state).",
        stats["stale_hardlink_rows_found"],
        stats["deleted"],
        stats["skipped_file_present"],
    )
    return stats


# ── v1.19.18: file_sha256 backfill for v1.18.5 walker artifacts ─


def maybe_backfill_file_sha256(
    db_path: Path, themes_dir: Path,
) -> dict:
    """v1.19.18 — stream sha256 for local_files rows where
    file_sha256 is NULL/empty.

    ## Why this exists

    v1.18.5's recovery walker rebuilt local_files rows from
    on-disk state but didn't compute file_sha256 (recovery_v55.py
    line ~410 inserts NULL for that column). The bug-fix walkers
    that followed (v1.19.13 + v1.19.14) only backfilled sha for
    rows they ACTIVELY reclassified — 224 rows total. The other
    578 rows kept their NULL sha indefinitely.

    Consequences of the NULL:
      * Future verification walkers (like v1.19.13's sha-match
        check for adopt-reclassification) can't operate on these
        rows. Without sha, no integrity check is possible.
      * Drift detection (file content changed unexpectedly) can't
        differentiate "file is intact" from "we never knew the sha".
      * The audit script's section D flagged 578 NULL rows on
        the user's instance.

    ## Strategy

    Walk local_files WHERE file_sha256 IS NULL OR file_sha256 = ''.
    For each row, compute streaming sha256 of the canonical file
    at themes_dir/file_path. Skip rows where the file is missing
    on disk (different damage class — would need a download or
    adopt to recover).

    Idempotent via marker
    `recovery_file_sha256_backfill_done_at_v1_19_18`. Hash work
    is bounded by streaming I/O — the user's instance: 578 files,
    ~1-10MB each, ~1-2GB streamed total. Completes in seconds on
    SSD-backed storage; minutes on spinning disk over NFS.
    """
    from .db import get_conn, transaction
    import hashlib

    stats = {
        "detected": False,
        "candidates": 0,
        "backfilled": 0,
        "file_missing": 0,
        "hash_error": 0,
    }
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_file_sha256_backfill_done_at_v1_19_18'"
        ).fetchone()
        if row is not None:
            log.info(
                "v1.19.18 sha256 backfill: marker already set — "
                "skipping. Remove "
                "`recovery_file_sha256_backfill_done_at_v1_19_18` "
                "key to re-trigger."
            )
            return stats
        candidates = conn.execute(
            "SELECT media_type, tmdb_id, section_id, file_path, "
            "       source_kind "
            "  FROM local_files "
            " WHERE (file_sha256 IS NULL OR file_sha256 = '') "
            "   AND file_path IS NOT NULL "
            "   AND file_path != ''"
        ).fetchall()
    stats["candidates"] = len(candidates)
    if not candidates:
        log.info(
            "v1.19.18 sha256 backfill: no NULL-sha rows — skipping "
            "marker stamp so future NULL-introducing paths can be "
            "caught."
        )
        return stats
    log.info(
        "v1.19.18 sha256 backfill: %d candidate rows (source_kind "
        "tally: %s)",
        len(candidates),
        ", ".join(
            f"{sk}={sum(1 for c in candidates if c['source_kind'] == sk)}"
            for sk in sorted(set(c["source_kind"] or "(null)"
                                 for c in candidates))
        ),
    )
    # Hash files OUTSIDE the write txn so a slow disk doesn't
    # hold a write lock for the whole walk. Build the action list
    # first, then apply in a single transaction.
    actions: list[tuple] = []
    for r in candidates:
        disk_path = themes_dir / r["file_path"]
        if not disk_path.is_file():
            stats["file_missing"] += 1
            log.info(
                "v1.19.18 sha256 backfill: file missing for "
                "%s/%s sec=%s at %s — skipping (would need "
                "download/adopt to recover the row)",
                r["media_type"], r["tmdb_id"], r["section_id"],
                disk_path,
            )
            continue
        try:
            h = hashlib.sha256()
            with disk_path.open("rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(chunk)
            actions.append((
                h.hexdigest(),
                r["media_type"], r["tmdb_id"], r["section_id"],
            ))
        except OSError as e:
            stats["hash_error"] += 1
            log.warning(
                "v1.19.18 sha256 backfill: hash failed for "
                "%s/%s sec=%s: %s",
                r["media_type"], r["tmdb_id"], r["section_id"], e,
            )
    log.info(
        "v1.19.18 sha256 backfill: hashed %d files, %d missing, "
        "%d hash errors — applying writes",
        len(actions), stats["file_missing"], stats["hash_error"],
    )
    now_iso = _now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        for sha, mt, tid, sec in actions:
            # Scope the WHERE clause to NULL/empty file_sha256 so
            # we don't overwrite a value written between the scan
            # and the apply (e.g., a worker download landed in the
            # interim).
            cur = conn.execute(
                "UPDATE local_files "
                "   SET file_sha256 = ? "
                " WHERE media_type = ? AND tmdb_id = ? "
                "   AND section_id = ? "
                "   AND (file_sha256 IS NULL OR file_sha256 = '')",
                (sha, mt, tid, sec),
            )
            if cur.rowcount:
                stats["backfilled"] += cur.rowcount
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES ("
            "  'recovery_file_sha256_backfill_done_at_v1_19_18', "
            "  ?, ?)",
            (now_iso, now_iso),
        )
    if stats["backfilled"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.19.18 sha256 backfill: DONE — %d candidates, "
        "BACKFILLED %d rows, %d files missing, %d hash errors. "
        "Provides foundation for future sha-based verification "
        "walkers.",
        stats["candidates"],
        stats["backfilled"],
        stats["file_missing"],
        stats["hash_error"],
    )
    return stats


# ── v1.19.21: re-enqueue force-place for stale-cache victims ──


def maybe_repair_stale_plex_cache_placements(db_path: Path) -> dict:
    """v1.19.21 — one-shot walker for rows where Plex's cached
    has_theme=1 was trusted indefinitely despite verified_ok=0.

    ## Why this exists

    Indiana Jones (movie/335977 sec=1) on the user's instance:
      * local_files: themerrdb, file present (7.8MB)
      * placements: empty (place was skipped)
      * plex_items: has_theme=1, verified_ok=0, no sidecar
      * Result: SRC pill = '–', motif's file unused, Plex's
        serving broken

    The worker fix in v1.19.21 makes FUTURE place jobs ignore
    the stale cache (forces placement when verified_ok=0). But
    the hourly retry sweep skips rows with
    last_place_attempt_reason='plex_has_theme', so existing
    damage never re-enqueues a place job. Without this walker,
    the user would have to manually PUSH TO PLEX every affected
    row.

    ## Strategy

    Find rows where:
      * local_files exists (motif has the file)
      * placements is missing OR media_folder is empty (placed
        via API but the API skipped due to stale has_theme — no
        media_folder stamp)
      * plex_items: has_theme=1 AND plex_theme_verified_ok=0
      * last_place_attempt_reason='plex_has_theme' (this is the
        signature of the prior skip)

    For each, enqueue a force-place job with kind=file (v1.19.17
    convention) so the worker uses the sidecar route. The worker's
    v1.19.21 stale-cache check will treat the row as if
    has_theme=False and proceed with the placement.

    Idempotent via marker
    `recovery_stale_plex_cache_placements_done_at_v1_19_21`.
    """
    from .db import get_conn, transaction

    stats = {
        "detected": False,
        "candidates": 0,
        "enqueued": 0,
        "skipped_job_in_flight": 0,
    }
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = "
            "'recovery_stale_plex_cache_placements_done_at_v1_19_21'"
        ).fetchone()
        if row is not None:
            log.info(
                "v1.19.21 stale-Plex-cache placement repair: "
                "marker already set — skipping."
            )
            return stats
        # v1.19.42: plex_cloud-backup local_files rows
        # (source_kind='plex_cloud', last_place_attempt_reason=
        # 'backup_only') are out-of-scope here — the WHERE clause
        # already filters `last_place_attempt_reason =
        # 'plex_has_theme'`, so plex_cloud naturally excludes.
        # If this filter ever widens (e.g. to also catch
        # `last_place_attempt_reason IS NULL`), plex_cloud must
        # be explicitly excluded — promoting a backup-only row
        # via this walker would defeat its backup intent.
        candidates = conn.execute("""
            SELECT lf.media_type, lf.tmdb_id, lf.section_id
              FROM local_files lf
              JOIN plex_items pi
                ON pi.guid_tmdb = lf.tmdb_id
               AND pi.section_id = lf.section_id
               AND ((lf.media_type = 'tv' AND pi.media_type = 'show')
                    OR pi.media_type = lf.media_type)
         LEFT JOIN placements p
                ON p.media_type = lf.media_type
               AND p.tmdb_id = lf.tmdb_id
               AND p.section_id = lf.section_id
             WHERE pi.has_theme = 1
               AND pi.plex_theme_verified_ok = 0
               AND lf.last_place_attempt_reason = 'plex_has_theme'
               AND (p.media_folder IS NULL OR p.media_folder = '')
        """).fetchall()
    stats["candidates"] = len(candidates)
    if not candidates:
        log.info(
            "v1.19.21 stale-Plex-cache placement repair: no "
            "candidates — skipping marker stamp so future drift "
            "can re-trigger."
        )
        return stats
    log.info(
        "v1.19.21 stale-Plex-cache placement repair: %d "
        "candidates (rows with motif file + stale Plex cache)",
        len(candidates),
    )
    now_iso = _now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        for c in candidates:
            mt, tid, sec = (c["media_type"], c["tmdb_id"],
                            c["section_id"])
            existing = conn.execute("""
                SELECT 1 FROM jobs
                 WHERE job_type = 'place'
                   AND media_type = ? AND tmdb_id = ?
                   AND section_id = ?
                   AND status IN ('pending', 'running')
            """, (mt, tid, sec)).fetchone()
            if existing:
                stats["skipped_job_in_flight"] += 1
                continue
            conn.execute("""
                INSERT INTO jobs
                  (job_type, media_type, tmdb_id, section_id,
                   payload, status, created_at, next_run_at)
                VALUES ('place', ?, ?, ?, ?, 'pending', ?, ?)
            """, (
                mt, tid, sec,
                '{"kind":"file","force":true,'
                '"reason":"v1.19.21_stale_plex_cache_repair"}',
                now_iso, now_iso,
            ))
            stats["enqueued"] += 1
            # v1.19.36: per-row INFO log so post-fact diagnosis can
            # correlate "row X got re-placed" with this walker.
            # Collections take the API-upload path inside
            # _do_place (kind=file is ignored by
            # _do_place_collection); flag explicitly so the
            # operator can trace plex_upload jobs back here.
            log.info(
                "v1.19.21 walker enqueued place: mt=%s tmdb=%s "
                "section=%s%s",
                mt, tid, sec,
                " (collection — kind=file ignored, "
                "_do_place_collection takes API route)"
                if mt == "collection" else "",
            )
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES ("
            "  'recovery_stale_plex_cache_placements_done_at_v1_19_21', "
            "  ?, ?)",
            (now_iso, now_iso),
        )
    if stats["enqueued"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.19.21 stale-Plex-cache placement repair: DONE — "
        "%d candidates, ENQUEUED %d force-place jobs (kind=file), "
        "skipped %d with in-flight jobs. Worker's v1.19.21 stale-"
        "cache check will treat these as has_theme=false and "
        "install the sidecar.",
        stats["candidates"],
        stats["enqueued"],
        stats["skipped_job_in_flight"],
    )
    return stats


# ── v1.19.61: PS-with-DL → BK backfill ──


def maybe_repair_ps_with_dl_to_bk(db_path: Path) -> dict:
    """v1.19.61 — convert legacy `plex_has_theme` stamps to
    `backup_only` on rows that have a local file.

    ## Why this exists

    Pre-v1.19.61 the worker stamped `last_place_attempt_reason=
    'plex_has_theme'` whenever a place job skipped because Plex was
    already serving its own theme. Same for LET PLEX SERVE handlers
    in api.py. These rows render LINK=PS (Plex-Serving) and don't
    surface PROMOTE TO ACTIVE on the recovery card — but
    functionally they ARE backups (motif has the file, Plex serves,
    placement would re-place if Plex stopped serving). the user's
    "86 EIGHTY-SIX" repro made the redundancy visible.

    v1.19.61's worker change stamps `backup_only` going forward,
    unifying PS-with-DL into the BK link state + recovery surface.
    This walker backfills existing rows so the v1.19.61 behavior
    applies to already-stamped data.

    ## Scope

    Match rows where ALL of:
      * `last_place_attempt_reason = 'plex_has_theme'`
      * `local_files.file_path IS NOT NULL` (motif actually has the
        downloaded file — without it the `plex_has_theme` stamp is
        just bookkeeping)
      * No placement (`media_folder IS NULL OR ''`)

    UPDATE to `last_place_attempt_reason = 'backup_only'`. Leaves
    the file untouched; only the bookkeeping field changes.

    Idempotent via `runtime_settings` marker
    `recovery_ps_to_bk_done_at_v1_19_61`.
    """
    from .db import get_conn, transaction

    stats = {
        "detected": False,
        "candidates": 0,
        "updated": 0,
    }
    marker_key = "recovery_ps_to_bk_done_at_v1_19_61"
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings WHERE key = ?",
            (marker_key,),
        ).fetchone()
        if row is not None:
            log.info(
                "v1.19.61 PS→BK backfill: marker already set — "
                "skipping."
            )
            return stats
        # v1.19.61: LEFT JOIN against placements so the "no
        # placement" filter is in SQL. Counted candidates BEFORE
        # the UPDATE so the walker can log a precise number.
        candidates = conn.execute("""
            SELECT lf.media_type, lf.tmdb_id, lf.section_id,
                   lf.source_kind
              FROM local_files lf
              LEFT JOIN placements p
                ON p.media_type = lf.media_type
               AND p.tmdb_id = lf.tmdb_id
               AND p.section_id = lf.section_id
             WHERE lf.last_place_attempt_reason = 'plex_has_theme'
               AND lf.file_path IS NOT NULL
               AND lf.file_path != ''
               AND (p.media_folder IS NULL OR p.media_folder = '')
        """).fetchall()
    stats["candidates"] = len(candidates)
    if not candidates:
        log.info(
            "v1.19.61 PS→BK backfill: zero candidates — nothing "
            "to convert. Stamping done marker so subsequent boots "
            "skip the scan."
        )
        with get_conn(db_path) as conn, transaction(conn):
            now_iso = _now_iso()
            conn.execute(
                "INSERT OR REPLACE INTO runtime_settings "
                "  (key, value, updated_at) "
                "VALUES (?, ?, ?)",
                (marker_key, now_iso, now_iso),
            )
        return stats
    # Bulk UPDATE in one transaction.
    with get_conn(db_path) as conn, transaction(conn):
        now_iso = _now_iso()
        cur = conn.execute("""
            UPDATE local_files
               SET last_place_attempt_reason = 'backup_only'
             WHERE last_place_attempt_reason = 'plex_has_theme'
               AND file_path IS NOT NULL
               AND file_path != ''
               AND NOT EXISTS (
                   SELECT 1 FROM placements p
                    WHERE p.media_type = local_files.media_type
                      AND p.tmdb_id = local_files.tmdb_id
                      AND p.section_id = local_files.section_id
                      AND p.media_folder IS NOT NULL
                      AND p.media_folder != ''
               )
        """)
        stats["updated"] = cur.rowcount or 0
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES (?, ?, ?)",
            (marker_key, now_iso, now_iso),
        )
    if stats["updated"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.19.61 PS→BK backfill: DONE — %d candidates, "
        "UPDATEd %d rows from last_place_attempt_reason="
        "'plex_has_theme' to 'backup_only'. These rows now "
        "render LINK=BK, get PROMOTE TO ACTIVE on the recovery "
        "card, and fire the v1.18.90 backup_ready notification "
        "on theme loss.",
        stats["candidates"], stats["updated"],
    )
    return stats


# ── v1.19.65: stale plex_has_theme stamps on placed rows ──


def maybe_repair_stale_plex_has_theme_stamps(db_path: Path) -> dict:
    """v1.19.65 — convert leftover `plex_has_theme` stamps to
    `placed` on rows that currently HAVE an active placement.

    ## Why this exists

    Pre-v1.19.61 the worker stamped `last_place_attempt_reason=
    'plex_has_theme'` whenever a place skipped because Plex was
    serving its own theme. Later a user PUSH TO PLEX (or some
    other code path) installed a placement, but the stamp wasn't
    re-written because the v1.19.21 retry-sweep filter excludes
    plex_has_theme rows from auto-retry — so no place job ran to
    overwrite the stamp.

    Result on the user's prod (2026-05-28): 77 rows with
    `last_place_attempt_reason = 'plex_has_theme'` AND an active
    placement. The placement renders correctly (HL/C/PU per
    placement_kind) but the stamp is misleading — a debugger
    would think the row's placement was skipped, when in fact
    it succeeded.

    v1.19.61's PS→BK backfill walker correctly skipped these
    (its predicate required no-placement, treating
    plex_has_theme + placed as out-of-scope). v1.19.65 cleans
    them up.

    ## Scope

    Match rows where ALL of:
      * `last_place_attempt_reason = 'plex_has_theme'`
      * An active `placements` row exists for the same
        (media_type, tmdb_id, section_id)

    UPDATE to `last_place_attempt_reason = 'placed'`. Pure
    bookkeeping — no file or placement is touched.

    Idempotent via `runtime_settings` marker
    `recovery_stale_plex_has_theme_stamps_done_at_v1_19_65`.
    """
    from .db import get_conn, transaction

    stats = {
        "detected": False,
        "candidates": 0,
        "updated": 0,
    }
    marker_key = "recovery_stale_plex_has_theme_stamps_done_at_v1_19_65"
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings WHERE key = ?",
            (marker_key,),
        ).fetchone()
        if row is not None:
            log.info(
                "v1.19.65 stale plex_has_theme stamps repair: "
                "marker already set — skipping."
            )
            return stats
        candidates = conn.execute("""
            SELECT lf.media_type, lf.tmdb_id, lf.section_id
              FROM local_files lf
              JOIN placements p
                ON p.media_type = lf.media_type
               AND p.tmdb_id = lf.tmdb_id
               AND p.section_id = lf.section_id
             WHERE lf.last_place_attempt_reason = 'plex_has_theme'
               AND p.media_folder IS NOT NULL
               AND p.media_folder != ''
        """).fetchall()
    stats["candidates"] = len(candidates)
    if not candidates:
        log.info(
            "v1.19.65 stale plex_has_theme stamps repair: zero "
            "candidates — nothing to convert. Stamping done "
            "marker so subsequent boots skip the scan."
        )
        with get_conn(db_path) as conn, transaction(conn):
            now_iso = _now_iso()
            conn.execute(
                "INSERT OR REPLACE INTO runtime_settings "
                "  (key, value, updated_at) "
                "VALUES (?, ?, ?)",
                (marker_key, now_iso, now_iso),
            )
        return stats
    with get_conn(db_path) as conn, transaction(conn):
        now_iso = _now_iso()
        cur = conn.execute("""
            UPDATE local_files
               SET last_place_attempt_reason = 'placed'
             WHERE last_place_attempt_reason = 'plex_has_theme'
               AND EXISTS (
                   SELECT 1 FROM placements p
                    WHERE p.media_type = local_files.media_type
                      AND p.tmdb_id = local_files.tmdb_id
                      AND p.section_id = local_files.section_id
                      AND p.media_folder IS NOT NULL
                      AND p.media_folder != ''
               )
        """)
        stats["updated"] = cur.rowcount or 0
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "  (key, value, updated_at) "
            "VALUES (?, ?, ?)",
            (marker_key, now_iso, now_iso),
        )
    if stats["updated"] > 0:
        stats["detected"] = True
    log.warning(
        "v1.19.65 stale plex_has_theme stamps repair: DONE — "
        "%d candidates, UPDATEd %d rows from "
        "last_place_attempt_reason='plex_has_theme' to "
        "'placed'. These rows have active placements; the "
        "'plex_has_theme' stamp was leftover from a prior skip "
        "and was misleading in debugger views.",
        stats["candidates"], stats["updated"],
    )
    return stats
