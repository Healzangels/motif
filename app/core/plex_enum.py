"""
Plex library enumeration: walk every managed section and upsert plex_items.

Runs as a worker job (job_type='plex_enum'). Triggered automatically after
each ThemerrDB sync, after a /libraries refresh, and on demand from the UI.

Per-row strategy:
- INSERT new rows with first_seen_at = last_seen_at = now
- UPDATE existing rows (matched by rating_key): refresh title/year/guids/
  folder_path/has_theme + bump last_seen_at
- Rows in plex_items whose section_id has been disabled stay in the table
  (history) but aren't included in the unified browse view (the browse
  query inner-joins managed plex_sections).
"""
from __future__ import annotations

import logging
import os
import threading
from pathlib import Path

from .db import get_conn, transaction
from .editions import edition_key_for_folder
from .events import log_event, now_iso
from .plex import PlexClient, PlexConfig, PlexLibraryItem
from .sections import list_sections
from . import progress as op_progress

log = logging.getLogger(__name__)


# v1.18.48: class-9 hot-path sub-pattern (see CLAUDE.md). find_theme_sidecar_path's
# OSError swallows used to be silent — a stale NFS mount or vanished folder
# returned None on every orphan-scan row with no breadcrumb, and the dashboard
# rendered "no orphan sidecar found" for paths that were really just unreachable.
# Once-per-process warn so a deploy actually flagging this lands one line in the
# logs; subsequent occurrences drop to debug to avoid drowning the log when the
# fault is fleet-wide (every row in a section traverses the same broken mount).
_FIND_THEME_SIDECAR_OSERROR_WARNED: bool = False

# v1.19.41: hot-path silent-fail downgrade flag for the v1.18.90
# reaper-dispatch loop. The Apprise dispatch was previously
# catching Exception → log.debug, which buried genuine config
# breakage at a level invisible to operators at INFO. First
# occurrence per process logs at WARNING; subsequent fall through
# to log.debug. Same pattern as the v1.17.11 hot-path warned
# flags (sync.py normalize_title, auth.py verify_password).
_THEME_LOST_NOTIFY_WARNED: bool = False

# v1.19.41: same pattern for the v1.18.79 backup_ready_to_deploy
# dispatch (plex_enum line ~1330). Independent flag so the two
# dispatch paths don't share state — a failure in one doesn't
# silence the warning for the other.
_BACKUP_READY_NOTIFY_WARNED: bool = False

# v1.19.44: module-level flag for the OUTER plex_theme_lost
# dispatch loop swallow (around line 1873). Pre-fix that swallow
# was at log.debug unconditionally — an import error or
# config-load crash after a partial deploy silently dropped the
# entire dispatch cycle with no operator visibility. Class-9
# outer-catch-all sub-pattern: warn on first occurrence so the
# operator sees the failure at boot; drop to debug subsequently
# so the log doesn't drown. Resets only on process restart,
# which matches the cadence a deploy fix can land.
_THEME_LOST_DISPATCH_OUTER_WARNED: bool = False

# v1.21.5: hot-path warned flag for the new_tdb_theme_available
# notify dispatch (_maybe_notify_theme_available, post-resolve in
# _upsert_items). Same first-occurrence-warn / subsequent-debug
# class-9 pattern as the theme-lost flags above.
_THEME_AVAIL_NOTIFY_WARNED: bool = False

# v1.21.5: ~fire-once dedupe window per (media_type, tmdb_id) for the
# theme-available push. The sweep only ever sees a row on the enum
# that first inserts it, so this window only guards the rk-churn
# re-add case (Plex removed + re-added an item with a new rk).
_THEME_AVAIL_DEDUPE_SECONDS: int = 30 * 86400

# v1.19.80 (Opus-4.8 audit LOW): hot-path warned flag for the
# _upsert_items normalize_title swallow (line ~1143). Its identical
# twin at sync.py:483 got _SYNC_NORMALIZE_TITLE_WARNED in v1.17.11;
# this one was missed. A broken normalize_title (ImportError on a
# half-deploy, or a raise on a single bad title) would silently
# degrade title-matching library-wide. Warn once per process so the
# operator sees it at boot; debug subsequently. Same cadence as the
# other v1.17.11 hot-path flags above.
_PLEX_ENUM_NORMALIZE_TITLE_WARNED: bool = False
# v1.20.21 (sync+refresh audit): _section_enum_overdue swallowed its
# timestamp-parse failure at log.debug and returned False — silently
# disabling the 24h overdue bypass that gives the v1.18.89 reaper a
# chance on quiet sections (class-9). Warn-once so a persistently-
# failing overdue check is visible at boot.
_SECTION_ENUM_OVERDUE_WARNED: bool = False

# v0.51.128: consecutive full enums a plex_items row must be MISSING from
# Plex's section listing before the reaper DELETEs it + fires 💔 Theme lost.
# A single miss is treated as a transient Plex glitch (partial catalog / API
# hiccup) — the row's counter is incremented but the row survives; it only
# reaps once the miss persists across this many enums. Reappearing resets the
# counter. Full enums run ~daily on cron + on every manual // REFRESH PLEX,
# so a GENUINE removal is reaped after ~one extra cycle. 2 = one grace enum.
_REAP_MISS_THRESHOLD: int = 2


def _section_enum_overdue(
    db_path: Path, section_id: str, *, hours: int = 24,
) -> bool:
    """v1.18.92: True if the section's last full enum is older
    than `hours`. The signal: MAX(plex_items.last_seen_at) for
    the section — last_seen_at is bumped on every upsert, so
    the max across the section is "when did we last actually
    enumerate this section's items."

    Used by the contentChangedAt-skip path to force a full
    enum on quiet sections. The v1.18.89 reaper only runs
    inside _upsert_items; without this bypass, sections that
    Plex says haven't changed accumulate stale plex_items
    rows indefinitely (the user's repro: section 1 with 12 stale
    rows that the reaper couldn't reach).

    Fail-safe defaults:
    - No plex_items rows for the section → return False (no
      enum needed, nothing to reap)
    - Unparseable timestamp → return False (don't trigger
      surprise full enums on bad data)
    """
    from datetime import datetime, timezone, timedelta
    try:
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT MAX(last_seen_at) FROM plex_items "
                "WHERE section_id = ?",
                (section_id,),
            ).fetchone()
        max_seen = row[0] if row else None
        if not max_seen:
            return False
        # Normalize Z-suffix → +00:00 for fromisoformat.
        max_seen_dt = datetime.fromisoformat(
            max_seen.replace("Z", "+00:00")
        )
        age = datetime.now(timezone.utc) - max_seen_dt
        return age > timedelta(hours=hours)
    except Exception as e:  # noqa: BLE001
        # v1.20.21: warn-once (was a flat log.debug that hid a
        # persistently-failing overdue check). The bypass exists to
        # let the reaper run on quiet sections; if this silently
        # returns False forever, stale phantom-P rows accrue with no
        # signal. (ValueError/TypeError were redundant under Exception.)
        global _SECTION_ENUM_OVERDUE_WARNED
        if not _SECTION_ENUM_OVERDUE_WARNED:
            log.warning(
                "_section_enum_overdue: failed for section %s (%s) — "
                "returning False (24h overdue bypass disabled). Further "
                "occurrences log at debug.",
                section_id, e,
            )
            _SECTION_ENUM_OVERDUE_WARNED = True
        else:
            log.debug(
                "_section_enum_overdue: failed for section %s (%s)",
                section_id, e,
            )
        return False


def run_plex_enum(db_path: Path, plex_cfg: PlexConfig,
                   *, only_section_id: str | None = None,
                   collections_only: bool = False,
                   skip_collections: bool = False,
                   cancel_check=lambda: False) -> dict:
    """Enumerate Plex sections, upsert plex_items. Returns stats.

    `only_section_id`: scope the enumeration to one section (used by the
    per-section REFRESH button on /settings#plex). Default is every
    managed section.

    `collections_only` (v1.18.4): skip the main item walk + sidecar
    stat + per-item folder fallback + verify_theme_claims; only call
    `enumerate_collections_for_section` per section and upsert those
    collection rows. Used by the /collections tab's REFRESH button so
    a refresh of "just collections" doesn't take the full
    10K-movie-walk hit. Per the user: "is there anyway to make it so it
    only refresh collections and not all items within the libraries
    to increase speed." Collections are a tiny fraction of items
    (~1,200 vs ~16K on the user's install) and the /library/sections/
    {id}/collections endpoint is its own page-paginated path —
    skipping the /all endpoint cuts the wall time by ~10×.

    Failures on individual sections are logged and skipped so a single
    broken section doesn't kill the whole pass.
    """
    stats = {"sections": 0, "items_seen": 0, "inserted": 0, "updated": 0, "errors": 0}
    sections = list_sections(db_path)
    managed = [s for s in sections if s["included"]]
    if only_section_id:
        managed = [s for s in managed if s["section_id"] == only_section_id]
    if not managed:
        log.info("plex_enum: no managed sections, nothing to do")
        return stats

    # v1.12.106: live progress for the ops side-drawer.
    # v1.12.108: stage_current/total now track CURRENT-SECTION items,
    # not section count. Pre-fix a single-section enum hit
    # stage_current=1/stage_total=1 = 100% the instant the section
    # started, before any items had been processed — bar showed
    # complete throughout the run. Now stage_total starts at 0 (no
    # work known yet); per-section we set it to the section's item
    # count after fetch and bump stage_current as the upsert
    # completes. processed_total in detail_json carries the
    # cumulative items count across sections.
    op_progress.start_progress(
        db_path, op_id="plex_enum", kind="plex_enum",
        # v1.23.32: action-oriented label + seeded activity line so this opening
        # step (the %-less paged fetch of every library item from Plex) reads as
        # actively working, not a blank step (the user's report).
        stage="enumerate", stage_label="Fetching libraries from Plex",
        stage_total=0, processed_est=0,
        activity="Reading your libraries and their items from Plex…",
    )
    # v1.24.19: stamp a concise SCOPE label so the LIVE OPS drawer card head
    # says WHAT this refresh covers — the generic "// PLEX REFRESH" title alone
    # didn't distinguish a full all-libraries scan from a single-section or
    # collections-only refresh. The per-section stage_label below still names
    # the section actively being walked; this is the at-a-glance scope. Derived
    # from the args (managed is already filtered to the target section) so no
    # raw scope string needs plumbing through the worker.
    if only_section_id is None:
        _scope_summary = "All libraries"
    else:
        _sec_title = managed[0]["title"]
        if collections_only:
            _scope_summary = f"{_sec_title} collections"
        elif skip_collections:
            _scope_summary = f"{_sec_title} (items only)"
        else:
            _scope_summary = _sec_title
    op_progress.set_detail_field(
        db_path, "plex_enum", "scope_label", _scope_summary)

    def _cancel_check():
        return cancel_check() or op_progress.is_cancelled(db_path, "plex_enum")

    # v1.13.19: section-progress label helper. When the user is only
    # scanning one section (the typical "// SCAN PLEX" click on a
    # single-tab library), the "1/1" prefix is redundant noise — drop
    # it and let the section title carry the context. With multiple
    # sections in flight (the daily cron / dashboard SYNC + auto-enum)
    # the prefix tells the user where in the pipeline they are.
    def _section_label(idx: int, total: int, title: str, suffix: str = "") -> str:
        if total <= 1:
            base = title
        else:
            base = f"Section {idx}/{total}: {title}"
        return f"{base} ({suffix})" if suffix else base

    try:
        with PlexClient(plex_cfg) as client:
            # v1.14.74: read the live contentChangedAt for every
            # managed section in a single /library/sections call.
            # Used per-section below to decide whether the full
            # enumerate_section_items work can be skipped (when
            # equal to the stored last_enum_content_changed_at).
            # /library/sections is cheap (~50ms total) and we'd
            # already call it on cron/sync anyway via
            # refresh_sections — moving the read into plex_enum
            # itself avoids stale-cache races (e.g., Plex bumped
            # contentChangedAt between the last refresh_sections
            # and this enum). Empty string back from Plex falls
            # through to the full enum below — the skip is opt-in
            # only when we have a concrete value to compare.
            try:
                live_sections = client.discover_sections()
                live_cca = {ls.section_id: ls.content_changed_at
                            for ls in live_sections}
            except Exception as e:  # noqa: BLE001 — defensive
                log.warning(
                    "plex_enum: live contentChangedAt fetch failed (%s) — "
                    "falling through to full enum on every section",
                    e,
                )
                live_cca = {}
            stats["skipped_unchanged"] = 0
            # v0.51.101: the section(s) that actually got walked this run (not
            # contentChangedAt-skipped). Used below to scope — or skip — the
            # three table-wide end passes (reconcile + the two FS-stat health
            # passes) so a stable-library / partial refresh doesn't stat every
            # section's theme.mp3 on a network mount for nothing.
            worked_sections: set[str] = set()
            for s in managed:
                # v1.11.36: cooperative cancellation between sections.
                if _cancel_check():
                    from .worker import _JobCancelled
                    raise _JobCancelled()
                section_id = s["section_id"]
                section_type = s["type"]
                stats["sections"] += 1
                # v1.14.74: section-level delta gate. Skip the
                # full enumerate_section_items + upsert + verify
                # work when Plex's contentChangedAt for this
                # section is unchanged since our last successful
                # enum. Saves ~30s-2m per stable section. The
                # gate fires only when ALL THREE conditions hold:
                #   - Plex returned a non-empty contentChangedAt
                #     for this section (live_value).
                #   - We have a stored baseline from a previous
                #     successful enum (stored_value).
                #   - The two values match exactly.
                # Otherwise we fall through to the full enum and
                # will store the live value on success below.
                live_value = live_cca.get(section_id, "")
                stored_value = s.get("last_enum_content_changed_at") or ""
                content_changed_at_match = (
                    live_value
                    and stored_value
                    and live_value == stored_value
                )
                # v1.18.92: bypass the contentChangedAt-skip when the
                # section's last full enum is > 24h old. The v1.18.89
                # reaper only runs inside _upsert_items; the skip path
                # short-circuits BEFORE _upsert_items so quiet sections
                # accumulate stale rows indefinitely. the user's repro:
                # section 1 (Movies) had 12 stale rows that the reaper
                # couldn't reach because contentChangedAt hadn't bumped
                # since pre-v1.18.89 deploy — every plex_enum was
                # hitting the skip path. MAX(plex_items.last_seen_at)
                # for the section is the cheapest "when did we last
                # enumerate this section" signal that doesn't need a
                # new schema column.
                should_skip = bool(content_changed_at_match)
                if should_skip:
                    overdue = _section_enum_overdue(
                        db_path, section_id, hours=24,
                    )
                    if overdue:
                        log.info(
                            "plex_enum: section %s — bypassing "
                            "contentChangedAt-skip (last full enum > "
                            "24h ago; running full enum to give the "
                            "v1.18.89 reaper a chance to clean stale "
                            "rows)",
                            s["title"],
                        )
                        should_skip = False
                if should_skip:
                    stats["skipped_unchanged"] += 1
                    log.info(
                        "plex_enum: section %s — skipped (no changes "
                        "since contentChangedAt=%s)",
                        s["title"], live_value,
                    )
                    op_progress.update_progress(
                        db_path, "plex_enum",
                        stage_label=_section_label(
                            stats['sections'], len(managed),
                            s['title'], "no changes"),
                        stage_current=0,
                        stage_total=0,
                        activity=(
                            f"'{s['title']}': no changes since last "
                            f"enum (Plex contentChangedAt unchanged)"),
                    )
                    continue
                # v0.51.101: this section is being walked (not delta-skipped)
                # → its placements/local_files may change, so it needs the end
                # passes. Record it for the scoping/skip decision below.
                worked_sections.add(section_id)
                # Pre-fetch: bar resets to 0 of "unknown" so it
                # doesn't carry the previous section's fill. Label
                # encodes section index for context.
                op_progress.update_progress(
                    db_path, "plex_enum",
                    stage_label=_section_label(stats['sections'], len(managed), s['title']),
                    stage_current=0,
                    stage_total=0,
                    activity=f"Fetching '{s['title']}' from Plex",
                )
                # v1.12.128: per-page progress emit during the Plex
                # fetch. Pre-fix the bar sat at stage_total=0
                # (indeterminate shimmer) for the whole fetch — on a
                # 10k-item section that's ~17s of "is this even
                # working?" silence. Now we tick (received / total)
                # as Plex feeds back pages of 500.
                import time as _t
                _last_fetch_emit = [_t.monotonic()]
                def _on_fetch_progress(received: int, total: int | None) -> None:
                    if (_t.monotonic() - _last_fetch_emit[0]) < 0.3:
                        return
                    _last_fetch_emit[0] = _t.monotonic()
                    op_progress.update_progress(
                        db_path, "plex_enum",
                        stage_label=_section_label(
                            stats['sections'], len(managed), s['title'], "fetch"),
                        stage_current=received,
                        stage_total=total or 0,
                        # v1.24.19: when Plex returns no totalSize the bar stays
                        # indeterminate (stage_total=0) AND the N/N counter is
                        # hidden (drawer shows the counter only when total>0) —
                        # so the climbing `received` was invisible and the fetch
                        # read as stuck. Surface the running count in the
                        # activity feed so it reads as actively working. When
                        # total IS known the counter + real bar already convey
                        # progress, so don't spam the feed (None = no append).
                        activity=(
                            None if total else
                            f"Fetching '{s['title']}' — "
                            f"{received} items so far…"),
                    )
                # v1.13.21 (was v1.13.20): per-show folder-path fallback
                # also emits progress so the bar doesn't appear stuck
                # at 100% while plex.py walks /library/metadata per
                # show. Stage label adds "(folder paths)" to make the
                # phase legible.
                _last_fb_emit = [_t.monotonic()]
                def _on_fallback_progress(done: int, total: int) -> None:
                    if (_t.monotonic() - _last_fb_emit[0]) < 0.3 and done < total:
                        return
                    _last_fb_emit[0] = _t.monotonic()
                    op_progress.update_progress(
                        db_path, "plex_enum",
                        stage_label=_section_label(
                            stats['sections'], len(managed),
                            s['title'], "fetch") + " (folder paths)",
                        stage_current=done,
                        stage_total=total,
                    )
                # v1.18.4: collections-only mode skips the heavy
                # /library/sections/{id}/all walk + sidecar stat +
                # per-item folder fallback. The /collections tab's
                # REFRESH button only needs the collection plex_items
                # rows; the rest is wasted work for that surface.
                # _scope_label is needed by the collections-pass
                # below regardless, so compute it up front.
                _scope_label = _section_label(
                    stats['sections'], len(managed), s['title'])
                if not collections_only:
                    try:
                        items = client.enumerate_section_items(
                            section_id=section_id, media_type=section_type,
                            progress_callback=_on_fetch_progress,
                            fallback_progress_callback=_on_fallback_progress,
                        )
                    except Exception as e:
                        log.warning("plex_enum: section %s failed: %s", s["title"], e)
                        stats["errors"] += 1
                        op_progress.update_progress(
                            db_path, "plex_enum",
                            error_count=stats["errors"],
                            activity=f"'{s['title']}' failed: {e}",
                        )
                        continue
                    # Now we know the section's item count. Set the bar
                    # to "0 of N" while the upsert runs.
                    op_progress.update_progress(
                        db_path, "plex_enum",
                        stage_total=len(items),
                        stage_current=0,
                        activity=f"Upserting {len(items)} items from '{s['title']}'",
                    )
                    stats["items_seen"] += len(items)
                    ins, upd = _upsert_items(
                        db_path, items, cancel_check=_cancel_check,
                        # v1.12.18: pass section_id so the resolve pass that
                        # runs at the end of _upsert_items can scope itself
                        # to JUST the rows we just touched. Pre-fix the
                        # resolve always ran against the full plex_items
                        # table (~10K+ rows on a populated install), which
                        # turned a 28-item section refresh into a 70s+ job.
                        section_id=section_id,
                        # v1.12.124: drive op_progress per batch so the
                        # bar reflects in-section progress instead of
                        # jumping 0 → 100% at section boundaries.
                        # v1.12.128: distinct phase labels so the user
                        # can read which step of the section's pipeline
                        # is currently running.
                        progress_op_id="plex_enum",
                        progress_total=len(items),
                        progress_phase1_label=f"{_scope_label} (sidecar)",
                        progress_phase2_label=f"{_scope_label} (upsert)",
                    )
                    stats["inserted"] += ins
                    stats["updated"] += upd
                    log.info("plex_enum: section %s — %d items (%d new, %d updated)",
                             s["title"], len(items), ins, upd)

                # v1.18.0: collections enumeration pass. After the
                # section's main item walk, also fetch every collection
                # in this section via GET /library/sections/{id}/
                # collections. Same upsert path as items — collections
                # get a plex_items row with media_type='collection'
                # (schema v55 widened the CHECK to accept it).
                #
                # Errors here don't tank the section: a 404 means the
                # section type doesn't support collections (rare), a
                # transient failure should retry on the next enum.
                # Catch-and-log so collection enumeration failure
                # doesn't kill the main item upsert that just
                # succeeded.
                # v1.23.77: items-only library-tab refresh (movies/tv/anime
                # REFRESH FROM PLEX) skips the collections walk — symmetric to
                # the v1.18.4 collections_only mode. The /collections tab, the
                # full "scan everything" refresh, and the nightly cron cascade
                # still walk both. The contentChangedAt stamp below is also
                # gated on skip_collections so an items-only pass doesn't
                # delta-suppress the next collections refresh of this section.
                collections_fetch_failed = False
                if not skip_collections:
                    try:
                        collections = client.enumerate_collections_for_section(
                            section_id=section_id,
                        )
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "plex_enum: collections fetch for section %s "
                            "failed (%s) — skipping this section's "
                            "collection pass, will retry next enum",
                            s["title"], e,
                        )
                        collections = []
                        # v1.22.72: count the failure + block the
                        # contentChangedAt stamp below — pre-fix the stamp
                        # made the next enum SKIP the section (delta gate),
                        # so "will retry next enum" was false and stale
                        # collection rows persisted ~24h behind a single
                        # WARN with the op finishing done/0 errors. Same
                        # class the v1.15.34 redesign closed for the items
                        # walk.
                        collections_fetch_failed = True
                        stats["errors"] += 1
                    if collections:
                        coll_ins, coll_upd = _upsert_items(
                            db_path, collections, cancel_check=_cancel_check,
                            section_id=section_id,
                            progress_op_id="plex_enum",
                            progress_total=len(collections),
                            progress_phase1_label=(
                                f"{_scope_label} (collections)"),
                            progress_phase2_label=(
                                f"{_scope_label} (collections upsert)"),
                        )
                        stats["inserted"] += coll_ins
                        stats["updated"] += coll_upd
                        stats["items_seen"] += len(collections)
                        log.info(
                            "plex_enum: section %s — %d collections "
                            "(%d new, %d updated)",
                            s["title"], len(collections),
                            coll_ins, coll_upd,
                        )

                # v1.12.112: verify Plex's theme claims for this
                # section's rows. Targets exactly the ambiguous case
                # (has_theme=1 with no fresh verification) — by far
                # the minority of rows in any library. The HEAD probe
                # distinguishes legitimate Plex-served themes
                # (themerr-plex embeds, Plex Pass cloud, sidecars)
                # from stale metadata cache. Steady-state cost is
                # near zero: once verified, the result is cached and
                # only re-tested when Plex's URI changes.
                # v1.18.4: skip the verify_theme_claims pass under
                # collections_only. Collections have their own theme
                # presence signal via the upload_collection_theme
                # path (pi.has_theme=1 is the canonical truth); HEAD-
                # probing each P-source movie/show row when the user
                # only asked for a collections refresh is exactly the
                # latency the collections_only flag exists to avoid.
                if collections_only:
                    verified_n = 0
                else:
                    verified_n = _verify_theme_claims(
                        db_path, client, section_id=section_id,
                        cancel_check=_cancel_check,
                    )
                # v1.14.74: stamp the section's last_enum_content_changed_at
                # with the LIVE value we read at enum start (not a
                # fresh re-read). Race-safe: if Plex bumped
                # contentChangedAt mid-enum (user added a file
                # during the fetch), our enum may have missed
                # those items, but storing the OLD live_value
                # ensures the next run's gate fires false (live
                # is now newer) and a follow-up enum picks up
                # the missed items. Storing the new value would
                # silently mask the gap.
                # v1.18.4: skip the stamp under collections_only.
                # The stored value gates the NEXT full enum's
                # skipping logic; rewriting it after a collections-
                # only pass would incorrectly tell the next full
                # enum that the section is "up to date" when it
                # never re-walked /all. Leaving the value
                # untouched keeps the full-enum delta gate honest.
                # v1.22.72: a failed collections fetch must NOT stamp —
                # the stamp arms the skip gate and defeats the retry.
                # v1.23.77: an items-only pass (skip_collections) must NOT
                # stamp either — the section never walked collections, so
                # arming the delta gate would let the next full enum skip it
                # and starve collections of their refresh.
                if (not collections_only and not skip_collections
                        and not collections_fetch_failed):
                    live_value = live_cca.get(section_id, "")
                    if live_value:
                        with get_conn(db_path) as conn, transaction(conn):
                            conn.execute(
                                "UPDATE plex_sections SET "
                                "last_enum_content_changed_at = ? "
                                "WHERE section_id = ?",
                                (live_value, section_id),
                            )
                # Section done — fill the bar, bump cumulative.
                # v1.14.96: pluralize the verified-count suffix —
                # `1 themes verified` reads wrong. Singular falls
                # out when verified_n == 1; everything else stays
                # `N themes verified`.
                verified_word = (
                    "theme" if verified_n == 1 else "themes"
                )
                # v1.18.4: under collections_only, `items` was never
                # populated (the main walk was skipped) and `ins` /
                # `upd` are zero. Source the activity counts from
                # the collection-pass results below instead — bar
                # advance happens after the collections-pass.
                bar_items_n = 0 if collections_only else len(items)
                bar_ins = 0 if collections_only else ins
                bar_upd = 0 if collections_only else upd
                op_progress.update_progress(
                    db_path, "plex_enum",
                    stage_current=bar_items_n,
                    stage_total=bar_items_n,
                    processed_total=stats["items_seen"],
                    activity=(f"'{s['title']}': {bar_ins} new, {bar_upd} updated"
                              + (f", {verified_n} {verified_word} verified"
                                 if verified_n else "")),
                )

        # v0.51.101: decide the scope for the three end passes (reconcile + the
        # two FS-stat health passes). A single-section REFRESH always runs them
        # for its target (the user asked for fresh state, even if the section
        # delta-skipped). A full/cron run scopes to the sections that were
        # actually walked; a no-work run (everything delta-skipped, no explicit
        # target) skips all three — nothing motif tracks on disk changed. The
        # nightly cron re-walks every overdue section (v1.18.92 24h bypass), so
        # per-section scoping there still sums to full coverage over ~24h.
        if only_section_id is not None:
            _scope_sections: list[str] | None = [only_section_id]
        else:
            _scope_sections = sorted(worked_sections) or None
        if _scope_sections is None:
            log.info("plex_enum: no sections walked (all unchanged) — skipping "
                     "the reconcile + health passes this run")
        op_progress.update_progress(
            db_path, "plex_enum",
            stage="reconcile",
            stage_label="Reconciling placement paths",
            activity="Checking for Plex folder renames",
        )
        # v1.10.8: detect Plex folder renames/moves and re-link the canonical
        # theme. plex_items.folder_path now reflects the current Plex-side
        # location; placements.media_folder reflects where motif previously
        # placed the hardlink. When they diverge, the OLD path is stale —
        # update the placement to the new path and enqueue a place job so
        # Plex finds the theme in its current folder.
        relinked = (reconcile_placement_paths(db_path, section_ids=_scope_sections)
                    if _scope_sections is not None else 0)
        stats["relinked"] = relinked

        # v0.50.43: distinct 'health' stage so the RUN INSIGHT waterfall breaks the
        # two stat-every-theme.mp3 passes (placement + canonical) out of the
        # 'reconcile' bar. On a slow Unraid/NFS mount these stats can dominate the
        # tail; lumped under 'reconcile' they read as folder-rename time. A
        # post-loop, forward-only transition — no per-section bouncing.
        op_progress.update_progress(
            db_path, "plex_enum",
            stage="health",
            stage_label="Verifying theme files on disk",
            activity="Checking placement + canonical theme.mp3 on disk",
        )
        # v1.23.25: stamp placement health so a broken placement (theme.mp3
        # gone from the Plex folder) ranks at the top of NEEDS WORK and groups
        # in the PL=broken sort bucket — the live red dot is a per-render stat
        # the paginated SQL sort can't see.
        try:
            # v0.51.101: scoped to the walked section(s); a no-work run gets a
            # benign empty result so the logging below stays a clean no-op.
            health = (
                verify_placement_health(db_path, section_ids=_scope_sections)
                if _scope_sections is not None
                else {"missing": 0, "checked": 0, "skipped": 0, "pruned": 0})
            stats["placements_missing"] = health["missing"]
            stats["placements_pruned"] = health.get("pruned", 0)
            _skipped = health.get("skipped", 0)
            if health["missing"] or _skipped:
                # v1.23.30: surface the skipped count too — a mount fault that
                # stat-raised on many folders is otherwise invisible (the
                # denominator excludes skips), making a hiccup look like a
                # clean pass.
                _tail = (f" ({_skipped} skipped — stat error)"
                         if _skipped else "")
                log_event(db_path, level="INFO", component="plex_enum",
                          message=(f"Placement health: {health['missing']} of "
                                   f"{health['checked']} sidecar placement(s) "
                                   f"missing theme.mp3{_tail}"))
            if health.get("pruned"):
                # v1.23.26: superseded sidecar records (a live API upload
                # serves the theme) pruned — they drove a false red PL dot +
                # a dual-placement JOIN fan-out.
                log_event(db_path, level="INFO", component="plex_enum",
                          message=(f"Pruned {health['pruned']} superseded "
                                   f"sidecar placement record(s) — a live "
                                   f"plex_upload serves the theme"))
        except Exception as e:
            # Best-effort: a health-pass failure must never fail the enum.
            log.warning("verify_placement_health failed: %s", e)

        # v1.23.37: stamp canonical health so a broken canonical (theme.mp3
        # gone from motif's storage, DL=red) groups in the DL=broken sort
        # bucket — the live red dot is a per-render stat the paginated SQL
        # sort can't see. The canonical-side mirror of the pass above.
        try:
            from ..config import Settings
            _themes_dir = Settings().themes_dir
            # v0.51.101: skip on a no-work run; scope to the walked section(s).
            if _themes_dir is not None and _scope_sections is not None:
                chealth = verify_canonical_health(
                    db_path, _themes_dir, section_ids=_scope_sections)
                stats["canonical_missing"] = chealth["missing"]
                _cskipped = chealth.get("skipped", 0)
                if chealth["missing"] or _cskipped:
                    _ctail = (f" ({_cskipped} skipped — stat error)"
                              if _cskipped else "")
                    log_event(db_path, level="INFO", component="plex_enum",
                              message=(f"Canonical health: {chealth['missing']} "
                                       f"of {chealth['checked']} canonical "
                                       f"theme(s) missing on disk{_ctail}"))
        except Exception as e:
            # Best-effort: a health-pass failure must never fail the enum.
            log.warning("verify_canonical_health failed: %s", e)

        # v1.14.74: include the contentChangedAt-skipped count so
        # the user can see the perf payoff in the LOGS view.
        skipped = stats.get("skipped_unchanged", 0)
        skipped_suffix = f", {skipped} skipped (no changes)" if skipped else ""
        log_event(db_path, level="INFO", component="plex_enum",
                  message=f"Enumerated {stats['sections']} sections, "
                          f"{stats['items_seen']} items "
                          f"({stats['inserted']} new, {stats['updated']} updated"
                          f"{', ' + str(relinked) + ' relinked' if relinked else ''}"
                          f"{skipped_suffix})")
        op_progress.update_progress(
            db_path, "plex_enum",
            activity=(
                f"Done — {stats['sections']} sections, "
                f"{stats['items_seen']} items, "
                f"{stats['inserted']} new, {stats['updated']} updated"
                + (f", {relinked} relinked" if relinked else "")
                + skipped_suffix
            ),
        )
        # v1.21.25: uniform done_summary so PLEX REFRESH's done line shows
        # the same structured breakdown as THEMERRDB SYNC instead of the
        # generic "Done — N items processed".
        _ds = [
            {"l": "items", "v": stats["items_seen"]},
            {"l": "new", "v": stats["inserted"]},
            {"l": "updated", "v": stats["updated"]},
        ]
        if relinked:
            _ds.append({"l": "relinked", "v": relinked})
        op_progress.set_detail_field(db_path, "plex_enum", "done_summary", _ds)
        op_progress.finish_progress(db_path, "plex_enum", status="done")
        return stats
    except Exception as e:
        from .worker import _JobCancelled
        op_progress.finish_progress(
            db_path, "plex_enum",
            status="cancelled" if isinstance(e, _JobCancelled) else "failed",
            error_message=("cancelled by user"
                           if isinstance(e, _JobCancelled) else str(e)),
        )
        raise


def delete_superseded_sidecar_placement(
        conn, media_type, tmdb_id, section_id, media_folder, edition_key) -> int:
    """v1.23.31: DELETE the sidecar placement record at `media_folder` for this
    item+section+edition — superseded by a live plex_upload (the API upload now
    serves the theme; the on-disk sidecar is stale). The ONE predicate shared by
    the worker's post-API-upload cleanup (_do_place_collection) and
    verify_placement_health's prune, so they can't drift. Returns rowcount."""
    cur = conn.execute(
        "DELETE FROM placements "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
        "  AND edition_key = ? AND media_folder = ? "
        "  AND placement_kind != 'plex_upload'",
        (media_type, tmdb_id, section_id, edition_key, media_folder))
    return cur.rowcount


def _section_scope_clause(section_ids: list[str] | None,
                          *, col: str = "section_id") -> tuple[str, tuple]:
    """v0.51.101: build an ' AND <col> IN (?,…)' fragment + params so a
    reconcile/health pass can scope to the section(s) a plex_enum actually
    walked (or the one a single-section REFRESH targeted). Returns ('', ())
    when section_ids is None/empty — the table-wide default the nightly
    all-sections cron relies on for full coverage."""
    if not section_ids:
        return "", ()
    ph = ",".join("?" for _ in section_ids)
    return f" AND {col} IN ({ph})", tuple(section_ids)


def verify_placement_health(db_path: Path, *,
                            section_ids: list[str] | None = None) -> dict:
    """v1.23.25: stamp placements.theme_present so the library's NEEDS WORK
    (attention) + PL sorts can rank a BROKEN placement — theme.mp3 gone from
    the Plex media folder — cheaply in SQL. The red PL dot is otherwise a
    per-render filesystem stat (_annotate_canonical_state's placement_missing)
    the paginated server-side sort can't see.

    Stats each sidecar placement's `media_folder/theme.mp3` — the SAME check
    the live PL dot uses, so the stored flag agrees with the rendered dot.
    Writes theme_present = 1 (present) / 0 (verified missing). A stat that
    raises OSError (transient Unraid/NFS fault) is INDETERMINATE: the row is
    SKIPPED so the prior value is preserved — a mount hiccup must never flag a
    healthy placement broken (which would spam NEEDS WORK). plex_upload rows
    (media_folder='') have no on-disk sidecar and are excluded.

    Stale by design: health is as fresh as the last enum, so a theme deleted
    between runs renders the live red dot immediately but only sorts to the
    NEEDS WORK / PL-broken top after the next enum stamps it.

    v1.23.26: a missing sidecar that has a LIVE plex_upload sibling for the
    same (media_type, tmdb_id, section_id, edition_key) is NOT broken — the API
    upload supersedes it and the sidecar record is stale cruft. The v1.18.59
    leftover-file cleanup unlinked the old theme.mp3 but LEFT the hardlink/copy
    placement row, so it read theme_present=0 (false red PL dot) AND collided
    with the new plex_upload row in the two-tier library JOIN — a dual-placement
    fan-out that rendered the user's Avatar/Borat/Flow/Zootopia rows cyan or red
    depending on which placement the query surfaced, and kept NEEDS WORK from
    surfacing them. Such records are PRUNED here. A missing sidecar with NO
    plex_upload sibling is genuinely broken and still stamps theme_present=0
    (surfaces in NEEDS WORK). Returns {checked, missing, pruned}."""
    # v0.51.101: scope the FS-stat sidecar sweep (the cost) to the walked
    # section(s) when the caller passes section_ids — a single-section REFRESH
    # or a partial cron no longer stats every section's theme.mp3. The
    # pure-DB plex_upload staleness pass below stays GLOBAL (it's cheap and its
    # rating_key-liveness check is inherently cross-section). section_ids=None
    # keeps the table-wide default the nightly all-sections cron uses.
    _scope_sql, _scope_params = _section_scope_clause(section_ids)
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT media_type, tmdb_id, section_id, media_folder, edition_key "
            "FROM placements "
            "WHERE media_folder IS NOT NULL AND media_folder != '' "
            "  AND placement_kind != 'plex_upload'" + _scope_sql,
            _scope_params,
        ).fetchall()
        # v1.23.26: items with a live API-upload placement. A missing sidecar
        # keyed here is superseded cruft, not a broken theme — prune it.
        upload_keys = {
            (r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"])
            for r in conn.execute(
                "SELECT media_type, tmdb_id, section_id, edition_key "
                "FROM placements WHERE placement_kind = 'plex_upload'"
                + _scope_sql, _scope_params)
        }
        total_placements = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE 1=1" + _scope_sql,
            _scope_params).fetchone()[0]
    now = now_iso()
    present_updates: list = []   # theme_present=1 — confirmed on disk
    missing_updates: list = []   # theme_present=0 — confirmed gone
    prune: list = []
    skipped = 0
    # v0.51.103: stat every sidecar's theme.mp3 across a bounded thread pool
    # (mirrors _upsert_items Phase-1). The stat is the cost on a network mount;
    # the bucketing below stays SERIAL so the skipped/present/missing/prune
    # accounting + first-skip-warns + mount-fault cap are byte-for-byte
    # unchanged. is_file() is GIL-releasing I/O, so the pool parallelizes it.
    from concurrent.futures import ThreadPoolExecutor

    def _stat_present(row_r):
        try:
            folder = Path(row_r["media_folder"])
            present = (folder / "theme.mp3").is_file()
            # v0.51.106: when the theme.mp3 reads missing, also probe the
            # CONTAINING folder. A /data mount fault takes the folder down too
            # (is_dir()→False), whereas a genuine theme.mp3 deletion leaves the
            # folder alive. This is the signal that tells the two apart for a
            # SMALL scoped section, where the absolute count cap can't trip
            # (finding #1). `present or ...` short-circuits the extra stat on
            # the healthy majority. A raised is_dir() (ESTALE) falls to the
            # except → skipped (preserving the row), same as a raised is_file().
            folder_alive = present or folder.is_dir()
            return (row_r, present, folder_alive, None)
        except OSError as e:  # ESTALE/EIO/ETIMEDOUT — indeterminate
            return (row_r, None, None, e)

    if rows:
        with ThreadPoolExecutor(max_workers=16) as _ex:
            stat_results = list(_ex.map(_stat_present, rows))
    else:
        stat_results = []
    folder_gone = 0  # v0.51.106: missing sidecars whose folder is ALSO gone
    for r, present, folder_alive, err in stat_results:
        if err is not None:
            # v1.23.30 (class-9): a RAISED stat (ESTALE/EIO/ETIMEDOUT) is
            # indeterminate — skip the row, preserving its prior value. First
            # skip warns so a mount hiccup isn't silent; the rest stay debug.
            skipped += 1
            (log.warning if skipped == 1 else log.debug)(
                "verify_placement_health: stat %s failed (%s) — skipped "
                "(preserving prior theme_present)", r["media_folder"], err)
            continue
        key = (r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"])
        if not present and key in upload_keys:
            # Superseded by a live API upload — prune the stale record.
            prune.append((r["media_type"], r["tmdb_id"], r["section_id"],
                          r["media_folder"], r["edition_key"]))
            continue
        if not present and not folder_alive:
            folder_gone += 1  # missing theme.mp3 AND the folder is gone too
        row = (1 if present else 0, now, r["media_type"], r["tmdb_id"],
               r["section_id"], r["media_folder"], r["edition_key"])
        (present_updates if present else missing_updates).append(row)
    # v1.23.30 (#1 code-review): is_file() returns False — NOT OSError — when a
    # folder's parent is unmounted (ENOENT/ENOTDIR are in pathlib's ignore
    # list), so a /data outage slips past the OSError skip above and reads
    # EVERY sidecar missing. Bound the 0-stamps AND the prune together: an
    # implausibly large missing set is a mount fault, not real breakage, so
    # skip BOTH this run — one blip must never flip the whole library to
    # false-broken in NEEDS WORK / the red PL sort (v1.23.26 capped only the
    # prune, leaving the theme_present=0 UPDATE unbounded). Confirmed-present
    # stamps are always safe.
    cap = max(50, total_placements // 4)
    # v1.23.42: fold the OSError-skipped count into the suspect total. A real
    # mount fault shows up as a MIX — some stats RAISE (skipped, ESTALE/EIO),
    # some return False (counted missing, ENOENT/ENOTDIR). Gating on missing +
    # prune alone let a mostly-ESTALE outage, whose handful of False-reads
    # stayed under the cap, stamp those few rows false-broken. missing + prune
    # + skipped over the cap means the mount is clearly unhealthy — skip both
    # the missing-stamps AND the prune this run.
    suspect = len(missing_updates) + len(prune) + skipped
    # v0.51.106: the absolute cap can't trip on a SMALL scoped section (the
    # v0.51.101 section scoping): a /data blip that reads every one of a
    # 30-sidecar section missing has suspect=30 < the floor of 50, so the guard
    # never fires and the whole section false-stamps broken. A COUNT/ratio gate
    # can't distinguish that from a small library where 30 themes are genuinely
    # broken (v1.23.30's contract: those MUST surface) — the two are identical
    # by count. The distinguisher is folder liveness: a mount fault takes the
    # CONTAINING folder down too (folder_gone), whereas a genuine theme.mp3
    # deletion leaves the folder alive. So trip when ~all of the examined set
    # read missing-AND-folder-gone (a live library's folders don't vanish en
    # masse). Floored at 5 so a couple of real folder removals still stamp
    # broken; folder-alive missing rows are never suppressed by this clause.
    examined = len(present_updates) + suspect
    if (suspect > cap
            or (folder_gone >= 5 and folder_gone * 10 >= examined * 9)):
        log.warning(
            "verify_placement_health: suspected mount fault (%d "
            "missing/superseded/skipped vs cap %d; %d of the missing had a "
            "gone folder) — stamping only the %d confirmed-present rows this "
            "run; skipping %d missing-stamps + prune",
            suspect, cap, folder_gone, len(present_updates),
            len(missing_updates))
        missing_updates = []
        prune = []
    updates = present_updates + missing_updates
    missing = len(missing_updates)
    pruned = 0
    if updates or prune:
        with get_conn(db_path) as conn, transaction(conn):
            if updates:
                conn.executemany(
                    "UPDATE placements SET theme_present = ?, "
                    "    placement_health_checked_at = ? "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                    "  AND media_folder = ? AND edition_key = ?",
                    updates)
            for p in prune:
                # p = (media_type, tmdb_id, section_id, media_folder, edition_key)
                if delete_superseded_sidecar_placement(
                        conn, p[0], p[1], p[2], p[3], p[4]):
                    pruned += 1
                    log.info(
                        "verify_placement_health: pruned superseded sidecar "
                        "placement %s for %s/%s section=%s (a live plex_upload "
                        "sibling serves the theme)", p[3], p[0], p[1], p[2])
    # v1.24.28: plex_upload placements have no on-disk sidecar to stat, so the
    # sidecar pass above EXCLUDES them — leaving theme_present=NULL and the UI
    # showing them "placed" forever even after a Plex delete+re-add destroys the
    # rating_key the theme was uploaded to. the user's Avenue Q: uploaded to rk
    # 660896, Plex re-added it as 714864 → nothing serves the theme, yet the row
    # read PL/PU. Stamp theme_present=1 when plex_rating_key is still a live
    # plex_items row, else 0 (stale → the read path nulls media_folder +
    # placement_kind and shows the RE-PUSH badge). Self-healing: a manual
    # re-push updates plex_rating_key to the new live rk → next enum re-stamps 1.
    # Guarded on plex_rating_key IS NOT NULL so legacy rows that never stored an
    # rk stay optimistically placed (no regression). rating_key is the plex_items
    # PK, so existence alone is the signal — a re-add always mints a fresh rk, so
    # no section/edition qualifier is needed. Pure-DB (no FS stat) so it's immune
    # to the mount-fault cap above and runs unconditionally.
    pu_stale = 0
    pu_skipped = False
    with get_conn(db_path) as conn, transaction(conn):
        # Confirmed-present stamps are always safe: a rating_key that IS in
        # plex_items genuinely serves the upload.
        conn.execute(
            """
            UPDATE placements SET theme_present = 1,
                                  placement_health_checked_at = ?
             WHERE placement_kind = 'plex_upload' AND plex_rating_key IS NOT NULL
               AND EXISTS (SELECT 1 FROM plex_items pi
                           WHERE pi.rating_key = placements.plex_rating_key)
            """,
            (now,),
        )
        # v1.24.39 (review #5): the 0-stamp drives RP + the v1.24.29 auto
        # re-push, so it's destructive. If plex_items didn't populate this run
        # (a failed/aborted enum left it EMPTY), EVERY uploaded rk reads stale
        # via the NOT EXISTS → a mass false re-push storm. Skip the 0-stamps
        # when plex_items is empty: EXISTS is meaningless against an empty
        # table, and a real library with uploads always has items. Partial
        # under-population is already bounded upstream by the v1.18.89 reaper's
        # `seen_rks` non-empty gate + 20%/50-row abort cap (it can't empty a
        # whole section in one bad enum), so this empty-guard covers the
        # residual catastrophic case. A genuine mass Plex re-add leaves
        # plex_items FULL with fresh rks, so it sails past this and the legit
        # re-push proceeds.
        plex_items_total = conn.execute(
            "SELECT COUNT(*) FROM plex_items").fetchone()[0]
        if plex_items_total == 0:
            pu_skipped = True
        else:
            conn.execute(
                """
                UPDATE placements SET theme_present = 0,
                                      placement_health_checked_at = ?
                 WHERE placement_kind = 'plex_upload'
                   AND plex_rating_key IS NOT NULL
                   AND NOT EXISTS (SELECT 1 FROM plex_items pi
                                   WHERE pi.rating_key = placements.plex_rating_key)
                """,
                (now,),
            )
        pu_stale = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE placement_kind = 'plex_upload' "
            "  AND plex_rating_key IS NOT NULL AND theme_present = 0"
        ).fetchone()[0]
    if pu_skipped:
        log.warning("verify_placement_health: plex_items is EMPTY — skipping the "
                    "plex_upload staleness 0-stamps this run (a failed enum, not "
                    "real loss; would falsely flip every upload to RP)")
    elif pu_stale:
        log.info("verify_placement_health: %d plex_upload placement(s) are "
                 "STALE (uploaded rating_key no longer live — re-push needed)",
                 pu_stale)
    return {"checked": len(updates), "missing": missing, "pruned": pruned,
            "skipped": skipped, "plex_upload_stale": pu_stale,
            "plex_upload_skipped": pu_skipped}


def verify_canonical_health(db_path: Path, themes_dir: Path, *,
                            section_ids: list[str] | None = None) -> dict:
    """v1.23.37: stamp local_files.canonical_present so the library's DL sort
    can rank a BROKEN canonical — the theme.mp3 gone from motif's canonical
    storage (themes_dir/file_path) while the local_files row persists — cheaply
    in SQL. The red DL dot is otherwise a per-render filesystem stat
    (_annotate_canonical_state's canonical_missing) the paginated server-side
    sort can't see. The canonical-side mirror of verify_placement_health
    (v1.23.25).

    Stats each local_files row's `themes_dir/file_path` — the SAME check the
    live DL dot uses, so the stored flag agrees with the rendered dot — and
    writes canonical_present = 1 (present) / 0 (verified missing). A stat that
    raises OSError (transient Unraid/NFS fault) is INDETERMINATE: the row is
    SKIPPED so the prior value is preserved — a mount hiccup must never flag a
    healthy canonical broken. is_file() returns False (NOT OSError) when the
    mount's parent is gone (ENOENT/ENOTDIR are in pathlib's ignore list), so an
    implausibly large missing set is a mount fault, not real breakage, and is
    capped — one /data blip must never flip the whole library to false-broken.
    Confirmed-present stamps are always safe.

    Stale by design: health is as fresh as the last enum, so a canonical
    deleted between runs renders the live red dot immediately but only sorts to
    the DL=broken bucket after the next enum stamps it. `themes_dir` is the
    canonical-storage root (file_path is relative to it); the caller passes
    Settings().themes_dir and skips the pass when it's unconfigured. Returns
    {checked, missing, skipped}."""
    # v0.51.101: scope the FS-stat sweep to the walked section(s) when given.
    _scope_sql, _scope_params = _section_scope_clause(section_ids)
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT media_type, tmdb_id, section_id, edition_key, file_path "
            "FROM local_files WHERE file_path IS NOT NULL AND file_path != ''"
            + _scope_sql, _scope_params
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) FROM local_files WHERE 1=1" + _scope_sql,
            _scope_params).fetchone()[0]
    # v1.23.42: themes_dir liveness gate. If the canonical-storage root itself
    # isn't a live directory (the /data mount dropped), EVERY file_path stat
    # reads missing via is_file()→False (ENOENT/ENOTDIR are in pathlib's ignore
    # list, so the per-row OSError skip never fires). The cap below would still
    # catch an all-missing run, but probing the root up front is the direct
    # signal: a dead root means real breakage is unknowable — preserve every
    # row's prior canonical_present instead of stamping the library
    # false-broken off a mount fault (present-stamps would be 0 anyway).
    try:
        root_alive = themes_dir.is_dir()
    except OSError:
        root_alive = False
    if not root_alive:
        log.warning(
            "verify_canonical_health: themes_dir %s is not a live directory "
            "(suspected mount fault) — skipping this run, preserving all %d "
            "prior canonical_present values", themes_dir, len(rows))
        return {"checked": 0, "missing": 0, "skipped": len(rows)}
    now = now_iso()
    present_updates: list = []
    missing_updates: list = []
    skipped = 0
    # v0.51.103: parallelize the per-file stat (mirror of verify_placement_
    # health); the bucketing below stays serial so the accounting + mount-fault
    # cap are unchanged.
    from concurrent.futures import ThreadPoolExecutor

    def _stat_present(row_r):
        try:
            return (row_r, (themes_dir / row_r["file_path"]).is_file(), None)
        except OSError as e:
            return (row_r, None, e)

    if rows:
        with ThreadPoolExecutor(max_workers=16) as _ex:
            stat_results = list(_ex.map(_stat_present, rows))
    else:
        stat_results = []
    for r, present, err in stat_results:
        if err is not None:
            skipped += 1
            (log.warning if skipped == 1 else log.debug)(
                "verify_canonical_health: stat %s failed (%s) — skipped "
                "(preserving prior canonical_present)", r["file_path"], err)
            continue
        row = (1 if present else 0, now, r["media_type"], r["tmdb_id"],
               r["section_id"], r["edition_key"])
        (present_updates if present else missing_updates).append(row)
    cap = max(50, total // 4)
    # v1.23.42: fold the OSError-skipped count into the suspect total (mirror
    # of verify_placement_health). A partial mount fault is a MIX — some stats
    # RAISE (skipped), some return False (missing). Gating on missing alone let
    # a mostly-ESTALE outage, whose few False-reads stayed under the cap, stamp
    # those rows false-broken. missing + skipped over the cap = unhealthy mount.
    # v0.51.106: the small-scoped-section blind spot that verify_placement_
    # health's folder-liveness signal fixes does NOT apply here — the themes_dir
    # ROOT probe above is section-count-independent (a /data mount fault fails
    # the root stat regardless of how many rows this scoped run examines), so a
    # small scoped canonical run is already covered. The absolute cap stays as
    # the secondary guard for a partial subtree fault under a live root.
    if len(missing_updates) + skipped > cap:
        log.warning(
            "verify_canonical_health: %d missing + %d skipped reads exceed cap "
            "%d (suspected mount fault) — stamping only the %d confirmed-"
            "present rows this run; skipping the missing-stamps",
            len(missing_updates), skipped, cap, len(present_updates))
        missing_updates = []
    updates = present_updates + missing_updates
    missing = len(missing_updates)
    if updates:
        with get_conn(db_path) as conn, transaction(conn):
            conn.executemany(
                "UPDATE local_files SET canonical_present = ?, "
                "    canonical_health_checked_at = ? "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                "  AND edition_key = ?",
                updates)
    return {"checked": len(updates), "missing": missing, "skipped": skipped}


def reconcile_placement_paths(db_path: Path, *,
                              section_ids: list[str] | None = None) -> int:
    """Find placements whose media_folder no longer matches the current
    plex_items.folder_path for that (media_type, tmdb_id), update the
    placement to the new path, and enqueue a forced place job so the
    canonical theme gets hardlinked into the new folder.

    Returns the number of placements relinked.

    Triggered at the end of every plex_enum run. The cost is one
    indexed JOIN against plex_items + an UPDATE per divergent row,
    so it scales with active placements (typically tens to low
    hundreds), not the full Plex catalog.

    v1.10.39: hardened against UNIQUE-constraint failures. The JOIN
    can produce multiple candidate new_folders per placement (Plex
    sometimes lists one movie under multiple ratingKeys, each with
    its own folder_path). Picking one new_folder via DISTINCT and
    detecting the case where the destination row already exists
    avoids the 'UNIQUE constraint failed' that pre-1.10.39 left
    plex_enum jobs in the failed state.
    """
    enqueued = 0
    # v0.51.101: scope the divergence-detect JOIN to the walked section(s) when
    # given (a single-section REFRESH / partial cron). The plex_paths_by_item
    # guard dict below stays GLOBAL — a superset only makes the "still Plex-
    # reported" skip MORE conservative, never wrongly relinking.
    _scope_sql, _scope_params = _section_scope_clause(section_ids, col="p.section_id")
    with get_conn(db_path) as conn:
        # DISTINCT collapses cases where the same (mt, tmdb, old, new)
        # tuple appears multiple times via different ratingKeys.
        # v1.12.81: include p.section_id in the SELECT and the
        # plex_items join so the relocate-detect is per-section.
        # Pre-fix the enqueue below dropped section_id and the
        # worker rejected the resulting place job with a v1.11.0
        # missing-section_id permanent failure, stuck-failing the
        # job and lighting up the topbar's red FAIL dot.
        # v1.18.49: exclude placement_kind='plex_upload' from the
        # folder-move reconcile. Plex-upload placements live entirely
        # in Plex's metadata bundle — their canonical media_folder
        # is the empty string `''` (the v1.18.0 sentinel) regardless
        # of what folder_path Plex reports for the corresponding
        # plex_items row. Without the filter, every enum loop saw
        # `p.media_folder = ''` vs `pi.folder_path = '/data/...'` as
        # a "move", queued a force place, the place job re-INSERTed
        # the row with media_folder='' (per the schema-v55 sentinel),
        # and the NEXT enum saw the same mismatch — infinite
        # re-upload + theme_added Discord spam every cycle. the user
        # reported 3 movies stuck in this loop with 4 cycles in ~2
        # minutes (16:24-16:26 logs). Same class-9 shape: a defensive
        # reconcile that silently amplified instead of fixing.
        # v1.21.94: join on edition_key too — a placement reconciles against
        # ITS OWN edition's plex_items folder. Pre-fix the edition-blind join
        # cross-products every edition: p(Theatrical) joined pi(Extended) and
        # looked "moved" (the old `old_folder in current_plex_paths` guard
        # masked it), and a metadata-only edition's un-tagged '' placement got
        # spuriously "moved" toward the {edition-X}-tagged folder. Genuine
        # same-edition moves (parent dir changed, tag unchanged) still match.
        rows = conn.execute(
            """SELECT DISTINCT p.media_type, p.tmdb_id, p.section_id,
                      p.edition_key,
                      p.media_folder AS old_folder,
                      pi.folder_path AS new_folder
               FROM placements p
               INNER JOIN plex_items pi
                 ON pi.guid_tmdb = p.tmdb_id
                AND pi.media_type = (CASE p.media_type WHEN 'tv' THEN 'show' ELSE 'movie' END)
                AND pi.section_id = p.section_id
                AND pi.edition_key = p.edition_key
               WHERE pi.folder_path IS NOT NULL
                 AND pi.folder_path != ''
                 AND p.media_folder != pi.folder_path
                 AND p.placement_kind != 'plex_upload'""" + _scope_sql,
            _scope_params,
        ).fetchall()
        # If a placement already covers one of the candidate new_folders
        # (multi-ratingKey case), don't move that placement — just drop
        # the stale row(s) pointing at folders Plex no longer reports.
        # Build a set of currently-Plex-reported folders per (mt, tmdb).
        plex_paths_by_item: dict[tuple, set[str]] = {}
        for pi in conn.execute(
            """SELECT pi.guid_tmdb AS tmdb_id, pi.media_type AS pi_mt,
                      pi.folder_path
               FROM plex_items pi
               WHERE pi.folder_path IS NOT NULL AND pi.folder_path != ''"""
        ).fetchall():
            mt = "tv" if pi["pi_mt"] == "show" else "movie"
            key = (mt, pi["tmdb_id"])
            plex_paths_by_item.setdefault(key, set()).add(pi["folder_path"])

        for r in rows:
            old_folder = r["old_folder"]
            new_folder = r["new_folder"]
            mt = r["media_type"]
            tmdb_id = r["tmdb_id"]
            section_id = r["section_id"]
            current_plex_paths = plex_paths_by_item.get((mt, tmdb_id), set())

            # Skip if the placement's current folder is still in Plex's
            # reported set — Plex just exposes another ratingKey at a
            # different folder, but the existing placement is still
            # valid.
            if old_folder in current_plex_paths:
                continue

            try:
                # v1.21.8 (audit MED): wrap the cancel + placement move
                # + force-place enqueue in ONE transaction. Pre-fix they
                # were 3 autocommit writes; if the INSERT raised AFTER
                # the placements UPDATE committed, the row's media_folder
                # was already moved, so the next enum's `media_folder !=
                # folder_path` no longer matched → the theme was NEVER
                # re-enqueued (silent placement drift; the per-row
                # except's "next plex_enum will retry" was wrong for that
                # ordering). Atomic now: all three commit or none do.
                with transaction(conn):
                    # Cancel any in-flight place to avoid racing the new one.
                    # v1.12.81: scope cancel + lookups + INSERT to the
                    # placement's section_id so a sibling section's place
                    # in flight isn't accidentally cancelled, and the
                    # follow-on enqueue doesn't fail v1.11.0's
                    # section_id requirement.
                    conn.execute(
                        """UPDATE jobs SET status = 'cancelled', finished_at = ?
                           WHERE job_type = 'place' AND media_type = ? AND tmdb_id = ?
                             AND section_id = ?
                             AND status IN ('pending','running')""",
                        (now_iso(), mt, tmdb_id, section_id),
                    )
                    # edition-blind OK (v1.21.94): keyed by media_folder (the
                    # relocated path uniquely identifies the placement row);
                    # the reconcile worklist above is already edition-scoped.
                    existing_at_new = conn.execute(
                        "SELECT 1 FROM placements "
                        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                        "  AND media_folder = ?",
                        (mt, tmdb_id, section_id, new_folder),
                    ).fetchone()
                    if existing_at_new:
                        # Destination row already exists — UPDATE would
                        # violate the composite UNIQUE. Drop the stale
                        # old_folder row instead; the destination row
                        # already covers it.
                        conn.execute(
                            """DELETE FROM placements
                               WHERE media_type = ? AND tmdb_id = ?
                                 AND section_id = ?
                                 AND media_folder = ?""",
                            (mt, tmdb_id, section_id, old_folder),
                        )
                    else:
                        conn.execute(
                            """UPDATE placements SET media_folder = ?
                               WHERE media_type = ? AND tmdb_id = ?
                                 AND section_id = ?
                                 AND media_folder = ?""",
                            (new_folder, mt, tmdb_id, section_id, old_folder),
                        )
                    # v1.21.94: carry edition_key so the force-place stages
                    # THIS edition's local_files into the relocated folder
                    # (the payload was edition-less → worker fell back to '').
                    import json as _json
                    _reloc_payload = _json.dumps({
                        "force": True, "reason": "folder_relocated",
                        "edition_key": r["edition_key"]})
                    conn.execute(
                        """INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                             payload, status, created_at, next_run_at)
                           VALUES ('place', ?, ?, ?, ?, 'pending', ?, ?)""",
                        (mt, tmdb_id, section_id, _reloc_payload,
                         now_iso(), now_iso()),
                    )
                log_event(db_path, level="INFO", component="plex_enum",
                          media_type=mt, tmdb_id=tmdb_id,
                          message="Plex folder moved; relinking theme",
                          detail={"old_folder": old_folder,
                                  "new_folder": new_folder,
                                  "kind": "delete-stale" if existing_at_new
                                          else "rename-in-place"})
                enqueued += 1
            except Exception as e:
                # One bad row shouldn't kill the whole reconcile pass.
                # Log + continue; the next plex_enum will retry.
                log.warning("reconcile_placement_paths: skipping row "
                            "(mt=%s tmdb=%s %s -> %s): %s",
                            mt, tmdb_id, old_folder, new_folder, e)
    return enqueued


# Tunable: how many items per write transaction. Smaller = more lock
# release windows for concurrent API writes; larger = fewer transaction
# round-trips. v0.51.103: 200 → 400 — the upsert writes are simple per-row
# INSERT/UPDATEs (sub-ms each), so a 400-row batch still holds BEGIN IMMEDIATE
# only briefly, but HALVES the number of inter-batch sleep yields on a large
# section (a 10K section: ~50 → ~25 batches → ~half the fixed sleep tax). The
# resolve pass's chunk_size stays 500 (its 7-UPDATE chunks hold the lock ~1-2s
# — deliberately not widened, see resolve_theme_ids' sleep note).
_UPSERT_BATCH = 400

# v1.22.65: Phase-1 sidecar-stat no-progress deadline — if NO folder
# stat completes within this window the mount is considered stalled and
# every remaining folder goes indeterminate (existing flags preserved)
# instead of wedging the enum forever. Module-level so tests can shrink
# it; 30s matches the v1.11.63 intent.
_SIDECAR_STALL_TIMEOUT_S = 30.0


# v1.11.61: host→container path-prefix translations to try when Plex
# returns a folder_path that doesn't exist inside motif's container.
# Plex on Unraid (and similar setups) reports the host's mount path,
# e.g. /mnt/user/data/media/anime/Show, while motif's container has
# /data/ bind-mounted to /mnt/user/data on the host. The same file
# is reachable under both paths from different vantage points; we
# try the original first, then each translated form, and use the
# first one that exists.
#
# Order matters — most-specific prefix first so /mnt/user/data/foo
# becomes /data/foo (preferred) rather than /user/data/foo.
# v1.14.57 audit clarification: the trailing `/mnt/disks/` → `/`
# rule below clobbers the container's root namespace for any Plex
# path mounted via UnRAID's individual-disk mount points (rare —
# most setups use the shared-fs `/mnt/user/` mount instead). For
# `/mnt/disks/foo/bar`, the translation produces `/foo/bar` which
# almost certainly doesn't exist inside the container; the
# `_candidate_local_paths` loop falls through to the next
# candidate and `stat_theme_sidecar` returns None gracefully. So
# this is correct in the failure-soft sense, just verbose-noisy
# in DEBUG. Kept for the rare individual-disk-mount setup; if a
# user reports unexpected behavior on `/mnt/disks/...` paths, the
# rule should be tightened to `/mnt/disks/<diskN>/data/` → `/data/`
# (mirroring the `/mnt/user/data/` → `/data/` shape) instead of
# the broad `/mnt/disks/` → `/` clobber.
#
# v1.15.100: env-var-driven user additions. The hardcoded defaults
# below cover stock Unraid layouts where /data is bind-mounted to
# /mnt/user/data. Users with non-standard layouts (e.g. /movies
# bind-mounted to /mnt/user/media/movies, or a TrueNAS setup with
# differing paths) can extend the table via MOTIF_PATH_TRANSLATIONS:
#
#   MOTIF_PATH_TRANSLATIONS="/mnt/user/movies=/data/media/movies,/mnt/user/tv=/data/media/tv"
#
# Each entry is `host_prefix=container_prefix`; multiple entries
# are comma-separated. Trailing slashes are normalized. User pairs
# are tried FIRST (more specific to the user's setup), then the
# hardcoded defaults as fallback. If MOTIF_PATH_TRANSLATIONS is
# unset / empty, behavior is identical to pre-v1.15.100 (defaults
# only). Closes the latent Phantom-M class for non-standard Unraid
# mount layouts — without this users had to either run the
# hardcoded translation patterns or accept that plex_enum's stat
# would return indeterminate (None) on every folder.

_HARDCODED_PATH_PREFIX_TRANSLATIONS: tuple[tuple[str, str], ...] = (
    ("/mnt/user/data/", "/data/"),
    ("/mnt/user/", "/"),
    ("/mnt/cache/data/", "/data/"),
    ("/mnt/cache/", "/"),
    ("/mnt/disks/", "/"),
)


def _parse_env_path_translations() -> tuple[tuple[str, str], ...]:
    """Parse MOTIF_PATH_TRANSLATIONS env var into normalized pairs.

    Format: "host1=container1,host2=container2".

    Normalization: each side gets a trailing slash (so prefix
    matching is unambiguous — `/mnt/user/foo/` vs `/mnt/user/foobar/`
    won't false-match) AND a leading slash (rejected if missing —
    relative paths don't make sense as mount prefixes).

    Malformed entries (no `=`, empty side, no leading `/`) are
    silently skipped. Returns an empty tuple when the env var is
    unset / empty / all entries malformed.
    """
    raw = os.environ.get("MOTIF_PATH_TRANSLATIONS", "").strip()
    if not raw:
        return ()
    pairs: list[tuple[str, str]] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if "=" not in entry:
            continue
        host, container = entry.split("=", 1)
        host = host.strip()
        container = container.strip()
        # Require absolute paths on both sides — relative prefixes
        # don't match Plex's reported absolute paths.
        if not host.startswith("/") or not container.startswith("/"):
            continue
        # Normalize trailing slash so prefix matching is unambiguous.
        if not host.endswith("/"):
            host += "/"
        if not container.endswith("/"):
            container += "/"
        pairs.append((host, container))
    return tuple(pairs)


# User-supplied pairs from env var are tried FIRST (most-specific
# to the user's setup), then hardcoded defaults as fallback.
_PATH_PREFIX_TRANSLATIONS: tuple[tuple[str, str], ...] = (
    *_parse_env_path_translations(),
    *_HARDCODED_PATH_PREFIX_TRANSLATIONS,
)


# v1.11.62: extension set + helpers shared with the unmanage / forget
# endpoints in api.py so a Plex-folder-perspective sidecar re-stat
# uses the same translation table as plex_enum.
SIDECAR_AUDIO_EXTS = {
    ".mp3", ".m4a", ".m4b", ".flac", ".ogg", ".oga", ".opus",
    ".wav", ".wma", ".aac", ".aif", ".aiff", ".alac",
}


def _candidate_local_paths(folder_path: str):
    """Yield Path candidates for a folder_path returned by Plex,
    starting with the literal value and falling through common
    host→container prefix translations. Caller iterates and uses
    the first one whose .is_dir() returns True."""
    if not folder_path:
        return
    yield Path(folder_path)
    for src, dst in _PATH_PREFIX_TRANSLATIONS:
        if folder_path.startswith(src):
            yield Path(dst + folder_path[len(src):])


def folder_has_theme_sidecar(folder_path: str) -> bool:
    """Public helper: True if any theme.<audio-ext> exists at
    folder_path (or any of its host→container translations).
    Mirrors the inline check used by _upsert_items' Phase 1.

    Convenience wrapper around stat_theme_sidecar() that maps the
    indeterminate (None) result to False — usable when the caller
    doesn't have a previous-known-good value to preserve.
    """
    out = stat_theme_sidecar(folder_path)
    return bool(out)


def find_theme_sidecar_path(folder_path: str) -> Path | None:
    """v1.18.43: like stat_theme_sidecar but returns the actual
    file path if a theme.<audio-ext> is found, None otherwise.

    Used by orphan_scan to surface orphan sidecars on
    plex_upload rows (theme.mp3 left over from pre-v1.18.36
    SWITCH file→api operations that didn't clean up) AND by
    the dashboard's DELETE SIDECAR action to know exactly
    which file to unlink. Walks the same host→container
    candidate paths as stat_theme_sidecar — first match wins.

    Returns None if folder_path is empty, no candidate path
    reaches a directory, or no theme.<audio-ext> file exists
    in any reachable candidate.
    """
    if not folder_path:
        return None
    for candidate in _candidate_local_paths(folder_path):
        try:
            if not candidate.is_dir():
                continue
            for entry in candidate.iterdir():
                try:
                    if not entry.is_file():
                        continue
                except OSError:
                    continue
                name = entry.name.lower()
                if not name.startswith("theme."):
                    continue
                if name[len("theme"):] in SIDECAR_AUDIO_EXTS:
                    return entry
        except OSError as e:
            # v1.18.48: silent-failure breadcrumb — pre-fix this returned None
            # on every unreachable candidate, indistinguishable from "scanned
            # the dir and found no sidecar." Orphan dashboard then rendered
            # "no orphan" even when the path was just NFS-stale or unmounted.
            global _FIND_THEME_SIDECAR_OSERROR_WARNED
            if not _FIND_THEME_SIDECAR_OSERROR_WARNED:
                log.warning(
                    "find_theme_sidecar_path: OSError scanning %s (%s) — "
                    "candidate skipped. Further occurrences will log at debug.",
                    candidate, e,
                )
                _FIND_THEME_SIDECAR_OSERROR_WARNED = True
            else:
                log.debug(
                    "find_theme_sidecar_path: OSError scanning %s (%s)",
                    candidate, e,
                )
            continue
    return None


def sweep_stale_placement_temps(db_path: Path, *,
                                section_ids: list[str] | None = None,
                                older_than_secs: int = 3600) -> int:
    """v0.51.104: delete orphaned `theme.mp3.motif-tmp` files — the atomic
    staging temp `_safe_link_or_copy` writes and then os.replace()s to
    theme.mp3. A crash / container restart / FS hiccup in that split-second
    window leaves it behind. Normally the NEXT placement to that folder cleans
    it (placement.py:206), but a folder that never gets re-placed — an edition
    drift, a switch to plex_upload — keeps the orphan indefinitely (the user's
    LotR {edition-theatrical} folder). A tmp older than older_than_secs
    (default 1h) can't be a placement in flight, so it's cruft.

    Walks distinct media folders (plex_items.folder_path), translating each
    host→container path via _candidate_local_paths (same table the reaper +
    health passes use), and unlinks any stale theme.mp3.motif-tmp. Best-effort:
    an unreachable folder / an undeletable temp is skipped + logged. Returns the
    count removed. Scoped by section_ids when given (default None = all)."""
    _scope_sql, _scope_params = _section_scope_clause(section_ids)
    try:
        with get_conn(db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT folder_path FROM plex_items "
                "WHERE folder_path IS NOT NULL AND folder_path != ''"
                + _scope_sql, _scope_params).fetchall()
    except Exception as e:  # noqa: BLE001 — defensive
        log.warning("sweep_stale_placement_temps: folder query failed: %s", e)
        return 0
    folders = [r["folder_path"] for r in rows]
    if not folders:
        return 0
    import time as _time
    cutoff = _time.time() - max(0, older_than_secs)

    def _sweep_one(folder_path: str) -> int:
        # First reachable candidate dir wins (mirror find_theme_sidecar_path).
        for candidate in _candidate_local_paths(folder_path):
            tmp = candidate / "theme.mp3.motif-tmp"
            try:
                st = tmp.stat()
            except OSError:
                continue  # not present in this candidate — try the next
            if st.st_mtime > cutoff:
                return 0  # too fresh — a placement may be mid-flight
            try:
                tmp.unlink()
                log.info("sweep_stale_placement_temps: removed stale %s", tmp)
                return 1
            except OSError as e:
                log.warning(
                    "sweep_stale_placement_temps: could not remove %s: %s",
                    tmp, e)
                return 0
        return 0

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=16) as ex:
        total = sum(ex.map(_sweep_one, folders))
    if total:
        log.info("sweep_stale_placement_temps: removed %d stale .motif-tmp "
                 "file(s) across %d media folder(s)", total, len(folders))
    return total


def stat_theme_sidecar(folder_path: str) -> bool | None:
    """v1.11.67: returns True if a theme.<audio-ext> exists, False
    if we successfully scanned the folder and confirmed no such
    file, or None if EVERY candidate path raised OSError on either
    is_dir() or iterdir() — i.e. we couldn't determine the answer.

    Pre-fix, transient Unraid user-share / NFS hiccups produced a
    silent False return that plex_enum committed to pi.local_theme_file=0,
    which combined with Plex's (correctly cached) has_theme=1 made
    the row render as P (Plex agent) when the file was really there.
    Returning None lets callers preserve the previous-known value
    instead of overwriting truth with a temporary fault.

    v1.21.42 (audit M2): an EMPTY folder_path returns None, not False.
    A blank path means "we don't know where to look" (same class as an
    OSError on every candidate) — NOT "scanned and confirmed no sidecar".
    Pre-fix it returned False, so when a transient get_item_paths_bulk
    failure left a TV show's folder_path='' for one enum, Phase 1's
    `1 if res else 0` wrote local_theme_file=0 and an M-row silently lost
    its sidecar flag (SRC flipped M→P) until a clean enum repopulated the
    path. None routes it through the existing indeterminate-preservation.
    Matches find_theme_sidecar_path, which already returns None for ''.
    """
    if not folder_path:
        return None
    any_reachable = False
    for candidate in _candidate_local_paths(folder_path):
        try:
            if not candidate.is_dir():
                continue
            any_reachable = True
            for entry in candidate.iterdir():
                try:
                    if not entry.is_file():
                        continue
                except OSError:
                    continue
                name = entry.name.lower()
                if not name.startswith("theme."):
                    continue
                if name[len("theme"):] in SIDECAR_AUDIO_EXTS:
                    return True
            return False
        except OSError as e:
            log.debug("stat_theme_sidecar: iterdir(%s) failed: %s",
                      candidate, e)
            continue
    if not any_reachable:
        log.warning("stat_theme_sidecar: no candidate path reachable for "
                    "%r — returning indeterminate (None)", folder_path)
        return None
    # We reached at least one candidate but it had no theme file
    return False


def _upsert_items(db_path: Path, items: list[PlexLibraryItem],
                   *, cancel_check=lambda: False,
                   section_id: str | None = None,
                   progress_op_id: str | None = None,
                   progress_total: int | None = None,
                   progress_base: int = 0,
                   progress_phase1_label: str | None = None,
                   progress_phase2_label: str | None = None) -> tuple[int, int]:
    """Upsert one section's items into plex_items. Returns
    (inserted_count, updated_count).

    Two performance properties matter on large libraries (10K+ items):
      - Sidecar stat() (folder_path/theme.mp3 existence) happens BEFORE
        the DB transaction opens — filesystem I/O is sequential but
        doesn't hold a write lock.
      - The DB writes are batched in transactions of `_UPSERT_BATCH`
        rows. Each batch ends with COMMIT, releasing the BEGIN IMMEDIATE
        lock so concurrent API writers (log_event in particular) don't
        stall for the entire enum duration.
    Pre-1.9.6 this was a single 10K-row transaction that soft-locked
    the API for 10+ seconds per enum.
    """
    # Phase 1: stat sidecars outside the transaction. List of (item, sidecar)
    # pairs. v1.11.62: parallelized + the per-folder check now goes
    # through folder_has_theme_sidecar (host→container path translation
    # + broad extension match). Same helper is reused by api.py's
    # unmanage/forget endpoints so the post-mutation re-stat sees the
    # same files plex_enum does.
    # v1.11.63: stall protection + progress logging. Pre-fix one hung
    # folder (dead NFS mount, stalled SMB read, anything where
    # iterdir() never returns) blocked the whole ex.map indefinitely
    # — the user's plex_enum job sat 'running' forever with no further
    # log output. v1.22.65: the v1.11.63 as_completed + result(timeout)
    # shape never actually fired (see the Phase-1 comment below); the
    # REAL protection is now a wait()-based no-progress deadline
    # (_SIDECAR_STALL_TIMEOUT_S of zero completions → remaining folders
    # marked indeterminate, enum continues). Periodic progress log
    # every 200 items so a long enum visibly advances.
    import time as _t
    enriched: list[tuple[PlexLibraryItem, int]] = []
    if items:
        # v1.22.65: wait() replaces the as_completed + per-result timeout
        # shape (which could never fire — see the Phase-1 comment below).
        from concurrent.futures import ThreadPoolExecutor, wait as _fut_wait
        # v1.11.67: pre-fetch existing pi.local_theme_file for every
        # rating_key we're about to enum, keyed by rk. When a per-item
        # stat comes back indeterminate (transient Unraid share hiccup,
        # NFS timeout, etc.) we preserve the previous value rather
        # than stomping a known-good 1 with a transient 0. Pre-fix
        # one bad iterdir() drove pi.local_theme_file=0 even though
        # Plex (correctly) reported has_theme=1, leaving the row
        # rendering as P (Plex agent) when the user's local theme.mp3
        # was clearly there.
        existing_local: dict[str, int] = {}
        if items:
            with get_conn(db_path) as conn:
                rks = [it.rating_key for it in items]
                # SQLite IN-clause limit ~999 — chunk just in case.
                for i in range(0, len(rks), 500):
                    chunk = rks[i:i + 500]
                    qmarks = ",".join("?" for _ in chunk)
                    for r in conn.execute(
                        f"SELECT rating_key, local_theme_file "
                        f"FROM plex_items WHERE rating_key IN ({qmarks})",
                        chunk,
                    ).fetchall():
                        existing_local[r["rating_key"]] = int(r["local_theme_file"] or 0)

        # sidecar_results values: 1 / 0 / None (indeterminate — use existing)
        sidecar_results: dict[int, int | None] = {}
        log.info("plex_enum: Phase 1 (sidecar stat) on %d items", len(items))
        phase1_start = _t.monotonic()
        # v1.12.128: emit op_progress at the start of Phase 1 so the
        # bar tick'd by the fetch stage carries forward into the
        # sidecar-stat phase. The label clarifies which phase is
        # running so the user can read the bar honestly.
        if progress_op_id is not None:
            op_progress.update_progress(
                db_path, progress_op_id,
                stage_label=(progress_phase1_label
                             or "Sidecar stat"),
                stage_current=0,
                stage_total=len(items),
            )
        last_phase1_emit = _t.monotonic()
        # v1.22.65 (audit round 2): the v1.11.63 "30s timeout per folder"
        # was DEAD CODE — as_completed(timeout=None) blocks forever
        # waiting for the next future, and by the time it yields one the
        # future is already done, so fut.result(timeout=30) returned
        # instantly and the except _FutTimeout was unreachable. One
        # folder hung on a stalled NFS/SMB mount wedged the whole enum
        # indefinitely — the exact incident v1.11.63 claims to fix. Now:
        # a wait()-based NO-PROGRESS deadline — 30s with ZERO completions
        # means the mount is stalled; mark every remaining folder
        # indeterminate (preserving existing flags) and continue the
        # enum. On the stall path the pool is abandoned without joining
        # (the hung stat threads can't be killed; joining would re-wedge)
        # — leaked once, logged loudly.
        ex = ThreadPoolExecutor(max_workers=16,
                                thread_name_prefix="motif-sidecar")
        futures = {
            ex.submit(stat_theme_sidecar, it.folder_path): idx
            for idx, it in enumerate(items)
        }
        pending = set(futures)
        done = 0
        stalled = False
        try:
            while pending:
                finished, pending = _fut_wait(
                    pending, timeout=_SIDECAR_STALL_TIMEOUT_S)
                if not finished:
                    stalled = True
                    for fut in pending:
                        sidecar_results[futures[fut]] = None
                    log.warning(
                        "plex_enum: sidecar stat made NO progress for 30s "
                        "— marking %d remaining folder(s) indeterminate "
                        "(stalled mount?) and continuing the enum",
                        len(pending),
                    )
                    break
                for fut in finished:
                    idx = futures[fut]
                    try:
                        res = fut.result()
                        if res is None:
                            sidecar_results[idx] = None  # indeterminate
                        else:
                            sidecar_results[idx] = 1 if res else 0
                    except Exception as e:
                        log.warning(
                            "plex_enum: sidecar stat error for %r: %s — "
                            "preserving existing flag",
                            items[idx].folder_path, e,
                        )
                        sidecar_results[idx] = None
                    done += 1
                    if done % 200 == 0:
                        log.info(
                            "plex_enum: Phase 1 progress %d/%d (%.1fs)",
                            done, len(items),
                            _t.monotonic() - phase1_start)
                    # v1.12.128: time-throttled op_progress emit so the
                    # bar tick'd in the fetch stage keeps moving through
                    # Phase 1 (typically ~1.5s on a 10k-item section, so
                    # 5-6 emits at 300ms is plenty).
                    if (progress_op_id is not None
                            and (_t.monotonic() - last_phase1_emit) > 0.3):
                        last_phase1_emit = _t.monotonic()
                        op_progress.update_progress(
                            db_path, progress_op_id,
                            stage_current=done,
                            stage_total=len(items),
                        )
        finally:
            # Clean path joins normally; stall path abandons the hung
            # threads (cancel whatever never started).
            ex.shutdown(wait=not stalled, cancel_futures=stalled)
        log.info("plex_enum: Phase 1 done in %.1fs",
                 _t.monotonic() - phase1_start)
        # Resolve indeterminate (None) results against the previously-
        # stored value so a transient stat fault doesn't downgrade a
        # known-good local_theme_file=1 to 0.
        for idx, it in enumerate(items):
            res = sidecar_results.get(idx)
            if res is None:
                # Keep whatever was last in the DB (defaults to 0 on
                # first enum). For a brand-new row we don't have one;
                # 0 is the safe default.
                enriched.append((it, existing_local.get(it.rating_key, 0)))
            else:
                enriched.append((it, res))

    # Phase 2: batched upserts.
    inserted = 0
    updated = 0
    # v1.21.5: rks inserted THIS enum — the post-resolve theme-available
    # sweep uses them as the inherent "new item in Plex" gate.
    new_item_rks: list[str] = []
    now = now_iso()
    import time as _t
    # v1.12.124: tick op_progress per batch + on a ~300ms timer so the
    # bar reflects forward progress through the upsert phase. Pre-fix
    # the caller set stage_current=0 before _upsert_items and
    # stage_current=len(items) after; the bar sat at 0 for the whole
    # upsert (5-15s on a 4K-item section) then jumped to 100%.
    last_ui = _t.monotonic()
    for batch_start in range(0, len(enriched), _UPSERT_BATCH):
        # v1.11.36: cooperative cancellation between upsert batches.
        if cancel_check():
            from .worker import _JobCancelled
            raise _JobCancelled()
        # v1.11.54: yield between batches so the writer lock has gaps for
        # concurrent claim/log_event/event-flusher writes. Pre-fix the
        # back-to-back transactions held the writer at near-100% duty cycle
        # through a 10K-row enum. v0.51.103: 150ms → 100ms — the v0.51.103
        # _UPSERT_BATCH 200→400 already halves the number of these yields, so a
        # shorter gap still leaves ample lock headroom (a 400-row batch is a
        # sub-second hold) while trimming the fixed sleep tax further.
        if batch_start > 0:
            _t.sleep(0.10)
        batch = enriched[batch_start:batch_start + _UPSERT_BATCH]
        # Emit progress at the START of each batch — ticks the bar
        # as we work through the section's items rather than at the
        # post-section update point only.
        if progress_op_id and progress_total is not None and (
                batch_start == 0
                or (_t.monotonic() - last_ui) > 0.3):
            last_ui = _t.monotonic()
            # v1.12.128: distinct label for Phase 2 so the bar text
            # changes from "(fetch)" → "Sidecar stat" → "Upsert"
            # as the user watches.
            update_kwargs = {
                "stage_current": batch_start,
                "stage_total": progress_total,
                "processed_total": progress_base + batch_start,
            }
            if progress_phase2_label is not None and batch_start == 0:
                update_kwargs["stage_label"] = progress_phase2_label
            op_progress.update_progress(
                db_path, progress_op_id, **update_kwargs,
            )
        with get_conn(db_path) as conn, transaction(conn):
            for it, sidecar in batch:
                # v1.10.32: precompute the normalized title so the library
                # JOIN's title-fallback can match against themes.title_norm
                # without re-running normalize_title at query time.
                # v1.19.80: class-9 breadcrumb (twin of sync.py:483,
                # which got the flag in v1.17.11). Warn once per
                # process, debug subsequently — hot path can't log
                # per-row.
                try:
                    from .normalize import normalize_title
                    tn = normalize_title(it.title or "")
                except Exception as e:  # noqa: BLE001
                    global _PLEX_ENUM_NORMALIZE_TITLE_WARNED
                    if not _PLEX_ENUM_NORMALIZE_TITLE_WARNED:
                        log.warning(
                            "plex_enum upsert: normalize_title failed "
                            "(%s: %s) — falling back to lowercase. Will "
                            "not log subsequent occurrences in this "
                            "process; check the deploy.",
                            type(e).__name__, e,
                        )
                        _PLEX_ENUM_NORMALIZE_TITLE_WARNED = True
                    else:
                        log.debug("normalize_title failed: %s", e)
                    tn = (it.title or "").lower()
                # v1.13.38: observe the +P composite signal. When
                # there's no sidecar at the folder yet (sidecar=0),
                # has_theme tells us definitively whether Plex has
                # an independent theme: 1=cloud/embed/Pass, 0=nothing.
                # When sidecar=1 the value is unobservable (Plex
                # always reports has_theme=1 because it sees the
                # sidecar) so we leave the existing column untouched
                # — past observations stick. Pass NULL to the
                # COALESCE so the SQL keeps the prior value.
                # v1.18.22: guard against a false +P stamp for
                # plex_upload placements (collections today,
                # opt-in movies/TV in a future tag). When motif
                # uploaded the audio to Plex's metadata via API,
                # Plex correctly reports has_theme=1 but the
                # theme isn't independent — motif IS the source.
                # The discriminator above ("no sidecar + has_theme
                # = independent") misfires here because there's
                # no sidecar location for collections at all.
                # Lookup placements: if a plex_upload placement
                # exists for this (media_type, tmdb_id, section_id),
                # the +P observation is invalid → set to 0 (Plex
                # is NOT serving its own theme; motif's upload IS
                # the theme).
                # v1.23.65 (audit #13): edition-SCOPED. The v1.21.94 "edition-
                # blind OK, plex_upload is collection-only" note went STALE —
                # worker.py:3583 (`_placed_kind = fell_back_kind or 'plex_upload'`)
                # writes plex_upload placements for movie/TV rows on the API-upload
                # path too. Without `p.edition_key = pi.edition_key`, enumerating a
                # sibling edition (rk2) that Plex independently serves (has_theme=1,
                # no sidecar) matched ANOTHER edition's (rk1) plex_upload placement
                # in the same section → wrongly cleared its +P observation. Scoped
                # to the enumerated row's own edition.
                has_plex_upload = bool(conn.execute(
                    "SELECT 1 FROM plex_items pi "
                    "INNER JOIN themes t ON t.id = pi.theme_id "
                    "INNER JOIN placements p "
                    "  ON p.media_type = t.media_type "
                    " AND p.tmdb_id = t.tmdb_id "
                    " AND p.section_id = pi.section_id "
                    " AND p.edition_key = pi.edition_key "
                    "WHERE pi.rating_key = ? "
                    "  AND p.placement_kind = 'plex_upload' "
                    "LIMIT 1",
                    (it.rating_key,),
                ).fetchone())
                if has_plex_upload:
                    indep_observation = 0
                elif sidecar == 1:
                    indep_observation = None
                elif it.has_theme:
                    indep_observation = 1
                else:
                    indep_observation = 0
                # v1.18.79: also fetch the prior has_theme value so
                # we can detect a 1→0 transition (Plex used to serve
                # its own theme, no longer does). On a backup-intent
                # row this transition means "user's backup is now
                # ready to deploy". v1.18.80 shipped the
                # backup_ready_to_deploy dispatch off this transition
                # (see ~line 1430); v1.18.90 + v1.19.41 added the
                # reaper-side four-way theme-lost split. (Pre-v1.18.80
                # this only logged a WARNING breadcrumb — the comment
                # said "future dispatch", which went stale once the
                # dispatch actually landed.)
                existing = conn.execute(
                    # v1.19.41: theme_id added to the SELECT so the
                    # v1.18.79 detection's Bug-A fallback (TVDB-bridge gap
                    # — resolve tmdb_id via theme_id linkage when
                    # guid_tmdb IS NULL) has access to it. Adding
                    # one column to a per-row SELECT is cheap; the
                    # alternative (separate roundtrip) wasn't.
                    "SELECT plex_theme_uri, has_theme, guid_tmdb, "
                    "       media_type, theme_id FROM plex_items "
                    "WHERE rating_key = ?",
                    (it.rating_key,),
                ).fetchone()
                # v1.21.56 (B3): edition_key is folder-derived (one
                # chokepoint), kept current on plex_items so Phase C's
                # library JOIN matches per edition without a fragile SQL
                # mirror. '' for a standard/untagged folder = today.
                ek = edition_key_for_folder(it.folder_path)
                if existing:
                    # v1.12.112: detect theme-URI change. Plex's URL
                    # has a trailing version suffix that bumps when
                    # the underlying theme content changes (or when
                    # the metadata refresh produces a new URL). If
                    # the URI changed, prior verification is invalid
                    # — reset verified_* to NULL so the post-upsert
                    # verification pass re-tests the new claim.
                    new_uri = it.plex_theme_uri or None
                    old_uri = existing["plex_theme_uri"] or None
                    if new_uri != old_uri:
                        # v1.13.38: COALESCE on plex_independent_theme keeps
                        # the prior value when the observation is unavailable
                        # (sidecar=1), updates when observable.
                        conn.execute(
                            """UPDATE plex_items SET
                                  section_id = ?, media_type = ?, title = ?, year = ?,
                                  guid_imdb = ?, guid_tmdb = ?, guid_tvdb = ?,
                                  folder_path = ?, edition_key = ?,
                                  plex_edition_title = ?,
                                  has_theme = ?, local_theme_file = ?,
                                  title_norm = ?, last_seen_at = ?,
                                  plex_theme_uri = ?,
                                  plex_theme_verified_at = NULL,
                                  plex_theme_verified_ok = NULL,
                                  plex_independent_theme = COALESCE(?, plex_independent_theme)
                               WHERE rating_key = ?""",
                            (it.section_id, it.media_type, it.title, it.year,
                             it.guid_imdb, it.guid_tmdb, it.guid_tvdb,
                             it.folder_path, ek, it.plex_edition_title or '',
                             1 if it.has_theme else 0, sidecar,
                             tn, now, new_uri, indep_observation, it.rating_key),
                        )
                    else:
                        # URI unchanged — keep prior verification.
                        conn.execute(
                            """UPDATE plex_items SET
                                  section_id = ?, media_type = ?, title = ?, year = ?,
                                  guid_imdb = ?, guid_tmdb = ?, guid_tvdb = ?,
                                  folder_path = ?, edition_key = ?,
                                  plex_edition_title = ?,
                                  has_theme = ?, local_theme_file = ?,
                                  title_norm = ?, last_seen_at = ?,
                                  plex_independent_theme = COALESCE(?, plex_independent_theme)
                               WHERE rating_key = ?""",
                            (it.section_id, it.media_type, it.title, it.year,
                             it.guid_imdb, it.guid_tmdb, it.guid_tvdb,
                             it.folder_path, ek, it.plex_edition_title or '',
                             1 if it.has_theme else 0, sidecar,
                             tn, now, indep_observation, it.rating_key),
                        )
                    updated += 1
                    # v1.18.79: detect plex_has_theme 1→0 transition
                    # on a row whose user_override carries
                    # intent='backup'. The transition means Plex
                    # stopped serving its own theme — motif's
                    # backup file is now the only candidate. Log
                    # a WARNING breadcrumb so the operator can
                    # `docker logs motif | grep backup_ready` and
                    # see which rows are eligible to deploy.
                    # v1.18.80 will add the Apprise dispatch on
                    # this same transition; v1.18.79 is the
                    # breadcrumb-only foundation so the detection
                    # logic can be verified in production before
                    # we add user-facing notifications.
                    _prior_has_theme = (
                        bool(existing["has_theme"])
                        if existing["has_theme"] is not None
                        else False
                    )
                    if _prior_has_theme and not it.has_theme:
                        _row_mt = (
                            "tv" if existing["media_type"] == "show"
                            else (
                                "collection"
                                if existing["media_type"] == "collection"
                                else existing["media_type"]
                            )
                        )
                        # v1.19.41 Bug A fix: widen guid_tmdb gate
                        # to include theme_id linkage. Pre-fix,
                        # `_row_tmdb = existing["guid_tmdb"]` then
                        # `if _row_tmdb is not None:` silently
                        # skipped anime-agent-matched rows (anime where
                        # Plex matched via TVDB/AniDB so guid_tmdb
                        # is NULL but theme_id is set via v1.15.142
                        # resolve_theme_ids fallback). the user's
                        # data showed Anime is 100% C1 in the
                        # cloud-backup probe — the cohort most
                        # at-risk for theme loss is the cohort the
                        # detection silently missed.
                        _row_tmdb = existing["guid_tmdb"]
                        if _row_tmdb is None and existing[
                            "theme_id"
                        ] is not None:
                            _resolved = conn.execute(
                                "SELECT tmdb_id FROM themes "
                                "WHERE id = ?",
                                (existing["theme_id"],),
                            ).fetchone()
                            if _resolved is not None:
                                _row_tmdb = _resolved["tmdb_id"]
                        # v1.19.41 Bug C: transition INFO breadcrumb
                        # BEFORE any gate so operators have signal
                        # for ALL has_theme: 1→0 transitions, not
                        # just the actionable backup-intent subset.
                        # Lets `docker logs motif | grep theme_transition`
                        # surface every Plex-stops-serving event so
                        # "did Plex Pass cause a loss today?" is
                        # answerable from logs alone. Cold-path-
                        # needs-MORE-logging sub-pattern (v1.18.5/
                        # v1.18.7) — detection code that rarely
                        # fires needs MORE explicit logging than
                        # the happy path, not less.
                        log.info(
                            "theme_transition: %s/%s (rk=%s, "
                            "title=%r) has_theme 1→0 detected. "
                            "Checking for backup-intent override...",
                            _row_mt, _row_tmdb,
                            it.rating_key, it.title,
                        )
                        if _row_tmdb is not None:
                            # edition-blind OK (v1.21.94): notify-only backup-
                            # ready detection. Changes no placement/download;
                            # the dispatched notification names the row's
                            # edition via edition_key_for_folder(it.folder_path)
                            # below. Section-prioritized via ORDER BY.
                            _backup_ovr = conn.execute(
                                "SELECT intent FROM user_overrides "
                                "WHERE media_type = ? AND tmdb_id = ? "
                                "  AND intent = 'backup' "
                                "ORDER BY (section_id = ?) DESC "
                                "LIMIT 1",
                                (_row_mt, _row_tmdb, it.section_id),
                            ).fetchone()
                            _cloud_backup = None
                            if _backup_ovr is None:
                                # v0.51.14 (audit #6): walker-staged cloud/TDB
                                # backups (// BACKUP CLOUD THEMES) write ONLY
                                # local_files (source_kind='plex_cloud' /
                                # reason='backup_only') — no user_overrides row
                                # — so the intent gate above missed the exact
                                # Plex-Pass-lapse mode the v1.19.42 pipe was
                                # built for: item still in Plex, has_theme 1→0,
                                # the staged backup sitting silently unused.
                                # The reaper's tier-1 classifier DOES match
                                # these rows, but it only runs when Plex
                                # DELETES the item. Fire the same notification
                                # here for the in-place transition.
                                # edition-blind OK (v1.21.94): notify-only
                                # backup-ready detection, mirrors the override
                                # query above.
                                _cloud_backup = conn.execute(
                                    "SELECT 1 FROM local_files "
                                    "WHERE media_type = ? AND tmdb_id = ? "
                                    "  AND (source_kind = 'plex_cloud' OR "
                                    "  last_place_attempt_reason = 'backup_only')"
                                    " ORDER BY (section_id = ?) DESC LIMIT 1",
                                    (_row_mt, _row_tmdb, it.section_id),
                                ).fetchone()
                            if (_backup_ovr is not None
                                    or _cloud_backup is not None):
                                log.warning(
                                    "backup_ready_to_deploy: "
                                    "Plex stopped serving theme "
                                    "for %s/%s (rk=%s, title=%r) "
                                    "— user_override intent='backup' "
                                    "is staged. Operator can click "
                                    "// PROMOTE TO ACTIVE on the "
                                    "row's INFO card to deploy.",
                                    _row_mt, _row_tmdb,
                                    it.rating_key, it.title,
                                )
                                # v1.18.80: Apprise dispatch on the
                                # same transition. Per the user's
                                # v1.18.77 q4 choice: "notify only —
                                # wait for user decision." Motif
                                # logs the breadcrumb (v1.18.79) AND
                                # dispatches a user-facing notification
                                # so the operator doesn't have to
                                # grep docker logs to see backup-ready
                                # transitions. The /api/items/.../intent
                                # endpoint already exists for the
                                # // PROMOTE TO ACTIVE button — this
                                # just surfaces the call-to-action.
                                #
                                # Best-effort (class-9 safe): notify
                                # dispatch wrapped in try/except so
                                # a notify-side failure doesn't
                                # interrupt the plex_enum loop.
                                try:
                                    # v1.18.80: load Settings() inside
                                    # the dispatch block — same pattern
                                    # the v1.16.0 tvdb_bridge uses at
                                    # line ~1703. plex_enum's signature
                                    # doesn't carry a settings object;
                                    # this is the established way.
                                    from ..config import Settings
                                    from . import notify as _notify
                                    from . import notify_content as _nc
                                    _settings = Settings()
                                    # edition-blind OK (v1.21.94): notify-only.
                                    _ovr_url_row = conn.execute(
                                        "SELECT youtube_url FROM user_overrides "
                                        "WHERE media_type = ? "
                                        "  AND tmdb_id = ? "
                                        "  AND intent = 'backup' "
                                        "ORDER BY (section_id = ?) DESC "
                                        "LIMIT 1",
                                        (_row_mt, _row_tmdb,
                                         it.section_id),
                                    ).fetchone()
                                    _ovr_url = (
                                        _ovr_url_row["youtube_url"]
                                        if _ovr_url_row else None
                                    )
                                    _ctx = _nc.enrich_item(
                                        db_path,
                                        media_type=_row_mt,
                                        tmdb_id=_row_tmdb,
                                        section_id=it.section_id,
                                        # v1.21.76: name the edition.
                                        edition_key=edition_key_for_folder(
                                            it.folder_path),
                                    )
                                    _notify.dispatch(
                                        db_path,
                                        _settings.cfg.notifications,
                                        event_kind="backup_ready_to_deploy",
                                        title=_nc.format_backup_ready_title(_ctx),
                                        body=_nc.format_backup_ready_body(
                                            _ctx, override_url=_ovr_url),
                                        body_format="markdown",
                                        # v1.23.75: FB-sourced backups attach
                                        # their thumb (v1.22.94) — was worker-
                                        # only; a fbcdn backup showed no image.
                                        attach_url=_nc.attachment_thumb_url(
                                            _ctx),
                                    )
                                except Exception as _e:  # noqa: BLE001
                                    # v1.19.41: hot-path silent-fail
                                    # downgrade per CLAUDE.md class-9
                                    # sub-pattern. First occurrence
                                    # per process gets log.warning so
                                    # an operator with a genuinely-
                                    # broken Apprise config sees ONE
                                    # warning at boot; subsequent
                                    # fall through to log.debug. Was
                                    # log.debug unconditionally —
                                    # invisible at INFO log level.
                                    global _BACKUP_READY_NOTIFY_WARNED
                                    if not _BACKUP_READY_NOTIFY_WARNED:
                                        log.warning(
                                            "v1.19.41: "
                                            "backup_ready_to_deploy "
                                            "notify dispatch failed "
                                            "for %s/%s: %s. "
                                            "Subsequent failures "
                                            "downgrade to debug; "
                                            "investigate Apprise "
                                            "config if this is a "
                                            "fresh deploy.",
                                            _row_mt, _row_tmdb, _e,
                                        )
                                        _BACKUP_READY_NOTIFY_WARNED = True
                                    else:
                                        # Single-line message so the
                                        # v1.18.80 test's substring
                                        # search ("notify dispatch
                                        # suppressed") finds it
                                        # contiguous in source.
                                        log.debug(
                                            "backup_ready_to_deploy notify dispatch suppressed for %s/%s: %s",
                                            _row_mt, _row_tmdb, _e,
                                        )
                else:
                    conn.execute(
                        """INSERT INTO plex_items
                           (rating_key, section_id, media_type, title, year,
                            guid_imdb, guid_tmdb, guid_tvdb, folder_path,
                            edition_key, plex_edition_title,
                            has_theme, local_theme_file, title_norm,
                            plex_theme_uri, plex_independent_theme,
                            first_seen_at, last_seen_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (it.rating_key, it.section_id, it.media_type, it.title,
                         it.year, it.guid_imdb, it.guid_tmdb, it.guid_tvdb,
                         it.folder_path, ek, it.plex_edition_title or '',
                         1 if it.has_theme else 0, sidecar,
                         tn, it.plex_theme_uri or None, indep_observation,
                         now, now),
                    )
                    inserted += 1
                    # v1.21.5: this rk is genuinely new (existing was
                    # None) — collect for the theme-available sweep.
                    new_item_rks.append(it.rating_key)
    # v1.18.89: reap stale plex_items rows. the user's Troy repro:
    # removed the movie from Plex → re-added it → motif still
    # showed SRC=P because the OLD rk's plex_items row stuck
    # around with has_theme=1 + plex_independent_theme=1. plex_
    # enum was add-and-update only; never deleted rows for rks
    # Plex no longer returned. The library row query picked the
    # stale row and rendered phantom-P.
    #
    # Reaper strategy: compare the set of rks Plex returned this
    # enum against existing plex_items.rating_key for the same
    # section. The set difference is the stale rks → DELETE.
    # Deterministic (no time-based heuristics — relies on Plex's
    # current enumeration as the truth).
    #
    # Safety guards (class-9 amplifier-sweep lesson, v1.18.10):
    #   1. Skip if Plex returned 0 items (API hiccup → don't
    #      mass-delete based on empty response)
    #   2. Skip if would-delete > 20% of section AND > 50 rows
    #      (Plex returned a suspiciously-smaller catalog → could
    #      be Plex's own enumeration bug or auth glitch; refuse
    #      to amplify the damage)
    # Sample log of first 5 deleted rows so the operator has a
    # breadcrumb if something unexpected disappears.
    # v1.18.90: collect "theme lost Plex P-claim" candidates from
    # the reaper for post-transaction notification dispatch. A
    # theme is "lost" when:
    #   (a) the reaper deletes a plex_items row with has_theme=1
    #       AND theme_id was set
    #   (b) AND no surviving plex_items row for the same theme has
    #       has_theme=1
    #   (c) AND motif has no fallback (no local_files / placements
    #       / user_overrides for the (mt, tmdb))
    # Dispatch happens AFTER the transaction commits so Apprise
    # HTTP calls can't roll back the reap.
    lost_theme_candidates: list[dict] = []
    if section_id and items:
        # v1.18.91 (Bug A fix): scope the reaper to media_type.
        # Pre-fix, `_upsert_items` runs twice per section — once
        # for items (`show`/`movie`) + once for `collection`s —
        # but the reaper compared `seen_rks` (current call only)
        # against ALL plex_items in the section (both media_types).
        # The collection enum saw 4168 "stale" rks that were
        # actually just the show rows from the prior items enum →
        # the 20% amplifier-sweep guard tripped at 96% on every
        # collection enum. Now: scope all queries to the
        # media_type of THIS call's items.
        items_media_type = items[0].media_type
        seen_rks = set(it.rating_key for it in items if it.rating_key)
        if seen_rks:
            with get_conn(db_path) as conn, transaction(conn):
                current_rks = set(
                    r[0] for r in conn.execute(
                        "SELECT rating_key FROM plex_items "
                        "WHERE section_id = ? "
                        "  AND media_type = ?",
                        (section_id, items_media_type),
                    ).fetchall()
                )
                stale_rks = current_rks - seen_rks
                # v0.51.128: the INSTANTANEOUS missing set THIS enum — the
                # amplifier-sweep mass-guard (v1.18.10) keys off this, NOT the
                # narrowed reap set below, so a churning large-scale Plex glitch
                # (a different partial subset missing each enum) can't sneak a
                # sub-50 threshold-crossed core past the >50/>20% abort.
                _instantaneous_stale = set(stale_rks)
                # v0.51.128: don't reap on a SINGLE missing enum — a transient
                # Plex glitch (partial catalog, API hiccup) that drops a row for
                # one enum would false-DELETE it + fire a false 💔 Theme lost.
                # Track consecutive misses; reset any previously-flagged row that
                # reappeared this enum; reap only rows that have now been missing
                # for >= _REAP_MISS_THRESHOLD consecutive enums. enumerate_section
                # _items already raises on partial fetches (v1.23.64), so a stale
                # set here is real-or-transient, not a mid-page truncation — this
                # counter is the grace window that tells the two apart.
                _flagged = set(
                    r[0] for r in conn.execute(
                        "SELECT rating_key FROM plex_items "
                        "WHERE section_id = ? AND media_type = ? "
                        "  AND consecutive_missing > 0",
                        (section_id, items_media_type),
                    ).fetchall()
                )
                _reappeared = list(_flagged - stale_rks)
                for _ri in range(0, len(_reappeared), 500):
                    _rc = _reappeared[_ri:_ri + 500]
                    _rph = ",".join(["?"] * len(_rc))
                    conn.execute(
                        f"UPDATE plex_items SET consecutive_missing = 0 "
                        f"WHERE section_id = ? AND media_type = ? "
                        f"  AND rating_key IN ({_rph})",
                        [section_id, items_media_type] + _rc,
                    )
                if stale_rks:
                    _stale_all = list(stale_rks)
                    for _si in range(0, len(_stale_all), 500):
                        _sc = _stale_all[_si:_si + 500]
                        _sph = ",".join(["?"] * len(_sc))
                        conn.execute(
                            f"UPDATE plex_items SET "
                            f"consecutive_missing = consecutive_missing + 1 "
                            f"WHERE section_id = ? AND media_type = ? "
                            f"  AND rating_key IN ({_sph})",
                            [section_id, items_media_type] + _sc,
                        )
                    # Post-increment, only rows at >= threshold reap. Reappeared
                    # rows were zeroed above, so a >= threshold row is necessarily
                    # a still-missing one — no need to re-filter by stale_rks.
                    _reap = set(
                        r[0] for r in conn.execute(
                            "SELECT rating_key FROM plex_items "
                            "WHERE section_id = ? AND media_type = ? "
                            "  AND consecutive_missing >= ?",
                            (section_id, items_media_type,
                             _REAP_MISS_THRESHOLD),
                        ).fetchall()
                    )
                    if not _reap:
                        log.info(
                            "v0.51.128 reaper: %d stale plex_items in section "
                            "%s (media_type=%s) now at 1 consecutive miss — "
                            "deferring reap until %d consecutive misses "
                            "(transient-glitch guard).",
                            len(stale_rks), section_id, items_media_type,
                            _REAP_MISS_THRESHOLD,
                        )
                    # Narrow the reap to the threshold-crossed set — the existing
                    # mass-guard, candidate capture + DELETE below all key off
                    # stale_rks, so they now act only on genuinely-lost rows.
                    stale_rks = _reap
                if stale_rks:
                    # v0.51.128: evaluate the mass-guard on the instantaneous
                    # stale count, not the narrowed reap set — a heavy-churn
                    # glitch whose threshold-crossed core is < 50 must still
                    # abort the whole reap when the section-wide miss is large.
                    pct = (
                        100 * len(_instantaneous_stale)
                        // max(1, len(current_rks))
                    )
                    if len(_instantaneous_stale) > 50 and pct > 20:
                        log.warning(
                            "v1.18.89 reaper: would delete %d/%d "
                            "(%d%%) stale plex_items in section %s "
                            "(media_type=%s) — exceeds 20%% + "
                            "50-row safety threshold, ABORTING "
                            "reap. Likely Plex returned a smaller "
                            "catalog than expected (API glitch?). "
                            "Investigate manually before re-running.",
                            len(_instantaneous_stale), len(current_rks),
                            pct, section_id, items_media_type,
                        )
                    else:
                        # Sample the first 5 for operator visibility.
                        sample_ph = ",".join(["?"] * min(5, len(stale_rks)))
                        sample_rks = list(stale_rks)[:5]
                        sample = conn.execute(
                            f"SELECT rating_key, title FROM plex_items "
                            f"WHERE rating_key IN ({sample_ph}) "
                            f"  AND media_type = ?",
                            sample_rks + [items_media_type],
                        ).fetchall()
                        # v1.18.90/v1.18.91 (Bug B fix): capture
                        # pre-delete theme info for stale rows with
                        # has_theme=1. Pre-v1.18.91 used INNER JOIN
                        # themes on pi.theme_id — but theme_id is
                        # NULL for rows that never got resolved
                        # (orphans, unresolved syncs), so the
                        # candidate set silently missed real
                        # lost-theme cases. Now: LEFT JOIN themes
                        # + COALESCE the keys against plex_items'
                        # own guid_tmdb so unresolved rows still
                        # surface. Translate plex_items.media_type
                        # ('show' → 'tv') for themes-domain lookup
                        # consistency.
                        stale_list = list(stale_rks)
                        ph_full = ",".join(["?"] * len(stale_list))
                        # v1.19.41: capture pi.folder_path so the
                        # dispatch-loop filesystem fallback check
                        # (sidecar at {folder_path}/theme.mp3 on
                        # disk) has the path. The reaper DELETEs
                        # these rows below, so the path must be
                        # captured BEFORE the delete. Also captures
                        # pi.media_type (the original 'show' vs 'tv'
                        # value before the COALESCE swap) so the
                        # Tier-3 sidecar check can match on
                        # plex_items.media_type directly without
                        # re-deriving.
                        lost_candidates_raw = conn.execute(
                            f"SELECT DISTINCT "
                            f"  COALESCE(t.media_type, "
                            f"    CASE pi.media_type "
                            f"      WHEN 'show' THEN 'tv' "
                            f"      ELSE pi.media_type "
                            f"    END) AS mt, "
                            f"  COALESCE(t.tmdb_id, pi.guid_tmdb) AS tmdb_id, "
                            f"  COALESCE(t.title, pi.title) AS title, "
                            f"  pi.folder_path AS folder_path, "
                            f"  pi.media_type AS pi_media_type "
                            f"FROM plex_items pi "
                            f"LEFT JOIN themes t ON t.id = pi.theme_id "
                            f"WHERE pi.section_id = ? "
                            f"  AND pi.media_type = ? "
                            f"  AND pi.rating_key IN ({ph_full}) "
                            f"  AND pi.has_theme = 1 "
                            f"  AND COALESCE(t.tmdb_id, pi.guid_tmdb) "
                            f"      IS NOT NULL",
                            [section_id, items_media_type] + stale_list,
                        ).fetchall()
                        # Batch the DELETE — large IN clauses are
                        # fine on SQLite but stay tidy with chunks
                        # of 500 just in case.
                        for i in range(0, len(stale_list), 500):
                            chunk = stale_list[i:i + 500]
                            ph = ",".join(["?"] * len(chunk))
                            conn.execute(
                                f"DELETE FROM plex_items "
                                f"WHERE section_id = ? "
                                f"  AND media_type = ? "
                                f"  AND rating_key IN ({ph})",
                                [section_id, items_media_type] + chunk,
                            )
                        log.warning(
                            "v1.18.89 reaper: deleted %d stale "
                            "plex_items rows from section %s "
                            "(media_type=%s, Plex no longer "
                            "returns these rks). Sample: %s",
                            len(stale_rks), section_id,
                            items_media_type,
                            [dict(r) for r in sample],
                        )
                        # v1.18.90: for each candidate theme,
                        # check if anything else still covers it.
                        # All checks INSIDE the transaction (same
                        # snapshot) so the gate is consistent.
                        # v1.18.91: the SELECT now aliases as
                        # `mt` / `tmdb_id` / `title` (was the
                        # underlying themes columns); update field
                        # references to match.
                        # v0.51.14 (audit #7): once ONE tier-2 fs check stalls,
                        # skip the rest this reap (mirrors the v1.22.65 Phase-1
                        # treatment) — 50 candidates × a 30s deadline each would
                        # still hold the writer lock for minutes on a dead mount.
                        _reaper_fs_stalled = False
                        for cand in lost_candidates_raw:
                            mt = cand["mt"]
                            tid = cand["tmdb_id"]
                            # v1.24.8: THIS lost edition's key (from its folder).
                            # The four-way fallback classifiers below keyed on
                            # (mt, tid) only, so on a multi-edition title a
                            # SIBLING edition's backup/sidecar got advertised as
                            # the lost edition's (PROMOTE/ADOPT the wrong file).
                            # Scope each to `edition_key IN (this, '')` — prefer
                            # this edition, allow the shared '' (standard) so a
                            # named loss with only a '' backup still recovers,
                            # but EXCLUDE other named siblings. still_p above
                            # stays title-wide by design (v1.22.32).
                            _cand_edition = edition_key_for_folder(
                                cand["folder_path"])
                            # Surviving P-claim anywhere?
                            # v1.22.32 (audit): match the SAME COALESCE the
                            # candidate set (above) uses. Pre-fix this INNER
                            # JOIN'd themes on pi.theme_id, so a SURVIVING
                            # sibling Plex still themes (has_theme=1) but motif
                            # never linked (theme_id NULL — a multi-edition rk,
                            # or a anime-agent match Plex tagged that TDB didn't cover)
                            # was invisible → the reaper fired a FALSE "theme
                            # lost" while Plex was still serving the title on
                            # that sibling. Now also matches surviving rows by
                            # plex_items.guid_tmdb directly (theme_id link
                            # optional), with the show↔tv media_type swap.
                            # v1.22.72: collections reach here too (the v1.18.91
                            # collections reaper pass) — 'movie' made the guid-
                            # direct branch inert for them (false theme_lost on
                            # multi-section collections) and could false-match an
                            # unrelated movie sharing the numeric id. The
                            # sidecar_db query below got this CASE right already.
                            plex_mt = ("show" if mt == "tv"
                                       else "collection" if mt == "collection"
                                       else "movie")
                            # v1.23.64 (audit): only a survivor in a MANAGED
                            # (included) section means the title is still themed.
                            # Pre-fix this had NO plex_sections gate, so a stale
                            # has_theme=1 row left in a DISABLED/removed section
                            # (retained in plex_items as history per the module
                            # docstring) masked a GENUINE theme loss in a managed
                            # section → the reaper silently SUPPRESSED the real
                            # 'theme lost' notification with no breadcrumb.
                            still_p = conn.execute(
                                "SELECT 1 FROM plex_items pi "
                                "JOIN plex_sections ps "
                                "  ON ps.section_id = pi.section_id "
                                "  AND ps.included = 1 "
                                "LEFT JOIN themes t ON t.id = pi.theme_id "
                                "WHERE pi.has_theme = 1 "
                                "  AND ( (t.media_type = ? AND t.tmdb_id = ?) "
                                "        OR (pi.media_type = ? "
                                "            AND pi.guid_tmdb = ?) ) "
                                "LIMIT 1",
                                (mt, tid, plex_mt, tid),
                            ).fetchone()
                            if still_p:
                                continue
                            # v1.19.41: four-way tier classifier
                            # (was a single has_fallback boolean
                            # gate that silently skipped on any
                            # fallback — that hid the backup-ready
                            # AND sidecar-available cases under
                            # the same no-notification rule).
                            #
                            # Tier 1: backup-ready
                            #   user_overrides.intent='backup' OR
                            #   local_files.source_kind='plex_cloud'
                            #   → PROMOTE TO ACTIVE notification
                            #
                            # Tier 2: sidecar-available
                            #   plex_items.local_theme_file=1 on any
                            #   rk for the same (mt, tmdb) OR
                            #   filesystem check finds theme.mp3 at
                            #   the doomed row's folder_path
                            #   → ADOPT notification
                            #
                            # Tier 3: other fallback (replace-intent
                            #   override, placement, non-backup
                            #   local_file) → silent skip (row
                            #   already has working theme)
                            #
                            # Tier 4: no fallback → existing
                            #   plex_theme_lost notification
                            #
                            # The `source_kind = 'plex_cloud'` check
                            # is forward-compatible with v1.19.42 —
                            # the v58 CHECK widening will allow that
                            # value. Pre-v58 the row can't have it
                            # so the gate always misses (correct).
                            # v1.19.61: third UNION ALL clause for
                            # rows stamped `backup_only` (the broader
                            # post-v1.19.61 BK class — any
                            # source_kind, including 'themerrdb' /
                            # 'url' / 'upload' that ended up unplaced
                            # because Plex was serving). Pre-v1.19.61
                            # only the explicit user-URL backup
                            # intent + the v1.19.42 plex_cloud signal
                            # fired the backup_ready notification.
                            # Worker's plex_has_theme skip now
                            # stamps backup_only (v1.19.61 unification)
                            # so 86 EIGHTY-SIX-shape rows get the
                            # backup_ready surface on theme loss.
                            # plex_cloud rows match BOTH the second
                            # and third clauses; the second wins via
                            # ORDER BY in the calling logic
                            # (UNION ALL preserves source order
                            # provided LIMIT 1 picks the first match).
                            backup_signal = conn.execute(
                                "SELECT 'user_url_backup' AS src "
                                "  FROM user_overrides "
                                " WHERE media_type = ? AND tmdb_id = ? "
                                "   AND intent = 'backup' "
                                "   AND COALESCE(edition_key, '') IN (?, '') "
                                "UNION ALL "
                                "SELECT 'plex_cloud_backup' AS src "
                                "  FROM local_files "
                                " WHERE media_type = ? AND tmdb_id = ? "
                                "   AND source_kind = 'plex_cloud' "
                                "   AND COALESCE(edition_key, '') IN (?, '') "
                                "UNION ALL "
                                "SELECT 'backup_only_stamp' AS src "
                                "  FROM local_files "
                                " WHERE media_type = ? AND tmdb_id = ? "
                                "   AND last_place_attempt_reason "
                                "       = 'backup_only' "
                                "   AND source_kind != 'plex_cloud' "
                                "   AND COALESCE(edition_key, '') IN (?, '') "
                                "LIMIT 1",
                                (mt, tid, _cand_edition,
                                 mt, tid, _cand_edition,
                                 mt, tid, _cand_edition),
                            ).fetchone()
                            # Tier-2 (sidecar) DB check: scan
                            # plex_items for the same tmdb_id on
                            # any rk (Plex might have re-added the
                            # item with a new rk during the same
                            # enum cycle's Phase 1 — that new row's
                            # local_theme_file would be set).
                            # v1.22.39 (audit): match the survivor via theme_id
                            # linkage too (mirrors the v1.22.32 still_p fix).
                            # Pre-fix this keyed on guid_tmdb ONLY, so an
                            # anime survivor (guid_tmdb NULL, theme_id-linked,
                            # local_theme_file=1) was missed → the row mis-tiered
                            # to no_fallback and fired the wrong "theme lost, no
                            # recovery" alert. sidecar_fs only covers it when the
                            # doomed row's folder_path was populated + on disk.
                            sidecar_db = conn.execute(
                                "SELECT 1 FROM plex_items pi "
                                " LEFT JOIN themes t ON t.id = pi.theme_id "
                                " WHERE pi.local_theme_file = 1 "
                                "   AND ( (CASE pi.media_type WHEN 'show' "
                                "            THEN 'tv' ELSE pi.media_type END = ? "
                                "          AND pi.guid_tmdb = ?) "
                                "         OR (t.media_type = ? AND t.tmdb_id = ?) ) "
                                "   AND COALESCE(pi.edition_key, '') IN (?, '') "
                                "LIMIT 1",
                                (mt, tid, mt, tid, _cand_edition),
                            ).fetchone()
                            # Tier-2 (sidecar) filesystem fallback:
                            # check disk directly using the doomed
                            # row's folder_path captured pre-delete.
                            # Catches the case where Plex hasn't yet
                            # re-added the item (Phase 1 of THIS
                            # cycle ran BEFORE the reaper) so the
                            # DB check above wouldn't find it. The
                            # OS stat is cheap; we're already
                            # outside the hot path of the loop.
                            sidecar_fs = False
                            _folder_path = cand["folder_path"]
                            if _folder_path and not _reaper_fs_stalled:
                                # v1.22.15: resolve host→container before
                                # stat (raw Path().exists() was ALWAYS False
                                # inside the container). v1.22.72: full
                                # SIDECAR_AUDIO_EXTS via the existing helper.
                                # v0.51.14 (audit #7): BOUNDED — this check
                                # runs inside the reap's BEGIN IMMEDIATE txn,
                                # and find_theme_sidecar_path does unbounded
                                # is_dir/iterdir on /data; a stalled NFS/SMB
                                # mount (the v1.11.63 hang class) held the
                                # SQLite writer lock indefinitely, failing
                                # every other motif write with 'database is
                                # locked'. Same no-progress deadline + abandon-
                                # without-join treatment the Phase-1 stats got
                                # in v1.22.65; on timeout the row goes
                                # indeterminate (sidecar_fs=False — the tier
                                # falls back conservatively) and the reap
                                # commits.
                                from concurrent.futures import (
                                    ThreadPoolExecutor as _TPE,
                                    TimeoutError as _FutTimeout,
                                )
                                _ex = _TPE(max_workers=1,
                                           thread_name_prefix="motif-reap-fs")
                                _fut = _ex.submit(
                                    find_theme_sidecar_path, _folder_path)
                                try:
                                    sidecar_fs = (
                                        _fut.result(
                                            timeout=_SIDECAR_STALL_TIMEOUT_S)
                                        is not None)
                                except _FutTimeout:
                                    _reaper_fs_stalled = True
                                    log.warning(
                                        "reaper: sidecar-fs check for %s made "
                                        "no progress for %.0fs — treating as "
                                        "indeterminate (stalled mount?), "
                                        "skipping remaining fs checks this "
                                        "reap", _folder_path,
                                        _SIDECAR_STALL_TIMEOUT_S)
                                except OSError as _e:
                                    log.debug(
                                        "v1.19.41 sidecar-fs check "
                                        "raised for %s: %s",
                                        _folder_path, _e,
                                    )
                                finally:
                                    # never join a possibly-hung thread —
                                    # leaked once, logged above (v1.22.65).
                                    _ex.shutdown(wait=False)
                            sidecar_present = bool(sidecar_db) or sidecar_fs
                            # Tier-3 (other fallback) check.
                            # source_kind != 'plex_cloud' so we
                            # don't double-count tier-1 plex_cloud
                            # rows as tier-3 other-fallback.
                            # intent != 'backup' so we don't
                            # double-count tier-1 user-URL backups.
                            other_fallback = conn.execute(
                                "SELECT 1 FROM local_files "
                                " WHERE media_type = ? AND tmdb_id = ? "
                                "   AND COALESCE(source_kind, '') "
                                "       != 'plex_cloud' "
                                "   AND COALESCE(edition_key, '') IN (?, '') "
                                "UNION ALL "
                                "SELECT 1 FROM placements "
                                " WHERE media_type = ? AND tmdb_id = ? "
                                "   AND COALESCE(edition_key, '') IN (?, '') "
                                "UNION ALL "
                                "SELECT 1 FROM user_overrides "
                                " WHERE media_type = ? AND tmdb_id = ? "
                                "   AND COALESCE(intent, 'replace') "
                                "       != 'backup' "
                                "   AND COALESCE(edition_key, '') IN (?, '') "
                                "LIMIT 1",
                                (mt, tid, _cand_edition,
                                 mt, tid, _cand_edition,
                                 mt, tid, _cand_edition),
                            ).fetchone()
                            # Decide tier — highest wins.
                            if backup_signal:
                                tier = "backup_ready"
                                backup_source = backup_signal["src"]
                            elif sidecar_present:
                                tier = "sidecar_available"
                                backup_source = None
                            elif other_fallback:
                                # Silent skip — row already has a
                                # working theme. Existing behavior.
                                continue
                            else:
                                tier = "no_fallback"
                                backup_source = None
                            lost_theme_candidates.append({
                                "media_type": mt,
                                "tmdb_id": tid,
                                "title": cand["title"],
                                "section_id": section_id,
                                # v1.21.76: carry folder_path so the
                                # dispatch can name the edition.
                                "folder_path": cand["folder_path"],
                                "tier": tier,
                                "backup_source": backup_source,
                                "sidecar_present": sidecar_present,
                            })
    # v1.18.90: dispatch plex_theme_lost notifications outside the
    # transaction so an Apprise HTTP failure doesn't roll back the
    # reap. Per-row dedup via notify_dedupe with composite key
    # `plex_theme_lost:<mt>:<tmdb>` and a 24h rate-limit — bulk
    # Plex changes can't produce a notification flood.
    # Best-effort (class-9 safe): wrap each dispatch in try/except
    # so one bad row doesn't poison the rest.
    # v1.19.41: four-way tier split — each candidate now carries a
    # `tier` field set during the gate above. Dispatch routes to:
    #   backup_ready      → theme_lost_backup_ready notification
    #                        (PROMOTE TO ACTIVE primary CTA)
    #   sidecar_available → theme_lost_sidecar_available
    #                        (ADOPT primary CTA, lower urgency)
    #   no_fallback       → legacy plex_theme_lost (SET URL /
    #                        UPLOAD MP3, retuned body)
    # Tier 3 (other fallback) is excluded above via `continue` —
    # rows with placement / replace-intent override already have
    # a working theme so no notification fires.
    if lost_theme_candidates:
        try:
            from ..config import Settings
            from . import notify as _notify
            from . import notify_content as _nc
            from . import notify_dedupe as _ndedupe
            _settings = Settings()
            for cand in lost_theme_candidates:
                mt = cand["media_type"]
                tid = cand["tmdb_id"]
                tier = cand.get("tier", "no_fallback")
                # v1.24.8: resolve this candidate's edition once — used in both
                # the dedupe key (so two editions of one title losing their theme
                # in the same 24h window EACH notify; pre-fix the 2nd was rate-
                # limited away by the 1st's edition-less key) and the enrich_item
                # edition label below.
                _disp_edition = edition_key_for_folder(cand.get("folder_path"))
                # Dedupe key includes tier so a row that flips from
                # one tier to another inside the 24h window still
                # gets a notification — e.g. user adds a KEEP AS
                # BACKUP override after the initial no-fallback
                # notification fires, then Plex loses the theme
                # again 12h later: the tier-1 notification SHOULD
                # fire even though we're inside the no_fallback
                # dedupe window.
                dedupe_key = f"plex_theme_lost:{tier}:{mt}:{tid}:{_disp_edition}"
                if not _ndedupe.should_fire(
                    db_path, dedupe_key,
                    rate_limit_seconds=86400,
                ):
                    continue
                try:
                    _ctx = _nc.enrich_item(
                        db_path,
                        media_type=mt, tmdb_id=tid,
                        section_id=cand["section_id"],
                        # v1.21.76: name the edition (💔 lost / 🎯 ready).
                        edition_key=_disp_edition,  # v1.24.8: resolved above
                        # v0.51.123: a lost P-row with no ThemerrDB match has no
                        # `themes` title, so enrich_item would fall back to the
                        # bare "tv/<tmdb>" id. The candidate carries the row's
                        # plex_items.title — pass it so the "💔 Theme lost —"
                        # subject names the actual content (the user).
                        fallback_title=cand.get("title") or "",
                    )
                    # Pick body + event_kind by tier.
                    if tier == "backup_ready":
                        _event_kind = "theme_lost_backup_ready"
                        _title = (
                            _nc.format_theme_lost_backup_ready_title(
                                _ctx
                            )
                        )
                        _body = (
                            _nc.format_theme_lost_backup_ready_body(
                                _ctx,
                                backup_source=cand["backup_source"],
                                sidecar_present=cand[
                                    "sidecar_present"
                                ],
                            )
                        )
                    elif tier == "sidecar_available":
                        _event_kind = "theme_lost_sidecar_available"
                        _title = (
                            _nc
                            .format_theme_lost_sidecar_available_title(
                                _ctx
                            )
                        )
                        _body = (
                            _nc
                            .format_theme_lost_sidecar_available_body(
                                _ctx
                            )
                        )
                    else:  # no_fallback (Tier 4)
                        _event_kind = "plex_theme_lost"
                        _title = _nc.format_plex_theme_lost_title(_ctx)
                        _body = _nc.format_plex_theme_lost_body(_ctx)
                    _notify.dispatch(
                        db_path,
                        _settings.cfg.notifications,
                        event_kind=_event_kind,
                        title=_title,
                        body=_body,
                        body_format="markdown",
                        # v1.23.75: FB-sourced backup/sidecar attaches its
                        # thumb (v1.22.94) — the reaper tiers were never
                        # retrofitted, so a fbcdn backup showed no image.
                        attach_url=_nc.attachment_thumb_url(_ctx),
                    )
                    _ndedupe.record_fire(db_path, dedupe_key)
                except Exception as _e:  # noqa: BLE001
                    # v1.19.41: hot-path silent-fail downgrade per
                    # CLAUDE.md class-9 sub-pattern. First
                    # occurrence per process gets log.warning;
                    # subsequent fall through to log.debug. The
                    # operator sees ONE warning at boot if the
                    # dispatch is genuinely broken (Apprise URL
                    # malformed, service down, scrubbed token, etc.)
                    # — was previously buried at log.debug
                    # invisible-at-INFO.
                    global _THEME_LOST_NOTIFY_WARNED
                    if not _THEME_LOST_NOTIFY_WARNED:
                        log.warning(
                            "v1.19.41: theme-lost notify "
                            "dispatch failed for %s/%s "
                            "(tier=%s): %s. Subsequent failures "
                            "downgrade to debug; investigate "
                            "Apprise config if this is a fresh "
                            "deploy.",
                            mt, tid, tier, _e,
                        )
                        _THEME_LOST_NOTIFY_WARNED = True
                    else:
                        log.debug(
                            "theme-lost notify dispatch "
                            "suppressed for %s/%s (tier=%s): %s",
                            mt, tid, tier, _e,
                        )
                    # v1.23.64 (audit): this candidate came from the v1.18.89
                    # reaper, which already DELETED the source plex_items row in
                    # the committed txn above — so a transient dispatch failure is
                    # UNRECOVERABLE: no future enum re-detects the loss to retry
                    # the push (the rk is gone). record_fire is correctly NOT
                    # reached (dedupe stays open) but that's moot with no source to
                    # re-fire from. Persist a durable events row so the loss is
                    # STILL surfaced in the LOGS UI even though the push didn't
                    # deliver — turns a silent-forever drop into an operator-
                    # visible record (the warn-once log above scrolls away).
                    try:
                        log_event(
                            db_path, level="WARNING", component="plex_enum",
                            message=(
                                "Plex stopped serving a theme but the theme-lost "
                                "notification failed to dispatch — the source row "
                                "was already reaped, so this will not retry"
                            ),
                            detail={
                                "media_type": mt, "tmdb_id": tid, "tier": tier,
                                "section_id": cand.get("section_id"),
                                "error": str(_e),
                            },
                        )
                    except Exception as _le:  # noqa: BLE001
                        log.debug(
                            "theme-lost durable log_event failed for "
                            "%s/%s: %s", mt, tid, _le,
                        )
        except Exception as _outer:  # noqa: BLE001
            # v1.19.44: outer dispatch swallow now follows the
            # v1.17.11 warn-then-debug-flag pattern. Pre-fix this
            # was log.debug unconditionally — an import error or
            # config-load failure after a partial deploy would
            # silently drop the ENTIRE plex_theme_lost dispatch
            # cycle with zero operator visibility (no breadcrumb
            # at default log level). Class-9 outer-catch-all
            # sub-pattern. First occurrence logs warning so the
            # operator sees the failure at boot; subsequent drop
            # to debug so the log doesn't drown if the failure
            # repeats every enum cycle.
            global _THEME_LOST_DISPATCH_OUTER_WARNED
            if not _THEME_LOST_DISPATCH_OUTER_WARNED:
                log.warning(
                    "plex_theme_lost outer dispatch loop "
                    "suppressed (first occurrence — subsequent "
                    "logged at debug): %s",
                    _outer,
                )
                _THEME_LOST_DISPATCH_OUTER_WARNED = True
            else:
                log.debug(
                    "plex_theme_lost outer dispatch loop "
                    "suppressed: %s",
                    _outer,
                )
    # v1.11.26: stamp theme_id once per enum so /api/library's row
    # query becomes a direct PK lookup instead of running a heavy
    # correlated subquery on every page render.
    # v1.12.18: scope to the section we just enumerated when called
    # from the per-section path. Pre-fix this always re-resolved
    # the entire plex_items table (~10K+ rows), turning a 28-item
    # section refresh into a multi-minute job and contending hard
    # with the writer lock the whole time.
    # v1.21.48: capture the rows resolve_theme_ids LINKS this run
    # (theme_id NULL→set) so the theme-available push triggers on the
    # actual "row became downloadable" event, not just newly-inserted
    # items. A PRE-EXISTING plex_item linked late (its guid resolved
    # this enum, or a theme was published after the item was already
    # enumerated) is never in new_item_rks, so the two proxy triggers
    # (sync is_new / new-item) both miss it — the user's repro: the 2026
    # movie "The Legend of Aang" sat SRC=— with a TDB theme available
    # but never downloaded.
    newly_linked: list[str] = []
    resolve_theme_ids(db_path, section_id=section_id,
                      cancel_check=cancel_check,
                      collect_newly_linked=newly_linked)
    # v1.21.5: theme-available push. resolve_theme_ids above stamped
    # theme_id on the SRC=— subset (no Plex theme, no motif content, TDB
    # theme available) — now knowable. v1.21.48: union newly-inserted +
    # newly-linked so each fires exactly once at the moment its
    # (item↔theme) link forms, whichever side was the "new" one. Gate on
    # updated>0 so the FIRST enum of a section (initial seed — every row
    # is an insert, updated=0) stays silent; every re-enum bumps
    # last_seen_at on existing rows (updated>0) so genuine new links fire.
    candidate_rks = list(dict.fromkeys(new_item_rks + newly_linked))
    if candidate_rks and updated > 0:
        _maybe_notify_theme_available(db_path, candidate_rks)
    return inserted, updated


def _maybe_notify_theme_available(
    db_path: Path, candidate_rks: list[str],
) -> None:
    """v1.21.5/v1.21.9: after a plex refresh, handle genuinely-new
    Plex items that have no theme anywhere (SRC=—) but match an
    available ThemerrDB theme. v1.21.9: if
    sync.auto_download_new_themes_for_unthemed_rows is ON, AUTO-ACQUIRE
    them (download → non-forced place) — the same closed-loop behavior
    the TDB-sync side already had for its is_new SRC=— rows; otherwise
    dispatch the new_tdb_theme_available notification (if that event is
    enabled). the user's ask: "after a plex refresh if there's a new item
    in plex that matches a themerrdb theme available and doesn't have a
    plex provided theme" — notify OR (now) auto-download.

    This ships the v1.17.0-deferred new_tdb_theme_available event by
    hooking the plex_enum side — sparse + naturally rate-limited —
    instead of resolve_theme_ids' 500-row hot loop (the original
    deferral blocker). v1.21.48: the caller scopes `candidate_rks` to
    rows that became downloadable THIS enum — newly INSERTED items PLUS
    rows resolve_theme_ids just LINKED (theme_id NULL→set, the late-link
    gap) — and only calls when the section was already populated (the
    updated>0 gate); here we further narrow to the SRC=— subset with a
    resolved theme_id.

    Bulk-shaped (one digest per refresh, not one ping per row) like
    themes_added_by_sync; per-(media_type, tmdb_id) dedupe so a
    Plex remove+re-add (new rk) doesn't re-ping. Best-effort
    (class-9): a notify failure never disturbs the enum."""
    try:
        from ..config import Settings
        _settings = Settings()
        # v1.21.9: ONE knob governs both discovery paths. If
        # sync.auto_download_new_themes_for_unthemed_rows is ON,
        # AUTO-ACQUIRE these SRC=— rows (mirrors the TDB-sync is_new
        # auto-download) — closing the sync-vs-plex discovery asymmetry
        # so a Plex-added unthemed row behaves the same as a
        # sync-discovered one. Otherwise dispatch the
        # new_tdb_theme_available notification if that event is on.
        _auto_dl = bool(getattr(
            _settings.cfg.sync,
            "auto_download_new_themes_for_unthemed_rows", False))
        _notify_on = bool(_settings.cfg.notifications.events.get(
            "new_tdb_theme_available", False))
        if not _auto_dl and not _notify_on:
            return
        # (media_type, tmdb_id) -> (title, year, section_id). The dict
        # collapses multi-edition rows (same tmdb across standard/4K)
        # to a single candidate.
        cand: dict[tuple[str, int], tuple[str, int | None, str | None]] = {}
        with get_conn(db_path) as conn:
            for i in range(0, len(candidate_rks), 400):
                chunk = candidate_rks[i:i + 400]
                ph = ",".join("?" * len(chunk))
                # edition-blind OK (v1.21.94): title-global candidate detection
                # — surfaces titles with NO motif state for the theme-available
                # offer. The NOT EXISTS subqueries are (mt, tmdb) by design (any
                # edition's state suppresses the candidate); section_id is only
                # an output column, collapsed per (mt, tmdb) into `cand`.
                rows = conn.execute(
                    "SELECT t.media_type AS mt, t.tmdb_id AS tmdb_id, "
                    "       COALESCE(t.title, pi.title) AS title, "
                    "       COALESCE(t.year, pi.year) AS year, "
                    "       pi.section_id AS section_id "
                    "  FROM plex_items pi "
                    "  JOIN themes t ON t.id = pi.theme_id "
                    f" WHERE pi.rating_key IN ({ph}) "
                    "   AND (pi.has_theme = 0 "
                    "        OR pi.plex_theme_verified_ok = 0) "
                    "   AND pi.local_theme_file = 0 "
                    "   AND NOT EXISTS (SELECT 1 FROM local_files lf "
                    "        WHERE lf.media_type = t.media_type "
                    "          AND lf.tmdb_id = t.tmdb_id) "
                    "   AND NOT EXISTS (SELECT 1 FROM user_overrides uo "
                    "        WHERE uo.media_type = t.media_type "
                    "          AND uo.tmdb_id = t.tmdb_id) "
                    "   AND NOT EXISTS (SELECT 1 FROM placements p "
                    "        WHERE p.media_type = t.media_type "
                    "          AND p.tmdb_id = t.tmdb_id)",
                    chunk,
                ).fetchall()
                for r in rows:
                    cand[(r["mt"], r["tmdb_id"])] = (
                        r["title"], r["year"], r["section_id"],
                    )
        if not cand:
            return
        # v1.21.9: auto-acquire branch. _enqueue_download dedupes
        # against pending/running downloads + matches via theme_id
        # linkage (handles anime), and chains into a NON-forced
        # place via auto_place — so an existing theme can never be
        # overwritten (the place skips on plex_has_theme / existing
        # sidecar). Mirrors sync.py's is_new auto-download exactly, so
        # a Plex-discovered SRC=— row and a sync-discovered one behave
        # identically under the same toggle.
        if _auto_dl:
            from .sync import _enqueue_download
            _enq = 0
            try:
                with get_conn(db_path) as conn, transaction(conn):
                    for (mt, tid) in cand:
                        _enq += _enqueue_download(
                            conn, media_type=mt, tmdb_id=tid,
                            reason="new")
            except Exception as _ee:  # noqa: BLE001
                log.warning(
                    "v1.21.9: plex-refresh auto-download enqueue "
                    "failed: %s", _ee)
            if _enq:
                log.info(
                    "v1.21.9: plex refresh auto-download enqueued %d "
                    "new SRC=— item(s) with a TDB theme available "
                    "(auto_download_new_themes_for_unthemed_rows on)",
                    _enq)
            return
        # Notify branch (the new_tdb_theme_available event is on).
        from . import notify as _notify
        from . import notify_content as _nc
        from . import notify_dedupe as _ndedupe
        # Dedupe-filter: only (mt, tmdb) not fired inside the window.
        fresh: list[tuple[str, int, str, int | None, str | None, str]] = []
        for (mt, tid), (title, year, section_id) in cand.items():
            key = f"new_tdb_theme_available:{mt}:{tid}"
            if _ndedupe.should_fire(
                    db_path, key,
                    rate_limit_seconds=_THEME_AVAIL_DEDUPE_SECONDS):
                fresh.append((mt, tid, title, year, section_id, key))
        if not fresh:
            return
        notif = _settings.cfg.notifications
        if len(fresh) == 1:
            mt, tid, _title, _year, section_id, _key = fresh[0]
            _ctx = _nc.enrich_item(
                db_path, media_type=mt, tmdb_id=tid,
                section_id=section_id,
            )
            _notify.dispatch(
                db_path, notif,
                event_kind="new_tdb_theme_available",
                title=_nc.format_theme_available_title(_ctx),
                body=_nc.format_theme_available_body(_ctx),
                body_format="markdown",
            )
        else:
            labels = [
                f"{title} ({year})" if year else f"{title}"
                for _mt, _tid, title, year, _sec, _key in fresh
            ]
            _notify.dispatch(
                db_path, notif,
                event_kind="new_tdb_theme_available",
                title=_nc.format_theme_available_batch_title(len(fresh)),
                body=_nc.format_theme_available_batch_body(labels),
                body_format="markdown",
            )
        for _mt, _tid, _title, _year, _sec, key in fresh:
            _ndedupe.record_fire(db_path, key)
    except Exception as _e:  # noqa: BLE001
        global _THEME_AVAIL_NOTIFY_WARNED
        if not _THEME_AVAIL_NOTIFY_WARNED:
            log.warning(
                "v1.21.5: new_tdb_theme_available notify dispatch "
                "failed: %s. Subsequent failures downgrade to debug "
                "— check Apprise config if this is a fresh deploy.",
                _e,
            )
            _THEME_AVAIL_NOTIFY_WARNED = True
        else:
            log.debug(
                "new_tdb_theme_available notify dispatch suppressed: %s",
                _e,
            )


def _verify_theme_claims(
    db_path: Path, client: PlexClient,
    *, section_id: str | None = None,
    cancel_check=lambda: False,
) -> int:
    """v1.12.112: HEAD-probe Plex's `theme="..."` claims so motif's
    SRC=P reflects ground truth instead of Plex's metadata cache.

    Targets only ambiguous rows: `has_theme=1` AND `local_theme_file=0`
    (no sidecar — so SRC would land in case 7) AND verification needs
    to run. v1.12.113 broadened "needs verification" to include
    `verified_ok=0` rows older than 7 days (handle transient Plex
    404s). v1.12.115 adds the symmetric TTL on `verified_ok=1` —
    after 30 days, re-verify successful claims too. Closes the
    edge case where Plex's in-memory cache served a just-deleted
    theme during PURGE inline-verify, motif marked verified_ok=1,
    and Plex evicted its cache later but motif kept the stale 1
    forever (until URI changed). 30-day cycle keeps steady-state
    HEAD cost near zero — for a library with 100 P-classified rows,
    ~3–4 verifications per day average.

    Sidecared rows (M/T/U/A) classify earlier in the SRC priority
    chain and don't need this signal.

    Each probe writes either ok=1 (200), ok=0 (404), or leaves
    ok=NULL on transient errors so a network blip doesn't penalize
    the row. Returns the count of rows that received a definitive
    verification (200 or 404) so the caller can surface progress.
    """
    # v1.19.10: julianday() on both comparison sides — same fix
    # as the v1.19.5 scheduler.py change. Writes to
    # plex_theme_verified_at go through now_iso() (ISO-T with
    # +00:00 timezone, e.g. `2026-05-24T01:32:18+00:00`).
    # SQLite's `datetime('now', '-N days')` produces space-
    # separated format (`2026-05-23 01:32:18`). String comparison
    # is lexicographic: T (0x54) > space (0x20). Any ISO-T
    # timestamp on the SAME date as the boundary always compares
    # "greater" regardless of actual time. Pre-v1.19.10 the
    # verification re-check cadence silently drifted by time-of-
    # day — some rows past their TTL stayed unverified, some
    # within TTL got re-checked prematurely. Latent (no failing
    # test before the v1.19.5 audit caught the class); fixed
    # here as the deferred follow-up flagged in v1.19.5's journal.
    sql = (
        "SELECT rating_key FROM plex_items "
        "WHERE has_theme = 1 "
        "  AND local_theme_file = 0 "
        "  AND ("
        "       plex_theme_verified_ok IS NULL "
        "    OR (plex_theme_verified_ok = 0 "
        "        AND (plex_theme_verified_at IS NULL "
        "             OR julianday(plex_theme_verified_at)"
        "                < julianday('now', '-7 days')))"
        "    OR (plex_theme_verified_ok = 1 "
        "        AND (plex_theme_verified_at IS NULL "
        "             OR julianday(plex_theme_verified_at)"
        "                < julianday('now', '-30 days')))"
        "  ) "
    )
    params: tuple = ()
    if section_id is not None:
        sql += "  AND section_id = ? "
        params = (section_id,)
    with get_conn(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    if not rows:
        return 0
    decided = 0
    # v1.12.116: open a single connection for the whole batch instead
    # of per-row, AND wrap each row's HEAD+UPDATE in its own try/except
    # so a single transient (DB lock timeout, network blip, etc.)
    # doesn't abort verification for the rest of the section.
    # Pre-fix the loop bailed on the first row's exception, plex_enum
    # then marked the whole op 'failed' — a 100-row library lost all
    # verification progress because one row hit a 5s lock.
    with get_conn(db_path) as conn:
        for row in rows:
            if cancel_check():
                from .worker import _JobCancelled
                raise _JobCancelled()
            rk = row["rating_key"]
            try:
                result = client.verify_theme_claim(rk)
                if result is None:
                    # Transient — leave verified_ok NULL so we retry
                    # next enum (or after the v1.12.115 TTL).
                    continue
                conn.execute(
                    "UPDATE plex_items SET plex_theme_verified_at = ?, "
                    "                      plex_theme_verified_ok = ? "
                    "WHERE rating_key = ?",
                    (now_iso(), 1 if result else 0, rk),
                )
                decided += 1
            except Exception as e:
                # v1.14.54: bumped log.debug → log.warning so
                # persistent verification failures actually surface
                # to operators. The v1.12.116 broad `except` was
                # intended to catch TRANSIENT errors (network blip,
                # Plex returning 5xx mid-batch) — but DEBUG level
                # meant any genuine programmer error (AttributeError
                # from a misconfigured PlexClient, library bug,
                # etc.) silently DEBUG-logged and verification did
                # ~0 work while SRC=P stayed optimistic forever.
                # WARNING surfaces the persistent case; transient
                # noise stays manageable since each row is logged at
                # most once per sweep.
                log.warning("verify_theme_claim row %s failed: %s", rk, e)
                continue
    return decided


def resolve_theme_ids(
    db_path: Path,
    *,
    chunk_size: int = 500,
    section_id: str | None = None,
    cancel_check=lambda: False,
    progress_op_id: str | None = None,
    collect_newly_linked: list[str] | None = None,
) -> int:
    """Bulk-populate plex_items.theme_id for every row whose match
    against themes can be resolved by tmdb_id, imdb_id, or
    (title_norm + year). Called at the end of plex_enum and sync so
    the cached column stays fresh.

    v1.21.48: if `collect_newly_linked` is provided, every rating_key
    whose theme_id transitions NULL→set during this run is appended to
    it. That transition is the REAL "row just became downloadable"
    event — the plex_enum caller fires the auto-download/notify on it,
    closing the gap where a pre-existing item linked late (guid resolved
    this enum, or a theme published after the item was enumerated) was
    never in `new_item_rks`. Off by default (recovery/sync callers don't
    pass it → zero extra queries).

    v1.11.35: chunked by rating_key in transactions of `chunk_size`
    rows so the writer lock releases between chunks. Pre-fix the
    single bulk UPDATE with the per-row correlated subquery held
    BEGIN IMMEDIATE for 30-60s on a 4K-item plex_items, blocking
    the auth middleware's session-touch UPDATE long enough to fire
    'database is locked' and 500 the user's request mid-sync.

    v1.12.14: serialized via _resolve_theme_ids_lock. Pre-fix two
    concurrent callers (sync's post-pass + plex_enum's post-pass on
    the long-job thread, or two plex_enums after a settings change)
    could both enter the loop. SQLite's BEGIN IMMEDIATE serializes
    each chunk transaction at the DB level, but the Python-side
    pagination (offset += chunk_size based on a row_count captured
    before either caller started) didn't coordinate — the second
    caller could re-walk rows the first already updated, doubling
    the wall-clock cost and the writer-lock pressure. The lock
    forces serial execution; the second caller's pass is then
    idempotent (no-op for already-resolved rows + picks up any new
    inserts that arrived while the first ran).

    Returns the total number of plex_items rows whose theme_id was
    rewritten across all chunks.
    """
    with _resolve_theme_ids_lock:
        return _resolve_theme_ids_impl(
            db_path, chunk_size=chunk_size,
            section_id=section_id, cancel_check=cancel_check,
            progress_op_id=progress_op_id,
            collect_newly_linked=collect_newly_linked,
        )


_resolve_theme_ids_lock = threading.Lock()


def _resolve_theme_ids_impl(
    db_path: Path,
    *,
    chunk_size: int = 500,
    section_id: str | None = None,
    progress_op_id: str | None = None,
    cancel_check=lambda: False,
    collect_newly_linked: list[str] | None = None,
) -> int:
    # v1.12.18: optional section_id scope. When set, restrict the
    # resolve pass to rating_keys in that section — relevant for
    # the per-section refresh path where we know exactly which rows
    # we just touched and don't need to re-walk the whole table.
    scope_clause = ""
    scope_params: tuple = ()
    if section_id is not None:
        scope_clause = "AND section_id = ?"
        scope_params = (section_id,)
    # v1.24.30: backfill themes.title_norm BEFORE the title-match passes below.
    # 4 of the 5 orphan-creation paths (adopt ×2, bulk import, the collection
    # SET-URL at api.py:12554) never stamped title_norm — only the v1.24.x movie
    # SET-URL path did — so ~340 plex_orphan themes carried title_norm=NULL and
    # could NEVER match the v1.24.25/.27 title re-link passes (NULL = anything is
    # false in SQL), leaving them silently un-recoverable on a Plex delete+re-add.
    # Stamp it here, the single chokepoint that CONSUMES title_norm, so every
    # refresh self-heals the whole cohort + any future NULL with no restart.
    # normalize_title is Python (can't be a SQL trigger). Idempotent: touches
    # only NULL rows (~0 after the first sweep). Defensive per class-9 — a broken
    # normalize must not take down resolve.
    try:
        from .normalize import normalize_title
        with get_conn(db_path) as conn:
            # v1.24.37 (review #8): scope the probe to plex_orphan rows so it
            # rides idx_themes_orphan (partial index on upstream_source) instead
            # of full-scanning ~50K themes EVERY resolve — this runs at the end of
            # every enum AND every sync. The NULL-title_norm cohort is ENTIRELY
            # plex_orphan: real TDB themes get title_norm stamped by the sync
            # importer, only the orphan-creation paths historically left it NULL.
            # Still self-heals any FUTURE orphan NULL with no restart (the cohort
            # the v1.24.25/.27 title re-link consumes). EXPLAIN: SCAN → SEARCH.
            tn_nulls = conn.execute(
                "SELECT id, title FROM themes "
                "WHERE upstream_source = 'plex_orphan' AND title_norm IS NULL "
                "  AND title IS NOT NULL AND title != ''"
            ).fetchall()
            if tn_nulls:
                with transaction(conn):
                    for r in tn_nulls:
                        conn.execute(
                            "UPDATE themes SET title_norm = ? WHERE id = ?",
                            (normalize_title(r["title"] or ""), r["id"]))
                log.info("resolve_theme_ids: backfilled title_norm on %d theme(s) "
                         "— orphan-creation paths historically left it NULL, "
                         "blocking the title-based re-link", len(tn_nulls))
    except Exception as e:  # noqa: BLE001
        log.warning("resolve_theme_ids: title_norm backfill skipped: %s", e)
    # v1.12.22: split the unified OR'd UPDATE into three single-
    # condition statements. Pre-fix the combined query had:
    #   WHERE (tmdb_id match) OR (imdb_id match) OR (title_norm+year match)
    # SQLite's OR optimization didn't reliably use idx_themes_title_norm
    # on the third branch when paired with the upstream_source filter,
    # so the title-fallback path went full-scan against themes
    # (~10K plex_items rows × ~50K themes rows × full scan = ~30s
    # per 500-row chunk on a populated install). Splitting lets each
    # UPDATE use its own dedicated index cleanly:
    #   1) tmdb_id    → themes PK (media_type, tmdb_id)
    #   2) imdb_id    → idx_themes_imdb
    #   3) title+year → idx_themes_title_norm
    # Each UPDATE only sets theme_id where it's currently NULL or
    # would be improved (orphan → real). Order matters: 1 first
    # (preferred), then 2 (orphan match), then 3 (last-resort title
    # fallback).
    # v1.18.16: COALESCE the subquery against the existing theme_id
    # so a no-match doesn't NUKE prior linkage. Pre-fix: when an
    # orphan-promoted row had pi.theme_id stamped by /manual-url
    # (api.py:9713) and the next plex_enum re-ran sql_tmdb against
    # it, the subquery filtered `upstream_source != 'plex_orphan'`
    # and returned NULL (because the only matching themes row was
    # the synthetic orphan). The UPDATE then wrote NULL to
    # theme_id, breaking the JOIN chain so the library SQL fell
    # through to SRC='P' (Plex agent) and the PL pill rendered as
    # awaiting placement. the user's symptom on collections (101
    # Dalmatians, Willy Wonka): SRC=P + PL=amber after a
    # successful SET URL + API upload, despite the placements row
    # being correctly inserted with placement_kind='plex_upload'.
    # COALESCE preserves the orphan linkage when no real TDB theme
    # exists yet; a later sync that imports the real TDB theme will
    # still promote the row because the subquery WILL return that
    # row's id (and COALESCE picks the non-NULL value).
    sql_tmdb = f"""
        UPDATE plex_items SET theme_id = COALESCE((
            SELECT t.id FROM themes t
            WHERE t.media_type = (CASE plex_items.media_type
                                       WHEN 'show' THEN 'tv'
                                       ELSE plex_items.media_type END)
              AND t.tmdb_id = plex_items.guid_tmdb
              AND t.upstream_source != 'plex_orphan'
            ORDER BY t.id DESC
            LIMIT 1
        ), theme_id)
        WHERE plex_items.guid_tmdb IS NOT NULL
          AND rating_key IN (
              SELECT rating_key FROM plex_items
              WHERE 1=1 {scope_clause}
              ORDER BY rating_key
              LIMIT ? OFFSET ?
          )
    """
    # v1.22.47 (the user's orphan-with-imdb-match question): match a REAL
    # ThemerrDB theme by imdb_id. Pre-fix the ONLY imdb pass (sql_imdb below)
    # filtered `upstream_source = 'plex_orphan'`, so it re-bonded a row to its
    # own SYNTHETIC orphan theme but NEVER matched a real TDB theme by imdb —
    # even though _upsert_theme stores themes.imdb_id (indexed via
    # idx_themes_imdb) and ThemerrDB is keyed by imdb/<id>.json. A Plex row with
    # only an imdb GUID (guid_tmdb NULL — older IMDB-agent libraries, or items
    # Plex only resolved to imdb) whose real TDB theme exists then fell through
    # to the fragile title_norm+year fallback and showed as orphan/no-tdb when
    # the title/year didn't align. This pass closes that: imdb is a PRECISE key
    # (lower mis-match risk than title+year), so it runs after sql_tmdb (the
    # canonical key) but BEFORE the orphan re-bond + the title fallback. Gated
    # on theme_id IS NULL — only helps UNLINKED rows, never overwrites a link.
    sql_imdb_real = f"""
        UPDATE plex_items SET theme_id = (
            SELECT t.id FROM themes t
            WHERE t.media_type = (CASE plex_items.media_type
                                       WHEN 'show' THEN 'tv'
                                       ELSE plex_items.media_type END)
              AND t.imdb_id = plex_items.guid_imdb
              AND t.upstream_source != 'plex_orphan'
            ORDER BY t.id DESC
            LIMIT 1
        )
        WHERE plex_items.theme_id IS NULL
          AND plex_items.guid_imdb IS NOT NULL
          AND rating_key IN (
              SELECT rating_key FROM plex_items
              WHERE 1=1 {scope_clause}
              ORDER BY rating_key
              LIMIT ? OFFSET ?
          )
    """
    sql_imdb = f"""
        UPDATE plex_items SET theme_id = (
            SELECT t.id FROM themes t
            WHERE t.media_type = (CASE plex_items.media_type
                                       WHEN 'show' THEN 'tv'
                                       ELSE plex_items.media_type END)
              AND t.imdb_id = plex_items.guid_imdb
              AND t.upstream_source = 'plex_orphan'
            ORDER BY t.id DESC
            LIMIT 1
        )
        WHERE plex_items.theme_id IS NULL
          AND plex_items.guid_imdb IS NOT NULL
          AND rating_key IN (
              SELECT rating_key FROM plex_items
              WHERE 1=1 {scope_clause}
              ORDER BY rating_key
              LIMIT ? OFFSET ?
          )
    """
    sql_title = f"""
        UPDATE plex_items SET theme_id = (
            SELECT t.id FROM themes t
            WHERE t.media_type = (CASE plex_items.media_type
                                       WHEN 'show' THEN 'tv'
                                       ELSE plex_items.media_type END)
              AND t.title_norm = plex_items.title_norm
              AND t.year = plex_items.year
              AND t.upstream_source != 'plex_orphan'
            ORDER BY t.id DESC
            LIMIT 1
        )
        WHERE plex_items.theme_id IS NULL
          AND plex_items.title_norm IS NOT NULL
          AND plex_items.year IS NOT NULL
          AND rating_key IN (
              SELECT rating_key FROM plex_items
              WHERE 1=1 {scope_clause}
              ORDER BY rating_key
              LIMIT ? OFFSET ?
          )
    """
    # v1.18.2: collection-specific resolve. Collections don't carry
    # a release year (the themerr-plex audited code + Plex API
    # confirm: collection metadata has no `year` attribute), so the
    # standard sql_title pass — which requires `t.year = pi.year`
    # AND `pi.year IS NOT NULL` — never fires for them. Result:
    # collection rows enumerated by plex_enum but whose Plex
    # metadata lacked a tmdb:// Guid (common on libraries using the
    # legacy Plex Movie Agent vs the new Plex agent) silently stayed
    # at theme_id=NULL even after sync added the matching TDB
    # record. UI then renders no_tdb / P / – with no connection to
    # the new themes row.
    #
    # Match on title_norm alone, scoped to media_type='collection'
    # on BOTH sides + upstream_source != 'plex_orphan' to avoid
    # bonding to a previously-orphan-promoted row. Title normalization
    # (normalize_title) already strips case + punctuation so
    # "Harry Potter Collection" and "Harry potter collection" match
    # — the same logic the movies/tv title-fallback relies on. The
    # year-free shape is a deliberate narrow: applies ONLY to
    # collection rows; the movie/tv title pass continues to require
    # year to disambiguate same-title remakes (the user's prior
    # "Wonka 1971 vs 2023" concern).
    sql_collection_title = f"""
        UPDATE plex_items SET theme_id = (
            SELECT t.id FROM themes t
            WHERE t.media_type = 'collection'
              AND t.title_norm = plex_items.title_norm
              AND t.upstream_source != 'plex_orphan'
            ORDER BY t.id DESC
            LIMIT 1
        )
        WHERE plex_items.theme_id IS NULL
          AND plex_items.media_type = 'collection'
          AND plex_items.title_norm IS NOT NULL
          AND plex_items.title_norm != ''
          AND rating_key IN (
              SELECT rating_key FROM plex_items
              WHERE 1=1 {scope_clause}
              ORDER BY rating_key
              LIMIT ? OFFSET ?
          )
    """
    # v1.24.25: title+year → ORPHAN re-bond — the missing counterpart of the
    # sql_imdb orphan pass. A non-TDB movie themed via SET URL (synthetic
    # plex_orphan theme, negative tmdb_id) loses its theme_id link when Plex
    # deletes + re-adds the item (new rating_key). The only orphan re-link
    # (sql_imdb) needs a guid_imdb, so a guid-less item (stage musical, an
    # IMDB-agentless library) stayed orphaned forever — its surviving canonical
    # invisible, no DL to restore (the user's "Avenue Q" re-add repro). Runs LAST,
    # after every REAL-theme pass, so a real match always wins first; this only
    # catches rows no real theme claimed, by the same title_norm+year key
    # sql_title uses (year still disambiguates remakes). theme_id IS NULL so it
    # never overwrites a live link.
    sql_title_orphan = f"""
        UPDATE plex_items SET theme_id = (
            SELECT t.id FROM themes t
            WHERE t.media_type = (CASE plex_items.media_type
                                       WHEN 'show' THEN 'tv'
                                       ELSE plex_items.media_type END)
              AND t.title_norm = plex_items.title_norm
              AND t.year = plex_items.year
              AND t.upstream_source = 'plex_orphan'
            ORDER BY t.id DESC
            LIMIT 1
        )
        WHERE plex_items.theme_id IS NULL
          AND plex_items.title_norm IS NOT NULL
          AND plex_items.year IS NOT NULL
          AND rating_key IN (
              SELECT rating_key FROM plex_items
              WHERE 1=1 {scope_clause}
              ORDER BY rating_key
              LIMIT ? OFFSET ?
          )
    """
    # v1.24.27: year-LESS orphan re-bond — the counterpart of sql_title_orphan
    # for when Plex LOSES the year on a delete+re-add. the user's Avenue Q came
    # back with year='' (folder "Avenue Q ()"), so sql_title_orphan's
    # `t.year = plex_items.year` couldn't match the orphan's year='2003' (and
    # the row had no guids) → stranded at theme_id=NULL, canonical+override
    # invisible. Falls back to title_norm alone, but ONLY when the row has NO
    # usable year (NULL or '') — a row that DOES carry a year still requires the
    # year to match via sql_title_orphan (preserves the Wonka 1971-vs-2023
    # remake guard) — AND only when EXACTLY ONE plex_orphan theme shares the
    # title (the v1.24.18 exactly-one-candidate gate: 2+ same-title orphans are
    # ambiguous without a year, so leave them for the operator). Runs LAST,
    # after the year-matching orphan pass, so a precise match always wins first.
    sql_title_orphan_yearless = f"""
        UPDATE plex_items SET theme_id = (
            SELECT t.id FROM themes t
            WHERE t.media_type = (CASE plex_items.media_type
                                       WHEN 'show' THEN 'tv'
                                       ELSE plex_items.media_type END)
              AND t.title_norm = plex_items.title_norm
              AND t.upstream_source = 'plex_orphan'
            ORDER BY t.id DESC
            LIMIT 1
        )
        WHERE plex_items.theme_id IS NULL
          AND plex_items.title_norm IS NOT NULL
          AND plex_items.title_norm != ''
          AND (plex_items.year IS NULL OR plex_items.year = '')
          AND (
              SELECT COUNT(*) FROM themes t2
              WHERE t2.media_type = (CASE plex_items.media_type
                                          WHEN 'show' THEN 'tv'
                                          ELSE plex_items.media_type END)
                AND t2.title_norm = plex_items.title_norm
                AND t2.upstream_source = 'plex_orphan'
          ) = 1
          AND rating_key IN (
              SELECT rating_key FROM plex_items
              WHERE 1=1 {scope_clause}
              ORDER BY rating_key
              LIMIT ? OFFSET ?
          )
    """
    import time as _time
    total = 0
    offset = 0
    with get_conn(db_path) as conn:
        count_sql = "SELECT COUNT(*) FROM plex_items"
        if section_id is not None:
            count_sql += " WHERE section_id = ?"
        row_count = conn.execute(count_sql, scope_params).fetchone()[0]
    if row_count == 0:
        return 0
    log.info(
        "resolve_theme_ids: starting on %d plex_items rows%s "
        "(chunk_size=%d)",
        row_count,
        f" (section {section_id})" if section_id else "",
        chunk_size,
    )
    progress_t0 = _time.monotonic()
    # v1.12.127: real-progress emit cadence into op_progress. Pre-fix
    # the resolve stage had stage_total=0 set by the caller, so the
    # ops drawer rendered an indeterminate shimmer bar — accurate
    # ("we don't know") but less informative than counting actual
    # rows progressed. With the row_count known up front we can
    # surface a real % via the same chunk-driven loop the log
    # cadence already uses. Time-throttled so SQLite write volume
    # stays bounded.
    if progress_op_id is not None:
        op_progress.update_progress(
            db_path, progress_op_id,
            stage_current=0, stage_total=row_count,
        )
    last_ui_emit = _time.monotonic()
    while offset < row_count:
        # v1.12.18: cancel-check between chunks so a stuck resolve
        # can be interrupted from the /logs CANCEL button. Also
        # guards against the user kicking off a sync mid-resolve;
        # the new sync's resolve waits on the mutex, but the
        # current one stays interruptible.
        if cancel_check():
            from .worker import _JobCancelled
            log.info("resolve_theme_ids: cancelled at %d/%d rows",
                     offset, row_count)
            raise _JobCancelled()
        # v1.12.22: run all three index-targeted UPDATEs per chunk
        # in one transaction. Most rows match by tmdb_id (PK index)
        # so sql_tmdb does the bulk of the work; sql_imdb covers
        # plex_orphan rows; sql_title is a last-resort fallback for
        # rows missing both GUIDs. Each uses its dedicated index;
        # combined wall-time is ~30x faster than the old OR'd query
        # that fell back to a full themes scan on the title branch.
        chunk_params = scope_params + (chunk_size, offset)
        with get_conn(db_path) as conn, transaction(conn):
            # v1.21.48: snapshot this chunk's currently-unlinked rows so
            # we can report which the UPDATEs below newly link (theme_id
            # NULL→set) — the actual auto-download trigger event. Only
            # when a caller asked (plex_enum); recovery/sync skip it.
            null_before: list[str] = []
            if collect_newly_linked is not None:
                null_before = [
                    r[0] for r in conn.execute(
                        "SELECT rating_key FROM plex_items "
                        "WHERE theme_id IS NULL AND rating_key IN ("
                        "  SELECT rating_key FROM plex_items WHERE 1=1 "
                        f" {scope_clause} ORDER BY rating_key "
                        "  LIMIT ? OFFSET ?)",
                        chunk_params,
                    ).fetchall()
                ]
            cur = conn.execute(sql_tmdb, chunk_params)
            total += cur.rowcount
            # v1.22.47: real-theme imdb match BEFORE the orphan re-bond + title
            # fallback (imdb is a precise key — see sql_imdb_real comment).
            cur = conn.execute(sql_imdb_real, chunk_params)
            total += cur.rowcount
            cur = conn.execute(sql_imdb, chunk_params)
            total += cur.rowcount
            cur = conn.execute(sql_title, chunk_params)
            total += cur.rowcount
            # v1.18.2: fourth pass — collection-specific title match
            # (year-less). Runs after the standard three so it only
            # picks up rows the others left as theme_id=NULL —
            # mostly collection rows lacking a tmdb:// Guid in
            # Plex's metadata.
            cur = conn.execute(sql_collection_title, chunk_params)
            total += cur.rowcount
            # v1.24.25: LAST pass — title+year orphan re-bond, so a re-added
            # non-TDB SET-URL movie re-links to its surviving orphan theme. Runs
            # after every real-theme pass so a real match always wins first.
            cur = conn.execute(sql_title_orphan, chunk_params)
            total += cur.rowcount
            # v1.24.27: year-LESS orphan re-bond (exactly-one-candidate) for a
            # row Plex re-added without a year — see sql_title_orphan_yearless.
            cur = conn.execute(sql_title_orphan_yearless, chunk_params)
            total += cur.rowcount
            # v1.21.48: of the rows that were NULL before, which are now
            # linked? Same transaction, so this sees the UPDATEs above.
            if null_before:
                qs = ",".join("?" * len(null_before))
                collect_newly_linked.extend(
                    r[0] for r in conn.execute(
                        "SELECT rating_key FROM plex_items "
                        "WHERE theme_id IS NOT NULL "
                        f"AND rating_key IN ({qs})",
                        null_before,
                    ).fetchall()
                )
        offset += chunk_size
        # v1.12.18: progress log every 5s so a stuck resolve is
        # visible in the logs instead of silent. Pre-fix the only
        # log line landed at completion, which made it impossible
        # to tell whether a "running" plex_enum job was making
        # progress or wedged.
        elapsed = _time.monotonic() - progress_t0
        if elapsed > 5.0:
            log.info("resolve_theme_ids: progress %d/%d (%.1fs)",
                     offset, row_count, elapsed)
            progress_t0 = _time.monotonic()
        # v1.12.127: op_progress emit at ~300ms cadence so the
        # ops-drawer bar ticks smoothly without saturating the
        # writer lock. Capped at offset (not min(offset, row_count))
        # because offset < row_count is the loop invariant — final
        # value is row_count.
        if (progress_op_id is not None
                and (_time.monotonic() - last_ui_emit) > 0.3):
            last_ui_emit = _time.monotonic()
            op_progress.update_progress(
                db_path, progress_op_id,
                stage_current=min(offset, row_count),
                stage_total=row_count,
            )
        # v1.11.54: yield ~250ms between chunks (was 50ms). Each chunk
        # holds BEGIN IMMEDIATE for ~1-2s on a 15K-row plex_items;
        # 50ms gap kept the writer at 95%+ duty cycle and starved
        # other workers' claim attempts. 250ms keeps the resolve
        # progressing without monopolizing the lock.
        _time.sleep(0.25)
    log.info("resolve_theme_ids: scanned %d plex_items rows (chunk_size=%d)",
             total, chunk_size)

    # v1.16.0: auto-incremental TVDB-bridge. After the standard 3
    # paths have settled, if a TMDB key is configured AND the
    # operator has run the manual REBUILD at least once (so
    # last_tvdb_bridge_at is set in runtime_settings), bridge ONLY
    # rows that have appeared since that timestamp. Bounded by
    # max_rows=100 so a plex_enum doesn't grow unbounded API calls
    # — new shows get themed incrementally, the historical backlog
    # stays the manual REBUILD's job.
    # Skips entirely when no last-run timestamp exists — the user
    # hasn't opted-in yet; auto-running the first 2000+ lookups in
    # the middle of a plex_enum would be a surprise latency spike.
    try:
        from . import runtime as _rt
        last_bridge_run_at = _rt.get_runtime_text(
            db_path, "last_tvdb_bridge_at", "")
        if last_bridge_run_at:
            # Lazy import to avoid circular deps + skip cost when
            # the gate above already exits.
            # v1.18.55: fixed `.config` → `..config`. The wrong
            # relative-import landed in v1.16.0 — `.config` resolves
            # to `app.core.config` which doesn't exist; the real
            # module is `app.config` (one level up). Every
            # plex_enum since v1.16.0 raised ModuleNotFoundError
            # here, was swallowed by the outer except, and silently
            # logged "auto-incremental TVDB bridge failed: No module
            # named 'app.core.config'". worker.py:27 + scheduler.py
            # :16 already use the correct `..config` pattern.
            # Class-9 silent-defensive-catch (CLAUDE.md class 9):
            # the breadcrumb log was there, but `from .config`
            # ALWAYS raised on this path so the auto-incremental
            # bridge had been dead for ~10 weeks (v1.16.0 → v1.18.54).
            from ..config import Settings
            from .tmdb import TMDBClient
            settings = Settings()
            if settings.tmdb_api_key:
                tmdb = TMDBClient(settings.tmdb_api_key, db_path)
                bridge_tvdb_to_tmdb(
                    db_path, tmdb,
                    section_id=section_id,
                    since_iso=last_bridge_run_at,
                    max_rows=100,
                    progress_op_id=progress_op_id,
                    cancel_check=cancel_check,
                )
    except Exception as e:
        # Auto-incremental bridge is best-effort — if it fails,
        # log and move on so plex_enum still completes.
        # v1.18.55: log line uses the TVDB name to match the UI
        # label (settings.html shows "// TVDB BRIDGE"). The
        # internal op_progress.kind stays 'tvdb_bridge' for
        # schema continuity (db.py v52 CHECK widening) — only
        # operator-visible surfaces get the rename.
        log.warning("auto-incremental TVDB bridge failed: %s", e)

    return total


# v1.16.0: TVDB-bridge — fills the linkage gap for plex_items rows
# whose Plex agent gave only a guid_tvdb (no guid_tmdb), via a
# TMDB API lookup (`/find/{tvdb_id}?external_source=tvdb_id`). The
# v1.15.143 diagnostic on the user's library reported 2054 such
# stranded rows; most are TV shows from Plex's default TV Series
# agent which prefers TheTVDB GUIDs over TMDB ones. After bridging,
# those rows participate fully in motif's TDB-tracked actions
# (DOWNLOAD TDB BACKUP, REPLACE TDB, REVERT, etc.).
#
# Mechanism:
#   1. Select plex_items rows where theme_id IS NULL AND guid_tvdb
#      IS NOT NULL AND has_theme = 1 AND section is included
#      (optionally: AND first_seen_at > since_iso, for incremental)
#   2. For each, call tmdb.lookup_by_tvdb(guid_tvdb, media_type).
#      Cached via the existing tvdb_lookup_cache 7-day TTL — second
#      run is mostly cache hits.
#   3. If TMDB returns a tmdb_id, check if motif's themes table has
#      a row for (media_type, tmdb_id). If yes, UPDATE plex_items
#      SET theme_id = t.id WHERE rating_key = ?.
#   4. Per-row stats + op_progress emit + cancel check.

def bridge_tvdb_to_tmdb(
    db_path: Path,
    tmdb_client,
    *,
    section_id: str | None = None,
    since_iso: str | None = None,
    max_rows: int | None = None,
    progress_op_id: str | None = None,
    cancel_check=lambda: False,
) -> dict:
    """v1.16.0: walk plex_items rows that have guid_tvdb but no
    theme_id and try to link them via a TMDB external-id lookup.

    Args:
      db_path: motif DB
      tmdb_client: TMDBClient instance (must be enabled)
      section_id: optional, restrict to one Plex section
      since_iso: optional ISO timestamp; restrict to rows whose
        first_seen_at > this (auto-incremental mode — bridge only
        rows that appeared since the previous bridge run)
      max_rows: optional cap on rows processed (auto-incremental
        bounds plex_enum's per-pass cost; manual rebuild leaves
        this None)
      progress_op_id: op_progress lifecycle id for live progress
      cancel_check: returns True when user cancelled

    Returns dict with counters: processed, linked, unmappable,
    no_themes_record, errors.
    """
    import time as _time
    if not tmdb_client or not tmdb_client.enabled:
        log.info("bridge_tvdb_to_tmdb: TMDB client disabled — skipping")
        return {"processed": 0, "linked": 0, "unmappable": 0,
                "no_themes_record": 0, "errors": 0}

    # Step 1: enumerate candidates. Single-shot SELECT (lighter than
    # chunked since the size is bounded by stranded count, typically
    # low thousands).
    where_clauses = [
        "pi.theme_id IS NULL",
        "pi.guid_tvdb IS NOT NULL",
        "pi.has_theme = 1",
        "ps.included = 1",
    ]
    params: list = []
    if section_id is not None:
        where_clauses.append("pi.section_id = ?")
        params.append(section_id)
    if since_iso:
        where_clauses.append("pi.first_seen_at > ?")
        params.append(since_iso)
    sql = (
        "SELECT pi.rating_key, pi.media_type, pi.guid_tvdb, "
        "       pi.title, pi.year, pi.section_id "
        "FROM plex_items pi "
        "JOIN plex_sections ps ON ps.section_id = pi.section_id "
        f"WHERE {' AND '.join(where_clauses)} "
        "ORDER BY pi.first_seen_at ASC, pi.rating_key ASC"
    )
    if max_rows:
        sql += f" LIMIT {int(max_rows)}"
    with get_conn(db_path) as conn:
        candidates = conn.execute(sql, params).fetchall()

    total = len(candidates)
    if total == 0:
        log.info("bridge_tvdb_to_tmdb: no candidates to bridge")
        if progress_op_id is not None:
            op_progress.update_progress(
                db_path, progress_op_id,
                stage_current=0, stage_total=0,
                activity="No stranded rows to bridge.",
            )
        return {"processed": 0, "linked": 0, "unmappable": 0,
                "no_themes_record": 0, "errors": 0}

    log.info("bridge_tvdb_to_tmdb: starting on %d candidates%s",
             total, f" (section {section_id})" if section_id else "")
    if progress_op_id is not None:
        op_progress.update_progress(
            db_path, progress_op_id,
            stage_current=0, stage_total=total,
            activity=f"Bridging {total} stranded TVDB rows…",
        )

    processed = 0
    linked = 0
    unmappable = 0       # TMDB has no mapping for this TVDB ID
    no_themes_record = 0  # TMDB resolved but motif's themes table doesn't have it
    errors = 0
    last_ui_emit = _time.monotonic()

    for row in candidates:
        if cancel_check():
            from .worker import _JobCancelled
            log.info(
                "bridge_tvdb_to_tmdb: cancelled at %d/%d "
                "(linked=%d, unmappable=%d, no_record=%d)",
                processed, total, linked, unmappable, no_themes_record,
            )
            raise _JobCancelled()

        rating_key = row["rating_key"]
        plex_media_type = row["media_type"]  # 'show' or 'movie'
        # Convert Plex format to motif format for the themes lookup
        # AND for the TMDB API call (lookup_by_tvdb takes motif
        # format).
        motif_mt = "tv" if plex_media_type == "show" else "movie"
        tvdb_id = row["guid_tvdb"]

        try:
            result = tmdb_client.lookup_by_tvdb(tvdb_id, motif_mt)
        except Exception as e:
            errors += 1
            log.warning(
                "bridge_tvdb_to_tmdb: TMDB lookup failed for "
                "tvdb=%s (%s): %s",
                tvdb_id, motif_mt, e,
            )
            processed += 1
            continue

        if result is None or not result.get("tmdb_id"):
            unmappable += 1
            processed += 1
        else:
            resolved_tmdb = int(result["tmdb_id"])
            # Step 3: check if motif's themes table has a record
            # for this (motif_mt, resolved_tmdb). If yes, link.
            with get_conn(db_path) as conn, transaction(conn):
                theme_row = conn.execute(
                    "SELECT id FROM themes "
                    "WHERE media_type = ? AND tmdb_id = ?",
                    (motif_mt, resolved_tmdb),
                ).fetchone()
                if theme_row is None:
                    no_themes_record += 1
                else:
                    conn.execute(
                        "UPDATE plex_items SET theme_id = ? "
                        "WHERE rating_key = ?",
                        (theme_row["id"], rating_key),
                    )
                    linked += 1
            processed += 1

        # Throttled op_progress update — 300ms cadence so the bar
        # ticks smoothly without saturating the writer lock.
        if (progress_op_id is not None
                and (_time.monotonic() - last_ui_emit) > 0.3):
            last_ui_emit = _time.monotonic()
            op_progress.update_progress(
                db_path, progress_op_id,
                stage_current=processed, stage_total=total,
                activity=(
                    f"{processed}/{total} — linked={linked}, "
                    f"unmappable={unmappable}, "
                    f"no_record={no_themes_record}, "
                    f"errors={errors}"
                ),
            )

    log.info(
        "bridge_tvdb_to_tmdb done: processed=%d linked=%d "
        "unmappable=%d no_themes_record=%d errors=%d",
        processed, linked, unmappable, no_themes_record, errors,
    )
    return {
        "processed": processed,
        "linked": linked,
        "unmappable": unmappable,
        "no_themes_record": no_themes_record,
        "errors": errors,
    }
