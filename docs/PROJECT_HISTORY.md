# motif Project Memory Digest

**Coverage**: v1.4.0 → v1.22.28. §§ 1–25 cover v1.4.0 → v1.19.55 at per-tag
granularity; §§ 26–28 are CONDENSED arc digests for v1.19.56 → v1.22.28 (the
recovery_v55 sunset, the per-edition theme-isolation arc, and the v1.22 edition
data-loss audit batches) — the WHY / bug-classes / patterns, not every tag. The
blow-by-blow for that span lives in `docs/SESSION_JOURNAL.md` (gitignored) and in
`app/__init__.py`'s per-tag `# vX.Y.Z:` history comments.
**Compiled**: Themes extracted by pattern, architecture pivot, recurring bugs, and feature evolution.
**Last digest update**: 2026-06-07 — added §§ 26–28 (condensed) to close the v1.19.56 → v1.22.28 gap. Earlier: 2026-05-19 — § 14 covers the v1.17.1 → v1.17.8 burst (8 patch tags in a single day: audit-cleared rollover → notify phase 2 → twice-iterated design audits). § 13 covers the v1.17.0 Apprise foundation. § 12 closes the v1.14.43 → v1.16.9 gap; § 11 covers v1.16.10–11. § 12 proposed two new bug classes (**T** silent-defensive-catch, **U** browser tab-throttle / poll-cap), both promoted to CLAUDE.md § Recurring bug classes 9 + 10 on 2026-05-18.

## 1. Recurring Bug Classes

### A. Phantom P (Plex agent cache): Stale metadata after deletion
**Pattern**: Item deleted or unmanaged locally, but plex_items.has_theme=1 remains because Plex's internal metadata cache didn't refresh.

**Canonical References**: v1.10.27, v1.10.38, v1.10.28
- Root cause: Plex doesn't immediately clear its own has_theme field when motif deletes the sidecar file.
- Fix pattern: Trigger per-item Plex refresh via HEAD request to refresh the Plex agent's metadata cache after delete/unmanage actions.
- Implementation: `worker._do_refresh` or per-item Plex API refresh call in api.py DELETE/UNMANAGE handlers.
- User signal: Row showed P (Plex-only) when they'd just deleted it; re-scanning or manual REFRESH FROM PLEX cleared it.

### B. UNIQUE constraint violations during orphan promotion
**Pattern**: Sync crashes on `UNIQUE constraint failed: user_overrides.media_type, user_overrides.tmdb_id` when promoting a synthetic orphan to real tmdb_id if stale rows exist at the target ID.

**Canonical References**: v1.11.71, v1.11.73
- Root cause: PRAGMA foreign_keys=OFF is a no-op inside transactions; orphan UPDATE moves rows under targets that already have conflicting UNIQUE children.
- Fix pattern: Before UPDATE, pre-delete conflicting rows at TARGET (media_type, new_tmdb_id) in user_overrides, local_files, placements, pending_updates. Then use PRAGMA defer_foreign_keys=ON (allowed in txns; defers FK checks until COMMIT).
- Implementation: sync.py _upsert_theme orphan-promotion branch + v1.11.71 transaction pragma switch.
- Lesson: defer_foreign_keys resets at txn boundary; no need to re-enable.

### C. Database is locked (SQLite writer contention)
**Pattern**: Worker crashes on `sqlite3.OperationalError 'database is locked'` when multiple writers compete for the single writer lock.

**Canonical References**: v1.11.51
- Root cause: SQLite has one writer at a time; job worker + API updates + sync threads collide.
- Fix pattern: Retry only 'database is locked' errors with exponential backoff (other OperationalError types = schema errors, propagate immediately). v1.11.51 added per-call retry logic.
- Implementation: worker.py _claim_next_job exception handler; checks for 'database is locked' in str(e).
- Workaround: Run with WAL mode + increased timeout; use connection pooling with short max_overflow.

### D. Edition sibling theme propagation (Theatrical vs 4K)
**Pattern**: User downloads theme for "Title" folder, but "Title {4K}" or "Title {edition}" doesn't inherit the hardlink. Row shows DL=off for the sibling.

**Canonical References**: v1.11.0, v1.11.43, v1.11.64, v1.11.65
- Root cause: local_files is keyed by (rating_key, section_id). Different Plex editions get different rating_keys, so hardlink sibling detection missed cross-edition items.
- Fix pattern: When downloading, check for sibling local_files rows BEFORE hitting YouTube. _do_download reads pi.theme_id (the v1.11.43 stamp linking plex_items orphan/real rows). Hardlink from sibling, record sibling inode, mark both sections as satisfied in one download.
- Implementation: worker._do_download sibling-detection loop via _record_local_file; synchronized _safe_link for hardlinks.
- Schema: local_files.section_id (PK component) + plex_items.theme_id (cross-section unifier).

### E. Edition pill rendering on non-edition rows
**Pattern**: Row with imdb_id but no edition shows yellow edition pill incorrectly (e.g., imdb-tt0111161 renders as edition when folder is base "Title (Year)").

**Canonical References**: v1.10.17, v1.10.31
- Root cause: Edition detection was overly broad; any imdb_id inside paren was classified as edition markup.
- Fix pattern: Gate pill on (folder contains explicit {-suffix} AND suffix is in a known-edition list). Base folders with (imdb-ttXXX) markup should NOT render pills. Renamed pill rule to exclude IMDB patterns.
- Implementation: frontend library.html renderLibraryRow edition-pill classifier; added regex exclusion for imdb-pattern rows.

### F. Race between syncWatcher and refreshTopbarStatus polls
**Pattern**: syncWatcher detects sync-in-progress, UI doesn't react for 15s (or 30s in later versions) because refreshTopbarStatus next tick hasn't fired yet.

**Canonical References**: v1.11.23, v1.11.48, v1.11.69
- Root cause: Optimistic updates race with periodic polling. syncWatcher sees state change first; topbar poll happens every 15-30s.
- Fix pattern: After user clicks SYNC, paint topbar optimistically via paintTopbarSyncing(label) IMMEDIATELY, don't wait for /api/stats. refreshTopbarStatus still polls but user sees instant feedback. v1.11.23 added immediate topbar refresh; v1.11.48 added paintTopbarSyncing helper.
- Implementation: Click handler calls paintTopbarSyncing before POST; refreshTopbarStatus polls thereafter to refine state.
- Lesson: Optimistic placeholders mask latency; periodic polls refine state. Both needed.

### G. Stale query results from /api/stats 1s TTL cache
**Pattern**: User approves pending item, PENDING nav dot stays red for 30s because refreshTopbarStatus hits cached stats showing pending_placements > 0.

**Canonical References**: v1.11.69, v1.11.75
- Root cause: /api/stats wraps in 1s TTL cache. After button click, refreshTopbarStatus may read stale cache for up to 1s, then waits for next poll (15-30s).
- Fix pattern: After state-changing action (approve, discard, delete, unmanage), call refreshTopbarStatus with a scheduled delay past the 1s TTL (e.g., 1.1s setTimeout).
- Implementation: Button handlers call refreshTopbarStatus(true) after ~1.1s delay instead of immediately.
- Lesson: Invalidate cache post-action or wait past TTL before re-reading.

### H. Placement folder not found (no_match) for edition-tagged Plex folders
**Pattern**: Place worker can't find Plex folder for "Title {4K}" / "Title {edition}" even though folder exists and was enumerated. Reason='no_match' in /pending.

**Canonical References**: v1.11.65, v1.11.68
- Root cause: place_theme used FolderIndex title+year+edition matching with strict_edition=true (default). For newly-downloaded items, pi.theme_id hadn't resolved yet, fallback to guid_tmdb matching, which then tried FolderIndex which required an exact edition key match in the folder index. Edition-tagged folders not in the index: no match.
- Fix pattern: Extend place_theme to accept cached_folder_path and cached_rk from plex_items. When provided AND reachable, use directly; skip FolderIndex entirely. Always try pi.theme_id JOIN first before guid_tmdb fallback.
- Implementation: worker._do_place SELECT pi.theme_id, pi.folder_path; pass both to place_theme; place_theme uses cached_folder_path if reachable.
- Lesson: Orphan rows need a stable cross-section unifier (theme_id); place worker should use it before falling back to index.

### I. Button text race (QUEUED vs DOWNLOAD)
**Pattern**: User clicks DOWNLOAD, button swaps to "QUEUED" but modal/notification shows different label due to race between DOM update and state sync.

**Canonical References**: v1.11.72, v1.13.14, v1.13.15, v1.13.16, v1.13.18
- Root cause: setSyncButtonState swapped button.textContent mid-click, but rapid subsequent polls could re-render the row, wiping the optimistic label.
- Fix pattern: v1.13.14 dropped the title-cell spinner; DL/PL pulse covers work-in-progress signal. v1.13.15-16 attempted multi-stage label swaps (DOWNLOAD → QUEUED → DOWNLOADING) but became over-complex. v1.13.18 reverted to single-label approach: button stays DL when clicked (pending pill shows progress).
- Implementation: library.html renderLibraryRow DL pill states; button text no longer swaps during flight.
- Lesson: Don't change button text during flight; use adjacent pills (DL, PL) for state. Simpler = fewer races.

### J. Hash-match adoption showing as hash_match even after delete
**Pattern**: User adopts sidecar via hash_match (file IS the same as existing theme), then scan re-runs and shows the same item as hash_match-adoptable again.

**Canonical References**: v1.9.5
- Root cause: Scan found file on disk, stored finding.hash_match=true. User adopted it, which wrote user_overrides + local_files. But scanner didn't clear the finding row; next scan saw the same orphan_resolvable again and re-offered hash_match.
- Fix pattern: scan adoption (adopt_finding branch) now marks finding.decision='lock' (or similar ack), preventing re-offer. Renamed UI from IGNORE → LOCK to make the state explicit.
- Implementation: adopt.py adopt_finding updates finding.decision; scanner._classify skips rows where decision != 'pending'.

### K. Cross-section bleed (title-global mutation across sibling sections)
**Pattern**: User performs a section-scoped action (4K row only) but the action touches title-global state, dismissing the same axis on standard / anime / other sections of the same title.

**Canonical References**: v1.13.32, v1.13.35, v1.13.47, v1.13.50, v1.13.54
- Root cause family: motif's schema mixes title-global rows (`themes.failure_acked_at`, `pending_updates` global '' fallback) with per-section rows (`local_files`, `placements`, `user_overrides` since v27). Endpoints that should be per-section sometimes wrote to the title-global axis.
- Examples:
  - `recovery-options` lookup ignored `section_id` → wrong "RESOLVED VIA ADOPT" tile on standard when only 4K had the local source. Fixed v1.13.35.
  - `adopt-and-ack` / `purge-and-ack` chains called `/clear-failure` without `section_id` → cleared the global ack flag for every section. Fixed v1.13.54 by adding section_failure_acks table + section_id param.
  - `redl` recovery action posted `/redownload` without `section_id` → fan-out re-download to every owning section. Fixed v1.13.50.
  - `/adopt-from-plex` required `mismatch_state` which didn't exist for failed-download rows. Rewired `adopt-and-ack` to `/plex_items/{rk}/adopt-sidecar` (section-aware via rating_key). Fixed v1.13.47.
- Fix pattern: any endpoint operating on `(media_type, tmdb_id)` that has section context available MUST accept `section_id` as a query param and scope the SQL accordingly. Library SQL reads per-section first, '' global fallback. Stats / topbar counts derived from per-section data are correctly section-aggregated by default.
- Lesson: per-section design has been the right call since v18; cross-section bleed bugs are bugs in code that forgot to thread `section_id` through, not the schema's fault.

### L. Phantom +P composite signal lost when motif places over P
**Pattern**: User has a row that's SRC=P (Plex serves cloud / themerr-plex / Plex Pass theme). User clicks REPLACE TDB → motif downloads + places its own sidecar. After: row reads bare T with no +P yellow dot, even though Plex's independent theme is still being served.

**Canonical References**: v1.13.31, v1.13.34, v1.13.38, v1.13.55
- Root cause: `plex_items.plex_independent_theme` (added in schema v39 / v1.13.38) is set by `plex_enum`'s batch loop only when sidecar=0 (the `has_theme=1 + local_theme_file=0` observation window). Once motif places, `local_theme_file` flips to 1 — the window closes, indep_observation becomes None, COALESCE preserves the prior value.
- If the column was NULL when the place ran (no plex_enum had observed the row in the right state since the column was added), the value stays NULL forever and the +P composite dot never lights up, even though Plex is still serving its own theme alongside motif's sidecar.
- Fix pattern: pre-place +P capture in `worker._do_place`. Just before invoking `place_theme()`, check `pi.has_theme=1 AND pi.local_theme_file=0 AND pi.plex_independent_theme IS NULL` and stamp `plex_independent_theme=1`. Guard ensures we never overwrite a definitive prior observation. Added v1.13.55.
- Backfill alternative: `// REPROBE PLEX THEMES` admin tool (v1.13.40) does the same observation across the entire library via byte-prefix comparison.

### M. urls_match phantom UPD on no-canonical SET URL
**Pattern**: User does SET URL with the same YouTube URL TDB has (e.g., to test a download after VPN/location change). Download still fails. Topbar gains a phantom `1 UPD`. User can't ACCEPT (would create a T row pointing at nothing) or KEEP CURRENT (nothing to keep).

**Canonical References**: v1.13.55
- Root cause: `api_manual_url` unconditionally inserted a synthetic `urls_match` `pending_updates` row when override URL == current TDB URL — the v1.12.62 "convert U → T" prompt. Designed for the case where there's already a working canonical to convert. On rows with no canonical (failed downloads / never downloaded), the prompt has nothing to act on.
- Fix pattern: gate the synthetic insert on a canonical-exists check (`SELECT 1 FROM local_files WHERE ... AND file_path IS NOT NULL`). Stale-pending DELETE still runs unconditionally so prior sync-driven entries get cleaned up.
- Lesson: synthetic state-change rows must verify the user has something to act on before surfacing the prompt — pending_updates without a working canonical is a dead-end for the user.

### N. /api/stats 1s TTL post-action sweep gaps
**Pattern**: User performs a state-changing action (bulk ack, bulk download, info-card adopt). Topbar pill (FAIL / UPD / DROP) clings to its stale value for 15-30s until the next slow poll cycle.

**Canonical References**: v1.11.69 (original fix), v1.13.51, v1.13.56
- Root cause: `/api/stats` has a 1s TTL cache. The convention since v1.11.69 has been to schedule `setTimeout(refreshTopbarStatus, 1100)` after every state-changing action so the next read lands past the TTL. Bulk handlers that landed during v1.13.x feature work consistently missed this scheduling.
- Fix pattern: post-action helpers (`closeAndReload` in info card, every bulk button's success callback) include the 1100ms `refreshTopbarStatus` schedule. v1.13.51 added it to closeAndReload; v1.13.56 swept the five remaining bulk handlers (ack-selected, download-selected, adopt-selected, accept-all-updates, decline-all-updates).
- Lesson: every new state-changing button must wire `setTimeout(refreshTopbarStatus, 1100)` or the topbar lies. CLAUDE.md flags this as a recurring bug class.

### O. Topbar pill click-through filters that don't match the pill's count semantics
**Pattern**: Topbar shows e.g. "3 FAIL". Click → land on a page showing 8+ matches with only 3 actually contributing to the count. User confused "where are the other 5?".

**Canonical References**: v1.13.57
- Root cause: pill counts use the unacked-only `?status=failures` filter SQL (`failure_kind IS NOT NULL AND failure_acked_at IS NULL AND sfa.acked_at IS NULL`). But the click-through link routed to `?tdb_pills=dead` — a different filter that includes ACKED dead-TDB rows.
- Fix pattern: the click-through must route to the exact filter that produced the count. v1.13.57 re-pointed three links (topbar FAIL pill, per-section coverage `failures` cell, failure breakdown insight chart) from `?tdb_pills=dead` to `?status=failures`.
- Lesson: when a pill renders a count, the click-through and the count must run the same SQL filter. Otherwise the page's row count contradicts the badge.

### P. Mirror-principle drift: count + breakdown + filter for the same surface drift apart
**Pattern**: A topbar pill renders count N. Click-through cycles through the (tab, fourk) breakdown and lands on a library page that shows zero matches. Or shows the wrong tab+variant first. Three SQL surfaces — the COUNT, the per-tab BREAKDOWN, the library FILTER — all derive from the same conceptual predicate but each has its own copy and they drift independently.

**Canonical References**: v1.13.84, v1.13.85, v1.13.86, v1.13.88 (four instances in two weeks)
- v1.13.84: UPD breakdown only required `pu.decision='pending'` while the library filter required EXISTS pending + SRC != '-'. Rows with src='-' (TDB-dropped) counted in breakdown but were hidden by filter. Same drift on FAIL: breakdown limited to 4 specific kinds while filter accepted any non-null `failure_kind`.
- v1.13.85: `/api/library` COUNT had a fast-path that skipped the themes/sfa joins; `no_pills` check forgot `attn_pills`, so attn-only requests went to the slim FROM and SQLite silently returned the unfiltered section total.
- v1.13.86: filter widened with has-something + URL-diff predicates to mirror the count exactly.
- v1.13.88: v1.13.86 tightened the filter but breakdown wasn't re-aligned (fourth iteration of the same shape).
- Fix pattern: any (count, breakdown, filter) trio for the same surface needs pinned-by-test agreement. Static guards on the production SQL ensure all three surfaces share the predicate. v1.14.8 carried the same lesson into `failures_total` (rewritten from a title-global COUNT against themes to a per-(title, section) count joining sfa, mirroring `failure_tab_breakdown_rows` shape exactly).
- Lesson: the bug class is now catalogued — every change touching one of the trio members must update all three atomically. CLAUDE.md flags this alongside the /api/stats TTL pattern.

### Q. YT-only legacy code wrapping non-YouTube URLs
**Pattern**: User pastes a SoundCloud URL into SET URL. The live preview correctly detects "soundcloud", `url_source()` and `extract_video_id()` parse it correctly. But the persistence path reconstructs a canonical URL from the synthetic `sc-<artist>-<slug>` sentinel as `https://www.youtube.com/watch?v=sc-<artist>-<slug>`. Download immediately fails at yt-dlp's YT extractor: `[youtube] sc-stevecom: Video unavailable`.

**Canonical References**: v1.14.9
- Root cause: `api_manual_url` and `api_override` both did `vid = extract_video_id(url); canonical = f"https://www.youtube.com/watch?v={vid}"` from before SC support landed. `extract_video_id` (widened in v1.14.0) returns the synthetic SC sentinel intentionally (so `local_files.source_video_id` can drive the SRC=U letter classification), but the YT-only canonical assignment then ships a malformed URL straight to yt-dlp.
- Fix pattern: route canonicalization by `url_source()`. YouTube → existing `watch?v=VID` reconstruction. SoundCloud → store the input URL verbatim. Unknown host → 400 with "URL must be a YouTube or SoundCloud link". Apply at every persistence path that touches user-provided URLs.
- Lesson: when widening a domain (URL parser, classifier, label) check every consumer of the parsed shape — not just the obvious ones. A passing live-preview test does not prove the persistence path is source-aware.

### R. URL params losing to localStorage on truthy-only gates
**Pattern**: Topbar UPD/FAIL pill click emits `?fourk=0&attn_pills=...` (breakdown SQL knows the right variant). Receiving page lands on the wrong variant — 4K when failure was in standard. The pill href is right; the receiving JS gate ignores the explicit `=0`.

**Canonical References**: v1.14.7
- Root cause: variant-resolution gate only treated `=true`/`=1` as explicit override. `=0` fell through to the localStorage fallback which carried `'fourk'` from a previous 4K visit on the same tab.
- Fix pattern: key the gate on `sp.has('fourk')` — any presence (including `=0`) is an explicit override. localStorage fallback only fires when the param is absent.
- Lesson: when a URL param is meaningful as 0/false, presence-check beats truthy-check. Same trap shape will recur for any future falsy-but-explicit param.

### S. User-override success without per-section ATTN ack
**Pattern**: A row with src=U (user override active, working canonical placed) keeps rendering the red ⚠ ATTN glyph and counting toward N FAIL forever. The INFO dialog says "✓ RESOLVED — TDB UNAVAILABLE" but the per-row glyph + topbar count + library filter all keep treating the section as failing.

**Canonical References**: v1.14.8
- Root cause: pre-v1.13.74 the worker blindly cleared `themes.failure_kind` on every successful download. v1.13.74 narrowed that to `source_kind='themerrdb'` (so the red TDB ✗ pill correctly survives a user-URL success — bug class fix). But the per-section ATTN axis (`section_failure_acks`, v1.13.54) wasn't addressed — nothing wrote sfa on user-override success, so the section stayed un-acked indefinitely.
- Fix pattern: worker INSERTs sfa with `acked_by='auto:user_override'` (ON CONFLICT DO NOTHING so manual acks aren't overwritten) on every successful URL/upload placement. Schema v44 backfill migration stamps sfa for sections where a healthy U override already exists but the parent themes row has un-acked failure (`acked_by='auto:user_override:backfill'`). `failures_total` rewritten to mirror the breakdown shape (joins sfa) — closes the mirror-principle leak that caused the count to lie even after sfa was written.
- Lesson: when narrowing a clear, audit every consumer of the cleared state for the new "stays set legitimately" axis. The TDB ✗ pill is one consumer; the per-section ATTN ack is another.

---

## 2. Architecture Pivots

### A. Volume layout: v1.3 (three volumes) → v1.5 (two-volume /config + /data model)
**When**: v1.4.0, v1.5.0
**Pivot**: Original had /themes, /media/movies, /media/tv as separate mounts. Dropped in favor of /config (user data, DB, settings) + /data (Plex folders + staging tree).

**Why**: Simpler user setup; Plex and motif staging use same mount (hardlinks work without cross-filesystem moves).
**Code markers**: Dockerfile groupadd guard (numeric chown), drop of movies_themes_dir / tv_themes_dir paths.
**Lesson**: Unified mount for content + staging = hardlink viability.

### B. Canonical theme file layout: v1.5.6 (per-video files) → per-folder theme.mp3
**When**: v1.5.6
**Pivot**: Was `<themes_dir>/{movies,tv}/<video_id>.mp3`; switched to `<themes_dir>/{movies,tv}/<sanitized_title_year>/theme.mp3`.

**Why**: Mirrors Plex's "Title (Year)" folder structure; users can browse staging tree without a map; file placement becomes literal folder ownership.
**Code markers**: canonical.py canonical_theme_subdir / sanitize_for_filesystem; worker._do_download output_dir composition.
**Lesson**: Align staging structure to Plex's visible hierarchy → user-friendly, hardlink-friendly.

### C. Database schema reset: v1.11.0 hard-stop migration (v17 → v18)
**When**: v1.11.0 "per-Plex-section themes layout"
**Pivot**: Added per-section staging isolation. local_files.section_id (PK component), placements.section_id, themes_subdir per section.

**Why**: Multi-section items (Matilda in Movies + 4K Movies) were cross-bleeding; hardlinks now per-section; FolderIndex cache isolated per section.
**Consequence**: HARD STOP migration; no auto-upgrade path (dev-mode policy). Existing DBs deleted on first run.
**Code markers**: DB schema v18, plex_sections.themes_subdir, _index_for_section, per-section FolderIndex cache.
**Lesson**: Single-section assumptions broke at scale; per-section PK component was structural necessity, justified hard stop.

### D. Sync transport tiers: remote → snapshot (database branch) → git (v1.13.0 Phase B)
**When**: v1.12.121 (snapshot), v1.13.0 (git)
**Pivot**: v1.4–v1.12 used per-item HTTP (remote). v1.12.121 added snapshot (tar of database branch). v1.13.0 added git differential (dulwich).

**Why**: Remote = 10k+ HTTP reqs/day regardless of changes. Snapshot = full tarball on any change. Git = delta only (~100 KB changed-day vs 5K replayed items).
**Cascade logic**: git → snapshot → remote on _GitMirrorError / _SnapshotError.
**Code markers**: app/core/sync.py _GitMirror / _DatabaseSnapshot; _classify_git_path; three-tier fallback in run_sync.
**Lesson**: Progressive optimization (remote → batch → diff) justified hard schema changes (git mirror state). Phase C (v1.13.1) added tdb_dropped_at tracking on dropped items.

### E2. Per-section failure dismissal (v1.13.54)
**Pivot**: `themes.failure_acked_at` was title-global since v1.10.50 — one ack covered every section that owned the title. By v1.13.x, with section-scoped adopt-and-ack and purge-and-ack chains, this caused cross-section bleed: dismissing the failure on the 4K row also cleared it for standard.
- **Architectural change**: New `section_failure_acks (media_type, tmdb_id, section_id, acked_at, acked_by)` table. `/clear-failure` accepts optional `section_id`; section-scoped writes land in the new table. After every per-section ack, code checks whether ALL owner sections have been individually dismissed; if so, also stamps the title-global flag for legacy-read convergence.
- **Read side**: library SQL gains LEFT JOIN section_failure_acks aliased `sfa`; row-level "is acked" derives from `COALESCE(sfa.acked_at, t.failure_acked_at)`. `/api/sections/coverage` failure count uses the same JOIN.
- **Migration cost**: schema v40→v41 (additive — new table + indexes); no data shape change to existing rows.
- **Lesson**: when a title-global axis develops a per-section need, add a per-section sink with the global axis as a derived/eventual-consistency mirror.

### F. Live ops drawer kinds + REPROBE PLEX THEMES admin tool (v1.13.40-43)
**Pivot**: The ops drawer covered tdb_sync + plex_enum + queue ops (download/place/refresh). v1.13.40 added a new admin-triggered op kind: `reprobe_plex_themes`.
- **Why**: the +P composite signal (plex_independent_theme=1) only gets captured by plex_enum's narrow observation window (sidecar=0). Rows that landed in sidecar=1 state when the column was added stayed NULL forever, missing the +P dot. REPROBE walks the whole library doing a non-destructive byte-prefix comparison: read 2KB from each local theme.mp3, GET the same range from Plex's `/library/metadata/{rk}/theme`, compare. Match=sidecar; differ=Plex serves an independent theme.
- **Migration cost**: schema v39→v40 widens op_progress.kind CHECK constraint. Required because v1.13.38 added the worker but didn't update the constraint, so the very first call raised IntegrityError on a clean install. SQLite can't ALTER CHECK in place — table rebuild + INSERT FROM SELECT.
- **Lesson**: every new op_progress.kind value needs a schema migration on the CHECK constraint. v1.13.41 caught this via a runtime IntegrityError; future kinds should bundle the migration with the worker addition.

### G. Smart TRY THIS NEXT recovery (v1.13.44-47)
**Pivot**: The info-card recovery section (v1.12.71) listed static recovery options per failure_kind — SET URL, UPLOAD MP3, ACK FAILURE. v1.13.44-47 added context-aware "smart" actions that read the row's Plex state to suggest the right one-click path.
- **ADOPT EXISTING THEME** appears when `pi.local_theme_file=1` AND the placement isn't motif's (manual sidecar at the Plex folder, adoptable in one click). Action chains `/adopt-sidecar` + `/clear-failure`.
- **LET PLEX SERVE** appears when `pi.plex_independent_theme=1` (Plex serves its own cloud / themerr-plex embed). Action chains `/forget` + `/clear-failure`.
- v1.13.47 extended this to non-failed +P rows too: any T/U/A/M row with `plex_independent_theme=1` can revert to Plex via the `purge-revert-to-plex` action (no failure to ack — just `/forget`).
- **Lesson**: recovery options should be data-driven. Reading the row's Plex state lets motif suggest the correct action automatically instead of asking the user to figure out which button to click.

### H. Customizable dashboard with DnD + hide/show (v1.13.48)
**Pivot**: The dashboard had ~10 sections in fixed order. v1.13.48 made it user-configurable.
- **Storage**: server-side via `runtime_settings.dashboard_layout` JSON blob (instead of localStorage). Survives across browsers.
- **UI**: `// CUSTOMIZE` button toggles `body.dash-customize-mode`. Each section gains a control bar (drag handle ⋮⋮ + label + // HIDE/SHOW button). HTML5 native drag-and-drop reorders DOM siblings; debounced save flushes to the server.
- **Reconciliation**: layout merges saved order with current DOM — newly-shipped sections appear by default, removed sections drop out cleanly. No migration needed when sections are added/removed in code.
- **Lesson**: UI customization should persist server-side for multi-device, not localStorage. New `runtime.get_runtime_text` / `set_runtime_text` helpers complement the existing bool helpers.

### I. Regional bypass options for yt-dlp (v1.13.53)
**Pivot**: yt-dlp now supports HTTP/SOCKS proxy + geo_bypass options for routing around regional content blocks (e.g., the SME-blocked "blocked it in your country on copyright grounds" pattern). Surfaced via DownloadsConfig.
- **Three knobs, layered**: `geo_bypass` (XFF spoof — usually doesn't work on YouTube rights blocks but cheap to enable), `geo_bypass_country` (2-letter ISO code), `proxy_url` (HTTP / SOCKS — actually solves YouTube content blocks since it changes source IP).
- **Security**: proxy_url accepts inline credentials; masked on `/api/config` GET via the same `***` pattern used by `plex.token` and `tmdb_api_key`. PATCH treats empty / `***` as "leave alone".
- **Lesson**: when adding sensitive config fields, mirror the existing token-mask pattern instead of inventing new semantics.

### E. Per-section REFRESH FROM PLEX (v1.10.6) + button locking unification (v1.11.5)
**When**: v1.10.6, v1.10.7, v1.11.5
**Pivot**: Originally single global REFRESH; became per-tab/variant, then unified all sync/refresh buttons under one lock.

**Why**: Users with multiple 4K/standard variants wanted granular control. Button spam-click when one enum was running broke UX. v1.11.5 unifies sync/refresh/enum under anyEnumInFlight signal.
**Code markers**: /api/library/refresh {tab, fourk} body; refreshTopbarStatus syncing-with-X label; lockBtn on (themerrdb_sync_in_flight OR plex_enum_in_flight).
**Lesson**: Granular controls need coordination; single in-flight signal beats per-operation checks.

### J. Verb taxonomy lock-in: SYNC / REFRESH / RE-SCAN (v1.13.70-71, v1.13.80)
**Pivot**: Three Plex-touching actions had drifted into the same `Sync` verb. v1.13.70-71 lock in a three-verb taxonomy across every UI surface.
- **SYNC** — ThemerrDB metadata pull (`// SYNC THEMERRDB`, `tdb_sync` job kind). The only true sync.
- **REFRESH** — Plex section enumeration (re-discover sections + sidecars + theme claims). User-clickable: `// REFRESH PLEX`, `// REFRESH MOVIES`. Drawer card label `PLEX REFRESH` (was `PLEX SCAN`).
- **RE-SCAN** — per-folder Plex metadata nudge fired automatically after every successful place (gated by `plex.analyze_after_placement`). Topbar reads `PLEX RE-SCAN QUEUED`; settings checkbox reads `PLEX RE-SCAN AFTER PLACING`. Drawer kind label `RE-SCAN QUEUE` (v1.13.80, the last straggler).
- **Why**: pre-fix `// SYNC PLEX` and `// SYNC THEMERRDB` read like the same op even though one's an HTTP enum and the other's a 5K-row metadata pull. Conflating the post-place `refresh_queue` op with the user-clicked `// REFRESH PLEX` action made the topbar lie by omission ("Plex refresh queued" — which one?).
- **Code markers**: element ids and config keys preserved (`sync-plex-btn`, `plex.analyze_after_placement`, `sync.auto_enum_after_sync`, internal `refresh_queue` job kind). Only user-visible strings moved.
- **Lesson**: when three actions converge on the same verb, pick three distinct verbs and sweep every surface in one pass; preserve internal identifiers so the rename can't churn external consumers.

### K. release/nightly branch model + local-only journal (between v1.13.73 and v1.13.74)
**Pivot**: GitHub default branch flipped to `nightly`; `release` holds the most recent shipped tag. `main` deleted.
- **Why**: rapid-iteration commits land on `nightly`; `release` should always equal a `vX.Y.Z` tag. Tags remain the unit of ship (GHA builds Docker on `v*.*.*`). Cleaner mental model than `main` that conflates "latest commit" and "latest release".
- **Side effect**: `docs/SESSION_JOURNAL.md` becomes gitignored, local-only. Contains mid-task thinking, half-formed debugging guesses, paths/tokens that shouldn't leave the dev box. SessionStart hook tails it as initial context for the next session — handoff notes between Claude sessions, never pushed.
- **Trap**: switching to a branch older than the journal-untrack commit clobbers the local-only journal. v1.13.80 release fast-forward hit this — recovered from `git show v1.13.73:docs/SESSION_JOURNAL.md` baseline. Permanent fix candidate: move journal to `.local/` path that never existed in any committed tree.
- **Lesson**: gitignored files in directories that DID have a tracked predecessor are silently destroyed by branch checkouts. Document the trap (now in `motif_journal_branch_switch_trap.md` memory).

### L. SoundCloud as a first-class theme source (v1.14.0-5)
**Pivot**: pre-v1.14 every URL parser, classifier, label, and yt-dlp opt assumed YouTube. v1.14.0-5 widens to a source-aware model end-to-end.
- **v1.14.0**: `url_source(url) → 'youtube'/'soundcloud'/'unknown'` in `app/core/sync.py`. `extract_video_id` widened to also match SC URLs and return the synthetic `sc-<artist>-<slug>` sentinel (lands in `local_files.source_video_id` so `_SRC_LETTER_SQL` tags as 'U' instead of falling through to 'A'). JS mirror: `urlSource()`, `urlSourceLabel()`, `THEME_URL_RE`. Dialog titles + placeholders + HTML5 pattern attrs widened to accept both schemes.
- **v1.14.1**: `classify_yt_dlp_error` widened with SC patterns ("private track", "track has been removed", "Go+ subscription required", etc.). `FailureKind.human` strings rewritten source-agnostic ("Track or video..." / "Cookies missing or expired" / "Network error reaching source"). Pre-fix SC errors hit UNKNOWN → infinite-retry loop until max_attempts.
- **v1.14.2**: `acceptUpdate` confirm prompt shows actual current + new URLs, prefixes "⚠ SOURCE CHANGE" warning when sources differ (SC override → YT TDB). New shared `bindUrlSourcePreview()` helper wired to both override + manual-url dialogs ("detected: YouTube" / "detected: SoundCloud" live as user pastes).
- **v1.14.3**: `/api/youtube/oembed` → `/api/source/oembed`. New `_OEMBED_PROVIDERS` table routes by host. `_oembed_source_for(url)` mirrors `url_source()` locally (narrow import graph). Legacy YT endpoint kept as alias for stale browser caches; both endpoints share the routing helper so they can't drift behaviorally.
- **v1.14.4**: long-tail UI copy pass — tooltips, glyph-titles, menu descriptions, PURGE warnings, kindHuman maps all widened from "YouTube" to "theme URL" / "YouTube or SoundCloud" / "the source". Tests use `_strip_line_comments` helper to dodge the recurring trap where rationale comments quoting a deleted string trip "must not appear" guards.
- **v1.14.5**: `_opts()` gains `source` kwarg. `js_runtimes`, `remote_components`, `extractor_args.youtube.player_client` move out of the dict literal into an `if source == "youtube":` block. Non-YT sources skip the YT-only opts (~1-3s saved per SC download; avoids confusing yt-dlp's extractor selection on 2025+ builds).
- **Lesson**: a domain widening that touches SQL + Python parsing + JS validation + UI copy + yt-dlp opts is best done as a sequential cluster (parse → classify → label → route → opts), each tag landing one layer with regression guards. The v1.14.9 SET URL bug class Q proves you can still miss a persistence path; future widenings should add an end-to-end live-test before tag-cut.

### M. Per-section sfa write on user-override success (v1.14.8)
**Pivot**: the per-section ATTN ack lifecycle (`section_failure_acks`, introduced v1.13.54) closed on TDB-success and explicit user ACK only. v1.14.8 adds user-override-success as a third closure path — without it, working overrides on TDB-dead rows kept glyphing red ⚠ forever (bug class S).
- **Architectural change**: worker INSERTs sfa on every URL/upload success (ON CONFLICT DO NOTHING). Schema v44 backfill catches existing healthy-U rows. `failures_total` rewritten to mirror `failure_tab_breakdown_rows` shape — same FROM/JOIN/WHERE so count + breakdown + filter all agree (mirror principle).
- **Read side**: no JS change — `failure_acked_at` is already `COALESCE(sfa.acked_at, themes.failure_acked_at)` since v1.13.54, so the per-row ⚠ glyph naturally suppresses once sfa is written.
- **Lesson**: when v1.13.74 narrowed the worker's failure-clear scope (`source_kind='themerrdb'` only), every consumer of the cleared state needed to grow its own narrowing. The TDB ✗ pill (the intended consumer) was addressed; the per-section sfa axis was missed for ~3 weeks until the user surfaced the stuck ⚠ glyphs.

---

## 3. Feature Axis Evolutions

### A. Ops drawer / live progress system

**Origin** (v1.8.12): Manual URL/upload didn't surface progress; user waited blind.
- v1.8.12 added "pending glyph" (spinner + DL/PL pills light in sequence) + rapid-poll (60s window).

**Evolution**:
- v1.10.5: DL/PL pills became primary progress signal; disabled destructive actions during flight.
- v1.10.7: Rapid-poll throttle 3s → 5s; skip when user interacting (input focused, dialog open, text selected).
- v1.11.7: Richer topbar status (download/place/probe/scan/idle) displayed active operation name.
- v1.11.23: Optimistic topbar refresh on click (paintTopbarSyncing); don't wait 15s for poll.
- v1.11.48: Unified topbar + sync button locking; scope-aware labels (SYNC 4K MOVIES, REFRESHING ANIME).
- v1.13.14: Dropped title-cell spinner; DL/PL pulse alone covers in-flight signal (fewer races).

**Current canonical model** (v1.13.x):
- Topbar: scope-aware status (SYNCING WITH PLEX (MOVIES), idle).
- Row level: DL pill (downloading/done), PL pill (placing/done). No spinner; pills are state.
- All sync/refresh/enum buttons: locked together on (themerrdb_sync_in_flight OR plex_enum_in_flight).
- Optimistic feedback on click; polling refines state.
- /pending page (v1.5.3) for staged-but-not-placed review + bulk approve/discard.

### B. SRC letter classification (T/A/U/M/P/-)

**Origin** (v1.6.0): Plex enumeration introduced per-row badge: T=ThemerrDB, P=Plex-only.

**Evolution**:
- v1.8.13 added U (user override) + A (adopted) badges.
- v1.9.5: Separated hash_match logic (now classed as hash-adopted → A badge).
- v1.9.7: Dropped U → M consolidation attempt (re-retained in v1.10.1 as separate).
- v1.10.19: Revert visibility tightened; only shows when source_kind='url' AND it's ThemerrDB-tracked (fixed over-broad revert on manual-url rows).
- v1.10.27: PURGE on U/A preserves Plex-folder file (flips to M); PURGE on T deletes both canonical + folder.
- v1.10.38: Reverted preservation; PURGE always deletes both.
- v1.11.43: Added plex_items.theme_id stamp to unify orphan/real rows.
- v1.11.62: Added M source for manual uploads without prior theme.

**Current canonical model** (v1.14.x):
- T (ThemerrDB): source_kind='remote', upstream_source points to TDB. DOWNLOAD/RE-DL/REPLACE/REVERT/ACK-FAILURE.
- U (user URL): source_kind='url', user-supplied YouTube OR SoundCloud URL (since v1.14.0). `source_video_id` carries the YT 11-char id OR the synthetic `sc-<artist>-<slug>` SoundCloud sentinel. DOWNLOAD/RE-DL/REPLACE-W-TDB/REVERT. Can flip to T via ACCEPT-UPDATE or revert to TDB.
- A (adopted): source_kind='adopt', file matched sidecar on disk. DOWNLOAD (re-adopt)/REPLACE-W-TDB.
- M (manual): uploaded MP3 or post-PURGE manual file. Resides in Plex folder. DL=off (static); UPLOAD/DELETE.
- P (Plex-only): plex_items row with no themes entry. UPLOAD.
- '-' (TDB-dropped): tdb_dropped_at set; TDB no longer publishes. Can ACK-DROP or CONVERT-TO-MANUAL.
- +P (composite, v1.14.10): the yellow-dot indicator on T/U/A/M chips when `plex_independent_theme=1` (Plex also serves its own theme alongside motif's sidecar). Wire token `Pp` (URL form-encoding turns `+` into a space, breaks round-trip); button label keeps `+P`. Strict subset of the existing `P` filter (which is OR of pure-P + composite); `+P` is composite-only — exactly the rows the user is asked to deduplicate against their motif-managed theme.

Rendering: IMDB → title → SRC badge → edition pill → TDB pill (color: green=tracked, yellow=cookies, red=failed, gray=dropped).

### C. TDB pills, DL pills, link pills (filter axes)

**TDB pills** (v1.10.33 onward):
- v1.10.33: Green TDB pill on every row, label "TDB" when upstream_source is real TDB record.
- v1.10.40: Red "TDB ✗" when failure_kind matches known patterns (video_removed, private, age, geo).
- v1.10.42: Amber "TDB ↑" for cookies_expired (retriable with cookie upload).
- v1.10.44: Yellow for other cookies_required states.
- v1.13.1: Gray "TDB◌" for tdb_dropped_at set (TDB stopped publishing; local theme still works).
- Filter row: TDB TRACKED / UNTRACKED chips; v1.13.0 fixed pill spacing (margin-left: 0 in filter row).

**DL pills** (v1.8.12 onward):
- Lights up during download job in flight, persists when download completes.
- v1.13.14: Now the sole in-flight indicator (title-cell spinner dropped).

**PL pills** (v1.10.0 onward):
- Lights up during place job, persists when placed. Mirrors DL pill.

**LINK pills** (v1.11.16 onward):
- New INFO pill showing IMDB link button. Not a true pill in filter sense; more of a row action.

**Edition pills** (v1.10.17 onward):
- Yellow pill after title when folder matches {-edition} pattern. Differentiates Director's Cut / Theatrical / etc.
- v1.10.31: Reverted copies_pill (was green, redundant with Plex's edition tracking).

**Filter row logic** (v1.12.4 onward):
- Split status axis (THEMED/UNTHEMED) from TDB-coverage axis (TDB TRACKED/UNTRACKED/ANY).
- Can now ask: UNTHEMED + TDB TRACKED = motif could fetch from upstream.
- UNTHEMED + TDB UNTRACKED = orphan, manual-source only.

**ATTN axis** (v1.13.68 onward — the canonical attention surface):
- New filter row below SRC, multi-select, mirrors the title-cell glyph priority: `⚠` red (un-acked failure), `!` blue (pending TDB update), `!M` amber (mismatch), `!P` amber (await placement), `↺` amber (broken canonical).
- Migration from `tdb_pills` to `attn_pills`: topbar FAIL pill click-through migrated `?status=failures` → `?attn_pills=fail` (v1.13.68). Topbar UPD pill migrated `?tdb_pills=update` → `?attn_pills=update` (v1.13.79). Bulk ACK SELECTED renamed ACK FAILURES, gated on `attnPills.has('fail')` instead of legacy `tdbPills.has('dead')`.
- Per-(tab, fourk) breakdown for click-cycle: `failure_tab_breakdown_rows` (v1.13.69 FAIL) + `update_tab_breakdown_rows` (v1.13.78 UPD) group un-acked attention items by (type, is_anime, is_4k); successive pill clicks rotate through the cycle (deterministic order: movies → 4K movies → tv → 4K tv → anime → 4K anime).
- Bulk-bar reveal extends to no-selection mode when any attn chip is active (`// PUSH ALL TO PLEX` on `await`, `// REVERT MISMATCH` on `mismatch`, `// RESTORE FROM PLEX` on `broken`).

### D. Pending-updates flow (accept upstream change or keep local)

**Origin** (v1.5.4): sync notices user_override on item when ThemerrDB has new URL.

**Evolution**:
- v1.5.4: Write pending_update when override exists + TDB changed. No UI to surface it.
- v1.11.73: Added topbar UPD badge (count from pending_updates rows with decision='pending').
- v1.11.74: Added library-row green ↑ glyph + UPDATES filter chip. SOURCE menu grows ACCEPT-UPDATE / KEEP-CURRENT.
- v1.12.5: Bulk-action bar renamed to "DOWNLOAD FROM TDB"; tri-state select-all.
- v1.12.6: TDB pill multi-select filter (TDB filter row became clickable).

**Current canonical model** (v1.13.x):
- pending_updates row exists when (media_type, tmdb_id, decision='pending') = TDB published new URL since user's last action.
- Topbar UPD badge links to /movies?status=updates.
- Library row shows green ↑ title glyph.
- SOURCE menu: ACCEPT UPDATE (re-download from new TDB URL) / KEEP CURRENT (decline, leave alone).
- Post-action, topbar UPD badge refreshes past /api/stats 1s TTL cache.

### E. Plex scans / orphan adoption

**Origin** (v1.6.0): Plex enumeration discovered orphans (items without themes).

**Evolution**:
- v1.6.0: plex_items table + plex_enum job type introduced.
- v1.8.0: Separate /scans tab with scan findings (content_mismatch, etc.).
- v1.9.3: Manual URL on Plex-only item allocates synthetic orphan (negative tmdb_id).
- v1.9.7: Bulk ADOPT (HASH) button; adopts all orphan_resolvable on page.
- v1.10.9: Inline ADOPT + REPLACE-WITH-THEMERRDB row actions (no modal needed).
- v1.10.39: Hardened orphan-promotion + reconcile_placement_paths against UNIQUE conflicts.
- v1.11.71: PRAGMA defer_foreign_keys fix for orphan promotion crashes.
- v1.11.73: Orphan-promotion pre-deletes stale child rows at target.
- v1.11.76: /api/debug/stat-folder diagnostic for placement issues.

**Current canonical model** (v1.13.x):
- REFRESH FROM PLEX: per-section plex_enum job via reconcile_placement_paths (detects Plex folder renames/moves).
- Scanner finds orphans (folder on disk, no themes row). Offers: LOCK (permanent decision), replace via SET URL, ADOPT (file matches sidecar), REPLACE-W-TDB.
- Manual URL / UPLOAD on Plex-only creates synthetic orphan; next sync promotes to real row if match found.
- Orphan promotion: defer_foreign_keys + pre-delete stale child rows at (media_type, new_tmdb_id).
- Placement fallback: use pi.theme_id + pi.folder_path (cached_rk/cached_folder_path) before FolderIndex lookup.

### F. Coverage / dashboard panels

**Origin** (v1.7.0): /coverage tab showed missing-themes banner + Plex catalog stats.

**Evolution**:
- v1.8.1: /coverage route → 302 to / (dropped separate tab). Coverage stats moved to dashboard "Last Sync" panel.
- v1.10.10: Dropped missing-themes banner entirely (UI clutter; library filters are sufficient).
- v1.11.77: Persist sync stats on cancel/fail; TDB pill gate on partial-capture data (not just successful syncs).
- v1.12.6: Dashboard syncing-status card; Last Sync / Next Sync lines.

**Current canonical model** (v1.13.x):
- Dashboard "Last Sync" panel: timestamp, status, movie/TV/new/updated/deleted counts. Updates on partial-capture (cancelled sync).
- "Next Sync" line derived from settings.cron schedule (parsed / displayed as human-readable interval).
- TDB pills render if themes data exists for that media_type (gate on __motif_themes_have, set from /api/stats, not just successful syncs).

### G. Section gating + 4K toggle

**Origin** (v1.5.2): libraries.html with section list + per-library include/exclude checkboxes. 4K toggle (// STANDARD vs // 4K).

**Evolution**:
- v1.6.0: Per-library per-variant (4K, anime) checkboxes.
- v1.8.8: Adaptive nav; hide 4K toggle when only one variant exists.
- v1.10.5: Fix 4K toggle flicker (default display:none, show only when both variants exist).
- v1.10.6: Per-section REFRESH FROM PLEX (respects tab + fourk).
- v1.11.0: Themes layout per-section isolation (themes_subdir keyed by section slug).
- v1.11.4: Library-flags 409 guarding against slug rewrites with staged files.
- v1.11.5: Unified sync/refresh button locking + scope-aware labels.
- v1.12.1: REFRESH fallback to other variant when requested has 0 sections.

**Current canonical model** (v1.13.x):
- Settings LIBRARIES tab: per-section MGD checkbox, A (anime) pill, 4K pill. SAVE recomputes themes_subdir if flags change (refuses 409 if item rows exist).
- Library pages (Movies/TV/Anime): tab filter + // STANDARD / // 4K toggle (scoped REFRESH FROM PLEX).
- Each section has isolated themes_subdir + per-section local_files/placements + per-section FolderIndex cache.
- Multi-section items (e.g. Matilda in Movies + 4K Movies) have N local_files rows, all hardlinked from same inode.

---

## 4. Schema Migrations

Capsule summary of each schema_version bump and user-facing capability unlocked:

- **v6** (v1.5.6): Canonical layout migration (no SQL change). Startup hook relocate_legacy_canonical_files moves per-video files to per-folder theme.mp3.
- **v7** (v1.6.0): plex_items table + plex_enum job type introduced. Additive (no breaking change).
- **v8** (v1.8.2): pending_updates table for tracking TDB upstream changes. Additive.
- **v9** (v1.8.5): Job index creation (idx_jobs_type). Migration adds missing index.
- **v10** (v1.8.6): Probe job type + tvdb_lookup_cache table.
- **v13** (v1.9.4): settings table + env overrides codification. Hard-stop; fresh start required.
- **v17** (v1.10.42): user_overrides table redesign (media_type + tmdb_id + decision columns). Hard-stop for devs.
- **v18** (v1.11.0): Per-section layout (local_files.section_id, placements.section_id, plex_sections.themes_subdir). **HARD STOP** migration; existing DBs deleted.
- **v19** (v1.11.2): Minor schema adjustments.
- **v20** (v1.11.6): Jobs acked_at column.
- **v22** (v1.11.58): themes_subdir mirrors Plex folder layout. Soft ALTER (backfilled).
- **v23** (v1.11.61): jobs.acked_at column renamed.
- **v24–v25** (v1.11.64–v1.11.65): plex_items.theme_id stamp (cross-section unifier for orphans). Soft ALTER.
- **v27** (v1.12.42): user_overrides redesign (per-section). Migration recreates table.
- **v28** (v1.12.50): audit_events table for compliance.
- **v29** (v1.12.52): sections_scans table; section_id added to audit_events.
- **v31** (v1.12.73): Per-section pending_updates (was global). Migration alters PK.
- **v34** (v1.13.0): Deprecated removal (previous_urls, etc.). Cleanup migration.
- **v36** (v1.13.1): themes.tdb_dropped_at column (NULL = current in TDB; set = dropped). Additive, enables Phase C UI.
- **v37** (v1.13.11): saved_filters table. Generic — `scope` column lets future pages reuse without another migration. Additive.
- **v38** (v1.13.x): minor adjustments to `plex_items` for theme_id resolver hot path.
- **v39** (v1.13.38): plex_items.plex_independent_theme column (NULL/0/1). Drives the +P composite yellow-dot indicator. Captured by plex_enum when sidecar=0; backfilled by the v1.13.40 REPROBE admin tool. Pre-fix +P was a runtime recomputation that only held in a narrow window before plex_enum stamped local_theme_file=1; now it's persisted. Additive.
- **v40** (v1.13.41): op_progress.kind CHECK widened to include 'reprobe_plex_themes'. Required because v1.13.38 added the worker but didn't update the constraint, so the very first call to start_progress raised IntegrityError. SQLite can't ALTER CHECK in place — table rebuild + INSERT FROM SELECT.
- **v41** (v1.13.54): three additive partial indexes for hot scans (`idx_plex_items_folder_path` for post-place stamps, `idx_plex_items_indep_null` for REPROBE candidate query, `idx_themes_dropped` for dashboard / library SQL on dropped-TDB rows) + new `section_failure_acks` table (per-section failure dismissal, closes the v1.13.K cross-section bleed class for clear-failure).
- **v42** (v1.13.61): cleanup of stale `urls_match` `pending_updates` rows (the dead-end synthetic prompts from bug class M). One-shot DELETE; no schema shape change.
- **v43** (v1.13.75): events-log backfill of wiped TDB `failure_kind` values. Correlated subquery with `json_extract` reconstructs the kind from the events log when v1.13.74's worker scope-narrowing left it null. URL-match guard skips if TDB URL changed since failure; suppression guard skips if a later TDB-success matched. `failure_acked_at` intentionally not restored (let the dashboard banner surface the count and let the user re-ack).
- **v44** (v1.14.8): backfill of `section_failure_acks` for sections where a healthy U override exists (`provenance='manual' AND source_kind in ('url','upload')`) but the parent themes row has un-acked failure. `acked_by='auto:user_override:backfill'` to distinguish from write-time acks. Pure INSERT … WHERE NOT EXISTS, idempotent. Closes bug class S retroactively for installs that landed user overrides between v1.13.74 and v1.14.8.
- **v53** (v1.17.9): DROP COLUMN `plex_items.motif_unplaced_at`. Column was added v34 (v1.12.110) as a phantom-P tombstone, deprecated v1.12.111 once `plex_theme_verified_ok` (HEAD verify) replaced it — dead through 5 years of tags. The v34 migration docstring explicitly anticipated this drop ("future migrations may drop it"). SQLite 3.35+ supports ALTER TABLE DROP COLUMN; motif's Dockerfile uses python:3.12-slim → SQLite 3.45+. Idempotency guard via `PRAGMA table_info` before the drop so a partial-replay scenario is safe. First column-drop migration in motif's history (previous "drops" recreated tables); the precedent matters for future dead-column cleanup work.
- **v54** (v1.17.17): retroactive UPDATE on `previous_urls` to fix `kind='themerrdb'` rows whose source `themes` row is `upstream_source='plex_orphan'`. Pre-fix the `_capture_previous_url` helper hardcoded `kind='themerrdb'` for any themes-row fallback, but plex_orphan rows' `themes.youtube_url` is the URL captured during ADOPT (not a real ThemerrDB URL). The mis-labeled kind triggered the v1.12.103 `revert_redundant` SQL branch to suppress RESTORE post-PURGE, leaving orphan rows with no recovery path. Idempotent UPDATE — WHERE clause already constrains to the wrong-kind+orphan-source intersection. Pure data fix (no schema shape change).

---

## 5. User Feedback Patterns and Resolutions

### A. Flicker on page navigation / rapid interaction
**Complaint**: "Page reloads while I'm interacting; toggles hide/show rapidly."

**Resolution pattern**: v1.10.7 throttle + skip logic. Skip rapid-poll tick when input focused, dialog open, or text selected. Auto-stop early when no job_in_flight rows remain (state is stable). Result: interaction feels smooth, no under-the-user updates.

### B. Button text swaps (DOWNLOAD → QUEUED) feeling janky / racing
**Complaint**: "Button says QUEUED but modal says DOWNLOAD; text jumps around."

**Resolution pattern**: v1.13.14–v1.13.18 refactor. Don't swap button text during flight. Use DL/PL pills as state signal instead (smaller, adjacent, won't race). Simpler DOM updates = fewer race windows.

### C. Status confusion (what does P mean? Is it downloading?)
**Complaint**: "Why is it still showing P after I clicked DELETE? What's P?"

**Resolution pattern**: v1.11.73 topbar red-dot clickable + tooltip. v1.11.74 UPDATES chip + ↑ glyph. v1.13.1 TDB◌ pill for dropped. Tooltips on every badge/pill explain meaning. SOURCE menu tailored per state (DOWNLOAD / REVERT / ACCEPT-UPDATE / ACK-FAILURE / ACK-DROP). Result: no guessing what P or ↑ means; menu options are context-aware.

### D. Generic text not helpful ("no such column", "network error")
**Complaint**: "Error message is cryptic; can't tell what went wrong."

**Resolution pattern**: v1.11.76 /api/debug/stat-folder for deep diagnostics. v1.11.77 partial-capture persistence (show counts even on cancel). v1.12.50 human error messages for common failures (video_removed / private / age-restricted / geo). Failure kind encoded as enum, render as tooltip + recovery hint in SOURCE menu. Result: user can diagnose without logs.

### E. Sync progress invisible (is it hung?)
**Complaint**: "Sync started 5 minutes ago; no feedback; is it working?"

**Resolution pattern**: v1.11.7 richer topbar (operation name). v1.11.48 optimistic paint (label updates on click, not at next poll). v1.11.76 per-item sync log (completed/total every 500 items, every 30s wall-clock). Result: topbar shows "SYNCING WITH PLEX (4K MOVIES)" live; logs show progress.

### F. Race between action + auto-refresh (action seems to disappear)
**Complaint**: "I clicked APPROVE on /pending; row vanished. Did it work?"

**Resolution pattern**: v1.11.69 post-action topbar refresh (scheduled past /api/stats 1s TTL). v1.11.48 optimistic state updates (button disables immediately, not on poll). Result: row feedback is instant; pending dot clears same frame as row leaves table.

### G. Long titles clip the inline TDB pill
**Complaint**: "Some rows with very long titles, the TDB pill and status (✗/⚠/etc.) get cut off — can't see the row's TDB state."

**Resolution pattern**: v1.13.55. Pre-fix the title-cell layout had `<span class="title-cell-name">{title}{edition}{tdb-pill}{section}</span>` with `overflow:hidden;text-overflow:ellipsis;white-space:nowrap` on the span. Long titles ellipsis-clipped the entire span — pills included. Restructured so the pills become flex SIBLINGS of the truncated title span (`{title-cell-name} {edition} {tdb-pill} {section}`). CSS: `.title-cell-name` gets `flex:1+min-width:0` (shrinks gracefully); siblings get `flex-shrink:0` via `.title-cell > *:not(.title-cell-name)`.

### H. Bulk-select header checkbox sticks on indeterminate
**Complaint**: "Click the title-row checkbox to select all, then unselect; box goes empty → `-`. Click again, selection clears but box still shows `-`."

**Resolution pattern**: v1.13.55. Two compounding causes: (1) click handler read `cb.checked` from row DOM to decide intent, but the browser's native click-on-indeterminate behavior auto-toggles `.checked` BEFORE `preventDefault` can stop it (notably on Firefox/Safari), making the read return a misleading value mid-handler. (2) The tri-state evaluator only wrote `.indeterminate` inside one of three branches — a stale `true` could survive when the canonical state went to unchecked. Fix: derive intent from `libraryState.selected` (single source of truth) instead of `cb.checked`, AND reset both `.checked` and `.indeterminate` first before assigning the new state.

### I. Topbar pill count != click-through page count
**Complaint**: "Topbar shows 3 FAIL but I click and the page shows 8 rows — only 1 of those is actually ackable."

**Resolution pattern**: v1.13.57. The pill count was unacked-only (`status=failures` SQL semantics) but the click-through routed to `?tdb_pills=dead` — a different filter that included ACKED dead-TDB rows. Re-pointed three click-throughs (topbar FAIL pill, per-section coverage failures cell, failure breakdown insight) to `?status=failures` so the page shows EXACTLY the rows the count represents. Cross-tab dimension (failures spread across tabs) still requires repeat clicks as `tab_hint` shifts after each ack — `tab_hint` updates server-side after stats refresh. See bug class O.

### J. Empty-state copy says "enable Plex sections" when filters just match nothing
**Complaint**: "Library shows 'no items — enable the relevant Plex sections in Settings' but I have thousands of rows; just my filter is too narrow. Copy mis-routes me to /settings."

**Resolution pattern**: v1.13.58. Three-way empty-state branch instead of two-way: (1) enum running → "scanning Plex now…", (2) filters active → "no items match the current filters — click `// CLEAR ALL` above or adjust the chips", (3) no filters and not enumerating → original "enable Plex sections" prompt (reserved for first-time-visitor case). Filter detection covers every libraryState axis (q, status, tdb, srcFilter, tdbPills, dlPills, plPills, linkPills, edPills).

### K. INFO-card chip duplication / wrong color
**Complaint**: "Info card shows '[Standard Movie]' green chip + '[Movies]' gray chip — duplicate info. And the edition chip should be yellow to match the row pill, not magenta."

**Resolution pattern**: v1.13.51. Dropped the green/cyan variant chip (the gray section-title chip already conveys variant via "Movies" vs "4K Movies" vs "TV Shows"). Variant_label moves to a `title=` tooltip on the section chip for the rare same-title cross-variant edge case. Edition chip recolored magenta → amber to match the row .edition-pill and the // ED filter button — same logical concept reads in the same color across all three surfaces.

### L. CSV export mojibake on non-ASCII titles
**Complaint**: "Pokémon Evolutions exporting as Pok√©mon Evolutions when I open in Excel/Numbers."

**Resolution pattern**: v1.14.6. Blob had `charset=utf-8` in MIME type but no BOM; Excel/Numbers ignore the MIME charset and sniff by BOM, falling back to MacRoman / Windows-1252. Prepend the literal U+FEFF (3-byte UTF-8 sequence) as the first Blob array element. Tests pin the BOM presence + position + byte-level sequence (not the `\u` escape).

### M. Working U override still glyphs red ⚠ FAIL
**Complaint**: "I set a SoundCloud URL, the download succeeded and Plex is serving it, but the row still shows the red ⚠ glyph and the FAIL count includes it. INFO card says ✓ RESOLVED."

**Resolution pattern**: v1.14.8 (bug class S). Worker INSERTs sfa on URL/upload success; schema v44 backfill catches existing rows; `failures_total` rewritten to mirror breakdown shape so the count + glyph + filter all agree once sfa is written. JS unchanged — the COALESCE(sfa, themes) read path from v1.13.54 picks up the new write naturally.

### N. SET URL with SoundCloud → "Video unavailable"
**Complaint**: "I pasted a SoundCloud URL into SET URL, motif accepted it, then the download immediately failed with `[youtube] sc-stevecom: Video unavailable`."

**Resolution pattern**: v1.14.9 (bug class Q). The persistence path was wrapping the synthetic SC sentinel as `youtube.com/watch?v=sc-<artist>-<slug>`. `api_manual_url` and `api_override` both routed canonicalization by `url_source()` post-fix: YouTube → existing watch?v= reconstruction, SoundCloud → store verbatim, unknown → 400.

---

## 6. Tried and Removed

### A. Three-volume Dockerfile layout (v1.3)
**Tried**: /themes, /media/movies, /media/tv as separate mounts.
**Removed**: v1.4.0. Replaced by /config + /data two-volume model.
**Reason**: Hardlinks don't cross volumes; separate staging tree layout confusing.

### B. IGNORE scan decision (v1.6.1)
**Tried**: IGNORE + KEEP both existed for findings; IGNORE = ack without committing, KEEP = permanent lock.
**Removed**: v1.6.1. Dropped IGNORE; two ways to "do nothing" was confusing.
**Reason**: KEEP's permanent marker (user_overrides) is the only useful state; IGNORE was redundant with leaving findings in 'pending'.

### C. Missing-themes banner (v1.10.10)
**Tried**: Topbar banner "N themes available in ThemerrDB; download all?"
**Removed**: v1.10.10. Dropped entirely.
**Reason**: UI clutter; library filters (status=unthemed, tdb_pills=tracked) are sufficient self-serve affordance.

### D. ANIME tab (v1.8.0 → v1.8.1)
**Tried**: Separate ANIME tab like MOVIES / TV.
**Removed**: v1.8.1. Dropped /anime route + template (though anime media_type filtering remains).
**Reason**: Anime is a flag on sections, not a separate library layout. Tab + 4K toggle + anime pill achieves same filtering without duplicate nav.

### E. COVERAGE tab (v1.7.0 → v1.8.1)
**Tried**: Dedicated /coverage page with stats + missing-themes list.
**Removed**: v1.8.1. Route → 302 to /, coverage.html deleted.
**Reason**: Stats moved to dashboard "Last Sync" panel. Missing-themes banner dropped. No need for separate tab.

### F. Multi-stage button label swaps (v1.13.15 → v1.13.18)
**Tried**: Button transitions DOWNLOAD → QUEUED → DOWNLOADING as job progresses.
**Removed**: v1.13.18. Reverted to single label approach.
**Reason**: Multi-stage swaps created race conditions (rapid polls + label changes = janky). DL/PL pills alone cover state; simpler.

### G. Title-cell spinner (v1.13.13 → v1.13.14)
**Tried**: Spinner in title cell when job_in_flight.
**Removed**: v1.13.14. Dropped; DL/PL pulse covers the signal.
**Reason**: Spinner + pill rendering together was redundant. Single DL/PL pill indicator is sufficient + less code.

### H. PLACEMENT MODE (v1.5.3, made optional → v1.12.x)
**Note**: Not removed, but de-emphasized. v1.5.3 added auto_place toggle (SYNC+PLACE vs DOWNLOAD-ONLY). Still present in settings but rarely used; most deploys leave default auto_place=true.
**Reason**: /pending page (v1.5.3) provides explicit review flow; auto_place toggle was redundant UX.

### I. ✓ DONE flash on busy → idle button transitions (v1.13.21 → v1.13.51)
**Tried**: Library SYNC PLEX + dashboard SYNC THEMERRDB buttons flashed `✓ DONE` for 1.5s when transitioning busy → idle. Pattern was the v1.13.21 `sawBusyScope` flag → flash → revert.
**Removed**: v1.13.51. Both flashes dropped. `setSyncButtonState('done')` now collapses straight to `idle`.
**Reason**: User feedback — the flash made the button look briefly clickable while the cascade was still settling (post-place refreshes, reprobe), and re-clicking during the flash kicked off duplicate work. Plus the flash didn't add signal beyond what the topbar mini-bar already shows. v1.13.51 also added an `opsSyncActive` gate so library/settings SYNC PLEX buttons stay locked through the full cascade (plex_enum + tdb_sync + reprobe + refresh queue all in flight).

### J. "+N OPS" overflow pill in topbar (v1.12.109 → v1.13.45)
**Tried**: When multiple queue ops ran concurrently (download_queue + place_queue + refresh_queue all in flight), topbar showed "+N OPS" pill alongside the main mini-bar. Click opened the drawer.
**Removed**: v1.13.45. Hidden unconditionally; element kept in DOM for future repurposing.
**Reason**: The pill counted concurrent ops but every extra was already visible as a card in the drawer when opened. Pill duplicated drawer content without adding navigation. The real signal users wanted — "how much is happening?" — is conveyed by the main mini-bar's stage_label + drawer content.

### K. /adopt-from-plex for the smart-TRY-NEXT ADOPT path (v1.13.44 → v1.13.47)
**Tried**: Smart TRY THIS NEXT's `adopt-and-ack` action POSTed to `/api/items/{mt}/{tmdb}/adopt-from-plex`.
**Removed**: v1.13.47. Rewired to `/api/plex_items/{rk}/adopt-sidecar` (the same endpoint the SOURCE-menu ADOPT button uses).
**Reason**: `/adopt-from-plex` requires `local_files.mismatch_state IS NOT NULL` — i.e., there must be an existing motif canonical that diverged from the placement. For the smart-TRY-NEXT case the row HAS NO local_files entry (the download failed), so the endpoint correctly 409'd with "no mismatch state to resolve". The SOURCE-menu adopt path (`/adopt-sidecar` via `adopt_folder()`) ingests the sidecar at the Plex folder into motif's themes_dir even with no local_files row — the right path. Lesson: smart-TRY-NEXT actions inherit row state from the recovery context; pick the endpoint that matches the row's actual shape, not the one that "feels right" by name.

### L. /clear-failure as title-global only (v1.10.50 → v1.13.54)
**Tried**: `/clear-failure` always wrote to `themes.failure_acked_at` (title-global).
**Removed**: v1.13.54. Endpoint now accepts optional `section_id` param; with it, writes to the new `section_failure_acks` table instead.
**Reason**: cross-section bleed (see bug class K). When the chained `adopt-and-ack` / `purge-and-ack` flows fired with a section_id-scoped action, the follow-up `/clear-failure` cleared the global flag for every section that owned the title. Per-section ack closes the bleed; legacy bulk path still uses title-global semantics when section_id is omitted.

---

## 7. Conventions Emerged

### A. UI Label styling: // PREFIX
**Pattern**: Buttons and nav labels prefixed with `//` for CRT/monospace aesthetic. Examples: `// DOWNLOAD`, `// REFRESH FROM PLEX`, `// SYNCING WITH PLEX`.
**When introduced**: v1.5.2 (typography swap to VT323).
**Consistency**: Uniform across nav, buttons, status text.
**Code**: HTML/Jinja literals with `// ` prefix (not CSS-generated).

### B. Inline version markers: # vX.Y.Z:
**Pattern**: Code comments mark when a feature/fix landed. Example: `# v1.11.43:` or `# v1.13.0:` in code near logic.
**Purpose**: Reverse-engineer which release introduced a behavior.
**Consistency**: Sparse (not every line); used for load-bearing fixes (e.g., orphan-promotion deferral, pi.theme_id stamp).

### C. Co-author trailer
**Pattern**: Every commit ends with `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`.
**Consistency**: 100% of releases v1.5.3 onward.

### D. Single-line commit message + detailed body
**Pattern**: First line is terse action verb (e.g., "fix(pending): use file_path column"), followed by blank line, then multi-paragraph explanation.
**Example**: v1.5.5 one-liner + explanation of local variable name confusion.
**Rationale**: Commit subject searchable; body explains WHY (not just WHAT).

### E. Hash-skip pattern for innerHTML swaps
**Not explicitly documented in commits, but evident**:
When library.html needs to re-render rows (rapid-poll), naive innerHTML replacement flickers. Code uses targeted updates (querySelector + textContent updates, not full re-render) or hash-tracking to skip unchanged rows.

### F. Optimistic placeholders for click→busy gaps
**Pattern**: After user action (DOWNLOAD button click), immediately paint button + topbar state to "busy" before async response. Don't wait for API ACK.
**Example**: v1.11.48 paintTopbarSyncing(label) fires on click, not on poll.
**Benefit**: Hides latency; user sees instant feedback.

### G. Bump-before-tag protocol (v1.13.79)
**Pattern**: bump `app/__init__.py` `__version__` BEFORE creating each git tag.
**Why**: the constant drives the topbar brand display (e.g. `MOTIF v1.13.79`) AND the GitHub release-check comparison. Forgetting it leaves the UI showing the previous version even though the deployed Docker image is newer. Pre-protocol the constant silently drifted v1.13.73 → v1.13.78 (five tags). Now codified in CLAUDE.md and pinned by `tests/test_v1_13_79_link_fixes.py::test_version_string_matches_current_release`.

### H. Mirror principle as a debug heuristic
**Pattern**: any time you see a (count, breakdown, filter) trio for the same surface — pill counts, click-cycle breakdowns, library filter — check they share predicate. Drift is silent and chronic; bug class P caught four instances in two weeks.
**Implementation discipline**: every change touching one of the trio members must update all three atomically AND ship a static-text or behavioral test that pins the shared predicate. v1.14.8's rewrite of `failures_total` to mirror `failure_tab_breakdown_rows` shape is the canonical example.

---

## 8. Active Known Issues / Debt

### A. Multi-section items + hardlink sibling detection edge cases
**Status**: Mostly fixed (v1.11.0 schema + v1.11.65 pi.theme_id stamp), but edge cases remain.
**Flag**: Comments in worker._do_download and adopt.py warn of brittle hardlink-across-sections logic.
**Scope**: Low frequency; only affects multi-library layouts.

### B. FolderIndex path-translation variability (Unraid, symlink)
**Status**: Partially addressed (v1.11.78 cached_folder_path in place worker).
**Flag**: plex_enum._candidate_local_paths attempts multiple path prefixes; place_theme falls back to FolderIndex lookup.
**Scope**: High (Unraid users reported path mismatches).
**Workaround**: Use cached_folder_path when available; /api/debug/stat-folder for diagnostics.

### C. SQLite 'database is locked' under load
**Status**: Mitigated (v1.11.51 retry logic), not solved.
**Flag**: Worker has built-in retry; API writes use connection pooling (max_overflow=5). Still possible on high concurrency.
**Scope**: Low (homelab scale; single-user typically).
**Workaround**: Deploy with WAL mode + increased timeout; avoid bulk operations during sync.

### D. Orphan-promotion UNIQUE conflicts (pre-delete may miss cross-section rows)
**Status**: Mostly fixed (v1.11.73 pre-delete all child tables at target ID).
**Flag**: v1.11.73 comment warns that orphan-promotion assumes orphan is most-recent action; could theoretically fail if concurrent bulk updates target the same ID.
**Scope**: Very low (would require specific race setup).

### E. Plex folder rename detection (reconcile_placement_paths) may miss edge cases
**Status**: Works for 95%+ cases (v1.10.8).
**Flag**: Uses media_folder string match vs plex_items.folder_path; some path-translation libraries (Unraid, Docker symlinks) may not normalize identically.
**Scope**: Medium (Unraid users).
**Workaround**: Force re-enum via REFRESH FROM PLEX.

### F. TDB pill color selection complexity
**Status**: Mostly working (v1.10.40, v1.10.42, v1.10.44, v1.13.1 incremental fixes).
**Flag**: computeTdbPill JS classifier has 8+ branches (tracked → green, cookies → yellow/amber, removed → red, dropped → gray, dead → red, no-theme → no pill). Precedence tuning in multiple commits.
**Scope**: Low (UI only; no data loss).

### G. Unmanage path-string matching (Unraid host vs container paths)
**Status**: Fixed (v1.11.78 pi.theme_id + pi.folder_path lookup).
**Flag**: Pre-fix used folder_path string match; container /data != host /mnt/user/data.
**Scope**: High (Unraid users).

### H. Per-section refresh lock timing (anyEnumRunning vs anyEnumInFlight)
**Status**: Fixed (v1.11.75 unified to anyEnumInFlight).
**Flag**: Per-section REFRESH should still be clickable when one section finishes; global REFRESH (settings page) should stay locked until all done.
**Scope**: Low (multi-section users with rapid clicks).

### I. "Code review" pass flagged in v1.12.50
**Note**: Commit message notes "Four issues from the latest audit pass"; resolves them but indicates more may exist.
**Scope**: Unspecified.

### J. api_item async refactor + /api/library subquery cleanup are off-limits
**Status**: Two refactor candidates flagged in audits but explicitly DO-NOT-TOUCH per the user (`motif_perf_offlimits` memory). Both are regression-risky; the wins are theoretical and the surface area touches every library page render.
**Scope**: Don't attempt without explicit ask. Note when reading audit output that flags either as a "performance opportunity" — leave them.

### K. GitHub release-check 404 (cosmetic)
**Status**: Visible in v1.14.x logs. The release-check call to GitHub returns 404 occasionally (likely transient API behavior or a stale URL). Doesn't affect functionality — just noise in the log stream.
**Scope**: Low (cosmetic only).

---

## 9. Development Meta-Patterns

### A. Release cadence and scope
- Early (v1.4–v1.6): Rapid feature iteration (volume layout, Plex enum, orphan handling).
- Mid (v1.7–v1.10): Stability + UX refinement (nav, button locking, filters, pills).
- Late (v1.11–v1.13): Architecture maturation (per-section schema, git transport, TDB tracking).

### B. Test coverage
- Per-feature test suite (tests/test_canonical.py, tests/test_normalize_titles.py, tests/test_sync_git.py, etc.).
- Fresh migrations tested via hard-stop policy (enforces migration correctness).
- No end-to-end test suite mentioned; validation is manual or implicit via commit testing.
- **Static-text guards on production SQL / templates**: every (count, breakdown, filter) trio change ships a static guard asserting the shared predicate appears in all three sites (or the inverse — a banned string isn't reintroduced). Test count grew from 218 (v1.13.68 baseline) to 501 (v1.14.8) — most additions are guard tests for the mirror principle.
- **Comment-strip helper trap**: rationale comments quoting deleted strings ("removed YouTube here, was 'youtube url'") trip "must not appear" guards. v1.14.4 introduced `_strip_line_comments` helper — strip `#` / `{# … #}` / `// …` lines before the substring search. Applied uniformly across copy-pass tests since.
- **Per-module test sweep** (v1.13.81-83 audit follow-up): targeted test files added for worker, placement+normalize, plex_enum, adopt. Bare modules remaining: plex.py (thin httpx wrapper, low value), auth.py (security boundary, separate design pass), nfo/tmdb/tvdb/scanner (smaller surfaces).

### C. Deprecation path
- Stale references dropped immediately post-migration (v1.11.0, v1.11.1).
- Removed code (movies_themes_dir, /coverage, ANIME tab) → routes 302 or deleted templates.
- Backward-compat at DB level maintained (legacy rows still valid; new code just doesn't write them).

### D. Issue surface patterns
- User reports in commit bodies (quoted feedback, observed behavior).
- Diagnostic endpoints added post-issue (/api/debug/stat-folder, per-item sync logs).
- No issue tracker visible in commit messages; all context in the message itself.

---

## Addendum: High-Value Debugging Checklist

When a future Claude debugs motif, prioritize:

1. **UNIQUE constraint violations**: Check for stale orphan rows at the target ID before promotion. See v1.11.73 pre-delete pattern.
2. **Placement no_match failures**: Enable cached_folder_path lookup in place_theme; fall back to FolderIndex only if needed. See v1.11.65.
3. **Plex cache stale-P**: Trigger per-item Plex refresh (HEAD request) after delete/unmanage. See v1.10.28.
4. **Edition sibling DL=off**: Check pi.theme_id stamp; hardlink sibling rows. See v1.11.43, v1.11.64.
5. **Topbar not updating**: Verify /api/stats not cached; call refreshTopbarStatus past TTL. See v1.11.69.
6. **Button text race**: Don't swap button.textContent during flight; use adjacent pills. See v1.13.18.
7. **FolderIndex cross-section bleed**: Use per-section cache (_index_for_section). See v1.11.0.
8. **Path mismatches on Unraid**: Use pi.folder_path + folder_has_theme_sidecar translation. See v1.11.78.
9. **Orphan promotion FK errors**: Use PRAGMA defer_foreign_keys inside txn. See v1.11.71.
10. **Database is locked**: Retry 'database is locked' only; other OperationalErrors propagate. See v1.11.51.


---

## 10. Digest: v1.14.11 → v1.14.42 (post-SoundCloud era)

**Coverage window**: 2026-05-10 → 2026-05-11. 32 tags. Schema bumps: v44 (sfa backfill, v1.14.8) → v45 (themes.last_probed_at, v1.14.28). 696 → 824 tests.

### A. +P / Pp composite chain (v1.14.10 → v1.14.17)
The yellow-dot indicator on T/U/A/M chips when Plex serves its own theme alongside motif's. URL token `Pp` (URL form-encoding turns `+` into a space, breaking round-trip), button label keeps `+P`. v1.14.11/13/17 closed three rounds of leak: pill query parser, P-narrowing to pure-P (composite excluded), Pp deep-link parser. Recurring class **R** (token chain leaks across ~6 layers per axis).

### B. Pagination + bulk-action UX (v1.14.14-16)
- v1.14.14: FIRST and LAST shortcuts on the pagination strip
- v1.14.15: ACK FAILURES + PUSH TO PLEX gain bulk no-selection path (act on visible rows when nothing's checked)
- v1.14.16: REVERT MISMATCH moves between KEEP CURRENT and PUSH TO PLEX in the SOURCE menu

### C. Live-ops drawer polish (v1.14.18-21)
- v1.14.18: INFO card cleanups — previous_url hidden when redundant, RESOLVED VIA color matched to source
- v1.14.19: live-ops "+N running" labelling + queue retry hint visibility
- v1.14.20: INFO card audit fixes (H1+H2+H3+M1+M2+M3+L1) — multiple small surfaces tightened
- v1.14.21: list multi-running titles + hide ELAPSED on pending+0-progress

### D. Audit Wave 1 (v1.14.22 → v1.14.25)
Surgical mirror-principle + cancel-pipeline fixes from the 65-finding audit (`docs/CODEBASE_AUDIT.md`).
- v1.14.22: bulk ACK passes section_id; buildLibraryFilterParams threads attn_pills (frontend H2+H3)
- v1.14.23: tdb_pills=dropped + pl_pills=broken JS layer leaks closed (frontend H1+H4)
- v1.14.24: api_manual_url + api_upload_theme switch from title-global `failure_acked_at` to per-section sfa write (api H2)
- v1.14.25: cancel pipeline robustness — `_mark_done`/`_mark_failed` gain `WHERE id = ? AND status = 'running'` guard; cooperative-cancel checkpoints in `_do_download` / `_do_place` / `_do_relink` (worker H1+H2)

### E. LET PLEX SERVE redesign (v1.14.26-27)
The big UX pivot from the user: LET PLEX SERVE was a one-way trip (PURGE deleted both motif's canonical AND the placement). v1.14.27 rewires to `/unplace` — deletes ONLY the placement file at the Plex folder, keeps motif's canonical in `/themes/` so PUSH TO PLEX recovers without TDB. M-row state mismatch fixed via `_rks_we_actually_touched = rk_clear & rk_from_placement` (don't flag-flip non-placement rks). v1.14.26 also: amber LET PLEX SERVE button (was violet — collided with U chip), header checkbox tri-state fix, CSV UTF-16 LE BOM via DataView for Excel-on-macOS.

### F. PROBE TDB URL feature (v1.14.28-29)
the user's safety-net concern — "what if the URL TDB provides is dead and I can't revert?" Single-row /probe-tdb endpoint (v1.14.28) + bulk PROBE TDB URLS background job (v1.14.29). 24h cooldown via SQL filter. Schema v45 adds `themes.last_probed_at TIMESTAMP`. cookies_expired = indeterminate (don't change row state); video_private/removed/age/geo (`needs_manual_override`) get failure_kind written preemptively. Bulk gated by opt-in confirm checkbox in LET PLEX SERVE flow.

### G. Audit Wave 2 — secondary stats sync (v1.14.30-36)
- v1.14.30: `_FAILURES_SFA_FROM_SQL` + `_FAILURES_SFA_WHERE_SQL` constants codify the sfa-aware predicate; six secondary stats consumers wired (api H1+H3+H4)
- v1.14.31: `/api/pending/count` adds `OR lf.mismatch_state = 'pending'` to mirror `/api/pending` page (api H5)
- v1.14.32: `m_available` keys on placements PK `(media_type, tmdb_id, section_id)` instead of never-set `plex_rating_key` (the user repro: ADOPT + LET PLEX SERVE on already-adopted rows)
- v1.14.33: PROBE TDB URL targets TDB URL specifically (single-row + bulk) — was `(override or TDB)` per v1.14.28, returned alive on a working override masking dead TDB (the user repro: 13 Assassins)
- v1.14.34: `_do_adopt` writes per-section sfa instead of title-global `themes.failure_acked_at` (closes the third user-resolves-locally bleed site after v1.14.24's SET URL + UPLOAD MP3 fixes)
- v1.14.35: section_id scope on `accept_update` + `unmanage` + `forget` (api M3+M4) — pending_updates + user_overrides reads
- v1.14.36: `storage_copies_bytes` JOIN includes `section_id` (api M2) — kills KPI inflation on multi-section copy placements

### H. Audit Wave 3 — perf + cleanup (v1.14.37-38, v1.14.41)
- v1.14.37: hash-skip on `refreshTopbarStatus` (frontend P1) — 656-line DOM-paint body skipped when /api/stats payload is byte-identical to prior tick
- v1.14.38: 5-item deadcode sweep — `_enqueueing` `'restore'` → `'restore-canonical'` typo (real bug: boostPoll never fired for RESTORE), `STAGE_TIMELINE_QUEUE` const + empty forEach in ops.js, dead `data-act === 'clear-failure'` row branch, unused `settingsRefreshBusy`, `fmt` shadow rename in `renderSyncHistory`
- v1.14.41: `/api/items` GET LIST endpoint deleted (audit M1) — was the legacy "Browse" page's data source with a 4-query N+1; the page (`browse.html`) was retired when /movies, /tv, /anime took over via /api/library, but its endpoint stayed orphaned. Total: 301 lines removed (95 server + 206 JS + browse.html template). Plus 4 silent-fail action handlers (override-save, deleteOrphan, acceptUpdate, declineUpdate) upgraded from `loadItems().catch()` to `loadLibrary().catch()` — library page now refreshes immediately after these actions instead of waiting 30s for auto-poll.

### I. LPS state design iteration (v1.14.39 → v1.14.40 → v1.14.42 hotfix)
The post-LET-PLEX-SERVE state (motif's canonical preserved + no placement + Plex serves its own) initially polluted // NEEDS WORK because it matched the existing `attn_pills="await"` predicate (DL+!PL → "needs placement"). Three-pass design:
- **v1.14.39 (rejected after the user feedback)**: blue PL chip + blue title `!` glyph + filter exclusion. the user's complaint: blue PL collided with blue pending-update title glyph; PL chip should honestly read "no placement" (gray) not carry intent.
- **v1.14.40**: LPS signal moved to a NEW `PS` chip in the LINK column (amber, matching motif's "Plex" color vocabulary). Reverts v1.14.39 visual changes. PL falls back to gray, title `!` no longer fires for LPS rows. LPS-aware recovery card: hides LET PLEX SERVE (already done) + ADOPT + LET PLEX SERVE; surfaces PUSH MOTIF'S THEME (priority 0, undoes LPS via existing /replace endpoint), REVERT TO USER URL (if previous_url with kind='user'), RE-DOWNLOAD FROM TDB (non-orphan rows).
- **v1.14.42 hotfix**: my v1.14.40 placement-detection query lived OUTSIDE the `with get_conn(db) as conn:` block in `api_recovery_options` → `sqlite3.ProgrammingError: Cannot operate on a closed database` on every INFO card open. Moved the `is_lps` derivation INSIDE the with-block. Same hotfix tag also widened the probe `indeterminate_set` to include UNKNOWN + NETWORK_ERROR (the user repro: KWXXC228g24 returned `Unknown error` red ✗ for yt-dlp's intentionally-ambiguous "This video is not available" → now amber `?` with kind-aware hint text).

### J. New invariants from this window

- **`is_lps` discriminator** (v1.14.39+): `file_path is not None AND media_folder IS NULL AND plex_independent_theme=1`. Mirrored in three places: `api_recovery_options` (server, inside with-block), JS `lpsState` const (renderLibraryRow), JS `_row_matches_pl` `is_lps` (post-stat filter). Drives: PS LINK chip render, exclusion from `attn_pills="await"` / `pl_pills="await"` / `pl_pills="broken"`, recovery-card option set.
- **Probe `indeterminate_set`** (v1.14.42): `{COOKIES_EXPIRED, NETWORK_ERROR, UNKNOWN}` — yt-dlp errors that aren't definitive enough to write `failure_kind`. Renders amber `?` with kind-aware hint instead of red `✗`. Distinct from `FailureKind.needs_manual_override` (the "definitively dead" set: video_private/removed/age/geo).
- **Section-id contract progress**: SET URL (v1.14.24), UPLOAD MP3 (v1.14.24), ADOPT (v1.14.34), bulk ACK (v1.14.22), api_accept_update / api_unmanage / api_forget (v1.14.35) all converted to per-section sfa writes / section-scoped reads. **Title-global writes still survive in**: `api_clear_failure` legacy/all-sections path (legitimate — title-global ack when every section is acked); `db.py` schema migration backfills (one-time, fine). The user-resolves-locally + section-write surfaces are all per-section now.
- **LINK column vocabulary**: `HL` (hardlink, orange) / `C` (copy, violet) / `M` (mismatch, magenta) / `PS` (Plex-serving — LPS state, amber, v1.14.40) / `—` (none, gray).

### K. Recurring patterns reconfirmed

- **Mirror principle drift (class P)** — 6 instances closed in v1.14.30-36 alone. The audit found them clustered in stats endpoints; the user reproed two in the wild post-fix. Pattern: same predicate lives in (count, breakdown, filter) — change one, miss the others.
- **Cross-section bleed (class K)** — closed at SET URL/UPLOAD MP3 (v1.14.24), ADOPT (v1.14.34), bulk ACK (v1.14.22), and the section_id-scope cluster (v1.14.35). v1.14.24's `auto:set_url` / `auto:upload` / `auto:adopt` actor labels distinguish in audit log.
- **Pill-axis chain (class R)** — confirmed in tdb_pills="dropped" (v1.14.23) + pl_pills="broken" (v1.14.23). The Theme 3 class fix (single config block at top of app.js) remains DEFERRED — flagged as high-blast-radius frontend refactor.
- **Test brittleness from over-pinning** — every revert / refactor in this window broke 1-3 pre-existing tests that pinned exact line shapes. Pattern: soften pinned literals to "either form" assertions when the contract holds across both shapes (see v1.14.30 `failures_total_query_joins_sfa` evolution; v1.14.40 brittle override-revert test fix; v1.14.42 indeterminate flag widening).

### L. Open at end of window (parked, not bugs)

- **Holistic recovery-options matrix audit** (post-v1.14.40 follow-up). the user's ask: "options that make sense across all (SRC × LINK × DL × PL) states". Plan parked at the table-out-and-review-deltas stage. Needs design discussion before scoping.
- **Pill-axis consolidation** (audit Theme 3 class fix). Single config block in app.js for every (axis, valid values, deep-link param, button selector) tuple. Higher-blast-radius frontend refactor — wants screenshot-verify time.
- **Color audit** (broader than the LPS-color discussion). the user flagged that amber is doing double-duty (Plex semantics + soft attention) but explicitly wanted to think more before any sweep.
- **`/api/items/{mt}/{tmdb_id}` SINGLE-item async refactor** — still off-limits per the prior memory note (sync→async needs lock-around-conn audit, INFO card depends on it). Distinct from the LIST endpoint deleted in v1.14.41.
- **Worker H3** (`_QUEUE_BURST_*` multi-worker race), **Worker H5** (sibling-hardlink waste) — audit MED, not blocking, lower priority than what shipped.

### M. Misc lessons

- **The hotfix that proved a test-coverage gap**: v1.14.40 shipped with all-static-text-guard tests, no runtime exercise of `api_recovery_options`. v1.14.42 was needed because the static guards pinned the SQL string but never ran the handler. A minimal TestClient bootstrap on `api_recovery_options` is on the optional follow-up list.
- **Color choice as load-bearing UX**: the v1.14.39 → v1.14.40 reversal (blue PL chip → PS LINK chip) cost ~50 lines of revert + new chip. Worth it. Lesson: when adding a visual variant, prefer a NEW slot (new column value, new glyph) over a render-variant of an existing slot — avoids the chip-color-collision audit Theme 3 keeps surfacing.
- **Off-limits items aren't always what they seem**: the audit M1 finding (/api/items N+1) sounded off-limits per the memory note, but the memory note was about the SINGLE-item endpoint. The LIST endpoint was orphaned dead code — safe to delete entirely. Always verify which "items" endpoint a flag refers to.


---

## 11. Digest: v1.16.10 → v1.16.11 (selection-wide cache + header master-toggle)

**Coverage window**: 2026-05-18 single session. 2 tags. Schema unchanged (still v45). 2267 → 2295 tests (+23 from v1.16.10 cache guards, +5 from v1.16.11 master-toggle guards).

### A. The bulk-action visible-page-only undercount (v1.16.10)

Pre-fix the user: "doing a select all is not reflecting correctly on the bulk actions" + "let plex server isn't displaying a number at all anytime." The root pattern: `libraryState.selected` (key Set) survived pagination via SELECT ALL FILTERED, but every bulk-bar count badge + click handler walked `libraryState.items` (visible page only) with a `selected.has(libKey(it))` filter. On 484 selected with page-size 50, every badge showed `(50)` and every handler silently dropped the 434 off-page rows.

Fix: new `libraryState.selectedRows` Map keyed by libKey, parallel to the `.selected` Set. Populated by SELECT ALL FILTERED pagination, per-row checkbox toggles, and a `syncSelectedRowsFromItems()` pass on every items render. Every `selected.clear()` paired with `selectedRows.clear()` (12 sites). Bucket-count walker + `effectiveCount()` walk `selectedRows.values()`. Each bulk handler uses a dual-mode source selector (`const source = useSelection ? selectedRows.values() : (libraryState.items || [])`). Adopt-selected + export-CSV retain their own per-click /api/library pagination (already correct).

Dropped along with the cache: the v1.13.35 + later off-page-selection warnings on PUSH / ACK / ADOPT+LPS, and the v1.15.60 LPS `onPlusPFilter` safety net + its bare-label-no-count branch (which was the actual cause of the user's "no number" complaint — a defensible patch for the undercount that masked the real bug for ~2 months).

### B. Header checkbox master-toggle (v1.16.11)

Pre-fix the `#library-select-all` click handler toggled visible-page selection only (v1.14.26 rule: "any visible selected → deselect visible"). the user: "when trying to clear the select all by clicking the check box next to title it gets into a weird stuck state … eventually it unchecks but still we get a weird selection of bulk actions while nothing is selected." A SELECT ALL FILTERED of 484 → click header → deselected the 50 visible → 434 stuck in the bulk bar.

Fix: master-toggle semantic.

  - `libraryState.selected.size === 0` → click selects all visible (unchanged)
  - `libraryState.selected.size  >  0` → click CLEARS EVERYTHING

Deselect branch mirrors `// CLEAR` button semantics — full clear of both selected + selectedRows, plus walk visible checkboxes to set `cb.checked = false` so the DOM matches state instantly. The visible-page tri-state RENDER in `updateLibrarySelectionUi` is unchanged (still drives checked/indeterminate from visibleSelected vs visibleCount). v1.15.61's explicit `headerCb.indeterminate = false; headerCb.checked = turnOn` writes before render preserved.

### C. LOGS-page secondary bar alignment + sticky thead bounce (v1.16.11)

the user: "left side is narrower" + "id, type, item bar bounces and disconnects from the top header." Two adjacent CSS issues:

1. **Height mismatch**: `.jobs-scroll thead th` inherited `.table-tight` padding (6px/10px); `.chips-bar` opposite uses 10px/18px → ~8px height delta at the same y-band. Bumped thead padding to 10px/18px so the two halves' secondary bars share a visual height.
2. **Sticky un-stick bounce**: `border-collapse: collapse` (global on `.table`) dropped the thead bottom border during the sticky un-stick transition. Replaced `border-bottom` with `box-shadow: inset 0 -1px 0 var(--line)` — box-shadow rides through sticky transitions cleanly. Pinned explicit `background: var(--bg-elev-2)` so the row stays opaque over scrolling content.

### D. New invariants from this window

- **selectedRows cache invariant**: every code path that mutates `libraryState.selected` must mutate `libraryState.selectedRows` in lock-step. The pair-with-clear pytest guard (`test_every_selected_clear_is_paired_with_rows_clear`) asserts call-site counts equal so a future refactor can't drop one side. Per-row checkbox uncheck + select-all-visible deselect call `.delete(k)`; header master-toggle deselect + every end-of-handler cleanup call `.clear()`.
- **dual-mode handler source**: `const source = useSelection ? libraryState.selectedRows.values() : (libraryState.items || [])` is the v1.16.10 canonical shape for any bulk handler that supports both selection-mode + visible-page-fallback (the v1.14.15 contract). Anchored in pytests across PUSH / REVERT / RESTORE / LPS / PROBE / ACK.
- **Header tri-state semantic split**: RENDER is visible-only (what's checked on this page), CLICK is full-selection-aware (master-toggle). Both rules co-exist; they're not in conflict because they answer different questions ("what does the user see now?" vs "what should clicking do?"). Pre-v1.16.11 the click question was answered with the render's data — that was the bug.

### E. Recurring patterns reconfirmed

- **Mirror principle drift (class P)** — the cache fix touched 9 handler walkers + a count walker + effectiveCount. Same predicate ("walk selection-eligible rows") was duplicated 11 times. Adding new bulk actions in the future must continue mirroring; the v1.16.10 guard tests pin the pattern across all 9 handlers.
- **Test brittleness from over-pinning** — 7 pre-existing tests broke because they pinned exact pre-fix line shapes (e.g. `"if (useSelection && !selectedKeys.has(libKey(it))) return false"`). Updated to pin the contract (dual-mode source selector exists, selection-wide cache walked, off-page warnings gone), not the literal old shape. Same lesson the user flagged in v1.14.x; the v1.16 cache reshape was a fresh stress test.

### F. Open at end of window

- **PROJECT_HISTORY v1.14.43 → v1.16.9 digest gap** (~140 tags). Acknowledged in the header note. The per-line `# vX.Y.Z:` markers + commit messages cover the gap for now; a future session can run a digest extraction pass. Not blocking.
- **Worker H3 / H5** (multi-worker locks, sibling-hardlink waste) — still open per the AUDIT_WORKER.md table; rare-trigger races, no user-visible bugs.
- **Color audit** (amber doing double-duty for Plex semantics + soft attention) — the user explicitly parked this in v1.14.x.
- **`/api/items/{mt}/{tmdb_id}` SINGLE-item async refactor + /api/library subquery cleanup** — explicit "off-limits without ask" per auto-memory `motif_perf_offlimits`.
- **Operational debt**: RATE/HR 60→120 bump, CONCURRENCY 1→2-3 bump, fresh cookies.txt drop in /config — per auto-memory `motif_open_followups`. Not code changes; runtime / config moves.

### G. Misc lessons

- **A safety net can become the bug.** v1.15.60's `onPlusPFilter` LPS fallback was added when `lpsOnlyCount=0` despite the filter being active — a defensible patch for the symptom. Two months later the user reported the secondary symptom the safety net created (bare-label LPS button). The fix was to attack the count, not the visibility — once `lpsOnlyCount` is selection-wide, the safety net comes out cleanly and the LPS button has a count whenever it's visible. Pattern: when a hack masks an undercount, the undercount stays around until something forces it to the surface.
- **Master-toggle vs tri-state for paginated tables**. Pure tri-state (visible-page only) was correct when pages didn't exist. The moment a "select-all-across-pages" affordance appeared (v1.10.49's SELECT ALL FILTERED), the visible-only mental model broke — users reach for the header as a "kill switch for the whole selection." Two-state master-toggle answers what they actually want; tri-state visual feedback still tells them what's selected on the current page.
- **`border-collapse: collapse` + sticky `<th>` = vanishing border on un-stick.** The thead bottom border lives on each cell; the collapse arithmetic gets ambiguous during the sticky transition and the border can disappear for a frame. `box-shadow: inset 0 -1px 0 var(--line)` is the durable substitute — shadows aren't subject to collapse logic and ride through sticky cleanly. Worth noting for any future scrollable table on /queue or beyond.
## 12. Digest: v1.14.43 → v1.16.9 (post-v1.14.42 → pre-v1.16.10)

**Coverage window**: 2026-05-11 → 2026-05-18 (a week of dense
churn). 215 tags. Schema bumps: v45 → v46 (`plex_sections.last_enum_content_changed_at`, v1.14.74) → v47 (`op_progress.kind` CHECK widened for `bulk_probe_tdb`, v1.14.93) → v48 (`op_progress.status` CHECK adds `'pending'`, v1.15.48) → v49 (`op_progress.kind` CHECK adds `'bulk_lps'`, v1.15.56). 824 → 2267 tests (+1443; the cookies / probe / silent-failure / SSR / design-system arcs all shipped guard-test bundles in parallel with the behavior changes).

### A. LPS-flow completion (v1.14.43-49, v1.14.95, v1.15.0-5, v1.15.28, v1.15.86, v1.15.95)
Closes the v1.14.40 LET PLEX SERVE design that § 10.E shipped. v1.14.43 wires the missing `PS` token through the LINK pill // ALL handler (class P drift — pill axis token-chain leak across yet another layer; same shape as v1.14.23). v1.14.44 gates LPS on "motif owns a placement" (was firing on pure-P rows where there was nothing to unplace). v1.14.45 adds DOWNLOAD TDB BACKUP for pure-P + TDB rows (let-the-user-grab-a-canonical-before-LPS path). v1.14.47 reshapes the recovery card: TRY THIS NEXT becomes failure-only; non-failure actions migrate to the SOURCE menu. v1.14.49 + v1.14.51 close the probe-alive feedback loop — a successful re-probe clears stale `failure_kind` on the row + on every sfa column. v1.15.1 adds a selection-scoped pre-flight probe to bulk LPS so dead-TDB rows in the selection are caught before unplacing. v1.15.28 ships the server-side bulk-LPS composite (one click → one op, fresh-target skip). v1.15.95 then reverses the gate logic — "skip if TDB dead" turned out to drop A/U rows that don't depend on TDB at all; switched to "canonical exists" as the real safety check (the user's repro: 4 U+P red-TDB rows that should have been LPS-eligible). v1.15.106 closes a stamping gap (BULK LPS now updates `plex_items.local_theme_file=0` so the next render reflects the unplaced state).

### B. The Plex-perf trilogy (v1.14.74-78)
the user: "can we go back to the plex scans speed now". Three independent levers from the v1.14.71 research note land back-to-back. v1.14.74 (schema v46): section-level delta gate via Plex's `contentChangedAt` attribute — stable libraries (~90% of daily-cron refreshes) drop from 30s-2m per section to ~50ms. Race-safe because the stamp stores the OLD live value read at enum start, so any bump mid-enum re-fires next tick. v1.14.75: `excludeElements` query param on the per-item Plex payload (drop Media / Genre / Country / Role from the XML — motif doesn't touch them). v1.14.76: bulk `/library/metadata?id=1,2,3` fetch in the show-folder fallback (N round-trips → 1). v1.14.78: switch Plex client XML → JSON parse (saves the lxml object-tree allocation per response). Combined: Plex enum on the user's library went from "noticeable" to "instant" for the stable-section common case.

### C. The v1.14.50 → v1.15.0 audit bundle wave (v1.14.50-61)
A holistic codebase audit ran across v1.14.50-58 (5 bundles, B through E3, surfacing ~65 findings — same playbook as the § 10.D-H "Audit Wave" cycle but reset to a fresh sweep over the v1.14.43+ surface area). v1.14.52 (Bundle B) closed 3 HIGH bugs: the PS-token mirror leak above; `_do_refresh` reporting fake-✓-DONE on missing `rating_key` payloads (`if rk:` gate dropped the dispatch failure on the floor — now raises `_JobPermanentFailure`); and `update_tab_row` using a pre-v1.13.86 permissive shape that disagreed with its sibling `update_tab_breakdown_rows`. v1.14.53-57 burned through Bundles C-E3: LPS-flow polish, silent-failure log-hygiene, dead-code, schema/security nits, log-hygiene + safety nets. v1.14.59 added behavioral test coverage for `api_recovery_options` (the § 10.I lesson — static-text guards weren't enough; the v1.14.42 hotfix was the evidence). **v1.14.60 was a CRITICAL self-inflicted HOTFIX**: v1.14.54's `_scrub` substring set added bare `"key"` to catch `api_key`, which silently redacted `rating_key` / `cache_key` / `theme_key` / `partition_key` etc. in events-table audit data — exactly the breadcrumb the phantom-P bug class (CLAUDE.md class A) needs to debug. Narrowed to explicit `private_key` / `signing_key` / `encryption_key` / `master_key`. v1.15.0 was the audit-cleared rollover cut — found 2 last bugs pre-tag (a `_upgrade_to_v47` migration that didn't restore `idx_op_progress_finished` + a `PlexClient.close` silent-swallow), shipped both, then bumped major-line.

### D. Cookies UX family (v1.15.8, v1.15.11-12, v1.15.14-17, v1.15.23, v1.15.43, v1.15.121)
The pre-window state: cookies-needed rendered as an amber TDB pill that read visually as "Plex problem." v1.15.8 added a `// TEST COOKIES` button + structured validation. v1.15.11 closed a bulk-probe writeback race via per-call cookies snapshot (3 workers all reading the same `/config/cookies.txt`, each with their own yt-dlp temp pointer, racing on a re-fetch). v1.15.12 was a major recovery: a deployed bulk-probe + REPROBE flagged 2005 rows as DEAD because YouTube IP-rate-limited motif and yt-dlp returned `"Video unavailable. This content isn't available, try again later. The current session has been rate-limited by YouTube..."` — that prefix matched the `VIDEO_REMOVED` (definitively-dead) classifier branch. Reclassified rate-limit as transient, dropped the pool size 3 → 2 (under YouTube's unauth threshold). v1.15.14 dropped probe concurrency further to 1 + early-bails on rate-limit. v1.15.16 bumped the `yt-dlp` floor to 2026.3.17 + logs the running version on startup (the user's recurring "is it a yt-dlp regression or motif?" diagnostic). **v1.15.17 was the cookies UX rework**: new `--yellow` token + `--tone-cookies`, ⚿ glyph (U+26BF squared key) replacing the generic ⚠ on the row pill, new topbar `#topbar-cookies-badge` chip, `attn_pills=cookies` STATUS filter button, INFO-card `FIX COOKIES` action (links to /settings#paths). Five-surface family in warm yellow visually divorced from the amber Plex semantics. v1.15.43 retuned to "biscuit brown"; v1.15.121 retuned again to "lemon gold." Each retune was a color-token sweep, not a behavior change.

### E. The probe lifecycle (v1.14.49-51, v1.15.5-7, v1.15.10, v1.15.15, v1.15.20, v1.15.33, v1.15.38, v1.15.58)
Built around the v1.14.28-29 PROBE TDB URL feature from § 10.F. v1.14.49 (probe-alive clears stale stored failure state) + v1.14.51 (bulk-probe parity for the same clear) closed the recovery half of the loop. v1.15.5 added the `FORMAT_NOT_AVAILABLE` classification (yt-dlp couldn't pick an audio stream) + queued-downloads badge. v1.15.7 added a stale-op filter to the probe watcher (interrupted operations were leaving the badge stuck). v1.15.10 shipped `// REPROBE FAILURES` (bypass cooldown to recover red-pilled rows after a YouTube IP-throttle window passes). v1.15.15 aligned REPROBE FAILURES SQL with the canonical FAIL chip predicate (mirror-principle drift — class P fixed again). v1.15.20 added `scope_items` bypass of the 24h cooldown for explicit operator-requested re-probes. v1.15.33 widened REPROBE FAILURES to include acked failures (the user's repro: acked rows stayed acked even after URL came back alive). v1.15.38 dropped ack predicates from the yellow ⚿ pill filter + count so they match the row-level pill. v1.15.58 dropped the `cookies_present` gate from TDB ⚠ pill filter (a cookies-missing config didn't change which rows had failures).

### F. SSR flash-bug sweep (v1.15.21-22, v1.15.25, v1.15.45, v1.15.50, v1.15.55, v1.15.64-65)
the user: "lets do a flash bug sweep." Pre-sweep every tab nav repainted nav-availability + topbar chips + dashboard stat cards from "—" → real values on the first JS poll tick (1s `/api/stats` TTL cache window made the flash unmissable). v1.15.21 SSR'd tab availability. v1.15.22 broadened to topbar chips + banners. v1.15.25 added SSR for Settings ENV badges + library resolution chips. v1.15.45 was Phase 1: `_dashboard_ssr_state()` helper baking 11 stat-num cells into the initial dashboard render. v1.15.50 Phase 2: PLEX MOVIES/TV/ANIME + TDB foots (13 more fields), template `ssr_num(X)` macro consolidating the `{{ "{:,}".format(X) if X is not none else "—" }}` conditional. v1.15.55 SSR'd the op-mini bar so live-ops persists across tab navs. v1.15.64 Phase 3: insight blocks + settings tabs. v1.15.65 caught 3 silent SSR-related bugs in the audit pass after Phase 3 landed. v1.15.78 closed an IDLE-flash race on cross-tab nav while an op was running.

### G. Dashboard rework (v1.14.70, v1.15.19, v1.15.27, v1.15.29, v1.15.31-32, v1.15.42, v1.15.44, v1.15.47, v1.15.72)
v1.14.70 split the top row from a single 5-card strip into four labeled sections (COVERAGE / OPERATIONS / PLEX LIBRARY / STORAGE) + added PLEX ANIME card + renamed ORPHANS to USER PROVIDED (the framing was alarming the user — "they're not orphans, they're rows themed by you, not in TDB"). v1.15.19 added TDB-specific labels + an ANIME tone + a derived `ANIME ThemerrDB-available` stat. v1.15.27 was an over-correction: dropped TDB cards entirely + led PLEX cards with coverage %. **v1.15.29 + v1.15.31 reverted it back to the pre-v1.15.27 shape** (TDB cards restored, ADDED TODAY / THIS WEEK kept as the only forward survivors of the revert). The lesson: TDB cards anchored the user's daily glance routine; the coverage-% view broke that mental model. v1.15.32 added per-card customize (reorder + hide individual cards across rows). v1.15.42 + v1.15.44 made dashboard cards click-through to `/queue?since=N`. v1.15.47 closed the TDB top-card foot math (themed ≤ total, anime included).

### H. Import workflow (v1.15.66-77, v1.15.84, v1.15.87)
v1.15.66 shipped bulk user-URL CSV import with a preview-first workflow. Export CSV gained a `Youtube_URL` column (populated only for `U`-source rows; non-U rows export blank cells so the round-trip is a no-op). New `POST /api/import/preview` parses (UTF-8 BOM, UTF-16 LE BOM, header aliases tolerated) + categorizes rows into clean / conflict / no_match / invalid_url / skipped. `POST /api/import/apply` writes user_overrides title-global (section_id=''), captures previous_urls for REVERT round-trip, audits with `source='import'`, cancels in-flight downloads + enqueues fresh. v1.15.67 added NO-TDB rows import + select-all label. v1.15.68-77 (8 polish rounds in one day) closed picker UX, info icon, DUP detection, download-only flag, button colors, URL compact rendering, IMDB link styling. v1.15.84 + v1.15.87 closed the column-spacing alignment + Apply-dropdown centering (a side-quest into `.input-tiny` having no CSS rule — see arc J below). v1.15.122 was a re-fix of cell-content centering ("real fix this time" — the alignment kept regressing as adjacent CSS landed).

### I. The v1.15.34-39 silent-failure audit (4-agent parallel sweep)
the user: "lets do a holistic silent failure check." Same playbook as the § 10 audit waves, scaled up. v1.15.34 (HIGH batch): 7 fixes across download-missing + bulk-download JSON coercion (silent malformed-body → fall-through to default-args path), bulk-probe cookies-snapshot fallback re-introducing the v1.15.11 race, mirror-compaction `shutil.rmtree(ignore_errors=True)` silently leaving old mirror dir on disk, `plex.enumerate_section_items` returning `[]` indistinguishably from a legit empty section, `_topbar_ssr_state` silent `except: pass` hiding FAIL/COOKIES/DISK LOW banners, `refreshTopbarStatus` + `openOverrideDialog` JS null-deref chains. Plus MARK ALIVE tone yellow → magenta (the user: visually grouped with FIX COOKIES, but FIX COOKIES is a config repair and MARK ALIVE is an operator override that masks a dead URL — muscle-memory hazard). v1.15.35 (MED): events.py retry, logging upgrades, JSON coercion. v1.15.36 (LOW tail). v1.15.37 audit-round-2: concurrency + data integrity + security + dead code. v1.15.39 systematic filter-logic audit: closed the v1.13.68 mixed-with-broken attn_pills punt (`broken` silently dropped when combined with `cookies` / `fail` / `update` etc. — added `attn_needs_post_stat` guard so the SQL fetch comes back unbounded + `_row_matches_attn` Python OR helper). v1.15.40 pinned the recurring patterns as regression guards: `BACKGROUND_OP_ROUTES` table asserts every singleton background-op route calls `log_event` BEFORE thread spawn (the "deploy looked fine but nothing happened" beacon class). 5 fresh class-P (mirror drift) instances closed; 3 fresh silent-`except` instances closed.

### J. Design-system birth (v1.15.87-99, v1.15.110-116, v1.15.118-119)
v1.15.87 surfaced a recurring CSS gap: the import-preview action select was rendered with `class="input input-tiny"` but `.input-tiny` had no CSS rule anywhere. Inherited only `.input`'s `padding: 8px 12px`, making the select taller than text cells → row stretched → "select sits at bottom" perception. Fix was a 4px/10px primitive but the meta-finding was bigger: invented class names with no backing rule are a silent gap class. **The `a06868c docs:` commit between v1.15.87 and v1.15.88 introduced `docs/DESIGN_SYSTEM.md`** + the CLAUDE.md pointer rule ("extract/follow the tokens + primitive classes before writing UI, mirror an existing sibling screen, never invent class names or inline hardcoded values"). v1.15.88 promoted `.input-tiny` as a real primitive. v1.15.89 added a JS classname hygiene lint (catches frontend invented classes). v1.15.96 + v1.15.98 + v1.15.111 caught mixed-attribute interpolation + class-mapping object-literal silent gaps + Jinja template invented classes. v1.15.99 caught a `var(--gap-2)` reference in a Jinja `style=` attribute where `--gap-2` was never declared (resolves to property's initial value — `margin-top: 0` silently for the backfill banner since inception). v1.15.110 added motion-duration tokens. v1.15.113 added `--<color>-rgb` triple tokens + migrated 185 rgba() sites. v1.15.114 migrated 137 CSS spacing sites onto the scale. v1.15.115-116 consolidated duplicate `.dlg-close` rules + added a CSS duplicate-rule lint. v1.15.118-119 added a hover-class lint that extends to `:focus` + `:active`. v1.15.120 added accessible names on glyph-only buttons. The cascade landed in two days because every fix added a lint that caught the next class of gap.

### K. The path-domain mismatch class (v1.15.90-92, v1.15.100)
Three sites where motif scanned CONTAINER paths (walking `/data/media/...`) but `plex_items.folder_path` stored Plex's reported HOST path on Unraid (`/mnt/user/...`). The pre-fix UPDATE keyed on `WHERE folder_path = str(media_folder)` missed on multi-volume setups → silent state drift (`pi.local_theme_file` stayed stale forever). v1.15.90 fixed unplace via natural-key lookup (theme_id). v1.15.91 fixed the place worker the same way. v1.15.92 fixed the scanner (couldn't use pure natural-key because orphan_unresolved rows may have theme_id NULL; two-stage fix with a candidate set built by inverting `_PATH_PREFIX_TRANSLATIONS`). v1.15.94 closed `api_clear_override` as a separate cross-section bleed (class K) — the lone override endpoint that wasn't section-scoped, silently deleted both sections' overrides on multi-section titles. v1.15.100 then shipped the `MOTIF_PATH_TRANSLATIONS` env var so non-standard mount layouts can declare their own host=container pairs (user pairs tried first, hardcoded defaults as fallback).

### L. HAMA / TVDB bridge (v1.15.143, v1.16.0, v1.16.2)
v1.15.143 added a diagnostic endpoint that reported 2054 stranded plex_items rows where motif's `themes` table had no `theme_id` link because Plex's TV Series agent (and HAMA for anime) gave only TVDB GUIDs, while motif's TDB sync produces TMDB-keyed records. v1.16.0 added `TMDBClient.lookup_by_tvdb()` (wraps `/find/{tvdb_id}?external_source=tvdb_id`, mirrors `lookup_by_imdb`, cached via existing `_cached_or_fetch`) + `bridge_tvdb_to_tmdb()` backfill loop. **The big realization**: 99.7% of stranded rows weren't anime — they were regular TV shows from Plex's default TheTVDB scraper preferring TVDB GUIDs. The "HAMA bridge" was a misnomer. v1.16.2 renamed the user-facing surfaces to `TVDB BRIDGE`. the user caught the mis-framing pre-build by pointing out `TMDBClient` + `tvdb_lookup_cache` were already in the codebase (`search_movie` / `search_show` / `lookup_by_imdb` already exist + cache, plus the `plex.tmdb_api_key` settings field) — collapsed the scope from 1-2 days to a single tag.

### M. Bulk-action density (v1.14.99, v1.15.4, v1.15.46, v1.15.49, v1.15.51, v1.15.54, v1.15.59, v1.15.124)
v1.14.99 closed the +P-narrows-primary-letters semantic the user asked for ("Pp + M should be M rows that are also +P, not M rows OR every composite row" — folds the composite requirement into the IN-clause as AND when both primary letters + Pp are selected). v1.15.4 added bulk DOWNLOAD TDB BACKUP for pure-P selections. v1.15.46 added bulk `// ADOPT + LET PLEX SERVE`. v1.15.49 split bucket selectors into `M` / `M+P` / `+P-only` with their own count badges (the M+P / +P split matters because LPS on M-sidecar would destroy the only theme). v1.15.51 + v1.15.59 standardized bulk button labels (count badges + RUNNING X/N format) across the family. v1.15.54 added per-job ACK button in the `/queue` failed-jobs list. v1.15.124 paginated bulk ADOPT to cover off-page selections (the same off-page-selection class § 11.A then closed at the cache layer). v1.15.60 added back the +P-filter-active path as a safety net on LPS visibility (the safety net § 11.A later confirmed was masking the real undercount bug — flagged as a known-suboptimal patch at the time).

### N. The v1.15.61 → v1.15.62 perf revert
v1.15.61 closed a FAIL-count-misses-orphans bug by adding an OR-join to `_FAILURES_SFA_FROM_SQL`: `JOIN plex_items pi ON (pi.guid_tmdb = t.tmdb_id AND pi.media_type = (CASE...)) OR pi.theme_id = t.id`. SQLite's OR-join optimizer bailed on the CASE expression in the first branch and degraded to ~cartesian scans, plus there was no `idx_plex_items_theme_id` — the theme_id side scanned the full table too. The constant is used in 3+ scalar subqueries inside `_topbar_ssr_state`, which runs on every page load. **60s+ page loads on the user's library**. v1.15.62 reverted the OR-join + added the missing index. Lesson: OR-joins with CASE-expression branches are an SQLite optimizer footgun; either UNION two queries or add the index first and trust it. v1.15.63 added a perf-budget regression test so a future ~30k-row plex_items wouldn't regress page-load to >2s in CI.

### O. Late-window polish + bulk-job recovery (v1.16.3-9)
v1.16.3-5: TMDB TEST KEY auto-dismisses + `manual-url` `download_only` correctness fix (was setting `force_place=False` when it should set `auto_place=False` — different effect on the worker pipeline; v1.16.5 then gated `force_place` on `pi.has_theme` so P-row manual URL replaces don't fire force-place on non-P rows). v1.16.6 → v1.16.8 was a UI revert dance: v1.16.6 moved LOGS chips out of the JOBS header into a sibling `.chips-bar` for both halves; v1.16.7 raised the rapid-poll ceiling 60s → 300s + added `visibilitychange` listener for stuck-amber DL chips after tab throttle (Chromium throttles `setInterval` to ~1/min in inactive tabs; the rapid-poll's 60s ceiling was tight enough that the first throttled tick already exceeded it → `clearInterval` → poll dead until user manually refreshed); v1.16.8 reverted v1.16.6's JOBS-chips move because the user's "make them line up" meant the EVENT STREAM side only, not both. v1.16.9 closed a phantom-P render mismatch (class A): the SRC chip render branch dropped the `verified_ok` gate that both `computeSrcLetter` (server SoT mirror) and the server `_SRC_LETTER_SQL` enforce — phantom-P rows correctly classified as letter='-' by both authorities still rendered as orange P chips, confusing the `T + -` filter ("filter is broken" — actually filter was correct, chip render was lying).

### P. New invariants from this window

- **TVDB→TMDB bridge** (v1.16.0): plex_items rows with `theme_id IS NULL AND guid_tvdb IS NOT NULL AND has_theme=1 AND included=1` are the stranded set. `TMDBClient.lookup_by_tvdb()` populates `theme_id` via `/find/{tvdb_id}?external_source=tvdb_id`. Cached negative results (7-day TTL) prevent re-hammering the API.
- **Probe `indeterminate_set` expanded** (v1.14.42 → v1.15.12): also includes YouTube IP-rate-limit responses ("Video unavailable... The current session has been rate-limited by YouTube..."). These are TRANSIENT — not `VIDEO_REMOVED`. Don't write `failure_kind`; renders amber `?` with kind-aware hint. Bulk-probe pool capped at 2 workers (v1.15.12); probe concurrency dropped to 1 (v1.15.14) to stay under YouTube's unauth threshold.
- **Plex enum delta gate** (v1.14.74, schema v46): per-section skip when live `contentChangedAt` matches `plex_sections.last_enum_content_changed_at`. Race-safe: stamp the OLD value read at enum start, not the post-enum value. Empty string = unknown → run full enum. The `(no changes)` op_progress stage_label surfaces the skip to the operator so the perf payoff is visible.
- **Background-op route shape** (pinned v1.15.40): every singleton background-op route MUST call `log_event(...)` BEFORE spawning its daemon thread. The pre-spawn log is the docker-log beacon proving the route was hit. Without it, a thread that crashes silently is indistinguishable from "deploy didn't pick up the new code." `BACKGROUND_OP_ROUTES` test table asserts the ordering for all 4 routes.
- **Path-translation contract** (v1.15.100): `_PATH_PREFIX_TRANSLATIONS` is now a list-of-pairs where user pairs from `MOTIF_PATH_TRANSLATIONS` env precede hardcoded defaults (most-specific wins). Used by `plex_enum`'s sidecar stat + the scanner's folder_path candidate set. The v1.15.90-92 trio replaced single-folder_path UPDATE keys with natural-key (theme_id) lookups OR candidate-set lookups where natural-key isn't available.
- **Design-system rule** (v1.15.87 + `docs/DESIGN_SYSTEM.md`): extract/follow tokens (`:root` in `app.css`) + primitive classes before writing UI, mirror an existing sibling screen, never invent class names. Codified after the `.input-tiny` silent-gap incident. Enforced by hygiene lints landed v1.15.89 / v1.15.96 / v1.15.98 / v1.15.111 / v1.15.116 / v1.15.118-119.
- **`_scrub` allowlist semantics** (v1.14.60): substring matchers in `_SCRUB_SUBSTRINGS` must be specific enough not to swallow legitimate non-secret fields. `"key"` was too broad (matched `rating_key` etc.); now uses explicit `api_key` / `apikey` / `private_key` / `signing_key` / `encryption_key` / `master_key` plus `token` / `secret` / `auth` / `cookie` / `bearer`. Events-table audit data is load-bearing for class-A debugging — silent redaction is a critical bug, not a privacy improvement.

### Q. Recurring patterns reconfirmed

- **Mirror principle drift (class P)** — at least 12 fresh instances closed across the window. PS-token leak on LINK pill // ALL handler (v1.14.43, v1.14.58). `update_tab_row` vs `update_tab_breakdown_rows` shape mismatch (v1.14.52 H8). REPROBE FAILURES SQL vs FAIL chip predicate (v1.15.15). Yellow ⚿ pill filter vs row pill predicate (v1.15.38). Mixed-with-broken attn_pills (v1.15.39). The v1.15.45/50/55/64 SSR cascade had to mirror each `/api/coverage` route's predicate exactly. The pattern keeps recurring because the codebase has axis-style filters with N consumers per axis; the v1.15.40 + § 10's pinned static-text guards are now catching most fresh drift at CI time, but new axes still bleed.
- **Cross-section bleed (class K)** — `api_clear_override` was the last unscoped override endpoint (v1.15.94, closed). Every other override-touching route was scoped via v1.12.72 / v1.13.54 / § 10.G.
- **Phantom P (class A)** — v1.16.9 closed a render-side mismatch: chip render dropped the `verified_ok` gate that `computeSrcLetter` + `_SRC_LETTER_SQL` both enforce. The bug class is documented as a server / cache / verify-loop problem, but this instance lived purely in the JS render branch.
- **Silent `except: pass` / fake-✓-DONE** — class instances closed at `_do_refresh` missing-rk path (v1.14.52 H5), `_topbar_ssr_state` bare except (v1.15.34), `_PlexClient.close` (v1.15.0 pre-cut), mirror-compaction `rmtree(ignore_errors=True)` (v1.15.34), `enumerate_section_items []`-on-failure (v1.15.34), `placement._safe_link_or_copy` pre-clean unlink (v1.15.117). The pattern is: any defensive `except` without a log line + functional fallback is a silent state-drift candidate. Propose **CLAUDE.md bug class T**: "silent-defensive-catch" — a `try/except` that absorbs a failure mode without a log breadcrumb is indistinguishable from a successful no-op, and accumulates state drift that surfaces weeks later as "the deploy looked fine but nothing happened."
- **Test brittleness from over-pinning** — the user flagged this in § 10, and it kept biting. The v1.15.40 lesson: pin the CONTRACT (e.g. "every background-op route calls log_event before thread spawn") not the literal old line shape. v1.15.62 added a perf-budget test that asserts a behavior (page-load < 2s for ~30k rows) instead of a SQL string.
- **Tab/poll lifecycle races** — v1.15.108 closed loadQueue + loadLibraries seq-guard races (analogous to v1.13.28's loadLibrary fix); v1.15.109 closed loadDashboard. v1.16.7's visibilitychange listener closed the background-tab `setInterval` throttle that killed the rapid-poll. Propose **CLAUDE.md bug class U**: "browser tab-throttle / poll-cap" — long-running `setInterval` polls with tight ceilings die in inactive tabs and need `visibilitychange` re-entry hooks; ceilings should accommodate ~1 tick/min throttling headroom.

### R. Open at end of window (parked, not bugs)

- **Selection-wide bulk-action cache** — the v1.15.49/51/54/59/60/124 bulk-action work and v1.15.60's known-suboptimal LPS visibility safety net all hit the same root: `libraryState.selected` survived SELECT ALL FILTERED but every count walker traversed `libraryState.items` (visible page only). Closed in v1.16.10 + v1.16.11 (see § 11.A-B), not in this window. the user's v1.15.60 quote: "let plex server isn't displaying a number at all anytime" was the early signal.
- **Color audit** — amber doing double-duty for Plex semantics + soft attention. Still parked from § 10. Cookies retunes (v1.15.43 biscuit brown, v1.15.121 lemon gold) were one-off color moves, not the broader audit.
- **`/api/items/{mt}/{tmdb_id}` SINGLE-item async refactor + `/api/library` subquery cleanup** — explicit off-limits per the auto-memory note. Untouched in this window.
- **Worker H3 / H5** (multi-worker locks, sibling-hardlink waste) — still open per AUDIT_WORKER.md.
- **Provenance="manual" mystery** (v1.15.101) — diagnostic doc + instrumentation shipped, root cause not yet found. Parked behind data collection.
- **Pill-axis consolidation (audit Theme 3)** — still parked from § 10. The v1.14.58 + v1.15.23 axis-token cross-ref tests catch fresh drift but the underlying duplication remains.

### S. Misc lessons

- **The OR-join CASE-expression footgun**: SQLite's optimizer bails on `JOIN ... ON (cond_with_case_expr) OR (other_cond)`, degrading to near-cartesian scans. v1.15.61 → v1.15.62 cost a 60s page-load regression that lasted a single tag. Either UNION two queries or add the supporting index FIRST and verify the EXPLAIN before shipping. Page-load perf-budget tests catch this class fast — landed v1.15.63.
- **A misnamed feature is technical debt**: the "HAMA bridge" name suggested anime-only; 99.7% of stranded rows were regular TV. v1.16.2's rename was cheap because the user caught the framing before the feature shipped to user-facing surfaces. The cost would have grown if the name had been baked into env-vars / settings UI / docs.
- **Silent redaction can be worse than silent logging**: v1.14.60 broke audit data for ~6 days. The recurring bug classes the audit log exists to debug (class A phantom-P, class K cross-section bleed, every silent-failure class) all needed `rating_key` / `cache_key` / `theme_key` breadcrumbs. The `_scrub` function had no allowlist test pinning "these key fields MUST pass through unredacted" — that's the test that would have caught the regression at PR time.
- **A revert beats a defensible-patch when the symptom recurs**: v1.15.60's LPS-visibility safety net was a defensible patch for the undercount, shipped knowing it might mask a deeper bug. The deeper bug (selection-wide cache) surfaced at v1.16.10 — the user's "no number" complaint was the safety net's secondary symptom. The lesson from § 11.G ("a safety net can become the bug") was already visible in the v1.15.60 commit body. Worth flagging defensible patches as "DEFENSIBLE PATCH —  re-evaluate if X" so future debugging knows where to look.
- **Lints catch the next class of gap**: the v1.15.87 design-system cascade landed in two days because each lint (classname hygiene, mixed-attribute interpolation, class-mapping object-literal gaps, var() reference check, CSS duplicate rules, hover-pseudo coverage) ran on the existing codebase and surfaced the next round of fixes. Each fix landed a new lint. The cycle settled when the lints stopped finding anything new.
- **The audit-rollover cadence is real**: § 10.D-H ran an audit wave that produced the v1.15.0 cut; this window then ran another full audit (v1.14.50-61 bundles + v1.15.34-40 silent-failure sweep + v1.15.37 round-2 + v1.15.39 filter-logic + v1.15.107 audit again at the end). Each wave nets ~50-65 findings, 60-80% of which ship within the wave. The leftover ~20% become the next window's open-thread list. The cadence is "audit → ship the HIGH/MED batch → roll the major-line → next window's work surfaces fresh findings."


---

## 13. v1.17.0: Apprise notifications (phase 1)

**Coverage window**: 2026-05-19. 1 tag. Schema unchanged (still v52). 2303 → 2329 tests (+26 from the v1.17.0 guard module).

### A. Design discussion

the user: "I want to add the ability to integrate with apprise so we can have notification support… setup would be in settings, we could have notifications of when a theme is added or when a theme is deleted or purged, or when a new theme is added or when a themerrdb sync occurs."

Three deployment shapes proposed in the design pass — embedded Apprise (in-process), external Apprise API container (HTTP), or both. the user picked **Option C (both)** — gives homelab users the choice between "one container, one new dep" and "already running apprise-api for other things, just point motif at it." Per-event toggles preferred over a single severity slider since the events have different value-shapes, not just different severities. Notify-health log-only via the events table (no topbar pill — visual debt avoided per § 11.A's "safety nets can become the bug" lesson; if a sink is broken we want the operator to see it in /queue logs, not as a permanent topbar fixture).

### B. Foundation (`app/core/notify.py`, `app/core/config_file.py`, `requirements.txt`)

* `NotificationsConfig` dataclass in config_file.py with three fields: `apprise_urls: list[str]`, `apprise_external_url: str`, `events: dict[str, bool]`. Hangs off `MotifConfig.notifications`. Persisted via YAML config (no DB schema change).
* `_DEFAULT_NOTIFY_EVENTS` dict is the source-of-truth for which event kinds exist and what their default toggle state is. Phase-1 ships **4 kinds**: `sync_completed` (ON), `sync_failed` (ON), `bulk_action_completed` (ON), `themes_added_by_sync` (OFF, opt-in). The remaining 7 from the design pass are deferred to v1.17.1+ — their dispatch sites need per-event dedupe logic (cookies-needed wants once-per-24h; disk-low wants threshold-crossing; worker_restarted wants last-pid dedupe; per-row events want aggregation windows). Surfacing toggles in the UI without backing dispatch wiring would be a footgun; a test (`test_settings_html_has_phase1_event_checkboxes`) asserts the deferred kinds do NOT appear in settings.html until they're wired.
* `apprise>=1.7.0` dep added to requirements.txt with a quarterly-review note alongside the yt-dlp floor.
* `notify.dispatch(db_path, notifications, event_kind, title, body)` — best-effort, fire-and-forget. Reads the per-event gate, no-ops if disabled or no sinks configured. Submits to a module-level 4-worker thread pool. Failed sends log to the events table with `component='notify'` (notify-health visibility) but never re-raise. `notify.test_dispatch(notifications, ...)` is the synchronous test path for the TEST NOTIFICATION button.
* Both dispatch paths coexist: embedded URLs go through `apprise.Apprise()` in-process; external URL POSTs `{title, body, type}` to a caronc/apprise-api `/notify/{key}` endpoint with a 10s timeout.

### C. Settings UI (`app/web/templates/settings.html`, `app/web/static/app.js`)

* New `NOTIFICATIONS` tab between RUNTIME and TOKENS — admin-config stays grouped before account-config (TOKENS / PASSWORD / HOMEPAGE).
* New `data-cfg-field-lines` attribute variant for textarea inputs that map to `list[str]` config fields. Reuses the existing `data-cfg-field` / `data-cfg-field-list` form scaffolding — populate and collect paths both handle the new variant. Apprise URLs are long enough that comma-separation (`data-cfg-field-list`) would force them onto one wrapped line; newline-per-URL matches how Apprise's own wiki documents them.
* `_apply_partial_config` (api.py:139) gained a `dict` branch with **merge semantics**, not replace. `notifications.events` is a `dict[str, bool]` — partial updates from an older client (or the per-checkbox save flow) must NOT drop event keys not in the patch body. The merge preserves untouched toggles; a regression here would silently disable events on every save once new event kinds land.
* `POST /api/admin/test-notification` — synchronous test endpoint. Calls `notify.test_dispatch()` (bypasses the per-event gate, per the design "the test verifies plumbing, not event semantics"). Returns `{embedded: {configured, ok, fail}, external: {configured, ok, fail}}` for inline per-sink rendering in the UI.
* `bindTestNotification()` JS handler mirrors the `bindTestCookies` / `bindSyncProbe` shape — disable button on click, show status text inline, surface per-sink outcome.
* Trimmed UI to the 4 phase-1 events. Hint text below the checkboxes explicitly calls out the v1.17.1 follow-up scope so users know more are coming.
* `base.html` SSR panel allowlist and `app.css` `data-settings-tab` rules both extended for the new `notifications` panel — the v1.15.64 / v1.15.65 mirror tests catch missing entries on either side.

### D. Wired dispatch sites (4)

* `worker.py` around the `run_sync()` call:
  - Try/except wraps the call. Success path dispatches `sync_completed` with the `{movies_seen + tv_seen} items processed, N new, M updated[, F errors]` summary. Exception path dispatches `sync_failed` with the error string + recovery hint, then re-raises so the worker's normal failure handling (retry / mark failed) takes over. Critical: the notify call is itself wrapped in a try/except so a dispatch failure can't mask the underlying sync error — pinned by `test_worker_sync_failed_does_not_mask_underlying_exception`.
  - Stamp `sync_started_at` before the call so the post-sync `themes_added_by_sync` query can scope to themes created in this run's window (`SELECT title, year FROM themes WHERE created_at >= ? ORDER BY created_at ASC LIMIT 11`). Sample-cap at 10 + an overflow check uses the 11th row to compute `(+N more)`.
* `api.py` `_bulk_lps_run` and `_bulk_probe_tdb_run` terminals — both already log a summary line via `log_event` at the natural `op_progress.finish_progress(status='done')` site. The notify dispatch piggybacks on that summary so the message text and the events-log message stay in sync (single source of truth — bug class P avoided by construction).

### E. New invariants from this window

* **Best-effort dispatch contract** (notify.py): every public dispatch function MUST swallow exceptions and never re-raise to the caller. Worker threads, scheduler ticks, request handlers, and bulk-op finalizers all call `notify.dispatch()` and assume it can't crash them. Test guards on each hook site verify the inner try/except wrapping.
* **Default event map = source of truth** (config_file.py:`_DEFAULT_NOTIFY_EVENTS`): adding a new event kind requires (a) adding to the default map, (b) adding the dispatch site, (c) adding a checkbox to settings.html, (d) adding a static-guard test. The phase-1-only test pin (`test_default_event_set_phase1`) forces this rule via assertion — the test must be updated whenever a new event lands, which forces the author to look at the full checklist.
* **dict-merge semantics for nested partial config** (api.py `_apply_partial_config`): `notifications.events` is the first `dict[str, bool]` config field. The `_apply_partial_config` branch for it MUST merge, not replace. Future dict-of-bool config fields should follow the same shape.

### F. Recurring patterns reconfirmed

* **Mirror principle (class P)** — applied by construction here. The bulk_action_completed body string is the same `_bulk_lps_summary` / `_bulk_probe_summary` local that gets passed to both `log_event` and `notify.dispatch`. Single source of truth means an edit to the summary format propagates to both surfaces.
* **Silent-defensive-catch (class T, v1.16.x promoted)** — every `try/except: pass` around a notify.dispatch call comes with a context-establishing comment explaining why the swallow is correct (the dispatch is best-effort by contract). The `_log_outcome` helper inside `notify.py` itself catches event-log write failures with a `log.debug` breadcrumb rather than bare `pass`, addressing class T head-on for the dispatcher's own internal calls.

### G. Open at end of window (v1.17.1+ scope)

Seven more event kinds deferred. Each needs per-event dedupe logic before it can ship without becoming spam:

- `cookies_needed` — once-per-24h via runtime_settings key `notify.cookies_needed.last_at`. First-detect hook should live in the bulk-probe loop where `n_indet > 0` AND `FailureKind.COOKIES_EXPIRED` was the cause.
- `disk_low` — threshold-crossing detection (notify on `OK→LOW` transition, not every check that observes `LOW`). Track last observed state in runtime_settings.
- `worker_restarted` — supervised-thread respawn hook. Dedupe via last-restart-id sentinel.
- `theme_added` (per-row) — needs aggregation window OR explicit opt-in user has to acknowledge the noise.
- `theme_deleted` (per-row) — PURGE / UNMANAGE / FORGET / DELETE all need the same notify hook with `actor=user|sync`.
- `new_tdb_theme_available` — fires when a TDB record newly matches a plex_items row that previously had no theme. Sync-internal hook.
- `release_available` — scheduler `_check_release_update` already detects + caches; just needs a notify call gated on "did we already notify for this version."

### H. Misc lessons

* **"Three deployment shapes" is one-too-many until proven otherwise** — the initial Option-C-dual-sink design felt over-engineered, but the user's reasoning ("give homelab users options") proved out fast: motif's deployment context is mixed (some users running 5+ Docker containers including apprise-api, others minimum-stack). The marginal complexity cost (two URL fields instead of one, two dispatch paths in `_dispatch_inline` instead of one) bought genuine UX flexibility. Worth the symmetry.
* **Trim event scope before shipping toggles** — the original design proposed 11 events; shipping all of them would have required wiring 11 dispatch sites + 11 dedupe strategies (some non-trivial). Trimming to 4 for v1.17.0 kept the ship cohesive — every toggle in the UI does something. The deferred 7 are listed in the settings hint AND in this digest section so the user knows what's coming.
* **Test-the-absence guards prevent feature drift** — the `test_settings_html_has_phase1_event_checkboxes` test asserts BOTH that the 4 phase-1 toggles exist AND that the 7 deferred ones DO NOT appear yet. The negative assertion is what catches a future contributor adding a toggle without wiring the dispatch site — a class-T-style silent feature.
* **JSON-mode partial save merge vs replace** — `notifications.events` was the first `dict[str, bool]` config field. The default `_apply_partial_config` else-branch would have stringified the dict, breaking every save. The `isinstance(current, dict)` branch is the canonical pattern for any future dict-shaped config field; future devs adding one should reuse rather than re-derive.


---

## 14. Digest: v1.17.1 → v1.17.8 (notify phase 2 + design-audit cycle)

**Coverage window**: 2026-05-19 single-day burst. 8 patch tags. Schema unchanged (still v52). 2329 → 2429 tests (+100). The cycle has three phases: audit-cleared rollover (v1.17.1), notification phase 2 (v1.17.2 / v1.17.4), then two iterated design audits (v1.17.3 / v1.17.5 / v1.17.6 / v1.17.7 / v1.17.8).

### A. v1.17.1 — audit-cleared rollover (14 fixes)

Three parallel agents + a self-review audit of the v1.17.0 ship surfaced 3 HIGH / 5 MEDIUM / 6 LOW findings. Bundled into a single rollover tag per the v1.14.50-61 → v1.15.0 cadence (§ 12.D).

* **H1 + L1** (class 9 / silent failure) — `themes_added_by_sync` SQL referenced `themes.created_at`, a column that does not exist. The opt-in toggle was dead code in production: every sync raised `OperationalError`, which the bare `except Exception: pass` at worker.py:879 swallowed silently. Companion v1.17.0 test pinned the literal `"created_at >= ?"` string so the bug was test-locked. Fixed: switched to `first_seen_sync_at` (the timestamp the sync's own upsert writes on first INSERT). Test rewritten to pin the CONTRACT (WHERE timestamp >= ? shape) via regex, not the literal column name.
* **H2** (class P / mirror drift) — `_hydrate_dataclass` (config_file.py) wholesale-replaced `notifications.events` on YAML load, undoing the dict-merge contract that `_apply_partial_config` enforced for PATCH. Forward-compat broken: any future ON-by-default event kind would silently OFF on existing installs. Fixed: dict-merge branch (`merged.update(v)`) before the wholesale-replace fallback.
* **H3 + L2** (class 9) — three bare `except Exception: pass` wrapping notify.dispatch in worker.py without log breadcrumbs. Fixed: `log.warning` on the H1-adjacent SQL swallow (default INFO-log visibility for future schema drift), `log.debug` on the two pure-dispatch swallows (notify infra failures shouldn't spam logs but should be diagnosable).
* **M1-M5 + L3 + L4 + class-9 sweep** — `_send_embedded` underreported rejected URLs (now reports `(added, rejected)`), `_apply_partial_config` rejected `events: null` (now `continue`'d, plus closed-set filter on event keys), `apprise_external_url` accepted any scheme (now http:// / https:// only via `validate()`), `bulk_action_completed` fired on 0-target invocations (now gated on `n_targets > 0`), `_send_external` always sent `type=info` even for sync_failed (per-event `_EVENT_NOTIFY_TYPE` map routes sync_failed → "failure", warnings → "warning", rest → "info"), `_bulk_lps_run` stamp only fired when placements existed (now unconditional, class-P drift with `api_unplace_item` closed), `bindTestNotification` null-deref guard, plex.`_parse_candidates` silent JSON parse → log.warning (self-review class-9 instance found while writing audit fixes).
* Class-9 follow-ups: `log.debug` breadcrumbs on three more bare-pass swallows in sync.py + worker.py (git-mirror compact OSError, fallback_reason JSON parse, relink tmp-unlink OSError). All have functional fallbacks; the breadcrumbs make them diagnosis-friendly.

### B. v1.17.2 — NOTIFICATIONS UX polish + ADOPT bucket-split tooltips

Three small UX fixes the user flagged in screenshots while testing the v1.17.0/.17.1 ship:

* **SAVE EVENTS button** below the toggle grid. the user's mental model split URL config from per-event toggles; the single SAVE NOTIFICATIONS button conflated them. Both buttons use `data-save="notifications"` (same PATCH, backend handles partials idempotently); `bindConfigSaves()` rewritten to use `querySelectorAll` so the ✓ saved flash appears on both adjacent status spans.
* **TEST NOTIFICATION auto-dismiss after 4s** in a finally block. Pre-fix the `✓ embedded 1/1` tag lingered indefinitely. Pattern mirrors the existing save-button 2.5s auto-clear; 4s on test status because the per-sink summary line takes longer to read.
* **ADOPT vs ADOPT+LPS bucket tooltips**. the user's repro: filtered SRC=M with 1342 selected, ADOPT SELECTED showed `(860)` instead of `(1342)` — the 482-row gap was M+P composites (yellow-dot M chip) routed to ADOPT+LPS per the v1.15.49 footgun-prevention design. Math was right; explanation wasn't surfaced. Added title tooltips to both buttons describing the bucket split + the v1.15.49 intent.

### C. v1.17.3 — class-10 visibilitychange sweep

The v1.17.0 audit's task #10 — sweep remaining `setInterval` sites for the v1.16.7 class-10 pattern. Audit revealed `libraryRapidPoll` (v1.16.7's fix) was actually the ONLY site with the ceiling-kill pattern; the other 9 `setInterval` sites just stall during tab inactivity without dying. Different fix shape:

* Extended the v1.16.7 visibilitychange handler from "loadLibrary only" to wake up every polled surface on tab return — `refreshTopbarStatus`, `motifOps.refresh()`, `loadDashboard` (if on `/`), `loadQueue` (if on `/queue`). Tab return now collapses any stale-state lag to milliseconds across every page, not just library tabs.
* `typeof X === 'function'` guards on each refresh call so a future bundle shipping a subset doesn't crash the handler.
* `refreshTopbarStatus().catch(() => {})` chained per the v1.15.108 contract (every unawaited refreshTopbarStatus call must catch — try/catch doesn't cover async rejection).
* Plus the L5 cosmetic UX hint on the Apprise URLs textarea explaining blank-line + whitespace normalization on save.

### D. v1.17.4 — notify phase 2 (6 events + dedupe primitive)

Wired 6 of the 7 events deferred from v1.17.0 § G:

* **cookies_needed** — worker.py download-failure branch for `FailureKind.COOKIES_EXPIRED`. 6h rate-limit (sync runs hitting many cookie-failed rows ping once).
* **disk_low** — worker.py job-dispatch min_free_disk_mb guard. 12h rate-limit. Edge-trigger was considered but requires a recovery-reset path that would add scope; rate-limit gives roughly the same UX.
* **worker_restarted** — main.py boot zombie-sweep, ONLY when `cur.rowcount > 0` (unclean prior shutdown signal). No dedupe — unclean shutdowns are rare + each one is meaningful.
* **theme_added** — worker.py `_do_place` success path. Per-row, OFF by default — opt-in is the dedupe.
* **theme_deleted** — `api_unmanage_item` + `api_forget_item` + `api_delete_item`. Per-row, OFF by default.
* **release_available** — scheduler.py `_check_release_update` on new tag detected. Tag-based edge dedupe — each new release pings exactly once across motif process restarts.

`new_tdb_theme_available` stays deferred (defined in `_DEFAULT_NOTIFY_EVENTS` and PATCH-accessible, but no UI toggle yet). Wiring it requires per-link dispatch inside `resolve_theme_ids`'s 500-row chunk loop — hot path that needs careful batching before that dispatch can ship without slowing the resolve pass.

New `app/core/notify_dedupe.py` module owns the per-event state. Three primitives:

* `should_fire(db, kind, *, rate_limit_seconds=N, edge_value=V)` — returns True if not deduped. Both gates AND'd; pass `None` to skip a gate. Fail-open on DB read errors (better to lose a dedupe round than swallow a notification).
* `record_fire(db, kind, *, value=V)` — stamps last_at + optional last_value in `runtime_settings` keyed by `notify_dedupe.<event_kind>`. No schema migration — `runtime_settings` is already a key/value store.
* `clear(db, kind)` — escape hatch for tests + future admin "reset notification state" surfaces.

### E. v1.17.5 — design audit v1 rollover

the user's three-part design audit ask plus an agent-found set of inconsistencies:

* **PROBE TDB URLs spacing** — `.form-actions` was calibrated for "follows a form-grid" (dashed top border + 42px margin). When it follows a `.form-hint` paragraph (the PROBE TDB URLs case) the paragraph IS the separator; 42px gap reads as "two separate forms." Adjacent-sibling `.form-hint + .form-actions` selector tightens for that specific shape.
* **5 button size/tone fixes**: `// RE-LINK ALL` tiny → full warn (peer to REPROBE / PROBE cluster); `// REBUILD BRIDGE` default → warn (mutating server job); `// APPLY IMPORT` default → warn (mutates user_overrides + enqueues downloads); DRY-RUN ENABLE/DISABLE both → warn (symmetric mutating toggles); `+ NEW TOKEN` → `// NEW TOKEN` (§ 3 prefix convention).
* **5 JS-rendered buttons** missing `// ` prefix (RELINK / DOWNLOAD / REFRESH / REVOKE / VIEW) — all gained the prefix.
* **Status auto-dismiss across 6 sites** — `bindTestNotification`'s v1.17.2 finally-block pattern propagated to bindConfigSaves (error path was inside success branch only, affected 9+ SAVE buttons), bindReprobePlexThemes / bindBulkProbeTdb / bindReprobeTdbFailures start-path catches, bindTestCookies, bindSyncProbe, password update form, info-card PROBE TDB URL slot. Intentionally NOT auto-dismissed: dialog-submit errors (override / manual-URL / upload) — dialog staying open IS the user signal.

### F. v1.17.6 — bulk-action bar constant height

the user's repro: bulk bar grew taller as buttons multiplied (text-wrap inside buttons + 3-line "bulk actions below" caption). Scoped `#library-bulk-bar` CSS overrides: `flex-wrap: nowrap; overflow-x: auto; min-height: 56px` on the bar, `flex-shrink: 0` on every child, `white-space: nowrap` on buttons, `flex-shrink: 1; min-width: 160px; white-space: nowrap` on the caption. Bar now stays one row at constant height; horizontal scroll on overflow. Trade-off: hidden buttons require scroll-to-discover, but the user explicitly asked for "height and box size remain constant" so wrap-to-next-line was off the table.

### G. v1.17.7 — DS § 6 hygiene (4 new primitives)

v1.17.5 audit's deferred C-class findings landed: stable inline `style=` overrides on existing primitives got promoted to real CSS classes per DESIGN_SYSTEM § 6.

* `.missing-banner-cyan` + `.missing-banner-green` — tone variants of the amber-default banner. Captures border-color + bg-tint + glyph color + strong color in one class. 2 consumers (library-scan-hint, library-bulk-bar) each dropped 3-4 inline overrides.
* `.block-body--tight` — `padding: var(--gap-3) 18px`. Closes the v1.15.114 mixed-axis padding smell (token vertical + raw horizontal). 2 consumers in the dashboard.
* `.block-head--divided` — `border-top: 1px solid var(--line); margin-top: var(--gap-5)`. For sub-section headers inside a single panel.
* `.help-text` — `font-size: var(--t-tiny); margin-bottom: var(--gap-2)`. Composes with `.muted` for narrative intros above structural content.

Intentionally NOT promoted: 4 inline `style="margin-top:var(--gap-N)"` overrides at different magnitudes (gap-2/3/4/6) for one-off contexts. 4 modifier classes for 4 magnitudes would be the v1.15.x "modifier explosion" anti-pattern.

### H. v1.17.8 — design audit v2 rollover

Second-pass audit covering 5 new dimensions (dialogs / empty states / typography / form inputs / motion tokens). Four findings closed:

* **A1 — new-token-dlg canonical shell**: was the outlier on every dialog dimension (no `<header class="dlg-head">`, `<h3>` not `<h2>`, no `// ` prefix on title, no cancel button, close button used absolute-corner variant). Refactored to match the 4 other dialogs. Retired the v1.15.115 `.dlg-body > .dlg-close` CSS rule since its only consumer is gone.
* **E1 — ops.css motion-token migration completed**: v1.15.110 migrated app.css but missed ops.css. 7 of 10 raw transitions now use `var(--motion-normal)` / `var(--motion-slow)`; 3 stay raw as deliberate-outlier timings (1s bar-fills + drawer slide pair) with marker comments. Also migrated `.ops-drawer-head h2` raw `font-size: 13px` → `var(--t-small)`.
* **C6 — `▲` → `⚠` on dashboard backfill banner**: `▲` was used both as warning glyph + column sort-asc indicator (§ 7 same-glyph-same-concept violation). Banners now read uniformly: `▸` info (cyan/green), `⚠` warning (amber).
* **C1 — `.ops-drawer-title` class**: `<h2>// LIVE OPS</h2>` was bare. Added class, replaced brittle `.ops-drawer-head h2` descendant selector with class-based rule.

### I. New invariants from this window

* **Best-effort dispatch contract scales to dedupe** (notify_dedupe.py): the v1.17.0 "notify.dispatch never re-raises" contract extends to dedupe — `should_fire` returns `True` on DB read errors (fail-open), `record_fire` swallows write errors at `log.debug`. Losing a dedupe round is strictly better than swallowing a notification.
* **Test contracts, not literals** (v1.17.1 L1 fix): the v1.17.0 test pinned `"created_at >= ?"` as a literal string. When the column name turned out to be wrong, the test passed while the feature was broken. v1.17.1 rewrote the test to pin the WHERE-shape via regex (`WHERE \w+_at >= ?`). Pattern documented as a recurring lesson — tests should encode the CONTRACT (a timestamp comparison exists), not the literal column name (which will inevitably drift).
* **Master-toggle vs tri-state for paginated selections** (v1.16.11 reconfirmed): header checkbox click = "is there ANY selection to clear?" — not "are all visible selected?" — once pagination enters the picture. Re-applied across v1.17.x bulk-action work without regression.
* **Closed-set merge for nested config dicts** (v1.17.1 M2): the dict-merge branch in `_apply_partial_config` accepts arbitrary keys. For `notifications.events` specifically, filter merged keys to `_DEFAULT_NOTIFY_EVENTS` — silently drops PATCH bodies with typo'd or stale event names so the YAML doesn't accumulate dead toggles.
* **Per-event dedupe state lives in `runtime_settings`** (notify_dedupe.py): keys like `notify_dedupe.cookies_needed.last_at` + `notify_dedupe.release_available.last_value`. No new table; the existing key/value store handles it cleanly. Cross-restart persistence via the same path that other motif state uses.
* **Adjacent-sibling CSS for contextual spacing** (v1.17.5 PROBE TDB fix): when a primitive needs a different spacing in one specific shape, prefer `.X + .Y` selector over a `.Y--variant` class. The selector encodes the WHY (this shape needs different spacing in THIS context), the variant class doesn't.
* **Auto-dismiss lives in finally, with class-aware reset** (v1.17.2 + v1.17.5): the canonical pattern is `} finally { setTimeout(() => { if (statusEl.classList.contains('form-status-ok') || ...) { textContent = ''; className = 'form-status'; } }, N); }`. The class check defends against a fresh run's message getting wiped by a stale timer.

### J. Recurring patterns reconfirmed

* **Mirror principle (class P)** — applied by construction across notify wiring. Bulk action summary strings drive both `log_event` and `notify.dispatch` from a single local. Dict-merge in `_apply_partial_config` and `_hydrate_dataclass` is the same shape — pre-v1.17.1 they diverged; v1.17.1 H2 brought them back into mirror.
* **Silent-defensive-catch (class 9)** — promoted to CLAUDE.md at the start of this cycle, then validated by 6 fresh instances closed during it. Pattern is durable: every defensive `except` needs a log line + a functional fallback. The v1.17.1 sweep + v1.17.4 dispatch wraps both used `log.debug` breadcrumbs for notify-infra catches + `log.warning` for SQL-class catches — graded severity matches diagnostic value.
* **Test brittleness from over-pinning** (the user flagged in § 10, recurring) — v1.17.1 L1 (the `themes.created_at` literal pin), v1.17.7 (the v1.15.99 tint-token assertion that broke when tokens moved from inline to class), v1.17.8 (the v1.15.115 absolute-corner assertion that inverted on retirement). Each updated to assert the CONTRACT under the new shape, not the previous literal. The lesson costs ~5 minutes per audit-rollover but compounds.
* **"Safety nets can become the bug" (§ 11.G)** — directly applicable to the v1.17.0 → v1.17.1 audit cycle. v1.17.0's defensive catches around dispatch and the v1.17.0 query bug were both "defensible at the time" patches; v1.17.1 reset them. Worth flagging defensive patches as "DEFENSIVE PATCH — re-evaluate if X" so future debugging knows where to look.

### K. Open at end of window

* **`new_tdb_theme_available` deferred** — its hook site needs per-link dispatch inside `resolve_theme_ids`'s 500-row chunk loop, which is hot. Needs batching design before per-link dispatch can ship without slowing the resolve pass. Still defined in `_DEFAULT_NOTIFY_EVENTS` + PATCH-accessible for users who want to experiment.
* **Color audit (amber double-duty)** — still parked from § 12.R. `⚠` warning banners + `// LET PLEX SERVE` btn-plex + `paths.cookies_file` warnings all use amber. the user explicitly wants to think before any sweep.
* **Pill-axis consolidation (audit Theme 3)** — still parked. High-blast-radius frontend refactor; cross-ref tests added in v1.14.58 + v1.15.23 catch fresh drift but the underlying duplication remains.
* **`provenance="manual"` mystery** — diagnostics shipped in v1.15.101 + class-9 log breadcrumbs added v1.17.1. Awaits the user's repro DB row.
* **`api_item` async refactor + `/api/library` subquery cleanup** — explicit off-limits per the `motif_perf_offlimits` memory note. Untouched in this window.
* **Worker H3 / H5** — multi-worker race + sibling-hardlink waste. Still rare-trigger; not user-reported.

### L. Misc lessons

* **Cycle cadence: audit → ship → audit → ship.** v1.17.x ran 2 design-audit waves (v1.17.5 + v1.17.8) plus 1 code-audit wave (v1.17.1's audit-cleared rollover). Each wave nets diminishing returns — v1.17.5 had 22 actionable findings, v1.17.8 had 4. Each successive audit hits a smaller surface. The pattern matches the v1.14.50-61 → v1.15.0 wave (§ 12.D): "audit → ship the HIGH/MED batch → roll the major-line." Each cycle compresses what changed since the prior tag-cut, then runs an audit that catches drift introduced during the burst.
* **Deferred-with-promise UI elements drift toward dead code.** v1.17.0's settings hint said "Other event toggles ... slated for v1.17.1." v1.17.1 shipped without wiring them, but the hint stayed (until v1.17.2 dropped it). Two weeks later this would have been confusing. Self-correcting rule: every "slated for vX" comment in the UI needs to either be a tracked task OR get dropped at the next non-trivial edit.
* **Inline style overrides are tempting but compound.** v1.17.5 audit found 9 inline `style=` overrides; v1.17.7 promoted 4 of them to primitives. The remaining 5 are one-off magnitudes that don't recur — leaving them inline IS the right call. The hygiene rule isn't "no inline styles ever," it's "no STABLE inline overrides — promote when the pattern recurs."
* **Three deployment shapes proved out** (v1.17.0 → v1.17.4): the Option-C dual-sink Apprise integration that felt over-engineered at design time gave the user exactly the flexibility homelab users wanted. Both URL fields + both dispatch paths are now used in the wild. Worth the marginal complexity cost when the target audience genuinely has mixed deployment shapes.
* **Adjacent-sibling CSS selectors are an underused tool.** v1.17.5 `.form-hint + .form-actions` fix is structurally similar to v1.14.X token migrations — when the override is contextual ("X needs different spacing AFTER Y"), encoding it in the selector reads more clearly than introducing a `.Y--variant-because-of-X` class.
* **Test runs are the audit's secret weapon.** v1.17.1 H1 was caught because the test SHOULD have failed but didn't (the literal pin matched the buggy SQL). Forces the lesson: when adding a test for behavior, run it BEFORE adding the implementation to verify it fails for the right reason. Documented in CLAUDE.md as a future TODO ("red-green-refactor for behavior tests").

---

## 15. Digest: v1.17.9 → v1.17.11 (hygiene audit cycle)

**Coverage window**: 2026-05-19 evening burst, immediately following the § 14 v1.17.1-8 day. 3 patch tags. Schema bumped v52 → **v53** (`plex_items.motif_unplaced_at` dropped). 2429 → 2458 tests (+29). One regression bug fix sandwiched between the two audit ships.

### A. v1.17.9 — Tier A hygiene audit rollover

Three-agent parallel audit ("silent issues, dead code, or database cleanup we can do") returned 24 candidates. Tier-A bundle (high-conviction + low-risk) shipped together — 13 fixes across 3 themes.

**Dead code retired (6):**
* `@app.get("/api/debug/stat-folder")` (api.py:14747) — v1.11.76 Unraid M-vs-P stat-iterdir diagnostic. The class was fixed multiple times since (v1.14.50 owner-stamp, v1.15.117 _safe_link_or_copy pre-clean); the endpoint had zero remaining callers and no UI surface.
* `get_managed_section_ids` (sections.py:209) + its import in api.py:59 — orphan pair. All callsites query `plex_sections.included = 1` directly with their own joins.
* `get_all_runtime` (runtime.py:83) — zero callers; the only runtime bool we care about is `dry_run` with its own dedicated accessors.
* JS dead trio in app.js: `relinkItem` (single-row wrapper, no template binding), `urlSourceLabel` (label-only helper, never called), `activePlexEnumScopeLabel` (defined v1.12.3, never wired — op-mini bar took over labels).

**Class-9 silent-catch breadcrumbs added (4):**
* `app/core/config_file.py` chmod 0600 on motif.yaml — pre-fix silent failure left the file at umask default. The file holds the Plex token + apprise URLs (which may carry service-side credentials), so a silent leak vector on multi-tenant hosts. log.warning with the OSError + path so the operator can repair the filesystem perms.
* `app/main.py` `scheduler.shutdown(wait=False)` — silent exception left apscheduler executor threads alive through the rest of teardown, making the next "motif shutting down" log a lie. log.warning surfaces the failure so postmortem can distinguish hung shutdown from clean.
* `app/web/api.py` `_compute_next_cron_fire` apscheduler ImportError — silent failure blanked the topbar next-sync pill on every /api/stats poll; operator saw a blank field, not a diagnostic. log.warning surfaces install drift.
* `app/core/plex.py` `get_item_paths` JSON decode — twin sites at 288/346/744/990 already log on malformed Plex JSON, but this one silently returned `[]`, losing the placement pipeline's media folder. Joined the chorus.

**DB hygiene (3):**
* **Schema v53** drops `plex_items.motif_unplaced_at`. First-ever ALTER TABLE DROP COLUMN in motif's migration history; previous "drops" recreated tables.
* New `_prune_tvdb_lookup_cache` scheduler job at 03:05 UTC. The cache uses `INSERT OR REPLACE` with `expires_at`; read paths skipped expired rows but nothing deleted them. Unbounded growth pattern.
* New `_prune_events` scheduler job at 03:10 UTC, 30-day retention matching the `/events?since=` UI cap (api.py:14735). Comments called the table "rotating" but pre-fix nothing actually rotated it.

### B. v1.17.10 — SAVE EVENTS toggle persistence bug

the user screenshot: checking the 6 phase-2 toggles in /settings → NOTIFICATIONS (cookies_needed, disk_low, worker_restarted, theme_added, theme_deleted, release_available) + clicking SAVE EVENTS auto-unchecks them on next render. Silent — no console error, no log line.

**Root cause — two-step contract drift across v1.17.0 → v1.17.4:**

1. v1.17.0 added `_DEFAULT_NOTIFY_EVENTS` with 4 phase-1 keys plus a "Phase 2 (v1.17.1+)" comment block listing 7 deferred events that would land when their hook sites did.
2. v1.17.1 added a closed-set filter at `api.py:218`: `if sub_k not in allowed_keys: continue` — silent drop of any PATCH key not in `_DEFAULT_NOTIFY_EVENTS`.
3. v1.17.4 wired hook sites + `notify_dedupe` primitive + UI toggles for 6 of the 7 deferred events. But the comment block was never reconciled and `_DEFAULT_NOTIFY_EVENTS` was never extended — the filter rejected every PATCH the 6 new toggles fired.

The drop at api.py:220 had no log breadcrumb (class-9 by another name), so the drift between UI surface and allowlist accumulated across 6 tags (v1.17.4 through v1.17.9) with zero diagnostic surface. The companion test at v1.17.0 (`test_default_event_set_phase1`) test-locked the literal 4-key dict — the same footgun as v1.17.1 H1 (`themes.created_at`). The pin should have caught the v1.17.4 gap; it instead masked the bug.

**Fix:**
* `_DEFAULT_NOTIFY_EVENTS` extended to all 7 v1.17.4-wired events with their correct defaults (cookies_needed / disk_low / worker_restarted ON because dedupe-rate-limited; theme_added / theme_deleted / release_available OFF because opt-in). `new_tdb_theme_available` also allowlisted (OFF) so manual PATCH stays reachable.
* `api.py:220` closed-set drop now `log.debug`s the dropped key + names `_DEFAULT_NOTIFY_EVENTS` so future contract drift is greppable.
* Parity regression test (`test_v1_17_10_notify_toggle_allowlist.py::test_ui_toggles_in_allowlist`) walks every `data-cfg-field="notifications.events.X"` in `settings.html` and asserts each key exists in `_DEFAULT_NOTIFY_EVENTS`. Fails at CI-time before the same bug class can ship again.
* Softened the v1.17.0 literal-dict pin to "phase-1 keys exist with these defaults" so legitimate phase-N additions can extend the map without breaking the test.

### C. v1.17.11 — Tier B hygiene rollover

Closed the three remaining items from the v1.17.9 audit's Tier B list. All hygiene-themed, no behavioral surface changes.

**Class-9 hot-path breadcrumbs (4) — the once-per-process pattern:**

Pre-fix `except Exception: ...` at `sync.py:416` (normalize_title hot-loop fallback) and `sync.py:1558` (_GitMirror.read_json) silently swallowed. The cleanup couldn't just log.warning per occurrence — the upsert loop runs once per theme per sync, so per-row logs would drown the operator's log on any persistent issue. **New pattern: module-level "warned-once" flag.**

```python
_FOO_WARNED: bool = False

try:
    ...
except Exception as e:
    global _FOO_WARNED
    if not _FOO_WARNED:
        log.warning(...)  # first occurrence, full diagnosis
        _FOO_WARNED = True
    else:
        log.debug(...)    # subsequent — breadcrumb without spam
    # fallback
```

The flag resets only on process restart, which matches the cadence at which root-cause changes can land (deploy / venv fix).

Applied to:
* `app/core/sync.py:416` normalize_title hot-loop swallow (`_SYNC_NORMALIZE_TITLE_WARNED`).
* `app/core/sync.py:1558` _GitMirror.read_json malformed JSON (`_GIT_MIRROR_READ_JSON_WARNED`).
* `app/core/auth.py:102` verify_password bcrypt ValueError/TypeError (`_VERIFY_PASSWORD_WARNED`). Corrupt admin hash makes every login fail — silent pre-fix.
* `app/core/auth.py:287` _verify_token bcrypt ValueError/TypeError (`_VERIFY_TOKEN_WARNED`). Same shape for corrupt api_tokens.token_hash.

**`_prune_history` unified retention sweep:**

Five append-forever tables from the audit roll into one daily job at 03:15 UTC (after `events_prune` 03:10 and `tvdb_lookup_cache_prune` 03:05). Single transaction, single summary log line.

| Table | Filter | Window |
|---|---|---|
| `jobs` | status IN (done, failed, cancelled) | 30 days |
| `sync_runs` | status IN (success, failed) | 90 days |
| `scan_runs` | status IN (complete, failed, cancelled) | 90 days |
| `scan_findings` | cascades from scan_runs ON DELETE CASCADE | — |
| `local_files_history` | (no status filter — append-only) | 180 days |

Per-table status filters ensure pending / running rows are never deleted mid-flight. 90d window for sync_runs / scan_runs matches the dashboard sparkline horizon; if the sparkline extends, this window must bump in lock-step (noted inline in the function).

**Why one function not five jobs:** single writer lock per sweep, one summary log per night, cohesive retention policy in one place.

### D. Tier-A / Tier-B / Tier-C audit triage

The three-agent audit returned 24 candidates. Tiering before shipping was deliberate:

* **Tier A** (v1.17.9): high-conviction + low-risk. Dead code deletion (zero callers, agent-verified), class-9 fixes that don't touch hot paths, schema drop of a 5-year-dead column, TTL sweeps on tables that already had `expires_at` / `ts` columns ready.
* **Tier B** (v1.17.11): bigger refactors needing care. Hot-path class-9 (drowning-the-log risk → once-per-process pattern), unified retention with per-table windows (UI horizon coupling), schema awareness across 5 tables.
* **Tier C** (NOT shipped): judgment-call leave-alone. `/api/library/download-missing` (has tests, no UI surface — test warmth kept), `get_runtime_bool`/`set_runtime_bool` (single-caller helpers, inlining loses extension point), `notify_dedupe.*` rename sweep (handful of stale rows on rename, not worth allowlist code).

The Tier-A → Tier-C distinction wasn't planned; it emerged from the audit reports. Worth codifying as a pattern for future multi-agent audits.

### E. New invariants from this window

* **Once-per-process flag for hot-path class-9** (sync.py / auth.py): when the silent-catch site is on a hot path (sync upsert per theme, auth verify per request), `log.warning` per occurrence drowns the log. Module-level flag means first occurrence logs at warn so operator sees the issue at boot, subsequent occurrences drop to log.debug so the breadcrumb still exists for granular diagnosis. Flag resets only on process restart, matching the cadence at which deploy-fix landing is possible. Cataloged as a class-9 sub-pattern.
* **Closed-set filter needs paired contract test** (v1.17.10): the v1.17.1 closed-set design (filter merged keys against `_DEFAULT_NOTIFY_EVENTS`) is correct in isolation but creates a contract between UI and code that drifts silently. Every closed-set filter must ship with a test that walks the UI surface and asserts each surfaced key exists in the allowlist. Without it, adding a new UI toggle without extending the allowlist is a silent regression. Pattern documented for future allowlist sites (none today, but the API token scope check has the same shape).
* **First-ever DROP COLUMN migration** (schema v53): SQLite 3.35+ supports ALTER TABLE DROP COLUMN. Python 3.12 ships SQLite 3.45+. Use the direct DROP for additive-only column retirements (not table-rebuild) — guard with `PRAGMA table_info` idempotency check so partial-replay scenarios are safe. Precedent for future dead-column cleanup work.
* **Tiered audit triage** (Tier A/B/C): high-conviction + low-risk in one tag, harder follow-ups in their own tag, leave-alone items NOT shipped. Pattern matches the v1.14.50-61 → v1.15.0 cadence but explicit about NOT shipping low-confidence items.
* **Retention windows coupled to UI horizons** (prune_history): when a table feeds a UI surface (sparkline, drawer, panel), the retention window must outlive that surface's display range with a buffer. The 90d sync_runs/scan_runs window pairs with the dashboard's 90-day sparkline; documenting that coupling inline lets future changes to the sparkline trigger the retention bump in lock-step.

### F. Recurring patterns reconfirmed

* **Silent-defensive-catch (class 9)** — promoted to CLAUDE.md at the start of v1.17.0, validated by 6 fresh instances in v1.17.1, then 4 more in v1.17.9 + 4 more in v1.17.11. Pattern is durable; the once-per-process sub-pattern for hot paths is the new variant.
* **Mirror principle (class P)** — applied by construction in `_prune_history` (one function mirrors the per-table retention contract; pre-v1.17.11 it would have been 5 separate jobs with 5 different status-filter spellings drifting independently).
* **Test brittleness from over-pinning** — v1.17.10 found the v1.17.0 literal-dict pin had test-locked the buggy state; same lesson cost as v1.17.1 H1. Softened to "contract not literal." The pattern is now durable enough to be a default — start with contract tests, only literal-pin when there's no contract to encode.
* **Defensive patches need re-evaluation hooks** (§ 14.J restated) — v1.17.10's bug accumulated specifically because v1.17.1's defensive closed-set filter became part of the contract drift. Same shape as v1.17.0 → v1.17.1: defensive patch becomes the bug. Worth flagging defensive patches as "DEFENSIVE PATCH — re-evaluate if X" so future debugging knows where to look.

### G. Open at end of window

* **VACUUM strategy parked.** `PRAGMA auto_vacuum = INCREMENTAL` only works on fresh installs; existing DBs would need a full rewrite. SQLite reuses freed pages internally for new inserts so the prune jobs are sufficient without it. Adds complexity for marginal gain — deliberately left alone.
* **Tier C judgment-call items** — `/api/library/download-missing`, `runtime.get_runtime_bool`/`set_runtime_bool`, `notify_dedupe.*` rename sweep. All audit-flagged but explicitly NOT shipped per § 15.D.
* **Long-term parked items unchanged** from § 14.K: `new_tdb_theme_available` UI wiring, color audit (amber double-duty), pill-axis consolidation, provenance="manual" mystery, Worker H3/H5.

### H. Lessons

* **Three-agent parallel audit pays off.** Each agent walks a different axis (class-9 catches, dead-code orphans, DB growth patterns) and the overlap is minimal. Single-agent "audit everything" would have produced shallower findings. Pattern reusable for future audit waves.
* **Tier before shipping.** Don't bundle the whole audit into one tag; sort by conviction × risk and ship the high-conviction-low-risk tier first. Tier B + C waited for v1.17.11 + dropped entirely respectively. Lower-risk-per-tag means each ship is easier to validate.
* **Dead-code sweeps must include tests.** v1.17.9's `urlSourceLabel` retirement broke a v1.14.0 test that pinned its existence as a "JS mirror of Python `url_source()`" contract. The agent's "zero callers" check missed test-pin contracts. Test-pin walks the production code surface but tests are part of that surface — agent prompts updated to grep `tests/` too.
* **Once-per-process flags for hot paths.** When tempted to write `log.warning` inside a hot loop, ask "would this drown the log on persistent failure?" If yes, use the module-level warned-once flag. The pattern's cost (one bool per site) is trivial; the diagnostic value (first occurrence in logs) is high.
* **Contract drift accumulates in silence.** v1.17.10's bug was visible for 6 tags before the user caught it because there was no breadcrumb at the drop site. The class-9 hygiene rule generalizes: any code path that "silently does the right thing" needs a debug-level log so the contract is greppable.
* **First-of-a-kind precedents matter.** v1.17.9's DROP COLUMN was motif's first; v1.17.11's unified `_prune_history` is the first multi-table retention job. Both are documented inline so the next person doing similar work has a pattern to follow rather than reinventing. The marginal cost of inline-doc-the-precedent is much less than re-deriving the choice space.

---

## 16. Digest: v1.17.13 → v1.17.19 (audit Tier A + Discord notification polish)

**Coverage window**: 2026-05-19 evening continuation, 7 patch tags. Two themes ran in parallel — closing the v1.17.12 three-agent audit's Tier A bundle (v1.17.13), then a notification-polish iteration cycle driven by the user's Discord screenshots (v1.17.14 / .16 / .19), with bug-fix tags interleaved (v1.17.15 search persistence, v1.17.17 orphan recovery, v1.17.18 bulk-bar overflow). Schema bumped v53 → **v54** (`previous_urls.kind` retroactive data fix). 2458 → 2555 tests (+97).

### A. v1.17.13 — Tier A audit rollover (the user screenshot driven)

The v1.17.12 three-agent audit (security / frontend race / error UX) returned 22 actionable findings. v1.17.13 ships the Tier-A bundle covering the most dangerous silent-failure paths.

**Error UX — 5 silent-loader sites** surface failures instead of stale/empty:
* `loadQueue` — accent-red row in #jobs-body on /api/jobs failure.
* `loadTokens` — same shape in #tokens-body.
* `loadConfigIntoForms` — **the dangerous one**. Pre-fix a silently-failed /api/config left form fields at HTML defaults; a subsequent SAVE would PATCH those blank values over the real server-side config — silent data loss. New: `configLoadFailed` module flag + visible `.missing-banner` with RETRY button. `bindConfigSaves` refuses to PATCH while the flag is set.
* `loadScansList` / `loadFindings` / `loadScanDetail` — accent-red rows in their respective tbodies + meta strip.
* `refreshDryRunState` — "STATE UNKNOWN" label (was silently blank).

**Frontend race — 5 optimistic-placeholder sites** got paired `clearOptimisticPlaceholder` on error path (class-5 silent topbar drift). Plus the per-libraries-row REFRESH btn (audit missed it; caught via grep during cleanup).

**Frontend race — 3 visibility-guard polling bodies**: `syncWatcher` (both arming paths) + `libraryRapidPoll` bail when `document.visibilityState !== 'visible'`. Class-10 sub-pattern. Under Chromium's ~1/min throttle in hidden tabs, these intervals were still running the body work over the network.

**Security — apprise_urls scheme validation + GET masking** (audit HIGH 2). Three new helpers in `config_file.py`:
* `_validate_apprise_url_scheme(url)` — URI shape + deny-list (file/ftp/sftp/gopher/data/javascript/vbscript/about/chrome). Lenient on the allowlist so new apprise plugins don't need code changes.
* `mask_apprise_url(url)` — returns `<scheme>://***` for GET response.
* `_is_masked_apprise_url(url)` — detects round-trip markers.

PATCH handler positionally substitutes masked entries with the existing URL at the same slot (mirrors plex.token's mask-equals-keep contract). `validate()` surfaces a per-URL error for any denied-scheme entry. Closes the asymmetry where `plex.token` / `tmdb_api_key` / `proxy_url` were masked but `apprise_urls` returned plaintext (admin-token theft would leak every embedded webhook credential).

### B. v1.17.14 — Discord rendering fix (markdown image → plain URL)

the user's screenshot of a v1.17.12 Discord theme_added notification showed the markdown image syntax rendering LITERALLY:

```
**Elektra (2005)**
[![theme thumbnail](https://i.ytimg.com/vi/qImxW9zTVWk/...)](...)
Source: ThemerrDB · YouTube
```

Two interacting Discord constraints:
1. Discord webhooks render bold / italic / links but DO NOT render `![alt](url)` image syntax — shows literally.
2. Wrapping a URL in `[text](url)` brackets disables Discord's auto-embed of YouTube. The v1.17.12 shape (`[![alt](thumb)](url)`) hit BOTH — no inline image AND no preview card.

Fix: drop the markdown image, send the URL plain on its own line. Discord / Slack / Telegram / Matrix auto-embed YouTube + SoundCloud URLs natively with a rich preview card (thumbnail + title + channel + play button) — strictly better than the markdown attempt.

For `theme_deleted` events: wrap the URL in `<...>` (Discord's no-embed marker) so deletion notifications don't spawn a giant preview card on every event. URL stays clickable; only Discord interprets the brackets specially.

### C. v1.17.15 — library search persistence

the user's ask: search "yu-gi-oh" in `/movies` → click TV SHOWS → search persists on `/tv`. Pre-fix the in-memory `libraryState.q` cleared on every full-page nav.

Threaded the needle the v1.15.52 fix had narrowed (localStorage `q` was removed because old searches from earlier sessions were resurfacing):
* **localStorage `q`**: stays excluded (the v1.15.52 contract) — cross-session resurfacing was the original complaint.
* **sessionStorage `q`**: new in v1.17.15. Per-tab; survives in-tab nav; clears on tab close; separate per tab.

Three helpers (`_writeSessionQ` / `_readSessionQ` / `_clearSessionQ`) hooked at four sites: search-input debounce, ✕ clear, CLEAR ALL, and `bindLibrary` URL-fallback (`sp.get('q') || _readSessionQ()`). Deep-link `?q=` still wins (explicit user intent); also seeds sessionStorage so a deep-link → tab-switch flow continues to carry the search.

### D. v1.17.16 — notification audit (title dedupe + pre-op enrichment)

the user's feedback on v1.17.14: title duplicated (subject + bold body line), and PURGE showed `motif: theme forgotten — movie/-26` instead of the title.

**Title duplication retired**: `format_theme_added_body` + `format_theme_deleted_body` no longer emit the `**<title>**` first line. The notification subject (`motif: theme added — <title>`) is the canonical title surface; body is now structural metadata + URL only. Net body is 2-3 lines (was 3-5).

**Pre-op enrichment capture**: `api_forget_item` and `api_delete_item` ran `enrich_item(...)` AFTER the destructive transaction. For orphan-drop paths (FORGET on `upstream_source='plex_orphan'`, all DELETEs) the themes row + FK children are gone by then — enrich_item finds nothing, falls back to bare-ID `display_title` like "movie/-26".

Fix: each of the three theme_deleted handlers captures the `ItemContext` BEFORE opening the connection / txn block. Captured `_notify_ctx` threads down to `notify.dispatch` after the destructive op completes. `api_unmanage_item` doesn't strictly need pre-capture (themes may survive UNMANAGE) but uses the same pattern for consistency.

### E. v1.17.17 — orphan recovery path

the user's screenshot: a `plex_orphan` row (TDB pill = "NO TDB") showed RE-DOWNLOAD TDB as the only visible SOURCE-menu action. Confusing AND functionally broken. Two interacting bugs:

**Bug A — frontend gate missing `isOrphan`** (`app.js:7696`):

The SOURCE-menu DOWNLOAD / RE-DOWNLOAD TDB gate didn't exclude `isOrphan`. Plex-orphan rows have a `themes` row + a captured `youtube_url` (from the original ADOPT), so the gate passed and the TDB-flavored action appeared. Fix: add `&& !isOrphan` to the gate.

**Bug B — capture mis-labels orphan-sourced URLs** (`_capture_previous_url`):

The capture hardcoded `kind='themerrdb'` for any themes-row fallback, regardless of `upstream_source`. For plex_orphan rows the URL came from adopt, not TDB. The mis-labeled kind triggered the v1.12.103 `revert_redundant` SQL branch at `api.py:1977-1980`:

```
lf.file_path IS NULL                          -- post-PURGE
AND kind = 'themerrdb'                        -- mis-label
AND hidden_url IS NULL
AND captured_url = themes.youtube_url
```

`revert_redundant=1` hides RESTORE in the SOURCE menu — combined with Bug A, the user had no recovery action at all.

Fix: when capturing from a plex_orphan-sourced themes row, emit `kind='user'`. The URL came from adopt; the worker handles user-kind RESTORE by writing to `user_overrides` + queuing a download — exactly the "re-instate my adopted theme" flow.

**Schema v54**: retroactive UPDATE on `previous_urls` to flip existing wrong-kind rows so already-stranded captures recover RESTORE on next boot. Idempotent (the WHERE clause already constrains to wrong-kind + orphan-source).

### F. v1.17.18 — bulk-bar overflow menu

the user's screenshot showed `// RESTORE FROM PLEX` cut off mid-word at the right edge of the bulk-actions bar. v1.17.6 had shipped horizontal-scroll fallback, but the scroll was undiscoverable (no visual cue) and clipped actions silently.

Replaced with a `// MORE ▾` overflow dropdown. When the bar's content width exceeds the container, the rightmost visible action buttons move into the dropdown panel until the bar fits. Constant-height contract (v1.17.6) preserved; actions discoverable instead of clipped.

Implementation:
* `library.html`: trailing `<details class="row-menu">` with `// MORE ▾` summary. Reuses the existing `.row-menu` / `.row-menu-panel` CSS primitive (same shape per-row SOURCE / PLACE / REMOVE menus use).
* `app.css`: `overflow-x: auto` → `overflow: hidden` on `#library-bulk-bar`. Scoped rule constrains the overflow panel width + caps height.
* `app.js`: `_layoutBulkBar()` measures `bar.scrollWidth > bar.clientWidth`, moves rightmost non-primary visible buttons into the panel until the bar fits. `_BULK_BAR_PRIMARY_IDS` set excludes SELECT ALL FILTERED + CLEAR (always-needed bookends). Idempotent: pulls every panel child back to the bar before re-measuring.

Hooks: called from `updateLibrarySelectionUi` + a `ResizeObserver` on the bar (covers width-only changes that don't go through that path). Close-on-action click handler scoped to the overflow menu (mirrors the v1.10.24 row-menu pattern).

### G. v1.17.19 — notification emoji set

the user's ask: "could we add some emoji or more indicators of a removal event vs an add event ... professional looking discord notifications." Previous subjects were near-identical prefixes (`motif: theme forgotten — ...` / `motif: theme added — ...`) — verb buried mid-string.

Cohesive emoji set, one per event KIND (not per minor variation):

| Event | Emoji | Subject |
|---|---|---|
| sync_completed | ✅ | `✅ Sync complete` |
| sync_failed | ❌ | `❌ Sync failed` |
| bulk_action_completed | ✅ | `✅ Bulk PROBE TDB done — N/M` / `✅ Bulk LPS done — N` |
| themes_added_by_sync | 🎵 | `🎵 N new themes added by sync` |
| theme_added | 🎵 | `🎵 Theme added — <title>` |
| theme_deleted | 🗑️ | `🗑️ Theme unmanaged/forgotten/deleted — <title>` |
| cookies_needed | 🍪 | `🍪 YouTube cookies expired or missing` |
| disk_low | 💾 | `💾 Low disk space — XMB free` |
| worker_restarted | ⚠️ | `⚠️ Worker restarted (unclean shutdown)` |
| release_available | 🆕 | `🆕 motif vX.Y.Z available` |
| test | 🧪 | `🧪 motif vX.Y.Z — notification test` |

**"motif:" prefix dropped** on most events (the bot username already attributes). Kept on attribution-critical cases: `🆕 motif vX.Y.Z available` ("vX.Y.Z available" alone is too vague) and `🧪 motif vX.Y.Z — notification test` (operator setup verification).

Structural lint test pins that no new dispatch site can slip back to `title="motif:"` shape.

### H. New invariants from this window

* **Mask-equals-keep contract scales to lists** (v1.17.13 apprise_urls): the plex.token / tmdb_api_key / proxy_url scalar mask was `***` ↔ "keep existing." For list fields, the equivalent is positional: a masked entry at index `i` means "keep `current[i]`." A real URL at index `i` is a replace. Missing entries delete. Round-trip safe; no schema migration needed (the merge happens at PATCH time).
* **sessionStorage threads cross-tab / cross-session needles** (v1.17.15): when localStorage persistence triggers UX complaints about stale data resurfacing across sessions but you want intra-tab nav persistence, sessionStorage is the per-tab scope that bridges. The session boundary matches the user's "fresh session = fresh state" mental model better than localStorage's "ever?" or in-memory's "this page only."
* **Pre-op enrichment capture for destructive flows** (v1.17.16): when a notification body needs metadata that the destructive op will erase (themes row drop, FK cascade), capture the enrichment context BEFORE the txn opens. Same shape as "save the data you need before you delete it" — but in a notification context, post-op enrichment failure is often silent (best-effort dispatch + bare-ID fallback), so the bug surfaces as confusing-but-not-broken notifications.
* **Two interacting bugs make symptoms misleading** (v1.17.17): the "no recovery path on orphan PURGE" symptom looked like one bug. Investigation surfaced TWO: a frontend over-permissive gate (wrong action visible) AND a backend mis-classification (correct action suppressed). Either alone would have been less visible — together they erased the recovery path entirely. Lesson: when a UX is "everything wrong at once," look for multi-bug interaction before settling on a single-cause story.
* **Overflow toolbar > horizontal scroll** (v1.17.18): when a horizontal toolbar would overflow, a scroll fallback hides actions undiscoverable. An overflow dropdown (`// MORE ▾`) surfaces them. Trade-off: hover-to-reveal adds one click vs scroll's drag. For low-frequency overflow (the bar fits in most cases), the dropdown wins on discoverability. For high-frequency overflow, the action set is too big and needs trimming.
* **Emoji subjects scale across services** (v1.17.19): one canonical emoji per event KIND (not per variation) gives at-a-glance scannability without overload. Apprise services that ignore subject emojis (rare) still get the verb in the subject text. Bot username attribution lets you drop redundant `motif:` prefix on most subjects.

### I. Recurring patterns reconfirmed

* **Test brittleness from over-pinning** (the user flagged in § 10, recurring) — v1.17.14 dropped tests pinning the markdown image syntax that Discord didn't render; v1.17.16 dropped tests pinning the bold-title body line; v1.17.19 left existing v1.17.12/14/16 tests untouched because they were already contract-style ("title contains display_title") not literal-pin. The trend: each iteration produces fewer test breakages because the test discipline shifted toward contract assertions.
* **Class-9 silent-defensive-catch** — v1.17.13's `loadConfigIntoForms` was a silent-data-loss path created by the same pattern: error → `console.error` + `return` → form fields at HTML defaults → SAVE overwrites real config. The fix wasn't just a log line; it was a stop-the-PATCH safety + visible banner. Silent error + destructive follow-on action is class-9 with teeth.
* **Defensive patches need re-evaluation hooks** (§ 14.J restated) — v1.17.10's bug (closed-set filter rejecting new event keys) was caused by a v1.17.1 defensive patch becoming part of the contract drift. v1.17.17's `revert_redundant` SQL branch was a v1.12.103 defensive patch ("don't show RESTORE when it's equivalent to DOWNLOAD TDB"); it became the bug for orphan rows because the kind classification was wrong. Defensive patches accumulate ownership over time; flag them so future debugging knows where to look.
* **First-of-a-kind documentation** — v1.17.17's schema v54 is motif's first retroactive data-fix migration (v42 was a one-shot DELETE; v54 updates kind values). The migration docstring documents WHY (the v1.12.103 SQL branch + the mis-classification interaction) so the precedent is searchable.

### J. Open at end of window

* **Documentation pass** — this section! Documents v1.17.13 → v1.17.19 cycle.
* **Tier B audit follow-ups** from v1.17.12: forward-auth IP allowlist (security HIGH 1), frontend race conditions (`finishWatcher` stuck setIntervals, refreshTopbarStatus seq guard / class-6 mirror, AbortController coverage), Error UX MED items (per-row "N FAILED" capture, page-stale banner architecture). Tier C explicitly NOT shipped.
* **Long-term parked items unchanged** from § 14.K / § 15.G: `new_tdb_theme_available` UI wiring, color audit, pill-axis consolidation, provenance="manual" mystery, Worker H3/H5.

### K. Lessons

* **Screenshot-driven iteration cycles are short and high-signal.** v1.17.14 → .16 → .19 each shipped within a single the user screenshot's feedback. Each tag's scope was a single concrete complaint with a clear fix; total cycle time was much shorter than the audit-driven tags. The cost is mental-context-switching (multiple small tags in one day); the benefit is rapid response when the user is actively testing.
* **One concrete user complaint can surface multiple bugs.** v1.17.17's "no recovery path on orphan PURGE" investigation surfaced two interacting bugs that wouldn't have been caught separately. When investigating a UX symptom, always ask "what's the second bug hiding behind this one?"
* **Schema migrations split into shape changes vs data fixes.** v1.17.9's v53 was a shape change (DROP COLUMN); v1.17.17's v54 is a pure data fix (UPDATE values). Both deserve a migration step + idempotency guard. The boundary matters because data-fix migrations don't need rollback scripts (the WHERE clause naturally bounds the fix), but shape-change migrations do.
* **Contract drift is a class of bug, not a one-off.** v1.17.10 / v1.17.17 / the user's "bring back CLEAR URL" question all stemmed from contract drift — semantics shifted under stable names. The CLAUDE.md class-9 catalog should pick up "contract drift" as a sub-pattern.
* **Tier-A / Tier-B / Tier-C audit triage** (§ 15.D restated): the v1.17.13 ship validated the pattern. Tier B is still queued; Tier C explicitly dropped. Worth keeping as the default cadence for multi-finding audits.


## 17. v1.18.0: Plex Collections (the first non-movie/show media type)

The first feature-feature ship of the v1.17.x line. Adds a fourth library tab `/collections` parallel to `/movies` / `/tv` / `/anime`, with full theme orchestration: ThemerrDB sync, motif download, HTTP upload to Plex, library-row management, SOURCE menu, notifications, dashboard counts.

The architectural novelty: collections have **no folder on disk**. Plex collections live entirely inside Plex's metadata bundle — themes are attached via `POST /library/metadata/{rk}/themes` (the same endpoint themerr-plex used via `python-plexapi`'s `Collection.uploadTheme()`). Every existing "place a theme" path in motif assumed a target folder for the hardlink/copy; v1.18.0 introduces `placement_kind='plex_upload'` with `media_folder=''` as the marker for the no-folder shape, and a dedicated worker adapter that POSTs bytes instead of linking files.

Shipped as a 7-phase sequence (one commit per phase) to keep each step independently reviewable. No intermediate tags — the whole feature ships under v1.18.0.

### A. Phase breakdown

| Phase | Commit | Surface | Lines added |
|---|---|---|---|
| 1 | Schema v55 | `themes.media_type`, `plex_items.media_type`, `previous_urls.media_type`, `section_failure_acks.media_type` CHECKs widened to include `'collection'`; `placements.placement_kind` widened to include `'plex_upload'`. `_widen_check_constraint` helper does dynamic CHECK rewrite via sqlite_master regex (vs hardcoding column lists). | ~200 |
| 2 | Plex client | `PlexClient.enumerate_collections_for_section` (GET `/library/sections/{id}/collections` paginated), `upload_collection_theme` (POST binary), `delete_collection_theme` (DELETE). Reuses `PlexLibraryItem` with `media_type='collection'`. | ~250 |
| 3 | Sync | `_classify_git_path` recognizes `movie_collections/themoviedb/<id>.json`; `_run_git_differential_upsert` + `_detect_and_stamp_drops_git` + `_detect_and_stamp_drops_full_walk` handle the third media bucket; `run_sync` main loop iterates `(("movie","movies"), ("tv","tv_shows"), ("collection","movie_collections"))`; snapshot tarball whitelist accepts `movie_collections/`; `_plex_supplies_theme` / `_enqueue_download` align themes ↔ plex_items on `'collection'` (no aliasing). | ~150 |
| 4 | Worker adapter | `_do_place` branches on `media_type=='collection'` to a new `_do_place_collection` helper. Helper: pre-resolve plex_items rating_key, honor dry-run + force_overwrite + skip_if_plex_has_theme, read mp3 bytes, POST via `upload_collection_theme`, upsert placements with `placement_kind='plex_upload'` + `media_folder=''`, stamp local_files attempt, nudge `plex_items.has_theme=1`, dispatch theme_added notification. | ~260 |
| 5 | SQL routing | `/api/library`, `/api/library/download-missing`, `/api/libraries/refresh` accept `tab='collections'` (regex/validation widened); `_library_query` maps to `pi.media_type='collection'` and skips the `is_4k` filter (collections aren't 4K-tagged); `plex_enum` per-section main loop adds a collections-pass via `enumerate_collections_for_section` + `_upsert_items` with class-9 defensive log on failure. | ~120 |
| 6 | Frontend | `/collections` route renders `library.html` with `tab='collections'` + `fourk=False`; `base.html` nav adds `<a href="/collections" data-nav="collections">COLLECTIONS</a>`; `computeSrcLetter` recognizes `placement_kind==='plex_upload'` as placed (since `media_folder=''` is falsy in JS); `rowMt` dispatch maps `theme_media_type/plex_media_type==='collection'`; `window.__motif_themes_have.collection` bucket sourced from `stats.collections.tdb_total`. | ~60 |
| 7 | Ship | `/api/stats` returns parallel `collections` bucket; `app/__init__.py __version__ = "1.18.0"`; `CLAUDE.md` schema v54→v55, SRC letter axis section mentions /collections; PROJECT_HISTORY § 17 (this). `notify_content.enrich_item` verified media-type-agnostic — no source change needed. | ~80 |

Suite end-state: **2678 passing** (was 2598 at v1.17.24 cut; +80 tests across the 7 phases).

### B. The "no folder" architectural decision

themerr-plex did this exact thing — its `update_theme_song` flow ran:

```python
plex.fetchItem(rating_key).uploadTheme(
    fileobj=requests.get(youtube_url, stream=True).raw,
)
```

The underlying HTTP call is `POST /library/metadata/{rating_key}/themes` with the audio bytes as the body. The same endpoint works for movies, shows, AND collections — Plex doesn't discriminate.

For collections we couldn't use the existing place_theme path because:

1. **No folder exists.** FolderIndex walks media folders looking for a title-year match. Collections aren't libraries-of-media in the filesystem sense — they're metadata-only groupings that Plex computes from member ratingKeys.
2. **No sidecar lookup applies.** `plex_items.local_theme_file` reflects whether Plex found a `theme.mp3` in the item's folder. Collections have no folder, so the column is always 0.
3. **No SCAN ALL discovery applies.** The scans page walks the themes_dir + the Plex folders looking for sidecars and matches. Collections have neither.

So the worker adapter is a pure HTTP path:

```python
def _do_place_collection(self, *, job, theme, local):
    # ... resolve cached_rk from plex_items by theme_id JOIN ...
    audio_bytes = source_file.read_bytes()
    plex.upload_collection_theme(rating_key=str(cached_rk),
                                 audio_bytes=audio_bytes,
                                 content_type='audio/mpeg')
    # placement_kind='plex_upload', media_folder=''
    # plex_items.has_theme=1 nudge for immediate UI reflection
```

### C. The "empty string is NOT NULL but is JS falsy" gotcha

The single most subtle aspect of the v1.18.0 wiring: `media_folder=''` for plex_upload placements.

**SQL side**: `p.media_folder IS NOT NULL` is **true** for `''` — SQL distinguishes NULL from empty string. The existing `_SRC_LETTER_SQL` CASE branches keyed on `p.media_folder IS NOT NULL` therefore fire correctly for collection rows (T/U branches based on source_kind).

**JS side**: `!!it.media_folder` is **false** for `''` — JavaScript falsy. The pre-v1.18 `computeSrcLetter`'s `placed = !!it.media_folder` classified motif-uploaded collections as not-placed, falling through to P / sidecar / '–'.

Fix: extend the JS check to recognize `placement_kind==='plex_upload'`:

```javascript
const placed = !!it.media_folder
               || it.placement_kind === 'plex_upload';
```

This keeps the SQL and JS in agreement, no schema gymnastics needed. The pattern is: when the storage layer represents "marker without payload" via empty string (vs NULL), the consuming JS needs an explicit-marker check to bridge the falsy gap. Worth a CLAUDE.md note for future row-axis additions — anywhere we use empty strings as semantic markers, the JS axis needs the explicit check.

### D. The dynamic CHECK constraint widening helper

Schema v55 needed to widen 5 CHECK constraints (4× `media_type`, 1× `placement_kind`). Hardcoding column lists for each table-rebuild is fragile (schema drift between dev/prod) and brittle (a future schema change can silently drop columns from the migration's INSERT SELECT). v1.18.0's `_widen_check_constraint` helper does the rebuild dynamically:

1. Read the table's `CREATE TABLE ...` SQL from `sqlite_master`.
2. Regex-match the CHECK clause for the target column (flexible across single-quote / double-quote / whitespace variations).
3. Substitute the widened value list.
4. Rebuild the table with `PRAGMA defer_foreign_keys=ON` + column-preserving `INSERT INTO ... SELECT ... FROM`.

The dynamic shape means future widenings can call `_widen_check_constraint(conn, 'themes', 'media_type', ('movie','tv','collection'), ('movie','tv','collection','playlist'))` without restating column lists. Same pattern would extend to widening other CHECK constraints (placement_kind, source_kind, etc.).

### E. Phase ordering — why this sequence

The 7-phase order was load-bearing:

* **Schema (1) before everything**: every downstream phase touches at least one widened CHECK. Wrong order → tests for Phase 2+ would fail on CHECK violations before the migration could land.
* **Plex client (2) before sync (3)**: sync uses the placement_kind alignment but not the new Plex methods. Could swap order, but Phase 3 references the upload semantics for the docstring.
* **Sync (3) before worker (4)**: worker depends on themes rows existing for collections. Sync populates themes from ThemerrDB.
* **Worker (4) before SQL routing (5)**: SQL routing references the `placement_kind='plex_upload'` placements that Phase 4's worker writes. The routing wouldn't be testable without Phase 4 first.
* **SQL routing (5) before frontend (6)**: frontend GETs `/api/library?tab=collections`. Without Phase 5, the endpoint 422s on the tab regex.
* **Frontend (6) before ship (7)**: ship Phase 7 wires `/api/stats.collections.tdb_total` which frontend's `__motif_themes_have.collection` consumes. Without Phase 6 first, the stat would have nowhere to land.

Validated by suite passing at every phase boundary (one full pytest run between commits).

### F. Open at end of window

* **Collection ADOPT / SCAN paths**: there's no ADOPT path for a collection theme (collections have no sidecar mp3 to claim). SCAN ALL on the themes_dir won't find any collection-shaped folders. Both surfaces correctly no-op on collection rows via the existing data prerequisites; no explicit gating needed.
* **Collection UNPLACE → Plex DELETE**: api_unmanage_item / api_forget_item on a collection row DELETEs the placements DB row but leaves the Plex-uploaded theme attached. This matches movies/tv UNMANAGE semantics ("stop tracking, leave content alone"). For an explicit "remove theme from Plex" affordance, plex.delete_collection_theme(rk) is available (Phase 2 method) but not wired into the API yet. Could be a v1.18.1 follow-up if user feedback indicates it.
* **Per-section collection enablement**: currently plex_enum enumerates collections for every included section. Some users might want a per-section "track collections in this section?" toggle. Not requested yet; design space left open.

### G. Lessons

* **Multi-phase ships beat single mega-commits.** Even though v1.18.0 is one tag, the 7-commit sequence let each step land with its own tests + commit message. If a Phase 4 regression surfaced post-ship, `git revert` on that single commit would back out the worker adapter without touching schema / sync / frontend. Smaller blast radius per change unit.
* **Empty string as marker needs explicit JS bridges.** The `media_folder=''` choice was deliberate (avoids the FK NOT NULL constraint dance NULL would require) but creates a SQL vs JS truthiness asymmetry. Always check the consuming language's falsy rules when adding a semantic marker.
* **Themerr-plex's audited code is gold for compat work.** Knowing themerr-plex used `Collection.uploadTheme()` (Python-plexapi wrapper around the same HTTP endpoint) meant Phase 2's `upload_collection_theme` could land with high confidence — no Plex-side experimentation needed.
* **Tests as architecture pins.** Each phase's tests pinned the cross-phase contract: Phase 1's CHECK-accepts-'collection' test would fail if a later phase tried to revert; Phase 5's `_SRC_LETTER_SQL`-stays-single-definition test guards against future drift into a collection variant. The pins make the architectural decisions searchable + enforced.
* **`_widen_check_constraint` will pay rent on future widenings.** The dynamic-rewrite approach handles arbitrary CHECK clauses without hardcoding. Future schema widenings (a `placement_kind='cloud'` for Plex Pass cloud uploads, say) can reuse it directly.


## 18. Digest: v1.18.1 → v1.18.10 (Collections feature-burn — 10 patches)

The v1.18.0 ship had ambitious scope (full Plex Collections support, 7-phase architecture) but landed with a class-9 catastrophic regression that wasn't caught until production testing. The recovery + follow-on patch cycle ran 10 tags over the v1.18.0 → v1.18.10 window, surfacing three new patterns worth documenting beyond their per-tag fixes.

Suite trajectory: **2598** (pre-v1.18.0 / end of § 17 boundary) → **2765** at v1.18.10 (+167 tests, mostly Phase-1-through-7 + per-patch regression pins).

### A. Tags shipped

- **v1.18.1** — per-library section toggles on /collections. v1.18.0 reused STANDARD/4K which was a category error: Plex collections span every managed section, aren't 4K-tagged. New `library_section_state(tab)` Jinja helper + `?section_id=` route param + JS chip handler + localStorage persistence (`motif:collections-section`).

- **v1.18.2** — collections TDB matching + on-disk staging path. Three v1.18.0 regressions: (a) `resolve_theme_ids` never linked collection rows (the year-required `sql_title` pass skipped them since collections have no year on either side); (b) sync orphan-promotion didn't fire for collections for the same year-required short-circuit reason; (c) collection downloads landed in `themes/<section>/` mixed with movies. Fixes: 4th SQL pass `sql_collection_title` (title-only, scoped to media_type='collection'), title-only orphan promotion branch, `_do_download` re-bases to `themes/collections/<section>/`.

- **v1.18.3** — /collections nav active-state + chip sizing parity. Two cosmetic regressions: no green underline under COLLECTIONS in the topbar (path→nav map at `app.js` missed `/collections`); per-section chips rendered smaller than STANDARD/4K (the v1.13.18 hero-chip CSS rule was scoped only to `aria-label="resolution"`).

- **v1.18.4** — `// REFRESH COLLECTIONS` label + collections-only enum. Two fixes: `libraryRefreshLabel()` had no `collections` entry → SSR-rendered label flickered to PLEX on first poll tick; `run_plex_enum` gained `collections_only=True` kwarg that skips `/library/sections/{id}/all` (~16K items on the user's install) + `_verify_theme_claims` + the `last_enum_content_changed_at` stamp. Only the collection-pass runs. ~10× faster refresh on populated installs.

- **v1.18.5** — **CRITICAL — fix v55 migration FK cascade + recover lost data.** The v1.18.0 catastrophic regression. See § B below for the deep story.

- **v1.18.6** — accept `media_type='collection'` on per-item API endpoints. The top-of-`api.py` `MediaType = Literal["movie", "tv"]` alias drove FastAPI's path-parameter validation on ~50 per-item endpoints. Every collection-row click on REDOWNLOAD / INFO / FORGET / etc. returned 422. Also: `api_manual_url` mapped `pi.media_type='collection'` → `theme_media_type='movie'` (fall-through branch) → orphans created at `movie/-N` instead of `collection/-N` → worker dispatched `_do_place` instead of `_do_place_collection` → "Skipped placement: no_match" (no folder).

- **v1.18.7** — recovery walker: ratio-based detection + visible logging. The v1.18.5 detector required `local_files == 0 AND placements == 0`. the user's single post-bug manual SET URL inserted ONE `local_files` row, which silently disqualified his entire library from auto-recovery. v1.18.5 walker no-op'd on his install with zero log lines. v1.18.7 detection switches to a ratio + floor heuristic: trigger when `local_files < 50% of plex_items_with_sidecar` (and sidecar count ≥ 50 to dodge fresh-install false positives). Every branch logs the decision so future "why isn't recovery firing" debug sessions have signal.

- **v1.18.8** — post-PURGE recovery path: RESTORE + DELETE menu items. Three coordinated bugs broke the post-PURGE recovery on zombie-orphan rows: (a) `has_previous_url` SQL used `!=` which evaluates as UNKNOWN against NULL (post-PURGE zombies have NULL on the right side) → CASE returned 0 → no RESTORE button. (b) PURGE button stayed visible on zombies whose lf/pl/url were all gone — clicking it ran an idempotent no-op. (c) `delete-orphan` action handler at `app.js:11795` was dead code with no menu entry. Fixes: `IS NOT` (NULL-safe), `orphanHasPurgeableState` gate, DELETE menu item with RESTORE-aware tooltip.

- **v1.18.9** — collection upload multipart + force + diagnostic logs. The first attempt at fixing the "PL flashing amber but never links" symptom the user reported for collection theme uploads. (a) Switched `upload_collection_theme` from raw-body POST to multipart/form-data POST (matches `python-plexapi`'s `Collection.uploadTheme(filepath=...)` shape) with raw-body fallback on 4xx. (b) Added comprehensive diagnostic logging at INFO level so every attempt logs URL + body size + HTTP status + truncated response + wall-clock time. (c) Defaulted `force_overwrite=True` for collections to bypass the skip-if-plex-has-theme gate. The (c) change was reverted in v1.18.10 per the user's preference; (a) and (b) stayed.

- **v1.18.10** — recover lost user_overrides + restore collection skip semantic. Three coordinated changes addressing the user's v1.18.7 deploy report: (a) New walker `maybe_recover_lost_user_overrides` scans the `events` table for historical "Manual URL set by admin" entries to recover user_overrides rows the v1.12.60 orphan-sweep deleted against the v1.18.0-broken DB state; reclassifies recovered `local_files.source_kind` from `'adopt'` → `'url'` so SRC renders U. (b) Reverted v1.18.9's collection default-force; Plex-has-theme respected same as movies/shows. (c) Collection skip path now stamps `plex_independent_theme=1` on the matched rating_key so the row renders as PS (Plex Serving) state instead of stalling at PL=await with !P attention nag.

### B. The v1.18.5 story — class-9 catastrophic data loss

v1.18.0's `_widen_check_constraint` rebuilt CHECK-constrained tables (themes, plex_items, placements, previous_urls, section_failure_acks) via SQLite's canonical four-step dance:

```
CREATE TABLE <name>_new with widened CHECK
INSERT INTO <name>_new SELECT FROM <name>
DROP TABLE <name>
ALTER TABLE <name>_new RENAME TO <name>
```

`_migrate_v54_to_v55` invoked the helper for `themes` FIRST in its widening list. The migration set `PRAGMA defer_foreign_keys = ON` under the assumption that it would suppress FK cascades during the DROP TABLE step.

That was wrong. **`defer_foreign_keys` only defers FK violation CHECKS to COMMIT time. It does NOT defer cascading actions** (`ON DELETE CASCADE`, `ON DELETE SET NULL`). When `DROP TABLE themes` fired, SQLite immediately ran:

- `ON DELETE CASCADE` on `local_files (media_type, tmdb_id)` → every `local_files` row deleted
- `ON DELETE CASCADE` on `placements (media_type, tmdb_id)` → every `placements` row deleted
- `ON DELETE SET NULL` on `plex_items.theme_id REFERENCES themes(id)` → every plex_items.theme_id nulled

Net effect on every install that ran the v55 migration with existing data: **total loss of motif's tracking metadata across all libraries**. The on-disk `theme.mp3` files survived (the filesystem wasn't touched); Plex's local-media-assets agent still saw them so `pi.local_theme_file=1` stayed; but the SRC letter SQL renders every previously-T/A/U row as 'M' because `p.media_folder IS NULL AND pi.local_theme_file = 1` is the M predicate.

the user's screenshot of 10,390 movies all reading SRC=M / DL=off / PL=off / LINK=none was the alarm.

**Part 1 — Prevent.** v1.18.5 switched `_widen_check_constraint`'s pragma dance from `defer_foreign_keys = ON` to `foreign_keys = OFF` — the SQLite-recommended pattern per [sqlite.org/lang_altertable.html](https://sqlite.org/lang_altertable.html) § "Making Other Kinds Of Table Schema Changes." `foreign_keys=OFF` disables BOTH violation checks AND cascading actions for the duration of the rebuild. Closed out with `PRAGMA foreign_key_check` (defensive integrity probe) and `PRAGMA foreign_keys = ON` (restore enforcement) via try/finally so a crash mid-rebuild can't leak FKs-off state.

**Part 2 — Recover.** New module `app/core/recovery_v55.py` with `maybe_recover_post_v55_data_loss(db_path, themes_dir)`. Invoked from `main.py` after `init_db`. Detects the loss pattern (themes populated, plex_items have `local_theme_file=1`, local_files coverage < 50% of sidecar evidence per the v1.18.7 ratio fix) and rebuilds tracking from on-disk state:

- For each themes row T + managed section S: check if the expected canonical path (`themes_dir/<themes_subdir>/canonical_theme_subdir(T.title, T.year)/theme.mp3`) exists.
- If yes → INSERT `local_files` with provenance/source_kind inferred from T.upstream_source + user_overrides presence (`plex_orphan` → adopt → A; TDB + override → url → U; TDB no override → themerrdb → T).
- For each plex_items row pi with local_theme_file=1 matching T (via guid_tmdb OR title_norm+year), INSERT placements pointing at `pi.folder_path` with `placement_kind='hardlink'`.
- Post-walk: `resolve_theme_ids` re-links `plex_items.theme_id`.
- Stamps `runtime_settings.recovery_v55_done_at` so subsequent boots skip.

The v1.18.10 secondary walker (`maybe_recover_lost_user_overrides`) handles the further data loss caused by the v1.12.60 orphan-sweep running on the broken DB state. See § D below.

### C. Three new patterns from this window

#### Pattern: FK cascade actions are NOT deferred by defer_foreign_keys

The SQLite docs are subtle on this — `defer_foreign_keys = ON` SOUNDS like it'd defer all FK behavior to COMMIT, but it only defers **constraint VIOLATION CHECKS**. Cascading **ACTIONS** (`ON DELETE CASCADE`, `ON DELETE SET NULL`, `ON DELETE SET DEFAULT`) fire immediately at row-delete or table-drop time regardless.

**The canonical SQLite table-rebuild pattern is `foreign_keys = OFF`**, NOT `defer_foreign_keys = ON`. The recommended dance:

```
PRAGMA foreign_keys = OFF;
BEGIN;
  -- rebuild
  CREATE TABLE new_x ...;
  INSERT INTO new_x SELECT ... FROM x;
  DROP TABLE x;
  ALTER TABLE new_x RENAME TO x;
PRAGMA foreign_key_check;  -- surface any orphaned refs
COMMIT;
PRAGMA foreign_keys = ON;
```

Added to CLAUDE.md class-9 catalog as a sub-pattern. Any future schema migration that rebuilds a parent table (CHECK widening, column-shape change, etc.) needs this pattern.

#### Pattern: post-data-loss sync sweeps amplify the damage

The v1.18.0 FK cascade nuked local_files + placements but NOT user_overrides (no FK to themes). HOWEVER, the next sync ran the v1.12.60 orphan-sweep that DELETEs user_overrides whose "theme presence" check fails. With local_files + placements already wiped by the upstream bug, the sweep's EXISTS check found nothing, and 98 user_overrides rows were collateral damage.

**The lesson: defensive sweeps that delete "stale" data can amplify damage from earlier bugs.** Any sweep that runs `DELETE WHERE NOT EXISTS (...)` is implicitly trusting the state of the EXISTS-checked tables. If a prior bug broke those tables, the sweep cascades the damage.

Pattern-fix: defensive sweeps should EITHER stage a dry-run / soft-delete pattern (so the damage is reversible) OR check for a "trust score" before nuking (e.g., "if more than X% of rows would be deleted, abort and alert"). The v1.18.10 walker recovers from this specific instance by reading the `events` table — historical audit trail as recovery source — but the more durable fix is teaching the sweeps to fail-safe under broken-state.

Added to CLAUDE.md class-9 catalog as the **"amplifier sweep"** sub-pattern.

#### Pattern: events table as recovery source

motif's `events` table (the in-app audit log) preserves a substantial amount of user-action history: every SET URL, every PURGE, every UPLOAD MP3, etc. logs an event with a stable message format + structured `detail` JSON.

The v1.18.10 walker leverages this directly: when user_overrides was wiped, the URL the user had set was still recoverable by parsing `"Manual URL set by admin: <url>"` events with `detail.rating_key`. The rating_key resolves through plex_items → themes to identify which orphan row got which URL.

**The events table is a write-ahead log of user intent.** When derivative state (user_overrides, local_files, placements) gets wiped but the events table survives, recovery is possible. Future migrations / data-fix paths should consider it a first-class recovery source.

Added to PROJECT_HISTORY § 9 (development meta-patterns) as a documented pattern.

### D. The v1.18.10 secondary walker

After v1.18.5's primary walker recovered local_files + placements, the user reported that some orphans still showed as M instead of U. The M*A*S*H 30th Anniversary Reunion example: orphan tmdb=-22 with SET URL'd YouTube URL, last placed 5/5/2026. The provenance log showed the SET URL clearly. But the row read M.

Diagnosis: user_overrides for that row was gone. The v1.18.5 walker's `_infer_source_kind` saw `upstream='plex_orphan' AND no user_overrides` → returned `('manual', 'adopt')` → row classified as A (which the v1.18.5 walker had already inserted on disk; v1.18.7 ratio detection then triggered the walker to fire even though one row existed).

Wait — that explains M not A? Looking more carefully: the SRC letter SQL classifies `(manual, adopt)` as A only when source_video_id has the orphan's adopt-shape. Without that condition fully matching, the row defaults to M (manual sidecar). The actual issue was the missing user_overrides AND the missing 'url' source_kind together.

The walker `maybe_recover_lost_user_overrides` scans `events` for `"Manual URL set by admin: ..."` entries (regex extracts the URL; `detail.rating_key` gives the Plex item), resolves through plex_items → themes, INSERTs missing user_overrides, and UPDATEs `local_files.source_kind` from `'adopt'` → `'url'`.

Independent runtime_settings marker (`recovery_user_overrides_done_at`) so it runs on installs where the primary v1.18.5 walker already finished. Idempotent via INSERT OR IGNORE + the marker. Won't clobber a user's later SET URL — the EXISTS check skips rows that already have a user_overrides entry.

the user's deploy after v1.18.10 should see logs like:
```
v1.18.10 user_overrides recovery: scanning N historical SET URL events
v1.18.10 user_overrides recovery: extracted N (rating_key → URL) mappings from events
v1.18.10 user_overrides recovery: DONE — N user_overrides re-inserted, N local_files reclassified to source_kind='url'
```

Pending: the user's v1.18.10 test confirms the M*A*S*H-style rows flip back to U.

### E. Open at end of window

- **Confirm v1.18.10 recovery on the user's install** — needs his post-deploy log share.
- **`api_upload_theme`** (UPLOAD MP3) — still rejects collections with a confusing path-construction failure. Needs a branch dispatching to `PlexClient.upload_collection_theme`. Probably v1.18.11 if the user wants it.
- **`api_adopt_folder`** / **`api_replace_with_themerrdb`** — folder/sidecar concepts don't apply to collections. Existing path-checks reject with HTTP 409, so behavior is correct if ugly.
- **Plex collection enumeration on TV-only sections** — works fine, but if a section type genuinely doesn't support collections, `enumerate_collections_for_section` would return empty silently. Class-9 hygiene says we should log the empty case explicitly; deferred until reported.

### F. Lessons

- **Production testing catches what unit tests can't.** The v1.18.0 ship had Phase 1's CHECK-widening unit test passing on an empty fixture DB. The catastrophic data-loss bug only surfaced when the migration ran on a populated install. Lesson: every schema-rebuild migration deserves an end-to-end fixture test that seeds RELATED data (placements, local_files, plex_items.theme_id) and verifies survival of dependent rows post-rebuild. The v1.18.5 test file (`test_v1_18_5_migration_fix_and_recovery.py`) added exactly this and would have caught the v1.18.0 bug at unit-test time.

- **`defer_foreign_keys` is a footgun.** The pragma's name suggests it defers ALL FK behavior; it only defers CHECKS. Cascading actions fire regardless. CLAUDE.md now documents this trap.

- **Silent-skip is a class-9 sub-pattern even on RECOVERY code paths.** v1.18.5's `_detect_loss_pattern` silently returned False on the user's install because his single post-bug SET URL inserted one local_files row. No log line, no signal — the user pulled v1.18.5, restarted, and saw no change. Recovery code is exactly the kind of cold-path code that needs MORE logging than the happy path, not less. v1.18.7 fixed this by logging every detection branch explicitly.

- **Class-9 has a new sub-pattern: amplifier sweep.** Defensive `DELETE WHERE NOT EXISTS (...)` sweeps that "clean up stale" data can amplify damage from prior bugs. The fix isn't to disable the sweep but to teach it to fail-safe under suspected broken state.

- **Events table is the write-ahead log of user intent.** Future migrations / data-fix paths should consider it a first-class recovery source. v1.18.10 walked it for `"Manual URL set by admin"` events to recover user_overrides; the same pattern would work for other lost user-state recovery scenarios.

- **One bug can have multiple cascading consequences.** v1.18.0's FK cascade nuked local_files + placements directly. The v1.12.60 sweep then ran on the broken state and wiped 98 user_overrides as collateral. the user's first deploy of v1.18.0 set off a chain of state-damage events; we needed TWO recovery walkers (v1.18.5 + v1.18.10) to fully restore his install. Always ask: what other system component might be running against the broken state and amplifying the damage?


## 19. Digest: v1.18.31 → v1.18.40 (Plex theme API investigation + LPS production landing)

the user's safety question on v1.18.27/28 ("if we LET PLEX SERVE on a row that had a `+P` stamp from the now-defunct themerr-plex plugin, do we lose Plex's original theme?") opened a 10-tag investigation+fix arc. The series mapped Plex's theme HTTP API empirically (six read-mostly probe tags), shipped the real fix (v1.18.36), patched two follow-on bugs the live test surfaced (v1.18.38 + v1.18.39), and landed an operator dashboard for the diagnostic (v1.18.40).

Suite trajectory: **2954** (pre-v1.18.31) → **3034** at v1.18.40 (+80 tests, mostly probe-shape pins + LPS branch coverage + dashboard rendering).

### A. The investigation chain

**v1.18.31 — GET probe of `/library/metadata/{rk}/themes`.** First diagnostic endpoint. Returned shape: `MediaContainer.Metadata[]` with `ratingKey`, `key`, `thumb`, `selected`. **Key finding**: every theme entry has its own ratingKey of form `upload://themes/<sha1>` (uploaded via API) or `metadata://themes/<sha1>` (Plex's own agent themes). the user's probe on 5 rks showed coexisting entries — M*A*S*H rk=530614 had 3 (1 metadata + 2 upload), `*batteries` rk=533875 had 2 uploads, etc.

**v1.18.32 — DELETE probe with 3 URL shapes**: `query` (`DELETE .../themes?url=<encoded>`), `subpath` (`DELETE .../themes/<encoded>`), `put_unselect` (`PUT .../themes?url=<encoded>&unset=1`). All three returned 404 against Plex. The plural `/themes` endpoint has no DELETE handler in Plex's router.

**v1.18.33 — python-plexapi archaeology + 4th probe shape.** Pulled `python-plexapi/mixins/resources.py` via `gh api`: `uploadTheme()` docstring says "**Warning: Themes cannot be deleted using PlexAPI!**" and `setTheme()` raises `NotImplementedError`. But `deleteTheme()` exists and uses a different URL — `/library/metadata/{rk}/theme` (**SINGULAR**, no `s`). Added `singular_delete` shape; the user's probe confirmed: **HTTP 200, all entries preserved, only the `selected:true` flag cleared**. The "lock the field" side-effect (per OpenAPI) doesn't block subsequent POSTs to plural `/themes` — motif's normal upload path overrides the lock.

**v1.18.34 — POST/PUT singular `/theme?url=...` probe**. The OpenAPI documented this as "Set an item's artwork, theme, etc" with a `url` param ("the url of the new asset"). Tested both methods. POST returned 404 (no handler at all); PUT returned 500 (route exists, but the `?url=` parameter is for uploading from a REMOTE url, not for selecting an existing internal entry by its `upload://`/`metadata://` rk). **There is no native "select an existing theme entry" API in Plex.** python-plexapi's NotImplementedError finally explained.

**v1.18.35 — Re-upload trick probe.** Plex content-dedupes uploads by SHA-1 (confirmed via repeated probes — M*A*S*H's repeated RE-PUSHes all landed on the same hash, size stayed at 2). So motif can "select" any existing entry by: (1) GET its bytes from `/library/metadata/{rk}/file?url=<entry-rk>`, (2) POST those bytes back to `/themes`. Plex sees the same hash → re-uses the existing entry → marks selected. Verified on the user's 12 Monkeys (rk=124233, themerr-plex `upload://themes/d02ec955...`): 2.37MB fetch, ~1MB POST (multipart), HTTP 200, post-state showed the entry still `selected:true` (no new duplicate created). **The only viable path for LPS auto-restore.**

### B. The production fix — v1.18.36

Shipped the bundle the probe series enabled:

- **URL change**: `delete_collection_theme` + `delete_theme` (alias): URL plural → singular (`/library/metadata/{rk}/theme`). **Latent bug fix**: this URL had been returning 404 silently since v1.18.0 — every collection UNPLACE/PURGE was a Plex-side no-op (motif's DB updated, Plex kept serving). Two months of silent failure with zero visible UI effect because nothing upstream asserted the return value.

- **New `PlexClient.set_active_theme_via_reupload(rating_key, theme_rating_key)`**: production-grade wrapper of the re-upload trick. Shares a `_fetch_and_reupload_theme` helper with the v1.18.35 probe so the diagnostic and production paths can't drift.

- **`api_unplace_item` plex_upload branch**: pre-v1.18.36 the unplace handler only walked `placements.media_folder` and unlinked `theme.mp3`. For plex_upload rows `media_folder=''` (sentinel) and the unlink silently no-op'd (`Path('') / 'theme.mp3'` → `.theme.mp3` in CWD, `.is_file()` False). Same shape as the v1.18.0 URL latent bug — UI updated, Plex unchanged. v1.18.36 added a branch: hash motif's canonical (identifies motif's own `upload://themes/<hash>` entry), GET `/themes`, pick fallback (prefer `metadata://`, fall back to `upload://` where hash ≠ motif's), DELETE singular `/theme` to clear motif's selection, re-upload trick on the fallback to restore Plex's serving.

- **`api_switch_placement` outgoing teardown (the user's M*A*S*H bug)**: pre-fix SWITCH dropped the placement DB row + queued a new place job — never tore down the OUTGOING placement's artifact. file→api left the sidecar `theme.mp3` orphan on disk; api→file left motif's Plex API upload as `selected:true` so the new sidecar lost Plex's resolution race. v1.18.36 explicitly tears down: file→api unlinks the sidecar, api→file calls `plex.delete_theme` (now singular, working).

- **Audit + log expanded**: `api_unplace_item` reports `api_handled` + `api_restored` counts so operators see whether the plex_upload teardown fired.

### C. The follow-on bugs — v1.18.37, v1.18.38, v1.18.39

v1.18.36's live verification on the user's install surfaced three issues in adjacent code I didn't write tests for:

**v1.18.37 — motif_hash diagnostic + Plex-side drift scanner.** the user's 12 Monkeys LPS test log showed `motif_hash=None`. The fallback heuristic picked correctly only by positional luck. v1.18.37 added: (a) explicit log breadcrumb naming the failure mode (`no local_files row` / `empty file_path` / `file missing: <path>` / `io error`) + unscoped fallback lookup, (b) a new module `app/core/orphan_scan.py` + admin endpoint `GET /api/admin/orphan-scan` that walks every plex_upload placement, cross-checks Plex's /themes, classifies drift across 8 types (`ok` / `rk_lookup_failed` / `plex_fetch_failed` / `no_plex_entries` / `motif_hash_unknown` / `motif_entry_missing` / `motif_not_selected` / `nothing_selected`).

**v1.18.38 — LPS restore fires for sidecar placements too.** the user's 100% Wolf test caught a design oversight. Sequence: REPLACE TDB(kind=api) → SWITCH(api→file) → LPS. Expected: row returns to P serving themerr-plex's theme. Actual: row went themeless (SRC=`-`), `api_handled: 0, api_restored: 0`. The v1.18.36 plex_upload LPS branch was gated on `placement_kind == 'plex_upload'`. By LPS time the placement was 'hardlink' (from the SWITCH), so the restore logic didn't fire. v1.18.38 unified the loop: walks `sidecar_placements + api_placements`, fires DELETE singular only when `was_plex_upload=True`, runs the re-upload trick for BOTH kinds. Sidecar rows after a SWITCH cycle (or with Plex Pass themes) now auto-restore correctly.

**v1.18.39 — resolve local_files.file_path against themes_dir.** v1.18.38 re-test surfaced `motif_hash unavailable for movie/520946 section_id=1 — reason: file missing: movies/100% Wolf (2020)/theme.mp3` — a RELATIVE path that doesn't exist at the filesystem root. **motif stores `local_files.file_path` relative to `settings.themes_dir`**, per the `themes_dir / local["file_path"]` pattern in worker.py:1812 + 2347. v1.18.36's hash code missed the join. The v1.18.37 orphan scan corroborated the breadth: `{"motif_hash_unknown": 10}` across the user's install. v1.18.39 added `_resolve_canonical(themes_dir, rel_or_abs)` to orphan_scan + matching join in api_unplace_item. the user's re-scan post-deploy: `{"ok": 10}` — every row now correctly classified.

### D. The dashboard — v1.18.40

`/admin/orphans` HTML page. Hits the v1.18.37 scan endpoint, renders findings as a filterable table with per-row action buttons (RE-PUSH / LET PLEX SERVE / PURGE / PROBE) that fire motif's existing endpoints. No new mutation paths — page is a thin wrapper around tested flows. Drift-type chips use motif's existing `lib-source-X` tone vocab (green = healthy, amber = needs attention, red = serious) for visual consistency.

### E. Patterns / invariants captured

- **`gh api` for library archaeology**: pulling python-plexapi's `mixins/resources.py` + themerr-plex's `plex_api_helper.py` + `general_helper.py` resolved questions that probing alone would have taken hours. themerr-plex's `shutil.rmtree` on `Uploads/themes` told us instantly that Plex has no targeted-DELETE HTTP API; python-plexapi's `deleteTheme()` URL told us the SINGULAR endpoint exists. Both lookups took <5 minutes.

- **Probe-first cadence**: 6 read-mostly probe tags before any production change. Each probe answered one question (GET shape, DELETE URL shape, POST/PUT select feasibility, re-upload trick). Better than one ambitious tag that lands on the wrong design. Template for any "we're touching someone else's system and don't know what works" investigation.

- **Plural-vs-singular endpoint convention** (now CLAUDE.md class 11): the Plex theme API has two URLs with completely different semantics. Motif's silent v1.18.0 → v1.18.36 bug was hitting the wrong one. Worth documenting in CLAUDE.md so future contributors don't repeat.

- **No native "select existing entry" API** → the **re-upload trick** is the canonical workaround. Plex's content-dedup-by-SHA-1 is the underlying mechanism that makes it work; without dedup we'd accumulate duplicates per LPS click.

- **Diagnostic features prove their value within days.** The v1.18.37 motif_hash diagnostic log was added "for future LPS drift" — by v1.18.39 it had already revealed the relative-path bug. The orphan scan added in v1.18.37 found the same bug at scale via `{"motif_hash_unknown": 10}`. Sweat the diagnostic logging early; it pays back fast.

- **State-derived behavior beats kind-gated behavior** (v1.18.38). v1.18.36's LPS restore keyed on motif's last `placement_kind` and broke after a SWITCH had changed the kind. v1.18.38's fix queries Plex's actual `/themes` state and decides from there. Generalizable: when motif's behavior depends on another system's state, query that state — don't infer it from motif's tracking.

- **Bugs cluster in big code drops.** v1.18.36 shipped with BOTH the placement_kind gating bug (fixed v1.18.38) AND the relative-path bug (fixed v1.18.39). After live-install testing surfaces ONE issue in a big code drop, look for adjacent issues in the same code before assuming the rest is fine.

- **Operator UI for diagnostic data has high leverage** (v1.18.40). v1.18.37's JSON endpoint was useful but only at the cost of a DevTools fetch. v1.18.40's page makes the same data instantly actionable. Pattern: every JSON diagnostic endpoint should have an operator-facing UI within a tag or two if the data is something operators routinely look at.

- **Latent-bug-via-always-False-return** (v1.18.0 collection delete): `delete_collection_theme` returned False silently on every call for ~2 months. Nothing upstream asserted on the True/False — the operation was "best-effort, log and move on." Pattern: any best-effort Plex op that can return False should have at least one upstream check that warns when False is the consistent outcome (e.g., a periodic sweep that reports "delete_theme returned False for N consecutive calls — Plex API may have changed").

### F. Latent bugs surfaced (and fixed) in this digest's window

| Bug | Latency | Fix tag |
|---|---|---|
| `delete_collection_theme` URL plural (404 silently) | v1.18.0 → v1.18.36 (~2 months) | v1.18.36 |
| `api_switch_placement` leaves sidecar orphan (file→api) | v1.18.23 → v1.18.36 | v1.18.36 |
| `api_switch_placement` leaves Plex API upload selected (api→file) | v1.18.23 → v1.18.36 | v1.18.36 |
| LPS plex_upload restore gated on placement_kind | v1.18.36 → v1.18.38 (hours) | v1.18.38 |
| motif_hash computed against relative path as if absolute | v1.18.36 → v1.18.39 (hours) | v1.18.39 |



## 20. v1.18.61 — full-application audit (3-agent pass) + roadmap

the user's request after the v1.18.60 cleanup-symmetry tag: "do a full audit of the application since we've made a lot of changes lately, let's test edge cases we may not have tested before and really broaden our scope to make sure we're not overlooking anything. Let's also review design choices to make sure we're meeting our design documented rule and style choices."

Ran 3 general-purpose audit agents in parallel:
1. **Class-9 silent-failure sweep** — 111 silent `except` blocks examined
2. **Edge-case + race-condition audit** — schema CHECK constraints, concurrency, multi-section, bulk actions, Plex failure modes
3. **Design-system compliance** — DESIGN_SYSTEM.md + CLAUDE.md UI conventions

20+ findings consolidated. The v1.18.61 tag ships the visible-breakage HIGH items; the rest is deferred to follow-up tags with a documented roadmap.

### Shipped in v1.18.61

- SYNC THEMERRDB button text live-syncs when AUTO-REFRESH PLEX setting toggles (the user's specific report)
- 6 undefined CSS tokens remapped to canonical motif tokens (`--bg-ridge` / `--bg-recess` / `--ink` / `--ink-dim` / `--ink-faint` / `--bg-deep` — all referenced but never defined; rules silently rendered with `color: unset` / `border-color: transparent`)
- v1.18.60 `_teardown_plex_api_artifacts_for_placements` helper's `plex_mt` mis-mapping for collections fixed (was: `'show' if 'tv' else 'movie'` → collections got 'movie' so fallback rk lookup silently missed)
- Forward-looking CSS-token audit guard test — fails forever-after if a new undefined token lands

### Deferred — HIGH severity items + suggested tag roadmap

| Tag    | Item                                                        | Severity / location                                    |
|--------|-------------------------------------------------------------|--------------------------------------------------------|
| v1.18.62 | **HIGH-B**: PURGE/UNPLACE/DELETE don't cancel running jobs | data corruption risk — api.py:13533, 12180, 14005     |
| v1.18.63 | **HIGH-C**: bulk_lps plex_upload teardown                  | completes v1.18.60 scope — api.py:3850-4046           |
| v1.18.64 | **HIGH-A**: worker payload-parse cluster (6 sites)         | contract-drift hazard — worker.py:765, 794, 1335, 1756, 1824, 2204 |
| v1.18.65 | **HIGH-D + HIGH-E**: bulk body-parse silent fallback       | api.py:16915 (bulk-probe-tdb), api.py:17031 (bulk-lps) |
| v1.18.66 | **HIGH-F + HIGH-G**: silent-fallback returns with no log   | api.py:4437 (_tab_availability_for_nav), scanner.py:431 |
| hygiene  | MED + LOW items rollover                                   | ~15 items including ops.css token misuse, inline styles, missing log breadcrumbs |

### Patterns reinforced (4 tags running)

The audit + audit-doc + roadmap pattern is now well-established:
- v1.18.52 row-refresh contract guard
- v1.18.53 status-bar three-map audit
- v1.18.57 settings-design audit
- v1.18.60 cleanup-symmetry audit (with `AUDIT_EXCEPTIONS` list)
- v1.18.61 full-application audit (with PROJECT_HISTORY § + roadmap)

Each audit produces:
1. Findings catalog (this § for v1.18.61)
2. Fixes for highest-impact items in the audit tag itself
3. Roadmap for deferred items in follow-up tags
4. Forward-looking guard test (where applicable) so the same gap can't reopen

the user's observation in v1.18.59 — "v1.X.Y partial fix → v1.X.Y+1 complete fix" — is the canonical shape. The v1.18.60→v1.18.61 sequence confirms it (v1.18.60 introduced the `plex_mt` mis-mapping bug; v1.18.61 caught it in audit).

### Source

Full findings + per-item rationale lives in `docs/AUDIT_2026_05_22.md` (gitignored per the .gitignore convention for local audit working artifacts). The HIGH items + roadmap are pinned here for the public record.


## 21. Digest: v1.18.62 → v1.18.75 (pending TDB URL + audit rollover cycle)

The v1.18.61 audit roadmap targeted v1.18.62 → v1.18.66 for specific HIGH items, but the user's bug-report cadence drove those tags onto a different arc — surfacing pending ThemerrDB URLs in the info card, then closing the data-integrity gaps the user testing exposed. The audit roadmap shipped behind-schedule in v1.18.71-73 once user pain had calmed down.

### Arc 1 — pending TDB URL surfacing + ACCEPT UPDATE correctness (v1.18.62 → v1.18.67)

the user's "Am I Actually the Strongest?" row drove this entire arc. The row had a user override (`je_uIV5zv5c`), a dead committed TDB URL (`kEp_ZMPWWdU`), AND a fresh pending TDB URL (`8budHRQkBLU`). Five tags chased its symptoms before the data-integrity root cause was found:

- **v1.18.62** — PROPOSED CHANGE diff respects user override. `renderPendingUpdateDiff` was rendering CURRENT from `pu.old_youtube_url` (stale TDB at detection time) instead of the user's actual override. Threaded `ovr` parameter through the function; when present, CURRENT tile uses `ovr.youtube_url` + label "(your URL)" + header hint "ACCEPT UPDATE drops your override and switches to the new TDB URL."

- **v1.18.63** — surface pending TDB URL + fix stale thumbnail. The info card's "themerrdb url" line showed `themes.youtube_url` (committed snapshot) but the pending URL was what TDB currently claimed. Added `_pendingTdbUrl = (data.pending_update.decision === 'pending' && kind !== 'urls_match') ? new_youtube_url : ''` dispatch; `tdbUrl = _pendingTdbUrl || _committedTdbUrl`. Same dispatch on PROBE TDB URL endpoint backend (`target_url = pending_url or committed_url`). Plus: ytId derivation flipped to extract-from-URL-first for YouTube (lf.source_video_id was stale from the pre-override download).

- **v1.18.64** — sync notification clarity. the user's separate complaint about Discord notifications: "1 items processed, 0 new, 1 updated" gave no hint which item updated. Added `SyncStats.updated_titles` capture; sync_completed body now lists updated titles (mirroring themes_added_by_sync's new-titles bullet list) + uses `body_format=markdown` + handles the 0/0/0 case with "No changes detected since last sync." instead of zeros.

- **v1.18.65** — pending pill beats failure + diff-tile thumb override-fresh. Two class-9 mirror-drift fixes. (a) `renderLibraryRow`'s inline TDB cell checked failure_kind BEFORE pending_update; `computeTdbPill` checked pending_update FIRST. Same priority logic, two sites, drifted. Re-ordered the inline block to match — a row offering an upgrade is actionable, blue ↑ beats historical red ✗. (b) Diff-tile thumbnail `currentVid = currentVidFromLf || extract(url)` short-circuited to stale lf id when an override existed. Flipped to extract-first when `hasOverride` (mirrors v1.18.63's info-card thumbnail fix).

- **v1.18.66** — **ACCEPT UPDATE writes new URL into themes (data integrity)**. The arc's actual root cause. v1.14.55 / v1.18.62 correctly preserve `themes.youtube_url` during sync when an override exists (override is authoritative). New URL lives in `pending_updates.new_youtube_url`. But ACCEPT UPDATE deleted the override + flipped decision='accepted' + enqueued a download WITHOUT updating themes.youtube_url. Worker's `yt_url = override or theme.youtube_url` fell back to the STALE OLD TDB URL → downloaded the wrong content despite the audit log showing the new URL. Fix: ACCEPT UPDATE now UPDATEs themes.youtube_url + youtube_video_id + clears failure_kind/failure_message/failure_acked_at inside the same transaction. Also resolves the "brief red flash" the user saw — pre-fix pending_update flag dropped while failure_kind survived; now the failure clears immediately because the user moved past the old URL.

- **v1.18.67** — polish + filter alignment. Three v1.18.66 verification reports rolled up: (a) `(pending — ACCEPT UPDATE to commit)` label shortened to `(pending)` + tooltip hint (was wrapping URL row). (b) Bottom thumbnail block hides when the PROPOSED CHANGE diff is visible — the CURRENT tile already shows the same thumbnail. (c) `attn_pills=update` filter switched from unsectioned `EXISTS pending_updates WHERE decision='pending'` to section-scoped `COALESCE(pu_sec.decision, pu_global.decision) = 'pending'` — matches `actionable_update`'s row-level predicate exactly. Pre-fix KEEP CURRENT removed the row's blue ! exclamation but the filter still matched because the '' global row stayed pending. Class-9 mirror-drift sibling of v1.18.65's TDB-pill priority fix.

### Arc 2 — destructive-before-confirm audit + atomic teardown (v1.18.68 + v1.18.71)

Discovered during v1.18.68's investigation: SWITCH PLACEMENT and REPLACE endpoints mutated Plex/disk state SYNCHRONOUSLY before enqueueing the place job that completes the operation. A place job failure left the row in a worse state than before — sidecar already unlinked, or Plex API selection already cleared. Two-tag fix mirrors v1.18.36's investigation cadence.

- **v1.18.68** — SWITCH PLACEMENT [file→api] atomic. the user's 27MB file repro: SWITCH PLACEMENT [file→api] unlinked the sidecar synchronously, then the Plex API upload failed (HTTP 500), leaving the row themeless until manual PUSH SIDECAR. Fix: sidecar paths threaded into the place job's payload as `remove_sidecar_paths`; worker unlinks them ONLY after a successful Plex upload. Failure leaves the sidecar intact.

- **v1.18.71** — SWITCH PLACEMENT [api→file] + REPLACE [kind=file] atomic. Inverse direction. Same shape: endpoints used to call `plex.delete_theme()` (or the shared teardown helper) synchronously before queueing the sidecar place job. Now rks threaded via `clear_plex_api_rks` payload; worker calls `delete_theme(rk)` ONLY after the sidecar hardlink lands. The shared helper stays synchronous for destructive callers (PURGE / FORGET / DELETE — no async work follows them).

### Arc 3 — Plex upload size handling (v1.18.69 + v1.18.70)

the user's 27MB upload exposed Plex's empirical ~25MB ceiling on theme uploads. Two-tag treatment: first add automatic recovery, then prevent the retry-loop when recovery isn't possible.

- **v1.18.69** — too-large → sidecar fallback. When `_do_place_collection`'s Plex API upload returns HTTP 500 AND size ≥20MB AND `media_type != 'collection'`, motif auto-falls-back to hardlinking the canonical to the Plex media folder. Row themes via HL/C instead of PU; WARNING event in HISTORY explains why. Collections still raise (no fallback target — no media folder).

- **v1.18.70** — size-rejection → `_JobPermanentFailure`. Even with v1.18.69's fallback, the failure path still exists for collections + fallback-itself-fails edge cases. Pre-fix the worker's retry-with-backoff (1m, 5m, terminal) burned ~6min before the job went terminal; the dashboard mini-bar showed "PLACE INTO PLEX QUEUED" the whole time. Plex consistently rejects the same bytes — retrying is pure waste. Classified the failure shape: HTTP 500 + size ≥20MB → `_JobPermanentFailure` (one-shot terminal); everything else → `RuntimeError` (retry budget intact). Bonus fix in same tag: `pagehide` listener flushes filter+search state on every navigation away, closing a click→nav race where `loadLibrary`'s save didn't commit before the user navigated.

### Arc 4 — audit rollovers (v1.18.72 + v1.18.73)

Two delayed rollovers from the v1.18.61 audit roadmap finally shipped once user pain had calmed.

- **v1.18.72** — class-9 silent-failure breadcrumb sweep. Audit (delegated to Explore agent) walked every try/except in app/, classified by severity. HIGH: 0 (prior sweeps covered them). MED: 2 — `_oEmbed_for` and `api_replace_item` body parse, both fixed with `log.debug` breadcrumbs. LOW: 15 catalogued (benign defensive catches on known-empty getters + parse-fallback paths). Debug level keeps cold paths quiet under default INFO but `--log-level=DEBUG` surfaces failures during investigation.

- **v1.18.73** — v1.18.61 audit roadmap closure (HIGH-B/C/D/E/F). (B) PURGE/UNPLACE/DELETE didn't cancel in-flight jobs → place/download/refresh could land bytes onto a row mid-destruction. Added shared `_cancel_jobs_for_row(conn, mt, tmdb, section_id?)` helper invoked from all three destructive endpoints before their DELETE blocks fire. (C) `_bulk_lps_run` didn't handle plex_upload placements — placements row got deleted but motif's API entry stayed serving in Plex. Widened the SELECT to include placement_kind + section_id; plex_upload placements route to the v1.18.60 teardown helper before the sidecar unlink loop. (D + E) Bulk endpoint body-parse silent fallbacks → log.warning breadcrumbs. (F) `_tab_availability_for_nav` silent all-False fallback → log.warning surfaces DB errors. (G) Re-verified scanner.py:431 st_ino swallow as benign (worst case: re-hardlink work, no data loss).

### Arc 5 — UX polish (v1.18.74 + v1.18.75)

Two visible-UX fixes that took multiple attempts to find the right layer.

- **v1.18.74** — close PROBE TDB URLS button gap (3rd attempt). the user's third report of the same gap. v1.17.5 / v1.18.57 / v1.18.59 each tried CSS — the actual root cause was a 10-line Jinja `{# ... #}` comment in the template between the form-actions div and the form-hint-divider hr. Jinja stripped the comment text but left the surrounding newlines, which under `.block-body`'s `white-space: pre-wrap` rendered as ~2 line-heights of visible gap. Prior CSS fixes addressed margins and child white-space but never the text-node-between-siblings root cause (`white-space: normal` on a child doesn't affect text nodes that live in the parent between siblings). Fix: delete the Jinja comment. New warning in the CSS for future template authors: multi-line `{# ... #}` blocks in pre-wrap contexts are visually load-bearing — stay on one line OR use `{#- ... -#}` whitespace-strip modifiers.

- **v1.18.75** — `isPlexAgentRow` excludes plex_upload. the user's repro: U+PU row → clicking UPLOAD MP3 or SET URL triggered the "Plex is already supplying a theme" defer-to-Plex prompt. That prompt is for true P-agent rows where Plex has its own theme that motif doesn't manage. A plex_upload row is motif's own content via Plex's HTTP API. Class-9 mirror-drift: computeSrcLetter's `placed` calculation was updated in v1.18.0 to recognize plex_upload but `isPlexAgentRow` wasn't. Fix: mirror the canonical `placed` shape — `motifPlaced = !!it.media_folder || it.placement_kind === 'plex_upload'`. Added a guard test that pins computeSrcLetter's `placed` shape so any future divergence trips the test. **CLAUDE.md updated in v1.18.76 to catalogue all 4 JS sites that share this predicate** (computeSrcLetter / renderLibraryRow inline-SRC / bulk-bar selection bucket / isPlexAgentRow) — they must stay aligned.

### Patterns reinforced across the digest

- **Pending state needs first-class display semantics.** Pre-v1.18.63 the pending_updates table held TDB's "current claim" but the info card displayed only the committed value. The pending semantic deserves visible primary surface with explicit "pending" indicator — not hidden in a comparison block. The v1.18.62→v1.18.66 arc landed only because each tag confronted what the previous one missed.

- **Data integrity beats UX polish; surface root cause before treating symptoms.** v1.18.62-65 each treated a symptom of the v1.18.66 root cause. The user-visible thread ("the URL the row plays after ACCEPT is wrong") only surfaced once the user verified post-deploy — five tags of UX treatment papered over the underlying "ACCEPT UPDATE never writes the new URL into themes" data bug.

- **Class-9 mirror-drift between row-level predicates is a recurring trap.** v1.18.65 (TDB pill priority), v1.18.67 (ATTN filter), v1.18.75 (isPlexAgentRow) all involved the same shape: a predicate calculated in N places where one site fell behind the others when the schema/contract widened. Mirror-drift guard tests pin the canonical shape so future divergence forces a coordinated update.

- **CSS fixes can mask the wrong layer of a layout bug.** v1.18.74's 3rd-attempt-the-real-fix shows the cost — three CSS-only attempts (margins, child white-space) never converged because the root cause was template-side text-node injection. When a CSS fix doesn't fully solve a layout issue, audit the source HTML for whitespace/comment artifacts before adding another CSS rule.

- **Atomic operations cross the API/worker boundary via payload.** v1.18.68 + v1.18.71 each established the same pattern in opposite directions: when endpoint state mutation needs to coordinate with async work, pass the "to-do" through the job payload rather than completing it synchronously. The worker is the unit that owns success/failure; let it own the destructive op too.

- **Retry budgets should match the failure shape.** v1.18.70's `_JobPermanentFailure` for size-rejection (Plex consistently rejects the same bytes) vs `RuntimeError` for transient errors (network blip, Plex restart). Whenever the next attempt is provably futile, terminal-fail early.

### Tag count + test suite trajectory

14 tags shipped across May 22 (v1.18.62 → v1.18.75). Test suite: 3245 → 3392 over the arc (+147 tests). Full suite stays under 60s through the entire cycle.


## 22. Digest: v1.18.76 → v1.18.85 (backup-intent + recovery walkers + diagnostic probes)

10 tags shipped across May 22 (v1.18.76 → v1.18.85). The arc opens with a small docs/audit follow-up (v1.18.76), pivots into the user's "backup vs replace" disambiguation request (v1.18.77 → v1.18.80 — 4 tags), surfaces a three-tag phantom fix (v1.18.81), polishes the CSV import UX (v1.18.82), recovers bulk-imported overrides lost to the v1.18.0 cascade (v1.18.83), fixes a latent cross-tab filter leak (v1.18.84), and ships a diagnostic probe for the cloud-themes-backup investigation (v1.18.85).

### Arc 1 — user_overrides.intent + the backup/replace distinction (v1.18.77 → v1.18.80)

the user reported two superficially-similar U-with-dead-TDB rows (1941 + Pokémon anime) showing different banners. Investigation found they're in genuinely different states but motif had no way to encode user *intent* — both rows were `intent='replace'` by default, motif's placement-blocked state read as failure.

v1.18.77 promoted intent to a first-class `user_overrides.intent` column (CHECK constraint `IN ('replace','backup')`), added schema v55→v56 migration with backfill (placement exists → 'replace', else 'backup'), added the KEEP AS BACKUP checkbox on the MANUAL THEME URL dialog (P-rows only), and updated banner copy.

v1.18.78 "fixed" banner ordering when the v1.18.77 conditional didn't match the expected row state — the patch reordered `if (overrideIntent === 'backup')` to fire before `if (data.resolved)`. Looked correct in isolation. **Shipped silently broken** — see Arc 4.

v1.18.79 added intent-aware WARNING breadcrumbs:
- Worker's `_do_place` upgrades INFO→WARNING when intent='replace' but placement blocked by `plex_has_theme`
- plex_enum logs `backup_ready_to_deploy:` when `plex_items.has_theme` transitions 1→0 on a backup-intent row

v1.18.80 wired Apprise notification dispatch on the same `backup_ready_to_deploy` detection — operator gets a push notification instead of grep'ing docker logs. Per the user's q4 design choice: "notify only, don't auto-promote" — the user keeps control of // PROMOTE TO ACTIVE.

### Arc 2 — the v1.18.81 phantom fix root cause

the user verified v1.18.80 post-deploy on the 1941 row + reported // PROMOTE TO ACTIVE still missing despite v1.18.78's "fix." Direct DB query confirmed `intent='backup'` was correctly persisted. Direct API curl on `/api/items/movie/11519/recovery-options?section_id=1` returned `override: null`.

**Root cause:** `api_recovery_options` (the endpoint feeding the banner + button conditionals) had NEVER returned `override` in its response shape. The v1.18.77 frontend read `data.override.intent`; for three tags it was reading `undefined.intent` (silently coerced to null). The BACKUP banner + PROMOTE TO ACTIVE button were silently dead since v1.18.77 shipped.

**Why v1.18.78's "fix" didn't fix anything:** the test asserted the JS conditional's source-text shape (`assert "overrideIntent === 'backup'" in src`) without ever exercising the actual API → frontend data flow. Source-text guards are phantom guards on data-flow contracts. Three tags shipped with the conditional in place but the data feeding it always undefined.

v1.18.81 added the `override` SELECT + return to `api_recovery_options` (section-scoped, mirrors `api_item`'s lookup), surfaced `intent` in the SET URL audit details (closing the provenance gap that made the phantom fix hard to diagnose post-fact), and added behavioral tests using real TestClient + DB inserts — NOT source-text guards. Also fixed an unrelated button-flash bug where the long success status text squeezed dialog buttons into wrapping ("// SAVE & DOWNLOAD" rendered on two lines for 700ms before auto-close).

Class-9 contract-drift sub-pattern, now catalogued in CLAUDE.md class 9 as the "phantom-fix" sub-pattern.

### Arc 3 — CSV import dropdown clarity (v1.18.82)

the user on the bulk-URL import preview: "keep vs replace vs skip — confused" + "the button that contains the down plex backup option is much bigger and looks out of place compared to the others and the text is cut off."

Three coordinated issues:
1. CONFLICT dropdown carried both "Keep" and "Skip" — api.py:11310 treats them identically (`if action in ("keep", "skip"): skipped += 1`). Two no-op options reading as different. Removed Skip from CONFLICT.
2. "Download backup (Plex stays)" (28 chars) made the select auto-size much wider than sibling "Keep" rows. Renamed to "Backup only" (11 chars) + added `#import-preview-table select[data-import-action] { min-width: 140px }` for consistent column alignment.
3. No tooltips. Added `title=` on every option (CLEAN + CONFLICT) so hover surfaces the action semantics in-place.

### Arc 4 — recovery walker for bulk-imported user_overrides (v1.18.83)

the user noticed adopted-looking rows on /anime that were actually bulk-imported via CSV. Investigation found their `user_overrides` were empty + their `local_files` carried the v1.18.5 walker signature (`source_kind='adopt'` + `source_video_id='recovered'`) → rendering as A instead of U.

**Root cause:** v1.18.10's `maybe_recover_lost_user_overrides` scans the `events` table for `"Manual URL set by admin:%"` messages. The per-row SET URL endpoint emits exactly that string via `log_event`, so v1.18.10 recovers those. But the **bulk CSV-import endpoint** wrote ONLY to `audit_events` with `{"source":"import"}` — never to `events`. When v1.18.0's cascade + v1.12.60's orphan sweep wiped `user_overrides`, the bulk-imported URLs were unrecoverable by v1.18.10.

This was a **class-9 contract-drift sub-pattern**: same conceptual surface (`user_overrides`), two write paths (per-row + bulk), recovery walker only knew about one. Catalogued in CLAUDE.md as the "one-conceptual-surface-multiple-writers" sub-pattern.

v1.18.83 added:
- `maybe_recover_lost_bulk_imports` walker in `recovery_v55.py` — scans `audit_events WHERE action='set_url' AND details LIKE '%"source":"import"%'`, INSERTs `user_overrides` at section_id='', reclassifies `local_files.source_kind='adopt' → 'url'` (narrow filter: + `source_video_id='recovered'` to avoid touching legitimate adopts), backfills `source_video_id` from URL. Independent marker `recovery_bulk_imports_done_at`.
- Forward-fix: `api_import_apply` now ALSO emits the canonical `log_event` so future bulk imports are recoverable by EITHER walker. Belt-and-suspenders.
- Behavioral tests with real DB fixtures verifying recovery + idempotency + skip-when-already-overridden.

### Arc 5 — cross-tab filter leak (v1.18.84)

the user on /collections post-v1.18.83: "continue after refresh to find myself back at this filter, or opening a new tab it's back at this filter."

**Root cause:** `_LIB_STATE_KEY` ('motif:library_filter_state') was stored in localStorage — origin-wide. Tab A's filter snapshot leaked into Tab B's hydrate path. v1.18.50's `_maybeClearStorageOnReload` targeted `type='reload'` specifically; new-tab navigation is `type='navigate'`, so the clear was a no-op.

Latent bug since v1.13.13 (3 years). Surfaced now because STATUS=! on /collections returns 0 matches (no collection rows have `failure_kind`), making the empty result unambiguously a filter problem. On /movies, /tv, /anime — the leak was invisible because there were always matches.

**Fix:** storage backend switched localStorage → sessionStorage. Same key, same payload, same three call sites. sessionStorage is per-tab:
- New tab → empty → fresh view
- Within-tab nav (MOVIES → TV → COLLECTIONS) → preserved (v1.13.13 intent)
- Refresh → preserved by sessionStorage BUT v1.18.50's reload-clear still calls `_clearLibraryFilterStorage` (now via sessionStorage.removeItem)
- Settings → back to library → preserved (sessionStorage survives within-tab nav even to non-library routes)
- Close tab → reopen → no filter (sessionStorage dies with tab)

Same scope discipline v1.17.15 applied to the search query `q`. One-shot eviction: hydrate calls `localStorage.removeItem` opportunistically so users upgrading from pre-v1.18.84 get the stale entry cleaned up on next page load. Catalogued in CLAUDE.md as the "storage-scope-mismatch" sub-pattern (instances: v1.15.52 + v1.18.84).

### Arc 6 — cloud-themes-backup investigation: v1.18.85 diagnostic probe

the user: can motif extract the theme Plex is serving on a P row and save it locally as a backup? Discussion narrowed scope to the only case where motif has no other recovery path: **Plex Pass cloud themes**. themerr-plex embeds have the TDB URL; user uploads have the user's source; sidecars are on disk. Cloud themes live on Plex's infrastructure + disappear with Plex Pass.

Three feasibility scenarios in roughly decreasing probability:
- (a) Cloud themes appear as entries in `/themes` with downloadable bytes (clean path forward — same as v1.18.35 re-upload trick)
- (b) Cloud entries return a 302 to a CDN (extra hop, still tractable)
- (c) Cloud themes don't appear in `/themes` at all — resolved live at playback time (much harder, would need fake-Plex-client to capture stream)

Same v1.18.31-40 cadence: probe first, characterize, then design.

v1.18.85 shipped the probe surface:
- `PlexClient.probe_theme_entry_bytes` — Range-GET 4KB of `/library/metadata/{rk}/file?url=<entry_uri>`, captures status + all headers + content_type + body_preview_hex
- `POST /api/admin/probe-plex-themes` — admin-gated, accepts `{"titles": [...]}` capped at 8, resolves to rating_keys via local plex_items LIKE (exact-match-first ORDER BY, no Plex search round-trip), walks /themes entries + probes bytes per entry, returns structured JSON
- Settings UI block under the v1.18.47 DIAGNOSTICS panel — textarea + Run button + `.codeblock` pre for JSON output
- 18 new tests including 5 behavioral

Production cloud-backup feature is a future tag gated on the probe findings.

### Patterns reinforced across the digest

- **Source-text guards are phantom guards on data-flow contracts (v1.18.81).** A test that asserts a JS conditional appears in source without exercising the data feeding it can pass while the feature silently doesn't work. v1.18.77/.78/.79/.80 all "shipped" the BACKUP banner + PROMOTE TO ACTIVE button — none of them worked on real installs until v1.18.81 fixed the API response shape. Rule: when adding a feature with backend → frontend data flow, the test MUST exercise the endpoint with real fixtures.

- **One conceptual surface, multiple writers (v1.18.83).** When state (user_overrides, local_files, placements) has multiple write paths, recovery must catalogue all of them. v1.18.10 covered per-row SET URL but missed bulk import — three years before catastrophic recovery was needed, the gap was invisible. Rule: when adding a new write path, audit recovery walkers.

- **Storage scope ≠ persistence duration (v1.18.84).** localStorage and sessionStorage both persist across page loads; the difference is SCOPE. localStorage is origin-wide (cross-tab); sessionStorage is tab-scoped. When the goal is "preserve within this session of work," scope matters as much as duration. v1.17.15 already knew this for the search query; v1.18.84 catches the filter snapshot up.

- **Probe-before-production cadence (v1.18.85).** Same v1.18.31-40 lesson re-applied: when motif touches an API surface where behavior is empirically uncertain (cloud themes returning bytes vs redirect vs nothing), ship the probe first, characterize, then design the production feature.

- **Behavioral tests beat mock-shape tests for data-flow contracts.** v1.18.81's behavioral fixture (real TestClient + DB inserts + assert on API response shape) was the discriminator that exposed the v1.18.77/.78/.79/.80 phantom-fix chain. v1.18.83's behavioral fixture verified end-to-end recovery walker behavior. v1.18.85's behavioral fixture covered 400/503 paths. Pattern: where the test must verify production data flow, use real fixtures even at the cost of more setup.

- **Latent bug surfaces with the right empty case (v1.18.84).** The cross-tab filter leak existed since v1.13.13 — three years of opportunity. It surfaced now because STATUS=! on /collections is one of very few combinations that produces an unambiguous empty result. On sibling tabs with denser data, the leak was always there but always invisible. Rule: audit assumptions when adding a new section (e.g. /collections in v1.18.0) — its empty cases may differ from siblings, exposing latent bugs that the siblings hid.

- **Provenance gaps compound diagnostic difficulty (v1.18.81/.83).** When the audit log doesn't record what was requested (the intent on a SET URL, the source on a bulk import), post-fact debugging becomes impossible. v1.18.81 added old_intent/new_intent to SET URL audits; v1.18.83 added the canonical log_event to bulk imports. Rule: when a column promotes to first-class, every endpoint writing it should record the transition in audit_events.

### Tag count + test suite trajectory

10 tags shipped across May 22 (v1.18.76 → v1.18.85). Test suite: 3392 → 3512 over the arc (+120 tests). Full suite stays at ~52s through the entire cycle. Notable: v1.18.81's behavioral-test discipline produced the catch that v1.18.77/.78's source-text tests missed for three tags.


## 23. Digest: v1.18.86 → v1.19.20 (line rollover + v1.19.x audit walkers + LOGS UI)

The v1.18.x line closed at v1.18.99 (15 more tags after § 22's v1.18.85 stop), then v1.19.0 cut on a clean 3-agent code audit — ZERO real bugs found because the v1.18.97–.99 defensive-surface catalogue had been audited the night before. v1.19.x opened with a recovery-walker burn that took 9 tags (v1.19.10 → v1.19.18) chasing the long tail of state-drift damage from the v1.18.0 FK cascade. Closing the arc, v1.19.19/.20 LOGS-page UI tweaks teach the "land + verify same session, before the screenshot leaves the user's screen" lesson the hard way.

### Arc 1 — v1.18.x line close + v1.19.0 rollover (v1.18.86 → v1.19.0)

v1.18.86-.99 ran the design-audit follow-up cycle: README documentation, NOTIFICATIONS settings page polish, the v1.18.90 reaper notification path (`plex_theme_lost` event — fires when Plex stops serving a theme on a row motif has no fallback for), the v1.18.93 row-refresh contract widening (per-row download/place/scan/refresh markers added to the `anyMutatingOpActive` signal so background-worker-initiated jobs trigger libraryRapidPoll), the v1.18.94 plex_rejected lockout (rows where Plex's upload endpoint returns 4xx three times in a row get marked permanently rejected; the BK pipe routes around them), the v1.18.95 op_progress jobs-panel synthesis (BULK PROBE TDB had been running for 16+ minutes invisible to the JOBS panel because it writes only to op_progress not jobs), the v1.18.96 dispatch fail-fast (notify_content body formatters that returned `None` were silently dispatching empty Apprise messages), and the v1.18.97/.98/.99 silent-fail audit (closed every remaining class-9 catch site identified by the post-audit walk).

v1.19.0 rolled the line under the same 3-agent audit cadence as v1.15.0. Zero real bugs surfaced. The clean rollover was earned by the v1.18.97-.99 silent-fail closure — every recent defensive surface carried both source pins and behavioral tests.

### Arc 2 — recovery-walker burn (v1.19.10 → v1.19.18)

the user's Bleach P-row repro (B-tier diagnostic from May 23) surfaced eight distinct state-drift findings, each rooted in v1.18.0's FK-cascade damage that the v1.18.5–.11 recovery walkers had only partially cleaned. The arc ran 9 tags chasing the long tail:

- **v1.19.10**: `julianday()` SQLite portability fix in plex_enum (DISTINCT-aggregate edge case)
- **v1.19.11**: LPS / accept-decline insurance tests (regression guards on the v1.18.x decisions)
- **v1.19.12**: orphan urls_match cleanup trigger (schema v56→v57) — pending_updates rows for rows whose user_overrides had been deleted by REPLACE TDB / ACCEPT UPDATE silently kept the !UPD banner alive. 12 known orphan paths → trigger handles cleanup at source-of-truth schema level instead of per-call-site DELETE
- **v1.19.13**: `maybe_recover_lost_adopts` recovery walker — finds rows incorrectly classified as A (adopt) when they should be U (user). v1.18.5's recovery picked the wrong default for adoption-prone rows
- **v1.19.14**: walker refinement to honor post-adopt T-intent (v1.19.13's first pass overcorrected)
- **v1.19.15**: walker provenance write + backfill of 207 affected rows
- **v1.19.16/.17**: stale-placement repair walkers (R + Z classes — placements where Plex's has_theme=0 disagrees with motif's placement; placements with kind=hardlink but file missing on disk; duplicate placements when v1.18.23's kind-dispatch routed force-place jobs through `_do_place_collection` and left the old hardlink rows orphaned)
- **v1.19.18**: file_sha256 backfill walker (578 NULL rows from pre-v1.18.5 writers; needed for the file_sha256 contract v1.19.x writers all stamp)

Every walker uses the canonical pattern: idempotent marker in `runtime_settings`, INFO log per detected case, single-pass on boot, leaves audit_events for provenance. The arc settled motif's state-drift surface; subsequent tags can write into local_files / placements / user_overrides without re-discovering the damage.

### Arc 3 — LOGS page UI (v1.19.19 / v1.19.20)

v1.19.19 attempted three small fixes to the /logs page (header alignment, op-row readability, background parity). Two of them landed broken. v1.19.20 reverted the broken pieces + actually fixed the three issues, teaching the "before the screenshot leaves the user's screen" lesson — when shipping a UI tweak, verify on the deployed image (not just unit tests) within the same session, because a 24-hour screenshot turnaround means the user's the discriminator and the test guards can't catch what they can't see.

### What we learned this arc

- **Recovery walkers compound debt.** Every recovery walker handles damage from a specific bug, but the absence of a walker for a given damage class is itself a hidden debt. v1.19.13-18's 6-walker burn was paying down debt accumulated from v1.18.0's cascade — each walker covers one shape, none cover the full damage matrix until all are written.

- **3-agent audit cadence as line-close ritual.** Both v1.15.0 and v1.19.0 used the same 3-agent audit pattern (silent failures + recent regressions + test-coverage gaps). Both found zero real bugs at the rollover line. The pattern works because it sweeps surfaces the regular dev pipeline already passes over.

- **Schema triggers > per-call-site DELETE for invariants.** v1.19.12 added a CASCADE-on-delete trigger that catches all 12 known paths AND any future paths. The alternative (DELETE-orphans at each call site) is brittle: a new code path could forget. Pattern: when invariant has N writers, schema-level trigger beats N DELETEs.

- **UI tweaks need same-session verification.** v1.19.19 → v1.19.20 surfaced this. CSS/layout changes that pass unit tests can fail visually; the user is the discriminator, and a 24-hour turnaround means broken UI ships. Land + verify before context-switching.

### Tag count + test suite trajectory

35 tags shipped (v1.18.86 → v1.19.20). Test suite: 3512 → ~3900 (+ ~400 tests). v1.18.95's op_progress jobs-panel synthesis alone needed 20+ behavioral tests; v1.19.13-18's recovery walkers added ~150 tests in aggregate.


## 24. Digest: v1.19.21 → v1.19.40 (BK pipe + P-row preservation + audit arc)

The v1.19.x audit-arc cycle: 20 tags addressing the audit findings surfaced after the Bleach repro stabilized in v1.19.18. Three coherent threads: the v1.19.21 BK-badge end-to-end pipe (foundation for the cloud-themes-backup feature in § 25), the v1.19.32-.37 P-row preservation work (six tags ensuring ACCEPT UPDATE / REVERT on P-rows doesn't blow away Plex's serving state), and the v1.19.38-.40 cross-reference / mirror-drift audits.

### Arc 1 — v1.19.21 BK badge end-to-end pipe

the user's Indiana Jones repro: `local_files` row with file present (7.8 MB), `placements` empty, `plex_items.has_theme=1` + `verified_ok=0`, no sidecar, SRC pill rendered as `–`. Motif's file unused, Plex's serving broken, no UI signal anything was amiss. The hourly retry sweep kept skipping the row because `last_place_attempt_reason='plex_has_theme'` was stale (the v1.18.x SoftSkip cache).

v1.19.21 fixed the cache (worker re-attempts when `verified_ok=0` regardless of cached reason) + added the foundation pieces that v1.19.42's cloud-backup walker would later reuse:

- `last_place_attempt_reason='backup_only'` stamp — load-bearing marker that gates retry-sweep skip + UI classification
- BK badge in the LINK column (blue, distinct from BK as a SOURCE letter) for rows with the backup_only stamp
- One-shot recovery walker `maybe_repair_stale_plex_cache_placements` to repair existing damage on the user's instance

This pipe became the v1.19.32-.37 P-row preservation foundation AND the v1.19.42 cloud-themes-backup foundation. The single stamp + badge + recovery-card synthesis is reused end-to-end across both features.

### Arc 2 — P-row preservation (v1.19.32-.37)

Six coordinated tags ensuring user actions on SRC=P rows (Plex serves its own theme) don't accidentally overwrite Plex's serving state:

- **v1.19.32**: ACCEPT UPDATE on a P-row keeps Plex serving (no flip to T). Pre-fix the accept handler force-placed motif's TDB version, replacing Plex's theme.
- **v1.19.33**: New `_is_p_row_for_section` helper at 3 callsites (REVERT, bulk ACCEPT ALL, ACCEPT UPDATE) + a whitelist of place-job kinds the worker re-targets to backup-only writes on P-rows
- **v1.19.34**: Tooltip + log message accuracy for the P-row backup paths
- **v1.19.35**: PROMOTE TO ACTIVE on BK rows + fix v1.18.77 phantom button bug (BK rows could surface the recovery banner but the PROMOTE button was wired against a stale data path)
- **v1.19.36**: MEDIUM cleanup (dead CSS, empty rule, chevron symmetry, walker log line, design doc)
- **v1.19.37**: UPLOAD MP3 BK badge fix + REVERT confirm dialog P-row branch (existing dialog said "this replaces Plex's theme" — wrong for P-rows now that the path is backup-only)

End state: every user action on a P-row that USED to overwrite Plex now writes a backup instead, preserving Plex's served theme while staging motif's recovery option. The BK badge surfaces the new state; PROMOTE TO ACTIVE deploys when the user wants to.

### Arc 3 — cross-reference audits (v1.19.38-.40)

Three audit-driven tags closing gaps the v1.19.32-.37 arc surfaced:

- **v1.19.38**: SRC-axis sixth-site drift — the v1.18.0 placement-kind alignment catalogue listed 5 JS sites; the bulk PUSH predicates were a 6th hidden site that misclassified plex_upload rows (their `media_folder=''` evaluated `!media_folder` as true, so bulk PUSH count over-reported on selections containing any plex_upload row).
- **v1.19.39**: MEDIUM coherence — bulk audit/HISTORY messaging, dialog copy on the confirm dialogs, PROMOTE tooltip accuracy, settings page label fix
- **v1.19.40**: LOW cleanup pass — text fixes, doc comments, dead field cleanup, CLAUDE.md catalogues + DESIGN_SYSTEM.md sibling-mirror pointer

### What we learned this arc

- **Single-stamp end-to-end pipes are reusable.** v1.19.21's `last_place_attempt_reason='backup_only'` started as one row's backup pipe; v1.19.32-.37 reused it for P-row preservation; v1.19.42 reused it again for cloud-themes-backup. The stamp gates retry-sweep skip + BK badge + PROMOTE TO ACTIVE recovery branch end-to-end. When designing a new flow, audit whether an existing stamp/marker can be reused before introducing a new one.

- **The 5-site placement-kind catalogue was actually 6.** v1.19.38's audit walked every `awaitingApproval` / `pushableCount` / `pushCount` predicate site and found the bulk PUSH handler. CLAUDE.md was updated with the 6-site list + a mirror-drift guard test (`test_v1_19_38_src_axis_sixth_site_drift.py`) that walks the JS at test time and fails loud on new predicates that don't reference both `media_folder` + `placement_kind === 'plex_upload'`.

- **Tooltip accuracy is correctness-affecting.** v1.19.34/.37/.39 all addressed UI copy that misled the user about what an action would do. Wrong tooltip + correct action = user makes the wrong choice + lays the blame on motif. Rule: when adding a new branch to an existing action, audit every tooltip the user might see on the way in.

### Tag count + test suite trajectory

20 tags (v1.19.21 → v1.19.40). Test suite: ~3900 → ~4070 (+ ~170 tests). The P-row preservation arc (v1.19.32-.37) added the most behavioral coverage; the mirror-drift audit tag (v1.19.38) added the audit-guard pattern.


## 25. v1.19.41 → v1.19.55 (cloud-themes-backup feature + notification polish)

The v1.19.41-.55 arc shipped the cloud-themes-backup feature in three coherent tags (v1.19.41 detection-pipe + sidecar awareness, v1.19.42 schema + walker + writer + endpoints, v1.19.43 UI surface) followed by a ten-tag stabilization + polish cycle (v1.19.44-.55) that addressed bugs the user surfaced in production use + closed every audit finding from a 3-agent post-ship review.

### Arc 1 — cloud-themes-backup three-tag arc (v1.19.41 → v1.19.43)

Plex Pass cloud themes are served only while Plex Pass is active. Losing Plex Pass (or catalog rotation, or item re-add with a new rating_key — the v1.18.90 reaper path) kills every P-row depending on a `metadata://themes/<sha>` entry, instantly, silently, with no recovery if motif never staged a backup.

The arc let motif stage Plex Pass cloud themes as local backups BEFORE loss. Distinct from the v1.18.77 user-URL backup flow — this targets Plex's INTERNAL `metadata://themes/<sha>` entries that exist only because Plex Pass is paid. Pre-flight characterization on May 26 (n=16 stratified probe against the user's prod DB) measured 50% C1 rows overall, 100% C1 on anime; ~1,940 C1 rows expected across 3,883 candidates with ~4.2 GB storage estimate.

- **v1.19.41 — detection-pipe + sidecar awareness.** Four-way notification tier split replacing v1.18.90's single `has_fallback` boolean: backup-ready (intent='backup' OR source_kind='plex_cloud') → PROMOTE TO ACTIVE; sidecar-available (local_theme_file=1 OR theme.mp3 on disk) → ADOPT; other fallback → silent skip; no fallback → existing message. New body formatters for the two new tiers; v1.18.79 HAMA-gap fix; transition INFO breadcrumb; silent-fail downgrade flags; `POST /api/admin/test-trigger-theme-lost` endpoint for synthesizing any tier against any real row; probe results copy-to-clipboard button.

- **v1.19.42 — schema + walker + endpoints + writer.** Schema v57→v58 widens `local_files.source_kind` CHECK to accept `'plex_cloud'`. New module `app/core/cloud_theme_backup.py` with `identify_c1_rows` (walks P-rows, classifies each via Plex's /themes, rate-limited 200ms inter-call, resumable cursor in runtime_settings, cancellable) + `backup_cloud_theme` (downloads bytes via v1.18.36 re-upload path, stages to themes_dir, writes full v1.19.x local_files contract). Two admin endpoints: dry-run (read-only, returns C1 set) + run (downloads bytes; accepts `{rks:[...]}` or `{only_anime:true}` scoping; per-row audit + log_event). Defensive comments on the 2 recovery_v55 walkers that could theoretically widen to plex_cloud rows in the future.

- **v1.19.43 — UI surface.** New B badge in the LINK column (`source_kind='plex_cloud'` rows, lemon→amber-bright after v1.19.48 polish). New `// B` filter chip + SQL filter via `link_pills=b`. New `// DOWNLOAD PLEX BACKUP` bulk-bar button (plex/amber tone). New `// DOWNLOAD PLEX BACKUP` per-row SOURCE-menu entry. v1.19.39 PROMOTE TO ACTIVE tooltip extended with a third variant describing the v1.18.36 re-upload round-trip for plex_cloud rows. `api_recovery_options` extended with `source_kind` passthrough so the JS PROMOTE tooltip can branch on it.

### Arc 2 — v1.19.44 audit fixes (BLOCKING + S1 silent-fails)

Three parallel audit agents (mirror-drift / behavioral trace / class-9 silent-fails) reviewed the v1.19.41-43 arc immediately after ship. The audit found one BLOCKING bug + four S1 silent-fails:

**BLOCKING**: PROMOTE TO ACTIVE on a plex_cloud row did NOT do the v1.18.36 re-upload trick the tooltip promised. The BK-no-override branch enqueued a vanilla place job which routed via `default_placement_method` → typically `'file'` → hardlink sidecar in the Plex folder. `set_active_theme_via_reupload` was never reached. Fix: widen `bk_local` SELECT to fetch `source_kind`; new plex_cloud branch reads bytes from disk, calls `plex.upload_collection_theme` synchronously, stamps a `plex_upload` placement, clears the backup_only marker → row transitions B → PU.

**S1 silent-fails:**
- F3: bare `except: body = {}` on admin endpoints — malformed JSON silently ran full ~4.2 GB catalog walk
- F8/F9: body formatters read `ctx['display_title']` unguarded → KeyError → `_THEME_LOST_NOTIFY_WARNED` flag goes hot → every future notification silently drops
- F11: outer dispatch `log.debug` swallow with no warn-flag

All four fixed in v1.19.44.

### Arc 3 — production bug repair (v1.19.45 → v1.19.50)

Six tags addressing bugs the user surfaced in production use:

- **v1.19.45**: Async endpoint conversion. v1.19.42 endpoints ran inline (~13 min for full catalog walk, blocked the FastAPI event loop, browser timed out with "Failed to fetch" while UI locked). Switched to acquire-then-spawn-thread pattern; endpoint returns immediately with op_id; surface via ops drawer.
- **v1.19.46**: FK constraint fix. v1.19.42 writer assumed a `themes` row existed for `(media_type, tmdb_id)` — but for the cohort cloud-backup exists to serve (rows TDB doesn't track), no themes row exists. INSERT failed with `IntegrityError: FOREIGN KEY constraint failed`. Fix: pre-create themes row with `upstream_source='plex_orphan'` if missing + stamp plex_items.theme_id (mirrors the upload-theme orphan precedent).
- **v1.19.47**: /logs JOBS panel pinning. Pre-fix stale terminal `op:` rows pinned above recent real jobs. Partitioned synth rows into active (still pin) vs terminal (merge with real jobs by recency).
- **v1.19.48**: B badge color + button naming polish. Switched lemon→amber-bright (lemon conflicted with cookies-needed UX). Renamed `// BACKUP THIS THEME` → `// DOWNLOAD PLEX BACKUP` (mirrors DOWNLOAD TDB BACKUP convention; eliminates demonstrative pronoun). Switched tone themerrdb→plex (action backs up Plex's content; tone should signal that).
- **v1.19.49**: plex_cloud backups don't block TDB switch options. After DOWNLOAD PLEX BACKUP lands, the row had `downloaded=true` which suppressed DOWNLOAD TDB BACKUP + REPLACE TDB. plex_cloud backups are insurance, not commitment — should still allow switching to TDB. New `isPlexCloudBackupRow` / `hasNonCloudCanonical` predicates.
- **v1.19.50**: BK filter chip (v1.19.21 surface gap — BK badge was un-filterable for 29 tags) + 0-eligible-target alert (cloud-backup walker silently completes when row is non-C1; new `motifOps.waitForOp` helper + click handler alert surfaces "not classified as C1" explanation).

v1.19.51 — selected-aware cloud-backup classifier. the user's '90 Day Fiancé: Happily Ever After?' probe showed three theme entries sharing the same SHA-1 (metadata://selected + two upload://siblings from defunct themerr-plex). The v1.19.42 classifier rejected the row because of upload siblings on the assumption "motif/themerr-plex upload already covers the bytes" — but that's wrong when motif has no local_files row (which the SQL push-down already enforces). Rewrote classifier to honor Plex's `selected: true` flag: if selected entry is metadata://, it's a backup target regardless of siblings.

### Arc 4 — bulk-action design audit (v1.19.52 → v1.19.54)

Three parallel audit agents reviewed bulk-action design consistency post-v1.19.51 ship. the user's ask: "make sure all bulk actions designs are follow the same same choices to show selected etc that keeps a uniform look."

- **v1.19.52** — bulk DOWNLOAD PLEX BACKUP parity + withCount labels. Bulk PLEX BACKUP gained `setOptimisticPlaceholder` + `waitForOp` + (N skipped) toast + 409 alert + refresh timers. Both BACKUP buttons switched from hand-rolled `// DOWNLOAD N TDB BACKUPS` to canonical `withCount('// DOWNLOAD TDB BACKUP', n)` → `// DOWNLOAD TDB BACKUP (N)` — uniform with every other bulk button.

- **v1.19.53** — wider bulk-handler hygiene. Added `setOptimisticPlaceholder` to 7 more async-bulk handlers (DOWNLOAD FROM TDB, PUSH TO PLEX, REVERT MISMATCH, RESTORE FROM PLEX, ADOPT SELECTED, ADOPT + LPS — every per-row-iterator handler that enqueues async jobs). 5 handlers intentionally skipped (LET PLEX SERVE / BULK PROBE TDB use real op_progress kinds; ACK / ACCEPT / DECLINE are immediate DB writes). New `test_all_async_bulk_handlers_have_placeholder` audit guard + `test_no_placeholder_uses_unknown_kind` catches typos.

- **v1.19.54** — PROMOTE TO ACTIVE SHA-drift defense (S2 correctness audit finding). Between motif's backup time and the user's PROMOTE click, Plex's cloud catalog can update the row's selected metadata entry to a different SHA-1. Pre-fix motif silently deploys stale bytes. Fix: before re-upload, probe Plex's current /themes, compare selected SHA against stored backup SHA. On drift + no `force_stale` flag, return 409 with structured detail. `api()` helper extended to attach `err.status` + parsed `err.detail` to thrown errors. Client confirm dialog shows both SHA fingerprints + recovery path (DOWNLOAD PLEX BACKUP) before retrying with `force_stale: true`.

### Arc 5 — notification polish (v1.19.55)

the user's review of the 9 AM sync notification surfaced five distinct asks bundled into one tag:

1. Drop duplicate body header from themes_added_by_sync (body started with `**N new theme(s) added by sync**` duplicating the title)
2. New `themes_updated_by_sync` event_kind with 🔄 icon — parallel to themes_added_by_sync but for the updated_titles list
3. Restructure sync_completed body — Updates list first (🔄 Updated:), then summary, then ✅ Sync complete at the END (moved from title). Rephrased misleading "Checked N upstream items" → "N item(s) had upstream changes this sync window"
4. Uniform YouTube thumbnail size — switched `hqdefault.jpg` (480x360) → `mqdefault.jpg` (320x180); always-available + compact + consistent across notification sinks
5. New `theme_pushed` event_kind for force-place dispatches (PUSH TO PLEX / REPLACE TDB / PROMOTE TO ACTIVE) — pre-fix all triggered `theme_added`, conflating "new theme appeared" with "Plex's serving state changed." New 📤 emoji + reason-aware title variants (`Theme replaced via TDB`, `Theme promoted from backup`, `Cloud backup re-uploaded`, generic `Theme pushed to Plex`). Discriminator: payload has `force` / `force_place` / `reason` → theme_pushed; empty payload (sync-driven) → theme_added.

### What we learned this arc

- **Probe-before-production cadence pays off twice.** v1.18.85/.87 probes characterized cloud-themes-backup before the feature shipped (probe-driven design). v1.19.51's classifier rewrite was driven by a SECOND probe (the user's '90 Day Fiancé' shape) — the production deployment surfaced a corner the pre-flight didn't cover. The cadence: probe pre-ship + probe post-ship-on-anomaly.

- **Re-use BEFORE invent for state plumbing.** Cloud-themes-backup reused the v1.19.21 BK pipe (backup_only stamp + BK badge + PROMOTE flow) end-to-end. The B badge is a NEW visible surface but the plumbing underneath is shared. Result: feature shipped in 3 tags instead of 8.

- **Async endpoint + event-loop discipline.** v1.19.45 was the BIG fix. FastAPI's `async def` containing synchronous work (httpx sync calls, time.sleep, conn.execute) blocks the entire event loop for the duration. The cure: acquire-then-spawn-thread pattern, return op_id immediately, surface progress via op_progress + ops drawer. Now ANY long-running admin endpoint motif adds will need this pattern by default.

- **FK invariants when adopting a new value space.** v1.19.46's FK fix taught: when adding a new `source_kind` value (`'plex_cloud'`), audit ALL writes to that table to confirm the FK targets exist. The cloud-backup writer assumed `themes` rows existed because the TDB-sync writer always created them first. For the cohort cloud-backup exists to serve (TDB-untracked rows), that's exactly wrong.

- **Audit guards calibrate slow.** v1.19.50's `test_no_placeholder_uses_unknown_kind` catches typos like `'downloads_queue'` that would silently fail to surface. Similar audit guards from v1.18.95 (op_progress kinds), v1.18.53 (ops drawer KIND_LABEL/TONE_BY_KIND), v1.14.58 (pill-axis cross-reference) etc. Every "everything must match" surface deserves an audit guard test.

- **The bulk-action UX is a contract, not a vibe.** v1.19.52-.53 unified setOptimisticPlaceholder + waitForOp + count surfacing + 409 messaging across 9 bulk handlers. The "uniform look" the user asked for is a contract: every async-bulk handler must do these 4 things. Audit guard now enforces it.

### Tag count + test suite trajectory

15 tags (v1.19.41 → v1.19.55). Test suite: ~4070 → 4232 (+ 162 tests). New `cloud_theme_backup.py` module (~440 LOC); 2 new admin endpoints; 3 new schema-aware test files (v1.19.42 backend, v1.19.43 UI, v1.19.46 FK fix). The 5-tag arc 1 (v1.19.41-.43) shipped the feature; the 10-tag arc 2-5 stabilized it. the user's production-use feedback drove every post-ship fix.

## 26. Condensed: v1.19.56 → v1.20.67 — v1.19.x close + recovery_v55 sunset

### Arc — v1.19.x close → v1.20.0 rollover
The v1.19.x line closed at .99 with the standard 3-agent audit (silent-fails /
recent-regressions / coverage). v1.20.0 rolled over with ONE real bug fixed
pre-cut: a v1.19.98 SRC-axis mirror-drift miss — `selectedEligibleUpdates`
(app.js) still used the bare `SRC!='-'` predicate without the
`new_theme_available` exception, hiding the bulk bar for selected SRC=—
new-theme rows. Fixed at altitude: extracted `pendingUpdateActionable()` so the
predicate can't drift across its 5 bulk-bar/accept sites again.

### Arc — recovery_v55 sunset (v1.20.x → v1.21.0, cut at v1.20.67)
By v1.20 the 18 one-shot boot recovery walkers (the v1.18.0 FK-cascade data-loss
recovery + the override / adopt / placement / sha256 backfills) had served their
purpose: their `runtime_settings` markers were set on every install, so each
boot logged ~16 INFO "marker already set — skipping" lines — pure noise. v1.21.0
(cut EARLY at v1.20.67, not the usual .99) DISCONNECTED them from the startup
path (`main.py`) while RETAINING `recovery_v55.py` + its tests as an archived,
callable safety net — re-wire a specific walker if a historical bug class ever
recurs; full file deletion deferred to a future `.99` line-close. The v1.20.x
tree was heavily audited the same session across the .59–.67 arc, so no fresh
3-agent audit at the cut.

### What we learned this arc
- **One-shot recovery code earns retirement, not deletion.** Once a
  marker-gated walker has run everywhere, disconnect it from boot (kill the
  noise) but keep it callable — the bug class it recovers from could recur.
- **Fix the shape, not the site.** The v1.19.98 mirror-drift (one predicate
  copied to 5 sites) was fixed by extracting one helper — same lesson as the
  SRC-axis sixth-site drift (v1.19.38) and the v1.22.10 actionable-gate consolidation.

### Tag count + test trajectory
~v1.19.56 → v1.20.67 + the v1.21.0 rollover. The suite kept climbing from
~4,232 (v1.19.55).

## 27. Condensed: v1.21.0 → v1.21.99 — the per-edition theme-isolation arc

The defining arc of the v1.21.x line. motif learned that a single TMDB title can
have MULTIPLE Plex EDITIONS (LotR Theatrical/Extended/Sam-Takes-a-Step; Watchmen
Theatrical/Midnight/Director's Cut; Godzilla Minus One/Minus Color; Yu-Gi-Oh
Odex Dub/Uncut) that share one `tmdb_id` but need INDEPENDENT theme state.
Pre-arc, all state keyed on `(media_type, tmdb_id, section_id)`, so an action on
one edition bled to its siblings.

### Arc — the edition_key chokepoint (v1.21.52 → v1.21.53)
Schema v63 (v1.21.52) widened every theme-state table's PK with `edition_key` —
the NORMALIZED Plex edition tag from the media folder (`{edition-Theatrical}` →
`'theatrical'`; untagged → `''` standard). It's FOLDER-derived on purpose:
stable across Plex's remove+re-add churn (the v1.18.90 reaper mints fresh
rating_keys, which would orphan rk-keyed state) and it shares `placement.py`'s
matching vocabulary. v1.21.53 landed `editions.py` as the single CHOKEPOINT —
every read/write of `edition_key` routes through `edition_key_for_folder` /
`edition_key_for_rating_key`. No call site may default `edition_key=''` inline;
that's the silent-wrong-classification class (writing an Extended URL onto
Theatrical).

### Arc — scope EVERY action by edition (v1.21.54 → v1.21.92)
A long, careful sweep made every per-row + bulk action edition-aware by
resolving the clicked `rating_key` → `edition_key` (the churn-proof UI contract:
the front-end sends `rating_key` on every action and never learns about
`edition_key`). The data-loss-class actions each got their own tag:
PUSH/REPLACE/SWITCH-PLACEMENT, ADOPT-FROM-PLEX/RESTORE-CANONICAL, REVERT
(rating_key plumbing), UPLOAD-MP3, KEEP-MISMATCH, per-edition ACCEPT/DECLINE
decisions (schema), PROMOTE/intent, CLEAR-override, re-download/replace/backup
cancel-jobs, PURGE/FORGET, UNMANAGE, bulk PUSH/DOWNLOAD, ADOPT-sidecar, and
`_do_download`'s override-URL read. The library read-path split its JOIN into
edition-detection vs per-edition-decision.

### Arc — shared-folder / metadata-only editions (v1.21.93 → v1.21.99)
Not every edition has a distinct `{edition-X}` folder. Some are metadata-only
(folder tagged, but the historical placement/local_files were written un-tagged
at `''`), and some share one physical folder (Watchmen's three cuts under one
dir). LET PLEX SERVE flip-flopped: v1.21.93 added a `''` fallback for
metadata-only editions, but on shared-folder titles it bled across all three
(v1.21.95 gated the fallback on single-edition). The Phase 0 diagnostic
(v1.21.96) characterized every multi-edition title on prod: 100%
distinct-folders, 0 shared-folder → the per-edition SIDECAR model is universal;
the per-rk-upload Phases 2/3 were DROPPED. v1.21.99 made LPS skip a doomed
re-upload POST over Plex's ~10MB ceiling (LotR/Watchmen themes exceed it → HTTP
500 → sidecar fallback).

### What we learned this arc
- **Folder-derived keys beat rating_key-derived keys** for anything that must
  survive Plex's remove+re-add churn. `edition_key` lives in the folder name;
  the rk is ephemeral.
- **A discriminator deserves one chokepoint.** `editions.py` is the only place
  `''` is returned for an untagged folder — every drift bug in this arc was a
  call site that defaulted to `''` inline instead of routing through it.
- **The UI sends rating_key; the backend resolves edition.** This contract let
  the entire arc happen without the front-end learning the edition vocabulary.
- **Diagnose on-disk reality before choosing an architecture.** The Phase 0
  read-only diagnostic killed the per-rk-upload Phases 2/3 with one prod run.
- **Plex's theme-upload ceiling (~10 MB) is a hard physical limit.**
  Over-ceiling editions fall to sidecars; code must SKIP the doomed POST.

### Tag count + test trajectory
~100 tags (v1.21.0 → v1.21.99), dominated by the per-edition arc. Shipped
`editions.py` (the chokepoint), schema v63 (edition_key PKs) + v64, the
edition-diagnostics admin endpoint, and a per-edition mirror-drift lint
(v1.21.94) that walks every edition-blind read.

## 28. Condensed: v1.22.0 → v1.22.28 — edition data-loss audit batches + maintenance

The v1.22.x line is mostly AUDIT: a fresh-eyes hunt for the edition-scope bleeds
the v1.21.x arc missed, plus deployment hardening and a docs/maintenance pass.

### Arc — v1.22.0 cut + the Unraid permissions saga (v1.22.0 → v1.22.7)
v1.22.0 rolled over at the v1.21.x .99 close with a 3-agent audit; ONE
regression fixed pre-cut (SET URL on a tagged edition wrote the override at
`edition_key` but enqueued an edition-LESS download → the v1.21.92 read resolved
at `''` → ignored the URL). v1.22.2 root-caused the recurring "LET PLEX SERVE
does nothing" on Watchmen to a JS `rating_key` OMISSION (not a backend bug) — the
menu item shipped without `rating_key`, so the flow re-derived the row via an
ambiguous `(mt,id,section).find()` → first edition → a sibling with no placement
→ 0/0. v1.22.3-6 was the PERMISSIONS saga: motif was the only container baked to
uid 99 on a uid-1000 *arr/Plex stack, so writes to the 1000-owned shfs share
silently failed (shfs `default_permissions` denies silently). Fix: a
PUID/PGID/UMASK entrypoint (linuxserver-style gosu-drop) + a boot writability
probe that logs a loud uid-vs-owner WARNING instead of crash-looping. v1.22.7
closed 3 more edition data-loss/drift findings from a 5-agent audit.

### Arc — the anime NEEDS-WORK read-path batch (v1.22.8 → v1.22.14)
A DB-dig into the user's anime "NEEDS WORK" flood. v1.22.8: sync clears undecided
pending_updates whose TDB theme went NULL upstream. v1.22.10 consolidated the
pending-update ACTIONABLE gate into ONE helper (`_pending_update_actionable_sql`)
feeding 12 surfaces — the single chokepoint that ended the pill-vs-sort
mirror-drift class. v1.22.11 stopped the upload-MP3 INFO card posing as
URL-sourced; v1.22.12 stopped flagging `new_theme_available` on pure SRC=P rows
(anime is ~100% Plex-served; every sync flooded the !UPD pill).

### Arc — the edition data-loss audit batches (v1.22.15 → v1.22.23)
The centerpiece. A full-codebase 8-agent audit + a 4-agent sweep of EVERY
mutation on the four edition-PK tables (`user_overrides` / `local_files` /
`placements` / `pending_updates`). Confirmed + fixed, each behaviorally tested:
path-translation on the over-ceiling sidecar fallback + reaper (Unraid
host-vs-container `/mnt/user` vs `/data`); bulk ACCEPT ALL deleting overrides
section-WIDE (data-loss); ACCEPT silent no-op on 0-enqueue; cloud-backup
mid-batch abort; HAMA/anime `guid_tmdb→theme_id` linkage at 4 sync detection
sites (HAMA resolves to TVDB GUIDs so `guid_tmdb` is NULL — the gates silently
excluded ~all anime); the worker placement-path edition-scope cluster (+P-stamp
reading a verified-broken claim; hint/mismatch/relink edition keys); the
INFO-card no-op-pending suppression that v1.21.68's edition-first branch dropped;
the **download-failure rollback** restoring a non-`''` edition's override onto
the `''` row (silent data-loss); three section-keyed-without-edition bleeds
(UNPLACE plex_items flag, SET URL urls_match, scanner theme_id disjunct); and
`_drop_motif_tracking` wiping a sibling pure-P edition's decisions on a
last-section purge.

### Arc — maintenance + doc currency (v1.22.24 → v1.22.28)
Quarterly dependency-floor review (apprise 1.10→1.11; yt-dlp re-verified
still-latest). Recovery-walker edition-correctness hardening (#5/#7 joins; #6
left as provably-dead). A Phase 1 edition-backfill admin endpoint (re-key the ~5
historical mis-keyed LotR/Hobbit `''` rows) — built with dry-run+apply + an
adversarial review, then REVERTED (v1.22.27) when the user confirmed he'd fixed
them manually. v1.22.28 closed the last deferred coverage follow-up (behavioral
test for `reconcile_placement_paths`' edition JOIN).

### What we learned this arc
- **A long edition arc leaves bleeds; audit the WHOLE mutation surface.** The
  4-agent sweep inventoried all 113 mutations on the edition-PK tables and found
  the rollback data-loss + 3 section-keyed bleeds the arc's per-action focus missed.
- **HAMA/anime breaks any `guid_tmdb`-keyed gate** — they resolve to
  TVDB/AniDB GUIDs, so `guid_tmdb` is NULL. Match by `theme_id` linkage (the
  v1.15.142 pattern); the failure mode is SILENT (no prompt, no count).
- **`''` is overloaded** — it means both "standard edition" and "unknown/legacy."
  Every fix that defaults to `''` (rollback restore, `_drop_motif_tracking`, the
  SET URL re-insert) risks stealing a real standard edition or a sibling's state.
  Scope to the resolved edition, or `'' OR <acted>` to keep the clean-slate
  intent without bleeding.
- **Build → adversarial-review → revert is a valid outcome.** The Phase 1
  backfill was correctly built (the review caught a HIGH local_files-ambiguity),
  then reverted when unneeded. The dry-run-first design made it harmless either way.
- **Behavioral tests, not just source-pins, for anything edition-scoped** (the
  v1.18.81 phantom-fix lesson) — every v1.22 data-loss fix shipped a behavioral test.

### Tag count + test trajectory
~28 tags (v1.22.0 → v1.22.28, minus the reverted v1.22.26). The suite climbed to
**5,513** (v1.22.28); schema reached v64. No new feature modules — the line is
audit, hardening, deployment, and docs.
