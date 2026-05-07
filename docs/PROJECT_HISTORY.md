# motif Project Memory Digest

**Coverage**: 346 releases (v1.4.0 → v1.13.22), ~12.2K lines of detailed commit messages.
**Compiled**: Themes extracted by pattern, architecture pivot, recurring bugs, and feature evolution.

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

### E. Per-section REFRESH FROM PLEX (v1.10.6) + button locking unification (v1.11.5)
**When**: v1.10.6, v1.10.7, v1.11.5
**Pivot**: Originally single global REFRESH; became per-tab/variant, then unified all sync/refresh buttons under one lock.

**Why**: Users with multiple 4K/standard variants wanted granular control. Button spam-click when one enum was running broke UX. v1.11.5 unifies sync/refresh/enum under anyEnumInFlight signal.
**Code markers**: /api/library/refresh {tab, fourk} body; refreshTopbarStatus syncing-with-X label; lockBtn on (themerrdb_sync_in_flight OR plex_enum_in_flight).
**Lesson**: Granular controls need coordination; single in-flight signal beats per-operation checks.

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

**Current canonical model** (v1.13.x):
- T (ThemerrDB): source_kind='remote', upstream_source points to TDB. DOWNLOAD/RE-DL/REPLACE/REVERT/ACK-FAILURE.
- U (user URL): source_kind='url', user-supplied YouTube URL. DOWNLOAD/RE-DL/REPLACE-W-TDB/REVERT. Can flip to T via ACCEPT-UPDATE or revert to TDB.
- A (adopted): source_kind='adopt', file matched sidecar on disk. DOWNLOAD (re-adopt)/REPLACE-W-TDB.
- M (manual): uploaded MP3 or post-PURGE manual file. Resides in Plex folder. DL=off (static); UPLOAD/DELETE.
- P (Plex-only): plex_items row with no themes entry. UPLOAD.
- '-' (TDB-dropped): tdb_dropped_at set; TDB no longer publishes. Can ACK-DROP or CONVERT-TO-MANUAL.

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

