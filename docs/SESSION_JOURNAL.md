# motif session journal

Append-only log of Claude Code sessions: what we worked on, why, and
where we left off. Read by `.claude/hooks/session-start.sh` so a fresh
chat (after a crash, after a /compact, or just on a new day) picks up
the tail of this file as initial context.

## How to use

- Every meaningful chunk of work (a fix, a feature, a refactor, an
  investigation that didn't ship) gets a dated entry at the **bottom**
  of the file.
- Newest entry goes last so `tail -250` surfaces the most recent
  context to the next session.
- Cross-link to the commit SHA + tag when applicable so the journal
  agrees with `git log`.
- Capture **why** more than what — the diff already shows what.
- Note open threads / next-steps so the next session knows what to
  pick up.

Format for each entry:

```
## YYYY-MM-DD — short topic
**Branch**: <branch>  **Tag/SHA**: <tag-or-sha-if-shipped>

### Context
<what was reported / what we set out to do>

### Changes
- file.py: short summary, why this approach
- ...

### Open threads
- <next steps, not-yet-done, things to verify>
```

---

## 2026-05-07 — v1.13.23: library loading regression + per-tab SYNC PLEX lock
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.23` (commit `a4b8385`)

### Context
User reported on v1.13.22 that:
1. Library section showed `loading…` during a SYNC PLEX, AND randomly
   when no sync was running. Once a scan completed, the table stayed
   `loading…` until the user navigated away and back.
2. Clicking `// SYNC PLEX` on `/tv` locked the SYNC PLEX button on
   `/movies`, `/anime`, AND the dashboard `// SYNC THEMERRDB` button.
   User wanted per-tab scope: only the impacted library's button
   should lock during a per-library enum. SCAN ALL (settings) and
   SYNC + SYNC PLEX (dashboard pipeline) still need to lock all
   library buttons until the sweep ends.

### Root causes
1. **Loading regression** — v1.13.21's `tbody.dataset.lastHash`
   skip-write optimization (intended to preserve scroll position
   on no-op polls) sat AFTER an unconditional clobber to `loading…`
   in `loadLibrary()`. When the new render hashed equal to the
   prior one (same items on a 5s rapid-poll tick), the populated-
   branch write was skipped — leaving the placeholder on screen.
   Visible during slow API responses (DB lock contention during
   sync plex) AND during ordinary polling.
2. **Lock scope** — v1.11.27's commit comment claimed the library-
   page REFRESH button was gated on
   `q.plex_enum_active[tab][variant]`, but the implementation in
   `refreshTopbarStatus` collapsed back to the unified v1.11.5
   gate (`refreshBtnBusy = themerrdbBusy || plexEnumBusy`). So any
   plex_enum job locked every // SYNC PLEX button across every
   library tab AND the dashboard SYNC THEMERRDB button.
3. **API per-section dedupe** — `/api/library/refresh` and
   `/api/libraries/{section_id}/refresh` short-circuited on ANY
   in-flight plex_enum job globally. Once the UI started letting
   users fire concurrent per-tab scans, the second click would
   silently no-op (`already_queued`).

### Changes
- `app/__init__.py`: `__version__` → `1.13.23`.
- `app/web/static/app.js`:
  - `loadLibrary()` (~line 3635): only paint the `loading…`
    placeholder when `tbody.dataset.lastHash == null` (no prior
    render). During a re-fetch, leave existing rows in place;
    the populated branch will overwrite or skip based on the
    hash-compare as v1.13.21 intended.
  - `refreshTopbarStatus` (~line 530): split the lock variables
    — `dashSyncBtnBusy` (themerrdbBusy only), `libRefreshBusy`
    (myTabBusy or globalEnumPipeline), `settingsRefreshBusy`
    (any plex_enum or sync-cascade incoming). Per-tab busy is
    `enumActive[tab][variant]`. `globalEnumPipeline` triggers on
    `(themerrdbBusy && autoEnum)` OR `enumTabsActive > 1`
    (multiple tabs simultaneously enumerating = SCAN ALL or the
    cascade itself).
- `app/web/api.py`:
  - `/api/libraries/{section_id}/refresh`: dedupe scoped to that
    section (added `AND json_extract(payload, '$.section_id') = ?`
    to the existing-job check).
  - `/api/library/refresh`: legacy global-refresh path keeps the
    global short-circuit; per-tab branch now checks
    `pending_section_ids` and skips sections that already have
    in-flight jobs (mirrors `/api/libraries/refresh`).

### How to verify (user testing)
1. `/tv` (or any library tab) — rows render once and stay rendered
   through 5s rapid-poll ticks.
2. Click `// SYNC PLEX` on `/tv`. While running, `/movies` +
   `/anime` SYNC PLEX buttons + dashboard `// SYNC THEMERRDB` stay
   clickable.
3. Settings global `// SYNC PLEX` (or dash SYNC + auto_enum) — all
   library tabs' SYNC PLEX lock during the sweep.
4. With `/tv` enum running, click `/movies` SYNC PLEX. Should
   queue movies enum (visible in ops drawer / topbar), not silently
   no-op.

### Open threads
- Docker image build for `v1.13.23` triggered via the tag push;
  user pulling once available. Verify smoke tests pass.
- If the per-tab lock works as intended, consider revisiting v1.11.5's
  comment block (lines ~438-447 in app.js) to retire the unified-
  lock rationale that no longer matches the implementation.

---

## 2026-05-07 — set up SessionStart hook + journal
**Branch**: `claude/migrate-to-code-H70WJ`

### Context
User noted that prior chats have locked up after large image uploads
(>2000px). Wanted a way to resume context in a new chat without
re-explaining everything.

### Changes
- `.claude/hooks/session-start.sh`: SessionStart hook that prints
  branch state + last 15 commits + latest tag + `tail -250` of
  this journal to stdout (Claude reads it as initial context).
  Also runs `pip install -r requirements.txt` + `pip install pytest`
  in the remote sandbox (`CLAUDE_CODE_REMOTE=true`) so a fresh
  session can run tests immediately.
- `.claude/settings.json`: registers the hook for the `SessionStart`
  event.
- `docs/SESSION_JOURNAL.md`: this file. Append-only log; new entries
  at the bottom so `tail` surfaces the latest.
- `CLAUDE.md`: short pointer to the journaling convention so future
  sessions know to maintain it.

### Open threads
- Convention: every meaningful task ends with a journal entry (this
  is in CLAUDE.md). When a session ends mid-task, the entry should
  capture the in-flight state under "Open threads" so the next
  session can resume.

---

## 2026-05-07 — v1.13.24: variant-flip DONE flash, drawer label legibility, nudge bar honesty
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.24`

### Context
Three small polish items the user reported while testing v1.13.23:

1. While `/tv` was syncing on STANDARD, clicking `// 4K` made the
   library SYNC PLEX button briefly flash `✓ DONE` even though the
   topbar status pill was still showing "// SYNCING TV SHOWS" — the
   STANDARD enum was still running, the 4K variant just happened
   to be idle.
2. The TDB sync drawer card's stage timeline labels (GIT / DIFF /
   APPLY / SNAP / EXTR / M·IX / MOV / T·IX / TV / RES / PRUN) felt
   squashed at 9px + 0.06em letter-spacing. APPLY in particular
   was on the verge of ellipsis on narrow drawers.
3. The topbar mini-bar for "NUDGING PLEX TO RE-SCAN — 1 QU…" sat
   at `0%` until completion, then snapped to `100%` — the bar was
   tracking a single-job burst (HW=1) which only ticks once at
   completion, so it was effectively binary not-done/done masquerading
   as a percentage.

### Root causes
1. **DONE flash on variant flip** — v1.13.21's `dataset.sawBusy=1`
   flag was scope-agnostic. When `libRefreshBusy` flipped from
   `true` to `false` on a variant change (because `enumActive[tab]
   [fourk]` was false even though `enumActive[tab][standard]` was
   still true), the idle branch fired the DONE flash. The flag
   needed to track WHICH (tab, variant) was observed busy.
2. **Squashed labels** — `font-size: 9px` + `letter-spacing:
   0.06em` left no slack for 5-char labels (APPLY) at typical
   drawer widths. Reasonable bump in size + drop letter-spacing
   buys back the room.
3. **Nudge bar 0% → 100%** — the topbar mini-bar's `indeterminate`
   condition was `(stage_total || 0) <= 0`, only treating zero as
   indeterminate. Single-job ops have `stage_total=1`, so the bar
   tried to render a real percentage that ticked exactly once.
   The card-level bar already used `useRealBar = hasRealPct ||
   stage_total > 1`, so the mini-bar just needed to match.

### Changes
- `app/__init__.py`: `__version__` → `1.13.24`.
- `app/web/static/app.js` (~line 580):
  - Replaced `dataset.sawBusy` with `dataset.sawBusyScope` set to
    `${tabKey}:${variantKey}`. The DONE-flash branch only fires
    when the scope on the dataset matches the current scope. The
    else branch silently resets the label without flashing — so a
    variant flip mid-sync drops back to the idle label without a
    spurious DONE.
- `app/web/static/ops.css`:
  - `.op-card-timeline-labels` font 9 → 10.5px, dropped
    letter-spacing, opacity 0.55 → 0.7, margin-top 4 → 7px,
    gap 4 → 6px. Added `cursor: help` since each label has a
    `title=` carrying the long form.
  - `.op-card-timeline` gap 4 → 6px so the bar segments stay
    column-aligned with the labels.
- `app/web/static/ops.js` (~line 569):
  - Topbar mini-bar `indeterminate` now also covers `stage_total
    <= 1` (unless `detail.bar_pct` is present from yt-dlp's real
    %). Single-job nudges now show a shimmering bar with no `0%`
    text instead of misleading 0/100 jumps.

### Open threads
- Tag `v1.13.24` pushed; user pulling the Docker image once the
  release workflow finishes.
- The variant-flip fix only covers the library-page SYNC PLEX
  button. Settings global SYNC PLEX uses the same single-button
  pattern but doesn't have the variant-flip ambiguity (its scope
  is "all sections"), so no change needed there.

---

## 2026-05-07 — v1.13.25: cascade as per-section jobs, pipeline-stable button locks, autoEnum-aware dash label
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.25`

### Context
User testing v1.13.24 reported:

1. Dash SYNC button became clickable mid-pipeline when SYNC THEMERRDB
   + auto_enum_after_sync was on — the click could be re-fired during
   the plex_enum cascade phase, allowing concurrent syncs.
2. Dash button label said `// SYNC THEMERRDB` even when auto_enum was
   on (the click ran both phases). User couldn't tell from the label
   what their click would do.
3. Per-library SYNC PLEX on /movies?fourk=1 reported as locking
   /tv?fourk=1 button (scope-mismatch reading; could be reproducer
   confusion or a state we can't trigger from code reading).

### Root causes
1. **Cascade is one job, not many.** worker._do_sync's auto-enum
   block enqueued a single empty-payload plex_enum job that the
   worker iterated section-by-section internally. Externally,
   /api/stats's enum_running_rows query couldn't link the job to
   any section (`json_extract(payload, '$.section_id')` returns
   NULL) so plex_enum_active[tab][variant] stayed all-false through
   the entire cascade — every UI signal that depended on per-tab
   activity (library button locks, dash SYNC lock via
   globalEnumPipeline) was blind to the cascade. Also broke the
   "scanning section X" topbar pill labeling for cascade runs.
2. **dashSyncBtnBusy = themerrdbBusy** (v1.13.24) released the lock
   the moment tdb-sync finished, before plex_enum cascade had any
   chance to start counting. With (1) above making the cascade
   invisible to globalEnumPipeline, the button stayed clickable
   for the entire plex phase. v1.13.24 had explicitly traded this
   away to fix a different per-library issue from v1.13.23.
3. **Dash button label hardcoded.** v1.13.19 stabilized the label
   to `// SYNC THEMERRDB` regardless of auto_enum, on the theory
   that the topbar would carry the live state. But the label also
   serves as a "what does this button do" affordance — if the
   click runs both phases, the label should say so.

### Changes
- `app/__init__.py`: `__version__` → `1.13.25`.
- `app/core/worker.py`:
  - `_do_sync`'s post-sync auto-enum block now enqueues one
    `plex_enum` job per included section (each with
    `{"section_id": "...", "scope": "cascade"}`) instead of one
    empty-payload global. Mirrors `/api/libraries/refresh` (SCAN
    ALL), which was already per-section. Per-section dedupe via
    pending_section_ids check matches `/api/libraries/refresh`'s
    pattern.
- `app/web/api.py`:
  - `/api/libraries/refresh` (SCAN ALL): jobs now carry
    `"scope": "scan_all"`.
  - `/api/stats`: new `plex_enum_pipeline_in_flight` count —
    plex_enum jobs whose payload scope ∈ ('cascade','scan_all')
    that are pending or running. UI uses this to keep
    globalEnumPipeline true through the entire pipeline (incl.
    the tail when only one section is left and enumTabsActive
    has dropped back to 1).
- `app/web/static/app.js`:
  - `globalEnumPipeline` now ORs in `pipelineInFlight` from the
    new stat field. Stable lock through cascade + SCAN ALL tail.
  - `dashSyncBtnBusy = themerrdbBusy || globalEnumPipeline`
    (was: `themerrdbBusy` only). Dash SYNC stays locked through
    the whole pipeline.
  - Dash button `dataset.origLabel` adapts to `autoEnum`:
    `// SYNC THEMERRDB + PLEX` when on, `// SYNC THEMERRDB`
    when off. setSyncButtonState's idle-restore reads the
    dataset, so the label stays in sync with the current
    setting after each run.

### How to verify (user testing)
1. Toggle "Sync ThemerrDB and Plex" on in Settings → Schedule.
   Dashboard SYNC button label reads `// SYNC THEMERRDB + PLEX`.
2. Click it. Button stays disabled + "// SYNCING…" through
   tdb sync AND through the entire cascade plex_enum phase
   (across all included sections). Reloading mid-flight keeps
   the button locked too — the page-load check in bindDashboard
   sees `plex_enum_pipeline_in_flight > 0` and re-establishes
   the local watcher.
3. While the cascade is running, navigate to /movies, /tv,
   /anime. All `// SYNC PLEX` buttons should be locked
   (globalEnumPipeline=true via pipelineInFlight).
4. Per-library // SYNC PLEX on /movies?fourk=1 (NOT the dashboard
   sync). Other library tabs' SYNC PLEX buttons + dash SYNC
   should stay clickable (per-library scan doesn't tag jobs as
   pipeline; only its own scope is busy).
5. Toggle "Sync ThemerrDB and Plex" off in Settings. Dashboard
   button label drops back to `// SYNC THEMERRDB`. Click runs
   sync only (no cascade); dash button releases as soon as the
   tdb sync finishes.

### Open threads
- User reported (a) "queue depth indicator" ask and (b) "coverage
  chart confusion" ask, plus a possible cross-library lock case
  that I couldn't trigger from reading code. Holding on those
  three pending clarification — see chat for the question batch.
- Tag `v1.13.25` pushed; image build in progress.

---

## 2026-05-07 — v1.13.26: SHA→date in sync activity, probe message cleanup, placement.auto_place save fix
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.26`

### Context
User testing v1.13.25 reported:
1. Sync activity feed and the settings probe-transport message
   surfaced raw 8-char SHAs ("06979829") which are meaningless to
   users. Visible in the drawer info, dashboard, and settings.
2. Settings → PLACEMENT MODE → // SAVE PLACEMENT returned 400
   `{"detail":"unknown config section: placement"}`, so users
   couldn't disable auto-place from the UI even though
   MotifConfig.placement: PlacementConfig has existed since v1.5.3.
3. Stale /pending references in the placement-mode help text
   (the /pending tab was removed in v1.12.41).

### Root causes
1. `_summary_sha(sha) -> sha[:8].decode()` was used for activity
   messages directly. No effort to resolve the SHA to its commit
   date, which is what users actually care about ("when was this
   commit?"). The probe message took the same shortcut on the
   refs lookup result.
2. `_ALLOWED_TOP_LEVEL` in `_apply_partial_config` listed the
   six original sections (paths/plex/downloads/matching/sync/
   web/runtime) but never picked up `placement` when v1.5.3
   added the dataclass field. Every save body whose top-level
   key was `placement` raised ValueError → 400.
3. Help-text drift; v1.12.41 dropped /pending without sweeping
   /settings copy.

### Changes
- `app/__init__.py`: `__version__` → `1.13.26`.
- `app/core/sync.py`:
  - New `_summary_commit(sha)` instance method that resolves the
    SHA via `self._repo[sha]` and returns
    `"YYYY-MM-DD HH:MM UTC"` from `commit.commit_time`. Falls
    back to `_summary_sha` on KeyError so a missing-from-mirror
    sentinel still degrades gracefully.
  - Replaced every activity-message call site that previously
    used `_summary_sha`: clone-at, fetch range, "no new commits
    since X", diff range. Drawer now reads "Fetched 2026-05-06
    13:00 UTC → 2026-05-07 14:30 UTC" instead of "Fetched
    df6b6835 → 06979829".
- `app/web/api.py`:
  - `_probe_git`: detail message dropped the SHA suffix; now
    just `"branch <name> reachable"`. The probe is a transport
    reachability check, not an "is this commit interesting?"
    check, so the SHA was incidental noise.
  - `_ALLOWED_TOP_LEVEL`: added `"placement"`. Settings →
    PLACEMENT MODE → // SAVE PLACEMENT now persists.
- `app/web/templates/settings.html`:
  - Replaced the auto-place help text. Was: "downloads land in
    /pending for manual approval — useful when you want to
    review what motif found before publishing. Toggle this off
    to require manual approval for every download (each one
    lands on /pending until the user approves)." Now describes
    the actual flow: download still happens (DL pill green),
    PL pill stays empty, // SOURCE menu picks up // PUSH TO
    PLEX, same affordance as the downloaded-but-missing-from-
    Plex case. No /pending references.

### How to verify (user testing)
1. Settings → SCHEDULE → PROBE TRANSPORT. Result reads
   "✓ GIT reachable · 274ms · branch database reachable" — no
   raw SHA on the end.
2. Trigger a sync (manual or wait for cron). LIVE OPS drawer
   activity feed for THEMERRDB SYNC reads "Fetched 2026-MM-DD
   HH:MM UTC → 2026-MM-DD HH:MM UTC" instead of two SHAs.
3. Settings → PLACEMENT MODE. Toggle AUTO-PLACE AFTER DOWNLOAD
   off, click // SAVE PLACEMENT. Banner should read "saved" (or
   similar) — no 400 error.
4. With auto-place off, trigger a download (RE-DL on a row).
   The download completes; row shows DL=green, PL=amber !,
   title gets the amber ! glyph. PLACE menu now offers // PUSH
   TO PLEX.
5. Click // PUSH TO PLEX. Place job runs; row goes to all-
   green DL=green, PL=green.

### Open threads
- Queue depth indicator (1 of N) on the topbar mini-bar — user
  picked the inline-counter option. Will land in v1.13.27.
- Coverage charts: user picked "cross-library comparison off"
  meaning the absolute counts make small libraries
  (e.g. 28-item 4K Movies) hard to compare with big ones
  (10K Movies). Will switch to percentage view in v1.13.27.
- Cross-library lock from per-library click reported as still
  reproducing. v1.13.25's per-section cascade fix may have
  resolved it; user to retest. If still buggy, ask for steps
  to reproduce.

---

## 2026-05-07 — v1.13.27: queue-depth on topbar, per-section coverage bars
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.27`

### Context
Two follow-up asks from the v1.13.25 testing batch:

1. When the user fires several // SYNC PLEX clicks across libraries,
   the topbar mini-bar should communicate "X of Y" — i.e. which
   queued job is currently running and how many remain.
2. The dashboard COVERAGE COMPARISON bars (added v1.13.22) collapsed
   every Plex section into Movies vs TV totals. With 4K Movies at 28
   items and Movies at 10K items, the small library's coverage was
   masked by the big one. User picked "cross-library comparison off"
   when asked what was confusing about the chart.

### Changes
- `app/__init__.py`: `__version__` → `1.13.27`.
- `app/web/static/app.js`:
  - `refreshTopbarStatus`: stash a per-kind queue snapshot on
    `window.__motif_queue.plex_enum = { current, hw }`. `current`
    is the live in-flight count from /api/stats; `hw` is the
    burst's high-water (max seen since last drain). Reset to 0
    when the queue empties so each new burst starts fresh.
  - `renderCoverageComparison`: rewritten to take per-section
    rows from `/api/sections/coverage`. One row per section,
    normalized to 100% of that section's total. Title carries
    the section name + STD/4K subtype. Each row is now an
    `<a>` click-through to the matching library tab. Bar drops
    to 2 segments (themed / unthemed) — the v1.13.22 third
    "TDB-available" segment is dropped because the section
    coverage payload doesn't carry that split, and the 2-segment
    view is what users actually compare across rows.
  - Hooked the call site: now invoked alongside
    `renderSectionCoverage` from the same `/api/sections/coverage`
    fetch, instead of from the aggregated movies/tv path.
- `app/web/static/ops.js`:
  - `renderTopbar`: when `op.kind` matches a key in
    `window.__motif_queue` and `hw > 1`, append `(X of Y)` to the
    label. Position computed as `hw - current + 1`. plex_enum is
    the common case (multi-tab // SYNC PLEX clicks, settings
    SCAN ALL, sync→enum cascade).
- `app/web/static/app.css`:
  - `.coverage-row` styled as a click-through anchor (no
    underline, color inherits, subtle hover).
- `app/web/templates/dashboard.html`:
  - // COVERAGE COMPARISON header subtitle updated to "themed
    vs unthemed · normalized per section". Block comment notes
    that v1.13.22's three-segment / aggregate-by-tab approach
    was replaced.

### How to verify (user testing)
1. With the cron disabled or before the next sync, fire
   // SYNC PLEX on /movies, then jump to /tv and fire // SYNC PLEX,
   then /anime. Three plex_enum jobs should be queued. Topbar
   reads "// SYNCING <section> (1 of 3)" and ticks "(2 of 3)",
   "(3 of 3)" as the worker drains the queue.
2. Same flow with the dashboard SYNC button (auto_enum on): the
   cascade enqueues one job per managed section. Topbar reads
   "(1 of 5)" through "(5 of 5)".
3. Dashboard // COVERAGE COMPARISON now shows one bar per
   managed Plex section. 4K Movies (28 items, X% themed) sits
   next to Movies (10K, Y% themed) with normalized bars — the
   small library's coverage is no longer masked by the large
   one. Clicking a row navigates to the matching library tab.

### Open threads
- Cross-library lock from per-library click — still pending
  user retest. v1.13.25's per-section cascade fix may have
  cleared it.

---

## 2026-05-07 — v1.13.28: audit triage (cron-vs-click race, tab validation, loadLibrary races)
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.28`

### Context
Subagent code review of v1.13.23 → v1.13.27 surfaced a P0/P1
punch list. Triaged and addressed in this release.

### Changes
- `app/__init__.py`: `__version__` → `1.13.28`.
- `app/core/worker.py` (#2 from audit, P1):
  - `_do_sync`'s post-sync cascade-enqueue block was reading
    `pending_section_ids` and inserting the new jobs in autocommit
    mode (`get_conn` uses `isolation_level=None`). A cron tick and
    a manual click landing in the same millisecond could both
    observe the same pre-insert snapshot and both insert duplicate
    per-section enum jobs. Wrapped in `transaction(conn)` (BEGIN
    IMMEDIATE), so the second caller blocks on the first's writer
    lock and re-reads after commit. Same pattern the rest of the
    codebase uses for race-sensitive inserts.
- `app/web/api.py` (#1 from audit, P1):
  - `/api/library/refresh` previously fell through to the legacy
    global insert when `body["tab"]` was anything other than the
    allowlisted three values (or null/missing). A typo or older
    client could therefore queue duplicate global plex_enum jobs
    bypassing dedupe. Now validates `tab` up front: `null`/missing
    keeps the legacy "scan everything" path; allowlisted strings
    take the per-tab branch; anything else 400s with a clear
    message. The dedupe short-circuit moved from the negative
    `tab not in (...)` check to an explicit `tab is None` check
    so the validation gate can't be bypassed.
- `app/web/static/app.js`:
  - **#7 colspan**, P2: error branch in `loadLibrary` wrote
    `colspan="8"` while the loading + empty-state branches
    wrote `colspan="9"` (table has 9 cols). Aligned to 9.
  - **#8 in-flight fetch race**, P1: a fast filter-chip click
    while a prior fetch was in flight could let either response
    win, possibly clobbering fresher data with stale. Added a
    monotonic `loadLibrary._seq` token; older calls bail before
    touching tbody when their token is superseded.
  - **#11 dead aggregate-coverage code**, nit: removed
    `movAvail/movNoTdb/tvAvail/tvNoTdb` computations and the
    transitional comment block. v1.13.27 moved comparison bars
    to per-section data; the aggregated movies/tv recomputation
    that fed the v1.13.22 design was orphaned.

### Deferred (lower-severity findings)
- #3 (json_extract on legacy global rows): tied to #1; resolved
  indirectly. The validation gate prevents new global rows from
  being mis-tagged.
- #4 (HW state can drift if user backgrounds tab): cosmetic
  inaccuracy on the topbar "(X of Y)" suffix when the tab
  background-pauses; would require server-side burst tracking
  to fully resolve. Acceptable for now.
- #5 (position math assumption): noted, single-worker queue
  drain is FIFO so the math holds for the kinds we surface.
- #6 (sawBusyScope cleared on flip): intentional; the existing
  comment captures the trade-off.
- #9, #10, #12, #13, #14, #15, #16, #17: minor / edge / nit.

### How to verify (user testing)
1. Race test: queue a manual sync from /dash while the cron
   would also fire (~midnight UTC). Worker should emit exactly
   N enum jobs (one per included section), not 2N. The
   transaction wrap holds the writer lock through the read+
   write window.
2. Bad-tab guard: `curl -X POST /api/library/refresh -d '{"tab":"foo"}'`
   returns 400 with a clear "unknown tab" detail instead of
   silently queuing a duplicate global plex_enum.
3. loadLibrary race: rapid-fire filter chips (status, TDB
   pills, src letter) on a slow connection — the table stays
   consistent with the latest click rather than reverting to
   stale data when an older fetch's response lands second.

### Open threads
- Cross-library lock from per-library click — still pending
  user retest.

---

## 2026-05-07 — v1.13.29: wider audit triage (v1.13.19 → v1.13.28 sweep)
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.29`

### Context
Subagent code review of v1.13.19 → v1.13.28. The user shipped
v1.13.19 from a different machine and v1.13.20-v1.13.22 were
pre-audit. Six P2 findings addressed.

### Changes
- `app/__init__.py`: `__version__` → `1.13.29`.
- `app/core/sync.py`:
  - **#1 tighten dulwich progress kwarg fallback**: `except
    TypeError` previously masked any TypeError as "old dulwich
    without progress=" and silently retried. Now only falls
    back when `'progress' in str(e)`; other TypeErrors raise
    immediately as `_GitMirrorError` with the original message.
  - **#2 buffer split progress chunks**: dulwich's progress
    callback delivers bytes split arbitrarily mid-line. Pre-fix
    `b"Receiving: 95% (1000/" + b"2380)\r"` lost both halves to
    a non-match; the most user-visible drop was the final
    cur==total emit landing on a chunk boundary. Added a
    function-attribute byte buffer that prepends to the next
    chunk and only parses complete `\r`-terminated samples.
- `app/web/static/ops.js`:
  - **#3 optimistic placeholder same-kind clear**: pre-fix any
    pre-existing op cleared a fresh placeholder on the next 1s
    poll, so a SYNC PLEX click landing while a tdb_sync was
    running felt unresponsive on the topbar. Now clears only
    when a SAME-KIND op arrives (or after the 5s expiry).
- `app/web/static/app.js`:
  - **#4/#5 phantom DONE on dashboard reload**: page-load probe
    set `primed = true` immediately, so a cron sync's tail
    finishing on the first poll tick after reload triggered a
    spurious `setSyncButtonState('done')` ✓ DONE flash. Now
    starts `primed = false` (matches the click path); if the
    sync actually finishes between page-load probe and first
    tick, watcher clears silently and the regular polling
    unlock takes over.
  - **#7 coverage cache key includes routing fields**:
    `_lastCoverageComparisonKey` hashed
    `[section_id, title, total, themed]` only. A section
    reclassified via /settings (toggling A or 4K flags) wouldn't
    bump the hash, so the user kept seeing the stale STD/4K
    subtype label until counts happened to change. Added
    `tab`, `is_4k`, `is_anime` to the key.
  - **#9 queue HW for late-arriving jobs**: pre-fix `hw` clamped
    to the burst's initial peak; once worker drained 4→1 and
    user fired another (current=2), hw stayed at 4 and the
    `(X of Y)` suffix said "(3 of 4)" while the actual total was
    5. Now tracks the increase delta and grows hw with
    late-arriving jobs without losing completed-position
    progress.

### Deferred (lower-severity)
- #6 cancel-button cosmetic flash, #8 defensive title escaping,
  #10 already guarded.

### How to verify (user testing)
1. Reload mid-cron: trigger a manual sync, then reload the
   dashboard. If the sync finishes near the moment the page
   loads, button reverts silently to its idle label instead of
   flashing ✓ DONE for a sync the user never triggered.
2. Mid-burst queue: trigger settings // SYNC PLEX (5 sections),
   wait until 3 are done ("(4 of 5)"), then click // SYNC PLEX
   on /movies?fourk=1. Suffix should bump to "(5 of 6)".
3. Optimistic placeholder hand-off: while a TDB sync is running,
   click // SYNC PLEX on a library tab. Topbar mini-bar carries
   the tdb_sync (both ops in flight is normal). When tdb_sync
   finishes, if plex_enum hasn't started yet, the placeholder
   surfaces — pre-fix it had been cleared on the very first
   poll after click and the topbar would briefly show idle.
4. Section reclassification: toggle a section's 4K or anime flag
   in /settings → SAVE. Dashboard COVERAGE COMPARISON's STD/4K
   subtype updates on the next dashboard poll instead of
   waiting for a count change.
5. Fetch progress smoothness: kick a TDB sync. The progress bar
   fills more smoothly through the fetch, especially near the
   100% finish (no dropped final-sample stuck-at-95%).

### Open threads
- Cross-library lock from per-library click — still pending
  user retest after v1.13.29.

---

## 2026-05-07 — v1.13.30: tab-wide SYNC PLEX lock, DONE button non-clickable, queue wording, yt-dlp 100%
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.30`

### Context
User testing v1.13.29 reported four issues:

1. Toggling STANDARD ↔ 4K mid-scan made the SYNC PLEX button
   click-eligible again — letting the user pile a 2nd scan on top
   of the still-running 1st (logs showed jobs 628-637, all rapid-
   fire plex_enums on section 18). v1.13.23's per-(tab,variant)
   lock was the regression: the variant chip changed scope, and
   the OTHER variant's idle state unlocked the shared button.
2. The ✓ DONE flash on the library SYNC PLEX button was clickable.
   It's a notification, not an action; double-clicking through it
   queued a fresh scan.
3. Topbar mini-bar read "NUDGING PLEX TO RE-SCAN — 1 QUEUED" when
   a lone nudge was the only thing in flight. User read "queued"
   as "queued behind something" rather than "this single item is
   in the queue".
4. yt-dlp download bar sometimes stopped short of 100% — yt-dlp's
   "downloading" status emits stopped before downloaded/total ever
   reached parity (e.g., chunked transfer where the last bytes
   arrived without a progress event), and "finished" didn't fire
   reliably on every file.

### Changes
- `app/__init__.py`: `__version__` → `1.13.30`.
- `app/web/static/app.js`:
  - **#1 tab-wide busy**: `myTabBusy` now ORs both variants of the
    current tab so the shared SYNC PLEX button stays locked while
    EITHER STANDARD or 4K is enumerating. The DONE-flash scope
    key (`tab:variant`) is unchanged so visual feedback remains
    per-variant.
  - **#2 DONE flash button disabled**: `libRefreshBtn.disabled =
    true` during the ✓ DONE flash; re-enabled in the setTimeout
    that restores origLabel. Mirrors how the dashboard SYNC
    button's setSyncButtonState('done') already worked.
- `app/core/progress.py`:
  - **#3 single-op queue label**: when `running_n + pending_n == 1`,
    drop the misleading count suffix entirely. Singular bursts
    show just the kind label ("NUDGING PLEX TO RE-SCAN") or the
    label + "queued" suffix (no count). Multi-op bursts keep the
    explicit "1 running, 2 queued" form.
- `app/core/downloader.py`:
  - **#4 yt-dlp final 1.0 emit**: after `ydl.download(...)` returns
    successfully, emit `progress_callback(1.0)` unconditionally.
    Belt-and-suspenders for the cases where yt-dlp's own
    "finished" hook didn't fire on the last file in a run.

### Deferred
- Concurrent download + sync visibility on the topbar (user's
  request from this session) — needs design work; not blocking.
- M-can-mask-P discussion — substantial UI/data-model question,
  user explicitly asked to discuss before action.
- REPLACE-WITH-TDB place skip on `plex_has_theme` — small
  hotfix, will land separately once the M+P direction is decided.

### How to verify
1. /movies?fourk=0, click SYNC PLEX (standard scan starts).
   Toggle to 4K mid-scan: button stays disabled and labeled
   `// SYNCING…`. Toggle back: same. Standard finishes:
   ✓ DONE flash for ~1.5s, button stays disabled during the
   flash, then reverts to `// SYNC PLEX` enabled.
2. Trigger a single Plex nudge (e.g., RE-PUSH on one row).
   Topbar reads "NUDGING PLEX TO RE-SCAN" without a misleading
   "1 QUEUED" suffix. Multi-row bulk push reads
   "NUDGING PLEX TO RE-SCAN — 3 queued" / "1 running, 2 queued"
   correctly.
3. Trigger a download (RE-DL on a row). The topbar percentage
   bar fills smoothly through completion (100% on success)
   instead of stopping short.

### Open threads
- M-vs-P coexistence design (B option recommended: separate PLX
  pill alongside SRC). Awaiting user direction.
- REPLACE-WITH-TDB place-skip hotfix (small, related to above).
- Concurrent ops topbar visibility (defer until needed).
