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

---

## 2026-05-07 — v1.13.31: composite SRC (M+P, T+P), PURGE consequence preview, bulk-download force-place
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.31`

### Context
User's M-can-mask-P design discussion landed on Option A: composite
letters that show both motif's primary and the Plex-side fallback
when both are present. Bundled with the bulk-download place-skip
hotfix that the M+P observation surfaced.

### Changes
- `app/__init__.py`: `__version__` → `1.13.31`.
- `app/web/api.py`:
  - **`/api/library` SRC pill filter**: `P` filter now matches
    `pi.has_theme = 1 AND COALESCE(plex_theme_verified_ok, 1) = 1`
    regardless of the primary SRC letter. Composite rows (T+P,
    U+P, A+P, M+P) appear in the P filter so the user can audit
    every row where Plex has a fallback. Other letter chips
    keep their original behavior. Multi-select OR semantics
    preserved.
  - **`/api/library/download-batch`**: now passes
    `force_place=True` to `_enqueue_download`. Pre-fix bulk
    DOWNLOAD on M-source rows downloaded the TDB theme but the
    place worker logged `Skipped placement: plex_has_theme`
    because the row had `has_theme=1`. The user's bulk-select
    action is an explicit "yes, replace" intent, mirroring
    single-row Replace-with-ThemerrDB.
- `app/web/static/app.js`:
  - **Composite SRC chip rendering**: when motif's primary
    letter is T/U/A/M and Plex also serves its own theme
    (verified or untested), the SRC cell renders an additional
    small `P` chip beside the primary. Pure-P rows unchanged.
    Two side-by-side mini-pills mirror the existing pill
    aesthetic.
  - **PURGE confirm preview**: `purgeTheme()` now takes a
    `plexAlso` flag (sourced from the row's plex_has_theme +
    verified state). When `plexAlso=true`, the dialog adds:
    "After PURGE: Plex serves its own theme — Plex's version
    becomes the active one." When `plexAlso=false`, dialog
    adds: "Plex has no fallback — title will be themeless until
    next sync." Bulk PURGE confirm not affected (different
    path).
- `app/web/static/app.css`:
  - `.src-also-plex` styling — secondary chip slightly muted
    (opacity 0.78) and smaller (0.92em) so the primary letter
    reads as the dominant signal.

### How to verify
1. Find a row where Plex has its own theme AND motif also
   manages it (e.g., a row that was M before, or any T row
   where Plex Pass cloud also serves a theme). The SRC cell
   should render two side-by-side chips: the primary letter
   (T/U/A/M) and a smaller `P`. Pure-P rows render a single P
   as before.
2. Click the `P` chip in the SRC filter. The row count expands
   to include every row where Plex serves a theme — including
   composite T+P/U+P/A+P/M+P rows AND pure P. Pre-fix only
   pure-P rows matched.
3. Click `× PURGE` on a composite row (e.g., M+P). Confirm
   dialog includes the "Plex serves its own theme — its
   version becomes active" preview. Click PURGE on a pure-T
   row with no Plex fallback: dialog says "Plex has no
   fallback — themeless until next sync."
4. Bulk-select a few M+P rows (e.g., 102 Dalmatians, 12 Years
   a Slave) and click DOWNLOAD. Each row's downloaded theme
   places successfully (no more "Skipped placement:
   plex_has_theme" in the logs).

### Open threads
- **Info-card adapt for dead-TDB rows** (user's last ask): when
  a row's TDB URL fails (video unavailable / private / age /
  geo) and the user works around via ADOPT (M→A) or SET URL
  (→U), the info card should adapt — drop the dead URL preview
  and shift the "try this next" guidance. On a future TDB sync
  if the URL becomes available again, restore the preview +
  guidance. Defer to v1.13.32.
- Concurrent ops topbar visibility (defer until needed).

---

## 2026-05-07 — v1.13.32: P0 filter-count fix (PL=await + only-PL filter combos 500'd)
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.32`

### Context
User testing v1.13.29-v1.13.31 hit a filter-induced 500:
"sqlite3.OperationalError: no such column: lf.file_path".
Reproduces when `pl_pills` is set with no other axis-filter that
triggers `needs_themes_for_count` (no tdb_pills, src_pills, status,
or tdb!=any). The slim count path's `needs_lf_for_count` check
omitted `bool(pl_pills)`, but the `pl_pills="await"` SQL branch
references `lf.file_path` ("p.media_folder IS NULL AND
lf.file_path IS NOT NULL"). Slim count → no lf join → 500.

### Changes
- `app/__init__.py`: `__version__` → `1.13.32`.
- `app/web/api.py`: added `bool(pl_pills)` to
  `needs_lf_for_count` so the slim count path joins `local_files`
  whenever PL is filtering. Comment captures the regression
  signature for future debugging.

### How to verify
1. With no other filters, click PL=`await` (the awaiting-placement
   state filter). The library count + rows render correctly
   instead of 500'ing.
2. PL=`off` alone (no other filters): no regression — `off`
   doesn't reference lf, but the join is harmless.

### Open threads (deferred to v1.13.33+)
- Info-card adapt for dead-TDB rows (M→A adopt path, replace
  failure with success guidance).
- Bulk PUSH TO PLEX for downloaded-but-not-placed rows.
- Concurrent ops topbar visibility.

---

## 2026-05-07 — v1.13.33: info-card resolved-state, bulk PUSH TO PLEX
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.33`

### Context
Two follow-up items the user queued earlier:
1. When a row's TDB URL fails (video private/removed/age/geo) but
   the user works around via ADOPT (M→A) or SET URL (→U) or UPLOAD,
   the info-card's "TRY THIS NEXT" section was misleading — the row
   is fine (has a working local source), there's nothing to act on.
2. Bulk PUSH TO PLEX for downloaded-but-not-placed selections.
   v1.13.31's bulk-download force-place fix landed for new
   downloads, but the user wanted a way to push a hand-picked
   selection of awaiting-placement rows in one click.

### Changes
- `app/__init__.py`: `__version__` → `1.13.33`.
- `app/web/api.py` — `/api/items/{mt}/{id}/recovery-options`:
  - Detects "locally-resolved" state: `failure_kind` set on
    themes row AND a `local_files` row exists with
    `source_kind ∈ ('adopt','url','upload')` and a non-NULL
    `file_path`. When true, returns `resolved=true,
    resolved_via=<source>` and replaces the recovery options
    list with a non-interactive "// RESOLVED VIA <SOURCE>" tile +
    ACK FAILURE (when not yet acked). Drops RE-DOWNLOAD / SET URL /
    UPLOAD MP3 since the row is already covered by a working
    local source.
- `app/web/static/app.js` — `hydrateRecoverySection` (~line 7637):
  - Section title swaps from "// TRY THIS NEXT" to
    "✓ RESOLVED — TDB UNAVAILABLE" on resolved rows.
  - Adds `.recovery-section-resolved` class so the title color
    flips amber → green-bright (calmer "this is fine" signal).
  - Annotates the THEMERRDB URL row at the top of the dialog
    with "(unavailable — using local source)" so the user
    sees the upstream-axis state without scrolling.
- `app/web/static/app.css`:
  - `.recovery-section-resolved .recovery-section-title` →
    green-bright color.
  - `.tdb-unavailable-badge` → amber, small left margin.
- `app/web/templates/library.html`:
  - New `library-push-selected-btn` ("// PUSH TO PLEX") in the
    bulk-action bar. Hidden by default; gated visible when the
    selection has at least one downloaded-but-not-placed row.
- `app/web/static/app.js` (bulk handler + selection-ui):
  - `updateLibrarySelectionUi` walks the selection and counts
    `pushableCount` (downloaded canonical + no media_folder +
    no job_in_flight + themed/non-orphan). Button label adapts:
    "// PUSH TO PLEX" when all selected qualify, "// PUSH N TO
    PLEX" when only some do.
  - Click handler: filters the selection to pushable rows,
    confirms count, fires per-row `/api/items/{mt}/{id}/replace`
    sequentially. Acceptable for the scale this surfaces
    (handful of awaiting-placement rows). Failed rows counted
    separately so one bad row doesn't abort the rest. Topbar
    poll re-fires after a short delay so the queue counts
    update.

### How to verify
1. **Resolved info-card**: find a row with `failure_kind` set
   that you've worked around (M→A adopt, or SET URL with a
   non-TDB YouTube URL). Open INFO. Recovery section reads
   "✓ RESOLVED — TDB UNAVAILABLE" in green; THEMERRDB URL row
   has "(unavailable — using local source)" badge. ACK FAILURE
   visible only if not yet acked. RE-DOWNLOAD / SET URL /
   UPLOAD MP3 hidden (row is already covered).
2. **Bulk PUSH TO PLEX**: select a few rows that show DL=green
   + PL=await (the awaiting-placement state — title glyph is
   the amber `!`). The bulk bar shows the new "// PUSH TO
   PLEX" button. Click → confirms → one /replace call per row.
   Rows show DL=green + PL=green after a few seconds.

### Open threads
- Concurrent ops topbar visibility (defer until needed).
- Future TDB sync that revives a dead URL: the info card's
  resolved state will revert automatically (failure_kind
  clears on next successful sync; recovery-options endpoint
  returns the regular flow). UPDATE pill (TDB↑) on the row
  surfaces re-evaluation. Already wired via existing logic;
  no v1.13.33 change needed for the restore path.

---

## 2026-05-07 — v1.13.34: dashboard insight charts (failure breakdown, sync sparkline, daily download activity)
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.34`

### Context
User asked for additional dashboard visualizations to round out
the "at a glance" picture. After triage I recommended three (and
explicitly recommended against scatter plots, redundant pies,
and historical-coverage snapshots that need new schema). User
greenlit all three for v1.13.34 to test and refine.

### Changes
- `app/__init__.py`: `__version__` → `1.13.34`.
- `app/web/api.py`: new `/api/dashboard/insights` endpoint.
  Single round-trip returning three series:
  - `failures`: GROUP BY `themes.failure_kind` for unacked
    failures (mirrors topbar FAIL pill's gate so chart and
    counter stay synchronized).
  - `syncs`: last 30 `sync_runs` rows (finished + non-NULL
    wall_clock), reversed to oldest → newest for sparkline
    paint order.
  - `downloads`: per-day `download` job counts for the last 30
    days. Server returns only days with at least one download;
    client backfills idle days for a fixed 30-slot axis.
- `app/web/templates/dashboard.html`: three new blocks
  (`#insight-failures-block`, `#insight-syncs-block`,
  `#insight-downloads-block`), each `display:none` until the
  fetch lands.
- `app/web/static/app.js`:
  - `loadDashboard` adds an `/api/dashboard/insights` fetch
    after the section-coverage call. Three render functions
    in series: `renderFailureBreakdown`, `renderSyncPerformance`,
    `renderDownloadActivity`.
  - **Failure breakdown**: horizontal bars sorted by count desc,
    each row is an `<a>` deeplink to the library filtered to
    `tdb_pills=dead&fk=<kind>` (kind hint reserved for future
    per-kind narrowing).
  - **Sync sparkline**: SVG line chart of last 30 wall-clock
    durations. Markers color-coded by status (green=ok,
    muted=no-op short-circuit, red=failed). Tooltip via
    `<title>` carries timestamp / transport / counts.
  - **Daily download bars**: 30-slot vertical bar row. Idle
    days render at zero height; busy days scale 0–100% of
    the peak.
  - All three use cache-key hashing (`_lastInsight*Key`) so the
    30s dashboard poll skips the DOM swap when nothing changed.
- `app/web/static/app.css`:
  - `.insight-row` block frame, shared body padding.
  - `.insight-fail-row` grid (label / bar / count) with a red
    fill bar (status: dead).
  - `.sync-spark-svg` + `.sync-spark-line` + status-tinted
    markers.
  - `.dl-activity-bars` flex row with green-bright bars on a
    shared baseline.

### Style notes
- No external chart lib. CSS+SVG only. Bundle stays small;
  motif's CRT/monospace look stays intact.
- Each chart hides on no-data so a fresh install doesn't
  render empty axes.
- Click-through where it makes sense (failure bars → library
  filter). Sparkline + download bars are tooltip-only — drilling
  into a specific day or run isn't useful for those charts.

### How to verify
1. **Fresh install**: dashboard renders without the three new
   blocks (insights endpoint returns empty arrays → render
   functions hide their sections).
2. **With failures**: trigger a few cookies-expired downloads
   or use a row with a known-dead URL. // FAILURE BREAKDOWN
   appears with one bar per kind. Clicking a bar navigates to
   /movies?tdb_pills=dead with the failure-kind hint in the
   URL.
3. **Sync sparkline**: kick a few syncs over time. // SYNC
   PERFORMANCE shows the wall-clock trend. no-op runs (Phase B
   git tree unchanged) render as muted dots; failed runs as red.
4. **Download activity**: bulk-download a few rows. Today's
   bar grows. The 30-slot row stays — idle days render as
   empty slots so the day cadence is consistent.

### Composite-+P regression fix (also v1.13.34)
User reported: "every item that has a src is also being shown as
having +P, which we know that's not accurate." Inspecting
v1.13.31's gate:
```js
const _plexAlso = !!it.plex_has_theme && verifiedOk
                  && letter !== 'P' && letter !== '-';
```
`pi.has_theme` is set by Plex whenever its metadata XML reports a
theme — including the sidecar motif itself placed. So every T/U/A/M
row had a phantom +P chip stacked underneath. Visually doubled
every row's chip column, and the claim ("Plex also has its own
theme") was usually false.

**Detection fix.** The right discriminator for "Plex serves its
own theme separately from motif's placement" is the absent-sidecar
case: `has_theme=1 AND local_theme_file=0`. That covers Plex Pass
cloud, themerr-plex embeds, and stale Plex agent claims — the
genuinely-separate-from-motif cases. When motif places a sidecar
successfully, `local_theme_file=1` and the composite condition
goes false (no separate theme to surface). New gate:
```js
const _plexAlso = !!it.plex_has_theme && !it.plex_local_theme
                  && verifiedOk && letter !== 'P' && letter !== '-';
```

**Visual fix.** The stacked-second-chip approach was double-stacked
in narrow SRC cells anyway. Replaced with a single corner dot on
the primary chip via CSS `::after` + `link-badge-also-plex` class.
Same chip, single visual unit, small amber dot in the top-right
corner signals the composite state. Tooltip on the chip explains.

**PURGE preview softened.** v1.13.31's negative branch said "Plex
has no fallback — themeless until next sync." That's overconfident
— motif can't actually tell whether Plex Pass has a cloud theme
when the sidecar wins. Reworded to "no DETECTED fallback" and
acknowledged the Plex Pass possibility.

### Files (additional, beyond the dashboard insights)
- app/web/static/app.js: rewrote composite +P detection,
  swapped the second-chip injection for a class-tag-on-primary
  approach, softened purgeTheme's negative-branch copy.
- app/web/static/app.css: dropped `.src-also-plex` rules,
  added `.link-badge-also-plex::after` corner-dot.

### Open threads
- Concurrent ops topbar visibility (still deferred).
- Future per-kind library filter so the failure-breakdown
  deeplink narrows to e.g. video_removed only (today the &fk=
  param is parsed but not applied — library filters to all
  dead-pill rows).

---

## 2026-05-07 — v1.13.35: holistic-audit triage (section-scope, off-page bulk, sparkline degenerate, doc cleanup)
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.35`

### Context
Subagent ran a holistic audit across the v1.13.29 → v1.13.34
window. **No P0/P1 crash or corruption bugs** — the recent
sweeps had genuinely consolidated state. A handful of edge-case
findings worth shipping:

### Changes (P1)
- `app/web/api.py` `/api/items/{mt}/{id}/recovery-options`:
  - Now accepts an optional `?section_id=` query param. The
    locally-resolved detection's `local_files` lookup pins to
    that section when provided. Pre-fix a 4K-only adopt on a
    multi-section title incorrectly flipped the **standard**
    section's info card to RESOLVED VIA ADOPT — same v18-class
    cross-section bleed the schema redesign was meant to close.
    Falls back to the title-global lookup when section_id is
    omitted (legacy callers).
- `app/web/static/app.js`:
  - `hydrateRecoveryOptions(root, mt, tmdb, sectionId)` and the
    re-hydrate call after ACK FAILURE both forward the section
    so the client uses the scoped path.
  - **Bulk PUSH TO PLEX off-page warning**: `libraryState.selected`
    survives across pagination but `libraryState.items` only
    holds the visible page. The handler silently dropped off-
    page selections from the bulk operation. Confirm dialog
    now surfaces an "⚠ N selected rows are not on this page"
    line so the user knows. (Pre-existing issue with bulk
    download too; could fold a similar warning there in a
    future polish pass.)
  - **renderSyncPerformance**: hide the chart when no row has
    `wall_clock_seconds > 0`. A series of immediate-cancel
    runs would otherwise normalize to a flat-baseline render
    that read as "everything's stuck" when really there's no
    duration signal.

### Changes (P2)
- `app/core/db.py` `_migrate_v33_to_v34`: docstring rewritten.
  The `motif_unplaced_at` column it introduced was deprecated
  in v1.12.111 (replaced by `plex_theme_verified_ok`); the old
  docstring still claimed PURGE/UNMANAGE stamp the tombstone
  and the SRC SQL suppresses 'P' on it — neither is true any
  more. Future archaeology for phantom-P regressions now
  points at the right signal.
- `tests/test_canonical.py`: updated `_` → `-` expectations to
  match the v1.10.23 sanitize_for_filesystem change (Plex's
  own folder convention). 4 tests now pass (were silently
  failing on `pytest tests/test_canonical.py`).

### Audit findings deferred (defensive-only / cosmetic)
- `/api/sections/coverage` under-counts on fresh DB before
  resolve_theme_ids runs. Brief window; cosmetic.
- `daily_downloads` `DATE(created_at)` is forgiving today
  since `now_iso()` always emits offset; only a regression in
  the timestamp emitter would surface it.
- `/api/library/refresh` accepts arbitrary `fourk` truthy
  values (UI never sends strings).
- Sequential bulk PUSH triggers `loadLibrary` after the first
  request for large selections; UX could surprise but
  functionally correct.
- Composite-+P regex `srcCell.replace` is fragile on a future
  non-`link-badge` chip variant; gated by the existing
  `letter !== '-'` check so safe today.
- `_QUEUE_BURST_HW` / `_DOWNLOAD_PROGRESS` could grow
  unboundedly on a host where /api/progress is never polled.
  Not realistic.
- Phase B git-mirror failure in `_run_git_differential_upsert`
  doesn't cascade to remote (skip_upsert path); rare-but-real
  trade-off, worth a code comment in a future polish pass.

### How to verify
1. **Section-scoped resolved**: a multi-section title where
   only the 4K section has been adopted post-failure. Open
   INFO on the Standard row — should show "TRY THIS NEXT"
   (failure not resolved at this scope), not "✓ RESOLVED".
   4K row's INFO still reads "✓ RESOLVED VIA ADOPT" as before.
2. **Bulk PUSH off-page**: select rows on page 1, navigate
   to page 2 (selection persists), press PUSH TO PLEX. The
   confirm dialog now warns "N selected rows are not on
   this page".
3. **Sparkline empty-data**: fresh install, no syncs yet OR
   only 0-second cancels. // SYNC PERFORMANCE block hidden.
4. **Tests**: `pytest tests/test_canonical.py` passes 13/13
   (was 9/13 with 4 stale `_`-vs-`-` failures).

### Open threads
- Concurrent ops topbar visibility (still deferred — comes
  back if the user surfaces multiple-op visibility complaints).
- Per-kind library filter for the failure-breakdown deeplink
  (`&fk=` param parsed but not applied).

---

## 2026-05-07 — v1.13.36: P0 dashboard-insights SQL + per-variant lock + P filter tightening + post-PURGE optimistic P
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.36`

### Context
User testing v1.13.35 surfaced a P0 + several user-visible
correctness regressions:

### P0 — `/api/dashboard/insights` 500
**`sqlite3.OperationalError: no such column: wall_clock_seconds`**.
The `sync_runs` table has `started_at` + `finished_at` only —
v1.13.34's insights query referenced a column that doesn't exist.
Every dashboard load 500'd.

**Fix:** derive the wall clock via `(julianday(finished_at) -
julianday(started_at)) * 86400.0` aliased as `wall_clock_seconds`
so the response shape is unchanged.

### P1 — variant lock too aggressive (revert v1.13.30 tab-wide)
v1.13.30 widened `myTabBusy` to lock the SYNC PLEX button while
EITHER STANDARD or 4K of the current tab was enumerating. User
asked for the inverse: per-variant independence, since the two
target different Plex sections and their jobs are independent.

**Fix:** revert to `myTabBusy = enumActive[tab][variant]` (the
v1.13.23 behavior). The mid-flip DONE-flash issue v1.13.30 was
addressing is now handled entirely by the scope-aware
`sawBusyScope` flag (v1.13.24+), so the wider gate is no longer
needed.

### P1 — P filter expanded to nearly every themed row
v1.13.31's P-filter SQL was `pi.has_theme = 1 AND verified_ok` —
matched almost every motif-placed row because Plex sets
`has_theme=1` whenever its metadata reports any theme (including
the sidecar motif itself placed). v1.13.34 tightened the
**render** gate to `!plex_local_theme` but the filter SQL kept
the loose condition.

**Fix:** P filter SQL now adds `pi.local_theme_file = 0`,
matching the render gate. Pure-P rows match via the letter
check; composite T+P/U+P/A+P/M+P (rare, true cloud/embed) match
via this clause.

### P1 — post-PURGE bounces to '-' when Plex has a Pass cloud theme
PURGE handler stamps `has_theme=0, local_theme_file=0,
verified_ok=0` for every cleared rk to avoid phantom-P. Inline
HEAD probe re-elevates to has_theme=1 if Plex serves a separate
theme — but the v1.13.21 gate skips the verify entirely for
`rk_from_placement` rows (motif owned the file, Plex's cache is
unreliable right after delete). So a T row with a real Plex
Pass cloud theme would bounce to SRC='-' until the next
plex_enum reconciled. User: "show P right away if Plex has a
fallback."

**Fix:** snapshot pre-purge `(has_theme, local_theme_file)`
per rk. Rows that pre-PURGE were in the +P composite state
(`has_theme=1 AND local_theme_file=0` — Plex serves a theme
not backed by a local sidecar) now keep `has_theme=1` +
`verified_ok=NULL` (optimistic). SRC SQL's COALESCE renders P
immediately. plex_enum confirms or corrects within seconds.
Rows that were motif-only (the rest) get the pessimistic stamp
as before.

### Files
- `app/__init__.py`: `__version__` → `1.13.36`.
- `app/web/api.py`:
  - `/api/dashboard/insights`: `sync_rows` SQL derives
    `wall_clock_seconds` via julianday.
  - `/api/library` SRC pill `P` filter: adds
    `pi.local_theme_file = 0` clause.
  - `api_forget_item`: PURGE clear-stamp split between
    `rk_keep_p` (composite +P pre-state, optimistic P-keep)
    and `rk_zero` (motif-only, pessimistic stamp).
- `app/web/static/app.js`:
  - `myTabBusy` reverts to per-variant
    `enumActive[tab][variant]`.

### Open threads
- "Yellow dot DL appearing before status bar appears" — needs
  more info to reproduce (which dot, which row state, which
  topbar pill).
- T-row briefly shows yellow-dot then clears after plex_enum:
  this is the post-place transient before plex_enum stamps
  `local_theme_file=1`. Acceptable; an aggressive fix would
  require an inline HEAD verify per place which is too costly.

---

## 2026-05-07 — v1.13.37: optimistic topbar placeholder on row DOWNLOAD click
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.37`

### Context
User: "the yellow dot DL is appearing before the dl status bar
appears." The row's amber DL pill (awaiting-placement) updates
within ~0.5s of the click via libraryRapidPoll, but the topbar
mini-bar lags 2-5s waiting for the worker's idle-wait + first
op_progress write. Result: row signals "something happening"
instantly, topbar still says IDLE.

### Changes
- `app/__init__.py`: `__version__` → `1.13.37`.
- `app/web/static/app.js` `redownload()`: now fires
  `motifOps.setOptimisticPlaceholder('download_queue',
  '// QUEUING DOWNLOAD')` right after the API call returns.
  Same mechanism the dash SYNC + library SYNC PLEX clicks
  already use — paints an indeterminate-shimmer mini-bar
  immediately, gets replaced by the real download_queue card
  when the worker's first op_progress write lands.

### Open thread (deferred for user direction)
The +P composite SRC dot is fundamentally broken: motif's data
model can't detect "Plex serves a cloud theme alongside motif's
sidecar" because Plex hides cloud-availability when a sidecar
wins (`has_theme=1 AND local_theme_file=1` regardless). Every
v1.13.34 +P render is either a transient stale-state false
positive (post-place before plex_enum) or undetectable in steady
state. Three options proposed (drop entirely / definitive flag /
softer tooltip); awaiting user choice. The post-PURGE
optimistic-P fix from v1.13.36 stays useful regardless since it
runs at PURGE time on a real signal (pre-PURGE state snapshot).

### How to verify
1. Click DOWNLOAD on a row. Topbar mini-bar appears
   immediately ("// QUEUING DOWNLOAD" indeterminate shimmer)
   instead of waiting for the worker to start the job.
2. Once the worker picks up the download, the placeholder
   gets replaced by the real download_queue card with the
   per-section label and progress (yt-dlp's real %).

---

## 2026-05-08 — v1.13.38: persisted +P composite signal + REPROBE PLEX THEMES
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.38`

### Context
v1.13.37 left the +P composite SRC dot broken in steady state.
User push-back on dropping the dot: "I really like the T U A
with the yellow dot as it's useful to know when you could be
saving space by ditching the side car and letting plex take
over...this isn't a good thing since then say you see a T
yellow dot and sync plex and it goes away how do you know it
has a P available any more?"

The fundamental problem: once a sidecar exists at
`<media_folder>/theme.mp3`, Plex's section enum reports
`has_theme=1` regardless of whether the source is the sidecar
OR a Plex-side cloud / themerr-plex embed. The v1.13.34
heuristic `has_theme=1 AND local_theme_file=0` only fires when
the sidecar is absent — which is a transient pre-place state,
not the steady state the user wants to inspect.

### Design
The "are there ALSO independent Plex themes here?" question
**can** be answered, but only at two moments:
1. During plex_enum, *before* a sidecar is placed: `has_theme`
   is the truth.
2. By temporarily moving the sidecar out of the way and HEAD-
   probing `/library/metadata/{rk}/theme`.

Solution: add a persisted column `plex_items.plex_independent_theme`
(NULL = unknown, 1 = Plex serves a separate theme, 0 = sidecar
was the only source). Capture the answer at every moment we can:
- `plex_enum.py` upserts the COALESCE-preserve flag during the
  section walk (set when `local_theme_file=0`, preserve otherwise
  so we don't blow away a known-good observation when a sidecar
  hides the truth on the next pass).
- PURGE inline-verify writes the column from the HEAD probe
  (sidecar is definitively gone post-PURGE so `verify_theme_claim`
  is the source of truth).
- New admin action **REPROBE PLEX THEMES** backfills historical
  rows: temporarily renames each sidecar to
  `theme.mp3.motif-probing`, asks Plex to refresh, polls
  `verify_theme_claim` up to ~13s, restores the file. Exposed
  via `/api/admin/reprobe-plex-themes` and a button in
  Settings → PLEX panel.

### Changes
- **Schema v38 → v39** (`app/core/db.py`): adds
  `plex_independent_theme INTEGER` to `plex_items`. Idempotent
  ALTER TABLE migration; column also added to fresh-install
  DDL. Header comment documents the set/clear/preserve rules.
- **`app/core/plex_enum.py`**: computes
  `indep_observation = None if sidecar else has_theme` and
  `COALESCE(?, plex_independent_theme)`-merges into both UPSERT
  branches (URI-changed and URI-unchanged) plus the INSERT
  path. Preserve-when-unknown semantics so re-enumeration
  doesn't clobber a previously-observed truth.
- **`app/web/api.py` PURGE**: now snapshots
  `plex_independent_theme` into the pre-state, prefers the
  persisted flag in the optimistic-keep heuristic
  (`_had_plex_independent`, falls back to v1.13.36's
  `has_theme=1 AND local_theme_file=0`), and the inline HEAD
  verify branch updates the column to match its 200/404
  verdict.
- **`app/web/api.py` library SQL**: P-filter now reads
  `pi.plex_independent_theme = 1` instead of recomputing
  from `has_theme + local_theme_file`. Chip + filter agree
  row-for-row off a single source of truth.
- **`app/web/api.py` `_reprobe_plex_themes_run`** + endpoint
  `POST /api/admin/reprobe-plex-themes`: daemon-thread worker
  that walks
  `plex_independent_theme IS NULL AND has_theme=1 AND local_theme_file=1`,
  rate-limited (1s sleep between polls, ~13s timeout per row).
  Always restores the sidecar in `finally`; logs ERROR if a
  restore fails so a stranded `theme.mp3.motif-probing` is
  surfaced. Surfaces in the live-ops drawer via op_progress;
  cancellable via the existing
  `/api/progress/{op_id}/cancel`.
- **`app/web/static/app.js`**: render gate reads
  `it.plex_independent_theme === 1` instead of the v1.13.34
  `plex_has_theme && !plex_local_theme && verifiedOk` triplet.
  Drops the runtime composite computation entirely. New
  `bindReprobePlexThemes()` wires the Settings button with a
  confirm dialog explaining the file-rename behaviour.
- **`app/web/templates/settings.html`**: new "// REPROBE PLEX
  THEMES" block in the PLEX panel below LIBRARY SECTIONS,
  with the warning text inline.

### Open threads
- "?" tooltip on the dashboard section header reportedly shows
  no info — needs investigation.
- Settings → SCHEDULE tab has the cron / source / urls / auto-
  enum settings in mixed order with extra vertical space —
  user wants a reorganization. Both pending; will land in
  v1.13.39 to keep this ship focused on the +P data-model fix.

### How to verify
1. Pull, restart container — schema migrates to v39 (check
   logs for "schema v38→v39").
2. Settings → PLEX → click `// REPROBE PLEX THEMES`. Confirm
   dialog appears; live-ops drawer shows
   "// REPROBE_PLEX_THEMES" running with row counts.
3. After completion, motif-placed rows whose Plex section
   *also* has a cloud / embed theme show the small amber
   `+P` dot on their primary SRC chip; pure sidecar-only
   rows render the chip without the dot. Behaviour persists
   across plex_enum + post-PURGE without flicker.

---

## 2026-05-08 — v1.13.39: per-section coverage tooltips + Schedule reorganization
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.39`

### Context
User feedback after v1.13.38 ship:
1. "the ? over the section doesn't show any information" —
   hovering a row in the dashboard's // PER-SECTION COVERAGE
   table produced no per-cell hint. Cursor went to pointer but
   no `title=` tooltip explained what each number meant.
2. "Settings → Schedule has a lot of different settings in
   mixed order" — the single // SYNC SCHEDULE block stacked
   CRON, then THEMERRDB URL (rarely-changed), then SYNC SOURCE
   (the field that decides which URL even matters), then more
   URLs, then AUTO-SYNC PLEX. Reading top-to-bottom didn't
   make sense; vertical spacing was also generous.

### Changes
- **`app/web/static/app.js` `renderSectionCoverage`**: every
  cell in the per-section coverage row now carries a `title=`
  hint. Section + type cells share a consistent `sectionTip`
  ("Section ID: X · MOVIES STD · click to open this library
  tab"); TOTAL / THEMED / UNTHEMED / FAILURES / PENDING each
  carry their own context-appropriate tooltip including the
  zero-state ("No unacked theme failures in this section",
  "Every item has a theme", etc.). Whole row gets the section
  tip too as a fallback hover target.
- **`app/web/templates/settings.html`**: SCHEDULE tab split
  from one block into three blocks ordered by question being
  answered:
  1. **// SYNC TIMING** — CRON expression and AUTO-SYNC PLEX
     AFTER SYNC. The two timing-related controls live
     together; SAVE TIMING button.
  2. **// SYNC TRANSPORT** — SYNC SOURCE selector first
     (since it determines which URL matters), then the four
     transport URLs/branch each labeled with which source
     uses them ("used by REMOTE", "used by GIT"). Cache
     gauge stays under TRANSPORT. SAVE TRANSPORT + PROBE
     TRANSPORT buttons.
  3. **// PLACEMENT MODE** — unchanged content but tightened
     hint copy.
  Hint paragraphs trimmed (the original copy explained the
  same point twice in places); each block uses
  `form-grid-tight` for shorter vertical rhythm.
- **`app/web/static/app.css`**: `.form-grid-tight` modifier
  with `gap: 14px` (vs default 24px) and a tightened
  `.form-hint` line-height. Scoped via the modifier class so
  only the SCHEDULE blocks pick it up; PATHS / PLEX / DOWNLOADS
  panels keep their existing breathing room.
- **`app/__init__.py`**: `__version__` → `1.13.39`.

### How to verify
1. Pull, restart container.
2. Dashboard → hover any row in // PER-SECTION COVERAGE.
   Each cell shows a tooltip: section id + type on the
   text columns, an explanatory line on each numeric cell.
3. Settings → SCHEDULE tab. Three blocks render in order:
   // SYNC TIMING, // SYNC TRANSPORT, // PLACEMENT MODE.
   Vertical density visibly tighter than before; no fields
   lost from the prior layout. SAVE TIMING / SAVE TRANSPORT
   / SAVE PLACEMENT each persist their respective slice.

---

## 2026-05-08 — v1.13.40: REPROBE PLEX THEMES → non-destructive prefix-byte probe
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.40`

### Context
v1.13.38 shipped REPROBE with a rename loop:
`theme.mp3 → theme.mp3.motif-probing → ask Plex to refresh → HEAD
probe → restore`. User after testing on the prod box (10,296-item
Movies section): "any other solution the rename makes me very
nervous." Reasonable — touching every user-visible asset to
backfill a UI hint is risky even with a finally-block restore.

### Investigation
- Looked for a non-destructive multi-source theme list endpoint
  (`/library/metadata/{rk}/themes` plural, analogous to
  `/posters` and `/arts`). Confirmed via python-plexapi source:
  Plex exposes a single `Theme` resource per item, no collection
  class, no plural endpoint. Dead end.
- Settled on a prefix-byte comparison: GET the first 2 KB of
  `/library/metadata/{rk}/theme` via HTTP Range, read 2 KB from
  the local sidecar, compare bytes. Plex serves themes as-is
  (no transcoding), so byte-equality is exact and 2 KB of MP3
  header + ID3 is more than enough to distinguish two different
  audio files.

### Changes
- `app/core/plex.py` `PlexClient.fetch_theme_prefix(rk, n_bytes=2048)`:
  Range-GET helper that returns bytes on 200/206 and None on any
  error.
- `app/web/api.py` `_reprobe_plex_themes_run`: rewritten to use
  `concurrent.futures.ThreadPoolExecutor(max_workers=6)`. Each
  worker holds its own `PlexClient` (httpx isn't thread-safe for
  concurrent reuse). Per-row: read 2 KB from
  `<media_folder>/theme.mp3`, fetch 2 KB from Plex, compare. Match
  → set `plex_independent_theme=0`. Mismatch → set 1. Drops the
  rename, the .motif-probing suffix, the per-row Plex refresh
  calls, and the 12-attempt poll loop entirely. Removes the
  `import time` that was only there for `time.sleep`.
- `app/web/templates/settings.html` warning copy: replaces "this
  will briefly rename your theme.mp3 files" with the read-only
  description of the Range probe.
- `app/web/static/app.js` confirm dialog: same.

### Properties (rename → prefix-byte)
| | rename | prefix-byte |
|---|---|---|
| Touches user files | yes | no |
| Plex cache lag dominates | yes | no |
| Catastrophic failure mode | sidecar stranded | none |
| Bandwidth | nil | ~6 MB / 3,000 rows |
| Wall time | hours | ~5 minutes |
| Parallelism | 1 | 6 |

### How to verify
1. Upgrade. Settings → PLEX → // REPROBE PLEX THEMES.
2. Confirm dialog now says "Read-only — no files are renamed".
3. Live-ops drawer shows the probe completing in minutes,
   not hours.
4. Rows with cloud / themerr-plex themes start showing the
   amber +P dot on their primary SRC chip.

### Open threads (still pending)
- "?" tooltip on dashboard section header shows no info.
- Settings → SCHEDULE reorganization (partial in v1.13.39 but
  user feedback may want more).

---

## 2026-05-08 — v1.13.41: P0 fix — op_progress.kind CHECK widening
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.41`

### Context
v1.13.38 added the REPROBE PLEX THEMES daemon-thread worker that
calls `op_progress.start_progress(kind="reprobe_plex_themes")`,
but the schema's `op_progress.kind` CHECK was still
`('tdb_sync', 'plex_enum')`. Result: every REPROBE click died
in the worker thread with `sqlite3.IntegrityError: CHECK
constraint failed: kind IN ('tdb_sync', 'plex_enum')` before
the first row was probed. Live-ops drawer stayed empty because
no row was ever inserted. User: "I don't see any progress or
status in the Live Ops drawer after clicking."

I missed this in v1.13.38 — added the new kind without checking
the CHECK; my own pattern of "every meaningful behaviour change
ships a tag" should have included a schema-version bump
alongside the new kind.

### Changes
- **Schema v39 → v40** (`app/core/db.py`):
  `_migrate_v39_to_v40` rebuilds `op_progress` with the widened
  CHECK `kind IN ('tdb_sync', 'plex_enum', 'reprobe_plex_themes')`.
  SQLite can't ALTER a CHECK in place, so the migration creates
  a new table, copies rows, drops the old one, renames, and
  recreates the indexes. Idempotent — schema_version table
  records v40 once applied.
- `CURRENT_SCHEMA_VERSION = 40`; CREATE TABLE in the SCHEMA
  literal also widened so fresh installs land at v40 directly.
- `app/__init__.py`: `__version__` → `1.13.41`.

### How to verify
1. Pull, restart container — log shows
   `Migrating to schema v40 (op_progress.kind widened)`.
2. Settings → PLEX → // REPROBE PLEX THEMES → Confirm.
3. Live-ops drawer immediately shows
   `// REPROBE_PLEX_THEMES running` with the per-row counter
   advancing (~6/sec on the prefix-byte path).

---

## 2026-05-08 — v1.13.42: P0 fix — expose plex_independent_theme to /api/library
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.42`

### Context
v1.13.40 + v1.13.41 fixed the REPROBE worker so it actually runs
to completion (`+P=7, sidecar-only=61` on the user's 4K Movies
section). But the amber +P dot still didn't render in the row
list. Cause: the `/api/library` SELECT exposed
`plex_has_theme` and `plex_local_theme` (the v1.13.34 fields) but
never added `plex_independent_theme` (the v1.13.38 field). JS
reads `it.plex_independent_theme === 1` — which is always
`undefined === 1 → false` if the field isn't selected. So the
column was correctly populated by REPROBE but never reached the
client.

A miss in v1.13.38: I added the column + capture logic + render
gate but forgot the read-path SELECT. Coverage gap between
`_SRC_LETTER_SQL` (used by P-filter, doesn't need to expose the
column to JS — it's pure server-side filtering) and the row
SELECT (which DOES need to expose every field JS reads).

### Changes
- `app/web/api.py` `_library_main_query` SELECT: added
  `pi.plex_independent_theme` next to the existing
  `plex_theme_verified_ok` selection. dict(row) propagation
  carries it through to the JSON response without further
  changes.
- `app/web/api.py` synthetic-themerrdb fallback SELECT (the
  "TDB row not in Plex" branch): added
  `NULL AS plex_independent_theme` so the column is present
  on every row shape JS receives. Synthetic rows can't have
  +P (no plex_items row exists yet) but the field needs to
  exist so the shape is uniform.
- `app/__init__.py`: 1.13.42.

### How to verify
1. Pull, restart container.
2. Reload Movies tab. Rows REPROBE confirmed as +P
   (the `+P=7` from the v1.13.41 user log) now show the
   small amber dot in the corner of their primary SRC chip.
3. P filter (already selected in the user's screenshot) now
   matches both pure-P rows AND composite-+P rows (motif-
   placed rows where Plex also has its own theme).

---

## 2026-05-08 — v1.13.43: P-filter + REPROBE coverage + ops drawer label
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.43`

### Context
After v1.13.42 surfaced the +P dot on Movies (REPROBE found +P=7
on the user's 4K library), the user tested TV/Anime and reported:
1. P-filter on TV STANDARD shows 0 matches (and Anime same).
   4,113 unfiltered TV rows, none match P-filter.
2. REPROBE ran on only 68 rows total — much smaller than
   expected for a library of ~3,000 sidecar-bearing items
   across all sections.
3. Live-ops drawer renders the new op as `// reprobe_plex_themes`
   (raw kind key) instead of the `// REPROBE PLEX THEMES`
   convention used by `// PLEX SCAN`, `// THEMERRDB SYNC`.
4. Hover tooltips on `ENUMERATE` / `RECONCILE` (and on TDB sync
   timeline abbrevs) "don't show anything" — meaning the tooltip
   text duplicates the visible label so hovering doesn't reveal
   anything new.

### Changes

**1. P-filter regression** (`app/web/api.py`):
v1.13.42 reduced the P-filter clause to just
`pi.plex_independent_theme = 1`. That broke libraries where the
column was still NULL for many P-eligible rows — TV/Anime hadn't
been REPROBE'd, and even pure-P rows hadn't all been
re-enumerated since the v1.13.38 schema landed. Reverts to OR
semantics: match BOTH pure-P (primary letter='P' per
`_SRC_LETTER_SQL`) AND composite-+P
(`plex_independent_theme=1`). Pure-P rows match through the
letter check the way they always did pre-v1.13.42; the +P
addition lets composite rows show up in the same filter.

**2. REPROBE coverage** (`app/web/api.py`):
The REPROBE SQL joined to `placements` for the sidecar path —
silently restricting probes to motif-placed rows (T/U/A primary
SRC). Manual sidecars (M letter — files Plex sees that motif
didn't place) and any TV/Anime row whose placements row was
absent or stale never made it into the work set. That's why the
user saw 68 rows on a library where ~3,000+ sidecars exist.
Switched to `pi.folder_path` from the section enum — every
sidecar-bearing row Plex sees is reachable. The probe code reads
from `<folder_path>/theme.mp3` directly (Plex's view of the
filesystem matches motif's `/data` mount, so the path resolves).

**3. Live-ops drawer label** (`app/web/static/ops.js`):
Added `reprobe_plex_themes` entries to:
- `STAGE_TIMELINE`: single `probe` stage with descriptive
  `long:` text.
- `TONE_BY_KIND`: 'plex' tone (sits visually next to PLEX SCAN).
- `KIND_LABEL`: `'REPROBE PLEX THEMES'`.

**4. Tooltips on stage labels** (`app/web/static/ops.js`):
Added `long:` descriptions to `plex_enum` stages
(`enumerate` → "Walk every managed Plex section…",
`reconcile` → "Re-link motif rows to plex_items, HEAD-verify
ambiguous theme claims…"). Pre-fix the `title=` attribute on
the timeline-step DIV and the label SPAN fell back to
`s.long || s.label = s.label`, so hovering on `Enumerate`
showed `Enumerate` — looking like the tooltip wasn't working.

**5. Version**: 1.13.42 → 1.13.43.

### How to verify
1. P-filter: Movies/TV/Anime/4K all show non-zero P matches
   when there are P-eligible rows; the count includes both
   pure-P (no sidecar, Plex serves) and composite-+P
   (sidecar present, Plex also has its own).
2. REPROBE: total probed count grows substantially (covers
   manual sidecars + all sections, not just motif placements).
3. Live-ops drawer: REPROBE shows as `// REPROBE PLEX THEMES`
   matching `// PLEX SCAN` / `// THEMERRDB SYNC`.
4. Hovering on PLEX SCAN's `Enumerate`/`Reconcile` labels now
   reveals the long-form description.

---

## 2026-05-08 — v1.13.44: clearer queued label + earlier topbar + smart TRY NEXT
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.44`

### Context
Live-test feedback after v1.13.43 surfaced four issues:
1. Topbar text "DOWNLOADING THEMES — QUEUED" reads as a
   contradiction (downloading AND queued at once) for the brief
   moment the worker hasn't picked up the job yet.
2. Row-level amber DL pill could appear before the topbar
   status bar — v1.13.37's optimistic placeholder fired
   *after* the POST awaited, so on a slow round-trip
   libraryRapidPoll could beat it to the screen.
3. Hot-reload of theme.mp3 in Plex without a page refresh —
   user wants to be playing on Plex, motif drops a new theme,
   and hear it without reloading. **Punted**: Plex client
   audio cache is opaque from motif's side; no API to push
   "your cache is stale" to a connected client. Plex's own
   `/refresh` invalidates server-side metadata cache (motif
   already calls this post-place via the refresh queue) but
   the client still has to re-fetch on its own cadence. Best
   we can do is ensure motif's refresh fires immediately;
   the cross-process invalidation is a Plex limitation.
4. Failed-download info card should offer ADOPT (when a
   manual sidecar is at the Plex folder) or PURGE (when Plex
   has its own theme) as TRY NEXT options that also ack the
   failure in one click.

### Changes

**1. Status bar wording** (`app/core/progress.py`):
Replaced the queue-state suffix `f"{stage_label} — queued"`
with a queue-leading label per kind. download_queue now reads
"Theme download queued" instead of "Downloading themes —
queued"; place_queue → "Place into Plex queued"; etc. The
running-state phrasing ("Downloading themes …") is unchanged
so the moment the worker picks up a job, the action verb
returns. Plural-pending still appends "(N)" so a 5-row burst
reads "Theme download queued (5)".

**2. Optimistic placeholder fires before await**
(`app/web/static/app.js` redownload dispatcher):
Moved `motifOps.setOptimisticPlaceholder('download_queue',
'// QUEUING DOWNLOAD')` to fire BEFORE `await api('POST',
url)`. The topbar mini-bar now lights up in the same frame
the user clicks, regardless of round-trip latency. Best-
effort: the placeholder ages out via its 5s TTL if the API
call fails.

**3. Smart TRY NEXT — ADOPT / LET PLEX SERVE**
(`app/web/api.py` `recovery-options` + `app/web/static/app.js`):
- Server-side: in `api_recovery_options`, after locally_resolved
  detection, check the row's plex_items state for two new
  signals:
    `m_available` = `local_theme_file=1 AND placements row
    absent` (a sidecar exists at Plex but motif didn't put
    it there — adoptable).
    `p_available` = `plex_independent_theme=1` (Plex serves
    its own theme, per the v1.13.38 column).
  When the failure_kind is in
  `{video_removed, video_private, video_age_restricted,
  geo_blocked, unknown}` and the row isn't already acked,
  prepend two extra options at priority=0:
  - `ADOPT EXISTING THEME` (action `adopt-and-ack`, tone user)
    when m_available.
  - `LET PLEX SERVE` (action `purge-and-ack`, tone user)
    when p_available.
- Client-side: `bindInfoDialog`'s recovery-button dispatcher
  now handles the two new acts. `adopt-and-ack` → POST
  `/adopt-from-plex` (section-scoped) then POST
  `/clear-failure` then close + reload. `purge-and-ack` →
  POST `/forget` (section-scoped — the PURGE endpoint) then
  POST `/clear-failure` then close + reload.

**4. Version**: 1.13.43 → 1.13.44.

### How to verify
1. Click DOWNLOAD on a row with a slow Plex round-trip.
   Topbar mini-bar appears the same frame as the click,
   never lagging the row's amber DL pill.
2. While a download is queued (worker hasn't picked it up
   yet), the topbar reads "Theme download queued" — no
   "DOWNLOADING THEMES — QUEUED" contradiction.
3. Open the info card on a row with `failure_kind=video_removed`
   AND a manual sidecar at the Plex folder. The TRY THIS NEXT
   list now shows "ADOPT EXISTING THEME" at the top —
   clicking adopts + acks in one shot, dialog closes, row
   flips to A primary letter.
4. Same pattern on a row with `plex_independent_theme=1` —
   "LET PLEX SERVE" appears, clicking purges + acks.

### Open thread
Plex client-side theme cache invalidation: not addressable
from motif. Mentioned in the response so the user knows.

---

## 2026-05-08 — v1.13.45: topbar clarity (drop +N OPS, show download title, fix pending headline)
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.45`

### Context
Live-test feedback on v1.13.42 topbar/drawer screenshots:
- "+1 OPS" pill is confusing — clicking it opens the drawer
  but only shows the same downloading card (queue synthetics
  duplicate what's already visible).
- User wants to see the currently-downloading title in the
  topbar without clicking into the drawer.
- (Also surfaced while looking: pending queue ops render
  with headline "Done" because renderCard's isLive only
  considered running/cancelling.)

### Changes

**1. Drop the "+N OPS" overflow pill** (`app/web/static/ops.js`):
The pill counted concurrent ops (download_queue + place_queue
+ refresh_queue all in flight = "+2 OPS") but every extra was
already a card the drawer surfaces. Net: pill duplicated the
drawer without adding navigation. Hidden unconditionally;
keeping the DOM element so it can be repurposed later (e.g.,
flash counter for newly-started ops) without a template
change.

**2. Show currently-downloading title** (`app/core/progress.py`):
- `_get_progress_rows`: query running download jobs alongside
  `themes.title` so we can surface what's actually running.
  Sorted DESC by job id so the most-recently-started job is
  index 0 (typical single-worker case puts one item in the
  list).
- `_synthesize_queue_ops`: new `running_dl_titles` kwarg.
  When `jt == 'download'` and there's at least one running
  job + a title, the running-state stage_label becomes
  "Downloading: <title>" (single) or
  "Downloading: <title> +N more" (multi-running, rare).
  When there are also pending jobs the suffix becomes
  "(N queued)" appended to the title-bearing label.
- The queued-only and other-kind paths are unchanged
  ("Theme download queued", "Placing themes into Plex", etc).

**3. Pending queue ops headline fix** (`app/web/static/ops.js`):
`renderCard` treated only `running` / `cancelling` as live,
so a queue op in `pending` status (no running job yet, queue
not drained) rendered with `_doneHeadline()` → "Done".
Widened the live-status set to include `pending` so the
stage_label shows through. Bar/cancel button gating was
already pending-aware via the active-set filter; only the
headline branch needed the widening.

**4. Version**: 1.13.44 → 1.13.45.

### Punted

User mentioned step-style timeline for downloads (à la
plex_enum's ENUMERATE/RECONCILE). Downloads don't have natural
stages — yt-dlp's per-job progress fraction already feeds the
mini-bar's smooth fill via `detail.bar_pct`, and the next
queue position is conveyed by the "(N queued)" suffix. Adding
synthetic stages (e.g., "fetching | encoding | placing")
would just be decoration that obscures the real signal.
Leaving as-is unless the user asks again.

### How to verify
1. Click DOWNLOAD on a single row. Topbar mini-bar reads
   "Downloading: <Title of theme>" once the worker picks it up,
   not "Downloading themes — 1 running".
2. Multi-row burst with one running + N queued: topbar reads
   "Downloading: <head title> (N queued)".
3. The "+N OPS" pill at the right of the topbar no longer
   appears even when multiple queue synthetics are active.
4. While a place_queue or refresh_queue sits in pending state
   (worker hasn't picked it up), its drawer card shows the
   queue's actual stage_label, not "Done".

---

## 2026-05-08 — v1.13.46: bulk DOWNLOAD eligibility + "unchanged" breakdown
**Branch**: `claude/migrate-to-code-H70WJ`  **Tag**: `v1.13.46`

### Context
User screenshot shows the Movies page filtered to TDB ✗ (red,
dead URL) with two rows selected. The bulk action bar still
displays `// DOWNLOAD FROM TDB` even though re-downloading
either row is guaranteed to fail (URL is broken). The user
also wants the same gate for amber TDB ⚠ (cookies-blocked)
rows, and for mixed bulk selections wants the confirm dialog
to surface the failed rows as "unchanged" so they understand
the bulk action carves them out.

### Changes (`app/web/static/app.js`)

**1. Eligibility gate excludes dead + cookies states.**
The `hasTdbEligible` calc now requires
`computeTdbPill(it) ∉ {'dead', 'cookies'}`. Pure-failure
selections hide `// DOWNLOAD FROM TDB` and `// DOWNLOAD &
REPLACE FROM TDB` entirely. Mixed selections still show the
button — the failure rows are filtered out at click time.

**2. Click handler routes failure rows to skip counters.**
In the bulk-download click handler, before pushing to
`items[]` we check the row's TDB state. Dead → `skippedDead++`,
cookies → `skippedCookies++`. Both kinds are counted
separately so the dialog can call out the correct fix path.

**3. Confirm dialog "Unchanged:" section.**
After the existing `Breakdown:` and warn lines, the dialog
now appends:
```
Unchanged:
  X dead-TDB rows (URL broken — fix per-row via SET URL /
                   UPLOAD MP3 / ADOPT EXISTING THEME)
  Y cookies-blocked rows (drop a fresh cookies.txt then retry)
```
Only renders when at least one row in the selection was
filtered out for TDB-failure reasons.

**4. Refined empty-selection alert.**
When the entire selection is filtered out for TDB-failure
reasons (no `themed`-but-not-failed rows, no sidecars, no
no-TDB rows), the empty-selection alert now spells out the
specific failure mix instead of saying "every selected row
is a sidecar". Routes the user to the same per-row fixes.

**5. Version**: 1.13.45 → 1.13.46.

### How to verify
1. Filter Movies to `TDB ✗` only, select 2 rows. The bulk
   action bar shows `// SELECT ALL FILTERED`, `// CLEAR`,
   `// ACK SELECTED`, `// EXPORT CSV` — no `// DOWNLOAD FROM
   TDB`.
2. Filter Movies to `ALL`, select a mix of T rows + dead
   rows + cookies rows. Click `// DOWNLOAD FROM TDB`. The
   confirm dialog still shows the per-letter breakdown for
   the actionable rows AND a new `Unchanged:` block listing
   the failure rows with the appropriate fix path.
3. Click OK; the bulk POST only carries the actionable rows
   (the failure ones aren't enqueued).
