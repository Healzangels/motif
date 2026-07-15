# motif — context for Claude Code sessions

Single-tenant FastAPI + SQLite + yt-dlp + dulwich service that
automates Plex theme orchestration from ThemerrDB. Single developer
(the user), homelab scale, deployed via Docker on Unraid behind NPM
+ Authentik. CRT-themed UI on port 5309.

For deep-dive context — recurring bug classes, architecture pivots,
schema migration history, things-that-were-tried-and-removed —
read **`docs/PROJECT_HISTORY.md`** (a structured digest of every
tagged release v1.4.0 → current). When debugging, that file is the
first place to look for the WHY behind a piece of code.

**Before any UI change** — read **`docs/DESIGN_SYSTEM.md`**. the user's
v1.15.87 design-system rule: extract/follow the tokens (`:root` in
`app.css`) + primitive classes before writing UI, mirror an existing
sibling screen, and never invent class names or inline hardcoded
values. Recurring CSS gaps in the v1.15.66-87 cascade are what
prompted the rule.

## Stack

| Layer | Detail |
|---|---|
| API + UI | FastAPI on `:5309`; Jinja2 templates in `app/web/templates/` |
| DB | SQLite at `/config/motif.db`; current schema **v71** (`CURRENT_SCHEMA_VERSION` in `app/core/db.py`), forward-only migrations in `app/core/db.py` |
| Worker | APScheduler cron (sync) + custom job loop (downloads/place/refresh) |
| Sync transport | tiered: git (dulwich differential) → snapshot (database branch tarball) → remote (per-item HTTP) |
| Download | yt-dlp with `cookies.txt` from `/config` |
| Placement | hardlink-first via `os.link()`, fallback to `shutil.copy2()` if cross-FS |
| Auth | local bcrypt session cookie OR `X-Authentik-Username` forward-auth |
| Notifications | Apprise (in-process via `apprise` pkg) + optional external apprise-api URL — `app/core/notify.py`, config in `notifications:` block of `motif.yaml` |

Two-volume container layout: `/config` (appdata) + `/data` (mirrors
Plex's view of the filesystem so hardlinks work).

**Container user (v1.22.4, +UMASK v1.22.5):** motif runs as a non-root
user set by `PUID`/`PGID` env vars (default `99:100` = Unraid
nobody:users) with `UMASK` (default `022`; Unraid best practice `002` =
group-writable so a 99:100 stack shares `/data` without 777). The
`docker-entrypoint.sh` starts as root, applies `umask`, `usermod`s the
bundled `motif` account to PUID/PGID, `chown`s `/config` (never `/data`),
and `gosu`-drops. A legacy `--user X:Y` override still works and skips the
PUID path. **The uid MUST match whoever owns the `/data` media share** —
on Unraid's `shfs` (`default_permissions`) a uid mismatch silently
denies every write (the user's 1000-vs-99 saga: his `*arr` stack is
PUID 1000, motif's old hardcoded `--user 99:100` broke all writes).
`main._probe_writability` test-writes `config_dir`/`themes_dir` at boot
and logs a loud `WRITABILITY:` uid-vs-owner error if it can't.

## SRC letter axis (the row pill across /movies, /tv, /anime, /collections)

| Letter | Source kind | Notes |
|---|---|---|
| `T` | ThemerrDB-managed | upstream_source ∈ ('imdb','themoviedb') |
| `A` | Adopted | sidecar matched, motif owns the inode |
| `U` | User URL | manual youtube_url override in user_overrides |
| `M` | Manual sidecar | someone else put theme.mp3 there |
| `P` | Plex-served | themerr-plex embed / Plex Pass cloud |
| `–` | none / dropped | no theme, or `tdb_dropped_at` set |

Same axis renders three places — keep them aligned when changing:
- DB read path: `_SRC_LETTER_SQL` in `app/web/api.py`
- Client logic: `computeSrcLetter` in `app/web/static/app.js`
- Dashboard donut: `renderThemeSourcePie`

**v1.18.0 collections**: media_type='collection' rows use the
SAME `_SRC_LETTER_SQL` / `computeSrcLetter` definitions — no
collection-specific variant. Collections never produce A
(no folder to adopt-from) or M (no sidecar location), but the
T/U/P/– branches all fire the same way. The placement kind for
collection theme uploads is `plex_upload` (vs `hardlink` / `copy`
for movies/tv), with `media_folder=''` — both checks recognize
empty string as "placed."

**v1.18.0 placement-kind alignment**: the predicate "does motif
own a placement on this row?" appears at FOUR sites in `app.js`
and one in `api.py` SQL. They must all share the same shape:
`!!it.media_folder || it.placement_kind === 'plex_upload'` (the
v1.18.0 widening added plex_upload as a third kind alongside
hardlink/copy, with `media_folder=''` as the sentinel). When
this list grows, audit every existing site.

| Site | File:line | Purpose |
|---|---|---|
| `computeSrcLetter` | `app.js:~9344` | SRC pill T/U/A vs P/– classification |
| `renderLibraryRow` inline-SRC | `app.js:~9548` | row table cell render (v1.18.24; plex_cloud→P branch v1.21.8) |
| `updateLibrarySelectionUi` selection bucket | `app.js:~11653` | themed-vs-not counts for bulk-bar (v1.18.24) |
| `isPlexAgentRow` | `app.js:~15487` | "Plex already supplying" confirm prompt gate (v1.18.75) |
| Bulk PUSH predicates (3 sites) | `app.js:~11810` / `~11905` / `~11959` | pushableCount + pushCount + bulk-PUSH click handler (v1.19.38 fix) |
| Bulk LPS M-sidecar gate | `app.js:~11841` | excludes M sidecars from bulk LET PLEX SERVE; must mirror the lpsOnlyCount bucket (v1.22.80 fix — bare `!media_folder` skipped plex_upload rows the bucket counted) |
| SRC SQL | `api.py:_SRC_LETTER_SQL` | DB-side equivalent — must agree with JS |

The forgetting cost is **silent UX wrong-classification**:
v1.18.0 → v1.18.24 the inline render lagged computeSrcLetter
(plex_upload rows showed SRC=– / blank); v1.18.0 → v1.18.75 the
isPlexAgentRow predicate falsely classified plex_upload as
P-agent and fired a "are you sure" prompt on every action; the
v1.19.38 audit caught the three bulk-PUSH predicates wrongly
counting plex_upload as "not yet placed" because their bare
`!it.media_folder` check evaluated `!''` as TRUE. Pin each new
site with a mirror-drift guard test (see
`test_v1_18_75_*.py::test_compute_src_letter_placed_logic_unchanged`
for the template + `test_v1_19_38_src_axis_sixth_site_drift.py`
for the lint that walks every `awaitingApproval` predicate).
**Note on line numbers**: kept approximate (~Nxx) to survive
minor edits. The mirror-drift tests use function-name anchors,
not line numbers — they don't go stale.

## Recurring bug classes (read PROJECT_HISTORY for full detail)

1. **Phantom P after PURGE** — Plex's metadata cache returns 200 to
   `/library/metadata/{rk}/theme` for several seconds after motif
   unlinks the file. Inline HEAD verify must skip rks where motif
   owned the placement (`rk_from_placement` set in `api.py`).
2. **Edition-sibling theme propagation** — `theme=` XML attribute
   propagates between standard/4K editions. Per-section scoping is
   essential; `local_files.section_id` is part of the PK (since v18).
3. **UNIQUE conflicts on orphan promotion** — synthetic-tmdb rows
   getting promoted into a target ID that has stale children. Pattern:
   pre-delete child rows at target then `PRAGMA defer_foreign_keys=ON`
   inside the txn.
4. **innerHTML flicker on poll** — naive tbody.innerHTML rewrites
   blow scroll position. Pattern: `tbody.dataset.lastHash` skip.
5. **Button text race** — don't swap `button.textContent` mid-flight.
   Use adjacent pills (DL/PL) for state. Optimistic placeholder via
   `motifOps.setOptimisticPlaceholder` covers the click→busy gap.
6. **Race between syncWatcher and refreshTopbarStatus** — when both
   own a button's lifecycle. Guard the unlock with `!syncWatcher`.
7. **/api/stats 1s TTL cache** — post-action refreshes need a
   `setTimeout(..., 1100)` to land past the cache.
8. **`database is locked`** — retry only that string in
   `OperationalError`; other OperationalErrors propagate (schema bugs).
9. **Silent-defensive-catch** — `try/except` that absorbs a failure
   mode without a log breadcrumb is indistinguishable from a
   successful no-op. State drift accumulates and surfaces weeks
   later as "the deploy looked fine but nothing happened." Every
   defensive `except` needs a log line + a functional fallback;
   bare `except: pass` is a bug. Instances: v1.14.52 H5
   (`_do_refresh` missing-rk fake-✓-DONE), v1.15.0
   (`PlexClient.close` swallow), v1.15.34 ×3 (`_topbar_ssr_state`,
   mirror-compaction `rmtree(ignore_errors=True)`,
   `enumerate_section_items` `[]`-on-failure), v1.15.117
   (`placement._safe_link_or_copy` pre-clean unlink), v1.17.9 ×4
   (config_file chmod, main.shutdown scheduler, api cron import,
   plex.get_item_paths JSON), v1.17.10 (`_apply_partial_config`
   closed-set drop on `notifications.events`), v1.17.13
   (`loadConfigIntoForms` silent-data-loss path — failed GET +
   subsequent SAVE overwrites real config with blanks),
   **v1.21.38–42 fresh-eyes audit batch** (H1 sync.py drop-detection
   ran after a partial fetch → transient timeouts/swallowed index pages
   mis-stamped live themes as TDB-dropped, now gated on stats.errors==0;
   H2 UNPLACE inline-verify called bool `item_has_theme` as a tristate →
   a transient Plex error zeroed has_theme, now uses `verify_theme_claim`;
   H3 bulk-let-plex-serve acquired the op slot before validating → a 400
   leaked a permanent PENDING lockout, now validates first like
   bulk-probe-tdb v1.18.96; M1 `placement_error` I/O fail was logged as an
   intentional "Skipped placement" + marked DONE → now WARNING +
   `_mark_failed_terminal` lights the FAIL dot; M2 `stat_theme_sidecar('')`
   returned False not None → an empty folder_path stomped an M-row's
   local_theme_file, now returns None into the v1.11.67 preservation).
   **Hot-path
   sub-pattern (v1.17.11)**: if the swallow site fires once per
   row/request, per-occurrence `log.warning` drowns the log. Use
   a module-level `_FOO_WARNED: bool = False` flag — first
   occurrence logs at warn (operator sees it at boot), subsequent
   drop to log.debug (breadcrumb stays). Flag resets only on
   process restart, which matches the cadence at which a deploy
   fix can land. Applied: sync.py:416 normalize_title, sync.py:1558
   _GitMirror.read_json, auth.py:102 verify_password, auth.py:287
   _verify_token. **Contract-drift sub-pattern (v1.17.10 /
   v1.17.17)**: a defensive patch from an earlier tag becomes a
   bug when a later change moves the contract under it. v1.17.10
   (closed-set filter dropping new event keys for 6 tags),
   v1.17.17 (`revert_redundant` SQL suppressing RESTORE because
   kind classification was wrong for orphans). Flag defensive
   patches inline with "DEFENSIVE PATCH — re-evaluate if X" so
   future debugging knows where to look. **Two-bug interaction
   sub-pattern (v1.17.17)**: when a UX symptom is "everything
   wrong at once," look for two bugs hiding each other before
   committing to a single-cause story. **Cold-path-needs-MORE-
   logging sub-pattern (v1.18.5/v1.18.7)**: recovery code,
   one-shot migrations, and other rarely-fired code paths need
   MORE explicit logging than the happy path, not less. v1.18.5's
   `_detect_loss_pattern` returned False silently when one stray
   `local_files` row existed → operator pulled the new build,
   restarted, saw no change, no log line, total black box.
   v1.18.7 added explicit log lines for every detection branch
   so future "why isn't recovery firing" debug has signal. Rule:
   any recovery/migration code path that early-returns must log
   WHY at INFO before the return — even on the happy "nothing
   to do" path. **FK cascade during table rebuild sub-pattern
   (v1.18.0/v1.18.5)**: SQLite's `PRAGMA defer_foreign_keys = ON`
   ONLY defers FK constraint VIOLATION CHECKS to COMMIT time.
   It does NOT defer cascading ACTIONS (`ON DELETE CASCADE`,
   `ON DELETE SET NULL`). When you `DROP TABLE parent`, child
   tables' cascading actions fire IMMEDIATELY regardless of
   `defer_foreign_keys`. The canonical table-rebuild pattern is
   `PRAGMA foreign_keys = OFF` for the rebuild duration (the
   sqlite.org/lang_altertable.html § "Making Other Kinds Of
   Table Schema Changes" recipe), followed by
   `PRAGMA foreign_key_check` to surface orphaned refs, then
   `PRAGMA foreign_keys = ON` to restore enforcement. v1.18.0's
   `_widen_check_constraint` used the wrong pragma and caused
   catastrophic data loss across every install: `themes` got
   rebuilt FIRST → DROP fired `ON DELETE CASCADE` on
   `local_files` + `placements` (full wipe) + `ON DELETE SET
   NULL` on `plex_items.theme_id` (every row nulled). The
   on-disk theme.mp3 files survived; motif's tracking didn't.
   See `app/core/recovery_v55.py` for the recovery walker that
   rebuilds tracking from on-disk state. **Amplifier-sweep
   sub-pattern (v1.18.10)**: defensive `DELETE WHERE NOT EXISTS
   (...)` sweeps that "clean up stale" data can amplify damage
   from earlier bugs. v1.18.0 wiped `local_files` + `placements`
   directly; the next sync's v1.12.60 orphan-sweep then ran
   against the broken state, found 98 `user_overrides` rows
   whose presence-EXISTS checks all failed, and DELETEd them
   too. Pattern-fix: defensive sweeps should fail-safe under
   suspected broken state (e.g., abort if the would-delete
   count exceeds X% of the table). When a sweep DID run against
   broken state, recovery often lives in the `events` audit-log
   table — v1.18.10's `maybe_recover_lost_user_overrides`
   parses historical `"Manual URL set by admin"` events to
   reconstruct the wiped `user_overrides` data. **Phantom-fix
   sub-pattern (v1.18.81)**: a test that pins JS source-text
   shape (`assert "X === 'backup'" in src`) without exercising
   the actual data pipe through that shape is a phantom guard.
   v1.18.77 added the `user_overrides.intent` column + JS
   branches keyed on `data.override.intent === 'backup'` to
   render the BACKUP banner + // PROMOTE TO ACTIVE button.
   v1.18.78's tests asserted the conditional appeared in
   app.js — it did. But `api_recovery_options` (the endpoint
   feeding the conditional) never returned `override` in its
   response, so the frontend read `undefined.intent` for THREE
   TAGS without anyone noticing. v1.18.81 added behavioral
   tests (real TestClient + DB inserts → assert on the API
   response shape) as the discriminator. Rule: when adding a
   feature that depends on backend → frontend data flow, the
   test MUST exercise the endpoint, not just the conditional.
   **One-conceptual-surface-multiple-writers sub-pattern
   (v1.18.83)**: When a state surface (`user_overrides`,
   `local_files`, etc.) has multiple write paths (per-row +
   bulk + recovery + sync), recovery walkers MUST catalogue
   all writers and recover from each. v1.18.10 covered the
   per-row SET URL path (events table, `"Manual URL set by
   admin:%"` message). The bulk CSV-import endpoint wrote
   ONLY to `audit_events` with `{"source":"import"}` — never
   to events — so the v1.18.0/.10 cascade left bulk-imported
   URLs unrecoverable. v1.18.83 added the symmetric walker +
   the forward-fix log_event so future bulk imports are
   recoverable by either walker. Pattern-fix: when adding a
   new write path to a recoverable surface, audit recovery
   walkers for symmetric coverage. **Storage-scope-mismatch
   sub-pattern (v1.18.84)**: localStorage on state that
   should be per-tab (or per-session) leaks across tab
   boundaries. Symptom: opening a new tab inherits state
   from another tab unexpectedly. Cause: localStorage as a
   default when sessionStorage was the right scope.
   Instances: v1.15.52 (search query `q` cross-session leak
   → fixed to sessionStorage), v1.18.84 (filter snapshot
   same bug, three years later, same fix). Pattern-fix:
   when persistent state should survive within-tab
   navigation but NOT cross-tab visits, use sessionStorage.
10. **Browser tab-throttle / poll-cap** — long-running `setInterval`
    polls with tight ceilings die in inactive tabs under Chromium's
    ~1/min throttle (the first throttled tick already exceeds a 60s
    ceiling → `clearInterval` fires → poll dead until user manual
    refresh). Ceilings need ~1 tick/min throttling headroom +
    `visibilitychange` listener to re-arm on tab return. Canonical
    fix: v1.16.7 (rapid-poll ceiling 60s → 300s + visibilitychange
    re-entry).

11. **Plex theme API plural-vs-singular semantics** — the Plex
    HTTP API has TWO theme endpoints with completely different
    semantics, and motif spent v1.18.0 → v1.18.36 silently
    hitting the wrong one for deletes. The convention:
    - `/library/metadata/{rk}/themes` (**plural**) — the theme
      collection. GET lists entries, POST uploads. **No DELETE
      handler** (returns 404). Each entry has its own
      `ratingKey` of form `upload://themes/<sha1>` or
      `metadata://themes/<sha1>`. Plex content-dedupes uploads
      by SHA-1 — same bytes re-uploaded re-uses the existing
      entry and auto-selects it.
    - `/library/metadata/{rk}/theme` (**singular**) — the
      currently-serving theme association. DELETE clears the
      `selected: true` flag without removing entries from the
      collection. Per OpenAPI: "This operation will also lock
      the field" but motif's subsequent POST to plural /themes
      auto-overrides the lock (confirmed via v1.18.33 probe).
    - **No native "select existing entry" API.** POST/PUT
      singular `/theme?url=<entry-rk>` 404s/500s (the `?url=`
      param is for uploading from a REMOTE url, not selecting
      an existing internal entry). The only way to make a
      specific entry active is the **re-upload trick** (v1.18.35
      probe / v1.18.36 production): GET the entry's bytes from
      `/library/metadata/{rk}/file?url=<entry-rk>`, POST those
      bytes to `/themes`. Content-dedup → existing entry
      becomes selected. Bandwidth: ~1MB round-trip per call.
    - **Latent-bug class**: any motif code touching Plex theme
      DELETE should be at SINGULAR `/theme`, not plural
      `/themes`. The v1.18.0 → v1.18.36 bug returned 404 every
      time, silently — `delete_collection_theme` returned False
      with no upstream check warning that "this should be
      succeeding most of the time." Latent-bug subclass of
      class 9: best-effort returns that nothing asserts on
      hide failures indefinitely.
    - **Investigation cadence pattern (v1.18.31-40)**: 6 read-
      mostly probe tags before any production change. Each
      probe characterized one piece of Plex's behavior (GET
      themes shape, DELETE URL shape, POST/PUT select feasibility,
      re-upload trick). Production fix landed in v1.18.36 only
      after the design space was empirically mapped. Worth as a
      template for any "we're touching someone else's API and
      don't know what works" investigation. See PROJECT_HISTORY §
      19 for the full series digest.

12. **Event-loop block** — a synchronous blocking call (PlexClient
    HTTP round-trip, `probe_youtube_url`, `requests`/`subprocess`/
    `time.sleep`) directly inside an `async def` FastAPI handler
    freezes the SINGLE asyncio event loop, so every concurrent
    request (UI polling included) hangs for the duration. Symptom
    reads as "this feature is slow and the whole app gets slower
    with it." Fix pattern: wrap the blocking sequence in a sync
    helper + `await run_in_threadpool(_run)`; for multi-item work
    add a ThreadPoolExecutor + per-worker PlexClient pool (httpx
    clients aren't thread-safe for concurrent reuse — mirror
    `_reprobe_plex_themes_run`). Instances: v1.21.20 (orphan scan),
    v1.22.31 (?rk= probe), v1.22.42 ×3 (promote/unplace/teardown),
    v1.22.57 (title-fragment probe, serial AND blocking),
    v1.22.58 ×5 (3 admin probes + 2 inline verify_theme_claim).
    **Standing guard**: `tests/test_v1_22_58_async_no_blocking_calls.py`
    AST-lints every `async def` in api.py (the only module with
    async handlers) for blocking calls in the DIRECT body —
    nested `def`s are exempt (they're the offload targets). When
    adding a blocking method to PlexClient, add its name to the
    lint's `PLEX_BLOCKING_METHODS` set.

## File-path conventions

- **`local_files.file_path` is RELATIVE to `settings.themes_dir`.**
  The canonical absolute path is `themes_dir / file_path` (worker.py:
  1812, 2347). Any code reading `file_path` for filesystem ops
  (stat, hash, open, unlink) must do the join — v1.18.36's LPS
  motif-hash code missed this and silently failed on every
  plex_upload row until v1.18.39 caught + fixed it via the user's
  orphan-scan diagnostic. The schema doesn't enforce or document
  this convention; reader discipline only.
- **`placements.media_folder`** stores the absolute path to the
  Plex media folder (where `theme.mp3` lives for sidecar
  placements). Empty string `''` is the plex_upload sentinel
  (no folder — theme lives in Plex's metadata store).

## Coding conventions

- **Inline `# vX.Y.Z:` markers** on load-bearing lines, explaining
  WHY the change had to happen. They're searchable archaeology — the
  PROJECT_HISTORY entry tells the full story, the marker is the breadcrumb.
- **Comments lean on WHY**, reference prior bugs/incidents. Skip
  comments that explain WHAT (the code already does that).
- **Single-line comments only.** No multi-line block comments, no
  docstring-style narration. One short line, max.
- **Never delete a `# vX.Y.Z:` marker** unless deleting the line it
  guards.
- **No "fallback" branches or feature flags** for behavior the user
  didn't ask for. No backwards-compat shims for removed features.
- **No premature abstractions.** Three duplicated lines is better
  than a helper that hides intent.

## UI conventions

- All button labels and section headers prefixed with `// `
  (e.g., `// SYNC THEMERRDB`, `// SOURCE BREAKDOWN`).
- Mono font (VT323), green-on-black palette.
- `// SYNCING…` is the canonical busy label (single-word). No
  multi-stage label transitions — they raced and got reverted.
- `✓ DONE` flash for 1.5s on busy → idle transition (dash SYNC,
  library SYNC PLEX). Pattern: `sawBusy` flag.
- Optimistic placeholders bridge click → busy gap. Section labels
  in the placeholder text where applicable (`// SYNCING 4K MOVIES`,
  not generic `// SYNCING PLEX`).

## Row-refresh contract (v1.18.52)

Library row chips (SRC / DL / PL / LINK / STATUS axes) MUST update
promptly when a backend op mutates the underlying data — not lag
the 30s background poll. The contract that enforces this:

- **Per-row jobs**: only `download` + `place` set the per-row
  `job_in_flight` marker (the `/api/library` subquery matches
  `job_type IN ('download','place')`); the `libraryRapidPoll`
  lifecycle handles those via the v1.15.6 consecutive-empty-debounce.
  The OTHER place-pool / scan jobs (`scan`, `refresh`, `relink`,
  `adopt`) are covered instead through the global `perJobBusy`
  union below — they don't get the per-row marker. (Pre-v1.20.53
  `relink`/`adopt` were in NEITHER, so a Storage relink / Scan
  adopt sweep left chips stale until the 30s tick.)
- **Global ops** that mutate row state without per-row markers go
  through the `anyMutatingOpActive` signal in
  `refreshTopbarStatus`. The signal unions:
  - `themerrdb_sync_in_flight` (tdb_sync — SRC, +P, ATTN)
  - `plex_enum_in_flight` (plex_items, placements — PL, LINK)
  - `op_progress_running` (catch-all: bulk_probe_tdb,
    bulk_lps, tvdb_bridge, reprobe_plex_themes — all kinds
    accepted by the `op_progress.kind` CHECK in db.py)
  - `download_in_flight + place_in_flight + scan_in_flight +
    refresh_in_flight + relink_in_flight + adopt_in_flight`
    (v1.18.93 + v1.20.53 — per-row jobs queued by
    background workers, not just user clicks. Pre-v1.18.93 the
    contract assumed per-row jobs were covered by their click
    handlers calling `libraryRapidPoll()` inline; that left
    background-worker-initiated jobs — TDB queue auto-pick,
    post-place refresh chain — without any trigger to fire
    rapid-poll until the next 30s background tick).

  False→true transition kicks `libraryRapidPoll`; true→false fires
  a one-shot `loadLibrary` so the post-op state lands without
  waiting for the next background tick.
- **Adding a new row-mutating op kind**: extend the
  `op_progress.kind` CHECK in `db.py` and the JS-side
  `KIND_LABEL` / `TONE_BY_KIND` / `OP_MINI_PRIORITY` maps —
  that's it. The catch-all `op_progress_running` count in
  `/api/stats` automatically picks up the new kind. The
  contract guard (`tests/test_v1_18_52_row_refresh_contract.py`)
  walks the CHECK list at test time and fails loud if a new
  kind needs explicit exclusion (i.e. it doesn't actually
  mutate row state).

The contract motivation: the user's collections-tab repro
(v1.18.51) showed that plex_enum cascade mutated rows but the
UI didn't refresh until 30s later because there was no per-row
signal. v1.18.52 widened the fix to every backend op that can
mutate visible row state — uniformly.

## Status bar consistency (v1.18.53)

Every op kind that surfaces in the topbar mini-bar or ops
drawer MUST have entries in all three JS-side maps in
`ops.js`:

  - `KIND_LABEL` — drawer card title + mini-bar fallback
    label. Without it the card renders the raw snake_case
    kind (e.g. `// bulk_lps`).
  - `TONE_BY_KIND` — color identity (tdb / plex / warn).
    Without it the card has no tone class and looks
    generic.
  - `OP_MINI_PRIORITY` — picker priority when multiple
    ops contend for the single mini-bar slot. Without it
    the kind inherits `OP_MINI_PRIORITY_FALLBACK = 99`
    (lowest), only winning when nothing else is running.
    Explicit entries document intent and prevent the
    "phantom" behavior where a kind silently never
    appears in the contended slot.

Two distinct sources of kinds:

  1. **Real op_progress kinds** (`db.py` CHECK list):
     `tdb_sync`, `plex_enum`, `reprobe_plex_themes`,
     `bulk_probe_tdb`, `bulk_lps`, `tvdb_bridge`.
  2. **Synthesized queue/pending kinds** emitted by
     `app/core/progress.py`:
     `tdb_sync_pending`, `plex_enum_pending`,
     `download_queue`, `place_queue`, `scan_queue`,
     `refresh_queue`, `relink_queue`, `adopt_queue`.

The audit guard (`tests/test_v1_18_53_status_bar_consistency.py`)
enumerates both sources at test time and asserts every kind
has all three map entries. A future kind landing on either
side fails the test until the JS maps catch up — same
contract-drift sub-pattern from v1.17.10.

Poll cadence (ops.js `poll()`):
  - 10s when no ops are running or pending
  - 1s when ANY op is running or pending (covers all kinds)
  - 1s while the drawer is open

The widened cadence + the row-refresh contract above
together guarantee row chips update within a poll cycle
(~2s for libraryRapidPoll) of any backend mutation.

## Cloud-themes-backup pipe (v1.19.41–43)

Plex Pass cloud themes are served only while Plex Pass is
active. Losing Plex Pass (or having Plex's cloud catalog drop
the entry, or the v1.18.90 reaper path where Plex removes
and re-adds the item with a new rating_key) kills every
P-row depending on a `metadata://themes/<sha1>` entry —
instantly, silently, with no recovery if motif never staged
a backup.

The three-tag arc landed the full pipe:

- **v1.19.41**: four-way notification tier split in the
  v1.18.90 reaper (`backup_ready` / `sidecar_available` /
  `other_fallback` / `no_fallback`). Pre-tag a single
  `has_fallback` boolean swallowed the signal whenever ANY
  fallback existed. New tier classifier in `plex_enum.py`
  + body formatters in `notify_content.py`. Test-trigger
  endpoint `POST /api/admin/test-trigger-theme-lost` lets
  the operator synthesize any dispatch path against any
  real row without waiting for genuine loss.
- **v1.19.42**: schema v57→v58 widens `local_files.source_kind`
  CHECK to accept `'plex_cloud'`. New module
  `app/core/cloud_theme_backup.py` walks `plex_items.
  has_theme=1` P-rows, classifies via Plex's `/themes`
  endpoint (C1 = single metadata:// entry, no upload
  sibling), downloads bytes via the v1.18.36 re-upload
  path, INSERTs `local_files` with full v1.19.x writer
  contract. Two admin endpoints: dry-run + run.
- **v1.19.43**: UI surface. New B badge (lemon, distinct
  from BK) in the LINK column for `source_kind='plex_cloud'`
  rows. `// B` filter chip + `link_pills=b` SQL.
  `// BACKUP CLOUD THEMES` bulk-bar button. `// BACKUP
  THIS THEME` per-row SOURCE-menu action. v1.19.39 PROMOTE
  TO ACTIVE tooltip extended with a third variant
  describing the v1.18.36 re-upload round-trip for
  `source_kind='plex_cloud'` synthetic overrides.

Writer contract (load-bearing): every plex_cloud row stamps
`last_place_attempt_reason='backup_only'` so the row reuses
the v1.19.21 BK pipe end-to-end — retry-sweep skip,
v1.19.35 PROMOTE TO ACTIVE BK-no-override branch,
v1.19.41 `theme_lost_backup_ready` notification path. The
B badge + filter chip add a SURFACE distinction; plumbing
is shared with BK.

Pre-flight (2026-05-26, n=16 stratified probe against
the user's prod DB): 50% C1 overall, anime 100% C1, ~1,940
C1 rows expected across 3,883 candidate P-rows (~4.2 GB).
The anime cohort is the highest-ROI target — TDB-less
rows where cloud-backup is the only viable recovery path.

## Commit + release conventions

- Subject: `vX.Y.Z: short summary` under 70 chars.
- Body: section headings + bulleted file-level change notes,
  each note explains WHY (often referencing prior bug or feedback).
- Trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- Every meaningful behavior change ships a tag. GitHub Actions
  builds + pushes the Docker image to Docker Hub on `v*.*.*`.
- Schema migrations: forward-only, idempotent. Bump
  `CURRENT_SCHEMA_VERSION` and add `_upgrade_to_vN` block in `db.py`.
- **Bump `app/__init__.py` `__version__` BEFORE creating each git
  tag.** It drives the topbar brand display (e.g. `MOTIF v1.13.79`)
  and the GitHub release-check comparison. Forgetting this leaves
  the UI showing the previous version even though the deployed
  image is newer. Pre-v1.13.79 the constant silently drifted
  from v1.13.73 through v1.13.78 — easy mistake to repeat.
- **Quarterly: bump the `yt-dlp` floor in `requirements.txt`** to
  current latest stable. yt-dlp ships frequent updates to stay
  ahead of YouTube's anti-bot moves; a stale floor masks the
  "old extractor" failure mode behind the looser `>=` resolution.
  Verify the running version on a deployed container via the
  startup log line `yt_dlp = X.Y.Z` (added v1.15.16). Last
  bumped: 2026.3.17 (v1.15.16); re-verified latest 2026-06-06
  (v1.22.24) — still the most recent stable, no newer release.
- **Quarterly: bump the `apprise` floor in `requirements.txt`**
  to current latest stable. Apprise ships service-specific
  transport updates as new notification services are added and
  URL schemas evolve; a stale floor masks "service unavailable"
  errors behind a generic dispatch failure. Last bumped: 1.11.0
  (v1.22.24).

## Deploy + branch state

- `nightly` is the active dev branch — all rapid-development commits
  land here. **Default branch on github.**
- `release` holds the most recent shipped tag. Fast-forward `release`
  to a `nightly` SHA only after that SHA has been tag-cut and
  validated. `release` should always equal a `vX.Y.Z` tag.
- Tags (`vX.Y.Z`) are the unit of ship. GitHub Actions builds +
  pushes the Docker image to Docker Hub on `v*.*.*`.
- Image tags: `healzangels/motif:vX.Y.Z` (exact) + a rolling
  `:nightly` pointer to the newest build. **No `:latest`** — it was
  removed at 0.50.0; deployments track `:nightly`.
- Production deployment: Unraid box, behind NPM + Authentik forward-auth.

## Things to NEVER do

- `git push --force` to `release` or `nightly` (or any tag-pointed ref).
- Run destructive git ops (reset --hard, clean -fdx, branch -D)
  without explicit ask.
- Add backwards-compat code paths for removed features.
- Write multi-paragraph docstrings or block comments.
- Invent abstractions for hypothetical reuse.
- Commit secrets (Plex token, GitHub PATs, cookies.txt). The events
  log scrubber (`app/core/events.py`) redacts: (a) `detail` dict VALUES
  whose KEY contains `token|secret|password|cookie|auth|api_key|bearer`
  etc., (b) URL userinfo (`://user:pass@host` → `://***@host`) in any
  string, and (c) sensitive URL query params (`?token=`/`?X-Plex-Token=`/
  `?api_key=` → `=***`) as of v1.21.17. It does NOT pattern-match secret
  VALUES in free text — the real control is discipline: never interpolate
  a raw secret into a `message`/`detail` string. The Plex token rides an
  HTTP header, never a URL, so it never reaches the events table anyway.

## When debugging

Open `docs/PROJECT_HISTORY.md` § "Recurring Bug Classes" first —
chances are the bug class is already catalogued with its fix
pattern and version markers. Only synthesize a new fix once you've
ruled out a known pattern.

## Session journaling

`docs/SESSION_JOURNAL.md` is an append-only log of every Claude
Code session — what we worked on, why, where we left off. The
SessionStart hook (`.claude/hooks/session-start.sh`) tails the file
and prints it as initial context for the next session, so a fresh
chat (after a crash, after `/compact`, or just on a new day) picks
up where the previous one left off without the user re-pasting.

**The journal is gitignored — local-only, never pushed.** It may
contain mid-task thinking, half-formed debugging guesses, or
references to local paths/tokens that shouldn't leave this machine.
Treat it as private scratchpad + handoff notes between sessions.

**Every meaningful task ends with a journal entry.** New entries
go at the **bottom** of the file (newest last, so `tail` surfaces
recent work). When a session ends mid-task, the entry captures
in-flight state under "Open threads" so the next session can
resume cleanly. See the file's header for the entry format.
