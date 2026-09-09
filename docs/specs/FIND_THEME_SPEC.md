# `// FIND THEME` — design spec (feature brief #2, candidate A)

Status: SHELVED 2026-09-09 — the operator chose candidate B (AnimeThemes) first;
the measured 41–49 % strict top-5 is a verification aid, not a picker, and the
naming gap only moves with an optional knowledge provider. Kept as the record;
the harness design in §4 is reused by ANIMETHEMES_SPEC.md.

## 1. Problem

A title with no ThemerrDB entry (the operator's 5 stranded rows today, and
every new NO-TDB arrival) is themed entirely outside motif: search the web,
judge candidates by ear and duration, copy a YouTube URL back into
`// SET URL`, wait for the probe + download to learn whether the pick was
good. Five context switches, and a bad pick loops.

The operator's own experience is the design constraint: a YouTube search by
show name often does NOT surface the theme, because the video that carries
the theme frequently doesn't say so in its title (an official music video
for the licensed song, a composer's Topic upload titled just "Main Title").
The web search step exists to learn the song's NAME first. Any design that
just wraps `ytsearch` and trusts YouTube's order would produce exactly the
bad searches the operator is worried about.

## 2. What the data says

Method (all scripts in the session scratchpad; the harness in §5 makes them
permanent). yt-dlp's built-in `ytsearchN:` with `--flat-playlist` (no
downloads, ~1.0–1.6 s per query, measured). Ground truth = ThemerrDB's own
`youtube_theme_url` for the same TMDB id, pulled from the `database` branch
(`movies/themoviedb/<id>.json`, `tv_shows/themoviedb/<id>.json`). "Strict"
= the exact TDB video id appears in the top-k. Strict is a LOWER bound: for
many films several uploads of the same cue are equally usable and TDB's
contributor picked one of them arbitrarily.

### 2.1 Hand-picked popular titles (n = 6, movies + TV)

| Title | TDB pick in top-6 of any template | best rank ("theme song") |
|---|---|---|
| Halloween (1978) | yes | 1 |
| Grey's Anatomy | yes | 1 |
| Sicario | yes | 1 |
| Dark (2017) | yes | 2 |
| Severance | yes | 5 (only via "main title") |
| The Bear | **no** | — |

The Bear's TDB pick is the official video for "New Noise" by Refused — the
song, not the show. No query built from the show name surfaces it.

### 2.2 Random ThemerrDB sample (seed 20260909, 35 movies + 35 TV)

Strict = exact TDB video id in the top-k of the primary query
`"{title} ({year}) theme song"`, YouTube's own order. n = 70 (35 movies,
35 TV — 22 of the 35 TV titles are Japanese animation, which is what a
random draw from ThemerrDB's TV side looks like).

| Metric | all (70) | movies (35) | TV (35) |
|---|---|---|---|
| strict top-1 | 14 (20 %) | 5 (14 %) | 9 (26 %) |
| strict top-3 | 22 (31 %) | 9 (26 %) | 13 (37 %) |
| strict top-5 | 34 (49 %) | 17 (49 %) | 17 (49 %) |
| strict, union of 3 templates (18 rows) | 39 (56 %) | 18 (51 %) | 21 (60 %) |
| strict top-5 after heuristic re-rank (best weighting tried) | 28 (40 %) | 12 (34 %) | 16 (46 %) |
| strict top-5 after round-robin merge of 3 templates | 27 (39 %) | 12 (34 %) | 15 (43 %) |
| usable-proxy top-5 (a theme-shaped candidate for THIS title present) | 65 (93 %) | 34 (97 %) | 31 (89 %) |
| TDB pick longer than 5 min | 3 / 68 | 3 / 34 | 0 / 34 |

Held-out seed 90210 (never used for tuning), n = 70 (35 + 35): strict
top-1 12 (17 %), top-3 24 (34 %), top-5 29 (41 %); union of 3 templates
38 (54 %); heuristic re-rank top-5 26 (37 %) — again BELOW raw; round-robin
merge 29 (41 %) — equal to raw; usable-proxy top-5 66 (94 %). Per template
top-5: theme song 29, opening theme 26, main title 23. The two seeds agree
on every conclusion below; the strict top-5 band for v1 is 41–49 %.

Per-template, strict (same sample):

| Template | top-1 | top-5 | top-5, anime only (22) | top-5, movies (35) |
|---|---|---|---|---|
| `{t} theme song` | 14 | **34** | 11 | **17** |
| `{t} opening theme` | 15 | 27 | 11 | 10 |
| `{t} main title` | 10 | 20 | 5 | 8 |
| union of the three | — | 39 | **14** | 18 |

Titles found ONLY by a secondary template: 4 of 70 — three of them anime
openings.

### 2.3 What that means for the design

1. **YouTube search alone finds TDB's exact pick about half the time** on
   random titles, and 5/6 on popular ones. Random TDB titles skew obscure
   and their "theme" is often a contributor's choice among several score
   cues — the strict metric punishes that, the product does not need to.
2. **Re-ranking by metadata heuristics did NOT beat YouTube's own order**
   on the strict metric (duration window, title-in-title, theme words,
   junk words, Topic channel, cross-template consensus — every weighting
   tried landed at or below the raw "theme song" top-5). The failures were
   instructive: hard filters dropped legitimate picks (Topic uploads titled
   "Main Theme" without the film's name; 335–347 s cuts), and boosts
   promoted a *different* upload of the same cue over TDB's. v1 therefore
   keeps YouTube's order and spends the metadata on LABELS, not ranking.
3. **"theme song" is the best single template — except for anime.**
   Merging the three templates round-robin DILUTES the top-5 (the
   secondary templates add 4 finds per 70 titles and push the primary's
   rank-2/3 down). So the secondary templates are a user-triggered
   `// MORE RESULTS`, appended below, never merged. Anime is the measured
   exception: "theme song" and "opening theme" tie (11/22 each) and their
   union reaches 14/22, so anime rows run BOTH as the primary — two calls,
   two labelled groups, still no merge.
4. **A theme-shaped candidate is almost always present** (usable-proxy
   top-5 = 93 %): title present or Topic channel, a theme word, 25 s–6 min,
   no junk word. That is why the human pick works: the job is to make the
   good candidate obvious, not to choose it.
5. **The residue is the licensed-song case** (The Bear, Pet Sematary's
   Ramones track). The only reliable fix is knowing the song's name.
   Keyless knowledge sources were measured: Wikidata's "theme music"
   property (P942) was empty for 6/6 titles; Wikipedia's infobox gave the
   composer for 4/6 and the article prose named the song for 2/6; Wikidata
   returned 429 after ~20 rapid requests. So the knowledge hop is an
   OPTIONAL hint at best, and the honest fallback is a free-text
   `// SEARCH BY SONG` box — the operator still does the web lookup for
   those, but search → preview → apply stays in-card.

## 3. Design

### 3.1 Entry points

- SOURCE menu (`menuItemHtml`, group 3 CUSTOM OVERRIDES, `app.js:~11567`):
  `find-theme` — `// FIND THEME` — "Search YouTube for this title's theme
  and preview the hits before setting one." Tone `user` like SET URL.
  Shown on every row that has a rating_key (same guard as SET URL).
- INFO card: the bare/untethered copy (`app.js:~17352`) and the
  `// TRY THIS NEXT` recovery section (`~18033`) gain the same action; the
  `TDB ∅` pill tooltip (`~11184`) names it.
- The action opens ONE dialog, `#find-theme-dlg`, built on the canonical
  dialog shell (`docs/DESIGN_SYSTEM.md` §"Dialog canonical shell") exactly as
  `#manual-url-dlg` is — `dialog.dlg > article.dlg-body > header.dlg-head`
  + `form.form-grid.form-grid-tight` + `div.form-actions`. Reached from both
  the row menu and the card, like SET URL.

### 3.2 The search (server)

`POST /api/plex_items/{rating_key}/find-theme` — admin-gated
(`_require_admin`), JSON body `{ "mode": "auto" | "song" | "more",
"q": "<free text, song mode only>" }`.

Query construction (`mode=auto`): `"{title} ({year}) theme song"` when the
row has a year, else `"{title} theme song"`. Rows in an `is_anime` section
(or whose TDB/TMDB genres say Animation with a Japanese original language)
run a second primary call `"{title} ({year}) opening theme"` and the dialog
shows both groups (§2.3 point 3). `mode=more`: the two secondary
templates `"{t} opening theme"` and `"{t} main title"`, plus
`"{original_title} opening"` when the TDB/TMDB `original_title` differs from
`title` (non-Latin originals — Strike the Blood's official Warner Japan
uploads only match the Japanese title). `mode=song`: the operator's text
verbatim.

yt-dlp options (a new `search_youtube(query, *, cookies_file, limit=6,
timeout_seconds=20)` in `app/core/downloader.py`, next to
`probe_youtube_url`): `extract_flat=True`, `skip_download=True`,
`socket_timeout`, the cookie snapshot (`_cookiefile_snapshot`), and the
YouTube extractor block factored OUT of `_opts()` (`js_runtimes`,
`remote_components`, `player_client`) into a helper both callers share —
the search must see the same YouTube the downloads see. Result rows carry
`id, title, channel, duration_s, view_count, url` (canonical
`https://www.youtube.com/watch?v=<id>`).

Handler shape = the probe handler's (`api.py:~11694` `_probe_sync` +
`await run_in_threadpool(...)`): the blocking call lives in a nested sync
`_run`. Add `search_youtube` to `BLOCKING_FUNCS` in
`tests/test_v1_22_58_async_no_blocking_calls.py` (the lint has NO `yt_dlp`
entry — a raw `YoutubeDL` call in an async body would pass today, so the
name goes in by hand).

Failure mapping reuses the probe's `FailureKind` classifier: a bot-check /
sign-in wall renders as "YouTube asked for a sign-in — check cookies.txt on
the Settings page", not a generic error. Every failure path logs a
breadcrumb (class-9 rule); a warn-once flag for the bot-check case (class-9
hot-path sub-pattern) since the operator will click it repeatedly.

Rate discipline: searches are flat and cheap but they are YouTube requests
from the same IP as the downloads. (a) One search in flight per process
(a lock; a second click waits, never queues a burst). (b) Per-(rating_key,
mode, q) result cache, in-memory, 10-minute TTL — re-opening the dialog is
free. (c) No background prefetch, ever: a search happens only on a click.

### 3.3 Candidate labelling (metadata → chips, not ranking)

Order is YouTube's. Each row shows: title, channel, duration, views, and
two chips derived from metadata:

- Length chip: `THEME-LENGTH` (25 s–6 min) / `LONG` (> 6 min) / `SHORT`
  (< 25 s). Neutral tone; informational. The eval showed 3/49 TDB picks over
  5 min, so LONG never hides a row.
- Junk chip: title matches the junk list (`cover, piano, tutorial,
  reaction, extended, 1 hour, 10 hours, slowed, reverb, 8d, karaoke,
  ringtone, lofi, nightcore, sped up, trailer, review, recap, explained,
  top N, ranked, compilation, every/all openings`) → the row is DEMOTED to a
  collapsed "// PROBABLY NOT" group at the bottom, still openable. Never
  dropped: the human decides. Hard drop only for the absurd: < 15 s or
  > 15 min (hour loops), because the preview would be pointless.
- `TOPIC` chip when the channel ends in " - Topic" (auto-generated official
  audio) — the strongest "this is the actual recording" signal we have.

### 3.4 The picker dialog

Mirrors the revision-history list (`app.js:~18608`, `dt/dd` rows inside a
`.dlg-grid`) and the SET URL dialog shell. Content top to bottom:

1. Header `// FIND THEME — <Title (Year)>` + `.dlg-close`.
2. The query row: a read-only rendering of the auto query, a text input for
   `// SEARCH BY SONG` (placeholder: `song title — artist`), a plain `.btn`
   `// SEARCH`. Enter submits.
3. Results group `// RESULTS` — up to 6 rows. Each row: `<dt>` = duration
   (tabular) + chips; `<dd>` = title · channel · views, then two tiny
   buttons: `// PREVIEW` (`.btn-tiny .btn-info`, non-mutating) and
   `// USE THIS` (`.btn-tiny .btn-warn`, mutating — the design-system rule
   for submit-vs-cancel tones).
4. `// MORE RESULTS` (plain `.btn`) appends the `mode=more` rows as a
   second group `// MORE`, deduped against the first, chips identical.
5. The demoted `// PROBABLY NOT` fold (`details.history-section.info-fold`),
   collapsed by default.
6. Status line under the form (`// SEARCHING…` while in flight, the
   failure text on error) using the status-text auto-dismiss pattern.
7. An `<audio controls class="info-audio">` player row appears under the
   results once a preview is ready (the EDIT AUDIO player, same classes).

Colour: chips use the neutral `.chip`; `TOPIC` may use the fixed `--ok`
family (it is a "present/healthy" signal), never `--green-bright`. No new
tokens, no new class names beyond `find-theme-*` modifiers on existing
primitives.

Empty state: "No results for this query — try `// SEARCH BY SONG` with the
song's name, or `// MORE RESULTS`."

### 3.5 Preview — the .281 candidate pipe, reused verbatim

`POST /api/plex_items/{rating_key}/find-theme/preview` `{ "video_id" }` →
downloads the audio (yt-dlp, mp3, LAME `-V5` for speed; the real download
later runs at the configured quality) into
`themes_dir/.edit-candidates/<cid>.mp3` using `audio_edit`'s directory,
`secrets.token_hex(16)` id, `_sweep_stale` on every render, and the
existing 1-hour TTL. Returns `{candidate_id, duration_s, file_size}`.
Playback streams through the existing range-capable route
`GET /api/items/{mt}/{id}/edit-candidate/{cid}.mp3` (it serves any valid cid
from the candidate dir; `candidate_path` is traversal-safe by the 32-hex
rule). Closing the dialog fires the existing cancel route for the last cid;
the TTL sweep is the backstop.

Guards: one preview in flight per process (server lock) and per dialog
(the `_previewRendering` pattern); refuse a preview when the flat duration
is > 15 min ("too long to preview — use SET URL if you're sure"); preview
downloads pass through the SAME shared `TokenBucket` the worker uses
(`worker.py:~4950`) so the hourly YouTube budget counts them; the preview
is admin-gated like every candidate route.

### 3.6 `// USE THIS` — it is SET URL

`// USE THIS` calls the existing `POST /api/plex_items/{rk}/manual-url`
with the canonical URL and the same body SET URL sends (`download_only`
mirrors the SET URL dialog's checkbox, default off; when the row ALREADY has
a theme the dialog pre-ticks it so a find-theme pick lands as a backup
revision rather than replacing the current theme silently). No new write
path: the `user_overrides` upsert, `_record_audit(action="set_url")`, the
auto-ack, the in-flight download cancel, the `jobs` enqueue and the
`log_event` all fire unchanged. The event message
`Manual URL set by <user>: <url>` stays BYTE-IDENTICAL (the v1.18.10
recovery walker parses it); the origin goes into `detail` as
`{"source": "find_theme", "video_title": ..., "channel": ...}` only. The row
becomes an ordinary U-row; revisions, backups and notifications apply.

### 3.7 Never

- No auto-set, no "best guess" pre-selection, no server-side pick.
- No background searches (no prefetch on card open, no sweep over NEEDS
  WORK rows).
- No new source kind / SRC letter — a find-theme pick IS a user URL.
- No knowledge-hop dependency in v1 (§2.3 point 5); it may return as an
  opt-in hint tag ONLY if the harness shows a strict-metric gain.

## 4. The evaluation harness (ships FIRST, as tag 1)

`tools/find_theme_eval.py` — a repo script, NOT a pytest test (it needs the
network). Deterministic sampling from the ThemerrDB `database` branch:
`--kind movies|tv_shows --n 35 --seed <int>`, skipping entries whose
`release_date`/`first_air_date` is in the future (the sample contained a
2026 film whose "theme" cannot exist yet). For each title it runs the
product's OWN query builder and `search_youtube` (not a re-implementation —
the harness imports `app.core.downloader`), records the raw lists, fetches
the truth video's flat metadata once, and writes `<out>/results.json` +
`<out>/summary.md` with:

- strict top-1 / top-3 / top-5 for the primary query;
- strict top-5 after the junk filter (must not drop truths: reported);
- per-template strict top-5 (what `// MORE RESULTS` buys);
- usable-proxy top-5 (labelled a proxy in the output);
- the miss list with the truth's title/channel/duration (the licensed-song
  residue, by eye).

Two fixed seeds are banked in `docs/specs/find_theme_eval/`: `tune`
(20260909) and `holdout` (90210). Rules:

1. Any change to the query builder, the junk list or a ranking rule must
   show its numbers on BOTH seeds in the tag's changelog entry, and may not
   regress strict top-5 on `holdout` by more than sampling noise (2 titles).
2. The v1 implementation tag must reproduce the banked baseline on `tune`
   (same lists for the same titles, modulo YouTube churn) — this proves the
   product code and the harness measure the same thing.
3. The offline pytest side pins everything that does not need the network,
   with canned yt-dlp entries via monkeypatch: query construction (title,
   year, original_title variant), order preserved, chip derivation, junk
   demotion never deletes, the absurd-duration drop, the cache TTL, the
   one-in-flight lock, the lint entry, admin gating on all three routes,
   the candidate id rule, the manual-url message string unchanged, the
   `detail.source` breadcrumb, and the version pin. Each pin gets the
   mutation-verification treatment before it counts.

Baseline numbers banked by this document (§2.2) are the bar; the per-tag
changelog carries the numbers forward.

## 5. Tags

| Tag | Scope | Gate |
|---|---|---|
| 1 | `tools/find_theme_eval.py`, banked `tune` + `holdout` results, this spec's §2 regenerated from the tool | harness reproduces §2.2 within noise |
| 2 | `search_youtube` + shared YouTube opts helper, `/find-theme` endpoint (auto/more/song), chips + junk demotion, cache, lock, lint entry, offline pins | pytest + ruff; harness on both seeds unchanged |
| 3 | `#find-theme-dlg`, SOURCE-menu + INFO-card entry points, `// USE THIS` through manual-url with the `detail.source` breadcrumb | pytest; live check on the operator's 5 stranded rows |
| 4 | `/find-theme/preview` through the `.edit-candidates` pipe, player row, TokenBucket accounting, size/duration refusal, close-discard | pytest; live preview on 3 rows |
| 5 | `// MORE RESULTS`, `// SEARCH BY SONG`, empty states, `TDB ∅` tooltip, README + CLAUDE.md notes | pytest; harness per-template table in the changelog |

Tag 1 lands alone so the numbers exist before any product code does.

## 6. Risks and open decisions

- **YouTube anti-bot.** Same exposure as downloads (same yt-dlp, cookies,
  node runtime). The one-in-flight lock, the cache and the shared
  TokenBucket keep the added load to human click rate. If the bot wall
  fires, the search surfaces the cookies message rather than failing quietly.
- **Obscure titles.** The random-sample numbers are the honest expectation
  for the rows this will actually be used on; the licensed-song residue is
  covered by `// SEARCH BY SONG`, not by cleverness.
- **Non-Latin originals.** Handled as a `// MORE RESULTS` variant, not in
  the primary query (an original-title query changes the primary's
  behaviour for every title; measure before promoting).

Open for the operator to decide:

1. Dialog (proposed) vs an in-card `dlg-section`. Dialog keeps the row-menu
   path and the card path identical, like SET URL.
2. Preview quality `-V5` (fast) vs the configured quality (slower, closer to
   what will be placed).
3. When the row already has a theme: default `// USE THIS` to backup intent
   (proposed) or to replace.
