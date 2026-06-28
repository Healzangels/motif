# motif diagnostics — checklists for hard-to-repro bugs

When a bug surfaces that needs the live DB state to root-cause,
this doc has the queries + context the user (or any operator)
can run + paste back for analysis. Each section is a known-
open mystery with its own checklist.

## Provenance="manual" after REPLACE TDB (v1.15.94 follow-up)

### The mystery

the user's "A Dog's Journey (2019)" row (movie tmdb=522518)
showed `local_files.provenance = "manual"` + empty
`source_video_id` after multiple REPLACE TDB calls. The
worker computes provenance from `override` presence at
download time:

```python
provenance = "manual" if override else "auto"
source_kind = "url" if override else "themerrdb"
```

REPLACE TDB *deletes* user_overrides for the row's section
before enqueueing the download. So the worker SHOULD see
`override = None` and write `provenance = "auto"` /
`source_kind = "themerrdb"`. Yet the actual DB state showed
the manual/empty combination characteristic of an A-source
adopt — as if the REPLACE TDB downloads never actually
ran, or ran with override truthy.

### Hypothesis A: stale user_overrides at a different section_id

`replace_with_themerrdb()` at `adopt.py:455-485` looks up
user_overrides scoped to the row's section_id first, then
falls back to the `''` global row. It deletes whichever it
found. But if BOTH a section-specific AND a global override
exist (unusual but possible from manual SQL or pre-v1.12.72
state), only one gets deleted.

Then the worker's lookup at `worker.py:952-962`:

```python
override = SELECT * FROM user_overrides WHERE ... AND section_id = ?
if override is None:
    override = SELECT * FROM user_overrides WHERE ... AND section_id = ''
```

If a stale '' override survives the delete, the worker picks
it up → provenance = "manual".

### Hypothesis B: a non-REPLACE-TDB path wrote local_files

Maybe the place worker re-stamped `local_files` somehow,
preserving an old provenance value. Less likely — the place
worker doesn't write local_files in normal paths.

### Hypothesis C: the download actually failed silently

If yt-dlp returned a sibling-hardlink result (v1.11.0 path)
where the sibling row's stale provenance was used. v1.12.37
explicitly fixed this — the worker now derives provenance
from CURRENT override state, not the sibling's. But if the
v1.12.37 fix has a corner case, this could regress.

### What v1.15.101 added

Instrumentation: `worker.py:_do_download` now logs at INFO
level whenever a download writes `provenance="manual"`,
including the override row's section_id + URL:

```
_do_download: provenance='manual' for movie/522518 section=1
  (override section=1, url=https://youtube.com/watch?v=...)
```

Next time the symptom reproduces, the log shows EXACTLY
which override the worker found.

### Diagnostic queries (run via `sqlite3 /config/motif.db`)

```sql
-- 1. Current user_overrides for the row. Look for unexpected
--    rows at different section_ids.
SELECT * FROM user_overrides
WHERE media_type = 'movie' AND tmdb_id = 522518;

-- 2. Current local_files. Look at provenance + source_kind +
--    source_video_id + downloaded_at to determine which
--    download wrote this row.
SELECT * FROM local_files
WHERE media_type = 'movie' AND tmdb_id = 522518;

-- 3. Placement state.
SELECT * FROM placements
WHERE media_type = 'movie' AND tmdb_id = 522518;

-- 4. Theme row state.
SELECT id, youtube_url, failure_kind, failure_acked_at,
       last_probed_at FROM themes
WHERE media_type = 'movie' AND tmdb_id = 522518;

-- 5. Recent audit log (last 20 actions on this row).
SELECT at, actor, action, section_id, details
FROM audit_log
WHERE media_type = 'movie' AND tmdb_id = 522518
ORDER BY at DESC LIMIT 20;

-- 6. Recent jobs (last 10).
SELECT id, job_type, section_id, status, payload, created_at,
       finished_at, last_error
FROM jobs
WHERE media_type = 'movie' AND tmdb_id = 522518
ORDER BY id DESC LIMIT 10;

-- 7. Local files history (URL snapshots).
SELECT saved_at, saved_reason, source_kind, source_video_id,
       youtube_url
FROM local_files_history
WHERE media_type = 'movie' AND tmdb_id = 522518
ORDER BY saved_at DESC LIMIT 10;

-- 8. Previous URLs (the REVERT snapshot).
SELECT * FROM previous_urls
WHERE media_type = 'movie' AND tmdb_id = 522518;
```

### What output to share back

Paste the output of all 8 queries plus any recent log lines
matching `_do_download: provenance='manual'`. With that, the
root cause should be identifiable in one round-trip.

### If it doesn't reproduce

The mystery may have been a transient state from the May 12
events that resolved naturally (e.g., a sync ran and
re-stamped provenance). If subsequent REPLACE TDB calls
produce `provenance="auto"` as expected, mark this section
RESOLVED and remove it from this file.
