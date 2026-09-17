# Backup bundle (feature D) — definition

Status: DECIDED 2026-09-12 — the operator took every proposed option (§ 4). Tag 1 BUILT as v0.51.335, tag 2 (restore) BUILT as v0.51.336, tag 3 (themes check) BUILT as v0.51.337 on the existing // CANONICAL HEALTH page (§ 3 note). Feature D complete. Mockup: the session artifact "Backup bundle".

## 0. What exists today (v1.23.15–17)

- **Database snapshots**: `VACUUM INTO` copies of `motif.db` in `/config/backups`
  (`motif-YYYYMMDD-HHMMSS.db`), `// CREATE BACKUP NOW`, a scheduled job with
  cron + retention, download / delete by validated name.
- **Restore**: staged, applied at the next container start after a pre-restore
  safety copy; from a listed snapshot or an uploaded `.db`; validated (SQLite
  header, integrity check, schema not newer than the build).
- The settings copy says it plainly: the snapshot holds motif's **tracking**
  — theme links, placements, overrides, previous URLs, the account — and
  "a backup of `/config` captures them too."

What the snapshot does not hold: `motif.yaml` (paths, Plex URL + token,
notification URLs, TVDB key, schedules), `cookies.txt`, and the theme files
themselves.

## 1. The gap — three scenarios, measured on the July snapshot

| | Lost | Still there | Today |
|---|---|---|---|
| **S1 appdata gone** | `/config`: DB, config, cookies | `/data`: 13 GB of canonicals + sidecars | an off-box `.db` snapshot restores tracking; config and cookies are re-typed by hand |
| **S2 themes directory gone** | 2,822 canonicals (13.03 GB, median 4.4 MB) | the DB; hardlinked sidecars in Plex folders (697, same inode); Plex's own store (1,675 `plex_upload`) | nothing in-app; 1,936 T + 314 U + 20 cloud rows have a source to re-download from, 532 adopted + 20 uploaded rows do not (their copy is the sidecar or Plex's store) |
| **S3 new host / rebuild from scratch** | everything on the old box | a Plex server, the media tree | DB restore + hand-typed config + cookies + copy the themes dir + re-verify |

A bundle serves S1 and S3 outright, and gives S2 a census to recover from.

## 2. Definition (proposed)

A **backup bundle** is one archive, `motif-bundle-YYYYMMDD-HHMMSS.tar.gz`,
written to `/config/backups` next to the snapshots, holding everything a
rebuilt or moved install needs except the theme bytes:

| Member | Source | Notes |
|---|---|---|
| `motif.db` | the existing `create_backup` (`VACUUM INTO`) | consistent, compacted |
| `motif.yaml` | `/config/motif.yaml` | secrets per decision 2 |
| `cookies.txt` | `paths.cookies_file`, when present | a secret too — decision 2 |
| `manifest.json` | generated | motif version, schema version, created_at, row counts per table, the configured paths, sha256 + size of each member, the redaction list, and the **themes census** |
| — | `.session_key`, `logs/`, `animethemes/` cache, `backups/` | excluded: regenerated, noise, or recursive |

The **themes census** is every `local_files` row as `{path (relative to
themes_dir), size, sha256 where the DB has it, source_kind, source id / URL,
placement kind}`. It weighs ~1 MB and turns S2 from "hope" into a list: what
was there, which rows can be re-fetched and from where, which cannot.

Size: the DB is 49 MB on the July snapshot; a bundle compresses to roughly
10–20 MB. Retention and listing reuse the snapshot machinery (the same
name-gate regex family, the same `prune_backups` window, bundles counted
alongside snapshots).

(v0.51.344) Names and retention, as `db_backup._KINDS` reads them:

| Name | Kind | Counts toward retention |
|---|---|---|
| `motif-YYYYMMDD-HHMMSS.db` | snapshot | yes |
| `motif-bundle-YYYYMMDD-HHMMSS.tar.gz` | bundle | yes |
| `motif-bundle-partial-YYYYMMDD-HHMMSS.tar.gz` | bundle, partial | yes |
| `motif-bundle-upload-YYYYMMDD-HHMMSS[-N].tar.gz` (N 2–99) | bundle | no — kept until deleted |
| `motif-prerestore-YYYYMMDD-HHMMSS.db` | pre-restore | no — kept until deleted |

A bundle that left `motif.yaml` or `cookies.txt` out over its cap is written
under the partial shape: the manifest is the archive's LAST member, so only the
name can say so without inflating the archive. `create_bundle` refuses a stamp
that already has a bundle of either shape. The list rows carry `partial`.

`prune_backups(config_dir, retention, now_stamp=, keep=)` runs after each
scheduled backup. It keeps the newest `retention` names that count and deletes
the older ones, with three exceptions: it never deletes `keep` (the file the
job just wrote); a name stamped later than `now_stamp` (read after the create)
is neither counted nor deleted, with a WARNING naming it; and while every
bundle inside the window is partial, the newest complete bundle below the
window is kept as well (logged). With no bundle inside the window (bundle mode
off, or nightly fallback snapshots) that keep does not apply. A name whose
stamp has non-ASCII digits matches no shape, so it is never listed, counted
or pruned.

Not in the bundle: the theme files. 13 GB of already-compressed MP3 is a job
for the appdata / share backup tools the box already runs (or `rsync`); an
in-app archive would fill `/config` and cannot be downloaded through a
browser sensibly. Decision 3 keeps this on the table as an opt-in.

## 3. Tags

- **Tag 1 — create + schedule** (~1 tag). `// CREATE BUNDLE NOW` in the
  DATABASE BACKUP block; bundles listed with the snapshots (download / delete /
  restore); a `bundle` toggle in SCHEDULED BACKUPS so each scheduled run writes
  a bundle (decision 4). `app/core/bundle.py` builds it from `db_backup`'s
  snapshot + the manifest; the API mirrors the four snapshot endpoints.
- **Tag 2 — restore from bundle** (~1 tag). `// RESTORE FROM FILE` accepts
  `.tar.gz`: validate the manifest (member sha256s, versions), run the existing
  DB checks, then stage the DB **and** the config / cookies together — applied
  at the same boot-time swap, after a pre-restore copy of `motif.yaml` — with
  a diff of the config shown first (paths differ on a new host) and a "keep my
  current config" choice (decision 5).
- **Tag 3 — themes check** (optional, ~1 tag). `// CHECK THEMES` after a
  restore or on demand: walk the census against `themes_dir` → present /
  missing / changed; "re-download the N missing rows with a source" through
  the download queue; "re-fetch the M missing `plex_upload` rows from Plex"
  through the v1.18.36 entry-bytes path. No full archive.
  *Built (v0.51.337) on the existing `/admin/canonical-health` page, which
  already stats every canonical (the live `local_files` IS the restored
  bundle's census) and offers REPAIR ALL for the rows with a source: it gains
  `// RESTORE FROM PLEX (N)` (sidecar first, then Plex's store) and a CHANGED
  block; the DATABASE tab's restore card links to it.*

## 4. Decisions for the operator

1. **Scope.** S1 + S3 outright, S2 via the census (proposed) · S1 only (config
   + DB, no census) · all three including theme bytes.
2. **Secrets in the bundle.** As-is — the Plex token, notification URLs, TVDB
   key and `cookies.txt` travel in the bundle, which is then exactly as
   sensitive as `/config` itself, admin-download only, with the settings copy
   and the filename saying so (proposed: a bundle that cannot restore without
   re-typing secrets does not serve S3) · redacted — placeholders + a
   redaction list in the manifest, re-entered on restore.
3. **Theme bytes.** Census only (proposed) · plus an opt-in full archive
   (~13 GB, uncompressed tar, written to a path of your choosing, never a
   browser download).
4. **Scheduled runs.** When the bundle toggle is on, the scheduled run writes a
   bundle *instead of* a bare snapshot — the bundle contains the same DB, so
   one artifact per run (proposed) · both every run.
5. **Config on restore.** Applied at the boot-time swap after a pre-restore
   copy, with the diff shown first (proposed) · diff only, applied by hand.

## 5. Surfaces (all on Settings → DATABASE, extending the v1.23.15–17 blocks)

**DATABASE BACKUP.** A second button beside `// CREATE BACKUP NOW`:
`// CREATE BUNDLE NOW` (`.btn-warn`, same tone — both write to `/config`).
The intro gains one sentence: a bundle adds `motif.yaml`, `cookies.txt` and
a themes census, and is as sensitive as `/config` itself. `// EXISTING
BACKUPS` lists snapshots and bundles together, newest first, each row
leading with a kind chip — `BUNDLE` (green), `SNAPSHOT` (dim), `PRE-RESTORE`
(amber) — then the name, `size · date`, and the same `// DOWNLOAD` /
`// RESTORE` / `// DELETE` trio (`bindDatabaseBackup` renders both kinds from
one list endpoint; the name gate admits `motif-bundle-<stamp>.tar.gz`). An
uploaded or a partial bundle keeps the `BUNDLE` chip; its tooltip says it sits
outside retention, or that `motif.yaml` or `cookies.txt` was left out over its
size cap when it was made (v0.51.344).

**SCHEDULED BACKUPS.** One more checkbox under ENABLE: `WRITE A BUNDLE` —
"each scheduled run writes a bundle (the snapshot is inside it) instead of a
bare snapshot; retention counts both kinds." Off by default so an existing
schedule keeps its behaviour until the operator opts in. (v0.51.344) Both
hints say uploaded bundles sit outside retention; the retention hint adds that
pre-restore copies never count, that the newest complete bundle is kept while
every kept bundle left a member out, and that a backup stamped later than now
is neither counted nor deleted. The scheduler and `// CREATE BUNDLE NOW` both
call `bundle.create_bundle_for(settings, now_stamp)`, the one spelling of a
live install's `create_bundle` arguments.

**RESTORE.** `// RESTORE FROM FILE` accepts `.db` and `.tar.gz`. Choosing a
bundle (from the list or an upload) runs the checks and, before anything is
staged, shows a **restore preview** card:

- the manifest line — `bundle from motif v0.51.3xx · schema v79 · 2026-09-12
  04:00 UTC · 2,822 themes in the census`;
- the DB check line the snapshot restore already prints;
- a **config diff** — only the keys that differ between the bundle's
  `motif.yaml` and the live one, secrets shown the way GET `/api/config`
  shows them (`***`, `<scheme>://***`, `://***@host`; v0.51.339) — with
  a checkbox `KEEP MY CURRENT CONFIG (restore the database only)`, unticked;
- `cookies.txt: in bundle, will replace yours` or `not in bundle`;
- (v0.51.339) when either `motif.yaml` does not parse there is no diff: the card
  names the side (`config_parse_error`), and a bundle config that does not
  parse forces KEEP MY CURRENT CONFIG — staging it without that is a 422.
  (v0.51.344) A value YAML cannot build is named by its dotted key — `<key>
  holds an integer too long to read` / `a date that does not exist` — never by
  its value; a diff value past Python's int-to-text limit shows as `(a N-bit
  integer, too long to show)`.
- (v0.51.344) a manifest `created_at` that is not an ISO stamp comes back null
  and the line reads `date unknown` (an upload's name never reads it); the
  cookies line names `cookies_target` whenever it is known, so a cookies file
  the restore leaves as it is is named too; the STAGE confirm names only the
  members the restore replaces, with one "stays as it is" clause for each it
  leaves live.

`// STAGE RESTORE` then stages the DB (as today) plus the config and cookies;
the pending banner reads "A restore is staged (database + config)". At boot
(v0.51.339) the database applies first; `motif.yaml` follows only a database
that applied (after a `motif.yaml.prerestore-<stamp>` copy), and the cookies
land on `paths.cookies_file` after a `<file>.prerestore-<stamp>` copy. A
snapshot staging drops a bundle's staged config and cookies (before the
database swap, under one staging lock — v0.51.341; a drop that fails refuses
the staging). The preview's cookies line names the file the boot restores to:
the bundle config's `paths.cookies_file`, env overrides applied. `// CANCEL
RESTORE` drops all of it. (v0.51.342) A member that fails to stage after the
database swap unstages the whole bundle — nothing from it stays pending, and
the refusal says so. A cookies file or `motif.yaml` bind-mounted as a single
file is written in place: the cookies file keeps the mode the host gave it,
`motif.yaml` ends 0600, and a write that fails part-way puts the original
bytes back.

(v0.51.344) When that unstaging cannot remove the staged database, it stops
there: the staged config and cookies stay with the database they were staged
with, and the refusal names every pending file to remove by hand before
staging again. A staged file's `.tmp` left by a crashed staging is unlinked
before the new bytes go in, so they are born 0600. An in-place write that
completes but whose `fsync` the mount refuses (EINVAL / ENOTSUP / EOPNOTSUPP)
is logged as written, not synced, instead of failing; a write that makes no
progress raises EIO instead of looping.

(v0.51.344) The restore answer and its event carry what the staging left
live. A confirm that does not keep the config fills `left_as_is` for each
member over its cap in this bundle, or noted in the manifest as left out when
the bundle was made: `<member> is N bytes, over its C-byte cap` or `<member>
was left out when the bundle was made (N bytes, over its C-byte cap)`, then `—
it is not restored, and your motif.yaml | cookies file is left as it is`. The
response's `left_as_is` is a list of `{member, why}` (a `cookies.txt` key
would be redacted by the events scrubber), its `message` — the restore card's
line — ends with each `why` as a sentence, and the WARNING event reads
`…; applies on restart.` plus the same sentences (a restore that leaves
nothing live keeps its message byte-identical), with `left_as_is` in its
detail.

(v0.51.342) Create and restore read one member-cap table: database 4 GiB,
manifest 64 MiB, `motif.yaml` 1 MiB, `cookies.txt` 16 MiB. An over-cap
database or manifest writes no bundle (take a plain snapshot). An over-cap
`motif.yaml` or `cookies.txt` is left out at create, with a manifest
`left_out` note and a WARNING, and the preview names it. At restore an
over-cap `cookies.txt` is skipped (the live cookies file stays as it is),
while an over-cap `motif.yaml` forces KEEP MY CURRENT CONFIG. The config
loader coerces lossless spellings — an unquoted number in a text setting, a
quoted number in a number setting — and the preview and staging judge the
same coerced values, so a config motif runs on is never refused for its
spelling.

(v0.51.344) Before its VACUUM, `create_bundle` estimates the database from its
live pages — `(page_count − freelist_count) × page_size` — and refuses it when
that is more than 5% over the database cap, so an over-cap database no longer
pays the whole VACUUM first; the snapshot is still measured against the cap
after it. Free pages never count, but pages left part-full by deletes do, so a
database whose snapshot would sit just under the cap can be refused. Every
over-cap refusal (database estimate, database snapshot, manifest) raises
`BundleOverCap`, a `ValueError`: `// CREATE BUNDLE NOW` answers it as a 422 in
words, and a scheduled run takes a plain snapshot instead with a WARNING event
("Scheduled backup bundle not written (…) — a plain database snapshot was
taken instead"). That snapshot counts toward retention like any other. An
over-cap `motif.yaml` or `cookies.txt` left out at create gives the bundle the
partial name (§ 2).

**THEMES CHECK (tag 3, optional).** A block below RESTORE: `// CHECK THEMES`
walks the newest bundle's census (or an uploaded one) against `themes_dir`
and reports `present · missing · changed`, with two buttons that appear only
when they apply: `// RE-DOWNLOAD N WITH A SOURCE` (the download queue) and
`// RE-FETCH M FROM PLEX` (the v1.18.36 entry-bytes path for `plex_upload`
rows). Adopted / uploaded rows with nothing to fetch from are listed by name.

## 6. `manifest.json`

```json
{
  "kind": "motif-bundle", "format": 1,
  "created_at": "2026-09-12T04:00:00Z",
  "motif_version": "0.51.335", "schema_version": 79,
  "members": {
    "motif.db":    {"size": 51823616, "sha256": "…"},
    "motif.yaml":  {"size": 2410,     "sha256": "…", "secrets": "as-is"},
    "cookies.txt": {"size": 3120,     "sha256": "…"}
  },
  "paths": {"config_dir": "/config", "themes_dir": "/data/media/themes"},
  "counts": {"plex_items": 17427, "themes": 6232, "local_files": 2822,
             "placements": 2372, "user_overrides": 325, "saved_filters": 2},
  "themes_census": [
    {"path": "tv/Cowboy Bebop (1998)/theme.mp3", "size": 4587123, "sha256": "…",
     "source_kind": "url", "source_video_id": "at-CowboyBebop-OP1",
     "placement_kind": "hardlink", "section_id": "3"}
  ]
}
```

`format` is the bundle's own version; a restore refuses a newer format the
way the DB check refuses a newer schema.

## 7. API

Mirrors the four snapshot endpoints under one list:

- `POST /api/admin/database-backup?kind=bundle` → creates a bundle (the
  existing endpoint with a kind, so the button wiring is one line);
- `GET /api/admin/database-backups` → rows gain `kind`;
- `GET …/download/{name}` and `POST …/delete` → the name gate admits both
  shapes;
- `POST /api/admin/database-restore` and `…/upload` → accept a bundle; the
  response carries the preview (`manifest`, `config_diff`, `cookies`), and a
  second call with `{"confirm": true, "keep_config": false}` stages;
- tag 3: `POST /api/admin/themes-check` (+ the two re-fetch actions).

(v0.51.344) What the endpoints now carry:

- The create response and the list rows carry `partial` (the list rows also
  `retained`, v0.51.343). A confirmed bundle restore answers `members` and
  `left_as_is` (§ 5).
- An uploaded bundle is checked in a temp file, then filed by
  `bundle.file_upload` under `motif-bundle-upload-<upload's UTC time>.tar.gz`,
  or `-2` … `-99` when that second's earlier names are taken. Each candidate
  passes the name gate, is claimed with `O_EXCL`, and only then is the checked
  upload renamed onto it. Two uploads in one second get two names — the 409
  "delete it first" is gone, and an upload already previewed is never
  replaced — and identical bytes uploaded twice are two files. With all 99
  names of a second taken the answer is a 503 ending "try the upload again".
- A disk fault before a bundle is judged is a 507 (ENOSPC / EDQUOT) or a 500 in
  words, never a wordless 500: the extraction directory for a check or a
  staging — `could not write the extraction beside the bundle | motif.db
  (<strerror>) — <name> was not judged; free space or fix permissions there,
  then try again` — and saving an upload — `could not save the upload in the
  backups directory | the config directory (<strerror>) — nothing was kept;
  free space or fix permissions there, then try again`. A refusal read from
  the archive names the error type and `strerror`, and a snapshot that cannot
  be opened reads `unreadable: <strerror>`: the absolute path goes to the log
  only.

## 8. Tests

- `bundle.py` (pure, clock-injected like `db_backup`): builds a bundle into
  a temp config dir, the manifest lists every member with a matching sha256,
  the census matches `local_files`, excluded files stay out; the name gate
  refuses traversal and foreign names; prune counts both kinds.
- Restore: a TestClient flow — upload a bundle, get the preview with the
  right diff, stage, and the boot applies the database, then `motif.yaml`,
  then the cookies file, leaving each pre-restore copy (v0.51.339); `keep_config` leaves `motif.yaml`
  untouched; a newer `format` or schema is refused with the existing 422.
- Secrets never reach the events log or the preview (masked by GET
  `/api/config`'s own rule since v0.51.339), pinned.
- Settings markup, the list chip rendering, the schedule toggle's config key
  (`database_backup.bundle`) in `_ALLOWED_TOP_LEVEL` and the env mirror.

## 9. Traps to design around

- **Boot order.** Config is read before the DB opens, so the swaps run before
  `get_settings()` — and (v0.51.339) the DB applies FIRST: `motif.yaml`
  follows only a DB that applied, and the cookies follow the settings read
  (they land on `paths.cookies_file`). Log every outcome — the cold path logs
  more, not less (CLAUDE.md class 9).
- **Ownership.** Files written at boot must carry the PUID/PGID the
  entrypoint drops to, or the next write fails (the `WRITABILITY:` probe).
- **Secrets in logs.** The manifest and the preview never carry secret
  values; the events scrubber keys on names, not values, so nothing
  interpolates `plex.token` into a message.
- **The tar member gate.** (v0.51.342) One forward pass (`r|`) judges each
  header before a byte of it is read: only the four member names, each once;
  regular files only (no links, devices, CONTTYPE or pax sparse members);
  per-member caps (database 4 GiB, manifest 64 MiB, `motif.yaml` 1 MiB,
  cookies 16 MiB); and tarfile's next-header offset must equal the gated
  size rounded to a block — stream mode seeks by the RAW header size one
  read at a time, and pax keys can change `TarInfo.size` after that offset
  is computed. Nothing is extracted with its archived path. The gzip
  trailer is verified, and what follows the end of the archive is bounded:
  one 2 MiB raw allowance shared by the last tar fetch and the trailer read,
  and 4 MiB inflated. Empty gzip members parse in a loop inside one
  `read()`, so only a raw-byte budget on the file stops a flood of them.
  (v0.51.344) tarfile reads an extension header's whole payload before it
  hands back the member the gate judges, so extension headers are judged
  first, in `_BoundedTarInfo._proc_member`, before their payloads are read:
  L / K / x / X headers share one 64 KiB budget per archive (each charged its
  header block plus its payload rounded to a block — `over 65536 bytes of
  extension headers`), and a global pax header (`g`), a GNU sparse header
  (`S`) or a pax sparse 1.0 member is refused on sight (`…, which motif never
  writes`). motif writes none of those kinds, and its own pax headers are far
  under the budget. On .343 a 510 KiB upload whose `L` header declared 512 MiB
  held 1.3 GiB before any gate. The raw guard is in force over the whole read,
  not only past the archive's end (its allowance is still refreshed by each
  tarfile fetch), so a trip inside the archive no longer names an end it never
  reached: the refusal reads `over 2097152 bytes of its gzip stream were read
  for too little data — padding or empty gzip members`. The inflate budget,
  which runs only in the drain after the tar loop, keeps its "after the
  archive's end" words; that refusal logs a WARNING like every other read-time
  refusal; a failed close of the extracted database is logged and never
  replaces the read fault already in flight, so a corrupt bundle is never
  reported as a disk fault.
- **Bind mounts.** Docker refuses a rename over a single-file bind mount
  (EBUSY/EXDEV/EPERM), so the boot writes those files in place after a
  `.prerestore-<stamp>` copy. The cookies file keeps the mode the host gave
  it (it can be shared with other containers); `motif.yaml` is set to 0600
  because it holds the Plex token.
