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
one list endpoint; the name gate admits `motif-bundle-<stamp>.tar.gz`).

**SCHEDULED BACKUPS.** One more checkbox under ENABLE: `WRITE A BUNDLE` —
"each scheduled run writes a bundle (the snapshot is inside it) instead of a
bare snapshot; retention counts both kinds." Off by default so an existing
schedule keeps its behaviour until the operator opts in.

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

`// STAGE RESTORE` then stages the DB (as today) plus the config and cookies;
the pending banner reads "A restore is staged (database + config)". At boot
(v0.51.339) the database applies first; `motif.yaml` follows only a database
that applied (after a `motif.yaml.prerestore-<stamp>` copy), and the cookies
land on `paths.cookies_file` after a `<file>.prerestore-<stamp>` copy. A
snapshot staging drops a bundle's staged config and cookies. `// CANCEL
RESTORE` drops all of it.

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
- **The tar member gate.** Only the four known member names are extracted,
  to the staging area, never with their archived paths — no `../`, no
  symlinks, size-capped.
