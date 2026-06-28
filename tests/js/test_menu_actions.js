// v1.12.81: regression harness for the SOURCE / PLACE / REMOVE
// menu gating in renderLibraryRow. The gating logic itself lives
// in app/web/static/lib/menu-actions.js — these tests pin which
// actions appear for representative row states (and which DON'T)
// so a v1.12.51-style "ACCEPT UPDATE leaks onto src='-' rows"
// regression fails CI before reaching the UI.
//
// Run: `node --test tests/js/`

"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { computeSrcLetter, computeMenuActions } = require(
  "../../app/web/static/lib/menu-actions.js",
);

// Minimal row factory — all the optional fields default to a state
// that produces a "no theme anywhere" (src='-') row so individual
// tests only need to flip the fields they care about.
function row(overrides) {
  return Object.assign(
    {
      media_type: "movie",
      tmdb_id: 42,
      theme_tmdb: 42,
      theme_media_type: "movie",
      title: "Test",
      year: 2024,
      rating_key: "rk-1",
      section_id: "1",
      upstream_source: "themerrdb",
      youtube_url: "https://youtu.be/abc12345678",
      // placement
      media_folder: null,
      placement_provenance: null,
      // local file
      file_path: null,
      canonical_missing: false,
      mismatch_state: "none",
      source_kind: null,
      source_video_id: null,
      provenance: null,
      // plex flags
      plex_local_theme: false,
      plex_has_theme: false,
      // tracking
      pending_update: 0,
      pending_update_kind: null,
      actionable_update: 0,
      accepted_update: 0,
      failure_kind: null,
      failure_acked_at: null,
      job_in_flight: null,
      has_previous_url: 0,
      previous_youtube_kind: null,
      revert_redundant: 0,
    },
    overrides || {},
  );
}

const acts = (entries) => entries.map((e) => e.act);

// ── computeSrcLetter ─────────────────────────────────────────

test("computeSrcLetter: '-' when nothing is themed or placed", () => {
  assert.equal(computeSrcLetter(row()), "-");
});

test("computeSrcLetter: T when placed with sourceKind=themerrdb", () => {
  assert.equal(
    computeSrcLetter(
      row({ media_folder: "/m/movies/T", source_kind: "themerrdb" }),
    ),
    "T",
  );
});

test("computeSrcLetter: U when placed with sourceKind=url", () => {
  assert.equal(
    computeSrcLetter(row({ media_folder: "/m/movies/T", source_kind: "url" })),
    "U",
  );
});

test("computeSrcLetter: A when placed with sourceKind=adopt", () => {
  assert.equal(
    computeSrcLetter(
      row({ media_folder: "/m/movies/T", source_kind: "adopt" }),
    ),
    "A",
  );
});

test("computeSrcLetter: M when sidecar-only", () => {
  assert.equal(
    computeSrcLetter(row({ media_folder: null, plex_local_theme: true })),
    "M",
  );
});

test("computeSrcLetter: P when plex_has_theme without local sidecar", () => {
  assert.equal(
    computeSrcLetter(row({ plex_has_theme: true, plex_local_theme: false })),
    "P",
  );
});

// ── SOURCE menu ──────────────────────────────────────────────

test("SOURCE on '-' row offers DOWNLOAD TDB + SET URL + UPLOAD MP3", () => {
  const m = computeMenuActions(row());
  const src = acts(m.source);
  assert.deepEqual(src, ["redl", "manual-url", "upload-theme"]);
  // The redl entry on a '-' row should label as DOWNLOAD TDB (not RE-).
  assert.equal(m.source.find((e) => e.act === "redl").label, "DOWNLOAD TDB");
});

test("SOURCE on T row offers RE-DOWNLOAD TDB (not DOWNLOAD)", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
    }),
  );
  const redl = m.source.find((e) => e.act === "redl");
  assert.ok(redl, "redl present on T row");
  assert.equal(redl.label, "RE-DOWNLOAD TDB");
});

test("SOURCE on M row hides DOWNLOAD TDB, offers ADOPT and REPLACE TDB", () => {
  const m = computeMenuActions(
    row({ media_folder: null, plex_local_theme: true }),
  );
  const src = acts(m.source);
  assert.ok(src.includes("adopt-sidecar"));
  assert.ok(src.includes("replace-with-themerrdb"));
  assert.ok(!src.includes("redl"), "redl must be hidden on sidecar-only");
});

test("SOURCE on U row hides DOWNLOAD TDB, offers REPLACE TDB", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "manual",
      source_kind: "url",
      file_path: "movie/Test (2024)/theme.mp3",
      source_video_id: "abc12345678",
    }),
  );
  const src = acts(m.source);
  assert.ok(!src.includes("redl"));
  assert.ok(src.includes("replace-with-themerrdb"));
});

test("SOURCE on U row STILL offers REPLACE TDB after a stale accepted_update (v1.24.72)", () => {
  // the user's Super Mario Galaxy: accepted a TDB update long ago, then
  // overrode with a user URL (SRC=U). accepted_update stays sticky=1 but
  // the active theme is the user URL, so REPLACE TDB is meaningful — the
  // bare !accepted_update gate wrongly hid it.
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "manual",
      source_kind: "url",
      file_path: "movie/Test (2024)/theme.mp3",
      source_video_id: "abc12345678",
      accepted_update: 1,
    }),
  );
  const r = row({
    media_folder: "/m/movies/T",
    placement_provenance: "manual",
    source_kind: "url",
    accepted_update: 1,
  });
  assert.equal(computeSrcLetter(r), "U", "row must classify as SRC=U");
  assert.ok(
    acts(m.source).includes("replace-with-themerrdb"),
    "REPLACE TDB must survive a sticky accepted_update on a non-T row",
  );
});

test("SOURCE: ACCEPT UPDATE hidden on src='-' even with pending_update (v1.12.51)", () => {
  const m = computeMenuActions(
    row({ pending_update: 1, actionable_update: 1 }),
  );
  const src = acts(m.source);
  assert.ok(
    !src.includes("accept-update"),
    "ACCEPT UPDATE must not appear on src='-' rows",
  );
  // DOWNLOAD TDB should be the prompt instead.
  assert.ok(src.includes("redl"));
});

test("SOURCE: ACCEPT + KEEP CURRENT visible on themed row with actionable_update", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
      pending_update: 1,
      actionable_update: 1,
    }),
  );
  const src = acts(m.source);
  assert.ok(src.includes("accept-update"));
  assert.ok(src.includes("decline-update"));
});

test("SOURCE: KEEP CURRENT hides once decision flips to declined", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
      pending_update: 1,
      actionable_update: 0, // declined
    }),
  );
  const src = acts(m.source);
  assert.ok(src.includes("accept-update"), "ACCEPT stays for the declined kind");
  assert.ok(!src.includes("decline-update"), "KEEP CURRENT hidden after decline");
});

test("SOURCE: DOWNLOAD TDB hidden on T row with pending_update (ACCEPT covers)", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
      pending_update: 1,
    }),
  );
  const src = acts(m.source);
  assert.ok(!src.includes("redl"), "redl suppressed when pending_update is up");
});

test("SOURCE: DOWNLOAD TDB returns on src='-' even with stale accepted_update (v1.12.78)", () => {
  // After PURGE on a section that previously did urls_match ACCEPT,
  // pending_updates(decision='accepted') survives globally — but on
  // src='-' the redundancy gate should release.
  const m = computeMenuActions(
    row({ accepted_update: 1, has_previous_url: 1, previous_youtube_kind: "user" }),
  );
  const src = acts(m.source);
  assert.ok(src.includes("redl"), "DOWNLOAD TDB must reappear on src='-'");
  assert.ok(src.includes("revert"), "RESTORE shown post-PURGE with previous URL");
});

test("SOURCE: REVERT hidden when previous_youtube_kind is themerrdb (v1.12.65)", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
      has_previous_url: 1,
      previous_youtube_kind: "themerrdb",
    }),
  );
  const src = acts(m.source);
  assert.ok(!src.includes("revert"));
});

test("SOURCE: REVERT visible when previous_youtube_kind is user", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
      has_previous_url: 1,
      previous_youtube_kind: "user",
    }),
  );
  const src = acts(m.source);
  assert.ok(src.includes("revert"));
  assert.equal(m.source.find((e) => e.act === "revert").label, "REVERT");
});

test("SOURCE: revert button labels as RESTORE on src='-' (v1.12.79)", () => {
  const m = computeMenuActions(
    row({ has_previous_url: 1, previous_youtube_kind: "user" }),
  );
  const revert = m.source.find((e) => e.act === "revert");
  assert.ok(revert, "revert entry present");
  assert.equal(revert.label, "RESTORE");
});

test("SOURCE: REVERT hidden when revert_redundant is set", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
      has_previous_url: 1,
      previous_youtube_kind: "user",
      revert_redundant: 1,
    }),
  );
  const src = acts(m.source);
  assert.ok(!src.includes("revert"));
});

test("SOURCE: RESTORE shown on src='M' even when captured URL is themerrdb-kind (v1.12.81)", () => {
  // Post-UNMANAGE the row flips to an M sidecar with previous_youtube_kind
  // captured from the dropped theme — which may be 'themerrdb', not 'user'.
  // A bare kind==='user' gate wrongly hid the recovery button; the
  // srcLetter==='M' branch of restoreEligibleKind brings it back, labelled
  // RESTORE, with the tone tracking the captured kind (green for TDB).
  const m = computeMenuActions(
    row({
      media_folder: null,
      plex_local_theme: true,
      has_previous_url: 1,
      previous_youtube_kind: "themerrdb",
    }),
  );
  const r = m.source.find((e) => e.act === "revert");
  assert.ok(r, "RESTORE present on M row with a themerrdb-kind previous URL");
  assert.equal(r.label, "RESTORE", "labels RESTORE on src='M'");
  assert.equal(r.tone, "themerrdb", "tone tracks the captured kind");
});

test("SOURCE: ACK FAILURE removed from menu (v1.12.87 — INFO-only)", () => {
  // Pre-v1.12.87 the SOURCE menu surfaced ACK FAILURE alongside
  // recovery actions; v1.12.87 moves it to the INFO card's
  // // TRY THIS NEXT section as the single entry-point so users
  // see the failure context (raw yt-dlp message + recovery
  // options) before dismissing. The row's red ! glyph also
  // routes through INFO. Verify clear-failure never appears in
  // the SOURCE list, regardless of failure / ack state.
  const themed = {
    media_folder: "/m/movies/T",
    placement_provenance: "auto",
    source_kind: "themerrdb",
    file_path: "movie/Test (2024)/theme.mp3",
  };
  const open = computeMenuActions(
    row(Object.assign({}, themed, { failure_kind: "video_private" })),
  );
  assert.ok(!acts(open.source).includes("clear-failure"));
  const acked = computeMenuActions(
    row(
      Object.assign({}, themed, {
        failure_kind: "video_private",
        failure_acked_at: "2026-01-01T00:00:00Z",
      }),
    ),
  );
  assert.ok(!acts(acked.source).includes("clear-failure"));
});

test("SOURCE: REPLACE TDB blocked on permanent failure_kind", () => {
  const m = computeMenuActions(
    row({
      media_folder: null,
      plex_local_theme: true,
      failure_kind: "video_removed",
    }),
  );
  const src = acts(m.source);
  assert.ok(!src.includes("replace-with-themerrdb"));
});

test("SOURCE: cookies_expired blocks REPLACE TDB unless cookies present", () => {
  const r = row({
    media_folder: null,
    plex_local_theme: true,
    failure_kind: "cookies_expired",
  });
  const blocked = computeMenuActions(r, { cookiesPresent: false });
  assert.ok(!acts(blocked.source).includes("replace-with-themerrdb"));
  const allowed = computeMenuActions(r, { cookiesPresent: true });
  assert.ok(acts(allowed.source).includes("replace-with-themerrdb"));
});

test("SOURCE: REPLACE TDB hidden when TDB-tracked row has no youtube_url (v1.24.71)", () => {
  // A title can be ThemerrDB-TRACKED (upstream_source set) but have no
  // theme video yet (themes.youtube_url empty — the user's Daredevil: Born
  // Again repro). REPLACE TDB on such a row 409'd "record has no
  // youtube_url"; the gate must skip it. M (sidecar-only) row so the
  // placement clause is satisfied and youtube_url is the only blocker.
  const withUrl = computeMenuActions(
    row({ media_folder: null, plex_local_theme: true }),
  );
  assert.ok(
    acts(withUrl.source).includes("replace-with-themerrdb"),
    "control: REPLACE TDB shows when youtube_url is present",
  );
  const noUrl = computeMenuActions(
    row({ media_folder: null, plex_local_theme: true, youtube_url: "" }),
  );
  assert.ok(
    !acts(noUrl.source).includes("replace-with-themerrdb"),
    "REPLACE TDB must be hidden when themes.youtube_url is empty",
  );
  // ADOPT (the sidecar's own action) still shows — only TDB is gated.
  assert.ok(acts(noUrl.source).includes("adopt-sidecar"));
});

test("SOURCE: REPLACE TDB hidden on an LPS row that already holds a canonical (v1.14.46)", () => {
  // LPS = Plex serves its own theme (isPlexAgent) AND motif already has
  // the canonical downloaded (from a prior backup / LET PLEX SERVE).
  // REPLACE TDB would re-download wastefully — PUSH TO PLEX reuses the
  // existing file — so the gate's !lpsHasCanonical clause hides it.
  const lps = computeMenuActions(
    row({
      plex_has_theme: true,
      plex_local_theme: false,
      media_folder: null,
      file_path: "movie/Test (2024)/theme.mp3",
    }),
  );
  assert.ok(
    !acts(lps.source).includes("replace-with-themerrdb"),
    "REPLACE TDB must be hidden on an LPS row with a non-cloud canonical",
  );
  // A pure-P row (Plex serves, NO motif canonical yet) still offers it —
  // there's no local file to PUSH, so REPLACE TDB is the right entry.
  const pureP = computeMenuActions(
    row({ plex_has_theme: true, plex_local_theme: false }),
  );
  assert.ok(
    acts(pureP.source).includes("replace-with-themerrdb"),
    "control: pure-P row (no canonical) still offers REPLACE TDB",
  );
});

test("SOURCE: REPLACE TDB still offered on a new_theme_available pending row (v1.20.2)", () => {
  // Net-new TDB themes surface as pending_update_kind='new_theme_available'
  // (esp. on SRC=P rows). A bare !it.pending_update would hide the whole TDB
  // toolkit whenever the blue !UPD pill was up; tdbActionPendingOk lets the
  // new-theme kind through so REPLACE TDB stays available.
  const newTheme = computeMenuActions(
    row({
      plex_has_theme: true,
      plex_local_theme: false,
      pending_update: 1,
      pending_update_kind: "new_theme_available",
    }),
  );
  assert.ok(
    acts(newTheme.source).includes("replace-with-themerrdb"),
    "REPLACE TDB must survive a new_theme_available pending update",
  );
  // A regular upstream_changed pending update still suppresses it (ACCEPT
  // UPDATE is the contextually-correct CTA there).
  const upstreamChanged = computeMenuActions(
    row({
      plex_has_theme: true,
      plex_local_theme: false,
      pending_update: 1,
      pending_update_kind: "upstream_changed",
    }),
  );
  assert.ok(
    !acts(upstreamChanged.source).includes("replace-with-themerrdb"),
    "control: upstream_changed pending still suppresses REPLACE TDB",
  );
});

test("SOURCE: RE-DOWNLOAD TDB hidden on a plex_orphan row (v1.17.17)", () => {
  // A plex_orphan has no ThemerrDB URL to (re-)download from — its
  // youtube_url is the ADOPT-captured URL, not a TDB one. RE-DOWNLOAD TDB
  // on a NO-TDB orphan 409'd (the user's screenshot). The !isOrphan clause
  // keeps it off. Use a downloaded-but-unplaced orphan so the OTHER redl
  // blockers (sidecar/plexAgent/manualPlacement) don't independently hide it.
  const orphan = computeMenuActions(
    row({
      upstream_source: "plex_orphan",
      file_path: "movie/Test (2024)/theme.mp3",
    }),
  );
  assert.ok(
    !acts(orphan.source).includes("redl"),
    "RE-DOWNLOAD TDB must be hidden on a plex_orphan row",
  );
  // The same state on a real TDB row DOES offer redl (control).
  const tdb = computeMenuActions(
    row({ file_path: "movie/Test (2024)/theme.mp3" }),
  );
  assert.ok(acts(tdb.source).includes("redl"));
});

// ── PLACE menu ───────────────────────────────────────────────

test("PLACE: PUSH TO PLEX appears on downloaded-but-not-placed row", () => {
  const m = computeMenuActions(
    row({ file_path: "movie/Test (2024)/theme.mp3" }),
  );
  const place = m.place;
  assert.equal(place.length, 1);
  assert.equal(place[0].act, "replace");
  assert.equal(place[0].label, "PUSH TO PLEX");
});

test("PLACE: RE-PUSH replaces PUSH on downloaded+placed row", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
    }),
  );
  const place = m.place;
  assert.equal(place.length, 1);
  assert.equal(place[0].label, "RE-PUSH");
});

test("PLACE: mismatch state surfaces three-way resolution", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "manual",
      source_kind: "url",
      file_path: "movie/Test (2024)/theme.mp3",
      mismatch_state: "pending",
    }),
  );
  const labels = m.place.map((e) => e.label);
  assert.deepEqual(labels, [
    "PUSH TO PLEX",
    "ADOPT FROM PLEX",
    "KEEP MISMATCH",
  ]);
});

test("PLACE: dlBroken row offers RESTORE FROM PLEX", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
      canonical_missing: true,
    }),
  );
  const place = m.place;
  assert.equal(place.length, 1);
  assert.equal(place[0].act, "restore-canonical");
});

// ── REMOVE menu ──────────────────────────────────────────────

test("REMOVE on T row: DEL + UNMANAGE + PURGE", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
    }),
  );
  const rem = acts(m.remove);
  assert.deepEqual(rem, ["unplace", "unmanage", "purge"]);
});

test("REMOVE on src='-' row: nothing (no destructive action available)", () => {
  const m = computeMenuActions(row());
  assert.equal(m.remove.length, 0);
});

test("REMOVE: CLEAR URL appears whenever has_previous_url is set", () => {
  const m = computeMenuActions(
    row({ has_previous_url: 1, previous_youtube_kind: "user" }),
  );
  const rem = acts(m.remove);
  assert.ok(rem.includes("clear-url"), "CLEAR URL gates on has_previous_url alone");
});

test("REMOVE: PURGE on DL-only row sets dlOnly flag", () => {
  const m = computeMenuActions(
    row({ file_path: "movie/Test (2024)/theme.mp3" }),
  );
  const purge = m.remove.find((e) => e.act === "purge");
  assert.ok(purge);
  assert.equal(purge.dlOnly, true);
});

test("REMOVE: PURGE hidden on a zombie orphan with nothing left to purge (v1.18.8)", () => {
  // Post-PURGE an orphan is a zombie: URLs nulled, no canonical, no
  // placement. The old `downloaded || isOrphan` gate kept PURGE visible
  // (idempotent no-op — the user: "after purge you're left with PURGE again
  // which doesn't do anything"). orphanHasPurgeableState hides it once the
  // row owns nothing.
  const zombie = computeMenuActions(
    row({
      upstream_source: "plex_orphan",
      youtube_url: "",
      file_path: null,
      media_folder: null,
    }),
  );
  assert.ok(
    !acts(zombie.remove).includes("purge"),
    "PURGE must be hidden on a zombie orphan",
  );
  // An orphan that still owns a captured URL keeps PURGE (control).
  const liveOrphan = computeMenuActions(
    row({ upstream_source: "plex_orphan" }),
  );
  assert.ok(
    acts(liveOrphan.remove).includes("purge"),
    "control: orphan with a youtube_url still offers PURGE",
  );
});

// ── Section-scope contract ───────────────────────────────────

test("section-scoped flag is set for per-edition actions", () => {
  const m = computeMenuActions(
    row({
      media_folder: "/m/movies/T",
      placement_provenance: "auto",
      source_kind: "themerrdb",
      file_path: "movie/Test (2024)/theme.mp3",
      has_previous_url: 1,
      previous_youtube_kind: "user",
    }),
  );
  const scoped = (entries) =>
    entries.filter((e) => e.sectionScoped).map((e) => e.act);
  // RE-DOWNLOAD, REVERT in source; DEL/UNMANAGE/PURGE in remove.
  assert.ok(scoped(m.source).includes("redl"));
  assert.ok(scoped(m.source).includes("revert"));
  assert.ok(scoped(m.remove).includes("unplace"));
  assert.ok(scoped(m.remove).includes("unmanage"));
  assert.ok(scoped(m.remove).includes("purge"));
});
