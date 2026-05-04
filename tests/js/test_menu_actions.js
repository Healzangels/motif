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
