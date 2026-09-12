// v0.51.333: behavioural pins for the row quick-play rule (spec § 4).
// Unlike test_menu_actions.js, the module under test is the LIVE code —
// base.html loads app/web/static/lib/quick-play.js before app.js — so
// these subtests exercise exactly what the row's ▶ does.
//
// Run: `node --test tests/js/test_quick_play.js`
// (wrapped for the pytest gate by tests/test_v0_51_333_row_quick_play.py)
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { computeQuickPlay, TIPS } = require("../../app/web/static/lib/quick-play.js");

// A row with nothing anywhere; each subtest flips only what it cares about.
function row(overrides) {
  return Object.assign(
    {
      theme_media_type: "tv",
      theme_tmdb: 777,
      section_id: "3",
      rating_key: "1001",
      media_folder: null,
      placement_kind: null,
      file_path: null,
      canonical_missing: 0,
      plex_has_theme: 0,
      plex_theme_verified_ok: null,
      last_place_attempt_reason: null,
      plex_local_theme: 0,
    },
    overrides || {},
  );
}

const FILE = "/api/items/tv/777/theme.mp3?section_id=3&rating_key=1001";
const PLEX = "/api/plex/theme/1001.mp3";

test("placed file (T/U/AT/A) plays motif's file, tip says placed", () => {
  const r = computeQuickPlay(row({ media_folder: "/x", file_path: "/t.mp3", source_kind: "url" }));
  assert.deepEqual(r, { kind: "file", src: FILE, tip: TIPS.FILE_PLACED });
});

test("plex_upload placement counts as placed", () => {
  const r = computeQuickPlay(row({ placement_kind: "plex_upload", file_path: "/t.mp3" }));
  assert.equal(r.kind, "file");
  assert.equal(r.tip, TIPS.FILE_PLACED);
});

test("file on disk, not placed, no backup stamp → motif's file, not placed yet", () => {
  const r = computeQuickPlay(row({ file_path: "/t.mp3" }));
  assert.deepEqual(r, { kind: "file", src: FILE, tip: TIPS.FILE_UNPLACED });
});

test("backup-only file while Plex serves → what Plex serves, copy stands by", () => {
  const r = computeQuickPlay(row({
    file_path: "/t.mp3", last_place_attempt_reason: "backup_only", plex_has_theme: 1,
  }));
  assert.deepEqual(r, { kind: "plex", src: PLEX, tip: TIPS.PLEX_STANDBY });
});

test("backup-only file but Plex silent → motif's file, not placed yet", () => {
  const r = computeQuickPlay(row({ file_path: "/t.mp3", last_place_attempt_reason: "backup_only" }));
  assert.deepEqual(r, { kind: "file", src: FILE, tip: TIPS.FILE_UNPLACED });
});

test("backup stamp on a PLACED row is ignored (it is placed, so motif's file plays)", () => {
  const r = computeQuickPlay(row({
    media_folder: "/x", file_path: "/t.mp3", last_place_attempt_reason: "backup_only", plex_has_theme: 1,
  }));
  assert.equal(r.kind, "file");
  assert.equal(r.tip, TIPS.FILE_PLACED);
});

test("nothing on disk, Plex serves its own (P) → what Plex serves", () => {
  const r = computeQuickPlay(row({ plex_has_theme: 1, plex_theme_verified_ok: 1 }));
  assert.deepEqual(r, { kind: "plex", src: PLEX, tip: TIPS.PLEX });
});

test("sidecar only (M) with Plex serving → what Plex serves", () => {
  const r = computeQuickPlay(row({ plex_local_theme: 1, plex_has_theme: 1 }));
  assert.equal(r.kind, "plex");
  assert.equal(r.src, PLEX);
});

test("Plex claims a theme but the last probe said 404 (verified_ok=0) → no control", () => {
  assert.equal(computeQuickPlay(row({ plex_has_theme: 1, plex_theme_verified_ok: 0 })), null);
});

test("nothing anywhere (–) → no control", () => {
  assert.equal(computeQuickPlay(row()), null);
});

test("canonical missing (dlBroken) → no control even when placed", () => {
  assert.equal(computeQuickPlay(row({ media_folder: "/x", file_path: "/t.mp3", canonical_missing: 1 })), null);
});

test("Plex branch needs a digits-only rating key", () => {
  assert.equal(computeQuickPlay(row({ plex_has_theme: 1, rating_key: "rk-1" })), null);
  assert.equal(computeQuickPlay(row({ plex_has_theme: 1, rating_key: "" })), null);
});

test("file branch needs the theme ids the items endpoint is keyed on", () => {
  assert.equal(computeQuickPlay(row({ file_path: "/t.mp3", theme_media_type: undefined })), null);
  assert.equal(computeQuickPlay(row({ file_path: "/t.mp3", theme_tmdb: null })), null);
});

test("file src encodes the section and omits absent keys", () => {
  const r = computeQuickPlay(row({ file_path: "/t.mp3", section_id: "a b", rating_key: null }));
  assert.equal(r.src, "/api/items/tv/777/theme.mp3?section_id=a%20b");
  const bare = computeQuickPlay(row({ file_path: "/t.mp3", section_id: null, rating_key: null }));
  assert.equal(bare.src, "/api/items/tv/777/theme.mp3");
});

test("null row → null", () => {
  assert.equal(computeQuickPlay(null), null);
});
