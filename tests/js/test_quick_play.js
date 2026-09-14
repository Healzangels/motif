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
const { isDeepStrictEqual } = require("node:util");
const { computeQuickPlay, fileSrc, plexSrc, formatClock, TIPS } = require("../../app/web/static/lib/quick-play.js");

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

// v0.51.334 (tag 2): the NOW PLAYING clock.
test("formatClock: m:ss with a zero-padded second field", () => {
  assert.equal(formatClock(0), "0:00");
  assert.equal(formatClock(7.9), "0:07");
  assert.equal(formatClock(72.6), "1:12");
  assert.equal(formatClock(600), "10:00");
  assert.equal(formatClock(3661), "61:01");
});

test("formatClock: unknown, infinite or negative durations read as an en-dash clock", () => {
  const dash = "\u2013:\u2013\u2013";
  assert.equal(formatClock(NaN), dash);
  assert.equal(formatClock(Infinity), dash);
  assert.equal(formatClock(-1), dash);
  assert.equal(formatClock(undefined), dash);
  assert.equal(formatClock("12"), dash);
});

// v0.51.343: the INFO card's players build their URLs with the row's builders — pinned for each site's shape.
test("fileSrc: the INFO card's motif-file and loudness-preview shape", () => {
  const card = (mt, tmdb, section, rk) => fileSrc({ theme_media_type: mt, theme_tmdb: tmdb, section_id: section, rating_key: rk });
  assert.equal(card("tv", 777, "3", "1001"), "/api/items/tv/777/theme.mp3?section_id=3&rating_key=1001");
  assert.equal(card("movie", 120, "1", undefined), "/api/items/movie/120/theme.mp3?section_id=1");
  assert.equal(card("movie", 120, "", "222"), "/api/items/movie/120/theme.mp3?rating_key=222");
  assert.equal(card("collection", -12, undefined, undefined), "/api/items/collection/-12/theme.mp3");
  assert.equal(card("tv", 777, null, 0), "/api/items/tv/777/theme.mp3");
  assert.equal(card("tv", "7 7", "a b", "rk&1"), "/api/items/tv/7%207/theme.mp3?section_id=a%20b&rating_key=rk%261");
  assert.equal(card("movie", 5, 4, 9001), "/api/items/movie/5/theme.mp3?section_id=4&rating_key=9001");
});

test("plexSrc: the bare card's row shape and the full card's key-only shape", () => {
  assert.equal(plexSrc({ rating_key: "778" }), "/api/plex/theme/778.mp3");
  assert.equal(plexSrc({ rating_key: 778 }), "/api/plex/theme/778.mp3");
  assert.equal(plexSrc(row({ plex_has_theme: 1 })), PLEX);
});

test("the row's ▶ plays exactly the URL the shared builders build", () => {
  const f = row({ file_path: "/t.mp3" });
  assert.equal(computeQuickPlay(f).src, fileSrc(f));
  const p = row({ plex_has_theme: 1 });
  assert.equal(computeQuickPlay(p).src, plexSrc(p));
});

// v0.51.343: the pre-.343 four-way ladder, spelled out as the oracle for the collapsed rule.
function ladder(it) {
  const placed = !!it.media_folder || it.placement_kind === "plex_upload";
  const downloaded = !!it.file_path;
  if (!!it.canonical_missing && downloaded) return null;
  const v = it.plex_theme_verified_ok;
  const rk = it.rating_key === undefined || it.rating_key === null ? "" : it.rating_key;
  const plexServes = !!it.plex_has_theme && (v === null || v === undefined || v === 1) && /^\d+$/.test(String(rk));
  const backup = !placed && downloaded && it.last_place_attempt_reason === "backup_only";
  const ids = [it.theme_media_type, it.theme_tmdb].every((x) => x !== undefined && x !== null && x !== "");
  const fileOk = downloaded && ids;
  if (fileOk && !backup) return { kind: "file", src: fileSrc(it), tip: placed ? TIPS.FILE_PLACED : TIPS.FILE_UNPLACED };
  if (backup && plexServes) return { kind: "plex", src: plexSrc(it), tip: TIPS.PLEX_STANDBY };
  if (backup && fileOk) return { kind: "file", src: fileSrc(it), tip: TIPS.FILE_UNPLACED };
  if (plexServes) return { kind: "plex", src: plexSrc(it), tip: TIPS.PLEX };
  return null;
}

test("computeQuickPlay equals the four-way ladder over every combination of its predicate inputs", () => {
  const axes = [
    ["media_folder", [null, "", "/x"]],
    ["placement_kind", [null, "plex_upload", "hardlink"]],
    ["file_path", [null, "", "/t.mp3"]],
    ["canonical_missing", [0, 1, null]],
    ["plex_has_theme", [0, 1, null]],
    ["plex_theme_verified_ok", [null, undefined, 0, 1, 2]],
    ["rating_key", ["1001", "rk-1", "", null, undefined, 778, "0"]],
    ["last_place_attempt_reason", [null, "backup_only", "placement_failed"]],
    ["theme_media_type", ["tv", null, undefined, ""]],
    ["theme_tmdb", [777, null, undefined, "", 0]],
  ];
  const outcomes = new Set();
  let rows = 0;
  const walk = (i, it) => {
    if (i === axes.length) {
      const want = ladder(it);
      const got = computeQuickPlay(it);
      if (!isDeepStrictEqual(got, want)) assert.fail(`${JSON.stringify(it)}: ${JSON.stringify(got)} != ${JSON.stringify(want)}`);
      outcomes.add(want ? `${want.kind}|${want.tip}` : "null");
      rows += 1;
      return;
    }
    const [key, values] = axes[i];
    for (const value of values) walk(i + 1, Object.assign({}, it, { [key]: value }));
  };
  walk(0, { section_id: "3" });
  assert.equal(rows, axes.reduce((n, [, values]) => n * values.length, 1));
  assert.deepEqual([...outcomes].sort(), [
    `file|${TIPS.FILE_PLACED}`, `file|${TIPS.FILE_UNPLACED}`, "null", `plex|${TIPS.PLEX}`, `plex|${TIPS.PLEX_STANDBY}`,
  ].sort(), "the grid reaches every outcome the rule has");
});
