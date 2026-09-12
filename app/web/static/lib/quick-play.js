// v0.51.333: row quick-play — WHAT a library row plays when its leading ▶
// is clicked (spec docs/specs/ROW_QUICK_PLAY_SPEC.md § 4). A dual-export
// module like menu-actions.js, but unlike that hand-mirrored subset this
// file IS the live code: base.html loads it before app.js (window.
// motifQuickPlay) and tests/js/test_quick_play.js requires the same file,
// so the rule cannot drift between the UI and its tests.
//
// The rule mirrors the INFO card's headline ("what motif holds · what
// plays", _derivePlaybackSourceLabel in app.js), decided from the fields
// the row already carries — the same inputs computeSrcLetter reads plus
// last_place_attempt_reason:
//
//   motif's file on disk, not standing by as a backup  → motif's file
//       (placed → "(placed)", else "(not placed yet)")
//   motif's file on disk as a backup, Plex serves      → what Plex serves
//   motif's file on disk as a backup, Plex silent      → motif's file
//   nothing on disk, Plex serves its own / a sidecar   → what Plex serves
//   nothing, or the canonical is missing (dlBroken)    → null (no control)
//
// Pure: no DOM, no fetch, no globals.
(function (root, factory) {
  if (typeof module === "object" && typeof module.exports === "object") {
    module.exports = factory();
  } else {
    root.motifQuickPlay = factory();
  }
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  var TIP_FILE_PLACED = "Play — motif's file (placed)";
  var TIP_FILE_UNPLACED = "Play — motif's file (not placed yet)";
  var TIP_PLEX_STANDBY = "Play — what Plex serves (motif's copy stands by)";
  var TIP_PLEX = "Play — what Plex serves";

  function hasThemeIds(it) {
    return it.theme_media_type !== undefined && it.theme_media_type !== null
      && it.theme_media_type !== ""
      && it.theme_tmdb !== undefined && it.theme_tmdb !== null && it.theme_tmdb !== "";
  }

  // The items endpoint the INFO card's "motif file" player uses (v1.12.90 /
  // v1.21.90): section_id scopes the local_files row, rating_key picks THIS
  // edition's canonical.
  function fileSrc(it) {
    var q = [];
    if (it.section_id) q.push("section_id=" + encodeURIComponent(String(it.section_id)));
    if (it.rating_key) q.push("rating_key=" + encodeURIComponent(String(it.rating_key)));
    return "/api/items/" + encodeURIComponent(String(it.theme_media_type))
      + "/" + encodeURIComponent(String(it.theme_tmdb)) + "/theme.mp3"
      + (q.length ? "?" + q.join("&") : "");
  }

  // The v0.51.322 proxy of what Plex serves; digits-only rating keys only.
  function plexSrc(it) {
    return "/api/plex/theme/" + encodeURIComponent(String(it.rating_key)) + ".mp3";
  }

  function computeQuickPlay(it) {
    if (!it) return null;
    var placed = !!it.media_folder || it.placement_kind === "plex_upload";
    var downloaded = !!it.file_path;
    var dlBroken = !!it.canonical_missing && !!it.file_path;
    var verified = it.plex_theme_verified_ok;
    var verifiedOk = verified === null || verified === undefined || verified === 1;
    var rkOk = /^\d+$/.test(String(it.rating_key === undefined || it.rating_key === null ? "" : it.rating_key));
    var plexServes = !!it.plex_has_theme && verifiedOk && rkOk;
    var isBackupOnly = !placed && downloaded && it.last_place_attempt_reason === "backup_only";
    var fileOk = downloaded && hasThemeIds(it);

    if (dlBroken) return null;
    if (fileOk && !isBackupOnly) {
      return { kind: "file", src: fileSrc(it), tip: placed ? TIP_FILE_PLACED : TIP_FILE_UNPLACED };
    }
    if (isBackupOnly && plexServes) {
      return { kind: "plex", src: plexSrc(it), tip: TIP_PLEX_STANDBY };
    }
    if (isBackupOnly && fileOk) {
      return { kind: "file", src: fileSrc(it), tip: TIP_FILE_UNPLACED };
    }
    if (plexServes) {
      return { kind: "plex", src: plexSrc(it), tip: TIP_PLEX };
    }
    return null;
  }

  // v0.51.334 (tag 2): m:ss for the NOW PLAYING strip; an unknown or
  // infinite duration (still loading, a live stream) reads as an en-dash clock.
  function formatClock(seconds) {
    if (typeof seconds !== "number" || !isFinite(seconds) || seconds < 0) return "\u2013:\u2013\u2013";
    var s = Math.floor(seconds);
    var m = Math.floor(s / 60);
    var r = s % 60;
    return m + ":" + (r < 10 ? "0" : "") + r;
  }

  return {
    computeQuickPlay: computeQuickPlay,
    formatClock: formatClock,
    TIPS: {
      FILE_PLACED: TIP_FILE_PLACED,
      FILE_UNPLACED: TIP_FILE_UNPLACED,
      PLEX_STANDBY: TIP_PLEX_STANDBY,
      PLEX: TIP_PLEX,
    },
  };
});
