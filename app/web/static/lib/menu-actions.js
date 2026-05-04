// v1.12.81: SOURCE / PLACE / REMOVE menu gating, extracted from
// renderLibraryRow into a dual-export module so the same code is
// loaded by the browser (window.motifMenu) and by Node tests
// (require('./menu-actions')). Pure functions — no DOM, no fetch,
// no globals. The HTML rendering still lives inline in app.js;
// this file owns "WHICH actions appear in WHICH menu" and the
// labels/tones/extras each one carries.
//
// Mirrors the action-gating matrix as documented in the inline
// comments of renderLibraryRow. Keeping the predicates here means
// the tests in tests/js/test_menu_actions.js can pin the expected
// menu set per (src state, modifier) tuple — regressions like the
// v1.12.51 "ACCEPT UPDATE on src='-'" footgun would now fail a
// unit test before reaching the UI.

(function (root, factory) {
  if (typeof module === "object" && typeof module.exports === "object") {
    module.exports = factory();
  } else {
    root.motifMenu = factory();
  }
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  const TDB_DEAD_FAILURES = new Set([
    "video_private",
    "video_removed",
    "video_age_restricted",
    "geo_blocked",
  ]);

  // Mirrors renderLibraryRow's `computeSrcLetter`. T/U/A/M/P/-
  // tracks v1.10.33-onwards; see app.js:3134-3154 for the
  // canonical comment block.
  function computeSrcLetter(it) {
    const placed = !!it.media_folder;
    const placedProv = it.placement_provenance;
    const sidecarOnly = !placed && !!it.plex_local_theme;
    const isOrphanRow = it.upstream_source === "plex_orphan";
    const sourceKind = it.source_kind || null;
    const svid = it.source_video_id || "";
    const looksLikeYoutubeId = /^[A-Za-z0-9_-]{11}$/.test(svid);
    if (placed && sourceKind === "themerrdb") return "T";
    if (placed && sourceKind === "adopt") return "A";
    if (placed && (sourceKind === "url" || sourceKind === "upload")) return "U";
    if (placed && placedProv === "auto") return "T";
    if (placed && placedProv === "manual") {
      const wasUploadedOrUrl = svid === "" || looksLikeYoutubeId;
      return !isOrphanRow || wasUploadedOrUrl ? "U" : "A";
    }
    if (sidecarOnly) return "M";
    if (it.plex_has_theme) return "P";
    return "-";
  }

  // Compute every flag renderLibraryRow needs to gate menu items.
  // Pulled into one place so tests can assert on the derivation.
  function deriveRowFlags(it) {
    const themed = it.theme_tmdb !== null && it.theme_tmdb !== undefined;
    const placed = !!it.media_folder;
    const placedProv = it.placement_provenance;
    const sidecarOnly = !placed && !!it.plex_local_theme;
    const downloaded = !!it.file_path;
    const dlBroken =
      themed && downloaded && it.file_path && it.canonical_missing === true;
    const isMismatch = !!it.mismatch_state && it.mismatch_state !== "none";
    const isOrphan = it.upstream_source === "plex_orphan";
    const isThemerrDb = it.upstream_source && it.upstream_source !== "plex_orphan";
    const isManual = it.provenance === "manual";
    const isManualPlacement = placed && placedProv === "manual";
    const isPlexAgent = !placed && !it.plex_local_theme && !!it.plex_has_theme;
    const lockManualActions = !!it.job_in_flight;
    const sourceKindForActions = (() => {
      if (it.source_kind) return it.source_kind;
      if (!isManual) return null;
      const svid = it.source_video_id || "";
      if (svid === "") return "upload";
      if (/^[A-Za-z0-9_-]{11}$/.test(svid)) return "url";
      return "adopt";
    })();
    return {
      themed,
      placed,
      placedProv,
      sidecarOnly,
      downloaded,
      dlBroken,
      isMismatch,
      isOrphan,
      isThemerrDb,
      isManual,
      isManualPlacement,
      isPlexAgent,
      lockManualActions,
      sourceKindForActions,
    };
  }

  // Returns { source: [...], place: [...], remove: [...] } where
  // each entry is { act, label, tone?, danger?, warn?, info?,
  //                 bypassLock?, dlOnly?, sectionScoped? }.
  // The renderer in app.js maps these to <button> elements; the
  // test harness asserts on `act` (and optionally on label/tone).
  function computeMenuActions(it, opts) {
    opts = opts || {};
    const cookiesPresent = !!opts.cookiesPresent;
    const themeId = it.theme_tmdb;
    const flags = deriveRowFlags(it);
    const srcLetter = computeSrcLetter(it);

    const source = [];
    const place = [];
    const remove = [];

    // ── 1. CONTEXTUAL PROMPT (ACCEPT UPDATE / KEEP CURRENT) ──
    if (
      it.pending_update &&
      flags.themed &&
      srcLetter !== "-" &&
      themeId !== null &&
      themeId !== undefined
    ) {
      source.push({
        act: "accept-update",
        label: "ACCEPT UPDATE",
        tone: "info",
        kind: it.pending_update_kind || "upstream_changed",
        sectionScoped: true,
      });
      if (it.actionable_update) {
        source.push({
          act: "decline-update",
          label: "KEEP CURRENT",
          kind: it.pending_update_kind || "upstream_changed",
        });
      }
    }

    // ── 2. PRIMARY ACQUISITION ───────────────────────────────
    if (flags.sidecarOnly) {
      source.push({ act: "adopt-sidecar", label: "ADOPT", tone: "adopt" });
    }

    if (
      flags.themed &&
      themeId !== null &&
      themeId !== undefined &&
      !flags.sidecarOnly &&
      !flags.isPlexAgent &&
      !flags.isManualPlacement &&
      !flags.lockManualActions &&
      (!it.pending_update || srcLetter === "-") &&
      (!it.accepted_update || srcLetter === "-")
    ) {
      const tdbDeadForDownload =
        it.failure_kind && TDB_DEAD_FAILURES.has(it.failure_kind);
      const tdbCookiesBlocked =
        it.failure_kind === "cookies_expired" && !cookiesPresent;
      const tdbBlocked = tdbDeadForDownload || tdbCookiesBlocked;
      const hasDownloadUrl =
        !!it.youtube_url || flags.sourceKindForActions === "url";
      if (!tdbBlocked && hasDownloadUrl) {
        const isFresh = !flags.downloaded || flags.dlBroken;
        source.push({
          act: "redl",
          label: isFresh ? "DOWNLOAD TDB" : "RE-DOWNLOAD TDB",
          tone: "themerrdb",
          sectionScoped: true,
        });
      }
    }

    // ── 3. CUSTOM OVERRIDES ──────────────────────────────────
    source.push({
      act: "manual-url",
      label: "SET URL",
      tone: "user",
    });
    source.push({
      act: "upload-theme",
      label: "UPLOAD MP3",
      tone: "user",
    });

    // ── 4. CROSS-SOURCE REPLACE ──────────────────────────────
    const tdbReplaceBlocked =
      (it.failure_kind && TDB_DEAD_FAILURES.has(it.failure_kind)) ||
      (it.failure_kind === "cookies_expired" && !cookiesPresent);
    if (
      flags.isThemerrDb &&
      !tdbReplaceBlocked &&
      !it.pending_update &&
      !it.accepted_update &&
      (flags.sidecarOnly || flags.isManualPlacement || flags.isPlexAgent)
    ) {
      source.push({
        act: "replace-with-themerrdb",
        label: "REPLACE TDB",
        tone: "themerrdb",
        warn: true,
      });
    }

    // ── 5. HOUSEKEEPING (ACK FAILURE) ────────────────────────
    if (
      it.failure_kind &&
      !it.failure_acked_at &&
      flags.themed &&
      themeId !== null &&
      themeId !== undefined
    ) {
      source.push({ act: "clear-failure", label: "ACK FAILURE" });
    }

    // ── 6. UNDO (REVERT / RESTORE) ───────────────────────────
    if (
      it.has_previous_url &&
      !it.revert_redundant &&
      it.previous_youtube_kind === "user"
    ) {
      const isRestore = srcLetter === "-";
      source.push({
        act: "revert",
        label: isRestore ? "RESTORE" : "REVERT",
        tone: "user",
        sectionScoped: true,
      });
    }

    // ── PLACE menu ───────────────────────────────────────────
    if (flags.themed && flags.downloaded && !flags.placed && !flags.dlBroken) {
      place.push({
        act: "replace",
        label: "PUSH TO PLEX",
        warn: true,
        bypassLock: true,
      });
    }
    if (
      flags.themed &&
      flags.downloaded &&
      flags.placed &&
      !flags.dlBroken &&
      !flags.isMismatch
    ) {
      place.push({
        act: "replace",
        label: "RE-PUSH",
        warn: true,
        bypassLock: true,
      });
    }
    if (flags.themed && flags.isMismatch && flags.downloaded && flags.placed) {
      place.push({
        act: "replace",
        label: "PUSH TO PLEX",
        info: true,
        bypassLock: true,
      });
      place.push({
        act: "adopt-from-plex",
        label: "ADOPT FROM PLEX",
        info: true,
        bypassLock: true,
      });
      if (it.mismatch_state === "pending") {
        place.push({
          act: "keep-mismatch",
          label: "KEEP MISMATCH",
          bypassLock: true,
        });
      }
    }
    if (flags.themed && flags.dlBroken && flags.placed) {
      place.push({
        act: "restore-canonical",
        label: "RESTORE FROM PLEX",
        warn: true,
        bypassLock: true,
      });
    }

    // ── REMOVE menu ──────────────────────────────────────────
    if (it.has_previous_url) {
      remove.push({ act: "clear-url", label: "CLEAR URL", danger: true });
    }
    if (flags.placed) {
      remove.push({
        act: "unplace",
        label: "DEL",
        danger: true,
        sectionScoped: true,
      });
    }
    if (flags.placed && flags.downloaded) {
      remove.push({
        act: "unmanage",
        label: "UNMANAGE",
        danger: true,
        sectionScoped: true,
      });
    }
    if (flags.downloaded || flags.isOrphan) {
      remove.push({
        act: "purge",
        label: "× PURGE",
        danger: true,
        bypassLock: true,
        sectionScoped: true,
        dlOnly: !flags.placed && flags.downloaded,
      });
    }

    return { source: source, place: place, remove: remove };
  }

  return {
    TDB_DEAD_FAILURES: TDB_DEAD_FAILURES,
    computeSrcLetter: computeSrcLetter,
    deriveRowFlags: deriveRowFlags,
    computeMenuActions: computeMenuActions,
  };
});
