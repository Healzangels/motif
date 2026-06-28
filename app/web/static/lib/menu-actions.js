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
//
// IMPORTANT: this is a HAND-MIRRORED SUBSET of app.js's inline
// renderLibraryRow gates — NOT loaded by any template, so it can
// drift silently and leave the harness under-testing the real UI.
// It models the gating-load-bearing SOURCE/PLACE/REMOVE actions
// but deliberately omits app.js's PU FILE/API split, SWITCH
// PLACEMENT, DOWNLOAD TDB BACKUP / DOWNLOAD PLEX BACKUP, ACK DROP /
// CONVERT TO MANUAL. The REPLACE TDB gate clause-set is pinned
// against the live gate by tests/test_v1_24_74_menu_actions_mirror_drift.py
// (the drift lint) — keep both in sync when either changes.

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
    // v1.19.72: SRC=— exception for kind='new_theme_available'
    // mirrors app.js gate (line ~8645). Without this the blue
    // !UPD glyph + TDB↑ pill rendered on SRC=— new-theme rows
    // but the SOURCE menu had no ACCEPT UPDATE button — broken
    // CTA.
    if (
      it.pending_update &&
      flags.themed &&
      (srcLetter !== "-"
        || it.pending_update_kind === "new_theme_available") &&
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

    // v1.19.72: tighten the SRC=— escape hatch so a SRC=— new-
    // theme row doesn't render BOTH ACCEPT UPDATE and DOWNLOAD
    // TDB. Mirrors app.js (line ~8732).
    const srcDashEscapeOk = srcLetter === "-"
      && it.pending_update_kind !== "new_theme_available";
    if (
      flags.themed &&
      themeId !== null &&
      themeId !== undefined &&
      !flags.sidecarOnly &&
      !flags.isPlexAgent &&
      !flags.isManualPlacement &&
      !flags.lockManualActions &&
      (!it.pending_update || srcDashEscapeOk) &&
      (!it.accepted_update || srcLetter === "-") &&
      // v1.17.17: plex_orphan rows have no ThemerrDB URL to (re-)download
      // from — themes.youtube_url for an orphan is the ADOPT-captured URL,
      // not a TDB one. RE-DOWNLOAD TDB on a NO-TDB row 409'd. Mirrors
      // app.js (~line 10002).
      !flags.isOrphan
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
    // v1.20.2: new_theme_available rows (net-new TDB themes, esp. SRC=P)
    // keep the full TDB toolkit — a bare !it.pending_update wrongly hid
    // REPLACE TDB whenever the blue !UPD pill was up. Mirrors app.js
    // (~line 10137, anchored by `const tdbActionPendingOk`).
    const tdbActionPendingOk =
      !it.pending_update || it.pending_update_kind === "new_theme_available";
    // v1.14.46 / v1.19.49: LPS rows already hold motif's canonical (from a
    // prior backup or LET PLEX SERVE) — REPLACE TDB would re-download
    // wastefully when PUSH TO PLEX reuses the existing file. plex_cloud
    // backup_only rows are AUTOMATED insurance, NOT a canonical
    // commitment, so they don't count as "has canonical" (the user can
    // still switch to TDB content). Mirrors app.js (~line 10125).
    const isPlexCloudBackupRow =
      flags.downloaded &&
      it.source_kind === "plex_cloud" &&
      it.last_place_attempt_reason === "backup_only";
    const hasNonCloudCanonical = flags.downloaded && !isPlexCloudBackupRow;
    const lpsHasCanonical = flags.isPlexAgent && hasNonCloudCanonical;
    if (
      flags.isThemerrDb &&
      // v1.24.71: a title can be TDB-TRACKED (upstream_source set) but
      // have NO theme video (themes.youtube_url empty) — REPLACE TDB
      // would 409 "ThemerrDB record has no youtube_url". Gate on it.
      it.youtube_url &&
      !tdbReplaceBlocked &&
      tdbActionPendingOk &&
      // v1.24.72: accepted_update is sticky — only a no-op while the
      // accepted TDB is still the active theme (srcLetter T). Once the
      // user overrides it (SRC=U etc.) REPLACE TDB is meaningful again.
      (!it.accepted_update || srcLetter !== "T") &&
      !lpsHasCanonical &&
      (flags.sidecarOnly || flags.isManualPlacement || flags.isPlexAgent)
    ) {
      source.push({
        act: "replace-with-themerrdb",
        label: "REPLACE TDB",
        tone: "themerrdb",
        warn: true,
      });
    }

    // v1.12.87: ACK FAILURE removed from the SOURCE menu — moved
    // to INFO card's // TRY THIS NEXT section as the single ACK
    // entry-point. Row's red ! glyph also routes there.

    // ── 6. UNDO (REVERT / RESTORE) ───────────────────────────
    // v1.12.65 / v1.12.81 / v1.17.17: REVERT needs a user-kind captured
    // URL OR a src='-'/'M' row (post-PURGE / post-UNMANAGE restore, where
    // the captured URL may be themerrdb-kind). A bare kind==='user' check
    // wrongly hid RESTORE on those recovery rows. Mirrors app.js (~10429).
    const restoreEligibleKind =
      it.previous_youtube_kind === "user" ||
      srcLetter === "-" ||
      srcLetter === "M";
    if (it.has_previous_url && !it.revert_redundant && restoreEligibleKind) {
      // v1.12.79 / v1.12.81: RESTORE label on '-' (post-PURGE) and 'M'
      // (post-UNMANAGE sidecar); REVERT elsewhere.
      const isRestore = srcLetter === "-" || srcLetter === "M";
      // v1.12.103: tone tracks the captured kind — themerrdb-kind restores
      // land green (matching the T chip), user-kind purple.
      const restoreTone =
        it.previous_youtube_kind === "themerrdb" ? "themerrdb" : "user";
      source.push({
        act: "revert",
        label: isRestore ? "RESTORE" : "REVERT",
        tone: restoreTone,
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
    // v1.18.8: hide PURGE on a zombie orphan (post-PURGE: URLs nulled, no
    // canonical, no placement) — clicking it ran an idempotent no-op.
    // Orphans only show PURGE while they still own something. Mirrors
    // app.js (~line 10761, `orphanHasPurgeableState`).
    const orphanHasPurgeableState =
      flags.isOrphan &&
      (!!it.user_youtube_url ||
        !!it.youtube_url ||
        flags.downloaded ||
        flags.placed);
    if (flags.downloaded || orphanHasPurgeableState) {
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
