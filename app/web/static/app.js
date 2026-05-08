// motif · vanilla JS frontend (no framework, no build step)
(() => {
  'use strict';

  // ---- Helpers ----

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const fmt = {
    num: (n) => (n ?? 0).toLocaleString(),
    // v1.13.8 (Phase D): byte-count humanizer. Used by the cache
    // gauge on /settings; kept generic so other size displays
    // (sync_runs, snapshot meta, etc.) can reuse without local
    // formatters.
    bytes: (n) => {
      const v = Number(n) || 0;
      if (v < 1024) return `${v} B`;
      const units = ['KB', 'MB', 'GB', 'TB'];
      let x = v;
      let i = -1;
      do { x /= 1024; i++; } while (x >= 1024 && i < units.length - 1);
      const decimals = x >= 100 ? 0 : x >= 10 ? 1 : 2;
      return `${x.toFixed(decimals)} ${units[i]}`;
    },
    time: (iso) => {
      if (!iso) return '—';
      const d = new Date(iso);
      const now = new Date();
      const diff = (now - d) / 1000;
      if (diff < 60) return `${Math.round(diff)}s ago`;
      if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
      if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
      return d.toLocaleDateString() + ' ' + d.toLocaleTimeString().slice(0, 5);
    },
    timeShort: (iso) => {
      if (!iso) return '—';
      const d = new Date(iso);
      return d.toTimeString().slice(0, 8);
    },
    // v1.12.93: format a timestamp from either an ISO 8601 string
    // or a unix-epoch (seconds; 10-digit number/string) into a
    // human-readable absolute date+time. Used by INFO card timestamp
    // rows where the source format varies — themes.youtube_added_at /
    // edited_at come from ThemerrDB as numeric seconds, while
    // motif's audit_events / local_files store ISO strings.
    timeAuto: (val) => {
      if (val === null || val === undefined || val === '') return '—';
      let d;
      if (typeof val === 'number'
          || (typeof val === 'string' && /^\d{10}$/.test(val.trim()))) {
        d = new Date(Number(val) * 1000);
      } else {
        d = new Date(val);
      }
      if (Number.isNaN(d.getTime())) return String(val);
      return d.toLocaleString(undefined, {
        year: 'numeric', month: 'short', day: '2-digit',
        hour: '2-digit', minute: '2-digit',
      });
    },
  };

  async function api(method, path, body) {
    const opts = { method, headers: {} };
    if (body) {
      if (body instanceof FormData) {
        opts.body = body;
      } else {
        opts.headers['Content-Type'] = 'application/json';
        opts.body = JSON.stringify(body);
      }
    }
    const r = await fetch(path, opts);
    if (!r.ok) {
      const text = await r.text().catch(() => '');
      throw new Error(`${r.status}: ${text || r.statusText}`);
    }
    return r.json();
  }

  function htmlEscape(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])
    );
  }

  // v1.10.17: extract edition tag(s) from a Plex folder path. Mirrors
  // app/core/normalize.py:parse_folder_name — strips trailing {tag}
  // groups from the basename, drops the 'edition-' prefix, joins with
  // '·'. Used to surface 'Director's Cut' / 'Extended' / etc. on
  // library rows so users can tell editions apart at a glance (the
  // title and year are identical across editions of the same film).
  // v1.10.25: GUID-style tags ({imdb-/tmdb-/tvdb-/plex-/agentid-}) are
  // skipped — they're scanner hints, not editions, and rendering them
  // as a pill cluttered the title cell.
  function parseEditionFromFolderPath(folderPath) {
    if (!folderPath) return '';
    const segs = String(folderPath).split(/[\\/]/).filter(Boolean);
    let s = segs.length ? segs[segs.length - 1] : '';
    const editions = [];
    const guidPrefix = /^(imdb|tmdb|tvdb|plex|agentid)-/i;
    while (true) {
      const m = s.match(/^(.*)\{([^}]*)\}\s*$/);
      if (!m) break;
      const tag = m[2];
      if (!guidPrefix.test(tag)) editions.unshift(tag);
      s = m[1].replace(/\s+$/, '');
    }
    return editions
      .map((e) => e.replace(/^edition-/i, ''))
      .filter(Boolean)
      .join(' · ');
  }

  // ---- Nav highlighting ----

  function highlightNav() {
    const path = window.location.pathname;
    const map = { '/': 'dashboard', '/movies': 'movies', '/tv': 'tv',
                  '/anime': 'anime', '/queue': 'queue',
                  '/pending': 'pending',
                  '/settings': 'settings' };
    const k = map[path];
    if (!k) return;
    const a = document.querySelector(`.nav a[data-nav="${k}"]`);
    if (a) a.classList.add('active');
  }

  // ---- Topbar status ----

  function applyTabAvailability(ta) {
    if (!ta) return;
    const showHide = (sel, has) => {
      const n = document.querySelector(sel);
      if (n) n.style.display = has ? '' : 'none';
    };
    const has = (k) => !!(ta[k] && (ta[k].standard || ta[k].fourk));
    showHide('.nav a[data-nav="movies"]', has('movies'));
    showHide('.nav a[data-nav="tv"]',     has('tv'));
    showHide('.nav a[data-nav="anime"]',  has('anime'));
  }

  // v1.11.33: restore tab visibility from localStorage immediately so
  // returning users don't see the brief flicker between page paint and
  // the first /api/stats response. The fresh response from
  // refreshTopbarStatus will overwrite if it's newer.
  try {
    const cached = localStorage.getItem('motif:tab_availability');
    if (cached) {
      const ta = JSON.parse(cached);
      applyTabAvailability(ta);
      // v1.13.16: also pre-paint the STANDARD/4K toggle chips on the
      // library page from cached availability, so a tab switch
      // (MOVIES → TV SHOWS) doesn't briefly show the chips at their
      // hidden default state before /api/stats lands and fills them
      // in. adaptLibraryFourkToggle no-ops gracefully when the
      // resolution chips don't exist (non-library pages).
      try { adaptLibraryFourkToggle(ta); } catch (_) { /* fine */ }
    }
  } catch (_) { /* malformed cache — ignore, the poll will fix it */ }

  // v1.12.50: same trick for the themes-have map. The library row
  // pill render gates on window.__motif_themes_have[rowMt] to avoid
  // misleading 'no TDB' pills on a fresh install with an empty
  // themes table. Without this cache, the first library render on
  // every page load (and every nav-back to the library tab) had
  // __motif_themes_have undefined, so the gate returned '' for every
  // row and pills only appeared after a filter click forced a
  // re-render. Reading the cached map here seeds the global before
  // the first render, so pills paint on first frame for any user
  // who has run /api/stats at least once. The poll still overwrites
  // with fresh data immediately after.
  try {
    const cachedTh = localStorage.getItem('motif:themes_have');
    if (cachedTh) window.__motif_themes_have = JSON.parse(cachedTh);
  } catch (_) { /* malformed cache — ignore, the poll will fix it */ }

  // v1.11.72: build a scope label for the library REFRESH FROM PLEX
  // button — 'MOVIES', '4K MOVIES', 'TV SHOWS', '4K TV', 'ANIME',
  // '4K ANIME'. Falls back to 'PLEX' on pages without a library-tab
  // input so the helper is safe to call from anywhere. libraryState
  // is hoisted to a higher line but only consumed inside callbacks
  // that run after DOMContentLoaded — its const binding is always
  // initialised by the time this helper is invoked.
  function libraryRefreshLabel() {
    const tabEl = document.getElementById('library-tab');
    if (!tabEl) return 'PLEX';
    const tab = tabEl.value;
    const fourk = !!libraryState.fourk;
    const tabName = (
      { movies: 'MOVIES', tv: 'TV SHOWS', anime: 'ANIME' }[tab]
    ) || 'PLEX';
    return fourk ? `4K ${tabName}` : tabName;
  }

  // v1.12.3: derive a human label for the active plex_enum scope
  // from the per-tab/variant active map. Used in the topbar status
  // text so the user sees WHICH library is currently being scanned
  // ("SYNCING 4K MOVIES" instead of generic "SYNCING WITH PLEX").
  // Iterates the active map deterministically (movies → tv → anime,
  // standard → fourk) so multi-section scans surface the
  // first-active scope; once that section finishes, the next poll
  // picks up the next-active one.
  function activePlexEnumScopeLabel(active) {
    if (!active) return null;
    const tabOrder = ['movies', 'tv', 'anime'];
    const tabName = { movies: 'MOVIES', tv: 'TV SHOWS', anime: 'ANIME' };
    for (const tab of tabOrder) {
      const v = active[tab];
      if (!v) continue;
      if (v.standard && v.fourk) return `${tabName[tab]} + 4K ${tabName[tab]}`;
      if (v.fourk) return `4K ${tabName[tab]}`;
      if (v.standard) return tabName[tab];
    }
    return null;
  }

  // v1.11.48: optimistic topbar paint helper for sync/refresh click
  // handlers. /api/stats has a 1s TTL cache, so an immediate
  // refreshTopbarStatus right after enqueue often hits a stale
  // entry and misses the freshly-pending row. This sets the label
  // and dot directly so feedback lands the same frame as the click,
  // then queues a real refresh past the cache TTL to reconcile.
  // v1.12.118: legacy paintTopbarSyncing retired with the dot+text.
  // Click feedback is now handled by triggering motifOps.refresh()
  // immediately so the new op-mini bar appears within a tick, plus
  // a follow-up refreshTopbarStatus to reconcile UPD/FAIL pills. The
  // function name is kept as a thin shim so existing callers don't
  // need rewiring.
  function paintTopbarSyncing(_label) {
    try {
      if (window.motifOps && typeof window.motifOps.refresh === 'function') {
        window.motifOps.refresh();
      }
    } catch (_) { /* swallow */ }
    setTimeout(refreshTopbarStatus, 1100);
  }

  async function refreshTopbarStatus() {
    try {
      const stats = await api('GET', '/api/stats');
      const q = stats.queue || {};
      // v1.12.118: legacy "what's running" status text + dot retired.
      // The new ops mini-bar (driven by /api/progress polling in
      // ops.js) covers every active operation surface — TDB sync,
      // Plex enum, download/place/scan/refresh/relink/adopt queues —
      // through one uniform pill style. The idle pill stays visible
      // when nothing's running. refreshTopbarStatus's job here is
      // limited to: keep UPD/FAIL pills in sync, kick the ops
      // poller when stats sees a fresh sync flag (so the bar
      // appears within a tick of the click rather than after the
      // ops idle-poll cadence), and reset the idle pill to its
      // healthy state.
      const idle = $('#op-status-idle');
      if (idle) {
        idle.classList.remove('op-pill-offline');
        idle.querySelector('.op-pill-label').textContent = 'IDLE';
      }
      // Trigger fast ops refresh on sync transitions so the
      // mini-bar lights up immediately.
      const plexEnumBusy = q.plex_enum_in_flight > 0;
      const themerrdbBusy = q.themerrdb_sync_in_flight > 0;
      const opsBar = $('#op-mini');
      const opsHidden = !opsBar || opsBar.hidden;
      if ((themerrdbBusy || plexEnumBusy) && opsHidden
          && window.motifOps && typeof window.motifOps.refresh === 'function') {
        try { window.motifOps.refresh(); } catch (_) { /* swallow */ }
      }
      // unacked count drives the FAIL pill below + the
      // __motif_failed_count global other code reads.
      const unacked = q.unacked_failures || 0;

      // v1.12.118: failure-count signaling moved to the FAIL op-pill
      // (already pulses red on its own). The legacy #topbar-status
      // click-through is retired since the dot+text are gone.
      // __motif_failed_count global stays so other code that gates
      // on it doesn't need rewiring.
      window.__motif_failed_count = unacked;

      // v1.11.77: per-media-type 'we have TDB data for this' flags.
      // Pre-fix the TDB pill was gated on last_sync_at (a successful
      // sync ever) — if the user cancelled the very first sync mid-
      // way, last_sync_at stayed null forever and pills never
      // rendered, even though themes had real data for items
      // captured before the cancel. Now we gate on the themes count
      // being > 0 for the row's media_type, so partial captures
      // still light up the pills truthfully.
      // v1.11.97: gate on the TDB-only count (themes whose
      // upstream_source is a real ThemerrDB hit, not 'plex_orphan').
      // Pre-fix any uploaded / adopted theme would bump the generic
      // total > 0 and trip the pill into showing "no TDB" for
      // every other row in the section, even if a sync had never
      // been run. The TDB pill is a statement about ThemerrDB
      // coverage; only TDB-sourced rows should drive it.
      const movies_tdb = (stats.movies && stats.movies.tdb_total) || 0;
      const tv_tdb = (stats.tv && stats.tv.tdb_total) || 0;
      window.__motif_themes_have = {
        movie: movies_tdb > 0,
        tv: tv_tdb > 0,
        // anime tab pulls from both depending on section type;
        // either source qualifies it as 'we have data'.
        // v1.12.2: was referring to undeclared movies_total/tv_total
        // (renamed to *_tdb in v1.11.97); ReferenceError tripped the
        // outer catch and lit the OFFLINE indicator on every page
        // load. Silent catch made it look like a network issue.
        any: (movies_tdb + tv_tdb) > 0,
      };
      // v1.12.50: cache the themes-have map in localStorage so the
      // NEXT page load can paint TDB pills on the FIRST render
      // without waiting for /api/stats. Pre-fix, library rows
      // rendered before stats landed, so the pill render gate
      // (haveTdb at row pill time) was always false on first paint;
      // pills only appeared after a filter click forced a re-render.
      // Mirrors the localStorage cache pattern established for
      // tab_availability in v1.11.33.
      try {
        localStorage.setItem(
          'motif:themes_have',
          JSON.stringify(window.__motif_themes_have),
        );
      } catch (_) { /* private mode / quota — fine, just lose the cache */ }

      // Updates badge — v1.12.106: op-pill primitive, [hidden] attr.
      const updBadge = $('#topbar-updates-badge');
      if (updBadge) {
        const n = (stats.updates && stats.updates.pending) || 0;
        if (n > 0) {
          $('#topbar-updates-count').textContent = n;
          updBadge.hidden = false;
          // v1.12.48: route the badge to whichever tab owns the
          // first row with a pending update (mirrors the FAIL
          // badge's tab_hint logic). Apply the blue TDB ↑ pill
          // filter so the user lands directly on the rows that
          // make up the count — matched 1:1 since the
          // tdb_pills=update SQL now scopes to decision='pending'.
          const tabHint = (stats.updates && stats.updates.tab_hint) || 'movies';
          updBadge.href = `/${tabHint}?tdb_pills=update`;
        } else {
          updBadge.hidden = true;
        }
      }
      // Failures badge — v1.12.106: op-pill primitive, [hidden] attr.
      // Driver unified with per-row glyph: count every unacked
      // failure_kind, not just the four 'unavailable' kinds.
      // Pre-fix the topbar showed FAIL=0 while rows with
      // cookies_expired / network_error / unknown still rendered
      // the red ⚠ glyph — visible mismatch between the topbar
      // attention surface and the per-row signal.
      const failBadge = $('#topbar-failures-badge');
      if (failBadge) {
        const n = (stats.failures && stats.failures.total) || 0;
        if (n > 0) {
          $('#topbar-failures-count').textContent = n;
          failBadge.hidden = false;
          // v1.12.11: route the badge to whichever tab owns the
          // first failing row (anime / tv / movies). Pre-fix the
          // link was hardcoded to /movies which mis-routed every
          // anime / tv failure.
          const tabHint = (stats.failures && stats.failures.tab_hint) || 'movies';
          failBadge.href = `/${tabHint}?tdb_pills=dead`;
        } else {
          failBadge.hidden = true;
        }
      }
      // v1.13.1 (Phase C): DROP badge — count of themes ThemerrDB
      // stopped publishing. No tab_hint yet (drops happen anywhere);
      // route to /movies as a sensible default — the pill filter
      // and the drilled-in row let the user reach any tab.
      const dropBadge = $('#topbar-drops-badge');
      if (dropBadge) {
        const n = (stats.drops && stats.drops.total) || 0;
        const cnt = $('#topbar-drops-count');
        if (n > 0) {
          if (cnt) cnt.textContent = n;
          dropBadge.hidden = false;
        } else {
          dropBadge.hidden = true;
        }
      }
      // v1.13.11: DISK warning pill. Only renders when the guard is
      // enabled (stats.disk.low is non-null) AND we're below the
      // threshold. Free-MB count goes inline so the operator sees
      // headroom at a glance.
      const diskBadge = $('#topbar-disk-badge');
      if (diskBadge) {
        const disk = stats.disk || {};
        const free = $('#topbar-disk-free');
        if (disk.low === true) {
          if (free) free.textContent = (disk.free_mb != null) ? `${disk.free_mb}M` : '!';
          diskBadge.hidden = false;
          if (disk.free_mb != null && disk.min_mb != null) {
            diskBadge.title = `${disk.free_mb}MB free on themes_dir's filesystem (min ${disk.min_mb}MB) — downloads are blocked until space frees up.`;
          }
        } else {
          diskBadge.hidden = true;
        }
      }
      // v1.12.118: legacy idle-dot retired — ops.js renderTopbar
      // owns the idle pill / op-mini handoff now (no flip).

      // v1.11.27: hide every tab's nav link when no managed section
      // backs it. v1.11.33: cache the availability map in localStorage
      // so the next page load can paint nav from the cache before the
      // first stats poll lands — eliminates the brief flicker where
      // unconfigured tabs flashed visible.
      if (stats.tab_availability) {
        const ta = stats.tab_availability;
        applyTabAvailability(ta);
        try {
          localStorage.setItem('motif:tab_availability', JSON.stringify(ta));
        } catch (_) { /* private mode / quota — fine, we just lose the cache */ }
        adaptLibraryFourkToggle(ta);
      }

      // v1.10.44: stash cookies-present so the library row's TDB pill
      // can flip green when cookies are configured and on disk
      // (regardless of any stale cookies_expired flag from earlier
      // probes). The pill goes amber only when the file is actually
      // missing.
      if (stats.config) {
        window.__motif_cookies_present = !!stats.config.cookies_present;
      }

      // v1.12.41: /pending tab removed. The pending-placements
      // indicator + library-page banner that pointed at it are
      // both gone — pending downloads now surface via the
      // library tab's TDB ↑ pill filter and the per-row UI.

      // Drive dry-run banner
      const banner = $('#dry-run-banner');
      if (banner) {
        banner.style.display = stats.dry_run ? '' : 'none';
        document.body.classList.toggle('dry-run-on', !!stats.dry_run);
      }
      // Drive paths-not-configured banner
      updatePathsBanner(stats);

      // v1.11.5: every sync/refresh button shares one lock — when EITHER
      // a ThemerrDB sync or a Plex enum is in flight, all of:
      //   - dashboard SYNC button
      //   - movies/tv/anime page REFRESH FROM PLEX
      //   - settings-page REFRESH FROM PLEX (LIBRARY SECTIONS top-right)
      //   - per-section REFRESH buttons
      // are disabled and text-stamped with the operation in flight.
      // Pre-v1.11.5 the page/settings/per-section refreshes only locked
      // on plex_enum, so during a ThemerrDB sync the user could fire a
      // concurrent Plex enum and end up with two sync banners running.
      // v1.11.27: granular per-tab / per-section button locking.
      // Pre-fix any plex_enum in flight locked every refresh button on
      // every page; the user couldn't refresh /tv while /movies was
      // still scanning. Now each refresh button checks whether ITS
      // scope is currently in the running set:
      //   - library page REFRESH: gated on q.plex_enum_active[tab][variant]
      //   - per-section REFRESH: gated on its section_id appearing in
      //     q.plex_enum_running_section_ids
      //   - settings global REFRESH FROM PLEX: gated on ANY section
      //     enumerating
      //   - dashboard SYNC: still gated on themerrdb_sync_in_flight
      //     (it's the only ThemerrDB sync trigger)
      const enumActive = q.plex_enum_active || {};
      const enumSectionIds = new Set(q.plex_enum_running_section_ids || []);
      // 'anyEnumRunning' = there's a section actively running RIGHT
      // NOW (status='running'). Used for per-section REFRESH buttons
      // since once a specific section finishes, the user can fire
      // it again immediately.
      const anyEnumRunning = enumSectionIds.size > 0;
      // v1.11.75: 'anyEnumInFlight' = there's any plex_enum job in
      // pending OR running state. Used for layoutLocked + the
      // settings global REFRESH so the lock holds through the
      // ENTIRE enum window (incl. brief gaps where the worker is
      // between running jobs but more are queued). Pre-fix the lock
      // released between running sections and the user could see
      // the per-section MGD checkboxes / SAVE button briefly
      // re-enable while 'REFRESHING PLEX…' was still showing in the
      // topbar — confusing.
      const anyEnumInFlight = plexEnumBusy;
      // Stash for the empty-state message in loadLibrary().
      window.__motif_enum_active = enumActive;
      // v1.12.69: per-tab busy indicator. Toggle .nav-busy on each
      // managed-tab anchor whenever any of that tab's variants
      // (standard / fourk) is currently enumerating. CSS adds a
      // small pulsing cyan dot via ::after so users can see at a
      // glance which library tabs are in flux without parsing the
      // topbar status text.
      ['movies', 'tv', 'anime'].forEach((tab) => {
        const anchor = document.querySelector(`.nav a[data-nav="${tab}"]`);
        if (!anchor) return;
        const tabBusy = !!(enumActive[tab]
                           && (enumActive[tab].standard
                               || enumActive[tab].fourk));
        anchor.classList.toggle('nav-busy', tabBusy);
      });
      // v1.12.69: section-count badge on the topbar dot. When
      // multiple sections are enumerating concurrently, show "N"
      // beside the pulsing dot so the user knows how many are in
      // flight. plex_enum_running_section_ids is the live set;
      // plex_enum_in_flight covers pending + running. Uses the
      // running set for the visible count (more accurate to "what
      // the worker is processing now") but falls back to
      // _in_flight as the lower bound when running is 0 but
      // something's queued.
      // (dotEl const already declared above at the failures
      //  tooltip pass; reusing it here would shadow that scope.
      //  Use a fresh selector to keep the two passes independent.)
      const enumDotEl = document.querySelector('#topbar-status .dot');
      let dotBadge = document.getElementById('topbar-dot-badge');
      const sectionCount = enumSectionIds.size
                        || (q.plex_enum_in_flight || 0);
      if (sectionCount > 1) {
        if (!dotBadge && enumDotEl) {
          dotBadge = document.createElement('span');
          dotBadge.id = 'topbar-dot-badge';
          dotBadge.className = 'topbar-dot-badge';
          enumDotEl.parentElement.insertBefore(dotBadge, enumDotEl.nextSibling);
        }
        if (dotBadge) {
          dotBadge.textContent = String(sectionCount);
          dotBadge.title = `${sectionCount} sections enumerating`;
          dotBadge.style.display = '';
        }
      } else if (dotBadge) {
        dotBadge.style.display = 'none';
      }
      // v1.13.18: silent variant of lockBtn — toggles disabled but
      // leaves the label intact. The topbar status pill carries the
      // live state, so swapping the button label was redundant AND
      // outlasted the pill in some cases. Pass a busyText only if
      // you genuinely need a label swap (no current callers do).
      const lockBtn = (btn, locked, _busyText) => {
        if (!btn) return;
        btn.disabled = !!locked;
      };
      // v1.13.23: split the lock scope so per-library SYNC PLEX
      // doesn't gate-lock other library tabs or the dashboard SYNC.
      // Pre-fix any plex_enum in flight locked every // SYNC PLEX
      // button on every library tab AND the dashboard SYNC THEMERRDB
      // button — the v1.11.27 intent ("library page REFRESH: gated
      // on q.plex_enum_active[tab][variant]", lines 449-459 above)
      // existed in the comments but the implementation collapsed
      // back to the unified v1.11.5 gate. This block restores the
      // granular lock:
      //   - dashboard SYNC: themerrdbBusy only; per-library plex_enum
      //     is an independent job and shouldn't block another sync.
      //   - library SYNC PLEX: only when MY tab+variant has an
      //     enum in flight, OR a global pipeline (sync→enum cascade
      //     with auto_enum on, or multi-tab SCAN ALL from settings)
      //     is sweeping every section.
      //   - settings SYNC PLEX: any plex_enum in flight (it'd dedupe
      //     poorly per-section), or sync→enum cascade incoming.
      const autoEnum = (q.auto_enum_after_sync !== false);
      const tabKey = libraryState.tab;
      const variantKey = libraryState.fourk ? 'fourk' : 'standard';
      // v1.13.36: per-variant busy (reverts v1.13.30's tab-wide
      // gate). User feedback: "when syncing the standard version
      // of a library the 4k version also becomes unclickable and
      // says syncing when only the actual library [variant] syncing
      // should." STANDARD and 4K target different Plex sections
      // (different jobs, can run independently); the lock should
      // mirror that. v1.13.30 widened the gate to address a
      // separate issue (mid-flip DONE flash) which is now handled
      // entirely by the scope-aware sawBusyScope flag below.
      const myTabBusy = !!(tabKey && enumActive[tabKey]
                           && enumActive[tabKey][variantKey]);
      // "Global pipeline" = SCAN ALL from settings (every section
      // queued, naturally lights up multiple tabs as it sweeps) OR
      // dashboard sync→enum cascade (tdb sync running with auto_enum
      // on guarantees a global plex_enum follows). Both warrant
      // locking every library tab's button until the sweep ends.
      const enumTabsActive = ['movies', 'tv', 'anime'].filter((t) =>
        enumActive[t] && (enumActive[t].standard || enumActive[t].fourk),
      ).length;
      // v1.13.25: pipelineInFlight = ANY in-flight plex_enum job
      // tagged scope=cascade (post-sync auto-enum) or scope=scan_all
      // (settings global SYNC PLEX). Keeps the lock stable through
      // the tail of a multi-section sweep instead of releasing as
      // each section drains and enumTabsActive falls back to 1.
      const pipelineInFlight = (q.plex_enum_pipeline_in_flight || 0) > 0;
      // v1.13.27: stash a per-kind queue snapshot on a window global
      // so ops.js renderTopbar can compose "(X of Y)" suffixes on the
      // mini-bar label. plex_enum_in_flight is the live count
      // (pending + running). hw (high water) tracks the burst's max
      // so position can be computed as (hw - current + 1). Reset hw
      // when the queue drains so the next burst starts fresh.
      const w = window;
      w.__motif_queue = w.__motif_queue || {};
      const plexInFlight = q.plex_enum_in_flight || 0;
      const prevPlex = w.__motif_queue.plex_enum || { current: 0, hw: 0 };
      // v1.13.29: detect mid-burst growth so the (X of Y) suffix
      // stays honest when the user queues another scan before the
      // existing burst drains. Pre-fix the burst's hw was clamped
      // to its initial peak — once the worker drained from 4→1
      // (current=1) and the user queued another (current=2),
      // hw stayed at 4 and the suffix said "(3 of 4)" even though
      // the real total was 5. Tracking the increase delta
      // (`grew = max(0, current - prev.current)`) lets hw grow with
      // late-arriving jobs without losing position progress.
      const grew = Math.max(0, plexInFlight - prevPlex.current);
      w.__motif_queue.plex_enum = {
        current: plexInFlight,
        hw: plexInFlight === 0
          ? 0
          : Math.max(prevPlex.hw + grew, plexInFlight),
      };
      const globalEnumPipeline = (themerrdbBusy && autoEnum)
                              || enumTabsActive > 1
                              || pipelineInFlight;
      // v1.13.25: dash SYNC stays locked through the WHOLE pipeline,
      // not just the tdb-sync phase. Pre-fix (v1.13.24) the bus was
      // `themerrdbBusy` only, which unlocked the button mid-pipeline
      // once the tdb sync flipped success → the cascade plex_enum
      // phase still had work to do but the button became clickable
      // and a second sync could be kicked off concurrently. Gating
      // on globalEnumPipeline keeps the lock through the cascade
      // without re-locking on per-library plex_enum (those don't
      // light up globalEnumPipeline since enumTabsActive ≤ 1 and
      // there's no concurrent tdb sync).
      const dashSyncBtnBusy = themerrdbBusy || globalEnumPipeline;
      const libRefreshBusy = myTabBusy || globalEnumPipeline;
      const settingsRefreshBusy = plexEnumBusy
                              || (themerrdbBusy && autoEnum);
      // Library page SYNC PLEX. v1.13.19: one-word busy label —
      // "// SYNCING…" stays put for the whole run instead of
      // changing mid-stream like the v1.13.16 multi-stage swap.
      // Idle label restored from dataset.origLabel set on click.
      // v1.13.21 (was v1.13.20): sawBusy flag drives a 1.5s ✓ DONE
      // flash on the busy → idle transition, mirroring the dashboard
      // SYNC button. Set when we observe busy; once it flips to
      // idle and the label is still '// SYNCING…', flash DONE,
      // schedule a revert, and clear the flag. The flash interval
      // window is short enough that subsequent polls during the
      // 1.5s don't fight the displayed text.
      const libRefreshBtn = document.getElementById('library-refresh-btn');
      if (libRefreshBtn) {
        // v1.13.24: scope the sawBusy flag to (tab, variant) so a
        // flip between // STANDARD and // 4K mid-sync doesn't fire
        // a phantom ✓ DONE flash. Pre-fix: when the standard enum
        // was running and the user clicked // 4K, libRefreshBusy
        // recomputed against enumActive[tab][fourk] (false → idle),
        // dataset.sawBusy === '1' was still set from the standard
        // observation, the label was still '// SYNCING…', so the
        // idle branch fired DONE while the topbar was still showing
        // the standard sync running. Now the flag carries which
        // scope was observed busy; the DONE flash only fires when
        // the SAME scope transitions busy → idle.
        const scopeKey = `${tabKey || ''}:${variantKey}`;
        if (libRefreshBusy) {
          libRefreshBtn.disabled = true;
          if (!libRefreshBtn.dataset.origLabel) {
            libRefreshBtn.dataset.origLabel = libRefreshBtn.textContent;
          }
          libRefreshBtn.textContent = '// SYNCING…';
          libRefreshBtn.dataset.sawBusyScope = scopeKey;
        } else if (libRefreshBtn.dataset.sawBusyScope === scopeKey
                   && libRefreshBtn.textContent === '// SYNCING…') {
          // Same scope just went busy → idle. Brief ✓ DONE flash,
          // then restore the idle label.
          // v1.13.30: stay disabled during the DONE flash. The flash
          // is a notification, not an action — clicking ✓ DONE used
          // to fire a fresh refresh which the user perceived as a
          // double-click hazard. Re-enable in the setTimeout that
          // restores origLabel.
          libRefreshBtn.disabled = true;
          libRefreshBtn.textContent = '✓ DONE';
          delete libRefreshBtn.dataset.sawBusyScope;
          const orig = libRefreshBtn.dataset.origLabel;
          setTimeout(() => {
            // Only revert if no fresh busy state has overwritten us.
            if (libRefreshBtn.textContent === '✓ DONE' && orig) {
              libRefreshBtn.textContent = orig;
              libRefreshBtn.disabled = false;
              delete libRefreshBtn.dataset.origLabel;
            }
          }, 1500);
        } else {
          // Either no prior busy observed (fresh page), or scope
          // changed (user flipped variant/tab mid-sync). Reset the
          // label silently — no DONE flash, since the operation we
          // observed was for a different scope and may still be
          // running there. The user can flip back to that scope to
          // see // SYNCING… resume.
          libRefreshBtn.disabled = false;
          delete libRefreshBtn.dataset.sawBusyScope;
          if (libRefreshBtn.dataset.origLabel
              && libRefreshBtn.textContent !== '✓ DONE') {
            libRefreshBtn.textContent = libRefreshBtn.dataset.origLabel;
            delete libRefreshBtn.dataset.origLabel;
          }
        }
      }
      // Settings global SYNC PLEX. v1.13.19: same busy treatment as
      // the library page button — one-word label, idle restore from
      // dataset.origLabel. v1.13.21: verb is unified to SYNC so both
      // branches read SYNCING; the conditional remains in case future
      // states want to differentiate, but for now both render the
      // same string.
      const settingsRefreshBtn = document.getElementById('refresh-libraries-btn');
      if (settingsRefreshBtn) {
        if (settingsRefreshBusy) {
          settingsRefreshBtn.disabled = true;
          if (!settingsRefreshBtn.dataset.origLabel) {
            settingsRefreshBtn.dataset.origLabel = settingsRefreshBtn.textContent;
          }
          settingsRefreshBtn.textContent = '// SYNCING…';
        } else {
          settingsRefreshBtn.disabled = false;
          if (settingsRefreshBtn.dataset.origLabel) {
            settingsRefreshBtn.textContent = settingsRefreshBtn.dataset.origLabel;
            delete settingsRefreshBtn.dataset.origLabel;
          }
        }
      }
      // Dashboard SYNC button. v1.13.25: label adapts to
      // auto_enum_after_sync — `// SYNC THEMERRDB + PLEX` when the
      // cascade is on (the click runs both phases) and `// SYNC
      // THEMERRDB` when off (sync-only). v1.13.19's note about
      // "stable label regardless of auto_enum" was reverted because
      // the user couldn't tell what their click would actually do
      // when the schedule setting was on. setSyncButtonState's
      // idle-restore reads dataset.origLabel so the label stays in
      // sync with the current setting after the run completes.
      const syncBtn = document.getElementById('sync-now-btn');
      if (syncBtn) {
        syncBtn.dataset.origLabel = autoEnum
          ? '// SYNC THEMERRDB + PLEX'
          : '// SYNC THEMERRDB';
        // v1.13.19: dash sync button busy state is owned by
        // setSyncButtonState (idle/running/done with stable
        // // SYNCING… label). Refresh-poll just toggles the
        // disabled flag so a sync triggered from another tab /
        // the daily cron also locks the button here.
        if (dashSyncBtnBusy && syncBtn.textContent === syncBtn.dataset.origLabel) {
          syncBtn.disabled = true;
          syncBtn.textContent = '// SYNCING…';
        } else if (!dashSyncBtnBusy && !syncWatcher
                   && syncBtn.textContent === '// SYNCING…') {
          // v1.13.21 (was v1.13.20): only unlock here when no
          // syncWatcher owns the lifecycle. setSyncButtonState's
          // running → done → idle pipeline (with the 1.5s ✓ DONE
          // flash) is the canonical owner whenever the user clicked
          // SYNC from THIS tab. Pre-fix a fast /api/stats poll could
          // flip the button back to "// SYNC THEMERRDB" before the
          // watcher's done-flash landed, so the button briefly looked
          // clickable mid-flight. Unlock only fires for the cross-tab /
          // cron path (no local watcher) where the poll IS the
          // lifecycle.
          syncBtn.disabled = false;
          syncBtn.textContent = syncBtn.dataset.origLabel;
        }
      }
      // Per-section REFRESH — lock only if THIS section is enumerating.
      document.querySelectorAll('button[data-section-refresh]').forEach((b) => {
        const sid = b.dataset.sectionRefresh;
        lockBtn(b, enumSectionIds.has(sid), '…');
      });
      // Lock per-section MGD checkboxes + A/4K flag pills + libraries
      // SAVE while ANY sync (themerrdb / plex_enum) is in flight at
      // all — pending OR running. v1.11.75: switched from
      // anyEnumRunning (running-only) to anyEnumInFlight so the
      // lock holds across brief gaps between running sections.
      // Pre-fix the user saw MGD checkboxes / SAVE re-enable between
      // sections while 'REFRESHING PLEX…' was still showing.
      const layoutLocked = themerrdbBusy || anyEnumInFlight;
      document.querySelectorAll('input[data-section-toggle]').forEach((cb) => {
        cb.disabled = layoutLocked;
      });
      document.querySelectorAll('button[data-section-row-toggle], button.lib-flag-pill').forEach((b) => {
        if (layoutLocked) {
          if (!b.dataset.preLockDisabled) {
            b.dataset.preLockDisabled = b.disabled ? '1' : '0';
          }
          b.disabled = true;
        } else if (b.dataset.preLockDisabled !== undefined) {
          b.disabled = b.dataset.preLockDisabled === '1';
          delete b.dataset.preLockDisabled;
        }
      });
      const libSaveBtn = document.getElementById('libraries-save-btn');
      if (libSaveBtn) {
        if (layoutLocked) {
          if (!libSaveBtn.dataset.preLockDisabled) {
            libSaveBtn.dataset.preLockDisabled = libSaveBtn.disabled ? '1' : '0';
          }
          libSaveBtn.disabled = true;
        } else if (libSaveBtn.dataset.preLockDisabled !== undefined) {
          libSaveBtn.disabled = libSaveBtn.dataset.preLockDisabled === '1';
          delete libSaveBtn.dataset.preLockDisabled;
          updateLibrariesSaveButton();
        }
      }
    } catch (e) {
      // v1.12.118: stats poll failed — flip the idle pill to an
      // OFFLINE state. Same visual family as IDLE but tone-keyed
      // red so the user notices motif lost its connection.
      const idle = $('#op-status-idle');
      if (idle) {
        idle.classList.add('op-pill-offline');
        const lbl = idle.querySelector('.op-pill-label');
        if (lbl) lbl.textContent = 'OFFLINE';
      }
      // v1.12.2: log the actual error so future "OFFLINE for no
      // apparent reason" debugging doesn't require code-archeology.
      // Silent catch in v1.11.97 hid a ReferenceError for ~5 days.
      try { console.error('refreshTopbarStatus failed:', e); } catch (_) {}
    }
  }

  function bindDryRunBanner() {
    const btn = $('#dry-run-disable-btn');
    if (!btn) return;
    btn.addEventListener('click', async () => {
      if (!confirm('Disable dry-run? Real downloads and placements will resume.')) return;
      try {
        const fd = new FormData();
        fd.append('enabled', 'false');
        await api('POST', '/api/dry-run', fd);
        refreshTopbarStatus();
      } catch (e) {
        alert('Failed: ' + e.message);
      }
    });
  }

  // ---- Dashboard ----

  function renderStat(path, value) {
    $$(`[data-stat="${path}"]`).forEach((el) => {
      el.textContent = typeof value === 'number' ? fmt.num(value) : (value ?? '—');
    });
  }

  async function loadDashboard() {
    if (!$('#stats-grid')) return;
    const stats = await api('GET', '/api/stats');
    renderStat('movies.total', stats.movies.total);
    renderStat('movies.downloaded', stats.movies.downloaded);
    renderStat('movies.placed', stats.movies.placed);
    renderStat('tv.total', stats.tv.total);
    renderStat('tv.downloaded', stats.tv.downloaded);
    renderStat('tv.placed', stats.tv.placed);
    renderStat('queue.pending', stats.queue.pending);
    renderStat('queue.running', stats.queue.running);
    renderStat('queue.failed', stats.queue.failed);

    // Bars
    const movPct = stats.movies.total ? (stats.movies.downloaded / stats.movies.total * 100) : 0;
    const tvPct = stats.tv.total ? (stats.tv.downloaded / stats.tv.total * 100) : 0;
    $('[data-bar-fill="movies"]').style.width = `${movPct}%`;
    $('[data-bar-fill="tv"]').style.width = `${tvPct}%`;

    // v1.13.21: LAST SYNC pre-block removed. Hero `Last run …`
    // line + LIVE OPS drawer cover this surface; no JS populates
    // a #last-sync element any more.

    // v1.13.1 (#2): live last/next sync line under the hero. Reads
    // stats.last_sync (most recent sync_runs row) + stats.next_sync_at
    // (computed from cron); renders relative times so the daily
    // rhythm is legible at a glance without a /settings detour.
    renderDashSyncLine(stats);

    // v1.13.2 (#1): sync history sparkline. Hidden when there's
    // no telemetry data yet (fresh install or pre-v37 schema).
    try {
      const hist = await api('GET', '/api/sync/history?limit=30');
      renderSyncHistory(hist);
    } catch (_) { /* swallow; widget stays hidden */ }

    // v1.12.67: per-section coverage. Hidden if there's only one
    // managed section (no comparison value); rendered as a table
    // with click-through to the matching library tab + 4K toggle.
    try {
      const sec = await api('GET', '/api/sections/coverage');
      renderSectionCoverage(sec.sections || []);
      // v1.13.27: comparison bars also fed from per-section data so
      // each library has its own normalized bar instead of being
      // collapsed into a Movies / TV aggregate.
      renderCoverageComparison(sec.sections || []);
    } catch (_) { /* non-fatal — dashboard still renders */ }

    // v1.13.34: dashboard insights (failure breakdown, sync
    // performance sparkline, daily download activity). Single
    // /api/dashboard/insights round-trip — three chart sections
    // each render from one slice of the response. Each render
    // function hides its block on empty data so a fresh install
    // doesn't render placeholder axes.
    try {
      const ins = await api('GET', '/api/dashboard/insights');
      renderFailureBreakdown(ins.failures || []);
      renderSyncPerformance(ins.syncs || []);
      renderDownloadActivity(ins.downloads || []);
    } catch (_) { /* non-fatal — insights are optional */ }

    // v1.13.21: theme-source pie. Buckets every plex_items row by
    // SRC letter. Hidden when the breakdown is empty (fresh install
    // before first plex_enum); legend is click-to-toggle.
    try {
      renderThemeSourcePie(stats.theme_sources || []);
    } catch (_) { /* non-fatal */ }

    // Recent events
    const evs = await api('GET', '/api/events?limit=20');
    const stream = $('#event-stream');
    stream.innerHTML = evs.events.map((e) => `
      <li>
        <span class="event-time">${htmlEscape(fmt.timeShort(e.ts))}</span>
        <span class="event-level event-level-${htmlEscape(e.level)}">${htmlEscape(e.level)}</span>
        <span class="event-component">${htmlEscape(e.component)}</span>
        <span class="event-msg" title="${htmlEscape(e.message)}">${htmlEscape(e.message)}</span>
      </li>
    `).join('');
  }

  // v1.13.1 (#2): dashboard last/next sync line. Renders relative
  // times next to the hero so the user can see at a glance when
  // motif last ran and when the next scheduled run is. Hidden
  // until the first /api/stats response lands so we don't render
  // an empty line on first paint.
  function fmtRelativePast(iso) {
    if (!iso) return null;
    try {
      const t = new Date(iso).getTime();
      if (!isFinite(t)) return null;
      const sec = Math.max(0, (Date.now() - t) / 1000);
      if (sec < 60)        return 'just now';
      if (sec < 3600)      return `${Math.floor(sec / 60)}m ago`;
      if (sec < 86400)     return `${Math.floor(sec / 3600)}h ago`;
      if (sec < 86400 * 7) return `${Math.floor(sec / 86400)}d ago`;
      return new Date(iso).toLocaleDateString();
    } catch (_) { return null; }
  }
  function fmtRelativeFuture(iso) {
    if (!iso) return null;
    try {
      const t = new Date(iso).getTime();
      if (!isFinite(t)) return null;
      const sec = Math.max(0, (t - Date.now()) / 1000);
      if (sec < 60)        return 'in <1m';
      if (sec < 3600)      return `in ${Math.floor(sec / 60)}m`;
      if (sec < 86400)     return `in ${Math.floor(sec / 3600)}h`;
      if (sec < 86400 * 7) return `in ${Math.floor(sec / 86400)}d`;
      return new Date(iso).toLocaleString();
    } catch (_) { return null; }
  }
  function renderDashSyncLine(stats) {
    const line = document.getElementById('dash-sync-line');
    const nextEl = document.getElementById('dash-sync-next');
    const lastEl = document.getElementById('dash-sync-last');
    if (!line || !nextEl || !lastEl) return;
    const nextRel = fmtRelativeFuture(stats.next_sync_at);
    const ls = stats.last_sync;
    let lastTxt = '';
    if (ls && ls.finished_at) {
      const rel = fmtRelativePast(ls.finished_at);
      const tot = (ls.new_count || 0) + (ls.updated_count || 0);
      const summary = (ls.status === 'success'
        ? (tot === 0 ? 'no changes' : `${fmt.num(tot)} change${tot !== 1 ? 's' : ''}`)
        : `${ls.status}`);
      lastTxt = `Last run ${rel || ls.finished_at} · ${summary}`;
    } else if (ls && ls.started_at) {
      lastTxt = `Last run started ${fmtRelativePast(ls.started_at) || ls.started_at} (still running)`;
    } else {
      lastTxt = 'No sync runs yet';
    }
    const nextTxt = nextRel
      ? `Next sync ${nextRel}`
      : 'Next sync schedule unavailable';
    nextEl.textContent = nextTxt;
    lastEl.textContent = lastTxt;
    line.style.display = '';
  }

  // v1.13.16 (B1): sync history bar chart replaced with a sparse
  // 5-row table. The bars made every run look like decoration; a
  // tabular view makes each datum legible at a glance: timestamp,
  // transport, duration, change count, status. Pre-fix runs that
  // pre-dated the v37 telemetry columns rendered as "UNKNOWN" with
  // an unhelpful tooltip — those rows now render with a muted
  // dash where their transport would be, no special-case visual.
  let _lastSyncHistoryKey = '';

  function renderSyncHistory(payload) {
    const block = document.getElementById('sync-history-block');
    const barsEl = document.getElementById('sync-history-bars');
    const sumEl = document.getElementById('sync-history-summary');
    if (!block || !barsEl || !sumEl) return;
    const runs = (payload && payload.runs) || [];
    if (!runs.length) { block.style.display = 'none'; return; }
    const key = JSON.stringify(payload);
    if (key === _lastSyncHistoryKey) {
      block.style.display = '';
      return;
    }
    _lastSyncHistoryKey = key;
    block.style.display = '';
    const fmt = (n) => (n == null ? '—' : Number(n).toLocaleString());
    // Most-recent first so the table reads top-down chronologically.
    const recent = runs.slice().sort((a, b) =>
      String(b.started_at || '').localeCompare(String(a.started_at || ''))
    ).slice(0, 5);
    const rows = recent.map((r) => {
      const startedShort = (r.started_at || '')
        .replace('T', ' ')
        .replace(/\..*$/, '')
        .replace(/\+.*$/, '');
      const transport = r.transport
        ? `<span class="sync-hist-transport sync-hist-transport-${r.transport}">${htmlEscape(r.transport.toUpperCase())}</span>`
        : '<span class="muted">—</span>';
      const wcTxt = r.wall_clock_seconds == null
        ? '<span class="muted">running…</span>'
        : `${r.wall_clock_seconds.toFixed(1)}s`;
      const tot = (r.new_count || 0) + (r.updated_count || 0);
      const changeTxt = r.no_changes
        ? '<span class="muted">no changes</span>'
        : tot > 0
          ? `${fmt(r.new_count)} new · ${fmt(r.updated_count)} upd`
          : '<span class="muted">no changes</span>';
      let statusBadge;
      if (r.status === 'failed') {
        statusBadge = '<span class="sync-hist-status sync-hist-status-failed">FAIL</span>';
      } else if (r.fallback_reason) {
        statusBadge = `<span class="sync-hist-status sync-hist-status-fallback" title="${htmlEscape(r.fallback_reason)}">FALLBACK</span>`;
      } else if (r.no_changes) {
        statusBadge = '<span class="sync-hist-status sync-hist-status-noop">NO-OP</span>';
      } else {
        statusBadge = '<span class="sync-hist-status sync-hist-status-ok">OK</span>';
      }
      return `<tr>
        <td class="sync-hist-when">${htmlEscape(startedShort)}</td>
        <td class="sync-hist-tx">${transport}</td>
        <td class="sync-hist-dur">${wcTxt}</td>
        <td class="sync-hist-changes">${changeTxt}</td>
        <td class="sync-hist-status-cell">${statusBadge}</td>
      </tr>`;
    }).join('');
    barsEl.innerHTML = `<table class="sync-hist-table">
      <thead><tr>
        <th>WHEN (UTC)</th><th>TRANSPORT</th><th>DURATION</th>
        <th>CHANGES</th><th>STATUS</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
    // Per-transport summary row — kept as a one-line footer so the
    // dashboard glance still answers "which path do I usually take?"
    const summary = (payload && payload.summary) || [];
    const known = summary.filter((s) => s.transport && s.transport !== 'unknown');
    if (!known.length) {
      sumEl.innerHTML = '<span class="muted small">// no completed runs yet</span>';
      return;
    }
    sumEl.innerHTML = known.map((s) => {
      const fbNote = s.fallback_count
        ? ` <span class="muted">· ${s.fallback_count} fb</span>` : '';
      const noopNote = s.no_change_count
        ? ` <span class="muted">· ${s.no_change_count} no-op</span>` : '';
      return `<span class="sync-history-sum sync-history-sum-${s.transport}">`
        + `<span class="sync-history-sum-label">${htmlEscape(s.transport.toUpperCase())}</span> `
        + `<b>${s.avg_wall_clock}s</b> avg `
        + `<span class="muted">· ${s.count} runs</span>`
        + fbNote + noopNote
        + `</span>`;
    }).join('');
  }

  // v1.13.21: theme-source pie. Renders a donut chart of the
  // SRC-letter distribution coming from /api/stats.theme_sources.
  // Legend pills are click-to-toggle: clicking dims the slice and
  // removes its share from the centered total + percentages so the
  // user can ask "of just the items WITH a theme, how are sources
  // split?" by hiding the "-" wedge.
  //
  // Persisted hide-set lives in localStorage so the user's filter
  // preference survives page reloads (mirrors the library filter
  // chip toolbar's persistence pattern).
  const _SOURCE_PIE_HIDE_KEY = 'motif:dash:src-hide';
  const _SOURCE_LETTER_META = [
    { letter: 'T', cls: 'T', name: 'ThemerrDB' },
    { letter: 'A', cls: 'A', name: 'Adopted' },
    { letter: 'U', cls: 'U', name: 'User-supplied' },
    { letter: 'M', cls: 'M', name: 'Manual sidecar' },
    { letter: 'P', cls: 'P', name: 'Plex-served' },
    { letter: '-', cls: 'X', name: 'No theme' },
  ];
  let _lastSourcePieKey = '';
  let _sourcePieHidden = (() => {
    try {
      const raw = localStorage.getItem(_SOURCE_PIE_HIDE_KEY);
      return raw ? new Set(JSON.parse(raw)) : new Set();
    } catch (_) { return new Set(); }
  })();
  function _persistSourcePieHidden() {
    try {
      localStorage.setItem(_SOURCE_PIE_HIDE_KEY,
        JSON.stringify(Array.from(_sourcePieHidden)));
    } catch (_) { /* localStorage full / disabled */ }
  }
  // Cache the most recent rows so legend clicks can re-render
  // without re-fetching /api/stats.
  let _lastSourcePieRows = [];

  function renderThemeSourcePie(rows) {
    const block = document.getElementById('source-breakdown-block');
    const slicesEl = document.getElementById('source-pie-slices');
    const legendEl = document.getElementById('source-breakdown-legend');
    const centerNum = document.getElementById('source-pie-center-num');
    const sumEl = document.getElementById('source-breakdown-summary');
    if (!block || !slicesEl || !legendEl || !centerNum) return;

    // Stash for legend click handler.
    _lastSourcePieRows = rows;

    // Aggregate per-letter across media types — the legend can
    // be filtered later if a media-type split is wanted; for v1
    // the chart shows the full library picture.
    const totals = Object.fromEntries(
      _SOURCE_LETTER_META.map((m) => [m.letter, 0])
    );
    let grand = 0;
    for (const r of rows) {
      const k = totals[r.letter] != null ? r.letter : '-';
      totals[k] += r.count || 0;
      grand += r.count || 0;
    }
    if (grand <= 0) { block.style.display = 'none'; return; }

    // Active = letters NOT in the hidden set.
    const visibleTotal = _SOURCE_LETTER_META
      .filter((m) => !_sourcePieHidden.has(m.letter))
      .reduce((acc, m) => acc + totals[m.letter], 0);

    const key = JSON.stringify({ totals, hidden: Array.from(_sourcePieHidden) });
    if (key === _lastSourcePieKey) { block.style.display = ''; return; }
    _lastSourcePieKey = key;
    block.style.display = '';

    // Build slice <circle>s. r=15.915 → circumference ≈ 100 so the
    // dasharray length doubles as a percentage. Each slice's offset
    // advances by the cumulative percentage of preceding visible
    // slices; hidden slices contribute 0 to the dasharray but stay
    // in the SVG (dimmed) so toggling back in animates from the
    // same axis.
    const denom = visibleTotal > 0 ? visibleTotal : 1;
    let cumulative = 0;
    const sliceMarkup = _SOURCE_LETTER_META.map((m) => {
      const n = totals[m.letter];
      const hidden = _sourcePieHidden.has(m.letter);
      const pct = (hidden || n <= 0) ? 0 : (n / denom) * 100;
      const dash = `${pct.toFixed(3)} ${(100 - pct).toFixed(3)}`;
      const offset = (100 - cumulative) % 100;
      cumulative += pct;
      return `<circle class="source-pie-slice source-pie-${m.cls} ${hidden ? 'dim' : ''}"
        cx="21" cy="21" r="15.915"
        stroke-dasharray="${dash}" stroke-dashoffset="${offset.toFixed(3)}"></circle>`;
    }).join('');
    slicesEl.innerHTML = sliceMarkup;

    centerNum.textContent = (visibleTotal || grand).toLocaleString();

    // Legend — letter, name, count, percent. Click toggles hide.
    legendEl.innerHTML = _SOURCE_LETTER_META.map((m) => {
      const n = totals[m.letter];
      const hidden = _sourcePieHidden.has(m.letter);
      const pct = visibleTotal > 0 && !hidden
        ? ((n / visibleTotal) * 100).toFixed(n / visibleTotal >= 0.1 ? 0 : 1)
        : '—';
      const safeLetter = m.letter === '-' ? '–' : m.letter;
      return `<button type="button" class="source-legend-item ${hidden ? 'off' : ''}"
                      data-letter="${m.letter}"
                      title="click to ${hidden ? 'show' : 'hide'} ${htmlEscape(m.name)}">
        <span class="source-legend-swatch source-legend-swatch-${m.cls}"></span>
        <span class="source-legend-letter source-pie-${m.cls}-text">${safeLetter}</span>
        <span class="source-legend-name">${htmlEscape(m.name)}</span>
        <span class="source-legend-count">${n.toLocaleString()}</span>
        <span class="source-legend-pct">${hidden ? 'hidden' : pct + '%'}</span>
      </button>`;
    }).join('');
    if (sumEl) {
      const hiddenCount = _sourcePieHidden.size;
      sumEl.textContent = hiddenCount > 0
        ? `${visibleTotal.toLocaleString()} of ${grand.toLocaleString()} items · ${hiddenCount} bucket${hiddenCount === 1 ? '' : 's'} hidden`
        : `${grand.toLocaleString()} items across all sources`;
    }
  }

  // Click delegate for legend toggle. Lives outside the renderer so
  // re-renders don't accumulate listeners.
  document.addEventListener('click', (ev) => {
    const btn = ev.target.closest('.source-legend-item');
    if (!btn) return;
    const letter = btn.dataset.letter;
    if (!letter) return;
    if (_sourcePieHidden.has(letter)) _sourcePieHidden.delete(letter);
    else _sourcePieHidden.add(letter);
    _persistSourcePieHidden();
    _lastSourcePieKey = '';  // force re-render
    renderThemeSourcePie(_lastSourcePieRows);
  });

  // v1.12.67: render the per-section coverage table on the
  // dashboard. Hidden when only one section is managed (no
  // comparison story); otherwise lists each section with its
  // total / themed / unthemed / failures / pending-updates plus
  // a click-through that lands the user on the right library
  // tab + 4K variant.
  // v1.13.12: section-coverage render guard. Pre-fix every dashboard
  // poll (30s + post-sync) re-wrote body.innerHTML even when the
  // numbers hadn't changed, causing a brief but visible flash. Hash
  // the inputs and skip the swap when nothing differs.
  let _lastSectionCoverageKey = '';

  function renderSectionCoverage(sections) {
    const block = document.getElementById('section-coverage-block');
    const body = document.getElementById('section-coverage-body');
    if (!block || !body) return;
    if (!sections.length || sections.length < 2) {
      block.style.display = 'none';
      return;
    }
    const key = JSON.stringify(sections);
    if (key === _lastSectionCoverageKey) {
      // Idempotent — no DOM swap, no flash.
      block.style.display = '';
      return;
    }
    _lastSectionCoverageKey = key;
    block.style.display = '';
    body.innerHTML = sections.map((s) => {
      const fourkLabel = s.is_4k ? '4K' : 'STD';
      const typeLabel = s.tab === 'anime' ? 'ANIME'
                      : s.tab === 'tv'    ? 'TV'
                      :                     'MOVIES';
      const href = `/${s.tab}?fourk=${s.is_4k ? 1 : 0}`;
      // v1.12.120: failures + pending cells own click-through to a
      // pre-filtered library view. Failures land on the red ✗ pill
      // filter; pending lands on the blue ↑ pill filter. Cells set
      // data-href + class="col-clickable" so the row's own
      // click-through skips them (handler checks the closest <td>).
      const failureHref = `/${s.tab}?fourk=${s.is_4k ? 1 : 0}&tdb_pills=dead`;
      const pendingHref = `/${s.tab}?fourk=${s.is_4k ? 1 : 0}&tdb_pills=update`;
      const failureCell = s.failures > 0
        ? `<td class="col-num accent-red col-clickable" data-href="${htmlEscape(failureHref)}" title="Click to filter library to broken upstream URLs">${fmt.num(s.failures)}</td>`
        : `<td class="col-num muted">0</td>`;
      const pendingCell = s.pending_updates > 0
        ? `<td class="col-num accent col-clickable" data-href="${htmlEscape(pendingHref)}" title="Click to filter library to pending TDB updates">${fmt.num(s.pending_updates)}</td>`
        : `<td class="col-num muted">0</td>`;
      const unthemedCell = s.unthemed > 0
        ? `<td class="col-num">${fmt.num(s.unthemed)}</td>`
        : `<td class="col-num muted">0</td>`;
      // Drill-through: clicking a row navigates to the library
      // tab. Whole-row clickability via data-href + cursor:pointer
      // styling so the click target isn't just a tiny link.
      return `
        <tr class="section-coverage-row" data-href="${htmlEscape(href)}">
          <td>${htmlEscape(s.title || '')}</td>
          <td class="col-year col-section-type">
            <span class="section-type-main">${typeLabel}</span>
            <span class="section-type-sub muted small">${fourkLabel}</span>
          </td>
          <td class="col-num"><b>${fmt.num(s.total || 0)}</b></td>
          <td class="col-num accent-green">${fmt.num(s.themed || 0)}</td>
          ${unthemedCell}
          ${failureCell}
          ${pendingCell}
        </tr>
      `;
    }).join('');
    // Bind click-through. Idempotent: removing-and-readding a
    // listener on each render avoids leaks across loadDashboard
    // calls. v1.12.120: a click that originated on a cell with
    // its own data-href (failures / pending count) takes
    // precedence over the row's default tab landing — so the
    // user lands on the filtered view they asked for instead of
    // the raw library tab.
    body.querySelectorAll('tr.section-coverage-row').forEach((tr) => {
      tr.addEventListener('click', (ev) => {
        const cell = ev.target.closest('td.col-clickable');
        if (cell && cell.dataset.href) {
          ev.stopPropagation();
          window.location.href = cell.dataset.href;
          return;
        }
        const href = tr.getAttribute('data-href');
        if (href) window.location.href = href;
      });
    });
  }

  // v1.13.34: failure-kind breakdown — horizontal bars, sorted by
  // count desc. Each bar links the user into the library filtered
  // to its failure kind (?status=failures&fk=<kind>). Hidden when
  // there are no unacked failures so a clean install doesn't
  // render an empty rail. Cache hash so the 30s dashboard poll
  // doesn't redraw when nothing changed.
  let _lastInsightFailuresKey = '';
  function renderFailureBreakdown(rows) {
    const block = document.getElementById('insight-failures-block');
    const body = document.getElementById('insight-failures-body');
    if (!block || !body) return;
    if (!rows.length) { block.style.display = 'none'; return; }
    const key = JSON.stringify(rows);
    if (key === _lastInsightFailuresKey) {
      block.style.display = '';
      return;
    }
    _lastInsightFailuresKey = key;
    block.style.display = '';
    const max = rows.reduce((acc, r) => Math.max(acc, r.count), 0) || 1;
    body.innerHTML = rows.map((r) => {
      const pct = (r.count / max) * 100;
      // Library deeplink: any tab works since the failure pill
      // filter is global; pick movies as a sensible default and
      // pre-apply the dead TDB pill so the user lands on rows
      // that actually carry failure_kind. The kind itself isn't
      // a per-row filter today — we pass it as a hint via &fk=
      // even though the library doesn't read it yet, so future
      // narrowing works without a dashboard rev.
      const href = `/movies?tdb_pills=dead&fk=${encodeURIComponent(r.kind)}`;
      return `<a class="insight-fail-row"
                  href="${htmlEscape(href)}"
                  title="${htmlEscape(r.label)}: ${fmt.num(r.count)} unacked. Click to view in library.">
        <span class="insight-fail-label">${htmlEscape(r.label)}</span>
        <span class="insight-fail-bar">
          <span class="insight-fail-bar-fill" style="width:${pct}%"></span>
        </span>
        <span class="insight-fail-count">${fmt.num(r.count)}</span>
      </a>`;
    }).join('');
  }

  // v1.13.34: sync-performance sparkline. SVG line chart of the
  // last 30 sync runs' wall_clock_seconds. Markers are color-coded
  // by status (no_changes = muted, failed = red, otherwise green).
  // Tooltip on hover via title= carries the per-point detail
  // (timestamp, transport, duration, no_changes/error).
  let _lastInsightSyncsKey = '';
  function renderSyncPerformance(rows) {
    const block = document.getElementById('insight-syncs-block');
    const body = document.getElementById('insight-syncs-body');
    if (!block || !body) return;
    if (rows.length < 2) { block.style.display = 'none'; return; }
    // v1.13.35: hide when there's no real duration signal — every
    // row at zero seconds (e.g., a series of immediate-cancel
    // runs) would normalize to a flat baseline that reads as
    // "everything's stuck" when really there's just no data.
    // Server-side filter excludes NULL wall_clock_seconds; this
    // additionally guards the all-zero edge.
    const haveRealDuration = rows.some((r) => (r.wall_clock_seconds || 0) > 0);
    if (!haveRealDuration) { block.style.display = 'none'; return; }
    const key = JSON.stringify(rows.map((r) => [
      r.finished_at, r.wall_clock_seconds, r.transport, r.status, r.no_changes,
    ]));
    if (key === _lastInsightSyncsKey) {
      block.style.display = '';
      return;
    }
    _lastInsightSyncsKey = key;
    block.style.display = '';
    // Geometry: fixed-aspect SVG (viewBox-driven so the chart
    // scales with the parent without being pixel-pegged). Padding
    // gives the markers room to render without clipping at the
    // chart edges.
    const W = 600;
    const H = 80;
    const PAD_X = 8;
    const PAD_Y = 6;
    const innerW = W - PAD_X * 2;
    const innerH = H - PAD_Y * 2;
    const seconds = rows.map((r) => r.wall_clock_seconds || 0);
    const maxSec = Math.max(...seconds, 1);
    const minSec = 0;
    const xAt = (i) => PAD_X + (rows.length === 1 ? innerW / 2 : (i / (rows.length - 1)) * innerW);
    const yAt = (s) => PAD_Y + innerH - ((s - minSec) / (maxSec - minSec)) * innerH;
    const linePath = rows.map((r, i) => {
      const x = xAt(i).toFixed(1);
      const y = yAt(seconds[i]).toFixed(1);
      return (i === 0 ? 'M' : 'L') + x + ',' + y;
    }).join(' ');
    // Markers: a small circle per run. Color = status.
    const markers = rows.map((r, i) => {
      const cx = xAt(i).toFixed(1);
      const cy = yAt(seconds[i]).toFixed(1);
      let cls = 'sync-mark-ok';
      if (r.status === 'failed') cls = 'sync-mark-fail';
      else if (r.no_changes) cls = 'sync-mark-noop';
      // Tooltip carries the full record so the user can drill
      // without leaving the page. Local time format keeps it
      // readable; transport tag is short.
      const when = (r.finished_at || '').replace('T', ' ').replace(/\..*/, '');
      const trans = (r.transport || '?').toUpperCase();
      const noOp = r.no_changes ? ' · no-op' : '';
      const fail = r.status === 'failed' ? ' · FAILED' : '';
      const detail = ` · ${seconds[i].toFixed(1)}s · ${r.new_count || 0}n/${r.updated_count || 0}u`;
      const title = `${when} · ${trans}${detail}${noOp}${fail}`;
      return `<circle class="${cls}" cx="${cx}" cy="${cy}" r="3">
        <title>${htmlEscape(title)}</title>
      </circle>`;
    }).join('');
    // Labels for context: max seconds on the right edge, run
    // count on the left. Keeps the chart self-explanatory without
    // a full axis system.
    const maxLabel = `${maxSec.toFixed(1)}s`;
    body.innerHTML = `
      <div class="sync-spark-wrap">
        <svg viewBox="0 0 ${W} ${H}"
             preserveAspectRatio="none"
             class="sync-spark-svg"
             aria-label="Sync wall-clock per run">
          <path class="sync-spark-line" d="${linePath}"/>
          ${markers}
        </svg>
        <div class="sync-spark-meta muted small">
          <span>oldest · ${rows.length} runs</span>
          <span>peak ${htmlEscape(maxLabel)}</span>
        </div>
      </div>`;
  }

  // v1.13.34: daily download activity bars. One bar per day for
  // the last 30 days (server already trims with the 30-day
  // window). Days with zero downloads still get a bar slot so
  // the X-axis cadence stays consistent — empty slots are
  // sized to a hairline rather than truly zero so the user
  // sees the day exists but no work happened.
  let _lastInsightDownloadsKey = '';
  function renderDownloadActivity(rows) {
    const block = document.getElementById('insight-downloads-block');
    const body = document.getElementById('insight-downloads-body');
    if (!block || !body) return;
    if (!rows.length) { block.style.display = 'none'; return; }
    const key = JSON.stringify(rows);
    if (key === _lastInsightDownloadsKey) {
      block.style.display = '';
      return;
    }
    _lastInsightDownloadsKey = key;
    block.style.display = '';
    // Backfill missing days so bars cadence cleanly. Server
    // returns only days WITH at least one download; we want a
    // fixed 30-slot horizontal axis so the user sees idle days
    // too. Iterate from 29 days ago → today, fill from the
    // server map.
    const have = new Map(rows.map((r) => [r.day, r.count]));
    const today = new Date();
    const days = [];
    for (let i = 29; i >= 0; i--) {
      const d = new Date(today);
      d.setDate(d.getDate() - i);
      const iso = d.toISOString().slice(0, 10);
      days.push({ day: iso, count: have.get(iso) || 0 });
    }
    const max = days.reduce((acc, r) => Math.max(acc, r.count), 0) || 1;
    const total = days.reduce((acc, r) => acc + r.count, 0);
    body.innerHTML = `
      <div class="dl-activity-wrap">
        <div class="dl-activity-bars">
          ${days.map((r) => {
            const pct = r.count > 0 ? Math.max(2, (r.count / max) * 100) : 0;
            const tip = r.count > 0
              ? `${r.day}: ${fmt.num(r.count)} download${r.count === 1 ? '' : 's'}`
              : `${r.day}: idle`;
            return `<span class="dl-activity-bar"
                          style="height:${pct}%"
                          title="${htmlEscape(tip)}"></span>`;
          }).join('')}
        </div>
        <div class="dl-activity-meta muted small">
          <span>30 days</span>
          <span>${fmt.num(total)} total · peak ${fmt.num(max)}/day</span>
        </div>
      </div>`;
  }

  // Polled by the SYNC button to know when both the sync + plex_enum
  // jobs have finished. Set when the user clicks // SYNC, cleared when
  // /api/stats reports queue.sync_in_flight == 0.
  let syncWatcher = null;

  function setSyncButtonState(state) {
    // v1.13.19: re-introduce a one-word busy label so the user gets
    // immediate confirmation their click landed (without waiting on
    // the topbar status pill's poll round-trip). The bad pattern in
    // v1.13.18 was multi-stage swaps ("SCANNING TV SHOWS…" → next
    // section) — those still don't happen because we use a single
    // generic // SYNCING… that's stable for the whole run.
    const btn = $('#sync-now-btn');
    if (!btn) return;
    const idleLabel = btn.dataset.origLabel || '// SYNC THEMERRDB';
    if (state === 'idle') {
      btn.disabled = false;
      btn.textContent = idleLabel;
    } else if (state === 'running') {
      btn.disabled = true;
      btn.textContent = '// SYNCING…';
    } else if (state === 'done') {
      btn.disabled = true;
      btn.textContent = '✓ DONE';
      setTimeout(() => setSyncButtonState('idle'), 1500);
    }
  }

  function bindDashboard() {
    const syncBtn = $('#sync-now-btn');
    if (!syncBtn) return;
    syncBtn.addEventListener('click', async (ev) => {
      setSyncButtonState('running');
      // v1.13.19: paint the topbar mini-bar optimistically so the
      // user sees a SYNCING pill immediately on click instead of
      // waiting for the worker's idle-wait + first poll round-trip
      // (1-2s of stale IDLE state pre-fix). The placeholder gets
      // replaced by the real op-progress row as soon as it lands.
      // boostPoll is called inside setOptimisticPlaceholder so we
      // don't need to call it again here.
      try {
        if (window.motifOps && window.motifOps.setOptimisticPlaceholder) {
          window.motifOps.setOptimisticPlaceholder('tdb_sync', '// SYNCING THEMERRDB');
        }
      } catch (_) {}
      try {
        // metadata_only: don't auto-enqueue downloads. Downloads happen
        // explicitly from /movies, /tv, /anime via the missing-themes
        // banner or per-row RE-DL.
        await api('POST', '/api/sync/now', { metadata_only: true });
      } catch (e) {
        alert('Sync failed: ' + e.message);
        setSyncButtonState('idle');
        return;
      }
      paintTopbarSyncing('SYNCING THEMERRDB');
      // v1.12.127: 'done' means the action this user triggered has
      // finished. With auto_enum=ON that's both phases (sync + the
      // auto-enqueued enum); with auto_enum=OFF it's just the sync
      // job. Polling auto_enum_after_sync per-tick lets the user
      // change the setting mid-flight and have the watcher adapt.
      if (syncWatcher) clearInterval(syncWatcher);
      let primed = false;
      syncWatcher = setInterval(async () => {
        try {
          const s = await api('GET', '/api/stats');
          const tdbInFlight = (s.queue && s.queue.themerrdb_sync_in_flight) || 0;
          const enumInFlight = (s.queue && s.queue.plex_enum_in_flight) || 0;
          const autoEnum = !(s.queue && s.queue.auto_enum_after_sync === false);
          const inFlight = autoEnum
            ? (tdbInFlight + enumInFlight)
            : tdbInFlight;
          if (inFlight > 0) primed = true;
          if (primed && inFlight === 0) {
            clearInterval(syncWatcher);
            syncWatcher = null;
            setSyncButtonState('done');
            loadDashboard().catch(console.error);
          }
        } catch (e) { /* ignore transient errors */ }
      }, 2000);
    });

    // If the page loads while a sync OR plex_enum is already in
    // progress (left running by another tab/session, or daily cron
    // mid-run), reflect that. v1.12.127: also honors auto_enum.
    api('GET', '/api/stats').then((s) => {
      const tdbBusy = (s && s.queue && s.queue.themerrdb_sync_in_flight) || 0;
      const enumBusy = (s && s.queue && s.queue.plex_enum_in_flight) || 0;
      const autoEnum = !(s && s.queue && s.queue.auto_enum_after_sync === false);
      const initialBusy = autoEnum ? (tdbBusy + enumBusy) : tdbBusy;
      if (initialBusy > 0) {
        setSyncButtonState('running');
        if (syncWatcher) clearInterval(syncWatcher);
        // v1.13.29: was `primed = true`, which fired ✓ DONE on the
        // reload path for a sync the user never triggered (cron tail
        // landing during page load → the FIRST poll saw inFlight=0
        // and immediately flashed DONE). Click path (~line 1268)
        // correctly starts primed=false; this reload path now
        // matches it. Cost: if the cron sync finishes between
        // setSyncButtonState('running') above and the first poll
        // tick below, the button just clears silently on the next
        // refreshTopbarStatus tick (which has its own unlock path
        // gated on `!syncWatcher && !dashSyncBtnBusy`) instead of
        // claiming "DONE" for an action the user didn't take.
        let primed = false;
        syncWatcher = setInterval(async () => {
          try {
            const s2 = await api('GET', '/api/stats');
            const t2 = (s2.queue && s2.queue.themerrdb_sync_in_flight) || 0;
            const e2 = (s2.queue && s2.queue.plex_enum_in_flight) || 0;
            const ae2 = !(s2.queue && s2.queue.auto_enum_after_sync === false);
            const inFlight2 = ae2 ? (t2 + e2) : t2;
            if (inFlight2 > 0) primed = true;
            if (primed && inFlight2 === 0) {
              clearInterval(syncWatcher);
              syncWatcher = null;
              setSyncButtonState('done');
              loadDashboard().catch(console.error);
            } else if (!primed && inFlight2 === 0) {
              // Sync finished between page-load probe and first
              // tick — we never saw busy here. Clear the watcher
              // and let refreshTopbarStatus's poll-driven unlock
              // handle the button label.
              clearInterval(syncWatcher);
              syncWatcher = null;
            }
          } catch (e) { /* ignore */ }
        }, 2000);
      }
    }).catch(()=>{});
  }

  // ---- Browse (movies / tv) ----

  const browseState = {
    mediaType: null,
    page: 1,
    perPage: 50,
    q: '',
    status: 'all',
  };

  async function loadItems() {
    const params = new URLSearchParams({
      media_type: browseState.mediaType,
      page: browseState.page,
      per_page: browseState.perPage,
      q: browseState.q,
      status: browseState.status,
    });
    const data = await api('GET', `/api/items?${params}`);
    const body = $('#items-body');
    if (!data.items.length) {
      body.innerHTML = '<tr><td colspan="9" class="muted center">no results — try clearing the search or broadening the filter chips above</td></tr>';
    } else {
      body.innerHTML = data.items.map((it) => {
        const dl = it.downloaded ? 'on' : '';
        const pl = it.placed ? 'on' : '';
        const linkKind = it.placed ? it.placed.placement_kind : null;
        const provenance = it.placed ? it.placed.provenance
                          : (it.downloaded ? it.downloaded.provenance : null);

        // Inline glyphs prepended to title cell.
        let rowExtra = '';
        const titleGlyphs = [];
        if (it.failure_kind) {
          const human = {
            'cookies_expired': 'YouTube cookies expired',
            'video_private': 'Video is private',
            'video_removed': 'Video was removed',
            'video_age_restricted': 'Age-restricted',
            'geo_blocked': 'Geo-blocked',
            'network_error': 'Network error',
            'unknown': 'Unknown failure'
          }[it.failure_kind] || it.failure_kind;
          const failMsg = it.failure_message ? ' — ' + it.failure_message : '';
          titleGlyphs.push(
            `<button class="title-glyph title-glyph-fail" title="${htmlEscape(human + failMsg)}" `
            + `data-act="open-override" data-mt="${it.media_type}" data-id="${it.tmdb_id}" `
            + `data-kind="${htmlEscape(it.failure_kind)}" data-kind-human="${htmlEscape(human)}" `
            + `data-msg="${htmlEscape(it.failure_message || '')}" type="button">⚠</button>`
          );
          rowExtra = ' class="row-failure"';
        }
        if (it.pending_update) {
          titleGlyphs.push(
            `<button class="title-glyph title-glyph-update" title="Upstream update available — click to review" `
            + `data-act="open" data-mt="${it.media_type}" data-id="${it.tmdb_id}" type="button">↑</button>`
          );
        }

        let linkCell;
        if (linkKind === 'hardlink') {
          linkCell = '<span class="link-glyph link-glyph-hardlink" title="hardlink (efficient)">HL</span>';
        } else if (linkKind === 'copy') {
          linkCell = '<span class="link-glyph link-glyph-copy" title="copy (uses extra disk)">C</span>';
        } else {
          linkCell = '<span class="link-glyph link-glyph-none">—</span>';
        }

        let srcCell;
        if (it.upstream_source === 'plex_orphan') {
          srcCell = '<span class="link-badge link-badge-orphan" title="adopted orphan (no upstream record)">O</span>';
        } else if (provenance === 'manual') {
          srcCell = '<span class="link-badge link-badge-manual" title="Manual sidecar (click ADOPT to manage)">M</span>';
        } else if (provenance === 'cloud') {
          srcCell = '<span class="link-badge link-badge-cloud" title="Plex cloud theme">☁</span>';
        } else if (provenance === 'auto') {
          srcCell = '<span class="muted" title="auto from ThemerrDB">A</span>';
        } else {
          srcCell = '<span class="muted">—</span>';
        }

        const ovr = it.override ? `<span title="user override URL set" style="color:var(--magenta)">⚑</span>` : '';
        const imdb = it.imdb_id || '';
        const imdbLink = imdb
          ? `<a href="https://www.imdb.com/title/${htmlEscape(imdb)}" target="_blank" rel="noopener">${htmlEscape(imdb)}</a>`
          : '<span class="muted">—</span>';

        const isOrphan = it.upstream_source === 'plex_orphan';
        const deleteBtn = isOrphan
          ? `<button class="btn btn-tiny btn-danger" data-act="delete-orphan" data-mt="${it.media_type}" data-id="${it.tmdb_id}" data-title="${htmlEscape(it.title || '')}" title="purge this orphan: deletes motif's database record and every associated theme file — your Plex library item itself stays put. Cannot be undone.">× DEL</button>`
          : '';
        const actions = it.pending_update
          ? `<button class="btn btn-tiny" data-act="open" data-mt="${it.media_type}" data-id="${it.tmdb_id}" title="Open details for this row">DETAILS</button>
             <button class="btn btn-tiny btn-info" data-act="accept-update" data-mt="${it.media_type}" data-id="${it.tmdb_id}" title="Apply the new TDB URL">ACCEPT</button>
             <button class="btn btn-tiny" data-act="decline-update" data-mt="${it.media_type}" data-id="${it.tmdb_id}" title="Dismiss this update">KEEP</button>
             ${deleteBtn}`
          : `<button class="btn btn-tiny" data-act="open" data-mt="${it.media_type}" data-id="${it.tmdb_id}" title="Open details for this row">DETAILS</button>
             <button class="btn btn-tiny btn-warn" data-act="redl" data-mt="${it.media_type}" data-id="${it.tmdb_id}" title="re-download from ThemerrDB — replaces motif's current theme file with a fresh download from the upstream URL">RE-DL</button>
             ${deleteBtn}`;

        return `
          <tr${rowExtra}>
            <td>
              <div class="title-cell">
                ${titleGlyphs.join('')}
                ${ovr}
                <span class="title-cell-name">${htmlEscape(it.title)}</span>
              </div>
            </td>
            <td class="col-year">${htmlEscape(it.year ?? '')}</td>
            <td class="col-state"><span class="state-pill ${dl}"></span></td>
            <td class="col-state"><span class="state-pill ${pl}"></span></td>
            <td class="col-state">${linkCell}</td>
            <td class="col-state">${srcCell}</td>
            <td class="col-imdb">${imdbLink}</td>
            <td class="col-actions">${actions}</td>
          </tr>
        `;
      }).join('');
    }
    const totalPages = Math.max(1, Math.ceil(data.total / browseState.perPage));
    $('#result-count').textContent = `· ${fmt.num(data.total)} match${data.total === 1 ? '' : 'es'}`;
    $('#pager').innerHTML = `
      <button data-page="${browseState.page - 1}" ${browseState.page <= 1 ? 'disabled' : ''}>« prev</button>
      <span>page ${browseState.page} / ${totalPages}</span>
      <button data-page="${browseState.page + 1}" ${browseState.page >= totalPages ? 'disabled' : ''}>next »</button>
    `;
  }

  function bindBrowse() {
    const input = $('#search-input');
    if (!input) return;
    browseState.mediaType = input.dataset.mediaType;

    // Honor ?status= URL param so the topbar UPD/! badges link to filtered views
    const urlStatus = new URLSearchParams(window.location.search).get('status');
    if (urlStatus && ['all','downloaded','missing','placed','unplaced','failures','updates','manual'].includes(urlStatus)) {
      browseState.status = urlStatus;
      $$('.chip[data-status]').forEach((c) => {
        if (c.dataset.status === urlStatus) c.classList.add('chip-active');
        else c.classList.remove('chip-active');
      });
    }

    let debounce;
    input.addEventListener('input', () => {
      clearTimeout(debounce);
      debounce = setTimeout(() => {
        browseState.q = input.value.trim();
        browseState.page = 1;
        loadItems().catch(console.error);
      }, 250);
    });

    $$('.chip[data-status]').forEach((c) => {
      c.addEventListener('click', () => {
        $$('.chip[data-status]').forEach((x) => x.classList.remove('chip-active'));
        c.classList.add('chip-active');
        browseState.status = c.dataset.status;
        browseState.page = 1;
        loadItems().catch(console.error);
      });
    });

    document.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-act]');
      if (!btn) return;
      const mt = btn.dataset.mt;
      const id = btn.dataset.id;
      if (btn.dataset.act === 'open') {
        openItemDialog(mt, id).catch(console.error);
      } else if (btn.dataset.act === 'redl') {
        redownload(mt, id, btn).catch(console.error);
      } else if (btn.dataset.act === 'relink') {
        relinkItem(mt, id, btn).catch(console.error);
      } else if (btn.dataset.act === 'accept-update') {
        acceptUpdate(mt, id, btn).catch(console.error);
      } else if (btn.dataset.act === 'decline-update') {
        declineUpdate(mt, id, btn).catch(console.error);
      } else if (btn.dataset.act === 'delete-orphan') {
        deleteOrphan(mt, id, btn.dataset.title || '').catch(console.error);
      } else if (btn.dataset.act === 'open-override') {
        openOverrideDialog({
          mediaType: mt,
          tmdbId: id,
          kindHuman: btn.dataset.kindHuman || btn.dataset.kind || 'failure',
          message: btn.dataset.msg || '',
        });
      } else if (btn.dataset.page) {
        browseState.page = Number(btn.dataset.page);
        loadItems().catch(console.error);
      }
    });

    $('#pager').addEventListener('click', (e) => {
      const b = e.target.closest('button[data-page]');
      if (!b || b.disabled) return;
      browseState.page = Number(b.dataset.page);
      loadItems().catch(console.error);
    });

    loadItems().catch(console.error);
  }

  // ---- Manual YouTube URL override modal ----

  const YOUTUBE_URL_RE = /^https?:\/\/(?:www\.|m\.)?(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/shorts\/)[A-Za-z0-9_-]{6,}/i;

  function openOverrideDialog({ mediaType, tmdbId, kindHuman, message }) {
    const dlg = document.getElementById('override-dlg');
    if (!dlg) return;
    document.getElementById('override-mt').value = mediaType;
    document.getElementById('override-id').value = tmdbId;
    const meta = document.getElementById('override-dlg-meta');
    const msgFrag = message ? ` — ${htmlEscape(message)}` : '';
    meta.innerHTML = `<p class="muted">// ${htmlEscape((kindHuman || 'failure').toUpperCase())}${msgFrag}</p>`;
    document.getElementById('override-url').value = '';
    document.getElementById('override-status').textContent = '';
    if (typeof dlg.showModal === 'function') dlg.showModal(); else dlg.setAttribute('open', '');
  }

  function closeOverrideDialog() {
    const dlg = document.getElementById('override-dlg');
    if (!dlg) return;
    if (typeof dlg.close === 'function') dlg.close(); else dlg.removeAttribute('open');
  }

  function bindOverrideDialog() {
    const dlg = document.getElementById('override-dlg');
    if (!dlg) return;
    document.getElementById('override-dlg-close')?.addEventListener('click', closeOverrideDialog);
    document.getElementById('override-cancel')?.addEventListener('click', closeOverrideDialog);
    const form = document.getElementById('override-form');
    form?.addEventListener('submit', async (e) => {
      e.preventDefault();
      const status = document.getElementById('override-status');
      const url = document.getElementById('override-url').value.trim();
      const mt = document.getElementById('override-mt').value;
      const id = document.getElementById('override-id').value;
      if (!YOUTUBE_URL_RE.test(url)) {
        status.textContent = '✗ enter a valid YouTube URL';
        status.classList.remove('ok'); status.classList.add('err');
        return;
      }
      status.textContent = 'saving…';
      status.classList.remove('err', 'ok');
      try {
        await api('POST', `/api/items/${mt}/${id}/override`, { youtube_url: url });
        status.textContent = '✓ override saved · download queued';
        status.classList.add('ok');
        setTimeout(() => {
          closeOverrideDialog();
          loadItems().catch(()=>{});
        }, 700);
      } catch (e) {
        status.textContent = '✗ ' + e.message;
        status.classList.add('err');
      }
    });
  }

  async function unplaceTheme(mediaType, tmdbId, title, sectionId) {
    // Removes the theme.mp3 from Plex's folder but keeps motif's canonical
    // so REPLACE can push it back later. No re-download needed if user
    // changes their mind.
    // v1.12.77: scope to section_id so DEL on a 4K row only
    // unlinks the 4K folder's theme.mp3 — sibling editions keep
    // playing motif's theme.
    const labelTitle = title ? `"${title}"` : `${mediaType} ${tmdbId}`;
    const scopeNote = sectionId
      ? '\n\nScoped to this section only — sibling editions keep their themes in place.'
      : '';
    const ok = confirm(
      `Remove ${labelTitle} from the Plex folder?\n\n` +
      `Plex will stop playing this theme until you push it back.\n\n` +
      `Motif's canonical copy stays in /data/media/themes — click PUSH TO PLEX ` +
      `on the row to restore it without re-downloading.${scopeNote}`);
    if (!ok) return;
    try {
      const url = sectionId
        ? `/api/items/${mediaType}/${tmdbId}/unplace?section_id=${encodeURIComponent(sectionId)}`
        : `/api/items/${mediaType}/${tmdbId}/unplace`;
      await api('POST', url);
    } catch (e) {
      alert('Unplace failed: ' + e.message);
    }
  }

  async function replaceTheme(mediaType, tmdbId, btn) {
    // Push motif's existing canonical back into the Plex folder. Force
    // overwrite so any sidecar that reappeared since unplace is replaced.
    if (btn) btn.disabled = true;
    try {
      await api('POST', `/api/items/${mediaType}/${tmdbId}/replace`);
      if (btn) btn.textContent = 'QUEUED';
      if (typeof libraryRapidPoll === 'function'
          && document.getElementById('library-body')) {
        libraryRapidPoll();
      }
      // v1.11.86: kick the topbar so the /pending banner + nav dot
      // re-evaluate against the new pending_placements count
      // (which now excludes items with a queued place job).
      // Schedule past /api/stats's 1s TTL.
      setTimeout(refreshTopbarStatus, 1100);
    } catch (e) {
      alert('Replace failed: ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function purgeTheme(mediaType, tmdbId, title, isOrphan, dlOnly, sectionId, plexAlso) {
    const labelTitle = title ? `"${title}"` : `${mediaType} ${tmdbId}`;
    // v1.10.38: PURGE is full destruction — delete both motif's
    // canonical at /data/media/themes AND the placement in the Plex
    // folder. If you want to keep the Plex-folder file but stop
    // managing the theme, use UNMANAGE instead.
    // v1.11.8: when there's no current placement (DL-only state — row
    // was placed and the user already DEL'd it), the warning shifts to
    // emphasize that PUSH TO PLEX won't be available afterward — the
    // user has to re-acquire the audio via DOWNLOAD / SET URL / UPLOAD
    // / ADOPT before any future placement.
    let warning;
    if (dlOnly && !isOrphan) {
      warning = '\n\nDownloaded but not placed — motif will delete the canonical at /themes.'
        + '\n\nAfter PURGE, PUSH TO PLEX is unavailable. To re-place, re-acquire via SOURCE:'
        + '\n  • DOWNLOAD TDB — re-fetch from ThemerrDB'
        + '\n  • SET URL — manual YouTube URL'
        + '\n  • UPLOAD MP3 — upload a local file'
        + '\n  • drop a sidecar at the Plex folder and use ADOPT';
    } else {
      warning = '\n\nMotif will delete the canonical at /themes AND the theme.mp3 in the Plex folder.';
      if (isOrphan) {
        warning += '\n\nUser-provided / adopted — the themes row is removed; the file is gone.'
          + ' To restore: SET URL, UPLOAD MP3, or drop a sidecar and ADOPT.';
      } else {
        warning += '\n\nIf ThemerrDB still tracks the title, the row flips to —;'
          + ' DOWNLOAD TDB becomes available in the SOURCE menu.';
      }
      warning += '\n\nUse UNMANAGE to keep the Plex-folder file.';
    }
    // v1.12.77: surface section-scope in the confirm copy so the
    // user knows the action only targets the row's edition. The
    // backend detects last-section and only drops the themes row +
    // tracking metadata when nothing else is managed for the title.
    const scopeNote = sectionId
      ? '\n\nScoped to this section only — sibling editions (e.g. 4K vs standard) keep their themes.'
      : '';
    // v1.13.31: post-PURGE preview.
    // v1.13.34: softened the no-fallback branch. When the row's
    // plexAlso flag is true, motif KNOWS Plex serves a separate
    // (cloud / embed) theme — confident claim is fine. When false,
    // motif can't actually tell whether Plex has a Pass cloud
    // theme available, because Plex hides cloud-availability when
    // a sidecar wins. The previous "Plex has no fallback" copy was
    // overconfident in those cases. Now the negative branch says
    // "no detected fallback" — accurate without claiming knowledge
    // motif doesn't have.
    const fallbackNote = plexAlso
      ? '\n\nAfter PURGE: Plex serves its own theme for this title (cloud / embed) — Plex\'s version becomes the active one. (To remove the Plex-side too, do it in Plex itself.)'
      : (dlOnly && !isOrphan)
        ? ''  // dlOnly already covers the consequence above.
        : '\n\nAfter PURGE: motif has no detected fallback for this title — it will be themeless until the next sync (assuming TDB has a URL). If Plex Pass has a cloud theme for this title, that may surface as the active one instead.';
    const ok = confirm(`Purge ${labelTitle}?${warning}${scopeNote}${fallbackNote}\n\nThis cannot be undone.`);
    if (!ok) return;
    try {
      const url = sectionId
        ? `/api/items/${mediaType}/${tmdbId}/forget?section_id=${encodeURIComponent(sectionId)}`
        : `/api/items/${mediaType}/${tmdbId}/forget`;
      const r = await fetch(url, { method: 'POST' });
      if (!r.ok && r.status !== 204) {
        const t = await r.text().catch(() => '');
        throw new Error(`${r.status}: ${t || r.statusText}`);
      }
    } catch (e) {
      alert('Purge failed: ' + e.message);
    }
  }

  async function deleteOrphan(mediaType, tmdbId, title) {
    const labelTitle = title ? `"${title}"` : `${mediaType} ${tmdbId}`;
    const ok = confirm(
      `Delete the orphan ${labelTitle}?\n\n` +
      `This will:\n` +
      `  · remove the theme.mp3 from every Plex folder it was placed in\n` +
      `  · delete the canonical file in /data/media/themes\n` +
      `  · delete the database row and all linked records\n\n` +
      `This cannot be undone. The next sync will not recreate this row.`,
    );
    if (!ok) return;
    try {
      const r = await fetch(`/api/items/${mediaType}/${tmdbId}`, { method: 'DELETE' });
      if (!r.ok && r.status !== 204) {
        const text = await r.text().catch(() => '');
        throw new Error(`${r.status}: ${text || r.statusText}`);
      }
    } catch (e) {
      alert('Delete failed: ' + e.message);
      return;
    }
    // Refresh the row list
    await loadItems().catch(()=>{});
  }

  async function revertToThemerrDb(mediaType, tmdbId, btn) {
    // v1.12.37: REVERT is now a one-step undo. The /revert endpoint
    // swaps the canonical URL back to themes.previous_youtube_url
    // (could be a user URL or a TDB URL depending on what was
    // captured). Skip the confirm dialog — REVERT is non-destructive
    // and round-trippable (clicking REVERT a second time returns
    // the row to its pre-first-revert state).
    // v1.12.47: scope to the row's section_id so REVERT only
    // re-themes the section the user clicked from (matches
    // ACCEPT UPDATE's per-section behavior).
    if (btn) btn.disabled = true;
    try {
      const sectionId = btn?.dataset?.sectionId;
      const url = sectionId
        ? `/api/items/${mediaType}/${tmdbId}/revert?section_id=${encodeURIComponent(sectionId)}`
        : `/api/items/${mediaType}/${tmdbId}/revert`;
      await api('POST', url);
      if (btn) btn.textContent = 'QUEUED';
      if (typeof libraryRapidPoll === 'function'
          && document.getElementById('library-body')) {
        libraryRapidPoll();
      }
    } catch (e) {
      alert('Revert failed: ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  // v1.12.37: REMOVE-menu CLEAR URL drops the user override and
  // re-downloads from TDB. Calls /clear-url which captures the
  // dropped URL into themes.previous_youtube_url so REVERT can
  // restore it later.
  // v1.12.64: while the request is in flight, lock every action
  // button on the row so the user can't accidentally click PURGE
  // / DEL between firing CLEAR URL and the row refresh that hides
  // the now-irrelevant CLEAR URL button. On error we re-enable the
  // locked buttons since there's no refresh coming; on success the
  // refresh re-renders them in their natural state.
  async function clearUrlOverride(mediaType, tmdbId, btn, sectionId) {
    if (btn) btn.disabled = true;
    const row = btn ? btn.closest('tr') : null;
    const lockedButtons = [];
    if (row) {
      row.querySelectorAll('details.row-menu button').forEach((b) => {
        if (!b.disabled) {
          b.disabled = true;
          lockedButtons.push(b);
        }
      });
    }
    try {
      // v1.12.86: optional section_id scopes the clear so only this
      // section's previous_urls row is dropped. Without it the
      // endpoint drops every section's snapshot for the title.
      const sec = sectionId
        ? `?section_id=${encodeURIComponent(sectionId)}`
        : '';
      await api('POST', `/api/items/${mediaType}/${tmdbId}/clear-url${sec}`);
      if (btn) btn.textContent = 'QUEUED';
      if (typeof libraryRapidPoll === 'function'
          && document.getElementById('library-body')) {
        libraryRapidPoll();
      }
    } catch (e) {
      alert('Clear URL failed: ' + e.message);
      lockedButtons.forEach((b) => { b.disabled = false; });
      if (btn) btn.disabled = false;
    }
  }

  async function redownload(mediaType, tmdbId, btn, sectionId) {
    if (btn) btn.disabled = true;
    try {
      // v1.12.73: pass section_id so the re-download targets only
      // the row's section. Pre-fix, RE-DOWNLOAD TDB / DOWNLOAD TDB
      // on a 4K row enqueued downloads for every section that
      // owned the title — wrong for per-edition theming since
      // sibling sections might have their own per-section
      // overrides (v1.12.72) the user wants to keep.
      const url = sectionId
        ? `/api/items/${mediaType}/${tmdbId}/redownload?section_id=${encodeURIComponent(sectionId)}`
        : `/api/items/${mediaType}/${tmdbId}/redownload`;
      await api('POST', url);
      if (btn) btn.textContent = 'QUEUED';
      // If we're on /movies, /tv, /anime, light up rapid-poll so the row
      // updates as the download/place transitions land.
      if (typeof libraryRapidPoll === 'function'
          && document.getElementById('library-body')) {
        libraryRapidPoll();
      }
    } catch (e) {
      alert('Re-download failed: ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function relinkItem(mediaType, tmdbId, btn) {
    if (btn) btn.disabled = true;
    try {
      await api('POST', `/api/items/${mediaType}/${tmdbId}/relink`);
      if (btn) btn.textContent = 'QUEUED';
    } catch (e) {
      alert('Relink failed: ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function acceptUpdate(mediaType, tmdbId, btn) {
    // v1.12.42: tailor the confirm prompt based on whether the
    // row's current source is a user URL.
    // v1.12.46: also scope the action to a single section if the
    // button carries a data-section-id. Pre-fix accept-update
    // fanned out across every section owning the title, so
    // accepting from a 4K library row also overwrote the
    // standard library's placement — wrong when the user wants
    // different themes per edition.
    let isUserSrcRow = false;
    if (btn) {
      const rowItem = (libraryState.items || []).find((it) =>
        it.theme_media_type === mediaType
        && String(it.theme_tmdb) === String(tmdbId)
      );
      if (rowItem) isUserSrcRow = computeSrcLetter(rowItem) === 'U';
    }
    const promptText = isUserSrcRow
      ? 'Accept the ThemerrDB update?\n\n'
        + 'Motif will download the new ThemerrDB URL and overwrite '
        + 'the current theme file in this section. Your manual URL '
        + 'is saved — if you change your mind, REVERT in the SOURCE '
        + 'menu will restore it. (If your URL exactly matched the '
        + 'new TDB URL, REVERT will be unavailable; the INFO card '
        + 'explains.)'
      : 'Accept the ThemerrDB update?\n\n'
        + 'Motif will download from the new YouTube URL and '
        + 'overwrite the current theme file in this section. The '
        + 'previous file is unrecoverable after this.';
    if (!confirm(promptText)) return;
    if (btn) btn.disabled = true;
    try {
      const sectionId = btn?.dataset?.sectionId;
      const url = sectionId
        ? `/api/updates/${mediaType}/${tmdbId}/accept?section_id=${encodeURIComponent(sectionId)}`
        : `/api/updates/${mediaType}/${tmdbId}/accept`;
      await api('POST', url);
      if (btn) btn.textContent = 'QUEUED';
      setTimeout(() => loadItems().catch(()=>{}), 600);
    } catch (e) {
      alert('Accept failed: ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function declineUpdate(mediaType, tmdbId, btn) {
    if (!confirm(
        'Keep your current theme and dismiss this update?\n\n' +
        'The topbar UPD count drops; the blue TDB ↑ pill stays so you can ' +
        'still spot the row. Your theme file is unchanged. You\'ll see another ' +
        'prompt only if ThemerrDB publishes a further URL change.')) return;
    if (btn) btn.disabled = true;
    try {
      // v1.12.99: pass sectionId so decline scopes to the row's
      // section. Pre-fix decline was title-global and silently
      // applied to sibling sections.
      const sectionId = btn?.dataset?.sectionId;
      const url = sectionId
        ? `/api/updates/${mediaType}/${tmdbId}/decline?section_id=${encodeURIComponent(sectionId)}`
        : `/api/updates/${mediaType}/${tmdbId}/decline`;
      await api('POST', url);
      if (btn) btn.textContent = 'KEPT';
      setTimeout(() => loadItems().catch(()=>{}), 600);
    } catch (e) {
      alert('Decline failed: ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function openItemDialog(mediaType, tmdbId) {
    const dlg = $('#item-dlg');
    if (!dlg) return;
    const body = $('#item-dlg-body');
    body.innerHTML = '<p class="muted">loading…</p>';
    dlg.showModal();
    let data;
    try {
      data = await api('GET', `/api/items/${mediaType}/${tmdbId}`);
    } catch (e) {
      body.innerHTML = `<p class="accent-red">error: ${htmlEscape(e.message)}</p>`;
      return;
    }
    const t = data.theme;
    const lf = data.local_file;
    const ovr = data.override;
    const placements = data.placements || [];
    body.innerHTML = `
      <button class="dlg-close" data-close>×</button>
      <h3>${htmlEscape(t.title)} <span class="muted" style="font-size:0.6em">(${htmlEscape(t.year || '?')})</span></h3>
      <div class="muted">${htmlEscape(t.original_title || '')}</div>
      <div class="dlg-section">
        <h4>identifiers</h4>
        <dl class="dlg-grid">
          <dt>tmdb</dt><dd>${htmlEscape(t.tmdb_id)}</dd>
          <dt>imdb</dt><dd>${t.imdb_id ? `<a href="https://www.imdb.com/title/${htmlEscape(t.imdb_id)}" target="_blank">${htmlEscape(t.imdb_id)}</a>` : '<span class="muted">—</span>'}</dd>
        </dl>
      </div>
      <div class="dlg-section">
        <h4>theme source</h4>
        <dl class="dlg-grid">
          <dt>youtube</dt><dd>${t.youtube_url ? `<a href="${htmlEscape(t.youtube_url)}" target="_blank">${htmlEscape(t.youtube_url)}</a>` : '<span class="muted">—</span>'}</dd>
          <dt>video id</dt><dd>${htmlEscape(t.youtube_video_id || '')}</dd>
          <dt>added</dt><dd>${htmlEscape(t.youtube_added_at || '—')}</dd>
          <dt>edited</dt><dd>${htmlEscape(t.youtube_edited_at || '—')}</dd>
        </dl>
      </div>
      <div class="dlg-section">
        <h4>local file</h4>
        ${lf ? `
        <dl class="dlg-grid">
          <dt>path</dt><dd>${htmlEscape(lf.file_path)}</dd>
          <dt>size</dt><dd>${fmt.num(lf.file_size)} B</dd>
          <dt>downloaded</dt><dd>${htmlEscape(lf.downloaded_at)}</dd>
          <dt>video</dt><dd>${htmlEscape(lf.source_video_id)}</dd>
        </dl>` : '<p class="muted">not downloaded yet</p>'}
      </div>
      <div class="dlg-section">
        <h4>placements (${placements.length})</h4>
        ${placements.length ? placements.map((p) => {
          const badgeClass = p.placement_kind === 'hardlink' ? 'link-badge-hardlink' :
                             p.placement_kind === 'copy' ? 'link-badge-copy' : 'link-badge-symlink';
          return `
          <div style="font-size:var(--t-tiny);margin-bottom:6px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <span class="ok">▸</span>
            <span class="link-badge ${badgeClass}">${htmlEscape(p.placement_kind)}</span>
            <span style="word-break:break-all">${htmlEscape(p.media_folder)}</span>
            ${p.plex_refreshed ? '<span class="ok">·refreshed</span>' : ''}
            ${p.placement_kind === 'copy' ? `<button class="btn btn-tiny" data-act="relink" data-mt="${mediaType}" data-id="${tmdbId}">RELINK</button>` : ''}
          </div>`;
        }).join('') : '<p class="muted">not placed</p>'}
      </div>
      <div class="dlg-section">
        <h4>override${ovr ? '<span class="accent" style="margin-left:8px">active</span>' : ''}</h4>
        ${ovr ? `
          <dl class="dlg-grid">
            <dt>url</dt><dd>${htmlEscape(ovr.youtube_url)}</dd>
            <dt>set by</dt><dd>${htmlEscape(ovr.set_by || '—')}</dd>
            <dt>set at</dt><dd>${htmlEscape(ovr.set_at)}</dd>
          </dl>
          <button class="btn btn-tiny btn-danger" data-clear-override
                  data-mt="${mediaType}" data-id="${tmdbId}" style="margin-top:8px">CLEAR OVERRIDE</button>
        ` : `
          <form class="dlg-form" data-set-override data-mt="${mediaType}" data-id="${tmdbId}">
            <input class="input" name="youtube_url" placeholder="https://www.youtube.com/watch?v=…" required />
            <button class="btn btn-tiny" type="submit">SET</button>
          </form>
        `}
      </div>
      <div class="dlg-actions">
        <button class="btn btn-warn" data-act="redl" data-mt="${mediaType}" data-id="${tmdbId}">RE-DOWNLOAD</button>
        <button class="btn" data-close>CLOSE</button>
      </div>
    `;
  }

  function bindDialog() {
    document.addEventListener('click', async (e) => {
      const dlg = $('#item-dlg');
      if (!dlg) return;
      if (e.target.matches('[data-close]')) {
        dlg.close();
      }
      const co = e.target.closest('[data-clear-override]');
      if (co) {
        if (!confirm('Clear override?')) return;
        await api('DELETE', `/api/items/${co.dataset.mt}/${co.dataset.id}/override`);
        openItemDialog(co.dataset.mt, co.dataset.id).catch(console.error);
      }
    });
    document.addEventListener('submit', async (e) => {
      const f = e.target.closest('form[data-set-override]');
      if (!f) return;
      e.preventDefault();
      const fd = new FormData(f);
      try {
        await api('POST', `/api/items/${f.dataset.mt}/${f.dataset.id}/override`, fd);
        openItemDialog(f.dataset.mt, f.dataset.id).catch(console.error);
      } catch (err) {
        alert(err.message);
      }
    });
  }

  // ---- Coverage ----

  function fmtBytes(n) {
    if (!n) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let i = 0;
    let v = n;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`;
  }

  async function loadCoverage() {
    // Guards on the storage stat card — present on both /coverage AND
    // /dashboard since the coverage cards moved up to the dash hero in v1.8.
    if (!$('#storage-hardlinks')) return;

    // Storage waste section (independent of Plex)
    try {
      const stats = await api('GET', '/api/stats');
      $('#storage-hardlinks').textContent = fmt.num(stats.storage.hardlinks);
      $('#storage-copies').textContent = fmt.num(stats.storage.copies);
      $('#storage-wasted').textContent = fmtBytes(stats.storage.copies_bytes);

      // Orphans count (themes adopted from Plex with no upstream match)
      const orphanEl = $('#orphan-count');
      if (orphanEl) {
        orphanEl.textContent = fmt.num(stats.storage.orphans || 0);
      }

      const copiesBlock = $('#copies-block');
      if (copiesBlock) {
        if (stats.storage.copies > 0) {
          copiesBlock.style.display = '';
          const copies = await api('GET', '/api/storage/copies');
          $('#copies-body').innerHTML = copies.items.map((c) => `
            <tr>
              <td>${htmlEscape(c.title)}</td>
              <td class="col-year">${htmlEscape(c.year || '')}</td>
              <td style="font-family:var(--font-mono);font-size:var(--t-tiny);color:var(--fg-dim)">${htmlEscape(c.media_folder)}</td>
              <td class="col-year">${fmtBytes(c.file_size)}</td>
              <td class="col-actions">
                <button class="btn btn-tiny" data-act="relink" data-mt="${c.media_type}" data-id="${c.tmdb_id}">RELINK</button>
              </td>
            </tr>
          `).join('') || '<tr><td colspan="5" class="muted center">no copies</td></tr>';
        } else {
          copiesBlock.style.display = 'none';
        }
      }
    } catch (e) {
      console.error('storage stats failed', e);
    }

    // Plex coverage report — populates the stat cards on every page that
    // includes them; missing-themes tables only render when present.
    let data;
    try {
      data = await api('GET', '/api/coverage/plex');
    } catch (e) {
      const mb = $('#movies-missing-body');
      if (mb) mb.innerHTML = `<tr><td colspan="4" class="accent-red">${htmlEscape(e.message)}</td></tr>`;
      return;
    }
    if (!data.enabled) {
      const mb = $('#movies-missing-body');
      if (mb) mb.innerHTML = '<tr><td colspan="4" class="muted">Plex integration disabled</td></tr>';
      return;
    }
    if (data.error) {
      const mb = $('#movies-missing-body');
      if (mb) mb.innerHTML = `<tr><td colspan="4" class="accent-red">Plex error: ${htmlEscape(data.error)}</td></tr>`;
      return;
    }

    const renderMissing = (items, bodyId) => {
      const tbody = $(bodyId);
      if (!tbody) return;
      const missing = items.filter((it) => !it.has_theme && it.motif_available);
      tbody.innerHTML = missing.length ? missing.map((it) => `
        <tr>
          <td>${htmlEscape(it.title)}</td>
          <td class="col-year">${htmlEscape(it.year || '')}</td>
          <td><span class="ok">▸ available</span></td>
          <td class="col-actions">
            ${it.tmdb_id ? `<button class="btn btn-tiny btn-warn" data-act="redl" data-mt="movie" data-id="${it.tmdb_id}">DOWNLOAD</button>` : ''}
          </td>
        </tr>
      `).join('') : '<tr><td colspan="4" class="muted center">no missing themes — fully covered ✓</td></tr>';
    };

    renderMissing(data.movies || [], '#movies-missing-body');
    renderMissing((data.tv || []), '#tv-missing-body');

    const total = data.movies.length;
    const withTheme = data.movies.filter((m) => m.has_theme).length;
    const motifAvail = data.movies.filter((m) => m.motif_available).length;
    $('#plex-movies-total').textContent = fmt.num(total);
    $('#plex-movies-with-theme').textContent = fmt.num(withTheme);
    $('#plex-movies-motif').textContent = fmt.num(motifAvail);

    const tvTotal = data.tv.length;
    const tvWithTheme = data.tv.filter((m) => m.has_theme).length;
    const tvMotif = data.tv.filter((m) => m.motif_available).length;
    $('#plex-tv-total').textContent = fmt.num(tvTotal);
    $('#plex-tv-with-theme').textContent = fmt.num(tvWithTheme);
    $('#plex-tv-motif').textContent = fmt.num(tvMotif);

    // v1.13.27: per-section comparison bars live in
    // renderCoverageComparison and are populated from
    // /api/sections/coverage (called alongside renderSectionCoverage
    // in loadDashboard's section-coverage fetch). The earlier
    // aggregated movies/tv recomputation that lived here was dead
    // code by v1.13.27 and was removed in v1.13.28.
  }

  // v1.13.22: per-row stacked-bar comparison block.
  // v1.13.27: input is now an array of plex_sections (from
  // /api/sections/coverage) instead of an aggregated movies / tv
  // pair. Each section gets its own row with a 2-segment bar
  // (themed vs unthemed), normalized to that section's total — so
  // a 28-item 4K Movies library at 90% themed reads as a near-full
  // bar even though Movies (10K items) at 30% themed reads as a
  // mostly-empty bar of similar visual width. Pre-fix the user
  // reported the aggregate view masked small-library coverage.
  //
  // The 3-segment "themed / TDB-available / no-TDB" axis from
  // v1.13.22 is dropped — the section coverage payload doesn't
  // carry the TDB-availability split (that data lives in
  // /api/coverage/plex's per-item motif_available flag), and the
  // 2-segment view is what users actually compare across rows.
  let _lastCoverageComparisonKey = '';
  function renderCoverageComparison(sections) {
    const block = document.getElementById('coverage-comparison-block');
    const body = document.getElementById('coverage-comparison-body');
    if (!block || !body) return;
    const rows = (sections || []).filter((s) => (s.total || 0) > 0);
    if (rows.length === 0) { block.style.display = 'none'; return; }
    // v1.13.29: include tab, is_4k, is_anime in the cache key. Pre-fix
    // a section reclassification from /settings (toggling A/4K flags)
    // would change the rendered tab + STD/4K subtype label but not
    // the totals, so the hash matched and the swap was skipped — the
    // user kept seeing the stale classification on screen until
    // numbers happened to change.
    const key = JSON.stringify(rows.map((s) => [
      s.section_id, s.title, s.total, s.themed,
      s.tab, s.is_4k, s.is_anime,
    ]));
    if (key === _lastCoverageComparisonKey) {
      block.style.display = '';
      return;
    }
    _lastCoverageComparisonKey = key;
    block.style.display = '';
    body.innerHTML = rows.map((s) => {
      const total = s.total || 0;
      const themed = s.themed || 0;
      const unthemed = Math.max(0, total - themed);
      const pctThemed = total ? (themed / total) * 100 : 0;
      const pctUnthemed = total ? (unthemed / total) * 100 : 0;
      const pctDisplay = pctThemed >= 10
        ? Math.round(pctThemed)
        : pctThemed.toFixed(1);
      const fourkLabel = s.is_4k ? '4K' : 'STD';
      const typeLabel = s.tab === 'anime' ? 'ANIME'
                      : s.tab === 'tv'    ? 'TV'
                      :                     'MOVIES';
      const sectionTitle = s.title || `${typeLabel} ${fourkLabel}`;
      const href = `/${s.tab}?fourk=${s.is_4k ? 1 : 0}`;
      return `<a class="coverage-row" data-tab="${htmlEscape(s.tab || '')}"
                  href="${htmlEscape(href)}">
        <div class="coverage-row-head">
          <span class="coverage-row-tab">
            ${htmlEscape(sectionTitle)}
            <span class="muted small">· ${typeLabel} ${fourkLabel}</span>
          </span>
          <span class="coverage-row-ratio">
            ${fmt.num(themed)} <span class="muted">/ ${fmt.num(total)} themed</span>
            <span class="coverage-row-pct">${pctDisplay}%</span>
          </span>
        </div>
        <div class="coverage-bar"
             title="${fmt.num(themed)} themed · ${fmt.num(unthemed)} unthemed">
          <span class="coverage-bar-seg coverage-bar-seg-themed"
                style="flex-basis:${pctThemed}%; ${themed === 0 ? 'display:none' : ''}"></span>
          <span class="coverage-bar-seg coverage-bar-seg-no-tdb"
                style="flex-basis:${pctUnthemed}%; ${unthemed === 0 ? 'display:none' : ''}"></span>
        </div>
      </a>`;
    }).join('');
  }

  function bindCoverage() {
    const btn = $('#relink-all-btn');
    if (!btn) return;
    btn.addEventListener('click', async () => {
      if (!confirm('Re-link all copies as hardlinks?')) return;
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = '// QUEUED';
      try {
        await api('POST', '/api/storage/relink');
      } catch (e) {
        alert('Relink failed: ' + e.message);
        btn.disabled = false;
        btn.textContent = orig;
        return;
      }
      setTimeout(() => {
        btn.disabled = false;
        btn.textContent = orig;
        loadCoverage().catch(console.error);
      }, 2000);
    });
  }

  // ---- Queue / Logs ----

  let queueFilter = 'all';

  async function loadQueue() {
    if (!$('#jobs-body')) return;
    const path = queueFilter === 'all' ? '/api/jobs' : `/api/jobs?status=${queueFilter}`;
    const data = await api('GET', path);
    // v1.12.81: piggyback a topbar refresh on every queue poll. /queue
    // is where the user sits when watching for jobs/events, so the
    // expectation is "if anything changed, the badges reflect it".
    // Pre-fix the UPD badge could lag the actual pending_updates row
    // by up to 30s (the topbar's own poll cadence) since loadQueue
    // didn't kick it. Cheap — one extra request per 10s while the
    // tab is /queue, not running otherwise.
    refreshTopbarStatus().catch(() => {});
    $('#jobs-body').innerHTML = data.jobs.map((j) => {
      // v1.11.36: cancel button on pending / running rows.
      // cancel_requested=1 + status='running' shows 'CANCELLING…' so
      // the user knows their click registered while the worker
      // walks to its next safe yield point.
      const cancellable = (j.status === 'pending' || j.status === 'running');
      const cancelling = j.cancel_requested && j.status === 'running';
      // v1.11.53: when a row is stuck in 'cancelling…' (worker thread
      // wedged on DB-lock contention or a long Plex call) the user
      // needs an escape hatch. × FORCE hits /api/jobs/{id}/cancel?force=true
      // which marks the row cancelled regardless of cooperative state.
      const actionCell = cancelling
        ? `<button class="btn btn-tiny btn-danger" data-act="cancel-job" data-job-id="${htmlEscape(j.id)}" data-force="1" title="worker isn't responding to the cooperative cancel; mark cancelled directly">× FORCE</button>`
        : (cancellable
            ? `<button class="btn btn-tiny btn-danger" data-act="cancel-job" data-job-id="${htmlEscape(j.id)}" title="cancel this job (running jobs bail at the next safe yield)">× CANCEL</button>`
            : '');
      // v1.12.12: failed + acked rows render in green ('ACKNOWLEDGED')
      // — they're historical records of failures the user has seen
      // and dismissed. Still status='failed' under the hood so the
      // FAILED filter chip surfaces them; the visual state just
      // de-emphasizes them next to live failures.
      const isAckedFail = j.status === 'failed' && j.acked_at;
      const stateLevel = isAckedFail ? 'INFO'
                       : j.status === 'failed' ? 'ERROR'
                       : j.status === 'running' ? 'WARNING'
                       : 'INFO';
      const stateLabel = isAckedFail ? 'failed (acknowledged)' : j.status;
      const stateTip = isAckedFail
        ? `acknowledged at ${fmt.time(j.acked_at)} — kept for history; original error: ${(j.last_error || '').slice(0, 200)}`
        : '';
      return `
      <tr>
        <td>${htmlEscape(j.id)}</td>
        <td>${htmlEscape(j.job_type)}</td>
        <td class="muted">${htmlEscape(j.media_type ?? '')} ${htmlEscape(j.tmdb_id ?? '')}</td>
        <td><span class="event-level event-level-${stateLevel}"${stateTip ? ` title="${htmlEscape(stateTip)}"` : ''}>${htmlEscape(stateLabel)}</span></td>
        <td class="muted">${htmlEscape(fmt.time(j.created_at))}</td>
        <td class="muted" title="${htmlEscape(j.last_error ?? '')}">${htmlEscape((j.last_error ?? '').slice(0, 60))}</td>
        <td>${actionCell}</td>
      </tr>
    `;
    }).join('') || '<tr><td colspan="7" class="muted center">no jobs in the queue — work appears here when you click SYNC THEMERRDB on the dashboard, REFRESH FROM PLEX on a library, or any per-row action that downloads / places / refreshes</td></tr>';

    // v1.12.76: removed the CLEAR FAILED visibility toggle. The
    // button itself is gone from the queue template — see
    // commit message for rationale. Per-row ACK FAILURE plus the
    // library bulk-ACK SELECTED cover the same workflow without
    // forcing the user to leave the library page.

    const evs = await api('GET', '/api/events?limit=200');
    $('#event-stream-full').innerHTML = evs.events.map((e) => `
      <li>
        <span class="event-time">${htmlEscape(fmt.timeShort(e.ts))}</span>
        <span class="event-level event-level-${htmlEscape(e.level)}">${htmlEscape(e.level)}</span>
        <span class="event-component">${htmlEscape(e.component)}</span>
        <span class="event-msg" title="${htmlEscape(e.detail || '')}">${htmlEscape(e.message)}</span>
      </li>
    `).join('');
  }

  function bindQueue() {
    if (!$('#jobs-body')) return;
    // v1.11.73: honor ?status=failed (etc) on initial /queue load so
    // the topbar 'red dot → click' shortcut lands on the right
    // filter without an extra click.
    const initialStatus = new URLSearchParams(window.location.search).get('status');
    if (initialStatus && ['pending','running','failed','cancelled','done'].includes(initialStatus)) {
      queueFilter = initialStatus;
      $$('.chip[data-jobfilter]').forEach((x) => x.classList.remove('chip-active'));
      const target = document.querySelector(
        `.chip[data-jobfilter="${initialStatus}"]`);
      if (target) target.classList.add('chip-active');
    }

    $$('.chip[data-jobfilter]').forEach((c) => {
      c.addEventListener('click', () => {
        $$('.chip[data-jobfilter]').forEach((x) => x.classList.remove('chip-active'));
        c.classList.add('chip-active');
        queueFilter = c.dataset.jobfilter;
        loadQueue().catch(console.error);
      });
    });

    // v1.12.76: CLEAR FAILED click handler removed alongside the
    // button. Per-row ACK FAILURE (api_clear_failure, v1.12.74)
    // now also cancels the matching failed download job, so the
    // bulk path through the LOGS page is no longer needed.
    // Library page bulk-ACK SELECTED handles "dismiss many at
    // once" via the same per-row endpoint, keeping the workflow
    // on the page where the user is already reviewing the rows.
    // The /api/jobs/clear-failed endpoint stays available for
    // scripts/external callers.

    // v1.11.36: cancel-job click handler. Posts to /api/jobs/{id}/cancel.
    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('button[data-act="cancel-job"]');
      if (!btn || !$('#jobs-body')) return;
      const id = btn.dataset.jobId;
      if (!id) return;
      const forced = btn.dataset.force === '1';
      const prompt = forced
        ? `Force-cancel job ${id}? The worker may be stuck on a stalled DB write or Plex call — this marks the job cancelled in the queue regardless of whether the worker thread ever responds.`
        : `Cancel job ${id}? Running jobs bail at the next safe yield (a few seconds).`;
      if (!confirm(prompt)) return;
      btn.disabled = true;
      btn.textContent = '…';
      try {
        const url = forced
          ? `/api/jobs/${encodeURIComponent(id)}/cancel?force=true`
          : `/api/jobs/${encodeURIComponent(id)}/cancel`;
        await api('POST', url);
        await loadQueue().catch(()=>{});
        // v1.11.78: poke the topbar past /api/stats's 1s TTL so the
        // 'SYNCING WITH …' label, the failed-job dot, and per-button
        // locks update the same frame as the cancel completes.
        // Pre-fix the user had to refresh the page to see the topbar
        // status reflect 'IDLE' after a force-cancel.
        setTimeout(refreshTopbarStatus, 1100);
      } catch (err) {
        alert('Cancel failed: ' + err.message);
        btn.disabled = false;
        btn.textContent = '× CANCEL';
      }
    });
  }

  // ---- Libraries ----

  async function loadLibraries() {
    if (!$('#libraries-movies-body')) return;
    let data;
    try {
      data = await api('GET', '/api/libraries');
    } catch (e) {
      $('#libraries-movies-body').innerHTML = `<tr><td colspan="7" class="accent-red">${htmlEscape(e.message)}</td></tr>`;
      return;
    }
    const movieRows = [];
    const tvRows = [];
    for (const s of data.sections) {
      const included = !!s.included;
      const stale = (() => {
        const last = new Date(s.last_seen_at);
        return (Date.now() - last.getTime()) > 1000 * 60 * 60 * 24 * 7;
      })();
      const locations = (s.location_paths || []).map(htmlEscape).join('<br>') || '<span class="muted">—</span>';
      const isAnime = !!s.is_anime;
      const is4k = !!s.is_4k;
      // v1.11.6: ROLE column is now two toggleable pills (A + 4K) instead
      // of a dropdown. Default = neither selected = 'standard'. A alone =
      // 'anime'. 4K alone = '4k'. Both = 'anime_4k'. Movie sections still
      // hide the A pill (motif's anime tabs draw from type='show' sections
      // in typical Plex layouts).
      const showAnime = s.type !== 'movie';
      const animePill = showAnime
        ? `<button type="button"
                   class="lib-flag-pill lib-flag-pill-anime${isAnime ? ' is-active' : ''}"
                   data-section-flag="anime"
                   data-section-id="${htmlEscape(s.section_id)}"
                   aria-pressed="${isAnime ? 'true' : 'false'}"
                   title="anime library — feeds the ANIME tab">A</button>`
        : '';
      const fourkPill = `<button type="button"
                                  class="lib-flag-pill lib-flag-pill-4k${is4k ? ' is-active' : ''}"
                                  data-section-flag="4k"
                                  data-section-id="${htmlEscape(s.section_id)}"
                                  aria-pressed="${is4k ? 'true' : 'false'}"
                                  title="4K library — feeds the 4K toggle on its tab">4K</button>`;
      // v1.11.13: explicit role label so 'standard' (= no pills active)
      // is visible. Pre-fix users sometimes thought a section was set
      // to standard but the saved is_4k flag was 1 from an earlier
      // save — the absence of an active pill meant nothing was clicked,
      // not that the section WAS standard. The label updates live as
      // pills toggle (handler below) so the displayed role matches what
      // SAVE will send.
      const roleLabel = (() => {
        if (isAnime && is4k) return 'anime 4K';
        if (isAnime) return 'anime';
        if (is4k) return '4K';
        return 'standard';
      })();
      const row = `
        <tr style="${stale ? 'opacity:0.45' : ''}" data-section-row="${htmlEscape(s.section_id)}">
          <td class="lib-col-id">${htmlEscape(s.section_id)}</td>
          <td class="lib-col-section"><strong>${htmlEscape(s.title)}</strong>${stale ? ' <span class="muted" style="font-size:var(--t-tiny)">(stale)</span>' : ''}</td>
          <td class="lib-col-type"><span class="muted">${htmlEscape(s.type)}</span></td>
          <td class="lib-col-mgd">
            <input type="checkbox" data-section-toggle="${htmlEscape(s.section_id)}" ${included ? 'checked' : ''} />
          </td>
          <td class="lib-col-role">
            <div class="lib-flag-group">${animePill}${fourkPill}<span class="lib-flag-label" data-section-role-label="${htmlEscape(s.section_id)}">${roleLabel}</span></div>
          </td>
          <td class="lib-locations" style="font-family:var(--font-mono);font-size:var(--t-tiny);color:var(--fg-dim)">${locations}</td>
          <td class="lib-col-actions">
            <button class="btn btn-tiny" data-section-refresh="${htmlEscape(s.section_id)}" title="re-enumerate just this section from Plex">REFRESH</button>
          </td>
        </tr>
      `;
      if (s.type === 'movie') movieRows.push(row); else tvRows.push(row);
    }
    $('#libraries-movies-body').innerHTML = movieRows.join('') ||
      '<tr><td colspan="7" class="muted center">no movie sections discovered</td></tr>';
    $('#libraries-tv-body').innerHTML = tvRows.join('') ||
      '<tr><td colspan="7" class="muted center">no TV sections discovered</td></tr>';
  }

  function bindLibraries() {
    const refresh = $('#refresh-libraries-btn');
    if (!refresh) return;
    refresh.addEventListener('click', async () => {
      const orig = refresh.dataset.origLabel || refresh.textContent;
      refresh.dataset.origLabel = orig;
      refresh.disabled = true;
      refresh.textContent = '// REFRESHING…';
      try {
        await api('POST', '/api/libraries/refresh');
        // v1.10.58: refresh now enqueues plex_enum jobs for every
        // included section. Stats poll's plex_enum_in_flight signal owns
        // the re-enable; reload the table opportunistically as the jobs
        // drain so the user sees updated counts.
        // v1.11.48: optimistic topbar paint to dodge the /api/stats
        // 1s TTL cache (see paintTopbarSyncing).
        paintTopbarSyncing('REFRESHING PLEX');
        setTimeout(() => loadLibraries().catch(()=>{}), 5000);
        setTimeout(() => loadLibraries().catch(()=>{}), 15000);
      } catch (e) {
        alert('Refresh failed: ' + e.message);
        refresh.disabled = false;
        refresh.textContent = orig;
      }
    });

    // Per-section refresh button — enumerate just this section from Plex.
    // The stats poll lock will keep the button disabled while any
    // plex_enum is in flight (across all sections) to prevent dupes.
    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('button[data-section-refresh]');
      if (!btn) return;
      const sid = btn.dataset.sectionRefresh;
      const orig = btn.dataset.origLabel || btn.textContent;
      btn.dataset.origLabel = orig;
      btn.disabled = true;
      btn.textContent = '…';
      try {
        await api('POST', `/api/libraries/${encodeURIComponent(sid)}/refresh`);
        // v1.11.48: optimistic paint (see paintTopbarSyncing) to
        // beat the /api/stats 1s TTL cache.
        paintTopbarSyncing('REFRESHING PLEX');
        setTimeout(() => loadLibraries().catch(()=>{}), 4000);
      } catch (err) {
        alert('Refresh failed: ' + err.message);
        btn.disabled = false;
        btn.textContent = orig;
        delete btn.dataset.origLabel;
      }
      // Don't re-enable here — stats poll's plex_enum_in_flight lock
      // keeps the row buttons disabled until the worker drains.
    });

    // Deferred save: capture every MGD / ANIME / 4K change into librariesDirty
    // (keyed by section_id). The // SAVE button commits everything in one
    // click — consistent with the rest of /settings, and the user can flip
    // several sections without each toggle firing a request.
    document.addEventListener('change', (e) => {
      const tog = e.target.closest('input[data-section-toggle]');
      if (!tog) return;
      const sid = tog.dataset.sectionToggle;
      if (!librariesDirty[sid]) librariesDirty[sid] = {};
      librariesDirty[sid].included = tog.checked;
      // v1.11.13: when MGD turns ON, also re-assert the current role so
      // SAVE always lands the section in the visible state — even if the
      // user didn't touch the pills. Pre-fix a user who enabled MGD on a
      // section that had been previously saved as 4K (gold pill active)
      // could click SAVE without touching the 4K pill and get a section
      // that was 'managed but with the wrong role' (4K instead of the
      // standard they expected if no pill was active in their head).
      if (tog.checked) {
        const row = tog.closest('tr');
        if (row) {
          const animePill = row.querySelector('button[data-section-flag="anime"]');
          const fourkPill = row.querySelector('button[data-section-flag="4k"]');
          const isAnime = animePill && animePill.getAttribute('aria-pressed') === 'true';
          const is4k = fourkPill && fourkPill.getAttribute('aria-pressed') === 'true';
          librariesDirty[sid].role = (isAnime && is4k) ? 'anime_4k'
                                   : isAnime           ? 'anime'
                                   : is4k              ? '4k'
                                   :                     'standard';
        }
      }
      updateLibrariesSaveButton();
    });
    // v1.11.6: A / 4K pill toggles. Each pill flips its own aria-pressed
    // state, then the change is captured into librariesDirty as a role
    // string ('standard' / 'anime' / '4k' / 'anime_4k') derived from the
    // current pressed state of both pills on the same row.
    document.addEventListener('click', (e) => {
      const pill = e.target.closest('button.lib-flag-pill');
      if (!pill) return;
      e.preventDefault();
      if (pill.disabled) return;
      const next = pill.getAttribute('aria-pressed') !== 'true';
      pill.setAttribute('aria-pressed', next ? 'true' : 'false');
      pill.classList.toggle('is-active', next);
      const sid = pill.dataset.sectionId;
      const row = pill.closest('tr');
      const animePill = row.querySelector('button[data-section-flag="anime"]');
      const fourkPill = row.querySelector('button[data-section-flag="4k"]');
      const isAnime = animePill && animePill.getAttribute('aria-pressed') === 'true';
      const is4k = fourkPill && fourkPill.getAttribute('aria-pressed') === 'true';
      const role = (isAnime && is4k) ? 'anime_4k'
                 : isAnime           ? 'anime'
                 : is4k              ? '4k'
                 :                     'standard';
      if (!librariesDirty[sid]) librariesDirty[sid] = {};
      librariesDirty[sid].role = role;
      // v1.11.13: keep the inline role label in sync as pills toggle.
      const label = row.querySelector('span[data-section-role-label]');
      if (label) {
        label.textContent = (isAnime && is4k) ? 'anime 4K'
                          : isAnime           ? 'anime'
                          : is4k              ? '4K'
                          :                     'standard';
      }
      updateLibrariesSaveButton();
    });

    // SAVE button: iterate dirty sections, fire per-row requests in
    // sequence (handful of rows, so parallelism not worth the
    // complexity), surface a summary status.
    document.getElementById('libraries-save-btn')?.addEventListener('click', async () => {
      const btn = document.getElementById('libraries-save-btn');
      const status = document.getElementById('libraries-save-status');
      const entries = Object.entries(librariesDirty);
      if (entries.length === 0) return;
      btn.disabled = true;
      status.textContent = `saving ${entries.length}…`;
      status.classList.remove('ok', 'err');
      let ok = 0;
      let failed = 0;
      const failures = [];
      for (const [sid, change] of entries) {
        try {
          if ('included' in change) {
            const fd = new FormData();
            fd.append('included', change.included ? 'true' : 'false');
            await api('POST', `/api/libraries/${encodeURIComponent(sid)}/include`, fd);
          }
          if ('role' in change) {
            await api('POST', `/api/libraries/${encodeURIComponent(sid)}/flags`,
                      { role: change.role });
          }
          ok += 1;
        } catch (err) {
          console.error('libraries save failed for', sid, err);
          failed += 1;
          // v1.11.4: keep the first failure's message visible in the
          // status line so the user actually sees WHY it failed (e.g.
          // a 409 from the role guard) instead of a vague "see console".
          failures.push(err.message || String(err));
        }
      }
      librariesDirty = {};
      if (failed === 0) {
        status.textContent = `✓ saved ${ok} section${ok === 1 ? '' : 's'}`;
      } else {
        const detail = failures[0] || '';
        status.textContent = `✗ ${failed} of ${ok + failed} failed: ${detail}`;
      }
      status.classList.add(failed === 0 ? 'ok' : 'err');
      updateLibrariesSaveButton();
      // v1.11.32: kick the topbar so adaptive nav (MOVIES / TV / ANIME
      // visibility) and the standard/4K toggle reflect the new flag
      // state immediately. Pre-fix the user had to wait up to 15s for
      // the next stats poll to surface a freshly-enabled tab.
      refreshTopbarStatus();
      // Re-fetch authoritative state, in case anything diverged
      setTimeout(() => loadLibraries().catch(()=>{}), 600);
      // v1.11.4: keep the message up longer when there's a failure so
      // the user has time to read it (4s was too brief for a multi-
      // line 409 detail).
      const clearAfter = failed === 0 ? 4000 : 12000;
      setTimeout(() => { status.textContent = ''; status.classList.remove('ok', 'err'); }, clearAfter);
    });
  }

  // Per-section pending changes captured by the libraries SAVE button.
  // Shape: {section_id: {included?: bool, role?: 'standard'|'4k'|'anime'|'anime_4k'}}.
  let librariesDirty = {};

  function updateLibrariesSaveButton() {
    const btn = document.getElementById('libraries-save-btn');
    if (!btn) return;
    const n = Object.keys(librariesDirty).length;
    btn.disabled = n === 0;
    btn.textContent = n === 0 ? '// SAVE' : `// SAVE (${n})`;
  }


  async function loadTokens() {
    if (!$('#tokens-body')) return;
    const data = await api('GET', '/api/tokens');
    if (!data.tokens.length) {
      $('#tokens-body').innerHTML = '<tr><td colspan="6" class="muted center">no tokens yet</td></tr>';
      return;
    }
    $('#tokens-body').innerHTML = data.tokens.map((t) => {
      const revoked = !!t.revoked_at;
      return `
        <tr style="${revoked ? 'opacity:0.4' : ''}">
          <td>${htmlEscape(t.name)}${revoked ? ' <span class="muted">(revoked)</span>' : ''}</td>
          <td><span class="link-badge ${t.scope === 'admin' ? 'link-badge-copy' : 'link-badge-hardlink'}">${htmlEscape(t.scope)}</span></td>
          <td class="muted" style="font-family:var(--font-mono);font-size:var(--t-tiny)">${htmlEscape(t.token_prefix)}…</td>
          <td class="muted">${htmlEscape(fmt.time(t.created_at))}</td>
          <td class="muted">${t.last_used_at ? htmlEscape(fmt.time(t.last_used_at)) : '<span class="muted">never</span>'}</td>
          <td class="col-actions">
            ${revoked ? '' : `<button class="btn btn-tiny btn-danger" data-revoke-token="${t.id}" title="Revoke this token (clients using it will start failing)">REVOKE</button>`}
          </td>
        </tr>
      `;
    }).join('');
  }

  function bindSettings() {
    const newBtn = $('#new-token-btn');
    if (!newBtn) return;

    const dlg = $('#new-token-dlg');
    const form = $('#new-token-form');
    const result = $('#new-token-result');
    const valEl = $('#new-token-value');

    newBtn.addEventListener('click', () => {
      form.style.display = '';
      result.style.display = 'none';
      form.reset();
      dlg.showModal();
    });

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const fd = new FormData(form);
      try {
        const r = await api('POST', '/api/tokens', fd);
        valEl.textContent = r.token;
        form.style.display = 'none';
        result.style.display = '';
        loadTokens().catch(console.error);
      } catch (err) {
        alert('Token creation failed: ' + err.message);
      }
    });

    document.addEventListener('click', async (e) => {
      const r = e.target.closest('[data-revoke-token]');
      if (!r) return;
      if (!confirm('Revoke this token? Anything using it will break immediately.')) return;
      try {
        await api('DELETE', `/api/tokens/${r.dataset.revokeToken}`);
        loadTokens().catch(console.error);
      } catch (err) {
        alert('Revoke failed: ' + err.message);
      }
    });

    const pwForm = $('#password-form');
    if (pwForm) {
      pwForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const fd = new FormData(pwForm);
        const status = $('#password-status');
        status.textContent = '';
        status.classList.remove('accent-red', 'ok');
        try {
          await api('POST', '/api/admin/password', fd);
          status.textContent = '✓ password updated';
          status.classList.add('ok');
          pwForm.reset();
        } catch (err) {
          status.textContent = '✗ ' + err.message;
          status.classList.add('accent-red');
        }
      });
    }

    // Dry-run runtime mode controls
    const onBtn = $('#dry-run-on-btn');
    const offBtn = $('#dry-run-off-btn');
    const cur = $('#dry-run-current');
    async function refreshDryRunState() {
      if (!cur) return;
      try {
        const r = await api('GET', '/api/dry-run');
        cur.textContent = r.dry_run ? 'DRY-RUN  (no real action)' : 'LIVE  (real downloads + placements)';
        cur.style.color = r.dry_run ? 'var(--amber-bright)' : 'var(--green-bright)';
        if (onBtn) onBtn.disabled = !!r.dry_run;
        if (offBtn) offBtn.disabled = !r.dry_run;
      } catch (e) { /* ignore */ }
    }
    async function setDryRun(value) {
      const fd = new FormData();
      fd.append('enabled', value ? 'true' : 'false');
      try {
        await api('POST', '/api/dry-run', fd);
        refreshDryRunState();
        refreshTopbarStatus();
      } catch (e) {
        alert('Failed: ' + e.message);
      }
    }
    if (onBtn) onBtn.addEventListener('click', () => {
      if (confirm('Enable dry-run? Pending downloads and placements will be simulated, not executed.')) setDryRun(true);
    });
    if (offBtn) offBtn.addEventListener('click', () => {
      if (confirm('Disable dry-run? Real downloads and placements will resume immediately.')) setDryRun(false);
    });
    if (cur) refreshDryRunState();
  }

  // ---- Scans page ----

  let scansState = {
    runId: null,
    filter: '',
    q: '',
    page: 0,
    pageSize: 50,
    findings: [],
    selectedIds: new Set(),
  };

  async function loadScansList() {
    const tbody = $('#scan-runs-body');
    if (!tbody) return;
    try {
      const data = await api('GET', '/api/scans');
      const runs = data.runs || [];
      if (!runs.length) {
        tbody.innerHTML = '<tr><td colspan="8" class="muted center">no scans yet — click SCAN PLEX FOLDERS to start</td></tr>';
        return;
      }
      tbody.innerHTML = runs.map((r) => {
        const status = htmlEscape(r.status);
        const dur = r.finished_at && r.started_at
          ? Math.round((new Date(r.finished_at) - new Date(r.started_at)) / 1000) + 's'
          : (r.status === 'running' ? '…' : '–');
        return `<tr data-run="${r.id}">
          <td>${r.id}</td>
          <td>${htmlEscape(r.started_at)}</td>
          <td><span class="status-${status}">${status.toUpperCase()}</span> ${dur}</td>
          <td>${r.sections_scanned}</td>
          <td>${r.folders_walked}</td>
          <td>${r.themes_found}</td>
          <td>${r.findings_count}</td>
          <td><button class="btn btn-tiny" data-view-scan="${r.id}">VIEW</button></td>
        </tr>`;
      }).join('');
      tbody.querySelectorAll('[data-view-scan]').forEach((btn) => {
        btn.addEventListener('click', () => loadScanDetail(parseInt(btn.dataset.viewScan, 10)));
      });

      // Auto-poll if any run is in progress. Also refresh the detail
      // pane (scansState.runId) so the actions disable/enable as the
      // run flips out of 'running'.
      if (data.running) {
        setTimeout(() => {
          loadScansList().catch(console.error);
          if (scansState.runId) {
            loadScanDetail(scansState.runId).catch(()=>{});
          }
        }, 3000);
      }
    } catch (e) {
      console.error('scans list failed', e);
    }
  }

  async function triggerScan() {
    const btn = $('#scan-trigger-btn');
    if (!btn) return;
    btn.disabled = true;
    btn.textContent = '// SCANNING...';
    try {
      await api('POST', '/api/scans');
      await loadScansList();
    } catch (e) {
      alert('Scan failed to start: ' + e.message);
    } finally {
      btn.disabled = false;
      btn.textContent = '// SCAN PLEX FOLDERS';
    }
  }

  async function loadScanDetail(runId) {
    scansState.runId = runId;
    scansState.page = 0;
    scansState.filter = '';
    scansState.selectedIds.clear();

    const block = $('#scan-detail-block');
    if (block) block.style.display = '';
    $('#scan-detail-id').textContent = runId;

    try {
      const data = await api('GET', `/api/scans/${runId}`);
      const run = data.run || {};
      // Track run status so renderFindings can disable the per-row decision
      // dropdowns while a scan is still mid-flight (clicking does nothing
      // server-side and confuses users).
      scansState.runStatus = run.status || '';
      const liveTag = scansState.runStatus === 'running'
        ? ' · <span class="accent-cyan">RUNNING — actions disabled until complete</span>'
        : '';
      $('#scan-detail-meta').innerHTML =
        `started ${htmlEscape(run.started_at)} · ${htmlEscape(String(run.findings_count || 0))} findings${liveTag}`;
      const sm = $('#scan-summary');
      const k = data.kind_counts || {};
      sm.innerHTML = `
        <div class="kpi-row" style="margin-top:10px">
          <div class="kpi"><div class="kpi-num">${k.exact_match || 0}</div><div class="kpi-lbl">EXACT</div></div>
          <div class="kpi"><div class="kpi-num">${k.hash_match || 0}</div><div class="kpi-lbl">HASH MATCH</div></div>
          <div class="kpi"><div class="kpi-num">${k.content_mismatch || 0}</div><div class="kpi-lbl">MISMATCH</div></div>
          <div class="kpi"><div class="kpi-num">${k.orphan_resolvable || 0}</div><div class="kpi-lbl">ORPHANS (RESOLVED)</div></div>
          <div class="kpi"><div class="kpi-num">${k.orphan_unresolved || 0}</div><div class="kpi-lbl">ORPHANS (UNRESOLVED)</div></div>
        </div>`;
      await loadFindings();
    } catch (e) {
      console.error(e);
    }
  }

  async function loadFindings() {
    const runId = scansState.runId;
    if (!runId) return;
    const params = new URLSearchParams({
      offset: String(scansState.page * scansState.pageSize),
      limit: String(scansState.pageSize),
    });
    if (scansState.filter) params.set('kind', scansState.filter);
    if (scansState.q) params.set('q', scansState.q);
    try {
      const data = await api('GET', `/api/scans/${runId}/findings?${params}`);
      scansState.findings = data.findings || [];
      renderFindings();
    } catch (e) {
      console.error('findings load failed', e);
    }
  }

  function renderFindings() {
    const tbody = $('#scan-findings-body');
    if (!tbody) return;
    if (!scansState.findings.length) {
      tbody.innerHTML = '<tr><td colspan="7" class="muted center">no findings match this filter</td></tr>';
      return;
    }
    tbody.innerHTML = scansState.findings.map((f) => {
      const folder = htmlEscape(f.media_folder.split('/').pop());
      let resolved = '';
      if (f.resolved_metadata) {
        try {
          const md = JSON.parse(f.resolved_metadata);
          const ids = [
            md.tmdb_id ? `tmdb:${md.tmdb_id}` : null,
            md.imdb_id ? md.imdb_id : null,
            md.tvdb_id ? `tvdb:${md.tvdb_id}` : null,
          ].filter(Boolean).join(' / ');
          resolved = `<span class="muted small">${htmlEscape(md.source || '')}</span> ${htmlEscape(ids)}`;
        } catch {}
      }
      const checked = scansState.selectedIds.has(f.id) ? 'checked' : '';
      // Display "lock" for the keep_existing internal value (cosmetic relabel
      // that doesn't touch the DB enum).
      const decisionLabel = f.decision === 'pending' ? '–'
                           : f.decision === 'keep_existing' ? 'lock'
                           : htmlEscape(f.decision);
      const isAdopted = !!f.adopted_at;
      // Lock per-row interaction while the scan run is still active.
      // The worker writes findings rows as it goes, so users can land on
      // a partially-populated table — clicking decisions then would hit
      // a moving target and actions would silently no-op.
      const scanRunning = scansState.runStatus === 'running';
      const lockReason = scanRunning ? 'scan still running — wait for completion' : '';
      let actions;
      if (isAdopted) {
        actions = '<span class="muted small">DONE</span>';
      } else if (scanRunning) {
        actions = '<span class="muted small" title="' + htmlEscape(lockReason) + '">SCANNING…</span>';
      } else {
        actions = `<select class="input" data-decide="${f.id}">
             <option value="">–</option>
             <option value="adopt">adopt</option>
             <option value="replace">replace</option>
             <option value="keep_existing">lock</option>
           </select>`;
      }
      const cbDisabled = isAdopted || scanRunning;
      return `<tr data-finding="${f.id}">
        <td><input type="checkbox" data-select="${f.id}" ${checked} ${cbDisabled ? 'disabled' : ''} /></td>
        <td><span class="kind-${htmlEscape(f.finding_kind)}">${htmlEscape(f.finding_kind)}</span></td>
        <td title="${htmlEscape(f.media_folder)}">${folder}</td>
        <td>${resolved}</td>
        <td><code class="small">${htmlEscape(f.file_sha256.substring(0, 12))}…</code></td>
        <td>${decisionLabel}</td>
        <td>${actions}</td>
      </tr>`;
    }).join('');

    tbody.querySelectorAll('[data-select]').forEach((cb) => {
      cb.addEventListener('change', () => {
        const id = parseInt(cb.dataset.select, 10);
        if (cb.checked) scansState.selectedIds.add(id);
        else scansState.selectedIds.delete(id);
        updateBulkBar();
      });
    });

    tbody.querySelectorAll('[data-decide]').forEach((sel) => {
      sel.addEventListener('change', async () => {
        const id = parseInt(sel.dataset.decide, 10);
        const decision = sel.value;
        if (!decision) return;
        try {
          await api('POST', `/api/scans/findings/${id}/decision`, { decision });
          await loadFindings();
        } catch (e) {
          alert('Decision failed: ' + e.message);
        }
      });
    });

    updateBulkBar();
  }

  function updateBulkBar() {
    const bar = $('#scan-bulk-bar');
    if (!bar) return;
    const n = scansState.selectedIds.size;
    if (n === 0) {
      bar.style.display = 'none';
      return;
    }
    bar.style.display = '';
    $('#scan-bulk-count').textContent = `${n} selected`;
  }

  function bindScans() {
    if (!$('#scan-trigger-btn')) return;
    $('#scan-trigger-btn').addEventListener('click', () => triggerScan().catch(console.error));

    document.querySelectorAll('.scan-filter-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.scan-filter-btn').forEach((b) =>
          b.classList.remove('scan-filter-active'));
        btn.classList.add('scan-filter-active');
        scansState.filter = btn.dataset.filterKind || '';
        scansState.page = 0;
        scansState.selectedIds.clear();
        loadFindings().catch(console.error);
      });
    });

    // Findings search box (debounced) — searches folder path / file path /
    // resolved-metadata title via the new ?q= server param.
    const findingsSearch = $('#scan-findings-search');
    if (findingsSearch) {
      let dt;
      findingsSearch.addEventListener('input', () => {
        clearTimeout(dt);
        dt = setTimeout(() => {
          scansState.q = findingsSearch.value.trim();
          scansState.page = 0;
          scansState.selectedIds.clear();
          loadFindings().catch(console.error);
        }, 250);
      });
    }

    const selectAll = $('#findings-select-all');
    if (selectAll) {
      selectAll.addEventListener('change', () => {
        scansState.findings.forEach((f) => {
          if (!f.adopted_at) {
            if (selectAll.checked) scansState.selectedIds.add(f.id);
            else scansState.selectedIds.delete(f.id);
          }
        });
        renderFindings();
      });
    }

    $('#findings-prev-btn')?.addEventListener('click', () => {
      if (scansState.page > 0) { scansState.page -= 1; loadFindings().catch(console.error); }
    });
    $('#findings-next-btn')?.addEventListener('click', () => {
      scansState.page += 1; loadFindings().catch(console.error);
    });

    // Generic bulk adopt by finding kind. The two adopt buttons share
    // this helper — only the kind filter and confirmation copy differ.
    async function bulkAdoptKind(kind, label) {
      const ids = scansState.findings
        .filter((f) => f.finding_kind === kind && !f.adopted_at)
        .map((f) => f.id);
      if (!ids.length) {
        alert(`No ${kind} findings on this page to bulk-adopt.`);
        return;
      }
      if (!confirm(`Adopt ${ids.length} ${label}? Each will be hardlinked into your themes_dir.`)) {
        return;
      }
      try {
        const r = await api('POST', '/api/scans/findings/decisions/bulk',
                            { finding_ids: ids, decision: 'adopt' });
        alert(`Enqueued ${r.enqueued} adoption(s).`);
        await loadFindings();
      } catch (e) {
        alert('Bulk adopt failed: ' + e.message);
      }
    }

    $('#scan-bulk-adopt-btn')?.addEventListener('click',
      () => bulkAdoptKind('hash_match', 'hash-matched theme(s) (these match a ThemerrDB record by content)'));
    $('#scan-bulk-adopt-tmdb-btn')?.addEventListener('click',
      () => bulkAdoptKind('orphan_resolvable', 'TMDB-matched theme(s) (motif will manage these as manual M sources)'));

    document.querySelectorAll('[data-bulk]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const decision = btn.dataset.bulk;
        const ids = Array.from(scansState.selectedIds);
        if (!ids.length) return;
        const label = decision === 'keep_existing' ? 'lock' : decision;
        if (!confirm(`Apply "${label}" to ${ids.length} finding(s)?`)) return;
        try {
          const r = await api('POST', '/api/scans/findings/decisions/bulk',
                              { finding_ids: ids, decision });
          alert(`Enqueued ${r.enqueued} action(s).`);
          scansState.selectedIds.clear();
          await loadFindings();
        } catch (e) {
          alert('Bulk action failed: ' + e.message);
        }
      });
    });

    loadScansList().catch(console.error);
  }



  function bindSettingsTabs() {
    const tabs = document.querySelectorAll('#settings-tabs .tab');
    if (!tabs.length) return;

    function showTab(name) {
      tabs.forEach((t) => t.classList.toggle('tab-active', t.dataset.tab === name));
      document.querySelectorAll('.tab-panel').forEach((p) => {
        p.style.display = p.dataset.panel === name ? '' : 'none';
      });
      // Update URL hash for deep links
      const newHash = '#' + name;
      if (window.location.hash !== newHash) {
        history.replaceState(null, '', newHash);
      }
    }

    tabs.forEach((t) => {
      t.addEventListener('click', () => showTab(t.dataset.tab));
    });

    // Honor #hash on page load
    const initial = (window.location.hash || '').replace(/^#/, '');
    const valid = Array.from(tabs).map((t) => t.dataset.tab);
    if (valid.includes(initial)) showTab(initial);
  }

  // ---- Config form (paths, plex, downloads, matching, sync, runtime tabs) ----
  // The form fields are declarative — we read /api/config once, populate
  // every [data-cfg-field], then on save we collect the dirty fields per tab
  // and PATCH /api/config.

  let configCache = null;       // { config, env_overrides, ... } from last GET

  async function loadConfigIntoForms() {
    if (!document.querySelector('[data-cfg-field], [data-cfg-field-list]')) return;
    let data;
    try {
      data = await api('GET', '/api/config');
    } catch (e) {
      console.error('config load failed', e);
      return;
    }
    configCache = data;
    populateConfigForms(data);
  }

  function getDotted(obj, dotted) {
    return dotted.split('.').reduce((o, k) => (o == null ? o : o[k]), obj);
  }

  function populateConfigForms(data) {
    const cfg = data.config;
    const envOverrides = data.env_overrides || {};

    // Scalar fields
    document.querySelectorAll('[data-cfg-field]').forEach((el) => {
      const path = el.dataset.cfgField;
      const v = getDotted(cfg, path);
      if (el.type === 'checkbox') {
        el.checked = !!v;
      } else if (path === 'plex.token') {
        // Special: token is masked. Show empty, but mark "(set)" placeholder.
        el.value = '';
        el.placeholder = cfg.plex && cfg.plex.token_set ? '(set — leave empty to keep)' : 'paste token here';
      } else {
        el.value = v == null ? '' : v;
      }

      // Env override: disable + show badge
      if (envOverrides[path]) {
        el.disabled = true;
      } else {
        el.disabled = false;
      }
    });

    // List fields (CSV in UI, list in JSON)
    document.querySelectorAll('[data-cfg-field-list]').forEach((el) => {
      const path = el.dataset.cfgFieldList;
      const v = getDotted(cfg, path);
      el.value = Array.isArray(v) ? v.join(', ') : '';
      el.disabled = !!envOverrides[path];
    });

    // Env-override badges
    document.querySelectorAll('[data-env-badge]').forEach((b) => {
      b.style.display = envOverrides[b.dataset.envBadge] ? '' : 'none';
    });
  }

  function collectFieldsForTab(tab) {
    // tab is "paths", "plex", etc. — find every [data-cfg-field*="<tab>."]
    // and assemble a partial PATCH body.
    const out = { [tab]: {} };

    document.querySelectorAll(`[data-cfg-field^="${tab}."]`).forEach((el) => {
      if (el.disabled) return;  // env-overridden
      const path = el.dataset.cfgField;
      const key = path.split('.').slice(1).join('.');
      let v;
      if (el.type === 'checkbox') {
        v = !!el.checked;
      } else if (el.type === 'number') {
        const num = el.value === '' ? null : Number(el.value);
        v = Number.isFinite(num) ? num : null;
      } else if (path === 'plex.token') {
        // Empty string = preserve. Don't include the field at all if empty.
        if (el.value === '') return;
        v = el.value;
      } else {
        v = el.value;
      }
      // Set via dotted path within out[tab]
      const parts = key.split('.');
      let cur = out[tab];
      for (let i = 0; i < parts.length - 1; i++) {
        cur[parts[i]] = cur[parts[i]] || {};
        cur = cur[parts[i]];
      }
      cur[parts[parts.length - 1]] = v;
    });

    document.querySelectorAll(`[data-cfg-field-list^="${tab}."]`).forEach((el) => {
      if (el.disabled) return;
      const path = el.dataset.cfgFieldList;
      const key = path.split('.').slice(1).join('.');
      const list = el.value.split(',').map((s) => s.trim()).filter(Boolean);
      out[tab][key] = list;
    });

    return out;
  }

  // v1.13.8 (Phase D): cache observability gauge for the settings
  // page. Read-only — surfaces what's in <db_dir>/cache/ so the
  // user can see Phase A's tarball + Phase B's git mirror + etc.
  // accumulating without surprise. Hidden until /api/cache/size
  // returns exists=true with a non-empty artifact list.
  async function loadCacheGauge() {
    const block = document.getElementById('cache-gauge');
    const totalEl = document.getElementById('cache-gauge-total');
    const listEl = document.getElementById('cache-gauge-list');
    if (!block || !totalEl || !listEl) return;
    try {
      const data = await api('GET', '/api/cache/size');
      if (!data || !data.exists || !data.artifacts || data.artifacts.length === 0) {
        block.hidden = true;
        return;
      }
      totalEl.textContent = `${fmt.bytes(data.total_bytes || 0)} total`;
      listEl.innerHTML = data.artifacts.map((a) => `
        <li>
          <span>${htmlEscape(a.label || a.name)}${a.is_dir ? '/' : ''}</span>
          <span class="cache-gauge-bytes">${fmt.bytes(a.bytes || 0)}</span>
        </li>
      `).join('');
      block.hidden = false;
    } catch (_) {
      block.hidden = true;
    }
  }

  // v1.13.2 (#3): pre-flight transport probe wiring. Auto-fires
  // once on settings load so the user sees today's reachability
  // without clicking; click-to-rerun for manual re-test (handy
  // after editing the URL fields). Uses CURRENTLY-SAVED config —
  // user must save first if they want to probe a new value.
  function bindSyncProbe() {
    const btn = document.getElementById('sync-probe-btn');
    const status = document.getElementById('sync-probe-status');
    if (!btn || !status) return;
    async function runProbe() {
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = '// PROBING…';
      status.textContent = '';
      status.className = 'form-status';
      try {
        const res = await api('POST', '/api/sync/probe');
        if (res.ok) {
          status.textContent = `✓ ${res.transport.toUpperCase()} `
            + `reachable · ${res.latency_ms}ms · ${res.detail || ''}`;
          status.classList.add('form-status-ok');
        } else {
          status.textContent = `✗ ${res.transport.toUpperCase()} failed: `
            + (res.error || 'unknown error');
          status.classList.add('form-status-fail');
        }
      } catch (e) {
        status.textContent = `✗ probe request failed: ${e.message}`;
        status.classList.add('form-status-fail');
      } finally {
        btn.disabled = false;
        btn.textContent = orig;
      }
    }
    btn.addEventListener('click', () => { runProbe().catch(()=>{}); });
    // Auto-probe once on load. Small delay so /api/config has
    // landed and the dropdown shows the actual current value
    // before the result text appears.
    setTimeout(() => { runProbe().catch(()=>{}); }, 800);
  }

  function bindConfigSaves() {
    document.querySelectorAll('[data-save]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const tab = btn.dataset.save;
        const status = document.querySelector(`[data-save-status="${tab}"]`);
        if (status) {
          status.textContent = 'saving…';
          status.classList.remove('ok', 'err');
        }
        const body = collectFieldsForTab(tab);
        try {
          const res = await api('PATCH', '/api/config', body);
          configCache = res;
          populateConfigForms(res);
          if (status) {
            status.textContent = '✓ saved';
            status.classList.add('ok');
          }
          // Refresh topbar so paths banner updates if themes_dir was just set
          refreshTopbarStatus();
          setTimeout(() => { if (status) status.textContent = ''; }, 2500);
        } catch (e) {
          if (status) {
            status.textContent = '✗ ' + e.message;
            status.classList.add('err');
          }
        }
      });
    });

    // Clear-token button
    document.querySelectorAll('[data-cfg-clear]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const path = btn.dataset.cfgClear;
        if (path !== 'plex.token') return;
        if (!confirm('Clear Plex token? motif will lose Plex access until you set a new one.')) return;
        try {
          await api('PATCH', '/api/config', { plex: { token: null } });
          await loadConfigIntoForms();
        } catch (e) {
          alert('Failed: ' + e.message);
        }
      });
    });
  }

  // ---- Paths-not-configured banner (every page) ----

  function updatePathsBanner(stats) {
    const banner = document.getElementById('paths-banner');
    const navDot = document.getElementById('nav-attn-settings');
    if (!banner) return;
    const ready = stats && stats.config && stats.config.paths_ready;
    if (ready) {
      banner.style.display = 'none';
      if (navDot) navDot.style.display = 'none';
    } else {
      banner.style.display = '';
      if (navDot) navDot.style.display = '';
    }
  }


  // ---- Pending (staged-but-not-placed) ----

  const pendingState = { items: [], selected: new Set() };

  function pendingKey(it) { return `${it.media_type}:${it.tmdb_id}`; }

  async function loadPending() {
    const tbody = $('#pending-body');
    if (!tbody) return;
    let data;
    try {
      data = await api('GET', '/api/pending');
    } catch (e) {
      tbody.innerHTML = `<tr><td colspan="8" class="accent-red">${htmlEscape(e.message)}</td></tr>`;
      return;
    }
    pendingState.items = data.items || [];
    const cntEl = $('#pending-count');
    if (cntEl) cntEl.textContent = pendingState.items.length;
    // Drop selections for items no longer present
    const liveKeys = new Set(pendingState.items.map(pendingKey));
    for (const k of Array.from(pendingState.selected)) {
      if (!liveKeys.has(k)) pendingState.selected.delete(k);
    }
    if (pendingState.items.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8" class="muted center">no staged downloads — every download has either been placed into Plex or discarded. Items land here when motif fetches a theme but the place worker defers (typically because a sidecar exists at the Plex folder that approval would overwrite).</td></tr>';
      updatePendingBulkBar();
      return;
    }
    // v1.10.47: reason cell colours by reason_kind so the most
    // destructive cases (overwrite an existing file) read amber/red,
    // and benign cases (auto-place off, queued) stay muted.
    const REASON_CLASS = {
      overwrites_sidecar: 'pending-reason-warn',
      overwrites_plex_agent: 'pending-reason-warn',
      auto_place_off: 'pending-reason-info',
      queued: 'pending-reason-muted',
    };
    const rows = pendingState.items.map((it) => {
      const k = pendingKey(it);
      const checked = pendingState.selected.has(k) ? 'checked' : '';
      const sourceLabel = it.provenance === 'manual' ? 'MANUAL' :
                          (it.upstream_source === 'plex_orphan' ? 'ORPHAN' : 'THEMERRDB');
      const dlAt = it.downloaded_at ? fmt.time(it.downloaded_at) : '—';
      const reasonCls = REASON_CLASS[it.reason_kind] || 'pending-reason-muted';
      const reasonText = it.reason || '—';
      return `
        <tr>
          <td><input type="checkbox" data-pending-key="${htmlEscape(k)}" ${checked} /></td>
          <td><strong>${htmlEscape(it.title || '—')}</strong></td>
          <td class="col-year">${htmlEscape(it.year || '—')}</td>
          <td><span class="muted">${htmlEscape(it.media_type)}</span></td>
          <td><span class="muted small">${sourceLabel}</span></td>
          <td><span class="${reasonCls}">${htmlEscape(reasonText)}</span></td>
          <td><span class="muted small">${dlAt}</span></td>
          <td class="col-actions">
            <button class="btn btn-tiny" data-pending-approve="${htmlEscape(k)}" title="Place into Plex (overwrites any existing sidecar)">APPROVE</button>
            <button class="btn btn-tiny btn-warn" data-pending-discard="${htmlEscape(k)}" title="delete the staged download and drop motif's local_files row — Plex's existing theme (if any) is left alone">DISCARD</button>
          </td>
        </tr>
      `;
    });
    tbody.innerHTML = rows.join('');
    updatePendingBulkBar();
  }

  function updatePendingBulkBar() {
    const bar = $('#pending-bulk-bar');
    if (!bar) return;
    const n = pendingState.selected.size;
    if (n > 0) {
      bar.style.display = '';
      $('#pending-bulk-count').textContent = `${n} selected`;
    } else {
      bar.style.display = 'none';
    }
  }

  function pendingItemsForKeys(keys) {
    const set = new Set(keys);
    return pendingState.items
      .filter((it) => set.has(pendingKey(it)))
      .map((it) => ({ media_type: it.media_type, tmdb_id: it.tmdb_id }));
  }

  async function pendingApprove(keys) {
    // Confirm explicitly when any selected item will overwrite a sidecar.
    // /api/pending populates plex_local_theme; we check it here so the
    // user sees a clear warning instead of just the row badge.
    const targetItems = keys === 'all'
      ? pendingState.items.slice()
      : pendingState.items.filter((it) => keys.includes(pendingKey(it)));
    const overwrites = targetItems.filter((it) => it.plex_local_theme);
    if (overwrites.length > 0) {
      const titles = overwrites.slice(0, 8)
        .map((it) => `  • ${it.title || '?'}${it.year ? ' (' + it.year + ')' : ''}`)
        .join('\n');
      const more = overwrites.length > 8
        ? `\n  ... and ${overwrites.length - 8} more`
        : '';
      const ok = confirm(
        `${overwrites.length} item(s) will OVERWRITE an existing theme.mp3 ` +
        `in their Plex folders:\n\n${titles}${more}\n\n` +
        `Motif will unlink the existing file and replace it with the new download. ` +
        `Proceed?`);
      if (!ok) return null;
    }
    const body = keys === 'all' ? { all: true } : { items: pendingItemsForKeys(keys) };
    const res = await api('POST', '/api/pending/place', body);
    pendingState.selected.clear();
    await loadPending();
    // v1.11.69: kick the topbar so the PENDING ● nav dot clears
    // immediately after the action drains the queue. The topbar's
    // own 30s poll would otherwise leave a stale lit dot for
    // half a minute. Schedule past /api/stats's 1s TTL cache so
    // the refresh sees the new pending_placements count.
    setTimeout(refreshTopbarStatus, 1100);
    return res;
  }

  async function pendingDiscard(keys) {
    if (!confirm(`Discard ${keys.length} download(s)? The file(s) will be deleted.`)) return null;
    const res = await api('POST', '/api/pending/discard', { items: pendingItemsForKeys(keys) });
    pendingState.selected.clear();
    await loadPending();
    // v1.11.69: discard drains the queue too; same topbar poke as
    // pendingApprove so the PENDING ● clears immediately.
    setTimeout(refreshTopbarStatus, 1100);
    return res;
  }

  function bindPending() {
    const tbody = $('#pending-body');
    if (!tbody) return;
    $('#pending-refresh-btn')?.addEventListener('click', () => loadPending().catch(console.error));
    $('#pending-place-all-btn')?.addEventListener('click', async () => {
      if (!confirm('Approve placement for ALL staged downloads?')) return;
      try { await pendingApprove('all'); } catch (e) { alert(e.message); }
    });
    $('#pending-select-all')?.addEventListener('change', (e) => {
      const on = e.target.checked;
      pendingState.selected.clear();
      if (on) for (const it of pendingState.items) pendingState.selected.add(pendingKey(it));
      // Mirror to row checkboxes without re-rendering
      tbody.querySelectorAll('input[data-pending-key]').forEach((el) => { el.checked = on; });
      updatePendingBulkBar();
    });
    tbody.addEventListener('change', (e) => {
      const cb = e.target.closest('input[data-pending-key]');
      if (!cb) return;
      const k = cb.getAttribute('data-pending-key');
      if (cb.checked) pendingState.selected.add(k); else pendingState.selected.delete(k);
      updatePendingBulkBar();
    });
    tbody.addEventListener('click', async (e) => {
      const ap = e.target.closest('[data-pending-approve]');
      const ds = e.target.closest('[data-pending-discard]');
      if (ap) {
        const k = ap.getAttribute('data-pending-approve');
        try { await pendingApprove([k]); } catch (err) { alert(err.message); }
      } else if (ds) {
        const k = ds.getAttribute('data-pending-discard');
        try { await pendingDiscard([k]); } catch (err) { alert(err.message); }
      }
    });
    $('#pending-bulk-approve')?.addEventListener('click', async () => {
      const keys = Array.from(pendingState.selected);
      if (keys.length === 0) return;
      try { await pendingApprove(keys); } catch (e) { alert(e.message); }
    });
    $('#pending-bulk-discard')?.addEventListener('click', async () => {
      const keys = Array.from(pendingState.selected);
      if (keys.length === 0) return;
      try { await pendingDiscard(keys); } catch (e) { alert(e.message); }
    });
  }


  // ---- Library (unified Plex-items browse) ----

  const libraryState = {
    tab: null,
    fourk: false,
    page: 1,
    perPage: 50,
    q: "",
    status: "all",
    // v1.10.20: secondary 'TDB MATCH' filter ∈ {any, tracked, untracked}.
    // Stacks on top of `status` to slice further (e.g. MANUAL + TRACKED
    // shows the manual rows that have a TDB alternative for REPLACE).
    tdb: "any",
    // v1.11.66: SRC letter filter (T / U / A / M / P / -). Empty
    // v1.11.89: multi-select Set of SRC letters (T/U/A/M/P/-) the user
    // wants to keep visible. Empty Set = no filter (show all). Click a
    // legend button to toggle that letter; CLEAR empties the set. Pure
    // client-side: pagination/total reflect the underlying status+tdb
    // pass.
    srcFilter: new Set(),
    // v1.12.7 / v1.12.23: TDB pill states. Possible values:
    // 'tdb', 'update', 'cookies', 'dead', 'none'. v1.12.23 moved
    // the actual filtering server-side so counts + pagination +
    // sort all honor the set; the client just tracks the
    // selection and ships it as ?tdb_pills=... on each fetch.
    tdbPills: new Set(),
    // v1.12.23: DL / PL / Link pill multi-select sets. Same
    // server-side pattern as srcFilter / tdbPills.
    //   dlPills: 'on' (green dot), 'off' (faded), 'broken' (red).
    //            v1.12.81 retired 'mismatch' — content divergence is
    //            a LINK fact ('m') only.
    //   plPills: 'on' (green dot), 'off' (faded), 'await' (amber),
    //            'broken' (red, v1.12.81 — placement row exists but
    //            theme.mp3 missing from the Plex folder).
    //   linkPills: 'hl', 'c', 'm' (mismatch), 'none'
    dlPills: new Set(),
    plPills: new Set(),
    linkPills: new Set(),
    // v1.12.41: EDITION axis. 'has' = rows with a Plex
    // {edition-...} folder tag, 'none' = rows without.
    edPills: new Set(),
    // v1.10.15: column sort state. sort key whitelisted server-side.
    sort: "title",
    sortDir: "asc",
    // Set of "media_type:tmdb_id" keys checked via the per-row checkbox.
    // Survives pagination (we restore checkboxes on render).
    selected: new Set(),
  };

  function libKey(it) {
    return `${it.theme_media_type || it.plex_media_type}:${it.theme_tmdb || it.rating_key}`;
  }

  // v1.10.20: hide the secondary TDB-match chips when the primary chip
  // already implies a TDB answer. THEMERRDB ⇒ tracked, UNTRACKED ⇒
  // untracked, THEMERRDB ONLY ⇒ tracked (and uses a different code
  // path on the backend that doesn't apply tdb filtering anyway).
  // Resets the filter to 'any' on hide so it doesn't silently leak
  // into the URL.
  function updateTdbFilterVisibility() {
    // v1.12.7: was the visibility gate for the old 3-chip TDB MATCH
    // row; replaced by the always-visible TDB pill multi-select row.
    // Kept as a no-op stub because several status-chip handlers
    // still call it and removing the call sites cleanly is out of
    // scope for this ship — strictly cosmetic.
    return;
  }

  function updateSortIndicators() {
    document.querySelectorAll('th.col-sort').forEach((th) => {
      const ind = th.querySelector('.sort-indicator');
      if (!ind) return;
      const isActive = th.dataset.sort === libraryState.sort;
      th.classList.toggle('col-sort-active', isActive);
      ind.textContent = isActive ? (libraryState.sortDir === 'desc' ? '▼' : '▲') : '';
    });
    // v1.12.68: reflect the attention-sort state on the
    // // NEEDS WORK chip. Active when libraryState.sort is
    // 'attention'; clicking toggles back to the prior column
    // sort (or title asc as the fallback).
    const attnBtn = document.getElementById('library-sort-attention-btn');
    if (attnBtn) {
      attnBtn.classList.toggle('chip-active', libraryState.sort === 'attention');
    }
  }

  async function loadLibrary() {
    const tabEl = document.getElementById('library-tab');
    if (!tabEl) return;
    libraryState.tab = tabEl.value;
    // v1.13.13: persist filter combo so a hop to another library
    // tab (MOVIES → TV SHOWS) lands with the same filters applied.
    _saveLibraryFilterState();
    const params = new URLSearchParams({
      tab: libraryState.tab,
      fourk: libraryState.fourk ? 'true' : 'false',
      page: libraryState.page,
      per_page: libraryState.perPage,
    });
    if (libraryState.q) params.set('q', libraryState.q);
    if (libraryState.status !== 'all') params.set('status', libraryState.status);
    if (libraryState.tdb && libraryState.tdb !== 'any') {
      params.set('tdb', libraryState.tdb);
    }
    // v1.12.23: pill axes ride the URL so the server filters
    // honor counts + pagination + sort. Pre-fix these were
    // client-side only — the count was the pre-filter total
    // and sort across pages dropped rows the filter rejected.
    if (libraryState.srcFilter && libraryState.srcFilter.size > 0) {
      params.set('src_pills', Array.from(libraryState.srcFilter).join(','));
    }
    if (libraryState.tdbPills && libraryState.tdbPills.size > 0) {
      params.set('tdb_pills', Array.from(libraryState.tdbPills).join(','));
    }
    if (libraryState.dlPills && libraryState.dlPills.size > 0) {
      params.set('dl_pills', Array.from(libraryState.dlPills).join(','));
    }
    if (libraryState.plPills && libraryState.plPills.size > 0) {
      params.set('pl_pills', Array.from(libraryState.plPills).join(','));
    }
    if (libraryState.edPills && libraryState.edPills.size > 0) {
      params.set('ed_pills', Array.from(libraryState.edPills).join(','));
    }
    if (libraryState.linkPills && libraryState.linkPills.size > 0) {
      params.set('link_pills', Array.from(libraryState.linkPills).join(','));
    }
    if (libraryState.sort && libraryState.sort !== 'title') {
      params.set('sort', libraryState.sort);
    }
    if (libraryState.sortDir && libraryState.sortDir !== 'asc') {
      params.set('sort_dir', libraryState.sortDir);
    }
    const tbody = document.getElementById('library-body');
    // v1.13.23: don't clobber tbody to "loading…" when prior rows are
    // already rendered. The v1.13.21 hash-skip (~3697) compares the
    // new HTML against tbody.dataset.lastHash — when the new render
    // matches (unchanged poll, same items), the populated-branch
    // write is skipped, so a fresh "loading…" placeholder painted
    // here would persist on screen until the user navigated away.
    // Visible during sync plex (slow API under DB lock contention)
    // AND during ordinary 5s rapid-poll ticks. Only paint the
    // placeholder on the empty-state case (no prior render); during
    // a re-fetch, leave existing rows in place until the new render
    // either matches (skip) or differs (overwrite).
    if (tbody.dataset.lastHash == null) {
      tbody.innerHTML = `<tr><td colspan="9" class="muted center">loading…</td></tr>`;
    }
    // v1.13.28: in-flight guard. Pre-fix two loadLibrary() calls
    // could race — a fast filter-chip click while the prior fetch
    // was still in flight would let whichever response landed second
    // win, possibly clobbering fresher data with stale. Bumping a
    // monotonic token on each call lets the older call detect it's
    // been superseded and bail before touching tbody.
    loadLibrary._seq = (loadLibrary._seq || 0) + 1;
    const _myToken = loadLibrary._seq;
    let data;
    try {
      data = await api('GET', '/api/library?' + params.toString());
    } catch (e) {
      if (loadLibrary._seq !== _myToken) return;
      tbody.innerHTML = `<tr><td colspan="9" class="accent-red">${htmlEscape(e.message)}</td></tr>`;
      return;
    }
    if (loadLibrary._seq !== _myToken) return;
    // v1.10.35: silent dedup keyed on (theme, media_type, folder_path).
    // Plex sometimes lists one movie twice in the same section under
    // different rating_keys (file versions Plex didn't merge). When
    // those rating_keys share the same folder_path the rows are
    // visually identical and just confuse the user — collapse them.
    // Distinct folder_paths (1408 base vs Director's Cut, or movie
    // mounted in standard + 4K sections) stay as separate rows so
    // the edition pill / section label differentiates correctly.
    // Untracked rows / orphans / blank folder_path fall back to
    // rating_key so unrelated items aren't accidentally merged.
    const seenItems = new Map();
    const dedupedItems = [];
    for (const it of (data.items || [])) {
      const themed = (it.theme_media_type
                      && it.theme_tmdb !== null
                      && it.theme_tmdb !== undefined);
      const key = (themed && it.folder_path)
        ? `t:${it.theme_media_type}:${it.theme_tmdb}:${it.folder_path}`
        : `rk:${it.rating_key}`;
      if (seenItems.has(key)) continue;
      seenItems.set(key, it);
      dedupedItems.push(it);
    }
    libraryState.items = dedupedItems;
    if (dedupedItems.length === 0) {
      // v1.11.27: when an enum is actively running for this tab, show
      // a 'scanning now' cue instead of the stale 'click REFRESH FROM
      // PLEX' instruction. window.__motif_enum_active is updated by
      // the topbar status tick (set when plex_enum_active[tab][variant]
      // is true). Until it's clear the banner sits on the same prompt
      // that's been there for first-time visitors.
      const tab = libraryState.tab;
      const variant = libraryState.fourk ? 'fourk' : 'standard';
      const enumActive = !!(window.__motif_enum_active
                            && window.__motif_enum_active[tab]
                            && window.__motif_enum_active[tab][variant]);
      const msg = enumActive
        ? 'scanning Plex now — items will appear as the enum completes…'
        : 'no items — enable the relevant Plex sections in Settings → PLEX and click REFRESH FROM PLEX';
      tbody.innerHTML = `<tr><td colspan="9" class="muted center">${msg}</td></tr>`;
      // v1.13.21: drop the hash so a return to a populated state on
      // the next poll forces a real render (otherwise a transition
      // from N items → 0 items → N items could hash-match the first
      // N's HTML and skip the rebuild).
      delete tbody.dataset.lastHash;
    } else {
      // v1.12.23: pill filtering moved server-side. The server's
      // sql_count + sql_rows + ORDER BY all honor the pill set, so
      // counts / pagination / sort are correct. dedupedItems is
      // the final list to render — the only client-side trim is
      // the rating_key dedup pass above.
      // v1.13.21 (was v1.13.20): hash-skip the innerHTML swap when the
      // newly-rendered HTML is byte-identical to the previous render.
      // Pre-fix every triggered loadLibrary (action click, +600ms /
      // +15s retries, post-place refetch) tore down + rebuilt the
      // entire tbody even when nothing visible had changed, blowing
      // the user's scroll position back to the top mid-action. Cost:
      // one string compare per loadLibrary. The non-empty branch is
      // the only place we hit; the empty-state branch above always
      // writes fresh HTML for the prompt and doesn't get hash-guarded.
      const newHtml = dedupedItems.map(renderLibraryRow).join('');
      if (tbody.dataset.lastHash !== newHtml) {
        tbody.innerHTML = newHtml;
        tbody.dataset.lastHash = newHtml;
      }
    }
    updateLibrarySelectionUi();
    const cntEl = document.getElementById('library-count');
    if (cntEl) {
      // v1.10.10: when the user is on THEMERRDB ONLY, label the count
      // with the media type so it's obvious the chip is tab-scoped (the
      // SQL filter is by t.media_type — a /movies tab will never include
      // tv rows, but the label removes any ambiguity for the user).
      let scope = '';
      if (libraryState.status === 'not_in_plex') {
        if (libraryState.tab === 'movies') scope = ' ThemerrDB-only movies';
        else if (libraryState.tab === 'tv') scope = ' ThemerrDB-only shows';
        else scope = ' ThemerrDB-only items';
      }
      cntEl.textContent =
        `· ${fmt.num(data.total)}${scope || ' match' + (data.total === 1 ? '' : 'es')}`;
    }

    // v1.10.10: missing-themes banner removed. The chip filters
    // (THEMERRDB / DOWNLOADED / READY TO PLACE) and per-row actions
    // give the user direct paths to every action the banner offered,
    // so a separate "X downloads available" callout was just noise
    // (and it was firing on the FAILURES tab too where it made no
    // sense). The scan-needed hint stays — different concept.
    const scanHint = document.getElementById('library-scan-hint');
    if (scanHint) {
      scanHint.style.display = data.plex_enumerated ? 'none' : '';
    }
    // v1.11.77: TDB pill gate now lives on window.__motif_themes_have
    // (set during the topbar /api/stats tick) — keyed per media_type
    // so a cancelled-mid-sync still renders pills for the side that
    // got data. The previous last_sync_at gate is gone.
    const totalPages = Math.max(1, Math.ceil(data.total / libraryState.perPage));
    document.getElementById('library-pager').innerHTML = `
      <button data-lib-page="${libraryState.page - 1}" ${libraryState.page <= 1 ? 'disabled' : ''}>« prev</button>
      <span>page ${libraryState.page} / ${totalPages}</span>
      <button data-lib-page="${libraryState.page + 1}" ${libraryState.page >= totalPages ? 'disabled' : ''}>next »</button>
    `;
  }

  // v1.11.66: standalone SRC-letter classifier. Mirrors the badge
  // branch order in renderLibraryRow so the click-to-filter on the
  // SRC legend agrees with what the row's badge actually shows.
  // Returns one of T / U / A / M / P / - (literal dash).
  // v1.12.7: shared with computeTdbPill below + the row-level pill
   // render. Must mirror the api.py classification of permanent
   // upstream failures.
   const TDB_DEAD_FAILURES_GLOBAL = new Set([
     'video_private', 'video_removed',
     'video_age_restricted', 'geo_blocked',
   ]);

  // v1.12.7: classify a row by its TDB pill state for the multi-
  // select TDB pill filter. Returns one of:
  //   'tdb'     — TDB-tracked, no failure  → green pill
  //   'update'  — pending upstream URL update (declined or pending)
  //                → blue TDB ↑ pill
  //   'cookies' — cookies-required failure (user-fixable)
  //                → amber TDB ⚠ pill
  //   'dead'    — permanent upstream failure → red TDB ✗ pill
  //   'none'    — no themes record / orphan → gray "no TDB" pill
  // Precedence matches the row pill render: update > dead > cookies
  // > tdb > none. The pending_update flag (decision IN pending,
  // declined) lights up blue regardless of the underlying tracked
  // state; the row's actionable_update flag is what gates the
  // ACCEPT / KEEP menu actions but isn't relevant for filtering.
  // ============================================================
  // v1.13.11: saved filter presets — snapshot the current library
  // filter combo as a named preset and replay later from a dropdown.
  // Uses /api/saved-filters (scope='library'). Snapshot string is the
  // exact URL query the deep-link hydration code in bindLibrary
  // already understands, so applying a preset = setting
  // window.location.search to it.
  // ============================================================

  function _buildPresetQueryString() {
    // Mirror loadLibrary's params logic but EXCLUDE pagination/tab/
    // fourk — those are page context, not filter state. The deep-link
    // hydration code (bindLibrary) reads each of these keys.
    const p = new URLSearchParams();
    if (libraryState.q) p.set('q', libraryState.q);
    if (libraryState.status && libraryState.status !== 'all') {
      p.set('status', libraryState.status);
    }
    if (libraryState.tdb && libraryState.tdb !== 'any') {
      p.set('tdb', libraryState.tdb);
    }
    if (libraryState.srcFilter && libraryState.srcFilter.size > 0) {
      p.set('src_pills', Array.from(libraryState.srcFilter).join(','));
    }
    if (libraryState.tdbPills && libraryState.tdbPills.size > 0) {
      p.set('tdb_pills', Array.from(libraryState.tdbPills).join(','));
    }
    if (libraryState.dlPills && libraryState.dlPills.size > 0) {
      p.set('dl_pills', Array.from(libraryState.dlPills).join(','));
    }
    if (libraryState.plPills && libraryState.plPills.size > 0) {
      p.set('pl_pills', Array.from(libraryState.plPills).join(','));
    }
    if (libraryState.edPills && libraryState.edPills.size > 0) {
      p.set('ed_pills', Array.from(libraryState.edPills).join(','));
    }
    if (libraryState.linkPills && libraryState.linkPills.size > 0) {
      p.set('link_pills', Array.from(libraryState.linkPills).join(','));
    }
    if (libraryState.sort && libraryState.sort !== 'title') {
      p.set('sort', libraryState.sort);
    }
    if (libraryState.sortDir && libraryState.sortDir !== 'asc') {
      p.set('sort_dir', libraryState.sortDir);
    }
    return p.toString();
  }

  // v1.13.13: cross-tab filter + search persistence.
  //
  // Pre-fix switching between MOVIES → TV SHOWS → ANIME walked the user
  // back to a no-filter library every time, since each tab is a fresh
  // page load with empty URL params. Now libraryState gets snapshotted
  // to localStorage on every loadLibrary() call and replayed when the
  // user lands on another library tab without filter URL params.
  //
  // Precedence: URL deep-links (e.g., topbar UPD click → /movies?
  // tdb_pills=update) always win; localStorage only fills in when the
  // URL carries no filter keys. CLEAR ALL nukes the localStorage
  // snapshot too so the next tab visit truly starts clean.
  const _LIB_STATE_KEY = 'motif:library_filter_state';
  const _LIB_FILTER_URL_KEYS = [
    'status', 'tdb', 'tdb_pills', 'src_pills', 'dl_pills', 'pl_pills',
    'link_pills', 'ed_pills', 'sort', 'sort_dir', 'q',
  ];

  function _saveLibraryFilterState() {
    try {
      const payload = {
        q: libraryState.q,
        status: libraryState.status,
        tdb: libraryState.tdb,
        srcFilter: Array.from(libraryState.srcFilter || []),
        tdbPills: Array.from(libraryState.tdbPills || []),
        dlPills: Array.from(libraryState.dlPills || []),
        plPills: Array.from(libraryState.plPills || []),
        linkPills: Array.from(libraryState.linkPills || []),
        edPills: Array.from(libraryState.edPills || []),
        sort: libraryState.sort,
        sortDir: libraryState.sortDir,
      };
      localStorage.setItem(_LIB_STATE_KEY, JSON.stringify(payload));
    } catch (_) { /* private mode / quota — fine */ }
  }

  function _hydrateLibraryFromStorage() {
    let raw;
    try { raw = localStorage.getItem(_LIB_STATE_KEY); } catch (_) { return; }
    if (!raw) return;
    let payload;
    try { payload = JSON.parse(raw); } catch (_) { return; }
    if (!payload || typeof payload !== 'object') return;
    if (payload.q) {
      libraryState.q = String(payload.q);
      const search = document.getElementById('library-search');
      if (search) search.value = libraryState.q;
    }
    if (payload.status && payload.status !== 'all') {
      libraryState.status = payload.status;
      document.querySelectorAll('[data-status]').forEach((x) =>
        x.classList.toggle('chip-active', x.dataset.status === payload.status));
    }
    if (payload.tdb && payload.tdb !== 'any') {
      libraryState.tdb = payload.tdb;
      document.querySelectorAll('[data-tdb]').forEach((x) =>
        x.classList.toggle('chip-active', x.dataset.tdb === payload.tdb));
    }
    const HYDRATE_MAP = [
      { key: 'srcFilter', attr: 'srcFilter', activeClass: 'src-key-btn-active' },
      { key: 'tdbPills',  attr: 'tdbPill',   activeClass: 'tdb-pill-btn-active' },
      { key: 'dlPills',   attr: 'dlPill',    activeClass: 'state-pill-btn-active' },
      { key: 'plPills',   attr: 'plPill',    activeClass: 'state-pill-btn-active' },
      { key: 'linkPills', attr: 'linkPill',  activeClass: 'link-pill-btn-active' },
      { key: 'edPills',   attr: 'edPill',    activeClass: 'state-pill-btn-active' },
    ];
    for (const m of HYDRATE_MAP) {
      const arr = payload[m.key];
      if (!Array.isArray(arr) || !arr.length) continue;
      arr.forEach((v) => libraryState[m.key].add(v));
      const kebab = m.attr.replace(/[A-Z]/g, (c) => '-' + c.toLowerCase());
      document.querySelectorAll(`[data-${kebab}]`).forEach((x) => {
        const xVal = x.dataset[m.attr];
        if (xVal && libraryState[m.key].has(xVal)) {
          x.classList.add(m.activeClass);
        }
      });
    }
    if (payload.sort && payload.sort !== 'title') libraryState.sort = payload.sort;
    if (payload.sortDir && payload.sortDir !== 'asc') libraryState.sortDir = payload.sortDir;
  }

  function _clearLibraryFilterStorage() {
    try { localStorage.removeItem(_LIB_STATE_KEY); } catch (_) { /* fine */ }
  }

  let _libraryPresets = [];
  let _activePresetId = null;

  // v1.13.18 (A2): popover-driven preset menu. Renders each saved
  // filter as a clickable list item with an inline × delete; the
  // bookmark glyph fills (★) when the current state matches a
  // saved preset so users get a glanceable indicator. The drift
  // detector flips the bookmark back to ☆ when libraryState
  // diverges. <details>/<summary> handles open/close natively;
  // an outside-click listener closes when focus drifts.
  async function loadLibraryPresets() {
    const list = document.getElementById('library-presets-list');
    if (!list) return;
    let data;
    try {
      data = await api('GET', '/api/saved-filters?scope=library');
    } catch (e) {
      console.error('saved-filters load failed:', e);
      return;
    }
    _libraryPresets = (data && data.filters) || [];
    _renderPresetsList();
    _updatePresetActiveState();
  }

  function _renderPresetsList() {
    const list = document.getElementById('library-presets-list');
    if (!list) return;
    if (!_libraryPresets.length) {
      list.innerHTML = '<li class="library-presets-popup-empty muted small">none yet</li>';
      return;
    }
    list.innerHTML = _libraryPresets.map((f) => `
      <li>
        <button type="button" class="library-presets-popup-apply"
                data-preset-id="${f.id}"
                title="Apply this preset">${htmlEscape(f.name)}</button>
        <button type="button" class="library-presets-popup-del"
                data-preset-del="${f.id}"
                title="Delete">×</button>
      </li>
    `).join('');
  }

  function _updatePresetActiveState() {
    const menu = document.getElementById('library-presets-menu');
    const bookmark = menu ? menu.querySelector('.library-presets-bookmark') : null;
    if (!menu || !bookmark) return;
    const here = _buildPresetQueryString();
    const match = _libraryPresets.find((f) => f.query_json === here);
    _activePresetId = match ? match.id : null;
    if (match) {
      menu.classList.add('has-active');
      bookmark.textContent = '★';
    } else {
      menu.classList.remove('has-active');
      bookmark.textContent = '☆';
    }
    // Highlight the active item in the list.
    const list = document.getElementById('library-presets-list');
    if (list) {
      list.querySelectorAll('.library-presets-popup-apply').forEach((btn) => {
        btn.classList.toggle('is-active',
          match && String(match.id) === btn.dataset.presetId);
      });
    }
  }

  async function saveLibraryPreset() {
    const queryStr = _buildPresetQueryString();
    if (!queryStr) {
      alert('No filters active — select at least one filter, search, or sort before saving.');
      return;
    }
    const name = (prompt('Save current filter as:') || '').trim();
    if (!name) return;
    try {
      await api('POST', '/api/saved-filters', {
        name, scope: 'library', query_json: queryStr,
      });
      await loadLibraryPresets();
    } catch (e) {
      alert('Save failed: ' + (e.message || e));
    }
  }

  function applyLibraryPreset(id) {
    const preset = _libraryPresets.find((f) => f.id === Number(id));
    if (!preset) return;
    const path = window.location.pathname;
    const hash = window.location.hash || '';
    const sep = preset.query_json ? '?' : '';
    window.location.href = `${path}${sep}${preset.query_json}${hash}`;
  }

  async function deletePresetById(id) {
    const preset = _libraryPresets.find((f) => f.id === Number(id));
    if (!preset) return;
    if (!confirm(`Delete saved filter "${preset.name}"?`)) return;
    try {
      await api('DELETE', `/api/saved-filters/${id}`);
      // If the deleted preset was active, strip the URL so we
      // land in a clean state. Otherwise just re-fetch the list.
      if (Number(id) === _activePresetId) {
        window.location.href = window.location.pathname;
        return;
      }
      await loadLibraryPresets();
    } catch (e) {
      alert('Delete failed: ' + (e.message || e));
    }
  }

  function bindLibraryPresets() {
    const menu = document.getElementById('library-presets-menu');
    const saveBtn = document.getElementById('library-presets-save');
    const list = document.getElementById('library-presets-list');
    if (!menu) return;
    if (saveBtn) {
      saveBtn.addEventListener('click', () => {
        menu.removeAttribute('open');
        saveLibraryPreset().catch((e) => console.error(e));
      });
    }
    if (list) {
      list.addEventListener('click', (ev) => {
        const apply = ev.target.closest('.library-presets-popup-apply');
        if (apply) {
          applyLibraryPreset(apply.dataset.presetId);
          return;
        }
        const del = ev.target.closest('.library-presets-popup-del');
        if (del) {
          ev.stopPropagation();
          deletePresetById(del.dataset.presetDel).catch((e) => console.error(e));
        }
      });
    }
    // Outside-click closes the popover.
    document.addEventListener('click', (ev) => {
      if (!menu.hasAttribute('open')) return;
      if (!menu.contains(ev.target)) menu.removeAttribute('open');
    });
    // Drift detection — refresh the bookmark active-state every 600ms
    // so applying a preset, then editing pills, flips the icon back
    // to ☆ without a polling-heavy approach.
    setInterval(_updatePresetActiveState, 600);
    loadLibraryPresets().catch((e) => console.error(e));
  }

  function computeTdbPill(it) {
    const isThemerrDbAvail = it.upstream_source
      && it.upstream_source !== 'plex_orphan';
    if (!isThemerrDbAvail) return 'none';
    // v1.12.48: pending_update only meaningful when the row is
    // actually themed. If src='-' (no theme anywhere) there's
    // nothing to "update" — fall through to plain green TDB so
    // the SOURCE menu prompts a fresh download instead of an
    // ACCEPT UPDATE that would just download the same URL.
    if (it.pending_update && computeSrcLetter(it) !== '-') return 'update';
    if (it.failure_kind && TDB_DEAD_FAILURES_GLOBAL.has(it.failure_kind)) {
      return 'dead';
    }
    if (it.failure_kind === 'cookies_expired'
        && !window.__motif_cookies_present) {
      return 'cookies';
    }
    // v1.13.1 (Phase C): dropped state ranks below failures — a
    // dropped-AND-broken row should still surface as red TDB✗ so
    // the user fixes the URL first. Only reach 'dropped' when the
    // URL still works.
    if (it.tdb_dropped_at) return 'dropped';
    return 'tdb';
  }

  function computeSrcLetter(it) {
    const placed = !!it.media_folder;
    const placedProv = it.placement_provenance;
    const sidecarOnly = !placed && !!it.plex_local_theme;
    const isOrphanRow = it.upstream_source === 'plex_orphan';
    const sourceKind = it.source_kind || null;
    const svid = it.source_video_id || '';
    const looksLikeYoutubeId = /^[A-Za-z0-9_-]{11}$/.test(svid);
    if (placed && sourceKind === 'themerrdb') return 'T';
    if (placed && sourceKind === 'adopt') return 'A';
    if (placed && (sourceKind === 'url' || sourceKind === 'upload')) return 'U';
    if (placed && placedProv === 'auto') return 'T';
    if (placed && placedProv === 'manual') {
      const wasUploadedOrUrl = (svid === '' || looksLikeYoutubeId);
      return (!isOrphanRow || wasUploadedOrUrl) ? 'U' : 'A';
    }
    if (sidecarOnly) return 'M';
    // v1.12.112: 'P' fires when Plex's theme claim is verified live
    // (HEAD against /library/metadata/{rk}/theme returned 200) OR
    // hasn't been tested yet (NULL → optimistic trust). Verified
    // stale (0) falls through to '-'. Mirrors the SRC SQL's
    // COALESCE(plex_theme_verified_ok, 1) = 1 check exactly so the
    // row badge agrees with server-side filtering. Reverts v1.12.111's
    // media_type gate — themerr-plex embeds on movies legitimately
    // classify as P, and verification distinguishes them from stale
    // post-PURGE cache without baking a media-type assumption.
    const verified = it.plex_theme_verified_ok;
    const verifiedOk = (verified === null || verified === undefined
                        || verified === 1);
    if (it.plex_has_theme && verifiedOk) return 'P';
    return '-';
  }

  function renderLibraryRow(it) {
    // v1.10.1: not_in_plex rows are ThemerrDB-only — synthesized into the
    // plex-shaped schema by the API. Render them with a distinct style and
    // limited actions (the title isn't in the user's Plex library, so
    // download still works but placement targets nothing).
    if (it.not_in_plex) {
      return renderLibraryRowNotInPlex(it);
    }
    const themed = it.theme_tmdb !== null && it.theme_tmdb !== undefined;
    const themeMt = it.theme_media_type;
    const themeId = it.theme_tmdb;
    const downloaded = !!it.file_path;

    // Source badge — reflects what Plex is actually playing. v1.10.4 split
    // the old generic M into U/A/M so the user can tell at a glance whether
    // motif owns the file:
    //   T = ThemerrDB-sourced; motif downloaded from upstream (auto)
    //   U = User-managed: motif placed a file the user provided (UI upload
    //       or manual YouTube URL). Either an override on a real ThemerrDB
    //       title, or an orphan with no upstream record.
    //   A = Adopted sidecar: motif took ownership of an existing theme.mp3
    //       at the Plex folder (no ThemerrDB link, file is the source of
    //       truth). Differentiated from U by source_video_id pattern.
    //   M = Loose theme.mp3 sidecar at the Plex folder that motif doesn't
    //       manage yet (run /scans → ADOPT to claim it).
    //   P = Plex agent / cloud — Plex has a theme but no local sidecar AND
    //       motif doesn't manage it.
    //   — = no theme anywhere.
    //
    // v1.10.12: source_kind on local_files is the authoritative
    // discriminator. 'themerrdb' / 'url' / 'upload' / 'adopt' are
    // stamped at insert time. The svid heuristic stays as a fallback
    // for rows older than the migration that didn't get backfilled
    // confidently.
    // v1.10.53: SRC badge leads with source_kind (the authoritative
    // sticker on the canonical) instead of placedProv. Pre-1.10.53 a
    // re-download on top of an adopted row could update local_files
    // (provenance=auto, source_kind=themerrdb) while leaving the
    // placement row at provenance=manual (because place_theme
    // skipped the placement when force=False and a sidecar
    // already existed). The old logic gated on placedProv first
    // and then used source_kind only inside the manual branch, so
    // 'manual placement + themerrdb local_files' rendered as U.
    // Now: source_kind tells the truth about who owns the canonical;
    // placedProv only matters for legacy rows with no source_kind.
    //   themerrdb → T
    //   adopt     → A
    //   url/upload→ U
    //   null      → fall back to placedProv + svid heuristic
    const placed = !!it.media_folder;
    const placedProv = it.placement_provenance;
    const sidecarOnly = !placed && !!it.plex_local_theme;
    const isOrphanRow = it.upstream_source === 'plex_orphan';
    const sourceKind = it.source_kind || null;
    const svid = it.source_video_id || '';
    const looksLikeYoutubeId = /^[A-Za-z0-9_-]{11}$/.test(svid);
    let srcCell;
    if (placed && sourceKind === 'themerrdb') {
      srcCell = '<span class="link-badge link-badge-themerrdb" title="motif manages from ThemerrDB">T</span>';
    } else if (placed && sourceKind === 'adopt') {
      srcCell = '<span class="link-badge link-badge-adopt" title="Adopted sidecar (no TDB link)">A</span>';
    } else if (placed && (sourceKind === 'url' || sourceKind === 'upload')) {
      srcCell = '<span class="link-badge link-badge-user" title="User-provided theme (upload or manual URL)">U</span>';
    } else if (placed && placedProv === 'auto') {
      // Legacy rows (source_kind NULL) — provenance='auto' === T.
      srcCell = '<span class="link-badge link-badge-themerrdb" title="motif manages from ThemerrDB">T</span>';
    } else if (placed && placedProv === 'manual') {
      // Legacy fallback heuristic for rows without source_kind.
      const wasUploadedOrUrl = (svid === '' || looksLikeYoutubeId);
      const kind = (!isOrphanRow || wasUploadedOrUrl) ? 'url' : 'adopt';
      if (kind === 'adopt') {
        srcCell = '<span class="link-badge link-badge-adopt" title="Adopted sidecar (no TDB link)">A</span>';
      } else {
        srcCell = '<span class="link-badge link-badge-user" title="User-provided theme (upload or manual URL)">U</span>';
      }
    } else if (sidecarOnly) {
      srcCell = '<span class="link-badge link-badge-manual" title="Manual sidecar (click ADOPT to manage)">M</span>';
    } else if (it.plex_has_theme) {
      srcCell = '<span class="link-badge link-badge-cloud" title="Plex agent / cloud theme">P</span>';
    } else {
      srcCell = '<span class="muted" title="no theme">—</span>';
    }
    // v1.13.34: composite-+P detection rewritten. v1.13.31's gate
    // (`plex_has_theme && verifiedOk`) over-fired massively because
    // Plex sets `pi.has_theme=1` on EVERY row where its metadata
    // reports a theme — including the sidecar motif itself placed.
    // So every T/U/A/M row got a phantom +P. The right discriminator
    // for "Plex also serves its own theme" is the absent-sidecar
    // case: `has_theme=1 AND local_theme_file=0`. That covers Plex
    // Pass cloud themes, themerr-plex embeds, and stale Plex agent
    // claims — all the cases where Plex's theme isn't backed by a
    // file at the folder. When motif placed a sidecar successfully,
    // local_theme_file=1 and the composite condition stays false
    // (no separate Plex theme to surface). Edge case: sidecar
    // externally deleted while motif's lf row still claims placement
    // — `canonical_missing` exclusion below would route that to the
    // dl-broken state instead.
    //
    // Visual change: drop the stacked second chip (it looked
    // double-stacked when the cell wrapped). Replace with a single
    // corner-dot indicator on the primary chip — same chip, single
    // visual unit. Class `link-badge-also-plex` triggers the
    // `::after` dot in app.css.
    const _verifiedOk = (it.plex_theme_verified_ok === null
                         || it.plex_theme_verified_ok === undefined
                         || it.plex_theme_verified_ok === 1);
    const _primaryLetter = computeSrcLetter(it);
    const _plexAlso = !!it.plex_has_theme && !it.plex_local_theme
                      && _verifiedOk
                      && _primaryLetter !== 'P'
                      && _primaryLetter !== '-';
    if (_plexAlso) {
      // Inject the indicator class onto the existing chip rather
      // than appending a new chip. The matchAll/replace pattern
      // touches only the first link-badge in srcCell (the primary
      // letter chip we just built) and adds an extra class +
      // tooltip explaining the composite state.
      srcCell = srcCell.replace(
        /^(<span class="link-badge[^"]*)("[^>]*title=")([^"]*)/,
        '$1 link-badge-also-plex$2$3 · Plex also serves its own theme (cloud / embed); the small dot signals the composite state.',
      );
      // If the chip didn't have a title= attr (rare path), still
      // tag the class so the dot renders.
      if (!srcCell.includes('link-badge-also-plex')) {
        srcCell = srcCell.replace(
          /class="link-badge/,
          'class="link-badge link-badge-also-plex',
        );
      }
    }

    // v1.11.62: 'broken' DL state — motif's local_files row says we
    // have a canonical, but a stat-check (server-side) found the file
    // missing. The placement in the Plex folder is still there, so
    // the row should call out 'still in plex, not downloaded' and
    // surface a RESTORE FROM PLEX action.
    const dlBroken = !!it.canonical_missing && !!it.file_path;
    // v1.11.99: mismatch_state tracks "canonical content diverged
    // from the placement file". v1.12.81 removed the amber
    // 'mismatch' DL state — the LINK column's M glyph is the
    // single source of truth for content divergence, and the
    // amber `!` row-title glyph is the at-a-glance attention
    // signal. DL now answers exactly one question: is the canonical
    // file present?
    const isMismatch = !!it.mismatch_state;
    const dl = dlBroken ? 'broken'
             : (downloaded ? 'on' : '');
    // v1.12.65: PL column gains a third state — 'await' (amber) —
    // when the canonical exists but no placement does. Pre-fix, a
    // post-DEL row dropped to a plain gray PL dot, the same as a
    // never-themed row, so the "you have the file, push it to
    // Plex" call-to-action wasn't visible from the column scan.
    // Amber matches the title-glyph color so the row's two
    // attention signals agree.
    // v1.12.66: awaitingApproval was originally declared further
    // down (after the title-cell glyphs), but the v1.12.65 pl
    // computation referenced it before its const declaration —
    // ReferenceError in the temporal dead zone, which crashed
    // renderLibraryRow and left every library tab stuck at
    // "loading…". Moved the declaration up here so pl can read it
    // safely; the original declaration site below is removed.
    // v1.12.81: PL gains 'broken' — placement row exists but the
    // theme.mp3 is missing from the Plex folder (Plex deleted it,
    // file moved manually, etc.). Symmetry with DL=broken; uses
    // the same red palette so users can spot the inverse case.
    const awaitingApproval = !it.job_in_flight && !!it.file_path && !it.media_folder;
    const placementBroken = !!placed && !!it.placement_missing;
    const pl = placementBroken ? 'broken'
             : placed ? 'on'
             : awaitingApproval ? 'await'
             : '';
    // v1.13.13 (Option C): pulse-tint the DL dot amber when a
    // download job is in flight for this row. Doesn't add a new
    // state to the legend — it's a transient cue that disappears
    // when the job lands. job_in_flight encodes the job_type
    // (download / place); pl gets a similar pulse when a place
    // job is in flight for symmetry.
    const downloadInFlight = it.job_in_flight === 'download';
    const placeInFlight = it.job_in_flight === 'place';
    // v1.12.81 / v1.13.13: hover tooltips for the DL / PL state dots
    // so each color is self-describing. KISS pass — short reasons
    // first, action hints stay terse.
    const dlTip = downloadInFlight
      ? 'Download in progress…'
      : dl === 'broken'
        ? 'Canonical missing on disk (Plex copy intact).'
        : dl === 'on'
          ? 'Canonical theme is on disk.'
          : 'No canonical downloaded.';
    const plTip = placeInFlight
      ? 'Placement in progress…'
      : pl === 'broken'
        ? 'Placement file missing from Plex folder.'
        : pl === 'on'
          ? 'Placed in Plex folder.'
          : pl === 'await'
            ? 'Downloaded, awaiting placement.'
            : 'Not placed in Plex folder.';
    let linkCell = '<span class="link-glyph link-glyph-none">—</span>';
    if (isMismatch && placed) {
      linkCell = '<span class="link-glyph link-glyph-mismatch" title="Mismatch — canonical differs from Plex copy">M</span>';
    } else if (it.placement_kind === 'hardlink') {
      linkCell = '<span class="link-glyph link-glyph-hardlink" title="Hardlink (shared inode)">HL</span>';
    } else if (it.placement_kind === 'copy') {
      linkCell = '<span class="link-glyph link-glyph-copy" title="Copy (cross-FS fallback — uses extra disk)">C</span>';
    }

    // Title-cell glyphs
    const titleGlyphs = [];
    let rowExtra = '';
    // v1.12.66: awaitingApproval declaration moved up to the dl/pl
    // block. It was here originally but pl now needs it earlier;
    // declaring twice would shadow + ReferenceError under TDZ.
    // v1.12.106: strict one-glyph hierarchy. Pre-fix a row could
    // stack up to FOUR glyphs (failure + job + update + await /
    // mismatch / broken) which read as visual noise — users had
    // to decode the order to figure out what was actually
    // attention-worthy. New rule: surface the highest-priority
    // signal as the single glyph; the rest stay reachable in the
    // INFO card and via the row tinting (.row-failure) /
    // state-pill columns.
    //
    // v1.13.14: in-flight (download/place) branch removed from the
    // glyph hierarchy. The DL/PL state-pill-pending pulse (v1.13.13
    // Option C) covers transient operational state in the columns
    // where it belongs; the title-cell glyph slot is reserved for
    // attention signals that need user intervention (failure,
    // pending update, await, mismatch, broken). Order is now:
    // failure > pending update > mismatch > await > broken > none.
    let glyphHtml = null;
    if (it.failure_kind && !it.failure_acked_at) {
      // v1.10.50: only show the ! glyph when the failure hasn't been
      // acknowledged. Acked rows keep their red TDB pill (still
      // failing upstream) but no longer pull attention; they're
      // hidden from the FAILURES filter for the same reason.
      // v1.12.87: clicking opens INFO — INFO is the single ACK
      // entry-point with the raw yt-dlp error + recovery options.
      const human = {
        'cookies_expired': 'YouTube cookies expired',
        'video_private': 'Video is private',
        'video_removed': 'Video was removed',
        'video_age_restricted': 'Age-restricted',
        'geo_blocked': 'Geo-blocked',
        'network_error': 'Network error',
        'unknown': 'Unknown failure'
      }[it.failure_kind] || it.failure_kind;
      const ackTip = `${human} — click to view in INFO and ACK`;
      glyphHtml =
        `<button class="title-glyph title-glyph-fail" title="${htmlEscape(ackTip)}" `
        + `data-act="info" data-mt="${themeMt}" data-id="${themeId}" `
        + `data-section-id="${htmlEscape(it.section_id || '')}" `
        + `data-kind-human="${htmlEscape(human)}" data-msg="${htmlEscape(it.failure_message || '')}" type="button">⚠</button>`;
      rowExtra = ' class="row-failure"';
    } else if (it.actionable_update && computeSrcLetter(it) !== '-') {
      // v1.12.78: blue ! glyph for pending upstream updates. Gated
      // on actionable_update (decision='pending') so KEEP CURRENT
      // clears it. Suppressed on src='-' rows (DOWNLOAD path covers
      // them, no theme to update from).
      const updTip = (it.pending_update_kind === 'urls_match')
        ? 'ThemerrDB now matches your override URL — open INFO to see the diff, ACCEPT UPDATE / KEEP CURRENT in the SOURCE menu.'
        : 'ThemerrDB has a new URL for this row — open INFO to see the proposed change, ACCEPT UPDATE / KEEP CURRENT in the SOURCE menu.';
      glyphHtml =
        `<span class="title-glyph title-glyph-update title-glyph-action" title="${htmlEscape(updTip)}">!</span>`;
    } else if (it.mismatch_state === 'pending') {
      // v1.12.81: canonical content diverged from the placement
      // file (SET URL / UPLOAD MP3 over an existing placement).
      // PLACE menu has the resolution actions.
      glyphHtml =
        `<span class="title-glyph title-glyph-await title-glyph-action" title="Mismatch — canonical differs from Plex copy. Open INFO for the diff.">!</span>`;
    } else if (awaitingApproval) {
      // v1.12.65: amber ! for canonical-downloaded-but-not-placed.
      // PLACE → PUSH TO PLEX applies, REMOVE → PURGE discards.
      glyphHtml =
        `<span class="title-glyph title-glyph-await" title="Downloaded, awaiting placement">!</span>`;
    } else if (dlBroken) {
      // v1.11.62: motif's canonical was deleted but the placement
      // is still in the Plex folder. RESTORE FROM PLEX recovers.
      glyphHtml =
        `<a class="title-glyph title-glyph-broken" title="Canonical missing — Plex copy intact (RESTORE FROM PLEX)" href="/${libraryState.tab}?status=dl_missing">↺</a>`;
    }
    if (glyphHtml) titleGlyphs.push(glyphHtml);

    const imdb = it.guid_imdb || '';
    const imdbLink = imdb
      ? `<a href="https://www.imdb.com/title/${htmlEscape(imdb)}" target="_blank" rel="noopener">${htmlEscape(imdb)}</a>`
      : '<span class="muted">—</span>';

    const sectionLabel = it.section_title ? ` <span class="muted small">[${htmlEscape(it.section_title)}]</span>` : '';
    // v1.10.17: edition tag from folder_path so users can tell apart
    // multiple Plex entries that share title+year (Director's Cut,
    // Extended, IMAX, etc.). Renders as a yellow chip after the title.
    const editionLabel = (() => {
      const ed = parseEditionFromFolderPath(it.folder_path);
      return ed
        ? ` <span class="edition-pill" title="Plex edition tag">${htmlEscape(ed)}</span>`
        : '';
    })();
    // v1.10.33: at-a-glance ThemerrDB-tracked indicator. v1.10.40
    // added the red TDB ✗ state for permanent-failure rows;
    // v1.10.42 splits cookies_expired out as a yellow TDB ⚠ since
    // that one's user-fixable (drop a cookies.txt file) rather
    // than a dead URL.
    // Set hoisted to row scope so v1.10.51's REPLACE-w/-TDB gate
    // can reuse it without redeclaring.
    const TDB_DEAD_FAILURES = new Set([
      'video_private', 'video_removed',
      'video_age_restricted', 'geo_blocked',
    ]);
    const tdbAvailLabel = (() => {
      if (it.not_in_plex) return '';
      // v1.11.29: hide the TDB pill entirely until we have data for
      // this row's media_type. Pre-sync the themes table is empty,
      // so every row would render as 'no TDB' — misleading.
      // v1.11.77: changed gate from last_sync_at (successful sync
      // ever) to per-media-type themes count > 0, so partial
      // captures (cancelled-mid-sync, sync only finished movies
      // before TV index was reached, etc.) still light up the pills
      // truthfully for the side that DID get data.
      const rowMt = (it.theme_media_type === 'tv'
                     || it.plex_media_type === 'show')
                    ? 'tv' : 'movie';
      const haveTdb = !!(window.__motif_themes_have
                         && window.__motif_themes_have[rowMt]);
      if (!haveTdb) return '';
      const isThemerrDbAvail = it.upstream_source
        && it.upstream_source !== 'plex_orphan';
      if (!isThemerrDbAvail) {
        return ' <span class="tdb-pill tdb-pill-no" title="No ThemerrDB record for this title.">no TDB</span>';
      }
      // v1.11.16: include the actual failure_kind + failure_message in
      // the pill tooltip when something failed, so hover gives concrete
      // 'error info' (was: a generic recovery list with no indication
      // of which specific reason applied to THIS row).
      const kindHuman = {
        'cookies_expired': 'cookies expired or missing',
        'video_private': 'video is private',
        'video_removed': 'video was removed from YouTube',
        'video_age_restricted': 'video is age-restricted',
        'geo_blocked': 'video is geo-blocked in this region',
        'network_error': 'network error reaching YouTube',
        'unknown': 'unknown failure',
      };
      const why = kindHuman[it.failure_kind] || it.failure_kind || '';
      const detail = it.failure_message
        ? `&#10;detail: ${htmlEscape(it.failure_message)}`
        : '';
      if (it.failure_kind === 'cookies_expired') {
        if (window.__motif_cookies_present) {
          // Cookies are configured — the next download attempt should
          // succeed and clear the flag. Stay green.
          return ' <span class="tdb-pill tdb-pill-yes" title="ThemerrDB tracked (cookies will refresh on next download)">TDB</span>';
        }
        return ` <span class="tdb-pill tdb-pill-cookies" title="Cookies required: ${htmlEscape(why)}${detail}">TDB ⚠</span>`;
      }
      if (it.failure_kind && TDB_DEAD_FAILURES.has(it.failure_kind)) {
        return ` <span class="tdb-pill tdb-pill-dead" title="Upstream URL broken: ${htmlEscape(why)}${detail}">TDB ✗</span>`;
      }
      // v1.12.108: restore the blue .tdb-pill-update for any row
      // with pending_update=true. v1.12.106 collapsed it under
      // "duplicate of glyph" but the user wants the pill as a
      // post-KEEP-CURRENT visual cue: glyph clears (action no
      // longer pending) but pill stays so the row is still
      // sortable / filterable as "has an upstream update". The
      // strict per-row hierarchy means the GLYPH is gated on
      // actionable_update (decision=='pending'); the PILL is
      // gated on pending_update (decision in pending+declined),
      // so KEEP CURRENT clears one but not the other.
      // v1.12.108: pending_update is now also gated on motif
      // tracking presence per-section in the SQL — the pill
      // doesn't show on post-PURGE / pure-P / pure-'-' rows.
      if (it.pending_update && computeSrcLetter(it) !== '-') {
        if (it.pending_update_kind === 'urls_match') {
          return ' <span class="tdb-pill tdb-pill-update" title="Your manual URL matches TDB — ACCEPT UPDATE to convert U → T (no re-download).">TDB ↑</span>';
        }
        return ' <span class="tdb-pill tdb-pill-update" title="Upstream URL changed — ACCEPT or KEEP from SOURCE menu.">TDB ↑</span>';
      }
      // v1.13.1 (Phase C): gray TDB◌ for items TDB used to publish
      // and has now stopped publishing. The local theme still works;
      // it's just no longer endorsed upstream. ACK DROP / CONVERT TO
      // MANUAL in the SOURCE menu let the user dismiss.
      if (it.tdb_dropped_at) {
        const dt = (typeof it.tdb_dropped_at === 'string'
                    && it.tdb_dropped_at.length >= 10)
                    ? it.tdb_dropped_at.slice(0, 10) : '';
        const since = dt ? ` (since ${dt})` : '';
        return ` <span class="tdb-pill tdb-pill-dropped" title="ThemerrDB stopped tracking this title${since}.">TDB ◌</span>`;
      }
      return ' <span class="tdb-pill tdb-pill-yes" title="ThemerrDB tracked">TDB</span>';
    })();

    // v1.10.24 Option C row actions — collapse the wide button row into
    // categorized menu buttons so each row stays in-bounds:
    //   [ⓘ INFO]  ·  [SOURCE ▾]  [PLACE ▾]  [REMOVE ▾]
    //
    // SOURCE ▾  — where the theme comes from
    //               DOWNLOAD/RE-DL/REVERT, URL, UPLOAD, ADOPT,
    //               REPLACE w/ TDB
    // PLACE ▾   — push the canonical to Plex's folder
    //               REPLACE (when downloaded but not placed),
    //               RE-PUSH (when already placed)
    // REMOVE ▾  — destructive
    //               DEL, UNMANAGE, × PURGE
    //
    // Native <details>/<summary> popover, same pattern as v1.10.13's
    // overflow with click-outside-close handler. Empty menus are
    // suppressed so untracked rows don't get a useless PLACE/REMOVE
    // button. INFO stays as its own clickable button.
    // v1.12.65: dropped awaitingApproval from the lock predicate.
    // Pre-fix, a post-DEL row (downloaded but unplaced) had every
    // SOURCE action greyed out with a tooltip pointing at the
    // removed /pending tab. Now SOURCE is unlocked — clicking
    // DOWNLOAD TDB / SET URL / etc. legitimately replaces the
    // awaiting canonical, which is what the user typically wants.
    // The amber title-glyph + amber PL pill remain as the visual
    // "action required" signal; PLACE → PUSH TO PLEX is still
    // the obvious recovery action.
    const lockManualActions = !!it.job_in_flight;
    const lockTitle = it.job_in_flight ? 'wait for current job to finish' : '';
    const isOrphan = it.upstream_source === 'plex_orphan';
    const isManual = it.provenance === 'manual';
    const isThemerrDb = it.upstream_source && it.upstream_source !== 'plex_orphan';
    const sourceKindForActions = (() => {
      if (it.source_kind) return it.source_kind;
      if (!isManual) return null;
      const svid = it.source_video_id || '';
      if (svid === '') return 'upload';
      if (/^[A-Za-z0-9_-]{11}$/.test(svid)) return 'url';
      return 'adopt';
    })();
    const isManualPlacement = placed && placedProv === 'manual';

    function menuItemHtml(act, label, tip, extras = {}) {
      const dataset = [
        extras.rk !== undefined ? `data-rk="${htmlEscape(extras.rk)}"` : '',
        extras.mt !== undefined ? `data-mt="${htmlEscape(extras.mt)}"` : '',
        extras.id !== undefined ? `data-id="${htmlEscape(extras.id)}"` : '',
        // v1.12.46: section_id flows through so handlers can scope
        // their action to the row's section (e.g. ACCEPT UPDATE
        // placing only in the section the user clicked from,
        // rather than fanning out to every section owning the
        // title — important when the same title lives in both
        // standard and 4K libraries with different editions).
        extras.sectionId !== undefined ? `data-section-id="${htmlEscape(extras.sectionId)}"` : '',
        `data-title="${htmlEscape(it.plex_title)}"`,
        `data-year="${htmlEscape(it.year || '')}"`,
        extras.orphan !== undefined ? `data-orphan="${extras.orphan ? '1' : '0'}"` : '',
        // v1.11.8: data-dl-only flags the DL-only PURGE case so the
        // confirm dialog can show the stronger warning ("after PURGE
        // you cannot PUSH TO PLEX"). Default '0' when absent so
        // btn.dataset.dlOnly === '1' is a clean predicate.
        extras.dlOnly !== undefined ? `data-dl-only="${extras.dlOnly}"` : '',
        // v1.12.54: TDB canonical URL flows through to SET URL so
        // the dialog can warn the user when their input matches —
        // setting the same URL as a manual override would just
        // create the U→T conversion loop the v1.12.53 sweep was
        // designed to surface, with extra UI churn for no benefit.
        extras.ytUrl !== undefined ? `data-yt-url="${htmlEscape(extras.ytUrl)}"` : '',
        // v1.12.107: row's currently-applied URL (per-section
        // override → '' override → themes.youtube_url) flows
        // through to the SET URL dialog so its match-warning
        // can fire when the user types the URL that's already
        // applied — covers the U-overrides-itself case the
        // v1.12.54 TDB-match warning didn't catch.
        extras.appliedUrl !== undefined ? `data-applied-url="${htmlEscape(extras.appliedUrl)}"` : '',
        // v1.12.62: row's current SRC letter flows through too so
        // the SET URL match-warning copy can branch — for src='-'
        // there's no file to "re-download", just to download.
        extras.srcLetter !== undefined ? `data-src-letter="${htmlEscape(extras.srcLetter)}"` : '',
        // v1.13.31: data-plex-also flags rows where Plex serves
        // its own theme regardless of motif's local placement.
        // PURGE confirm dialog reads it to surface the "Plex has a
        // fallback" preview so the user knows whether removing
        // motif's manage state leaves the title themeless or
        // gracefully falls back to Plex's own served theme.
        extras.plexAlso !== undefined ? `data-plex-also="${extras.plexAlso ? '1' : '0'}"` : '',
      ].filter(Boolean).join(' ');
      // v1.11.14: extras.tone tints the source menu entries to match
      // the SRC column badge colors so the user can read at a glance
      // which source state each action lands them in:
      //   themerrdb = T (green) — TDB download / re-download / revert
      //   user      = U (violet) — SET URL / UPLOAD MP3
      //   adopt     = A (cyan) — ADOPT
      //   manual    = M (magenta)
      //   cloud     = P (amber)
      // Falls back to the existing danger / warn / plain styling.
      const tone = extras.tone ? ` lib-source-${extras.tone}` : '';
      // v1.11.96: extras.info = blue "informational action" variant
      // for ACCEPT UPDATE (matches .chip-info / .tdb-pill-update /
      // .title-glyph-update). danger > warn > info > plain.
      const cls = extras.danger ? `btn btn-tiny btn-danger${tone}`
                : extras.warn   ? `btn btn-tiny btn-warn${tone}`
                : extras.info   ? `btn btn-tiny btn-info${tone}`
                :                 `btn btn-tiny${tone}`;
      // v1.10.34: extras.bypassLock lets actions like PUSH TO PLEX
      // remain clickable when the row is awaitingApproval (downloaded
      // but not placed). That state IS what PUSH TO PLEX resolves;
      // pre-1.10.34 the lock disabled it and forced users to /pending.
      // Real in-flight jobs still disable the button — clicking
      // mid-run would race the worker.
      const isLocked = extras.bypassLock
        ? !!it.job_in_flight
        : lockManualActions;
      const lockMsg = it.job_in_flight
        ? 'wait for current job to finish'
        : lockTitle;
      const titleAttr = isLocked
        ? ` disabled title="${htmlEscape(lockMsg)}"`
        : ` title="${htmlEscape(tip)}"`;
      return `<button class="${cls}" data-act="${act}" ${dataset}${titleAttr}>${label}</button>`;
    }

    // SOURCE menu
    //
    // v1.12.39: rewritten for consistent intent-grouping and
    // tone palette so the user can scan the menu top-to-bottom
    // and the order maps to "what motif thinks the row's most
    // useful action is" → "general source overrides" →
    // "housekeeping" → "undo".
    //
    // Section order (each section conditionally renders):
    //   1. CONTEXTUAL PROMPT     — ACCEPT UPDATE / KEEP CURRENT
    //                              when blue TDB ↑ is up.
    //   2. PRIMARY ACQUISITION   — ADOPT (sidecar), DOWNLOAD TDB
    //                              (initial fetch), RE-DOWNLOAD
    //                              TDB (refresh canonical).
    //   3. CUSTOM OVERRIDES      — SET URL, UPLOAD MP3.
    //   4. CROSS-SOURCE REPLACE  — REPLACE TDB on M/U/A/P rows.
    //   5. HOUSEKEEPING          — ACK FAILURE.
    //   6. UNDO                  — REVERT.
    //
    // Tone palette: blue (info) for upstream-update prompts;
    // themerrdb-green for TDB-fetching actions; user-violet for
    // user-source actions; adopt-cyan for ADOPT; plain for
    // dismiss-style actions (KEEP CURRENT, ACK FAILURE); REVERT
    // tone tracks its target kind so the button color hints at
    // where the row will land.
    const sourceItems = [];

    // v1.10.41: detect P-agent rows so SOURCE shows REPLACE w/ TDB
    // instead of DOWNLOAD. The user's mental model on those rows is
    // 'replace Plex's existing theme', not 'fetch one from scratch'.
    const isPlexAgent = !placed && !it.plex_local_theme && !!it.plex_has_theme;

    // ── 1. CONTEXTUAL PROMPT ──────────────────────────────────
    // ACCEPT UPDATE stays on the menu whenever the blue TDB ↑
    // pill is up (pending_update), even after KEEP CURRENT.
    // KEEP CURRENT only shows while the row is actionable_update
    // (decision='pending') — once declined, the row's already in
    // the "kept" state and the action would be a no-op.
    // v1.12.51: also gate on src!='-'. A pending_update on a row
    // the user doesn't have themed yet has nothing to "update
    // from"; ACCEPT UPDATE would just download the current TDB
    // URL, which is exactly what DOWNLOAD TDB does. Falling
    // through to PRIMARY ACQUISITION below gives the user the
    // green DOWNLOAD button instead of the violet ACCEPT UPDATE.
    // Mirrors the v1.12.48 row-pill gate.
    const srcLetter = computeSrcLetter(it);
    if (it.pending_update && themed && srcLetter !== '-'
        && themeId !== null && themeId !== undefined) {
      // v1.12.46: pass sectionId so the accept-update endpoint
      // scopes the download + place to ONLY this row's section.
      // Pre-fix _enqueue_download fanned out to every section
      // that owned the title, so accepting from the 4K row
      // would also overwrite the standard library's theme —
      // which the user wanted independently themed (different
      // editions = different themes).
      // v1.12.54: tooltip branches on pending_update_kind. For
      // 'urls_match' rows the action is really a U→T reclassify
      // (no new content gets downloaded — the file on disk
      // already matches what TDB would fetch); for the regular
      // 'upstream_changed' case the file genuinely changes.
      // Same endpoint handles both: api_accept_update's
      // url_match path covers urls_match and the v1.12.53 eager
      // flip lands the row at T immediately.
      const acceptTip = (it.pending_update_kind === 'urls_match')
        ? 'Convert this row from U to T. Your override URL already matches ThemerrDB, so the file on disk stays put — only the classification changes.'
        : 'Download the new ThemerrDB URL and replace the current theme in this section only.';
      sourceItems.push(menuItemHtml(
        'accept-update', 'ACCEPT UPDATE',
        acceptTip,
        { mt: themeMt, id: themeId, sectionId: it.section_id, info: true },
      ));
      if (it.actionable_update) {
        const declineTip = (it.pending_update_kind === 'urls_match')
          ? 'Dismiss the prompt; the row stays U (your manual override stays in place). The blue TDB ↑ pill stays for filter/sort.'
          : 'Dismiss the prompt; the blue TDB ↑ pill stays for filter/sort.';
        sourceItems.push(menuItemHtml(
          'decline-update', 'KEEP CURRENT',
          declineTip,
          { mt: themeMt, id: themeId, sectionId: it.section_id },
        ));
      }
    }

    // ── 2. PRIMARY ACQUISITION ────────────────────────────────
    // ADOPT — sidecar-only rows (theme.mp3 in the Plex folder
    // but no motif canonical). Cyan tone matches the A badge
    // the row will land on.
    if (sidecarOnly) {
      sourceItems.push(menuItemHtml(
        'adopt-sidecar', 'ADOPT',
        "Take ownership of the existing theme.mp3 sidecar.",
        { rk: it.rating_key, tone: 'adopt' },
      ));
    }
    // DOWNLOAD TDB / RE-DOWNLOAD TDB — T-source rows where the
    // upstream URL is healthy. Split into two mutually-exclusive
    // labels so the action's intent is unambiguous:
    //   !downloaded || dlBroken  → DOWNLOAD TDB    (initial / recovery)
    //   downloaded && !dlBroken  → RE-DOWNLOAD TDB (refresh canonical)
    // Suppressed on M/U/A (those have REPLACE TDB which fits
    // their mental model better) and on P-agent (REPLACE TDB
    // appears further down). Suppressed when pending_update is
    // up (ACCEPT UPDATE replaces both options with the
    // contextually-correct phrasing) and when accepted_update is
    // recent (canonical already came from the current TDB URL,
    // so DOWNLOAD/RE-DOWNLOAD would be a no-op).
    // Tone: themerrdb-green — matches the T badge the row will
    // land on.
    // v1.12.51: !it.pending_update suppresses DOWNLOAD whenever
    // a pending update exists, on the assumption ACCEPT UPDATE
    // covers it. That assumption breaks for src='-' rows (no
    // theme to update from) — those need DOWNLOAD to come back.
    // The CONTEXTUAL PROMPT block above now skips ACCEPT UPDATE
    // on src='-', so we mirror the gate here: a src='-' row with
    // a stale pending_update should still see the green DOWNLOAD
    // button.
    // v1.12.78: accepted_update gate now mirrors the pending_update
    // gate's src='-' escape hatch. Pre-fix a section that PURGEd
    // after a prior urls_match ACCEPT UPDATE saw DOWNLOAD TDB
    // suppressed because pending_updates(decision='accepted') is
    // title-global — it survives section PURGE on non-last sections.
    // The "downloading would be a no-op" justification breaks on
    // src='-' (no canonical to be redundant with), so let DOWNLOAD
    // through and trust the worker to fetch the current TDB URL.
    if (themed && themeId !== null && themeId !== undefined
        && !sidecarOnly && !isPlexAgent && !isManualPlacement
        && !lockManualActions && (!it.pending_update || srcLetter === '-')
        && (!it.accepted_update || srcLetter === '-')) {
      const tdbDeadForDownload = it.failure_kind
        && TDB_DEAD_FAILURES.has(it.failure_kind);
      const tdbCookiesBlocked = it.failure_kind === 'cookies_expired'
        && !window.__motif_cookies_present;
      const tdbBlocked = tdbDeadForDownload || tdbCookiesBlocked;
      const hasDownloadUrl = !!it.youtube_url
        || sourceKindForActions === 'url';
      if (!tdbBlocked && hasDownloadUrl) {
        if (!downloaded || dlBroken) {
          // v1.12.73: pass sectionId so RE-DOWNLOAD/DOWNLOAD TDB
          // targets only this row's section. Without it, the
          // re-download fanned out to every section that owned
          // the title — wrong for per-edition theming.
          sourceItems.push(menuItemHtml(
            'redl', 'DOWNLOAD TDB',
            'Download from ThemerrDB and place into the Plex folder for this section.',
            { mt: themeMt, id: themeId, sectionId: it.section_id,
              tone: 'themerrdb' },
          ));
        } else {
          // v1.12.39: RE-DOWNLOAD TDB returned per user feedback
          // for the canonical-refresh edge case (corrupted file,
          // post-recovery rebuild, etc.). Distinct from REPLACE
          // TDB because RE-DOWNLOAD applies to T rows whose
          // current canonical IS already from TDB; REPLACE TDB
          // applies to M/U/A/P rows whose current canonical is
          // NOT from TDB.
          sourceItems.push(menuItemHtml(
            'redl', 'RE-DOWNLOAD TDB',
            'Re-fetch from ThemerrDB and overwrite the canonical for this section (refresh / corruption recovery).',
            { mt: themeMt, id: themeId, sectionId: it.section_id,
              tone: 'themerrdb' },
          ));
        }
      }
    }

    // ── 3. CUSTOM OVERRIDES ───────────────────────────────────
    // SET URL / UPLOAD MP3 — always available, on any row. The
    // user can choose a custom source at any time regardless of
    // the row's current state. Violet tone matches the U badge
    // the row will land on.
    sourceItems.push(menuItemHtml(
      'manual-url', 'SET URL',
      'Provide a YouTube URL as a manual override.',
      { rk: it.rating_key, tone: 'user', ytUrl: it.youtube_url || '',
        appliedUrl: it.applied_youtube_url || '',
        srcLetter: srcLetter },
    ));
    sourceItems.push(menuItemHtml(
      'upload-theme', 'UPLOAD MP3',
      'Upload an MP3 file as the theme.',
      { rk: it.rating_key, tone: 'user' },
    ));

    // ── 3.5. TDB DROPPED REMEDIATION (Phase C, v1.13.1) ─────
    // When ThemerrDB stops publishing this title, the row gets a
    // gray TDB◌ pill. Two dismissal paths:
    //   ACK DROP — clear the flag, leave SRC=T as-is. The local
    //              theme stays linked to the (no-longer-published)
    //              TDB record. If TDB ever re-adds it, the next
    //              sync clears the flag automatically.
    //   CONVERT TO MANUAL — promote the current youtube_url into
    //              user_overrides; SRC reclassifies T → U; future
    //              syncs leave the row alone.
    if (it.tdb_dropped_at && themeMt && themeId !== null
        && themeId !== undefined) {
      sourceItems.push(menuItemHtml(
        'ack-drop', 'ACK DROP',
        "ThemerrDB stopped publishing this title. Dismiss the TDB◌ flag — the local theme stays as-is.",
        { mt: themeMt, id: themeId, tone: 'themerrdb' },
      ));
      if (it.youtube_url) {
        sourceItems.push(menuItemHtml(
          'convert-to-manual', 'CONVERT TO MANUAL',
          "Take ownership of this row: promote the current URL into user_overrides. SRC flips T → U; sync stops touching it.",
          { mt: themeMt, id: themeId, sectionId: it.section_id || '',
            tone: 'user' },
        ));
      }
    }

    // ── 4. CROSS-SOURCE REPLACE ───────────────────────────────
    // REPLACE TDB — fires when the user is swapping motif's
    // ThemerrDB download in for an existing theme from another
    // source (sidecar M, manual U, adopted A, Plex agent P).
    // Suppressed on T rows (DOWNLOAD/RE-DOWNLOAD TDB covers that
    // case), on rows with a permanent TDB failure (red ✗ pill —
    // would re-fail), on rows with cookies blocked, when a blue
    // TDB ↑ pill is up (ACCEPT UPDATE covers it), and when a
    // recent accept already pulled the current TDB URL (no-op).
    const tdbReplaceBlocked = (it.failure_kind
        && TDB_DEAD_FAILURES.has(it.failure_kind))
      || (it.failure_kind === 'cookies_expired'
          && !window.__motif_cookies_present);
    if (isThemerrDb && !tdbReplaceBlocked && !it.pending_update
        && !it.accepted_update
        && (sidecarOnly || isManualPlacement || isPlexAgent)) {
      const replaceTip = sidecarOnly
        ? "Download from ThemerrDB and replace the local sidecar."
        : isPlexAgent
          ? "Download from ThemerrDB and replace the Plex agent theme."
          : "Download from ThemerrDB and replace the current local theme.";
      sourceItems.push(menuItemHtml(
        'replace-with-themerrdb', 'REPLACE TDB',
        replaceTip,
        { rk: it.rating_key, warn: true, tone: 'themerrdb' },
      ));
    }

    // ── 5. HOUSEKEEPING ───────────────────────────────────────
    // v1.12.87: ACK FAILURE moved out of the SOURCE menu — INFO
    // card's // TRY THIS NEXT section is now the single ACK
    // entry-point. The row's red ! glyph also routes there
    // (was inline-clear pre-.87) so users always see the failure
    // context (kind, raw yt-dlp message, recovery options) before
    // dismissing. Reduces SOURCE-menu clutter and removes the
    // "what's the difference between the glyph and SOURCE → ACK"
    // ambiguity.

    // ── 6. UNDO ───────────────────────────────────────────────
    // REVERT — one-step undo. Surfaces only when there's a
    // captured previous URL on the row (themes.previous_youtube_url).
    // Populated by SET URL replacing an existing override,
    // ACCEPT UPDATE consuming a U row, REPLACE TDB consuming a
    // U row, or REVERT itself (round-trippable). Tone tracks
    // previous_youtube_kind so the button color hints at where
    // the row will land — violet for user, themerrdb-green for
    // upstream.
    // v1.12.40: also suppress when revert_redundant=1 — the
    // previous URL is a TDB URL that exactly matches the
    // pending_updates.new_youtube_url that ACCEPT UPDATE would
    // fetch. REVERT and ACCEPT UPDATE would do the same thing,
    // so showing both would be confusing. A user-kind previous
    // URL stays revertible regardless because reverting to U
    // is a meaningfully different state from accepting the
    // upstream update. If the previous is a TDB URL that
    // DOESN'T match the new URL (e.g., TDB rolled forward
    // again), both buttons render and serve different actions.
    // v1.12.65: REVERT now requires previous_youtube_kind='user'.
    // For 'themerrdb' kind the action is functionally identical
    // to DOWNLOAD TDB / RE-DOWNLOAD TDB / REPLACE TDB — the worker
    // discards the previous URL value and downloads from
    // themes.youtube_url either way (no per-job URL override
    // mechanism). Showing REVERT alongside those actions was
    // pure UI redundancy that confused users. The INFO card
    // explains the hidden-REVERT case so the missing button
    // doesn't read as a bug.
    // v1.12.101: relax the gate for src='-' and src='M' rows.
    // After PURGE / UNMANAGE there's no canonical and no
    // DOWNLOAD TDB action competing with RESTORE — the "kind
    // collides with TDB action" reasoning above only applies
    // to T rows where DOWNLOAD TDB is also visible. Without
    // this relaxation, UNMANAGEing a T row left the user with
    // no one-step path back ("here's what you just dropped,
    // bring it back") — they'd have to use DOWNLOAD TDB and
    // hope the upstream URL still matched. RESTORE makes the
    // intent explicit. Still gated on revert_redundant=0 so
    // we don't double-show alongside ACCEPT UPDATE.
    const restoreEligibleKind = it.previous_youtube_kind === 'user'
      || (srcLetter === '-' || srcLetter === 'M');
    if (it.has_previous_url && !it.revert_redundant
        && restoreEligibleKind) {
      // v1.12.47: pass sectionId so REVERT scopes the
      // re-download + place to only this row's section
      // (matches ACCEPT UPDATE per-section behavior).
      // v1.12.79: relabel to RESTORE on src='-' rows. PURGE /
      // UNMANAGE both capture the dropped URL into
      // previous_youtube_url so the user can bring it back
      // one-step. "RESTORE" reads more naturally than "REVERT"
      // for that scenario — "revert" implies undoing a config
      // change, "restore" implies bringing back a destroyed
      // theme. Same endpoint, different copy.
      // v1.12.81: also label RESTORE on src='M' (post-UNMANAGE
      // sidecar). Pre-fix the user UNMANAGEd a U row → became M
      // sidecar → button still read REVERT, which was inconsistent
      // with PURGE's RESTORE label even though both are
      // "destructive action just happened, bring my URL back".
      const isRestore = (srcLetter === '-' || srcLetter === 'M');
      const restoreLabel = isRestore ? 'RESTORE' : 'REVERT';
      // v1.12.103: tone tracks the captured kind, not a hardcoded
      // 'user'. Pre-fix the post-PURGE RESTORE on a T row landed
      // purple even though it was bringing back a themerrdb URL —
      // visually inconsistent with the green TDB chip the row
      // showed before the purge. Now: themerrdb-kind → green,
      // user-kind → purple, so the button color answers "where is
      // this URL coming from?" the same way the row's SRC badge does.
      const restoreTone = it.previous_youtube_kind === 'themerrdb'
        ? 'themerrdb' : 'user';
      const restoreTip = isRestore
        ? (restoreTone === 'themerrdb'
            ? 'Restore the ThemerrDB URL captured before PURGE/UNMANAGE and re-download in this section.'
            : 'Restore the user URL captured before PURGE/UNMANAGE and re-download in this section.')
        : 'Revert to the previously-active URL and re-download in this section.';
      sourceItems.push(menuItemHtml(
        'revert', restoreLabel,
        restoreTip,
        { mt: themeMt, id: themeId, sectionId: it.section_id, tone: restoreTone },
      ));
    }

    // PLACE menu — single-action category, but rendered as a menu for
    // visual symmetry with the others. Hidden entirely when there's
    // nothing to push.
    // v1.11.63: PUSH / RE-PUSH / RESTORE are mutually exclusive based
    // on the current canonical+placement state:
    //   downloaded && !placed → PUSH TO PLEX (canonical only, push it out)
    //   downloaded && placed && !dlBroken → RE-PUSH (force re-place)
    //   dlBroken && placed → RESTORE FROM PLEX (no canonical to push;
    //                        recover it from the surviving placement)
    // Pre-fix RE-PUSH stayed visible alongside RESTORE FROM PLEX even
    // though there's no canonical to re-push, and PUSH TO PLEX was
    // theoretically reachable on a row whose 'downloaded' flag was
    // truthy only because file_path was non-null (ignoring the
    // missing canonical). Now: if the canonical is gone, PUSH/RE-PUSH
    // are suppressed; if the canonical is present, RESTORE is suppressed.
    const placeItems = [];
    if (themed && downloaded && !placed && !dlBroken) {
      // v1.10.34: bypassLock=true so this stays clickable in the
      // 'downloaded but not placed' state (a.k.a. awaitingApproval).
      // Pushing IS the resolution action there — locking it forced
      // users to /pending unnecessarily.
      placeItems.push(menuItemHtml(
        'replace', 'PUSH TO PLEX',
        "Push the downloaded theme into the Plex folder.",
        { mt: themeMt, id: themeId, warn: true, bypassLock: true },
      ));
    }
    if (themed && downloaded && placed && !dlBroken && !isMismatch) {
      placeItems.push(menuItemHtml(
        'replace', 'RE-PUSH',
        "Re-place the canonical at the Plex folder (no re-download).",
        { mt: themeMt, id: themeId, warn: true, bypassLock: true },
      ));
    }
    // v1.11.99: mismatch state — canonical and placement diverged via
    // SET URL / UPLOAD MP3. Three resolution paths, surfaced together.
    // bypassLock so they all work even though the row is technically
    // "awaiting approval" (which would otherwise grey out the menu).
    if (themed && isMismatch && downloaded && placed) {
      placeItems.push(menuItemHtml(
        'replace', 'PUSH TO PLEX',
        "Overwrite the Plex-folder file with the new download.",
        { mt: themeMt, id: themeId, info: true, bypassLock: true },
      ));
      placeItems.push(menuItemHtml(
        'adopt-from-plex', 'ADOPT FROM PLEX',
        "Discard the new download and re-adopt the existing Plex-folder file.",
        { mt: themeMt, id: themeId, info: true, bypassLock: true },
      ));
      if (it.mismatch_state === 'pending') {
        placeItems.push(menuItemHtml(
          'keep-mismatch', 'KEEP MISMATCH',
          "Dismiss from /pending. Library row keeps DL=amber + LINK=M.",
          { mt: themeMt, id: themeId, bypassLock: true },
        ));
      }
    }
    if (themed && dlBroken && placed) {
      placeItems.push(menuItemHtml(
        'restore-canonical', 'RESTORE FROM PLEX',
        "Rebuild the canonical from the surviving Plex-folder file.",
        { mt: themeMt, id: themeId, warn: true, bypassLock: true },
      ));
    }

    // REMOVE menu
    const removeItems = [];
    // v1.12.37 (revised): CLEAR URL drops the captured PREVIOUS
    // URL (themes.previous_youtube_url) so REVERT becomes
    // unavailable for the row going forward. Useful when the
    // user is satisfied with the current state and wants to
    // "commit" — guards against accidental REVERTs and tidies
    // the INFO card. Only surfaces when there's actually a
    // previous URL to clear, mirroring the SOURCE-menu REVERT
    // gating (also has_previous_url-driven).
    if (it.has_previous_url) {
      // v1.12.79: tooltip mirrors the SOURCE-menu label flip — on
      // src='-' / src='M' rows the SOURCE button reads RESTORE
      // (post-PURGE / post-UNMANAGE recovery), elsewhere REVERT.
      // Same action, same wording in the menu copy keeps the two
      // sides consistent.
      const clearTip = (srcLetter === '-' || srcLetter === 'M')
        ? "Drop the captured previous URL — RESTORE will no longer be available."
        : "Drop the captured previous URL — REVERT will no longer be available.";
      removeItems.push(menuItemHtml(
        'clear-url', 'CLEAR URL',
        clearTip,
        { mt: themeMt, id: themeId, sectionId: it.section_id,
          danger: true },
      ));
    }
    if (placed) {
      // v1.12.77: section_id scopes DEL so only this row's
      // placement gets unlinked. Sibling editions keep playing.
      removeItems.push(menuItemHtml(
        'unplace', 'DEL',
        "Remove from this Plex folder. Canonical stays — PUSH TO PLEX restores.",
        { mt: themeMt, id: themeId, sectionId: it.section_id, danger: true },
      ));
    }
    if (placed && downloaded) {
      // v1.12.73: scope UNMANAGE to this row's section so sibling
      // sections (4K vs standard, anime vs plain) keep their motif
      // management. The endpoint detects last-section and only
      // drops the themes row + tracking metadata when nothing else
      // is managed for the title.
      removeItems.push(menuItemHtml(
        'unmanage', 'UNMANAGE',
        "Drop motif's tracking for this section and delete the canonical. Plex-folder file stays; row flips to M.",
        { mt: themeMt, id: themeId, sectionId: it.section_id, danger: true },
      ));
    }
    if (downloaded || isOrphan) {
      // v1.11.8: surface PURGE on the DL-only state too. After a DEL
      // (unplaced) the row is "downloaded but not placed" — pre-fix
      // the menu showed PUSH TO PLEX without a way to also drop the
      // canonical. PURGE was technically already added (downloaded ||
      // isOrphan covers !placed && downloaded) but the description
      // didn't make clear what happens to the recovery path.
      const purgeDesc = isOrphan
        ? 'Delete the orphan record and all files. Cannot be undone.'
        : (placed
            ? 'Delete the canonical, the Plex-folder file, and motif tracking.'
            : 'Delete the downloaded canonical. Re-acquire via DOWNLOAD TDB / SET URL / UPLOAD MP3 / ADOPT.');
      removeItems.push(menuItemHtml(
        'purge', '× PURGE',
        purgeDesc,
        { mt: themeMt, id: themeId, orphan: isOrphan,
          // v1.12.77: scope PURGE to this row's section so
          // sibling sections (4K vs standard, anime vs plain)
          // keep their files. Backend detects last-section and
          // only drops the themes row + tracking metadata when
          // nothing else is managed for the title.
          sectionId: it.section_id,
          danger: true,
          // v1.11.88: bypassLock so PURGE stays clickable when the row
          // is awaitingApproval (downloaded but not placed). PURGE *is*
          // the legitimate exit from that state — the prior lock left
          // the user with PUSH TO PLEX as the only enabled action,
          // forcing them to either accept the placement or leave the
          // canonical sitting there indefinitely. Real in-flight jobs
          // still disable the button (job_in_flight).
          bypassLock: true,
          dlOnly: !placed && downloaded ? '1' : '0',
          // v1.13.31: flag whether Plex serves its own theme so the
          // PURGE confirm can preview the post-action state. Composite
          // SRC rows (T+P, U+P, A+P, M+P) have a fallback; pure
          // letter rows (T/U/A/M) without Plex don't.
          plexAlso: _plexAlso },
      ));
    }

    function menuButtonHtml(label, items, kindClass) {
      // v1.12.25: absent buttons render nothing so present buttons
      // collapse to the right edge of the actions cell. Each
      // button keeps a consistent per-label min-width via CSS
      // (.row-menu-source / .row-menu-place / .row-menu-remove)
      // so the visible buttons stay at constant size regardless
      // of which siblings are present. v1.12.24's fixed-slot
      // approach reserved horizontal space for absent buttons,
      // which scattered the present buttons across the cell —
      // the opposite of the intended right-anchored layout.
      if (!items.length) return '';
      const labelClass = `row-menu-${label.toLowerCase()}`;
      const cls = `row-menu ${labelClass} ${kindClass || ''}`.trim();
      return `<details class="${cls}">`
        + `<summary class="btn btn-tiny" title="${htmlEscape(label)} actions">${htmlEscape(label)} ▾</summary>`
        + `<div class="row-menu-panel">${items.join('')}</div>`
        + `</details>`;
    }

    const acts = [];
    if (themed && themeId !== null && themeId !== undefined) {
      acts.push(`<button class="btn btn-tiny row-info-btn" data-act="info" data-mt="${themeMt}" data-id="${themeId}" data-section-id="${htmlEscape(it.section_id || '')}" title="ThemerrDB record details">ⓘ</button>`);
    }
    acts.push(menuButtonHtml('SOURCE', sourceItems));
    acts.push(menuButtonHtml('PLACE', placeItems));
    acts.push(menuButtonHtml('REMOVE', removeItems, 'row-menu-danger'));

    const actions = `<div class="row-actions">${acts.filter(Boolean).join('')}</div>`;

    const selKey = libKey(it);
    const selected = libraryState.selected.has(selKey);
    // v1.10.29: hover-tooltip on the title cell shows the Plex folder
    // path so duplicate rows (same title+year, different folders) can
    // be told apart at a glance.
    const titleTooltip = it.folder_path ? `Plex folder: ${it.folder_path}` : '';
    return `
      <tr${rowExtra}>
        <td class="col-state"><input type="checkbox" data-lib-select="${htmlEscape(selKey)}" ${selected ? 'checked' : ''} /></td>
        <td>
          <div class="title-cell" title="${htmlEscape(titleTooltip)}">
            ${titleGlyphs.join('')}
            <span class="title-cell-name">${htmlEscape(it.plex_title)}${editionLabel}${tdbAvailLabel}${sectionLabel}</span>
          </div>
        </td>
        <td class="col-year">${htmlEscape(it.year || '')}</td>
        <td class="col-state">${srcCell}</td>
        <td class="col-state"><span class="state-pill ${dl}${downloadInFlight ? ' state-pill-pending' : ''}" title="${htmlEscape(dlTip)}"></span></td>
        <td class="col-state"><span class="state-pill ${pl}${placeInFlight ? ' state-pill-pending' : ''}" title="${htmlEscape(plTip)}"></span></td>
        <td class="col-state">${linkCell}</td>
        <td class="col-imdb">${imdbLink}</td>
        <td class="col-actions">${actions}</td>
      </tr>
    `;
  }

  function renderLibraryRowNotInPlex(it) {
    // ThemerrDB row with no matching plex_items — informational only.
    // Title is in ThemerrDB but not in the user's Plex library, so
    // download/place don't make sense (no Plex folder to target). Show
    // a dim T badge so the source is unambiguous, and provide INFO +
    // IMDb link so the user can decide whether to add the title to
    // Plex manually. PURGE is offered if motif somehow already has a
    // local file for this row (legacy from earlier installs).
    const themeMt = it.theme_media_type;
    const themeId = it.theme_tmdb;
    const downloaded = !!it.file_path;
    const imdb = it.guid_imdb || '';
    const imdbLink = imdb
      ? `<a href="https://www.imdb.com/title/${htmlEscape(imdb)}" target="_blank" rel="noopener">${htmlEscape(imdb)}</a>`
      : '<span class="muted">—</span>';

    const acts = [
      `<button class="btn btn-tiny" data-act="info" data-mt="${themeMt}" data-id="${themeId}" data-section-id="${htmlEscape(it.section_id || '')}" title="ThemerrDB record details">ⓘ</button>`,
    ];
    if (downloaded) {
      acts.push(`<button class="btn btn-tiny btn-danger" data-act="purge" data-mt="${themeMt}" data-id="${themeId}" data-title="${htmlEscape(it.theme_title || it.plex_title)}" data-orphan="0" title="motif has a stale local file for a title you don't own in Plex — purge to remove">× PURGE</button>`);
    }

    const selKey = libKey(it);
    const selected = libraryState.selected.has(selKey);
    return `
      <tr class="row-not-in-plex">
        <td class="col-state"><input type="checkbox" data-lib-select="${htmlEscape(selKey)}" ${selected ? 'checked' : ''} /></td>
        <td>
          <div class="title-cell">
            <span class="title-cell-name muted">${htmlEscape(it.plex_title)}</span>
            <span class="muted small" style="margin-left:6px">(not in your Plex library)</span>
          </div>
        </td>
        <td class="col-year">${htmlEscape(it.year || '')}</td>
        <td class="col-state"><span class="link-badge link-badge-themerrdb-only" title="ThemerrDB-tracked title; not in your Plex library">T</span></td>
        <td class="col-state"><span class="state-pill" title="not applicable"></span></td>
        <td class="col-state"><span class="state-pill" title="not applicable"></span></td>
        <td class="col-state"><span class="link-glyph link-glyph-none">—</span></td>
        <td class="col-imdb">${imdbLink}</td>
        <td class="col-actions"><div class="row-actions">${acts.join('')}</div></td>
      </tr>
    `;
  }

  function adaptLibraryFourkToggle(ta) {
    // v1.11.34: when both variants exist the chips work as a toggle
    // and chip-active follows libraryState.fourk so a return visit
    // renders the active variant correctly. When only one variant
    // exists the chip becomes a non-clickable label (chip-active +
    // chip-label, disabled) so it reads as 'this tab is 4K' rather
    // than as a button waiting to be pressed. Pre-fix the visible
    // chip was a regular button that didn't reflect selected state
    // on revisit.
    const tabEl = document.getElementById('library-tab');
    if (!tabEl) return;
    const tab = tabEl.value;
    const av = ta && ta[tab];
    if (!av) return;
    const toggle = document.querySelector('.chips[aria-label="resolution"]');
    if (!toggle) return;
    const stdBtn = toggle.querySelector('[data-fourk="0"]');
    const fkBtn  = toggle.querySelector('[data-fourk="1"]');
    const showAny = av.standard || av.fourk;
    toggle.style.display = showAny ? '' : 'none';
    if (stdBtn) stdBtn.style.display = av.standard ? '' : 'none';
    if (fkBtn)  fkBtn.style.display  = av.fourk    ? '' : 'none';
    // Auto-flip libraryState.fourk when only one variant exists.
    // v1.12.9: persist the auto-flip outcome too so subsequent
    // visits (including page reloads) start from the right place
    // without waiting on /api/stats to land.
    const persistVariant = () => {
      try {
        localStorage.setItem(
          `motif:variant:${tab}`,
          libraryState.fourk ? 'fourk' : 'standard',
        );
      } catch (_) { /* private mode / quota — fine */ }
    };
    if (av.fourk && !av.standard && libraryState.fourk === false) {
      libraryState.fourk = true;
      persistVariant();
      loadLibrary().catch(()=>{});
    } else if (av.standard && !av.fourk && libraryState.fourk === true) {
      libraryState.fourk = false;
      persistVariant();
      loadLibrary().catch(()=>{});
    }
    // Sync chip-active + chip-label state. The 'sole variant' becomes
    // a label; when both exist the chips act as a toggle.
    const sole = (av.standard !== av.fourk);
    if (stdBtn) {
      const active = sole ? av.standard : !libraryState.fourk;
      stdBtn.classList.toggle('chip-active', active);
      stdBtn.classList.toggle('chip-label',  sole);
      stdBtn.disabled = sole;
    }
    if (fkBtn) {
      const active = sole ? av.fourk : libraryState.fourk;
      fkBtn.classList.toggle('chip-active', active);
      fkBtn.classList.toggle('chip-label',  sole);
      fkBtn.disabled = sole;
    }
  }

  // Rapid-poll mode: after the user kicks off a manual URL or upload,
  // poll loadLibrary every ~5s for up to 60s so the row reflects the
  // download → place transitions without waiting for the regular 30s
  // tick.
  //
  // v1.10.7: 'often times when on the tab within a library the page
  // will continue to do a quick reload which is disruptive'. Three
  // changes to keep polling out of the user's way:
  //   1. Bumped interval 3s → 5s.
  //   2. Skip the tick when the user is interacting (search input
  //      focused, an open <dialog>, or text is selected). The next
  //      tick still fires, so the row eventually catches up.
  //   3. Auto-stop early once no visible row has job_in_flight set
  //      (state is stable; nothing left to watch transition).
  let libraryRapidTimer = null;
  let libraryRapidUntil = 0;
  function libraryRapidPoll(durationMs = 60000) {
    libraryRapidUntil = Date.now() + durationMs;
    if (libraryRapidTimer) return;
    libraryRapidTimer = setInterval(async () => {
      if (Date.now() > libraryRapidUntil) {
        clearInterval(libraryRapidTimer);
        libraryRapidTimer = null;
        return;
      }
      // Skip ticks where the user is doing something — re-rendering the
      // whole tbody mid-interaction is the disruptive behavior.
      const ae = document.activeElement;
      const typingInSearch = ae && ae.id === 'library-search';
      const dialogOpen = !!document.querySelector('dialog[open]');
      const sel = window.getSelection && window.getSelection();
      const hasTextSelection = !!(sel && sel.toString().length > 0);
      if (typingInSearch || dialogOpen || hasTextSelection) return;

      try {
        await loadLibrary();
      } catch (_) { /* network blip — try again next tick */ }
      // Auto-stop: if none of the rendered rows have a job in flight,
      // there's nothing left to watch transition — drop polling so we
      // don't keep flickering the tbody for nothing.
      const stillBusy = (libraryState.items || []).some(it => !!it.job_in_flight);
      if (!stillBusy) {
        clearInterval(libraryRapidTimer);
        libraryRapidTimer = null;
      }
    }, 5000);
  }

  function updateLibrarySelectionUi() {
    const bar = document.getElementById('library-bulk-bar');
    const cnt = document.getElementById('library-selected-count');
    const all = document.getElementById('library-select-all');
    if (!bar || !cnt) return;
    const n = libraryState.selected.size;
    cnt.textContent = fmt.num(n);
    // v1.12.55: bulk bar also shows when the user is on the
    // tdb_pills=update click-through (topbar UPD badge target),
    // even with nothing selected — so the // ACCEPT ALL UPDATES /
    // KEEP ALL CURRENT actions are reachable without forcing a
    // SELECT ALL FILTERED first.
    const onUpdateFilter = libraryState.tdbPills.has('update');
    // v1.12.120: "tdb_pills=update WITHOUT other filters" gate.
    // ACCEPT ALL UPDATES is global by design (server-side it walks
    // every eligible per-section pending update). If the user has
    // narrowed the view with status / src / dl / pl / link /
    // edition / search filters, the visible page is a subset and
    // // ACCEPT ALL would silently fan out beyond it. So the
    // bulk-no-selection path is gated to "update pill is the
    // ONLY active filter".
    const noOtherFilters = (
      libraryState.status === 'all'
      && (!libraryState.tdb || libraryState.tdb === 'any')
      && (libraryState.srcFilter?.size || 0) === 0
      && (libraryState.dlPills?.size  || 0) === 0
      && (libraryState.plPills?.size  || 0) === 0
      && (libraryState.linkPills?.size|| 0) === 0
      && (libraryState.edPills?.size  || 0) === 0
      && libraryState.tdbPills.size === 1   // exactly {'update'}
      && !(libraryState.q && libraryState.q.trim())
    );
    // v1.12.101: count rows on the current page that are actually
    // actionable as a pending update (pending_update flag set, src
    // is not '-' since we suppress update treatment on plex-orphans
    // — mirrors the row-level gate at computePillState). When the
    // user is on the update filter but no rows on this page have a
    // live pending_update, the ACCEPT ALL / KEEP ALL buttons act on
    // a global state that doesn't reflect what's visible — clicking
    // ACCEPT ALL "does nothing" from the user's POV. Hide the
    // banner entirely in that case.
    const visiblePendingUpdates = (libraryState.items || []).filter(
      (it) => it.pending_update && computeSrcLetter(it) !== '-',
    ).length;
    // v1.12.120: also expose ACCEPT/KEEP ALL when items are
    // selected and at least one carries a pending_update — the
    // click handler scopes the action to just the eligible
    // selection, ignoring the rest. Pre-fix the buttons were
    // hidden whenever the user had any other filter active +
    // selection, so the only way to bulk-accept a hand-picked
    // set was to first strip filters (lost intent).
    const selectedEligibleUpdates = (libraryState.items || []).filter(
      (it) => libraryState.selected.has(libKey(it))
              && it.pending_update
              && computeSrcLetter(it) !== '-',
    ).length;
    const showBarForUpdates = (
      (onUpdateFilter && noOtherFilters && visiblePendingUpdates > 0)
      || selectedEligibleUpdates > 0
    );
    bar.style.display = (n > 0 || showBarForUpdates) ? '' : 'none';
    // When nothing is selected but the bar is showing because of
    // the update filter, hide the "N selected" prefix entirely and
    // show "viewing pending updates" instead — bulk actions explain
    // their own scope without needing a row count.
    const detail = document.getElementById('library-bulk-detail');
    const countWrap = document.getElementById('library-bulk-count-wrap');
    if (showBarForUpdates && n === 0) {
      if (countWrap) countWrap.style.display = 'none';
      if (detail) detail.textContent = `${visiblePendingUpdates} pending update${visiblePendingUpdates !== 1 ? 's' : ''} · bulk actions below`;
    } else {
      if (countWrap) countWrap.style.display = '';
      if (detail) detail.textContent = '';
    }
    // v1.12.5: tri-state header checkbox — checked when every visible
    // row is selected, indeterminate when some are, unchecked when
    // none. Lets the user see at a glance whether clicking the
    // header will select-all or deselect-all on the current page.
    if (all) {
      const rowBoxes = document.querySelectorAll('#library-body input[data-lib-select]');
      const visibleCount = rowBoxes.length;
      const visibleSelected = Array.from(rowBoxes).filter((cb) => cb.checked).length;
      if (visibleCount === 0 || visibleSelected === 0) {
        all.checked = false;
        all.indeterminate = false;
      } else if (visibleSelected === visibleCount) {
        all.checked = true;
        all.indeterminate = false;
      } else {
        all.checked = false;
        all.indeterminate = true;
      }
    }
    const ackBtn = document.getElementById('library-ack-selected-btn');
    const dlBtn = document.getElementById('library-download-selected-btn');
    const adoptBtn = document.getElementById('library-adopt-selected-btn');
    // v1.11.28: walk the current selection and decide which bulk
    // actions are meaningful. M-only sidecars can only be ADOPTed
    // (no themes record to download from); TDB-tracked rows can
    // become T via // TDB SELECTED. Mixed selections show both.
    let hasSidecarOnly = false;
    let hasTdbEligible = false;
    // v1.12.60: track whether any selected row would have its
    // existing theme replaced by a bulk DOWNLOAD-FROM-TDB action.
    // Pure-'-' selections download cleanly; selections that mix in
    // U/A/M/P (anything with a current theme other than T) get an
    // adjusted button label so the user knows they're about to
    // overwrite content, not just fetch.
    let hasReplaceTarget = false;
    // v1.13.33: count rows in the selection that are downloaded
    // but awaiting placement. Mirrors the per-row awaitingApproval
    // gate (line ~4291): file_path set, no media_folder, no
    // job_in_flight. PUSH TO PLEX bulk action only renders when at
    // least one such row is selected.
    let pushableCount = 0;
    if (n > 0) {
      const selectedKeys = libraryState.selected;
      for (const it of (libraryState.items || [])) {
        const key = libKey(it);
        if (!selectedKeys.has(key)) continue;
        const placed = !!it.media_folder;
        const sidecarOnly = !placed && !!it.plex_local_theme;
        const themed = (it.theme_media_type
                        && it.theme_tmdb !== null
                        && it.theme_tmdb !== undefined
                        && it.upstream_source !== 'plex_orphan');
        if (sidecarOnly) hasSidecarOnly = true;
        if (themed) hasTdbEligible = true;
        // v1.12.60: any T row is "already TDB-sourced" so the
        // download is a refresh, not a replace — same for '-'
        // (no theme to replace). Anything else (U/A/M/P) IS a
        // replace target.
        const srcLetter = computeSrcLetter(it);
        if (srcLetter !== 'T' && srcLetter !== '-') {
          hasReplaceTarget = true;
        }
        // v1.13.33: pushable = downloaded canonical exists, no
        // placement, not currently in flight. Excludes orphans
        // (theme_tmdb null) since the /replace endpoint won't
        // resolve them.
        const awaitingApproval = !it.job_in_flight
                              && !!it.file_path
                              && !it.media_folder;
        if (awaitingApproval && themed) pushableCount++;
      }
    }
    // v1.12.6: TDB-only browse hides DOWNLOAD-FROM-TDB / ADOPT —
    // the rows aren't in your Plex library so neither action makes
    // sense. CSV export becomes the primary action there. EXPORT
    // CSV stays visible regardless of mode (it's a pure read-out).
    const onTdbOnly = libraryState.status === 'not_in_plex';
    // v1.12.11: ACK SELECTED is now scoped to the red TDB ✗ pill
    // filter — that's the canonical "I'm reviewing failures" view
    // post-removal of the FAILURES status chip. Outside that filter
    // ACK is hidden (still reachable per-row via ACK FAILURE in the
    // SOURCE menu).
    const onTdbDead = libraryState.tdbPills.has('dead');
    if (ackBtn) ackBtn.style.display = onTdbDead ? '' : 'none';
    // v1.12.101: hide DOWNLOAD-FROM-TDB on the update filter. The
    // update filter is the ACCEPT/KEEP workflow surface; bulk
    // download competes with that by silently overwriting the
    // pending update with a fresh fetch (which then triggers the
    // accept path anyway). Removing it from this filter keeps the
    // intent clear: choose for each row, then accept-all or keep-all.
    if (dlBtn)    dlBtn.style.display    = (!onTdbOnly && !onUpdateFilter && hasTdbEligible) ? '' : 'none';
    if (adoptBtn) adoptBtn.style.display = (!onTdbOnly && !onUpdateFilter && hasSidecarOnly) ? '' : 'none';
    // v1.13.33: PUSH TO PLEX bulk button. Visible when the selection
    // contains at least one downloaded-but-not-placed row. Gated off
    // on the TDB-only browse and the update-filter view since
    // neither flow targets an existing canonical.
    const pushBtn = document.getElementById('library-push-selected-btn');
    if (pushBtn) {
      pushBtn.style.display = (!onTdbOnly && !onUpdateFilter && pushableCount > 0) ? '' : 'none';
      if (pushableCount > 0) {
        pushBtn.textContent = pushableCount === n
          ? '// PUSH TO PLEX'
          : `// PUSH ${pushableCount} TO PLEX`;
      }
    }
    // v1.12.60: relabel DOWNLOAD-FROM-TDB based on selection mix so
    // the user reads accurate intent. Pure '-' (or T) selections
    // are clean fetches; mixing in U/A/M/P means existing themes
    // will be overwritten — say so in both the label and tooltip.
    if (dlBtn) {
      if (hasReplaceTarget) {
        dlBtn.textContent = '// DOWNLOAD & REPLACE FROM TDB';
        dlBtn.title = 'Download from ThemerrDB. Selected rows with existing themes (U / A / M / P) will be overwritten with the TDB version.';
      } else {
        dlBtn.textContent = '// DOWNLOAD FROM TDB';
        dlBtn.title = 'Download and place each selected row from ThemerrDB.';
      }
    }
    const exportBtn = document.getElementById('library-export-csv-btn');
    if (exportBtn) exportBtn.style.display = '';
    // v1.12.55: bulk update actions visible only when the user is
    // on the blue-↑-pill click-through (the topbar UPD badge
    // target). Outside that filter the actions don't fit the
    // mental model — ACCEPT ALL UPDATES would silently fan out
    // beyond the rows the user is looking at.
    // v1.12.101: also gate on visiblePendingUpdates so the buttons
    // disappear when the filter is up but no rows actually have a
    // pending update (e.g., user already accepted/declined them).
    const acceptAllBtn = document.getElementById('library-accept-all-updates-btn');
    const declineAllBtn = document.getElementById('library-decline-all-updates-btn');
    if (acceptAllBtn) acceptAllBtn.style.display = showBarForUpdates ? '' : 'none';
    if (declineAllBtn) declineAllBtn.style.display = showBarForUpdates ? '' : 'none';
    // v1.12.101: keep // EXPORT CSV anchored as the rightmost
    // action regardless of which other buttons render. Without
    // this the bar renders in DOM order (DOWNLOAD / ADOPT / EXPORT
    // / ACK / ACCEPT ALL / KEEP ALL), so the dynamic accept/keep
    // pair pushes EXPORT into the middle on the update filter.
    // Re-append ensures it always sits at the end of the flex row.
    if (exportBtn && exportBtn.parentNode) {
      exportBtn.parentNode.appendChild(exportBtn);
    }
  }

  function bindLibrary() {
    const tabEl = document.getElementById('library-tab');
    if (!tabEl) return;

    // v1.10.50: pre-load library state from URL query params so a
    // deep-link like /movies?status=failures (the topbar failure
    // badge target) lands with the filter already active. Falls
    // through to the chip-active states for the matching value.
    try {
      const sp = new URLSearchParams(window.location.search);
      const wantStatus = sp.get('status');
      if (wantStatus) {
        const valid = new Set(['all','has_theme','themed','manual','plex_agent','untracked',
                               'downloaded','placed','unplaced','failures',
                               'not_in_plex']);
        if (valid.has(wantStatus)) {
          libraryState.status = wantStatus;
          document.querySelectorAll('[data-status]').forEach((x) =>
            x.classList.toggle('chip-active', x.dataset.status === wantStatus));
          // v1.12.6: faded-T TDB-only pill mirrors not_in_plex mode.
          // Sync its active class on initial load so deep-links land
          // with the right indicator.
          const tdbOnlyBtn = document.querySelector('[data-tdb-only]');
          if (tdbOnlyBtn) {
            tdbOnlyBtn.classList.toggle(
              'src-key-btn-active',
              wantStatus === 'not_in_plex',
            );
          }
        }
      }
      const wantTdb = sp.get('tdb');
      if (wantTdb && ['any','tracked','untracked'].includes(wantTdb)) {
        libraryState.tdb = wantTdb;
        document.querySelectorAll('[data-tdb]').forEach((x) =>
          x.classList.toggle('chip-active', x.dataset.tdb === wantTdb));
      }
      // v1.12.9 / v1.12.23: deep-link to any of the pill multi-
      // selects via ?tdb_pills= / ?src_pills= / ?dl_pills= /
      // ?pl_pills= / ?link_pills= (comma-separated). The failures
      // badge uses ?tdb_pills=dead; future surfaces (e.g. dashboard
      // shortcuts) can deep-link any combination.
      const PILL_DEEP_LINKS = [
        { param: 'tdb_pills',  state: 'tdbPills',  attr: 'tdbPill',
          activeClass: 'tdb-pill-btn-active',
          values: new Set(['tdb','update','cookies','dead','none']) },
        { param: 'src_pills',  state: 'srcFilter', attr: 'srcFilter',
          activeClass: 'src-key-btn-active',
          values: new Set(['T','U','A','M','P','-']) },
        { param: 'dl_pills',   state: 'dlPills',   attr: 'dlPill',
          activeClass: 'state-pill-btn-active',
          values: new Set(['on','off','broken']) },
        { param: 'pl_pills',   state: 'plPills',   attr: 'plPill',
          activeClass: 'state-pill-btn-active',
          values: new Set(['on','await','off','broken']) },
        { param: 'link_pills', state: 'linkPills', attr: 'linkPill',
          activeClass: 'link-pill-btn-active',
          values: new Set(['hl','c','m','none']) },
        { param: 'ed_pills',   state: 'edPills',   attr: 'edPill',
          activeClass: 'state-pill-btn-active',
          values: new Set(['has','none']) },
      ];
      for (const dl of PILL_DEEP_LINKS) {
        const raw = sp.get(dl.param);
        if (!raw) continue;
        raw.split(',').map((s) => s.trim()).forEach((p) => {
          if (dl.values.has(p)) libraryState[dl.state].add(p);
        });
        const kebab = dl.attr.replace(/[A-Z]/g, (c) => '-' + c.toLowerCase());
        document.querySelectorAll(`[data-${kebab}]`).forEach((x) => {
          const xVal = x.dataset[dl.attr];
          const active = !!xVal && libraryState[dl.state].has(xVal);
          x.classList.toggle(dl.activeClass, active);
        });
      }
      if (sp.get('fourk') === 'true' || sp.get('fourk') === '1') {
        libraryState.fourk = true;
        document.querySelectorAll('.chips [data-fourk]').forEach((x) =>
          x.classList.toggle('chip-active', x.dataset.fourk === '1'));
      } else {
        // v1.12.9: no explicit ?fourk= override → fall back to the
        // last variant the user picked on THIS tab. Persisted per-
        // tab so movies/tv/anime each remember independently.
        // adaptLibraryFourkToggle still wins when only one variant
        // is enabled in settings (the auto-flip preserves the
        // "only-4K-shows" case).
        try {
          const tabKey = (document.getElementById('library-tab') || {}).value;
          if (tabKey) {
            const saved = localStorage.getItem(`motif:variant:${tabKey}`);
            if (saved === 'fourk') {
              libraryState.fourk = true;
              document.querySelectorAll('.chips [data-fourk]').forEach((x) =>
                x.classList.toggle('chip-active', x.dataset.fourk === '1'));
            }
          }
        } catch (_) { /* private mode / quota — fine */ }
      }
    } catch (_) { /* URLSearchParams not supported — skip */ }

    // v1.13.13: if the URL carried no filter params, hydrate from
    // the localStorage snapshot (last loadLibrary() call). Lets a
    // user filter MOVIES then click TV SHOWS without starting over.
    // URL deep-links still win — only fires when sp has none of the
    // recognized filter keys.
    try {
      const sp2 = new URLSearchParams(window.location.search);
      const hasFilterParam = _LIB_FILTER_URL_KEYS.some((k) => sp2.has(k));
      if (!hasFilterParam) _hydrateLibraryFromStorage();
    } catch (_) { /* fine */ }

    // 4K toggle
    document.querySelectorAll('.chips [data-fourk]').forEach((b) => {
      b.addEventListener('click', () => {
        document.querySelectorAll('.chips [data-fourk]').forEach((x) =>
          x.classList.remove('chip-active'));
        b.classList.add('chip-active');
        libraryState.fourk = b.dataset.fourk === '1';
        // v1.12.9: persist per-tab variant choice. Next visit to this
        // tab restores the same variant rather than defaulting back
        // to standard. Cleared on logout naturally (localStorage).
        try {
          const tabKey = (document.getElementById('library-tab') || {}).value;
          if (tabKey) {
            localStorage.setItem(
              `motif:variant:${tabKey}`,
              libraryState.fourk ? 'fourk' : 'standard',
            );
          }
        } catch (_) { /* private mode / quota — fine */ }
        libraryState.page = 1;
        loadLibrary().catch(console.error);
        // v1.11.84: REFRESH FROM PLEX is locked per-variant (standard
        // vs 4K). Toggling the chip changes the variant the button
        // targets, so re-evaluate the lock state immediately rather
        // than waiting for the next periodic poll — otherwise a 4K
        // section sits visually disabled while its standard sibling
        // is the one actually enumerating.
        refreshTopbarStatus();
      });
    });

    // Status filter chips (scoped to library section to avoid collision
    // with browse.html's status chips)
    document.querySelectorAll('#library-body, #library-pager').length &&
      document.querySelectorAll('[data-status]').forEach((b) => {
        // Only bind on the library page (skip if no library tbody)
        if (!document.getElementById('library-body')) return;
        b.addEventListener('click', () => {
          document.querySelectorAll('[data-status]').forEach((x) =>
            x.classList.remove('chip-active'));
          b.classList.add('chip-active');
          libraryState.status = b.dataset.status;
          libraryState.page = 1;
          updateTdbFilterVisibility();
          // v1.12.6: top-bar status chip click implicitly exits
          // THEMERRDB-only mode (the faded T pill in the SRC row),
          // since the user picked a non-tdb-only status chip.
          const tdbOnlyBtn = document.querySelector('[data-tdb-only]');
          if (tdbOnlyBtn) {
            tdbOnlyBtn.classList.toggle(
              'src-key-btn-active',
              libraryState.status === 'not_in_plex',
            );
          }
          // v1.10.51: bulk-bar action buttons swap on filter change
          // (e.g. FAILURES → ACK SELECTED only).
          updateLibrarySelectionUi();
          loadLibrary().catch(console.error);
        });
      });

    // v1.12.7: TDB pill multi-select filter. Replaces the v1.10.20
     // 3-state TDB MATCH chips. Same toggle pattern as the SRC pill
     // row — empty data-tdb-pill = CLEAR, data-tdb-pill-all = ALL,
     // a state name (tdb / update / cookies / dead / none) toggles
     // membership in the libraryState.tdbPills set. Pure client-side
     // (visible-page filter) like SRC.
    if (document.getElementById('library-body')) {
      // v1.12.23: factor the per-axis pill click handlers so TDB,
      // DL, PL, LINK all share one implementation. The axis is
      // identified by the data-* attribute name; each maps to a
      // libraryState.<axis>Pills Set + the CSS active-class
      // suffix. SRC uses its own block below because the row
      // markup is different (link-badge buttons, an extra ALL/
      // CLEAR clickable pair).
      const pillAxes = [
        { attr: 'tdbPill', allAttr: 'tdbPillAll', state: 'tdbPills',
          activeClass: 'tdb-pill-btn-active',
          values: ['tdb', 'update', 'cookies', 'dead', 'none'] },
        { attr: 'dlPill', allAttr: 'dlPillAll', state: 'dlPills',
          activeClass: 'state-pill-btn-active',
          values: ['on', 'off', 'broken'] },
        { attr: 'plPill', allAttr: 'plPillAll', state: 'plPills',
          activeClass: 'state-pill-btn-active',
          values: ['on', 'await', 'off'] },
        { attr: 'linkPill', allAttr: 'linkPillAll', state: 'linkPills',
          activeClass: 'link-pill-btn-active',
          values: ['hl', 'c', 'm', 'none'] },
        { attr: 'edPill', allAttr: 'edPillAll', state: 'edPills',
          activeClass: 'state-pill-btn-active',
          values: ['has', 'none'] },
      ];
      for (const axis of pillAxes) {
        const dataKey = axis.attr;       // camelCase for dataset
        const allKey = axis.allAttr;
        // Convert camelCase → kebab-case for querySelector:
        const kebab = dataKey.replace(/[A-Z]/g, (c) => '-' + c.toLowerCase());
        const allKebab = allKey.replace(/[A-Z]/g, (c) => '-' + c.toLowerCase());
        const sel = `[data-${kebab}], [data-${allKebab}]`;
        document.querySelectorAll(sel).forEach((b) => {
          b.addEventListener('click', () => {
            const set = libraryState[axis.state];
            if (b.dataset[allKey]) {
              axis.values.forEach((v) => set.add(v));
            } else {
              const want = b.dataset[dataKey];
              if (!want) {
                set.clear();
              } else if (set.has(want)) {
                set.delete(want);
              } else {
                set.add(want);
              }
            }
            document.querySelectorAll(`[data-${kebab}]`).forEach((x) => {
              const xVal = x.dataset[dataKey];
              const active = !!xVal && set.has(xVal);
              x.classList.toggle(axis.activeClass, active);
            });
            libraryState.page = 1;
            loadLibrary().catch(console.error);
          });
        });
      }
    }

    // v1.11.66: SRC legend buttons toggle a client-side SRC-letter
    // filter on top of whatever status / TDB chips are already
    // active. Clicking the active letter again or hitting CLEAR
    // resets the filter. Pure client-side: pagination/total still
    // reflect the underlying status+tdb pass.
    if (document.getElementById('library-body')) {
      const allLetters = ['T', 'U', 'A', 'M', 'P', '-'];
      // v1.12.6: include data-tdb-only in the bind so the faded T
      // pill in the SRC row gets a click handler. It's a mode switch
      // (status='not_in_plex') rather than an additive filter, so
      // it short-circuits the per-letter toggle path.
      document.querySelectorAll('[data-src-filter], [data-src-filter-all], [data-tdb-only]').forEach((b) => {
        b.addEventListener('click', () => {
          if (b.dataset.tdbOnly) {
            // Toggle the THEMERRDB-ONLY browse mode. When activating,
            // wipe the SRC letter set (per-row source isn't meaningful
            // in TDB-only rows) and flip status. When deactivating,
            // return to status='all'.
            const goingOn = libraryState.status !== 'not_in_plex';
            if (goingOn) {
              libraryState.srcFilter.clear();
              libraryState.status = 'not_in_plex';
            } else {
              libraryState.status = 'all';
            }
            libraryState.page = 1;
            // Sync status-chip visual state (so the top filter bar
            // visually de-activates / reactivates ALL).
            document.querySelectorAll('[data-status]').forEach((x) =>
              x.classList.toggle('chip-active', x.dataset.status === libraryState.status));
            // Repaint SRC legend — every letter inactive when
            // TDB-only is on; the TDB pill itself is the only active.
            document.querySelectorAll('[data-src-filter]').forEach((x) => {
              const xVal = x.dataset.srcFilter;
              const active = !!xVal && libraryState.srcFilter.has(xVal);
              x.classList.toggle('src-key-btn-active', active);
            });
            b.classList.toggle('src-key-btn-active', goingOn);
            loadLibrary().catch(console.error);
            return;
          }
          // v1.11.89: multi-select. Empty data-src-filter = CLEAR
          // (drop all). data-src-filter-all = ALL (add every letter).
          // A letter = toggle membership in the set.
          // v1.11.90: ALL button added opposite CLEAR for the
          // inverse-filter pattern ("light them all up, then
          // deselect the ones I want excluded").
          if (b.dataset.srcFilterAll) {
            allLetters.forEach((l) => libraryState.srcFilter.add(l));
          } else {
            const want = b.dataset.srcFilter;
            if (!want) {
              libraryState.srcFilter.clear();
            } else if (libraryState.srcFilter.has(want)) {
              libraryState.srcFilter.delete(want);
            } else {
              libraryState.srcFilter.add(want);
            }
          }
          // v1.12.6: any letter / ALL / CLEAR click also implicitly
          // exits THEMERRDB-only mode if it was active — they're
          // mutually exclusive (can't filter not-in-Plex rows by
          // per-row SRC).
          if (libraryState.status === 'not_in_plex') {
            libraryState.status = 'all';
            document.querySelectorAll('[data-status]').forEach((x) =>
              x.classList.toggle('chip-active', x.dataset.status === 'all'));
          }
          // Repaint active styling on the legend — every letter in
          // the set lights up. CLEAR / ALL aren't "active" states;
          // they're actions. The faded-T TDB-only pill mirrors the
          // status mode rather than the SRC set.
          document.querySelectorAll('[data-src-filter]').forEach((x) => {
            const xVal = x.dataset.srcFilter;
            const active = !!xVal && libraryState.srcFilter.has(xVal);
            x.classList.toggle('src-key-btn-active', active);
          });
          const tdbOnlyBtn = document.querySelector('[data-tdb-only]');
          if (tdbOnlyBtn) {
            tdbOnlyBtn.classList.toggle(
              'src-key-btn-active',
              libraryState.status === 'not_in_plex',
            );
          }
          libraryState.page = 1;
          loadLibrary().catch(console.error);
        });
      });
    }

    // Library tab chips (data-libtab) — only used on /coverage
    document.querySelectorAll('[data-libtab]').forEach((b) => {
      b.addEventListener('click', () => {
        document.querySelectorAll('[data-libtab]').forEach((x) =>
          x.classList.remove('chip-active'));
        b.classList.add('chip-active');
        libraryState.tab = b.dataset.libtab;
        const tabEl = document.getElementById('library-tab');
        if (tabEl) tabEl.value = libraryState.tab;
        libraryState.page = 1;
        loadLibrary().catch(console.error);
      });
    });

    // Search debounce
    const search = document.getElementById('library-search');
    let dt;
    // v1.13.12: ✕ clear button. Toggle visibility by input emptiness;
    // click fires the same load path as typing-and-deleting would.
    const clearBtn = document.getElementById('library-search-clear');
    function _syncClearVisibility() {
      if (!clearBtn || !search) return;
      clearBtn.style.display = search.value ? '' : 'none';
    }
    search?.addEventListener('input', () => {
      _syncClearVisibility();
      clearTimeout(dt);
      dt = setTimeout(() => {
        libraryState.q = search.value.trim();
        libraryState.page = 1;
        loadLibrary().catch(console.error);
      }, 250);
    });
    if (clearBtn) {
      clearBtn.addEventListener('click', () => {
        if (!search) return;
        search.value = '';
        _syncClearVisibility();
        libraryState.q = '';
        libraryState.page = 1;
        search.focus();
        loadLibrary().catch(console.error);
      });
    }
    _syncClearVisibility();

    // v1.13.13: CLEAR ALL — wipe every filter axis, search, and sort
    // back to defaults; clear the cross-tab persistence snapshot too
    // so the next library tab the user visits also lands clean. Saved
    // presets are explicitly NOT touched (selecting a preset is the
    // path to re-apply it, not lose it).
    document.getElementById('library-clear-all')?.addEventListener('click', () => {
      libraryState.q = '';
      libraryState.status = 'all';
      libraryState.tdb = 'any';
      libraryState.srcFilter.clear();
      libraryState.tdbPills.clear();
      libraryState.dlPills.clear();
      libraryState.plPills.clear();
      libraryState.linkPills.clear();
      libraryState.edPills.clear();
      libraryState.sort = 'title';
      libraryState.sortDir = 'asc';
      libraryState.page = 1;
      const search = document.getElementById('library-search');
      if (search) search.value = '';
      const clearBtn = document.getElementById('library-search-clear');
      if (clearBtn) clearBtn.style.display = 'none';
      // Reset every visual chip / pill back to its default.
      document.querySelectorAll('[data-status]').forEach((x) =>
        x.classList.toggle('chip-active', x.dataset.status === 'all'));
      document.querySelectorAll('[data-tdb]').forEach((x) =>
        x.classList.toggle('chip-active', x.dataset.tdb === 'any'));
      [['src-key-btn-active', 'src-key-btn'],
       ['tdb-pill-btn-active', 'tdb-pill-btn'],
       ['state-pill-btn-active', 'state-pill-btn'],
       ['link-pill-btn-active', 'link-pill-btn']].forEach(([cls]) => {
        document.querySelectorAll('.' + cls).forEach((x) => x.classList.remove(cls));
      });
      _clearLibraryFilterStorage();
      // Also strip URL filter keys so a refresh + share-link land
      // clean rather than re-applying whatever the URL still carries.
      try {
        const url = new URL(window.location.href);
        _LIB_FILTER_URL_KEYS.forEach((k) => url.searchParams.delete(k));
        window.history.replaceState(null, '', url.pathname + url.search + url.hash);
      } catch (_) { /* fine */ }
      loadLibrary().catch(console.error);
    });

    // Refresh — v1.10.6: send {tab, fourk} so the backend only enumerates
    // sections backing the current tab variant. While the enum runs the
    // button is left in a 'REFRESHING…' state; refreshTopbarStatus's
    // plex_enum_in_flight signal flips it back when the worker finishes.
    document.getElementById('library-refresh-btn')?.addEventListener('click', async (e) => {
      const btn = e.currentTarget;
      btn.disabled = true;
      if (!btn.dataset.origLabel) btn.dataset.origLabel = btn.textContent;
      btn.textContent = '// SYNCING…';
      // v1.13.19: optimistic topbar pill so the click → busy
      // transition is instant. Tone 'plex' tints the placeholder
      // green to match the real plex_enum op when it lands.
      // v1.13.21 (was v1.13.20): the optimistic label reads the
      // section name ("// SYNCING 4K MOVIES") via libraryRefreshLabel
      // instead of the generic "// SCANNING PLEX". The real
      // plex_enum op when it lands carries the same section name in
      // its stage_label, so there's no jank when the placeholder is
      // replaced — same string, same tone, no flicker.
      try {
        if (window.motifOps && window.motifOps.setOptimisticPlaceholder) {
          const label = (typeof libraryRefreshLabel === 'function')
            ? libraryRefreshLabel()
            : 'PLEX';
          window.motifOps.setOptimisticPlaceholder(
            'plex_enum', `// SYNCING ${label}`,
          );
        }
      } catch (_) {}
      try {
        await api('POST', '/api/library/refresh', {
          tab: libraryState.tab,
          fourk: !!libraryState.fourk,
        });
        paintTopbarSyncing(`SYNCING ${libraryRefreshLabel()}`);
        setTimeout(() => loadLibrary().catch(()=>{}), 5000);
        setTimeout(() => loadLibrary().catch(()=>{}), 15000);
      } catch (err) {
        alert('Refresh failed: ' + err.message);
        btn.disabled = false;
        btn.textContent = btn.dataset.origLabel || '// SYNC PLEX';
      }
      // Don't re-enable here — refreshTopbarStatus owns the lock based on
      // plex_enum_in_flight. The button restores once the worker drains.
    });

    // Pager
    document.getElementById('library-pager')?.addEventListener('click', (e) => {
      const b = e.target.closest('button[data-lib-page]');
      if (!b || b.disabled) return;
      libraryState.page = Number(b.dataset.libPage);
      loadLibrary().catch(console.error);
    });

    // v1.12.68: // NEEDS WORK chip toggles the attention sort.
    // Stores the prior (column) sort on first activation so a
    // second click can restore it cleanly. Title-asc is the
    // fallback when no prior sort is recorded.
    document.getElementById('library-sort-attention-btn')?.addEventListener('click', () => {
      if (libraryState.sort === 'attention') {
        // Toggle off — restore the prior sort or default to title
        const prior = libraryState._attentionPriorSort
                   || { sort: 'title', sortDir: 'asc' };
        libraryState.sort = prior.sort;
        libraryState.sortDir = prior.sortDir;
        libraryState._attentionPriorSort = null;
      } else {
        // Toggle on — remember the current sort to restore later
        libraryState._attentionPriorSort = {
          sort: libraryState.sort,
          sortDir: libraryState.sortDir,
        };
        libraryState.sort = 'attention';
        libraryState.sortDir = 'asc';  // ascending = highest priority first
      }
      libraryState.page = 1;
      updateSortIndicators();
      loadLibrary().catch(console.error);
    });

    // v1.10.15: column sort. Click a th[data-sort] to sort. Re-clicking
    // the active column toggles asc → desc → asc. Switching columns
    // resets to asc. The active column shows a ▲/▼ indicator.
    document.querySelectorAll('th.col-sort').forEach((th) => {
      th.addEventListener('click', () => {
        const key = th.dataset.sort;
        if (!key) return;
        // v1.12.68: clicking a column header while attention sort is
        // active clears the "prior sort" memory — the user has
        // explicitly picked a new column, so the // NEEDS WORK
        // toggle no longer needs to restore an older value.
        if (libraryState.sort === 'attention') {
          libraryState._attentionPriorSort = null;
        }
        if (libraryState.sort === key) {
          libraryState.sortDir = libraryState.sortDir === 'asc' ? 'desc' : 'asc';
        } else {
          libraryState.sort = key;
          libraryState.sortDir = 'asc';
        }
        libraryState.page = 1;  // reset to first page on sort change
        updateSortIndicators();
        loadLibrary().catch(console.error);
      });
    });
    updateSortIndicators();

    // (v1.10.10) // DOWNLOAD ALL handler removed alongside the
    // missing-themes banner; /api/library/download-missing endpoint is
    // still available for scripted callers but no UI binds to it.

    // Per-row select checkbox toggle
    document.getElementById('library-body')?.addEventListener('change', (e) => {
      const cb = e.target.closest('input[data-lib-select]');
      if (!cb) return;
      const k = cb.dataset.libSelect;
      if (cb.checked) libraryState.selected.add(k);
      else libraryState.selected.delete(k);
      updateLibrarySelectionUi();
    });

    // Select-all / deselect-all on the visible page.
    // v1.12.5: tri-state behavior — if every visible row is already
    // selected, click DEselects them; otherwise SELECT all visible.
    // Uses the click handler (instead of change) so we can override
    // the checkbox's own state machine when the page already has a
    // mix; the indeterminate visual is set by updateLibrarySelectionUi.
    document.getElementById('library-select-all')?.addEventListener('click', (e) => {
      e.preventDefault();
      const rowBoxes = document.querySelectorAll('#library-body input[data-lib-select]');
      if (rowBoxes.length === 0) return;
      const allSelected = Array.from(rowBoxes).every((cb) => cb.checked);
      const turnOn = !allSelected;
      rowBoxes.forEach((cb) => {
        cb.checked = turnOn;
        const k = cb.dataset.libSelect;
        if (turnOn) libraryState.selected.add(k);
        else libraryState.selected.delete(k);
      });
      e.currentTarget.checked = turnOn;
      e.currentTarget.indeterminate = false;
      updateLibrarySelectionUi();
    });

    // Bulk-bar buttons
    document.getElementById('library-clear-selection-btn')?.addEventListener('click', () => {
      libraryState.selected.clear();
      document.querySelectorAll('#library-body input[data-lib-select]').forEach((cb) => {
        cb.checked = false;
      });
      updateLibrarySelectionUi();
    });
    document.getElementById('library-download-selected-btn')?.addEventListener('click', async (e) => {
      // v1.11.30: filter the bulk-TDB action to ONLY items that are
      // ThemerrDB-tracked. Pre-fix the handler split the selKey on ':'
      // and treated parts[1] as a tmdb_id even when the row was
      // sidecar-only (M) and parts[1] was actually a Plex rating_key.
      // That made the worker try to download a 'tmdb_id=<rating_key>'
      // — at best a 404, at worst a redownload loop on a synthetic
      // orphan with the same id. Now we walk libraryState.items and
      // include only rows where theme_tmdb is real and the row isn't
      // a plex_orphan (which has no upstream URL to fetch from).
      const items = [];
      const skipped = [];
      // v1.12.66: per-source-letter breakdown so the confirm dialog
      // can show "3 will be downloaded fresh, 2 will replace U content,
      // 1 will replace P-agent" instead of a faceless "12 rows" count.
      // The user picked this up from a v1.11.30 selection that mixed
      // M sidecars (now ADOPT-only) with downloadable rows; making the
      // mix legible avoids accidental overwrites.
      const breakdown = { T: 0, U: 0, A: 0, M: 0, P: 0, '-': 0 };
      const selectedKeys = libraryState.selected;
      for (const it of (libraryState.items || [])) {
        const key = libKey(it);
        if (!selectedKeys.has(key)) continue;
        const themed = (it.theme_media_type
                        && it.theme_tmdb !== null
                        && it.theme_tmdb !== undefined
                        && it.upstream_source !== 'plex_orphan');
        if (!themed) {
          skipped.push(it.plex_title || key);
          continue;
        }
        const srcLetter = computeSrcLetter(it);
        if (srcLetter in breakdown) breakdown[srcLetter]++;
        items.push({
          media_type: it.theme_media_type,
          tmdb_id: it.theme_tmdb,
        });
      }
      if (items.length === 0) {
        alert('Nothing downloadable in selection — every selected row is a sidecar (use ADOPT SELECTED) or has no ThemerrDB record.');
        return;
      }
      // v1.12.66: confirm dialog with kind-by-kind breakdown so the
      // user knows exactly which source classes get overwritten. Only
      // shows breakdown lines for non-zero counts; if everything is
      // '-' (a clean download spread) the dialog reads simpler.
      const lines = [];
      const parts = [];
      if (breakdown['-']) parts.push(`${breakdown['-']} unthemed (clean download)`);
      if (breakdown.T)    parts.push(`${breakdown.T} T (refresh)`);
      if (breakdown.U)    parts.push(`${breakdown.U} U (replace user URL)`);
      if (breakdown.A)    parts.push(`${breakdown.A} A (replace adopted)`);
      if (breakdown.M)    parts.push(`${breakdown.M} M (replace sidecar)`);
      if (breakdown.P)    parts.push(`${breakdown.P} P (replace Plex agent)`);
      const willReplace = breakdown.T + breakdown.U + breakdown.A
                        + breakdown.M + breakdown.P;
      lines.push(`Download ${items.length} theme${items.length === 1 ? '' : 's'} from ThemerrDB?`);
      lines.push('');
      lines.push('Breakdown:');
      lines.push('  ' + parts.join('\n  '));
      if (willReplace > 0) {
        lines.push('');
        lines.push(`⚠ ${willReplace} row${willReplace === 1 ? '' : 's'} will have their existing theme replaced. This cannot be undone in bulk; per-row REVERT remains available where applicable.`);
      }
      if (skipped.length) {
        lines.push('');
        lines.push(`(${skipped.length} skipped — sidecars use ADOPT, no-TDB rows have no upstream to fetch.)`);
      }
      if (!confirm(lines.join('\n'))) return;
      const btn = e.currentTarget;
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = '// QUEUING';
      try {
        const r = await api('POST', '/api/library/download-batch', { items });
        const skipNote = skipped.length ? ` (${skipped.length} skipped — use ADOPT for sidecars)` : '';
        btn.textContent = `// ${r.enqueued} QUEUED${skipNote}`;
        libraryState.selected.clear();
        setTimeout(() => loadLibrary().catch(()=>{}), 1000);
        libraryRapidPoll();
      } catch (err) {
        alert('Bulk download failed: ' + err.message);
      }
      setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 4000);
    });

    // v1.13.33: bulk PUSH TO PLEX. Walks the selection, fires the
    // per-row /replace endpoint for each downloaded-but-not-placed
    // row. The /replace path force-overwrites whatever's in the
    // Plex folder so a row with M+P state gets motif's canonical
    // hardlinked over Plex's served theme. Per-row dispatch
    // matches what the row's PLACE → PUSH TO PLEX action does;
    // there's no /api/library/place-batch endpoint. Acceptable
    // for the scale this surfaces (handful of awaiting-placement
    // rows, not 10K).
    document.getElementById('library-push-selected-btn')?.addEventListener('click', async (e) => {
      const btn = e.currentTarget;
      const selectedKeys = libraryState.selected;
      const candidates = [];
      const skipped = [];
      for (const it of (libraryState.items || [])) {
        const key = libKey(it);
        if (!selectedKeys.has(key)) continue;
        const themed = (it.theme_media_type
                        && it.theme_tmdb !== null
                        && it.theme_tmdb !== undefined
                        && it.upstream_source !== 'plex_orphan');
        const awaitingApproval = !it.job_in_flight
                              && !!it.file_path
                              && !it.media_folder;
        if (awaitingApproval && themed) {
          candidates.push({
            mt: it.theme_media_type,
            id: it.theme_tmdb,
            title: it.plex_title || '',
          });
        } else {
          skipped.push(it.plex_title || key);
        }
      }
      if (candidates.length === 0) {
        alert('Nothing to push — every selected row is already placed, in flight, or has no downloaded canonical.');
        return;
      }
      // v1.13.35: surface off-page selections in the confirm
      // dialog. libraryState.selected survives across pagination
      // but libraryState.items only holds the visible page, so an
      // off-page selection is silently dropped from the bulk
      // operation. Tell the user explicitly so they don't think
      // the action covered every selected row.
      const offPageCount = libraryState.selected.size
                        - candidates.length - skipped.length;
      const lines = [`Push ${candidates.length} downloaded theme${candidates.length === 1 ? '' : 's'} into Plex?`];
      if (skipped.length) {
        lines.push('');
        lines.push(`(${skipped.length} skipped — already placed or no downloaded canonical.)`);
      }
      if (offPageCount > 0) {
        lines.push('');
        lines.push(`⚠ ${offPageCount} selected row${offPageCount === 1 ? '' : 's are'} not on this page — bulk PUSH only operates on the current page's items. Navigate to those pages and run PUSH again, or use a tighter filter to bring everything onto one page.`);
      }
      if (!confirm(lines.join('\n'))) return;
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = '// PUSHING…';
      let ok = 0;
      let failed = 0;
      // Fire sequentially so we don't pile 50 concurrent place
      // jobs on the worker queue when the user has a big selection.
      // Each /replace call enqueues a place job; the worker drains
      // them serially anyway. Per-row failure is caught + counted
      // so a bad row doesn't abort the rest.
      for (const c of candidates) {
        try {
          await api('POST', `/api/items/${c.mt}/${c.id}/replace`);
          ok++;
        } catch (_) {
          failed++;
        }
      }
      const skipNote = skipped.length ? ` (${skipped.length} skipped)` : '';
      const failNote = failed ? `, ${failed} failed` : '';
      btn.textContent = `// ${ok} QUEUED${failNote}${skipNote}`;
      libraryState.selected.clear();
      setTimeout(() => loadLibrary().catch(()=>{}), 1000);
      setTimeout(refreshTopbarStatus, 1100);
      libraryRapidPoll();
      setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 4000);
    });

    // v1.12.94: shared param-builder for the bulk-pagination handlers
    // (SELECT ALL FILTERED, EXPORT CSV). Pre-fix each handler had its
    // own copy of the URL-param construction and both forgot to
    // include the pill filters (src_pills / tdb_pills / dl_pills /
    // pl_pills / link_pills / ed_pills), so a filter like
    // TDB=NO TDB silently dropped — the bulk query then selected /
    // exported the broader pre-pill result set. Producing a count
    // higher than the visible // RESULTS. Extracting the builder
    // ensures the two stay in sync; the main loadLibrary path uses
    // its own (more sort-aware) constructor and isn't routed
    // through this helper.
    function buildLibraryFilterParams(perPage = 200) {
      const params = new URLSearchParams({
        tab: libraryState.tab,
        fourk: libraryState.fourk ? 'true' : 'false',
        per_page: String(perPage),
      });
      if (libraryState.q) params.set('q', libraryState.q);
      if (libraryState.status !== 'all') params.set('status', libraryState.status);
      if (libraryState.tdb && libraryState.tdb !== 'any') {
        params.set('tdb', libraryState.tdb);
      }
      if (libraryState.srcFilter && libraryState.srcFilter.size > 0) {
        params.set('src_pills', Array.from(libraryState.srcFilter).join(','));
      }
      if (libraryState.tdbPills && libraryState.tdbPills.size > 0) {
        params.set('tdb_pills', Array.from(libraryState.tdbPills).join(','));
      }
      if (libraryState.dlPills && libraryState.dlPills.size > 0) {
        params.set('dl_pills', Array.from(libraryState.dlPills).join(','));
      }
      if (libraryState.plPills && libraryState.plPills.size > 0) {
        params.set('pl_pills', Array.from(libraryState.plPills).join(','));
      }
      if (libraryState.linkPills && libraryState.linkPills.size > 0) {
        params.set('link_pills', Array.from(libraryState.linkPills).join(','));
      }
      if (libraryState.edPills && libraryState.edPills.size > 0) {
        params.set('ed_pills', Array.from(libraryState.edPills).join(','));
      }
      return params;
    }

    // v1.10.49: SELECT ALL FILTERED — pulls every page of the current
    // filter and adds each row's key to libraryState.selected. Useful
    // for 'manual + TDB tracked → DOWNLOAD ALL' and similar bulk
    // workflows. Uses a high per_page (200, the API max) to keep
    // round-trips bounded.
    document.getElementById('library-select-all-filtered-btn')?.addEventListener('click', async (e) => {
      const btn = e.currentTarget;
      btn.disabled = true;
      const origLabel = btn.textContent;
      btn.textContent = '// LOADING…';
      try {
        const params = buildLibraryFilterParams(200);
        let page = 1;
        let collected = 0;
        while (true) {
          params.set('page', String(page));
          const data = await api('GET', '/api/library?' + params.toString());
          for (const it of (data.items || [])) {
            libraryState.selected.add(libKey(it));
            collected++;
          }
          const total = data.total || 0;
          const perPage = data.per_page || 200;
          if (page * perPage >= total) break;
          page++;
          if (page > 200) break;  // safety; never expected
        }
        // Reflect new state in the visible page checkboxes.
        document.querySelectorAll('#library-body input[data-lib-select]').forEach((cb) => {
          cb.checked = libraryState.selected.has(cb.dataset.libSelect);
        });
        updateLibrarySelectionUi();
        btn.textContent = `// ${collected} SELECTED`;
      } catch (err) {
        alert('Select all filtered failed: ' + err.message);
        btn.textContent = origLabel;
      }
      setTimeout(() => {
        btn.disabled = false;
        btn.textContent = origLabel;
      }, 2500);
    });

    // v1.12.6: EXPORT CSV — pulls every selected row (paginating
    // /api/library to recover row data when SELECT ALL FILTERED was
    // used) and builds a 2-column CSV "Title (Year)","imdb_id" for
    // mdblist.com / radarr / sonarr importlists. Selection-driven
    // rather than filter-driven so the user can hand-curate the
    // export by toggling individual rows before clicking.
    document.getElementById('library-export-csv-btn')?.addEventListener('click', async (e) => {
      const btn = e.currentTarget;
      const selectedKeys = libraryState.selected;
      if (selectedKeys.size === 0) {
        alert('No rows selected — pick rows first or click // SELECT ALL FILTERED');
        return;
      }
      btn.disabled = true;
      const origLabel = btn.textContent;
      btn.textContent = '// EXPORTING…';
      try {
        // Build a key → {title, year, imdb_id} map by walking every
        // page of the current filter. Same pagination loop the
        // SELECT ALL FILTERED handler uses; we already paid for
        // these rows on the way in. v1.12.94: pill filters are now
        // included via buildLibraryFilterParams (was a copy-paste
        // omission shared with select-all).
        const params = buildLibraryFilterParams(200);
        const rowsByKey = new Map();
        let page = 1;
        while (true) {
          params.set('page', String(page));
          const data = await api('GET', '/api/library?' + params.toString());
          for (const it of (data.items || [])) {
            const k = libKey(it);
            if (selectedKeys.has(k)) {
              rowsByKey.set(k, {
                title: it.theme_title || it.plex_title || it.title || '',
                year: it.year || '',
                imdb: it.imdb_id || it.guid_imdb || '',
              });
            }
          }
          const total = data.total || 0;
          const perPage = data.per_page || 200;
          if (page * perPage >= total) break;
          page++;
          if (page > 200) break;  // safety
        }
        if (rowsByKey.size === 0) {
          alert('No selected rows are visible under the current filter.');
          btn.textContent = origLabel;
          btn.disabled = false;
          return;
        }
        // CSV escape per RFC 4180 — wrap in quotes when the value
        // contains a comma, quote, or newline; double any embedded
        // quotes. Movie titles routinely contain commas
        // ('Lock, Stock, and Two Smoking Barrels').
        const csvEscape = (s) => {
          const v = String(s ?? '');
          return /[",\r\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
        };
        const lines = ['Title,IMDB'];
        const sorted = Array.from(rowsByKey.values()).sort((a, b) =>
          a.title.localeCompare(b.title, undefined, { sensitivity: 'base' })
        );
        for (const r of sorted) {
          const titleYear = r.year ? `${r.title} (${r.year})` : r.title;
          lines.push(`${csvEscape(titleYear)},${csvEscape(r.imdb)}`);
        }
        const blob = new Blob([lines.join('\r\n') + '\r\n'],
                              { type: 'text/csv;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
        const tab = libraryState.tab || 'library';
        const tag = libraryState.status === 'not_in_plex' ? 'tdb-only' : 'selection';
        const a = document.createElement('a');
        a.href = url;
        a.download = `motif-${tab}-${tag}-${stamp}.csv`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
        btn.textContent = `// EXPORTED ${rowsByKey.size}`;
      } catch (err) {
        alert('Export CSV failed: ' + err.message);
        btn.textContent = origLabel;
      }
      setTimeout(() => {
        btn.disabled = false;
        btn.textContent = origLabel;
      }, 2500);
    });

    // v1.10.49: ADOPT SELECTED — bulk action for sidecar-only rows.
    // Only fires the inline adopt-sidecar endpoint for selected rows
    // that are actually sidecar-only (rows without a sidecar are
    // silently skipped). Each row is rk-keyed so we look it up in
    // libraryState.items / fall back to fetching per-row data.
    document.getElementById('library-adopt-selected-btn')?.addEventListener('click', async (e) => {
      const candidates = (libraryState.items || []).filter((it) => {
        const k = libKey(it);
        if (!libraryState.selected.has(k)) return false;
        // sidecar-only state: no placement + a sidecar exists.
        return !it.media_folder && !!it.plex_local_theme;
      });
      // Selections from "SELECT ALL FILTERED" cover items not in the
      // current page — we can't tell their sidecar state without
      // re-fetching. Tell the user to switch the filter to MANUAL or
      // page through to scope correctly.
      const onPageSelected = (libraryState.items || []).filter((it) =>
        libraryState.selected.has(libKey(it))).length;
      const offPageSelected = libraryState.selected.size - onPageSelected;
      if (offPageSelected > 0) {
        const ok = confirm(
          `${offPageSelected} selected row(s) aren't on the visible page; ADOPT will run on the ${candidates.length} sidecar-only rows on this page.\n\nTip: click the SRC M pill to scope to sidecars.`
        );
        if (!ok) return;
      }
      if (candidates.length === 0) {
        alert('No sidecar-only rows selected. ADOPT applies to rows showing the M pill (Plex has a theme.mp3 motif doesn\'t manage).');
        return;
      }
      if (!confirm(`Adopt ${candidates.length} sidecar(s)? Each is hardlinked into /themes and managed by motif from now on.`)) return;
      const btn = e.currentTarget;
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = `// ADOPTING 0/${candidates.length}`;
      let ok = 0, fail = 0;
      for (let i = 0; i < candidates.length; i++) {
        const it = candidates[i];
        try {
          await api('POST', `/api/plex_items/${encodeURIComponent(it.rating_key)}/adopt-sidecar`);
          ok++;
        } catch (err) {
          fail++;
        }
        btn.textContent = `// ADOPTING ${i + 1}/${candidates.length}`;
      }
      btn.textContent = `// ${ok} ADOPTED${fail ? ` · ${fail} FAILED` : ''}`;
      libraryState.selected.clear();
      setTimeout(() => loadLibrary().catch(()=>{}), 1000);
      libraryRapidPoll();
      setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 3500);
    });

    // v1.10.51: ACK SELECTED — bulk acknowledge failures. Visible only
    // on the FAILURES filter. Walks the selection, fires
    // /clear-failure for each themed row that has an unacked
    // failure_kind. Off-page selections are filtered to candidates
    // we know about from libraryState.items; the user is told if
    // any selected rows aren't on the visible page.
    // v1.12.55: bulk accept-all / decline-all handlers. Both call
    // server-side endpoints that iterate every pending_updates row
    // with decision='pending', so the action is one HTTP round-trip
    // regardless of how many pending updates exist.
    // v1.12.120: accept/decline ALL UPDATES is two-mode now.
    // (a) No selection — call /api/updates/accept-all (server walks
    //     every eligible per-section pending update). The button
    //     only renders in this mode when tdb_pills=update is the
    //     ONLY active filter (see updateLibrarySelectionUi); using
    //     it on a narrowed view would silently fan out beyond what
    //     the user is looking at.
    // (b) With selection — iterate the selected rows, call per-row
    //     /api/updates/{type}/{id}/accept (?section_id=...) on the
    //     ones with a live pending_update, skip the rest. User's
    //     bug report: "if someone tries a bulk action against an
    //     item which doesn't allow that don't attempt the action
    //     against that row" — the filter is the skip rule.
    function _selectedActionableForUpdates() {
      return (libraryState.items || []).filter(
        (it) => libraryState.selected.has(libKey(it))
                && it.pending_update
                && computeSrcLetter(it) !== '-',
      );
    }

    document.getElementById('library-accept-all-updates-btn')?.addEventListener('click', async (e) => {
      const btn = e.currentTarget;
      const orig = btn.textContent;
      const selection = _selectedActionableForUpdates();
      if (selection.length > 0) {
        const skipped = libraryState.selected.size - selection.length;
        const skipNote = skipped > 0
          ? `\n\n${skipped} selected row${skipped !== 1 ? 's' : ''} without a pending update will be skipped.`
          : '';
        const ok = confirm(
          `Accept ${selection.length} pending update${selection.length !== 1 ? 's' : ''} from selection?`
          + skipNote
          + `\n\nFor URL-match rows this is instant; the rest get a download queued and the existing theme replaced. Per-row REVERT remains available.`
        );
        if (!ok) return;
        btn.disabled = true;
        let ok_n = 0, fail_n = 0;
        for (let i = 0; i < selection.length; i++) {
          const it = selection[i];
          btn.textContent = `// ACCEPTING ${i + 1}/${selection.length}`;
          try {
            const params = it.section_id ? `?section_id=${encodeURIComponent(it.section_id)}` : '';
            await api('POST', `/api/updates/${it.theme_media_type}/${it.theme_tmdb}/accept${params}`);
            ok_n++;
          } catch (err) {
            // v1.13.8: surface the per-item error to the browser
            // console so the user has a stack trace when X failed
            // pops in the toast — pre-fix the underlying reason
            // was completely silent. fail_n still drives the toast
            // count; the console gets the diagnostic detail.
            try { console.error('bulk action failed for item:', it, err); } catch (_) {}
            fail_n++;
          }
        }
        btn.textContent = `// ${ok_n} ACCEPTED${fail_n ? ` · ${fail_n} FAILED` : ''}${skipped ? ` · ${skipped} SKIPPED` : ''}`;
        libraryState.selected.clear();
        libraryRapidPoll();
        setTimeout(() => loadLibrary().catch(()=>{}), 600);
        setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 3000);
        return;
      }
      // No selection — global accept-all path.
      let pending = 0;
      try {
        const res = await api('GET', '/api/updates/count');
        pending = res.pending || 0;
      } catch (_) { /* fall through; the endpoint will handle the noop */ }
      if (pending === 0) {
        alert('No pending updates to accept.');
        return;
      }
      const ok = confirm(
        `Accept ${pending} pending ThemerrDB update`
          + `${pending !== 1 ? 's' : ''}?\n\n`
          + `For URL-match rows (your override URL == TDB URL) this is `
          + `instant — no download. For the rest motif will queue a `
          + `download per row, replacing the current theme. The action `
          + `cannot be undone in bulk; per-row REVERT remains available.`
      );
      if (!ok) return;
      btn.disabled = true;
      btn.textContent = `// ACCEPTING ${pending}…`;
      try {
        const res = await api('POST', '/api/updates/accept-all');
        const flipped = res.eager_flipped || 0;
        const queued = res.downloads_queued || 0;
        btn.textContent = `// ${res.accepted} ACCEPTED`
          + (flipped ? ` · ${flipped} FLIPPED` : '')
          + (queued ? ` · ${queued} QUEUED` : '');
        libraryRapidPoll();
      } catch (err) {
        btn.textContent = '// FAILED';
        alert('Bulk accept failed: ' + err.message);
      } finally {
        setTimeout(() => loadLibrary().catch(()=>{}), 600);
        setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 3000);
      }
    });

    document.getElementById('library-decline-all-updates-btn')?.addEventListener('click', async (e) => {
      const btn = e.currentTarget;
      const orig = btn.textContent;
      const selection = _selectedActionableForUpdates();
      if (selection.length > 0) {
        const skipped = libraryState.selected.size - selection.length;
        const skipNote = skipped > 0
          ? `\n\n${skipped} selected row${skipped !== 1 ? 's' : ''} without a pending update will be skipped.`
          : '';
        const ok = confirm(
          `Dismiss ${selection.length} pending update${selection.length !== 1 ? 's' : ''} from selection?`
          + skipNote
          + `\n\nThe blue ↑ pill stays on each row for filter/sort, but the topbar UPD count drops accordingly.`
        );
        if (!ok) return;
        btn.disabled = true;
        let ok_n = 0, fail_n = 0;
        for (let i = 0; i < selection.length; i++) {
          const it = selection[i];
          btn.textContent = `// DISMISSING ${i + 1}/${selection.length}`;
          try {
            const params = it.section_id ? `?section_id=${encodeURIComponent(it.section_id)}` : '';
            await api('POST', `/api/updates/${it.theme_media_type}/${it.theme_tmdb}/decline${params}`);
            ok_n++;
          } catch (err) {
            // v1.13.8: surface the per-item error to the browser
            // console so the user has a stack trace when X failed
            // pops in the toast — pre-fix the underlying reason
            // was completely silent. fail_n still drives the toast
            // count; the console gets the diagnostic detail.
            try { console.error('bulk action failed for item:', it, err); } catch (_) {}
            fail_n++;
          }
        }
        btn.textContent = `// ${ok_n} DISMISSED${fail_n ? ` · ${fail_n} FAILED` : ''}${skipped ? ` · ${skipped} SKIPPED` : ''}`;
        libraryState.selected.clear();
        libraryRapidPoll();
        setTimeout(() => loadLibrary().catch(()=>{}), 600);
        setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 2500);
        return;
      }
      // No selection — global decline-all path.
      let pending = 0;
      try {
        const res = await api('GET', '/api/updates/count');
        pending = res.pending || 0;
      } catch (_) { /* fall through */ }
      if (pending === 0) {
        alert('No pending updates to dismiss.');
        return;
      }
      const ok = confirm(
        `Dismiss ${pending} pending update${pending !== 1 ? 's' : ''}?\n\n`
          + `The blue ↑ pill stays on each row for filter/sort, but the `
          + `topbar UPD count drops to 0. Won't re-prompt unless ThemerrDB `
          + `updates again.`
      );
      if (!ok) return;
      btn.disabled = true;
      btn.textContent = `// DISMISSING ${pending}…`;
      try {
        const res = await api('POST', '/api/updates/decline-all');
        btn.textContent = `// ${res.declined} DISMISSED`;
      } catch (err) {
        btn.textContent = '// FAILED';
        alert('Bulk dismiss failed: ' + err.message);
      } finally {
        setTimeout(() => loadLibrary().catch(()=>{}), 600);
        setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 2500);
      }
    });

    document.getElementById('library-ack-selected-btn')?.addEventListener('click', async (e) => {
      const candidates = (libraryState.items || []).filter((it) => {
        if (!it.theme_media_type || it.theme_tmdb === null
            || it.theme_tmdb === undefined) return false;
        if (!it.failure_kind || it.failure_acked_at) return false;
        return libraryState.selected.has(libKey(it));
      });
      const onPageSelected = (libraryState.items || []).filter((it) =>
        libraryState.selected.has(libKey(it))).length;
      const offPageSelected = libraryState.selected.size - onPageSelected;
      if (offPageSelected > 0) {
        const ok = confirm(
          `${offPageSelected} selected row(s) aren't on the visible page; ACK will run on the ${candidates.length} on-page failures.`
        );
        if (!ok) return;
      }
      if (candidates.length === 0) {
        alert('No unacknowledged failures in selection.');
        return;
      }
      if (!confirm(`Acknowledge ${candidates.length} failure(s)?`)) return;
      const btn = e.currentTarget;
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = `// ACKING 0/${candidates.length}`;
      let ok = 0, fail = 0;
      for (let i = 0; i < candidates.length; i++) {
        const it = candidates[i];
        try {
          await api('POST', `/api/items/${it.theme_media_type}/${it.theme_tmdb}/clear-failure`);
          ok++;
        } catch (err) {
          fail++;
        }
        btn.textContent = `// ACKING ${i + 1}/${candidates.length}`;
      }
      btn.textContent = `// ${ok} ACKED${fail ? ` · ${fail} FAILED` : ''}`;
      libraryState.selected.clear();
      setTimeout(() => loadLibrary().catch(()=>{}), 600);
      setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 2500);
    });

    // v1.10.24: close any open row-menu popover when the user clicks
    // anywhere outside it. Native <details> handles open/close on the
    // summary itself but doesn't auto-dismiss on outside click.
    document.addEventListener('click', (e) => {
      const inside = e.target.closest('.row-menu');
      document.querySelectorAll('.row-menu[open]').forEach((d) => {
        if (d !== inside) d.removeAttribute('open');
      });
    });

    // v1.10.14: helpers to look up the row a button belongs to and
    // detect Plex-agent rows. Used to gate destructive overrides
    // (DOWNLOAD / URL / UPLOAD) behind a confirmation when Plex is
    // already supplying a theme — the user wants Plex's own themes to
    // win by default and only override on explicit opt-in.
    function findItemForButton(btn) {
      const rk = btn.dataset.rk;
      const mt = btn.dataset.mt;
      const id = btn.dataset.id;
      return (libraryState.items || []).find((it) =>
        (rk && it.rating_key === rk) ||
        (mt && id && it.theme_media_type === mt
              && String(it.theme_tmdb) === String(id))
      );
    }
    function isPlexAgentRow(it) {
      return !!it && !it.media_folder && !it.plex_local_theme && !!it.plex_has_theme;
    }
    function confirmPlexAgentOverride(action, title, sourceLabel) {
      // v1.12.59: parenthetical now names the actual source motif
      // will use (the ThemerrDB version / your manual URL / your
      // uploaded MP3) instead of the vague "motif's version" — the
      // user reading the prompt for REPLACE TDB on a P-agent row
      // shouldn't have to wonder what "motif's version" means when
      // motif itself is fetching from ThemerrDB.
      return confirm(
        `Plex is already supplying a theme for "${title || 'this item'}".\n\n`
        + `Motif's default is to defer to Plex when it has its own theme. `
        + `Are you sure you want to ${action}?\n\n`
        + `(This will replace what Plex currently plays with ${sourceLabel || "motif's version"}.)`
      );
    }

    // Row clicks: redl, upload-theme, manual-url, delete-orphan, override, info
    document.getElementById('library-body')?.addEventListener('click', async (e) => {
      const btn = e.target.closest('button[data-act]');
      if (!btn) return;
      // v1.10.24: action buttons inside a row-menu popover should close
      // the menu after firing. Schedule the close after the click
      // handler runs so other handlers see the open state if they
      // care.
      const menuParent = e.target.closest('.row-menu');
      if (menuParent) {
        setTimeout(() => menuParent.removeAttribute('open'), 0);
      }
      const act = btn.dataset.act;
      // v1.13.21 (was v1.13.20): boost the ops poll cadence the moment
      // the user enqueues work. Pre-fix a single fast yt-dlp call
      // (~10s) could complete entirely between the drawer's idle
      // 10s polls, so the topbar status pill never appeared and the
      // user thought nothing happened. The optimistic placeholder
      // fills the very-first-frame gap; boostPoll keeps it lit until
      // /api/progress reflects the running op.
      const _enqueueing = new Set([
        'download', 'redl', 'place', 'unplace', 'restore', 'refresh',
        'relink', 'adopt', 'manual-url', 'upload-theme', 'replace',
        'replace-with-themerrdb', 'accept-update', 'revert', 'clear-url',
      ]);
      if (_enqueueing.has(act)) {
        try {
          if (window.motifOps && typeof window.motifOps.boostPoll === 'function') {
            window.motifOps.boostPoll();
          }
        } catch (_) { /* swallow — boost is a UX nicety, never fatal */ }
      }
      // P-agent override gate: prompt before actions that would replace
      // Plex's own theme with motif content.
      if (act === 'redl' || act === 'manual-url' || act === 'upload-theme'
          || act === 'revert' || act === 'replace-with-themerrdb') {
        const it = findItemForButton(btn);
        if (isPlexAgentRow(it)) {
          const verb = act === 'redl'                  ? 'download from ThemerrDB'
                     : act === 'manual-url'            ? 'set a manual YouTube URL'
                     : act === 'upload-theme'          ? 'upload an MP3'
                     : act === 'revert'                ? 'revert to ThemerrDB'
                     :                                   "replace with ThemerrDB's version";
          // v1.12.59: source label names what's about to play
          // instead of the vague "motif's version" the prompt
          // used to show. Maps each action to the canonical
          // wording the rest of the UI uses (ThemerrDB / your
          // manual YouTube URL / your uploaded MP3).
          const sourceLabel =
              act === 'manual-url'  ? 'your manual YouTube URL'
            : act === 'upload-theme' ? 'your uploaded MP3'
            :                          'the ThemerrDB version';
          if (!confirmPlexAgentOverride(verb, btn.dataset.title, sourceLabel)) return;
        }
      }
      if (act === 'redl') {
        // v1.12.73: pass section_id from the menu button (set by
        // menuItemHtml(extras.sectionId)) so RE-DOWNLOAD TDB /
        // DOWNLOAD TDB target only the row's section. Mirrors the
        // ACCEPT UPDATE / REVERT scoping we already do.
        redownload(btn.dataset.mt, btn.dataset.id, btn,
                   btn.dataset.sectionId || undefined).catch(console.error);
      } else if (act === 'revert') {
        revertToThemerrDb(btn.dataset.mt, btn.dataset.id, btn).catch(console.error);
      } else if (act === 'clear-url') {
        // v1.12.37 (revised): CLEAR URL drops the captured
        // previous URL on the row so REVERT becomes unavailable.
        // Doesn't touch the canonical or user_overrides — the
        // current playing theme is unaffected.
        // v1.12.86: pass section_id so the clear scopes to the
        // row's section. Without it the endpoint clears every
        // section's snapshot for the title (legacy behavior).
        const title = btn.dataset.title || 'this theme';
        if (!confirm(`Clear previous URL for "${title}"?\nREVERT will no longer be available.`)) return;
        clearUrlOverride(
          btn.dataset.mt, btn.dataset.id, btn,
          btn.dataset.sectionId || undefined,
        ).catch(console.error);
      } else if (act === 'info') {
        // v1.12.72: pass section_id from the row so INFO surfaces
        // the section-specific override (when per-section overrides
        // exist). Closest tr's first <details data-section-id>
        // attribute would also work, but the menu button's
        // data-section-id (set by menuItemHtml) is the cleanest path.
        // Fall back to undefined when not present.
        const sid = btn.dataset.sectionId
                 || btn.closest('tr')?.dataset.sectionId
                 || undefined;
        // v1.12.73: blur the trigger button before opening the
        // dialog so the focus-visible cyan outline doesn't linger
        // on the row's ⓘ button after the user closes the
        // dialog with Esc. Same pattern used elsewhere where a
        // click hands focus off to a modal.
        btn.blur();
        openInfoDialog(btn.dataset.mt, btn.dataset.id, sid).catch(console.error);
      } else if (act === 'delete-orphan') {
        await deleteOrphan(btn.dataset.mt, btn.dataset.id, btn.dataset.title || '');
        await loadLibrary().catch(()=>{});
      } else if (act === 'unplace') {
        await unplaceTheme(btn.dataset.mt, btn.dataset.id,
                           btn.dataset.title || '',
                           btn.dataset.sectionId || undefined);
        await loadLibrary().catch(()=>{});
      } else if (act === 'replace') {
        await replaceTheme(btn.dataset.mt, btn.dataset.id, btn);
      } else if (act === 'adopt-from-plex') {
        // v1.11.99: discard motif's new download, re-adopt the file
        // currently at the Plex folder. Confirm explicitly because
        // the new download will be replaced.
        const ok = confirm(
          "ADOPT FROM PLEX will discard motif's new download and " +
          "re-adopt the file currently at the Plex folder as the " +
          "canonical.\n\nThe new theme content motif fetched will be " +
          "lost — the Plex-folder file becomes the source of truth.\n\n" +
          "Proceed?");
        if (!ok) return;
        try {
          if (btn) btn.disabled = true;
          await api('POST',
            `/api/items/${btn.dataset.mt}/${btn.dataset.id}/adopt-from-plex`);
          await loadLibrary().catch(() => {});
          setTimeout(refreshTopbarStatus, 1100);
        } catch (e) {
          alert('Adopt from Plex failed: ' + e.message);
          if (btn) btn.disabled = false;
        }
      } else if (act === 'keep-mismatch') {
        // v1.11.99: ack the mismatch — drops it from /pending but
        // keeps both files in place. Library row keeps DL=amber +
        // LINK=≠ as a passive reminder.
        try {
          if (btn) btn.disabled = true;
          await api('POST',
            `/api/items/${btn.dataset.mt}/${btn.dataset.id}/keep-mismatch`);
          await loadLibrary().catch(() => {});
          setTimeout(refreshTopbarStatus, 1100);
        } catch (e) {
          alert('Keep mismatch failed: ' + e.message);
          if (btn) btn.disabled = false;
        }
      } else if (act === 'accept-update') {
        // v1.11.74: same flow as the browse-page ACCEPT button —
        // helper handles the API call + alerting; we just reload
        // the library so the ↑ glyph clears and the new theme
        // metadata appears.
        try {
          await acceptUpdate(btn.dataset.mt, btn.dataset.id, btn);
          await loadLibrary().catch(() => {});
          setTimeout(refreshTopbarStatus, 1100);
        } catch (e) {
          alert('Accept failed: ' + e.message);
        }
      } else if (act === 'decline-update') {
        try {
          await declineUpdate(btn.dataset.mt, btn.dataset.id, btn);
          await loadLibrary().catch(() => {});
          setTimeout(refreshTopbarStatus, 1100);
        } catch (e) {
          alert('Decline failed: ' + e.message);
        }
      } else if (act === 'restore-canonical') {
        // v1.11.62: recreate the missing canonical from the surviving
        // placement file. Hardlink first, copy fallback on cross-FS.
        const title = btn.dataset.title || 'this item';
        if (!confirm(`Restore canonical /themes copy for "${title}" from the Plex-folder file?`)) return;
        try {
          const r = await api('POST', `/api/items/${btn.dataset.mt}/${btn.dataset.id}/restore-canonical`);
          if (r.skipped && r.skipped.length) {
            const reasons = r.skipped.map((s) => `${s.section_id}: ${s.reason}`).join('\n');
            alert(`Restored ${r.restored}; skipped:\n${reasons}`);
          }
          await loadLibrary().catch(()=>{});
        } catch (e) {
          alert('Restore failed: ' + e.message);
        }
      } else if (act === 'purge') {
        await purgeTheme(btn.dataset.mt, btn.dataset.id,
                         btn.dataset.title || '',
                         btn.dataset.orphan === '1',
                         btn.dataset.dlOnly === '1',
                         btn.dataset.sectionId || undefined,
                         btn.dataset.plexAlso === '1');
        await loadLibrary().catch(()=>{});
      } else if (act === 'clear-failure') {
        // v1.10.42: silent acknowledge — no confirm prompt, the
        // action is non-destructive (doesn't delete files or
        // change placements, just clears a flag column).
        try {
          await api('POST', `/api/items/${btn.dataset.mt}/${btn.dataset.id}/clear-failure`);
          await loadLibrary().catch(()=>{});
        } catch (e) {
          alert('Clear-failure failed: ' + e.message);
        }
      } else if (act === 'unmanage') {
        // v1.10.18: drop tracking but leave the Plex-folder file in
        // place. Confirms before firing — destructive and silent
        // about which file gets deleted (the user may not realize
        // the canonical is in /themes vs. their Plex folder).
        const title = btn.dataset.title || 'this item';
        if (!confirm(
          `Stop managing the theme for "${title}"?\n\n`
          + `Motif will:\n`
          + `  • delete its canonical copy at /themes/...\n`
          + `  • drop its tracking (local_files + placement rows)\n`
          + `  • LEAVE the theme.mp3 in your Plex folder alone\n\n`
          + `The row will flip back to M (unmanaged sidecar). You can `
          + `re-adopt or replace it later.`
        )) return;
        try {
          // v1.12.73: pass section_id from the menu button so
          // UNMANAGE targets only this row's section. Sibling
          // sections keep their motif management.
          const sid = btn.dataset.sectionId;
          const unmanageUrl = sid
            ? `/api/items/${btn.dataset.mt}/${btn.dataset.id}/unmanage?section_id=${encodeURIComponent(sid)}`
            : `/api/items/${btn.dataset.mt}/${btn.dataset.id}/unmanage`;
          await api('POST', unmanageUrl);
          libraryRapidPoll();
          await loadLibrary().catch(()=>{});
        } catch (e) {
          alert('Unmanage failed: ' + e.message);
        }
      } else if (act === 'upload-theme') {
        openUploadDialog({
          ratingKey: btn.dataset.rk,
          title: btn.dataset.title || '',
          year: btn.dataset.year || '',
        });
      } else if (act === 'manual-url') {
        openManualUrlDialog({
          ratingKey: btn.dataset.rk,
          title: btn.dataset.title || '',
          year: btn.dataset.year || '',
          tdbUrl: btn.dataset.ytUrl || '',
          appliedUrl: btn.dataset.appliedUrl || '',
          srcLetter: btn.dataset.srcLetter || '',
        });
      } else if (act === 'open-override') {
        openOverrideDialog({
          mediaType: btn.dataset.mt,
          tmdbId: btn.dataset.id,
          kindHuman: btn.dataset.kindHuman || 'failure',
          message: btn.dataset.msg || '',
        });
      } else if (act === 'ack-drop') {
        // v1.13.1 (Phase C): clear tdb_dropped_at without removing
        // the row. The TDB◌ pill goes away; SRC stays as-is.
        const mt = btn.dataset.mt, id = btn.dataset.id;
        if (!mt || !id) return;
        try {
          await api('POST', `/api/items/${mt}/${id}/ack-drop`);
          loadLibrary().catch(()=>{});
        } catch (err) {
          alert('ACK DROP failed: ' + err.message);
        }
      } else if (act === 'convert-to-manual') {
        // v1.13.1 (Phase C): promote themes.youtube_url into
        // user_overrides for this section. SRC reclassifies T→U.
        const mt = btn.dataset.mt, id = btn.dataset.id;
        const sectionId = btn.dataset.sectionId || '';
        if (!mt || !id) return;
        if (!confirm('Convert to manual? Future syncs will skip this row.')) return;
        try {
          const params = sectionId ? `?section_id=${encodeURIComponent(sectionId)}` : '';
          await api('POST',
            `/api/items/${mt}/${id}/convert-to-manual${params}`);
          loadLibrary().catch(()=>{});
        } catch (err) {
          alert('CONVERT TO MANUAL failed: ' + err.message);
        }
      } else if (act === 'adopt-sidecar') {
        // v1.10.9: inline adopt — claim the sidecar at this Plex folder.
        // v1.10.21: surface the match kind in the confirm prompt so the
        // user knows whether the result will be linked to a ThemerrDB
        // record (REPLACE w/ TDB available after) or a pure orphan
        // (file is the source of truth, no TDB alternative).
        // v1.10.53: third case — TMDB-matched but the YouTube URL is
        // dead (red TDB ✗ pill). REPLACE w/ TDB won't work until
        // ThemerrDB updates the URL.
        const title = btn.dataset.title || 'this item';
        const it = findItemForButton(btn);
        const tdbTracked = !!(it && it.upstream_source
                              && it.upstream_source !== 'plex_orphan');
        const tdbDead = !!(it && it.failure_kind && new Set([
          'video_private', 'video_removed',
          'video_age_restricted', 'geo_blocked',
        ]).has(it.failure_kind));
        const matchNote = !tdbTracked
          ? "No ThemerrDB record — your file is the source of truth."
          : tdbDead
            ? "ThemerrDB tracks this title but the YouTube URL is broken (removed / private / restricted). REPLACE TDB stays unavailable until upstream updates."
            : "ThemerrDB tracks this title — REPLACE TDB will be available after adopt.";
        if (!confirm(
          `Adopt the existing theme.mp3 for "${title}"?\n\n${matchNote}\n\n`
          + `Motif will hardlink the file into /themes and manage it from now on.`
        )) return;
        try {
          await api('POST', `/api/plex_items/${encodeURIComponent(btn.dataset.rk)}/adopt-sidecar`);
          libraryRapidPoll();
          await loadLibrary().catch(()=>{});
        } catch (e) {
          alert('Adopt failed: ' + e.message);
        }
      } else if (act === 'replace-with-themerrdb') {
        const title = btn.dataset.title || 'this item';
        // v1.11.55: skip the per-action confirm when the row is a
        // P-agent — the upstream P-agent override gate at line 3737
        // already asked the same question ('Plex is supplying a
        // theme; replace with motif's version?'). Pre-fix the user
        // got two near-identical confirms back to back. The gate
        // doesn't fire for sidecarOnly (M) or isManualPlacement
        // rows, so this confirm still runs in those cases where
        // the gate gave no warning.
        const it = findItemForButton(btn);
        if (!isPlexAgentRow(it)) {
          if (!confirm(`Replace "${title}" theme with the ThemerrDB version?`)) return;
        }
        try {
          await api('POST', `/api/plex_items/${encodeURIComponent(btn.dataset.rk)}/replace-with-themerrdb`);
          libraryRapidPoll();
          await loadLibrary().catch(()=>{});
        } catch (e) {
          alert('Replace failed: ' + e.message);
        }
      }
    });
  }

  // ---- ThemerrDB info dialog ----

  async function openInfoDialog(mediaType, tmdbId, sectionId) {
    const dlg = document.getElementById('info-dlg');
    if (!dlg) return;
    const body = document.getElementById('info-dlg-body');
    body.innerHTML = '<p class="muted">loading…</p>';
    if (typeof dlg.showModal === 'function') dlg.showModal();
    else dlg.setAttribute('open', '');
    let data;
    try {
      // v1.12.72: pass section_id so the INFO endpoint surfaces
      // the section-specific user_overrides row in the legacy
      // `override` field. Falls through to global ('') and then
      // any-section if no per-section override exists.
      const url = sectionId
        ? `/api/items/${mediaType}/${tmdbId}?section_id=${encodeURIComponent(sectionId)}`
        : `/api/items/${mediaType}/${tmdbId}`;
      data = await api('GET', url);
    } catch (e) {
      body.innerHTML = `<p class="accent-red">${htmlEscape(e.message)}</p>`;
      return;
    }
    const t = data.theme || {};
    const ovr = data.override;
    const lf = data.local_file;
    const placements = data.placements || [];
    const pu = data.pending_update;
    // v1.12.37 / v1.12.46: three URL rows render top-of-card —
    // ThemerrDB, currently applied, previous.
    //
    // "currently applied" only fills in when the row's canonical
    // was actually downloaded from a YouTube URL — i.e., the
    // local_files row has source_kind 'themerrdb' / 'url' /
    // 'upload'. M (manual sidecar), A (adopted), and P (Plex
    // agent) rows have a theme.mp3 in place but its contents
    // didn't come from a YouTube URL motif controls. Pre-fix
    // the field showed t.youtube_url for those rows even though
    // the displayed URL wasn't being applied — misleading.
    // v1.12.46 leaves it blank for non-URL sources.
    const tdbUrl = t.youtube_url || '';
    const lfSource = lf?.source_kind || null;
    const isUrlSourced = lfSource === 'themerrdb'
      || lfSource === 'url'
      || lfSource === 'upload';
    const currentUrl = isUrlSourced
      ? (ovr?.youtube_url || t.youtube_url || '')
      : '';
    // v1.12.86: per-section previous URL. Payload comes from a
    // top-level `previous_url` object {youtube_url, kind,
    // captured_at} populated by api_item via _load_previous_url.
    // Pre-v1.12.86 the same data lived on the title-global
    // themes.previous_youtube_url + previous_youtube_kind columns —
    // those were dropped in schema v30 along with the cross-section
    // bleed they caused.
    const previousUrlObj = data.previous_url || null;
    const previousUrl = previousUrlObj?.youtube_url || '';
    const previousKind = previousUrlObj?.kind || null;
    // ytId for the embedded YouTube thumbnail tracks the currently
    // applied URL so it always matches what's being played.
    const ytUrl = currentUrl;
    const ytId = ovr?.youtube_video_id || t.youtube_video_id ||
                 (ytUrl ? (ytUrl.match(/[?&]v=([^&]+)/) || [])[1] : '');
    const imdb = t.imdb_id ? `<a href="https://www.imdb.com/title/${htmlEscape(t.imdb_id)}" target="_blank" rel="noopener">${htmlEscape(t.imdb_id)}</a>` : '<span class="muted">—</span>';
    const tmdbLink = t.tmdb_id && t.tmdb_id > 0
      ? `<a href="https://www.themoviedb.org/${mediaType === 'tv' ? 'tv' : 'movie'}/${t.tmdb_id}" target="_blank" rel="noopener">${t.tmdb_id}</a>`
      : '<span class="muted">orphan</span>';
    // v1.12.37: three URL rows render top-of-card so the user
    // can see ThemerrDB's URL, what's currently active, and what
    // REVERT will restore. The currently-applied URL gets violet
    // styling when sourced from a user override (matches the U
    // badge); themerrdb-green when sourced from the upstream
    // record. Previous URL color tracks previous_youtube_kind
    // similarly.
    const linkOrDash = (url, color) =>
      url
        ? `<a href="${htmlEscape(url)}" target="_blank" rel="noopener"${color ? ` style="color:${color}"` : ''}>${htmlEscape(url)}</a>`
        : '<span class="muted">—</span>';
    const tdbUrlLink = linkOrDash(tdbUrl, 'var(--green-bright)');
    // v1.12.81: append a kind label after "currently applied" so it
    // mirrors the "previous url" treatment and the user can read
    // the source at a glance instead of inferring from color alone.
    // Color + label match the SRC badge ("user" violet for U-source
    // overrides; "themerrdb" green for upstream URLs).
    const currentKind = ovr ? 'user' : (currentUrl ? 'themerrdb' : null);
    const currentColor = currentKind === 'user'
      ? 'var(--violet)'
      : currentKind === 'themerrdb' ? 'var(--green-bright)' : null;
    const currentKindLabel = currentKind === 'user'
      ? ' <span class="muted small" style="color:var(--violet)">user</span>'
      : currentKind === 'themerrdb'
        ? ' <span class="muted small" style="color:var(--green-bright)">themerrdb</span>'
        : '';
    const currentUrlLink = currentUrl
      ? `${linkOrDash(currentUrl, currentColor)}${currentKindLabel}`
      : '<span class="muted">—</span>';
    const prevColor = previousKind === 'user'
      ? 'var(--violet)'
      : previousKind === 'themerrdb' ? 'var(--green-bright)' : null;
    const prevKindLabel = previousKind === 'user'
      ? '<span class="muted small" style="color:var(--violet)">user</span>'
      : previousKind === 'themerrdb'
        ? '<span class="muted small" style="color:var(--green-bright)">themerrdb</span>'
        : '';
    // v1.13.4 (Issue 2): suppress the INFO card's "previous url"
    // row when the captured-prev equals what's currently applied —
    // three identical URLs is visual noise. The library row's
    // CLEAR URL / REVERT menu items are gated by the same condition
    // server-side via has_previous_url's WHERE-clause check
    // (api.py: COALESCE(pv...) != COALESCE(uo..., t.youtube_url)),
    // so the menu and the card stay in lockstep without a JS shim.
    const hidePrev = previousUrl !== '' && previousUrl === currentUrl;
    const previousUrlLink = (previousUrl && !hidePrev)
      ? `${linkOrDash(previousUrl, prevColor)} ${prevKindLabel}`
      : '<span class="muted">—</span>';
    const ytLink = currentUrlLink;
    const failBlock = t.failure_kind
      ? `<dt>last failure</dt><dd class="accent-red">${htmlEscape(t.failure_kind)}${t.failure_message ? ' · ' + htmlEscape(t.failure_message) : ''}</dd>`
      : '';
    // v1.12.37: override block shows the metadata around the
    // user-override row (set_by, set_at, note). The URL itself is
    // already in the "currently applied" row above, so this block
    // only renders the audit context.
    const ovrBlock = ovr
      ? `<dt>override set</dt><dd><span class="muted small">by ${htmlEscape(ovr.set_by || '')} at ${htmlEscape(fmt.timeAuto(ovr.set_at))}${ovr.note ? ' · ' + htmlEscape(ovr.note) : ''}</span></dd>`
      : '';
    // v1.12.37: pending-update block now only carries the
    // upstream-update decision metadata (detected/decided
    // timestamps). The actual URLs that drove it (TDB old/new)
    // are visible from the "themerrdb url" + "previous url"
    // rows above when they're meaningful — keeps the card from
    // listing four+ URLs which is hard to scan.
    let puBlock = '';
    if (pu) {
      const decisionLabel = pu.decision === 'accepted' ? 'accepted (current)'
                          : pu.decision === 'declined' ? 'declined (kept old)'
                          : 'pending — awaiting ACCEPT UPDATE / KEEP CURRENT';
      puBlock = `<dt>upstream update</dt>`
        + `<dd class="muted small">${htmlEscape(decisionLabel)}`
        + (pu.detected_at ? ` · detected ${htmlEscape(pu.detected_at)}` : '')
        + (pu.decision_at && pu.decision !== 'pending'
              ? ` · ${htmlEscape(pu.decision)} ${htmlEscape(pu.decision_at)}`
              : '')
        + '</dd>';
    }
    // v1.12.65: REVERT-hidden hint moved out of the pu.decision
    // block — REVERT can be hidden for reasons unrelated to a
    // pending update (e.g., previous_kind='themerrdb' is now
    // hidden by design). Block fires whenever there's a previous
    // URL captured but REVERT wouldn't be useful, so the user
    // never wonders why an action they expect is missing. Three
    // cases covered:
    //   - previous URL matches current canonical (no-op — same
    //     URL would be re-applied)
    //   - previous_kind='themerrdb' (functionally identical to
    //     DOWNLOAD TDB / RE-DOWNLOAD TDB / REPLACE TDB; worker
    //     ignores the captured URL and pulls themes.youtube_url
    //     either way, so REVERT was pure UI duplication)
    //   - no previous URL captured at all (legacy / never-changed)
    // Each case names the alternative action so the user has a
    // clear next step without trial and error.
    const currentCanonical = (ovr && ovr.youtube_url) || tdbUrl || '';
    let revertHint = '';
    if (!previousUrl && pu && pu.decision === 'accepted') {
      revertHint = "unavailable — no previous URL was captured.";
    } else if (previousUrl && previousUrl === currentCanonical) {
      revertHint = "unavailable — the previous URL is identical to what's currently applied, so reverting would just re-create the override at the same URL (no-op).";
    } else if (previousUrl && previousKind === 'themerrdb') {
      revertHint = "unavailable — the previous URL was a ThemerrDB URL, so reverting would just re-download the current themerrdb URL. Use DOWNLOAD TDB / RE-DOWNLOAD TDB / REPLACE TDB instead — they cover the same outcome with clearer intent.";
    }
    if (revertHint) {
      puBlock += `<dt class="muted">revert</dt>`
        + `<dd class="muted small">${revertHint}</dd>`;
    }
    const placedBlock = placements.length
      ? `<dt>placed in</dt><dd>${placements.map(p => `<div class="muted small">${htmlEscape(p.media_folder)} <span class="muted">(${htmlEscape(p.placement_kind)})</span></div>`).join('')}</dd>`
      : '';
    const dlBlock = lf
      ? `<dt>downloaded</dt><dd class="muted small">${htmlEscape(lf.abs_path || lf.file_path)} · ${fmt.num(lf.file_size)}B · ${htmlEscape(lf.provenance)}</dd>`
      : '';
    // v1.12.90: in-card audio player. The INFO dialog now serves
    // the canonical theme.mp3 via /api/items/{mt}/{tmdb}/theme.mp3
    // so users can preview what's actually playing without leaving
    // the dialog. Rendered only when there's a local_files row;
    // canonical_missing rows (dlBroken state) get the endpoint's
    // 410 — the <audio> element handles that gracefully (no-source
    // state). preload="metadata" so duration shows without
    // streaming the body until the user hits play.
    const audioBlock = lf && t.media_type !== undefined && t.tmdb_id !== undefined
      ? (() => {
          const sec = sectionId
            ? `?section_id=${encodeURIComponent(sectionId)}`
            : '';
          const src = `/api/items/${encodeURIComponent(t.media_type)}/${encodeURIComponent(t.tmdb_id)}/theme.mp3${sec}`;
          return `<dt>play</dt><dd><audio controls preload="metadata" src="${htmlEscape(src)}" class="info-audio">your browser doesn't support inline audio playback</audio></dd>`;
        })()
      : '';
    // v1.13.8 (#5): in-card preview of the TDB-suggested YouTube
    // URL — distinct from `play` above (which streams the on-disk
    // theme.mp3). The preview lets the user hear what motif WOULD
    // download via DOWNLOAD-FROM-TDB or what ACCEPT UPDATE would
    // pull in, before actually doing it.
    //
    // Suppression rules to keep the card clean:
    //   - hide when the row has no TDB video id (no upstream URL)
    //   - hide when on-disk file's source_video_id matches the TDB
    //     video id (would play the same content twice)
    // Result: T rows in steady state see only `play`; rows with a
    // pending TDB↑ update or rows without an on-disk file see the
    // preview as a distinct row.
    //
    // Click-to-load: the iframe stays absent until the user clicks
    // the button, so opening the dialog doesn't fire a YouTube
    // page request the user didn't ask for. Iframe params clip to
    // 30 seconds via start=0 + end=30; modestbranding=1 keeps the
    // YouTube logo subdued; autoplay=1 starts immediately on
    // click.
    const tdbVidId = t.youtube_video_id || null;
    const onDiskVidId = (lf && lf.source_video_id) || null;
    // v1.13.16: TDB preview removed (YouTube embed blocked for many
    // videos). Variable kept as empty string so the template
    // interpolation site stays unchanged; one less render branch.
    const tdbPreviewBlock = '';
    // v1.12.56: pending-update diff section. When an actionable
    // upstream-changed update is queued, show side-by-side tiles
    // (current vs proposed) so the user can pre-validate ACCEPT
    // UPDATE — thumbnails are static URLs, video titles hydrate
    // async from /api/youtube/oembed. Skipped for urls_match (URLs
    // are identical, no diff to display) and for non-pending
    // decisions (already accepted/declined).
    const diffSection = renderPendingUpdateDiff(pu, lf, t);
    // v1.12.71: TRY THIS NEXT recovery section. Renders only when
    // the row has an active failure_kind. Each suggested action is
    // a button that hooks into the existing data-act dispatch — no
    // new click plumbing. The list is fetched async; placeholder
    // rendered now and replaced once the fetch lands.
    const recoverySectionId = (t.failure_kind && t.media_type
                               && t.tmdb_id !== undefined)
      ? 'recovery-section'
      : null;
    const recoveryPlaceholder = recoverySectionId
      ? `<section id="${recoverySectionId}" class="recovery-section">
           <header class="recovery-section-head">
             <span class="recovery-section-title">// TRY THIS NEXT</span>
             <span class="muted small">loading recovery options…</span>
           </header>
         </section>`
      : '';
    // v1.12.66: per-row events timeline. The INFO endpoint already
    // returns the last 25 events for this (media_type, tmdb_id);
    // pre-fix the dialog discarded them. Surfacing the timeline
    // gives a "what happened to this row" debug log without needing
    // to grep the global LOGS tab. Collapsed by default to keep
    // the dialog scannable; expand-on-click for the deep dive.
    const events = (data.events || []);
    const historySection = renderRowHistory(events, t.media_type, t.tmdb_id);
    // v1.12.80: audit_events provenance section. Loaded
    // asynchronously after the dialog paints — keeps the initial
    // open snappy and the audit query off the synchronous /api/items
    // path. Empty placeholder swapped in by hydrateAuditSection when
    // the fetch lands; if the row has no audit history (older row
    // pre-v1.12.80), the section stays hidden.
    const auditPlaceholder = '<div id="audit-section-slot"></div>';
    // v1.13.3 (Issue 4): explicit section + edition tag right
    // under the title so the user doesn't have to infer 4K-vs-
    // Standard from the placement folder path. Renders as a
    // small chip row: e.g. [4K Movie] [edition: 4K] for the
    // 4K-section copy of Willy Wonka, or [Standard Movie] for
    // the std copy. Hidden when section_context is null
    // (legacy callers / orphan paths).
    const sc = data.section_context;
    let scopeChips = '';
    if (sc) {
      const scopeLbl = htmlEscape(sc.scope_label || '');
      const variantTone = sc.is_4k ? 'fourk' : 'standard';
      scopeChips = `<div class="info-scope-row">`
        + `<span class="info-scope-chip info-scope-chip-${variantTone}">${scopeLbl}</span>`
        + (sc.section_title
            ? `<span class="info-scope-chip info-scope-chip-section">${htmlEscape(sc.section_title)}</span>`
            : '')
        + (sc.edition
            ? `<span class="info-scope-chip info-scope-chip-edition">edition: ${htmlEscape(sc.edition)}</span>`
            : '')
        + `</div>`;
    }
    body.innerHTML = `
      <h3>${htmlEscape(t.title || '—')}${t.year ? ' (' + htmlEscape(t.year) + ')' : ''}</h3>
      ${scopeChips}
      <dl class="dlg-grid">
        <dt>imdb</dt><dd>${imdb}</dd>
        <dt>tmdb</dt><dd>${tmdbLink}</dd>
        <dt>upstream</dt><dd>${htmlEscape(t.upstream_source || '')}</dd>
        ${t.upstream_source === 'plex_orphan' ? '' : `<dt>themerrdb url</dt><dd>${tdbUrlLink}</dd>`}
        <dt>currently applied</dt><dd>${currentUrlLink}</dd>
        <dt>previous url</dt><dd>${previousUrlLink}</dd>
        <dt>video id</dt><dd>${htmlEscape(ytId || '—')}</dd>
        <dt>themerrdb added</dt><dd class="muted small">${htmlEscape(fmt.timeAuto(t.youtube_added_at))}</dd>
        <dt>themerrdb edited</dt><dd class="muted small">${htmlEscape(fmt.timeAuto(t.youtube_edited_at))}</dd>
        <dt>motif added</dt><dd class="muted small">${htmlEscape(fmt.timeAuto(data.motif_added_at))}</dd>
        <dt>motif edited</dt><dd class="muted small">${htmlEscape(fmt.timeAuto(data.motif_edited_at))}</dd>
        ${failBlock}
        ${ovrBlock}
        ${puBlock}
        ${dlBlock}
        ${placedBlock}
        ${audioBlock}
        ${tdbPreviewBlock}
      </dl>
      ${recoveryPlaceholder}
      ${diffSection}
      ${ytId ? `<div style="margin-top:18px;padding-top:14px;border-top:1px solid var(--line)">
        <a href="${htmlEscape(ytUrl)}" target="_blank" rel="noopener"
           style="display:block;text-decoration:none">
          <img src="https://img.youtube.com/vi/${htmlEscape(ytId)}/hqdefault.jpg"
               alt="YouTube thumbnail" loading="lazy"
               style="width:100%;max-width:480px;display:block;margin:0 auto;border:1px solid var(--line)" />
          <p class="muted small" style="margin-top:6px;text-align:center">
            ▸ click to watch on YouTube
          </p>
        </a>
      </div>` : ''}
      ${auditPlaceholder}
      ${historySection}
    `;
    // v1.12.101: thumbnail moved up above PROVENANCE / HISTORY (was
    // last in the card). Closing + reopening the dialog now starts
    // with PROVENANCE + HISTORY collapsed (the audit section's
    // `open` attribute was removed; HISTORY was already closed by
    // default), so the visible card height is bounded.
    // Hydrate diff-tile titles asynchronously (oEmbed proxy); no
    // await — failures fall back to bare video IDs already in the
    // markup. Runs after innerHTML so the DOM nodes exist.
    hydrateDiffTitles(body);
    // v1.12.71: hydrate the TRY THIS NEXT section if the row has
    // a failure. Best-effort — endpoint failure leaves the
    // placeholder copy ("loading recovery options…") in place.
    if (recoverySectionId) {
      // v1.13.35: pass section_id so the resolved-state lookup
      // is section-scoped — pre-fix a 4K-only adopt could flip
      // the standard section's info card to RESOLVED VIA ADOPT.
      hydrateRecoveryOptions(body, t.media_type, t.tmdb_id, sectionId);
    }
    // v1.12.83: wire the CLEAR button rendered into the // HISTORY
    // section by renderRowHistory. The PROVENANCE CLEAR is wired
    // inside hydrateAuditSection because that section is rendered
    // asynchronously into a placeholder slot; HISTORY is part of
    // the body's initial innerHTML so we hook it here.
    body.querySelectorAll('.info-clear-btn[data-clear="events"]').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        // stopPropagation so the click doesn't toggle the parent
        // <details> element open/closed via summary semantics.
        ev.preventDefault();
        ev.stopPropagation();
        handleInfoClear(ev.currentTarget, body);
      });
    });
    // v1.12.80: load the audit_events provenance log for this row
    // and render it into #audit-section-slot. Async so the dialog
    // paints first; failure leaves the slot empty (no error noise
    // since most rows won't have audit history yet).
    // v1.12.81: pass section_id so PROVENANCE renders only the
    // events for the row's section (plus title-global ones like
    // ADOPT). Pre-fix the standard and 4K cards showed identical
    // timelines.
    hydrateAuditSection(body, t.media_type, t.tmdb_id, sectionId);
  }

  // v1.12.80: fetch audit_events for a row and render a // PROVENANCE
  // section into the placeholder. Distinct from the // HISTORY
  // section which renders log_event rows (rolling, includes
  // sync/worker noise) — this section is the curated "who changed
  // what" feed sourced from audit_events.
  async function hydrateAuditSection(root, mediaType, tmdbId, sectionId) {
    if (!root || !mediaType || tmdbId === undefined) return;
    const slot = root.querySelector('#audit-section-slot');
    if (!slot) return;
    let data;
    try {
      const sec = sectionId
        ? `&section_id=${encodeURIComponent(sectionId)}`
        : '';
      data = await api('GET', `/api/items/${mediaType}/${tmdbId}/audit?limit=50${sec}`);
    } catch (_) {
      return;
    }
    const events = data?.events || [];
    if (!events.length) return;
    const fmtTs = (iso) => {
      if (!iso) return '—';
      try {
        const d = new Date(iso);
        if (Number.isNaN(d.getTime())) return iso;
        return d.toLocaleString();
      } catch (_) { return iso; }
    };
    const actionLabel = (a) => ({
      set_url: 'SET URL',
      clear_override: 'CLEAR OVERRIDE',
      clear_previous_url: 'CLEAR URL',
      accept_update: 'ACCEPT UPDATE',
      decline_update: 'KEEP CURRENT',
      revert: 'RESTORE / REVERT',
      purge: 'PURGE',
      unmanage: 'UNMANAGE',
      unplace: 'DEL',
      adopt: 'ADOPT',
      replace_with_themerrdb: 'REPLACE TDB',
      redownload: 'RE-DOWNLOAD',
      ack_failure: 'ACK FAILURE',
    }[a] || a.replace(/_/g, ' ').toUpperCase());
    const renderDetail = (d) => {
      if (!d || typeof d !== 'object') return '';
      const lines = [];
      if (d.old_url) lines.push(`old: ${d.old_url}`);
      if (d.new_url) lines.push(`new: ${d.new_url}`);
      if (d.restored_url) lines.push(`restored: ${d.restored_url}`);
      const extras = Object.entries(d)
        .filter(([k]) => !['old_url', 'new_url', 'restored_url'].includes(k))
        .filter(([, v]) => v !== null && v !== undefined && v !== '')
        .map(([k, v]) => `${k}: ${typeof v === 'object' ? JSON.stringify(v) : v}`);
      lines.push(...extras);
      return lines.length
        ? `<pre class="history-detail">${htmlEscape(lines.join('\n'))}</pre>`
        : '';
    };
    const rows = events.map((e) => {
      const sec = e.section_id ? ` <span class="muted small">[section ${htmlEscape(e.section_id)}]</span>` : '';
      return `
        <div class="history-row">
          <div class="history-row-head">
            <span class="history-ts">${htmlEscape(fmtTs(e.occurred_at))}</span>
            <span class="history-level history-level-info">${htmlEscape(actionLabel(e.action))}</span>
            <span class="history-component muted small">${htmlEscape(e.actor || '')}${sec}</span>
          </div>
          ${renderDetail(e.details)}
        </div>
      `;
    }).join('');
    // v1.12.83: per-row CLEAR button. Useful when iterating during
    // testing — repeated SET URL / ACCEPT UPDATE / PURGE cycles can
    // bloat PROVENANCE to dozens of entries that aren't useful for
    // future debugging. data-attrs let the dispatcher know which
    // (media_type, tmdb_id, section_id) tuple to delete.
    const clearBtn = `<button type="button" class="btn btn-tiny btn-danger info-clear-btn"
      data-clear="audit"
      data-mt="${htmlEscape(mediaType)}"
      data-id="${htmlEscape(tmdbId)}"
      data-section-id="${htmlEscape(sectionId || '')}"
      title="Delete every PROVENANCE entry for this row${sectionId ? ' (this section + title-global rows)' : ''}.">CLEAR</button>`;
    slot.innerHTML = `
      <details class="history-section" data-info-section="audit">
        <summary>
          <span class="history-section-title">// PROVENANCE</span>
          <span class="muted small">${events.length} audited change${events.length === 1 ? '' : 's'}</span>
          ${clearBtn}
        </summary>
        <div class="history-body">${rows}</div>
      </details>
    `;
    slot.querySelector('.info-clear-btn')?.addEventListener('click', (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      handleInfoClear(ev.currentTarget, root);
    });
  }

  // v1.12.83: shared dispatcher for the // HISTORY and // PROVENANCE
  // CLEAR buttons. Confirms with the user, fires the DELETE, then
  // re-hydrates just the affected section so the dialog stays open.
  // v1.12.88: target the correct section via [data-info-section]
  // attribute. Pre-fix the selector was `:not(.history-section-audit)`
  // looking for a class that was never set, so HISTORY clear matched
  // the FIRST .history-section in DOM order — which was actually
  // the audit section's slot. Result: HISTORY stayed visible, audit
  // section got replaced by an empty placeholder, and the audit
  // CLEAR button was lost (looked locked from the user's POV).
  async function handleInfoClear(btn, root) {
    const which = btn.dataset.clear;  // 'audit' | 'events'
    const mt = btn.dataset.mt;
    const id = btn.dataset.id;
    const sid = btn.dataset.sectionId || null;
    const label = which === 'audit' ? 'PROVENANCE' : 'HISTORY';
    if (!confirm(`Clear ${label} for this row?\n\nThis cannot be undone.`)) return;
    btn.disabled = true;
    try {
      // v1.12.119: HISTORY clear is now per-section too. Pre-fix the
      // events DELETE was title-wide, so clearing on the standard's
      // INFO card wiped the 4K's history (different libraries / editions
      // despite the shared tmdb_id). audit was already per-section
      // since v1.12.83; this aligns history with it.
      const sec = sid
        ? `?section_id=${encodeURIComponent(sid)}`
        : '';
      const path = which === 'audit'
        ? `/api/items/${mt}/${id}/audit${sec}`
        : `/api/items/${mt}/${id}/events${sec}`;
      await api('DELETE', path);
      if (which === 'audit') {
        // Re-hydrate the audit slot. With zero events the slot
        // ends up empty (hydrateAuditSection bails on no events),
        // which is the correct visual signal.
        const slot = root.querySelector('#audit-section-slot');
        if (slot) slot.innerHTML = '';
        hydrateAuditSection(root, mt, id, sid || undefined);
      } else {
        // Replace the HISTORY <details> in place with a "cleared"
        // marker. No CLEAR button needed — there's nothing to clear.
        const node = root.querySelector('details[data-info-section="history"]');
        if (node) {
          node.outerHTML = '<details class="history-section history-section-empty"><summary><span class="history-section-title">// HISTORY</span><span class="muted small">cleared</span></summary></details>';
        }
      }
    } catch (e) {
      alert(`Clear ${label} failed: ${e.message}`);
      btn.disabled = false;
    }
  }

  // v1.12.71: fetch the recovery options for a failed row and
  // render them as a vertical action list inside the existing
  // placeholder. Each option is a button that fires the same
  // data-act dispatch as the SOURCE menu items, so click handling
  // is shared. Disabled options (e.g. RE-DOWNLOAD when cookies
  // aren't present for cookies_expired) render greyed with the
  // disabled_reason as the tooltip.
  async function hydrateRecoveryOptions(root, mediaType, tmdbId, sectionId) {
    if (!root || !mediaType || tmdbId === undefined) return;
    const section = root.querySelector('#recovery-section');
    if (!section) return;
    let data;
    try {
      // v1.13.35: append section_id so the server's locally-resolved
      // detection scopes to this row's section. Optional — when
      // omitted the server falls back to title-global lookup for
      // backward compatibility with callers that pre-date the
      // section-scope addition.
      const sec = sectionId
        ? `?section_id=${encodeURIComponent(sectionId)}`
        : '';
      data = await api(
        'GET',
        `/api/items/${encodeURIComponent(mediaType)}/${encodeURIComponent(tmdbId)}/recovery-options${sec}`,
      );
    } catch (_) {
      section.querySelector('.muted').textContent = 'recovery options unavailable';
      return;
    }
    if (!data || !data.failure_kind || !(data.options || []).length) {
      section.remove();
      return;
    }
    const human = data.human || data.failure_kind;
    // v1.13.33: locally-resolved rows annotate the dead TDB URL row
    // at the top of the dialog so the user sees the axis state at a
    // glance — the title swap on the section header below carries
    // the rest of the signal.
    if (data.resolved) {
      section.classList.add('recovery-section-resolved');
      try {
        const dt = root.querySelectorAll('dt');
        for (const node of dt) {
          if ((node.textContent || '').trim().toLowerCase() === 'themerrdb url') {
            const dd = node.nextElementSibling;
            if (dd && !dd.querySelector('.tdb-unavailable-badge')) {
              const badge = document.createElement('span');
              badge.className = 'tdb-unavailable-badge muted small';
              badge.textContent = ' (unavailable — using local source)';
              dd.appendChild(badge);
            }
            break;
          }
        }
      } catch (_) { /* annotation is cosmetic — never fail the render */ }
    }
    // v1.12.92: render the acked note on its own line below the
    // header instead of inlining it next to the human label. The
    // inline version made the header wrap to two lines after ACK,
    // which jumped the buttons down. The dedicated line keeps the
    // header height constant pre/post ack. The line is always in
    // the DOM (empty when not acked) so the section's vertical
    // size doesn't shift either.
    const ackedNoteLine = data.acked
      ? '<p class="recovery-section-note muted small">failure acknowledged — these options stay available until upstream changes</p>'
      : '<p class="recovery-section-note recovery-section-note-empty"></p>';
    const items = data.options.map((opt) => {
      const tone = opt.tone ? ` lib-source-${opt.tone}` : '';
      const disabledAttr = opt.disabled ? 'disabled' : '';
      const disabledClass = opt.disabled ? ' recovery-option-disabled' : '';
      const tooltip = opt.disabled && opt.disabled_reason
        ? `${opt.tooltip || ''}\n\n(disabled: ${opt.disabled_reason})`
        : (opt.tooltip || '');
      // 'info' actions are non-interactive hints (e.g. "drop a
      // cookies.txt"); render as a styled tile rather than a button
      // so the user doesn't expect a click to do something.
      if (!opt.interactive) {
        return `<div class="recovery-option recovery-option-info${disabledClass}"
                     title="${htmlEscape(tooltip)}">
          <span class="recovery-option-label">${htmlEscape(opt.label)}</span>
          <span class="recovery-option-tip muted small">${htmlEscape(opt.tooltip || '')}</span>
        </div>`;
      }
      // Interactive option — render as a button with the same
      // data-act / data-mt / data-id / data-rk attributes the
      // SOURCE menu uses. Clicking dispatches into the existing
      // library click handler if the row is on-page; otherwise
      // we fall back to closing the dialog and navigating.
      return `<button type="button"
                      class="btn btn-tiny${tone} recovery-option-btn${disabledClass}"
                      data-act="${htmlEscape(opt.action)}"
                      data-mt="${htmlEscape(mediaType)}"
                      data-id="${htmlEscape(tmdbId)}"
                      data-recovery="1"
                      ${disabledAttr}
                      title="${htmlEscape(tooltip)}">
        ${htmlEscape(opt.label)}
      </button>`;
    }).join('');
    // v1.13.33: title swaps when locally-resolved so the user reads
    // "this row is fine, the upstream is just dead" instead of "act
    // now". Body text + options already adapted server-side.
    const sectionTitleText = data.resolved
      ? '✓ RESOLVED — TDB UNAVAILABLE'
      : '// TRY THIS NEXT';
    section.innerHTML = `
      <header class="recovery-section-head">
        <span class="recovery-section-title">${htmlEscape(sectionTitleText)}</span>
        <span class="muted small">${htmlEscape(human)}</span>
      </header>
      ${ackedNoteLine}
      <div class="recovery-section-body">${items}</div>
    `;
    // v1.12.71: dispatch recovery-button clicks. Each button's
    // data-act matches an existing handler keyword:
    //   redl, clear-failure → direct API call (mt + tmdb)
    //   manual-url, upload-theme → open the corresponding dialog
    //     (needs rating_key from the API response)
    // Closing the INFO dialog after dispatch lets the user see the
    // result on the row without manually closing.
    const ratingKey = data.rating_key || null;
    section.querySelectorAll('button.recovery-option-btn').forEach((btn) => {
      btn.addEventListener('click', async (ev) => {
        ev.stopPropagation();
        const act = btn.dataset.act;
        const mt = btn.dataset.mt;
        const id = btn.dataset.id;
        const closeAndReload = () => {
          closeInfoDialog();
          libraryRapidPoll();
          loadLibrary().catch(() => {});
        };
        try {
          if (act === 'redl') {
            await api('POST', `/api/items/${mt}/${id}/redownload`);
            closeAndReload();
          } else if (act === 'clear-failure') {
            // v1.12.88: ACK from INFO doesn't close the dialog —
            // re-render the recovery section in place so the user
            // can keep reading the failure context + recovery
            // options (now without the ACK FAILURE button, which
            // is filtered out server-side once acked). Topbar dot
            // refresh kicks via the standard helper.
            // v1.12.92: don't innerHTML='' before the re-fetch.
            // hydrateRecoveryOptions overwrites innerHTML once the
            // new data arrives, so the section's old content stays
            // visible during the ~100ms fetch instead of collapsing
            // to empty and re-painting (which felt jarring). Also
            // kick loadLibrary so the row's red ! glyph clears
            // without requiring a refresh / external click.
            await api('POST', `/api/items/${mt}/${id}/clear-failure`);
            // v1.13.35: forward sectionId on re-hydrate so the
            // resolved-state lookup stays section-scoped after
            // ACK FAILURE.
            await hydrateRecoveryOptions(root, mt, id, sectionId);
            refreshTopbarStatus().catch(() => {});
            loadLibrary().catch(() => {});
          } else if (act === 'manual-url') {
            if (!ratingKey) {
              alert('No rating_key available for this row — open SET URL from the row\'s SOURCE menu.');
              return;
            }
            closeInfoDialog();
            // Resolve title/year from the dialog's stale data;
            // use bare strings since the dialog is closing.
            openManualUrlDialog({
              ratingKey,
              title: '',
              year: '',
              tdbUrl: '',
              srcLetter: '',
            });
          } else if (act === 'upload-theme') {
            if (!ratingKey) {
              alert('No rating_key available for this row — open UPLOAD MP3 from the row\'s SOURCE menu.');
              return;
            }
            closeInfoDialog();
            openUploadDialog({ ratingKey, title: '', year: '' });
          }
        } catch (err) {
          alert('Recovery action failed: ' + err.message);
        }
      });
    });
  }

  // v1.12.66: per-row events timeline. Renders the last 25 events
  // for this row (already loaded by /api/items/{mt}/{tmdb}) as a
  // collapsed details popover. Each line shows local-formatted
  // timestamp · level · component · message, color-coded by level.
  // Detail JSON is rendered as a nested <pre> when present, so a
  // failed download's raw error or a sync's index/payload counts
  // are inspectable without leaving the dialog.
  function renderRowHistory(events, mediaType, tmdbId) {
    if (!events || !events.length) return '';
    const fmtTs = (iso) => {
      if (!iso) return '—';
      try {
        const d = new Date(iso);
        if (Number.isNaN(d.getTime())) return iso;
        return d.toLocaleString();
      } catch (_) {
        return iso;
      }
    };
    const levelClass = (level) => {
      const l = (level || '').toUpperCase();
      if (l === 'ERROR') return 'history-level-error';
      if (l === 'WARNING' || l === 'WARN') return 'history-level-warn';
      return 'history-level-info';
    };
    const rows = events.map((e) => {
      const detail = (() => {
        if (!e.detail) return '';
        try {
          const parsed = typeof e.detail === 'string' ? JSON.parse(e.detail) : e.detail;
          const pretty = JSON.stringify(parsed, null, 2);
          return `<pre class="history-detail">${htmlEscape(pretty)}</pre>`;
        } catch (_) {
          return `<pre class="history-detail">${htmlEscape(String(e.detail))}</pre>`;
        }
      })();
      return `
        <div class="history-row">
          <div class="history-row-head">
            <span class="history-ts">${htmlEscape(fmtTs(e.ts))}</span>
            <span class="history-level ${levelClass(e.level)}">${htmlEscape((e.level || '').toUpperCase())}</span>
            <span class="history-component muted small">${htmlEscape(e.component || '')}</span>
          </div>
          <div class="history-msg">${htmlEscape(e.message || '')}</div>
          ${detail}
        </div>
      `;
    }).join('');
    // v1.12.83: per-row CLEAR button. The HISTORY section sources
    // from the rolling events table which has no section_id column,
    // so this clears across every section that owns the title.
    // Useful while testing — repeated cycles can bloat HISTORY with
    // worker / sync chatter that no longer matters. Click handler
    // is wired by the openInfoDialog dispatcher (data-clear="events").
    const clearBtn = (mediaType !== undefined && tmdbId !== undefined)
      ? `<button type="button" class="btn btn-tiny btn-danger info-clear-btn"
          data-clear="events"
          data-mt="${htmlEscape(mediaType)}"
          data-id="${htmlEscape(tmdbId)}"
          title="Delete history for this row.">CLEAR</button>`
      : '';
    return `
      <details class="history-section" data-info-section="history">
        <summary>
          <span class="history-section-title">// HISTORY</span>
          <span class="muted small">${events.length} event${events.length === 1 ? '' : 's'} · click to expand</span>
          ${clearBtn}
        </summary>
        <div class="history-body">${rows}</div>
      </details>
    `;
  }

  // v1.12.56: render the side-by-side diff for an actionable
  // pending update. Returns '' (no section) when there's nothing
  // to show. Tiles ship with thumbnail src baked in (constructed
  // from video ID — works for any public YouTube video) and a
  // title placeholder that hydrateDiffTitles fills in via the
  // server oEmbed proxy.
  function renderPendingUpdateDiff(pu, lf, t) {
    if (!pu || pu.decision !== 'pending') return '';
    if (pu.kind === 'urls_match') return '';
    const newUrl = pu.new_youtube_url || '';
    const newVid = extractYouTubeVideoId(newUrl);
    if (!newVid) return '';
    const currentVid = (lf && lf.source_video_id)
      || extractYouTubeVideoId(pu.old_youtube_url || '')
      || '';
    const currentUrl = currentVid
      ? `https://www.youtube.com/watch?v=${currentVid}`
      : (pu.old_youtube_url || '');
    const tile = (label, vid, url, slot, accent) => {
      if (!vid) {
        return `<div class="diff-tile diff-tile-empty">
          <div class="diff-tile-label" style="color:${accent}">${htmlEscape(label)}</div>
          <p class="muted small">no recorded video</p>
        </div>`;
      }
      return `<div class="diff-tile">
        <div class="diff-tile-label" style="color:${accent}">${htmlEscape(label)}</div>
        <a href="${htmlEscape(url)}" target="_blank" rel="noopener"
           class="diff-tile-thumb-link">
          <img src="https://img.youtube.com/vi/${htmlEscape(vid)}/hqdefault.jpg"
               alt="" loading="lazy" class="diff-tile-thumb" />
        </a>
        <div class="diff-tile-title" data-oembed-slot="${htmlEscape(slot)}"
             data-oembed-url="${htmlEscape(url)}">
          <span class="muted small">${htmlEscape(vid)}</span>
        </div>
      </div>`;
    };
    return `
      <section class="diff-section">
        <header class="diff-section-head">
          <span class="diff-section-title">// PROPOSED CHANGE</span>
          <span class="muted small">ACCEPT UPDATE will replace the left video with the right.</span>
        </header>
        <div class="diff-tiles">
          ${tile('CURRENT', currentVid, currentUrl, 'current', 'var(--violet)')}
          <div class="diff-arrow" aria-hidden="true">→</div>
          ${tile('PROPOSED', newVid, newUrl, 'proposed', 'var(--blue)')}
        </div>
      </section>
    `;
  }

  // v1.12.56: walk the diff-tile slots in `root` and replace each
  // placeholder span with the oEmbed video title. Best-effort —
  // a 404 from the proxy (private/removed/geo-blocked video)
  // leaves the bare-vid placeholder in place, so the UI never
  // empties out on lookup failure.
  async function hydrateDiffTitles(root) {
    if (!root) return;
    const slots = root.querySelectorAll('[data-oembed-slot]');
    await Promise.all(Array.from(slots).map(async (slot) => {
      const url = slot.getAttribute('data-oembed-url');
      if (!url) return;
      try {
        const data = await api(
          'GET',
          `/api/youtube/oembed?url=${encodeURIComponent(url)}`,
        );
        if (data && data.title) {
          const author = data.author_name
            ? `<span class="muted small">${htmlEscape(data.author_name)}</span>`
            : '';
          slot.innerHTML = `<div>${htmlEscape(data.title)}</div>${author}`;
        }
      } catch (_) {
        // Leave placeholder; vid is already shown.
      }
    }));
  }

  function closeInfoDialog() {
    const dlg = document.getElementById('info-dlg');
    if (!dlg) return;
    // v1.12.73: clear focus from any focused element inside the
    // dialog before closing. Pre-fix, clicking the X (or Esc)
    // closed the dialog with the X button still :focus, so when
    // the dialog re-opened next time the X carried a stale cyan
    // focus-visible outline. Calling blur() releases focus
    // cleanly; the host page's <body> takes focus instead.
    const focused = dlg.querySelector(':focus');
    if (focused && typeof focused.blur === 'function') focused.blur();
    // v1.12.98: pause + reset any audio elements inside the dialog
    // before closing. Pre-fix the v1.12.90 in-card preview kept
    // playing after Esc/× because the <audio> element survived in
    // the detached DOM until the next open re-rendered it. The user
    // had no way to stop it short of reopening + scrubbing to the
    // end. pause() halts playback; setting currentTime=0 rewinds
    // so the next open starts fresh (also matches the user's
    // mental model that closing the card = "I'm done with this").
    dlg.querySelectorAll('audio').forEach((el) => {
      try {
        el.pause();
        el.currentTime = 0;
      } catch (_) { /* defensive — element may already be torn down */ }
    });
    if (typeof dlg.close === 'function') dlg.close();
    else dlg.removeAttribute('open');
  }

  function bindInfoDialog() {
    const dlg = document.getElementById('info-dlg');
    if (!dlg) return;
    document.getElementById('info-dlg-close')?.addEventListener('click', closeInfoDialog);
    // v1.13.16: TDB preview removed. YouTube blocks the embed for
    // many videos (Error 153 / video player configuration error)
    // and there's no clean alternative without violating ToS. The
    // INFO card's on-disk play already gives the user audio
    // verification of motif's canonical; the TDB URL is one click
    // away in any browser if they want to listen before accepting.
  }


  // ---- Manual YouTube URL dialog (Coverage tab) ----

  // v1.12.54: extract the canonical YouTube video ID from a URL
  // for SET URL match detection. Mirrors the server's
  // extract_video_id (downloader.py): handles watch?v=, youtu.be/,
  // shorts/. Returns null on no-match. Used to compare the user's
  // input to the row's TDB URL so the dialog can warn before the
  // user pins an identical URL as a manual override.
  function extractYouTubeVideoId(url) {
    if (!url) return null;
    const m = String(url).match(
      /(?:youtube\.com\/watch\?(?:[^&]*&)*v=|youtu\.be\/|youtube\.com\/shorts\/)([A-Za-z0-9_-]{6,})/i
    );
    return m ? m[1] : null;
  }

  function openManualUrlDialog({ ratingKey, title, year, tdbUrl, appliedUrl, srcLetter }) {
    const dlg = document.getElementById('manual-url-dlg');
    if (!dlg) return;
    document.getElementById('manual-url-rk').value = ratingKey;
    // v1.12.54: stash the row's TDB URL on the dialog element so
    // the input-listener (bound once at page load) can read it
    // each open without rebinding. dataset persists until the
    // next openManualUrlDialog overwrites it.
    // v1.12.62: also stash srcLetter so the match-warning can
    // branch its copy — '-' rows have no file to re-download
    // (the alternative action is DOWNLOAD TDB, not RE-DOWNLOAD).
    // v1.12.107: stash appliedUrl too so the warning can fire
    // when the user is about to set the SAME URL that's already
    // applied — covers the U-overrides-itself case (typing the
    // same user URL again is a guaranteed no-op).
    dlg.dataset.tdbUrl = tdbUrl || '';
    dlg.dataset.appliedUrl = appliedUrl || '';
    dlg.dataset.srcLetter = srcLetter || '';
    const meta = document.getElementById('manual-url-dlg-meta');
    const ylabel = year ? ` (${htmlEscape(year)})` : '';
    meta.innerHTML = `<p class="muted">// ${htmlEscape((title || 'untitled').toUpperCase())}${ylabel}</p>`;
    document.getElementById('manual-url-input').value = '';
    document.getElementById('manual-url-status').textContent = '';
    // v1.12.54: clear any leftover match-hint from a previous open.
    const hint = document.getElementById('manual-url-match-hint');
    if (hint) { hint.style.display = 'none'; hint.textContent = ''; }
    // v1.12.97: reset the live preview block so a stale thumbnail
    // from the previous open doesn't flash before the user types.
    const preview = document.getElementById('manual-url-preview');
    if (preview) preview.hidden = true;
    if (typeof dlg.showModal === 'function') dlg.showModal();
    else dlg.setAttribute('open', '');
  }

  function closeManualUrlDialog() {
    const dlg = document.getElementById('manual-url-dlg');
    if (!dlg) return;
    if (typeof dlg.close === 'function') dlg.close();
    else dlg.removeAttribute('open');
  }

  function bindManualUrlDialog() {
    const dlg = document.getElementById('manual-url-dlg');
    if (!dlg) return;
    document.getElementById('manual-url-dlg-close')?.addEventListener('click', closeManualUrlDialog);
    document.getElementById('manual-url-cancel')?.addEventListener('click', closeManualUrlDialog);
    // v1.12.54: live match-warning. Whenever the user pauses
    // typing, compare the input's video ID to the row's TDB URL
    // (stored on dlg.dataset.tdbUrl by openManualUrlDialog). If
    // they match, surface an inline hint that pinning the same
    // URL as an override creates a U row that the next sync
    // (v1.12.53) will surface as a "convert U → T" prompt anyway.
    // Pure UX nudge — submit is still allowed.
    const input = document.getElementById('manual-url-input');
    const hint = document.getElementById('manual-url-match-hint');
    if (input && hint) {
      let matchTimer = null;
      const checkMatch = () => {
        const tdbUrl = dlg.dataset.tdbUrl || '';
        const appliedUrl = dlg.dataset.appliedUrl || '';
        const userVid = extractYouTubeVideoId(input.value.trim());
        if (!userVid) {
          hint.style.display = 'none';
          return;
        }
        // v1.12.107: two match cases land different copy.
        // 1) Input == currently-applied URL → no-op set. Most
        //    actionable case for the user — they'd be replacing
        //    a URL with itself. Suggest DOWNLOAD/RE-DOWNLOAD as
        //    the action they probably wanted.
        // 2) Input == TDB URL (and the row's TDB URL differs from
        //    the applied URL — i.e. they're not setting their
        //    own existing override) → U→T conversion case the
        //    v1.12.54 hint covered.
        // The applied-URL match is checked first because it's the
        // strict superset on T rows (where applied == TDB) but
        // distinct on U rows.
        const appliedVid = extractYouTubeVideoId(appliedUrl);
        const tdbVid = extractYouTubeVideoId(tdbUrl);
        const srcLetter = dlg.dataset.srcLetter || '';
        if (appliedVid && userVid === appliedVid) {
          const altAction = (srcLetter === '-') ? 'DOWNLOAD TDB' : 'RE-DOWNLOAD TDB';
          const altIntent = (srcLetter === '-')
            ? 'fetch from ThemerrDB'
            : 'refresh the file';
          // Differentiate U-row vs T-row copy. On a U row the
          // applied URL IS the user override, so the user is
          // about to set it to the same thing — pure no-op. On
          // a T row applied == TDB, so the U→T-conversion warning
          // is more accurate.
          if (appliedVid !== tdbVid) {
            // Pure U-overrides-itself case.
            hint.textContent = 'This URL matches the row\'s currently-applied user URL. '
              + 'Setting it again would be a no-op — no override change, no download. '
              + `Use ${altAction} if you want to ${altIntent}.`;
          } else {
            // Applied == TDB (T row).
            hint.textContent = 'This URL matches the current ThemerrDB URL. '
              + 'Setting it as a manual override pins the row as U-source — '
              + 'the next sync will surface a "convert U → T" prompt anyway. '
              + `Use ${altAction} instead if you just want to ${altIntent}.`;
          }
          hint.style.display = '';
        } else if (tdbVid && userVid === tdbVid) {
          // Input matches TDB but not applied — user is on a U
          // row and is about to flip back to TDB-by-URL. Same
          // U→T-conversion advice as before.
          const altAction = (srcLetter === '-') ? 'DOWNLOAD TDB' : 'RE-DOWNLOAD TDB';
          const altIntent = (srcLetter === '-')
            ? 'fetch from ThemerrDB'
            : 'refresh the file';
          hint.textContent = 'This URL matches the current ThemerrDB URL. '
            + 'Setting it as a manual override pins the row as U-source — '
            + 'the next sync will surface a "convert U → T" prompt anyway. '
            + `Use ${altAction} instead if you just want to ${altIntent}.`;
          hint.style.display = '';
        } else {
          hint.style.display = 'none';
        }
      };
      input.addEventListener('input', () => {
        clearTimeout(matchTimer);
        matchTimer = setTimeout(checkMatch, 250);
        // v1.12.97: live preview alongside the match-hint. Same
        // debounce window so we don't hit oembed on every keystroke.
        clearTimeout(previewTimer);
        previewTimer = setTimeout(updatePreview, 350);
      });
    }
    // v1.12.97: live YouTube preview. Watches the input and renders
    // the thumbnail + oembed-fetched title in the dialog as the user
    // types/pastes a URL. Each new video ID kicks a fresh oembed
    // fetch (debounced, deduped against the most recent ID). Falls
    // back to "no title (private/removed/geo-blocked)" when oembed
    // 404s — same fallback the v1.12.56 PROPOSED CHANGE diff uses.
    const preview = document.getElementById('manual-url-preview');
    const previewThumb = document.getElementById('manual-url-preview-thumb');
    const previewLink = document.getElementById('manual-url-preview-link');
    const previewTitle = document.getElementById('manual-url-preview-title');
    const previewVid = document.getElementById('manual-url-preview-vid');
    let previewTimer = null;
    let previewLastVid = null;
    async function updatePreview() {
      if (!input || !preview) return;
      const vid = extractYouTubeVideoId(input.value.trim());
      if (!vid) {
        preview.hidden = true;
        previewLastVid = null;
        return;
      }
      if (vid === previewLastVid) return;
      previewLastVid = vid;
      const url = `https://www.youtube.com/watch?v=${encodeURIComponent(vid)}`;
      previewThumb.src = `https://img.youtube.com/vi/${encodeURIComponent(vid)}/hqdefault.jpg`;
      previewLink.href = url;
      previewTitle.textContent = 'loading title…';
      previewVid.textContent = vid;
      preview.hidden = false;
      try {
        const data = await api(
          'GET',
          `/api/youtube/oembed?url=${encodeURIComponent(url)}`,
        );
        // Race guard: another keystroke may have changed previewLastVid
        // before this fetch resolved; bail if we're stale.
        if (vid !== previewLastVid) return;
        previewTitle.textContent = data?.title || vid;
        if (data?.author_name) {
          previewTitle.textContent += ` · ${data.author_name}`;
        }
      } catch (_) {
        if (vid !== previewLastVid) return;
        previewTitle.textContent = 'no title — video may be private, removed, or geo-blocked';
      }
    }
    const form = document.getElementById('manual-url-form');
    form?.addEventListener('submit', async (e) => {
      e.preventDefault();
      const status = document.getElementById('manual-url-status');
      const rk = document.getElementById('manual-url-rk').value;
      const url = document.getElementById('manual-url-input').value.trim();
      if (!YOUTUBE_URL_RE.test(url)) {
        status.textContent = '✗ enter a valid YouTube URL';
        status.classList.remove('ok'); status.classList.add('err');
        return;
      }
      status.textContent = 'saving…';
      status.classList.remove('err', 'ok');
      try {
        await api('POST', `/api/plex_items/${encodeURIComponent(rk)}/manual-url`,
                  { youtube_url: url });
        status.textContent = '✓ saved · download queued';
        status.classList.add('ok');
        setTimeout(() => {
          closeManualUrlDialog();
          loadLibrary().catch(()=>{});
          libraryRapidPoll();  // catch the download → place transitions
        }, 700);
      } catch (err) {
        status.textContent = '✗ ' + err.message;
        status.classList.add('err');
      }
    });
  }

  // ---- Manual upload dialog ----

  function openUploadDialog({ ratingKey, title, year }) {
    const dlg = document.getElementById('upload-dlg');
    if (!dlg) return;
    document.getElementById('upload-rk').value = ratingKey;
    const meta = document.getElementById('upload-dlg-meta');
    const ylabel = year ? ` (${htmlEscape(year)})` : '';
    meta.innerHTML = `<p class="muted">// ${htmlEscape((title || 'untitled').toUpperCase())}${ylabel}</p>`;
    document.getElementById('upload-file').value = '';
    document.getElementById('upload-status').textContent = '';
    if (typeof dlg.showModal === 'function') dlg.showModal();
    else dlg.setAttribute('open', '');
  }

  function closeUploadDialog() {
    const dlg = document.getElementById('upload-dlg');
    if (!dlg) return;
    if (typeof dlg.close === 'function') dlg.close();
    else dlg.removeAttribute('open');
  }

  function bindUploadDialog() {
    const dlg = document.getElementById('upload-dlg');
    if (!dlg) return;
    document.getElementById('upload-dlg-close')?.addEventListener('click', closeUploadDialog);
    document.getElementById('upload-cancel')?.addEventListener('click', closeUploadDialog);
    const form = document.getElementById('upload-form');
    form?.addEventListener('submit', async (e) => {
      e.preventDefault();
      const status = document.getElementById('upload-status');
      const rk = document.getElementById('upload-rk').value;
      const fileEl = document.getElementById('upload-file');
      const file = fileEl.files && fileEl.files[0];
      if (!file) {
        status.textContent = '✗ choose a file first';
        status.classList.add('err');
        return;
      }
      status.textContent = 'uploading…';
      status.classList.remove('err', 'ok');
      const fd = new FormData();
      fd.append('file', file);
      try {
        const r = await fetch(`/api/plex_items/${encodeURIComponent(rk)}/upload-theme`, {
          method: 'POST', body: fd,
        });
        if (!r.ok) {
          const t = await r.text().catch(() => '');
          throw new Error(`${r.status}: ${t || r.statusText}`);
        }
        status.textContent = '✓ uploaded · placement queued';
        status.classList.add('ok');
        setTimeout(() => {
          closeUploadDialog();
          loadLibrary().catch(()=>{});
          libraryRapidPoll();  // watch the place job land
        }, 700);
      } catch (err) {
        status.textContent = '✗ ' + err.message;
        status.classList.add('err');
      }
    });
  }


  // ---- Bootstrap ----

  // v1.13.8 (#8): self-update notifier. /api/release/latest reads
  // a daily-cached payload populated by the scheduler's
  // _check_release_update job. When the cached latest tag parses
  // higher than the running version, reveal a small "→ vX.Y.Z"
  // suffix next to the brand-version pill. Click opens the GitHub
  // release page in a new tab via the static href in base.html.
  // Polled once at page load — release availability changes on the
  // order of days, no need for repeat checks per session.
  async function checkSelfUpdate() {
    const el = document.getElementById('brand-update-suffix');
    if (!el) return;
    try {
      const r = await api('GET', '/api/release/latest');
      if (r && r.update_available && r.latest) {
        el.textContent = `→ ${r.latest}`;
        if (r.html_url) el.href = r.html_url;
        el.title = `motif ${r.latest} is available `
          + `(running ${r.current}). Click to view release notes.`;
        el.hidden = false;
      } else {
        el.hidden = true;
      }
    } catch (_) {
      el.hidden = true;
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    highlightNav();
    refreshTopbarStatus();
    checkSelfUpdate().catch(()=>{});

    // v1.11.73: clicking the topbar dot/text when red navigates to
    // the failed-jobs filter on /queue. The data-failed-link attr
    // is toggled by refreshTopbarStatus based on q.failed > 0.
    document.getElementById('topbar-status')?.addEventListener('click', (e) => {
      // Don't hijack clicks on the logout link or update/failure badges
      // they have their own hrefs.
      if (e.target.closest('a')) return;
      const el = e.currentTarget;
      if (el.getAttribute('data-failed-link') === '1') {
        window.location.href = '/queue?status=failed';
      }
    });

    // v1.11.40: relaxed from 15s → 30s. /api/stats is now cached
    // 1s server-side (v1.11.37) and event-driven refreshes fire
    // immediately on sync/refresh button clicks, so background
    // polling can be less aggressive without losing responsiveness.
    // v1.12.81: dropped from 30s → 10s. Pre-fix the topbar's UPD /
    // FAIL badges and idle-dot color could sit stale for up to 30s
    // after a sync wrote new pending_updates rows or a worker job
    // finished. The new cadence matches the /queue page's job poll
    // so users watching LOGS no longer see "refresh job done" and
    // then wait another half-minute for the topbar to catch up.
    // Tripled traffic but the response is small (one cached query
    // bundle) and the per-action `setTimeout(refreshTopbarStatus,
    // 1100)` calls remain so explicit clicks still feel immediate.
    setInterval(refreshTopbarStatus, 10000);

    // v1.12.108: when the ops mini-bar transitions running → idle
    // (or vice versa), force a stats refresh right away. Pre-fix
    // the legacy "REFRESHING TV SHOWS…" text + yellow dot lingered
    // for up to 10s after the sync actually finished because they
    // were driven by /api/stats's own poll cadence — meanwhile
    // the ops mini-bar disappeared on the next ops poll (1s).
    // Same lag in reverse on sync start. Tying the two state
    // surfaces together makes them appear/disappear together.
    window.addEventListener('motif:ops-state-changed', () => {
      refreshTopbarStatus().catch(() => {});
    });

    bindDashboard();
    bindBrowse();
    bindDialog();
    bindQueue();
    bindCoverage();
    bindSettings();
    bindLibraries();
    bindDryRunBanner();
    bindSettingsTabs();
    bindConfigSaves();
    bindScans();
    bindPending();
    bindOverrideDialog();
    bindLibrary();
    // v1.13.11: saved filter presets — only meaningful on the library
    // pages where #library-presets-select exists. Bind unconditionally
    // (the function no-ops when the elements are absent).
    bindLibraryPresets();
    bindUploadDialog();
    bindManualUrlDialog();
    bindInfoDialog();

    // TMDB test key handler
    const tmdbBtn = document.getElementById('tmdb-test-btn');
    if (tmdbBtn) {
      tmdbBtn.addEventListener('click', async () => {
        const result = document.getElementById('tmdb-test-result');
        const input = document.querySelector('[data-cfg-field="plex.tmdb_api_key"]');
        const key = input && input.value && input.value !== '***' ? input.value : null;
        result.textContent = '... testing';
        result.style.color = '';
        try {
          const body = key ? { api_key: key } : {};
          const r = await api('POST', '/api/tmdb/test', body);
          if (r.ok) {
            result.textContent = '✓ ' + r.message;
            result.style.color = 'var(--green)';
          } else {
            result.textContent = '✗ ' + r.message;
            result.style.color = 'var(--red)';
          }
        } catch (e) {
          result.textContent = '✗ ' + e.message;
          result.style.color = 'var(--red)';
        }
      });
    }

    loadDashboard().catch(console.error);
    loadCoverage().catch(console.error);
    loadQueue().catch(console.error);
    loadTokens().catch(console.error);
    loadLibraries().catch(console.error);
    loadConfigIntoForms().catch(console.error);
    bindSyncProbe();
    loadCacheGauge().catch(()=>{});
    loadPending().catch(console.error);
    loadLibrary().catch(console.error);

    // Auto-refresh on relevant pages
    const path = window.location.pathname;
    // v1.11.40: relaxed polling intervals across all pages. Each page
    // already re-fetches eagerly after relevant user actions; the
    // background poll just keeps stats fresh while the page sits
    // open. Halving poll frequency cuts steady-state DB load.
    if (path === '/') setInterval(() => loadDashboard().catch(() => {}), 30000);
    if (path === '/queue') setInterval(() => loadQueue().catch(() => {}), 10000);
    if (path === '/pending') setInterval(() => loadPending().catch(() => {}), 15000);
    if (path === '/movies' || path === '/tv' || path === '/anime') {
      // v1.10.7: skip the background tick when the user is interacting
      // with the page so the table doesn't redraw out from under them.
      // The rapid-poll path already has the same guard.
      setInterval(() => {
        const ae = document.activeElement;
        const typingInSearch = ae && ae.id === 'library-search';
        const dialogOpen = !!document.querySelector('dialog[open]');
        const sel = window.getSelection && window.getSelection();
        const hasTextSelection = !!(sel && sel.toString().length > 0);
        if (typingInSearch || dialogOpen || hasTextSelection) return;
        loadLibrary().catch(() => {});
      }, 30000);
    }
  });
})();
