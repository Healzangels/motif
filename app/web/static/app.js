// motif · vanilla JS frontend (no framework, no build step)
(() => {
  'use strict';

  // ---- Helpers ----

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const fmt = {
    num: (n) => (n ?? 0).toLocaleString(),
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
    if (cached) applyTabAvailability(JSON.parse(cached));
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
  function paintTopbarSyncing(label) {
    const topbarText = $('#topbar-status-text');
    if (topbarText) topbarText.textContent = label;
    const topbarDot = $('#topbar-status .dot');
    if (topbarDot) {
      topbarDot.classList.remove('dot-red');
      topbarDot.classList.add('dot-amber');
    }
    setTimeout(refreshTopbarStatus, 1100);
  }

  async function refreshTopbarStatus() {
    try {
      const stats = await api('GET', '/api/stats');
      // v1.11.17: probe job_type retired — only real worker activities
      // (sync / plex_enum / download / place / scan) drive the topbar
      // text. Priority: sync > download > place > scan > idle.
      const q = stats.queue || {};
      // v1.11.35: banner text reflects what's *actually running*, not
      // what's queued. _in_flight (pending OR running) drives the
      // button locks; _running (status='running' only) drives the
      // banner so we don't claim 'SYNCING WITH PLEX' while plex_enum
      // is queued behind a sync that hasn't finished yet (the worker
      // is single-threaded today).
      const plexEnumBusy = q.plex_enum_in_flight > 0;
      const themerrdbBusy = q.themerrdb_sync_in_flight > 0;
      const plexEnumRunning = (q.plex_enum_running || 0) > 0;
      const themerrdbRunning = (q.themerrdb_sync_running || 0) > 0;
      const downloadBusy = q.download_in_flight > 0;
      const placeBusy = q.place_in_flight > 0;
      const scanBusy = q.scan_in_flight > 0;
      // v1.11.48: banner text reflects the user's mental model
      // post-click. Pre-fix the "SYNCING WITH X" text only fired
      // on _running (status='running'), so the 1-2s window between
      // job enqueue and worker pickup left the banner showing the
      // generic "{N}R / {M}P" fallback — the user clicked SYNC
      // and saw nothing change. Now: prefer _running when
      // available (avoids claiming concurrent activity that's
      // actually queued behind a different long-thread job), but
      // fall back to _in_flight when nothing's running so a
      // freshly-enqueued job lights the banner immediately.
      // v1.12.21: align verb with the button that triggered the
      // action. SYNC THEMERRDB → "SYNCING THEMERRDB"; REFRESH FROM
      // PLEX → "REFRESHING <scope>". Pre-fix the topbar said
      // "SYNCING <scope>" while the row button said "REFRESHING
      // <scope>" — same job, two verbs, mild user confusion.
      const plexScope = activePlexEnumScopeLabel(q.plex_enum_active);
      const plexLabel = plexScope ? `REFRESHING ${plexScope}` : 'REFRESHING PLEX';
      let txt;
      if (themerrdbRunning && plexEnumRunning) {
        txt = `SYNCING THEMERRDB + ${plexLabel}`;
      } else if (themerrdbRunning) {
        txt = 'SYNCING THEMERRDB';
      } else if (plexEnumRunning) {
        txt = plexLabel;
      } else if (themerrdbBusy) {
        txt = 'SYNCING THEMERRDB';
      } else if (plexEnumBusy) {
        txt = plexLabel;
      } else if (downloadBusy) {
        txt = `DOWNLOADING ${q.download_in_flight}`;
      } else if (placeBusy) {
        txt = `PLACING ${q.place_in_flight}`;
      } else if (scanBusy) {
        txt = 'SCANNING DISK';
      } else if ((q.running || 0) > 0 || (q.pending || 0) > 0) {
        // v1.12.15: friendlier label when nothing's actively running
        // but the queue isn't empty — the most common case is
        // post-place refresh nudges with a 30s delay before they
        // pick up. Surface what's actually queued instead of the
        // cryptic "0R / 1P".
        const refresh = q.refresh_in_flight || 0;
        const dl = q.download_in_flight || 0;
        const pl = q.place_in_flight || 0;
        if (refresh > 0 && refresh === (q.pending || 0) + (q.running || 0)) {
          txt = `REFRESH PENDING · ${refresh}`;
        } else if (dl > 0) {
          txt = `DOWNLOAD QUEUED · ${dl}`;
        } else if (pl > 0) {
          txt = `PLACE QUEUED · ${pl}`;
        } else {
          txt = `QUEUED · ${q.running || 0}R / ${q.pending || 0}P`;
        }
      } else {
        txt = 'IDLE';
      }
      $('#topbar-status-text').textContent = txt;
      // v1.12.15: dot + status-text tooltip so the amber dot isn't
      // mute. Breaks down what's actually queued / running so the
      // user can see whether the activity is meaningful work
      // (download / place) or just the post-place refresh nudge
      // (which sits at "pending" for ~30s by design — see
      // _do_place's delayed-refresh schedule in worker.py).
      const tipParts = [];
      if (themerrdbBusy) tipParts.push(`ThemerrDB sync: ${q.themerrdb_sync_in_flight}`);
      if (plexEnumBusy) tipParts.push(`Plex enum: ${q.plex_enum_in_flight}`);
      if (downloadBusy) tipParts.push(`download: ${q.download_in_flight}`);
      if (placeBusy) tipParts.push(`place: ${q.place_in_flight}`);
      if (scanBusy) tipParts.push(`scan: ${q.scan_in_flight}`);
      if ((q.refresh_in_flight || 0) > 0) {
        tipParts.push(`refresh: ${q.refresh_in_flight} (Plex metadata nudge, post-place delay ~30s)`);
      }
      const queueTip = tipParts.length
        ? `In flight:\n  • ${tipParts.join('\n  • ')}\n\nClick LOGS for details.`
        : ((q.failed || 0) > 0
            ? `${q.failed} failed job(s) — click to review on LOGS`
            : 'idle');
      const dotEl = $('#topbar-status .dot');
      if (dotEl && (q.failed || 0) === 0) {
        // failures-tip already wired below — only override when no
        // failures (failures-tooltip wins on click affordance).
        dotEl.title = queueTip;
      }
      const dot = $('#topbar-status .dot');
      dot.classList.remove('dot-amber', 'dot-red');
      const anyActive = themerrdbBusy || plexEnumBusy || downloadBusy
                     || placeBusy || scanBusy;
      if (q.failed > 0) dot.classList.add('dot-red');
      else if (anyActive) dot.classList.add('dot-amber');
      else if (q.pending > 0) dot.classList.add('dot-amber');

      // v1.11.73: red dot → click jumps to /queue?status=failed.
      // Pre-fix the user saw a red ● next to 'IDLE' with no
      // explanation of what was wrong or how to clear it. Now the
      // dot + status text get a tooltip, a pointer cursor, and a
      // click handler when failed > 0; clicking lands on the
      // failed-jobs filter where a CLEAR FAILED button can dismiss
      // them in one action. Stash the count on window so /queue's
      // CLEAR FAILED button can read it without an extra round
      // trip.
      const statusEl = $('#topbar-status');
      const failed = q.failed || 0;
      window.__motif_failed_count = failed;
      if (failed > 0) {
        const tip = `${failed} failed job(s) — click to review on /queue`;
        if (dot) dot.title = tip;
        $('#topbar-status-text').title = tip;
        statusEl?.classList.add('topbar-status-clickable');
        statusEl?.setAttribute('data-failed-link', '1');
      } else {
        if (dot) dot.removeAttribute('title');
        $('#topbar-status-text').removeAttribute('title');
        statusEl?.classList.remove('topbar-status-clickable');
        statusEl?.removeAttribute('data-failed-link');
      }

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

      // Updates badge
      const updBadge = $('#topbar-updates-badge');
      if (updBadge) {
        const n = (stats.updates && stats.updates.pending) || 0;
        if (n > 0) {
          $('#topbar-updates-count').textContent = n;
          updBadge.style.display = '';
        } else {
          updBadge.style.display = 'none';
        }
      }
      // Failures badge
      const failBadge = $('#topbar-failures-badge');
      if (failBadge) {
        const n = (stats.failures && stats.failures.unavailable) || 0;
        if (n > 0) {
          $('#topbar-failures-count').textContent = n;
          failBadge.style.display = '';
          // v1.12.11: route the badge to whichever tab owns the
          // first failing row (anime / tv / movies). Pre-fix the
          // link was hardcoded to /movies which mis-routed every
          // anime / tv failure.
          const tabHint = (stats.failures && stats.failures.tab_hint) || 'movies';
          failBadge.href = `/${tabHint}?tdb_pills=dead`;
        } else {
          failBadge.style.display = 'none';
        }
      }

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
      const lockBtn = (btn, locked, busyText) => {
        if (!btn) return;
        const orig = btn.dataset.origLabel || btn.textContent;
        if (locked) {
          if (!btn.dataset.origLabel) btn.dataset.origLabel = orig;
          btn.disabled = true;
          btn.textContent = busyText;
        } else if (btn.dataset.origLabel) {
          btn.disabled = false;
          btn.textContent = btn.dataset.origLabel;
          delete btn.dataset.origLabel;
        }
      };
      // Library page REFRESH FROM PLEX — lock if THIS tab+fourk variant
      // is the one currently enumerating. v1.11.72: busy text reflects
      // the actual scope ('REFRESHING ANIME', 'REFRESHING 4K MOVIES',
      // etc.) so the user can tell at a glance which library they
      // kicked off.
      const libRefreshBtn = document.getElementById('library-refresh-btn');
      if (libRefreshBtn) {
        const tabEl = document.getElementById('library-tab');
        const tab = tabEl ? tabEl.value : null;
        const variant = libraryState.fourk ? 'fourk' : 'standard';
        const tabBusy = !!(tab && enumActive[tab] && enumActive[tab][variant]);
        lockBtn(libRefreshBtn, tabBusy,
          `// REFRESHING ${libraryRefreshLabel()}…`);
      }
      // Settings global REFRESH FROM PLEX — locked through the whole
      // enum window (pending OR running) so the button doesn't
      // flicker enabled between sections.
      lockBtn(
        document.getElementById('refresh-libraries-btn'),
        anyEnumInFlight, '// REFRESHING PLEX…',
      );
      // Dashboard SYNC — only the ThemerrDB sync drives this.
      lockBtn(
        document.getElementById('sync-now-btn'),
        themerrdbBusy, '// SYNCING THEMERRDB…',
      );
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
      $('#topbar-status-text').textContent = 'OFFLINE';
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
      if (!confirm('Disable dry-run? After this, downloads will hit YouTube and themes will be placed into Plex media folders.')) return;
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

    // Last sync
    if (stats.last_sync) {
      const s = stats.last_sync;
      $('#last-sync').textContent = [
        `started:    ${s.started_at}`,
        `finished:   ${s.finished_at || '— still running'}`,
        `status:     ${s.status}`,
        `movies:     ${fmt.num(s.movies_seen)}`,
        `tv:         ${fmt.num(s.tv_seen)}`,
        `new:        ${fmt.num(s.new_count)}`,
        `updated:    ${fmt.num(s.updated_count)}`,
        s.error ? `error:      ${s.error}` : '',
      ].filter(Boolean).join('\n');
    } else {
      $('#last-sync').textContent = 'no sync runs yet — click SYNC NOW to start';
    }

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

  // Polled by the SYNC button to know when both the sync + plex_enum
  // jobs have finished. Set when the user clicks // SYNC, cleared when
  // /api/stats reports queue.sync_in_flight == 0.
  let syncWatcher = null;

  function setSyncButtonState(state) {
    const btn = $('#sync-now-btn');
    if (!btn) return;
    if (state === 'idle') {
      btn.disabled = false;
      btn.textContent = '// SYNC THEMERRDB';
    } else if (state === 'running') {
      btn.disabled = true;
      btn.textContent = '// SYNCING THEMERRDB…';
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
      // v1.11.47: poll only themerrdb_sync_in_flight (was sync_in_flight,
      // which includes plex_enum). The dashboard SYNC button represents
      // a ThemerrDB sync only; if a plex_enum runs concurrently the
      // user shouldn't see SYNCING… stuck on this button until the
      // unrelated enum drains. The two-thread worker pool runs sync
      // and plex_enum in parallel anyway.
      if (syncWatcher) clearInterval(syncWatcher);
      let primed = false;
      syncWatcher = setInterval(async () => {
        try {
          const s = await api('GET', '/api/stats');
          const inFlight = (s.queue && s.queue.themerrdb_sync_in_flight) || 0;
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

    // If the page loads while a sync is already in progress (left running
    // by another tab/session), reflect that.
    api('GET', '/api/stats').then((s) => {
      if (s && s.queue && (s.queue.themerrdb_sync_in_flight || 0) > 0) {
        setSyncButtonState('running');
        if (syncWatcher) clearInterval(syncWatcher);
        let primed = true;
        syncWatcher = setInterval(async () => {
          try {
            const s2 = await api('GET', '/api/stats');
            const inFlight = (s2.queue && s2.queue.themerrdb_sync_in_flight) || 0;
            if (primed && inFlight === 0) {
              clearInterval(syncWatcher);
              syncWatcher = null;
              setSyncButtonState('done');
              loadDashboard().catch(console.error);
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
          srcCell = '<span class="link-badge link-badge-manual" title="Manual sidecar — theme.mp3 at the Plex folder that motif doesn\'t manage yet (click ADOPT to take ownership)">M</span>';
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
          ? `<button class="btn btn-tiny" data-act="open" data-mt="${it.media_type}" data-id="${it.tmdb_id}" title="open the per-item dialog with full theme details, history and available actions">DETAILS</button>
             <button class="btn btn-tiny btn-info" data-act="accept-update" data-mt="${it.media_type}" data-id="${it.tmdb_id}" title="apply the new ThemerrDB URL — re-downloads from the new YouTube source and overwrites motif's current theme file">ACCEPT</button>
             <button class="btn btn-tiny" data-act="decline-update" data-mt="${it.media_type}" data-id="${it.tmdb_id}" title="ignore this update — the ↑ glyph clears and your existing theme stays in place">KEEP</button>
             ${deleteBtn}`
          : `<button class="btn btn-tiny" data-act="open" data-mt="${it.media_type}" data-id="${it.tmdb_id}" title="open the per-item dialog with full theme details, history and available actions">DETAILS</button>
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

  async function unplaceTheme(mediaType, tmdbId, title) {
    // Removes the theme.mp3 from Plex's folder but keeps motif's canonical
    // so REPLACE can push it back later. No re-download needed if user
    // changes their mind.
    const labelTitle = title ? `"${title}"` : `${mediaType} ${tmdbId}`;
    const ok = confirm(
      `Remove ${labelTitle} from the Plex folder?\n\n` +
      `Plex will stop playing this theme until you push it back.\n\n` +
      `Motif's canonical copy stays in /data/media/themes — click PUSH TO PLEX ` +
      `on the row to restore it without re-downloading.`);
    if (!ok) return;
    try {
      await api('POST', `/api/items/${mediaType}/${tmdbId}/unplace`);
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

  async function purgeTheme(mediaType, tmdbId, title, isOrphan, dlOnly) {
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
    const ok = confirm(`Purge ${labelTitle}?${warning}\n\nThis cannot be undone.`);
    if (!ok) return;
    try {
      const r = await fetch(`/api/items/${mediaType}/${tmdbId}/forget`, { method: 'POST' });
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
  async function clearUrlOverride(mediaType, tmdbId, btn) {
    if (btn) btn.disabled = true;
    try {
      await api('POST', `/api/items/${mediaType}/${tmdbId}/clear-url`);
      if (btn) btn.textContent = 'QUEUED';
      if (typeof libraryRapidPoll === 'function'
          && document.getElementById('library-body')) {
        libraryRapidPoll();
      }
    } catch (e) {
      alert('Clear URL failed: ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function redownload(mediaType, tmdbId, btn) {
    if (btn) btn.disabled = true;
    try {
      await api('POST', `/api/items/${mediaType}/${tmdbId}/redownload`);
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
      await api('POST', `/api/updates/${mediaType}/${tmdbId}/decline`);
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
  }

  function bindCoverage() {
    const btn = $('#relink-all-btn');
    if (!btn) return;
    btn.addEventListener('click', async () => {
      if (!confirm('Re-link all copies? This converts copies to hardlinks where the filesystem allows it. Safe to run; failures are skipped.')) return;
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

    // v1.11.73: show CLEAR FAILED button only when at least one
    // failed row exists across the WHOLE queue (not just the
    // current filter view, otherwise switching to PENDING would
    // hide the button while failed jobs still exist). Reads
    // window.__motif_failed_count, set by refreshTopbarStatus.
    const clearBtn = document.getElementById('jobs-clear-failed-btn');
    if (clearBtn) {
      // v1.12.12: gate on UNACKED failures only — already-acked
      // failed rows are historical records, no need to surface
      // CLEAR FAILED for them.
      const anyFailed = (window.__motif_failed_count || 0) > 0
        || (data.jobs || []).some((j) => j.status === 'failed' && !j.acked_at);
      clearBtn.style.display = anyFailed ? '' : 'none';
    }

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

    // v1.11.73: CLEAR FAILED dismisses every job in the failed state.
    // Visible only when at least one failed job exists, hidden
    // otherwise. Calls POST /api/jobs/clear-failed.
    document.getElementById('jobs-clear-failed-btn')?.addEventListener('click',
      async () => {
        if (!confirm('Dismiss every failed job from the queue history? '
                     + 'This only clears the queue rows — files and DB state '
                     + 'for the underlying items are unaffected.')) return;
        try {
          const r = await api('POST', '/api/jobs/clear-failed');
          if (r && typeof r.cleared === 'number') {
            // Repaint immediately + re-poll the topbar so the red dot
            // clears the same frame.
            await loadQueue().catch(()=>{});
            setTimeout(refreshTopbarStatus, 1100);
          }
        } catch (e) {
          alert('Clear failed: ' + e.message);
        }
      });
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
            ${revoked ? '' : `<button class="btn btn-tiny btn-danger" data-revoke-token="${t.id}" title="revoke this API token — anything currently using it will start returning 401 immediately. Cannot be undone; create a fresh token if you need access back.">REVOKE</button>`}
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
        if (!confirm('Clear the saved Plex token? motif will not be able to talk to Plex until you set a new one.')) return;
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
            <button class="btn btn-tiny" data-pending-approve="${htmlEscape(k)}" title="place this download into the matching Plex media folder; if a sidecar already exists there it will be overwritten">APPROVE</button>
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
    //   dlPills: 'on' (green dot), 'off' (faded), 'broken' (red)
    //   plPills: 'on' (green dot), 'off' (faded)
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
  }

  async function loadLibrary() {
    const tabEl = document.getElementById('library-tab');
    if (!tabEl) return;
    libraryState.tab = tabEl.value;
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
    tbody.innerHTML = `<tr><td colspan="9" class="muted center">loading…</td></tr>`;
    let data;
    try {
      data = await api('GET', '/api/library?' + params.toString());
    } catch (e) {
      tbody.innerHTML = `<tr><td colspan="8" class="accent-red">${htmlEscape(e.message)}</td></tr>`;
      return;
    }
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
    } else {
      // v1.12.23: pill filtering moved server-side. The server's
      // sql_count + sql_rows + ORDER BY all honor the pill set, so
      // counts / pagination / sort are correct. dedupedItems is
      // the final list to render — the only client-side trim is
      // the rating_key dedup pass above.
      tbody.innerHTML = dedupedItems.map(renderLibraryRow).join('');
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
  function computeTdbPill(it) {
    const isThemerrDbAvail = it.upstream_source
      && it.upstream_source !== 'plex_orphan';
    if (!isThemerrDbAvail) return 'none';
    if (it.pending_update) return 'update';
    if (it.failure_kind && TDB_DEAD_FAILURES_GLOBAL.has(it.failure_kind)) {
      return 'dead';
    }
    if (it.failure_kind === 'cookies_expired'
        && !window.__motif_cookies_present) {
      return 'cookies';
    }
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
    if (it.plex_has_theme) return 'P';
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
      srcCell = '<span class="link-badge link-badge-adopt" title="motif adopted an existing local theme.mp3 (sidecar is the source of truth, no ThemerrDB link)">A</span>';
    } else if (placed && (sourceKind === 'url' || sourceKind === 'upload')) {
      srcCell = '<span class="link-badge link-badge-user" title="motif manages this user-provided theme (UI upload or manual YouTube URL)">U</span>';
    } else if (placed && placedProv === 'auto') {
      // Legacy rows (source_kind NULL) — provenance='auto' === T.
      srcCell = '<span class="link-badge link-badge-themerrdb" title="motif manages from ThemerrDB">T</span>';
    } else if (placed && placedProv === 'manual') {
      // Legacy fallback heuristic for rows without source_kind.
      const wasUploadedOrUrl = (svid === '' || looksLikeYoutubeId);
      const kind = (!isOrphanRow || wasUploadedOrUrl) ? 'url' : 'adopt';
      if (kind === 'adopt') {
        srcCell = '<span class="link-badge link-badge-adopt" title="motif adopted an existing local theme.mp3 (sidecar is the source of truth, no ThemerrDB link)">A</span>';
      } else {
        srcCell = '<span class="link-badge link-badge-user" title="motif manages this user-provided theme (UI upload or manual YouTube URL)">U</span>';
      }
    } else if (sidecarOnly) {
      srcCell = '<span class="link-badge link-badge-manual" title="local theme.mp3 sidecar — motif does not manage this file (click ADOPT to take ownership)">M</span>';
    } else if (it.plex_has_theme) {
      srcCell = '<span class="link-badge link-badge-cloud" title="theme present in Plex (Plex agent / cloud) — motif does not manage this file">P</span>';
    } else {
      srcCell = '<span class="muted" title="no theme">—</span>';
    }

    // v1.11.62: 'broken' DL state — motif's local_files row says we
    // have a canonical, but a stat-check (server-side) found the file
    // missing. The placement in the Plex folder is still there, so
    // the row should call out 'still in plex, not downloaded' and
    // surface a RESTORE FROM PLEX action.
    const dlBroken = !!it.canonical_missing && !!it.file_path;
    // v1.11.99: mismatch states ('pending' / 'acked') tint the DL dot
    // amber and force the LINK glyph to ≠ — motif holds a download
    // that does NOT match the file currently at the placement, so
    // claiming a green DL or a clean = / C link would be a lie. The
    // 'acked' state behaves the same visually as 'pending'; the only
    // difference is acked rows are absent from /pending (already
    // dismissed by the user via KEEP MISMATCH).
    const isMismatch = !!it.mismatch_state;
    const dl = dlBroken ? 'broken'
             : (isMismatch ? 'mismatch'
             : (downloaded ? 'on' : ''));
    const pl = placed ? 'on' : '';
    let linkCell = '<span class="link-glyph link-glyph-none">—</span>';
    if (isMismatch && placed) {
      linkCell = '<span class="link-glyph link-glyph-mismatch" title="MISMATCH — canonical (DL) does not match the file at the Plex folder. Resolve via PUSH TO PLEX, ADOPT FROM PLEX, or KEEP MISMATCH in the PLACE menu.">M</span>';
    } else if (it.placement_kind === 'hardlink') {
      linkCell = '<span class="link-glyph link-glyph-hardlink" title="hardlink (efficient — canonical and Plex-folder file share an inode)">HL</span>';
    } else if (it.placement_kind === 'copy') {
      linkCell = '<span class="link-glyph link-glyph-copy" title="copy (uses extra disk space — fallback when canonical and Plex folder are on different filesystems)">C</span>';
    }

    // Title-cell glyphs
    const titleGlyphs = [];
    let rowExtra = '';
    // Awaiting-placement-approval state: motif has the download but the
    // place job was deferred (typically because a sidecar exists at the
    // Plex folder that approval would overwrite).
    const awaitingApproval = !it.job_in_flight && !!it.file_path && !it.media_folder;
    if (it.job_in_flight) {
      // Theme has a pending or running download/place job — pulse a cyan
      // glyph so users can watch their just-clicked URL/upload land. Pairs
      // with the rapid-poll mode kicked off by manual actions.
      const jobLabel = it.job_in_flight === 'download' ? 'downloading'
                     : it.job_in_flight === 'place'    ? 'placing into Plex folder'
                     :                                   'processing';
      titleGlyphs.push(
        `<span class="title-glyph title-glyph-pending" title="${htmlEscape(jobLabel)}">⟳</span>`
      );
    } else if (awaitingApproval) {
      titleGlyphs.push(
        `<a class="title-glyph title-glyph-await" title="awaiting placement approval — click to review at /pending" href="/pending">!</a>`
      );
    }
    // v1.11.62: 'DL broken' glyph — motif's canonical was deleted but
    // the placement in the Plex folder is still there. Click jumps to
    // the dl_missing filter so the user can find every affected row.
    if (dlBroken) {
      titleGlyphs.push(
        `<a class="title-glyph title-glyph-broken" title="canonical missing under /themes — placement in Plex folder is still there. Open RESTORE FROM PLEX in PLACE menu to recover." href="/${libraryState.tab}?status=dl_missing">↺</a>`
      );
    }
    // v1.12.5: ↑ row-title glyph removed. The blue TDB ↑ pill (from
    // the TDB-pill render below) is now the only update indicator —
    // a single visual cue is less noisy than two redundant ones.
    // ACCEPT UPDATE / KEEP CURRENT live in the SOURCE menu.
    // v1.10.50: only show the ! glyph when the failure hasn't been
    // acknowledged. Acked rows keep their red TDB pill (still failing
    // upstream) but no longer pull attention; they're hidden from the
    // FAILURES filter for the same reason.
    if (it.failure_kind && !it.failure_acked_at) {
      const human = {
        'cookies_expired': 'YouTube cookies expired',
        'video_private': 'Video is private',
        'video_removed': 'Video was removed',
        'video_age_restricted': 'Age-restricted',
        'geo_blocked': 'Geo-blocked',
        'network_error': 'Network error',
        'unknown': 'Unknown failure'
      }[it.failure_kind] || it.failure_kind;
      // v1.11.8: clicking the red ! glyph acknowledges the failure
      // (silent — clears the alert state, leaves failure_kind so the
      // TDB pill stays red). Pre-fix this opened the SET URL prompt,
      // which was a footgun for failures that aren't URL-fixable
      // (e.g. cookies_expired or geo_blocked) and confused users who
      // just wanted to dismiss the warning glyph.
      const ackTip = `${human} — click to acknowledge`;
      titleGlyphs.push(
        `<button class="title-glyph title-glyph-fail" title="${htmlEscape(ackTip)}" `
        + `data-act="clear-failure" data-mt="${themeMt}" data-id="${themeId}" `
        + `data-kind-human="${htmlEscape(human)}" data-msg="${htmlEscape(it.failure_message || '')}" type="button">⚠</button>`
      );
      rowExtra = ' class="row-failure"';
    }

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
          return ' <span class="tdb-pill tdb-pill-yes" title="ThemerrDB tracks this title. cookies.txt is configured; the next download will clear the cookies_expired flag.">TDB</span>';
        }
        return ` <span class="tdb-pill tdb-pill-cookies" title="Cookies required: ${htmlEscape(why)}${detail}\n\nDrop a valid cookies.txt at the configured path, or use SET URL / UPLOAD MP3 / ADOPT.">TDB ⚠</span>`;
      }
      if (it.failure_kind && TDB_DEAD_FAILURES.has(it.failure_kind)) {
        return ` <span class="tdb-pill tdb-pill-dead" title="Upstream URL broken: ${htmlEscape(why)}${detail}\n\nRecover via SET URL, UPLOAD MP3, or drop a sidecar and ADOPT.">TDB ✗</span>`;
      }
      // v1.11.96: pending-update state takes precedence over the
      // generic green TDB pill so the user can scan the library page
      // and immediately see which rows have an upstream URL update
      // available. Blue ties to the ↑ glyph and the UPDATES filter.
      if (it.pending_update) {
        return ' <span class="tdb-pill tdb-pill-update" title="ThemerrDB upstream URL changed — ACCEPT UPDATE in the SOURCE menu to switch, or KEEP CURRENT to dismiss.">TDB ↑</span>';
      }
      return ' <span class="tdb-pill tdb-pill-yes" title="ThemerrDB has this title — TDB action available in the SOURCE menu">TDB</span>';
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
    const lockManualActions = !!it.job_in_flight || awaitingApproval;
    const lockTitle = it.job_in_flight
      ? 'wait for current job to finish'
      : (awaitingApproval ? 'pending placement approval — review at /pending'
                          : '');
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
    if (it.pending_update && themed
        && themeId !== null && themeId !== undefined) {
      // v1.12.46: pass sectionId so the accept-update endpoint
      // scopes the download + place to ONLY this row's section.
      // Pre-fix _enqueue_download fanned out to every section
      // that owned the title, so accepting from the 4K row
      // would also overwrite the standard library's theme —
      // which the user wanted independently themed (different
      // editions = different themes).
      sourceItems.push(menuItemHtml(
        'accept-update', 'ACCEPT UPDATE',
        'Download the new ThemerrDB URL and replace the current theme in this section only.',
        { mt: themeMt, id: themeId, sectionId: it.section_id, info: true },
      ));
      if (it.actionable_update) {
        sourceItems.push(menuItemHtml(
          'decline-update', 'KEEP CURRENT',
          'Dismiss the prompt; the blue TDB ↑ pill stays for filter/sort.',
          { mt: themeMt, id: themeId },
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
    if (themed && themeId !== null && themeId !== undefined
        && !sidecarOnly && !isPlexAgent && !isManualPlacement
        && !lockManualActions && !it.pending_update
        && !it.accepted_update) {
      const tdbDeadForDownload = it.failure_kind
        && TDB_DEAD_FAILURES.has(it.failure_kind);
      const tdbCookiesBlocked = it.failure_kind === 'cookies_expired'
        && !window.__motif_cookies_present;
      const tdbBlocked = tdbDeadForDownload || tdbCookiesBlocked;
      const hasDownloadUrl = !!it.youtube_url
        || sourceKindForActions === 'url';
      if (!tdbBlocked && hasDownloadUrl) {
        if (!downloaded || dlBroken) {
          sourceItems.push(menuItemHtml(
            'redl', 'DOWNLOAD TDB',
            'Download from ThemerrDB and place into the Plex folder.',
            { mt: themeMt, id: themeId, tone: 'themerrdb' },
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
            'Re-fetch from ThemerrDB and overwrite the canonical (refresh / corruption recovery).',
            { mt: themeMt, id: themeId, tone: 'themerrdb' },
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
      { rk: it.rating_key, tone: 'user' },
    ));
    sourceItems.push(menuItemHtml(
      'upload-theme', 'UPLOAD MP3',
      'Upload an MP3 file as the theme.',
      { rk: it.rating_key, tone: 'user' },
    ));

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
    // ACK FAILURE — clear the failure flag on the theme so the
    // red ! glyph + topbar FAIL count drop this row. Doesn't fix
    // anything; it's a "stop showing me the warning" dismiss.
    // Re-fires on the next failed download attempt.
    if (it.failure_kind && !it.failure_acked_at
        && themed && themeId !== null && themeId !== undefined) {
      sourceItems.push(menuItemHtml(
        'clear-failure', 'ACK FAILURE',
        "Drop this row from the topbar FAIL count. The red TDB ✗ pill stays.",
        { mt: themeMt, id: themeId },
      ));
    }

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
    if (it.has_previous_url && !it.revert_redundant) {
      const revertTone = it.previous_youtube_kind === 'themerrdb'
        ? 'themerrdb' : 'user';
      const revertTip = it.previous_youtube_kind === 'themerrdb'
        ? "Revert to the previously-active ThemerrDB URL and re-download in this section."
        : "Revert to the previously-active user URL and re-download in this section.";
      // v1.12.47: pass sectionId so REVERT scopes the
      // re-download + place to only this row's section
      // (matches ACCEPT UPDATE per-section behavior).
      sourceItems.push(menuItemHtml(
        'revert', 'REVERT',
        revertTip,
        { mt: themeMt, id: themeId, sectionId: it.section_id, tone: revertTone },
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
      removeItems.push(menuItemHtml(
        'clear-url', 'CLEAR URL',
        "Drop the captured previous URL — REVERT will no longer be available.",
        { mt: themeMt, id: themeId, danger: true },
      ));
    }
    if (placed) {
      removeItems.push(menuItemHtml(
        'unplace', 'DEL',
        "Remove from the Plex folder. Canonical stays — PUSH TO PLEX restores.",
        { mt: themeMt, id: themeId, danger: true },
      ));
    }
    if (placed && downloaded) {
      removeItems.push(menuItemHtml(
        'unmanage', 'UNMANAGE',
        "Drop motif's tracking and delete the canonical. Plex-folder file stays; row flips to M.",
        { mt: themeMt, id: themeId, danger: true },
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
          danger: true,
          // v1.11.88: bypassLock so PURGE stays clickable when the row
          // is awaitingApproval (downloaded but not placed). PURGE *is*
          // the legitimate exit from that state — the prior lock left
          // the user with PUSH TO PLEX as the only enabled action,
          // forcing them to either accept the placement or leave the
          // canonical sitting there indefinitely. Real in-flight jobs
          // still disable the button (job_in_flight).
          bypassLock: true,
          dlOnly: !placed && downloaded ? '1' : '0' },
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
      acts.push(`<button class="btn btn-tiny row-info-btn" data-act="info" data-mt="${themeMt}" data-id="${themeId}" title="ThemerrDB record details">ⓘ</button>`);
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
        <td class="col-state"><span class="state-pill ${dl}"></span></td>
        <td class="col-state"><span class="state-pill ${pl}"></span></td>
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
      `<button class="btn btn-tiny" data-act="info" data-mt="${themeMt}" data-id="${themeId}" title="ThemerrDB record details">ⓘ</button>`,
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
    bar.style.display = n > 0 ? '' : 'none';
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
    if (dlBtn)    dlBtn.style.display    = (!onTdbOnly && hasTdbEligible) ? '' : 'none';
    if (adoptBtn) adoptBtn.style.display = (!onTdbOnly && hasSidecarOnly) ? '' : 'none';
    const exportBtn = document.getElementById('library-export-csv-btn');
    if (exportBtn) exportBtn.style.display = '';
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
          values: new Set(['on','off']) },
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
          values: ['on', 'off'] },
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
    search?.addEventListener('input', () => {
      clearTimeout(dt);
      dt = setTimeout(() => {
        libraryState.q = search.value.trim();
        libraryState.page = 1;
        loadLibrary().catch(console.error);
      }, 250);
    });

    // Refresh — v1.10.6: send {tab, fourk} so the backend only enumerates
    // sections backing the current tab variant. While the enum runs the
    // button is left in a 'REFRESHING…' state; refreshTopbarStatus's
    // plex_enum_in_flight signal flips it back when the worker finishes.
    document.getElementById('library-refresh-btn')?.addEventListener('click', async (e) => {
      const btn = e.currentTarget;
      btn.disabled = true;
      const orig = btn.dataset.origLabel || btn.textContent;
      btn.dataset.origLabel = orig;
      btn.textContent = `// REFRESHING ${libraryRefreshLabel()}…`;
      try {
        await api('POST', '/api/library/refresh', {
          tab: libraryState.tab,
          fourk: !!libraryState.fourk,
        });
        paintTopbarSyncing(`REFRESHING ${libraryRefreshLabel()}`);
        setTimeout(() => loadLibrary().catch(()=>{}), 5000);
        setTimeout(() => loadLibrary().catch(()=>{}), 15000);
      } catch (err) {
        alert('Refresh failed: ' + err.message);
        btn.disabled = false;
        btn.textContent = orig;
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

    // v1.10.15: column sort. Click a th[data-sort] to sort. Re-clicking
    // the active column toggles asc → desc → asc. Switching columns
    // resets to asc. The active column shows a ▲/▼ indicator.
    document.querySelectorAll('th.col-sort').forEach((th) => {
      th.addEventListener('click', () => {
        const key = th.dataset.sort;
        if (!key) return;
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
        items.push({
          media_type: it.theme_media_type,
          tmdb_id: it.theme_tmdb,
        });
      }
      if (items.length === 0) {
        alert('Nothing downloadable in selection — every selected row is a sidecar (use ADOPT SELECTED) or has no ThemerrDB record.');
        return;
      }
      const btn = e.currentTarget;
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = '// QUEUING';
      try {
        const r = await api('POST', '/api/library/download-batch', { items });
        // v1.11.30: surface the skip count so the user knows their
        // M-row selection wasn't ignored silently.
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
        const params = new URLSearchParams({
          tab: libraryState.tab,
          fourk: libraryState.fourk ? 'true' : 'false',
          per_page: '200',
        });
        if (libraryState.q) params.set('q', libraryState.q);
        if (libraryState.status !== 'all') params.set('status', libraryState.status);
        if (libraryState.tdb && libraryState.tdb !== 'any') {
          params.set('tdb', libraryState.tdb);
        }
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
        // these rows on the way in.
        const params = new URLSearchParams({
          tab: libraryState.tab,
          fourk: libraryState.fourk ? 'true' : 'false',
          per_page: '200',
        });
        if (libraryState.q) params.set('q', libraryState.q);
        if (libraryState.status !== 'all') params.set('status', libraryState.status);
        if (libraryState.tdb && libraryState.tdb !== 'any') {
          params.set('tdb', libraryState.tdb);
        }
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
      if (!confirm(`Acknowledge ${candidates.length} failure(s)? The topbar FAIL count drops; the red TDB ✗ pill stays so the broken upstream is still visible on the row.`)) return;
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
    function confirmPlexAgentOverride(action, title) {
      return confirm(
        `Plex is already supplying a theme for "${title || 'this item'}".\n\n`
        + `Motif's default is to defer to Plex when it has its own theme. `
        + `Are you sure you want to ${action}?\n\n`
        + `(This will replace what Plex currently plays with motif's version.)`
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
          if (!confirmPlexAgentOverride(verb, btn.dataset.title)) return;
        }
      }
      if (act === 'redl') {
        redownload(btn.dataset.mt, btn.dataset.id, btn).catch(console.error);
      } else if (act === 'revert') {
        revertToThemerrDb(btn.dataset.mt, btn.dataset.id, btn).catch(console.error);
      } else if (act === 'clear-url') {
        // v1.12.37 (revised): CLEAR URL drops the captured
        // previous URL on the row so REVERT becomes unavailable.
        // Doesn't touch the canonical or user_overrides — the
        // current playing theme is unaffected.
        const title = btn.dataset.title || 'this theme';
        if (!confirm(`Clear the captured previous URL for "${title}"?\n\nREVERT will no longer be available on this row. Current theme is unaffected.`)) return;
        clearUrlOverride(btn.dataset.mt, btn.dataset.id, btn).catch(console.error);
      } else if (act === 'info') {
        openInfoDialog(btn.dataset.mt, btn.dataset.id).catch(console.error);
      } else if (act === 'delete-orphan') {
        await deleteOrphan(btn.dataset.mt, btn.dataset.id, btn.dataset.title || '');
        await loadLibrary().catch(()=>{});
      } else if (act === 'unplace') {
        await unplaceTheme(btn.dataset.mt, btn.dataset.id, btn.dataset.title || '');
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
        if (!confirm(`Restore the canonical /themes copy for "${title}" from the existing Plex-folder file?\n\nMotif will hardlink (or copy on cross-FS) the placement back to <themes_dir>/<canonical_path>.`)) return;
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
                         btn.dataset.dlOnly === '1');
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
          await api('POST', `/api/items/${btn.dataset.mt}/${btn.dataset.id}/unmanage`);
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
        });
      } else if (act === 'open-override') {
        openOverrideDialog({
          mediaType: btn.dataset.mt,
          tmdbId: btn.dataset.id,
          kindHuman: btn.dataset.kindHuman || 'failure',
          message: btn.dataset.msg || '',
        });
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
          if (!confirm(`Replace the existing theme for "${title}" with the ThemerrDB download?\n\nMotif will fetch from upstream and overwrite the current sidecar.`)) return;
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

  async function openInfoDialog(mediaType, tmdbId) {
    const dlg = document.getElementById('info-dlg');
    if (!dlg) return;
    const body = document.getElementById('info-dlg-body');
    body.innerHTML = '<p class="muted">loading…</p>';
    if (typeof dlg.showModal === 'function') dlg.showModal();
    else dlg.setAttribute('open', '');
    let data;
    try {
      data = await api('GET', `/api/items/${mediaType}/${tmdbId}`);
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
    const previousUrl = t.previous_youtube_url || '';
    const previousKind = t.previous_youtube_kind || null;
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
    const currentColor = ovr ? 'var(--violet)' : 'var(--green-bright)';
    const currentUrlLink = linkOrDash(currentUrl, currentColor);
    const prevColor = previousKind === 'user'
      ? 'var(--violet)'
      : previousKind === 'themerrdb' ? 'var(--green-bright)' : null;
    const prevKindLabel = previousKind === 'user'
      ? '<span class="muted small" style="color:var(--violet)">user</span>'
      : previousKind === 'themerrdb'
        ? '<span class="muted small" style="color:var(--green-bright)">themerrdb</span>'
        : '';
    const previousUrlLink = previousUrl
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
      ? `<dt>override set</dt><dd><span class="muted small">by ${htmlEscape(ovr.set_by || '')} at ${htmlEscape(ovr.set_at || '')}${ovr.note ? ' · ' + htmlEscape(ovr.note) : ''}</span></dd>`
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
      if (pu.decision === 'accepted' && !previousUrl) {
        // No previous URL captured — usually because the user's
        // URL matched the new TDB URL at accept time, OR the row
        // wasn't U at accept time. Surface why REVERT isn't
        // available so the missing button doesn't read as a bug.
        puBlock += `<dt class="muted">revert</dt>`
          + `<dd class="muted small">unavailable — no previous URL captured (either none was set, or it matched the TDB URL exactly).</dd>`;
      }
    }
    const placedBlock = placements.length
      ? `<dt>placed in</dt><dd>${placements.map(p => `<div class="muted small">${htmlEscape(p.media_folder)} <span class="muted">(${htmlEscape(p.placement_kind)})</span></div>`).join('')}</dd>`
      : '';
    const dlBlock = lf
      ? `<dt>downloaded</dt><dd class="muted small">${htmlEscape(lf.abs_path || lf.file_path)} · ${fmt.num(lf.file_size)}B · ${htmlEscape(lf.provenance)}</dd>`
      : '';
    body.innerHTML = `
      <h3>${htmlEscape(t.title || '—')}${t.year ? ' (' + htmlEscape(t.year) + ')' : ''}</h3>
      <dl class="dlg-grid">
        <dt>imdb</dt><dd>${imdb}</dd>
        <dt>tmdb</dt><dd>${tmdbLink}</dd>
        <dt>upstream</dt><dd>${htmlEscape(t.upstream_source || '')}</dd>
        <dt>themerrdb url</dt><dd>${tdbUrlLink}</dd>
        <dt>currently applied</dt><dd>${currentUrlLink}</dd>
        <dt>previous url</dt><dd>${previousUrlLink}</dd>
        <dt>video id</dt><dd>${htmlEscape(ytId || '—')}</dd>
        <dt>added</dt><dd class="muted small">${htmlEscape(t.youtube_added_at || '—')}</dd>
        <dt>edited</dt><dd class="muted small">${htmlEscape(t.youtube_edited_at || '—')}</dd>
        ${failBlock}
        ${ovrBlock}
        ${puBlock}
        ${dlBlock}
        ${placedBlock}
      </dl>
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
    `;
  }

  function closeInfoDialog() {
    const dlg = document.getElementById('info-dlg');
    if (!dlg) return;
    if (typeof dlg.close === 'function') dlg.close();
    else dlg.removeAttribute('open');
  }

  function bindInfoDialog() {
    const dlg = document.getElementById('info-dlg');
    if (!dlg) return;
    document.getElementById('info-dlg-close')?.addEventListener('click', closeInfoDialog);
  }


  // ---- Manual YouTube URL dialog (Coverage tab) ----

  function openManualUrlDialog({ ratingKey, title, year }) {
    const dlg = document.getElementById('manual-url-dlg');
    if (!dlg) return;
    document.getElementById('manual-url-rk').value = ratingKey;
    const meta = document.getElementById('manual-url-dlg-meta');
    const ylabel = year ? ` (${htmlEscape(year)})` : '';
    meta.innerHTML = `<p class="muted">// ${htmlEscape((title || 'untitled').toUpperCase())}${ylabel}</p>`;
    document.getElementById('manual-url-input').value = '';
    document.getElementById('manual-url-status').textContent = '';
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

  document.addEventListener('DOMContentLoaded', () => {
    highlightNav();
    refreshTopbarStatus();

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
    setInterval(refreshTopbarStatus, 30000);

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
