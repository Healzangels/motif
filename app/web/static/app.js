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

  // ---- Nav highlighting ----

  function highlightNav() {
    const path = window.location.pathname;
    const map = { '/': 'dashboard', '/movies': 'movies', '/tv': 'tv',
                  '/anime': 'anime', '/queue': 'queue',
                  '/pending': 'pending', '/scans': 'scans',
                  '/settings': 'settings' };
    const k = map[path];
    if (!k) return;
    const a = document.querySelector(`.nav a[data-nav="${k}"]`);
    if (a) a.classList.add('active');
  }

  // ---- Topbar status ----

  async function refreshTopbarStatus() {
    try {
      const stats = await api('GET', '/api/stats');
      const txt = `${stats.queue.running}r/${stats.queue.pending}p`;
      $('#topbar-status-text').textContent = txt;
      const dot = $('#topbar-status .dot');
      dot.classList.remove('dot-amber', 'dot-red');
      if (stats.queue.failed > 0) dot.classList.add('dot-red');
      else if (stats.queue.pending > 0) dot.classList.add('dot-amber');

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
        } else {
          failBadge.style.display = 'none';
        }
      }

      // Adaptive nav: hide tabs that have no managed sections backing them.
      // Anime is the only optional content tab; Movies/TV always render
      // (even empty) so users land somewhere familiar after first install.
      if (stats.tab_availability) {
        const ta = stats.tab_availability;
        const animeNav = document.querySelector('.nav a[data-nav="anime"]');
        if (animeNav) {
          const hasAnime = ta.anime.standard || ta.anime.fourk;
          animeNav.style.display = hasAnime ? '' : 'none';
        }
        adaptLibraryFourkToggle(ta);
      }

      // Pending-placements indicator: light the dot on the PENDING nav
      // link whenever there are items awaiting placement approval.
      // Surfaces the workflow even when the user is mid-action on /movies.
      const pendingDot = document.getElementById('nav-attn-pending');
      const pendingCount = (stats.queue && stats.queue.pending_placements) || 0;
      if (pendingDot) pendingDot.style.display = pendingCount > 0 ? '' : 'none';
      // Library-page banner: show a click-through when there are pending
      // placements. Subtle vs the missing-themes banner; same JS refresh
      // cadence (every loadLibrary tick, plus the 15s topbar poll).
      const pendingBanner = document.getElementById('library-pending-banner');
      if (pendingBanner) {
        if (pendingCount > 0) {
          document.getElementById('library-pending-count').textContent =
            fmt.num(pendingCount);
          pendingBanner.style.display = '';
        } else {
          pendingBanner.style.display = 'none';
        }
      }

      // Drive dry-run banner
      const banner = $('#dry-run-banner');
      if (banner) {
        banner.style.display = stats.dry_run ? '' : 'none';
        document.body.classList.toggle('dry-run-on', !!stats.dry_run);
      }
      // Drive paths-not-configured banner
      updatePathsBanner(stats);

      // v1.10.6: lock the SYNC + REFRESH FROM PLEX buttons while their
      // corresponding worker job is running. Prevents spam-clicks and
      // surfaces the in-flight state textually so the user knows the
      // click registered.
      const plexEnumBusy = (stats.queue && stats.queue.plex_enum_in_flight > 0);
      const themerrdbBusy = (stats.queue && stats.queue.themerrdb_sync_in_flight > 0);
      const refreshBtn = document.getElementById('library-refresh-btn');
      if (refreshBtn) {
        const orig = refreshBtn.dataset.origLabel || refreshBtn.textContent;
        if (plexEnumBusy) {
          if (!refreshBtn.dataset.origLabel) refreshBtn.dataset.origLabel = orig;
          refreshBtn.disabled = true;
          refreshBtn.textContent = '// REFRESHING…';
        } else if (refreshBtn.dataset.origLabel) {
          refreshBtn.disabled = false;
          refreshBtn.textContent = refreshBtn.dataset.origLabel;
        }
      }
      // SYNC button — disable during BOTH a ThemerrDB sync AND a Plex
      // enum, per user request: 'when a refresh for a given library is
      // occurring for plex lets disable the sync button so we don't
      // allow spam clicking'. The button lives on /dashboard; lock here
      // even when the user is on /movies because the topbar status
      // poll fires on every page.
      const syncBusy = plexEnumBusy || themerrdbBusy;
      const syncBtn = document.getElementById('sync-now-btn');
      if (syncBtn) {
        const orig = syncBtn.dataset.origLabel || syncBtn.textContent;
        if (syncBusy) {
          if (!syncBtn.dataset.origLabel) syncBtn.dataset.origLabel = orig;
          syncBtn.disabled = true;
          syncBtn.textContent = themerrdbBusy ? '// SYNCING…' : '// REFRESHING…';
        } else if (syncBtn.dataset.origLabel) {
          syncBtn.disabled = false;
          syncBtn.textContent = syncBtn.dataset.origLabel;
        }
      }
    } catch (e) {
      $('#topbar-status-text').textContent = 'OFFLINE';
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
      btn.textContent = '// SYNC';
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
      // Poll /api/stats until the sync + auto-enqueued plex_enum settle.
      // Once neither job is in flight we flash DONE then revert.
      if (syncWatcher) clearInterval(syncWatcher);
      let primed = false;
      syncWatcher = setInterval(async () => {
        try {
          const s = await api('GET', '/api/stats');
          const inFlight = (s.queue && s.queue.sync_in_flight) || 0;
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
      if (s && s.queue && s.queue.sync_in_flight > 0) {
        setSyncButtonState('running');
        if (syncWatcher) clearInterval(syncWatcher);
        let primed = true;
        syncWatcher = setInterval(async () => {
          try {
            const s2 = await api('GET', '/api/stats');
            const inFlight = (s2.queue && s2.queue.sync_in_flight) || 0;
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
      body.innerHTML = '<tr><td colspan="9" class="muted center">no results</td></tr>';
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
          linkCell = '<span class="link-glyph link-glyph-hardlink" title="hardlink (efficient)">=</span>';
        } else if (linkKind === 'copy') {
          linkCell = '<span class="link-glyph link-glyph-copy" title="copy (uses extra disk)">C</span>';
        } else {
          linkCell = '<span class="link-glyph link-glyph-none">—</span>';
        }

        let srcCell;
        if (it.upstream_source === 'plex_orphan') {
          srcCell = '<span class="link-badge link-badge-orphan" title="adopted orphan (no upstream record)">O</span>';
        } else if (provenance === 'manual') {
          srcCell = '<span class="link-badge link-badge-manual" title="manually overridden">M</span>';
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
          ? `<button class="btn btn-tiny btn-danger" data-act="delete-orphan" data-mt="${it.media_type}" data-id="${it.tmdb_id}" data-title="${htmlEscape(it.title || '')}" title="delete this orphan and all associated files">× DEL</button>`
          : '';
        const actions = it.pending_update
          ? `<button class="btn btn-tiny" data-act="open" data-mt="${it.media_type}" data-id="${it.tmdb_id}">DETAILS</button>
             <button class="btn btn-tiny btn-warn" data-act="accept-update" data-mt="${it.media_type}" data-id="${it.tmdb_id}">ACCEPT</button>
             <button class="btn btn-tiny" data-act="decline-update" data-mt="${it.media_type}" data-id="${it.tmdb_id}">KEEP</button>
             ${deleteBtn}`
          : `<button class="btn btn-tiny" data-act="open" data-mt="${it.media_type}" data-id="${it.tmdb_id}">DETAILS</button>
             <button class="btn btn-tiny btn-warn" data-act="redl" data-mt="${it.media_type}" data-id="${it.tmdb_id}">RE-DL</button>
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
      `Motif's canonical copy stays in /data/media/themes — click REPLACE ` +
      `on the row to push it back without re-downloading.`);
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
    } catch (e) {
      alert('Replace failed: ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function purgeTheme(mediaType, tmdbId, title, isOrphan) {
    const labelTitle = title ? `"${title}"` : `${mediaType} ${tmdbId}`;
    const orphanWarning = isOrphan
      ? '\n\nThis is an adopted/manual entry. The themes row will be deleted'
      + ' permanently; future syncs will NOT recreate it.'
      : '\n\nMotif drops the canonical file in /data/media/themes AND the placement'
      + ' in the Plex folder. If ThemerrDB has this title, it will reappear as'
      + ' missing on next view; URL/UPLOAD again to restore.';
    const ok = confirm(`Purge ${labelTitle}?${orphanWarning}\n\nThis cannot be undone.`);
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
    if (!confirm('Replace your manual override with the ThemerrDB version?\n\n'
        + 'Motif will clear the override and download from upstream. The SRC '
        + 'will switch from U back to T.')) return;
    if (btn) btn.disabled = true;
    try {
      await api('POST', `/api/items/${mediaType}/${tmdbId}/revert`);
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
    if (!confirm('Accept upstream update? This will re-download with the new YouTube URL.')) return;
    if (btn) btn.disabled = true;
    try {
      await api('POST', `/api/updates/${mediaType}/${tmdbId}/accept`);
      if (btn) btn.textContent = 'QUEUED';
      setTimeout(() => loadItems().catch(()=>{}), 600);
    } catch (e) {
      alert('Accept failed: ' + e.message);
      if (btn) btn.disabled = false;
    }
  }

  async function declineUpdate(mediaType, tmdbId, btn) {
    if (!confirm('Keep current theme and decline this update?')) return;
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
    $('#jobs-body').innerHTML = data.jobs.map((j) => `
      <tr>
        <td>${htmlEscape(j.id)}</td>
        <td>${htmlEscape(j.job_type)}</td>
        <td class="muted">${htmlEscape(j.media_type ?? '')} ${htmlEscape(j.tmdb_id ?? '')}</td>
        <td><span class="event-level event-level-${j.status === 'failed' ? 'ERROR' : (j.status === 'running' ? 'WARNING' : 'INFO')}">${htmlEscape(j.status)}</span></td>
        <td class="muted">${htmlEscape(fmt.time(j.created_at))}</td>
        <td class="muted" title="${htmlEscape(j.last_error ?? '')}">${htmlEscape((j.last_error ?? '').slice(0, 60))}</td>
      </tr>
    `).join('') || '<tr><td colspan="6" class="muted center">no jobs</td></tr>';

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
    $$('.chip[data-jobfilter]').forEach((c) => {
      c.addEventListener('click', () => {
        $$('.chip[data-jobfilter]').forEach((x) => x.classList.remove('chip-active'));
        c.classList.add('chip-active');
        queueFilter = c.dataset.jobfilter;
        loadQueue().catch(console.error);
      });
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
      const role = isAnime && is4k ? 'anime_4k'
                 : isAnime         ? 'anime'
                 : is4k            ? '4k'
                 :                   'standard';
      // Movie sections only get standard/4k options — anime tabs draw from
      // type='show' sections in motif's typical Plex layout. Show/show-4K
      // sections get the full set including anime + anime 4K.
      const animeOpts = s.type === 'movie'
        ? ''
        : `<option value="anime"${role === 'anime' ? ' selected' : ''}>anime</option>
           <option value="anime_4k"${role === 'anime_4k' ? ' selected' : ''}>anime 4k</option>`;
      const row = `
        <tr style="${stale ? 'opacity:0.45' : ''}">
          <td class="lib-col-id">${htmlEscape(s.section_id)}</td>
          <td class="lib-col-section"><strong>${htmlEscape(s.title)}</strong>${stale ? ' <span class="muted" style="font-size:var(--t-tiny)">(stale)</span>' : ''}</td>
          <td class="lib-col-type"><span class="muted">${htmlEscape(s.type)}</span></td>
          <td class="lib-col-mgd">
            <input type="checkbox" data-section-toggle="${htmlEscape(s.section_id)}" ${included ? 'checked' : ''} />
          </td>
          <td class="lib-col-role">
            <select class="input" data-section-role="${htmlEscape(s.section_id)}" title="which Movies/TV/Anime tab does this section feed">
              <option value="standard"${role === 'standard' ? ' selected' : ''}>standard</option>
              <option value="4k"${role === '4k' ? ' selected' : ''}>4k</option>
              ${animeOpts}
            </select>
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
      refresh.disabled = true;
      const orig = refresh.textContent;
      refresh.textContent = '// REFRESHING';
      try {
        await api('POST', '/api/libraries/refresh');
      } catch (e) {
        alert('Refresh failed: ' + e.message);
        refresh.disabled = false;
        refresh.textContent = orig;
        return;
      }
      setTimeout(() => {
        refresh.disabled = false;
        refresh.textContent = orig;
        loadLibraries().catch(console.error);
      }, 1500);
    });

    // Per-section refresh button — enumerate just this section from Plex
    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('button[data-section-refresh]');
      if (!btn) return;
      const sid = btn.dataset.sectionRefresh;
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = '…';
      try {
        await api('POST', `/api/libraries/${encodeURIComponent(sid)}/refresh`);
        btn.textContent = '✓';
        setTimeout(() => loadLibraries().catch(()=>{}), 4000);
      } catch (err) {
        alert('Refresh failed: ' + err.message);
      }
      setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 4000);
    });

    // Deferred save: capture every MGD/ROLE change into librariesDirty
    // (keyed by section_id). The // SAVE button commits everything in
    // one click — consistent with the rest of /settings, and the user
    // can flip several sections without each toggle firing a request.
    document.addEventListener('change', (e) => {
      const tog = e.target.closest('input[data-section-toggle]');
      const roleSel = e.target.closest('select[data-section-role]');
      if (tog) {
        const sid = tog.dataset.sectionToggle;
        if (!librariesDirty[sid]) librariesDirty[sid] = {};
        librariesDirty[sid].included = tog.checked;
        updateLibrariesSaveButton();
      } else if (roleSel) {
        const sid = roleSel.dataset.sectionRole;
        if (!librariesDirty[sid]) librariesDirty[sid] = {};
        librariesDirty[sid].role = roleSel.value;
        updateLibrariesSaveButton();
      }
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
        }
      }
      librariesDirty = {};
      status.textContent = failed === 0
        ? `✓ saved ${ok} section${ok === 1 ? '' : 's'}`
        : `✗ ${failed} of ${ok + failed} failed — see console`;
      status.classList.add(failed === 0 ? 'ok' : 'err');
      updateLibrariesSaveButton();
      // Re-fetch authoritative state, in case anything diverged
      setTimeout(() => loadLibraries().catch(()=>{}), 600);
      setTimeout(() => { status.textContent = ''; status.classList.remove('ok', 'err'); }, 4000);
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
            ${revoked ? '' : `<button class="btn btn-tiny btn-danger" data-revoke-token="${t.id}">REVOKE</button>`}
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
      tbody.innerHTML = `<tr><td colspan="7" class="accent-red">${htmlEscape(e.message)}</td></tr>`;
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
      tbody.innerHTML = '<tr><td colspan="7" class="muted center">no staged downloads — everything is either placed or unsynced</td></tr>';
      updatePendingBulkBar();
      return;
    }
    const rows = pendingState.items.map((it) => {
      const k = pendingKey(it);
      const checked = pendingState.selected.has(k) ? 'checked' : '';
      const sourceLabel = it.provenance === 'manual' ? 'MANUAL' :
                          (it.upstream_source === 'plex_orphan' ? 'ORPHAN' : 'THEMERRDB');
      const dlAt = it.downloaded_at ? fmt.time(it.downloaded_at) : '—';
      // Sidecar warning: a theme.mp3 already exists at the Plex folder.
      // Approval will overwrite it (force=true on the place job).
      const overwriteBadge = it.plex_local_theme
        ? ' <span class="link-badge" title="approving will overwrite an existing theme.mp3 in the Plex folder" style="color:var(--amber);border-color:var(--amber)">⚠ OVERWRITES</span>'
        : '';
      return `
        <tr>
          <td><input type="checkbox" data-pending-key="${htmlEscape(k)}" ${checked} /></td>
          <td><strong>${htmlEscape(it.title || '—')}</strong>${overwriteBadge}</td>
          <td class="col-year">${htmlEscape(it.year || '—')}</td>
          <td><span class="muted">${htmlEscape(it.media_type)}</span></td>
          <td><span class="muted small">${sourceLabel}</span></td>
          <td><span class="muted small">${dlAt}</span></td>
          <td class="col-actions">
            <button class="btn btn-tiny" data-pending-approve="${htmlEscape(k)}">APPROVE</button>
            <button class="btn btn-tiny btn-warn" data-pending-discard="${htmlEscape(k)}">DISCARD</button>
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
    return res;
  }

  async function pendingDiscard(keys) {
    if (!confirm(`Discard ${keys.length} download(s)? The file(s) will be deleted.`)) return null;
    const res = await api('POST', '/api/pending/discard', { items: pendingItemsForKeys(keys) });
    pendingState.selected.clear();
    await loadPending();
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
    // Set of "media_type:tmdb_id" keys checked via the per-row checkbox.
    // Survives pagination (we restore checkboxes on render).
    selected: new Set(),
  };

  function libKey(it) {
    return `${it.theme_media_type || it.plex_media_type}:${it.theme_tmdb || it.rating_key}`;
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
    const tbody = document.getElementById('library-body');
    tbody.innerHTML = `<tr><td colspan="9" class="muted center">loading…</td></tr>`;
    let data;
    try {
      data = await api('GET', '/api/library?' + params.toString());
    } catch (e) {
      tbody.innerHTML = `<tr><td colspan="8" class="accent-red">${htmlEscape(e.message)}</td></tr>`;
      return;
    }
    libraryState.items = data.items || [];
    if (data.items.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9" class="muted center">no items — enable the relevant Plex sections in Settings → PLEX and click REFRESH FROM PLEX</td></tr>';
    } else {
      tbody.innerHTML = data.items.map(renderLibraryRow).join('');
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
    const totalPages = Math.max(1, Math.ceil(data.total / libraryState.perPage));
    document.getElementById('library-pager').innerHTML = `
      <button data-lib-page="${libraryState.page - 1}" ${libraryState.page <= 1 ? 'disabled' : ''}>« prev</button>
      <span>page ${libraryState.page} / ${totalPages}</span>
      <button data-lib-page="${libraryState.page + 1}" ${libraryState.page >= totalPages ? 'disabled' : ''}>next »</button>
    `;
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
    const placed = !!it.media_folder;
    const placedProv = it.placement_provenance;
    const sidecarOnly = !placed && !!it.plex_local_theme;
    const isOrphanRow = it.upstream_source === 'plex_orphan';
    const sourceKind = it.source_kind || null;
    const svid = it.source_video_id || '';
    const looksLikeYoutubeId = /^[A-Za-z0-9_-]{11}$/.test(svid);
    let srcCell;
    if (placed && placedProv === 'auto') {
      // T can come from a real ThemerrDB download or a hash/exact
      // match adopt (byte-identical to the upstream); both are 'auto'.
      srcCell = '<span class="link-badge link-badge-themerrdb" title="motif manages from ThemerrDB">T</span>';
    } else if (placed && placedProv === 'manual') {
      let kind = sourceKind;
      if (!kind) {
        // Pre-1.10.12 fallback heuristic.
        const wasUploadedOrUrl = (svid === '' || looksLikeYoutubeId);
        kind = (!isOrphanRow || wasUploadedOrUrl) ? 'url' : 'adopt';
      }
      if (kind === 'adopt') {
        srcCell = '<span class="link-badge link-badge-adopt" title="motif adopted an existing local theme.mp3 (sidecar is the source of truth, no ThemerrDB link)">A</span>';
      } else {
        srcCell = '<span class="link-badge link-badge-user" title="motif manages this user-provided theme (UI upload or manual YouTube URL)">U</span>';
      }
    } else if (sidecarOnly) {
      srcCell = '<span class="link-badge link-badge-manual" title="local theme.mp3 sidecar — motif does not manage this file (run /scans → ADOPT to take ownership)">M</span>';
    } else if (it.plex_has_theme) {
      srcCell = '<span class="link-badge link-badge-cloud" title="theme present in Plex (Plex agent / cloud) — motif does not manage this file">P</span>';
    } else {
      srcCell = '<span class="muted" title="no theme">—</span>';
    }

    const dl = downloaded ? 'on' : '';
    const pl = placed ? 'on' : '';
    let linkCell = '<span class="link-glyph link-glyph-none">—</span>';
    if (it.placement_kind === 'hardlink') {
      linkCell = '<span class="link-glyph link-glyph-hardlink" title="hardlink">=</span>';
    } else if (it.placement_kind === 'copy') {
      linkCell = '<span class="link-glyph link-glyph-copy" title="copy">C</span>';
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
      titleGlyphs.push(
        `<button class="title-glyph title-glyph-fail" title="${htmlEscape(human)}" `
        + `data-act="open-override" data-mt="${themeMt}" data-id="${themeId}" `
        + `data-kind-human="${htmlEscape(human)}" data-msg="${htmlEscape(it.failure_message || '')}" type="button">⚠</button>`
      );
      rowExtra = ' class="row-failure"';
    }

    const imdb = it.guid_imdb || '';
    const imdbLink = imdb
      ? `<a href="https://www.imdb.com/title/${htmlEscape(imdb)}" target="_blank" rel="noopener">${htmlEscape(imdb)}</a>`
      : '<span class="muted">—</span>';

    const sectionLabel = it.section_title ? ` <span class="muted small">[${htmlEscape(it.section_title)}]</span>` : '';

    // v1.10.13 Option A row-action reorg. Each row renders at most:
    //   [ⓘ INFO]  [primary contextual]  [DEL]  [⋯ overflow]
    // ⓘ — only when themed (ThemerrDB record exists)
    // primary — single contextual button picked by row state (see
    //   primaryRowAction below). Never a duplicate of an overflow item.
    // DEL — only when there's a motif placement to remove
    // ⋯ — overflow menu containing URL, UPLOAD, REPLACE w/ TDB,
    //   PURGE, and any state-specific alts. Native <details> popover.
    //
    // While a job is in flight (lockManualActions), the primary button
    // and every overflow item is disabled. INFO and the overflow trigger
    // stay clickable so the user can still inspect.
    const lockManualActions = !!it.job_in_flight || awaitingApproval;
    const lockTitle = it.job_in_flight
      ? 'wait for current job to finish'
      : (awaitingApproval ? 'pending placement approval — review at /pending'
                          : '');
    const isOrphan = it.upstream_source === 'plex_orphan';
    const isManual = it.provenance === 'manual';
    const isThemerrDb = it.upstream_source && it.upstream_source !== 'plex_orphan';

    function lockedAttrs(extraTitle) {
      return lockManualActions
        ? ` disabled title="${htmlEscape(lockTitle)}"`
        : ` title="${htmlEscape(extraTitle)}"`;
    }

    // Primary picks at most one contextual action.
    function primaryRowAction() {
      if (sidecarOnly) {
        return {
          act: 'adopt-sidecar',
          label: 'ADOPT',
          tip: "take ownership of the existing theme.mp3 sidecar; motif manages it from now on",
          rk: it.rating_key,
        };
      }
      if (themed && themeId !== null && themeId !== undefined) {
        if (isManual && isThemerrDb) {
          return {
            act: 'revert',
            label: 'REVERT',
            tip: 'clear manual override and download from ThemerrDB',
            mt: themeMt, id: themeId,
          };
        }
        if (!downloaded) {
          return {
            act: 'redl',
            label: 'DOWNLOAD',
            tip: 'download from ThemerrDB',
            mt: themeMt, id: themeId,
          };
        }
        if (downloaded && !placed) {
          return {
            act: 'replace',
            label: 'REPLACE',
            tip: "push motif's downloaded theme back into the Plex folder (no re-download)",
            mt: themeMt, id: themeId,
          };
        }
        // downloaded + placed
        return {
          act: 'redl',
          label: 'RE-DL',
          tip: 're-download from ThemerrDB',
          mt: themeMt, id: themeId,
        };
      }
      return null;  // untracked, P, etc — overflow URL/UPLOAD only
    }

    function primaryButtonHtml(p) {
      if (!p) return '';
      const dataset = [
        p.rk !== undefined ? `data-rk="${htmlEscape(p.rk)}"` : '',
        p.mt !== undefined ? `data-mt="${htmlEscape(p.mt)}"` : '',
        p.id !== undefined ? `data-id="${htmlEscape(p.id)}"` : '',
        `data-title="${htmlEscape(it.plex_title)}"`,
        `data-year="${htmlEscape(it.year || '')}"`,
      ].filter(Boolean).join(' ');
      const cls = (p.act === 'replace' || p.act === 'revert')
        ? 'btn btn-tiny btn-warn' : 'btn btn-tiny';
      return `<button class="${cls}" data-act="${p.act}" ${dataset}${lockedAttrs(p.tip)}>${p.label}</button>`;
    }

    function overflowItemHtml(act, label, tip, extras = {}) {
      const dataset = [
        extras.rk !== undefined ? `data-rk="${htmlEscape(extras.rk)}"` : '',
        extras.mt !== undefined ? `data-mt="${htmlEscape(extras.mt)}"` : '',
        extras.id !== undefined ? `data-id="${htmlEscape(extras.id)}"` : '',
        `data-title="${htmlEscape(it.plex_title)}"`,
        `data-year="${htmlEscape(it.year || '')}"`,
        extras.orphan !== undefined ? `data-orphan="${extras.orphan ? '1' : '0'}"` : '',
      ].filter(Boolean).join(' ');
      const cls = extras.danger ? 'btn btn-tiny btn-danger'
                : extras.warn   ? 'btn btn-tiny btn-warn'
                :                 'btn btn-tiny';
      return `<button class="${cls}" data-act="${act}" ${dataset}${lockedAttrs(tip)}>${label}</button>`;
    }

    // Overflow items, in order. Drop entries that don't apply to this
    // row's state. URL + UPLOAD are always available so users can
    // re-source the theme regardless of current state.
    const overflow = [];
    overflow.push(overflowItemHtml(
      'manual-url', 'SET URL',
      'provide a YouTube URL (manual override)',
      { rk: it.rating_key, warn: true },
    ));
    overflow.push(overflowItemHtml(
      'upload-theme', 'UPLOAD MP3',
      'upload an MP3 file as the theme',
      { rk: it.rating_key },
    ));
    // v1.10.14: REPLACE w/ TDB also offered on motif-managed manual
    // rows (U / A) when ThemerrDB tracks the title — lets the user
    // swap their manual choice for the upstream version without
    // first DELing. Hidden when ThemerrDB doesn't have the title
    // (no point — there's nothing to replace from).
    const isManualPlacement = placed && placedProv === 'manual';
    if (isThemerrDb && (sidecarOnly || isManualPlacement)) {
      overflow.push(overflowItemHtml(
        'replace-with-themerrdb', 'REPLACE w/ TDB',
        sidecarOnly
          ? "overwrite the existing sidecar with motif's ThemerrDB download"
          : "swap your manual theme for the ThemerrDB download",
        { rk: it.rating_key, warn: true },
      ));
    }
    // RE-DL is offered as a secondary on T-rows when REPLACE is the
    // primary (downloaded + not placed) — lets the user force a fresh
    // fetch instead of re-pushing the cached file.
    if (themed && downloaded && !placed && !(isManual && isThemerrDb)) {
      overflow.push(overflowItemHtml(
        'redl', 'RE-DL',
        're-download from ThemerrDB',
        { mt: themeMt, id: themeId },
      ));
    }
    if (downloaded || isOrphan) {
      overflow.push(overflowItemHtml(
        'purge', '× PURGE',
        isOrphan ? 'delete everything: orphan record + files'
                 : 'delete everything: motif drops the canonical + tracking',
        { mt: themeMt, id: themeId, orphan: isOrphan, danger: true },
      ));
    }

    const acts = [];
    if (themed && themeId !== null && themeId !== undefined) {
      acts.push(`<button class="btn btn-tiny" data-act="info" data-mt="${themeMt}" data-id="${themeId}" title="ThemerrDB record details">ⓘ</button>`);
    }
    const primaryHtml = primaryButtonHtml(primaryRowAction());
    if (primaryHtml) acts.push(primaryHtml);
    if (placed) {
      const delTip = "remove from Plex folder; canonical stays (use REPLACE to put it back)";
      acts.push(`<button class="btn btn-tiny btn-danger" data-act="unplace" data-mt="${themeMt}" data-id="${themeId}" data-title="${htmlEscape(it.theme_title || it.plex_title)}"${lockedAttrs(delTip)}>DEL</button>`);
    }
    // Overflow popover. Native <details>; CSS positions the panel and a
    // global click-outside handler closes any other open menus.
    if (overflow.length) {
      acts.push(
        `<details class="row-overflow"><summary class="btn btn-tiny" title="more actions">⋯</summary>`
        + `<div class="row-overflow-panel">${overflow.join('')}</div></details>`
      );
    }
    const actions = `<div class="row-actions">${acts.join('')}</div>`;

    const selKey = libKey(it);
    const selected = libraryState.selected.has(selKey);
    return `
      <tr${rowExtra}>
        <td class="col-state"><input type="checkbox" data-lib-select="${htmlEscape(selKey)}" ${selected ? 'checked' : ''} /></td>
        <td>
          <div class="title-cell">
            ${titleGlyphs.join('')}
            <span class="title-cell-name">${htmlEscape(it.plex_title)}${sectionLabel}</span>
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
    // Hide the STANDARD/4K toggle when only one variant exists for the
    // active tab. If only 4K exists, auto-flip libraryState.fourk so the
    // page actually shows content. Idempotent — safe to call repeatedly.
    const tabEl = document.getElementById('library-tab');
    if (!tabEl) return;
    const tab = tabEl.value;
    const av = ta && ta[tab];
    if (!av) return;
    const toggle = document.querySelector('.chips[aria-label="resolution"]');
    if (!toggle) return;
    const both = av.standard && av.fourk;
    toggle.style.display = both ? '' : 'none';
    if (!both && av.fourk && !av.standard && libraryState.fourk === false) {
      libraryState.fourk = true;
      loadLibrary().catch(()=>{});
    } else if (!both && av.standard && !av.fourk && libraryState.fourk === true) {
      libraryState.fourk = false;
      loadLibrary().catch(()=>{});
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
    if (all) all.checked = false;  // sync state-all is reset on render
  }

  function bindLibrary() {
    const tabEl = document.getElementById('library-tab');
    if (!tabEl) return;

    // 4K toggle
    document.querySelectorAll('.chips [data-fourk]').forEach((b) => {
      b.addEventListener('click', () => {
        document.querySelectorAll('.chips [data-fourk]').forEach((x) =>
          x.classList.remove('chip-active'));
        b.classList.add('chip-active');
        libraryState.fourk = b.dataset.fourk === '1';
        libraryState.page = 1;
        loadLibrary().catch(console.error);
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
          loadLibrary().catch(console.error);
        });
      });

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
      btn.textContent = '// REFRESHING…';
      try {
        await api('POST', '/api/library/refresh', {
          tab: libraryState.tab,
          fourk: !!libraryState.fourk,
        });
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

    // Select-all on the visible page
    document.getElementById('library-select-all')?.addEventListener('change', (e) => {
      const on = e.currentTarget.checked;
      document.querySelectorAll('#library-body input[data-lib-select]').forEach((cb) => {
        cb.checked = on;
        const k = cb.dataset.libSelect;
        if (on) libraryState.selected.add(k);
        else libraryState.selected.delete(k);
      });
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
      // Build the items list. Only entries whose key looks like
      // "<movie|tv>:<numeric>" can be downloaded — Plex-only items keyed
      // by rating_key get filtered out (no themes row to download from).
      const items = [];
      for (const k of libraryState.selected) {
        const parts = k.split(':');
        if (parts.length !== 2) continue;
        const id = Number(parts[1]);
        if (!Number.isFinite(id)) continue;
        items.push({ media_type: parts[0], tmdb_id: id });
      }
      if (items.length === 0) {
        alert('Nothing downloadable in selection (Plex-only items have no ThemerrDB record).');
        return;
      }
      const btn = e.currentTarget;
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = '// QUEUING';
      try {
        const r = await api('POST', '/api/library/download-batch', { items });
        btn.textContent = `// ${r.enqueued} QUEUED`;
        libraryState.selected.clear();
        setTimeout(() => loadLibrary().catch(()=>{}), 1000);
        libraryRapidPoll();
      } catch (err) {
        alert('Bulk download failed: ' + err.message);
      }
      setTimeout(() => { btn.disabled = false; btn.textContent = orig; }, 2500);
    });

    // v1.10.13: close any open row-overflow popover when the user clicks
    // anywhere outside it. Native <details> handles open/close on the
    // summary itself, but doesn't auto-dismiss on outside click.
    document.addEventListener('click', (e) => {
      const inside = e.target.closest('.row-overflow');
      document.querySelectorAll('.row-overflow[open]').forEach((d) => {
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
      // Action buttons inside an overflow panel should also close the
      // popover after firing; let the action handler run first, then
      // collapse the parent <details>.
      const overflowParent = e.target.closest('.row-overflow');
      const btn = e.target.closest('button[data-act]');
      if (!btn) return;
      if (overflowParent) {
        // schedule close after handler runs (microtask)
        setTimeout(() => overflowParent.removeAttribute('open'), 0);
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
      } else if (act === 'purge') {
        await purgeTheme(btn.dataset.mt, btn.dataset.id,
                         btn.dataset.title || '',
                         btn.dataset.orphan === '1');
        await loadLibrary().catch(()=>{});
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
        const title = btn.dataset.title || 'this item';
        if (!confirm(`Adopt the existing theme.mp3 for "${title}"?\n\nMotif will hardlink the file into /themes and manage it from now on.`)) return;
        try {
          await api('POST', `/api/plex_items/${encodeURIComponent(btn.dataset.rk)}/adopt-sidecar`);
          libraryRapidPoll();
          await loadLibrary().catch(()=>{});
        } catch (e) {
          alert('Adopt failed: ' + e.message);
        }
      } else if (act === 'replace-with-themerrdb') {
        const title = btn.dataset.title || 'this item';
        if (!confirm(`Replace the existing theme for "${title}" with the ThemerrDB download?\n\nMotif will fetch from upstream and overwrite the current sidecar.`)) return;
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
    const ytUrl = ovr?.youtube_url || t.youtube_url || '';
    const ytId = ovr?.youtube_video_id || t.youtube_video_id ||
                 (ytUrl ? (ytUrl.match(/[?&]v=([^&]+)/) || [])[1] : '');
    const imdb = t.imdb_id ? `<a href="https://www.imdb.com/title/${htmlEscape(t.imdb_id)}" target="_blank" rel="noopener">${htmlEscape(t.imdb_id)}</a>` : '<span class="muted">—</span>';
    const tmdbLink = t.tmdb_id && t.tmdb_id > 0
      ? `<a href="https://www.themoviedb.org/${mediaType === 'tv' ? 'tv' : 'movie'}/${t.tmdb_id}" target="_blank" rel="noopener">${t.tmdb_id}</a>`
      : '<span class="muted">orphan</span>';
    const ytLink = ytUrl
      ? `<a href="${htmlEscape(ytUrl)}" target="_blank" rel="noopener">${htmlEscape(ytUrl)}</a>`
      : '<span class="muted">—</span>';
    const failBlock = t.failure_kind
      ? `<dt>last failure</dt><dd class="accent-red">${htmlEscape(t.failure_kind)}${t.failure_message ? ' · ' + htmlEscape(t.failure_message) : ''}</dd>`
      : '';
    const ovrBlock = ovr
      ? `<dt>override</dt><dd>${htmlEscape(ovr.youtube_url || '')}<br><span class="muted small">set by ${htmlEscape(ovr.set_by || '')} at ${htmlEscape(ovr.set_at || '')}${ovr.note ? ' · ' + htmlEscape(ovr.note) : ''}</span></dd>`
      : '';
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
        <dt>youtube url</dt><dd>${ytLink}</dd>
        <dt>video id</dt><dd>${htmlEscape(ytId || '—')}</dd>
        <dt>added</dt><dd class="muted small">${htmlEscape(t.youtube_added_at || '—')}</dd>
        <dt>edited</dt><dd class="muted small">${htmlEscape(t.youtube_edited_at || '—')}</dd>
        ${failBlock}
        ${ovrBlock}
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
    setInterval(refreshTopbarStatus, 15000);

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
    if (path === '/') setInterval(() => loadDashboard().catch(() => {}), 10000);
    if (path === '/queue') setInterval(() => loadQueue().catch(() => {}), 5000);
    if (path === '/pending') setInterval(() => loadPending().catch(() => {}), 8000);
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
