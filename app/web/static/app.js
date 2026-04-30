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
                  '/coverage': 'coverage', '/queue': 'queue',
                  '/scans': 'scans',
                  '/libraries': 'libraries',
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

      // Drive dry-run banner
      const banner = $('#dry-run-banner');
      if (banner) {
        banner.style.display = stats.dry_run ? '' : 'none';
        document.body.classList.toggle('dry-run-on', !!stats.dry_run);
      }
      // Drive paths-not-configured banner
      updatePathsBanner(stats);
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

  function bindDashboard() {
    const dlPlaceBtn = $('#sync-download-place-btn');
    const dlOnlyBtn = $('#sync-download-only-btn');
    const placeStagedBtn = $('#sync-place-staged-btn');
    if (!dlPlaceBtn && !dlOnlyBtn && !placeStagedBtn) return;

    async function runSync(btn, originalLabel, body) {
      btn.disabled = true;
      btn.textContent = '// QUEUED';
      try {
        await api('POST', '/api/sync/now', body);
      } catch (e) {
        alert('Sync failed: ' + e.message);
      }
      setTimeout(() => {
        btn.disabled = false;
        btn.textContent = originalLabel;
        loadDashboard().catch(console.error);
      }, 1500);
    }

    dlPlaceBtn?.addEventListener('click', (ev) => {
      runSync(ev.currentTarget, '// SYNC + PLACE', { download_only: false });
    });
    dlOnlyBtn?.addEventListener('click', (ev) => {
      runSync(ev.currentTarget, '// DOWNLOAD ONLY', { download_only: true });
    });
    placeStagedBtn?.addEventListener('click', async (ev) => {
      const btn = ev.currentTarget;
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = '// PLACING';
      try {
        const res = await api('POST', '/api/pending/place', { all: true });
        if (res.enqueued === 0) alert('Nothing staged to place.');
      } catch (e) {
        alert('Place staged failed: ' + e.message);
      }
      setTimeout(() => {
        btn.disabled = false;
        btn.textContent = orig;
        loadDashboard().catch(console.error);
      }, 1500);
    });
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

        // Failure flag
        let failCell = '<span class="muted">—</span>';
        let rowExtra = '';
        if (it.failure_kind) {
          const needsManual = ['video_private','video_removed','video_age_restricted','geo_blocked'].includes(it.failure_kind);
          const human = {
            'cookies_expired': 'YouTube cookies expired',
            'video_private': 'Video is private',
            'video_removed': 'Video was removed',
            'video_age_restricted': 'Age-restricted',
            'geo_blocked': 'Geo-blocked',
            'network_error': 'Network error',
            'unknown': 'Unknown failure'
          }[it.failure_kind] || it.failure_kind;
          if (needsManual) {
            failCell = `<span class="fail-glyph fail-glyph-bad" title="${htmlEscape(human)} — needs manual override">!</span>`;
          } else {
            failCell = `<span class="fail-glyph fail-glyph-warn" title="${htmlEscape(human)}">·</span>`;
          }
          rowExtra = ' class="row-failure"';
        } else if (it.pending_update) {
          failCell = '<span class="fail-glyph fail-glyph-info" title="Update available">↑</span>';
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

        const actions = it.pending_update
          ? `<button class="btn btn-tiny" data-act="open" data-mt="${it.media_type}" data-id="${it.tmdb_id}">DETAILS</button>
             <button class="btn btn-tiny btn-warn" data-act="accept-update" data-mt="${it.media_type}" data-id="${it.tmdb_id}">ACCEPT</button>
             <button class="btn btn-tiny" data-act="decline-update" data-mt="${it.media_type}" data-id="${it.tmdb_id}">KEEP</button>`
          : `<button class="btn btn-tiny" data-act="open" data-mt="${it.media_type}" data-id="${it.tmdb_id}">DETAILS</button>
             <button class="btn btn-tiny btn-warn" data-act="redl" data-mt="${it.media_type}" data-id="${it.tmdb_id}">RE-DL</button>`;

        return `
          <tr${rowExtra}>
            <td class="col-state">${failCell}</td>
            <td>
              <div class="title-cell">
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

  async function redownload(mediaType, tmdbId, btn) {
    if (btn) btn.disabled = true;
    try {
      await api('POST', `/api/items/${mediaType}/${tmdbId}/redownload`);
      if (btn) btn.textContent = 'QUEUED';
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
    if (!$('#movies-missing-body')) return;

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

      if (stats.storage.copies > 0) {
        $('#copies-block').style.display = '';
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
        $('#copies-block').style.display = 'none';
      }
    } catch (e) {
      console.error('storage stats failed', e);
    }

    // Plex coverage report
    let data;
    try {
      data = await api('GET', '/api/coverage/plex');
    } catch (e) {
      $('#movies-missing-body').innerHTML = `<tr><td colspan="4" class="accent-red">${htmlEscape(e.message)}</td></tr>`;
      return;
    }
    if (!data.enabled) {
      $('#movies-missing-body').innerHTML = '<tr><td colspan="4" class="muted">Plex integration disabled</td></tr>';
      return;
    }
    if (data.error) {
      $('#movies-missing-body').innerHTML = `<tr><td colspan="4" class="accent-red">Plex error: ${htmlEscape(data.error)}</td></tr>`;
      return;
    }

    const renderMissing = (items, bodyId) => {
      const missing = items.filter((it) => !it.has_theme && it.motif_available);
      $(bodyId).innerHTML = missing.length ? missing.map((it) => `
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
      const row = `
        <tr style="${stale ? 'opacity:0.45' : ''}">
          <td class="lib-col-section"><strong>${htmlEscape(s.title)}</strong>${stale ? ' <span class="muted" style="font-size:var(--t-tiny)">(stale)</span>' : ''}</td>
          <td class="lib-col-type"><span class="muted">${htmlEscape(s.type)}</span></td>
          <td class="lib-col-mgd">
            <input type="checkbox" data-section-toggle="${htmlEscape(s.section_id)}" ${included ? 'checked' : ''} />
          </td>
          <td class="lib-col-num">${fmt.num(s.placed_count)}</td>
          <td class="lib-col-num">${s.copies_count > 0 ? '<span class="accent">'+fmt.num(s.copies_count)+'</span>' : fmt.num(s.copies_count)}</td>
          <td class="lib-locations" style="font-family:var(--font-mono);font-size:var(--t-tiny);color:var(--fg-dim)">${locations}</td>
          <td class="lib-col-actions">
            <span class="muted" style="font-size:var(--t-tiny)">id=${htmlEscape(s.section_id)}</span>
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

    document.addEventListener('change', async (e) => {
      const cb = e.target.closest('input[data-section-toggle]');
      if (!cb) return;
      const fd = new FormData();
      fd.append('included', cb.checked ? 'true' : 'false');
      try {
        await api('POST', `/api/libraries/${encodeURIComponent(cb.dataset.sectionToggle)}/include`, fd);
      } catch (err) {
        alert('Update failed: ' + err.message);
        cb.checked = !cb.checked;  // revert
      }
    });
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

      // Auto-poll if any run is in progress
      if (data.running) {
        setTimeout(() => loadScansList().catch(console.error), 3000);
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
      $('#scan-detail-meta').textContent =
        `started ${run.started_at} · ${run.findings_count} findings`;
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
      const decision = f.decision === 'pending' ? '–' : htmlEscape(f.decision);
      const isAdopted = !!f.adopted_at;
      const actions = isAdopted
        ? '<span class="muted small">DONE</span>'
        : `<select class="input" data-decide="${f.id}">
             <option value="">–</option>
             <option value="adopt">adopt</option>
             <option value="replace">replace</option>
             <option value="keep_existing">keep</option>
             <option value="ignore">ignore</option>
           </select>`;
      return `<tr data-finding="${f.id}">
        <td><input type="checkbox" data-select="${f.id}" ${checked} ${isAdopted ? 'disabled' : ''} /></td>
        <td><span class="kind-${htmlEscape(f.finding_kind)}">${htmlEscape(f.finding_kind)}</span></td>
        <td title="${htmlEscape(f.media_folder)}">${folder}</td>
        <td>${resolved}</td>
        <td><code class="small">${htmlEscape(f.file_sha256.substring(0, 12))}…</code></td>
        <td>${decision}</td>
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

    $('#scan-bulk-adopt-btn')?.addEventListener('click', async () => {
      // Auto-collect all hash_match findings on this page
      const ids = scansState.findings
        .filter((f) => f.finding_kind === 'hash_match' && !f.adopted_at)
        .map((f) => f.id);
      if (!ids.length) {
        alert('No hash_match findings on this page to bulk-adopt.');
        return;
      }
      if (!confirm(`Adopt ${ids.length} hash-matched theme(s)? Each will be hardlinked into your themes_dir.`)) {
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
    });

    document.querySelectorAll('[data-bulk]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const decision = btn.dataset.bulk;
        const ids = Array.from(scansState.selectedIds);
        if (!ids.length) return;
        if (!confirm(`Apply "${decision}" to ${ids.length} finding(s)?`)) return;
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
      return `
        <tr>
          <td><input type="checkbox" data-pending-key="${htmlEscape(k)}" ${checked} /></td>
          <td><strong>${htmlEscape(it.title || '—')}</strong></td>
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

    // TVDB test key handler
    const tvdbBtn = document.getElementById('tvdb-test-btn');
    if (tvdbBtn) {
      tvdbBtn.addEventListener('click', async () => {
        const result = document.getElementById('tvdb-test-result');
        const input = document.querySelector('[data-cfg-field="plex.tvdb_api_key"]');
        const key = input && input.value && input.value !== '***' ? input.value : null;
        result.textContent = '... testing';
        result.style.color = '';
        try {
          const body = key ? { api_key: key } : {};
          const r = await api('POST', '/api/tvdb/test', body);
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

    // Auto-refresh on relevant pages
    const path = window.location.pathname;
    if (path === '/') setInterval(() => loadDashboard().catch(() => {}), 10000);
    if (path === '/queue') setInterval(() => loadQueue().catch(() => {}), 5000);
    if (path === '/pending') setInterval(() => loadPending().catch(() => {}), 8000);
  });
})();
