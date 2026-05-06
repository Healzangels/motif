/* v1.12.106: ops side-drawer.
 *
 * Polls /api/progress (1s active, 10s idle), renders one card per
 * running/recently-finished op into the drawer, and drives the topbar
 * mini-bar + per-tone op-pill counts. Self-contained module exposed
 * as window.motifOps so app.js can co-exist without sharing IIFE
 * scope.
 *
 * UI surfaces:
 *   - Topbar mini-bar (#op-mini): visible whenever ≥1 op is running.
 *     Shows the most recent op's stage label + thin progress strip
 *     + percent. Click → opens drawer.
 *   - Drawer (#ops-drawer): full per-op detail. Renders running ops
 *     first, then a "// LAST OPS" section with up to 3 recently-
 *     finished rows so users can see what just happened.
 *   - Counter interpolation: between polls we tween the visible
 *     "current" number toward the latest sample using the smoothed
 *     throughput rate. Makes the UI feel alive at 1s polling.
 */
(function () {
  'use strict';

  // ── stage timelines per op kind ───────────────────────────────
  const STAGE_TIMELINE = {
    // v1.12.121 (Phase A): snapshot stages run before index/fetch
    // when sync.source = "database". They never appear on the
    // remote path, so the timeline render flags them as is-skipped
    // when stage advances past them without their key showing up.
    tdb_sync: [
      { key: 'snapshot_download', label: 'Snap dl' },
      { key: 'snapshot_extract',  label: 'Extract' },
      { key: 'index_movie',  label: 'Movies idx' },
      { key: 'fetch_movie',  label: 'Movies' },
      { key: 'index_tv',     label: 'TV idx' },
      { key: 'fetch_tv',     label: 'TV' },
      { key: 'resolve',      label: 'Resolve' },
      { key: 'prune',        label: 'Prune' },
    ],
    plex_enum: [
      { key: 'enumerate',    label: 'Enumerate' },
      { key: 'reconcile',    label: 'Reconcile' },
    ],
    // Queue ops have no fixed stage timeline — just a single
    // indeterminate stage that pulses for as long as work remains.
    download_queue: [],
    place_queue:    [],
    scan_queue:     [],
    refresh_queue:  [],
    relink_queue:   [],
    adopt_queue:    [],
  };

  const TONE_BY_KIND = {
    tdb_sync:       'tdb',
    plex_enum:      'plex',
    download_queue: 'warn',
    place_queue:    'warn',
    scan_queue:     'warn',
    // v1.12.118: post-place Plex refresh / relink-stale-paths /
    // adopt-sidecar queues join the same ops surface.
    refresh_queue:  'warn',
    relink_queue:   'warn',
    adopt_queue:    'warn',
  };
  const KIND_LABEL = {
    tdb_sync:       'THEMERRDB SYNC',
    plex_enum:      'PLEX SCAN',
    download_queue: 'DOWNLOAD QUEUE',
    place_queue:    'PLACE QUEUE',
    scan_queue:     'DISK SCAN',
    refresh_queue:  'REFRESH QUEUE',
    relink_queue:   'RELINK QUEUE',
    adopt_queue:    'ADOPT QUEUE',
  };
  const STAGE_TIMELINE_QUEUE = []; // queue ops have no fixed timeline
  ['refresh_queue', 'relink_queue', 'adopt_queue'].forEach(
    (k) => { /* placeholder; STAGE_TIMELINE map updated below */ },
  );

  // ── state ─────────────────────────────────────────────────────
  let state = {
    ops: [],
    pollTimer: null,
    pollInterval: 10000,           // 10s idle
    drawerOpen: false,
    // per op_id: smoothed counter state for interpolation
    counters: {},
  };

  // ── network ───────────────────────────────────────────────────
  async function fetchProgress() {
    try {
      const r = await fetch('/api/progress', { credentials: 'same-origin' });
      if (!r.ok) return null;
      return r.json();
    } catch (_) {
      return null;
    }
  }

  async function postCancel(opId) {
    try {
      const r = await fetch(
        `/api/progress/${encodeURIComponent(opId)}/cancel`,
        { method: 'POST', credentials: 'same-origin' });
      return r.ok;
    } catch (_) {
      return false;
    }
  }

  // ── helpers ───────────────────────────────────────────────────
  function esc(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  function fmtNum(n) {
    if (n == null) return '—';
    return Number(n).toLocaleString();
  }

  function fmtDuration(seconds) {
    if (!isFinite(seconds) || seconds < 0) return '—';
    if (seconds < 60) return `${Math.round(seconds)}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  }

  function fmtClock(iso) {
    if (!iso) return '';
    try {
      const d = new Date(iso);
      return d.toLocaleTimeString(undefined,
        { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch (_) { return ''; }
  }

  function smoothedRate(throughput) {
    if (!throughput || !throughput.length) return 0;
    // Last 10 samples, simple mean. Throughput buffer is already
    // capped at 30 in detail_json — taking the tail gives a
    // "right now" feel rather than averaging across the whole op.
    const tail = throughput.slice(-10);
    const sum = tail.reduce((a, x) => a + (x.rate || 0), 0);
    return sum / tail.length;
  }

  function eta(op) {
    const rate = smoothedRate(op.detail && op.detail.throughput);
    const total = op.stage_total || op.processed_est || 0;
    const cur = op.stage_current || op.processed_total || 0;
    const remaining = total - cur;
    if (rate <= 0 || remaining <= 0) return null;
    return remaining / rate;
  }

  function pctOf(op) {
    const total = op.stage_total || op.processed_est || 0;
    const cur = op.stage_current || op.processed_total || 0;
    if (total <= 0) return null;
    return Math.min(100, Math.max(0, (cur / total) * 100));
  }

  // ── render ────────────────────────────────────────────────────
  function renderTimeline(op) {
    const stages = STAGE_TIMELINE[op.kind];
    if (!stages || !stages.length) return '';
    const currentIdx = stages.findIndex((s) => s.key === op.stage);
    const cells = stages.map((s, i) => {
      let cls = '';
      if (currentIdx >= 0) {
        if (i < currentIdx) cls = 'is-done';
        else if (i === currentIdx) cls = 'is-current';
      } else if (op.status !== 'running' && op.status !== 'cancelling') {
        // Op finished — every step counts as done.
        cls = 'is-done';
      }
      return `<div class="op-card-timeline-step ${cls}" title="${esc(s.label)}"></div>`;
    }).join('');
    const labels = stages.map((s) => `<span>${esc(s.label)}</span>`).join('');
    return `<div class="op-card-timeline">${cells}</div>
            <div class="op-card-timeline-labels">${labels}</div>`;
  }

  function renderSparkline(op) {
    const buf = (op.detail && op.detail.throughput) || [];
    if (!buf.length) return '';
    const max = Math.max(...buf.map((x) => x.rate || 0));
    if (max <= 0) return '';
    const bars = buf.map((x) => {
      const h = Math.max(2, Math.round((x.rate / max) * 16));
      return `<div class="op-card-spark-bar" style="height:${h}px"></div>`;
    }).join('');
    return `<div class="op-card-spark">${bars}</div>`;
  }

  function renderActivity(op) {
    const items = (op.detail && op.detail.activity) || [];
    if (!items.length) return '';
    return '<div class="op-card-activity">' +
      items.map((it) => `
        <div class="op-card-activity-item">
          <span class="op-card-activity-time">${esc(fmtClock(it.ts))}</span>
          <span class="op-card-activity-msg">${esc(it.msg)}</span>
        </div>`).join('') +
      '</div>';
  }

  function renderError(op) {
    const msg = op.detail && op.detail.error_message;
    if (!msg) return '';
    return `<div class="op-card-error">${esc(msg)}</div>`;
  }

  // v1.12.126 Phase A.5: green-tone callout for a no-op sync — the
  // 304-short-circuit path. Codeload reported the database tree
  // hasn't moved since last sync, so motif skipped the entire
  // upsert pipeline and only ran the local prune sweeps. This is
  // the desired-good-case for a daily cron run and the user should
  // be able to tell at a glance that nothing changed (vs. a full
  // run with 0 new + 0 updated, which looks identical from the
  // movies_seen / tv_seen counters alone).
  function renderNoChangesBadge(op) {
    if (!(op.detail && op.detail.no_changes)) return '';
    return `
      <div class="op-card-nochanges" title="ThemerrDB tree at HEAD is byte-identical to the last sync — codeload returned 304 Not Modified. Local prune sweeps still ran.">
        <span class="op-card-nochanges-mark">✓</span>
        <span class="op-card-nochanges-text">
          // NO CHANGES — TDB tree unchanged since last sync
        </span>
      </div>`;
  }

  // v1.12.121 (Phase A): sticky fallback indicator.
  // When the snapshot path failed and the run fell back to remote,
  // sync.py sets detail.fallback_active=true (+ detail.fallback_reason
  // for the tooltip). The op-card surfaces it as a warn-tone callout;
  // the idle pill picks it up from the most-recent finished tdb_sync
  // and stays warn-tinted until the next successful sync clears it.
  function renderFallbackBadge(op) {
    if (!(op.detail && op.detail.fallback_active)) return '';
    const why = op.detail.fallback_reason
      ? esc(op.detail.fallback_reason)
      : 'GitHub snapshot unavailable';
    return `
      <div class="op-card-fallback" title="${why}">
        <span class="op-card-fallback-mark">!</span>
        <span class="op-card-fallback-text">
          // FALLBACK · ran via slow remote path (${why})
        </span>
      </div>`;
  }

  function latestSyncFallbackInfo(ops) {
    // Find the most-recently-updated tdb_sync row (running or
    // finished). If it carries fallback_active, the idle pill should
    // tint warn until the next clean run.
    const syncs = ops.filter((o) => o.kind === 'tdb_sync');
    if (!syncs.length) return null;
    syncs.sort((a, b) =>
      String(b.updated_at || b.finished_at || '')
        .localeCompare(String(a.updated_at || a.finished_at || '')));
    const top = syncs[0];
    if (top && top.detail && top.detail.fallback_active) {
      return { reason: top.detail.fallback_reason || 'GitHub snapshot unavailable' };
    }
    return null;
  }

  function renderCard(op) {
    const tone = TONE_BY_KIND[op.kind] || 'tdb';
    const isLive = (op.status === 'running' || op.status === 'cancelling');
    const pct = pctOf(op);
    const rate = smoothedRate(op.detail && op.detail.throughput);
    const etaSec = eta(op);
    const elapsed = op.started_at
      ? (new Date(op.finished_at || Date.now())
          - new Date(op.started_at)) / 1000
      : null;

    return `
      <div class="op-card op-tone-${tone} op-status-${op.status}"
           data-op-id="${esc(op.op_id)}">
        <div class="op-card-head">
          <span class="op-card-kind">// ${esc(KIND_LABEL[op.kind] || op.kind)}</span>
          <span class="op-card-status">${esc(op.status.toUpperCase())}</span>
        </div>
        <div class="op-card-stage">${esc(op.stage_label || op.stage || '…')}</div>
        ${(op.stage_total > 0) ? `
          <div class="op-card-counter">
            <span class="op-card-counter-current"
                  data-op-counter
                  data-op-counter-target="${op.stage_current || 0}">
              ${fmtNum(op.stage_current)}
            </span>
            <span class="op-card-counter-total">/ ${fmtNum(op.stage_total)}</span>
          </div>
          <div class="op-card-bar">
            <div class="op-card-bar-fill"
                 style="width:${pct != null ? pct.toFixed(1) : 0}%"></div>
          </div>
        ` : (isLive ? `
          <!-- v1.12.124: indeterminate bar for live ops with no known
               total (queue ops + tdb_sync's pre-fetch / extract /
               resolve / prune phases). Pulses full-width instead of
               the bar disappearing entirely; counter is hidden so
               we don't display a fake "X / 0". -->
          <div class="op-card-bar op-card-bar-indet">
            <div class="op-card-bar-fill"></div>
          </div>
        ` : '')}
        <div class="op-card-meta">
          ${(rate > 0) ? `
            <span class="op-card-meta-item">
              <span class="op-card-meta-label">RATE</span>
              <span>${rate.toFixed(1)}/s</span>
            </span>` : ''}
          ${(etaSec != null && isLive) ? `
            <span class="op-card-meta-item">
              <span class="op-card-meta-label">ETA</span>
              <span>${esc(fmtDuration(etaSec))}</span>
            </span>` : ''}
          ${(elapsed != null) ? `
            <span class="op-card-meta-item">
              <span class="op-card-meta-label">${isLive ? 'ELAPSED' : 'RAN'}</span>
              <span>${esc(fmtDuration(elapsed))}</span>
            </span>` : ''}
          ${(op.error_count > 0) ? `
            <span class="op-card-meta-item" style="color:var(--red)">
              <span class="op-card-meta-label">ERRORS</span>
              <span>${fmtNum(op.error_count)}</span>
            </span>` : ''}
        </div>
        ${renderNoChangesBadge(op)}
        ${renderFallbackBadge(op)}
        ${renderTimeline(op)}
        ${renderSparkline(op)}
        ${renderActivity(op)}
        ${renderError(op)}
        ${isLive && !(op.detail && op.detail.synthetic) ? `
          <button class="op-card-cancel" data-op-cancel="${esc(op.op_id)}"
                  ${op.status === 'cancelling' ? 'disabled' : ''}>
            ${op.status === 'cancelling' ? '// CANCELLING…' : '// CANCEL'}
          </button>` : ''}
        ${isLive && (op.detail && op.detail.synthetic) ? `
          <div class="op-card-cancel-note muted small"
               style="margin-top:10px;text-align:center;opacity:0.6">
            // per-job cancel via /queue
          </div>` : ''}
      </div>`;
  }

  function renderDrawerBody(ops) {
    const running = ops.filter((o) => o.status === 'running' || o.status === 'cancelling');
    const finished = ops.filter((o) => o.status !== 'running' && o.status !== 'cancelling').slice(0, 3);
    const body = document.getElementById('ops-drawer-body');
    if (!body) return;
    if (!running.length && !finished.length) {
      body.innerHTML = '<div class="ops-drawer-empty">// idle · no ops in the last 24 hours</div>';
      return;
    }
    let html = '';
    if (running.length) {
      html += running.map(renderCard).join('');
    } else {
      html += '<div class="ops-drawer-empty" style="padding:14px 0">// idle · no ops running</div>';
    }
    if (finished.length) {
      html += `<div class="op-card-kind" style="margin:18px 0 6px">// LAST OPS</div>`;
      html += finished.map(renderCard).join('');
    }
    body.innerHTML = html;
  }

  function renderTopbar(ops) {
    const running = ops.filter((o) =>
      o.status === 'running' || o.status === 'pending' || o.status === 'cancelling');
    const mini = document.getElementById('op-mini');
    const overflow = document.getElementById('op-mini-overflow');
    const idle = document.getElementById('op-status-idle');
    if (!mini) return;
    // v1.12.121 (Phase A): idle pill picks up the most-recent
    // tdb_sync run's fallback flag and stays warn-tinted with a
    // descriptive tooltip until the next successful sync clears it.
    const fallback = latestSyncFallbackInfo(ops);
    if (idle) {
      idle.classList.toggle('op-pill-fallback', !!fallback);
      if (fallback) {
        idle.title = `Last sync used the slow remote fallback (${fallback.reason}) — GitHub may have been unreachable. Will retry the snapshot next sync.`;
        const lbl = idle.querySelector('.op-pill-label');
        if (lbl) lbl.textContent = 'FALLBACK';
      } else {
        idle.title = 'No active ops — click to view recent history';
        const lbl = idle.querySelector('.op-pill-label');
        if (lbl) lbl.textContent = 'IDLE';
      }
    }
    if (!running.length) {
      // v1.12.118: idle pill replaces the legacy green dot + "IDLE"
      // text. Same visual family as the FAIL/UPD/active op-pills, no
      // dot-to-bar flip when an op finishes.
      mini.hidden = true;
      if (overflow) overflow.hidden = true;
      if (idle) idle.hidden = false;
      return;
    }
    if (idle) idle.hidden = true;
    // v1.12.109: when multiple ops run concurrently (e.g., TDB sync
    // + downloads + places), the topbar carries one mini-bar for
    // the most-recently-updated op plus a "+N ops" pill that opens
    // the drawer where the rest live. Keeps the topbar uncluttered
    // without losing surface area for the others.
    const op = running.slice().sort((a, b) =>
      String(b.updated_at).localeCompare(String(a.updated_at)))[0];
    const tone = TONE_BY_KIND[op.kind] || 'tdb';
    const pct = pctOf(op);
    const indeterminate = (op.stage_total || 0) <= 0;
    mini.hidden = false;
    mini.className = `op-mini op-tone-${tone}` + (indeterminate ? ' op-mini-indet' : '');
    mini.innerHTML = `
      <span class="op-mini-label">${esc(op.stage_label || KIND_LABEL[op.kind] || '…')}</span>
      <span class="op-mini-bar"><span class="op-mini-bar-fill"
            style="width:${indeterminate ? 100 : (pct != null ? pct.toFixed(1) : 30)}%"></span></span>
      <span class="op-mini-pct">${indeterminate ? '' : (pct != null ? pct.toFixed(0) + '%' : '')}</span>
    `;
    if (overflow) {
      const extra = running.length - 1;
      if (extra > 0) {
        overflow.hidden = false;
        overflow.innerHTML = `<span class="op-pill-count">+${extra}</span><span class="op-pill-label">OPS</span>`;
      } else {
        overflow.hidden = true;
      }
    }
  }

  // ── interpolation tween ───────────────────────────────────────
  // Between polls, advance the "current" counter toward the latest
  // sample using the smoothed rate. Stops when the visible value
  // reaches the target. ~16ms tick (rAF-paced).
  function tickCounters() {
    const now = performance.now();
    document.querySelectorAll('[data-op-counter]').forEach((el) => {
      const target = +el.getAttribute('data-op-counter-target') || 0;
      const current = +el.getAttribute('data-op-counter-current') || target;
      if (current >= target) {
        if (current !== target) {
          el.setAttribute('data-op-counter-current', target);
          el.textContent = fmtNum(target);
        }
        return;
      }
      // Advance ~1/30 of the gap per frame, smooth easing.
      const next = Math.min(target, current + Math.max(1, (target - current) / 30));
      el.setAttribute('data-op-counter-current', next);
      el.textContent = fmtNum(Math.round(next));
    });
    requestAnimationFrame(tickCounters);
  }

  // ── poll loop ─────────────────────────────────────────────────
  let lastRunning = false;
  async function poll() {
    const data = await fetchProgress();
    if (data && Array.isArray(data.ops)) {
      state.ops = data.ops;
      const running = state.ops.some((o) =>
        o.status === 'running' || o.status === 'cancelling');
      // v1.12.108: body attribute drives the CSS suppression of
      // the legacy refresh UI (yellow dot + REFRESHING text +
      // per-tab nav-busy). Switch immediately on transitions so
      // there's no overlap window between the mini-bar appearing
      // and the legacy text disappearing.
      document.body.setAttribute(
        'data-ops-running', running ? '1' : '0');
      // When ops transitions running → idle, fire a state-change
      // event so app.js can re-pull /api/stats and clear any
      // stale "REFRESHING…" text the legacy poller was holding.
      if (running !== lastRunning) {
        lastRunning = running;
        try {
          window.dispatchEvent(new CustomEvent('motif:ops-state-changed', {
            detail: { running, ops: state.ops },
          }));
        } catch (_) { /* old browsers */ }
      }
      const newInterval = running ? 1000 : 10000;
      if (newInterval !== state.pollInterval) {
        state.pollInterval = newInterval;
        if (state.pollTimer) clearTimeout(state.pollTimer);
      }
      renderTopbar(state.ops);
      if (state.drawerOpen) renderDrawerBody(state.ops);
    }
    state.pollTimer = setTimeout(poll, state.pollInterval);
  }

  // ── drawer open/close ─────────────────────────────────────────
  function openDrawer() {
    const drawer = document.getElementById('ops-drawer');
    if (!drawer) return;
    drawer.hidden = false;
    // Force layout so the slide-in transition fires.
    void drawer.offsetWidth;
    drawer.classList.add('is-open');
    state.drawerOpen = true;
    renderDrawerBody(state.ops);
    // Tighten poll while the user's looking.
    if (state.pollInterval !== 1000) {
      state.pollInterval = 1000;
      if (state.pollTimer) clearTimeout(state.pollTimer);
      state.pollTimer = setTimeout(poll, 50);
    }
  }

  function closeDrawer() {
    const drawer = document.getElementById('ops-drawer');
    if (!drawer) return;
    drawer.classList.remove('is-open');
    state.drawerOpen = false;
    setTimeout(() => { drawer.hidden = true; }, 280);
  }

  // ── DOM wiring ────────────────────────────────────────────────
  function init() {
    // Click on the mini-bar or any op-pill with [data-ops-trigger]
    // opens the drawer. Click on the scrim or × closes it.
    document.addEventListener('click', (e) => {
      const trigger = e.target.closest('[data-ops-trigger]');
      if (trigger) {
        e.preventDefault();
        openDrawer();
        return;
      }
      const close = e.target.closest('.ops-drawer-close, .ops-drawer-scrim');
      if (close) {
        closeDrawer();
        return;
      }
      const cancel = e.target.closest('[data-op-cancel]');
      if (cancel) {
        e.preventDefault();
        const opId = cancel.getAttribute('data-op-cancel');
        cancel.disabled = true;
        cancel.textContent = '// CANCELLING…';
        postCancel(opId).then((ok) => {
          if (!ok) {
            cancel.disabled = false;
            cancel.textContent = '// CANCEL';
          } else {
            // Force a poll right away so the UI reflects the
            // status='cancelling' flip without waiting.
            if (state.pollTimer) clearTimeout(state.pollTimer);
            state.pollTimer = setTimeout(poll, 200);
          }
        });
      }
    });
    // ESC closes the drawer.
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && state.drawerOpen) closeDrawer();
    });
    // Kick off polling + counter tween.
    poll();
    requestAnimationFrame(tickCounters);
  }

  // Public API.
  window.motifOps = {
    init,
    open: openDrawer,
    close: closeDrawer,
    refresh: poll,
    state: () => state,
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
