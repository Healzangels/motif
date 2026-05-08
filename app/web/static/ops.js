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
    // when sync.source = "database".
    // v1.13.0 (Phase B): git_fetch + git_diff + git_apply run when
    // sync.source = "git" and supplant the snapshot + index/fetch
    // stages entirely on the differential path. resolve + prune
    // always run.
    // v1.13.12: short labels (≤5 chars) so 11 stages fit a typical
    // drawer width without overlap. Hover tooltip on each step still
    // carries the long form via title=.
    tdb_sync: [
      { key: 'git_fetch',         label: 'GIT',   long: 'Git fetch' },
      { key: 'git_diff',          label: 'DIFF',  long: 'Diff' },
      { key: 'git_apply',         label: 'APPLY', long: 'Apply' },
      { key: 'snapshot_download', label: 'SNAP',  long: 'Snapshot download' },
      { key: 'snapshot_extract',  label: 'EXTR',  long: 'Snapshot extract' },
      { key: 'index_movie',  label: 'M·IX', long: 'Movies index' },
      { key: 'fetch_movie',  label: 'MOV',  long: 'Movies fetch' },
      { key: 'index_tv',     label: 'T·IX', long: 'TV index' },
      { key: 'fetch_tv',     label: 'TV',   long: 'TV fetch' },
      { key: 'resolve',      label: 'RES',  long: 'Resolve theme ids' },
      { key: 'prune',        label: 'PRUN', long: 'Prune stale state' },
    ],
    // v1.13.43: long: descriptions added so the timeline-step and
    // label hover tooltip carries an explanation, not a duplicate
    // of the visible text. Pre-fix the title= attribute fell back
    // to s.label for stages without a `long` field — `Enumerate`
    // hovered to `Enumerate`, looking like the tooltip wasn't
    // working at all.
    plex_enum: [
      { key: 'enumerate', label: 'Enumerate',
        long: 'Walk every managed Plex section and upsert one plex_items row per item (ratingKey, has_theme, local_theme_file, folder_path).' },
      { key: 'reconcile', label: 'Reconcile',
        long: 'Re-link motif rows to plex_items, HEAD-verify ambiguous theme claims, refresh theme_id, sweep stale state.' },
    ],
    // v1.13.43: REPROBE PLEX THEMES. Single read-only stage that
    // walks every sidecar-bearing row and prefix-byte compares
    // Plex's served theme bytes against the local sidecar.
    reprobe_plex_themes: [
      { key: 'probe', label: 'Probe',
        long: 'Read 2 KB from each local theme.mp3 and compare against a Range-GET of Plex\'s /library/metadata/{rk}/theme — match=sidecar, differ=Plex serves an independent theme.' },
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
    tdb_sync:            'tdb',
    plex_enum:           'plex',
    reprobe_plex_themes: 'plex',
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
    tdb_sync:            'THEMERRDB SYNC',
    plex_enum:           'PLEX SCAN',
    reprobe_plex_themes: 'REPROBE PLEX THEMES',
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
    // v1.13.18 (6C): detail.bar_pct (set by the synthesized
    // download_queue card with real yt-dlp progress) takes precedence
    // over the integer stage_current/stage_total ratio. Range is 0..1.
    const detail = op.detail || {};
    if (typeof detail.bar_pct === 'number') {
      return Math.min(100, Math.max(0, detail.bar_pct * 100));
    }
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
      return `<div class="op-card-timeline-step ${cls}" title="${esc(s.long || s.label)}"></div>`;
    }).join('');
    // v1.13.12: each label gets a fixed flex slot matching the bar
    // width above so labels stay column-aligned and never overflow
    // into each other. title= carries the long form for hover.
    const labels = stages.map((s) => {
      const long = esc(s.long || s.label);
      return `<span title="${long}">${esc(s.label)}</span>`;
    }).join('');
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

  // v1.13.12: when an op finishes, the headline used to freeze on
  // whatever stage_label was last in flight ("Pruning stale state",
  // "Reconciling placement paths", etc.), which made the drawer
  // read like work was still happening even with the corner
  // status flipped to DONE. Synthesize a completion headline from
  // the op's terminal state instead.
  //
  // v1.13.17: tdb_sync rows now carry detail.done_summary with the
  // full {movies_seen, tv_seen, new, updated, errors} breakdown so
  // the headline can match the docker log line ("Done — 5177
  // items · 0 new · 0 updated") instead of the generic
  // "Done — N items processed".
  function _doneHeadline(op) {
    if (op.status === 'cancelled') return 'Cancelled';
    if (op.status === 'failed') return 'Failed';
    const detail = op.detail || {};
    if (detail.no_changes) return 'Done — no upstream changes';
    const ds = detail.done_summary;
    if (ds && typeof ds === 'object') {
      const total = (ds.movies_seen || 0) + (ds.tv_seen || 0);
      if (total > 0) {
        const errs = ds.errors > 0 ? ` · ${fmtNum(ds.errors)} err` : '';
        return `Done — ${fmtNum(total)} item${total === 1 ? '' : 's'} `
             + `· ${fmtNum(ds.new || 0)} new · ${fmtNum(ds.updated || 0)} upd${errs}`;
      }
    }
    if (op.processed_total > 0) {
      return `Done — ${fmtNum(op.processed_total)} item${op.processed_total === 1 ? '' : 's'} processed`;
    }
    return 'Done';
  }

  function renderCard(op) {
    const tone = TONE_BY_KIND[op.kind] || 'tdb';
    const isLive = (op.status === 'running' || op.status === 'cancelling'
                    || op.status === 'pending');
    const pct = pctOf(op);
    const rate = smoothedRate(op.detail && op.detail.throughput);
    const etaSec = eta(op);
    const elapsed = op.started_at
      ? (new Date(op.finished_at || Date.now())
          - new Date(op.started_at)) / 1000
      : null;
    const headline = isLive
      ? (op.stage_label || op.stage || '…')
      : _doneHeadline(op);

    // v1.13.17: finished cards use a compact variant — drop the
    // live-only sections to keep the LAST OPS pile readable.
    //
    // v1.13.19: bring back the timeline strip and the activity feed
    // even on finished cards — those carry the breadcrumb of WHAT
    // happened that the user wants in the archive. Sparkline stays
    // live-only (a frozen rate chart isn't useful post-completion)
    // and the cancel button stays live-only too.
    const showLiveSections = isLive;
    const showHistorySections = true;  // timeline + activity, always

    return `
      <div class="op-card op-tone-${tone} op-status-${op.status}${showLiveSections ? '' : ' op-card-compact'}"
           data-op-id="${esc(op.op_id)}">
        <div class="op-card-head">
          <span class="op-card-kind">// ${esc(KIND_LABEL[op.kind] || op.kind)}</span>
          <span class="op-card-status">${esc(op.status.toUpperCase())}</span>
        </div>
        <div class="op-card-stage">${esc(headline)}</div>
        ${(() => {
          // v1.13.18: split the counter and bar decisions.
          //  - Counter: show whenever stage_total > 0 (any value,
          //    even 1) so the operator sees "0 / 1" → "1 / 1" for
          //    single-job operations like place/refresh/nudge.
          //  - Bar style:
          //      detail.bar_pct present (yt-dlp real %) → real bar
          //      stage_total > 1                         → real bar
          //      isLive                                  → indeterminate
          //      else                                    → no bar
          if (!showLiveSections) return '';
          const hasRealPct = op.detail && typeof op.detail.bar_pct === 'number';
          const useRealBar = hasRealPct || op.stage_total > 1;
          const showCounter = op.stage_total > 0;
          const counterHtml = showCounter ? `
            <div class="op-card-counter">
              <span class="op-card-counter-current"
                    data-op-counter
                    data-op-counter-target="${op.stage_current || 0}">
                ${fmtNum(op.stage_current)}
              </span>
              <span class="op-card-counter-total">/ ${fmtNum(op.stage_total)}</span>
            </div>` : '';
          const barHtml = useRealBar
            ? `<div class="op-card-bar">
                 <div class="op-card-bar-fill"
                      style="width:${pct != null ? pct.toFixed(1) : 0}%"></div>
               </div>`
            : `<div class="op-card-bar op-card-bar-indet">
                 <div class="op-card-bar-fill"></div>
               </div>`;
          return counterHtml + barHtml;
        })()}
        <div class="op-card-meta">
          ${(showLiveSections && rate > 0) ? `
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
        ${showHistorySections ? renderTimeline(op) : ''}
        ${showLiveSections ? renderSparkline(op) : ''}
        ${showHistorySections ? renderActivity(op) : ''}
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

  // v1.13.12: cache the last rendered HTML of the drawer body. Skip
  // the swap when nothing changed (idle polls + the 750ms /api/progress
  // server-side cache mean most poll cycles return identical data).
  // Pre-fix every poll tore down + rebuilt every op-card, which read
  // as a hard flicker during active sync runs.
  let _lastDrawerHtml = '';

  function renderDrawerBody(ops) {
    // v1.13.5: 'pending' counts as active. Queue-synthesized rows
    // (REFRESH QUEUE, DOWNLOAD QUEUE, etc.) sit in 'pending' status
    // when the worker hasn't picked up the next job yet — they're
    // still ongoing work the user wants visible at the top of the
    // drawer, not buried under finished ops in // LAST OPS.
    // Sort within active: running first, then pending; both ordered
    // by updated_at DESC so the freshest activity floats to the top.
    const active = ops.filter((o) =>
      o.status === 'running' || o.status === 'cancelling' || o.status === 'pending');
    active.sort((a, b) => {
      // running > cancelling > pending — by raw status weight first.
      const w = (s) => s === 'running' ? 0 : s === 'cancelling' ? 1 : 2;
      const dw = w(a.status) - w(b.status);
      if (dw !== 0) return dw;
      return String(b.updated_at || '').localeCompare(String(a.updated_at || ''));
    });
    const finished = ops
      .filter((o) => o.status !== 'running' && o.status !== 'cancelling'
                     && o.status !== 'pending')
      .slice(0, 3);
    const body = document.getElementById('ops-drawer-body');
    if (!body) return;
    let html;
    if (!active.length && !finished.length) {
      html = '<div class="ops-drawer-empty">// idle · no ops in the last 24 hours</div>';
    } else {
      html = '';
      if (active.length) {
        // Header so the active section reads as deliberately first
        // even when only a single pending row is present (which on
        // its own could look like a stray finished card).
        html += `<div class="op-card-kind" style="margin:0 0 6px">// ACTIVE</div>`;
        html += active.map(renderCard).join('');
      } else {
        html += '<div class="ops-drawer-empty" style="padding:14px 0">// idle · no ops running</div>';
      }
      if (finished.length) {
        html += `<div class="op-card-kind" style="margin:18px 0 6px">// LAST OPS</div>`;
        html += finished.map(renderCard).join('');
      }
    }
    if (html === _lastDrawerHtml) return;
    _lastDrawerHtml = html;
    body.innerHTML = html;
  }

  // v1.13.19: optimistic placeholder for the topbar mini-bar. When
  // the user clicks SYNC / SCAN PLEX, we want the IDLE pill to flip
  // to a SYNCING/SCANNING state immediately — but the worker has up
  // to a 2s idle wait before it picks up the job, so /api/progress
  // doesn't see a 'running' row for 1-2s after the click. Pre-fix
  // the IDLE pill sat there during that gap, making the click feel
  // unresponsive. Now setOptimisticPlaceholder paints a tone-tinted
  // pill that holds for up to 5s OR until the real running op
  // arrives (whichever comes first).
  let _optimisticOp = null;
  function setOptimisticPlaceholder(kind, label) {
    _optimisticOp = {
      kind,
      label,
      expiresAt: Date.now() + 5000,
    };
    renderTopbar(state.ops || []);
    boostPoll();
  }

  function renderTopbar(ops) {
    const running = ops.filter((o) =>
      o.status === 'running' || o.status === 'pending' || o.status === 'cancelling');
    const mini = document.getElementById('op-mini');
    const overflow = document.getElementById('op-mini-overflow');
    const idle = document.getElementById('op-status-idle');
    if (!mini) return;
    // v1.13.19: clear the optimistic placeholder once a real running
    // op arrives — the placeholder has done its job. Also clear if
    // it has expired (worker never picked up the job? rare).
    // v1.13.29: only clear when a SAME-KIND op is running. Pre-fix a
    // pre-existing tdb_sync would clear a fresh plex_enum placeholder
    // on the next 1s poll, reverting the topbar to the unrelated
    // tdb_sync mini-bar — the user's plex_enum click felt like it
    // didn't take. Match by kind (plex_enum click waits for a
    // plex_enum running row; tdb_sync click waits for a tdb_sync
    // row) so the placeholder hands off to the correct successor.
    if (_optimisticOp) {
      const sameKindRunning = ops.some((o) =>
        (o.status === 'running' || o.status === 'pending' || o.status === 'cancelling')
        && o.kind === _optimisticOp.kind,
      );
      if (sameKindRunning || _optimisticOp.expiresAt < Date.now()) {
        _optimisticOp = null;
      }
    }
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
    if (!running.length && !_optimisticOp) {
      // v1.12.118: idle pill replaces the legacy green dot + "IDLE"
      // text. Same visual family as the FAIL/UPD/active op-pills, no
      // dot-to-bar flip when an op finishes.
      mini.hidden = true;
      if (overflow) overflow.hidden = true;
      if (idle) idle.hidden = false;
      return;
    }
    // v1.13.19: paint the optimistic placeholder as a fake op until
    // the real running row lands. Indeterminate shimmer carries the
    // "we're working" cue without claiming progress we haven't made.
    if (!running.length && _optimisticOp) {
      if (idle) idle.hidden = true;
      mini.hidden = false;
      if (overflow) overflow.hidden = true;
      const tone = TONE_BY_KIND[_optimisticOp.kind] || 'tdb';
      mini.className = `op-mini op-tone-${tone} op-mini-indet`;
      mini.innerHTML = `
        <span class="op-mini-label">${esc(_optimisticOp.label)}</span>
        <span class="op-mini-bar"><span class="op-mini-bar-fill" style="width:100%"></span></span>
        <span class="op-mini-pct"></span>
      `;
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
    // v1.13.24: treat single-job bursts (nudge plex / single place /
    // single refresh) as indeterminate at the topbar mini-bar too.
    // Pre-fix: stage_total=1 → bar showed "0%" until the job
    // completed, then jumped to "100%" — never reflecting actual
    // work, just a binary not-done/done. The card-level bar already
    // does this (useRealBar = hasRealPct || stage_total > 1, see
    // ~line 369); the mini-bar's condition was stricter (≤ 0) so it
    // still drew a literal 0% bar. Indeterminate covers stage_total
    // = 1 unless yt-dlp has fed real bar_pct in, in which case the
    // bar fills smoothly within the single job.
    const hasRealPct = op.detail && typeof op.detail.bar_pct === 'number';
    const indeterminate = !hasRealPct && (op.stage_total || 0) <= 1;
    mini.hidden = false;
    mini.className = `op-mini op-tone-${tone}` + (indeterminate ? ' op-mini-indet' : '');
    // v1.13.27: append a queue-position suffix when this op is one
    // of multiple jobs of the same kind in flight. plex_enum is the
    // common case — user fires // SYNC PLEX on movies + tv + anime
    // in quick succession, the worker serializes them, and the user
    // wants to see "I'm on #2 of 4 right now". Position computed
    // from a window-scoped HW (high water) updated by app.js's
    // refreshTopbarStatus tick. Suffix only renders when hw > 1.
    let labelText = op.stage_label || KIND_LABEL[op.kind] || '…';
    try {
      const q = (window.__motif_queue || {})[op.kind];
      if (q && q.hw > 1 && q.current > 0) {
        const position = Math.min(q.hw, Math.max(1, q.hw - q.current + 1));
        labelText = `${labelText} (${position} of ${q.hw})`;
      }
    } catch (_) { /* fall through with bare label */ }
    mini.innerHTML = `
      <span class="op-mini-label">${esc(labelText)}</span>
      <span class="op-mini-bar"><span class="op-mini-bar-fill"
            style="width:${indeterminate ? 100 : (pct != null ? pct.toFixed(1) : 30)}%"></span></span>
      <span class="op-mini-pct">${indeterminate ? '' : (pct != null ? pct.toFixed(0) + '%' : '')}</span>
    `;
    if (overflow) {
      // v1.13.45: hide the overflow pill entirely. Pre-fix the
      // "+N OPS" pill counted every concurrent op (download_queue +
      // place_queue + refresh_queue all in flight = "+2 OPS") but
      // the drawer click revealed only the same active cards the
      // user could already see by opening the drawer normally —
      // the pill duplicated the drawer's content without adding
      // navigation. The real signal the user wants ("how much is
      // happening?") is conveyed by the main mini-bar's stage_label
      // ("Downloading themes — 7/8") and the drawer carries the
      // detail for users who want it. Keeping the element so we
      // can repurpose later (e.g., as a flash-counter for newly
      // -started ops) — just hidden.
      overflow.hidden = true;
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
      // v1.13.47: cadence-bump must include 'pending' too. Pre-fix
      // a click → optimistic placeholder → boostPoll() set the
      // interval to 1s, but the very next poll observed only a
      // 'pending' synth row (worker hadn't picked the job up yet),
      // saw running=false, and downshifted to 10s. The next poll
      // didn't fire for 10 seconds — by which time the worker had
      // run the download AND queued the refresh, so the mini-bar
      // jumped straight from "Theme download queued" to
      // "Plex refresh queued" with no visible "Downloading: <title>"
      // step or % and the row's amber DL pill kept flashing alone.
      // Keep the body attribute tied to actual running state (its
      // CSS hooks the legacy refresh UI suppression on real work);
      // only the cadence decision widens.
      const pending = state.ops.some((o) => o.status === 'pending');
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
      const newInterval = (running || pending) ? 1000 : 10000;
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

  // v1.13.18: boost the poll cadence to 1s + fire an immediate
  // poll. Used by the SYNC click handler so a fast sync (no-op git
  // <3s) doesn't fly under the 10s idle-poll radar — pre-fix the
  // user could click SYNC and never see the topbar status pill
  // appear because no poll fired during the running window.
  function boostPoll() {
    state.pollInterval = 1000;
    if (state.pollTimer) clearTimeout(state.pollTimer);
    state.pollTimer = setTimeout(poll, 50);
  }

  // Public API.
  window.motifOps = {
    init,
    open: openDrawer,
    close: closeDrawer,
    refresh: poll,
    boostPoll,
    setOptimisticPlaceholder,
    state: () => state,
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
