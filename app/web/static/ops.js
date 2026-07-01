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
      { key: 'index', label: 'INDEX',
        long: 'Index + upsert ThemerrDB movie / TV / collection rows' },
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
      // v0.50.43: the stat-every-theme.mp3 passes (placement + canonical health)
      // are their own waterfall bar — on a slow mount they can dominate the tail
      // and were previously hidden inside the 'reconcile' bar.
      { key: 'health', label: 'Health',
        long: 'Stat each placement + canonical theme.mp3 on disk to flag broken (missing-file) rows for the NEEDS WORK / PL / DL sort buckets.' },
    ],
    // v1.13.43: REPROBE PLEX THEMES. Single read-only stage that
    // walks every sidecar-bearing row and prefix-byte compares
    // Plex's served theme bytes against the local sidecar.
    reprobe_plex_themes: [
      { key: 'probe', label: 'Probe',
        long: 'Read 2 KB from each local theme.mp3 and compare against a Range-GET of Plex\'s /library/metadata/{rk}/theme — match=sidecar, differ=Plex serves an independent theme.' },
    ],
    // v1.21.25: the remaining real op_progress kinds were missing from
    // STAGE_TIMELINE, so their step strip rendered empty while siblings
    // (tdb_sync / plex_enum / reprobe) showed one. Added so every real op
    // shows its phases. Stage keys mirror each worker's
    // update_progress(stage=...) — bulk_lps + cloud_themes_backup have
    // genuine two-phase flows; bulk_probe_tdb + tvdb_bridge are single-stage
    // (like reprobe). Full-word labels follow the plex_enum/reprobe
    // Title-case convention (CSS uppercases for display).
    bulk_probe_tdb: [
      { key: 'probe', label: 'Probe',
        long: 'Probe each selected ThemerrDB URL to classify alive vs dead.' },
    ],
    bulk_lps: [
      { key: 'probe',   label: 'Probe',
        long: 'Probe each selected row\'s served theme to classify alive vs dead.' },
      { key: 'unplace', label: 'Unplace',
        long: 'Clear motif\'s theme selection so Plex serves its own.' },
    ],
    tvdb_bridge: [
      { key: 'bridge', label: 'Bridge',
        long: 'Link stranded TVDB-only rows to motif\'s TMDB-keyed themes table.' },
    ],
    cloud_themes_backup: [
      { key: 'walk',     label: 'Walk',
        long: 'Walk candidate P-rows and classify each via Plex\'s /themes endpoint.' },
      { key: 'download', label: 'Download',
        long: 'Download Plex\'s cloud theme bytes and stage them as a backup.' },
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
    // v1.14.90: synth card for sync queued behind a running
    // long-worker job (plex_enum / scan / another sync). Same
    // tdb tone as the running THEMERRDB SYNC card so the user
    // reads it as related (mirror plex_enum_pending shape).
    tdb_sync_pending:    'tdb',
    plex_enum:           'plex',
    // v1.13.87: synth card for plex_enum jobs queued behind a
    // running one. Same plex tone so the user reads it as
    // related to the running PLEX REFRESH card above it.
    plex_enum_pending:   'plex',
    reprobe_plex_themes: 'plex',
    // v1.19.88: queue ops use the dedicated 'queue' tone (cyan) so
    // they stay distinct from Plex's amber. Pre-fix they shared
    // 'warn' (amber) with the disk-low badge + the realigned Plex
    // ops would have collided. TDB→green / Plex→amber / queue→cyan.
    download_queue: 'queue',
    place_queue:    'queue',
    scan_queue:     'queue',
    // v1.12.118: post-place Plex refresh / relink-stale-paths /
    // adopt-sidecar queues join the same ops surface.
    refresh_queue:  'queue',
    relink_queue:   'queue',
    adopt_queue:    'queue',
    // v1.16.0 / labeled v1.16.2: tdb tone (green) since the
    // bridge's outcome is linking rows to motif's TDB-keyed
    // themes table — same visual identity as THEMERRDB SYNC.
    tvdb_bridge:    'tdb',
    // v1.18.51: bulk_probe_tdb and bulk_lps were emitted by
    // api.py but missing from TONE_BY_KIND — the drawer card
    // rendered with no tone class so the toolbar/header lost
    // the per-op color identity. bulk_probe_tdb hits TDB URLs
    // (tdb tone). bulk_lps walks plex_items / placements (plex
    // tone). the user audit: "make sure they still make sense,
    // apply when needed and show the correct response during
    // all different event types."
    bulk_probe_tdb: 'tdb',
    bulk_lps:       'plex',
    // v1.19.45: cloud_themes_backup walks Plex's /themes endpoint +
    // downloads bytes from Plex. Plex-family operation (motif
    // talking to Plex's API on the user's behalf) so 'plex' tone
    // matches bulk_lps + PUSH/RESTORE FROM PLEX UX.
    cloud_themes_backup: 'plex',
  };

  // v1.15.82: priority order for the contended single-slot mini-bar
  // when multiple ops run concurrently. Lower index = higher
  // priority. the user: "Let's have download take prio followed by
  // refresh, sync themerrdb, prob url, probe plex sidecar." Anything
  // not in this map gets _PRIORITY_FALLBACK (sorted by updated_at
  // DESC as the tiebreaker). The synth-pending companions
  // (plex_enum_pending / tdb_sync_pending) share the priority of
  // their real-running parent so a queued sync doesn't preempt a
  // real download. Lives in TopbarPicker.pickMain too — keep the
  // SSR side (_topbar_ssr_state in api.py) in lockstep.
  const OP_MINI_PRIORITY = {
    download_queue:      1,  // downloads (most user-visible work)
    plex_enum:           2,  // library refresh
    plex_enum_pending:   2,  // queued refresh shares its parent's slot
    tdb_sync:            3,  // themerrdb sync
    tdb_sync_pending:    3,
    bulk_probe_tdb:      4,  // probe TDB URLs
    // v1.18.51: bulk_lps shares the bulk-probe slot — both are
    // user-initiated bulk operations that surface in the mini-
    // bar; sitting just above plex sidecar probe keeps the
    // user's "I clicked the bulk action" feedback visible.
    bulk_lps:            4,
    // v1.19.45: cloud_themes_backup shares the bulk-op tier
    // (priority 4) so a user-initiated cloud backup wins the
    // mini-bar slot over background reprobes. Same tier as
    // bulk_probe_tdb / bulk_lps which it most resembles UX-wise.
    cloud_themes_backup: 4,
    reprobe_plex_themes: 5,  // probe Plex sidecars
    // v1.18.53: tvdb_bridge is a real op_progress kind (TVDB
    // bridge rebuild from /settings) that mutates library row
    // state but was inheriting FALLBACK=99. Sits at the
    // background-op tier next to reprobe_plex_themes so a
    // user-initiated bulk op (priority 4) wins the slot but
    // tvdb_bridge still displays when running alone or against
    // queue synths. Aligns with TONE_BY_KIND='tdb' — same
    // visual family as reprobe.
    tvdb_bridge:         5,
    // v1.18.53: downstream queue synths get explicit priority 6
    // (one rung below background ops) so the priority ladder is
    // fully documented. These are emitted by progress.py:
    // _synthesize_queue_ops for each job_type in
    // ('download','place','scan','refresh','relink','adopt') —
    // we already gave download_queue priority 1 above, the
    // remaining 5 land here. Behaviorally identical to FALLBACK
    // when they're the only op running (still wins the empty
    // slot), but explicit so a `null` lookup never happens.
    place_queue:         6,
    scan_queue:          6,
    refresh_queue:       6,
    relink_queue:        6,
    adopt_queue:         6,
  };
  const OP_MINI_PRIORITY_FALLBACK = 99;

  const KIND_LABEL = {
    tdb_sync:            'THEMERRDB SYNC',
    // v1.14.90: distinct label so the user can tell at a glance
    // which is running and which is queued. Mirror the v1.13.87
    // plex_enum_pending naming.
    tdb_sync_pending:    'THEMERRDB SYNC (QUEUED)',
    // v1.13.70: drawer label sync→refresh (was 'PLEX SCAN'). The
    // user-visible action is now `// REFRESH PLEX` / `// REFRESH
    // <NAME>` everywhere, so the drawer reads the same verb.
    plex_enum:           'PLEX REFRESH',
    // v1.13.87: synth card for queued plex_enum. Distinct label
    // ("QUEUED" suffix) so the user sees both cards in the drawer
    // and can tell at a glance which is running and which is next.
    plex_enum_pending:   'PLEX REFRESH (QUEUED)',
    reprobe_plex_themes: 'REPROBE PLEX THEMES',
    download_queue: 'DOWNLOAD QUEUE',
    place_queue:    'PLACE QUEUE',
    scan_queue:     'DISK SCAN',
    // v1.13.80: rename REFRESH QUEUE → RE-SCAN QUEUE so the drawer
    // card title aligns with the stage label "Plex re-scan queued"
    // and the user-action "// REFRESH PLEX" stays distinct (the
    // section-enum action is plex_enum, NOT refresh_queue). Internal
    // kind id stays refresh_queue for state-tracking continuity.
    refresh_queue:  'RE-SCAN QUEUE',
    relink_queue:   'RELINK QUEUE',
    adopt_queue:    'ADOPT QUEUE',
    // v1.16.0 / labeled v1.16.2 / renamed v1.24.93: TVDB bridge
    // background job (the /settings TVDB BRIDGE rebuild).
    // op_progress.kind is "tvdb_bridge" (renamed in v1.24.93 from
    // an old anime-agent misnomer, schema-checked); the drawer
    // label says TVDB BRIDGE — the feature links TVDB-only rows,
    // it isn't anime-specific. Without this entry the LIVE OPS
    // card renders the raw kind as "// tvdb_bridge".
    tvdb_bridge:    'TVDB BRIDGE',
    // v1.18.51: kinds that op_progress accepts via the schema
    // CHECK (db.py:354-363) but were never given a friendly
    // label. Without these the drawer rendered the raw snake-
    // case kind in the card header ("// bulk_probe_tdb").
    // bulk_probe_tdb is the bulk-probe TDB URL sweep (api.py
    // :2930). bulk_lps is the bulk LET-PLEX-SERVE operation
    // (api.py:3492 — v1.15.28's bulk version of LPS).
    bulk_probe_tdb: 'BULK PROBE TDB',
    bulk_lps:       'BULK LET PLEX SERVE',
    // v1.19.45: cloud-themes-backup walker (v1.19.42 feature).
    // v1.19.48: label renamed to mirror the bulk-bar button +
    // SOURCE-menu entry (DOWNLOAD PLEX BACKUP). The drawer card
    // surfaces this so user reads "// DOWNLOAD PLEX BACKUP" in
    // the ops drawer. Without this entry the drawer rendered
    // "// cloud_themes_backup" raw.
    cloud_themes_backup: 'DOWNLOAD PLEX BACKUP',
  };
  // v1.14.38: dropped STAGE_TIMELINE_QUEUE const + empty
  // forEach loop. Stale scaffolding from when queue-op
  // timelines were going to live in a separate map; the const
  // was never referenced and the forEach body was a placeholder
  // comment. Audit L1 cleanup.

  // ── state ─────────────────────────────────────────────────────
  let state = {
    ops: [],
    pollTimer: null,
    pollInterval: 10000,           // 10s idle
    drawerOpen: false,
    // per op_id: smoothed counter state for interpolation
    counters: {},
    // v1.20.22 / v1.21.28: op_ids whose RUN INSIGHT panel is expanded.
    // A Set — multiple cards can be unfurled at once (the user: "make it so
    // both can be unfurled at the same time"). expandedEvents caches the
    // fetched run log per op_id; expandedFetching guards overlapping fetches.
    expandedOpIds: new Set(),
    expandedEvents: {},
    expandedFetching: {},
  };

  // ── network ───────────────────────────────────────────────────
  // v1.15.36: track consecutive fetchProgress failures so a
  // persistent network outage logs once at console.warn instead
  // of silently freezing the drawer. Pre-fix `catch (_): return
  // null` gave the operator no signal — drawer just stopped
  // updating, looked like a stuck poll. The threshold (5
  // consecutive failures ≈ 5-10s of polling) keeps transient
  // blips quiet. Reset on first successful response.
  let _fetchProgressFailStreak = 0;
  const _FETCH_PROGRESS_FAIL_THRESHOLD = 5;
  async function fetchProgress() {
    try {
      const r = await fetch('/api/progress', { credentials: 'same-origin' });
      if (!r.ok) {
        _fetchProgressFailStreak++;
        if (_fetchProgressFailStreak === _FETCH_PROGRESS_FAIL_THRESHOLD) {
          try {
            console.warn(
              'fetchProgress: %d consecutive non-OK responses '
              + '(latest status %d) — drawer may be stale',
              _fetchProgressFailStreak, r.status);
          } catch (_) { /* ignore log failures */ }
        }
        return null;
      }
      _fetchProgressFailStreak = 0;
      return r.json();
    } catch (e) {
      _fetchProgressFailStreak++;
      if (_fetchProgressFailStreak === _FETCH_PROGRESS_FAIL_THRESHOLD) {
        try {
          console.warn(
            'fetchProgress: %d consecutive network failures '
            + '(latest %s) — drawer may be stale',
            _fetchProgressFailStreak, e && e.message ? e.message : e);
        } catch (_) { /* ignore log failures */ }
      }
      return null;
    }
  }

  // v1.20.32: bulk-cancel every PENDING job of a type (download / place
  // / …). Powers the // CANCEL ALL PENDING button on the synthetic queue
  // cards so a long bulk run can be drained in one click.
  async function postBulkCancel(jobType) {
    try {
      const r = await fetch(
        `/api/jobs/cancel-pending?job_type=${encodeURIComponent(jobType)}`,
        { method: 'POST', credentials: 'same-origin' });
      return r.ok;
    } catch (e) {
      try { console.error('postBulkCancel failed:', e); }
      catch (_) { /* ignore log failures */ }
      return false;
    }
  }

  async function postCancel(opId) {
    try {
      const r = await fetch(
        `/api/progress/${encodeURIComponent(opId)}/cancel`,
        { method: 'POST', credentials: 'same-origin' });
      return r.ok;
    } catch (e) {
      // v1.15.35: log the failure. Pre-fix `catch (_): return
      // false` silently absorbed network/auth errors — the
      // caller treated false the same as "server returned
      // not-ok" so the user saw a stale CANCEL button without
      // knowing why. Console logging surfaces it in dev tools.
      try {
        console.error('postCancel failed:', e);
      } catch (_) { /* ignore log failures */ }
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

  // v0.50.40: magnitude-aware rate text for the live RATE pill — 1 decimal only
  // below 10/s (where it's meaningful), comma-grouped integer above, matching the
  // RUN INSIGHT peak/avg readout (so "10000.0/s" reads "10,000/s").
  function fmtRate(rate) {
    return rate < 10 ? rate.toFixed(1) : fmtNum(Math.round(rate));
  }

  function fmtDuration(seconds) {
    if (!isFinite(seconds) || seconds < 0) return '—';
    if (seconds < 60) return `${Math.round(seconds)}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  }

  // v0.50.42: ETA-specific formatter. A projection over an hour out is dominated
  // by rate noise (a slow early sample makes remaining/rate balloon), so a precise
  // "7h 23m" reads as false precision — bucket anything past an hour as ">1h".
  // ELAPSED / RAN stay on fmtDuration (those are measured, not projected).
  function fmtEta(seconds) {
    if (!isFinite(seconds) || seconds < 0) return '—';
    if (seconds > 3600) return '>1h';
    return fmtDuration(seconds);
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
    // v1.15.79: average the FULL throughput buffer (was last 10).
    // the user reported the TDB sync stalling visually with ETA 0s
    // — root cause: batch-flush spikes (one sample at 12,703/s
    // while the long-term average was ~43/s) dominated the
    // 10-sample tail, so ETA = remaining / spike = sub-1s and
    // rounded to 0s on screen even with thousands of items still
    // to process. The full buffer (30 samples, ~9s of history)
    // dilutes spikes enough that ETA tracks the actual completion
    // rate. JS still smooths post-fetch — server samples once per
    // processed_total advance.
    const sum = throughput.reduce((a, x) => a + (x.rate || 0), 0);
    return sum / throughput.length;
  }

  function eta(op) {
    const rate = smoothedRate(op.detail && op.detail.throughput);
    const total = op.stage_total || op.processed_est || 0;
    const cur = op.stage_current || op.processed_total || 0;
    const remaining = total - cur;
    if (rate <= 0 || remaining <= 0) return null;
    const projected = remaining / rate;
    // v1.15.79: sanity-clamp. When projected ETA rounds to 0s but
    // there's still meaningful work remaining, the rate sample is
    // unreliable (typically a fresh batch-flush spike). Return null
    // → UI renders "—" instead of the misleading "0s" the user flagged.
    // Threshold: > 50 items remaining + < 1s projected = "calculating".
    if (projected < 1 && remaining > 50) return null;
    return projected;
  }

  function pctOf(op) {
    // v1.22.37: a finished op is 100% by definition — snap the bar to full so
    // it visibly reaches 100 at completion (the renderCard finished-bar + the
    // mini-bar DONE flash both rely on this) instead of freezing at whatever
    // running-% the last 1s poll happened to catch. the user: "progress bars
    // jumping from some % to done without ever hitting 100%."
    if (op && op.status === 'done') return 100;
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

  // v1.15.2: shared bar-variant decision. Pre-fix three call sites
  // (renderCard, _structuralHash, renderMiniBar) each computed
  // their own version of `useRealBar` / `indeterminate`. The
  // logic was equivalent (`useRealBar = !indeterminate`) but
  // written differently in each site, making the consistency
  // non-obvious to readers + brittle if one site is updated
  // without the others. The rule: render a real bar when
  // we have either yt-dlp's real % (detail.bar_pct) or a
  // multi-job stage_total > 1; otherwise render the
  // indeterminate-shimmer bar. Keep both helpers for readability
  // — call sites pick whichever framing reads better.
  function _useRealBar(op) {
    const hasRealPct = !!(
      op.detail && typeof op.detail.bar_pct === 'number'
    );
    return hasRealPct || (op.stage_total || 0) > 1;
  }

  function _isIndeterminate(op) {
    return !_useRealBar(op);
  }

  // ── render ────────────────────────────────────────────────────
  function renderTimeline(op) {
    const stages = STAGE_TIMELINE[op.kind];
    if (!stages || !stages.length) return '';
    const currentIdx = stages.findIndex((s) => s.key === op.stage);
    // v1.21.25: a DONE op marks the WHOLE strip green. Pre-fix this all-done
    // branch was effectively dead code: every timelined kind finishes ON its
    // last stage (still set + present in the strip — tdb_sync='prune',
    // plex_enum='health' (v0.50.43), reprobe='probe'), so currentIdx>=0 always won
    // and the last step stayed `is-current` (amber, pulsing) on a completed
    // card. Check it FIRST so the whole strip goes green.
    // v1.21.26: gate strictly on status==='done', NOT "anything not running".
    // A 'pending' op (try_acquire inserts the row with stage=NULL before the
    // worker sets a stage) and a 'failed'-before-first-stage op both have
    // currentIdx=-1; the looser predicate painted their whole strip green —
    // a not-started / failed op looking complete. 'done'-only keeps a
    // completed strip green while leaving pending grey and failed/cancelled
    // showing where they stopped (the status badge conveys the outcome).
    const finished = (op.status === 'done');
    const cells = stages.map((s, i) => {
      let cls = '';
      if (finished) {
        cls = 'is-done';
      } else if (currentIdx >= 0) {
        if (i < currentIdx) cls = 'is-done';
        else if (i === currentIdx) cls = 'is-current';
      }
      return `<div class="op-card-timeline-step ${cls}" title="${esc(s.long || s.label)}"></div>`;
    }).join('');
    // v1.13.12: each label gets a fixed flex slot matching the bar
    // width above so labels stay column-aligned and never overflow
    // into each other. title= carries the long form for hover.
    const labels = stages.map((s) => {
      // v1.20.22: only attach the help cursor + tooltip when there's a
      // DISTINCT long-form. Pre-fix every label got cursor:help but the
      // title fell back to the label, so the "?" promised more and just
      // repeated the visible text. Full step context lives in the
      // expanded run view now.
      const hasLong = s.long && s.long !== s.label;
      return hasLong
        ? `<span class="has-help" title="${esc(s.long)}">${esc(s.label)}</span>`
        : `<span>${esc(s.label)}</span>`;
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
  // v1.13.17 / v1.21.25: every kind now carries a structured
  // detail.done_summary so the headline can match the docker log line
  // ("Done — 5177 items · 0 new · 0 updated") instead of the generic
  // "Done — N items processed". Shape (an ordered [{l,v}] list) is
  // documented at the formatter below.
  function _doneHeadline(op) {
    if (op.status === 'cancelled') return 'Cancelled';
    if (op.status === 'failed') return 'Failed';
    const detail = op.detail || {};
    if (detail.no_changes) return 'Done — no upstream changes';
    // v1.21.25: uniform structured done line for EVERY op kind. Each worker
    // stamps detail.done_summary as an ordered list of {l: label, v: value}
    // parts; format them identically here so a finished PLEX REFRESH /
    // REPROBE / BULK / TVDB BRIDGE / DOWNLOAD PLEX BACKUP reads the same
    // "Done — N x · M y · …" shape as a THEMERRDB SYNC, instead of the bare
    // "Done — N processed" they all fell back to (each computed an equally
    // rich breakdown but routed it only to the activity log).
    const ds = detail.done_summary;
    if (Array.isArray(ds)) {
      const parts = ds
        .filter((p) => p && p.l != null && p.v != null)
        .map((p) => `${fmtNum(p.v)} ${p.l}`);
      if (parts.length) return `Done — ${parts.join(' · ')}`;
    }
    if (op.processed_total > 0) {
      return `Done — ${fmtNum(op.processed_total)} item${op.processed_total === 1 ? '' : 's'} processed`;
    }
    return 'Done';
  }

  // ── v1.20.22: expanded run view ───────────────────────────────
  // v1.20.41: accordion. Clicking a card expands IT downward in place —
  // the run log (op-card-detail) renders BELOW the card's own content,
  // inside the card. The panel never resizes, nothing floats, and no
  // other card moves horizontally; the cards below just shift down. The
  // only layout that's truly "just this one card expands" (the user's ask)
  // — a right-pinned fixed-width panel has no room to grow a card
  // leftward without widening the whole panel.

  function fetchOpEvents(opId) {
    if (state.expandedFetching[opId]) return;
    state.expandedFetching[opId] = true;
    fetch(`/api/ops/${encodeURIComponent(opId)}/events`,
          { credentials: 'same-origin' })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        state.expandedFetching[opId] = false;
        if (!data) return;
        state.expandedEvents[opId] = data.events || [];
        if (state.drawerOpen && state.expandedOpIds.has(opId)) {
          // v1.20.41: the run log is in the card — re-render so the
          // expanded card picks up its newly-fetched events.
          renderDrawerBody(state.ops);
        }
      })
      .catch(() => { state.expandedFetching[opId] = false; });
  }

  function toggleExpand(opId, forceCollapse) {
    // v1.21.28: independent per-card expand — toggling one card no longer
    // collapses the others (the set tracks every open card).
    if (forceCollapse || state.expandedOpIds.has(opId)) {
      state.expandedOpIds.delete(opId);
    } else {
      state.expandedOpIds.add(opId);
      if (state.expandedEvents[opId] === undefined) fetchOpEvents(opId);
    }
    renderDrawerBody(state.ops);
  }

  // v1.21.27: a stage-timing waterfall — where the run's time went. Reads
  // detail.stage_timings (captured centrally in progress.py). For a LIVE op
  // we append the in-flight stage (from the _stage_* trackers) so its bar
  // grows in real time. Returns '' when there's nothing to show (e.g. an
  // old history card from before stage-timing capture, or a 0-stage op).
  function _renderWaterfall(op, isLive) {
    const d = op.detail || {};
    const timings = Array.isArray(d.stage_timings) ? d.stage_timings.slice() : [];
    if (isLive && d._stage_key && d._stage_started) {
      const liveSecs = Math.max((Date.now() - new Date(d._stage_started)) / 1000, 0);
      timings.push({ stage: d._stage_key, label: d._stage_label || d._stage_key,
                     seconds: liveSecs, live: true });
    }
    if (!timings.length) return '';
    const total = timings.reduce((a, t) => a + (t.seconds || 0), 0) || 1;
    const rows = timings.map((t) => {
      const w = Math.max(3, Math.round(((t.seconds || 0) / total) * 100));
      const dur = (t.seconds || 0) >= 1 ? fmtDuration(t.seconds) : `${(t.seconds || 0).toFixed(1)}s`;
      return `<div class="op-wf-row${t.live ? ' op-wf-live' : ''}">
        <span class="op-wf-label" title="${esc(t.stage)}">${esc(String(t.label || t.stage).toUpperCase())}</span>
        <span class="op-wf-track"><span class="op-wf-bar" style="width:${w}%"></span></span>
        <span class="op-wf-dur">${esc(dur)}</span>
      </div>`;
    }).join('');
    return `<div class="op-insight-section">
      <div class="op-insight-head">STAGE BREAKDOWN</div>
      <div class="op-wf">${rows}</div>
    </div>`;
  }

  // v0.50.36: honest throughput numbers (the user saw 400000 peak/s + 207301 avg/s
  // on a 10,514-item/77s run that really averaged ~136/s). avg/s = total processed /
  // wall-clock elapsed. v0.50.39: peak/s = max sample rate — now correct because the
  // per-sample rate is itself sane (progress.py floors dt at 1.0s, so a sub-second
  // burst can no longer produce a ~400000/s sample). This same fix makes the live
  // items/sec pill, the ETA, and the sparkline bars/tooltips sane too.
  function _throughputStats(op) {
    const tp = (op.detail && op.detail.throughput) || [];
    const elapsedS = op.started_at
      ? (new Date(op.finished_at || Date.now()) - new Date(op.started_at)) / 1000
      : 0;
    const processed = op.processed_total || op.stage_current || 0;
    const avg = elapsedS > 0 ? processed / elapsedS : 0;
    const peak = tp.length ? Math.max(0, ...tp.map((x) => x.rate || 0)) : 0;
    return { peak, avg };
  }

  // v1.21.27: glanceable stat readout — the done_summary as big numbers
  // plus peak/avg throughput + error count. The "mission-control" line.
  function _renderInsightStats(op) {
    const d = op.detail || {};
    const stats = [];
    // v0.50.40: skip the done_summary completion stats on a CANCELLED op — some
    // workers (cloud_themes_backup) stamp done_summary right before cancelling, so a
    // "N backed up · M skipped" readout reads as complete while the headline says
    // "Cancelled". The headline already conveys the partial state.
    const ds = d.done_summary;
    if (Array.isArray(ds) && op.status !== 'cancelled') {
      ds.filter((p) => p && p.l != null && p.v != null)
        .forEach((p) => stats.push({ label: p.l, value: fmtNum(p.v) }));
    }
    if ((d.throughput || []).length || op.started_at) {
      const { peak, avg } = _throughputStats(op);
      // v0.50.40: comma-group peak/avg so they match done_summary's fmtNum (was
      // "10,514 items · 207301 avg/s" — grouped + un-grouped side by side).
      // v0.50.42: hint tooltips disambiguate these whole-run figures from the live
      // RATE pill (recent ~10s smoothed) — they legitimately differ when the rate
      // ramps or stalls, which read as a contradiction without the labels.
      if (peak > 0) stats.push({ label: 'peak/s', value: fmtNum(Math.round(peak)),
        hint: 'Highest single 1s throughput sample across the whole run.' });
      if (avg > 0) stats.push({ label: 'avg/s', value: fmtNum(Math.round(avg)),
        hint: 'Whole-run average: items processed ÷ elapsed. Differs from the live RATE pill, which is the recent (~10s) rate.' });
    }
    if ((op.error_count || 0) > 0) {
      stats.push({ label: 'errors', value: fmtNum(op.error_count), bad: true });
    }
    if (!stats.length) return '';
    return `<div class="op-insight-section">
      <div class="op-insight-head">STATS</div>
      <div class="op-insight-stats">${stats.map((s) => `
        <div class="op-stat${s.bad ? ' op-stat-bad' : ''}"${s.hint ? ` title="${esc(s.hint)}"` : ''}>
          <span class="op-stat-value">${esc(s.value)}</span>
          <span class="op-stat-label">${esc(s.label)}</span>
        </div>`).join('')}</div>
    </div>`;
  }

  // v1.21.27: a taller throughput chart (the live sparkline blown up).
  function _renderThroughputChart(op) {
    const tp = (op.detail && op.detail.throughput) || [];
    if (!tp.length) return '';
    const max = Math.max(...tp.map((x) => x.rate || 0));
    if (max <= 0) return '';
    const bars = tp.map((x) => {
      const h = Math.max(2, Math.round(((x.rate || 0) / max) * 40));
      return `<span class="op-tpchart-bar" style="height:${h}px" title="${(x.rate || 0).toFixed(1)}/s"></span>`;
    }).join('');
    // v0.50.39: bar heights + the headline peak both ride the raw per-sample max,
    // which is now the sane peak too (progress.py floors dt at 1.0s, so no sample
    // is an inflated sub-second burst). Bars, tooltips, and header agree.
    // v0.50.42: comma-group the header peak (fmtNum) so it matches the STATS peak/s
    // readout — was max.toFixed(0) ("12703") next to STATS' grouped "12,703".
    return `<div class="op-insight-section">
      <div class="op-insight-head">THROUGHPUT <span class="op-insight-head-aux">items/sec · peak ${fmtNum(Math.round(max))}</span></div>
      <div class="op-tpchart">${bars}</div>
    </div>`;
  }

  function renderExpandedDetail(op) {
    const isLive = (op.status === 'running' || op.status === 'cancelling'
                    || op.status === 'pending');
    const evs = state.expandedEvents[op.op_id];
    const meta = [];
    if (op.started_at) {
      meta.push(`<span>START <b>${esc(fmtClock(op.started_at))}</b></span>`);
    }
    if (op.finished_at) {
      meta.push(`<span>END <b>${esc(fmtClock(op.finished_at))}</b></span>`);
    }
    const elapsed = op.started_at
      ? (new Date(op.finished_at || Date.now())
          - new Date(op.started_at)) / 1000
      : null;
    if (elapsed != null) {
      meta.push(`<span>${isLive ? 'ELAPSED' : 'RAN'} <b>${esc(fmtDuration(elapsed))}</b></span>`);
    }
    if ((op.stage_total || 0) > 0) {
      meta.push(`<span>PROGRESS <b>${fmtNum(op.stage_current)}/${fmtNum(op.stage_total)}</b></span>`);
    }
    let logHtml;
    if (evs === undefined) {
      logHtml = '<div class="op-card-runlog-loading">// loading run log…</div>';
    } else if (!evs.length) {
      logHtml = '<div class="op-card-runlog-empty">// no events recorded in this run window</div>';
    } else {
      logHtml = evs.map((e) => {
        const lvl = esc(String(e.level || '').toLowerCase());
        return `<div class="op-card-runlog-item lvl-${lvl}">
          <span class="op-card-runlog-time">${esc(fmtClock(e.ts))}</span>
          <span class="op-card-runlog-comp">${esc(e.component || '')}</span>
          <span class="op-card-runlog-msg">${esc(e.message || '')}</span>
        </div>`;
      }).join('');
    }
    return `
      <div class="op-card-detail-head">
        <span>// RUN INSIGHT${isLive ? ' · LIVE' : ''}</span>
        <span class="op-card-detail-collapse" data-op-collapse="${esc(op.op_id)}">// COLLAPSE ✕</span>
      </div>
      <div class="op-card-detail-meta">${meta.join('')}</div>
      ${_renderWaterfall(op, isLive)}
      ${_renderInsightStats(op)}
      ${_renderThroughputChart(op)}
      <div class="op-insight-section">
        <div class="op-insight-head">RUN LOG</div>
        <div class="op-card-runlog">${logHtml}</div>
      </div>`;
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
    // v1.20.22: expand state. Synthetic queue ops (download_queue, etc.)
    // have no op_progress run window, so they're not expandable.
    const isSynthetic = !!(op.detail && op.detail.synthetic);
    const isExpanded = state.expandedOpIds.has(op.op_id);
    const expandCls = (isSynthetic ? '' : ' op-card-expandable')
                    + (isExpanded ? ' op-card-expanded' : '');

    return `
      <div class="op-card op-tone-${tone} op-status-${op.status}${showLiveSections ? '' : ' op-card-compact'}${expandCls}"
           data-op-id="${esc(op.op_id)}">
        <div class="op-card-main">
        <div class="op-card-head">
          ${isSynthetic ? '' : '<span class="op-card-caret" aria-hidden="true">&#9656;</span>'}
          <span class="op-card-kind">// ${esc(KIND_LABEL[op.kind] || op.kind)}${
            // v1.24.19: append the refresh SCOPE (plex_enum stamps
            // detail.scope_label = "All libraries" / "Movies" /
            // "Movies (items only)" / "Movies collections") so the card
            // head says WHAT is refreshing, not just the generic kind. The
            // stage line below still names the section actively walked.
            (op.detail && op.detail.scope_label)
              ? ` · ${esc(String(op.detail.scope_label).toUpperCase())}`
              : ''
          }</span>
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
          // v1.22.37: a finished real-bar op renders a full 100% bar (no live
          // counter) so the card visibly completes at 100% instead of dropping
          // the bar at the last running-%. Single-job / indeterminate ops never
          // had a meaningful %, so they still show no bar.
          if (!showLiveSections) {
            // v0.50.40: only a genuinely-DONE op snaps to 100% — a cancelled / failed
            // real-bar op (a download stopped at 40%, an enum that failed at 12/200)
            // freezes at its last % so the bar doesn't falsely read as complete (the
            // headline already states the status).
            if (!_useRealBar(op)) return '';
            const finPct = op.status === 'done' ? 100 : Math.round(pctOf(op) || 0);
            return `<div class="op-card-bar"><div class="op-card-bar-fill" style="width:${finPct}%"></div></div>`;
          }
          // v1.15.2: shared helper — was inlined as
          // `hasRealPct || op.stage_total > 1`. Same rule, but
          // call site stays in lockstep with mini-bar +
          // structural-hash via the helper.
          const useRealBar = _useRealBar(op);
          const showCounter = op.stage_total > 0;
          // v1.18.51: show the % alongside N/N when a real-bar
          // is in play. the user: "any section that displays a %
          // in the status bar [should show] a similar % in the
          // drawer view. when you open the drawer you get the
          // N out N and the bar but not a % like you did just
          // viewing the status bar." Mirrors the topbar mini-
          // bar's `pct.toFixed(0) + '%'` rendering so the two
          // surfaces agree at a glance. Hidden when the bar is
          // indeterminate (no real percentage to show).
          const showPct = useRealBar && pct != null;
          const counterHtml = showCounter ? `
            <div class="op-card-counter">
              <span class="op-card-counter-current"
                    data-op-counter
                    data-op-counter-target="${op.stage_current || 0}">
                ${fmtNum(op.stage_current)}
              </span>
              <span class="op-card-counter-total">/ ${fmtNum(op.stage_total)}</span>
              ${showPct
                ? `<span class="op-card-counter-pct" data-op-counter-pct>${pct.toFixed(0)}%</span>`
                : ''}
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
            <span class="op-card-meta-item" data-meta-key="rate"
                  title="Recent throughput (smoothed over the last ~10s) — drives the ETA. The whole-run average is in RUN INSIGHT → STATS (avg/s), which can differ when the rate ramps or stalls.">
              <span class="op-card-meta-label">RATE</span>
              <span class="op-card-meta-value">${fmtRate(rate)}/s</span>
            </span>` : ''}
          ${(etaSec != null && isLive) ? `
            <span class="op-card-meta-item" data-meta-key="eta">
              <span class="op-card-meta-label">ETA</span>
              <span class="op-card-meta-value">${esc(fmtEta(etaSec))}</span>
            </span>` : ''}
          ${(() => {
            // v1.14.21: hide the ELAPSED meta-row when the op is
            // pending with zero progress — i.e. it hasn't actually
            // STARTED, it's queued behind something else (place
            // queue waiting on downloads to land is the canonical
            // case). The PENDING badge in the card header + the
            // 0/N counter already convey "waiting"; an elapsed
            // timer ticking up from 0 reads as "stuck working"
            // even though the worker hasn't picked the job up.
            // the user's repro: PLACE QUEUE showed "Place into Plex
            // queued (20)" with 0/20 and ELAPSED 1m 18s — looked
            // broken; the workers were busy on concurrent
            // downloads.
            const isStuckPending = isLive
                                 && op.status === 'pending'
                                 && (op.stage_current || 0) === 0;
            if (elapsed == null || isStuckPending) return '';
            return `
            <span class="op-card-meta-item" data-meta-key="elapsed">
              <span class="op-card-meta-label">${isLive ? 'ELAPSED' : 'RAN'}</span>
              <span class="op-card-meta-value">${esc(fmtDuration(elapsed))}</span>
            </span>`;
          })()}
          ${(op.error_count > 0) ? `
            <span class="op-card-meta-item" data-meta-key="errors" style="color:var(--red)">
              <span class="op-card-meta-label">ERRORS</span>
              <span class="op-card-meta-value">${fmtNum(op.error_count)}</span>
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
          ${/_queue$/.test(op.kind) ? `
            <button class="op-card-cancel" data-bulk-cancel="${esc(op.kind.replace(/_queue$/, ''))}">
              // CANCEL ALL PENDING
            </button>` : ''}
          <div class="op-card-cancel-note muted small">
            // per-job cancel via <a href="/queue">LOGS</a>
          </div>` : ''}
        </div>
        ${isExpanded ? `<div class="op-card-detail">${renderExpandedDetail(op)}</div>` : ''}
      </div>`;
  }

  // v1.13.12: cache the last rendered HTML of the drawer body. Skip
  // the swap when nothing changed (idle polls + the 750ms /api/progress
  // server-side cache mean most poll cycles return identical data).
  // Pre-fix every poll tore down + rebuilt every op-card, which read
  // as a hard flicker during active sync runs.
  // v1.14.81: replaced by the per-card hash-skip inside
  // renderDrawerBody. The body-level cache only skipped when the
  // ENTIRE rendered HTML matched — which never fired during an
  // active op (counter/percent/elapsed tick every poll). The
  // per-card approach localizes re-renders to just the card whose
  // content changed; stable cards (finished ops, headers) keep
  // their DOM nodes and stop flickering.
  // v1.14.90: per-card hash-skip alone wasn't enough — for ACTIVE
  // cards every poll changed elapsed/counter/percent/rate, so the
  // hash never matched and the entire card DOM was replaced every
  // tick. The replace itself is the source of the visible text
  // flicker the user still saw on running PLEX REFRESH /
  // THEMERRDB SYNC cards. Fix: compute a "structural hash"
  // (kind + status + meta-presence flags + stage + badge
  // presences) that excludes high-frequency dynamic values. If
  // the structural hash matches existing, the card's structure
  // hasn't changed — call updateCardInPlace to twiddle just the
  // textContent of dynamic spans (counter, bar fill, meta values,
  // sparkline, activity). Full DOM replacement is reserved for
  // genuine structural transitions (status flip, meta-item
  // toggle on/off, badge appearance, etc.).

  function _structuralHash(op) {
    const isLive = (op.status === 'running' || op.status === 'cancelling'
                    || op.status === 'pending');
    const rate = smoothedRate(op.detail && op.detail.throughput);
    const etaSec = eta(op);
    const isStuckPending = isLive && op.status === 'pending'
                           && (op.stage_current || 0) === 0;
    const hasElapsed = !!op.started_at && !isStuckPending;
    const hasCounter = (op.stage_total || 0) > 0;
    // v1.15.2: shared helper, see _useRealBar definition.
    const useRealBar = _useRealBar(op);
    const buf = (op.detail && op.detail.throughput) || [];
    const sparkMax = buf.length ? Math.max(...buf.map((x) => x.rate || 0)) : 0;
    return JSON.stringify({
      k: op.kind,
      s: op.status,
      sg: op.stage,
      hc: hasCounter ? 1 : 0,
      ub: useRealBar ? 1 : 0,
      rt: rate > 0 ? 1 : 0,
      et: (etaSec != null && isLive) ? 1 : 0,
      el: hasElapsed ? 1 : 0,
      er: (op.error_count || 0) > 0 ? 1 : 0,
      nc: !!(op.detail && op.detail.no_changes) ? 1 : 0,
      fa: !!(op.detail && op.detail.fallback_active) ? 1 : 0,
      fr: (op.detail && op.detail.fallback_reason) || '',
      em: !!(op.detail && op.detail.error_message) ? 1 : 0,
      sp: sparkMax > 0 ? 1 : 0,
      sy: !!(op.detail && op.detail.synthetic) ? 1 : 0,
      isL: isLive ? 1 : 0,
      // v1.21.27: exp/evn were REMOVED from the structural hash. Pre-fix
      // they forced a full card DOM REPLACE on every expand/collapse + every
      // run-log event accrual — the click flicker the user flagged. The RUN
      // INSIGHT panel now expands/collapses + refreshes via
      // _updateCardInPlace (injects/removes the .op-card-detail child in
      // place, no card repaint). The full `cardHash` still changes when the
      // detail appears/changes, which routes here (cardSkel stable).
    });
  }

  function _updateCardInPlace(el, op) {
    const isLive = (op.status === 'running' || op.status === 'cancelling'
                    || op.status === 'pending');
    const headline = isLive
      ? (op.stage_label || op.stage || '…')
      : _doneHeadline(op);
    const rate = smoothedRate(op.detail && op.detail.throughput);
    const etaSec = eta(op);
    const elapsed = op.started_at
      ? (new Date(op.finished_at || Date.now())
          - new Date(op.started_at)) / 1000
      : null;
    const pct = pctOf(op);

    const stage = el.querySelector('.op-card-stage');
    if (stage && stage.textContent !== headline) stage.textContent = headline;

    const statusEl = el.querySelector('.op-card-status');
    if (statusEl) {
      const want = String(op.status || '').toUpperCase();
      if (statusEl.textContent !== want) statusEl.textContent = want;
    }

    // Counter — update target AND textContent directly.
    // v1.15.13: pre-fix this only set data-op-counter-target and
    // relied on tickCounters() to advance textContent. tickCounters
    // has a `current = +attr || target` fallback that triggers
    // when data-op-counter-current is unset (which it always is on
    // first render — initial textContent is set inline by the
    // template, but the current attr is never seeded). The fallback
    // makes current === target on every read → the early-return
    // fires before the textContent write → the displayed counter
    // stays stuck on whatever value the template literal injected.
    // the user v1.15.12 repro: drawer "130/2,127" big number stayed
    // stuck while activity log + elapsed timer kept ticking
    // (those use direct metaUpdate writes, not the counter
    // animation). Snap to target instead — losing the smooth
    // interpolation effect is a fair trade for correctness.
    const cur = el.querySelector('.op-card-counter-current');
    if (cur) {
      const target = String(op.stage_current || 0);
      if (cur.getAttribute('data-op-counter-target') !== target) {
        cur.setAttribute('data-op-counter-target', target);
        cur.setAttribute('data-op-counter-current', target);
        cur.textContent = fmtNum(op.stage_current || 0);
      }
    }
    const tot = el.querySelector('.op-card-counter-total');
    if (tot) {
      const want = `/ ${fmtNum(op.stage_total)}`;
      if (tot.textContent.trim() !== want.trim()) tot.textContent = want;
    }
    // v1.18.51: live-tick the counter % so it advances alongside
    // current/total. Counterpart to the initial-render block in
    // the template literal above. Skips when the element isn't
    // present (indeterminate bars don't render the %).
    const pctEl = el.querySelector('[data-op-counter-pct]');
    if (pctEl && pct != null) {
      const want = `${pct.toFixed(0)}%`;
      if (pctEl.textContent !== want) pctEl.textContent = want;
    }

    // Real-bar fill width. Indeterminate bars are CSS-animated; no
    // poll-tick update needed.
    const realBar = el.querySelector(
      '.op-card-bar:not(.op-card-bar-indet) .op-card-bar-fill');
    if (realBar) {
      const want = `${pct != null ? pct.toFixed(1) : 0}%`;
      if (realBar.style.width !== want) realBar.style.width = want;
    }

    // Meta values — found by data-meta-key attribute (rate / eta /
    // elapsed / errors). Structural hash gates whether each item
    // exists; if it's there, just update its value span.
    const metaUpdate = (key, want) => {
      const item = el.querySelector(
        `.op-card-meta-item[data-meta-key="${key}"] .op-card-meta-value`);
      if (item && item.textContent !== want) item.textContent = want;
    };
    if (rate > 0) metaUpdate('rate', `${fmtRate(rate)}/s`);
    if (etaSec != null && isLive) metaUpdate('eta', fmtEta(etaSec));
    if (elapsed != null) metaUpdate('elapsed', fmtDuration(elapsed));
    if ((op.error_count || 0) > 0) metaUpdate('errors', fmtNum(op.error_count));

    // Timeline cell classes — update in place so the active step
    // marker advances without repainting the whole strip.
    const timeline = el.querySelector('.op-card-timeline');
    if (timeline) {
      const stages = STAGE_TIMELINE[op.kind] || [];
      const currentIdx = stages.findIndex((s) => s.key === op.stage);
      // v1.21.25/.26: DONE-only (see renderTimeline) — a completed op's
      // whole strip is green; pending + failed-before-start stay un-green.
      const finished = (op.status === 'done');
      const cells = timeline.querySelectorAll('.op-card-timeline-step');
      cells.forEach((cell, i) => {
        let cls = 'op-card-timeline-step';
        if (finished) {
          cls += ' is-done';
        } else if (currentIdx >= 0) {
          if (i < currentIdx) cls += ' is-done';
          else if (i === currentIdx) cls += ' is-current';
        }
        if (cell.className !== cls) cell.className = cls;
      });
    }

    // Sparkline + activity — small lists, fine to rebuild as a
    // string and only innerHTML if differs. innerHTML on a small
    // sub-element doesn't repaint surrounding nodes the way a
    // body.replaceChildren does.
    const spark = el.querySelector('.op-card-spark');
    if (spark) {
      const html = renderSparkline(op);
      if (html) {
        const tmp = document.createElement('div');
        tmp.innerHTML = html;
        const newSpark = tmp.firstElementChild;
        if (newSpark && spark.innerHTML !== newSpark.innerHTML) {
          spark.innerHTML = newSpark.innerHTML;
        }
      }
    }
    const activity = el.querySelector('.op-card-activity');
    if (activity) {
      const html = renderActivity(op);
      if (html) {
        const tmp = document.createElement('div');
        tmp.innerHTML = html;
        const newActivity = tmp.firstElementChild;
        if (newActivity && activity.innerHTML !== newActivity.innerHTML) {
          activity.innerHTML = newActivity.innerHTML;
        }
      }
    }

    // Cancelling state on the cancel button (text flips
    // "// CANCEL" → "// CANCELLING…" + disabled).
    const cancelBtn = el.querySelector('button.op-card-cancel');
    if (cancelBtn) {
      const wantCancelling = op.status === 'cancelling';
      if (wantCancelling && !cancelBtn.disabled) cancelBtn.disabled = true;
      const wantText = wantCancelling ? '// CANCELLING…' : '// CANCEL';
      if (cancelBtn.textContent.trim() !== wantText) {
        cancelBtn.textContent = wantText;
      }
    }

    // v1.21.27: expand/collapse + RUN INSIGHT refresh IN PLACE. Inject or
    // remove the .op-card-detail child on this card only — so opening a card
    // doesn't trigger a full card DOM replace (the click flicker). A live,
    // expanded card also gets its insight refreshed here (run log loads,
    // throughput / stage-timing grow) without repainting the card body.
    const wantExpanded = state.expandedOpIds.has(op.op_id)
                         && !(op.detail && op.detail.synthetic);
    const detailEl = el.querySelector(':scope > .op-card-detail');
    if (wantExpanded && !detailEl) {
      el.classList.add('op-card-expanded');
      el.insertAdjacentHTML('beforeend',
        `<div class="op-card-detail">${renderExpandedDetail(op)}</div>`);
    } else if (!wantExpanded && detailEl) {
      el.classList.remove('op-card-expanded');
      detailEl.remove();
    } else if (wantExpanded && detailEl) {
      const want = renderExpandedDetail(op);
      if (detailEl.innerHTML !== want) detailEl.innerHTML = want;
    }
  }

  function renderDrawerBody(ops) {
    // v1.20.22: reconcile the expanded card. If the op aged out of the
    // list (finished + dropped past the LAST-OPS tail), collapse and
    // drop its cached run log.
    if (state.expandedOpIds.size) {
      const live = new Set(ops.map((o) => o.op_id));
      state.expandedOpIds.forEach((id) => {
        if (!live.has(id)) {
          state.expandedOpIds.delete(id);
          delete state.expandedEvents[id];
        }
      });
    }
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
      // v1.20.40: stable kind-priority tiebreak BEFORE updated_at. With
      // the dedicated place worker (v1.20.40) the download_queue and
      // place_queue synth cards run concurrently, and BOTH refresh their
      // updated_at every poll — so an updated_at-only tiebreak made the
      // two top cards swap positions each tick ("bouncing to placing,
      // download and back"). OP_MINI_PRIORITY gives a deterministic order
      // (download above place); updated_at only breaks same-kind ties.
      const pa = OP_MINI_PRIORITY[a.kind] != null
        ? OP_MINI_PRIORITY[a.kind] : OP_MINI_PRIORITY_FALLBACK;
      const pb = OP_MINI_PRIORITY[b.kind] != null
        ? OP_MINI_PRIORITY[b.kind] : OP_MINI_PRIORITY_FALLBACK;
      if (pa !== pb) return pa - pb;
      return String(b.updated_at || '').localeCompare(String(a.updated_at || ''));
    });
    // v1.20.41: ALL cards always render. Expanding one grows that card
    // downward (its run log renders inside it); the cards below shift
    // down, the panel keeps its width, nothing moves sideways.
    const finished = ops
      .filter((o) => o.status !== 'running' && o.status !== 'cancelling'
                     && o.status !== 'pending')
      .slice(0, 3);
    const body = document.getElementById('ops-drawer-body');
    if (!body) return;

    // v1.14.81: per-card render + per-card hash-skip. Pre-fix the
    // entire drawer body re-rendered every poll (1Hz when active),
    // tearing down + rebuilding every card's DOM nodes — so even
    // historical timestamps inside finished cards visibly flickered
    // every second alongside the active card's tick. the user flagged
    // the per-second flicker on the LIVE OPS drawer (THEMERRDB SYNC
    // done card's activity timestamps + the active op's enumerate/
    // reconcile tab indicators).
    //
    // Each card now carries `data-card-key` (stable identity:
    // header:active / header:lastops / op:<op_id>) + `data-card-hash`
    // (per-card content hash). On poll, we walk the desired card list:
    //  - existing card with same key + same hash → leave in place
    //    (DOM nodes preserved → no flicker, text selection survives)
    //  - existing with same key but different hash → replace just
    //    that card
    //  - desired card not in existing → insert new
    //  - existing card not in desired → remove
    // Stable cards (the THEMERRDB SYNC done timestamp, etc.) stop
    // flickering entirely; only the actively-changing card paints
    // each tick.
    // v1.14.90: each desired entry now also carries `op` (when it
    // represents an op card) and `skel` (its structural hash) so
    // the diff loop can do in-place text updates instead of full
    // DOM replacement when only dynamic values changed.
    const desired = [];  // [{key, hash, html, op?, skel?}]
    if (!active.length && !finished.length) {
      desired.push({
        key: 'empty:idle-24h',
        hash: 'idle-24h',
        html: '<div class="ops-drawer-empty">// idle · no ops in the last 24 hours</div>',
      });
    } else {
      if (active.length) {
        desired.push({
          key: 'header:active',
          hash: 'active',
          html: '<div class="op-card-kind" style="margin:0 0 6px">// ACTIVE</div>',
        });
        active.forEach((op) => {
          const html = renderCard(op);
          desired.push({
            key: `op:${op.op_id}`,
            hash: html,
            html,
            op,
            skel: _structuralHash(op),
          });
        });
      } else {
        desired.push({
          key: 'empty:active',
          hash: 'no-active',
          html: '<div class="ops-drawer-empty" style="padding:14px 0">// idle · no ops running</div>',
        });
      }
      if (finished.length) {
        desired.push({
          key: 'header:lastops',
          hash: 'lastops',
          html: '<div class="op-card-kind" style="margin:18px 0 6px">// LAST OPS</div>',
        });
        finished.forEach((op) => {
          const html = renderCard(op);
          desired.push({
            key: `op:${op.op_id}`,
            hash: html,
            html,
            op,
            skel: _structuralHash(op),
          });
        });
      }
    }

    // v1.14.90: fast path — if the existing children match desired
    // by key in the same order, do per-slot in-place updates
    // instead of building a fragment + body.replaceChildren. The
    // body.replaceChildren is what was causing the visible text
    // flicker on every poll for active ops (elapsed/counter/rate
    // change → hash differs → full replace → DOM repaint).
    const existingChildren = Array.from(body.children);
    const sameStructure = existingChildren.length === desired.length
      && existingChildren.every(
        (el, i) => el.dataset.cardKey === desired[i].key);
    if (sameStructure) {
      desired.forEach((card, i) => {
        const el = existingChildren[i];
        if (el.dataset.cardHash === card.hash) return;  // no change
        if (card.op && card.skel
            && el.dataset.cardSkel === card.skel) {
          // Structural shape unchanged — twiddle dynamic values
          // in place. No DOM tear-down → no flicker.
          _updateCardInPlace(el, card.op);
          el.dataset.cardHash = card.hash;
        } else {
          // Structural change in this slot only — replace the
          // single element in place. Cheaper than rebuilding
          // the whole body via replaceChildren.
          const tmp = document.createElement('div');
          tmp.innerHTML = card.html;
          const newEl = tmp.firstElementChild;
          if (newEl) {
            newEl.dataset.cardKey = card.key;
            newEl.dataset.cardHash = card.hash;
            if (card.skel) newEl.dataset.cardSkel = card.skel;
            body.replaceChild(newEl, el);
          }
        }
      });
      return;
    }

    // Different shape (cards added/removed/reordered) — walk
    // desired list, build a fragment reusing existing nodes whose
    // hash matches + creating fresh nodes otherwise.
    const existingByKey = {};
    existingChildren.forEach((el) => {
      const key = el.dataset.cardKey;
      if (key) existingByKey[key] = el;
    });
    const frag = document.createDocumentFragment();
    desired.forEach((card) => {
      const existing = existingByKey[card.key];
      if (existing && existing.dataset.cardHash === card.hash) {
        // Same content — reuse the existing node. The node moves
        // in the DOM (detach → fragment → reattach) but its
        // children + textContent + attributes are untouched, so
        // browsers preserve text selection across the move and
        // there's no visible flicker.
        frag.appendChild(existing);
        delete existingByKey[card.key];
      } else if (existing && card.op && card.skel
                 && existing.dataset.cardSkel === card.skel) {
        // Structural match across reorder — in-place update + reuse.
        _updateCardInPlace(existing, card.op);
        existing.dataset.cardHash = card.hash;
        frag.appendChild(existing);
        delete existingByKey[card.key];
      } else {
        const tmp = document.createElement('div');
        tmp.innerHTML = card.html;
        // v1.14.83: use firstElementChild instead of firstChild
        // so leading whitespace text nodes (renderCard returns
        // a template literal starting with `\n      <div>`)
        // don't get picked as the "card" — they have no
        // dataset property and the next .dataset.cardKey
        // assignment would TypeError, silently bailing the
        // whole forEach loop and leaving body.replaceChildren
        // with an empty fragment. the user saw this as a stuck
        // "// loading…" drawer on idle pages where the empty
        // state happened to slip through but the next render
        // tick (with active ops) wedged.
        const newEl = tmp.firstElementChild;
        if (newEl) {
          newEl.dataset.cardKey = card.key;
          newEl.dataset.cardHash = card.hash;
          if (card.skel) newEl.dataset.cardSkel = card.skel;
          frag.appendChild(newEl);
        }
        if (existing) delete existingByKey[card.key];
      }
    });
    // Stale children (in body but not in desired) get dropped here.
    body.replaceChildren(frag);
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
  // v1.22.37: mini-bar DONE-flash state. _lastMiniOpId is the op currently
  // shown in the mini-bar; when it finishes we hold a full 100% bar for ~1.5s
  // (mirroring the ✓ DONE flash on the dash/library buttons) before going
  // idle, so the most-watched surface visibly completes instead of jumping
  // from ~80% straight to IDLE. _flashedMiniOpId guards against re-flashing the
  // same op on subsequent idle polls.
  let _lastMiniOpId = null;
  let _flashedMiniOpId = null;
  let _doneFlashTimer = null;
  function setOptimisticPlaceholder(kind, label) {
    _optimisticOp = {
      kind,
      label,
      expiresAt: Date.now() + 5000,
    };
    // v0.50.68/76: kick the hero wave the INSTANT the user clicks (the user — the
    // wave "doesn't begin right away"). Without this the wave wouldn't start moving
    // until refreshTopbarStatus saw the enqueued job past the /api/stats 1s cache
    // (~1.1s). __motifHeroWaveBump raises the wave's energy target to the optimistic
    // floor NOW so it begins accelerating on the click (gas pedal); refreshTopbar
    // then refines the target from the real score, and its busy calc unions
    // hasOptimistic() so the target can't drop before the real op lands.
    if (window.__motifHeroWaveBump) window.__motifHeroWaveBump();
    renderTopbar(state.ops || []);
    boostPoll();
  }

  // v0.50.68: is a click-time optimistic op still live? refreshTopbarStatus
  // unions this into its busy calc so the hero wave can't be cleared in the
  // gap between the click and the enqueued job showing up in /api/stats.
  function hasOptimistic() {
    return !!_optimisticOp && _optimisticOp.expiresAt > Date.now();
  }

  // v1.15.35: explicit clear for the failure path. Pre-fix the
  // placeholder lingered for its 5s TTL even when the action it
  // shadowed had failed — the user dismissed the alert and saw
  // the drawer still saying "// QUEUING DOWNLOAD" for a row
  // that wasn't actually queued. Callers that surface an error
  // alert should also call this so the optimism doesn't outlive
  // the action's actual outcome.
  function clearOptimisticPlaceholder(kind) {
    if (_optimisticOp && (kind == null || _optimisticOp.kind === kind)) {
      _optimisticOp = null;
      renderTopbar(state.ops || []);
    }
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
      // v1.22.37: DONE flash. If the op that was just in the mini-bar has
      // finished, hold its bar at a full 100% for ~1.5s before going idle —
      // the op's bar otherwise never renders 100% because it leaves the
      // running set the instant it reaches the final count. Only real-bar ops
      // (multi-job / yt-dlp %) flash; single-job indeterminate ops had no
      // meaningful %. Guarded by _flashedMiniOpId so it fires once per op.
      if (_lastMiniOpId && _flashedMiniOpId !== _lastMiniOpId) {
        const doneOp = ops.find((o) => o.op_id === _lastMiniOpId
                                       && o.status === 'done');
        if (doneOp && _useRealBar(doneOp)) {
          _flashedMiniOpId = _lastMiniOpId;
          const tone = TONE_BY_KIND[doneOp.kind] || 'tdb';
          if (idle) idle.hidden = true;
          if (overflow) overflow.hidden = true;
          mini.hidden = false;
          mini.className = `op-mini op-tone-${tone}`;
          mini.innerHTML = `
            <span class="op-mini-label">${esc(KIND_LABEL[doneOp.kind] || doneOp.kind)} ✓</span>
            <span class="op-mini-bar"><span class="op-mini-bar-fill" style="width:100%"></span></span>
            <span class="op-mini-pct">100%</span>
          `;
          clearTimeout(_doneFlashTimer);
          _doneFlashTimer = setTimeout(() => {
            mini.hidden = true;
            if (idle) idle.hidden = false;
          }, 1500);
          return;
        }
      }
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
    // v1.14.65: prefer status='running' over status='pending' when
    // picking the mini-bar candidate. Pre-fix the synth
    // `plex_enum_pending` card (api.py: refreshes its updated_at
    // every /api/progress poll) competed with the actually-running
    // plex_enum op for the most-recent-updated slot, so the
    // mini-bar label visibly flipped between "MOVIES (FETCH)" and
    // "1 LIBRARY REFRESH QUEUED" each tick. the user repro: queued
    // a 2nd library while one was refreshing → topbar pinged
    // between the two labels making the actual progress hard to
    // read. The pending card stays visible in the drawer (where
    // it has room to coexist); only the contended single mini-
    // bar slot now strictly favors the running op.
    // v1.15.82: stable priority order for the picker. Pre-fix
    // two concurrent real-running ops (e.g., download + plex_enum
    // refresh) tied at the updated_at-DESC sort and the mini-bar
    // visibly flipped between them — whichever ticked its
    // updated_at last won the slot for that frame. the user:
    // "Let's have download take prio followed by refresh, sync
    // themerrdb, prob url, probe plex sidecar." Now we pick by
    // OP_PRIORITY (lower index = higher priority); updated_at
    // is only the tiebreaker within the same kind.
    const runningOnly = running.filter(
      (o) => o.status === 'running' || o.status === 'cancelling');
    const candidates = runningOnly.length > 0 ? runningOnly : running;
    const op = candidates.slice().sort((a, b) => {
      const pa = OP_MINI_PRIORITY[a.kind];
      const pb = OP_MINI_PRIORITY[b.kind];
      const ra = (pa == null) ? OP_MINI_PRIORITY_FALLBACK : pa;
      const rb = (pb == null) ? OP_MINI_PRIORITY_FALLBACK : pb;
      if (ra !== rb) return ra - rb;
      return String(b.updated_at).localeCompare(String(a.updated_at));
    })[0];
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
    // v1.15.2: shared helper, see _isIndeterminate definition.
    // Pre-fix this site inlined `!hasRealPct && stage_total <= 1`
    // — equivalent to the card-bar rule but written inverted, so
    // a future change to one site wouldn't obviously land in the
    // other.
    const indeterminate = _isIndeterminate(op);
    mini.hidden = false;
    mini.className = `op-mini op-tone-${tone}` + (indeterminate ? ' op-mini-indet' : '');
    // v1.13.27: append a queue-position suffix when this op is one
    // of multiple jobs of the same kind in flight. plex_enum is the
    // common case — user fires // SYNC PLEX on movies + tv + anime
    // in quick succession, the worker serializes them, and the user
    // wants to see "I'm on #2 of 4 right now". Position computed
    // from a window-scoped HW (high water) updated by app.js's
    // refreshTopbarStatus tick. Suffix only renders when hw > 1.
    // v1.14.84: retired the inline "(N of M)" suffix. the user read
    // it as per-section phase progress ("phase 1 of 3 within this
    // section's work") rather than queue depth across multiple
    // jobs — the wording was ambiguous when attached to a section
    // name. The signal moved to the #op-mini-overflow pill below
    // ("+N QUEUED" in plex tone) where it can't be misread as
    // section-internal.
    const labelText = op.stage_label || KIND_LABEL[op.kind] || '…';
    mini.innerHTML = `
      <span class="op-mini-label">${esc(labelText)}</span>
      <span class="op-mini-bar"><span class="op-mini-bar-fill"
            style="width:${indeterminate ? 100 : (pct != null ? pct.toFixed(1) : 30)}%"></span></span>
      <span class="op-mini-pct">${indeterminate ? '' : (pct != null ? pct.toFixed(0) + '%' : '')}</span>
    `;
    // v1.22.37: remember which op the mini-bar is showing + reset the DONE-
    // flash guard so this op flashes once when it finishes. A fresh running
    // render also cancels any pending flash timer (a new op took the slot).
    _lastMiniOpId = op.op_id;
    _flashedMiniOpId = null;
    clearTimeout(_doneFlashTimer);
    // v1.14.84: repurpose the v1.13.45-hidden #op-mini-overflow
    // pill as the "+N QUEUED" queue-depth badge. Source the count
    // from the plex_enum_pending synth row's detail.queue_depth
    // (added server-side same release). The synth row is only
    // emitted when there's a REAL queue (per v1.14.77: pending+
    // running >= 2 OR pending >= 2), so the badge naturally hides
    // for single-job bursts. Tone is plex (--green) matching the
    // drawer's PLEX REFRESH (QUEUED) card so the queued-state
    // visual identity is consistent across surfaces.
    if (overflow) {
      const pendingSynth = ops.find(
        (o) => o.kind === 'plex_enum_pending');
      const queueDepth = pendingSynth
        && pendingSynth.detail
        && typeof pendingSynth.detail.queue_depth === 'number'
        ? pendingSynth.detail.queue_depth
        : 0;
      // v1.14.90: also surface tdb_sync_pending. Pre-fix the user
      // reported clicking SYNC THEMERRDB while a Plex refresh
      // was running showed nothing in the topbar — only the
      // dash button's lock + the "// SYNCING THEMERRDB…" label
      // hinted that anything was queued. SYNC has its own tone
      // ('tdb' = orange) distinct from plex_enum_pending's plex
      // tone, so when both are queued the badge labels disambiguate.
      const syncPendingSynth = ops.find(
        (o) => o.kind === 'tdb_sync_pending');
      const hasSyncPending = !!syncPendingSynth;
      // v1.15.5: also surface download_queue's pending count.
      // Pre-fix the topbar label inlined "Downloading: X (5
      // queued)" but the queue depth had no separate visual
      // signal — the user wanted the same +N QUEUED treatment
      // plex_enum_pending and tdb_sync_pending get. The
      // download_queue synth's detail.queue_depth (added v1.15.5)
      // is set only when running_n>0 + pending_n>0 (active
      // download with more queued behind), so the badge fires
      // exactly when a separate queue signal is meaningful.
      // Pure-pending cases (running=0) keep their existing
      // mini-bar label "Theme download queued (N)" — no badge
      // needed since the whole queue IS the mini-bar.
      const dlQueueSynth = ops.find((o) => o.kind === 'download_queue');
      const dlQueueDepth = dlQueueSynth
        && dlQueueSynth.detail
        && typeof dlQueueSynth.detail.queue_depth === 'number'
        ? dlQueueSynth.detail.queue_depth
        : 0;
      // v1.15.30: lift live-probe detection ABOVE the cascade so
      // every overflow branch can append the probe label as a
      // suffix. Pre-fix the probe pill only rendered in the final
      // else-branch (no other queue/sync signals); when sync got
      // queued on top of a running probe + running plex_enum, the
      // sync-queued branch fired and the probe info was lost.
      // the user: "when a sync of themerrdb is queued on top of
      // [a probe + queued refresh] it just says sync pending and
      // you lose the information on the probe. Can we treat it
      // similar to if there are multiple refresh but sync queued
      // — include both in the status bar pill for the queued
      // items." Same composition shape as v1.14.90's
      // "+N QUEUED · SYNC" — the queue label, then a separator,
      // then the auxiliary signal.
      const probeKinds = new Set([
        'bulk_probe_tdb', 'reprobe_plex_themes', 'bulk_lps',
      ]);
      const liveProbe = ops.find((o) => (
        probeKinds.has(o.kind)
        && (o.status === 'running' || o.status === 'cancelling')
        && o.kind !== op.kind
      ));
      const probeLabel = liveProbe ? (
        liveProbe.kind === 'bulk_probe_tdb' ? 'PROBING TDB'
          : liveProbe.kind === 'reprobe_plex_themes' ? 'REPROBING PLEX'
            : 'BULK LPS'
      ) : null;
      const probeSuffix = probeLabel ? ` · ${probeLabel}` : '';
      const probeTitleClause = probeLabel
        ? ` + ${probeLabel} running in the background`
        : '';
      if (queueDepth > 0 && hasSyncPending) {
        // Both queues active. Render two pieces of info: the
        // plex queue depth + a SYNC-WAIT marker. Tone stays plex
        // (the larger/more-visual queue) but the label gets a
        // " · SYNC" suffix to flag the queued sync. The drawer
        // has the full breakdown.
        // v1.15.30: also append probe label when one is running.
        overflow.className = 'op-pill op-tone-plex';
        overflow.title = (
          `${queueDepth} library refresh${queueDepth === 1 ? '' : 'es'} `
          + `queued + a THEMERRDB SYNC waiting${probeTitleClause} — `
          + 'click to expand the drawer.'
        );
        overflow.innerHTML = (
          `<span class="op-pill-count">+${queueDepth}</span>`
          + `<span class="op-pill-label">QUEUED · SYNC${probeSuffix}</span>`
        );
        overflow.hidden = false;
      } else if (queueDepth > 0) {
        // Override the template's static op-tone-warn with
        // op-tone-plex so the badge reads as a "queued plex
        // refresh" (same family as the running mini-bar's
        // plex tone, same family as the drawer's QUEUED card).
        // v1.15.30: append probe label when one is running.
        overflow.className = 'op-pill op-tone-plex';
        overflow.title = (
          `${queueDepth} library refresh${queueDepth === 1 ? '' : 'es'} `
          + `queued behind the running one${probeTitleClause} — `
          + 'click to expand the drawer.'
        );
        overflow.innerHTML = (
          `<span class="op-pill-count">+${queueDepth}</span>`
          + `<span class="op-pill-label">QUEUED${probeSuffix}</span>`
        );
        overflow.hidden = false;
      } else if (hasSyncPending) {
        // v1.14.90: only sync queued (no plex queue) — distinct
        // tdb tone so the badge reads as a queued sync, not a
        // queued refresh. Most common when the user clicks SYNC
        // THEMERRDB while a single Plex refresh is mid-flight.
        // v1.15.30: append probe label so a probe running
        // alongside a queued sync stays visible (the user's
        // exact repro — pre-fix the sync-queued branch hid
        // the probe info).
        overflow.className = 'op-pill op-tone-tdb';
        overflow.title = (
          'A THEMERRDB SYNC is queued behind the running Plex '
          + `refresh / scan${probeTitleClause} — `
          + 'click to expand the drawer.'
        );
        overflow.innerHTML = (
          `<span class="op-pill-label">SYNC QUEUED${probeSuffix}</span>`
        );
        overflow.hidden = false;
      } else if (dlQueueDepth > 0) {
        // v1.15.5: only downloads queued (no plex/sync queue) —
        // the badge's tone matches the download_queue card's tone
        // family so the user reads "downloads waiting" (vs the
        // amber plex-refresh / green sync-queued badges) at a
        // glance. the user: parity with refresh / sync queued
        // surfaces.
        // v1.19.88: warn→queue (cyan). The download_queue card
        // moved off the amber 'warn' tone to the dedicated cyan
        // 'queue' tone in the ops-drawer tone realignment; this
        // badge follows so the two surfaces stay in lockstep.
        // v1.15.30: append probe label when one is running.
        overflow.className = 'op-pill op-tone-queue';
        overflow.title = (
          `${dlQueueDepth} download${dlQueueDepth === 1 ? '' : 's'} `
          + `queued behind the running one(s)${probeTitleClause} — `
          + 'click to expand the drawer.'
        );
        overflow.innerHTML = (
          `<span class="op-pill-count">+${dlQueueDepth}</span>`
          + `<span class="op-pill-label">QUEUED${probeSuffix}</span>`
        );
        overflow.hidden = false;
      } else if (liveProbe) {
        // v1.15.7: probe running but hidden by the mini-bar's
        // higher-priority pick. Pre-fix when bulk_probe_tdb or
        // reprobe_plex_themes was running concurrently with
        // another op (e.g., a plex_enum refresh started mid-
        // probe), the mini-bar showed only the most-recently-
        // updated one — the user reported "you can't tell the
        // probe is still going without looking in the drawer
        // or waiting for the refresh to finish."
        // Tone: tdb (orange) — probes are TDB-side metadata
        // checks, distinct from the running mini-bar's tone
        // (likely plex / warn). The label names the probe
        // kind for unambiguous readout.
        overflow.className = 'op-pill op-tone-tdb';
        overflow.title = (
          `${probeLabel} is still running in the background — `
          + 'click to expand the drawer for full progress.'
        );
        overflow.innerHTML = (
          `<span class="op-pill-label">${probeLabel}</span>`
        );
        overflow.hidden = false;
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
      // v1.21.30: keep the fast cadence while the drawer is OPEN even when
      // nothing is running yet. Pre-fix, opening the drawer and THEN starting
      // a sync/refresh downshifted to 10s on the very next poll — the worker
      // hadn't created the op_progress row yet (running/pending both false) —
      // so the open drawer sat stale and a short op could finish before the
      // next 10s tick. You had to close+reopen to see live progress.
      const newInterval = (running || pending || state.drawerOpen) ? 1000 : 10000;
      if (newInterval !== state.pollInterval) {
        state.pollInterval = newInterval;
        if (state.pollTimer) clearTimeout(state.pollTimer);
      }
      renderTopbar(state.ops);
      if (state.drawerOpen) {
        renderDrawerBody(state.ops);
        // v1.20.22: while a LIVE op is expanded, refresh its run log
        // each poll tick so new events stream into the detail pane.
        // The fetch guard prevents overlap; finished ops are fetched
        // once on expand and not re-polled (their window is fixed).
        if (state.expandedOpIds.size) {
          state.expandedOpIds.forEach((id) => {
            const exp = state.ops.find((o) => o.op_id === id);
            if (exp && (exp.status === 'running' || exp.status === 'cancelling'
                        || exp.status === 'pending')) {
              fetchOpEvents(id);
            }
          });
        }
      }
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
    // v1.21.30: lock the page behind the drawer so there's only ONE
    // scrollbar (the drawer's), not two (page + drawer). On Windows /
    // classic-scrollbar systems the second scrollbar takes width and the
    // page shifts; scrollbar-gutter:stable on html keeps it from jumping
    // when the page scrollbar is suppressed.
    document.documentElement.classList.add('ops-drawer-locked');
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
    // v1.20.22: reset expand state so the drawer reopens collapsed.
    state.expandedOpIds.clear();
    document.documentElement.classList.remove('ops-drawer-locked');  // v1.21.30
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
          }
          // v1.22.44 (audit): ALWAYS reconcile. Pre-fix a failed cancel
          // (non-2xx — the op already went terminal → 404/409, or a 500)
          // reset the button with no poll + no message, so the user saw a
          // stale CANCEL button and no signal. The forced re-poll re-fetches
          // the real state: an already-ended op drops its card; a genuine
          // failure leaves a clickable button to retry.
          if (state.pollTimer) clearTimeout(state.pollTimer);
          state.pollTimer = setTimeout(poll, 200);
        });
        return;
      }
      // v1.20.32: bulk-cancel every PENDING job of this queue's type.
      // Native confirm() blocks the event loop so the 1s drawer poll
      // can't re-render the button out from under a two-click confirm.
      const bulkCancel = e.target.closest('[data-bulk-cancel]');
      if (bulkCancel) {
        e.preventDefault();
        const jobType = bulkCancel.getAttribute('data-bulk-cancel');
        if (!window.confirm(
          `Cancel ALL pending ${jobType} jobs? The one job currently `
          + `running will finish on its own; everything still queued is `
          + `dropped (you can re-queue it later).`)) return;
        bulkCancel.disabled = true;
        bulkCancel.textContent = '// CANCELLING…';
        postBulkCancel(jobType).then((ok) => {
          if (!ok) {
            bulkCancel.disabled = false;
            bulkCancel.textContent = '// CANCEL ALL PENDING';
          }
          // v1.22.44 (audit): always reconcile so a failed bulk-cancel
          // isn't silent — see the per-op cancel handler above.
          if (state.pollTimer) clearTimeout(state.pollTimer);
          state.pollTimer = setTimeout(poll, 200);
        });
        return;
      }
      // v1.20.22: expand/collapse a card's in-depth run view. The
      // explicit ✕ collapses; clicking the card body toggles. Skip
      // when an interactive child (cancel, link) was the target.
      const collapse = e.target.closest('[data-op-collapse]');
      if (collapse) {
        toggleExpand(collapse.getAttribute('data-op-collapse'), true);
        return;
      }
      // Clicks INSIDE the expanded run log (reading/scrolling it) must
      // not collapse the card — only the ✕ (handled above) collapses.
      if (e.target.closest('.op-card-detail')) return;
      if (e.target.closest('[data-op-cancel], [data-bulk-cancel], a')) return;
      const expCard = e.target.closest('.op-card-expandable[data-op-id]');
      if (expCard) toggleExpand(expCard.getAttribute('data-op-id'));
    });
    // ESC collapses an expanded card first, else closes the drawer.
    document.addEventListener('keydown', (e) => {
      if (e.key !== 'Escape' || !state.drawerOpen) return;
      if (state.expandedOpIds.size) {
        // v1.21.28: first ESC collapses ALL open cards, second closes.
        state.expandedOpIds.clear();
        renderDrawerBody(state.ops);
      } else {
        closeDrawer();
      }
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
  // v1.19.50: poll /api/progress for a specific op until it
  // reaches a terminal state, then resolve with the final row.
  // Used by the cloud-themes-backup click handlers to surface
  // 0-eligible-target feedback (the user's repro: clicked
  // DOWNLOAD PLEX BACKUP on a non-C1 row → ops drawer showed
  // 'completed: 0 backed up' but the row didn't change, and
  // without an explicit alert the user couldn't tell what
  // happened). Returns null on timeout. Polls at 1.5s; the
  // built-in poll() is on a 1s cadence when ops are running,
  // so this introduces no additional load — it just gives the
  // caller a deterministic completion signal.
  async function waitForOp(opId, { timeoutMs = 30000 } = {}) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      try {
        const res = await fetch('/api/progress', { credentials: 'same-origin' });
        if (res.ok) {
          const body = await res.json();
          const op = (body.ops || []).find((o) => o.op_id === opId);
          if (op && op.status !== 'running' && op.status !== 'pending'
              && op.status !== 'cancelling') {
            return op;
          }
        }
      } catch (_) {
        // Transient fetch failure — try again on the next tick.
      }
      await new Promise((r) => setTimeout(r, 1500));
    }
    return null;
  }

  window.motifOps = {
    init,
    open: openDrawer,
    close: closeDrawer,
    refresh: poll,
    boostPoll,
    setOptimisticPlaceholder,
    clearOptimisticPlaceholder,
    hasOptimistic,
    waitForOp,
    state: () => state,
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
