/* v1.13.48: dashboard customize — drag-and-drop reorder + hide/show.
 *
 * Persists in runtime_settings.dashboard_layout via /api/dashboard/layout.
 * Layout shape:
 *   { sections: [{ id, hidden, cards?: [{ id, hidden }, ...] }, ...] }
 *
 * `id` matches each section's data-dash-section attribute. Sections in
 * the DOM but not in the saved layout get appended to the end with
 * default visibility (so newly-shipped sections show up automatically).
 * Sections in the layout but no longer in the DOM are dropped.
 *
 * v1.15.32: optional `cards` array per section. Each card carries a
 * stable global `id` (matches data-dash-card on the <article>). Cards
 * may live in any grid section — the layout records both the
 * section→card assignment AND the order within that section. the user:
 * "could reorder and put user provided next to hardlinked or the row
 * above if I wanted." On apply, cards are MOVED (not just reordered)
 * to whichever section their layout entry sits under, then ordered
 * per the section's `cards` array.
 *
 * Section visibility has two axes that don't conflict:
 *   - inline style="display:none"  → set by hydrators when no data
 *   - .dash-user-hidden            → set by this module when user toggles off
 * Card visibility uses .dash-user-hidden-card (separate axis from
 * the section's class so the same article element can be card-hidden
 * AND live in a section-hidden parent without ambiguity).
 * Customize mode forces both visible (so the user can edit) via CSS.
 */
(function () {
  'use strict';

  // Only run on the dashboard. Bail elsewhere — the // CUSTOMIZE
  // button + #dash-sections container only exist on /.
  function dashContainer() {
    return document.getElementById('dash-sections');
  }
  function customizeBtn() {
    return document.getElementById('dash-customize-btn');
  }

  let LAYOUT = { sections: [] };
  let CUSTOMIZE = false;
  let SAVE_TIMER = null;

  // ── network ──────────────────────────────────────────────────────
  async function fetchLayout() {
    try {
      const r = await fetch('/api/dashboard/layout', {
        credentials: 'same-origin',
      });
      if (!r.ok) return { sections: [] };
      const data = await r.json();
      return data && Array.isArray(data.sections)
        ? data : { sections: [] };
    } catch (_) {
      return { sections: [] };
    }
  }

  function scheduleSave() {
    if (SAVE_TIMER) clearTimeout(SAVE_TIMER);
    SAVE_TIMER = setTimeout(saveLayout, 300);
  }

  let _saveLayoutWarned = false;
  async function saveLayout() {
    // v1.24.78: cancel any armed debounce so a direct call (exitCustomize's
    // immediate flush) doesn't leave a pending setTimeout that fires a
    // redundant duplicate PUT ~300ms later. No-op when we ARE the timer.
    if (SAVE_TIMER) clearTimeout(SAVE_TIMER);
    SAVE_TIMER = null;
    try {
      const r = await fetch('/api/dashboard/layout', {
        method: 'PUT',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(LAYOUT),
      });
      // v1.21.43 (audit L5): fetch does NOT reject on a 4xx/5xx, so a
      // non-ok PUT (expired session → 401/403, server 500) used to slip
      // past the catch and SILENTLY lose the layout. The old "next save
      // retries" comment was fiction — saves only fire on the next edit
      // or on customize-mode exit, so a failed final flush is just lost.
      if (!r.ok) throw new Error('HTTP ' + r.status);
      _saveLayoutWarned = false;  // recovered
    } catch (e) {
      // Surface it: console.warn always (greppable), + a single alert per
      // failure streak so the user knows their layout isn't persisting
      // without spamming on every debounced autosave.
      console.warn('saveLayout failed — dashboard layout not persisted:', e);
      if (!_saveLayoutWarned) {
        _saveLayoutWarned = true;
        alert('Dashboard layout could not be saved ('
              + ((e && e.message) || e)
              + '). Your changes may not persist on reload.');
      }
    }
  }

  // v1.24.62: pinned sections (RECENTLY ADDED carousel, SERVICES) live OUTSIDE
  // #dash-sections so they keep a fixed top/bottom position. They're hideable
  // but not reorderable — a hide-only toggle that shares the same hidden-state
  // persistence as the sortable sections (the user: "the option to hide a section").
  function pinnedSections() {
    return document.querySelectorAll('[data-dash-section][data-dash-pinned]');
  }
  function isPinnedId(id) {
    return !!document.querySelector(
      `[data-dash-section="${cssEscape(id)}"][data-dash-pinned]`);
  }

  // v1.24.65: customizable per-library accent colors. Each drives a PLEX
  // dashboard card's left bar AND the active nav-tab underline (via the CSS
  // var). Defaults match the :root tokens (--amber/--blue/--magenta/--red);
  // edited in customize mode via the // LIBRARY COLORS panel, persisted in
  // localStorage 'motif:dashColors', applied pre-paint by the base.html head
  // script. // RESET COLORS clears the overrides → back to the token defaults.
  const DASH_COLORS_KEY = 'motif:dashColors';
  const DASH_COLORS = [
    { key: 'movies', cssVar: '--dash-movies-color', label: 'MOVIES', def: '#ffb84a' },
    { key: 'tv', cssVar: '--dash-tv-color', label: 'TV', def: '#6d8fff' },
    { key: 'anime', cssVar: '--dash-anime-color', label: 'ANIME', def: '#ff7ad6' },
    { key: 'collections', cssVar: '--dash-collections-color', label: 'COLLECTIONS', def: '#ff6b6b' },
  ];
  function readDashColors() {
    try { return JSON.parse(localStorage.getItem(DASH_COLORS_KEY) || '{}') || {}; }
    catch (_) { return {}; }
  }
  function saveDashColors(obj) {
    try { localStorage.setItem(DASH_COLORS_KEY, JSON.stringify(obj)); }
    catch (_) { /* localStorage full / disabled */ }
  }
  function isHex(v) { return /^#[0-9a-fA-F]{6}$/.test(v || ''); }
  // Re-apply on the dashboard too (the base.html head script already ran, but
  // this keeps the module self-contained + correct if colors changed this tab).
  function applyDashColors() {
    const saved = readDashColors();
    const r = document.documentElement;
    DASH_COLORS.forEach((c) => {
      if (isHex(saved[c.key])) r.style.setProperty(c.cssVar, saved[c.key]);
      else r.style.removeProperty(c.cssVar);
    });
  }

  function injectColorPanel() {
    const plex = document.querySelector('[data-dash-section="plex-coverage"]');
    if (!plex || document.getElementById('dash-color-panel')) return;
    const saved = readDashColors();
    const panel = document.createElement('div');
    panel.id = 'dash-color-panel';
    panel.className = 'dash-color-panel';
    panel.innerHTML =
      '<span class="dash-color-panel-title">// LIBRARY COLORS</span>'
      + DASH_COLORS.map((c) => {
        const val = isHex(saved[c.key]) ? saved[c.key] : c.def;
        return `<label class="dash-color-item"><input type="color"`
          + ` data-dash-color="${escAttr(c.key)}" value="${escAttr(val)}">`
          + `<span>${escHtml(c.label)}</span></label>`;
      }).join('')
      + '<button type="button" class="dash-section-toggle dash-color-reset"'
      + ' id="dash-color-reset" title="Reset all library colors to defaults">'
      + '// RESET COLORS</button>';
    plex.parentNode.insertBefore(panel, plex);
    panel.addEventListener('input', onColorInput);
    panel.querySelector('#dash-color-reset')
      .addEventListener('click', onColorReset);
  }

  function removeColorPanel() {
    const panel = document.getElementById('dash-color-panel');
    if (panel) panel.remove();
  }

  // v1.24.77: the // LIBRARY COLORS panel is a standalone sibling injected
  // before the plex-coverage section — reorders move sections but not the
  // panel, detaching it from the PLEX cards it controls (the user). Re-pin it
  // immediately before the plex section after every reorder.
  function repositionColorPanel() {
    const panel = document.getElementById('dash-color-panel');
    const plex = document.querySelector('[data-dash-section="plex-coverage"]');
    if (panel && plex && plex.previousElementSibling !== panel) {
      plex.parentNode.insertBefore(panel, plex);
    }
  }

  function onColorInput(ev) {
    const inp = ev.target.closest('[data-dash-color]');
    if (!inp || !isHex(inp.value)) return;
    const key = inp.getAttribute('data-dash-color');
    const cfg = DASH_COLORS.find((c) => c.key === key);
    if (!cfg) return;
    document.documentElement.style.setProperty(cfg.cssVar, inp.value);
    const saved = readDashColors();
    saved[key] = inp.value;
    saveDashColors(saved);
  }

  function onColorReset() {
    const r = document.documentElement;
    DASH_COLORS.forEach((c) => {
      r.style.removeProperty(c.cssVar);  // fall back to the :root token default
      const inp = document.querySelector(`[data-dash-color="${cssEscape(c.key)}"]`);
      if (inp) inp.value = c.def;
    });
    try { localStorage.removeItem(DASH_COLORS_KEY); } catch (_) { /* ignore */ }
  }

  // ── layout apply ─────────────────────────────────────────────────
  // Reorder DOM children of #dash-sections to match LAYOUT.sections,
  // appending any unsaved sections at the end. Hidden sections get
  // the .dash-user-hidden class so CSS hides them outside customize
  // mode (and dims them inside).
  // v1.15.32: also reorder/migrate cards per LAYOUT.sections[*].cards.
  function applyLayout() {
    const container = dashContainer();
    if (!container) return;
    // Build a map of existing section nodes by id.
    const present = new Map();
    container.querySelectorAll('[data-dash-section]').forEach((el) => {
      const id = el.getAttribute('data-dash-section');
      if (id) present.set(id, el);
    });
    // Reorder per saved layout.
    const seen = new Set();
    for (const entry of LAYOUT.sections) {
      const node = present.get(entry.id);
      if (!node) continue;
      seen.add(entry.id);
      container.appendChild(node);
      node.classList.toggle('dash-user-hidden', !!entry.hidden);
    }
    // Append any sections not in the saved layout (new dashboard
    // additions get default-visible state and land at the end).
    for (const [id, node] of present) {
      if (seen.has(id)) continue;
      container.appendChild(node);
      node.classList.remove('dash-user-hidden');
      LAYOUT.sections.push({ id, hidden: false });
    }
    // Drop layout entries whose section no longer exists in the DOM.
    // v1.24.62: keep pinned-section entries (carousel, services) — they live
    // OUTSIDE #dash-sections so `present` doesn't include them.
    LAYOUT.sections = LAYOUT.sections.filter(
      (e) => present.has(e.id) || isPinnedId(e.id),
    );
    // v1.24.62: apply the hidden flag to pinned sections. They keep their fixed
    // position (not reordered — they're outside the sortable container) but can
    // be hidden like any other section, sharing the same persistence.
    pinnedSections().forEach((node) => {
      const id = node.getAttribute('data-dash-section');
      if (!id) return;
      const entry = LAYOUT.sections.find((e) => e.id === id);
      if (entry) {
        node.classList.toggle('dash-user-hidden', !!entry.hidden);
      } else {
        node.classList.remove('dash-user-hidden');
        LAYOUT.sections.push({ id, hidden: false });
      }
    });
    // v1.15.32: card-level apply. Build a map of every card in the
    // dashboard by id (cards may have moved sections, so search the
    // whole container, not just per-section). Then for each section
    // in the saved layout, move its `cards` entries into that section
    // in the saved order, applying the per-card hidden flag. Any
    // cards in the DOM but not in any section's saved layout stay
    // where the template put them (default placement) and get
    // appended to that section's layout entry.
    const allCards = new Map();
    container.querySelectorAll('[data-dash-card]').forEach((el) => {
      const cid = el.getAttribute('data-dash-card');
      if (cid) allCards.set(cid, el);
    });
    const seenCardIds = new Set();
    for (const entry of LAYOUT.sections) {
      if (!Array.isArray(entry.cards) || entry.cards.length === 0) continue;
      const sectionNode = present.get(entry.id);
      if (!sectionNode) continue;
      // Filter cards to those still present in the DOM. Apply in
      // saved order — appendChild moves the node, so successive
      // appends produce the saved sequence.
      const validCards = entry.cards.filter((c) => allCards.has(c.id));
      entry.cards = validCards;  // drop stale ids
      for (const card of validCards) {
        const node = allCards.get(card.id);
        sectionNode.appendChild(node);
        node.classList.toggle('dash-user-hidden-card', !!card.hidden);
        seenCardIds.add(card.id);
      }
    }
    // Cards present in the DOM but missing from every section's
    // saved layout: register them under their CURRENT section
    // (the template default) so a future save captures their
    // position. Don't move them — the template order is the
    // baseline.
    for (const [cid, node] of allCards) {
      if (seenCardIds.has(cid)) continue;
      const parentSection = node.closest('[data-dash-section]');
      if (!parentSection) continue;
      const sid = parentSection.getAttribute('data-dash-section');
      const entry = LAYOUT.sections.find((e) => e.id === sid);
      if (!entry) continue;
      if (!Array.isArray(entry.cards)) entry.cards = [];
      entry.cards.push({ id: cid, hidden: false });
      node.classList.remove('dash-user-hidden-card');
    }
  }

  // ── customize mode ───────────────────────────────────────────────
  // v1.13.62: customize trigger moved from a button to a muted text
  // link in the dash hero description. Label-toggling now uses
  // lowercase to match the new "// customize layout" copy; entering
  // customize mode swaps to "// done editing" so the affordance to
  // exit is obvious without dragging the eye.
  function enterCustomize() {
    CUSTOMIZE = true;
    document.body.classList.add('dash-customize-mode');
    const btn = customizeBtn();
    if (btn) btn.textContent = '// done editing';
    injectControls();
    injectPinnedControls();  // v1.24.62
    injectColorPanel();      // v1.24.65
  }

  function exitCustomize() {
    CUSTOMIZE = false;
    document.body.classList.remove('dash-customize-mode');
    const btn = customizeBtn();
    if (btn) btn.textContent = '// customize layout';
    removeControls();
    removePinnedControls();  // v1.24.62
    removeColorPanel();      // v1.24.65
    saveLayout();  // flush any pending changes immediately
  }

  // v1.24.62: pinned-section controls — a hide-only toggle (no drag handle / no
  // reorder arrows; pinned sections keep their fixed position). The toggle is
  // delegated on `document` because pinned sections live outside the
  // #dash-sections container that the main delegated listener is bound to.
  function injectPinnedControls() {
    pinnedSections().forEach((section) => {
      if (section.querySelector(':scope > .dash-section-controls')) return;
      const id = section.getAttribute('data-dash-section');
      const label = section.getAttribute('data-dash-label') || id;
      const hidden = section.classList.contains('dash-user-hidden');
      const controls = document.createElement('div');
      controls.className = 'dash-section-controls dash-section-controls-pinned';
      controls.innerHTML = `
        <span class="dash-section-name">${escHtml(label)}</span>
        <button type="button" class="dash-section-toggle"
                data-dash-pinned-toggle="${escAttr(id)}"
                title="${hidden ? 'Show this section' : 'Hide this section'}">
          ${hidden ? '// SHOW' : '// HIDE'}
        </button>`;
      section.insertBefore(controls, section.firstChild);
    });
    document.addEventListener('click', onPinnedToggleClick);
  }

  function removePinnedControls() {
    pinnedSections().forEach((section) => {
      const ctrl = section.querySelector(
        ':scope > .dash-section-controls-pinned');
      if (ctrl) ctrl.remove();
    });
    document.removeEventListener('click', onPinnedToggleClick);
  }

  function onPinnedToggleClick(ev) {
    const btn = ev.target.closest('[data-dash-pinned-toggle]');
    if (!btn) return;
    ev.preventDefault();
    ev.stopPropagation();
    const id = btn.getAttribute('data-dash-pinned-toggle');
    const section = document.querySelector(
      `[data-dash-section="${cssEscape(id)}"][data-dash-pinned]`);
    if (!section) return;
    const nowHidden = !section.classList.contains('dash-user-hidden');
    section.classList.toggle('dash-user-hidden', nowHidden);
    btn.textContent = nowHidden ? '// SHOW' : '// HIDE';
    btn.title = nowHidden ? 'Show this section' : 'Hide this section';
    const entry = LAYOUT.sections.find((e) => e.id === id);
    if (entry) entry.hidden = nowHidden;
    else LAYOUT.sections.push({ id, hidden: nowHidden });
    scheduleSave();
  }

  function injectControls() {
    const container = dashContainer();
    if (!container) return;
    container.querySelectorAll('[data-dash-section]').forEach((section) => {
      if (section.querySelector(':scope > .dash-section-controls')) return;
      const id = section.getAttribute('data-dash-section');
      const label = section.getAttribute('data-dash-label') || id;
      const hidden = section.classList.contains('dash-user-hidden');
      const controls = document.createElement('div');
      controls.className = 'dash-section-controls';
      // v1.13.64: ▲/▼ arrow buttons added alongside the drag handle so
      // touchscreen users (HTML5 native dnd doesn't fire on touch)
      // can reorder via taps. Drag stays the primary affordance on
      // desktop; arrows are the universal fallback. data-dash-move
      // direction = 'up' | 'down' steps the section one slot in
      // that direction within #dash-sections.
      controls.innerHTML = `
        <span class="dash-section-handle" title="Drag to reorder">⋮⋮</span>
        <button type="button" class="dash-section-arrow"
                data-dash-move="up" data-dash-section-id="${escAttr(id)}"
                title="Move up">▲</button>
        <button type="button" class="dash-section-arrow"
                data-dash-move="down" data-dash-section-id="${escAttr(id)}"
                title="Move down">▼</button>
        <span class="dash-section-name">${escHtml(label)}</span>
        <button type="button" class="dash-section-toggle"
                data-dash-toggle="${escAttr(id)}"
                title="${hidden ? 'Show this section' : 'Hide this section'}">
          ${hidden ? '// SHOW' : '// HIDE'}
        </button>`;
      // Drag handle lives on the section itself for native HTML5 dnd
      // (draggable attr can't be scoped to a child without re-routing
      // the event). Header acts as the visual handle.
      section.setAttribute('draggable', 'true');
      section.insertBefore(controls, section.firstChild);
    });
    // v1.15.32: per-card controls. Each <article data-dash-card>
    // inside a grid section gets a small bar with ◀ ▶ + // HIDE.
    // ◀ moves the card one slot left (across section boundaries —
    // the leftmost card in a section walks back to the end of the
    // previous grid section); ▶ symmetrical. Cards behave like
    // text reflow across rows.
    container.querySelectorAll('[data-dash-card]').forEach((card) => {
      if (card.querySelector(':scope > .dash-card-controls')) return;
      const cid = card.getAttribute('data-dash-card');
      const cardHidden = card.classList.contains('dash-user-hidden-card');
      const ctrl = document.createElement('div');
      ctrl.className = 'dash-card-controls';
      ctrl.innerHTML = `
        <span style="display:flex;gap:var(--gap-1);">
          <button type="button" class="dash-card-arrow"
                  data-dash-card-move="prev" data-dash-card-id="${escAttr(cid)}"
                  title="Move card to previous slot (across rows)">◀</button>
          <button type="button" class="dash-card-arrow"
                  data-dash-card-move="next" data-dash-card-id="${escAttr(cid)}"
                  title="Move card to next slot (across rows)">▶</button>
        </span>
        <button type="button" class="dash-card-toggle"
                data-dash-card-toggle="${escAttr(cid)}"
                title="${cardHidden ? 'Show this card' : 'Hide this card'}">
          ${cardHidden ? '// SHOW' : '// HIDE'}
        </button>`;
      card.insertBefore(ctrl, card.firstChild);
    });
    // Wire dnd + toggle events. Single delegated listener on the
    // container so we don't have to re-bind per controls injection.
    container.addEventListener('click', onToggleClick);
    container.addEventListener('dragstart', onDragStart);
    container.addEventListener('dragover', onDragOver);
    container.addEventListener('dragleave', onDragLeave);
    container.addEventListener('drop', onDrop);
    container.addEventListener('dragend', onDragEnd);
  }

  function removeControls() {
    const container = dashContainer();
    if (!container) return;
    container.querySelectorAll('[data-dash-section]').forEach((section) => {
      const ctrl = section.querySelector(':scope > .dash-section-controls');
      if (ctrl) ctrl.remove();
      section.removeAttribute('draggable');
      section.classList.remove('dash-drag-over');
    });
    // v1.15.32: tear down per-card controls too.
    container.querySelectorAll('[data-dash-card]').forEach((card) => {
      const ctrl = card.querySelector(':scope > .dash-card-controls');
      if (ctrl) ctrl.remove();
    });
    container.removeEventListener('click', onToggleClick);
    container.removeEventListener('dragstart', onDragStart);
    container.removeEventListener('dragover', onDragOver);
    container.removeEventListener('dragleave', onDragLeave);
    container.removeEventListener('drop', onDrop);
    container.removeEventListener('dragend', onDragEnd);
  }

  // ── hide/show + arrow reorder ────────────────────────────────────
  // v1.13.64: single delegated click handler covers both
  // [data-dash-toggle] (hide/show) and [data-dash-move] (arrow
  // reorder). Arrows are the touchscreen fallback for HTML5 dnd —
  // tap ▲ to step the section up one slot, ▼ to step down. Saves
  // debounced via scheduleSave so a flurry of arrow taps coalesces
  // into one PUT.
  // v1.15.32: same delegated handler also dispatches per-card
  // controls — [data-dash-card-move] (◀ ▶ cross-section reorder)
  // and [data-dash-card-toggle] (// HIDE per card).
  function onToggleClick(ev) {
    const cardMoveBtn = ev.target.closest('[data-dash-card-move]');
    if (cardMoveBtn) {
      ev.stopPropagation();
      ev.preventDefault();
      onCardArrowMove(ev.currentTarget, cardMoveBtn);
      return;
    }
    const cardToggleBtn = ev.target.closest('[data-dash-card-toggle]');
    if (cardToggleBtn) {
      ev.stopPropagation();
      onCardToggle(ev.currentTarget, cardToggleBtn);
      return;
    }
    const moveBtn = ev.target.closest('[data-dash-move]');
    if (moveBtn) {
      ev.stopPropagation();
      ev.preventDefault();
      onArrowMove(ev.currentTarget, moveBtn);
      return;
    }
    const btn = ev.target.closest('[data-dash-toggle]');
    if (!btn) return;
    ev.stopPropagation();
    const id = btn.getAttribute('data-dash-toggle');
    const section = ev.currentTarget.querySelector(
      `[data-dash-section="${cssEscape(id)}"]`);
    if (!section) return;
    const nowHidden = !section.classList.contains('dash-user-hidden');
    section.classList.toggle('dash-user-hidden', nowHidden);
    btn.textContent = nowHidden ? '// SHOW' : '// HIDE';
    btn.title = nowHidden ? 'Show this section' : 'Hide this section';
    // Mirror to LAYOUT and persist.
    const entry = LAYOUT.sections.find((e) => e.id === id);
    if (entry) entry.hidden = nowHidden;
    else LAYOUT.sections.push({ id, hidden: nowHidden });
    scheduleSave();
  }

  function onArrowMove(container, moveBtn) {
    const direction = moveBtn.getAttribute('data-dash-move');
    const id = moveBtn.getAttribute('data-dash-section-id');
    const section = container.querySelector(
      `[data-dash-section="${cssEscape(id)}"]`);
    if (!section) return;
    // v1.24.77: walk PAST non-section siblings — the // LIBRARY COLORS panel is
    // a foreign node pinned before the plex section, so a single-step
    // previousElementSibling/nextElementSibling check would dead-button the
    // plex ▲ (and the above-section's ▼) when the panel is the neighbor.
    const adjacentSection = (el, dir) => {
      let n = dir === 'up' ? el.previousElementSibling : el.nextElementSibling;
      while (n && !n.matches('[data-dash-section]')) {
        n = dir === 'up' ? n.previousElementSibling : n.nextElementSibling;
      }
      return n;
    };
    if (direction === 'up') {
      const prev = adjacentSection(section, 'up');
      if (prev) container.insertBefore(section, prev);
    } else if (direction === 'down') {
      const next = adjacentSection(section, 'down');
      if (next) container.insertBefore(next, section);
    }
    rebuildLayoutFromDOM();
    scheduleSave();
  }

  // v1.15.32: card-level cross-section reorder.
  //
  // ◀ moves the card one slot to the left within its current
  //   section. If it's already the leftmost card in the section,
  //   it walks back to the END of the previous grid section
  //   (the previous [data-dash-section] that has at least one
  //   [data-dash-card] child).
  //
  // ▶ symmetrical — rightmost card walks forward to the START
  //   of the next grid section.
  //
  // This gives the user the "cards reflow across rows" feel
  // the user asked for: "reorder and put user provided next to
  // hardlinked or the row above if I wanted."
  function onCardArrowMove(container, moveBtn) {
    const direction = moveBtn.getAttribute('data-dash-card-move');
    const cid = moveBtn.getAttribute('data-dash-card-id');
    const card = container.querySelector(
      `[data-dash-card="${cssEscape(cid)}"]`);
    if (!card) return;
    const currentSection = card.closest('[data-dash-section]');
    if (!currentSection) return;
    if (direction === 'prev') {
      const prevSibling = card.previousElementSibling
        && card.previousElementSibling.matches('[data-dash-card]')
        ? card.previousElementSibling : null;
      if (prevSibling) {
        currentSection.insertBefore(card, prevSibling);
      } else {
        // Walk to the end of the previous grid section.
        const targetSection = previousGridSection(currentSection);
        if (targetSection) targetSection.appendChild(card);
      }
    } else if (direction === 'next') {
      const nextSibling = card.nextElementSibling
        && card.nextElementSibling.matches('[data-dash-card]')
        ? card.nextElementSibling : null;
      if (nextSibling) {
        // Insert AFTER nextSibling (i.e. before nextSibling.next).
        currentSection.insertBefore(card, nextSibling.nextSibling);
      } else {
        // Walk to the start of the next grid section.
        const targetSection = nextGridSection(currentSection);
        if (targetSection) {
          const firstCard = targetSection.querySelector(':scope > [data-dash-card]');
          if (firstCard) targetSection.insertBefore(card, firstCard);
          else targetSection.appendChild(card);
        }
      }
    }
    rebuildLayoutFromDOM();
    scheduleSave();
  }

  function previousGridSection(section) {
    let cur = section.previousElementSibling;
    while (cur) {
      if (cur.matches('[data-dash-section]')
          && cur.querySelector(':scope > [data-dash-card]')) return cur;
      cur = cur.previousElementSibling;
    }
    return null;
  }

  function nextGridSection(section) {
    let cur = section.nextElementSibling;
    while (cur) {
      if (cur.matches('[data-dash-section]')
          && cur.querySelector(':scope > [data-dash-card]')) return cur;
      cur = cur.nextElementSibling;
    }
    return null;
  }

  function onCardToggle(container, toggleBtn) {
    const cid = toggleBtn.getAttribute('data-dash-card-toggle');
    const card = container.querySelector(
      `[data-dash-card="${cssEscape(cid)}"]`);
    if (!card) return;
    const nowHidden = !card.classList.contains('dash-user-hidden-card');
    card.classList.toggle('dash-user-hidden-card', nowHidden);
    toggleBtn.textContent = nowHidden ? '// SHOW' : '// HIDE';
    toggleBtn.title = nowHidden ? 'Show this card' : 'Hide this card';
    rebuildLayoutFromDOM();
    scheduleSave();
  }

  // ── drag and drop ────────────────────────────────────────────────
  let DRAGGING = null;

  function onDragStart(ev) {
    const section = ev.target.closest('[data-dash-section]');
    if (!section || !section.parentElement
        || section.parentElement.id !== 'dash-sections') return;
    DRAGGING = section;
    section.classList.add('dash-dragging');
    // Firefox needs a payload for the drag to start; the actual
    // value is unused — we read DRAGGING from module state.
    if (ev.dataTransfer) {
      ev.dataTransfer.effectAllowed = 'move';
      try { ev.dataTransfer.setData('text/plain', section.getAttribute('data-dash-section') || ''); }
      catch (_) { /* IE / quirky envs */ }
    }
  }

  function onDragOver(ev) {
    if (!DRAGGING) return;
    const target = ev.target.closest('[data-dash-section]');
    if (!target || target === DRAGGING) return;
    ev.preventDefault();  // permit drop
    if (ev.dataTransfer) ev.dataTransfer.dropEffect = 'move';
    // Mark the hovered section so CSS can render an indicator line.
    document.querySelectorAll('.dash-drag-over').forEach((el) => {
      if (el !== target) el.classList.remove('dash-drag-over');
    });
    target.classList.add('dash-drag-over');
  }

  function onDragLeave(ev) {
    const target = ev.target.closest('[data-dash-section]');
    if (target) target.classList.remove('dash-drag-over');
  }

  function onDrop(ev) {
    if (!DRAGGING) return;
    const target = ev.target.closest('[data-dash-section]');
    if (!target || target === DRAGGING) {
      cleanupDrag();
      return;
    }
    ev.preventDefault();
    // Decide before-vs-after based on cursor position relative to
    // the target's vertical midpoint. Feels more natural than always
    // inserting before.
    const rect = target.getBoundingClientRect();
    const before = ev.clientY < rect.top + rect.height / 2;
    if (before) target.parentElement.insertBefore(DRAGGING, target);
    else target.parentElement.insertBefore(DRAGGING, target.nextSibling);
    rebuildLayoutFromDOM();
    scheduleSave();
    cleanupDrag();
  }

  function onDragEnd() {
    cleanupDrag();
  }

  function cleanupDrag() {
    if (DRAGGING) {
      DRAGGING.classList.remove('dash-dragging');
      DRAGGING = null;
    }
    document.querySelectorAll('.dash-drag-over').forEach(
      (el) => el.classList.remove('dash-drag-over'));
  }

  function rebuildLayoutFromDOM() {
    const container = dashContainer();
    if (!container) return;
    // Preserve existing hidden flags; just rewrite the order to
    // match the current DOM.
    const hiddenById = new Map();
    LAYOUT.sections.forEach((e) => hiddenById.set(e.id, !!e.hidden));
    // v1.24.62: pinned-section entries (carousel, services) live outside the
    // container, so the container-only rebuild below would drop them. Capture
    // them and re-append after.
    const pinnedEntries = LAYOUT.sections.filter((e) => isPinnedId(e.id));
    LAYOUT.sections = [];
    container.querySelectorAll(':scope > [data-dash-section]').forEach((el) => {
      const id = el.getAttribute('data-dash-section');
      if (!id) return;
      const entry = {
        id,
        hidden: hiddenById.has(id)
          ? hiddenById.get(id)
          : el.classList.contains('dash-user-hidden'),
      };
      // v1.15.32: capture per-section card order + hidden state.
      // Walk the section's [data-dash-card] descendants in DOM
      // order. Cards that have been moved cross-section land in
      // their new section's `cards` list.
      const cards = [];
      el.querySelectorAll(':scope > [data-dash-card]').forEach((cardEl) => {
        const cid = cardEl.getAttribute('data-dash-card');
        if (!cid) return;
        cards.push({
          id: cid,
          hidden: cardEl.classList.contains('dash-user-hidden-card'),
        });
      });
      if (cards.length > 0) entry.cards = cards;
      LAYOUT.sections.push(entry);
    });
    // v1.24.62: re-append preserved pinned entries (deduped by id).
    pinnedEntries.forEach((e) => {
      if (!LAYOUT.sections.some((x) => x.id === e.id)) LAYOUT.sections.push(e);
    });
    repositionColorPanel();  // v1.24.77: keep colors pinned to the plex section
  }

  // ── helpers ──────────────────────────────────────────────────────
  function escHtml(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;',
         '"': '&quot;', "'": '&#39;' }[c]));
  }
  function escAttr(s) { return escHtml(s); }
  function cssEscape(s) {
    if (window.CSS && CSS.escape) return CSS.escape(s);
    return String(s).replace(/[^a-zA-Z0-9_-]/g, '\\$&');
  }

  // ── boot ─────────────────────────────────────────────────────────
  async function init() {
    if (!dashContainer()) return;  // not on dashboard
    applyDashColors();  // v1.24.65 (the head script already ran; keep in sync)
    LAYOUT = await fetchLayout();
    applyLayout();
    const btn = customizeBtn();
    if (btn) {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        if (CUSTOMIZE) exitCustomize();
        else enterCustomize();
      });
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
