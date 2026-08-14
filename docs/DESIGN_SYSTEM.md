# motif Design System

> **Status:** v1 extraction from existing CSS (2026-05-17 under the user's
> v1.15.87 "design system discipline" rule). Pre-existing tokens and
> primitive families are documented as-is; known gaps are flagged
> under § Gaps so future UI work can close them properly instead of
> patching around them.

## 0. Stack adaptation

The "single source of truth for design tokens" lives in CSS, not
TypeScript:

| Concept             | Where it lives                                  |
|---------------------|-------------------------------------------------|
| Design tokens       | `:root { --foo: ... }` block in `app/web/static/app.css` (lines 4–54) |
| Component primitives| CSS classes in `app.css` (~149 rule families)   |
| Reference screens   | `app/web/templates/*.html` (Jinja) + JS render paths in `app/web/static/app.js` |
| Component library   | None as a separate directory — primitives are classes consumed by both Jinja and JS template literals |
| Build pipeline      | None — CSS is static and served as-is           |

There is no React, no Tailwind, no PostCSS, no design-tokens TS
file. **Treat `:root` + the primitive class blocks in `app.css`
as the canonical system.**

## 1. Tokens

All values live in `:root` at the top of `app.css`. Any UI change
that needs a color, size, font, radius, or shadow must reference
these — **hardcoded hex / px outside `:root` is forbidden.**

### Color tokens

```
Background layers:  --bg (#0a0d0c)  --bg-elev (#111614)  --bg-elev-2 (#161e1b)
Lines / borders:    --line (#1c2622)  --line-bright (#2a3833)
Foreground text:    --fg (#c8e8d4)  --fg-dim (#6b8779)  --fg-mute (#4a5e54)

Primary (motif identity):
  --green        #6dffb5   default action / default button border-color
  --green-bright #9affc9   hover state, THEMERRDB tone (T pill, sync button)
  --green-deep   #2d8c5c   button border at rest

Semantic / state colors:
  --amber  / --amber-bright  #ffb84a / #ffd47a   Plex tone, P link-badge, busy states
  --orange #ff7a3a                                HL link-pill (distinct from amber)
  --red    #ff6b6b                                FAIL count, danger actions
  --brown / --brown-bright #c08552 / #d8a47a      Cookies-needed family (v1.15.43)

Source-attribution colors (T/A/U/M/P axis):
  T (ThemerrDB)     --green-bright
  A (Adopted)       --cyan        #6dd3ff
  U (User URL)      --violet      #c46dff
  M (Manual)        --red         #ff6b6b
  P (Plex-served)   --amber       #ffb84a

Aux:
  --blue    #6d8fff   ACCEPT UPDATE / informational
  --magenta #ff7ad6   EXPORT CSV / MARK ALIVE
```

The source-attribution palette is the most important consistency
contract — the SAME color must represent the SAME source across
every surface (row pill, dashboard donut slice, link-badge, button
variant). Pre-v1.15.71 there were divergences (e.g. v1.15.66
invented `.src-pill src-X` classes with no color rules); now the
canonical `.link-badge-*` family is the single source.

### Typography tokens

```
--font-mono     'JetBrains Mono', 'IBM Plex Mono', ui-monospace
--font-display  'VT323', 'Share Tech Mono', var(--font-mono)

Type scale:
  --t-tiny  11px   table cells, dropdowns, helper text
  --t-small 12px   buttons, chips, body small
  --t-base  13px   default body / inputs
  --t-med   15px   sub-headings
  --t-large 22px   section titles
  --t-huge  64px   the dashboard hero numerics
```

The body font is `var(--font-mono)` — the entire UI is monospace.
`var(--font-display)` (VT323) is reserved for hero numerics and
specific stylized labels.

### Radii, shadows, motion

```
--radius        2px           applied to nearly all rounded corners
--shadow-glow   0 0 12px rgba(109,255,181,0.08)   default glow
```

**Gap:** no motion tokens. Transition durations are written ad-hoc
(`0.12s`, `0.15s`, `0.2s`). See § Gaps.

**Gap:** no spacing scale. Paddings/margins use raw px values
clustering around 4/6/8/10/12/14/20px but there's no canonical
scale. See § Gaps.

## 2. Primitive families

### Buttons (`.btn`, `.btn-*`)

Base: `.btn` (line 268 of app.css). All buttons share the same
height/padding/font-weight/letter-spacing/text-transform; tone
variants override only color + border-color.

| Variant         | Tone     | Use                                        |
|-----------------|----------|--------------------------------------------|
| `.btn`          | green    | Default action (e.g. // SYNC THEMERRDB)    |
| `.btn-tiny`     | (size)   | Compact variant — 4/10 padding, t-tiny font |
| `.btn-warn`     | amber    | Warning action                              |
| `.btn-info`     | blue     | Informational action (ACCEPT UPDATE)       |
| `.btn-plex`     | amber    | Touches Plex's view (PUSH TO PLEX)         |
| `.btn-export`   | magenta  | Read-only data export (EXPORT CSV)         |
| `.btn-cookies`  | brown    | Cookies-needed recovery (FIX COOKIES)      |
| `.btn-magenta`  | magenta  | Operator-override (MARK URL ALIVE)         |
| `.btn-danger`   | red      | Destructive (REMOVE)                       |
| `.lib-source-*` | per-src  | Per-source-letter button variants in SOURCE menu |

Convention: button text is always prefixed with `// ` (e.g.
`// SYNC THEMERRDB`, `// EXPORT CSV`). Letter-spacing 0.15em,
uppercase, mono font.

### Inputs (`.input`, `.input-tiny`)

* `.input` — base text input (line 756). Padding 8/12, mono font, t-base.
* `.input-tiny` — **GAP: no CSS rule exists.** JS references this class on the import-preview action `<select>` but it inherits only `.input` styles. v1.15.87 worked around by adding a per-site padding override on `.col-import-action select`. Should be a real primitive. See § Gaps.

### Tables (`.table`, `.table-tight`, `.table-compact`)

* `.table` — base table (line 836). td padding 10/14, vertical-align middle, t-base font.
* `.table-tight` — smaller variant: td padding 6/10, t-tiny font. Used by the import-preview table.
* `.table-compact` — for dense dashboards.

Column-class conventions: `.col-state`, `.col-title`, `.col-imdb`, `.col-X` per data column. Each declares width + text-align. **Convention from the v1.15.71-87 cascade:** when sibling columns center content, equalize the *flanking budget* (column width minus content width), not the column widths themselves. See § Patterns.

### Pills, badges, chips

* `.chip` — filter chip. Used by the topbar filter row + library page filter chips.
* `.link-badge` + `.link-badge-X` — source-attribution pill (T/A/U/M/P palette). One canonical family across library, info card, and dashboard donut.
* `.state-pill` + `.state-pill-pending` — row state dots (DL/PL). Pulse-amber for in-flight.
* `.tdb-pill`, `.attn-pill`, `.op-pill` — semantic pills carrying status counts in the topbar.

### Form layout

* `.form-label`, `.form-label-row`, `.form-hint`, `.form-hint.block-intro` — vertical-stack labelled inputs.
* `.form-input-actions` — siblings to the input for action buttons + status text.
* `.form-checkbox` — checkbox row inside a form.

**Convention (updated v1.19.25):** all labelled inputs — including `<input type="file">` — wrap in `<label class="form-label">` (no `for=` attribute, the input is a direct child). v1.15.70/.73 originally banned `<label>` around file inputs because explicit `for=` semantics popped the picker on label-text click; v1.19.25 reverted because that footgun is avoided when the input is wrapped INSIDE the label (no `for=` attribute → label-text click still focuses the input but doesn't open the OS file dialog before the user has read the surrounding hint). Confirmed via test `test_v1_19_30_file_picker_styling_generalized.py::test_library_upload_file_input_carries_input_class` + behavioral check on the two file inputs in the codebase (`#import-csv-file`, `#upload-file`).

### Settings section header (v1.18.57)

Every `<section class="block tab-panel">` in `settings.html` follows the same three-part header shape:

```html
<section class="block tab-panel" data-panel="X" style="display:none">
  <header class="block-head">
    <h2 class="block-title">// SECTION TITLE</h2>
    <span class="muted small">one-line context phrase</span>   {# subtitle #}
  </header>
  <div class="block-body">
    <p class="form-hint block-intro">…</p>                      {# optional lead paragraph #}
    …
  </div>
</section>
```

* **`// PREFIX`** on the `<h2>` title — universal motif voice.
* **Muted subtitle** in the `.block-head` — one short noun phrase that contextualizes the section ("set the tempo for YouTube downloads", "drift between motif's tracking and Plex's actual theme state"). Audit (v1.18.57): 17 of 18 top-level settings sections carry one. The single exception is `// API TOKENS`, which replaces the subtitle slot with a `// NEW TOKEN` action button — a documented secondary pattern when the section has an obvious primary action.
* **`<p class="form-hint block-intro">`** as the first child of `.block-body` for sections that need narrative context (10 of 18). Skip it when the form fields are self-explanatory.
* **`<p class="form-subhead">`** for inline sub-section headers within a single block (5 sites — INTEGRATION layouts where multiple form-grids share one block).
* **`<hr class="form-hint-divider">`** for visual separation between two button rows within the same block (only used in DOWNLOADS → PROBE TDB URLS / REPROBE FAILURES today). Pairs with the v1.17.5 `.form-hint + .form-actions` adjacent-sibling rule that tightens spacing. Margins (`10px 0 6px` after v1.18.57) sit closer to the button above; pre-v1.18.57 the 18px top margin floated the divider orphaned in dead space.

Adding a new settings section: copy the template above, pick a one-line subtitle, decide whether the block needs a lead paragraph. The audit guard (`tests/test_v1_18_57_settings_design_audit.py`) walks every `.block tab-panel` section and verifies the header shape so future additions can't drift.

**Uniform reading measure (v1.22.35).** Every standard settings section reads as ONE left-aligned column of width `--measure-form` (720px). The lead paragraph (`.block-intro`), the form fields (`.form-grid`), and the save-action row (`.form-actions`) all share that measure — so the narrative text and the dashed save separator end at the same right edge as the form, instead of stair-stepping between a full-width intro and a 720px form (the user's "full page vs half page text"). Wide-content sections that own a table opt out by tagging `.block-body block-body-flush` (LIBRARY SECTIONS, API TOKENS): the table runs full-width, only the intro paragraph still narrows to the measure. Don't reintroduce inline `style="margin-top:…"` on `.form-actions` — the shared rules (`.form-hint + .form-actions`, `.form-actions + .form-hint`, `.tab-panel .form-grid > .form-actions:last-child`) cover the spacing cases; the IMPORT preview-then-apply flow is the one documented bespoke exception. Guard: `tests/test_v1_22_35_settings_measure.py`.

**Hybrid field layout + uniform rhythm (v1.22.55 — ground-up settings redesign).** the user's "the settings pages are all over the place" — per-tab spacing voids, buttons floating in different spots, squished half-text. Root cause + the three primitives that fix it:

* **The phantom-whitespace void.** `.block-body` carries `white-space: pre-wrap` (other consumers depend on it), so the SOURCE indentation between settings controls rendered as real vertical gaps that varied with how each block's Jinja happened to be indented. Fix: `.tab-panel .block-body` switches to `white-space: normal` + becomes a `display: flex; flex-direction: column; gap: var(--gap-5)` — the GAP owns ALL inter-child spacing, so every section breathes identically regardless of source indentation. Per-child `margin-top/bottom` are zeroed (`.tab-panel .block-body > *`) so nothing stacks on top of the gap. `.tab-panel .form-grid` is pinned to one density (`gap-5`) — the old tab-by-tab `form-grid` vs `form-grid-tight` split is gone for tab-panels (the `-tight` class still exists for the new-token DIALOG, a different surface).

* **Scalar fields → `.field-row` (label | control two-column).** A labelled scalar input (text / number / select / textarea / file) wraps in `<label class="field-row">` with two children: `<span class="field-name">LABEL` (fixed 200px column, hosts the label text + any `.muted small` context note like "used by REMOTE" + the `.form-env-badge`) and `<span class="field-control">` (the input + its `.form-input-actions` button row + the `.form-hint`). Collapses to a single column under 760px. This REPLACES the old `<label class="form-label"> > <div class="form-label-row"><span class="form-label-text">` stack — the input no longer sits below a full-width label, so short fields stop looking squished.

* **Checkbox stacks + prose stay full-width.** `.form-checkbox` toggles (AUTOMATION, NOTIFICATIONS → EVENTS — long hint paragraphs each) and bare narrative `.form-hint` keep `form-label` / full width — the hybrid rule. Don't force a checkbox into a `.field-row`.

* **Action surfaces → `.control-group` / `.control-row`.** The canonical surface for a section whose primary content is an action + live status (DRY-RUN, TVDB BRIDGE — and the model for any future one): `<div class="control-group">` holds a `<div class="control-row">` (label/status left via `.field-name`, buttons pushed right via `.control-actions { margin-left: auto }`) followed by `.form-hint` below. This retired the bespoke `.dry-run-state` family (which floated two column-stacked amber buttons far-right of a 22px display-font status) — dry-run status now renders as a standard `.pill` (`.pill` = LIVE, `.pill-warn` = DRY-RUN, `.pill-danger` = refresh-failed; `.pill-danger` is the red tone added this tag). Section-level single-button actions (PROBE TDB URLS, REPROBE FAILURES, ORPHAN SCAN, RUN PROBE, REPROBE PLEX THEMES) keep the existing left-aligned `.form-actions` row — already uniform, not touched.

Guards: `tests/test_v1_18_57_settings_design_audit.py` (header shape) + `tests/test_v1_22_35_settings_measure.py` (measure) both still pass against the new shape. When adding a settings field, reach for `.field-row` for scalars, `.form-checkbox` for toggles, `.control-group` for action+status surfaces — never reintroduce `.form-label-row` / `.dry-run-state` (retired) on a tab-panel.

**Grouped checkbox stacks + the compact grid (v0.51.263 — NOTIFICATIONS relayout).** A long flat run of `.form-checkbox` toggles reads as a wall; the fix is structure, not prose:

* **Group with `.form-subhead`** family headers inside the same `form-grid` (the v1.13.49 PLEX-panel pattern) — NOTIFICATIONS → EVENTS groups its 20 toggles under `// SYNC & BULK` / `// THEME LIFECYCLE` / `// AVAILABLE & ARRIVED` / `// LOSS & RECOVERY` / `// SYSTEM HEALTH`.
* **Hints are ONE line** (≤ ~45 words, guarded by `test_v0_51_263_notifications_settings_layout.py`). The long-form WHY lives in PROJECT_HISTORY / tag comments, not settings prose. Pinned phrases (reason-branch titles, default rationales, action paths) survive — tests hold them.
* **Dependent toggles nest with `.form-checkbox-sub`** (v1.21.19) — the two sync fold-in toggles sit indented under SYNC COMPLETED instead of stating the dependency in prose.
* **`.form-grid-cols2`** — a two-column checkbox grid for toggle sets whose labels are self-explanatory (IN-APP INBOX: its ten kinds are named identically to their EVENTS twins, so they carry no per-kind hints). Collapses to one column ≤760px (the field-row breakpoint).

### Dialogs (`<dialog>`, `.dlg-close`)

Modal `<dialog>` elements are the canonical modal surface. The `.dlg-close` `×` button uses the standard glyph and focus suppression pattern.

### Ops drawer + topbar mini-bar

The ops drawer (`#ops-drawer`, `.op-card`) and topbar mini-bar (`#op-mini`, `.op-pill`, `#op-status-idle`) are the canonical surfaces for live operation progress. Per-kind priority is encoded in `OP_MINI_PRIORITY` (ops.js) and `_topbar_ssr_state`'s SQL CASE (api.py) — both sites must agree (v1.15.82).

## 3. Patterns

These are the load-bearing UX patterns; any new screen should mirror an existing one rather than invent.

### `// PREFIX` for buttons + section headers

Every interactive label and every section title uses the `// ` prefix (e.g. `// SYNC THEMERRDB`, `// SOURCE BREAKDOWN`, `// PREVIEW IMPORT`). This is the canonical motif voice.

### Source-letter axis (T/A/U/M/P/–)

The row "SRC" letter renders in three places — keep aligned when changing:
* DB read path: `_SRC_LETTER_SQL` in `app/web/api.py`
* Client logic: `computeSrcLetter` in `app/web/static/app.js`
* Dashboard donut: `renderThemeSourcePie`

Each letter has a canonical color (see § 1 Color tokens) and a canonical action set in the SOURCE menu.

### Equal-flanking column spacing (v1.15.84)

For tables with centered content across multiple columns, equalize the `(column_width - content_width)` flanking budget across columns to produce uniform-looking inter-column gaps. Equal *column widths* produce *unequal gaps* when content widths differ. See `docs/SESSION_JOURNAL.md` v1.15.84 entry for the derivation.

### Hash-skip pattern for innerHTML swaps

When polling re-renders a table, store the new HTML's hash in `tbody.dataset.lastHash` and skip the swap if unchanged. Prevents scroll-position reset during routine polling.

### Optimistic placeholder for click→busy gap

Click handlers that kick off an op should call `motifOps.setOptimisticPlaceholder(kind, label)` so the topbar lights immediately, before the server's first `/api/progress` poll returns. The placeholder has a 5s TTL and hands off when the real op arrives.

### Load-time auto-kick (v1.15.85)

When a library page loads and any row has `job_in_flight` set, kick `libraryRapidPoll()` so the row repaints as the worker progresses. Prevents stale amber pills from sticking after navigation.

### // TRY THIS NEXT — recovery options

Failure recovery options surface in the info card under "// TRY THIS NEXT" (or "✓ RESOLVED" / "✓ ACKED" / "✓ PLEX SERVES" depending on state). The action set is recipe-keyed by `failure_kind`. **Additive recoveries** (ADOPT) surface regardless of ack state; **transitional recoveries** (REVERT, LET PLEX SERVE) gate on `not effective_acked_at`. See v1.15.86 entry.

### Two-state pill pairs SSR'd in inverse

For mutually-exclusive topbar pills (e.g. `#op-mini` and `#op-status-idle`), SSR both halves with inverse conditions on the same gate — otherwise the JS reconciliation creates a one-frame flash on the unbaked side (v1.15.55, v1.15.78).

### Sticky thead with `border-collapse: collapse` (v1.16.11)

`<thead>` cells under `.table` inherit `border-collapse: collapse`. When the `<th>` row uses `position: sticky`, the collapse arithmetic gets ambiguous during the sticky transition and the bottom border can disappear for a frame — the thead visually "disconnects from the top header" on scroll-back. Substitute `box-shadow: inset 0 -1px 0 var(--line)` for the `border-bottom` rule and pin `background: var(--bg-elev-2)` so the row stays opaque over scrolling content. (The LOGS JOBS panel sidesteps this entirely — since v1.19.6 it's a `<div>` grid, not a `<table>`, with the header as a non-scrolling sibling above `.jobs-scroll-y`.)

### View toggle — one full-width panel at a time (v1.22.56)

When two related views compete for the same screen region and a side-by-side split would cramp both (the LOGS page's JOBS | EVENT STREAM panes — each needed horizontal scroll to fit), toggle between them instead of splitting. The pattern mirrors the library STANDARD/4K resolution chips:

* **Toggle chips** in the `.block-head`: `<div class="chips" role="tablist">` with two `<button class="chip" data-X="...">` — exactly the resolution-toggle shape.
* **One panel per view**: `<div class="log-panel" data-logpanel="...">`; CSS hides them (`display: none`) and shows the active one (`.is-active { display: block }`). JS `setLogView(view)` flips the `.is-active` class + the chips' `chip-active`/`aria-selected` + any view-scoped affordances.
* **SSR the initial panel** from the deep-link params in the route handler (`/queue` reads `?status` → jobs, `?since`/`?level`/`?component` → events) so the right panel paints first — no flash — the same idea as settings' `data-settings-tab` pre-paint stamp. The JS re-applies it on load for SPA-style nav.
* **Full width unlocks simpler layout.** Going from a half-width pane to full width let the LOGS rework DELETE the horizontal-scroll machinery (`.jobs-scroll-x` + `.scroll-chevron-wrap` + the chevron fade pseudos + the `_updateScrollAffordance` JS): the jobs grid's columns became fluid `minmax(0, Nfr)` (truncating instead of scrolling), and the event rows wrap. **Lesson: a layout that needs a horizontal scrollbar is often a layout that's too narrow — widen it before adding scroll affordances.**
* **`[hidden]` + a `display` class don't mix.** A view-scoped element toggled via the `hidden` attribute (the LOGS LIVE indicator) needs an explicit `.X[hidden] { display: none }` rule, because a class's `display: flex` outranks the UA `[hidden]{display:none}` and the element leaks onto the other view.

Guard: `tests/test_v1_22_56_logs_toggle.py`.

### Master-toggle for paginated select-all headers (v1.16.11)

A header checkbox in a paginated table needs two semantics that look identical but answer different questions:

* **RENDER** (`checked` / `indeterminate` / unchecked) is **visible-only** — it tells the user what's selected on the current page at a glance.
* **CLICK** is **full-selection-aware** — `selected.size === 0` selects all visible, any non-zero size clears the entire selection (visible + off-page). Tri-state click semantics worked before SELECT ALL FILTERED existed; once a row Set survives pagination, two-state master-toggle is what users reach for. See `#library-select-all` click handler for the canonical implementation.

### Selection-wide row cache for bulk actions (v1.16.10)

When a UI affords a "select all across pages" action (SELECT ALL FILTERED at `loadLibrary`), maintain a parallel `Map<key, row>` populated by:

1. the pagination loop that adds keys to the Set,
2. a sync pass on every visible-page render (refreshes cached row data for the current page),
3. each per-row checkbox toggle.

Every `Set.clear()` must be paired with `Map.clear()` (pytest guard in the cache test suite asserts call-site counts match). Bulk-bar count badges + click handlers walk the Map's values so the action covers the entire selection, not just the visible page. Off-page-warning workarounds (pre-v1.16.10 PUSH / ACK / ADOPT+LPS) become unnecessary once the cache is in place. See `libraryState.selectedRows` for the canonical implementation.

### Status-text auto-dismiss in `finally` block (v1.17.2 / v1.17.5)

Every `.form-status` span that shows a transient `✓ saved` / `✗ <error>` / `✓ embedded 1/1` text after a button click must auto-dismiss after a few seconds. Canonical pattern (`bindTestNotification` is the reference):

```js
} finally {
  setTimeout(() => {
    if (status.classList.contains('form-status-ok')
        || status.classList.contains('form-status-fail')) {
      status.textContent = '';
      status.className = 'form-status';
    }
  }, 4000);
}
```

The class-aware reset defends against a fresh run's message getting wiped by a stale timer. Durations: 2500ms for SAVE buttons (small `✓ saved` text), 4000ms for diagnostic results (per-sink summary lines, etc.), 6000ms for richer payloads (info-card probe results). v1.17.5 propagated the pattern to 6 status sites previously missing it. Intentionally NOT auto-dismissed: dialog-submit errors — the dialog staying open IS the user signal, removing the explanation leaves them clueless.

### Bulk-action bar constant height under heavy button count (v1.17.6 + v1.17.18)

When a UI hosts a variable-count button row that the user can stretch with filters / selections, the default flex behavior is to compress children below natural width and let browser text-wrap kick in (button labels split across lines, captions stack). Bar height balloons proportionally.

Fix: scope a CSS rule to the specific bar's ID (don't generalize — other consumers of the same primitive may want wrap behavior):

```css
#library-bulk-bar { flex-wrap: nowrap; overflow: hidden; min-height: 56px; }
#library-bulk-bar > * { flex-shrink: 0; }
#library-bulk-bar .btn { white-space: nowrap; }
#library-bulk-bar .missing-banner-text { flex-shrink: 1; min-width: 160px; white-space: nowrap; }
```

**v1.17.18 follow-up — overflow dropdown replaces horizontal scroll.** The original v1.17.6 design used `overflow-x: auto` (horizontal scroll fallback when buttons exceeded container width). In practice the scroll was undiscoverable — buttons clipped at the right edge with no visual cue. v1.17.18 swaps the scroll for a `// MORE ▾` overflow dropdown that absorbs rightmost overflow buttons until the bar fits. Constant-height contract preserved; actions discoverable instead of clipped.

```html
<details class="row-menu" id="library-bulk-overflow-menu" style="display:none">
  <summary class="btn btn-tiny" data-bulk-overflow-toggle>// MORE ▾</summary>
  <div class="row-menu-panel" data-bulk-overflow-panel></div>
</details>
```

`_layoutBulkBar()` (in `app.js`) measures `bar.scrollWidth > bar.clientWidth` and moves the rightmost non-primary visible button into the panel until the bar fits. Idempotent: pulls every panel child back to the bar before re-measuring. Primary bookends (SELECT ALL FILTERED + CLEAR) excluded via `_BULK_BAR_PRIMARY_IDS` set — they're the always-needed selection controls.

Wired to `updateLibrarySelectionUi` (filter / selection changes) + a `ResizeObserver` on the bar (covers width-only changes — sidebar collapse, drawer open, window resize). Close-on-action handler scoped to the overflow menu mirrors the v1.10.24 row-menu close-on-action pattern.

**General lesson — overflow toolbar > horizontal scroll.** For low-frequency overflow (the bar fits in most cases) the dropdown wins on discoverability. For high-frequency overflow, the action set is too big and needs trimming.

### Adjacent-sibling CSS for contextual spacing (v1.17.5)

When a primitive needs different spacing in one specific shape, prefer the adjacent-sibling combinator (`.X + .Y`) over a `.Y--variant` modifier class:

```css
.form-hint + .form-actions {
  margin-top: var(--gap-3);
  padding-top: var(--gap-2);
  border-top: none;
}
```

The selector encodes the WHY (`.form-actions` after a `.form-hint` paragraph doesn't need the dashed-separator pattern because the paragraph IS the separator). A variant class doesn't carry that semantic. PROBE TDB URLs section was the v1.17.5 canonical case.

### Dialog canonical shell (v1.17.8)

Every `<dialog class="dlg">` shares the same shell:

```html
<dialog class="dlg" id="...-dlg">
  <article class="dlg-body">
    <header class="dlg-head">
      <h2 class="dlg-title">// TITLE</h2>
      <button class="dlg-close" data-close>×</button>
    </header>
    <form class="form-grid form-grid-tight">
      <!-- form fields -->
      <div class="form-actions">
        <button type="submit" class="btn btn-warn">// PRIMARY ACTION</button>
        <button type="button" class="btn" data-close>// CANCEL</button>
        <span class="form-status"></span>
      </div>
    </form>
  </article>
</dialog>
```

Rules:
- `<h2 class="dlg-title">` (not `<h3>` or bare `<h2>`). The `// ` prefix is required.
- Close button nests inside `.dlg-head` (NOT a bare child of `.dlg-body` — the v1.15.115 `.dlg-body > .dlg-close` absolute-corner variant was retired in v1.17.8).
- Form-actions row always has both submit + cancel. Submit is `.btn-warn` (amber, mutating); cancel is plain `.btn` (green).
- Library dialogs (upload / override / manual-url) skip the `.form-label-row` wrapper. Settings-page convention uses it for the env-badge slot; dialog forms don't need it.

### Apprise notification dispatch contract (v1.17.0 / v1.17.4)

`app/core/notify.py` exposes a single best-effort dispatch:

```python
notify.dispatch(db_path, notifications, event_kind, title, body)
```

Contracts:
- **Never re-raise.** Worker threads, scheduler ticks, request handlers all call this and assume it can't crash them. The function wraps its internals in try/except + logs to the events table at `component='notify'` for in-app health visibility.
- **Per-event gate.** Reads `notifications.events[event_kind]`; no-op if disabled or unknown.
- **Two sinks coexist.** Embedded Apprise URLs (in-process via `apprise.Apprise()`) + an optional external Apprise API URL (HTTP POST). Both populated = both fire.
- **Per-event severity routing** (v1.17.1+): `_EVENT_NOTIFY_TYPE` maps event_kind → `"info"` / `"warning"` / `"failure"` so caronc/apprise-api routes via severity correctly.

For events that can fire many times per minute / hour, use `notify_dedupe.should_fire(db, kind, rate_limit_seconds=N)` or `edge_value=V`. State lives in `runtime_settings` (no schema migration). Fail-open: read errors return True (better to lose a dedupe round than swallow a notification).

### Notification subject emoji set (v1.17.19)

One cohesive emoji per event KIND (not per minor variation) for at-a-glance scannability without overload. Bot username already attributes ("Motif APP") so the redundant `motif:` subject prefix is dropped — except on attribution-critical events where the subject alone is too vague.

| Event | Emoji | Subject shape |
|---|---|---|
| sync_completed | ✅ | `Motif sync — no changes` / `Motif sync — <summary>` (v1.19.55: ✅ moved to the body's closing line; subject is a neutral summary, `worker.py:1012/1021`) |
| sync_failed | ❌ | `❌ Sync failed` |
| bulk_action_completed | ✅ | `✅ Bulk PROBE TDB done — N/M` / `✅ Bulk LPS done — N` |
| themes_added_by_sync | 🎵 | `🎵 N new themes added by sync` |
| themes_updated_by_sync | 🔄 | `🔄 N themes updated by sync` (v1.18.80, `worker.py:1158`) |
| theme_added | 🎵 | `🎵 Theme added — <title>` |
| theme_pushed | 📤 | `📤 Theme pushed — <title>` (v1.19.55 re-deploy: PUSH / REPLACE / PROMOTE, `notify_content.py:453`) |
| theme_deleted | 🗑️ | `🗑️ Theme unmanaged/forgotten/deleted — <title>` |
| backup_ready_to_deploy | 🎯 | `🎯 Backup ready — <title>` (v1.18.79, `notify_content.py:565`) |
| plex_theme_lost / theme_lost_backup_ready / theme_lost_sidecar_available | 💔 | `💔 Theme lost — <title>` (v1.18.90 / v1.19.41 four-way tier split, `notify_content.py:617/703/766`) |
| cookies_needed | 🍪 | `🍪 YouTube cookies expired or missing` |
| disk_low | 💾 | `💾 Low disk space — XMB free` |
| worker_restarted | ⚠️ | `⚠️ Worker restarted (unclean shutdown)` |
| release_available | 🆕 | `🆕 motif vX.Y.Z available` (keeps "motif" — "vX.Y.Z available" alone is vague) |
| test | 🧪 | `🧪 motif vX.Y.Z — notification test` (operator setup verifies attribution) |

Rules:
- **One emoji per event KIND**, not per minor variation. theme_added + themes_added_by_sync share 🎵 (same conceptual event). All three destructive verbs (unmanaged / forgotten / deleted) share 🗑️.
- **Drop the `motif:` prefix** wherever the bot username carries the attribution.
- **Keep "motif" on attribution-critical events** — release announcements + setup-test pings.
- **Anti-regression**: structural lint test (`test_v1_17_19_notification_emoji.py::test_no_lingering_motif_colon_titles`) flags any new dispatch site that slips back to `title="motif:"` shape.

### Notification body shape — Discord-compatible (v1.17.14 / v1.17.16)

Discord webhooks have two constraints that shape the body markdown:

1. **No inline markdown images.** `![alt](url)` renders LITERALLY in Discord (and most other Apprise services). Don't use it.
2. **URL auto-embed only fires on bare URLs.** Wrapping a URL in `[text](url)` brackets disables Discord's preview card. Keep theme URLs plain on their own line.

Canonical body for `theme_added`:

```
Source: ThemerrDB · YouTube
https://www.youtube.com/watch?v=...
```

Two lines. Discord auto-embeds the URL below the text content (full preview card: thumbnail + title + channel + play button). Plain-text services see a clickable URL line.

Canonical body for `theme_deleted`:

```
Forgotten by admin · 1 placement(s) unlinked · orphan row dropped
Source: ThemerrDB · YouTube
Previous theme: <https://www.youtube.com/watch?v=...>
```

Three lines. URL wrapped in `<...>` (Discord's no-embed marker) so deletion notifications don't dominate the channel with a giant preview card. The URL stays clickable; only Discord interprets the brackets specially. Action + extra-note collapse onto one line with `·` separator when both are present.

Rules:
- **Don't repeat the title in the body.** Subject already has it. v1.17.16 retired `**<title>**` as line 1 of both body formatters.
- **Order matters: source line BEFORE URL.** Discord renders text content above the auto-embed card, so the URL must be the last line — user reads provenance before the preview appears below.
- **Capture enrichment context BEFORE destructive ops** (v1.17.16). FORGET drops orphan themes rows; DELETE drops everything. Post-op `enrich_item` finds nothing and falls back to "movie/-26" bare-ID form. Capture `ItemContext` near `_require_admin(...)`, thread it through to `notify.dispatch` after the destructive op completes.

## 4. Reference screens for common problem shapes

| Need to build…                  | Reference screen                                           |
|---------------------------------|------------------------------------------------------------|
| Library-style row table         | `library.html` + `renderLibraryRow` in app.js              |
| Filter chip row                 | `library.html` chip clusters + `loadLibrary` chip handlers |
| Settings form section           | `settings.html` form-grid blocks                           |
| Info card / modal               | `openInfoDialog` in app.js + the `<dialog>` markup         |
| Dashboard stat card             | `dashboard.html` + `.stat-card` family in app.css          |
| Preview-then-apply flow         | Settings page import flow (`#import-preview-table`)        |
| Live progress on a long job     | Ops drawer + topbar mini-bar (ops.js)                      |
| View toggle (one panel at a time) | LOGS page JOBS/EVENT STREAM (`queue.html` + `setLogView` in app.js) — see § 3 |

## 5. Workflow before writing UI code (the user's rule)

1. **Read the tokens.** Open `app.css` lines 4–54.
2. **Read the relevant primitives.** Search app.css for the class families you'll use.
3. **Find the reference screen.** Pick a sibling that solves the same shape.
4. **List the tokens + classes you'll use BEFORE writing.** Anchor your output to existing class names; grep to confirm they exist.
5. **If a primitive is missing or partial** (e.g., `.input-tiny`): build it as a real CSS rule first. Don't inline an override on the consumer.
6. **Hardcoded hex / px outside `:root` is forbidden** unless it's a genuinely-dynamic computed value. Always prefer `var(--foo)`.
7. **For ambiguous design choices: stop and ask.** the user's "consistency beats novelty" overrides the default "work without stopping" preference for visual/interaction design decisions specifically.

## 6. Gaps (flagged for cleanup, not blocking)

| Gap                                      | Impact                                                          | Suggested fix                                                                                              |
|------------------------------------------|-----------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `.input-tiny` has no CSS rule            | JS references class with no styles → silent inheritance of `.input` padding 8/12; caught v1.15.87 | **DONE in v1.15.88** — promoted to a real primitive at app.css:1073 (`.input-tiny { … }`). The per-site override in `.col-import-action select` was the immediate workaround; the primitive supersedes it. |
| No spacing scale tokens                  | Padding/margin values are raw px clustering around 4/6/8/10/12/14; no canonical scale | **DONE in v1.15.114** — `--gap-1` (4px) through `--gap-7` (32px) added in v1.15.99 (Jinja side). v1.15.114 completed the CSS-side migration: 137 sites where every px component matched a token value were converted to `var(--gap-N)`. Mixed-axis rules (e.g. `padding: 4px 10px`) kept raw — the half-tokenized form would have looked broken. |
| Hardcoded `rgba()` in CSS (151 sites)    | Every `.btn-*:hover` defines `background: rgba(<color>, 0.08)` and `box-shadow: rgba(<color>, 0.15)` inline. A future tint/opacity adjustment needs touching 151 places. | **DONE in v1.15.113** — 11 `--<color>-rgb` tokens added to `:root` (green/amber/orange/red/cyan/blue/violet/magenta/brown plus white/black for overlay sites). 185 of 201 raw rgba calls migrated to `rgba(var(--<color>-rgb), <alpha>)`. Remaining 16 raw calls are one-off color variants with no token equivalent (alt-greens, light-amber, pink-anime, etc.) — future tokens can absorb them as patterns recur. |
| No motion tokens                          | Transition durations written as `0.12s` / `0.15s` / `0.2s` ad-hoc; no consistency | **DONE in v1.15.110** — `--motion-fast` (80ms) / `--motion-normal` (120ms) / `--motion-slow` (400ms) added; 17/20 transition sites migrated. Outliers: 0.6s coverage-bar width (intentional slowness) + 0.2s pulsed-pill opacity (narrow). |
| No breakpoint tokens                      | Responsive behavior is ad-hoc (mostly desktop-only)              | Add `--bp-tablet` / `--bp-desktop` if mobile support ever becomes a goal                                   |
| `.btn-warn` / `.btn-info` / `.btn-plex` etc. duplicate structure | Each adds a per-color block; only color + border-color differ | Consider `--btn-tone-color` CSS variable + a single `.btn[data-tone=X]` selector. Not urgent — names are clear and palette is small |
| Per-site `padding` overrides in v1.15.x markers | Each iteration patched a primitive's consumer rather than the primitive itself | Audit the inline overrides; promote stable ones (like v1.15.87's 4/10 select padding) back into the primitive (`.input-tiny`) |
| Invented class names in JS template literals | Risk of silent gaps (pre-v1.15.71 `.src-pill src-X`, pre-v1.15.87 `.input-tiny`) | **DONE in v1.15.89** — `tests/test_v1_15_89_js_classname_hygiene_lint.py` diffs JS class tokens against CSS rules with an explicit allowlist. New silent-gap classes fail the test. Current allowlist documents 8 legacy entries for future cleanup. |
| Invented class names in Jinja templates | Same risk class, SSR-side. v1.15.89's lint scans JS only — Jinja templates weren't lint-scoped. Initial 2026-05-18 scan found 3 sites: `.stat-label` (13×), `.cache-gauge-total` (1×), `.dash-customize-line` (1×) | **DONE in v1.15.111** — `tests/test_v1_15_111_jinja_classname_hygiene.py` extends the lint to templates. `.stat-label` promoted to real primitive (`flex: 0 1 auto`); the other two were redundant structural markers and got removed (id + `.muted .small` already covered them). |
| `.mono`, `.small`, `.pill`, `.pill-warn` referenced in JS but no CSS rules | JS used these as utility-flavored classes that worked-by-accident via parent inheritance — except `.pill` + `.pill-warn` which rendered as plain inline text (visible bug on the VERIFY badge). | **DONE in v1.15.93** — promoted to real primitives. `.mono`/`.small` are utilities, `.pill` is the badge base, `.pill-warn` is the amber variant. Removed from v1.15.89 lint allowlist. |
| `.btn-tone-ok`, `.btn-tone-muted` referenced via JS template interpolation | The v1.15.89 lint missed these because it only scans static `class="..."` literals — `class="pill ${tone[r.status]} small"` is invisible to it. Pre-fix the STATUS pills (CLEAN, DUPLICATE, NO MATCH, SKIPPED) rendered as plain text since no tone class had CSS. | **DONE in v1.15.93** — added pure-color tone modifier rules. Composes with `.pill` (for STATUS badges) or `.btn` (future use). The lint should ideally catch template-interpolated classes too — flagged as a v1.15.89 lint enhancement. |
| `.op-card-meta-value`, `.op-card-cancel-note` referenced in JS but no CSS rules | Structural classes in the ops drawer. Style comes from sibling `.muted small` classes. | **DONE in v1.15.112** — both promoted to real CSS rules. `.op-card-cancel-note` absorbed its inline style (uses `--gap-2`); `.op-card-meta-value` made its `font-variant-numeric: inherit` cascade explicit. Both entries removed from the v1.15.89 lint allowlist. |
| `scanner.py:275` UPDATE plex_items keyed on `WHERE folder_path = ? AND section_id = ?` | Same path-domain mismatch class as v1.15.90 / v1.15.91 — on Unraid the scanner runs against container paths while plex_items.folder_path is Plex's host path. The UPDATE silently misses. | **DONE in v1.15.92** — multi-candidate UPDATE landed. Strategy: theme_id first (canonical FK; covers most rows after `resolve_theme_ids`), fall back to folder_path matching against every candidate the path could be (the container path the scanner saw + each reverse-translated host-path form). Reverse iterates `_PATH_PREFIX_TRANSLATIONS`. v1.15.100 then surfaced `MOTIF_PATH_TRANSLATIONS` as a user-overridable env var. |
| `local_files.provenance` not updated on REPLACE TDB | the user's "A Dog's Journey" info card showed `provenance="manual"` after multiple REPLACE TDB calls — expected `provenance="auto"` since the worker writes fresh provenance based on user_overrides presence (and REPLACE TDB deletes user_overrides). Suspected but unconfirmed: a stale `''` user_overrides row that survived the section-scoped delete fallback. | Needs DB-state diagnostic to root-cause. Flagged for follow-up — the user to run `SELECT * FROM user_overrides WHERE tmdb_id = 522518;` if it reproduces. |

## 7. Tone, copy, terminology

* **Sentence case** for button labels and section headers, AFTER the `// ` prefix, but motif typically renders ALL CAPS via `text-transform: uppercase` so the source can stay sentence-case. (Some are already uppercase in source as a stylistic choice.)
* Consistent verbs:
  * **SYNC** — ThemerrDB pull (`// SYNC THEMERRDB`)
  * **REFRESH** — Plex section enumeration (`// REFRESH FROM PLEX`)
  * **RE-SCAN** — post-place per-folder Plex nudge (`// RE-SCAN QUEUE`)
* Source letters always in single-quoted form when in prose: 'T-row', 'M-source', 'P-available'.
* Failure-related copy: `// TRY THIS NEXT` (live) / `✓ RESOLVED` / `✓ ACKED` / `✓ PLEX SERVES — OPTIONAL UPGRADES` (resolved-via-plex) / `✓ BACKUP READY — DEFERRING TO PLEX` (user explicitly chose backup intent via KEEP AS BACKUP, v1.18.77+).
* Intent-flip button pair (v1.18.77+, lives in the recovery section): `// PROMOTE TO ACTIVE` (flip backup → replace, force-place motif's URL) + `// MARK AS BACKUP` (flip replace → backup, stop trying to push past plex_has_theme). Both are `.btn-warn` (amber, mutating — they change `user_overrides.intent` and trigger worker-side state changes). PROMOTE shows whenever intent='backup'; MARK AS BACKUP only when intent='replace' AND `data.plex_resolved` (Plex has a fallback theme to serve).
* **Row-menu labels stay BARE — the `// ` prefix is the ONE exception to the universal rule (v1.20.49 → reverted v1.20.51).** v1.20.49 added the `// `/`× ` prefix to the per-row SOURCE/PLACE/REMOVE menu (summaries + items) to match the bulk-bar voice. It broke the layout: the `.row-menu > summary` buttons have `min-width: 78px` sized for `SOURCE ▾`/`PLACE ▾`/`REMOVE ▾`, and `// SOURCE ▾` overran it, overflowing the right-anchored `.row-actions` cluster (320px cell, flex-end) left into the IMDB column (the user's repro). v1.20.51 reverted it — **the library row menu (`menuItemHtml` / `menuButtonHtml`) renders bare labels** (`SOURCE ▾`, `PUSH TO PLEX`, `DEL`, `× PURGE`). Don't re-add the prefix here without first widening the actions cell / summary min-width. The bulk bar keeps `// ` (it has room); the orphans page keeps its `// `/`× ` labels (roomy findings table, no overlap) + the red `.btn-danger` `DELETE SIDECAR`. Guard: `tests/test_v1_20_49_row_menu_label_voice.py` (now pins the row menu BARE).

## 8. Definition of done

A UI change is only complete when:

- [ ] All styling flows through tokens (no raw hex / px outside `:root`)
- [ ] All interactive elements use shared primitives (or properly extend them)
- [ ] The screen visually + behaviorally matches a sibling screen
- [ ] No new ad-hoc values introduced
- [ ] If a primitive was missing: built as a real CSS rule, not inlined as an override
- [ ] Visually compared against a reference screen
- [ ] Journal entry references which sibling screen the work followed
