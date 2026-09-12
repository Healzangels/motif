# Row quick-play (feature E) — design spec

Status: tag 1 BUILT as v0.51.333, tag 2 (NOW PLAYING strip) BUILT as v0.51.334 (2026-09-12). Feature E complete.
Mockup: see the session artifact "Row quick-play".

## 1. Goal

Hear a row's theme from the library table without opening the INFO card.
One click, one sound at a time, an obvious stop. Today the only way to
listen is ⓘ → the AUDIO group's player; auditioning a page of rows is a
dialog per row.

## 2. Non-goals

- Not a player: no seek, no volume, no download — the INFO card's
  `<audio controls>` rows keep those.
- No queue, no autoplay-next, no crossfade.
- No new audio endpoints. The two that exist are reused:
  `/api/items/{mt}/{tmdb}/theme.mp3?section_id&rating_key` (motif's file,
  v1.12.90 / v1.21.90) and `/api/plex/theme/{rk}.mp3` (what Plex serves,
  v0.51.322, Range-aware, 204 on no theme).
- No gain / Web Audio. The element is never routed through
  `createMediaElementSource`, so the loudness audition's reset trap does
  not apply.

## 3. The control

A **leading play slot** in the title cell, before the attention glyph, on
every row:

```html
<td><div class="title-cell">
  <button type="button" class="title-glyph row-play" data-act="quick-play"
          data-src="…" aria-label="Play theme: Cowboy Bebop" title="…">▶</button>
  …attention glyph… <span class="title-cell-name">Cowboy Bebop</span> …
```

- Reuses the `.title-glyph` primitive (transparent button, mono 14px,
  `padding: 0 2px`, hover brightness, focus-visible ring). New modifier
  `.row-play` only fixes the slot width (`width: 18px; text-align: center`)
  so titles stay aligned whether or not a row is playable, and sets
  `letter-spacing: 0` (a lone glyph — the v0.51.332 convention).
- Tones: idle `var(--fg-dim)`; hover = the primitive's brightness; **playing:
  `■` in `var(--green)` with the `.title-glyph-action` glow** so the playing
  row reads from across the table; `aria-pressed="true"`.
- Rows with nothing to play render an empty slot (`<span class="row-play
  row-play-none"></span>`), never a disabled glyph — a quiet table, the
  slot still holds the column.
- Mobile (≤600px): 30px tap target on `.row-play`, the v0.51.6 pattern.

Why leading and not trailing or ACTIONS: the actions cluster is a 320px
flex-end cell already at its limit (DESIGN_SYSTEM § 7, the v1.20.49
overflow); trailing would sit a control among the fact badges (4K, level,
library). A leading play glyph is the media-list convention and, reserved,
costs 18px of the 540px title cell.

## 4. What plays — mirrors the INFO card headline

The card's `_derivePlaybackSourceLabel` already states "what motif holds ·
what plays" from `source_kind`, placement and the backup intent. The row
plays the same thing, decided from the fields the row already has
(`computeSrcLetter`'s inputs plus `last_place_attempt_reason`):

| Row state | Plays | Tooltip |
|---|---|---|
| motif's file on disk and placed (T / U / AT / A) | motif's file | Play — motif's file (placed) |
| motif's file on disk, not placed yet | motif's file | Play — motif's file (not placed yet) |
| motif's file on disk as a backup (BK / TB / UB / PB), Plex serves its own | what Plex serves | Play — what Plex serves (motif's copy stands by) |
| nothing on disk, Plex serves its own theme (P) or a sidecar (M) | what Plex serves | Play — what Plex serves |
| no theme (–), or canonical missing (dlBroken) | no control | — |

"Plex serves" is `plex_has_theme && verified ok`, the test `computeSrcLetter`
uses for `P`. A 204 from the Plex proxy (Plex claims a theme it will not
serve, or Plex unreachable) reverts the glyph and the results status line
says what the card says: "Plex reports a theme but it did not play".

## 5. Player mechanics

- One shared `<audio id="row-quick-play" preload="none">` in
  `library.html` (no controls, not displayed). `libraryState.quickPlay =
  { key, src } | null`.
- Click ▶: pause every other `<audio>` on the page (INFO card, EDIT AUDIO,
  anime-themes picker), set `src`, `play()`; `playing` → the glyph turns
  `■`; `ended` / `pause` / `error` → state cleared, glyph back to ▶ (and a
  status line on `error`).
- Click ■: `pause()` + `currentTime = 0`.
- Clicking another row's ▶ switches (the previous glyph reverts).
- Re-render safe: `renderLibraryRow` reads `libraryState.quickPlay`, so a
  filter, sort or page re-render keeps the ■ on the playing row; a page
  turn keeps the audio playing (the row is simply off-page — see tag 2).
- Exclusivity both ways: a capture-phase `play` listener on `document`
  pauses the row player when any other `<audio>` starts.
- Keyboard: it is a `<button>` (Enter / Space). Escape stays with dialogs.
- Nothing is written; no event is logged (a listen is not an operation).

## 6. Tags

- **Tag 1 (v0.51.333)** — the control, the rule, the shared player,
  exclusivity, tests, docs.
- **Tag 2 (optional, v0.51.334)** — NOW PLAYING in the results header:
  `▶ Cowboy Bebop · 0:12 / 1:30 · ■`, so a play started far down a 50-row
  page can be stopped from the top; elapsed via `timeupdate`. The mockup
  shows it dimmed as optional.

## 7. Tests (tag 1)

- `renderLibraryRow`: the slot renders `▶` for the four playable states and
  an empty slot for `–` and dlBroken; `data-src` picks the motif endpoint
  vs the Plex proxy per § 4 (a table-driven JS-static test against the
  branch, the same shape as `test_v0_51_329`'s classifier mirror).
- `library.html` carries the shared element; app.js binds
  `[data-act="quick-play"]`; the document-level exclusivity listener exists;
  the renderer reads `libraryState.quickPlay`.
- CSS: `.row-play` extends `.title-glyph`, fixed slot width, zero tracking
  (the v0.51.332 sweep test admits it as a glyph-only label).
- Python: none new — both endpoints are already covered (v1.12.90 /
  v0.51.322 tests).
- Docs: DESIGN_SYSTEM § 2 (the `.title-glyph` family gains `.row-play`),
  § 7 tooltip voice; CLAUDE.md map line; README's library section, one line.

## 8. Decisions for the operator

1. **Placement** — leading slot in the title cell (proposed) · trailing after
   the badges · the ACTIONS cell (does not fit at 320px).
2. **What plays** — the card's rule above (proposed) · always what Plex
   serves · always motif's file.
3. **Playing look** — `■` in green with the action glow (proposed) · a
   pulsing `▶` · source-toned (amber for Plex, green for motif's file).
4. **Tag 2** — the NOW PLAYING strip: yes / later / no.
