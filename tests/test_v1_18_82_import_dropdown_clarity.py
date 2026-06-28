"""v1.18.82 — CSV import preview dropdown clarity.

the user on the v1.18.78-era import preview: "keep vs replace vs
skip — confused" + "the button that contains the down plex
backup option is much bigger and looks out of place compared
to the others and the text is cut off."

Two root causes:

1. **Redundant Skip on CONFLICT** — api.py:11310 treats
   action='keep' and action='skip' identically (both increment
   the `skipped` counter, no DB write). Pre-v1.18.82 the
   CONFLICT dropdown carried both, which read as "two slightly-
   different no-ops" when they're actually the same backend
   call. Removing Skip from CONFLICT collapses to three
   genuinely-distinct intents: Keep current / Replace / Backup
   only (P-rows).

2. **Auto-sizing select** — `.input-tiny`'s natural-width
   styling meant the select widened to fit its currently-
   selected option's text. "Keep" rendered narrow, "Download
   backup (Plex stays)" rendered ~3x wider, jagging the ACTIONS
   column's right edge on any row where the P-row option was
   chosen. Fix: shorter label ("Backup only") + `min-width:
   140px` on the action select for visual consistency.

Each option also gains an explicit `title=` tooltip so a future
"what does this do?" hover produces an answer in-place instead
of requiring the user to open settings docs.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _import_panel_body() -> str:
    """Extract the bindImportPanel function body so the assertions
    target the import-preview render path and aren't fooled by
    sibling code that happens to mention 'Skip' or 'Keep'."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    start = js.index("function bindImportPanel()")
    end = js.index("// ---- Config form", start)
    return js[start:end]


def _strip_line_comments(src: str) -> str:
    """Strip `//`-comments line-by-line so assertions about
    rendered markup aren't fooled by the v1.18.82 archaeology
    comment that references the OLD labels ("Download backup
    (Plex stays)" / "Skip") in its rename history. Block
    `/* ... */` comments aren't used in this file so a simple
    line-prefix strip suffices."""
    out = []
    for line in src.splitlines():
        # Find a `//` that's NOT inside a string literal.
        i = 0
        in_sgl = False  # single-quote string
        in_dbl = False  # double-quote string
        in_btk = False  # backtick template literal
        while i < len(line):
            c = line[i]
            if c == "\\" and i + 1 < len(line):
                i += 2
                continue
            if not in_dbl and not in_btk and c == "'":
                in_sgl = not in_sgl
            elif not in_sgl and not in_btk and c == '"':
                in_dbl = not in_dbl
            elif not in_sgl and not in_dbl and c == "`":
                in_btk = not in_btk
            elif (not in_sgl and not in_dbl and not in_btk
                  and c == "/" and i + 1 < len(line)
                  and line[i + 1] == "/"):
                line = line[:i]
                break
            i += 1
        out.append(line)
    return "\n".join(out)


# ── 1. CONFLICT dropdown drops the redundant Skip ────────────


def test_conflict_dropdown_omits_redundant_skip_option():
    """v1.18.82: the CONFLICT branch's <option value="skip">
    must NOT render — backend treats it identically to 'keep'
    (api.py:11310), so two no-op options on the same dropdown
    is visual noise without semantic difference. Removed in
    v1.18.82 to leave three genuinely-distinct intents."""
    body = _strip_line_comments(_import_panel_body())
    # Anchor on the actual CONFLICT branch — `else if (r.status
    # === 'conflict')` — NOT the canApply guard a few lines
    # earlier that also references the literal.
    conflict_start = body.index("else if (r.status === 'conflict')")
    conflict_end = body.index("const statusLabel", conflict_start)
    conflict_block = body[conflict_start:conflict_end]
    # The new CONFLICT dropdown has Keep current / Replace /
    # [Backup only]. Skip is gone.
    assert '<option value="skip"' not in conflict_block, (
        "v1.18.82: CONFLICT dropdown must not carry an explicit "
        "Skip option — it duplicates 'Keep' (both no-op per "
        "api.py:11310). the user flagged the confusion."
    )


def test_clean_dropdown_keeps_skip_option():
    """Sanity: CLEAN rows STILL have Skip (the apply checkbox
    is row-level inclusion; the action dropdown picks WHAT
    happens when the row IS included). On CLEAN there's no
    'keep' alternative — Skip is the only no-op alternative
    to Apply, so it's not redundant on this branch."""
    body = _strip_line_comments(_import_panel_body())
    # Anchor on the `if (r.status === 'clean')` branch, not the
    # earlier canApply guard that mentions the literal too.
    clean_start = body.index("if (r.status === 'clean')")
    clean_end = body.index("else if (r.status === 'conflict')",
                           clean_start)
    clean_block = body[clean_start:clean_end]
    assert '<option value="skip"' in clean_block, (
        "v1.18.82: CLEAN dropdown must retain Skip — it's the "
        "only no-op alternative on this branch (no 'keep' to "
        "merge into). Don't blanket-remove."
    )


# ── 2. Updated labels are clearer ────────────────────────────


def test_conflict_keep_option_renamed_for_clarity():
    """'Keep current' makes the intent explicit — pre-fix
    plain 'Keep' was ambiguous next to Skip (which kept things
    too)."""
    body = _import_panel_body()
    assert ">Keep current</option>" in body, (
        "v1.18.82: CONFLICT 'Keep' must read 'Keep current' "
        "to disambiguate from the row-level apply checkbox"
    )


def test_conflict_p_row_option_renamed_short():
    """v1.15.71 shipped 'Download backup (Plex stays)' — long
    enough to widen the select past its sibling rows. v1.18.82
    shortens to 'Backup only' — same semantics, fits the
    140px min-width without truncation."""
    body = _strip_line_comments(_import_panel_body())
    assert ">Backup only</option>" in body, (
        "v1.18.82: the P-row CONFLICT option must read 'Backup "
        "only' (short label); was 'Download backup (Plex stays)'"
    )
    assert "Download backup (Plex stays)" not in body, (
        "v1.18.82: the old long label must NOT remain — it's "
        "what caused the select-width spread the user flagged"
    )


def test_p_row_option_still_gated_on_current_src():
    """Sanity: the v1.15.71 gating (only P rows get the backup
    option) must survive the label rename."""
    body = _import_panel_body()
    # Gating: either current_src === 'P' or current_src=="P".
    assert "current_src === 'P'" in body or "current_src==\"P\"" in body, (
        "v1.18.82: Backup only must still be gated on current_src=P"
    )


# ── 3. Tooltips explain semantics in-place ───────────────────


def test_conflict_options_carry_title_tooltips():
    """Every CONFLICT option must have a `title=` tooltip so
    hovering surfaces the full semantics. the user's "keep vs
    replace vs skip" confusion is solved by exposing the
    answer where the question is asked (the dropdown itself),
    not in separate docs."""
    body = _strip_line_comments(_import_panel_body())
    conflict_start = body.index("else if (r.status === 'conflict')")
    conflict_end = body.index("const statusLabel", conflict_start)
    conflict_block = body[conflict_start:conflict_end]
    # Three options: Keep current, Replace, Backup only — each
    # must have a title attr.
    keep_match = re.search(
        r'<option value="keep"[^>]*title="[^"]+"[^>]*>Keep current',
        conflict_block,
    )
    replace_match = re.search(
        r'<option value="replace"[^>]*title="[^"]+"[^>]*>Replace',
        conflict_block,
    )
    backup_match = re.search(
        r'<option value="download_only"[^>]*title="[^"]+"[^>]*>Backup only',
        conflict_block,
    )
    assert keep_match, "v1.18.82: Keep current must carry a title= tooltip"
    assert replace_match, "v1.18.82: Replace must carry a title= tooltip"
    assert backup_match, "v1.18.82: Backup only must carry a title= tooltip"


# ── 4. Width consistency CSS ─────────────────────────────────


def test_import_action_select_has_min_width():
    """v1.18.82: the action select must carry a min-width so
    all rows align visually regardless of which option is
    selected. Pre-fix the select auto-sized to its selected
    option's text — "Keep"-selected rows narrow vs "Backup
    only"-selected rows wider, creating a jagged right edge.
    140px fits the longest label ('Backup only') with native
    chevron padding."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    rule_start = css.index(
        "#import-preview-table select[data-import-action]"
    )
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    m = re.search(r"min-width:\s*(\d+)px", rule)
    assert m, (
        "v1.18.82: the action select must have an explicit "
        "min-width to pin column alignment"
    )
    assert int(m.group(1)) >= 130, (
        f"v1.18.82: min-width {m.group(1)}px is below the 130px "
        "floor — 'Backup only' + chevron padding doesn't fit "
        "comfortably below 130"
    )
