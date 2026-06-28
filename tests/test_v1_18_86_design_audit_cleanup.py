"""v1.18.86 — design audit cleanup (Tier 1 + Tier 2 from v1.18.85 audit).

The v1.18.85 3-agent design-system audit surfaced 66 findings. This
tag ships Tier 1 (real bugs) + Tier 2 (recent-arc cleanup) minus
T2-D (state-matrix gap, held for separate discussion).

## What's pinned

**Tier 1 — real bugs:**
- T1-A: JS empty-state row `colspan` matches the 11-column header
  (was 10; v1.15.53 bumped Jinja loading row but missed JS).
- T1-B: `var(--accent-red, #ff6d6d)` replaced with `var(--red)`.
  The token was never declared; consumers silently used the
  hardcoded hex which didn't even match canonical --red.
- T1-C: v1.18.85 probe `<pre>` inline `max-height: 480px;
  overflow:auto; margin-top: 12px` moved to a CSS rule
  (`#probe-plex-themes-output`). overflow:auto was redundant
  with `.codeblock`.
- T1-D: `bindProbePlexThemes` calls `_autoDismissOpStatus` in
  finally per v1.17.5's mandatory auto-dismiss contract.
- T1-E: upload-dlg checkbox copy aligned with manual-url-dlg
  KEEP AS BACKUP / PROMOTE TO ACTIVE (was DOWNLOAD ONLY / PUSH
  TO PLEX — divergent since v1.18.77's rename missed sibling).
- T1-F: `// KEEP AS BACKUP` checkbox label drops the `// `
  prefix (DESIGN_SYSTEM §3: prefix is for buttons + section
  titles only, not checkbox labels).

**Tier 2 — recent-arc cleanup:**
- T2-A/B: Recovery flip block uses new `.recovery-section-flip`
  primitive; inline `margin-top:8px` / `margin-left:10px` raw
  px gone.
- T2-C: BACKUP READY title + PROMOTE/MARK pair catalogued in
  DESIGN_SYSTEM.md §7.
- T2-E: Missing `.recovery-option-info-tone-*` CSS rules added
  for themerrdb / info / danger / magenta (server emits all
  four; lint blindspot per v1.15.93 sub-pattern).
- T2-F: MARK AS BACKUP `.btn-info` → `.btn-warn` (mutating
  action, symmetric with PROMOTE TO ACTIVE per §2 table).
- T2-I: Probe button label `// PROBE PLEX THEME API` → `// RUN
  PROBE` (matches 2-3 word sibling cadence).
- T2-J: "characterise" → US English in subtitle copy.
- T2-K: `code` primitive added (inline `<code>` tags now have
  visual distinction; pre-fix rendered as plain mono prose).
- Stale doc: app.css palette comment U=blue → U=violet (stale
  since v1.10.14).
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
DESIGN_SYSTEM = REPO / "docs" / "DESIGN_SYSTEM.md"


def _strip_jinja_comments(src: str) -> str:
    """Strip `{# ... #}` Jinja comments. v1.18.86's archaeology
    comments reference OLD copy ("DOWNLOAD ONLY", "characterise")
    by design — the rendered markup must NOT contain those, but
    the comments legitimately do. Tests that assert old-copy
    absence must strip comments first."""
    return re.sub(r"\{#.*?#\}", "", src, flags=re.DOTALL)


# ── Tier 1 ────────────────────────────────────────────────────


def test_t1a_js_empty_state_colspan_matches_header():
    """v1.18.86 T1-A: the JS empty-state row's colspan must
    match the v1.15.53 11-column table header. Pre-fix it was
    10 — the v1.18.84 STATUS=! repro on /collections trailed
    an empty 11th cell at the right edge."""
    js = APP_JS.read_text()
    assert 'colspan="11" class="muted center">${msg}' in js, (
        "v1.18.86 T1-A: JS empty-state row must use colspan=11"
    )
    # Stale colspan must not survive.
    assert 'colspan="10" class="muted center">${msg}' not in js


def test_t1b_accent_red_token_replaced_with_red():
    """T1-B: `var(--accent-red, #ff6d6d)` was used at 3 sites
    but the token was never declared. Replaced with var(--red)."""
    css = APP_CSS.read_text()
    assert "var(--accent-red" not in css, (
        "v1.18.86 T1-B: --accent-red references must be gone"
    )
    assert "#ff6d6d" not in css, (
        "v1.18.86 T1-B: the hardcoded fallback hex must be gone "
        "(it didn't even match canonical --red #ff6b6b)"
    )


def test_t1c_probe_output_pre_has_no_raw_px_inline():
    """T1-C: the v1.18.85 probe `<pre>` previously carried
    inline `max-height: 480px; overflow:auto; margin-top: 12px`.
    Moved to a CSS rule (#probe-plex-themes-output)."""
    html = SETTINGS_HTML.read_text()
    pre_idx = html.index('id="probe-plex-themes-output"')
    pre_chunk = html[pre_idx:pre_idx + 400]
    # Only style allowed is display:none (canonical initial-hidden).
    assert "480px" not in pre_chunk
    assert "12px" not in pre_chunk
    assert "overflow:auto" not in pre_chunk
    # CSS rule exists.
    css = APP_CSS.read_text()
    assert "#probe-plex-themes-output {" in css
    rule_start = css.index("#probe-plex-themes-output {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    assert "max-height: 480px" in rule
    assert "var(--gap-" in rule  # margin-top via token


def test_t1d_probe_handler_auto_dismisses_status():
    """T1-D: v1.18.85's bindProbePlexThemes was missing the
    v1.17.5 auto-dismiss contract — status text stuck after a
    run. Fixed in v1.18.86."""
    js = APP_JS.read_text()
    fn_idx = js.index("function bindProbePlexThemes()")
    fn_end = js.find("\n  function ", fn_idx + 1)
    body = js[fn_idx:fn_end if fn_end != -1 else len(js)]
    assert "_autoDismissOpStatus(status" in body, (
        "v1.18.86 T1-D: probe handler must call "
        "_autoDismissOpStatus in finally"
    )


def test_t1e_upload_dlg_copy_aligned_with_manual_url_dlg():
    """T1-E: pre-fix the upload-dlg said 'DOWNLOAD ONLY' while
    manual-url-dlg (v1.18.77) said 'KEEP AS BACKUP' — sibling
    dialogs on the same P-row state had divergent copy."""
    html = _strip_jinja_comments(LIBRARY_HTML.read_text())
    # Anchor on the upload checkbox row; the matching hint
    # paragraph closes ~2000 chars after. Slice generously
    # since Jinja-comment stripping shifts offsets — anchor
    # again on the closing </label> to scope cleanly.
    upload_idx = html.index('id="upload-download-only-row"')
    upload_end = html.index("</label>", upload_idx)
    upload_chunk = html[upload_idx:upload_end]
    # Collapse whitespace so line-break wraps in the source
    # don't fool substring matches ("PROMOTE TO\n  ACTIVE" →
    # "PROMOTE TO ACTIVE").
    upload_flat = " ".join(upload_chunk.split())
    # New copy.
    assert "KEEP AS BACKUP" in upload_flat
    # Old copy must be gone from rendered markup.
    assert "DOWNLOAD ONLY" not in upload_flat
    # Hint references the canonical promotion path (v1.18.77+).
    assert "PROMOTE TO ACTIVE" in upload_flat
    # And the deprecated v1.15.x "PUSH TO PLEX later" wording is gone.
    assert "PUSH TO PLEX later" not in upload_flat


def test_t1f_keep_as_backup_label_drops_slash_prefix():
    """T1-F: per DESIGN_SYSTEM §3 the `// ` prefix is for
    buttons + section titles only. v1.18.77's KEEP AS BACKUP
    rename slipped the prefix onto a checkbox label. v1.18.86
    removes it."""
    html = LIBRARY_HTML.read_text()
    # Both checkbox surfaces (upload-dlg + manual-url-dlg) carry
    # the bare label.
    assert "KEEP AS BACKUP (Plex keeps serving)" in html
    # No `// ` prefix anywhere on the label.
    assert "// KEEP AS BACKUP" not in html


# ── Tier 2 ────────────────────────────────────────────────────


def test_t2ab_recovery_flip_uses_dedicated_primitive():
    """T2-A/B: pre-fix the intent-flip block was rendered as a
    second `.recovery-section-body` wrapper with inline
    `style="margin-top:8px"` + per-caption `style="margin-left:10px"`.
    v1.18.86 introduces `.recovery-section-flip` primitive."""
    css = APP_CSS.read_text()
    assert ".recovery-section-flip {" in css, (
        "v1.18.86 T2-A: .recovery-section-flip CSS primitive must exist"
    )
    rule_start = css.index(".recovery-section-flip {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    # Uses var(--gap-*) for spacing (no raw px).
    assert "var(--gap-" in rule
    # Flex layout with gap, not margin-left hacks.
    assert "display: flex" in rule
    assert "gap:" in rule

    js = APP_JS.read_text()
    # JS uses the new class name on both flip surfaces.
    assert 'class="recovery-section-flip"' in js, (
        "v1.18.86 T2-A: JS must render with the new primitive"
    )
    # Raw px inline styles in the recovery flip block are gone.
    # Strip JS line comments (`// ...`) so v1.18.86's own
    # archaeology comment referencing "margin-top:8px" doesn't
    # pollute the search.
    fn_idx = js.index("intentFlipBtnsHtml")
    fn_end = js.index("section.innerHTML", fn_idx)
    block = js[fn_idx:fn_end]
    # Drop the leading `//`-comment lines (line-by-line).
    block_no_comments = "\n".join(
        line for line in block.splitlines()
        if not line.strip().startswith("//")
    )
    assert 'style="margin-top:8px"' not in block_no_comments, (
        "v1.18.86 T2-A: raw 8px margin in flip block must be gone"
    )
    assert 'style="margin-left:10px"' not in block_no_comments, (
        "v1.18.86 T2-B: raw 10px margin on caption must be gone"
    )


def test_t2c_design_system_catalogues_backup_ready_title():
    """T2-C: the new `✓ BACKUP READY — DEFERRING TO PLEX` title
    + PROMOTE/MARK pair were undocumented in §7. Documented in
    v1.18.86 so future titles + button pairs join the catalogue."""
    md = DESIGN_SYSTEM.read_text()
    assert "BACKUP READY — DEFERRING TO PLEX" in md
    assert "PROMOTE TO ACTIVE" in md
    assert "MARK AS BACKUP" in md


def test_t2e_recovery_tone_classes_cover_server_emitted_tones():
    """T2-E: pre-fix only 4 `.recovery-option-info-tone-*`
    rules existed (user/adopt/plex/cookies). Server (api.py
    api_recovery_options) emits 4 more (themerrdb/info/danger/
    magenta). The class is template-interpolated
    (`recovery-option-info-tone-${opt.tone}` at app.js:13912)
    so v1.15.89/.111 lints miss it. v1.18.86 closes the gap."""
    css = APP_CSS.read_text()
    for tone in ("themerrdb", "info", "danger", "magenta"):
        assert f".recovery-option-info-tone-{tone} " in css, (
            f"v1.18.86 T2-E: missing CSS rule for tone={tone!r} — "
            "server emits this tone but pre-fix label fell through "
            "to the default color"
        )


def test_t2f_mark_as_backup_uses_btn_warn_not_btn_info():
    """T2-F: MARK AS BACKUP is a mutating action (flips
    user_overrides.intent, cancels force-place). Per §2 table
    .btn-info is for informational non-mutating actions
    (ACCEPT UPDATE). Symmetric with PROMOTE TO ACTIVE (also
    mutating, also .btn-warn)."""
    js = APP_JS.read_text()
    # Find the MARK AS BACKUP button.
    mab_idx = js.index("// MARK AS BACKUP")
    # Walk back to find the class= attribute.
    btn_start = js.rfind('<button', 0, mab_idx)
    button_block = js[btn_start:mab_idx + 50]
    assert "btn-warn" in button_block, (
        "v1.18.86 T2-F: MARK AS BACKUP must use .btn-warn "
        "(mutating amber), not .btn-info (informational blue)"
    )
    # Old class must be gone for this button.
    assert "btn-info" not in button_block, (
        "v1.18.86 T2-F: .btn-info should not be on MARK AS BACKUP"
    )


def test_t2i_probe_button_label_tightened():
    """T2-I: '// PROBE PLEX THEME API' (6 words) shortened to
    '// RUN PROBE' (2 words) to match the 2-3 word sibling
    cadence (// PROBE TDB URLS / // REPROBE PLEX THEMES /
    // REPROBE FAILURES). Section title still carries the full
    name."""
    html = SETTINGS_HTML.read_text()
    btn_idx = html.index('id="probe-plex-themes-btn"')
    btn_end = html.index("</button>", btn_idx)
    btn_chunk = html[btn_idx:btn_end]
    assert "// RUN PROBE" in btn_chunk
    # The section's <h2> title still uses the long form
    # (canonical block name).
    assert '<h2 class="block-title">// PROBE PLEX THEME API</h2>' in html


def test_t2j_subtitle_uses_us_english():
    """T2-J: 'characterise' (British) → US English in subtitle.
    Other subtitles in the file consistently use US spelling."""
    html = _strip_jinja_comments(SETTINGS_HTML.read_text())
    assert "characterise" not in html


def test_t2k_code_primitive_exists():
    """T2-K: `code` element primitive was missing — inline
    <code> tags rendered as plain mono text with no visual
    distinction. Used at 8+ sites (file paths, env vars,
    button-label refs)."""
    css = APP_CSS.read_text()
    # Find a `code {` rule (not inside `.codeblock` selector).
    assert re.search(r"^code\s*\{", css, flags=re.MULTILINE), (
        "v1.18.86 T2-K: `code` element primitive must exist"
    )


def test_stale_palette_comment_corrected():
    """Stale doc: app.css palette comment claimed `U = blue`
    but U was renamed to violet in v1.10.14 itself. Reader
    discipline shouldn't have to memorize that the comment is
    lying."""
    css = APP_CSS.read_text()
    # Anchor on the v1.10.14 palette block.
    block_start = css.index("v1.10.14 palette")
    block_end = css.index("*/", block_start)
    block = css[block_start:block_end]
    # The U row must now say violet, not blue.
    assert "U = violet" in block
    assert "U = blue" not in block
