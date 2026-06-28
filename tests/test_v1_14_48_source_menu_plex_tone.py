"""v1.14.48 — SOURCE-menu LET PLEX SERVE actions get the amber Plex tone.

the user repro on v1.14.47: the new SOURCE-menu LET PLEX SERVE +
ADOPT + LET PLEX SERVE buttons rendered in the default plain
styling (no Plex-amber tint) despite being wired with
`tone: 'plex'` in `menuItemHtml(..., { tone: 'plex' })`.

Root cause: the SOURCE-menu tinting reads `extras.tone` and emits
the class `lib-source-{tone}`. The CSS file had
`.btn.lib-source-cloud` (amber) provisioned in v1.11.14 for a
future "P/cloud" action that never landed, but no
`.btn.lib-source-plex` rule. v1.14.47's two new callers used
`tone: 'plex'` to mirror the recovery-card vocab (`tone: "plex"`
→ `btn-plex` → amber) and silently fell back to default styling.

Fix: rename `.btn.lib-source-cloud` → `.btn.lib-source-plex` so
the SOURCE-menu tone vocab aligns with the recovery-card vocab.
The rename is safe — the cloud class had zero callers (`grep -rn
lib-source-cloud` returned only the declaration). Same color
values; same hover treatment.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── CSS: lib-source-plex defined; lib-source-cloud removed ───


def test_lib_source_plex_class_defined_and_amber():
    """`.btn.lib-source-plex` must exist + use the amber color
    variable so SOURCE-menu LET PLEX SERVE buttons render
    Plex-amber."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    assert ".btn.lib-source-plex {" in css
    anchor = css.index(".btn.lib-source-plex {")
    block = css[anchor:anchor + 400]
    assert "var(--amber)" in block
    # Hover state present + amber border on hover (matches the
    # other lib-source-* tones' hover pattern).
    assert ".btn.lib-source-plex:hover:not(:disabled) {" in css


def test_lib_source_cloud_class_removed():
    """Regression guard: the v1.11.14 `.btn.lib-source-cloud`
    rule declaration must not survive the v1.14.48 rename. If a
    future refactor re-adds it, the SOURCE-menu vocab fragments
    again (cloud and plex would both map to amber, with callers
    disagreeing on which to use).

    Anchor on the rule declaration shape (`.btn.lib-source-cloud
    {`) rather than the bare class name, so the v1.14.48 rename
    marker comment that mentions the old name doesn't trip the
    guard."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    assert ".btn.lib-source-cloud {" not in css
    assert ".btn.lib-source-cloud:hover" not in css


# ── JS: SOURCE-menu LET PLEX SERVE entries pass tone: 'plex' ─


def test_let_plex_serve_source_menu_entry_uses_plex_tone():
    """The SOURCE-menu LET PLEX SERVE menuItemHtml call must
    pass `tone: 'plex'` so the rendered button picks up the
    amber `lib-source-plex` styling."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    anchor = js.index("'purge-revert-to-plex', 'LET PLEX SERVE'")
    # The extras object sits after the label + tip args, ~600
    # chars in the same menuItemHtml() call.
    block = js[anchor:anchor + 800]
    assert "tone: 'plex'" in block


def test_adopt_and_let_plex_serve_source_menu_entry_uses_plex_tone():
    """Same contract on the ADOPT + LET PLEX SERVE SOURCE-menu
    entry."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    anchor = js.index("'adopt-and-let-plex-serve', 'ADOPT + LET PLEX SERVE'")
    block = js[anchor:anchor + 800]
    assert "tone: 'plex'" in block


def test_menu_item_html_comment_documents_plex_tone():
    """The menuItemHtml tone vocab comment must list `plex` (not
    `cloud`) so future contributors writing a new SOURCE-menu
    entry pick the right name. v1.14.48 also notes the rename
    rationale inline."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function menuItemHtml(act, label, tip, extras = {}) {")
    body = js[fn_anchor:fn_anchor + 6500]
    # The new vocab line.
    assert "plex      = P (amber)" in body
    # The v1.14.48 rename marker.
    assert "v1.14.48: renamed `cloud` → `plex`" in body
    # The pre-rename `cloud` vocab line must NOT survive (would
    # mislead a future contributor into using a non-existent class).
    assert "cloud     = P (amber)" not in body


# ── Reuse pin: recovery-card btn-plex still amber ────────────


def test_btn_plex_class_still_amber():
    """Sanity: the recovery-card `.btn.btn-plex` class (used by
    the failure-recovery LET PLEX SERVE button at api.py's
    purge-and-ack option) must still be amber. v1.14.48 doesn't
    touch this class; pin so a future refactor doesn't lose
    parity with the SOURCE-menu lib-source-plex."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    assert ".btn-plex" in css
    anchor = css.index(".btn-plex")
    block = css[anchor:anchor + 400]
    assert "var(--amber)" in block
