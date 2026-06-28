"""v1.19.89 — info card (// MOTIF INFO) style audit.

The info card was already clean (real primitives: .dlg shell,
.dlg-grid, .recovery-section, .info-scope-chip, .info-audio; all
tokenized; no hardcoded hex). The drifts were inline styles, plus
one genuine clarity gap the user flagged: the `provenance` value
(auto/manual) in the downloaded row was raw jargon.

  M1 — the source-thumbnail block's divider was a duplicated inline
       `margin-top:18px;padding-top:14px;border-top` (×2); now reuses
       the existing .dlg-section primitive + an .info-thumb-caption
       class for the centered caption.
  M2 — URL source/state tags (user/themerrdb/pending) moved off inline
       color styles onto .info-src-tag-user / -tdb / .info-tag-pending.
  L1 — probe-result + last-probed inline `margin-left:10px` →
       .info-probe-meta.
  L2 — the body item-title <h3> was styled by the brittle `.dlg h3`
       descendant selector; promoted to a class (.info-title).
  provenance — the auto/manual token now carries a tooltip explaining
       what it means (auto = motif-picked, manual = you set it).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _info_body() -> str:
    idx = APP_JS.index('<h3 class="info-title">${htmlEscape(t.title')
    # v1.22.52: 4000→4400 (the upstream plex_orphan relabel added two lines).
    # v1.24.86: 4400→4800 (the grid-into-hero move added a ~280-char comment
    # before the thumbnail block, pushing it past the old window).
    return APP_JS[idx:idx + 4800]


# ── M1: thumbnail divider reuses .dlg-section ────────────────


def test_thumbnail_divider_uses_dlg_section():
    body = _info_body()
    # No inline duplicated divider; reuses the existing primitive.
    assert 'style="margin-top:18px;padding-top:14px;border-top' not in APP_JS, (
        "v1.19.89: the thumbnail divider must reuse .dlg-section, not "
        "an inline margin/padding/border"
    )
    assert "info-thumb-caption" in body
    assert ".info-thumb-caption" in APP_CSS


# ── M2: source/state tags are classes ────────────────────────


def test_source_tags_use_classes():
    for cls in ("info-src-tag-user", "info-src-tag-tdb", "info-tag-pending"):
        assert f".{cls}" in APP_CSS, f"v1.19.89: .{cls} CSS rule missing"
        assert cls in APP_JS, f"v1.19.89: {cls} not applied in the render"
    # The inline source-tag colors are gone.
    assert 'style="color:var(--violet)">user</span>' not in APP_JS
    assert 'style="color:var(--green-bright)">themerrdb</span>' not in APP_JS


# ── L1: probe spans use a class ──────────────────────────────


def test_probe_spans_use_class():
    # The probe block is assembled into probeBtnHtml before the body
    # template, so match against the whole file.
    assert 'id="probe-result" class="muted small info-probe-meta"' in APP_JS
    assert ".info-probe-meta" in APP_CSS


# ── L2: item title is a class, not .dlg h3 ───────────────────


def test_item_title_uses_info_title_class():
    assert '<h3 class="info-title">' in APP_JS
    assert ".info-title {" in APP_CSS
    # The brittle descendant selector is gone.
    assert ".dlg h3 {" not in APP_CSS, (
        "v1.19.89: .dlg h3 promoted to the .info-title class"
    )


# ── provenance clarity tooltip ───────────────────────────────


def test_provenance_value_has_explanatory_tooltip():
    """The downloaded row's auto/manual token must carry a title
    explaining what it means (the user: 'not sure what auto or manual
    means')."""
    dl_idx = APP_JS.index("htmlEscape(lf.provenance)")
    window = APP_JS[max(0, dl_idx - 200):dl_idx + 20]
    assert "title=" in window and "auto =" in window and "manual =" in window, (
        "v1.19.89: the provenance auto/manual value must be wrapped in "
        "a span with an explanatory title"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_89_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
