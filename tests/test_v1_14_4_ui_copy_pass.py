"""v1.14.4 — UI copy pass: source-agnostic labels outside the SET URL dialogs.

v1.14.0-3 widened the SoundCloud rollout into:
  - URL parser + SRC computation (v1.14.0)
  - Failure classifier + FailureKind.human source-agnostic (v1.14.1)
  - ACCEPT UPDATE confirm + SET URL live preview (v1.14.2)
  - oEmbed proxy routes by source (v1.14.3)

But a long tail of tooltip + glyph-title + menu-prompt strings
across the rendered UI still said "YouTube" specifically — wrong
on a SoundCloud-source row, and inconsistent with the source-
agnostic labels FailureKind.human now produces server-side.

v1.14.4 sweeps these:

  - library.html: 4 tooltip strings (TDB ⚠ / SRC U / ATTN ⚠ /
    ATTN !) widened to source-agnostic phrasing.
  - app.js failure-glyph human-label tables (two copies — one in
    the orphan path, one in the row-render path) rewritten to match
    FailureKind.human's source-agnostic shape.
  - app.js TDB-pill tooltip kindHuman table widened ("track or
    video" instead of bare "video", "the source" instead of
    "YouTube").
  - app.js PURGE warning + SET URL menu-item description widened
    to mention both schemes.

Tests pin via static-text guards on the rendered files so a
future refactor that re-narrows any of these surfaces back to
YouTube fails loudly.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── library.html tooltips ─────────────────────────────────────


def test_tdb_cookies_tooltip_is_source_agnostic():
    """The TDB ⚠ pill tooltip must mention both YT cookies and SC
    cookies (Go+ subscription requirement) — pre-v1.14.4 it said
    only 'YouTube cookies needed.'"""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    # The widened tooltip names both sources via parenthetical examples.
    assert "Source cookies needed" in html
    # Pre-fix copy must not survive.
    assert 'title="YouTube cookies needed."' not in html


def test_src_user_tooltip_mentions_both_sources():
    """The SRC `U` filter tooltip must mention both YouTube and
    SoundCloud — a SC user-override row gets the same U letter."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    # Look for the U-pill tooltip (anchored on the SRC row context).
    # v1.20.26: widened to include Instagram (another user-URL source).
    assert (
        'title="User-provided — manual theme URL '
        '(YouTube, SoundCloud, Instagram, or Facebook) or MP3 upload.">U</button>'
    ) in html


def test_attn_fail_tooltip_says_theme_url_not_youtube_url():
    """The ⚠ ATTN-pill tooltip describes the failure axis. Pre-fix
    it said 'YouTube URL is broken' — wrong on a SC failure."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    assert "its theme URL is broken" in html
    assert "its YouTube URL is broken" not in html


def test_attn_update_tooltip_says_theme_url_not_youtube_url():
    """The ! ATTN-pill tooltip on the pending-update axis. Same
    rationale — TDB pushes either source kind."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    assert "upstream changed the theme URL" in html
    assert "upstream changed the YouTube URL" not in html


def test_manual_url_dialog_html_comment_says_theme_not_youtube():
    """Sanity check that the dialog widening from v1.14.4 stays
    documented in the manual-url-dlg block — it seeds future
    readers' mental model of what the dialog accepts.

    v1.14.83: the original `<!-- Manual theme URL dialog -->`
    comment was orphaned ABOVE the info-dlg (a different dialog
    entirely). When info-dlg moved to base.html, the comment
    moved with it and stopped pinning anything useful in
    library.html. The actual manual-url-dlg block carries its
    own widening marker `{# v1.14.0: same widening as the
    override dialog above. #}` which is the load-bearing
    documentation now."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    # The v1.14.0 widening marker on the manual-url-dlg.
    assert "{# v1.14.0: same widening as the override dialog above. #}" in html
    # The pre-fix "YouTube URL dialog" wording must NOT survive
    # anywhere in library.html — confirms the widening rename
    # held across all the dialog blocks.
    assert "Manual YouTube URL" not in html


# ── app.js failure-glyph human-label tables ───────────────────


def _strip_line_comments(js: str) -> str:
    """Helper: drop // … line comments so a comment that documents
    a rename (and quotes the pre-fix string) doesn't trip the
    "must not appear" guards. Block comments (/* … */) are left
    alone — none of the surfaces under test use them."""
    return "\n".join(
        line for line in js.splitlines()
        if not line.lstrip().startswith("//")
    )


def test_orphan_failure_glyph_table_is_source_agnostic():
    """The orphan-row failure glyph table. Pre-fix said 'YouTube
    cookies expired' / 'Video is private' / 'Video was removed'.
    v1.14.4 mirrors FailureKind.human's 'Track or video …' /
    'Cookies expired' shape.

    v1.14.41: lower bound dropped from `>= 2` to `>= 1`. Pre-
    v1.14.41 there were two copies of this table — one in
    renderLibraryRow, one in the dead loadItems() that v1.14.41
    deleted. The surviving copy still pins the source-agnostic
    contract."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = _strip_line_comments(js_raw)
    # The bad string must appear nowhere in live code.
    assert "'YouTube cookies expired'" not in js
    # 'Video is private' / 'Video was removed' — pre-fix bare-Video
    # phrasing must not survive.
    assert "'video_private': 'Video is private'" not in js
    assert "'video_removed': 'Video was removed'" not in js
    # The widened phrasing must appear in the surviving copy.
    assert js.count("'cookies_expired': 'Cookies expired'") >= 1
    assert js.count("'video_private': 'Track or video is private'") >= 1
    assert js.count("'video_removed': 'Track or video was removed'") >= 1


def test_tdb_pill_kindhuman_table_is_source_agnostic():
    """The kindHuman map used by the TDB-pill tooltip lived next to
    a 'video was removed from YouTube' / 'network error reaching
    YouTube' phrasing. Pin the source-agnostic rewording."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = _strip_line_comments(js_raw)
    # Pre-fix YT-specific strings must be gone from live code.
    assert "'video was removed from YouTube'" not in js
    assert "'network error reaching YouTube'" not in js
    # Widened phrasings.
    assert "'track or video was removed at the source'" in js
    assert "'network error reaching the source'" in js
    # The "video is private" → "track or video is private" rename.
    assert "'video_private': 'video is private'" not in js
    assert "'track or video is private'" in js


# ── app.js prompt + menu-item copy ────────────────────────────


def test_purge_warning_mentions_both_schemes_for_set_url():
    """The PURGE confirm warning lists re-acquisition options.
    Pre-fix it said 'SET URL — manual YouTube URL'. v1.14.4
    expands to mention SoundCloud too."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = _strip_line_comments(js_raw)
    assert "SET URL — manual YouTube or SoundCloud URL" in js
    assert "SET URL — manual YouTube URL'" not in js


def test_set_url_menu_description_mentions_both_schemes():
    """The SOURCE menu item for SET URL must describe both schemes
    in its hover description — that's the discovery surface for the
    SC support."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = _strip_line_comments(js_raw)
    assert "Provide a YouTube or SoundCloud URL as a manual override." in js
    assert "Provide a YouTube URL as a manual override." not in js


# ── Cross-file consistency ────────────────────────────────────


def test_failure_kind_human_strings_match_js_render_phrasing():
    """The server's FailureKind.human values are sent to the client
    via /api/info but the client also has its own local table for
    the inline glyph (rendered before /api/info resolves). The two
    should agree on the source-agnostic phrasing — otherwise the
    glyph tooltip and the INFO dialog say different things about
    the same row.

    This isn't a literal-string equality check (the JS strips the
    prefix that /api/info shows), just that neither side mentions
    'YouTube' or bare 'Video' for the same kinds."""
    py_raw = (REPO / "app" / "core" / "downloader.py").read_text()
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = _strip_line_comments(js_raw)

    # Server side: FailureKind.human block — must not mention YouTube
    # in live code. Narrow the window precisely to the property body
    # (start = `def human` line, end = the `}[self]` closer) so a
    # neighbouring docstring on `needs_manual_override` doesn't bleed
    # into the check. Then strip # comment lines so the rationale
    # comment quoting the rename doesn't trip the guard either.
    human_block_anchor = py_raw.index("def human(self) -> str:")
    human_block_end = py_raw.index("}[self]", human_block_anchor)
    human_block_raw = py_raw[human_block_anchor:human_block_end]
    human_block_live = "\n".join(
        line for line in human_block_raw.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "YouTube" not in human_block_live, (
        "v1.14.1 already source-agnosticised these — the regression "
        "guard pins it"
    )

    # Client side: scan only the inline-glyph human-label tables.
    # Both tables sit immediately after a `failure_kind` switch.
    for marker in ("'cookies_expired': 'Cookies expired'",):
        assert marker in js


def test_no_remaining_user_facing_youtube_specificity_in_failure_strings():
    """Whole-file sweep: no live failure-label string (across the
    inline glyph tables) should still say 'YouTube' or bare 'Video'.

    This is a regression guard against a future revert that
    cherry-picks a copy fix backwards."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = _strip_line_comments(js_raw)
    # The two specific pre-fix label strings.
    forbidden = (
        "'YouTube cookies expired'",
        "'Video is private'",
        "'Video was removed'",
    )
    for s in forbidden:
        assert s not in js, (
            f"v1.14.4: {s} re-appeared — failure labels must stay "
            f"source-agnostic"
        )
