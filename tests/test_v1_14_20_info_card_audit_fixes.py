"""v1.14.20 — info-card audit fix bundle (H1 + H2 + H3 + M1 + M2 + M3 + L1).

Implements 7 of the 9 findings from `docs/INFO_CARD_AUDIT.md`:

  H1. video id field shows TDB id on U/A/M/P rows
  H2. PROPOSED CHANGE diff missing for SoundCloud
  H3. YouTube thumbnail breaks on SC overrides
  M1. `currently applied: —` reads as "no theme" on M/A/P
  M2. label format mismatch (themerrdb vs currently applied)
  M3. `themerrdb added/edited` show — on orphans
  L1. vestigial tdbPreviewBlock variable

Skipped: L2 (tmdb-vs-tvdb naming, wider data refactor), L3
(raw enum cosmetic, not worth a tag).

Single tag because all changes touch the same render function
(`openInfoDialog` + its helper `renderPendingUpdateDiff`); shipping
in one pass avoids merge friction and a single deploy/verify
round-trip covers every edge case.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── H1: video id derives from currentUrl, not TDB ─────────────


def test_yt_id_derivation_uses_lf_source_video_id():
    """The new ytId logic must read from `lf.source_video_id`
    (the canonical video id of what's actually playing) not
    `t.youtube_video_id` (TDB-side). Pre-fix the chain
    `ovr?.youtube_video_id || t.youtube_video_id || ...` always
    landed on the TDB id because user_overrides has no
    youtube_video_id column."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The v1.14.20 H1 marker.
    marker = "v1.14.20 (H1): rewritten to derive from currentUrl"
    assert marker in js
    block_start = js.index(marker)
    block = js[block_start:block_start + 2500]
    # New derivation reads lf.source_video_id first.
    assert "ytId = (lf && lf.source_video_id) || ''" in block
    # YT URL fallback parse only fires for youtube source.
    assert "urlSource(currentUrl) === 'youtube'" in block


def test_yt_id_no_longer_falls_back_to_tdb_video_id():
    """Regression guard: the pre-fix line

        const ytId = ovr?.youtube_video_id || t.youtube_video_id || ...

    must not survive in live code. Comment-stripped to dodge
    the rationale comment quoting the deleted shape."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "const ytId = ovr?.youtube_video_id || t.youtube_video_id" not in js


def test_yt_id_blank_when_no_current_url():
    """For M/A/P rows currentUrl is '' (per v1.12.46 design).
    The new derivation gates the entire computation on
    `if (currentUrl)`, so ytId stays '' for those rows. The
    `video id: —` field becomes honest."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    marker = "v1.14.20 (H1): rewritten to derive from currentUrl"
    block_start = js.index(marker)
    block = js[block_start:block_start + 2500]
    # The gating if.
    assert "if (currentUrl) {" in block


# ── H2: diff section is source-aware ──────────────────────────


def test_diff_section_renders_for_soundcloud_proposed_url():
    """Pre-fix renderPendingUpdateDiff bailed early on SC URLs
    via `extractYouTubeVideoId` returning null. v1.14.20
    accepts SC URLs and renders a tile without the YT
    thumbnail (audio handled inline via oembed title hydration)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function renderPendingUpdateDiff(pu, lf, t, ovr)")
    body = js[fn_anchor:fn_anchor + 8000]
    # New gate: only bail on truly unknown source.
    assert "if (newSrc === 'unknown') return ''" in body
    # The pre-fix `if (!newVid) return ''` (which dropped SC) is gone.
    assert "if (!newVid) return ''" not in body


def test_diff_tile_skips_yt_thumbnail_for_soundcloud_source():
    """SC tiles must NOT render `img.youtube.com/vi/{vid}/hqdefault.jpg`
    — that URL 404s for SC (img.youtube.com doesn't host SC art).
    The thumbHtml expression gates on `src === 'youtube' && vid`."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function renderPendingUpdateDiff(pu, lf, t, ovr)")
    body = js[fn_anchor:fn_anchor + 8000]
    assert "const thumbHtml = (src === 'youtube' && vid)" in body
    # The non-YT branch returns empty string for thumb.
    assert "thumbHtml = (src === 'youtube' && vid)" in body


def test_diff_tile_oembed_slot_works_for_both_sources():
    """The oembed title hydration is source-agnostic since v1.14.3
    (/api/source/oembed). The tile must always emit the
    `data-oembed-slot` + `data-oembed-url` attrs so SC titles
    hydrate too — the fix is keeping those attrs OUTSIDE the
    YT-only thumbHtml branch."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function renderPendingUpdateDiff(pu, lf, t, ovr)")
    body = js[fn_anchor:fn_anchor + 8000]
    # The oembed-slot / oembed-url attrs render unconditionally
    # (they sit OUTSIDE the thumbHtml branch).
    assert 'data-oembed-slot="${htmlEscape(slot)}"' in body
    assert 'data-oembed-url="${htmlEscape(url)}"' in body


def test_diff_tile_proposed_yt_still_gets_thumbnail():
    """Regression guard: a YouTube proposed update must still
    show the YT thumbnail. v1.14.20 derives newVid for the
    PROPOSED tile only when newSrc==='youtube'."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("function renderPendingUpdateDiff(pu, lf, t, ovr)")
    body = js[fn_anchor:fn_anchor + 8000]
    # newVid extraction gated on YT.
    assert "const newVid = newSrc === 'youtube'" in body
    assert "extractYouTubeVideoId(newUrl)" in body


# ── H3: bottom YT thumbnail gates on YT source ────────────────


def test_bottom_thumbnail_gates_on_youtube_url_source():
    """The card's bottom YouTube thumbnail block must check
    `urlSource(ytUrl) === 'youtube'` in addition to `ytId`.
    After H1 lands, ytId for an SC override is the
    sc-<artist>-<slug> sentinel — without the source gate the
    block would render a broken img.youtube.com 404.

    v1.15.129: the gate moved into an IIFE that branches by
    `tUrlSrc = urlSource(ytUrl)` so SoundCloud now ALSO gets a
    thumbnail block (hydrated async from oembed.thumbnail_url).
    Check for the new contract instead of the v1.14.20 inline
    ternary."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "tUrlSrc = urlSource(ytUrl)" in js
    assert "tUrlSrc === 'youtube'" in js
    # The YouTube branch still uses img.youtube.com (fast path,
    # no oembed round-trip needed since the vid is deterministic).
    assert "img.youtube.com/vi/" in js


def test_bottom_thumbnail_no_longer_gates_only_on_yt_id():
    """Regression guard: the pre-fix bare `${ytId ? ...}` gate
    must not survive — it would render the broken thumbnail on
    SC overrides post-H1."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Restrict to the body.innerHTML template area; pre-fix gate
    # was the literal `${ytId ? \`<div style="margin-top:18px`.
    assert "${ytId ? `<div style=\"margin-top:18px" not in js_raw


# ── M1: applied url label ─────────────────────────────────────


def test_currently_applied_label_renamed_to_applied_url():
    """The label "currently applied" becomes "applied url" so
    the empty-dash case (M/A/P rows with no URL) reads
    consistently — "applied url: —" is honest, "currently
    applied: —" reads as "no theme" even though the theme is
    playing.

    v1.24.9: the label is now computed via `appliedUrlLabel`
    (a const) rather than a hardcoded literal, so a backup-only
    row can read "backup url" instead. The default (non-backup)
    value must still be 'applied url', and the <dt> must render
    that variable."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # v0.51.61: the (youtube) platform span after the label is dropped — the dt is
    # now just the appliedUrlLabel variable.
    assert "<dt>${appliedUrlLabel}</dt>" in js
    assert "? 'backup url'" in js and ": 'applied url';" in js
    assert "<dt>currently applied ${currentUrl ?" not in js


# ── M2: harmonized parens label format ────────────────────────


def test_themerrdb_label_drops_platform_tag():
    """v0.51.61 (the user, polish): the `(youtube)`/`(soundcloud)` PLATFORM tag
    that v1.14.20 M2 added to the themerrdb url label is dropped — it repeated
    identically across the 3 URL rows (wrapping each label to 2 lines) and the
    value already shows the full URL. tdbSrcTag now carries ONLY the (pending)
    suffix (not derivable from the value); tdbDeadTag + the dt are unchanged."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # tdbSrcTag reduced to the pending suffix (no urlSource platform label).
    assert "const tdbSrcTag = _pendingSuffix;" in js
    # the platform-tag fragment is gone from the whole file.
    assert "(${urlSource(tdbUrl)})" not in js
    # the render site + the dead-url tag are preserved.
    assert "<dt>themerrdb url${tdbSrcTag}${tdbDeadTag}</dt>" in js


def test_previous_url_label_has_no_platform_tag():
    """v0.51.61: the previous-url platform tag is dropped too — the label is now
    just `previous url` (prevSrcTag var removed)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const prevSrcTag" not in js
    assert "<dt>previous url</dt>" in js
    assert "(${urlSource(previousUrl)})" not in js


def test_old_themerrdb_label_format_is_gone():
    """Regression guard against the pre-fix inline `themerrdb
    {source}` shape that used `urlSourceLabel(...).split(' ')[0]`
    to extract just the source word."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "themerrdb ${urlSourceLabel(tdbUrl).split(' ')[0]}" not in js


# ── M3: themerrdb added/edited hidden on orphans ──────────────


def test_themerrdb_added_edited_hidden_for_plex_orphan_rows():
    """The two themerrdb timestamp rows must be wrapped in the
    same `upstream_source === 'plex_orphan' ? '' : ...` gate
    that the `themerrdb url` row already uses. Pre-fix orphans
    rendered both rows with empty `—` cells — pure noise.

    Anchor on the imdb row (`<dt>imdb</dt>`) which is unique to
    the openInfoDialog body template — there are many
    `body.innerHTML = ...` assignments in app.js, so a generic
    anchor would land on the wrong template."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the unique imdb row.
    # v0.50.64: scope to the FULL card (openInfoDialog) first — the
    # universal bare card (renderBareInfoCard) also emits a
    # `<dt>imdb</dt><dd>${imdb}</dd>` row earlier in the file, so a bare
    # js.index() now lands on the bare card (which has no plex_orphan gate).
    card = js.index("async function openInfoDialog(")
    body_anchor = js.index("<dt>imdb</dt><dd>${imdb}</dd>", card)
    # Window covers the dlg-grid through the closing </dl>.
    block_end = js.index("</dl>", body_anchor)
    block = js[body_anchor:block_end]
    # Count plex_orphan-skip ternaries. Pre-fix: 1 (URL row).
    # Post-fix: 2 (URL row + added/edited block).
    pattern_count = block.count("t.upstream_source === 'plex_orphan' ? '' : `")
    assert pattern_count >= 2, (
        f"v1.14.20 (M3): expected at least 2 plex_orphan-skip "
        f"ternaries (URL row + added/edited block), found "
        f"{pattern_count}"
    )
    # And the added/edited rows live inside the second ternary.
    assert "themerrdb added" in block
    assert "themerrdb edited" in block


# ── L1: vestigial tdbPreviewBlock removed ─────────────────────


def test_tdb_preview_block_variable_removed():
    """The vestigial `const tdbPreviewBlock = '';` variable
    (left behind by v1.13.16's preview removal) is gone. Same
    for the `${tdbPreviewBlock}` template interpolation site."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "const tdbPreviewBlock = '';" not in js
    assert "${tdbPreviewBlock}" not in js


def test_tdb_preview_block_supporting_vars_removed():
    """The two unused locals that fed the deleted preview block
    (tdbVidId, onDiskVidId) are also gone — pure deadcode
    cleanup."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "const tdbVidId = t.youtube_video_id || null;" not in js
    assert "const onDiskVidId = (lf && lf.source_video_id) || null;" not in js


# ── Defensive: body.innerHTML template literal still parseable ─


def test_body_innerhtml_template_literal_balanced():
    """Defensive check after the L1 deletion + H3 gate change:
    the body.innerHTML template literal must still have
    balanced backticks. Counts opening/closing backtick + `;`
    sequences — a stray `*/;` outside the literal (which I
    introduced + reverted while writing this tag) would break
    this count."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Find the body.innerHTML opening backtick + the closing
    # backtick that ends with `;`.
    open_pos = js.index("body.innerHTML = `")
    # Walk to the template's closing — should be `\n    `;` shape.
    # The simplest check: there's exactly ONE template that
    # spans the dlg-grid + auditPlaceholder + historySection.
    close_marker = "${historySection}\n    `;"
    assert close_marker in js, (
        "v1.14.20: body.innerHTML template literal must end with "
        "${historySection} followed by a closing backtick + ; — "
        "if this fails, the template parsing is broken"
    )
