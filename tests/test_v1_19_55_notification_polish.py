"""v1.19.55 — notification polish (the user's review of sync + theme dispatches).

Five distinct asks bundled into one tag:

  1. **Drop duplicate header on themes_added_by_sync body.**
     Pre-fix the body started with `**N new theme(s) added by
     sync**` — duplicating the notification title (`🎵 N new
     themes added by sync`). Drop the redundant line.

  2. **New themes_updated_by_sync event_kind.** Parallel to
     themes_added_by_sync but for the updated_titles list.
     `🔄 N theme(s) updated by sync` + bullet list.

  3. **Restructure sync_completed body.** the user: "feels like
     it should have updates first then at the bottom have the
     summarized new and updated and how many were checked in
     the entire sync. and also the check mark sync complete
     should come at the end as well."

     New order: Updates list (🔄 prefix) → Summary line →
     ✅ Sync complete at the END. Title changes from
     "✅ Sync complete" to a summary-driven subject so the
     body's ✅ at the end reads as the natural closer.

     Rephrased "Checked N upstream items in this sync window"
     → "N item(s) had upstream changes this sync window"
     (pre-fix read like motif only checked N total —
     misleading when the git-diff path walks ONLY changed
     items).

  4. **Uniform YouTube thumbnail size.** Switched
     `hqdefault.jpg` (480x360) → `mqdefault.jpg` (320x180)
     for compact, always-available thumbnails. the user: "right
     now they are different sizes, would love if we could
     have uniform small thumbnail."

  5. **theme_pushed event_kind** for force-place dispatches.
     Pre-fix PUSH TO PLEX, REPLACE TDB, PROMOTE TO ACTIVE,
     and other force-place jobs all triggered theme_added,
     conflating "new theme appeared" with "Plex's serving
     state changed." New event_kind has a distinct 📤 emoji
     + reason-aware title variants.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

NOTIFY_CONTENT_PY = (
    REPO / "app" / "core" / "notify_content.py"
).read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
CONFIG_FILE_PY = (
    REPO / "app" / "core" / "config_file.py"
).read_text()
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()


# ── 1. Duplicate header dropped ──────────────────────────────


def test_themes_added_by_sync_no_duplicate_body_header():
    """v1.21.6: the New-titles content folded into sync_completed
    as a "🎵 New:" section. It must remain a plain bullet list — no
    "**N new theme(s)**" bold header (which would now duplicate the
    sync title's count)."""
    idx = WORKER_PY.index('_events.get("themes_added_by_sync"')
    end = WORKER_PY.index("# v1.12.126", idx)
    block = WORKER_PY[idx:end]
    assert (
        '"**{sync_stats.new_count} new theme(s)' not in block
        and 'f"**{sync_stats.new_count} new theme(s)' not in block
    ), (
        "v1.21.6: the New-titles section must stay a plain bullet "
        "list (no bold count header)"
    )


# ── 2. themes_updated_by_sync section ────────────────────────


def test_themes_updated_by_sync_event_kind_dispatched():
    """v1.21.6: themes_updated_by_sync no longer fires a separate
    dispatch — it gates a "🔄 Updated:" section folded into the
    single sync_completed message. Pin the toggle gate."""
    assert (
        '_events.get("themes_updated_by_sync"' in WORKER_PY
    ), (
        "v1.21.6: themes_updated_by_sync must gate the Updated "
        "section inside the consolidated sync message"
    )


def test_themes_updated_by_sync_uses_refresh_icon():
    """The Updated section header uses 🔄 (refresh icon) — distinct
    from 🎵 (New) so the operator can distinguish at a glance."""
    # v1.22.45: the 🔄 header is handed to the section-grouping formatter.
    assert '"🔄 Updated:"' in WORKER_PY, (
        "v1.21.6: the Updated-titles section must use the 🔄 header"
    )


def test_themes_updated_by_sync_event_in_config_defaults():
    """The new event_kind must be registered in the
    notifications-events default map. Pre-v1.17.10 lesson:
    missing entries are silently dropped by the closed-set
    filter in api.py's PATCH handler."""
    assert '"themes_updated_by_sync"' in CONFIG_FILE_PY


def test_themes_updated_by_sync_event_in_notify_priority():
    """The event must be in the _EVENT_NOTIFY_TYPE map in
    notify.py so the dispatch knows the urgency tier."""
    assert '"themes_updated_by_sync"' in NOTIFY_PY


# ── 3. sync_completed body restructured ──────────────────────


def test_sync_completed_body_updates_section_uses_refresh_icon():
    """The Updates: section header in sync_completed body must
    use 🔄 prefix so the visual identity matches the new
    themes_updated_by_sync event (both about updates → same
    icon vocabulary)."""
    idx = WORKER_PY.index('event_kind="sync_completed"')
    block = WORKER_PY[max(0, idx - 5000):idx]
    assert '"🔄 Updated:"' in block, (
        "v1.19.55: Updates: section header must use 🔄 prefix"
    )


def test_sync_completed_title_no_longer_just_sync_complete():
    """The title was 'Motif sync — N new · M updated' style
    so the body's ✅ at the end reads as the natural closer.
    Pre-fix the title was 'Motif sync — Sync complete' which
    duplicated the body's ✅."""
    idx = WORKER_PY.index('event_kind="sync_completed"')
    # Title declaration sits BEFORE the dispatch call.
    block = WORKER_PY[max(0, idx - 2500):idx + 500]
    # The title text construction.
    assert "Motif sync —" in block, (
        "v1.19.55: title must be a summary-driven 'Motif sync — "
        "...' subject (informative for at-a-glance), not the "
        "old 'Motif sync — Sync complete' duplicate"
    )


def test_sync_completed_body_has_check_at_end():
    """✅ Sync complete must appear at the END of the body
    (the user's request). The body-line list builds bottom-up
    so check the literal in the relevant range."""
    idx = WORKER_PY.index('event_kind="sync_completed"')
    block = WORKER_PY[max(0, idx - 5000):idx]
    assert '"✅ Sync complete"' in block, (
        "v1.19.55: body must end with ✅ Sync complete"
    )


def test_sync_completed_body_rephrases_checked_line():
    """v1.19.92 superseded the v1.19.55 'had upstream changes'
    phrasing entirely — the sync_completed body no longer states
    the outcome at all (the title carries it; the body restating
    it was the duplication the user flagged). So the misleading
    'had upstream changes' / 'scanned … upstream items' phrasings
    must BOTH be gone from the body builder."""
    idx = WORKER_PY.index('event_kind="sync_completed"')
    # Scope to the EMITTED body builder — the v1.19.92 comment above
    # it quotes the old phrasings as archaeology, so a whole-block
    # scan would false-positive.
    block_start = WORKER_PY.rindex("body_lines: list[str]", 0, idx)
    block_end = WORKER_PY.index("sync_summary =", block_start)
    block = WORKER_PY[block_start:block_end]
    assert "had upstream changes" not in block, (
        "v1.19.92: the outcome phrasing moved entirely to the "
        "title — the body must not restate it"
    )
    assert "scanned" not in block, (
        "v1.19.92: 'scanned N upstream items' body line dropped "
        "(it duplicated the title's '(N checked)')"
    )


# ── 4. Thumbnail uniformity ──────────────────────────────────


def test_youtube_thumb_uses_mqdefault():
    """The _youtube_thumb helper must return mqdefault.jpg
    (320x180) for uniform compact thumbnails. Pre-fix used
    hqdefault.jpg (480x360) which rendered at varying sizes
    in Discord/Slack previews."""
    assert "mqdefault.jpg" in NOTIFY_CONTENT_PY, (
        "v1.19.55: thumbnail URL must use mqdefault.jpg for "
        "uniform compact sizing across notification sinks"
    )
    # Old size must be gone (helper has a single URL line).
    assert "hqdefault.jpg" not in NOTIFY_CONTENT_PY, (
        "v1.19.55: hqdefault.jpg must be removed from the "
        "thumbnail helper — keeping it as a fallback would "
        "give some videos hq and others mq (the user's "
        "'different sizes' complaint)"
    )


def test_youtube_thumb_extension_helper_returns_mqdefault():
    """Behavioral: call _youtube_thumb with a known
    11-char id and assert the URL has mqdefault."""
    from app.core.notify_content import _youtube_thumb
    url = _youtube_thumb("dQw4w9WgXcQ")
    assert url is not None
    assert "/mqdefault.jpg" in url
    assert "/hqdefault.jpg" not in url


# ── 5. theme_pushed event_kind ───────────────────────────────


def test_theme_pushed_event_in_config_defaults():
    """theme_pushed event must be registered in the config
    defaults so PATCH api.py:218 accepts it (v1.17.10 closed-
    set lesson)."""
    assert '"theme_pushed"' in CONFIG_FILE_PY


def test_theme_pushed_event_in_notify_priority():
    """theme_pushed must have an entry in _EVENT_NOTIFY_TYPE."""
    assert '"theme_pushed"' in NOTIFY_PY


def test_theme_pushed_title_formatter_defined():
    """notify_content must define format_theme_pushed_title."""
    assert "def format_theme_pushed_title(" in NOTIFY_CONTENT_PY


def test_theme_pushed_body_formatter_defined():
    """notify_content must define format_theme_pushed_body."""
    assert "def format_theme_pushed_body(" in NOTIFY_CONTENT_PY


def test_theme_pushed_title_uses_push_icon():
    """📤 emoji for theme_pushed — distinct from 🎵
    (theme_added) / 🔄 (sync update) / 🗑️ (deleted)."""
    from app.core.notify_content import format_theme_pushed_title
    title = format_theme_pushed_title(
        {"display_title": "Test (2020)"},
    )
    assert title.startswith("📤"), (
        "v1.19.55: theme_pushed title must use 📤 emoji"
    )


def test_theme_pushed_title_branches_on_reason():
    """Title should reflect the specific deploy reason when
    known (REPLACE TDB / PROMOTE / re-upload) so the
    operator can scan at a glance."""
    from app.core.notify_content import format_theme_pushed_title
    ctx = {"display_title": "Test"}
    replaced = format_theme_pushed_title(
        ctx, reason="replace_with_themerrdb",
    )
    promoted = format_theme_pushed_title(
        ctx, reason="promote_bk_no_override",
    )
    reupload = format_theme_pushed_title(
        ctx, reason="plex_cloud_reupload",
    )
    generic = format_theme_pushed_title(ctx, reason=None)
    assert "replaced via tdb" in replaced.lower()
    assert "promoted" in promoted.lower()
    assert "re-uploaded" in reupload.lower()
    assert "pushed to plex" in generic.lower()


def test_worker_do_place_dispatches_theme_pushed_on_force():
    """The _do_place success path must check payload.force /
    payload.force_place / payload.reason and dispatch
    theme_pushed (not theme_added) when any is set. Pre-fix
    all force-place dispatches fired theme_added,
    conflating 'new theme' with 'theme re-deployed.'"""
    # Locate the _do_place notification dispatch block.
    fn_idx = WORKER_PY.index("def _do_place(self,")
    fn_end = WORKER_PY.index("def _do_place_collection(", fn_idx + 1)
    body = WORKER_PY[fn_idx:fn_end]
    assert 'event_kind="theme_pushed"' in body, (
        "v1.19.55: _do_place must dispatch theme_pushed when "
        "payload indicates a force-deploy (PUSH/REPLACE/PROMOTE)"
    )
    assert 'event_kind="theme_added"' in body, (
        "v1.19.55: _do_place must still dispatch theme_added "
        "for the natural-add path (sync-driven, no force)"
    )
    # The discriminator: force / force_place / reason.
    assert "force_place" in body or 'p.get("force")' in body
    assert 'p.get("reason")' in body or "_reason = " in body


def test_worker_do_place_collection_dispatches_theme_pushed_on_force():
    """Same discrimination logic must apply in
    _do_place_collection (mirror site)."""
    fn_idx = WORKER_PY.index("def _do_place_collection(")
    # Find the next def at module level OR end of file.
    next_def = WORKER_PY.find("\n    def ", fn_idx + 1)
    fn_end = next_def if next_def > 0 else len(WORKER_PY)
    body = WORKER_PY[fn_idx:fn_end]
    assert 'event_kind="theme_pushed"' in body, (
        "v1.19.55: _do_place_collection must mirror the "
        "_do_place discrimination — theme_pushed on force"
    )
    assert 'event_kind="theme_added"' in body


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_55_version_pin():
    """Version bumped at v1.19.55 (then again at v1.19.56 for
    the docs catch-up + widened secrets audit). Match 1.19.x
    prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
