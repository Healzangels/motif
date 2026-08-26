"""v1.18.80 — Apprise dispatch on backup_ready_to_deploy.

Builds on v1.18.79's plex_enum log.warning detection. Same
detection logic now ALSO fires an Apprise notification so the
user doesn't have to grep docker logs to see backup-ready
transitions.

Per the user's v1.18.77 q4 design choice: "notify only, don't
auto-promote." Motif logs + notifies; the user keeps control
of when to deploy via // PROMOTE TO ACTIVE.

## What's new

- **notify.py**: 'backup_ready_to_deploy' added to LEVEL_BY_KIND
  (info — actionable but not an error).
- **config_file.py**: 'backup_ready_to_deploy' → True (ON by
  default). Rare AND actionable, so default-on without spam risk.
- **notify_content.py**: `format_backup_ready_title` (🎯 emoji)
  + `format_backup_ready_body` (markdown body with override
  URL + // PROMOTE TO ACTIVE call-to-action).
- **plex_enum.py**: notify.dispatch wired into the existing
  v1.18.79 backup_ready_to_deploy detection branch. Loads
  Settings() inside the block (same pattern as v1.16.0
  tvdb_bridge). Best-effort: try/except + log.debug on
  failure so a notify-side issue doesn't interrupt the
  enum loop.
- **settings.html**: new checkbox under NOTIFICATIONS for the
  toggle, with hint explaining the trigger + recovery path.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── notify.py event_kind registered ─────────────────────────


def test_notify_py_lists_backup_ready_to_deploy():
    """LEVEL_BY_KIND must include backup_ready_to_deploy so the
    dispatcher accepts it. info-level (not warning) — Plex
    deciding to stop serving its own theme isn't a motif
    problem, it's an actionable signal for the user."""
    src = (REPO / "app" / "core" / "notify.py").read_text()
    assert '"backup_ready_to_deploy":' in src
    # Level is info.
    body_flat = re.sub(r"\s+", " ", src)
    assert (
        '"backup_ready_to_deploy": "info"' in body_flat
    ), (
        "v1.18.80: event_kind must be info-level (not warning) — "
        "the transition itself is fine; the user gets to decide "
        "whether to promote"
    )


# ── config_file.py default ──────────────────────────────────


def test_config_default_is_on():
    """Default ON because the event is rare (Plex doesn't drop
    its own themes often) AND actionable (the user's backup is
    now eligible to deploy). Default-on without spam risk."""
    src = (REPO / "app" / "core" / "config_file.py").read_text()
    fn_idx = src.index("_DEFAULT_NOTIFY_EVENTS")
    fn_end = src.index("}", fn_idx)
    block = src[fn_idx:fn_end]
    assert '"backup_ready_to_deploy":  True' in block, (
        "v1.18.80: default must be True (ON-by-default)"
    )


def test_config_default_documented():
    """The default-True choice should be documented inline so
    a future audit knows WHY this is the only per-row notification
    defaulting ON (theme_added / theme_deleted default OFF)."""
    src = (REPO / "app" / "core" / "config_file.py").read_text()
    fn_idx = src.index("_DEFAULT_NOTIFY_EVENTS")
    fn_end = src.index("}", fn_idx)
    block = src[fn_idx:fn_end]
    body_flat = " ".join(block.split())
    assert "rare AND actionable" in body_flat or "rare" in body_flat.lower(), (
        "v1.18.80: marker should explain why default ON is safe "
        "(low frequency)"
    )


# ── notify_content.py formatters ────────────────────────────


def test_format_backup_ready_title_present():
    """Subject must use the 🎯 emoji to distinguish from 🎵
    (theme added) / 🗑️ (deleted) / ⚠ (failure). At-a-glance
    scannability per v1.17.19's emoji-mapping convention."""
    src = (REPO / "app" / "core" / "notify_content.py").read_text()
    assert "def format_backup_ready_title(" in src
    assert "🎯 Backup ready" in src


def test_format_backup_ready_body_includes_override_url():
    """Body must accept an optional `override_url` arg and
    include it inline. Discord/Slack render URLs as links."""
    src = (REPO / "app" / "core" / "notify_content.py").read_text()
    assert "def format_backup_ready_body(" in src
    fn_idx = src.index("def format_backup_ready_body(")
    # Walk to the next top-level `def ` OR end of file — the
    # function body may contain blank lines within its
    # docstring, so a simple `\n\n` anchor cuts too early.
    nxt_def = src.find("\ndef ", fn_idx + 1)
    fn_end = nxt_def if nxt_def != -1 else len(src)
    body = src[fn_idx:fn_end]
    assert "override_url" in body
    assert "Backup URL:" in body


def test_format_backup_ready_body_names_promote_action():
    """Body must explicitly tell the user to click // PROMOTE
    TO ACTIVE. The notification is half of a call-to-action UX
    — the row's INFO card button is the other half. Names them
    explicitly so the user knows where to look."""
    src = (REPO / "app" / "core" / "notify_content.py").read_text()
    fn_idx = src.index("def format_backup_ready_body(")
    # Walk to the next top-level `def ` OR end of file — the
    # function body may contain blank lines within its
    # docstring, so a simple `\n\n` anchor cuts too early.
    nxt_def = src.find("\ndef ", fn_idx + 1)
    fn_end = nxt_def if nxt_def != -1 else len(src)
    body = src[fn_idx:fn_end]
    body_flat = re.sub(r'"\s*"', "", body)
    assert "PROMOTE TO ACTIVE" in body_flat
    assert "INFO card" in body_flat or "MOTIF INFO" in body_flat


def test_format_backup_ready_body_notes_no_auto_promote():
    """the user's v1.18.77 q4 design choice was "notify only,
    don't auto-promote." The notification body must reinforce
    that — the user keeps control."""
    src = (REPO / "app" / "core" / "notify_content.py").read_text()
    fn_idx = src.index("def format_backup_ready_body(")
    # Walk to the next top-level `def ` OR end of file — the
    # function body may contain blank lines within its
    # docstring, so a simple `\n\n` anchor cuts too early.
    nxt_def = src.find("\ndef ", fn_idx + 1)
    fn_end = nxt_def if nxt_def != -1 else len(src)
    body = src[fn_idx:fn_end]
    body_flat = re.sub(r'"\s*"', "", body)
    assert "won't auto-deploy" in body_flat or "auto-promote" in body_flat.lower()


# ── plex_enum.py dispatch wired ─────────────────────────────


def test_plex_enum_dispatches_backup_ready():
    """The existing v1.18.79 detection branch must ALSO call
    notify.dispatch with event_kind='backup_ready_to_deploy'."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    # Anchor on the dispatch call.
    assert 'event_kind="backup_ready_to_deploy"' in src
    # And the dispatch lives inside the v1.18.79 transition-
    # detection block (after the log.warning). The log.warning
    # text is split across adjacent string literals — flatten
    # the source before searching for it.
    src_strflat = re.sub(r'"\s*"', "", src)
    warn_idx = src_strflat.index(
        "backup_ready_to_deploy: Plex stopped serving theme ")
    dispatch_idx = src_strflat.index(
        'event_kind="backup_ready_to_deploy"', warn_idx)
    assert dispatch_idx > warn_idx, (
        "v1.18.80: dispatch must follow the log.warning so the "
        "v1.18.79 breadcrumb survives + the notification is "
        "additive"
    )


def test_plex_enum_dispatch_loads_settings_inline():
    """plex_enum's signature doesn't carry a Settings object —
    the dispatch block must load Settings() inline, same
    pattern as v1.16.0 tvdb_bridge uses at line ~1703."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    dispatch_idx = src.index('event_kind="backup_ready_to_deploy"')
    # v0.51.300: was a fixed backward 2500-char window (the .261 class) —
    # the dedupe block rotted it. Anchor to the transition gate instead.
    gate_idx = src.rindex("if _prior_has_theme and not it.has_theme:",
                          0, dispatch_idx)
    pre_dispatch = src[gate_idx:dispatch_idx]
    assert "from ..config import Settings" in pre_dispatch
    assert "_settings = Settings()" in pre_dispatch


def test_plex_enum_dispatch_uses_enrich_item():
    """The notification body uses ItemContext (display_title +
    source provenance + thumbnail URL). enrich_item builds this
    from the DB — same pattern as theme_added's dispatch."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    # v0.51.300: the dispatch moved to a post-txn block (the in-txn
    # dispatch deadlocked record_fire); candidates carry a dict now.
    blk_start = src.index("if _backup_ready_pending:")
    dispatch_idx = src.index('event_kind="backup_ready_to_deploy"', blk_start)
    pre_dispatch = src[blk_start:dispatch_idx]
    assert "_nc.enrich_item(" in pre_dispatch
    body_flat = re.sub(r"\s+", " ", pre_dispatch)
    assert 'media_type=_brd["mt"]' in body_flat
    assert 'tmdb_id=_brd["tid"]' in body_flat


def test_plex_enum_dispatch_passes_override_url():
    """The override URL must be passed to
    format_backup_ready_body so the notification names the
    URL the user staged. SELECTed from user_overrides inside
    the dispatch block."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    # v0.51.300: the SELECT stays at the in-txn COLLECT site; the url rides
    # the pending dict into the post-txn dispatch.
    collect_idx = src.index("_backup_ready_pending.append(")
    collect_blk = src[max(0, collect_idx - 1800):collect_idx]
    assert "SELECT youtube_url FROM user_overrides" in collect_blk
    assert "intent = 'backup'" in collect_blk
    blk_start = src.index("if _backup_ready_pending:")
    dispatch_end = src.index("_ndd.record_fire", blk_start)
    body_flat = re.sub(r"\s+", " ", src[blk_start:dispatch_end])
    assert 'override_url=_brd["ovr_url"]' in body_flat


def test_plex_enum_dispatch_uses_markdown_body_format():
    """body_format='markdown' so Discord/Slack/Telegram render
    bullets + URL links inline. Same as theme_added/sync_completed."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    dispatch_idx = src.index('event_kind="backup_ready_to_deploy"')
    post_dispatch = src[dispatch_idx:dispatch_idx + 800]
    assert 'body_format="markdown"' in post_dispatch


def test_plex_enum_dispatch_is_best_effort():
    """A notify-side failure (Apprise transport down, config
    invalid, etc.) must NOT interrupt the plex_enum loop. The
    dispatch wrapped in try/except + log.debug so the operator
    has a thread to pull on without the worker crashing.

    v1.19.41: widened post-dispatch slice 2500 → 4500 chars
    because the v1.19.41 silent-fail downgrade fix wrapped the
    log.debug in a first-occurrence WARN branch via the
    `_BACKUP_READY_NOTIFY_WARNED` module flag. The debug branch
    is still present (under `else:`) but pushed further down.
    v1.21.94: widened 2500 → 3200 — an `edition-blind OK` marker on
    the in-try backup-URL read added a few lines between try: and
    the dispatch (same structural-comment-drift class)."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    dispatch_idx = src.index('event_kind="backup_ready_to_deploy"')
    # The try statement precedes the dispatch. v0.51.300: structural anchor
    # (was a twice-widened backward 3200-char window, the .261 class).
    gate_idx = src.rindex("if _prior_has_theme and not it.has_theme:",
                          0, dispatch_idx)
    pre_dispatch = src[gate_idx:dispatch_idx]
    assert "try:" in pre_dispatch
    # The except handler follows the dispatch.
    post_dispatch = src[dispatch_idx:dispatch_idx + 4500]
    assert "except Exception as _e:" in post_dispatch
    assert "log.debug" in post_dispatch
    assert "notify dispatch suppressed" in post_dispatch


def test_plex_enum_v1_18_80_marker_explains_dispatch():
    """The v1.18.80 marker must reference the user's q4 design
    choice ('notify only — don't auto-promote') so future
    code-archaeologists know why motif doesn't auto-promote
    on this transition. The marker text is split across
    adjacent string-literal comment lines — flatten and
    collapse `# ` prefixes before substring-checking."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    # v0.51.300: the v1.18.80 design-choice narration lives at the in-txn
    # COLLECT gate now; anchor on the transition gate, span to the append.
    gate_idx = src.index("if _prior_has_theme and not it.has_theme:")
    append_idx = src.index("_backup_ready_pending.append(", gate_idx)
    pre_dispatch = src[gate_idx:append_idx]
    assert "v1.18.80" in pre_dispatch
    # Strip Python comment markers + collapse whitespace so the
    # design-choice phrase matches across line wraps.
    body_flat = re.sub(r"\s*#\s*", " ", pre_dispatch)
    body_flat = " ".join(body_flat.split())
    assert ("notify only" in body_flat
            or "user keeps control" in body_flat
            or "stays in your hands" in body_flat
            or "wait for user decision" in body_flat), (
        "v1.18.80: marker must reference the no-auto-promote "
        "design choice"
    )


# ── settings.html UI toggle ─────────────────────────────────


def test_settings_html_has_backup_ready_toggle():
    """The notifications block in /settings must include a
    checkbox for backup_ready_to_deploy so the user can opt-
    out if they don't want the ping."""
    src = (
        REPO / "app" / "web" / "templates" / "settings.html"
    ).read_text()
    assert "notifications.events.backup_ready_to_deploy" in src


def test_settings_html_toggle_label_explains_trigger():
    """The label + hint must explain WHEN the event fires
    (Plex's theme transitions away on a backup-intent row)
    and HOW to act (// PROMOTE TO ACTIVE on the INFO card).
    The hint is the only place the user reads about this
    feature outside the row itself."""
    src = (
        REPO / "app" / "web" / "templates" / "settings.html"
    ).read_text()
    toggle_idx = src.index(
        "notifications.events.backup_ready_to_deploy")
    # The form-hint <p> follows shortly after.
    block = src[toggle_idx:toggle_idx + 1500]
    assert "BACKUP READY" in block
    assert "PROMOTE TO ACTIVE" in block
    # Body explains intent='backup' anchor.
    assert "intent='backup'" in block or "backup" in block.lower()
