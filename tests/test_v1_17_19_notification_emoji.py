"""v1.17.19 — notification subject emoji + cleanup.

the user's feedback on the v1.17.16 shipped notifications:

> could we add some emoji or more indicators of a removal event
> vs an add event. same would be the case for other events type
> to make it clear what is happening at a glance. Would like to
> have professional looking discord notifications.

The screenshot showed two stacked notifications — `theme
forgotten` and `theme added` — with near-identical subject
prefixes (`motif: theme forgotten — …` / `motif: theme added —
…`). The verb is buried mid-string; at a glance the user can't
tell which is which without reading the whole line.

## Emoji set (one per event kind)

The set is designed for at-a-glance scannability with minimal
overload:

* **🎵** Theme add (per-row + aggregate + new TDB)
* **🗑️** Theme remove (unmanage / forget / delete — all
  destructive)
* **✅** Sync / bulk action complete
* **❌** Sync failed
* **🍪** YouTube cookies needed (specific glyph for the
  scenario)
* **🗄️** Disk space low (v1.23.84: was 💾, which collided with
  theme_backed_up's 💾)
* **⚠️** Worker restarted (unclean shutdown signal)
* **🆕** Release update available
* **🧪** Notification test (TEST NOTIFICATION button)

## "motif:" prefix dropped (except attribution-critical cases)

Discord/Slack already attribute the bot in the message header
("Motif APP"). The redundant `motif:` / `motif sync:` prefix
doubled up. Now dropped for:
- sync / bulk action subjects
- theme add / remove
- disk_low / cookies_needed / worker_restarted

Kept on:
- `release_available` ("🆕 motif vX.Y.Z available" — vX.Y.Z
  alone is too vague)
- `notification test` ("🧪 motif vX.Y.Z — notification test"
  — operator setting up Apprise verifies attribution)
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
NOTIFY_CONTENT_PY = REPO / "app" / "core" / "notify_content.py"
WORKER_PY = REPO / "app" / "core" / "worker.py"
MAIN_PY = REPO / "app" / "main.py"
SCHEDULER_PY = REPO / "app" / "core" / "scheduler.py"
API_PY = REPO / "app" / "web" / "api.py"
APP_INIT = REPO / "app" / "__init__.py"


# ── Per-item formatters (notify_content.py) ───────────────────


def test_theme_added_title_uses_music_note_emoji():
    """🎵 = "theme add" indicator. Subject must lead with the
    emoji + drop the old `motif:` prefix."""
    from app.core.notify_content import format_theme_added_title
    title = format_theme_added_title({
        "display_title": "Foo (2020)",
        "media_type": "movie",
        "tmdb_id": 1,
    })
    assert title.startswith("🎵 "), (
        "v1.17.19: theme_added title must lead with the 🎵 "
        "emoji for at-a-glance add-vs-remove signaling."
    )
    assert "Theme added" in title
    assert title == "🎵 Theme added — Foo (2020)"


def test_theme_deleted_title_uses_trash_emoji_for_all_actions():
    """🗑️ = "theme remove" indicator. All three destructive
    actions (unmanaged / forgotten / deleted) share the same
    emoji so the visual signal is "this row lost its theme"
    regardless of which destructive verb the operator chose."""
    from app.core.notify_content import format_theme_deleted_title
    ctx = {
        "display_title": "Foo (2020)",
        "media_type": "movie",
        "tmdb_id": 1,
    }
    for action in ("unmanaged", "forgotten", "deleted"):
        title = format_theme_deleted_title(ctx, action=action)
        assert title.startswith("🗑️ "), (
            f"v1.17.19: theme_deleted title (action={action}) "
            "must lead with 🗑️."
        )
        # Verb stays so the specific destructive shape is still
        # legible.
        assert action in title
        assert "Foo (2020)" in title


def test_titles_drop_redundant_motif_prefix():
    """Discord shows the bot name ("Motif APP") above every
    message. The `motif:` prefix on the subject was redundant
    visual chrome; the v1.17.19 cleanup drops it."""
    from app.core.notify_content import (
        format_theme_added_title, format_theme_deleted_title,
    )
    ctx = {
        "display_title": "Foo (2020)",
        "media_type": "movie",
        "tmdb_id": 1,
    }
    add = format_theme_added_title(ctx)
    delete = format_theme_deleted_title(ctx, action="unmanaged")
    for title in (add, delete):
        assert "motif:" not in title.lower(), (
            f"v1.17.19: the redundant 'motif:' prefix must be "
            f"dropped (the bot username already attributes). "
            f"Got: {title!r}"
        )


# ── Inline titles in worker.py / main.py / scheduler.py / api.py ─


def test_sync_completed_title_uses_check_emoji():
    """v1.19.55 moved the ✅ from the title to the END of the
    body so the rendered notification reads naturally
    (the user's request: "check mark sync complete should come
    at the end as well"). The ✅ is now in the body, not the
    title — pin the body's literal."""
    src = WORKER_PY.read_text()
    # Locate the sync_completed dispatch.
    idx = src.index('event_kind="sync_completed"')
    block = src[max(0, idx - 5000):idx]
    assert '"✅ Sync complete"' in block, (
        "v1.19.55: ✅ Sync complete must appear in the body's "
        "trailing line (moved from the title)"
    )


def test_sync_failed_title_uses_x_emoji():
    src = WORKER_PY.read_text()
    # v1.23.82: unified onto the "Motif sync — {state}" stem so the pair
    # reads uniformly with sync_completed; ❌ stays for at-a-glance alarm.
    assert 'title="❌ Motif sync — failed"' in src, (
        "v1.23.82: sync_failed must use the ❌ Motif sync — failed "
        "subject (was '❌ Sync failed')."
    )


def test_themes_added_by_sync_title_uses_music_note():
    src = WORKER_PY.read_text()
    # v1.21.6: themes_added_by_sync folded into the sync_completed
    # summary — its content is now a "🎵 New:" section header
    # rather than a standalone notification title. The 🎵 emoji
    # (matching the per-row theme_added family) carries over.
    # v1.22.45: the 🎵 header is handed to the section-grouping formatter.
    assert '"🎵 New:"' in src, (
        "v1.21.6: the New-titles section must use the 🎵 header."
    )


def test_disk_low_title_uses_disk_emoji():
    # v1.23.84: 🗄️ (was 💾, which collided with theme_backed_up's 💾).
    src = WORKER_PY.read_text()
    assert 'title=f"🗄️ Low disk space — {free_mb}MB free"' in src


def test_cookies_needed_title_uses_cookie_emoji():
    src = WORKER_PY.read_text()
    assert 'title="🍪 YouTube cookies expired or missing"' in src


def test_worker_restarted_title_uses_warning_emoji():
    src = MAIN_PY.read_text()
    assert 'title="⚠️ Worker restarted (unclean shutdown)"' in src


def test_release_available_title_keeps_motif_attribution():
    """release_available is an attribution-critical event —
    "vX.Y.Z available" alone is too vague (a user gets it,
    thinks "available for what?"). Keep "motif" in the title
    along with the 🆕 prefix."""
    src = SCHEDULER_PY.read_text()
    assert (
        "title=f\"🆕 motif {payload['tag_name']} available\""
    ) in src


def test_bulk_action_completed_titles_use_check_emoji():
    src = API_PY.read_text()
    assert (
        'title=f"✅ Bulk PROBE TDB done — {done}/{total}"'
    ) in src, (
        "v1.17.19: bulk PROBE TDB completion must use the "
        "✅ prefix."
    )
    assert (
        'title=f"✅ Bulk LPS done — {n_targets}"'
    ) in src, (
        "v1.17.19: bulk LPS completion must use the ✅ prefix."
    )


def test_test_notification_title_keeps_motif_attribution():
    """Test pings are attribution-critical (the operator is
    setting up Apprise and needs to confirm this came from the
    motif instance they expect). Keep "motif vX.Y.Z" + 🧪
    prefix."""
    src = API_PY.read_text()
    assert (
        'title=f"🧪 motif v{motif_version} — notification test"'
    ) in src


# ── Counter-pin: no remaining "motif:" or "motif sync" titles ─


def test_no_lingering_motif_colon_titles():
    """Counter-pin: after the v1.17.19 cleanup, no `title=` in
    a dispatch site should still use the bare `motif:` or
    `motif sync` prefix (except for the attribution-critical
    release + test cases pinned above).

    This is a structural lint — if a new dispatch site is added
    in the future and the developer pastes the old prefix shape,
    this test fails."""
    # v1.20.56: scan notify_content.py too — the theme_pushed /
    # backup_ready / theme_lost_* title formatters live there, outside
    # the original worker/main/scheduler/api scan.
    targets = [WORKER_PY, MAIN_PY, SCHEDULER_PY, API_PY, NOTIFY_CONTENT_PY]
    bad_patterns = (
        'title="motif:',
        'title=f"motif:',
        'title="motif sync',
        'title=f"motif sync',
    )
    findings: list[str] = []
    for p in targets:
        src = p.read_text()
        for pat in bad_patterns:
            if pat in src:
                # Allow the explicit motif-attribution exceptions
                # (release + test). Check context.
                idx = src.index(pat)
                window = src[max(0, idx - 100):idx + 200]
                if "release" in window.lower():
                    continue
                if "notification test" in window.lower():
                    continue
                findings.append(f"{p.name}: {pat}")
    assert not findings, (
        "v1.17.19: dispatch title strings should not use the "
        "redundant `motif:` prefix (drop it — the bot username "
        "attributes). Found:\n  " + "\n  ".join(findings)
    )


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_19():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 19), (
        f"v1.17.19: __version__ must be >= 1.17.19 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
