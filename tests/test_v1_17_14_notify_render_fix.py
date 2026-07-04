"""v1.17.14 — notification body Discord rendering fix.

the user screenshot of a v1.17.12 Discord notification showed the
markdown image syntax rendering literally:

    motif: theme added — Elektra (2005)
    **Elektra (2005)**
    [![theme thumbnail](https://i.ytimg.com/vi/qImxW9zTVWk/...)](...)
    Source: ThemerrDB · YouTube

Two problems with the v1.17.12 body shape:

1. **Markdown image not supported.** Discord webhooks render
   bold / italic / links but DO NOT render `![alt](url)` image
   syntax — it shows literally. Slack and most other services
   are the same. Telegram's MarkdownV2 supports it via a
   different syntax, but the cross-service common-denominator
   doesn't.

2. **URL wrapped in markdown link suppresses Discord auto-embed.**
   Discord auto-embeds plain URLs that appear in a message
   (YouTube preview card with thumbnail + title + play). But
   when the URL is inside `[text](url)` brackets, Discord
   treats it as already-linked and DOESN'T auto-embed. So the
   v1.17.12 shape lost both rendering paths.

## The fix

For `theme_added`: drop the markdown image, put the URL plain
on its own line. Discord/Slack/Telegram/Matrix all auto-embed
YouTube + SoundCloud URLs natively, giving users the full
preview card (thumbnail + title + channel + play button) —
strictly better than what the markdown image attempted.

For `theme_deleted`: wrap the URL in `<...>` (Discord's
no-embed marker) so deletion notifications don't spawn giant
preview cards on every event. The URL stays clickable; the
brackets are mildly ugly on services that don't recognize
them (Telegram, Pushover) but Discord — the dominant motif
sink — gets the cleaner shape.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
NOTIFY_CONTENT_PY = REPO / "app" / "core" / "notify_content.py"
APP_INIT = REPO / "app" / "__init__.py"


def test_added_body_does_not_use_markdown_image_syntax():
    """Anti-regression: a future "let's render a thumbnail
    inline" edit must not re-introduce markdown image syntax.
    Discord webhooks render `![alt](url)` literally — that
    was the v1.17.12 bug the user caught."""
    from app.core.notify_content import format_theme_added_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 42,
        "display_title": "Elektra (2005)",
        "theme_url": "https://www.youtube.com/watch?v=qImxW9zTVWk",
        "thumb_url": "https://i.ytimg.com/vi/qImxW9zTVWk/hqdefault.jpg",
        "provenance": "themerrdb",
    }
    body = format_theme_added_body(ctx)
    assert "![" not in body, (
        "v1.17.14: markdown image syntax must NOT appear — "
        "Discord renders it literally. See the user's screenshot "
        "in v1.17.14 commit message."
    )


def test_added_body_url_is_plain_for_auto_embed():
    """v1.24.31 flipped this: the YouTube URL now ships wrapped in <...> to
    SUPPRESS Discord's auto-embed (its rich card sized to the source video —
    landscape vs Shorts portrait — looked ragged in a mixed feed). The uniform
    mqdefault thumb attaches instead. The URL stays on its own line + clickable,
    just without the unfurl. Pre-v1.24.31 it asserted the bare URL."""
    from app.core.notify_content import format_theme_added_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 42,
        "display_title": "Elektra (2005)",
        "theme_url": "https://www.youtube.com/watch?v=qImxW9zTVWk",
        "thumb_url": "https://i.ytimg.com/vi/qImxW9zTVWk/hqdefault.jpg",
        "provenance": "themerrdb",
    }
    body = format_theme_added_body(ctx)
    lines = body.split("\n")
    url = "https://www.youtube.com/watch?v=qImxW9zTVWk"
    assert f"<{url}>" in lines, (
        f"v1.24.31: theme_added wraps the YouTube URL in <...> to suppress the "
        f"variable-size auto-embed (uniform thumb attaches instead). "
        f"Got body:\n{body}"
    )
    # still no markdown link wrap (that would kill the clickability differently)
    assert "](https://" not in body


def test_added_body_does_not_wrap_url_in_link_syntax():
    """`[Watch theme](url)` would suppress Discord auto-embed.
    The v1.17.12 shape with `[![alt](thumb)](url)` had the
    same effect — the URL became part of a link, so Discord
    didn't auto-embed."""
    from app.core.notify_content import format_theme_added_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 11,
        "display_title": "Some Theme",
        "theme_url": "https://soundcloud.com/foo/bar",
        "thumb_url": None,
        "provenance": "user_url",
    }
    body = format_theme_added_body(ctx)
    assert "](https://" not in body, (
        "v1.17.14: URL must not appear inside markdown link "
        "syntax — `[text](url)` suppresses Discord auto-embed."
    )


def test_added_body_order_is_source_url():
    """Order matters: source → URL. Discord renders the text
    content above the auto-embed card, so the URL is the last
    line — the user reads source provenance before the preview
    card appears below.

    v1.17.16: dropped the title check (the bold title was
    retired — subject line already has it)."""
    from app.core.notify_content import format_theme_added_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 42,
        "display_title": "Elektra (2005)",
        "theme_url": "https://www.youtube.com/watch?v=qImxW9zTVWk",
        "thumb_url": "https://i.ytimg.com/vi/qImxW9zTVWk/hqdefault.jpg",
        "provenance": "themerrdb",
    }
    body = format_theme_added_body(ctx)
    # v1.17.16: no bold title to check.
    assert "**Elektra (2005)**" not in body
    source_idx = body.index("Source: 🟢 ThemerrDB")
    url_idx = body.index("https://www.youtube.com")
    assert source_idx < url_idx, (
        f"v1.17.16: order must be source → URL, got:\n{body}"
    )


def test_deleted_body_wraps_url_in_no_embed_brackets():
    """v1.17.14: deletion notifications use `<url>` (Discord's
    no-embed marker) so a deletion event doesn't spawn a giant
    YouTube preview card — the user's intent is "I'm tracking
    that this happened" not "show me the theme again"."""
    from app.core.notify_content import format_theme_deleted_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 42,
        "display_title": "Elektra (2005)",
        "theme_url": "https://youtu.be/qImxW9zTVWk",
        "thumb_url": None,
        "provenance": "themerrdb",
    }
    body = format_theme_deleted_body(
        ctx, action="unmanaged", actor="user",
    )
    assert "Previous theme: <https://youtu.be/qImxW9zTVWk>" in body, (
        "v1.17.14: deleted body must wrap the previous URL in "
        "Discord's `<url>` no-embed marker so the deletion "
        "notification doesn't dominate the channel."
    )


def test_added_body_handles_missing_url_gracefully():
    """When the URL is missing (rare — e.g. an adopted sidecar
    whose source URL we never captured), the body skips the
    URL line cleanly. Bold title + source line still render."""
    from app.core.notify_content import format_theme_added_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 99,
        "display_title": "Some Title",
        "theme_url": None,
        "thumb_url": None,
        "provenance": "manual",
    }
    body = format_theme_added_body(ctx)
    # v1.17.16: title is in the subject, not the body.
    assert "**Some Title**" not in body
    assert "Source: 🟤 Manual sidecar" in body
    # No "None" leaked anywhere.
    assert "None" not in body
    assert "<None>" not in body


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_14():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 14), (
        f"v1.17.14: __version__ must be >= 1.17.14 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
