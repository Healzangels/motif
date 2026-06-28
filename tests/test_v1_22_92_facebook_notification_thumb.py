"""v1.22.92 — Facebook notification: suppress login-wall unfurl, embed thumb.

the user's repro: the theme_added Discord notification for the Castle
Impossible Facebook theme rendered FB's "Log in or sign up to view"
card instead of a thumbnail — Discord's unfurler can't see behind
Facebook's login wall (YouTube/SoundCloud unfurl fine).

Fix: _render_url_lines is the chokepoint for the four auto-embed
formatters (added / pushed / backed-up / available). For Facebook
URLs it wraps the URL in <...> (Discord's no-embed marker — kills
the login card, URL stays clickable) and appends the thumbnail
motif already resolved (the SET URL preview persists it to the
oembed_cache DB tier) as a bare image URL, which Discord renders
as an image embed. fbcdn URLs are signed with expiry, but Discord
fetches once at post time and proxies the bytes.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
NOTIFY = (REPO / "app" / "core" / "notify_content.py").read_text()

FB_URL = ("https://www.facebook.com/HGTV/videos/"
          "castle-impossible-once-upon-a-time/705368522162731/")
FB_THUMB = "https://scontent-bos5-1.xx.fbcdn.net/v/t15.5256-10/x.jpg?oe=6A"
TS = "2026-06-11T12:00:00"


@pytest.fixture
def fb_theme_db(tmp_path: Path):
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   youtube_url, youtube_video_id, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 90001, 'Castle Impossible', "
            "        'castle impossible', '2025', ?, "
            "        'fb-705368522162731', 'plex_orphan', ?, ?)",
            (FB_URL, TS, TS),
        )
        conn.execute(
            "INSERT INTO oembed_cache (url, payload, fetched_at) "
            "VALUES (?, ?, ?)",
            (FB_URL, json.dumps({
                "title": "Castle Impossible: Once Upon a Time...",
                "author_name": "HGTV",
                "thumbnail_url": FB_THUMB,
            }), TS),
        )
        conn.commit()
    return db_path


def test_enrich_pulls_fb_thumb_from_oembed_cache(fb_theme_db):
    from app.core.notify_content import enrich_item
    ctx = enrich_item(fb_theme_db, media_type="movie", tmdb_id=90001)
    assert ctx.get("thumb_url") == FB_THUMB


def test_added_body_suppresses_unfurl_and_omits_thumb_line(fb_theme_db):
    """v1.22.94 superseded the v1.22.92 bare-image-URL line (it
    rendered the signed fbcdn URL as a wall of link text) — the thumb
    now travels as an apprise attachment, NOT a body line."""
    from app.core.notify_content import enrich_item, format_theme_added_body
    ctx = enrich_item(fb_theme_db, media_type="movie", tmdb_id=90001)
    body = format_theme_added_body(ctx)
    assert f"<{FB_URL}>" in body, (
        "v1.22.92: the FB URL must be angle-bracketed so Discord "
        "doesn't render the login-wall card"
    )
    assert FB_THUMB not in body, (
        "v1.22.94: the raw fbcdn URL must NOT appear in the body"
    )


def test_youtube_body_unchanged(fb_theme_db):
    """v1.24.31 flipped this: YouTube URLs now ship wrapped in <...> (auto-embed
    suppressed) so the uniform mqdefault thumb attaches instead of Discord's
    variable-size card (landscape vs Shorts portrait). The raw i.ytimg URL still
    never appears in the body — it travels as an apprise attachment."""
    db_path = fb_theme_db
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   youtube_url, youtube_video_id, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 90002, 'The Mole', 'the mole', '2022', "
            "        'https://www.youtube.com/watch?v=uJy8KTiMkbI', "
            "        'uJy8KTiMkbI', 'themoviedb', ?, ?)",
            (TS, TS),
        )
        conn.commit()
    from app.core.notify_content import enrich_item, format_theme_added_body
    ctx = enrich_item(db_path, media_type="movie", tmdb_id=90002)
    body = format_theme_added_body(ctx)
    # v1.24.31: angle-bracketed now (auto-embed suppressed), URL still present.
    assert "<https://www.youtube.com/watch?v=uJy8KTiMkbI>" in body
    # the i.ytimg thumb never appears in the body — it travels as an attachment.
    assert "i.ytimg.com" not in body


def test_fb_without_cached_thumb_still_suppresses_unfurl(tmp_path):
    """Cache miss (e.g. theme added without ever opening the
    preview): no image line, but the login-wall card is still
    suppressed — strictly better than before."""
    db_path = tmp_path / "m.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   youtube_url, youtube_video_id, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 90001, 'Castle Impossible', "
            "        'castle impossible', '2025', ?, "
            "        'fb-705368522162731', 'plex_orphan', ?, ?)",
            (FB_URL, TS, TS),
        )
        conn.commit()
    from app.core.notify_content import enrich_item, format_theme_added_body
    ctx = enrich_item(db_path, media_type="movie", tmdb_id=90001)
    assert ctx.get("thumb_url") is None
    body = format_theme_added_body(ctx)
    assert f"<{FB_URL}>" in body
    assert "fbcdn" not in body


def test_proxied_cache_path_rejected(fb_theme_db):
    """A same-origin proxy path in the cache (the UI's rewritten
    variant) is useless off-site — must not leak into Discord."""
    with sqlite3.connect(fb_theme_db) as conn:
        conn.execute(
            "UPDATE oembed_cache SET payload = ? WHERE url = ?",
            (json.dumps({"thumbnail_url":
                         "/api/source/ig-thumbnail?url=x"}), FB_URL),
        )
        conn.commit()
    from app.core.notify_content import enrich_item
    ctx = enrich_item(fb_theme_db, media_type="movie", tmdb_id=90001)
    assert ctx.get("thumb_url") is None


def test_all_four_autoembed_formatters_use_the_chokepoint():
    # v1.24.38: +1 for format_theme_auto_restored_body (review #6) — it also
    # routes its URL line through the _render_url_lines chokepoint.
    assert NOTIFY.count("lines.extend(_render_url_lines(ctx))") == 5, (
        "added / pushed / backed-up / available / auto-restored all route "
        "their URL line through _render_url_lines"
    )
    # the deleted formatter keeps its own <...> shape (no embed at
    # all is correct there).
    assert 'lines.append(f"Previous theme: <{url}>")' in NOTIFY
