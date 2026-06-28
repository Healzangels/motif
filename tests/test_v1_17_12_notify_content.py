"""v1.17.12 — rich notification body content.

the user's screenshot of a Discord notification triggered this:

> motif: theme placed for movie/347201
> motif placed a theme at /data/media/movies/Boruto - Naruto the
> Movie (2015). Section 1 now serves this theme via Plex on next
> library refresh.

Three issues:
1. Subject says "movie/347201" not "Boruto - Naruto the Movie
   (2015)" — operators see internal IDs, not human titles.
2. Body has internal-mechanics prose ("Section 1 now serves
   this theme via Plex on next library refresh") that's
   meaningless to anyone outside the codebase.
3. No link to the actual theme that was placed, no thumbnail,
   no source classification (YouTube? SoundCloud? User URL? TDB?).

This ship introduces `app/core/notify_content.py` with:
* `enrich_item(db, media_type, tmdb_id, section_id?)` — loads
  title / year / source / theme URL / video_id / section name
  from the DB. Best-effort — corrupt / missing data gracefully
  falls back to bare-ID display so notifications never crash.
* `format_theme_added_body(ctx)` / `format_theme_deleted_body(...)`
  — compose a markdown body with bold title + optional thumbnail
  + source line. Discord / Slack / Telegram / Pushover all
  render the markdown natively; plain-text services see the
  source text which is still legible.
* New `body_format` param on `notify.dispatch` (default "text"
  for backward compatibility; per-item callers pass "markdown").

The four per-item dispatch sites (`theme_added` in worker.py,
`theme_deleted` × 3 in api.py) all migrate to the rich format.
The aggregate `themes_added_by_sync` body switches from comma-
joined titles to a markdown bullet list for the same reason —
consistent rendering across services that respect markdown.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
import pytest


REPO = Path(__file__).resolve().parent.parent
NOTIFY_PY = REPO / "app" / "core" / "notify.py"
NOTIFY_CONTENT_PY = REPO / "app" / "core" / "notify_content.py"
WORKER_PY = REPO / "app" / "core" / "worker.py"
API_PY = REPO / "app" / "web" / "api.py"


# ── DB-backed enrich_item tests ───────────────────────────────


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db)
    return db


def _seed_theme(
    db: Path, *, media_type="movie", tmdb_id=42,
    title="Boruto - Naruto the Movie", year=2015,
    upstream="themoviedb",
    yt_url="https://www.youtube.com/watch?v=abcdefghijk",
    yt_id="abcdefghijk",
):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "  youtube_url, youtube_video_id, upstream_source, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, "
            "        datetime('now'), datetime('now'))",
            (media_type, tmdb_id, title, year, yt_url, yt_id, upstream),
        )
        conn.commit()


def _seed_section(db: Path, section_id="1", title="Movies"):
    with sqlite3.connect(db) as conn:
        # plex_sections has many columns + a NOT NULL discovered_at;
        # touch only the ones the enrichment lookup reads.
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, included, "
            "   discovered_at, last_seen_at) "
            "VALUES (?, ?, 'movie', 1, "
            "        datetime('now'), datetime('now'))",
            (section_id, title),
        )
        conn.commit()


def test_enrich_item_returns_themerrdb_title_year_url(fresh_db):
    """The common-case enrichment: row in `themes`, fully
    populated. Caller gets display_title, theme_url,
    youtube_video_id, thumb_url, provenance=themerrdb."""
    from app.core.notify_content import enrich_item
    _seed_theme(fresh_db, tmdb_id=42)
    ctx = enrich_item(fresh_db, media_type="movie", tmdb_id=42)
    assert ctx["title"] == "Boruto - Naruto the Movie"
    # themes.year is TEXT (SQLite schema), so the value comes back
    # as a string. The display_title interpolation handles both —
    # we test that downstream instead of pinning the raw type.
    assert str(ctx["year"]) == "2015"
    assert ctx["display_title"] == "Boruto - Naruto the Movie (2015)"
    assert ctx["theme_url"].endswith("v=abcdefghijk")
    assert ctx["youtube_video_id"] == "abcdefghijk"
    # v1.19.55: switched hqdefault → mqdefault (uniform compact
    # thumbnails per the user's feedback).
    assert ctx["thumb_url"] == (
        "https://i.ytimg.com/vi/abcdefghijk/mqdefault.jpg"
    )
    assert ctx["provenance"] == "themerrdb"


def test_enrich_item_user_override_wins_over_tdb_url(fresh_db):
    """When a user_override.url is set, it replaces the TDB URL
    (the post-override placement uses the user URL). The
    provenance label flips to user_url. v1.17.12: also re-derives
    the youtube_video_id from the override URL so the thumb URL
    matches the actually-placed theme."""
    from app.core.notify_content import enrich_item
    _seed_theme(fresh_db, tmdb_id=99)
    with sqlite3.connect(fresh_db) as conn:
        # user_overrides column is `youtube_url` (not `url`) —
        # kept for migration safety since v1.14.0 added SoundCloud
        # but didn't rename the column. Required `set_at` + per-
        # section `section_id` (DEFAULT '' = global fallback).
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   set_at, set_by) "
            "VALUES (?, ?, ?, ?, datetime('now'), 'test')",
            ("movie", 99, "1",
             "https://www.youtube.com/watch?v=zzzzzzzzzzz"),
        )
        conn.commit()
    ctx = enrich_item(fresh_db, media_type="movie", tmdb_id=99)
    assert ctx["theme_url"].endswith("v=zzzzzzzzzzz")
    assert ctx["youtube_video_id"] == "zzzzzzzzzzz"
    assert ctx["provenance"] == "user_url"


def test_enrich_item_falls_back_to_id_when_row_missing(fresh_db):
    """No themes / no local_files row → display_title falls back
    to bare-ID form. provenance defaults to unknown."""
    from app.core.notify_content import enrich_item
    ctx = enrich_item(fresh_db, media_type="movie", tmdb_id=99999)
    assert ctx["display_title"] == "movie/99999"
    assert ctx["provenance"] == "unknown"
    assert ctx.get("title") in (None, "")
    assert ctx["thumb_url"] is None


def test_enrich_item_resolves_section_label(fresh_db):
    """section_id → plex_sections.title lookup, threaded through
    as ctx['section_label']. Used in body composers that want
    the human section name."""
    from app.core.notify_content import enrich_item
    _seed_theme(fresh_db, tmdb_id=1)
    _seed_section(fresh_db, section_id="1", title="4K Movies")
    ctx = enrich_item(
        fresh_db, media_type="movie", tmdb_id=1, section_id="1",
    )
    assert ctx["section_label"] == "4K Movies"


def test_enrich_item_plex_orphan_upstream_marks_adopted(fresh_db):
    """A theme row with upstream_source='plex_orphan' (orphan
    promoted from sidecar scan) is the 'adopted' provenance, not
    'themerrdb'."""
    from app.core.notify_content import enrich_item
    _seed_theme(fresh_db, tmdb_id=7, upstream="plex_orphan")
    ctx = enrich_item(fresh_db, media_type="movie", tmdb_id=7)
    assert ctx["provenance"] == "adopted"


def test_enrich_item_strips_soundcloud_id_from_thumb(fresh_db):
    """`youtube_video_id` also stores 'sc-...' SoundCloud ids
    (sync.extract_video_id contract). thumb_url is YouTube-only;
    SoundCloud has no stable thumbnail URL pattern → None."""
    from app.core.notify_content import enrich_item
    _seed_theme(
        fresh_db, tmdb_id=11,
        yt_url="https://soundcloud.com/foo/bar",
        yt_id="sc-foo-bar",
    )
    ctx = enrich_item(fresh_db, media_type="movie", tmdb_id=11)
    assert ctx["thumb_url"] is None
    assert ctx["theme_url"].startswith("https://soundcloud.com/")


# ── Body / title formatting ───────────────────────────────────


def test_format_theme_added_body_full_context():
    """Rich context → bold title, source line, plain URL for
    Discord/Slack auto-embed.

    v1.17.14: the body intentionally does NOT use markdown image
    syntax. Discord (the most-common motif sink) renders
    `![alt](url)` literally — the user's screenshot caught the
    raw markdown showing up verbatim. The new shape relies on
    Discord / Slack / Telegram auto-embedding plain YouTube /
    SoundCloud URLs with a rich preview card (thumbnail + title
    + channel + play). That's strictly better than what the
    markdown image attempt would have produced."""
    from app.core.notify_content import format_theme_added_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 42,
        "display_title": "Boruto - Naruto the Movie (2015)",
        "theme_url": "https://www.youtube.com/watch?v=abcdefghijk",
        "thumb_url": (
            "https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg"
        ),
        "provenance": "themerrdb",
    }
    body = format_theme_added_body(ctx)
    # v1.17.16: the bold title is GONE — the subject line already
    # has it. Body starts with the source line.
    assert body.startswith("Source: ThemerrDB · YouTube\n"), (
        "v1.17.16: body must start with the Source line — the "
        "bold title was dropped because the notification subject "
        "already includes the title."
    )
    # The bold title must not appear at all (subject has it).
    assert "**Boruto - Naruto the Movie (2015)**" not in body, (
        "v1.17.16: the title must not appear bold in the body — "
        "it duplicates the subject line."
    )
    # Source line with provenance + platform classification.
    assert "Source: ThemerrDB · YouTube" in body
    # Plain URL on its own line (Discord auto-embeds).
    assert "https://www.youtube.com/watch?v=abcdefghijk" in body
    # v1.17.14: markdown image syntax must NOT appear.
    assert "![theme thumbnail]" not in body
    assert "[![" not in body
    assert "[Watch theme]" not in body
    # Critically: NO internal mechanics tail.
    assert "Section" not in body
    assert "library refresh" not in body
    assert "next refresh" not in body


def test_format_theme_added_body_no_thumb_still_includes_url():
    """v1.17.14: SoundCloud (no YouTube thumb) still gets a plain
    URL line so Discord can auto-embed the SoundCloud preview."""
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
    # v1.17.16: title is in the subject, not the body.
    assert "**Some Theme**" not in body
    # v1.17.14: plain URL, not markdown link wrap.
    assert "https://soundcloud.com/foo/bar" in body
    assert "[Watch theme]" not in body
    assert "Source: User URL · SoundCloud" in body


def test_format_theme_added_body_bare_id_fallback():
    """When DB lookup failed and display_title is the bare ID
    form, the body still composes — just without the URL/thumb
    lines."""
    from app.core.notify_content import format_theme_added_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 99999,
        "display_title": "movie/99999",
        "theme_url": None,
        "thumb_url": None,
        "provenance": "unknown",
    }
    body = format_theme_added_body(ctx)
    # v1.17.16: bare-id form goes in the subject (display_title);
    # body just has the source/url metadata.
    assert "**movie/99999**" not in body
    assert "Source: Unknown" in body


def test_format_theme_deleted_body_includes_actor_and_action():
    """theme_deleted body composes the same display_title + source
    shape but adds 'unmanaged by <actor>' + optional extra note.

    v1.17.14: the URL is wrapped in `<...>` to suppress Discord's
    auto-embed — a deletion notification shouldn't dominate the
    channel with a giant YouTube preview card. The URL is still
    parseable + clickable in every service; only Discord
    interprets the brackets specially."""
    from app.core.notify_content import format_theme_deleted_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 42,
        "display_title": "Boruto - Naruto the Movie (2015)",
        "theme_url": "https://youtu.be/abcdefghijk",
        "thumb_url": None,
        "provenance": "themerrdb",
    }
    body = format_theme_deleted_body(
        ctx, action="unmanaged", actor="user",
        extra_note="3 placement(s) unlinked",
    )
    # v1.17.16: title is in the subject, not the body.
    assert "**Boruto - Naruto the Movie (2015)**" not in body
    # v1.17.16: action + extra-note collapse onto one line with `·`.
    # v1.19.92: lead with the actor, not the action verb — the title
    # ("🗑️ Theme unmanaged — …") already carries the verb, so it was
    # dropped from the body.
    assert "By user · 3 placement(s) unlinked" in body
    # v1.17.14: URL wrapped in `<...>` to suppress Discord embed.
    assert "Previous theme: <https://youtu.be/abcdefghijk>" in body
    assert "Source: ThemerrDB · YouTube" in body


def test_format_titles_use_display_title():
    """The compact subject-line helpers also use the rich
    display_title — the bug in the original screenshot was the
    title showing 'movie/347201' instead of the human name."""
    from app.core.notify_content import (
        format_theme_added_title,
        format_theme_deleted_title,
    )
    ctx = {
        "media_type": "movie",
        "tmdb_id": 42,
        "display_title": "Boruto - Naruto the Movie (2015)",
    }
    add_title = format_theme_added_title(ctx)
    del_title = format_theme_deleted_title(ctx, action="unmanaged")
    assert "Boruto - Naruto the Movie (2015)" in add_title
    # Critically: NO bare-id form leaks into the subject.
    assert "movie/42" not in add_title
    assert "Boruto - Naruto the Movie (2015)" in del_title
    assert "movie/42" not in del_title


# ── notify.dispatch body_format plumbing ──────────────────────


def test_dispatch_accepts_body_format_param():
    """v1.17.12: notify.dispatch must accept a body_format kwarg
    and thread it through to the pool worker. Backward compat:
    default is 'text' so v1.17.0-era callers keep working."""
    src = NOTIFY_PY.read_text()
    # Type alias exists.
    assert "BodyFormat = Literal[" in src, (
        "v1.17.12: notify.py must declare a BodyFormat type alias."
    )
    # Dispatch signature has the param.
    idx = src.index("def dispatch(")
    sig = src[idx:idx + 800]
    assert "body_format: BodyFormat = \"text\"" in sig, (
        "v1.17.12: dispatch() must accept `body_format: BodyFormat "
        "= 'text'` (default text for backward compat with v1.17.0-"
        "era callers)."
    )
    # _send_embedded threads it to apprise.NotifyFormat.
    e_idx = src.index("def _send_embedded(")
    e_body = src[e_idx:e_idx + 2500]
    assert "body_format" in e_body, (
        "v1.17.12: _send_embedded must accept body_format."
    )
    assert "NotifyFormat.MARKDOWN" in e_body, (
        "v1.17.12: _send_embedded must map BodyFormat → "
        "apprise.NotifyFormat (so markdown actually renders)."
    )
    # _send_external POSTs format.
    x_idx = src.index("def _send_external(")
    x_body = src[x_idx:x_idx + 2000]
    assert '"format": body_format' in x_body, (
        "v1.17.12: _send_external must POST `format` in the JSON "
        "body so caronc/apprise-api fans out with markdown."
    )


# ── Call-site migration ───────────────────────────────────────


def test_worker_theme_added_uses_notify_content():
    """worker.py `_do_place` theme_added dispatch must call
    enrich_item + format_theme_added_* (not hand-roll the body)
    and pass body_format='markdown'.

    v1.19.55: the dispatch block grew with the theme_added vs
    theme_pushed discriminator (force/reason in payload) so
    the windowed slice needs to be wider to capture both
    dispatch branches."""
    src = WORKER_PY.read_text()
    # Find the theme_added dispatch block.
    idx = src.index('event_kind="theme_added"')
    # v1.21.6: widened back-window (the theme_pushed branch grew a
    # had_prior_theme comment block ahead of the theme_added else).
    # v1.24.38: widened again — the auto_restore branch (review #6) sits
    # ahead of theme_pushed/theme_added, pushing theme_added further from
    # the notify_content import at the top of the if-outcome.placed block.
    window = src[max(0, idx - 6000):idx + 1500]
    assert "from . import notify_content as _nc" in window, (
        "v1.17.12: worker.py theme_added must import notify_content."
    )
    assert "_nc.enrich_item(" in window, (
        "v1.17.12: worker.py theme_added must call enrich_item."
    )
    assert "_nc.format_theme_added_title(" in window
    assert "_nc.format_theme_added_body(" in window
    assert 'body_format="markdown"' in window, (
        "v1.17.12: theme_added must pass body_format='markdown'."
    )
    # The legacy "Section X now serves this theme via Plex" body
    # must be gone.
    assert "now serves this theme via Plex on next library refresh" not in src, (
        "v1.17.12: the verbose Plex-mechanics tail must be retired."
    )


def test_api_theme_deleted_sites_use_notify_content():
    """All 3 theme_deleted sites (UNMANAGE, FORGET, DELETE) must
    use enrich_item + format_theme_deleted_*."""
    src = API_PY.read_text()
    # Count migrations: each site should reference both helpers.
    # If any site still hand-rolls f"motif: theme unmanaged ({...})"
    # style title, this test will fail.
    assert src.count("_nc.format_theme_deleted_title(") == 3, (
        "v1.17.12: all 3 theme_deleted sites must call "
        "format_theme_deleted_title (UNMANAGE / FORGET / DELETE)."
    )
    assert src.count("_nc.format_theme_deleted_body(") == 3, (
        "v1.17.12: all 3 theme_deleted sites must call "
        "format_theme_deleted_body."
    )
    # Legacy hand-rolled strings must be gone.
    assert "hardlink placements survive at Plex's copy" not in src, (
        "v1.17.12: legacy theme_deleted/unmanage body tail "
        "must be retired."
    )


def test_themes_added_by_sync_uses_markdown_bullets():
    """The New-titles section (v1.21.6: folded into sync_completed,
    gated by themes_added_by_sync) renders titles as markdown
    bullets, and the consolidated message ships body_format=
    'markdown'."""
    src = WORKER_PY.read_text()
    idx = src.index('_events.get("themes_added_by_sync"')
    end = src.index("# v1.12.126", idx)
    block = src[idx:end]
    assert 'body_format="markdown"' in block, (
        "v1.21.6: the consolidated sync message must pass "
        "body_format='markdown'."
    )
    # v1.22.45: bullet construction moved into the section-grouping formatter
    # (notify_content.format_section_grouped_lines) so titles render under
    # Plex-section sub-headers; the worker block calls it for the New list.
    assert 'format_section_grouped_lines(' in block, (
        "v1.22.45: the New-titles section renders via the section-grouping "
        "formatter (markdown bullets under per-library headers)."
    )


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_12():
    import re
    src = (REPO / "app" / "__init__.py").read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 12), (
        f"v1.17.12: __version__ must be >= 1.17.12 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
