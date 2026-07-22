"""v1.17.12: rich notification body composition.

The v1.17.0 → v1.17.4 dispatch sites all hand-rolled their own
title + body strings interpolating raw `media_type` / `tmdb_id` /
`section_id` values. the user's screenshot of a Discord embed
showed why that breaks down — the notification body read
"placed for movie/347201" instead of the title "Boruto - Naruto
the Movie (2015)", and tail prose described internal Plex
mechanics rather than what the user actually wants to see.

This module centralizes the per-item enrichment so every event
that touches a (media_type, tmdb_id, [section_id]) tuple gets:

* the actual title + year (looked up from `themes` or
  `local_files`, depending on whether the row is TDB-managed or
  orphan)
* a source-provenance string (themerrdb / user URL /
  manual sidecar / adopted / unknown)
* the theme URL (YouTube, SoundCloud, etc.)
* a YouTube-derived thumbnail URL where available (other
  platforms don't ship a stable thumbnail URL pattern)
* the section name (resolved from `plex_sections.title`)

The dispatcher then composes a markdown body — services that
render markdown (Discord, Slack, Telegram, Pushover, Matrix,
…) display titles + thumbnails + clickable URLs; plain-text
services see the markdown source which is still readable.

Lookups are best-effort: on DB-read failure or missing row the
helper returns a minimal payload (just media_type / tmdb_id)
and the body falls back to the ID-only form. Notification
dispatch must never crash the caller (the v1.17.0 contract).

Copy standard (v1.19.92): the TITLE carries the headline —
emoji + outcome + identity (item title / count / status). The
BODY carries ONLY what the title can't hold: source/provenance,
the theme URL, the action to take, error/breakdown detail, and
the trailing ✅/⚠ status line. A body must never repeat the
item title, restate the title's headline phrase, or repeat a
count already shown in the title. This applies to EVERY motif
notification (the inline ones in worker.py / api.py / scheduler.py
too), not just the formatters here. The piecemeal de-dup of
v1.17.16 / v1.19.55 / v1.19.69 became a uniform rule here after
the user's 2026-05-29 "title + body say the same thing" report.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Literal, TypedDict

from .db import get_conn
from .editions import edition_label_for_folder, prettify_edition_key


log = logging.getLogger(__name__)


# v1.17.12: source-provenance string set. Mirrors the v1.10.x SRC
# letter axis on the library page but expanded for prose. Keep in
# sync with the letter map in CLAUDE.md → SRC letter axis.
ProvenanceKind = Literal[
    "themerrdb",     # T — TDB-managed (upstream_source ∈ imdb/themoviedb)
    "user_url",      # U — manual youtube_url override in user_overrides
    "user_upload",   # U — v1.18.58: explicit MP3 upload via UPLOAD MP3 dialog
                     #     Pre-v1.18.58 this collapsed into 'adopted' for
                     #     plex_orphan rows because the upstream-source
                     #     branch fired before local_files was consulted.
                     #     the user's BASTARD!! repro: uploaded an mp3,
                     #     Discord said "Source: Adopted" — wrong.
    "manual",        # M — sidecar a human placed without motif
    "adopted",       # A — sidecar matched + motif owns the inode
    "plex_served",   # P — themerr-plex embed / Plex Pass cloud
    "unknown",       # — no theme / failed to resolve
]


class ItemContext(TypedDict, total=False):
    """Enriched notification context for a per-item event."""
    media_type:       str            # 'movie' | 'tv'
    tmdb_id:          int
    section_id:       str | None
    title:            str
    year:             int | None
    display_title:    str            # "Title (Year)" or "Title [· Edition]"
    edition:          str            # "Extended Edition" etc. ('' = standard) — DISPLAY label
    edition_key:      str            # v0.51.220: RAW key ('' = standard) — persisted to
                                     # notifications.edition_key for edition-exact click-through
    provenance:       ProvenanceKind
    theme_url:        str | None
    youtube_video_id: str | None
    thumb_url:        str | None
    section_label:    str | None     # "// 4K MOVIES"-style human name


def _youtube_thumb(video_id: str | None) -> str | None:
    """YouTube exposes a deterministic thumbnail URL for any
    video id at /vi/{id}/mqdefault.jpg. Other source platforms
    (SoundCloud) don't ship a stable equivalent, so this is
    YouTube-only by design.

    v1.19.55: switched hqdefault (480x360) → mqdefault (320x180)
    for uniform compact thumbnails. the user's feedback: "right
    now they are different sizes, would love if we could have
    uniform small thumbnail." Both YouTube sizes are 4:3 with
    side-bars; mqdefault renders smaller + more compact in
    Discord/Slack notification previews. mqdefault is the
    always-available size (some videos lack maxresdefault but
    every video has mqdefault).
    """
    if not video_id:
        return None
    # The `youtube_video_id` column also stores `sc-<...>` ids
    # for SoundCloud (see sync.extract_video_id). Filter those out
    # — only true 11-char YouTube ids get a thumb URL.
    # v1.22.90: ig-/fb- too — an 8-char shortcode/id would make an
    # 11-char prefixed value that slips the length check.
    if (len(video_id) != 11
            or video_id.startswith(("sc-", "ig-", "fb-"))):
        return None
    return f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg"


def enrich_item(
    db_path: Path, *,
    media_type: str, tmdb_id: int,
    section_id: str | None = None,
    edition_key: str = "",
    fallback_title: str = "",
) -> ItemContext:
    """Best-effort: load title + source + URL + thumb from DB so a
    per-item notification renders something human-readable.

    Lookup order is deliberate — TDB-managed rows win over user-
    override rows over orphan/local-files. The first match wins;
    we don't merge across rows. (User overrides legitimately
    replace the TDB URL; reporting the TDB URL in a notification
    triggered by a U-source replacement would be wrong.)

    Returns a partially-populated dict on missing/corrupt data
    rather than raising — the contract is "never crash the
    caller" (v1.17.0). Caller can safely call `.get(...)`."""
    ctx: ItemContext = {
        "media_type": media_type,
        "tmdb_id": tmdb_id,
        "section_id": section_id,
        # v0.51.220: carry the RAW edition_key (not just the ctx["edition"] display
        # label below) so notify.dispatch can persist it and the inbox click-through
        # opens that exact cut. '' = standard; callers with no edition pass the default.
        "edition_key": edition_key,
    }
    try:
        with get_conn(db_path) as conn:
            # 1. ThemerrDB-managed row (the common case).
            row = conn.execute(
                """
                SELECT title, year, youtube_url, youtube_video_id,
                       upstream_source
                FROM themes
                WHERE media_type = ? AND tmdb_id = ?
                LIMIT 1
                """,
                (media_type, tmdb_id),
            ).fetchone()
            if row:
                ctx["title"] = row["title"] or ""
                ctx["year"] = row["year"]
                ctx["theme_url"] = row["youtube_url"]
                ctx["youtube_video_id"] = row["youtube_video_id"]
                # plex_orphan upstream means motif inherited the
                # row from a Plex-side adopt/orphan resolution
                # rather than from ThemerrDB itself.
                if row["upstream_source"] == "plex_orphan":
                    ctx["provenance"] = "adopted"
                else:
                    ctx["provenance"] = "themerrdb"
            # 2. User-URL override may stack on top — replaces the
            #    theme_url for any post-override placement. Column
            #    is `youtube_url` despite the v1.14.0 rename to
            #    support SoundCloud URLs (kept for migration
            #    safety — see CLAUDE.md historical-name pattern).
            #    Per-section override preferred over global ('')
            #    fallback when section_id is known.
            if section_id:
                uo = conn.execute(
                    """
                    SELECT youtube_url FROM user_overrides
                    WHERE media_type = ? AND tmdb_id = ?
                      AND section_id IN (?, '')
                      AND youtube_url IS NOT NULL
                      AND youtube_url != ''
                    ORDER BY (section_id = ?) DESC
                    LIMIT 1
                    """,
                    (media_type, tmdb_id, section_id, section_id),
                ).fetchone()
            else:
                uo = conn.execute(
                    """
                    SELECT youtube_url FROM user_overrides
                    WHERE media_type = ? AND tmdb_id = ?
                      AND youtube_url IS NOT NULL
                      AND youtube_url != ''
                    LIMIT 1
                    """,
                    (media_type, tmdb_id),
                ).fetchone()
            if uo and uo["youtube_url"]:
                ctx["theme_url"] = uo["youtube_url"]
                # Re-derive video id from the override URL.
                from .sync import extract_video_id
                ctx["youtube_video_id"] = extract_video_id(uo["youtube_url"])
                ctx["provenance"] = "user_url"
            # 3. local_files lookup — ALWAYS consulted now (v1.18.58).
            #    Pre-fix this branch only fired when title was still
            #    missing (orphans whose themes row title was blank).
            #    For plex_orphan rows with a title set by the themes
            #    row, local_files was skipped entirely → the step-1
            #    `provenance='adopted'` default leaked into the
            #    notification even when local_files.source_kind told
            #    a different story.
            #
            #    the user's BASTARD!! repro: UPLOAD MP3 sets
            #    source_kind='upload' (api.py:9819) AND
            #    upstream_source='plex_orphan' (synthesized orphan
            #    theme). Step-1 said "adopted", step-3 was skipped
            #    because title was set, Discord lied.
            #
            #    Fix: walk local_files unconditionally, and let its
            #    source_kind override the step-1 upstream-based
            #    default. The override hierarchy is:
            #      source_kind='upload'    → 'user_upload'  (NEW)
            #      source_kind='url'       → 'user_url'     (also leave
            #                                user_overrides path above)
            #      source_kind='themerrdb' → 'themerrdb'
            #      source_kind='adopt'     → 'adopted'
            #      provenance='manual'     → 'manual' (sidecar a human
            #                                placed; source_kind blank)
            #      else                    → preserve step-1 default
            # v1.18.58: SELECT only columns that exist on local_files.
            # The pre-fix SELECT included title / year / youtube_url
            # which DO NOT exist on local_files — those live on
            # themes. The OperationalError was swallowed by the
            # outer except (v1.17.12 best-effort breadcrumb), so the
            # local_files lookup silently never ran. Latent bug since
            # the table was created. Caught while fixing the user's
            # provenance leak.
            lf = conn.execute(
                """
                SELECT source_video_id, provenance, source_kind
                FROM local_files
                WHERE media_type = ? AND tmdb_id = ?
                LIMIT 1
                """,
                (media_type, tmdb_id),
            ).fetchone()
            if lf:
                # video_id backfill — only set if step-1 didn't.
                if not ctx.get("youtube_video_id"):
                    ctx["youtube_video_id"] = lf["source_video_id"]
                # Provenance override based on the most-specific
                # signal — source_kind is the canonical T/U/A/M
                # discriminator (CLAUDE.md SRC letter axis +
                # _SRC_LETTER_SQL line 917-919).
                sk = lf["source_kind"]
                if sk == "upload":
                    ctx["provenance"] = "user_upload"
                elif sk == "url":
                    # Already 'user_url' if user_overrides hit above;
                    # also set here for the case where overrides got
                    # cleared but the source_kind stamp survives.
                    ctx["provenance"] = "user_url"
                elif sk == "themerrdb":
                    ctx["provenance"] = "themerrdb"
                elif sk == "adopt":
                    ctx["provenance"] = "adopted"
                elif lf["provenance"] == "manual":
                    # Manual-provenance with no source_kind = a
                    # sidecar a human placed that motif tracks but
                    # didn't classify. Rare path; the SRC=M label.
                    ctx["provenance"] = "manual"
                # else: leave step-1's upstream-derived default in
                # place (themerrdb / adopted for plex_orphan).
            # 4. Section human name — for the body's "section X" tail.
            if section_id:
                sr = conn.execute(
                    "SELECT title FROM plex_sections WHERE section_id = ?",
                    (section_id,),
                ).fetchone()
                if sr and sr["title"]:
                    ctx["section_label"] = sr["title"]
            # 5. v1.21.75: edition label so multi-edition titles name the
            #    specific cut in notifications. v1.21.76: route the
            #    {edition-X} parse through the editions.py chokepoint —
            #    prefer the RAW folder casing, fall back to a pretty-printed
            #    edition_key when no folder matches. '' = standard.
            if edition_key:
                plex_mt = "show" if media_type == "tv" else media_type
                er = conn.execute(
                    "SELECT folder_path FROM plex_items "
                    "WHERE guid_tmdb = ? AND media_type = ? "
                    "  AND edition_key = ? LIMIT 1",
                    (str(tmdb_id), plex_mt, edition_key),
                ).fetchone()
                _elabel = edition_label_for_folder(
                    er["folder_path"] if er else None)
                ctx["edition"] = _elabel or prettify_edition_key(edition_key)
            # 6. v0.51.124: title fallback from plex_items. A row with no
            #    `themes` entry (a Plex-cloud P-row with no ThemerrDB match) gets
            #    no title from step 1, so a "💾 Theme backed up" / "💔 Theme
            #    lost" notification would render the bare "media_type/tmdb_id"
            #    id. Plex knows the title — fill it here so every titleless-row
            #    notification names the content (the user). Runs only when
            #    nothing above set a title; a real `themes` title always wins.
            #    The explicit fallback_title arg (v0.51.123) still backstops the
            #    rare case where the row was deleted between enum + dispatch and
            #    this lookup misses. guid_tmdb is INTEGER; the str() coercion
            #    matches via column affinity (same as the edition lookup above).
            if not ctx.get("title"):
                # v0.51.126: match guid_tmdb OR the theme-linkage tmdb. The
                # notification tmdb is COALESCE(themes.tmdb_id, guid_tmdb)
                # (plex_enum.py:2427), so a theme-linked row with a NULL
                # guid_tmdb (anime / non-TMDB agents) is only reachable via pi.theme_id →
                # themes.tmdb_id — a bare guid_tmdb match missed it.
                _plex_mt = "show" if media_type == "tv" else media_type
                pr = conn.execute(
                    "SELECT pi.title, pi.year FROM plex_items pi "
                    "WHERE (pi.guid_tmdb = ? OR pi.theme_id IN "
                    "        (SELECT id FROM themes "
                    "          WHERE tmdb_id = ? AND media_type = ?)) "
                    "  AND pi.media_type = ? "
                    "  AND (? = '' OR pi.section_id = ?) LIMIT 1",
                    (str(tmdb_id), tmdb_id, media_type, _plex_mt,
                     section_id or "", section_id or ""),
                ).fetchone()
                if pr and pr["title"]:
                    ctx["title"] = pr["title"]
                    if pr["year"] and not ctx.get("year"):
                        ctx["year"] = pr["year"]
    except Exception as e:  # noqa: BLE001
        # v1.17.12: class-9 breadcrumb — best-effort lookup must
        # not crash the caller, but a recurring failure should be
        # debuggable from logs without enabling apprise debug.
        log.debug("notify_content.enrich_item failed for "
                  "%s/%s: %s", media_type, tmdb_id, e)
    # Derive thumb + display_title after all sources are settled.
    ctx["thumb_url"] = _youtube_thumb(ctx.get("youtube_video_id"))
    # v1.22.92: Facebook themes get their thumbnail from the
    # persistent oembed cache — Discord's unfurler can't see behind
    # FB's login wall, so _render_url_lines suppresses the unfurl
    # and embeds this image instead.
    if not ctx.get("thumb_url"):
        _u = ctx.get("theme_url") or ""
        if "facebook.com" in _u or "fb.watch" in _u:
            ctx["thumb_url"] = _cached_oembed_thumb(db_path, _u)
    # v0.51.123: fall back to the caller-supplied Plex title (e.g. a lost row's
    # plex_items.title) when the themes lookup found no title — otherwise a
    # TDB-less row (a lost P-row with no ThemerrDB match) renders as the bare
    # "media_type/tmdb_id" id in the subject. the user: "Theme lost —"
    # notifications should show the actual name of the content lost. The
    # caller's title only fills the gap; a real themes title still wins.
    title = ctx.get("title") or (fallback_title or None)
    if title and not ctx.get("title"):
        ctx["title"] = title
    year = ctx.get("year")
    if title and year:
        ctx["display_title"] = f"{title} ({year})"
    elif title:
        ctx["display_title"] = title
    else:
        # Fallback to the bare ID shape — at least the operator
        # can grep events / library for the failing row.
        ctx["display_title"] = f"{media_type}/{tmdb_id}"
    # v1.21.75: append the edition to the subject so every notification
    # subject line (added / pushed / backed-up / available / lost / …,
    # all of which flow through _safe_display_title) names the cut.
    _edition_label = ctx.get("edition")
    if _edition_label:
        ctx["display_title"] = ctx["display_title"] + f" · {_edition_label}"
    ctx.setdefault("provenance", "unknown")
    return ctx


def _cached_oembed_thumb(db_path: Path, url: str) -> str | None:
    """v1.22.92: thumbnail for a login-walled URL from the persistent
    oembed cache (populated by the SET URL live preview — the only
    path Facebook themes arrive through, since ThemerrDB never
    publishes FB URLs). Best-effort: None on miss or any failure.
    fbcdn thumbnail URLs are signed with an expiry, but Discord
    fetches once at post time and proxies the bytes, so a later
    expiry doesn't break the posted message; an already-expired
    entry just renders no image (same as before this fix)."""
    try:
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT payload FROM oembed_cache WHERE url = ?",
                (url,),
            ).fetchone()
        if not row or not row["payload"]:
            return None
        thumb = (json.loads(row["payload"]) or {}).get("thumbnail_url")
        # Guard against the same-origin proxy path the UI variant of
        # this payload uses (/api/source/ig-thumbnail?...) — only an
        # absolute URL can render off-site in Discord.
        if thumb and thumb.startswith(("http://", "https://")):
            return thumb
        return None
    except Exception as e:  # noqa: BLE001
        log.debug("notify_content._cached_oembed_thumb failed "
                  "for %s: %s", url, e)
        return None


def _render_url_lines(ctx: ItemContext) -> list[str]:
    """v1.22.92: the URL line(s) for auto-embed formatters. YouTube /
    SoundCloud / Instagram URLs go in plain — Discord renders its own
    preview card. Facebook sits behind a login wall Discord's
    unfurler can't pass, so the "preview" rendered as a useless
    "Log in or sign up to view" card (the user's Castle Impossible
    repro) — suppress the unfurl with <...> (URL stays clickable).

    v1.22.94: the thumbnail is NO LONGER a body line. The v1.22.92
    bare-image-URL trick worked but rendered the signed fbcdn URL as
    a six-line wall of link text above the image (the user: "we're
    getting a huge url instead of just url of the video"). The thumb
    now travels as a real apprise attachment — see
    attachment_thumb_url + notify._prepare_attachment."""
    url = ctx.get("theme_url")
    if not url:
        return []
    if "facebook.com" in url or "fb.watch" in url:
        return [f"<{url}>"]
    # v1.24.31: suppress the YouTube auto-embed too (same <...> trick as FB) so
    # the uniform mqdefault thumb travels as a normalized attachment instead of
    # Discord's variable-size card (landscape vs Shorts portrait).
    # v1.24.35 (review #3): but ONLY when we actually have a thumb to attach.
    # _youtube_thumb returns None for a non-11-char / NULL youtube_video_id
    # (playlist/channel URLs, older rows) — suppressing the embed there would
    # leave the notification with NO image at all. With no thumb, keep the native
    # unfurl so something still renders.
    if (("youtube.com" in url or "youtu.be" in url)
            and ctx.get("thumb_url")):
        return [f"<{url}>"]
    return [url]


def attachment_thumb_url(ctx: ItemContext) -> str | None:
    """v1.22.94: the thumbnail to send as an apprise ATTACHMENT for
    this item's notification, or None. Login-walled sources (Facebook)
    need one — their unfurl is suppressed by _render_url_lines, so
    without an attachment there'd be no image at all.

    v1.24.31: YouTube now returns its thumb too. Pre-fix YouTube returned
    None and relied on Discord's own preview card, but that card sizes to the
    source video (landscape vs Shorts portrait) → a ragged mixed feed. Now
    _render_url_lines suppresses the YouTube unfurl and the uniform mqdefault
    thumb attaches (normalized to 400x224 by _prepare_attachment). SoundCloud
    still returns None (no deterministic thumb URL — keep its native unfurl)."""
    url = ctx.get("theme_url") or ""
    if ("facebook.com" in url or "fb.watch" in url
            or "youtube.com" in url or "youtu.be" in url):
        return ctx.get("thumb_url")
    return None


_PROVENANCE_LABEL: dict[str, str] = {
    "themerrdb":   "ThemerrDB",
    "user_url":    "User URL",
    # v1.18.58: explicit MP3-upload provenance distinct from
    # user_url (URL-driven download). Discord/Apprise body
    # reads "Source: User upload" so the operator can tell at
    # a glance which user-driven path the theme came from.
    "user_upload": "User upload",
    "manual":      "Manual sidecar",
    "adopted":     "Adopted",
    "plex_served": "Plex-served",
    "unknown":     "Unknown",
}


# v0.51.54: the Source line's colour DOT now encodes the SOURCE (provenance) so
# the operator can tell at a glance WHERE a theme came from — matching the SRC
# pill palette (T green / U violet / A cyan / M magenta / P amber). Discord only
# ships solid colour circles, so cyan->blue and magenta->brown are the closest
# available. Was per-platform (YouTube-red) since v0.50.37; the platform now
# reads as plain text + its real preview card renders from the URL.
_PROVENANCE_DOT: dict[str, str] = {
    "themerrdb":   "🟢 ",  # green — SRC=T
    "user_url":    "🟣 ",  # violet — SRC=U
    "user_upload": "🟣 ",  # violet — user-driven, same family as user_url
    "adopted":     "🔵 ",  # blue — SRC=A (no cyan circle emoji)
    "manual":      "🟤 ",  # brown — SRC=M (no magenta circle emoji)
    "plex_served": "🟡 ",  # amber — SRC=P
    "unknown":     "⚪ ",
}


def _format_provenance_line(ctx: ItemContext) -> str:
    """Source provenance line — distinguishes 'ThemerrDB / YouTube',
    'User URL / SoundCloud', etc. The URL host gets included so
    Discord / Slack auto-link previews show the actual platform.
    """
    prov_key = ctx.get("provenance", "unknown")
    prov = _PROVENANCE_LABEL.get(prov_key, "Unknown")
    # v0.51.54: the leading colour DOT encodes the SOURCE, not the platform (the
    # user: "colour the dot by the source ... at a glance show where it came
    # from"). Apprise's Discord embed bar is severity-coloured (INFO=blue for
    # every theme-add) and the external apprise-api path can't set a colour at
    # all — so the colour lives in the body, where it renders identically on
    # every transport. It now sits next to the provenance label; the platform
    # (YouTube / Facebook / …) reads as plain text + its preview card below.
    dot = _PROVENANCE_DOT.get(prov_key, _PROVENANCE_DOT["unknown"])
    url = ctx.get("theme_url") or ""
    if not url:
        return f"Source: {dot}{prov}"
    # Cheap platform classification — no need to re-import url_source here.
    plat = "URL"
    if "youtube.com" in url or "youtu.be" in url:
        plat = "YouTube"
    elif "soundcloud.com" in url:
        plat = "SoundCloud"
    elif "instagram.com" in url:  # v1.20.26
        plat = "Instagram"
    elif "facebook.com" in url or "fb.watch" in url:  # v1.22.90
        plat = "Facebook"
    return f"Source: {dot}{prov} · {plat}"


def format_theme_added_body(ctx: ItemContext) -> str:
    """Markdown body for `theme_added` notifications. Layout:

        Source: <provenance> · <platform>
        <plain url>

    v1.17.14: dropped the `[![alt](thumb)](url)` markdown image
    shape — Discord renders the image syntax literally and
    wrapping the URL in `[text](url)` suppresses Discord's
    YouTube auto-embed. Plain URL on its own line gives the
    full preview card.

    v1.17.16: dropped the bold `**<title>**` line. The
    notification subject already includes the title
    (`motif: theme added — <title>`) so the body line was
    pure duplication. the user: "I think maybe we could remove
    the duplication of the title." Also makes the body two
    lines instead of three — meaningful in Discord where the
    auto-embed card already takes substantial vertical space.

    Order: Source line BEFORE the URL so the text content reads
    coherently above the auto-embed card that Discord renders
    below it."""
    lines: list[str] = []
    # Source line — provenance + platform.
    lines.append(_format_provenance_line(ctx))
    # Plain URL on its own line for auto-embed. Discord etc.
    # render the YouTube / SoundCloud preview card below the
    # text content.
    lines.extend(_render_url_lines(ctx))
    return "\n".join(lines)


def format_theme_deleted_body(
    ctx: ItemContext, *,
    action: str,           # "unmanaged" | "forgotten" | "deleted"
    actor: str,
    extra_note: str = "",  # e.g. "3 placement(s) unlinked"
) -> str:
    """Markdown body for `theme_deleted` notifications. Layout:

        By <actor> · <optional extra-note>
        Source: <provenance> · <platform>
        Previous theme: <<url>>

    v1.17.14: the URL is wrapped in `<...>` (Discord's
    no-embed marker) so deletion notifications don't spawn a
    big YouTube preview card on every event. The URL stays
    clickable; only Discord interprets the brackets specially.

    v1.17.16: dropped the bold `**<title>**` line (subject
    already has it). Also collapsed `Action by actor` + the
    optional extra-note onto a single line with `·` separator
    when both are present — same separator the source line
    uses for `prov · plat`. Net body is 2-3 lines instead of
    4-5, matching the compact theme_added shape."""
    lines: list[str] = []
    # v1.19.92: lead with the actor, not the action verb — the title
    # ("🗑️ Theme {action} — {title}") already carries the verb +
    # title, so repeating "Unmanaged"/"Forgotten"/"Deleted" here was
    # duplication. Body adds only the actor + the placement-impact
    # note. The `·` separator matches _format_provenance_line.
    first = f"By {actor}"
    if extra_note:
        first += f" · {extra_note}"
    lines.append(first)
    lines.append(_format_provenance_line(ctx))
    # The theme URL still shows even on delete — the operator
    # may want to recover the URL from the notification later.
    # Angle brackets suppress Discord's auto-embed card.
    url = ctx.get("theme_url")
    if url:
        lines.append(f"Previous theme: <{url}>")
    return "\n".join(lines)


def format_theme_added_title(ctx: ItemContext) -> str:
    """Compact subject line. Services that distinguish title
    from body (Discord embeds, Pushover, etc.) use this; the
    body has the full markdown payload.

    v1.17.19: 🎵 emoji prefix + dropped redundant "motif:" prefix
    (the bot username already attributes — Discord shows "Motif
    APP" before the subject). the user: "could we add some emoji
    or more indicators of a removal event vs an add event ...
    professional looking discord notifications." Emoji set
    designed for at-a-glance scannability — see
    PROJECT_HISTORY § 15 (v1.17.19) for the full mapping."""
    return f"🎵 Theme added — {_safe_display_title(ctx)}"


def format_theme_pushed_title(
    ctx: ItemContext, *, reason: str | None = None,
    had_prior_theme: bool = True,
) -> str:
    """v1.19.55: Compact subject for `theme_pushed` events —
    fires when motif re-deploys an EXISTING theme to Plex
    (PUSH TO PLEX, REPLACE TDB, PROMOTE TO ACTIVE, force-place
    jobs). Distinct from theme_added (which fires on FIRST
    placement). the user's feedback: pre-v1.19.55 these all
    triggered theme_added, conflating "new theme appeared"
    with "Plex's serving state changed."

    `reason` (optional) tunes the verb so the operator can
    distinguish at a glance:
      'replace_with_themerrdb' → "Theme replaced via TDB", OR
        "Theme set via TDB" when `had_prior_theme` is False —
        REPLACE TDB applied to a row with no theme yet means
        nothing was actually replaced (the user's The Last
        Starfighter repro: a brand-new TDB theme read as
        "replaced something" when there was nothing there).
      'promote_bk_no_override' → "Theme promoted from backup"
      'plex_cloud_reupload'    → "Cloud backup re-uploaded"
      'user_switch_placement'  → "Placement switched" (sidecar ⇄ Plex-upload;
        v1.24.70 — these read as new themes otherwise, per the user)
      anything else / None      → "Theme pushed to Plex"

    📤 emoji signals "pushed/deployed" — distinct from 🎵
    (added) / 🔄 (sync update) / 🗑️ (deleted)."""
    title = _safe_display_title(ctx)
    if reason in ("replace_with_themerrdb", "download_tdb"):
        # v1.23.91: 'download_tdb' (bulk DOWNLOAD & push of a TDB theme) shares
        # the "set/replaced via TDB" verb — the bytes still came from TDB — but
        # its via-label is "DOWNLOAD TDB" (not "REPLACE TDB"); see
        # _PUSH_REASON_LABEL.
        verb = "replaced" if had_prior_theme else "set"
        return f"📤 Theme {verb} via TDB — {title}"
    if reason == "promote_bk_no_override":
        return f"📤 Theme promoted from backup — {title}"
    if reason == "plex_cloud_reupload":
        return f"📤 Cloud backup re-uploaded — {title}"
    # v1.24.70: SWITCH PLACEMENT (sidecar ⇄ Plex-upload) re-deploys the SAME
    # theme by a different mechanism — its own verb so it doesn't read as a
    # brand-new theme (the user: "these are switch-to-api actions but look like
    # new themes based on the notification").
    if reason == "user_switch_placement":
        return f"📤 Placement switched — {title}"
    return f"📤 Theme pushed to Plex — {title}"


# v1.19.55: human labels for the theme_pushed `reason` tokens.
# v1.19.95: lifted to module level so the batched-notification item
# label (format_theme_pushed_item) reuses the same mapping.
_PUSH_REASON_LABEL: dict[str, str] = {
    "replace_with_themerrdb": "REPLACE TDB",
    # v1.23.91: a bulk DOWNLOAD (& push) of a TDB theme. Distinct from
    # REPLACE TDB (ACCEPT UPDATE / the REPLACE TDB action swap the active
    # source TO TDB); a bulk download just re-fetches + pushes the TDB theme,
    # so labelling it "REPLACE TDB" mis-described the action (the user: bulk
    # download of green-TDB rows read as "via REPLACE TDB").
    "download_tdb": "DOWNLOAD TDB",
    # v1.23.93: SET URL pushed a USER-provided URL (SRC=U), not a TDB theme —
    # so it gets a "SET URL" label and the neutral "Theme pushed to Plex" title
    # (NOT the "via TDB" verb branch, which is reserved for actual TDB sources).
    "set_url": "SET URL",
    "promote_bk_no_override": "PROMOTE TO ACTIVE",
    "plex_cloud_reupload": "v1.18.36 re-upload trick",
    "manual_backup": "DOWNLOAD TDB BACKUP",
    # v1.24.70: SWITCH PLACEMENT (sidecar ⇄ Plex-upload) — pairs with the
    # "Placement switched" title so the body reads "… · via SWITCH PLACEMENT".
    "user_switch_placement": "SWITCH PLACEMENT",
}


def format_theme_pushed_body(
    ctx: ItemContext, *, reason: str | None = None,
) -> str:
    """v1.19.55: Markdown body for theme_pushed. Same shape as
    format_theme_added_body (Source line + plain URL for the
    auto-embed card) plus an optional `via <reason>` clarifier
    in the source line when the reason is known."""
    lines: list[str] = []
    src_line = _format_provenance_line(ctx)
    reason_label = _PUSH_REASON_LABEL.get(reason or "")
    if reason_label:
        src_line = f"{src_line} · via {reason_label}"
    lines.append(src_line)
    lines.extend(_render_url_lines(ctx))
    return "\n".join(lines)


# v1.19.95: batched-notification helpers. A bulk place (the user's
# 2026-05-29 33-item anime place) fired one theme_pushed Discord
# notification PER item → Discord 429 rate-limits + 12s throttles
# flooding the channel + event stream. notify.dispatch_coalesced
# collapses a burst into ONE summary: a per-item LABEL (no URL — 15
# URLs would render 15 embed cards) under a count headline. A lone
# event still sends the rich single notification (with the URL embed)
# via the format_*_title/body pair.


def format_theme_pushed_item(
    ctx: ItemContext, *, reason: str | None = None,
) -> str:
    """One-line label for a theme_pushed event in a batched summary:
    'Title (Year) — via REPLACE TDB' (reason suffix only when known)."""
    label = _safe_display_title(ctx)
    reason_label = _PUSH_REASON_LABEL.get(reason or "")
    if reason_label:
        label = f"{label} — via {reason_label}"
    return label


def format_theme_added_item(ctx: ItemContext) -> str:
    """One-line label for a theme_added event in a batched summary."""
    return _safe_display_title(ctx)


def _format_batch_body(labels: list[str], *, cap: int = 15,
                       buckets: dict | None = None) -> str:
    """Bulleted item list capped at `cap`, with a '…and N more'
    overflow line. No URLs — a batch of 15+ URLs would spawn an
    embed card per line in Discord (the very spam we're collapsing).

    v1.23.75: when the coalesced items carried their Plex library and span
    ≥2 distinct libraries (`buckets`: section title → labels), group the
    bullets under **bold** section headers like the sync summary (v1.22.45)
    so the operator can tell which library each theme belongs to. A single-
    library bulk stays a flat list — one header would just be noise."""
    grouped = [s for s in (buckets or {}) if buckets[s]]
    if len(grouped) >= 2:
        lines: list[str] = []
        shown = 0
        for section in sorted(grouped):
            lines.append(f"**{section}**")
            for lbl in buckets[section]:
                if shown >= cap:
                    break
                lines.append(f"* {lbl}")
                shown += 1
            if shown >= cap:
                break
        extra = len(labels) - shown
        if extra > 0:
            lines.append(f"…and {extra} more")
        return "\n".join(lines)
    lines = [f"* {lbl}" for lbl in labels[:cap]]
    extra = len(labels) - cap
    if extra > 0:
        lines.append(f"…and {extra} more")
    return "\n".join(lines)


def format_section_grouped_lines(
    header: str, buckets: dict, total_count: int, shown_count: int,
) -> list[str]:
    """v1.22.45: render one sync-summary block (New / Updated) grouped under
    Plex-section sub-headers so the operator can tell at a glance WHICH library
    each theme belongs to (Movies / 4K Movies / TV Shows / Anime / Anime
    Movies …) instead of guessing from the title. the user's ask after a sync
    listed a flat mix of movies + TV + anime.

    `buckets` maps a section title → its already-built item labels
    ('Title (Year)'); a title in two sections (e.g. a movie in Movies + 4K
    Movies) legitimately appears under both. Sections render alphabetically
    with markdown **bold** headers. `total_count` is the run's full count for
    this block, `shown_count` the number of distinct items `buckets` holds —
    their difference is the '…and N more' overflow line. Returns body LINES so
    the caller can join them with the surrounding summary."""
    lines = [header]
    for section in sorted(buckets):
        lines.append(f"**{section}**")
        for label in buckets[section]:
            lines.append(f"* {label}")
    extra = total_count - shown_count
    if extra > 0:
        lines.append(f"…and {extra} more")
    return lines


def format_theme_pushed_batch_title(n: int) -> str:
    return f"📤 {n} themes pushed to Plex"


def format_theme_pushed_batch_body(labels: list[str],
                                   buckets: dict | None = None) -> str:
    return _format_batch_body(labels, buckets=buckets)


def format_theme_added_batch_title(n: int) -> str:
    return f"🎵 {n} themes added"


def format_theme_added_batch_body(labels: list[str],
                                  buckets: dict | None = None) -> str:
    return _format_batch_body(labels, buckets=buckets)


# v1.24.38 (review #6): theme_auto_restored fires when the scheduler's hourly
# _restore_lost_placements sweep re-deploys a theme whose Plex copy went missing
# (a delete+re-add that wiped the sidecar or destroyed the uploaded rating_key).
# Pre-fix the scheduler dispatched ONE aggregate at ENQUEUE time — it claimed
# "restored" before the place job ran, so a place that later failed still pinged
# success. Now the worker fires this per-item on the place ACTUALLY landing,
# coalesced (bulk=True) so a re-add burst still collapses to one summary. 🛠️ =
# "self-healed" — distinct from 📤 pushed (user re-deploy) / 🎵 added (first time).


def format_theme_auto_restored_title(ctx: ItemContext) -> str:
    return f"🛠️ Theme restored from backup — {_safe_display_title(ctx)}"


def format_theme_auto_restored_body(ctx: ItemContext) -> str:
    lines: list[str] = []
    lines.append(
        "Its Plex theme went missing (most often a Plex delete + re-add) — "
        "motif re-deployed it from its own backup."
    )
    lines.append(_format_provenance_line(ctx))
    lines.extend(_render_url_lines(ctx))
    return "\n".join(lines)


def format_theme_auto_restored_item(ctx: ItemContext) -> str:
    return _safe_display_title(ctx)


def format_theme_auto_restored_batch_title(n: int) -> str:
    return f"🛠️ motif restored {n} missing theme" + ("s" if n != 1 else "")


def format_theme_auto_restored_batch_body(labels: list[str],
                                          buckets: dict | None = None) -> str:
    return _format_batch_body(labels, buckets=buckets)


def format_theme_backed_up_title(ctx: ItemContext) -> str:
    """v1.21.46: subject for `theme_backed_up` — fires when a theme
    DOWNLOADS as a backup (the worker stamps the local_file
    `backup_only` instead of placing it). Covers P-row bulk ACCEPT,
    DOWNLOAD TDB BACKUP, SET URL + KEEP AS BACKUP, CSV import-as-
    backup. 💾 emoji = "saved / staged" — distinct from 🎵 added (a
    theme started playing), 📤 pushed (re-deployed), 🎯 backup-ready
    (Plex DROPPED its theme — deploy NOW), 🗑️ deleted. Pre-v1.21.46
    these landings were silent: theme_added/theme_pushed are gated on
    `outcome.placed` and a backup never places."""
    return f"💾 Theme backed up — {_safe_display_title(ctx)}"


def format_theme_backed_up_body(ctx: ItemContext) -> str:
    """v1.21.46: markdown body. The landing is non-actionable — Plex
    (or whatever was already serving) keeps playing; the backup just
    sits staged until the active theme is lost, at which point the
    v1.18.80 backup_ready_to_deploy event fires the "deploy now" CTA.
    So the body states that, then the source line + URL for an auto-
    embed preview. Mirrors format_theme_added_body's compact shape
    (v1.19.92 copy standard — no title duplication)."""
    lines: list[str] = []
    lines.append(
        "Staged as a backup — nothing was placed, your current theme "
        "keeps playing. Motif will prompt you to deploy this if the "
        "active theme is ever lost."
    )
    lines.append(_format_provenance_line(ctx))
    lines.extend(_render_url_lines(ctx))
    return "\n".join(lines)


def format_theme_backed_up_item(ctx: ItemContext) -> str:
    """One-line label for a theme_backed_up event in a batched summary."""
    return _safe_display_title(ctx)


def format_theme_backed_up_batch_title(n: int) -> str:
    return f"💾 {n} themes backed up"


def format_theme_backed_up_batch_body(labels: list[str],
                                      buckets: dict | None = None) -> str:
    return _format_batch_body(labels, buckets=buckets)


def format_theme_available_title(ctx: ItemContext) -> str:
    """v1.21.5: single 'TDB theme available to add' subject. Fires
    for a newly-added Plex item that matches a ThemerrDB theme but
    has no theme in Plex or motif (SRC=—). v1.23.82: ✨ emoji (was
    🎵, which collided with theme_added) — this is an ACTIONABLE
    suggestion ("you could add this"), distinct from 🎵 "a theme
    started playing"; every event now has its own glyph."""
    return f"✨ Theme available — {_safe_display_title(ctx)}"


def format_theme_available_body(ctx: ItemContext) -> str:
    """v1.21.5: markdown body. The title carries identity; the body
    adds only the action + the TDB URL for an auto-embed preview
    (v1.19.92 copy standard — no title duplication)."""
    lines: list[str] = []
    # v1.23.82: split the state sentence from the action + add a bold
    # "**To add:**" lead-in so the CTA matches its siblings
    # (backup_ready's "**To deploy:**" / plex_theme_lost's "**To
    # restore:**") — the action was an inline clause before.
    lines.append(
        "A new item in your Plex library has no theme yet, but "
        "ThemerrDB has one."
    )
    lines.append("")
    lines.append(
        "**To add:** open the row's // SOURCE menu and click "
        "// DOWNLOAD TDB."
    )
    lines.extend(_render_url_lines(ctx))
    return "\n".join(lines)


def format_theme_available_batch_title(n: int) -> str:
    """v1.21.5: digest subject when one refresh adds several
    themeable items at once."""
    return f"✨ {n} themes available to add"


def format_theme_available_batch_body(labels: list[str],
                                      buckets: dict | None = None) -> str:
    return _format_batch_body(labels, buckets=buckets)


def format_arrived_themed_title(ctx: ItemContext) -> str:
    """v0.51.150: subject for `plex_item_arrived_themed` — a genuinely-new Plex
    item that arrived ALREADY themed by Plex (has_theme=1) and that motif doesn't
    own. 📺 emoji = "new content in Plex" (distinct from ✨ theme-available, which
    is an ACTIONABLE suggestion; this is a pure FYI). The title carries identity;
    no CTA, since there is nothing to do."""
    return f"📺 New in Plex, already themed — {_safe_display_title(ctx)}"


def format_arrived_themed_body(ctx: ItemContext) -> str:
    """v0.51.150: short FYI body. No CTA — Plex is already serving a theme, so
    the operator needs to do nothing; the message just surfaces that it happened
    (title already carries identity, v1.19.92 no-duplication copy standard)."""
    return (
        "A new item in your Plex library already has a theme playing — Plex is "
        "serving one, so there's nothing to do. Just letting you know it arrived "
        "already themed."
    )


def format_arrived_themed_batch_title(n: int) -> str:
    """v0.51.150: digest subject when one refresh surfaces several new
    already-themed arrivals at once."""
    return f"📺 {n} new items arrived already themed"


def format_arrived_themed_batch_body(labels: list[str],
                                     buckets: dict | None = None) -> str:
    return _format_batch_body(labels, buckets=buckets)


def format_theme_deleted_title(
    ctx: ItemContext, *, action: str,
) -> str:
    """v1.17.19: 🗑️ emoji prefix for all three destructive
    actions (unmanaged / forgotten / deleted) — operators
    distinguish "this row lost its theme" at a glance from
    "this row gained one." The action verb stays in the
    subject so the specific destructive shape is still
    legible (forget unlinks placements; delete drops the
    row entirely)."""
    return f"🗑️ Theme {action} — {_safe_display_title(ctx)}"


def format_backup_ready_title(ctx: ItemContext) -> str:
    """v1.18.80: subject for the backup_ready_to_deploy event.
    Fires when Plex's theme transitions 1→0 on a row with
    user_overrides.intent='backup'. 🎯 emoji signals
    "actionable" — distinct from 🎵 (theme added) / 🗑️
    (deleted) / ⚠ (failure). Bot username already attributes,
    so subject leads with the action context."""
    return f"🎯 Backup ready — {_safe_display_title(ctx)}"


def format_backup_ready_body(
    ctx: ItemContext, *, override_url: str | None = None,
) -> str:
    """v1.18.80: body for the backup_ready_to_deploy event.
    Plex stopped serving its own theme on a backup-intent row
    — the user's URL is now the only candidate to serve. Body
    names the title + the URL the user staged + the action to
    take (// PROMOTE TO ACTIVE).

    Mirrors the markdown shape of format_theme_added_body so
    Discord / Slack / Telegram render bullet lines + the URL
    inline. body_format='markdown' on the dispatch.

    the user's design (v1.18.77 q4 answer): notify only, don't
    auto-promote. The user keeps control."""
    # v1.19.92: dropped the "...for **{title}**." lead + the separate
    # "ready to deploy" sentence — the title ("🎯 Backup ready —
    # {title}") already carries both. Body opens with the state +
    # the actionable deploy step.
    lines = [
        "Plex stopped serving its own theme — your backup URL is "
        "ready to deploy.",
    ]
    # v1.23.75: name the backup's source (User URL / Plex-served / …) so a
    # user-staged backup reads distinctly from a Plex-cloud copy at a glance,
    # matching the provenance line in format_theme_added_body.
    lines.append(_format_provenance_line(ctx))
    if override_url:
        lines.append("")
        lines.append(f"Backup URL: <{override_url}>")
    lines.append("")
    lines.append(
        "**To deploy:** open the row's // MOTIF INFO card and "
        "click // PROMOTE TO ACTIVE. Motif force-places your URL "
        "over Plex's empty theme slot.")
    lines.append("")
    lines.append(
        "(motif won't auto-deploy backups — your override "
        "decision stays in your hands.)")
    return "\n".join(lines)


def format_plex_theme_lost_title(ctx: ItemContext) -> str:
    """v1.18.90: subject for the plex_theme_lost event. Fires
    when Plex stops serving its own theme on a row AND motif
    has no fallback (no user_override, no local_file, no
    placement). Distinct from backup_ready_to_deploy (which
    fires when motif HAS a backup); this one is "user needs
    to add something."

    💔 emoji signals "broken / lost state" — distinct from
    🎯 (backup ready) / 🎵 (theme added) / 🗑️ (deleted)
    per v1.17.19's emoji-mapping convention.

    v1.23.81: this is the BARE (no-qualifier) tier — Plex lost the
    theme AND motif has no fallback, the broken case. Its two
    sibling tiers now add a title qualifier ("— backup ready —" /
    "— still playing —") so an operator can tell them apart at a
    glance; the unqualified "💔 Theme lost —" is reserved for this
    no-fallback case (all three were byte-identical before)."""
    return f"💔 Theme lost — {_safe_display_title(ctx)}"


def format_plex_theme_lost_body(ctx: ItemContext) -> str:
    """v1.18.90: body for the plex_theme_lost event. The row's
    Plex-served theme is gone (typical cause: user removed +
    re-added the item in Plex, breaking Plex's theme cache;
    or themerr-plex plugin removal). Motif has no fallback —
    the row is now silent. v0.51.127: the item is no longer in
    Plex's listing (the reaper deleted the row), so the body
    frames the cause (the item left Plex) + the conditional
    restore path if Plex re-added it, rather than the pre-tag
    "open the row's INFO card" (there's no row to open).

    body_format='markdown' on the dispatch so Discord / Slack
    / Telegram render bullets + bold inline.

    v1.19.41: dropped the "no user-provided URL, no uploaded
    MP3, no local file" line. With cloud-themes-backup (v1.19.42)
    shipping, "no local file" becomes misleading — plex_cloud
    rows DO have a local file but route through a different
    notification (theme_lost_backup_ready). The new "no backup
    configured" phrasing stays accurate across all backup-source
    types.
    """
    # v1.19.92: dropped the "Plex was serving a theme for **{title}**"
    # lead — the title ("💔 Theme lost — {title}") already carries it.
    # v0.51.127: this event fires from the v1.18.90 reaper — the item is no
    # longer in Plex's section listing (removed, or removed + re-added under a
    # new rating_key), so motif deleted the row. The pre-tag "open the row's
    # INFO card to restore" copy was misleading: there's no row to open. Reframe
    # around the actual state (the item left Plex) + the conditional path if it
    # comes back (the user, after searching for a reaped row and finding nothing).
    lines = [
        "Plex no longer lists this item — it was removed from your "
        "library, or Plex re-added it under a new ID — so the theme "
        "it was serving is gone, and motif has no backup configured "
        "for it.",
        "",
        "If Plex re-added it, motif re-detects the item on the next "
        "// REFRESH PLEX; add a theme from its // MOTIF INFO card then "
        "via // SET URL or // UPLOAD MP3. If it was removed for good, "
        "no action is needed.",
        "",
        "(Detected during plex_enum. Motif rate-limits this "
        "notification to once per 24h per row.)",
    ]
    return "\n".join(lines)


# v1.19.41: two new notification bodies for the four-way
# tier split in the v1.18.90 reaper dispatch. The reaper now
# routes lost-theme events through one of four paths:
#   Tier 1 — backup-ready (this body's path 1)
#   Tier 2 — sidecar-available (this body's path 2)
#   Tier 3 — other fallback (silent skip, no body)
#   Tier 4 — no fallback (existing format_plex_theme_lost_body)
# See plex_enum.py:1518+ for the tier-selection logic and
# docs/SESSION_JOURNAL.md v1.19.41 entry for the precedence
# rationale.


def _safe_display_title(ctx: "ItemContext") -> str:
    """v1.19.44: defensive accessor for ctx['display_title'].

    Pre-fix the formatters read `ctx['display_title']` directly,
    which KeyErrors when the formatter is called with a ctx that
    didn't go through `enrich_item` (e.g. a row deleted between
    enum and dispatch, a future caller that synthesizes ctx
    directly, or the v1.19.41 test-trigger endpoint's path if
    the row's enrich data was partial).

    The class-9 audit flagged this S1 because the outer
    `except Exception` in plex_enum dispatch + the
    `_THEME_LOST_NOTIFY_WARNED` flag combine to silence ALL
    subsequent notifications after one bad ctx:
      1. KeyError raised by formatter
      2. plex_enum's outer except catches it
      3. _THEME_LOST_NOTIFY_WARNED flag goes hot
      4. Every future row in the same process drops to
         log.debug — operator loses all visibility

    Fallback synthesizes media_type/tmdb_id so the body still
    identifies the row (no `<unknown>` placeholder)."""
    title = ctx.get("display_title")
    if title:
        return title
    mt = ctx.get("media_type", "?")
    tid = ctx.get("tmdb_id", "?")
    return f"{mt}/{tid}"


def format_theme_lost_backup_ready_title(ctx: "ItemContext") -> str:
    """v1.19.41: subject for theme_lost_backup_ready. v1.23.81: 💔
    stays as the lost-theme family marker, but the title now adds a
    "— backup ready —" tier qualifier. This is the ACTIONABLE tier
    (motif has a deploy-ready backup — the body's CTA is PROMOTE TO
    ACTIVE), so it must read distinctly from the still-playing
    sidecar tier (format_theme_lost_sidecar_available_title) and the
    broken no-fallback tier (format_plex_theme_lost_title) in a
    Discord list — all three subjects were byte-identical before."""
    return f"💔 Theme lost — backup ready — {_safe_display_title(ctx)}"


def format_theme_lost_backup_ready_body(
    ctx: "ItemContext",
    *,
    backup_source: str,
    sidecar_present: bool = False,
) -> str:
    """v1.19.41: body for theme_lost_backup_ready. Fires when
    Plex stops serving AND motif has a deploy-ready backup
    (user-marked KEEP AS BACKUP override OR plex_cloud
    walker-downloaded local file).

    `backup_source` discriminates the body phrasing:
      'user_url_backup' — user-marked KEEP AS BACKUP override
      'plex_cloud_backup' — motif's local copy of Plex's cloud
                            theme (v1.19.42+)

    `sidecar_present` adds the ADOPT alternative line when a
    sidecar also exists at the Plex folder. Tier-1 wins primary
    CTA per the v1.19.41 precedence ladder; ADOPT surfaces as
    awareness only.
    """
    backup_phrase = (
        "a URL you marked KEEP AS BACKUP"
        if backup_source == "user_url_backup"
        else "a local copy of Plex's cloud theme"
    )
    # v1.19.92: dropped the "Plex was serving a theme for **{title}**"
    # lead — the title carries it. Body opens with the state, then the
    # deploy-ready backup + action.
    lines = [
        "Plex's theme for this row is no longer available.",
        "",
        f"✓ A backup is ready to deploy — motif has "
        f"{backup_phrase} staged.",
        "",
        "**To deploy:** open the row's // MOTIF INFO card and "
        "click // PROMOTE TO ACTIVE.",
    ]
    if sidecar_present:
        lines.extend([
            "",
            "**Alternative:** an existing `theme.mp3` is at "
            "the Plex folder — // ADOPT brings it under "
            "motif's management WITHOUT re-deploying the "
            "backup. Use this if you'd rather keep whatever's "
            "currently on disk than swap in the staged backup.",
        ])
    lines.extend([
        "",
        "(Theme will not play until you deploy. Motif rate-"
        "limits this notification to once per 24h per row.)",
    ])
    return "\n".join(lines)


def format_theme_lost_sidecar_available_title(
    ctx: "ItemContext",
) -> str:
    """v1.19.41: subject for theme_lost_sidecar_available.
    v1.23.81: 💔 family marker + a "— still playing —" tier
    qualifier — the sidecar keeps the theme audible as M, so this is
    the NO-URGENCY tier, distinct from "— backup ready —" (actionable)
    and the bare no-fallback "💔 Theme lost —" (broken)."""
    return f"💔 Theme lost — still playing — {_safe_display_title(ctx)}"


def format_theme_lost_sidecar_available_body(
    ctx: "ItemContext",
) -> str:
    """v1.19.41: body for theme_lost_sidecar_available. Fires
    when Plex stops serving AND a sidecar (`theme.mp3`) exists
    at the Plex media folder. The theme keeps playing as M
    even without action — lower urgency than backup-ready
    (path 1) or no-fallback (path 4).

    Recommends ADOPT to bring the sidecar under motif tracking.
    Mentions SET URL / UPLOAD MP3 as replace alternatives.
    Calls out the sidecar's unknown provenance so the user
    knows what ADOPT actually does."""
    # v1.19.92: dropped the "Plex was serving a theme for **{title}**"
    # lead — the title carries it.
    lines = [
        "Plex's theme for this row is no longer available.",
        "",
        "✓ Existing sidecar detected — `theme.mp3` is at the "
        "Plex media folder. The theme will continue playing as "
        "a manual (M) source even without action — no urgency.",
        "",
        "The sidecar's content is unverified — it could be a "
        "leftover from a previous Plex Pass plugin write, a "
        "prior motif state, or something dropped manually. "
        "Motif's INFO card surfaces the file's SHA-256 + size "
        "after ADOPT so you can verify.",
        "",
        "**To bring it under motif's management** (recommended "
        "— enables tracking, theme update prompts, re-download): "
        "open the row's // MOTIF INFO card and click // ADOPT.",
        "",
        "**Alternatives** (replace the existing sidecar with a "
        "known source):",
        "- // SET URL — paste a YouTube or SoundCloud URL.",
        "- // UPLOAD MP3 — upload a different MP3.",
        "",
        # v1.23.82: append the 24h rate-limit note its two reaper
        # siblings (no-fallback + backup-ready) already carry — all
        # three loss tiers are rate-limited identically (86400s in
        # plex_enum dispatch), so the footer should read uniformly.
        "(Detected during plex_enum. Common cause: the item "
        "was removed and re-added in Plex, breaking its theme "
        "cache. Motif rate-limits this notification to once per "
        "24h per row.)",
    ]
    return "\n".join(lines)
