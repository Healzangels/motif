"""motif notification dispatcher (Apprise integration).

v1.17.0: best-effort, fire-and-forget notifications to user-
configured sinks. Two dispatch paths coexist:

  1. **Embedded Apprise** — `notifications.apprise_urls` is a list
     of Apprise service URLs (discord://webhook_id/webhook_token,
     pushover://user@token, tgram://bot_token/chat_id, etc.). Sent
     in-process via the `apprise` package's batched Apprise() API.
  2. **External Apprise API** — `notifications.apprise_external_url`
     points at a caronc/apprise-api container's /notify/{key}
     endpoint. motif POSTs {"title": ..., "body": ...} to it; the
     external service handles fan-out via its own configured URLs.

Both can be populated — `dispatch()` fires both paths concurrently
and reports per-sink outcome to the events table with
`component='notify'` for in-app health visibility (no topbar pill —
log-only, per the v1.17.0 design discussion).

The dispatcher is BEST-EFFORT: a failing send must never crash the
caller. Worker threads, scheduler ticks, and request handlers fire-
and-forget; we swallow exceptions, log to the events table, and
return.

Per-event toggles gate the dispatch — disabled events return
immediately. Event kinds live in
`config_file._DEFAULT_NOTIFY_EVENTS` (the source of truth for
what's available + what the default state is).
"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from .config_file import NotificationsConfig


log = logging.getLogger(__name__)


# v1.17.1: per-event severity routing. caronc/apprise-api maps the
# `type` field to severity-based fan-out (info / warning / failure
# → service-specific severity), and the embedded apprise library
# threads `NotifyType.X` to per-service severity rendering (Discord
# embed color, Pushover priority, etc.). Pre-fix every notification
# shipped as `type=info`, so sync_failed / cookies_needed /
# disk_low / worker_restarted all rendered as neutral chatter on
# the operator's notification stack — exactly the events that
# need to bypass severity filters. Each event_kind maps to its
# canonical severity here; the dispatcher looks it up so callers
# don't have to pass it.
NotifyType = Literal["info", "warning", "failure"]

# v1.17.12: body content format. Most Apprise services accept
# markdown (Discord, Slack, Telegram, Pushover, Matrix, Mattermost,
# Pushbullet, …) and render bold / links / inline image previews
# from the markdown source. Plain-text-only services see the
# markdown source which is still legible. Default is "text" so
# v1.17.0-era callers passing plain bodies still work unchanged;
# enriched callers (theme_added / theme_deleted via
# notify_content.format_*) pass "markdown" explicitly.
BodyFormat = Literal["text", "markdown", "html"]

_EVENT_NOTIFY_TYPE: dict[str, NotifyType] = {
    # failures — operator wants to know NOW
    "sync_failed":             "failure",
    # warnings — operator action recommended
    "cookies_needed":          "warning",
    "disk_low":                "warning",
    "worker_restarted":        "warning",
    # info — routine lifecycle pings
    "sync_completed":          "info",
    "bulk_action_completed":   "info",
    "themes_added_by_sync":    "info",
    # v1.19.55: themes_updated_by_sync — parallel to
    # themes_added_by_sync; fires when sync has updates.
    "themes_updated_by_sync":  "info",
    "theme_added":             "info",
    # v1.19.55: theme_pushed — distinct from theme_added.
    # Fires on re-deployment of an EXISTING theme (PUSH /
    # REPLACE / PROMOTE / force-place). Same info-level as
    # theme_added — routine deploy.
    "theme_pushed":            "info",
    "theme_deleted":           "info",
    "new_tdb_theme_available": "info",
    "release_available":       "info",
    # v1.18.80: fires when Plex stops serving its own theme on a row
    # with user_overrides.intent='backup'. The user's backup file is
    # now the only candidate; PROMOTE TO ACTIVE deploys it.
    # info-level (not warning) — Plex deciding to stop serving its
    # own theme isn't a motif problem; it's an actionable signal.
    "backup_ready_to_deploy":  "info",
    # v1.18.90: fires when Plex stops serving its own theme on a row
    # AND motif has no fallback (no user_override, no local_file,
    # no placement). Distinct from backup_ready_to_deploy (which
    # fires when motif HAS a backup ready); this one fires when
    # the user needs to ADD a theme. warning-level — silence is
    # the user-visible state, and they may want to fix it.
    "plex_theme_lost":         "warning",
    # v1.23.62 (audit #8): the v1.19.41 four-way reaper split dispatches these
    # three kinds (plex_enum.py:2373/2389, worker.py:2213) but they were never
    # added here, so .get(event_kind, "info") rendered them neutral. The two
    # theme-LOST tiers join plex_theme_lost as warnings (Plex actively dropped a
    # tracked theme — the operator should notice, even though a recovery exists);
    # theme_backed_up is a routine insurance confirmation → info.
    "theme_lost_backup_ready":     "warning",
    "theme_lost_sidecar_available": "warning",
    "theme_backed_up":             "info",
    # v1.24.26: the hourly restore sweep re-deployed a motif-owned sidecar
    # whose Plex theme.mp3 went missing (delete+re-add churn). warning, not
    # info — the LOSS is the notable thing the operator should notice (a
    # tracked theme went missing on a Plex folder), same class as the
    # theme-lost tiers above; that motif auto-healed it is the good news in
    # the body, not a reason to demote the signal.
    "theme_auto_restored":         "warning",
}


# v1.17.0: bounded thread pool so a slow Apprise service (e.g. a
# misconfigured Discord webhook timing out at 10s) doesn't pile up
# background threads. 4 workers handles bursty traffic (a bulk
# action firing N notifications back-to-back) without unbounded
# growth. The pool is module-level + lazy so module import stays
# cheap; lock-protected init guards against the rare boot race
# where two threads call dispatch() concurrently before the pool
# is built.
_POOL: ThreadPoolExecutor | None = None
_POOL_LOCK = threading.Lock()


def _get_pool() -> ThreadPoolExecutor:
    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                _POOL = ThreadPoolExecutor(
                    max_workers=4, thread_name_prefix="motif-notify")
    return _POOL


def _log_outcome(
    db_path: Path, *, event_kind: str, level: str,
    message: str, detail: dict | None = None,
) -> None:
    """Best-effort event-log write. If the events table itself is
    unwriteable (DB lock, schema drift, whatever), drop the log
    line on the floor rather than re-raising — the dispatcher's
    contract is "never crash the caller." Standard log fall-back
    keeps the dev-loop visibility."""
    try:
        from .events import log_event
        log_event(
            db_path, level=level, component="notify",
            message=message, detail=detail or {},
        )
    except Exception as e:
        log.debug("notify event-log write failed: %s", e)


# v1.22.94: same CDN allowlist as the ig-thumbnail proxy — the
# attachment fetcher must not be coercible into an open URL fetcher.
# v1.24.31: + ytimg.com so YouTube theme thumbnails (i.ytimg.com/.../
# mqdefault.jpg) attach as a uniform 400x224 image like FB/IG, instead of
# Discord auto-embedding the raw watch URL (a variable-size card — landscape
# for normal videos, tall portrait for Shorts).
_ATTACH_THUMB_HOSTS = ("cdninstagram.com", "fbcdn.net", "ytimg.com")


def _prepare_attachment(url: str) -> str | None:
    """v1.22.94: fetch a notification thumbnail server-side and
    normalize it to a uniform landscape canvas so a login-walled
    source's vertical poster doesn't render as a giant tall image
    (the user's Castle Impossible follow-up). ffmpeg does the resize
    (already a container dependency for the mp3 transcode). Without
    ffmpeg (bare-metal dev) the raw bytes attach as-is. Returns a
    temp-file path the CALLER must unlink, or None on any failure
    (no attachment, body unchanged).

    v1.22.97/.99 sized the canvas to Discord's display rules
    (attachments render at NATIVE px below the ~500px chat-column
    cap; YT video embeds lay out at ~400px), but v1.22.99's 400x225
    had an ODD height — yuv420 JPEG encoding needs even dimensions,
    ffmpeg errored, and the silent raw-bytes fallback shipped the
    vertical poster untouched (the class-9 lesson: the fallback hid
    the failure; it now logs a WARNING with stderr).

    v1.23.0: COVER-CROP (scale to cover, center-crop), not
    letterbox — the INFO card's object-fit cover look the user asked
    to match: full-bleed, no black bars, even dimensions,
    native-size under the cap.

    v1.23.1: 400x300 → 400x224. The 4:3 frame matched the YT
    embeds' width but rendered TALLER than their 16:9 players
    (the user: "make it identical now"). 224 not 225 — 16:9 of 400
    is odd, and odd heights fail yuv420 JPEG encoding (the exact
    v1.22.99 failure); one pixel off-ratio is invisible."""
    import os
    import shutil
    import subprocess
    import tempfile
    from urllib.parse import urlparse
    try:
        host = (urlparse(url).hostname or "").lower()
        if not any(host == h or host.endswith("." + h)
                   for h in _ATTACH_THUMB_HOSTS):
            return None
        import httpx
        with httpx.Client(timeout=8.0, follow_redirects=False) as c:
            resp = c.get(url, headers={
                "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; "
                               "x64) AppleWebKit/537.36 (KHTML, like "
                               "Gecko) Chrome/124.0 Safari/537.36"),
            })
        ctype = resp.headers.get("content-type", "")
        if resp.status_code != 200 or not ctype.startswith("image/"):
            return None
        fd, raw_path = tempfile.mkstemp(
            suffix=".jpg", prefix="motif-notify-thumb-")
        with os.fdopen(fd, "wb") as f:
            f.write(resp.content)
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            return raw_path
        norm_path = raw_path[:-4] + "-320.jpg"
        proc = subprocess.run(
            [ffmpeg, "-y", "-i", raw_path, "-vf",
             "scale=400:224:force_original_aspect_ratio=increase,"
             "crop=400:224",
             "-frames:v", "1", norm_path],
            capture_output=True, timeout=20)
        if proc.returncode == 0 and os.path.getsize(norm_path) > 0:
            os.unlink(raw_path)
            return norm_path
        # v1.23.0 (class 9): the raw-bytes fallback is functional but
        # must not be SILENT — v1.22.99's odd-height encode failure
        # shipped vertical posters for hours with zero breadcrumb.
        log.warning(
            "notify thumb normalize failed (rc=%s); attaching raw "
            "bytes: %s",
            proc.returncode,
            (proc.stderr or b"")[-300:].decode("utf-8", "replace"))
        try:
            os.unlink(norm_path)
        except OSError:
            pass
        return raw_path
    except Exception as e:  # noqa: BLE001
        log.debug("notify attachment prep failed for %s: %s", url, e)
        return None


def _send_embedded(
    urls: list[str], title: str, body: str,
    notify_type: NotifyType = "info",
    body_format: BodyFormat = "text",
    attach_path: str | None = None,
) -> tuple[int, int]:
    """Dispatch via the embedded Apprise library. Returns
    (success_count, fail_count). Apprise.notify() returns True if
    EVERY configured URL succeeded; on partial failure it returns
    False but doesn't tell us which subset failed. We treat the
    aggregate result as either all-ok or some-failed for logging
    purposes. (v1.23.92: apprise's own logger is clamped to WARNING in
    main.configure_logging — it logs the FULL webhook URL incl. the secret
    token at DEBUG — so per-URL DEBUG diagnostics are intentionally
    suppressed; apprise still surfaces failures at WARNING/ERROR.)

    v1.17.1: `notify_type` threads into apprise.NotifyType so
    per-service severity rendering (Discord embed color, etc.)
    matches the event's actual urgency. Also: counts URLs that
    apprise.add() rejected as malformed against the fail-count
    return so the TEST UI surfaces the typo'd URL count instead
    of silently swallowing the rejected subset.

    v1.17.12: `body_format` threads through to `apobj.notify(...,
    body_format=...)`. Per-item callers (theme_added/deleted/
    themes_added_by_sync) pass "markdown" so the rich body
    composed in notify_content.py renders as a Discord embed /
    Slack mrkdwn / Telegram markdown / etc."""
    try:
        import apprise  # type: ignore[import-not-found]
    except ImportError:
        log.warning("apprise package not installed; embedded "
                    "notification dispatch unavailable")
        return (0, len(urls))
    if not urls:
        return (0, 0)
    type_map = {
        "info": apprise.NotifyType.INFO,
        "warning": apprise.NotifyType.WARNING,
        "failure": apprise.NotifyType.FAILURE,
    }
    apprise_type = type_map.get(notify_type, apprise.NotifyType.INFO)
    fmt_map = {
        "text":     apprise.NotifyFormat.TEXT,
        "markdown": apprise.NotifyFormat.MARKDOWN,
        "html":     apprise.NotifyFormat.HTML,
    }
    apprise_fmt = fmt_map.get(body_format, apprise.NotifyFormat.TEXT)
    apobj = apprise.Apprise()
    added = 0
    for url in urls:
        if apobj.add(url):
            added += 1
    rejected = len(urls) - added
    if added == 0:
        return (0, len(urls))
    try:
        # v1.22.94: optional image attachment (login-walled sources'
        # thumbnails). Services without attachment support ignore it.
        _kw = {"attach": [attach_path]} if attach_path else {}
        ok = apobj.notify(
            title=title, body=body,
            notify_type=apprise_type, body_format=apprise_fmt,
            **_kw,
        )
    except Exception as e:
        # v1.22.31 (audit): WARNING, not DEBUG. The aggregate sent_fail is
        # surfaced as a WARNING events-row by _dispatch_inline, but the
        # SPECIFIC failure reason (auth, connection, service down) was buried
        # at DEBUG — operators couldn't diagnose why a channel went silent
        # without flipping log levels. Surface it by default.
        log.warning("apprise embedded notify raised: %s", e)
        return (0, added + rejected)
    # v1.17.1 (M1 fix): success path now reports rejected URLs as
    # failures. Pre-fix `(added, 0)` hid Apprise-unparseable URLs
    # from the TEST UI — the operator saw "1/2 ok" with no "1
    # failed" tag, leaving the typo invisible.
    return ((added, rejected) if ok else (0, added + rejected))


def _send_external(
    api_url: str, title: str, body: str,
    notify_type: NotifyType = "info",
    body_format: BodyFormat = "text",
) -> tuple[int, int]:
    """Dispatch via an external Apprise API container. Returns
    (success, fail) tuple where success/fail are 0/1 (single
    target). caronc/apprise-api accepts POST {"title": "...",
    "body": "...", "type": "info" | "warning" | "failure",
    "format": "text" | "markdown" | "html"} at /notify/{key} or
    /notify (depending on the container's URL key config).

    v1.17.12: `format` field added so caronc/apprise-api fans out
    with markdown rendering for per-item events that compose rich
    bodies via notify_content."""
    if not api_url:
        return (0, 0)
    try:
        import httpx
        # 10s connect + read timeout — Apprise services routinely
        # respond in <500ms; a 10s ceiling catches misconfigured
        # external services without blocking the pool worker for
        # the full 60s urllib default.
        r = httpx.post(
            api_url,
            json={
                "title": title,
                "body": body,
                "type": notify_type,
                "format": body_format,
            },
            timeout=10.0,
        )
        if 200 <= r.status_code < 300:
            return (1, 0)
        # v1.22.31 (audit): WARNING, not DEBUG — see _send_embedded. A non-2xx
        # from the external apprise-api (bad key, service down) was invisible
        # by default behind the aggregate WARNING events-row.
        log.warning("apprise external API returned %d: %s",
                    r.status_code, r.text[:200])
        return (0, 1)
    except Exception as e:
        log.warning("apprise external API call raised: %s", e)
        return (0, 1)


# v1.23.2: severity → Discord embed sidebar color for the native
# embed send; amber/red mirror the UI's state palette for
# warning/failure. v1.23.5: info is FACEBOOK brand blue (#1877F2) —
# the bar plays the same role as the red bar on YouTube's own
# preview cards (source identity at a glance, the user's ask), and
# today the native path fires ONLY for Facebook-sourced thumbnails.
# If a second login-walled source ever joins the attachment pipe
# (Instagram), this must become source-aware instead of a constant.
_DISCORD_EMBED_COLOR = {
    "info": 0x1877F2, "warning": 0xFFB84A, "failure": 0xFF6B6B,
}


_DISCORD_WEBHOOK_HOSTS = (
    "discord.com", "www.discord.com", "ptb.discord.com",
    "canary.discord.com", "discordapp.com", "www.discordapp.com",
)


def _parse_discord_webhook(url: str) -> tuple[str, str] | None:
    """v1.23.2: (webhook_id, token) from a Discord sink URL, or None
    when it isn't one / has a shape we don't recognize — those keep
    the apprise path. v1.23.3: ALSO matches the raw webhook form
    https://discord.com/api/webhooks/{id}/{token} — apprise accepts
    it natively (parse_native_url converts it to discord://), and
    the user's config carries that form, so the v1.23.2 scheme-only
    check silently fell back to apprise and the embed card never
    fired. Forms: discord://id/token, discord://botname@id/token,
    https://discord(app).com/api/webhooks/id/token."""
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        if p.scheme == "discord":
            wid = p.hostname or ""
            token = p.path.strip("/").split("/")[0] if p.path else ""
        elif p.scheme in ("http", "https"):
            host = (p.hostname or "").lower()
            if host not in _DISCORD_WEBHOOK_HOSTS:
                return None
            parts = [seg for seg in (p.path or "").split("/") if seg]
            if (len(parts) < 4 or parts[0] != "api"
                    or parts[1] != "webhooks"):
                return None
            wid, token = parts[2], parts[3]
        else:
            return None
        if wid.isdigit() and token:
            return wid, token
        return None
    except Exception:
        return None


def _send_discord_embed(
    url: str, *, title: str, body: str,
    notify_type: NotifyType, attach_path: str,
) -> bool:
    """v1.23.2: native Discord webhook send with the thumbnail INSIDE
    an embed card. the user's side-by-side showed the v1.23.1 bare
    attachment matched the YouTube preview's image size but lacked
    its card chrome — and apprise structurally can't produce that
    look (its attachment POST strips the embed, so the image always
    dangles below the card). FB-thumbnail notifications route here
    for discord:// sinks; every other sink and every attachment-less
    notification keeps the apprise path. Never raises; never logs
    the URL (it carries the webhook token)."""
    import json as _json
    import os
    parsed = _parse_discord_webhook(url)
    if not parsed:
        return False
    wid, token = parsed
    try:
        import httpx
        fname = os.path.basename(attach_path)
        # v1.23.4: title/source/URL ride as plain CONTENT above the
        # card, mirroring the YouTube notifications (whose text sits
        # outside Discord's own unfurl card); the embed carries only
        # the framed image + severity color bar. Pre-fix the text
        # lived inside the embed — a different message anatomy than
        # every sibling notification (the user: "make it so they are
        # all one style").
        payload = {
            "content": f"{title}\n{body}",
            "embeds": [{
                "color": _DISCORD_EMBED_COLOR.get(notify_type, 0x6DFFB5),
                "image": {"url": f"attachment://{fname}"},
            }],
        }
        with open(attach_path, "rb") as f, \
                httpx.Client(timeout=15.0) as c:
            resp = c.post(
                f"https://discord.com/api/webhooks/{wid}/{token}",
                data={"payload_json": _json.dumps(payload)},
                files={"files[0]": (fname, f, "image/jpeg")},
            )
        if 200 <= resp.status_code < 300:
            return True
        log.warning("discord native embed send failed: HTTP %s %s",
                    resp.status_code, resp.text[:200])
        return False
    except Exception as e:  # noqa: BLE001
        log.warning("discord native embed send raised: %s", e)
        return False


def _dispatch_inline(
    db_path: Path, *, event_kind: str,
    urls: list[str], external_url: str,
    title: str, body: str,
    notify_type: NotifyType = "info",
    body_format: BodyFormat = "text",
    attach_url: str | None = None,
) -> None:
    """Run both dispatch paths in this thread (already inside the
    pool worker). Aggregates the outcome into one events-table
    entry for in-app health visibility.

    v1.22.94: `attach_url` — remote thumbnail to fetch/normalize and
    attach (embedded apprise only; the external apprise-api JSON
    shape has no attachment field, so it sends body-only there).
    v1.23.2: discord:// sinks take the native embed-card send when a
    thumbnail rides along; non-discord sinks keep the apprise
    attachment behavior."""
    sent_ok = 0
    sent_fail = 0
    attach_path = _prepare_attachment(attach_url) if attach_url else None
    try:
        apprise_urls = urls
        if attach_path and urls:
            native = [u for u in urls if _parse_discord_webhook(u)]
            apprise_urls = [u for u in urls if u not in native]
            # v1.23.3: route breadcrumb — the v1.23.2 parser silently
            # rejected raw-https webhook configs and every sink fell
            # back to apprise with zero signal; this line makes "why
            # no embed card" answerable from the container log.
            log.info(
                "notify attachment routing: %d discord native, "
                "%d apprise", len(native), len(apprise_urls))
            for u in native:
                if _send_discord_embed(
                        u, title=title, body=body,
                        notify_type=notify_type,
                        attach_path=attach_path):
                    sent_ok += 1
                else:
                    sent_fail += 1
        if apprise_urls:
            ok, fail = _send_embedded(
                apprise_urls, title, body, notify_type, body_format,
                attach_path=attach_path)
            sent_ok += ok
            sent_fail += fail
    finally:
        if attach_path:
            try:
                import os
                os.unlink(attach_path)
            except OSError:
                pass
    if external_url:
        ok, fail = _send_external(
            external_url, title, body, notify_type, body_format)
        sent_ok += ok
        sent_fail += fail
    detail = {
        "event_kind": event_kind,
        "sent_ok": sent_ok,
        "sent_fail": sent_fail,
        "embedded_url_count": len(urls),
        "external_url_configured": bool(external_url),
    }
    if sent_fail and not sent_ok:
        _log_outcome(
            db_path, event_kind=event_kind, level="WARNING",
            message=f"Notification dispatch failed: {event_kind}",
            detail=detail,
        )
    elif sent_fail:
        _log_outcome(
            db_path, event_kind=event_kind, level="WARNING",
            message=f"Notification partially failed: {event_kind} "
                    f"({sent_ok} ok, {sent_fail} failed)",
            detail=detail,
        )
    elif sent_ok:
        _log_outcome(
            db_path, event_kind=event_kind, level="INFO",
            message=f"Notification sent: {event_kind} "
                    f"(× {sent_ok})",
            detail=detail,
        )
    # Both 0 means no sinks configured — silent no-op (covered by
    # the gate in dispatch() but defensive: do not log on that
    # path; the caller already returned).


def dispatch(
    db_path: Path,
    notifications: "NotificationsConfig",
    *,
    event_kind: str,
    title: str,
    body: str,
    body_format: BodyFormat = "text",
    attach_url: str | None = None,
    _sync: bool = False,
    _record_inbox: bool = True,
) -> None:
    """Best-effort notification dispatch. Returns immediately —
    the actual send happens in the module thread pool. Never
    raises.

    v1.22.94: `attach_url` — optional remote thumbnail attached to
    the message (login-walled sources whose Discord unfurl we
    suppress). Fetched + normalized inside the pool worker so the
    caller never blocks on it.

    v1.20.63: `_sync=True` runs the send INLINE (blocking) instead of
    pool-submitting. Used only by flush_all_coalesced() at shutdown,
    where the daemon pool may not run the queued task before the
    interpreter exits.

    Caller passes:
      * `db_path` — for event-log writes (health visibility)
      * `notifications` — the NotificationsConfig section
      * `event_kind` — e.g. 'sync_completed'. Must be a key in
        notifications.events. Unknown kinds fall through with a
        WARN log (defensive — typo guard, not enforced via
        Literal so hook sites can be added incrementally).
      * `title` — short summary line (≤100 chars). Becomes the
        notification subject in services that distinguish title /
        body (Discord embed title, Pushover title, etc.).
      * `body` — full message body. Newlines OK.

    Per-event gate: if `notifications.events[event_kind]` is False
    (or missing — defensive default OFF), this call no-ops. Both
    sinks empty → no-op."""
    if event_kind not in notifications.events:
        log.warning("notify.dispatch called with unknown event_kind=%r; "
                    "defaulting to OFF — add it to "
                    "_DEFAULT_NOTIFY_EVENTS in config_file.py",
                    event_kind)
        return
    # v0.51.147: record the in-app INBOX row for inbox kinds UNCONDITIONALLY, before
    # the per-event Apprise toggle gate below — so an auto-add shows in the topbar
    # drawer even when that kind's Discord/Apprise toggle is off. The coalescer records
    # per-item itself and passes _record_inbox=False on its own dispatch()/flush sends
    # so a bulk burst isn't double-counted.
    if _record_inbox:
        from . import notify_inbox
        if event_kind in notify_inbox.INBOX_EVENT_KINDS:
            notify_inbox.record_notification(
                db_path, event_kind=event_kind,
                severity=_EVENT_NOTIFY_TYPE.get(event_kind, "info"),
                title=title, body=body,
            )
    if not notifications.events.get(event_kind, False):
        return  # event disabled by user (Apprise send only — inbox already recorded)
    urls = [u for u in (notifications.apprise_urls or []) if u]
    external_url = notifications.apprise_external_url or ""
    if not urls and not external_url:
        return  # no sinks configured
    # v1.17.1: per-event severity routing — look up the canonical
    # type for this event_kind, default to info. Threaded down to
    # the embedded / external send paths.
    notify_type = _EVENT_NOTIFY_TYPE.get(event_kind, "info")
    if _sync:
        # v1.20.63: shutdown-flush path — send in-thread so the batch
        # actually leaves before the process exits.
        _dispatch_inline(
            db_path, event_kind=event_kind, urls=urls,
            external_url=external_url, title=title, body=body,
            notify_type=notify_type, body_format=body_format,
            attach_url=attach_url,
        )
        return
    # Hand off to the pool — fire-and-forget. We don't await or
    # capture the future; the dispatcher's own logging is the
    # visibility surface.
    try:
        _get_pool().submit(
            _dispatch_inline,
            db_path,
            event_kind=event_kind,
            urls=urls,
            external_url=external_url,
            title=title,
            body=body,
            notify_type=notify_type,
            body_format=body_format,
            attach_url=attach_url,
        )
    except Exception as e:
        # Pool shutdown / RuntimeError shouldn't crash callers.
        log.debug("notify.dispatch pool.submit failed: %s", e)


# v1.19.95: coalescing layer for bursty per-item events
# (theme_pushed / theme_added). A bulk place — the user's 2026-05-29
# 33-item anime place — fired ONE Discord notification per item,
# tripping Discord's 429 rate-limit (12s throttles) and flooding the
# channel + event stream. dispatch_coalesced() buffers events per
# kind for a short debounce window; a lone event flushes as the
# normal rich single notification (URL embed intact), while ≥2 within
# the window collapse into ONE summary (count headline + capped
# bulleted labels, no URLs so Discord doesn't render N embed cards).
_COALESCE_LOCK = threading.Lock()
# Trailing buffer (events AFTER the leading edge) + a per-kind
# "burst window open?" flag. v1.19.97 reworked this from a pure
# debounce (every event delayed) to leading-edge + trailing tail.
_COALESCE_BUF: dict[str, list[dict]] = {}
_COALESCE_ACTIVE: dict[str, bool] = {}
_COALESCE_TIMERS: dict[str, threading.Timer] = {}
# v1.20.44: 120s window. Pre-v1.20.40 the worker drained a place
# burst in a tight ms-apart cluster, so an 8s window comfortably
# trailed it. Place-as-you-go (v1.20.40) now interleaves each place
# with its own download, spacing the theme_added/theme_pushed
# dispatches ~15-60s apart during a bulk download — every place
# landed AFTER the old 8s window had closed, re-leading as a fresh
# rich single = one Discord message per placed theme (the user's
# 2026-05-30 report: "after every push it sends a notification where
# the bulk place would have been in batches"). 120s spans the
# inter-place gap so a bulk's placements coalesce into batches again,
# exactly as the pre-v1.20.40 end-of-bulk burst did. Lone placements
# are unaffected — the leading edge still fires immediately; only the
# trailing batch summary waits out the (now longer) window. A tail
# cap flushes early so a huge burst can't grow the buffer unbounded.
_COALESCE_WINDOW_S = 120.0
_COALESCE_MAX_TAIL = 50

# v1.21.46: per-kind trailing-window overrides. The 120s default is
# tuned for theme_added/theme_pushed, whose place-as-you-go cadence
# spaces dispatches up to ~60s apart (v1.20.44) — shortening THAT
# window re-leads each placement as its own Discord message, exactly
# the per-place spam the user reported on 2026-05-30. theme_backed_up
# is different: its events arrive on the rate-limited DOWNLOAD cadence
# (~15-30s apart for a bulk ACCEPT / DOWNLOAD BACKUP), so a 60s window
# still spans the gap and coalesces the bulk into one summary while
# landing a full minute sooner — the user's "notifications feel delayed"
# follow-up. Scoped per-kind so the theme_added/pushed tuning is
# untouched. A kind absent from this map uses _COALESCE_WINDOW_S.
_COALESCE_WINDOW_BY_KIND: dict[str, float] = {
    "theme_backed_up": 60.0,
}


def _arm_coalesce_timer(db_path, notifications, event_kind, window_seconds):
    """(Re)arm the trailing-window flush timer for a kind. Cancels any
    prior timer so the window trails the LAST event in the burst.
    Caller MUST hold _COALESCE_LOCK."""
    prev = _COALESCE_TIMERS.pop(event_kind, None)
    if prev is not None:
        prev.cancel()
    timer = threading.Timer(
        window_seconds, _flush_coalesced,
        args=(db_path, notifications, event_kind),
    )
    timer.daemon = True
    _COALESCE_TIMERS[event_kind] = timer
    timer.start()


def dispatch_coalesced(
    db_path: Path,
    notifications: "NotificationsConfig",
    *,
    event_kind: str,
    item_label: str,
    single_title: str,
    single_body: str,
    batch_title_fn,
    batch_body_fn,
    body_format: BodyFormat = "text",
    window_seconds: float | None = None,
    single_attach_url: str | None = None,
    bulk: bool = False,
    section: str = "",
) -> None:
    """Coalesce ONLY genuine bulk actions; single actions notify immediately.

    v1.23.46: `bulk` is stamped on the job by the bulk endpoint (and propagated
    through the download→place chain). When False — a single user action — the
    rich single notification fires IMMEDIATELY with its preview, never buffered.
    When True — an actual bulk action — every item buffers into ONE list (no
    per-item previews) that flushes when the burst goes quiet. Pre-tag the
    coalescer inferred "bulk" purely from a time window, so N single manual
    actions in that window wrongly collapsed into a bulk-looking summary
    (the user's SET URL / UPLOAD MP3 fix-ups read as one batch).

    `item_label` — one-line summary for the batched bullet list.
    `single_title` / `single_body` / `body_format` — the rich payload.
    `batch_title_fn(n)` / `batch_body_fn(labels)` — build the summary.
    `bulk` — True only for an actual bulk action.

    Same gate as dispatch(): disabled events / no sinks don't buffer.
    Never raises."""
    # Gate up-front so a disabled event never buffers / sends.
    if event_kind not in notifications.events:
        log.warning("notify.dispatch_coalesced unknown event_kind=%r; "
                    "defaulting to OFF", event_kind)
        return
    # v0.51.147: record the PER-ITEM inbox row here (single_title/single_body),
    # before the toggle gate, so a bulk sync's auto-adds each get their own inbox
    # row even when the Apprise toggle is off. The dispatch()/flush sends below pass
    # _record_inbox=False so the item isn't double-counted.
    from . import notify_inbox
    if event_kind in notify_inbox.INBOX_EVENT_KINDS:
        notify_inbox.record_notification(
            db_path, event_kind=event_kind,
            severity=_EVENT_NOTIFY_TYPE.get(event_kind, "info"),
            title=single_title, body=single_body,
        )
    if not notifications.events.get(event_kind, False):
        return
    urls = [u for u in (notifications.apprise_urls or []) if u]
    external_url = notifications.apprise_external_url or ""
    if not urls and not external_url:
        return
    # v1.23.46: a SINGLE action (bulk=False) never coalesces — it fires the
    # rich single IMMEDIATELY, preview intact, no window, no shutdown-drop.
    # the user: "these were single actions taken one after another … only do the
    # bulk notifications if it's an actual bulk action."
    if not bulk:
        _akw = ({"attach_url": single_attach_url}
                if single_attach_url else {})
        # _record_inbox=False: the inbox row was already recorded above (per-item).
        dispatch(db_path, notifications, event_kind=event_kind,
                 title=single_title, body=single_body,
                 body_format=body_format, _record_inbox=False, **_akw)
        return
    # v1.21.46: resolve the trailing window per-kind unless the caller passed
    # an explicit override.
    if window_seconds is None:
        window_seconds = _COALESCE_WINDOW_BY_KIND.get(
            event_kind, _COALESCE_WINDOW_S)
    # Bulk path — buffer EVERY item (no leading edge) so the whole burst lands
    # as ONE list (no per-item previews) when it goes quiet. A bulk that yields
    # exactly one item flushes via _dispatch_batch as the rich single (the user:
    # "bulk of one → still a single notification with preview").
    flush_tail: list[dict] | None = None
    try:
        with _COALESCE_LOCK:
            buf = _COALESCE_BUF.setdefault(event_kind, [])
            buf.append({
                "label": item_label,
                # v1.23.75: the item's Plex library, so a bulk batch can group
                # bullets by library (v1.22.45 sectioning) in _dispatch_batch.
                "section": section,
                "title": single_title,
                "body": single_body,
                "body_format": body_format,
                # v1.22.94: kept so a lone-tail flush re-sends the rich single
                # WITH its thumbnail attachment.
                "attach_url": single_attach_url,
                "batch_title_fn": batch_title_fn,
                "batch_body_fn": batch_body_fn,
            })
            if len(buf) >= _COALESCE_MAX_TAIL:
                # Bound the buffer — flush now but keep collecting (the timer
                # re-arms on every event) so a huge burst groups in chunks.
                flush_tail = _COALESCE_BUF.pop(event_kind)
            _arm_coalesce_timer(
                db_path, notifications, event_kind, window_seconds)
    except Exception as e:
        # v1.20.9 (class-9): warn, not debug — a jammed buffer dark-holes the
        # bulk list. The next bulk event re-arms the timer, so a one-off arm
        # failure loses coalescing for this chunk, not the notification.
        log.warning("notify.dispatch_coalesced buffer failed: %s", e)
    if flush_tail is not None:
        _dispatch_batch(db_path, notifications, event_kind, flush_tail)


def _flush_coalesced(db_path, notifications, event_kind: str) -> None:
    """Trailing-window timer callback — close the burst window and
    flush whatever buffered in the tail."""
    try:
        with _COALESCE_LOCK:
            tail = _COALESCE_BUF.pop(event_kind, [])
            _COALESCE_TIMERS.pop(event_kind, None)
            _COALESCE_ACTIVE[event_kind] = False
    except Exception as e:
        log.debug("notify._flush_coalesced drain failed: %s", e)
        return
    if tail:
        _dispatch_batch(db_path, notifications, event_kind, tail)


def _dispatch_batch(db_path, notifications, event_kind, items,
                    _sync: bool = False) -> None:
    """1 item → the rich single notification; ≥2 → one summary.

    v1.20.0 (rollover audit): wrapped so a formatter / dispatch raise
    can't silently kill the daemon Timer thread (the _flush_coalesced
    path) or escape the cap-flush synchronous path — either way the
    batch would vanish with no breadcrumb. log.warning (not debug) so
    a genuinely-dropped batch is visible.

    v1.20.63: `_sync` threads through to dispatch() for the
    shutdown-flush path."""
    # v1.20.63: only pass _sync when True so the normal (async) path
    # keeps its prior call signature — sync is the exceptional
    # shutdown-flush path, and dispatch() callers/mocks that predate
    # the flag stay compatible.
    sync_kw = {"_sync": True} if _sync else {}
    try:
        if len(items) == 1:
            it = items[0]
            # v1.22.94: attach_url only when set (v1.20.63 convention).
            _akw = ({"attach_url": it["attach_url"]}
                    if it.get("attach_url") else {})
            # _record_inbox=False: buffered items were recorded per-item in dispatch_coalesced.
            dispatch(db_path, notifications, event_kind=event_kind,
                     title=it["title"], body=it["body"],
                     body_format=it["body_format"], _record_inbox=False,
                     **_akw, **sync_kw)
            return
        labels = [it["label"] for it in items]
        # v1.23.75: group the bulk batch by Plex library (v1.22.45 sectioning,
        # extended from sync to the coalesced bulk batches) when EVERY item
        # carried its section. The formatter renders headers only for ≥2
        # libraries, so a single-library bulk stays a flat list. Missing
        # section on any item → flat (None).
        buckets: dict = {}
        if items and all(it.get("section") for it in items):
            for it in items:
                buckets.setdefault(it["section"], []).append(it["label"])
        dispatch(
            db_path, notifications, event_kind=event_kind,
            title=items[0]["batch_title_fn"](len(items)),
            body=items[0]["batch_body_fn"](labels, buckets or None),
            body_format="markdown", _record_inbox=False, **sync_kw,
        )
    except Exception as e:
        log.warning(
            "notify._dispatch_batch dropped %d %s notification(s): %s",
            len(items), event_kind, e)


def flush_all_coalesced() -> None:
    """v1.20.63: drain every pending coalesced tail SYNCHRONOUSLY on
    shutdown. The trailing-window timers are daemon threads (killed at
    interpreter exit with their batch un-sent), and with the v1.20.44
    window at 120s a bulk place finishing < 2 min before a redeploy
    would silently lose its batch summary. Wired from the main.py
    SIGTERM/SIGINT handler. Never raises — shutdown must proceed.

    Each timer carries (db_path, notifications, event_kind) in its
    .args, so we recover the config to dispatch without a registry."""
    drained: list[tuple] = []
    try:
        with _COALESCE_LOCK:
            for event_kind, timer in list(_COALESCE_TIMERS.items()):
                try:
                    timer.cancel()
                except Exception:
                    pass
                try:
                    db_path, notifications, _ek = timer.args
                except Exception:
                    db_path, notifications = None, None
                items = _COALESCE_BUF.pop(event_kind, [])
                _COALESCE_ACTIVE[event_kind] = False
                if items and db_path is not None:
                    drained.append((db_path, notifications, event_kind, items))
            _COALESCE_TIMERS.clear()
    except Exception as e:
        log.warning("notify.flush_all_coalesced snapshot failed: %s", e)
        return
    if not drained:
        return
    log.info("notify: flushing %d coalesced batch(es) on shutdown",
             len(drained))
    for db_path, notifications, event_kind, items in drained:
        _dispatch_batch(db_path, notifications, event_kind, items, _sync=True)


def test_dispatch(
    notifications: "NotificationsConfig",
    *,
    title: str = "motif notification test",
    body: str = "This is a test from motif. If you see this, your sinks are wired up correctly.",
) -> dict:
    """Synchronous test path for the TEST NOTIFICATION button.
    Runs each configured sink inline (no thread pool) and returns
    a structured outcome dict the API endpoint can surface in the
    UI. Bypasses the per-event gate (the test is meant to verify
    plumbing, not per-event semantics)."""
    urls = [u for u in (notifications.apprise_urls or []) if u]
    external_url = notifications.apprise_external_url or ""
    result: dict = {
        "embedded": {"configured": len(urls), "ok": 0, "fail": 0},
        "external": {"configured": bool(external_url), "ok": 0, "fail": 0},
    }
    if not urls and not external_url:
        result["error"] = (
            "No notification sinks configured. Add at least one "
            "Apprise URL or set an external Apprise API URL.")
        return result
    if urls:
        ok, fail = _send_embedded(urls, title, body)
        result["embedded"]["ok"] = ok
        result["embedded"]["fail"] = fail
    if external_url:
        ok, fail = _send_external(external_url, title, body)
        result["external"]["ok"] = ok
        result["external"]["fail"] = fail
    return result
