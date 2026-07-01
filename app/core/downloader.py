"""
YouTube theme song downloader.

Uses yt-dlp as a Python library (not subprocess). Cookies are mandatory in
practice — without them YouTube blocks unauthenticated downloads aggressively.
The cookies file path comes from settings; if the file is missing, downloads
will be attempted but warned.

Failures are classified into a structured FailureKind enum so the UI can
distinguish 'cookies expired' (global problem) from 'video deleted' (per-item,
needs manual override).
"""
from __future__ import annotations

import enum
import hashlib
import logging
import os
import shutil
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yt_dlp  # type: ignore
except ImportError:  # pragma: no cover
    yt_dlp = None

log = logging.getLogger(__name__)


class FailureKind(str, enum.Enum):
    COOKIES_EXPIRED = "cookies_expired"
    VIDEO_PRIVATE = "video_private"
    VIDEO_REMOVED = "video_removed"
    VIDEO_AGE_RESTRICTED = "video_age_restricted"
    GEO_BLOCKED = "geo_blocked"
    NETWORK_ERROR = "network_error"
    UNKNOWN = "unknown"

    @property
    def human(self) -> str:
        # v1.14.1: source-agnostic phrasing. Pre-fix every label
        # said "YouTube" / "Video" — wrong on a SoundCloud failure.
        # "Track or video" reads naturally for either source; bare
        # "Source" reads for any future yt-dlp extractor.
        return {
            FailureKind.COOKIES_EXPIRED: "Cookies missing or expired",
            FailureKind.VIDEO_PRIVATE: "Track or video is private",
            FailureKind.VIDEO_REMOVED: "Track or video was removed",
            FailureKind.VIDEO_AGE_RESTRICTED: "Track or video is age-restricted",
            FailureKind.GEO_BLOCKED: "Track or video is geo-blocked from your region",
            FailureKind.NETWORK_ERROR: "Network error reaching source",
            FailureKind.UNKNOWN: "Unknown error",
        }[self]

    @property
    def needs_manual_override(self) -> bool:
        """True if the only fix is for the user to provide a different
        theme URL (YouTube or SoundCloud)."""
        return self in (
            FailureKind.VIDEO_PRIVATE,
            FailureKind.VIDEO_REMOVED,
            FailureKind.VIDEO_AGE_RESTRICTED,
            FailureKind.GEO_BLOCKED,
        )


def classify_yt_dlp_error(msg: str) -> FailureKind:
    """Classify a yt-dlp error message into a FailureKind. Patterns are based
    on yt-dlp's actual error messages — these can change between versions, so
    we look for stable substrings rather than exact matches.
    """
    m = (msg or "").lower()
    # Order matters: more specific first
    # v1.14.1: SoundCloud variants added alongside YouTube. yt-dlp
    # uses different phrasing per extractor; we look for stable
    # substrings that should survive yt-dlp version bumps.
    if ("private video" in m or "this video is private" in m
            or "private track" in m or "track is private" in m
            # v1.20.26: Instagram private account / post.
            or "account is private" in m
            or "this post is private" in m):
        return FailureKind.VIDEO_PRIVATE
    # v1.15.12: YouTube IP rate-limit MUST be classified before the
    # generic "video unavailable" branch below, because YouTube's
    # rate-limit response starts with that phrase:
    #   "Video unavailable. This content isn't available, try again
    #    later. The current session has been rate-limited by YouTube
    #    for up to an hour. ..."
    # Pre-fix: bulk PROBE TDB URLS / REPROBE FAILURES would
    # red-pill thousands of actually-alive rows whenever the probe
    # ran fast enough to trigger YouTube's throttling (the user
    # v1.15.11 repro: 2005 "dead" out of 2507 probed, the vast
    # majority of which were rate-limit responses on alive videos).
    # Bulk-probe handler treats NETWORK_ERROR as transient → does
    # NOT write failure_kind → row state is preserved instead of
    # being falsely marked dead.
    if "rate-limited" in m or "rate limit" in m:
        return FailureKind.NETWORK_ERROR
    # v1.23.42 (mirrors the v1.15.12 rate-limit fix): transient signals MUST
    # be classified BEFORE the broad "video unavailable" / "is unavailable" /
    # "no longer available" dead patterns below. yt-dlp emits "try again later"
    # / "temporarily unavailable" / read-timeouts / connection errors on
    # transient YouTube anti-bot + network faults whose messages ALSO carry an
    # "unavailable" substring — pre-fix those fell through to VIDEO_REMOVED and
    # red-pilled LIVE themes (needs_manual_override → dropped from "ready to
    # add", no auto-retry), the same false-dead the rate-limit case caused
    # before v1.15.12. NETWORK_ERROR is transient → row state preserved +
    # retried; a genuinely-removed video never carries these tokens. (The
    # generic "http error" stays at the tail so a "410 Gone … has been removed"
    # still hits the dead pattern below first.)
    if ("try again later" in m or "temporarily unavailable" in m
            or "temporary failure" in m or "service unavailable" in m
            or "timed out" in m or "timeout" in m or "connection" in m):
        return FailureKind.NETWORK_ERROR
    if "video unavailable" in m and "private" not in m:
        # 'Video unavailable. The uploader has not made this video available'
        return FailureKind.VIDEO_REMOVED
    if ("removed by the user" in m or "this video has been removed" in m
            or "track has been removed" in m
            or "track is no longer available" in m):
        return FailureKind.VIDEO_REMOVED
    if "no longer available" in m or "is unavailable" in m:
        return FailureKind.VIDEO_REMOVED
    if "this video has been removed" in m or "account associated" in m:
        return FailureKind.VIDEO_REMOVED
    # v1.14.1: SoundCloud's "track" wording for the general
    # "couldn't find / 404" case. Maps to REMOVED so the recovery
    # card surfaces SET URL / UPLOAD MP3 (the only fixes).
    if ("track unavailable" in m or "track not available" in m
            or "track was deleted" in m):
        return FailureKind.VIDEO_REMOVED
    # v1.12.85 added a broad "is not available" / "not available" catch
    # here that classified yt-dlp's generic "This video is not available"
    # message as VIDEO_REMOVED (permanent). v1.12.88 reverted: the
    # message is too ambiguous — yt-dlp prints it for actually-removed
    # videos AND for transient situations (anti-bot, cookies needed,
    # IP throttling, parser quirks) where the URL still works in a
    # browser. The other patterns above catch yt-dlp's specific
    # known-removed phrasings ("video unavailable", "no longer
    # available", "removed by the user", "this video has been
    # removed"). The bare "is not available" case falls through to
    # UNKNOWN, which is transient → retry with backoff up to
    # max_attempts → eventual permanent failure if it never recovers.
    # The topbar dot still reads themes failure_kind/failure_acked_at
    # so users see the problem immediately and can ACK / fix it,
    # regardless of how the worker classifies it.
    if "age" in m and ("restrict" in m or "confirm your age" in m):
        return FailureKind.VIDEO_AGE_RESTRICTED
    if ("inappropriate" in m and "audience" in m):
        return FailureKind.VIDEO_AGE_RESTRICTED
    # v1.13.52: rights-holder regional blocks land here too. Pre-fix
    # YouTube's "This video contains content from <X>, who has
    # blocked it in your country on copyright grounds" fell through
    # to UNKNOWN — none of the prior patterns ("not available in
    # your country", "geo", "region"+"block") matched the literal
    # phrasing. Same recovery path as GEO_BLOCKED (SET URL /
    # UPLOAD MP3 — the user can usually find an alternative
    # source) so bucketed there rather than adding a new kind.
    if ("not available in your country" in m or "geo" in m
            or ("region" in m and "block" in m)
            or "blocked it in your country" in m
            or "copyright grounds" in m
            # v1.14.1: SoundCloud's region-restriction phrasing.
            or "track is not available in your location" in m
            or "not available in your location" in m):
        return FailureKind.GEO_BLOCKED
    if (("sign in" in m and ("confirm" in m or "bot" in m))
            or "cookies" in m
            # v1.14.1: SoundCloud Go+ / paid-only tracks.
            # Treating as cookies-expired so the recovery card
            # offers SET URL (find a non-paywalled alternative)
            # AND DROP cookies.txt (the existing fallback). A
            # dedicated PAID_CONTENT kind would be cleaner but
            # this covers the user's homelab use case without a
            # schema change.
            or "go+ subscription" in m
            or "premium content" in m
            or "subscription required" in m
            # v1.20.26: Instagram's login wall. Without IG cookies in
            # cookies.txt, yt-dlp returns "login required" /
            # "requested content is not available, rip" for most reels.
            # Map to COOKIES_EXPIRED so the recovery card surfaces the
            # add-cookies UX (the common cause on a homelab without IG
            # cookies); a genuinely-removed reel offers SET URL / UPLOAD
            # MP3 from the same card, so the misclassification is benign.
            or "login required" in m
            or "you need to log in" in m
            or "requested content is not available" in m
            or ("instagram" in m and "log in" in m)
            # v1.22.90: Facebook's login wall (same UX as IG's) —
            # yt-dlp reports age/region-gated or login-walled FB
            # videos as "only available for registered users".
            or "registered users" in m
            or ("facebook" in m and "log in" in m)):
        return FailureKind.COOKIES_EXPIRED
    # v1.23.42: the transient network tokens (timeout/connection/temporary)
    # moved UP above the dead patterns; only the generic "http error" stays
    # here — a 4xx/5xx with no removed-text is transient (retry), while a
    # "410 Gone … has been removed" already hit the dead pattern above.
    if "http error" in m:
        return FailureKind.NETWORK_ERROR
    return FailureKind.UNKNOWN


class DownloadError(Exception):
    """Carries a FailureKind so the worker can act differently per kind."""
    def __init__(self, msg: str, kind: FailureKind = FailureKind.UNKNOWN):
        super().__init__(msg)
        self.kind = kind


class CookiesMissingError(DownloadError):
    def __init__(self, msg: str):
        super().__init__(msg, kind=FailureKind.COOKIES_EXPIRED)


@dataclass
class DownloadResult:
    file_path: Path
    file_size: int
    file_sha256: str
    video_id: str


def _cookiefile_snapshot(cookies_file: Path) -> str:
    # v1.22.77: yt-dlp's close() truncate-rewrites its cookiefile. With
    # concurrent downloads/probes sharing /config/cookies.txt, the LAST
    # closer wrote a stale jar back (YouTube rotates mid-run → session
    # invalidated → fleet-wide COOKIES_EXPIRED that read as "my cookies
    # aged out"), and a kill mid-save could truncate the operator's
    # file. Per-thread throwaway copy, overwritten per call so /tmp
    # stays bounded; rotations are deliberately NOT persisted — the
    # operator owns cookies.txt.
    tmp = Path(tempfile.gettempdir()) / (
        f"motif-cookies-{os.getpid()}-{threading.get_ident()}.txt")
    shutil.copyfile(cookies_file, tmp)
    # v0.50.89 (audit MEDIUM): shutil.copyfile doesn't preserve the source's
    # permission bits (it never calls copymode/copystat), so this throwaway
    # copy — holding live session cookies — landed at whatever the
    # container's umask produces (644 under the documented default UMASK=022,
    # i.e. world-readable) even when the operator hardened cookies_file
    # itself to 600. Lock it down explicitly instead of trusting umask.
    os.chmod(tmp, 0o600)
    return str(tmp)


def probe_youtube_url(
    youtube_url: str,
    *,
    cookies_file: Path | None = None,
    timeout_seconds: float = 15.0,
) -> FailureKind | None:
    """v1.10.43: cheap availability check — extract metadata without
    downloading. Returns None when the URL is reachable + playable;
    a FailureKind otherwise. Used by sync to flag dead / cookied /
    restricted ThemerrDB URLs before the user tries to download.

    yt-dlp's extract_info(download=False) does an HTTP fetch of the
    /watch page + cipher decode (~0.5–1.5 s per URL on a warm DNS
    cache). Caller is responsible for batching / rate-limiting; the
    sync flow only probes new + url_changed entries to keep the
    cost bounded.

    `cookies_file` is honored when provided so cookies-required
    videos resolve correctly when the user has cookies.txt set up
    (otherwise we'd flag every age-restricted item as cookies_expired
    even though the user has the file).
    """
    if yt_dlp is None:
        return FailureKind.UNKNOWN
    if not youtube_url:
        return FailureKind.UNKNOWN
    opts: dict[str, Any] = {
        "quiet": True,
        "noprogress": True,
        "no_warnings": True,
        "skip_download": True,
        "socket_timeout": timeout_seconds,
        # Cheap probe — don't fetch playlist metadata or comments.
        "extract_flat": False,
        "playlistend": 1,
    }
    if cookies_file and cookies_file.is_file():
        # v1.22.77: throwaway per-thread copy — see _cookiefile_snapshot.
        opts["cookiefile"] = _cookiefile_snapshot(cookies_file)
    # v1.15.26: mirror the YouTube extractor opts from _opts() (the
    # download path). Pre-fix the probe used yt-dlp's defaults
    # which — without a declared js_runtime — fall back to the
    # `android_vr` player client. The v1.12.89 _opts() comment
    # documented this exact failure: android_vr "returns 'This
    # video is not available' for many otherwise-playable videos".
    # Result: the probe reported "indeterminate / could be dead"
    # for URLs that the download path successfully fetched
    # seconds later (the user: KWXXC228g24 = Rubble & Crew theme,
    # ?Unknown error in INFO card but actual download succeeded
    # via Replace-with-ThemerrDB). The probe SHOULD use the same
    # extractor pipeline so its verdict matches what download
    # would actually do. Gated on YouTube URLs (matches _opts()
    # source-aware gating); SoundCloud + others don't need the
    # js_runtime / EJS / player_client list.
    if ("youtube.com" in youtube_url or "youtu.be" in youtube_url):
        opts["js_runtimes"] = {"node": {}}
        opts["remote_components"] = ["ejs:github"]
        opts["extractor_args"] = {
            "youtube": {
                "player_client": [
                    "default",
                    "android",
                    "ios",
                    "mweb",
                ],
            },
        }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(youtube_url, download=False)
    except yt_dlp.utils.DownloadError as e:
        # v1.15.5: "Requested format is not available" means the
        # video EXISTS (yt-dlp got far enough to query formats)
        # but the format spec we want isn't available right now.
        # Common causes: stale yt-dlp version, age-gated video
        # without auth (only low-res formats exposed), region-
        # locked format manifest, members-only content. NONE of
        # these are deadness signals — the URL is alive enough
        # to enumerate formats. Treat as alive (return None) so
        # the bulk-probe flow doesn't red-pill rows whose only
        # issue is a transient format-spec mismatch. the user's
        # repro: bulk-probe-tdb run flooded the docker logs with
        # "Requested format is not available" lines and the UI
        # showed "Unknown error (ambiguous — could be transient
        # anti-bot/cookies/throttling, or actually dead)" on
        # rows whose videos were actually fine.
        msg = str(e).lower()
        if "requested format is not available" in msg:
            return None
        return classify_yt_dlp_error(str(e))
    except Exception as e:  # network blip, redirect loop, etc.
        return classify_yt_dlp_error(str(e))
    # extract_info returned a dict — playable. (Age-restricted videos
    # without cookies typically raise DownloadError; if we get here
    # the metadata fetch succeeded.)
    if info is None:
        return FailureKind.UNKNOWN
    return None


def _source_for(url: str) -> str:
    """Mirror of sync.url_source() — returns 'youtube' / 'soundcloud'
    / 'unknown'. Kept local rather than imported so this module's
    import graph stays narrow (downloader.py is intentionally near
    the bottom of the dep tree)."""
    if not url:
        return "unknown"
    if "youtube.com" in url or "youtu.be" in url:
        return "youtube"
    if "soundcloud.com" in url:
        return "soundcloud"
    # v1.20.26: Instagram reels/posts/IGTV. Uses the default (non-YT)
    # yt-dlp path — no JS runtime / EJS / player_client needed.
    if "instagram.com" in url:
        return "instagram"
    # v1.22.90: Facebook videos/reels/watch + fb.watch shortlinks.
    # Same default non-YT yt-dlp path as Instagram.
    if "facebook.com" in url or "fb.watch" in url:
        return "facebook"
    return "unknown"


def _opts(
    *,
    output_path: Path,
    cookies_file: Path | None,
    audio_quality: str = "0",
    geo_bypass: bool = False,
    geo_bypass_country: str = "",
    proxy_url: str = "",
    source: str = "youtube",
) -> dict[str, Any]:
    """Build yt-dlp options dict.

    v1.13.53: geo_bypass / geo_bypass_country / proxy_url are
    user-configurable knobs for routing around regional content
    blocks. geo_bypass injects fake X-Forwarded-For headers (cheap,
    YouTube usually ignores for rights blocks); proxy_url routes
    through a real HTTP/SOCKS5 proxy (the option that actually
    solves YouTube content blocks because it changes the source IP).

    v1.14.5: source-aware — YouTube-only opts (js_runtimes,
    remote_components, extractor_args.youtube) are skipped when the
    URL targets a non-YT source (SoundCloud). yt-dlp tolerates
    extraneous opts but the EJS remote-component fetch + node
    runtime startup add 1-3s of pointless latency to every SC
    download, and the `youtube` extractor_args block confuses
    yt-dlp's extractor selection in some 2025+ builds.
    """
    opts: dict[str, Any] = {
        "format": "bestaudio/best",
        "outtmpl": str(output_path.with_suffix(".%(ext)s")),
        "quiet": True,
        "noprogress": True,
        "no_warnings": True,
        # Convert to MP3 via ffmpeg (must be installed in the container)
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": audio_quality,
        }],
        # Don't write info JSON, descriptions, thumbnails, etc.
        "writeinfojson": False,
        "writethumbnail": False,
        "writesubtitles": False,
        # Only download the audio, no video
        "noplaylist": True,
        # Retry behavior
        "retries": 2,
        "fragment_retries": 2,
        # Don't pollute filesystem with .part files on partial failure
        "continuedl": True,
        # Speed limits help avoid getting flagged
        "ratelimit": 5_000_000,  # 5 MB/s ceiling
    }
    if source == "youtube":
        # v1.12.89: declare nodejs as the JS runtime so yt-dlp can use
        # the full `web` player client. Without a JS runtime yt-dlp
        # falls back to `android_vr` (the only client that doesn't
        # need JS to derive cipher signatures), which returns "This
        # video is not available" for many otherwise-playable videos.
        # Format is `{runtime_name: {config}}` per yt-dlp's source —
        # empty config dict picks up the binary from PATH. The
        # Dockerfile installs nodejs alongside ffmpeg; native installs
        # can pre-install deno or node and yt-dlp will find either.
        # v1.14.5: gated on source=='youtube' — SoundCloud's extractor
        # doesn't need a JS runtime (no cipher signatures), and the
        # node startup cost was being paid for every SC download.
        opts["js_runtimes"] = {"node": {}}
        # v1.12.91: enable yt-dlp's remote-component fetcher (ejs from
        # the official yt-dlp GitHub repo) so node can solve YouTube's
        # signature + n challenges. Without this, even with node as a
        # JS runtime, yt-dlp falls back to clients that don't need
        # signature solving (web_embedded, android_vr) which on many
        # videos return only the low-quality 360p MP4 fallback (format
        # 18 — 22 kHz mono audio). Enabling EJS unlocks formats 140
        # (128k AAC, 44.1 kHz stereo) and 251 (160k Opus, 48 kHz
        # stereo) — far better source material for the MP3 transcode.
        # v1.14.5: gated on YT — the EJS bundle is YouTube-specific.
        opts["remote_components"] = ["ejs:github"]
        # v1.12.89: tell yt-dlp to try multiple YouTube player clients
        # in order. If `web` (JS-required) trips anti-bot or fails to
        # extract, the fallback list keeps trying — `android` and
        # `mweb` work for many videos that `web` rejects, and don't
        # need a JS runtime. Belt-and-suspenders alongside the nodejs
        # install above.
        # v1.12.91: dropped 'tv_embedded' from the list — yt-dlp 2025+
        # logs "Skipping unsupported client" for it (no-op).
        # v1.14.5: gated on YT — the player_client list is YT-extractor
        # specific; pre-fix it sat in the opts even on SC downloads
        # and confused yt-dlp's extractor selection on some 2025+ builds.
        opts["extractor_args"] = {
            "youtube": {
                "player_client": [
                    "default",
                    "android",
                    "ios",
                    "mweb",
                ],
            },
        }
    if cookies_file and cookies_file.exists():
        # v1.22.77: throwaway per-thread copy — see _cookiefile_snapshot.
        opts["cookiefile"] = _cookiefile_snapshot(cookies_file)
    # v1.13.53: bypass options. geo_bypass + geo_bypass_country pass
    # through to yt-dlp's built-in XFF spoofer; proxy_url routes the
    # whole HTTP transport through the given proxy (yt-dlp respects
    # urllib's standard proxy-URL scheme).
    if geo_bypass:
        opts["geo_bypass"] = True
    gbc = (geo_bypass_country or "").strip().upper()
    if gbc:
        opts["geo_bypass_country"] = gbc
    proxy = (proxy_url or "").strip()
    if proxy:
        opts["proxy"] = proxy
    return opts


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download_theme(
    *,
    youtube_url: str,
    video_id: str,
    output_dir: Path,
    cookies_file: Path | None,
    audio_quality: str = "0",
    geo_bypass: bool = False,
    geo_bypass_country: str = "",
    proxy_url: str = "",
    progress_callback: "callable | None" = None,
) -> DownloadResult:
    """
    Download a single YouTube theme to {output_dir}/theme.mp3.

    output_dir is item-specific (themes_dir/<movies|tv>/<sanitized title (year)>),
    so a flat filename "theme.mp3" mirrors what motif places into Plex.

    v1.13.18 (6C): progress_callback is invoked with a fraction (0.0-1.0)
    on each yt-dlp progress event. None disables the per-job
    progress feed; the worker passes a callback that updates a shared
    dict so /api/progress can render real download % in the topbar.

    If theme.mp3 already exists at the target, this is a no-op — callers that
    need to force a fresh download (e.g. video_id changed) must unlink it first.
    """
    if yt_dlp is None:
        raise DownloadError("yt-dlp is not installed in this environment")

    if cookies_file and not cookies_file.exists():
        log.warning(
            "Cookies file %s does not exist — downloads may fail", cookies_file
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / "theme"  # extension added by yt-dlp postproc
    expected_mp3 = output_dir / "theme.mp3"

    if expected_mp3.exists():
        # Already downloaded — return existing file's metadata. The worker
        # is responsible for invalidating this when source_video_id changes.
        # v1.22.29 (audit): but ONLY if it's non-empty. Pre-fix a 0-byte /
        # truncated theme.mp3 left by a crashed prior download (container OOM
        # mid-ffmpeg, disk full, power loss) was returned as a SUCCESSFUL
        # DownloadResult — the worker recorded local_files + hardlinked a broken
        # theme into Plex behind a green ✓ (the worker's unlink-stale only fires
        # on a source_video_id/mismatch change, not a same-video retry of a
        # failed download). A 0-byte file is removed + re-downloaded instead.
        _existing_size = expected_mp3.stat().st_size
        if _existing_size > 0:
            return DownloadResult(
                file_path=expected_mp3,
                file_size=_existing_size,
                file_sha256=_sha256(expected_mp3),
                video_id=video_id,
            )
        log.warning(
            "download_theme: stale 0-byte %s from a prior failed download — "
            "removing + re-downloading", expected_mp3)
        try:
            expected_mp3.unlink()
        except OSError as e:
            log.warning("download_theme: could not remove stale 0-byte file "
                        "%s: %s", expected_mp3, e)

    # v1.14.5: source-aware opts — skip YT-only options (js_runtimes,
    # remote_components, extractor_args.youtube) when downloading from
    # SoundCloud. Saves the node startup + EJS remote-fetch cost on
    # every SC download.
    opts = _opts(
        output_path=target,
        cookies_file=cookies_file,
        audio_quality=audio_quality,
        geo_bypass=geo_bypass,
        geo_bypass_country=geo_bypass_country,
        proxy_url=proxy_url,
        source=_source_for(youtube_url),
    )

    # v1.13.18 (6C): wire yt-dlp's progress_hooks to the optional
    # callback. The hook fires every ~250ms during a download with
    # downloaded_bytes / total_bytes (or total_bytes_estimate) so we
    # can compute a fraction and feed it through to op_progress.
    # We swallow callback exceptions so a buggy/disconnected progress
    # consumer can't fail the actual download.
    if progress_callback is not None:
        def _yt_progress_hook(d):
            try:
                status = d.get("status")
                if status == "downloading":
                    downloaded = d.get("downloaded_bytes") or 0
                    total = (d.get("total_bytes")
                             or d.get("total_bytes_estimate") or 0)
                    if total > 0:
                        progress_callback(min(1.0, downloaded / total))
                elif status == "finished":
                    progress_callback(1.0)
            except Exception as e:
                # v1.15.35: log progress-callback failures at WARNING.
                # Pre-fix bare `pass` silently broke the topbar
                # progress bar — operator saw a stalled "0%" and
                # may have cancelled wrongly. The download itself
                # continues either way; this is a UX-feedback bug,
                # not a download bug. Surfacing lets the operator
                # correlate stuck bars with the actual cause.
                log.warning(
                    "yt-dlp progress hook callback failed: %s", e,
                )
        opts = dict(opts)
        opts["progress_hooks"] = [_yt_progress_hook]

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([youtube_url])
    except yt_dlp.utils.DownloadError as e:
        kind = classify_yt_dlp_error(str(e))
        if kind == FailureKind.COOKIES_EXPIRED:
            raise CookiesMissingError(str(e)) from e
        raise DownloadError(str(e), kind=kind) from e
    except Exception as e:
        # Sometimes yt-dlp wraps exceptions weirdly — fall through to a classifier
        kind = classify_yt_dlp_error(str(e))
        raise DownloadError(f"yt-dlp failed: {e}", kind=kind) from e

    # v1.13.30: emit a final 1.0 fraction after yt-dlp returns
    # successfully. Pre-fix the topbar download bar sometimes stuck
    # short of 100% — yt-dlp's "downloading" status emits stopped
    # before downloaded/total ever reached parity (e.g., chunked
    # transfer where the last few bytes arrive without firing a
    # progress event), and the "finished" status didn't always
    # fire on every file in the run. Belt-and-suspenders: once
    # ydl.download() returns without raising, the file is on disk
    # and the user-facing % should reflect that.
    if progress_callback is not None:
        try:
            progress_callback(1.0)
        except Exception as e:
            # v1.15.35: log post-download finalization callback
            # failures at DEBUG. Lower severity than the in-loop
            # hook (above) — at this point the file IS on disk,
            # so the worst case is the topbar bar stays at 99%
            # for a tick instead of snapping to 100%.
            log.debug(
                "yt-dlp post-download progress(1.0) callback failed: %s",
                e,
            )

    if not expected_mp3.exists():
        # ffmpeg may have produced a different extension if conversion failed.
        for f in output_dir.glob("theme.*"):
            if f.suffix.lower() == ".mp3":
                expected_mp3 = f
                break
        else:
            # v1.15.36: include the actual files found in the
            # error message. Pre-fix `no MP3 produced` gave the
            # operator zero hint about WHY (ffmpeg produced
            # `.m4a` because conversion failed? container has
            # broken ffmpeg? yt-dlp wrote nothing?). Listing
            # the surviving extensions in output_dir narrows
            # the diagnosis substantially.
            try:
                surviving = sorted(
                    f.suffix.lower() for f in output_dir.glob("theme.*")
                )
            except OSError:
                surviving = []
            hint = (
                str(surviving) if surviving
                else "(none — yt-dlp wrote no theme.* file; "
                     "check ffmpeg + container build)"
            )
            raise DownloadError(
                f"Download finished but no MP3 produced for "
                f"{video_id} — found extensions in output_dir: {hint}"
            )

    # v1.23.95 (audit): a FRESH download that produced a 0-byte theme.mp3 (ffmpeg
    # FFmpegExtractAudio postproc emitted an empty file without yt-dlp raising)
    # must NOT return success — the worker would record file_size=0 + hardlink an
    # empty theme into Plex behind a green ✓ (the v1.22.30 0-byte class; the
    # pre-existing-file branch above already guards >0, but this fresh return did
    # not — `if not expected_mp3.exists()` only catches a MISSING file, not a
    # present-but-empty one). Drop it + raise so the worker's retry ladder re-runs.
    _fresh_size = expected_mp3.stat().st_size
    if _fresh_size == 0:
        log.warning(
            "download_theme: fresh download produced a 0-byte %s for %s — "
            "treating as a failed download (ffmpeg/postproc likely failed)",
            expected_mp3, video_id)
        try:
            expected_mp3.unlink()
        except OSError:
            pass
        raise DownloadError(
            f"Download produced a 0-byte theme.mp3 for {video_id} — treating as "
            f"a failed download (ffmpeg / postprocessing likely failed)")
    # v0.50.89 (audit): this guard is deliberately scoped to the 0-byte class
    # only. A truncated-but-nonzero mp3 (e.g. ffmpeg killed mid-encode) still
    # passes — validating completeness would need an ffprobe subprocess on the
    # download hot path, and the case is both rare (yt-dlp only renames off its
    # `.part` file once its own download completes) and low-harm (a short theme
    # still plays). We intentionally accept that over a fixed size floor, which
    # would false-reject legitimately short themes.
    return DownloadResult(
        file_path=expected_mp3,
        file_size=_fresh_size,
        file_sha256=_sha256(expected_mp3),
        video_id=video_id,
    )
