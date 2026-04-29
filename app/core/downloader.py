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
import re
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
        return {
            FailureKind.COOKIES_EXPIRED: "YouTube cookies missing or expired",
            FailureKind.VIDEO_PRIVATE: "Video is private",
            FailureKind.VIDEO_REMOVED: "Video was removed or deleted",
            FailureKind.VIDEO_AGE_RESTRICTED: "Video is age-restricted",
            FailureKind.GEO_BLOCKED: "Video is geo-blocked from your region",
            FailureKind.NETWORK_ERROR: "Network error reaching YouTube",
            FailureKind.UNKNOWN: "Unknown error",
        }[self]

    @property
    def needs_manual_override(self) -> bool:
        """True if the only fix is for the user to provide a different YouTube URL."""
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
    if "private video" in m or "this video is private" in m:
        return FailureKind.VIDEO_PRIVATE
    if "video unavailable" in m and "private" not in m:
        # 'Video unavailable. The uploader has not made this video available'
        return FailureKind.VIDEO_REMOVED
    if "removed by the user" in m or "this video has been removed" in m:
        return FailureKind.VIDEO_REMOVED
    if "no longer available" in m or "is unavailable" in m:
        return FailureKind.VIDEO_REMOVED
    if "this video has been removed" in m or "account associated" in m:
        return FailureKind.VIDEO_REMOVED
    if "age" in m and ("restrict" in m or "confirm your age" in m):
        return FailureKind.VIDEO_AGE_RESTRICTED
    if ("inappropriate" in m and "audience" in m):
        return FailureKind.VIDEO_AGE_RESTRICTED
    if ("not available in your country" in m or "geo" in m
            or "region" in m and "block" in m):
        return FailureKind.GEO_BLOCKED
    if ("sign in" in m and ("confirm" in m or "bot" in m)) or "cookies" in m:
        return FailureKind.COOKIES_EXPIRED
    if "http error" in m or "connection" in m or "timeout" in m or "timed out" in m:
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


def _opts(
    *,
    output_path: Path,
    cookies_file: Path | None,
    audio_quality: str = "0",
) -> dict[str, Any]:
    """Build yt-dlp options dict."""
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
    if cookies_file and cookies_file.exists():
        opts["cookiefile"] = str(cookies_file)
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
) -> DownloadResult:
    """
    Download a single YouTube theme to {output_dir}/{video_id}.mp3.

    File naming uses the video ID (not the title) for stability — the upstream
    title can change without the audio actually changing. The mp3 is named after
    the video so we can detect "this is the same theme" cheaply.
    """
    if yt_dlp is None:
        raise DownloadError("yt-dlp is not installed in this environment")

    if cookies_file and not cookies_file.exists():
        log.warning(
            "Cookies file %s does not exist — downloads may fail", cookies_file
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / video_id  # extension will be added by yt-dlp postproc
    expected_mp3 = output_dir / f"{video_id}.mp3"

    if expected_mp3.exists():
        # Already downloaded — return existing file's metadata
        return DownloadResult(
            file_path=expected_mp3,
            file_size=expected_mp3.stat().st_size,
            file_sha256=_sha256(expected_mp3),
            video_id=video_id,
        )

    opts = _opts(
        output_path=target,
        cookies_file=cookies_file,
        audio_quality=audio_quality,
    )

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

    if not expected_mp3.exists():
        # ffmpeg may have produced a different extension if conversion failed.
        # Look for any file matching {video_id}.* and try to recover.
        for f in output_dir.glob(f"{video_id}.*"):
            if f.suffix.lower() == ".mp3":
                expected_mp3 = f
                break
        else:
            raise DownloadError(
                f"Download finished but no MP3 produced for {video_id}"
            )

    return DownloadResult(
        file_path=expected_mp3,
        file_size=expected_mp3.stat().st_size,
        file_sha256=_sha256(expected_mp3),
        video_id=video_id,
    )
