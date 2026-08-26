# syntax=docker/dockerfile:1.7
# motif · automated theme orchestration for Plex
#
# Build stage installs Python deps into a venv that we copy into the runtime
# image. Runtime is python-slim with ffmpeg added — yt-dlp shells out to it
# for MP3 conversion.

# ---------- builder ----------
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip wheel \
    && pip install -r requirements.txt

# ---------- runtime ----------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    MOTIF_CONFIG_DIR=/config \
    MOTIF_DATA_DIR=/data \
    MOTIF_COOKIES_FILE=/config/cookies.txt \
    MOTIF_WEB_HOST=0.0.0.0 \
    MOTIF_WEB_PORT=5309

# ffmpeg for yt-dlp's audio extraction; tini for clean PID 1 signal handling;
# curl for the healthcheck. v1.12.89: nodejs as a JS runtime for yt-dlp's
# YouTube extractor — the 2025-era yt-dlp deprecated extraction without a
# JS runtime (https://github.com/yt-dlp/yt-dlp/wiki/EJS), and the
# JS-less fallback (android_vr player client) returns "This video is
# not available" for many otherwise-playable videos. nodejs from the
# debian repo is sufficient — yt-dlp picks it up via the `js_runtimes`
# opt set in app/core/downloader.py.
# v1.22.4: gosu for the entrypoint's privilege drop (runtime PUID/PGID).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        tini \
        curl \
        nodejs \
        gosu \
    && rm -rf /var/lib/apt/lists/*

# v1.24.42 (security audit 2026-06-25): fail the build on a pre-trixie ffmpeg.
# CVE-2026-8461 "PixelSmash" (MagicYUV decoder heap OOB-write → RCE) is fixed in
# Debian trixie's ffmpeg 7.1.x (DSA-6361-1) but UNPATCHED in bookworm's 5.1.x.
# The base `python:3.12-slim` tracks trixie (ffmpeg 7.x); this guard catches an
# accidental `-bookworm` pin before it ships. yt-dlp decodes downloaded media +
# notify.py rescales CDN thumbnails through ffmpeg, so a vulnerable build is
# reachable. (apt already pulls the latest patched point release within trixie.)
RUN ffmpeg -version | head -1 | grep -qE 'version (7|8|9|[1-9][0-9])\.' \
    || (echo "FATAL: ffmpeg < 7.x — Debian bookworm 5.x is UNPATCHED for CVE-2026-8461 (PixelSmash). Use a trixie+ base." && exit 1)

# v0.51.164: mp3gain for loudness normalization (Phase 1). Lossless gain-only MP3
# adjustment — it edits each frame's global_gain field (no re-encode) and is reversible
# via its MP3GAIN_UNDO / APEv2 tag (`mp3gain -u`). TOLERANT install (|| echo) so a repo
# or package-name hiccup can't brick the image build — the `// PROBE MP3GAIN` diagnostic
# reports whether the binary is actually present + whether apply→undo is bit-exact before
# any real file is touched.
RUN apt-get update \
    && (apt-get install -y --no-install-recommends mp3gain \
        || echo "WARN: mp3gain not installed — loudness normalize will be unavailable") \
    && rm -rf /var/lib/apt/lists/*

# Non-root user, default UID/GID matches Unraid's "nobody" so hardlinks across
# mounts don't end up with root-owned files.
ARG PUID=99
ARG PGID=100
RUN if ! getent group ${PGID} >/dev/null; then \
        groupadd -g ${PGID} motif; \
    fi && \
    useradd -u ${PUID} -g ${PGID} -m -s /usr/sbin/nologin motif

# Copy venv from builder
COPY --from=builder /opt/venv /opt/venv

# Copy application
WORKDIR /app
COPY app /app/app

# v1.22.4: entrypoint that adopts PUID/PGID at runtime + drops privileges.
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Create the dirs the app expects so volumes mount cleanly even on first run
RUN mkdir -p /config /data && \
    chown -R ${PUID}:${PGID} /config /data /app

# v1.22.4: NO static `USER motif`. The entrypoint starts as root, aligns the
# motif account to PUID/PGID (env, defaults 99:100 — the Unraid 'nobody'), and
# `gosu`-drops to it. A legacy `--user X:Y` override still works: the entrypoint
# detects the non-root start and execs the app directly (PUID/PGID ignored).
# Pre-fix the baked USER=99 with no PUID handling meant the only way to match a
# non-99 host (the user's *arr stack runs as 1000) was --user, and a template
# reset to --user 99:100 silently broke every write to the 1000-owned share.

EXPOSE 5309

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl --fail --silent --show-error http://127.0.0.1:${MOTIF_WEB_PORT:-5309}/healthz || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
CMD ["python", "-m", "app.main"]

LABEL org.opencontainers.image.title="motif" \
      org.opencontainers.image.description="Automated theme orchestration for Plex via ThemerrDB" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/healzangels/motif"
