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
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        tini \
        curl \
        nodejs \
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

# Create the dirs the app expects so volumes mount cleanly even on first run
RUN mkdir -p /config /data && \
    chown -R ${PUID}:${PGID} /config /data /app

USER motif

EXPOSE 5309

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl --fail --silent --show-error http://127.0.0.1:5309/healthz || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "app.main"]

LABEL org.opencontainers.image.title="motif" \
      org.opencontainers.image.description="Automated theme orchestration for Plex via ThemerrDB" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/healzangels/motif"
