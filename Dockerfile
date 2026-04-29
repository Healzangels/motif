# syntax=docker/dockerfile:1.7
# motif · automated theme orchestration for Plex
#
# Build stage installs Python deps into a venv that we copy into the runtime
# image. Runtime is python-slim with ffmpeg added — yt-dlp shells out to it
# for MP3 conversion.
#
# Path model (v1.4.0+):
#   /config — appdata: SQLite DB, motif.yaml, cookies.txt
#   /data   — unified data root (mirrors what Plex sees)
# Themes path is configured at runtime from the web UI; no per-library
# mount points are baked into the image.

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
# curl for the healthcheck.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        tini \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user, default UID/GID matches Unraid's "nobody"/"users" so hardlinks
# across mounts don't end up with root-owned files. Note: GID 100 is already
# claimed by the 'users' group in the python:3.12-slim base image — we detect
# that and reuse the existing group rather than failing.
ARG PUID=99
ARG PGID=100
RUN if ! getent group ${PGID} >/dev/null; then \
        groupadd -g ${PGID} motif; \
    fi && \
    if ! getent passwd ${PUID} >/dev/null; then \
        useradd -u ${PUID} -g ${PGID} -m -s /usr/sbin/nologin motif; \
    fi

# Copy venv from builder. Owned by root, readable by all — no chown needed.
COPY --from=builder /opt/venv /opt/venv

# Copy application
WORKDIR /app
COPY app /app/app

# Create the two mount points and chown /config so the non-root user can
# write SQLite, cookies, and motif.yaml on first boot. /data is created
# but NOT chowned — at runtime the host directory is bind-mounted in with
# its own ownership.
#
# We chown by numeric UID:GID rather than 'motif:motif' because when
# GID 100 already exists as 'users' in the base image (Debian behavior),
# we skip groupadd and the literal 'motif' group never gets created.
RUN mkdir -p /config /data && \
    chown -R ${PUID}:${PGID} /config /app && \
    chmod 0755 /config /data

USER ${PUID}:${PGID}

EXPOSE 5309

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl --fail --silent --show-error http://127.0.0.1:5309/healthz || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "app.main"]

LABEL org.opencontainers.image.title="motif" \
      org.opencontainers.image.description="Automated theme orchestration for Plex via ThemerrDB" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/healzangels/motif"
