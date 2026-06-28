#!/bin/sh
# motif entrypoint — adopt PUID/PGID at runtime, then drop privileges.
#
# v1.22.4: motif previously baked USER=99:100 with NO runtime user handling, so
# the only way to match a non-99 environment was a --user override — and a
# transient uid drift (--user reverting on a template change) silently broke
# every write to the permission-enforcing Unraid shfs share. This brings motif
# in line with the linuxserver/Unraid convention: PUID/PGID env (defaults
# 99:100 = nobody:users).
# v1.22.5: + UMASK (default 022). Unraid best practice is 002 — group-writable
# (dirs 775 / files 664) so a 99:100 stack (Sonarr/Radarr/Plex) can
# cooperatively write a shared media tree WITHOUT resorting to 777.
set -e

PUID="${PUID:-99}"
PGID="${PGID:-100}"
UMASK="${UMASK:-022}"

# v1.22.6: validate PUID/PGID are NUMERIC. A template slip that passes a
# non-numeric value (e.g. the literal "PGID" landing in the value field) made
# gosu fail with a cryptic "unable to find group PGID" and crash-LOOP the whole
# container. Fall back to the default + a loud error so motif still boots and
# the operator can see + fix the bad env var, rather than a dead container.
case "${PUID}" in
    ""|*[!0-9]*)
        echo "motif: ERROR PUID='${PUID}' is not a number — falling back to 99. Fix the PUID env var (it must be a numeric uid)."
        PUID=99 ;;
esac
case "${PGID}" in
    ""|*[!0-9]*)
        echo "motif: ERROR PGID='${PGID}' is not a number — falling back to 100. Fix the PGID env var (it must be a numeric gid)."
        PGID=100 ;;
esac

# Apply UMASK to THIS shell so it's inherited across the exec/gosu into the app
# (umask is a process attribute, not reset by exec). Affects new dirs/files
# motif creates under /data + /config. Independent of the user, so set it before
# the explicit-user branch too.
if ! umask "${UMASK}" 2>/dev/null; then
    echo "motif: WARNING invalid UMASK '${UMASK}' — keeping default 022."
    umask 022
    UMASK=022
fi

# If an explicit non-root user was forced (legacy --user / compose `user:`),
# honor it and skip PUID/PGID — we can't usermod or drop privileges anyway.
if [ "$(id -u)" != "0" ]; then
    echo "motif: started as an explicit non-root user (uid=$(id -u) gid=$(id -g)) — PUID/PGID ignored; umask=${UMASK} applied."
    exec "$@"
fi

# Align the bundled 'motif' account to PUID/PGID. Best-effort (the -o flags
# allow non-unique ids); gosu drops to the NUMERIC ids below regardless, so a
# usermod hiccup is non-fatal — it only affects the cosmetic username.
groupmod -o -g "${PGID}" motif 2>/dev/null || true
usermod  -o -u "${PUID}" -g "${PGID}" motif 2>/dev/null || true

# /config is motif's OWN appdata (SQLite DB, motif.yaml, cookies, session key)
# — ensure the runtime user can write it. NEVER touch /data: it's the shared
# media tree owned by the *arr/Plex stack; its ownership is the operator's
# responsibility (and a recursive chown there could be enormous).
if ! chown -R "${PUID}:${PGID}" /config /home/motif 2>/dev/null; then
    echo "motif: WARNING could not chown /config to ${PUID}:${PGID} — the SQLite DB may not be writable; check the appdata mount's ownership."
fi

export HOME=/home/motif
echo "motif: running as uid=${PUID} gid=${PGID} umask=${UMASK} (PUID/PGID/UMASK). /data ownership is yours to manage — it must be writable by ${PUID}:${PGID}."
exec gosu "${PUID}:${PGID}" "$@"
