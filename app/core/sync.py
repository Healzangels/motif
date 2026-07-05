"""
ThemerrDB sync engine.

ThemerrDB publishes its database to the gh-pages branch daily at UTC 12:00.
The published structure is:
  /movies/pages.json                  -> {"count": N, "pages": M, "imdb_count": K}
  /movies/all_page_<n>.json           -> [{"id": tmdb_id, "imdb_id": "tt...", "title": "..."}, ...]
  /movies/imdb/<imdb_id>.json         -> full TMDB-shaped record + youtube_theme_url
  /movies/themoviedb/<tmdb_id>.json   -> same shape, keyed by tmdb_id (some lack imdb_id)
  /tv_shows/...                       -> identical layout
  /movie_collections/pages.json       -> identical pages/all_page shape (v1.18.0)
  /movie_collections/themoviedb/<tmdb_collection_id>.json
                                      -> top-level TMDB collection JSON with
                                         `name`, `youtube_theme_url`, etc.
                                         (no imdb/ subtree — collections aren't
                                         in IMDB; only themoviedb/-keyed)

Sync strategy:
1. Fetch all pages and build a set of {(media_type, tmdb_id)}.
2. For each tmdb_id, fetch the per-item JSON (preferring imdb/ over themoviedb/).
3. Compare youtube_theme_added/youtube_theme_edited against what's in our DB.
   - New items: insert + enqueue download.
   - Changed YouTube URL: update + enqueue re-download.
   - Otherwise: just update last_seen_sync_at.
4. Items in our DB that didn't appear in this sync are NOT deleted — upstream
   may have transient drops, and we want our local copy to survive.
"""
from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import httpx

from .db import get_conn, transaction
from .events import _redact_url_credentials, log_event, now_iso
from . import progress as op_progress
from .normalize import titles_equal

log = logging.getLogger(__name__)

# v1.17.11: once-per-process flags for class-9 swallows in hot
# paths. Tight-loop log.warning would drown the log; module-level
# flag means the operator sees the issue at first occurrence and
# subsequent attempts stay at log.debug.
_SYNC_NORMALIZE_TITLE_WARNED: bool = False
_GIT_MIRROR_READ_JSON_WARNED: bool = False
_SAFE_JSON_WARNED: bool = False


# ----- HTTP client config -----

_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0)
# v1.11.49: parallel per-item fetches use up to _FETCH_WORKERS concurrent
# requests; bump max_connections so the pool isn't the bottleneck. GitHub
# Pages happily handles ~32 concurrent connections from one client; we
# stay well under any sane CDN throttle.
_HTTP_LIMITS = httpx.Limits(max_connections=40, max_keepalive_connections=20)
_FETCH_WORKERS = 16


def _make_client() -> httpx.Client:
    return httpx.Client(
        timeout=_HTTP_TIMEOUT,
        limits=_HTTP_LIMITS,
        follow_redirects=True,
        headers={"User-Agent": "motif-sync/1.0 (homelab)"},
    )


# ----- Helpers -----

# v1.20.43: added /shorts/ — a YouTube Shorts URL is a perfectly valid
# theme source, but pre-fix the regex matched only v=/youtu.be//embed/,
# so url_source returned 'unknown' AND extract_video_id returned None →
# the download failed with "could not extract video id". Mirror in
# app.js YOUTUBE_URL_RE.
_YT_VID_RE = re.compile(r"(?:v=|youtu\.be/|/embed/|/shorts/)([A-Za-z0-9_-]{11})")
# v1.14.0: SoundCloud URLs as theme sources. Format:
# https://soundcloud.com/<artist>/<track-slug>(?...). The "id" we
# stash in source_video_id columns is "sc-<artist>-<slug>" — short
# enough for the column, recognizably non-YouTube via the sc- prefix
# (the v1.14.0 _SRC_LETTER_SQL relaxation keys on this prefix).
_SC_URL_RE = re.compile(
    r"^https?://(?:www\.|m\.)?soundcloud\.com/"
    r"([a-z0-9](?:[a-z0-9_-]*[a-z0-9])?)/"
    r"([a-z0-9](?:[a-z0-9_-]*[a-z0-9])?)",
    re.IGNORECASE,
)
# v1.20.26: Instagram reels / video-posts / IGTV as theme sources.
# Forms: instagram.com/reel(s)/<code>, /p/<code>, /tv/<code>, optionally
# behind a /<username>/ segment. The id stashed in source_video_id is
# "ig-<shortcode>" (mirrors the sc- prefix convention) — recognizably
# non-YouTube. The shortcode is base64-ish and CASE-SENSITIVE, so unlike
# the SoundCloud slug it must NOT be lower-cased. ThemerrDB never
# publishes Instagram URLs, so this is a USER source (SET URL) only.
_IG_URL_RE = re.compile(
    r"^https?://(?:www\.|m\.)?instagram\.com/"
    r"(?:[A-Za-z0-9._]+/)?"
    r"(?:reels?|p|tv)/([A-Za-z0-9_-]+)",
    re.IGNORECASE,
)
# v1.22.90: Facebook videos / reels / watch links as theme sources.
# Forms: facebook.com/<page>/videos/[<slug>/]<id>, /watch?v=<id>,
# /video.php?v=<id>, /reel/<id>, /share/v/<code>, fb.watch/<code>.
# The id stashed in source_video_id is "fb-<id>" (mirrors ig-/sc-).
# Canonical forms carry a numeric id; share/fb.watch codes are
# base64-ish and CASE-SENSITIVE so, like ig-, they are NOT
# lower-cased. Like Instagram, ThemerrDB never publishes Facebook
# URLs, so this is a USER source (SET URL) only.
_FB_URL_RE = re.compile(
    r"^https?://"
    r"(?:"
    r"(?:www\.|m\.|web\.|mbasic\.)?facebook\.com/"
    r"(?:"
    r"(?:[A-Za-z0-9.\-]+/)?videos/(?:[^/?#]+/)?(\d+)"
    r"|reel/(\d+)"
    r"|share/[vr]/([A-Za-z0-9_-]+)"
    r"|(?:watch/?|video\.php)\?(?:[^#]*&)?v=(\d+)"
    r")"
    r"|fb\.watch/([A-Za-z0-9_-]+)"
    r")",
    re.IGNORECASE,
)


def url_source(url: str | None) -> str:
    """v1.14.0: classify a theme URL by its source platform.
    Returns 'youtube' | 'soundcloud' | 'instagram' | 'facebook'
    | 'unknown'. Used by:
      - the info card to label URL rows ("youtube url" /
        "soundcloud url" / "instagram url" / "url" instead of bare "url")
      - SET URL dialog live preview ("detected: Instagram")
      - ACCEPT UPDATE confirm dialog (warns when source differs)
      - extract_video_id below to pick the right id format
    Pure function. Mirror in app.js as `urlSource(url)`.
    """
    if not url:
        return "unknown"
    # v1.22.90: Facebook BEFORE YouTube — _YT_VID_RE matches an
    # unanchored `v=`, so facebook.com/watch?v=<id> would classify
    # as YouTube (with the id truncated to 11 chars). _FB_URL_RE is
    # host-anchored and can't steal a real YouTube URL.
    if _FB_URL_RE.search(url):
        return "facebook"
    if _YT_VID_RE.search(url):
        return "youtube"
    if _SC_URL_RE.search(url):
        return "soundcloud"
    # v1.20.26: check Instagram before unknown.
    if _IG_URL_RE.search(url):
        return "instagram"
    return "unknown"


def extract_video_id(url: str | None) -> str | None:
    """Return a stable id for the URL, or None if unrecognized.

    YouTube: the 11-char video id (legacy behavior).
    SoundCloud: "sc-<artist>-<slug>" — distinguishable from a
    YouTube id by length (>11) AND the "sc-" prefix. Stored in
    themes.youtube_video_id / local_files.source_video_id (column
    names kept for v1.14.0 — see CLAUDE.md historical-name pattern).
    """
    if not url:
        return None
    # v1.22.90: Facebook video id → "fb-<id>", checked BEFORE YouTube
    # for the same reason as url_source (the unanchored `v=` in
    # _YT_VID_RE would truncate facebook.com/watch?v=<id> to 11
    # chars). Whichever alternation branch matched carries the id;
    # share/fb.watch codes are case-sensitive (preserved, like ig-),
    # numeric ids unaffected.
    m_fb = _FB_URL_RE.search(url)
    if m_fb:
        fb_id = next(g for g in m_fb.groups() if g)
        return f"fb-{fb_id[:60]}"
    m_yt = _YT_VID_RE.search(url)
    if m_yt:
        return m_yt.group(1)
    m_sc = _SC_URL_RE.search(url)
    if m_sc:
        # Truncate components defensively — long slugs exist.
        artist = m_sc.group(1).lower()[:60]
        slug = m_sc.group(2).lower()[:60]
        return f"sc-{artist}-{slug}"
    # v1.20.26: Instagram shortcode → "ig-<code>". CASE-SENSITIVE — the
    # shortcode is a base64-ish id, so it is NOT lower-cased.
    m_ig = _IG_URL_RE.search(url)
    if m_ig:
        return f"ig-{m_ig.group(1)[:60]}"
    return None


@dataclass
class SyncStats:
    movies_seen: int = 0
    tv_seen: int = 0
    # v1.18.0: ThemerrDB's movie_collections/ tree. Counted
    # separately so the dashboard sparkline can show collection
    # sync activity without skewing the movies count.
    collections_seen: int = 0
    new_count: int = 0
    updated_count: int = 0
    errors: int = 0
    # v1.18.64: capture rows whose upstream URL changed this sync run. The
    # sync_completed Apprise body samples this list so the operator can see
    # WHICH titles got updated, not just a count. Cap at 50 entries — bounded
    # memory for big initial syncs, body renders only the first 10 with "…and N
    # more" overflow (mirrors themes_added_by_sync). the user's repro on v1.18.63:
    # "1 items processed, 0 new, 1 updated" gave no hint which title.
    # v1.22.45: entries are (media_type, tmdb_id, title, year) — the
    # media_type+tmdb_id let the worker group the Updated list by Plex section
    # (Movies / 4K / TV Shows / Anime …) in the notification body.
    updated_titles: list = field(default_factory=list)


# ----- Sync logic -----


def _fetch_index(
    client: httpx.Client, base_url: str, media_path: str
) -> tuple[list[dict], int]:
    """Fetch every page → (merged list of {id, imdb_id, title}, failed_pages).

    v1.21.38: failed_pages counts pages that 5xx'd / errored. The caller
    folds it into stats.errors so a partial index BLOCKS the full-walk
    drop sweep — a swallowed page leaves its items un-fetched, their
    last_seen_sync_at stale, and the sweep would mis-stamp them as
    TDB-dropped (the v1.18.10 amplifier-sweep trap)."""
    pages_url = f"{base_url}/{media_path}/pages.json"
    log.info("Fetching index: %s", pages_url)
    r = client.get(pages_url)
    r.raise_for_status()
    meta = r.json()
    # v1.24.14 (holistic review): a 200 with a non-dict / empty body (stale CDN
    # cache, an error/HTML page that still JSON-decodes) used to silently return
    # ([], 0) — a green zero-theme sync of a whole media type, indistinguishable
    # from a real empty catalog, with no breadcrumb. The snapshot tier got this
    # guard in v1.23.67; the remote-tier catalog read was missed. Raise on a
    # clearly-bad body (run_sync marks the run failed loudly); breadcrumb a
    # zero-page catalog (could be legit-empty, but surface it).
    if not isinstance(meta, dict):
        raise ValueError(
            f"remote index {pages_url}: expected a JSON object, got "
            f"{type(meta).__name__} — treating as a failed fetch")
    total_pages = int(meta.get("pages", 0))
    if total_pages == 0:
        log.warning(
            "remote index %s: pages=0 — syncing ZERO items for this media "
            "type. Suspicious (a stale/error body decodes the same way); "
            "verify the catalog if this wasn't expected.", pages_url)
        return [], 0

    items: list[dict] = []
    failed_pages = 0
    for n in range(1, total_pages + 1):
        page_url = f"{base_url}/{media_path}/all_page_{n}.json"
        try:
            pr = client.get(page_url)
            pr.raise_for_status()
            body = pr.json()
            # v1.22.29 (audit): a page that 200s with a non-list or EMPTY body
            # (CDN/edge stale-empty, a truncated/corrupt all_page_N.json) is a
            # SILENTLY-partial index — ThemerrDB's pages are non-empty lists by
            # construction. Pre-fix only httpx errors counted as a failed page,
            # so a 200-with-bad-body left index_incomplete=False and the
            # full-walk drop sweep ran against a partial catalog → mis-stamped
            # live themes as TDB-dropped (the v1.21.38 mis-stamp class, through
            # the door the v1.21.44 HTTP-error gate doesn't watch). Counting it
            # as failed errs SAFE (suppresses that run's drop sweep, next clean
            # run catches up). `extend(dict)` would also corrupt items with keys.
            if not isinstance(body, list) or not body:
                raise ValueError(
                    f"page body empty or not a list ({type(body).__name__})")
            items.extend(body)
        except (httpx.HTTPError, ValueError) as e:
            failed_pages += 1
            log.warning("Failed to fetch %s: %s", page_url, e)
        # Tiny politeness delay every 25 pages
        if n % 25 == 0:
            time.sleep(0.5)
    return items, failed_pages


def _fetch_item(
    client: httpx.Client,
    base_url: str,
    media_path: str,
    *,
    imdb_id: str | None,
    tmdb_id: int,
) -> dict | None:
    """Fetch the per-item record. Prefer the imdb/ folder when imdb_id is known."""
    candidates = []
    if imdb_id:
        candidates.append(f"{base_url}/{media_path}/imdb/{imdb_id}.json")
    candidates.append(f"{base_url}/{media_path}/themoviedb/{tmdb_id}.json")

    last_err: Exception | None = None
    for url in candidates:
        try:
            r = client.get(url)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            last_err = e
            continue
    if last_err:
        log.debug("Item fetch failed (tmdb=%s): %s", tmdb_id, last_err)
    return None


# v1.15.81: motif-relevant fields used to fingerprint a TDB record.
# Anything NOT in this list is allowed to vary between syncs without
# triggering a full upsert. Keep this list in lockstep with the
# columns _upsert_theme writes (youtube_url, youtube_video_id,
# youtube_added_at, youtube_edited_at, imdb_id, title, original_title,
# year, release_date). raw_json is intentionally excluded — nothing
# in motif reads it after write, so a metadata-only JSON change
# (which is the common LizardByte "noisy commit" case) should NOT
# count as content drift requiring a write.
_FINGERPRINT_FIELDS = (
    "imdb_id",
    "title",
    "name",
    "original_title",
    "original_name",
    "release_date",
    "first_air_date",
    "youtube_theme_url",
    "youtube_theme_added",
    "youtube_theme_edited",
)


def _record_fingerprint(record: dict) -> str:
    """SHA1 of the motif-relevant fields from a TDB JSON record.

    Stable JSON serialization (sorted keys, compact separators)
    ensures records with the same fields-but-different-formatting
    produce the same hash. Used by the sync apply loop to skip a
    full upsert when nothing motif reads has actually changed —
    the common case when LizardByte rebases / reformats / touches
    metadata fields motif doesn't consume.

    v1.15.81: introduced to fix the "9733 paths apply over 2+
    minutes, result new=0/updated=0" pathology the user reported.
    Adding a fingerprint column to themes + this comparison turns
    the noisy-commit / same-data case from 5000 UPDATEs into 5000
    cheap last_seen_sync_at bumps."""
    relevant = {k: record.get(k) for k in _FINGERPRINT_FIELDS}
    payload = json.dumps(relevant, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _upsert_theme(
    conn: sqlite3.Connection,
    *,
    media_type: str,
    tmdb_id: int,
    record: dict,
    upstream_source: str,
    sync_ts: str,
) -> tuple[bool, bool, str | None]:
    """Insert or update one theme row.
    Returns (is_new, url_changed, old_video_id_if_changed)."""
    imdb_id = record.get("imdb_id") or None
    # TMDB uses different field names for movies vs TV: title/release_date for
    # movies, name/first_air_date for TV. ThemerrDB stores records as-is, so
    # accept either shape.
    title = record.get("title") or record.get("name") or ""
    original_title = record.get("original_title") or record.get("original_name") or None
    rd = record.get("release_date") or record.get("first_air_date") or ""
    year = rd[:4] if rd else None
    yt_url = record.get("youtube_theme_url") or None
    yt_vid = extract_video_id(yt_url)
    yt_added = record.get("youtube_theme_added") or None
    yt_edited = record.get("youtube_theme_edited") or None

    existing = conn.execute(
        "SELECT youtube_video_id, youtube_url, youtube_edited_at, "
        "       tdb_content_fingerprint "
        "FROM themes "
        "WHERE media_type = ? AND tmdb_id = ?",
        (media_type, tmdb_id),
    ).fetchone()

    is_new = existing is None
    url_changed = False
    old_vid: str | None = None
    old_url: str | None = None

    # v1.15.81: fast-path skip when motif-relevant fields haven't
    # changed since the last sync. Avoids ~90% of the apply-loop's
    # DB work in the "LizardByte noisy commit, same data" case
    # the user reported (9733 paths apply → new=0/updated=0 → still
    # took 2+ minutes of UPDATE writes). We still bump
    # last_seen_sync_at so drop detection (which looks at rows
    # whose last_seen_sync_at < this run's sync_ts) doesn't false-
    # positive these as "removed upstream." Returns (is_new=False,
    # url_changed=False, old_vid=None, old_url=None) → batch loop
    # skips the cross-match prompt + download-enqueue branches,
    # which is correct: nothing changed for them to react to.
    # v1.19.60: old_url added to the return tuple alongside old_vid
    # so the url_changed branch can capture the previous URL into
    # pending_updates (pre-fix `old_youtube_url` was bound to None
    # on the INSERT, leaving every row's diff context blank).
    if existing is not None:
        new_fp = _record_fingerprint(record)
        stored_fp = existing["tdb_content_fingerprint"]
        if stored_fp == new_fp:
            # v1.20.13: also clear tdb_dropped_at. Pre-fix the fast-path
            # only bumped last_seen_sync_at, so a title that was dropped
            # upstream and later RE-PUBLISHED byte-identical (transient
            # delete/restore, or a re-key landing the same content) hit
            # this path — fingerprint matched → the drop stamp was never
            # cleared and the row stayed SRC=– / gray TDB◌ forever. The
            # slow-path UPDATE branches clear it; this one was the gap.
            # The row IS in this sync's walk, so it's definitionally not
            # dropped — clearing is always correct (audit round 3 H1).
            conn.execute(
                "UPDATE themes SET last_seen_sync_at = ?, "
                "       tdb_dropped_at = NULL "
                "WHERE media_type = ? AND tmdb_id = ?",
                (sync_ts, media_type, tmdb_id),
            )
            return (False, False, None, None)

    # Orphan promotion: before inserting a new theme, check if there's a
    # plex_orphan row with negative tmdb_id matching this real record by
    # imdb_id, or by (title, year). If so, promote it: update the orphan's
    # tmdb_id to the real value, propagate the change to FK'd tables, and
    # treat this as an UPDATE rather than an INSERT.
    if is_new:
        orphan = None
        if imdb_id:
            orphan = conn.execute(
                """SELECT id, tmdb_id FROM themes
                   WHERE upstream_source = 'plex_orphan'
                     AND media_type = ? AND imdb_id = ?
                   LIMIT 1""",
                (media_type, imdb_id),
            ).fetchone()
        if orphan is None and title and year:
            # Pull every plex_orphan candidate for this media_type+year (cheap
            # filter) and select the one whose normalized title matches. This
            # handles things like "(500) Days of Summer" vs "500 Days of
            # Summer" or roman-numeral / word-number variations that exact
            # case-insensitive comparison would miss.
            # v1.11.93: ORDER BY id ASC so the candidate set is
            # iterated in a stable insert-order. Without it SQLite is
            # free to return rows in any order, which means two sync
            # runs against an unchanged DB could pick *different*
            # orphans to promote when more than one matches (rare but
            # possible: same title+year duplicated across sections).
            # Picking deterministically also makes log replay /
            # debug reproducible.
            orphan_candidates = conn.execute(
                """SELECT id, tmdb_id, title FROM themes
                   WHERE upstream_source = 'plex_orphan'
                     AND media_type = ? AND year = ?
                   ORDER BY id ASC""",
                (media_type, year),
            ).fetchall()
            for cand in orphan_candidates:
                if titles_equal(cand["title"] or "", title):
                    orphan = cand
                    break
        # v1.18.2: collection-specific orphan promotion — title-only
        # match (no year). Collections have no release year (the
        # themerr-plex audited source + Plex API confirm: collection
        # metadata has no `year` attribute), so the year-required
        # branch above never fires for them. Pre-v1.18.2 a user
        # who SET URL on a collection got an orphan row at
        # tmdb_id=-N; the next TDB sync added the real collection
        # at tmdb_id=<positive>; orphan promotion didn't fire (no
        # year to match on); UI showed two rows for the same
        # collection (orphan + real) and the user's URL stayed on
        # the orphan, disconnected from the TDB record.
        # the user's Willy Wonka Collection test reproduced exactly
        # this shape — SET URL → orphan at tmdb=-27 → TDB sync
        # added Willy Wonka Collection at tmdb=<positive> → no
        # promotion. Same title+media_type match is robust here
        # because TDB collections are uniquely-titled (no
        # "Willy Wonka 1971 vs 2023" remake disambiguation
        # problem applies — collections aren't dated remakes).
        if (orphan is None and media_type == "collection"
                and title):
            orphan_candidates = conn.execute(
                """SELECT id, tmdb_id, title FROM themes
                   WHERE upstream_source = 'plex_orphan'
                     AND media_type = 'collection'
                   ORDER BY id ASC""",
            ).fetchall()
            for cand in orphan_candidates:
                if titles_equal(cand["title"] or "", title):
                    orphan = cand
                    break
        if orphan:
            old_tmdb = orphan["tmdb_id"]
            log.info("Promoting orphan theme id=%d (%s/%d) → real tmdb_id=%d",
                     orphan["id"], media_type, old_tmdb, tmdb_id)
            # Migrate the orphan row's tmdb_id and FK'd tables in one txn.
            # The row keeps its `id`; only tmdb_id changes.
            #
            # v1.11.71: use PRAGMA defer_foreign_keys = ON instead of
            # foreign_keys = OFF. The latter is documented as a no-op
            # when issued inside an open transaction (we're already
            # inside _flush_sync_batch's BEGIN IMMEDIATE), so FK
            # enforcement stayed live and the first UPDATE themes
            # crashed with 'FOREIGN KEY constraint failed' — the
            # existing local_files / placements / user_overrides rows
            # still pointed at (media_type, old_tmdb) but the parent
            # had just moved to (media_type, tmdb_id). defer_foreign_keys
            # IS allowed inside a txn and defers all checks to COMMIT,
            # at which point we've updated the children too and the
            # final state is consistent.
            conn.execute("PRAGMA defer_foreign_keys = ON")
            # v1.11.73: clear any leftover rows at the TARGET (media_type,
            # tmdb_id) before moving the orphan's rows there. Without this
            # the UPDATE collides with phantom rows from prior failed sync
            # attempts / DB-state edge cases (e.g. a user_overrides row
            # stuck at (movie, 252) from an earlier code path) and the
            # whole sync run dies on UNIQUE constraint failed. UNIQUE
            # is NOT covered by defer_foreign_keys; v1.11.71's deferred
            # FK fix only covered foreign-key violations.
            #
            # The orphan represents the user's most recent action
            # (SET URL / UPLOAD / adopt happened recently); any
            # pre-existing rows at the target tmdb are stale leftovers.
            # Drop them so the UPDATEs always succeed.
            conn.execute(
                "DELETE FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            conn.execute(
                "DELETE FROM placements WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            conn.execute(
                "DELETE FROM pending_updates WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            conn.execute(
                "DELETE FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            # v1.22.51: previous_urls is ALSO FK'd to themes(media_type,tmdb_id)
            # — promoting an orphan that carries a REVERT/url-change history row
            # left it pointing at the old synthetic id → FOREIGN KEY constraint
            # failed at COMMIT. the user's de-orphan run surfaced it on 'Hokum'.
            conn.execute(
                "DELETE FROM previous_urls WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            )
            conn.execute(
                "UPDATE themes SET tmdb_id = ?, upstream_source = ? "
                "WHERE id = ?",
                (tmdb_id, upstream_source, orphan["id"]),
            )
            conn.execute(
                "UPDATE local_files SET tmdb_id = ? WHERE media_type = ? AND tmdb_id = ?",
                (tmdb_id, media_type, old_tmdb),
            )
            conn.execute(
                "UPDATE placements SET tmdb_id = ? WHERE media_type = ? AND tmdb_id = ?",
                (tmdb_id, media_type, old_tmdb),
            )
            conn.execute(
                "UPDATE pending_updates SET tmdb_id = ? WHERE media_type = ? AND tmdb_id = ?",
                (tmdb_id, media_type, old_tmdb),
            )
            conn.execute(
                "UPDATE user_overrides SET tmdb_id = ? WHERE media_type = ? AND tmdb_id = ?",
                (tmdb_id, media_type, old_tmdb),
            )
            conn.execute(
                "UPDATE previous_urls SET tmdb_id = ? WHERE media_type = ? AND tmdb_id = ?",
                (tmdb_id, media_type, old_tmdb),
            )
            # v1.22.87: re-key in-flight jobs too — a pending download/
            # place enqueued at the synthetic id (SET URL on an orphan)
            # died confusingly when the sync promoted the id mid-window:
            # the worker resolved the override/themes URL at the OLD id
            # (already re-keyed away → empty) and the user's requested
            # download failed as a mystery job error.
            conn.execute(
                "UPDATE jobs SET tmdb_id = ? WHERE media_type = ? "
                "AND tmdb_id = ? AND status IN ('pending', 'running')",
                (tmdb_id, media_type, old_tmdb),
            )
            existing = conn.execute(
                "SELECT youtube_video_id, youtube_url, youtube_edited_at "
                "FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            is_new = False

    # v1.10.32: precompute the normalized title so the library JOIN's
    # title-fallback (when Plex GUIDs disagree with ThemerrDB) can
    # match without re-running normalize_title() at query time.
    # v1.17.11: class-9 fix — pre-fix `except Exception: ...lower()`
    # silently swallowed everything from ImportError (broken venv)
    # to normalize_title raising on a single bad title. Log once
    # per process at warn so the operator sees the issue, debug
    # subsequently. Hot path → can't log per-row.
    try:
        from .normalize import normalize_title
        title_norm = normalize_title(title or "")
    except Exception as e:  # noqa: BLE001
        global _SYNC_NORMALIZE_TITLE_WARNED
        if not _SYNC_NORMALIZE_TITLE_WARNED:
            log.warning(
                "sync upsert: normalize_title failed (%s: %s) — "
                "falling back to lowercase. Will not log subsequent "
                "occurrences in this process; check the deploy.",
                type(e).__name__, e,
            )
            _SYNC_NORMALIZE_TITLE_WARNED = True
        else:
            log.debug("normalize_title failed: %s", e)
        title_norm = (title or "").lower()

    # v1.15.81: fingerprint stored on every slow-path write so the
    # next sync's fast-path can compare against it. Computed once
    # here; threaded into the INSERT + all UPDATE paths below.
    new_fp = _record_fingerprint(record)

    if is_new:
        conn.execute(
            """
            INSERT INTO themes (
                media_type, tmdb_id, imdb_id, title, original_title, year, release_date,
                youtube_url, youtube_video_id, youtube_added_at, youtube_edited_at,
                upstream_source, raw_json, last_seen_sync_at, first_seen_sync_at,
                title_norm, tdb_content_fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                media_type, tmdb_id, imdb_id, title, original_title, year, rd or None,
                yt_url, yt_vid, yt_added, yt_edited,
                upstream_source, _safe_json(record), sync_ts, sync_ts,
                title_norm, new_fp,
            ),
        )
    else:
        old_vid = existing["youtube_video_id"]
        # v1.19.60: capture the previous URL so the url_changed
        # branch's pending_updates INSERT can stamp old_youtube_url
        # for the v1.12.119 gate's diff check + the INFO card's
        # before/after display. Pre-fix the column stayed empty.
        old_url = existing["youtube_url"]
        url_changed = (yt_vid != old_vid) and yt_vid is not None
        # If the user has set a manual override for this theme, do NOT silently
        # overwrite themes.youtube_url with the new upstream value — the
        # override is the user's authoritative choice. Keep the previous
        # upstream URL on the row; the new one will be surfaced via
        # pending_updates so the user can decide whether to revert.
        # v1.14.55 audit clarification: lookup is INTENTIONALLY title-
        # global (no section_id filter). Rationale: themes.youtube_url
        # is the title-global fallback; per-section overrides win over
        # it via the library SQL's COALESCE chain. ANY section's
        # override means "the user has expressed an opinion about this
        # title" → pause auto-roll + emit a pending_update the user
        # accepts or declines per-section. Per-section scoping here
        # would silently auto-roll sections WITHOUT overrides past a
        # section-A user's intent, which the v1.13.81 cross-section
        # bleed work was supposed to prevent. Same intent at sync.py:649
        # (cross-match) + sync.py:705 (url_changed pending_update).
        # edition-blind OK (v1.21.94): title-global by design (v1.13.81) — ANY
        # edition's override pauses the title's auto-roll; per-edition scoping
        # would auto-roll edition-less rows past a sibling edition's intent.
        has_override = conn.execute(
            "SELECT 1 FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
            (media_type, tmdb_id),
        ).fetchone() is not None
        if has_override and url_changed:
            conn.execute(
                """
                UPDATE themes SET
                    imdb_id = ?, title = ?, original_title = ?, year = ?, release_date = ?,
                    upstream_source = ?, raw_json = ?, last_seen_sync_at = ?,
                    title_norm = ?, tdb_content_fingerprint = ?,
                    -- v1.13.1 (Phase C): if the item was previously
                    -- dropped from TDB and has now reappeared, clear
                    -- the dropped flag automatically — re-adding is
                    -- the canonical "undo drop" signal.
                    tdb_dropped_at = NULL
                WHERE media_type = ? AND tmdb_id = ?
                """,
                (
                    imdb_id, title, original_title, year, rd or None,
                    upstream_source, _safe_json(record), sync_ts,
                    title_norm, new_fp,
                    media_type, tmdb_id,
                ),
            )
            return is_new, url_changed, old_vid, old_url
        conn.execute(
            """
            UPDATE themes SET
                imdb_id = ?, title = ?, original_title = ?, year = ?, release_date = ?,
                youtube_url = ?, youtube_video_id = ?, youtube_added_at = ?,
                youtube_edited_at = ?, upstream_source = ?, raw_json = ?,
                last_seen_sync_at = ?, title_norm = ?,
                tdb_content_fingerprint = ?,
                tdb_dropped_at = NULL
            WHERE media_type = ? AND tmdb_id = ?
            """,
            (
                imdb_id, title, original_title, year, rd or None,
                yt_url, yt_vid, yt_added, yt_edited,
                upstream_source, _safe_json(record), sync_ts, title_norm,
                new_fp,
                media_type, tmdb_id,
            ),
        )

    return is_new, url_changed, old_vid, old_url


def _safe_json(record: dict) -> str:
    import json
    try:
        return json.dumps(record, ensure_ascii=False)
    except (TypeError, ValueError) as e:
        # v1.19.79 (Opus-4.8 audit MED): class-9 hot-path swallow.
        # Fires once per raw_json write — drop a breadcrumb so a
        # systemically un-serializable TDB record doesn't silently
        # store '{}' forever. First occurrence at warning, rest debug.
        global _SAFE_JSON_WARNED
        if not _SAFE_JSON_WARNED:
            log.warning(
                "_safe_json: record not JSON-serializable (%r) — "
                "storing '{}'. Suppressing further warnings this "
                "process.", e,
            )
            _SAFE_JSON_WARNED = True
        else:
            log.debug("_safe_json: non-serializable record (%r)", e)
        return "{}"


# v1.23.23: sync-discovered auto-downloads are "held" — parked at a
# far-future next_run_at the worker's claim query (status='pending' AND
# next_run_at <= now) skips — until the sync dispatches its summary, so the
# per-theme `theme_added` cards land AFTER the "Motif sync — N new" message
# instead of racing ahead of it (the user's 2026-06-13 report: the Bad Guys 2 /
# Chainsaw Man / Cosmic Princess cards arrived before the sync summary that
# explained them). _do_sync releases the holds right after the summary
# dispatch returns; the download pipeline's own latency (claim tick →
# rate-limited fetch → place) then keeps theme_added a comfortable step
# behind. The sentinel is a unique timestamp no real job carries, so the
# release UPDATE matches exactly these parked rows — and a hold orphaned by a
# worker that died mid-sync is swept by the next sync's release.
_SYNC_HOLD_NEXT_RUN_AT = "9999-12-31T23:59:59+00:00"


def release_held_downloads(conn) -> int:
    """v1.23.31: make any sync-held download jobs (parked at the sentinel)
    runnable now, on a CALLER-supplied connection. The single statement the
    end-of-sync release AND the boot reclaim share, so the sentinel-release
    contract can't drift across them. Returns the count released."""
    cur = conn.execute(
        "UPDATE jobs SET next_run_at = ? "
        "WHERE job_type = 'download' AND status = 'pending' "
        "  AND next_run_at = ?",
        (now_iso(), _SYNC_HOLD_NEXT_RUN_AT),
    )
    return cur.rowcount


def release_sync_held_downloads(db_path) -> int:
    """Make any sync-held download jobs (parked at _SYNC_HOLD_NEXT_RUN_AT)
    runnable now. Idempotent — a no-op when nothing is held. Returns the
    count released."""
    with get_conn(db_path) as conn:
        n = release_held_downloads(conn)
        conn.commit()
        return n


def _enqueue_download(
    conn: sqlite3.Connection,
    *,
    media_type: str,
    tmdb_id: int,
    reason: str,
    auto_place: bool | None = None,
    force_place: bool = False,
    only_section_id: str | None = None,
    rollback: dict | None = None,
    edition_key: str = "",
    hold_for_summary: bool = False,
    bulk: bool = False,
) -> int:
    """v1.11.0: enqueue one download job per (media_type, tmdb_id, section)
    where this item lives — sections discovered via plex_items joined to
    plex_sections (only INCLUDED sections). Items present in 0 sections
    don't get any download job (nothing to drop the file into yet); the
    next sync after plex_enum will pick them up.

    Dedupes against any existing pending/running download for the same
    (item, section). Returns the count of jobs actually enqueued.

    `auto_place=None` means "use the global setting"; True/False writes
    an explicit override into the job payload that the worker honors.

    v1.12.43: `force_place=True` adds force_place to the payload so the
    chained place job runs with force=True, bypassing the worker's
    skip_if_plex_has_theme guard. Used by ACCEPT UPDATE so the new TDB
    theme actually overwrites the existing user-source theme.mp3 in the
    Plex folder. Pre-fix the place job ran but skipped with
    "plex_has_theme" because Plex still cached the old theme as
    has_theme=true — the file got downloaded into canonical but never
    replaced the placement file in /movies/, leaving Plex playing the
    old U theme.

    v1.12.114: `rollback` carries optional recovery state the worker
    reads on permanent download failure. Used by ACCEPT UPDATE
    (rollback to user_overrides + decision='pending') and REVERT
    (rollback to prior previous_urls + override). Without rollback,
    a download failure leaves motif in the post-action state with
    the old state already destroyed — user has no easy recovery.
    """
    # v1.18.0: themes ↔ plex_items media_type alignment. For
    # collections, plex_items.media_type is also 'collection' —
    # the post-Phase-2 enumerate_collections_for_section emits
    # PlexLibraryItem(media_type='collection') directly, no
    # aliasing applied.
    if media_type == "tv":
        plex_type = "show"
    elif media_type == "collection":
        plex_type = "collection"
    else:
        plex_type = "movie"
    # v1.12.46: scope to a single section when only_section_id is
    # set (e.g., ACCEPT UPDATE clicked from a specific library
    # row should only re-theme that section's edition, not fan
    # out to every section that owns the title).
    # v1.15.142: match by pi.theme_id linkage (joined to themes)
    # instead of strict pi.guid_tmdb. Pre-fix, Plex rows matched
    # via the anime agent (most anime) typically have guid_tmdb=
    # NULL because the anime agent resolves to TVDB/AniDB GUIDs, not TMDB.
    # plex_enum's resolve_theme_ids (plex_enum.py:1058-1199) sets
    # pi.theme_id via three fallback paths:
    #   1. guid_tmdb (preferred)
    #   2. guid_imdb (orphan match)
    #   3. title_norm + year (last-resort title fallback)
    # The library list query joins on pi.theme_id (api.py:1913),
    # so anime-agent-matched rows DO appear in the UI with TDB-tracked
    # state — but the legacy guid_tmdb-only filter here would
    # silently fail to enqueue any download for them. the user's
    # repro on Dark Gathering (anime, no IMDB ID): DOWNLOAD TDB
    # BACKUP returned "(0 sections, scoped to 3)" with no
    # download enqueued, no user-visible feedback.
    # The fix matches by themes-row identity (the actual semantic
    # target) regardless of WHICH plex GUID kind got the linkage.
    # v1.22.46 (Tires repro): match guid_tmdb OR the theme_id linkage. The
    # v1.15.142 theme_id-ONLY rewrite silently regressed the is_new auto-
    # download: during a sync's batch processing, a BRAND-NEW theme's
    # pi.theme_id is not linked yet (run_sync's resolve_theme_ids runs AFTER
    # the batches, line ~3789), so t.tmdb_id is NULL on every row → 0 sections
    # → the toggle-ON SRC=— auto-download enqueued nothing and the post-sync
    # enum then missed it too (the sync's own resolve pre-linked theme_id, so
    # the row wasn't "newly_linked" that enum). the user's Tires (2024): theme
    # synced new, auto-download ON, never downloaded. The OR restores the
    # guid_tmdb path (works before theme_id is linked) while keeping the
    # theme_id path (anime, guid_tmdb NULL) — the canonical pattern used
    # by in_plex / has_sidecar across sync.py.
    common_where = """
        FROM plex_items pi
        JOIN plex_sections ps ON ps.section_id = pi.section_id
        LEFT JOIN themes t ON t.id = pi.theme_id
        WHERE pi.media_type = ?
          AND ps.included = 1
          AND (pi.guid_tmdb = ?
               OR (t.tmdb_id = ? AND t.media_type = ?))
    """
    if only_section_id is not None:
        sections = conn.execute(
            f"SELECT DISTINCT pi.section_id {common_where} "
            "  AND pi.section_id = ?",
            (plex_type, tmdb_id, tmdb_id, media_type, only_section_id),
        ).fetchall()
    else:
        sections = conn.execute(
            f"SELECT DISTINCT pi.section_id {common_where}",
            (plex_type, tmdb_id, tmdb_id, media_type),
        ).fetchall()
    if not sections:
        # Nothing to enqueue — the item isn't in any included Plex
        # section yet. Common during the window between sync (which
        # creates the themes row) and plex_enum (which populates
        # plex_items). User-initiated callers (manual_url, redownload,
        # override, accept_update, etc.) should treat the 0 return as
        # "queued nothing — Plex hasn't seen this item yet" and surface
        # that to the user.
        log.info(
            "_enqueue_download: no included Plex section owns "
            "(%s, %s) — skipping (reason=%s)",
            media_type, tmdb_id, reason,
        )
        return 0
    enqueued = 0
    for sec in sections:
        section_id = sec["section_id"]
        # v1.21.82: dedup PER EDITION. jobs has no edition_key column — the
        # download payload carries it (v74 json_valid guard; a standard ''
        # job omits it -> COALESCE ''). Pre-fix the dedup was section-only,
        # so a sibling edition's pending download suppressed THIS edition's
        # enqueue — re-downloading Extended did nothing while Theatrical had
        # a queued download. Single-edition libraries (all jobs '') are
        # unaffected: the filter reduces to the old (section) behavior.
        existing = conn.execute(
            """
            SELECT id, status, next_run_at FROM jobs
            WHERE job_type = 'download' AND media_type = ? AND tmdb_id = ?
              AND section_id = ?
              AND COALESCE(CASE WHEN json_valid(payload)
                  THEN json_extract(payload, '$.edition_key') END, '') = ?
              AND status IN ('pending', 'running')
            """,
            (media_type, tmdb_id, section_id, edition_key),
        ).fetchone()
        if existing:
            # v1.23.31: a MANUAL enqueue (hold_for_summary=False — RE-DOWNLOAD /
            # ACCEPT UPDATE / SET URL …) supersedes a still-HELD sync auto-
            # download (parked at _SYNC_HOLD_NEXT_RUN_AT, not yet running): drop
            # the parked row so THIS call's explicit intent (force_place / new
            # URL / rollback in its payload) runs now, instead of being deduped
            # to a silent no-op until the sync's post-summary release (minutes
            # on a big diff). A genuinely in-flight ('running') or already-
            # runnable ('pending' at a real next_run_at) download still dedups.
            held = (not hold_for_summary
                    and existing["status"] == "pending"
                    and existing["next_run_at"] == _SYNC_HOLD_NEXT_RUN_AT)
            if not held:
                continue
            conn.execute("DELETE FROM jobs WHERE id = ?", (existing["id"],))
            # fall through to INSERT this call's download
        payload: dict = {"reason": reason}
        if bulk:
            # v1.23.46: mark this download as part of an ACTUAL bulk action so
            # its per-item notifications coalesce into one list (the flag rides
            # through the download→place chain). Single actions never set it →
            # each fires its own immediate rich notification.
            payload["bulk"] = True
        if auto_place is not None:
            payload["auto_place"] = bool(auto_place)
        if force_place:
            payload["force_place"] = True
        if edition_key:
            # v1.21.62 (C2c): the worker (B2a/B2b) stages + places this
            # download into the edition's own folder, keyed per edition.
            payload["edition_key"] = edition_key
        if rollback is not None:
            # v1.12.114: stamped per-section so the worker can restore
            # the right override / previous_urls row on failure.
            payload["rollback"] = {**rollback, "section_id": section_id}
        # v1.23.23: hold_for_summary parks the job at the far-future
        # sentinel so the worker's claim query skips it until _do_sync
        # releases it after dispatching the sync summary (see
        # release_sync_held_downloads).
        next_run_at = (
            _SYNC_HOLD_NEXT_RUN_AT if hold_for_summary else now_iso()
        )
        conn.execute(
            """
            INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                              payload, status, created_at, next_run_at)
            VALUES ('download', ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (media_type, tmdb_id, section_id, json.dumps(payload),
             now_iso(), next_run_at),
        )
        enqueued += 1
    return enqueued


# ----- Public entry point -----

def resolve_new_theme_pending_update(
    conn: sqlite3.Connection,
    *,
    media_type: str,
    tmdb_id: int,
    section_id: str | None,
    decided_by: str,
) -> bool:
    """v1.20.10: resolve a pending new_theme_available update once motif
    actually HOLDS the TDB theme — called from the worker's post-download
    success path (_record_local_file) so the blue !UPD/TDB↑ pill clears
    on download SUCCESS, not optimistically at click time. Pre-v1.20.10
    the DOWNLOAD TDB BACKUP / REPLACE TDB endpoints flipped the decision
    synchronously at click; a later download failure then left the pill
    cleared with no theme backed up (Finding 2 of the silent-bug audit).
    Now the pill stays up until the theme actually lands (and stays up if
    the download fails) — and self-heals if motif gains the theme via any
    TDB download path.

    Acts only when the acted section's EFFECTIVE pending update (its own
    per-section row first, else the global '' sync row) IS the pending
    new_theme_available one — never touches a different-kind decision
    (e.g. a user-DECLINED upstream change; v1.20.9 audit). Returns True
    when a row was resolved (no-op + False otherwise)."""
    section = section_id if section_id is not None else ""
    # edition-blind OK (v1.21.94): pending_update detection lives at
    # edition_key='' (v81 design — sync writes the upstream-change row
    # edition-blind); the per-edition ACCEPT/DECLINE decision is resolved in
    # api.py against the clicked rk's edition. Section-tiered via IN (?, '').
    eff = conn.execute(
        """SELECT kind, COALESCE(decision, 'pending') AS decision,
                  new_video_id, new_youtube_url, old_youtube_url,
                  upstream_edited_at, detected_at
             FROM pending_updates
            WHERE media_type = ? AND tmdb_id = ?
              AND section_id IN (?, '')
            ORDER BY (section_id = ?) DESC
            LIMIT 1""",
        (media_type, tmdb_id, section, section),
    ).fetchone()
    if eff is None or eff["kind"] != "new_theme_available" \
            or eff["decision"] != "pending":
        return False
    conn.execute(
        """INSERT INTO pending_updates (
              media_type, tmdb_id, section_id,
              new_video_id, new_youtube_url, old_youtube_url,
              upstream_edited_at, detected_at, decision,
              decision_at, decision_by, kind
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'accepted', ?, ?,
                     'new_theme_available')
           ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET
               decision = excluded.decision,
               decision_at = excluded.decision_at,
               decision_by = excluded.decision_by""",
        (media_type, tmdb_id, section,
         eff["new_video_id"], eff["new_youtube_url"], eff["old_youtube_url"],
         eff["upstream_edited_at"], eff["detected_at"],
         now_iso(), decided_by),
    )
    return True


# How many fetched items to process per write transaction. Mirrors the
# plex_enum batching from v1.9.6: smaller batches release the BEGIN
# IMMEDIATE lock more often, letting concurrent API writers (log_event
# in particular) make progress while a multi-thousand-item sync is
# running. Pre-1.9.9 sync did one transaction per item, ~10K transactions
# end-to-end, which soft-locked the API for the duration.
_SYNC_BATCH = 50


def _plex_title_present(
    conn: sqlite3.Connection, plex_type: str, tmdb_id: int,
    theme_row_id: int | None, *, sidecar_only: bool = False,
) -> bool:
    """v1.23.89: index-friendly "is this title in Plex" (optionally requiring a
    local sidecar). Two EXISTS, one per plex_items index — guid_tmdb then
    theme_id — instead of `pi.guid_tmdb = ? OR (t.tmdb_id = ? ...)` across a
    themes JOIN, which the planner served with a media_type SCAN (~1,341 anime /
    11,476 movies per call → the sync flush's per-changed-item cost). theme_row_id
    is the upserted themes.id; matching pi.theme_id = it is exactly the old
    t.tmdb_id-via-pi.theme_id join (anime, guid_tmdb NULL). plex_type is the
    plex_items.media_type ('show' for tv); the themes row's media_type is implicit
    in theme_row_id, so no separate t.media_type clause is needed."""
    extra = " AND local_theme_file = 1" if sidecar_only else ""
    row = conn.execute(
        "SELECT 1 WHERE "
        f"  EXISTS (SELECT 1 FROM plex_items "
        f"           WHERE media_type = ?{extra} AND guid_tmdb = ?) "
        f"  OR EXISTS (SELECT 1 FROM plex_items "
        f"              WHERE media_type = ?{extra} AND theme_id = ?)",
        (plex_type, tmdb_id, plex_type, theme_row_id),
    ).fetchone()
    return row is not None


def _plex_supplies_theme(
    conn: sqlite3.Connection, media_type: str, tmdb_id: int,
) -> bool:
    """v1.10.16: True iff Plex's own agent supplies a theme for this
    title and motif doesn't manage the file. Used by sync to skip the
    auto-download for P-agent items so we honour the user's 'Plex-first'
    preference.

    Conditions (all must hold):
      - plex_items row exists for this (media_type, tmdb_id) — the
        title is in the user's Plex library.
      - pi.has_theme = 1 — Plex says the item has a theme.
      - pi.local_theme_file = 0 — there's no theme.mp3 sidecar at the
        Plex folder.
      - no local_files row — motif hasn't downloaded for this item.
      - no placements row — motif hasn't placed for this item.
    Falls back to False (= proceed with download) if plex_items hasn't
    been enumerated yet (fresh install) — safe default.

    v1.18.0: for media_type='collection', `pi.media_type` is also
    'collection' (no aliasing — themes and plex_items align on
    this value), and the sidecar concept doesn't apply (Plex
    stores collection themes in its metadata bundle, not on disk).
    The query's pi.local_theme_file=0 clause is still meaningful:
    collections never get a local_theme_file=1 (no folder to
    sidecar into) so the gate is effectively "Plex has a theme
    attribute but motif hasn't uploaded one yet" — the right
    skip condition.
    """
    # v1.18.0: themes ↔ plex_items media_type alignment.
    if media_type == "tv":
        plex_type = "show"
    elif media_type == "collection":
        plex_type = "collection"
    else:
        plex_type = "movie"
    # v1.22.17: ALSO match the title's plex_items via pi.theme_id
    # linkage (joined to themes), not just strict pi.guid_tmdb.
    # AniDB anime (the bulk of SRC=P rows) resolve to TVDB GUIDs
    # so guid_tmdb stays NULL; the legacy gate returned False for
    # them, so the v1.22.12 P-row suppression (which keys on this
    # helper) never recognized anime P-rows as Plex-supplied — they
    # fell to the new_theme_available branch and re-flooded the !UPD
    # pill. The OR keeps the guid_tmdb path (no regression) and adds
    # the theme_id path v1.15.142 _enqueue_download established.
    row = conn.execute(
        """SELECT 1
             FROM plex_items pi
             LEFT JOIN themes t ON t.id = pi.theme_id
             LEFT JOIN local_files lf
               ON lf.media_type = ? AND lf.tmdb_id = ?
             LEFT JOIN placements p
               ON p.media_type = ? AND p.tmdb_id = ?
            WHERE pi.media_type = ?
              AND (pi.guid_tmdb = ?
                   OR (t.tmdb_id = ? AND t.media_type = ?))
              AND pi.has_theme = 1
              AND pi.local_theme_file = 0
              AND lf.file_path IS NULL
              AND p.media_folder IS NULL
            LIMIT 1""",
        (media_type, tmdb_id, media_type, tmdb_id,
         plex_type, tmdb_id, tmdb_id, media_type),
    ).fetchone()
    return row is not None


# v1.11.17: probe enqueue + handler removed entirely. The TDB pill
# color is still driven by failure_kind from real download attempts;
# the speculative probe pass added 25-50 minutes of yt-dlp work per
# sync without a meaningful UX gain.


def _flush_sync_batch(
    db_path,
    batch: list[tuple[str, int, dict, str]],
    *,
    sync_ts: str,
    enqueue_downloads: bool,
    auto_place_override: bool | None,
    auto_download_new_themes: bool,
    stats: SyncStats,
) -> None:
    """Process one batch of fetched ThemerrDB records inside a single txn.

    `batch` is a list of (media_type, tmdb_id, record, upstream_source).

    v1.19.71: `auto_download_new_themes` (passed from
    cfg.sync.auto_download_new_themes_for_unthemed_rows) gates
    the legacy "auto-download on new theme for SRC=— rows"
    behavior. Default False — sync writes a pending_updates row
    with kind='new_theme_available' instead, surfacing the new
    theme as a manual ACCEPT/KEEP CURRENT prompt.
    """
    if not batch:
        return
    with get_conn(db_path) as conn, transaction(conn):
        for media_type, tmdb_id, record, upstream_source in batch:
            is_new, url_changed, old_video_id, old_url = _upsert_theme(
                conn,
                media_type=media_type,
                tmdb_id=tmdb_id,
                record=record,
                upstream_source=upstream_source,
                sync_ts=sync_ts,
            )
            # v1.23.88: an UNCHANGED theme (not new, no URL change) needs no
            # further per-item work — the plex_items presence joins + notify
            # counting + enqueue below are consumed ONLY by the is_new /
            # url_changed branches. A no-change sync (new=0/updated=0) was
            # running ~5600 wasted 52k-row plex_items joins per flush — the
            # v1.23.80 timing's DB-flush 279s. _upsert_theme already bumped
            # last_seen_sync_at + cleared tdb_dropped_at, so skip to next item.
            if not (is_new or url_changed):
                continue
            # v1.10.16: Plex-first default — when Plex is already supplying
            # a theme for this title (P-agent state) and motif doesn't
            # manage the file yet, skip the auto-download. The user can
            # still kick a download manually from the row's primary
            # button (which prompts for confirmation per v1.10.14).
            # 'auto' here means motif initiated the enqueue; manual
            # /redownload calls bypass this gate.
            plex_supplies = _plex_supplies_theme(conn, media_type, tmdb_id)
            # v1.19.70: pre-compute the plex_mt mapping + the
            # in_plex predicate up front so both is_new and
            # url_changed branches can gate notification-count
            # increments on plex_items presence. the user's repro:
            # "The Family Circumstances of the Irregular Witch
            # (2023)" appeared in his sync notification but isn't
            # in his Plex library — purely a TDB-only title.
            # User cares about themes for their own library, not
            # the global TDB catalog. The faded T-only filter
            # chip still lets discovery of TDB-only titles in
            # the library UI; the notification just stops
            # surfacing them.
            if media_type == "tv":
                _plex_mt = "show"
            elif media_type == "collection":
                _plex_mt = "collection"
            else:
                _plex_mt = "movie"
            # v1.22.17: match by pi.theme_id linkage, not just strict
            # pi.guid_tmdb. AniDB anime (the bulk of the user's library)
            # resolve to TVDB GUIDs so guid_tmdb stays NULL, but
            # resolve_theme_ids still sets pi.theme_id (guid_tmdb → guid_imdb →
            # title+year fallbacks). The legacy guid_tmdb-only gate silently
            # excluded ~100% of anime from new_count AND from the
            # new_theme_available prompt. _plex_title_present keeps the guid_tmdb
            # path (no regression for rows linked only that way) and adds the
            # theme_id path the v1.15.142 _enqueue_download fix established.
            # v1.23.89: resolve the just-upserted themes.id ONCE here so those
            # presence checks match pi.theme_id directly (index lookup) instead
            # of OR-ing across a themes JOIN that scanned the media_type. Reused
            # by in_plex + has_sidecar in BOTH the is_new / url_changed branches.
            _theme_row = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            _theme_row_id = _theme_row[0] if _theme_row else None
            in_plex = _plex_title_present(conn, _plex_mt, tmdb_id, _theme_row_id)
            if is_new:
                if in_plex:
                    stats.new_count += 1
                # v1.12.17: cross-sync match detection. If motif (or Plex
                # via the local_theme_file flag) already has theme
                # content for this title, surface a blue update pill
                # via pending_updates instead of auto-downloading. Lets
                # the user explicitly choose to switch from their
                # existing theme (M sidecar / U manual URL / A adopted)
                # to motif-managed TDB content. Pre-fix the sync just
                # auto-downloaded over the existing local theme.
                has_local_files = conn.execute(
                    "SELECT 1 FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone() is not None
                # v1.14.55 audit clarification: title-global by design;
                # see sync.py:377 marker for the full rationale.
                has_override = conn.execute(
                    "SELECT 1 FROM user_overrides WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone() is not None
                # v1.18.0: collection maps to 'collection' on
                # plex_items.media_type (no aliasing). Sidecar concept
                # doesn't apply to collections (no folder), so the
                # query naturally returns False for them.
                # v1.19.70: reuses the outer-scope `_plex_mt` (same
                # mapping) — pre-fix the if/elif/else block lived
                # twice (once here, once nowhere else). Now hoisted
                # to outer scope so both is_new + url_changed can
                # share the predicate.
                # v1.22.17: ALSO match theme_id linkage (see in_plex
                # above) so anime M-rows (guid_tmdb NULL) are
                # recognized as having a Plex-folder sidecar.
                # v1.23.89: index-friendly split-EXISTS (was an OR-across-themes-
                # JOIN that scanned the media_type) — see _plex_title_present.
                has_sidecar = _plex_title_present(
                    conn, _plex_mt, tmdb_id, _theme_row_id, sidecar_only=True)
                yt_url = record.get("youtube_theme_url")
                # v1.19.71: surface new TDB themes as pending_updates
                # with kind='new_theme_available' so the operator sees
                # a blue !UPD glyph on every in-Plex row that just
                # gained a TDB theme (regardless of motif/Plex state).
                # the user's repro: pre-fix only the cross-match case
                # (motif/Plex already has content) wrote a
                # pending_update; SRC=— rows auto-downloaded silently
                # and SRC=P rows did nothing visible. Operator had no
                # way to know TDB just discovered themes for their
                # untouched library items.
                #
                # Branches (in_plex gate first):
                #   - has_user_or_motif_content (U/A/M/P-with-sidecar):
                #     write pending_updates kind='new_theme_available'
                #     — operator decides via ACCEPT / KEEP CURRENT.
                #   - plex_supplies (SRC=P):
                #     write pending_updates kind='new_theme_available'
                #     — operator can ACCEPT (download + take over) or
                #     KEEP CURRENT (let Plex keep serving).
                #   - SRC=— (no motif, no Plex):
                #     if auto_download_new_themes AND enqueue_downloads
                #     → enqueue_download (existing pre-v1.19.71 behavior
                #     preserved for opt-in users). Else → write
                #     pending_updates so user prompted manually.
                #
                # Non-in_plex titles take no action (themes row still
                # written by _upsert_theme above for the library's
                # T-only discovery surface; no pending_update + no
                # download since no plex_items row exists to attach
                # to).
                if yt_url and in_plex:
                    has_content = (
                        has_local_files or has_override or has_sidecar
                    )
                    yt_vid = extract_video_id(yt_url)
                    if (not has_content and not plex_supplies
                            and auto_download_new_themes
                            and enqueue_downloads):
                        # SRC=— + opt-in setting + sync enqueue
                        # allowed → auto-download (existing v1.7+
                        # default behavior, now opt-in via
                        # auto_download_new_themes_for_unthemed_rows).
                        _enqueue_download(
                            conn, media_type=media_type, tmdb_id=tmdb_id,
                            reason="new", auto_place=auto_place_override,
                            hold_for_summary=True,
                        )
                    elif plex_supplies and not has_content:
                        # v1.22.12: pure SRC=P row — Plex serves its own theme
                        # and motif owns nothing. the user: do NOT flag these as
                        # new_theme_available. v1.19.71 surfaced them so the
                        # operator could take over, but anime is ~100% Plex-
                        # served + TDB's catalog keeps growing, so every sync
                        # flooded the blue !UPD pill / UPD badge / NEEDS WORK
                        # with rows motif doesn't manage. The theme is still
                        # discoverable; the operator can DOWNLOAD TDB per-row
                        # from the SOURCE menu to take over. (The api.py
                        # actionable gate also _not_p_row-gates new_theme so
                        # rows detected by PRIOR syncs go silent too.)
                        pass
                    else:
                        # Manual prompt path — covers cross-match
                        # (has_content), Plex-serves (plex_supplies),
                        # and SRC=— without the opt-in. v1.12.99:
                        # schema v31 added section_id to the PK; sync
                        # writes the title-global '' row. ON CONFLICT
                        # preserves any 'declined' decision the user
                        # may have set in a prior cycle.
                        conn.execute(
                            """
                            INSERT INTO pending_updates (
                                media_type, tmdb_id, section_id,
                                old_video_id, new_video_id,
                                old_youtube_url, new_youtube_url,
                                upstream_edited_at, detected_at,
                                decision, kind
                            ) VALUES (?, ?, '', NULL, ?, NULL, ?, ?, ?,
                                      'pending', 'new_theme_available')
                            ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET
                                new_video_id = excluded.new_video_id,
                                new_youtube_url = excluded.new_youtube_url,
                                upstream_edited_at = excluded.upstream_edited_at,
                                detected_at = excluded.detected_at,
                                kind = excluded.kind,
                                decision = CASE
                                    WHEN pending_updates.decision = 'declined'
                                    THEN 'declined'
                                    ELSE 'pending'
                                END
                            """,
                            (
                                media_type, tmdb_id, yt_vid, yt_url,
                                record.get("youtube_theme_edited"), sync_ts,
                            ),
                        )
            elif url_changed:
                already_have = conn.execute(
                    "SELECT 1 FROM local_files WHERE media_type = ? AND tmdb_id = ?",
                    (media_type, tmdb_id),
                ).fetchone() is not None
                # v1.14.55 audit clarification: title-global by design;
                # see sync.py:377 marker for the full rationale.
                # v1.19.60: fetch the override URL too (was a bare
                # EXISTS pre-fix) so the kind-decision logic below
                # can detect the override-coincidence case (user's
                # override URL happens to match the new TDB URL,
                # making it a U→T conversion offer rather than a
                # routine upstream_changed event).
                override_row = conn.execute(
                    "SELECT youtube_url FROM user_overrides "
                    "WHERE media_type = ? AND tmdb_id = ? LIMIT 1",
                    (media_type, tmdb_id),
                ).fetchone()
                has_override = override_row is not None
                override_url = (override_row["youtube_url"]
                                if override_row else None)
                # v1.19.58: actionability gate. Pre-fix the
                # `stats.updated_count` increment fired on EVERY
                # url_changed event — including the
                # not-already_have + not-has_override + not-enqueue
                # case where motif silently rolled forward
                # themes.youtube_url with no user-visible effect.
                # the user's repro: "Bastard!!" + "The Beginning
                # After the End" appeared in the Updated:
                # notification but the row's INFO card showed
                # nothing actionable. The notification was firing
                # on rows where motif had no effect — pure TDB-
                # metadata bookkeeping.
                #
                # Fix: only count + list in updated_titles when
                # the change is USER-ACTIONABLE:
                #   - already_have or has_override → pending_
                #     update INSERT (lights up !UPD glyph)
                #   - else: enqueue_downloads AND not plex_supplies
                #     → re-download enqueued (DL chip changes)
                # Rows where motif silently rolled forward only
                # stay bookkeeping — themes.youtube_url updates
                # but the user doesn't get a notification or UI
                # surface saying "look at this."
                # v1.21.10: mirror the is_new branch. An M-row (manual
                # sidecar) is "has content", so a TDB url change must
                # NOT auto-download it — it prompts via the if-block
                # below instead; and the SRC=— auto-download must respect
                # the auto_download opt-in. Pre-fix this branch checked
                # only already_have/has_override and ignored BOTH
                # has_sidecar AND the toggle — the lone closed-loop break
                # the v1.21.9 audit found (M-rows silently re-downloaded
                # TDB bytes every cron sync; SRC=— ignored the opt-in).
                # _plex_mt hoisted to outer scope (v1.19.70).
                # v1.22.39 (audit): mirror the is_new twin (above) — match the
                # sidecar via theme_id linkage too. Pre-fix this keyed on
                # guid_tmdb ONLY, so a anime M-row (guid_tmdb NULL, linked
                # only via pi.theme_id) read has_sidecar=False on a TDB URL
                # change and re-downloaded TDB bytes OVER the user's manual
                # sidecar every cron sync. The v1.22.17 "4 sites" fix missed
                # this 5th one — the closed-loop break the v1.21.10 comment
                # above claims to have closed, still open for the guid_tmdb-NULL
                # cohort.
                # v1.23.89: index-friendly split-EXISTS (was an OR-across-themes-
                # JOIN that scanned the media_type) — see _plex_title_present.
                # _theme_row_id was resolved once in the shared per-item scope.
                has_sidecar = _plex_title_present(
                    conn, _plex_mt, tmdb_id, _theme_row_id, sidecar_only=True)
                will_enqueue_download = (
                    not (already_have or has_override or has_sidecar)
                    and enqueue_downloads
                    and not plex_supplies
                    and auto_download_new_themes
                )
                is_actionable = (
                    (already_have or has_override or has_sidecar)
                    or will_enqueue_download
                )
                # v1.19.70: gate the notification count + title
                # list on plex_items presence. the user's repro
                # was on the is_new path but the same principle
                # applies: items not in user's Plex library
                # should not surface in sync notifications.
                # `in_plex` computed at outer scope (line ~865).
                if is_actionable and in_plex:
                    stats.updated_count += 1
                    # v1.18.64: capture title+year for the
                    # sync_completed Apprise body. Cap at 50
                    # stored to bound memory on big syncs — body
                    # samples the first 10.
                    if len(stats.updated_titles) < 50:
                        _title = (record.get("title")
                                  or record.get("name") or "")
                        _rd = (record.get("release_date")
                               or record.get("first_air_date") or "")
                        _year = _rd[:4] if _rd else None
                        if _title:
                            # v1.22.45: carry media_type+tmdb_id so the worker
                            # can group the Updated list by Plex section.
                            stats.updated_titles.append(
                                (media_type, tmdb_id, _title, _year))
                if already_have or has_override or has_sidecar:
                    # v1.12.99: schema v31 added section_id to the PK.
                    # Sync writes the title-global '' row.
                    # v1.19.60: bind the captured `old_url` for
                    # old_youtube_url (pre-fix bound None, leaving
                    # the column blank and erasing the v1.12.119
                    # gate's ability to detect a real diff + the
                    # INFO card's ability to show before/after).
                    # ON CONFLICT preserves the existing
                    # old_youtube_url if a row already existed for
                    # this title — captures the FIRST diff, not the
                    # latest, so the user sees the URL they had
                    # when motif first noticed the change.
                    yt_vid = extract_video_id(record.get("youtube_theme_url"))
                    new_url = record.get("youtube_theme_url")
                    # v1.19.60: kind discriminator. Pre-fix sync
                    # always defaulted to 'upstream_changed' which
                    # the read-path gate's URL-diff branch then
                    # suppressed in the override-coincidence case
                    # (user's override URL coincidentally matches
                    # the new TDB URL — the user's "Beginning After
                    # the End" repro). When the override exists and
                    # its video_id matches the new TDB video_id,
                    # write kind='urls_match' so the gate's existing
                    # U→T conversion branch fires.
                    pu_kind = "upstream_changed"
                    if (has_override and override_url
                            and yt_vid is not None
                            and extract_video_id(override_url) == yt_vid):
                        pu_kind = "urls_match"
                    conn.execute(
                        """
                        INSERT INTO pending_updates (
                            media_type, tmdb_id, section_id, old_video_id, new_video_id,
                            old_youtube_url, new_youtube_url,
                            upstream_edited_at, detected_at, decision, kind
                        ) VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, 'pending', ?)
                        ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET
                            new_video_id = excluded.new_video_id,
                            new_youtube_url = excluded.new_youtube_url,
                            upstream_edited_at = excluded.upstream_edited_at,
                            detected_at = excluded.detected_at,
                            kind = excluded.kind,
                            decision = CASE
                                -- v1.20.14: a KEEP CURRENT (declined)
                                -- stays declined only while TDB keeps
                                -- serving the SAME video the user
                                -- dismissed; a GENUINELY NEW video
                                -- re-arms the prompt ('re-prompt on a
                                -- further URL change' — the user's round-3
                                -- policy). Pre-fix it stayed declined
                                -- forever regardless of later changes.
                                WHEN pending_updates.decision = 'declined'
                                     AND pending_updates.new_video_id
                                         IS excluded.new_video_id
                                THEN 'declined'
                                ELSE 'pending'
                            END
                        """,
                        (
                            media_type, tmdb_id,
                            old_video_id, yt_vid,
                            old_url, new_url,
                            record.get("youtube_theme_edited"),
                            sync_ts, pu_kind,
                        ),
                    )
                    # v1.20.14: re-arm per-section KEEP CURRENT decisions
                    # on a genuinely new video. The '' UPSERT above only
                    # touches the global row; a per-section 'declined' row
                    # (written by KEEP CURRENT) shadows '' via the read-path
                    # COALESCE and would otherwise stay dismissed forever —
                    # contradicting the decline dialog's "prompt again only
                    # on a further URL change" promise (M1 / round-3 policy).
                    # `new_video_id IS NOT ?` only re-arms sections whose
                    # declined video differs from the incoming one; same-
                    # video declines stay declined (no re-nag).
                    conn.execute(
                        """
                        UPDATE pending_updates SET
                            decision = 'pending',
                            decision_at = NULL, decision_by = NULL,
                            new_video_id = ?, new_youtube_url = ?,
                            old_video_id = ?, old_youtube_url = ?,
                            upstream_edited_at = ?, detected_at = ?,
                            kind = ?
                        WHERE media_type = ? AND tmdb_id = ?
                          AND section_id != ''
                          AND decision = 'declined'
                          AND new_video_id IS NOT ?
                        """,
                        (yt_vid, new_url, old_video_id, old_url,
                         record.get("youtube_theme_edited"), sync_ts, pu_kind,
                         media_type, tmdb_id, yt_vid),
                    )
                else:
                    # Same Plex-first gate as the is_new branch above:
                    # don't pull a fresh download for a P-agent item.
                    # v1.21.10: + the auto_download_new_themes opt-in, so
                    # a SRC=— row only auto-downloads on a url change when
                    # the toggle is on (mirrors is_new). Toggle OFF → the
                    # re-arm below keeps its !UPD prompt fresh instead.
                    if (enqueue_downloads and not plex_supplies
                            and auto_download_new_themes):
                        _enqueue_download(
                            conn, media_type=media_type, tmdb_id=tmdb_id,
                            reason="url_changed",
                            auto_place=auto_place_override,
                            hold_for_summary=True,
                        )
                    # v1.20.14: refresh the new_theme_available prompt for
                    # SRC=– rows when TDB changes the theme. new_theme is
                    # emitted only on is_new, so without this the '' (and
                    # any per-section) new_theme row kept its ORIGINAL URL
                    # forever — an un-actioned prompt went stale (ACCEPT /
                    # the v1.20.10 worker resolver download the row's stored
                    # URL) and a declined section never re-prompted (M2 /
                    # round-3 policy). Re-arm declined rows only on a
                    # genuinely new video; same-video declines + already-
                    # accepted rows are left untouched.
                    _nt_vid = extract_video_id(
                        record.get("youtube_theme_url"))
                    _nt_url = record.get("youtube_theme_url")
                    conn.execute(
                        """
                        UPDATE pending_updates SET
                            new_video_id = ?, new_youtube_url = ?,
                            upstream_edited_at = ?, detected_at = ?,
                            decision = CASE
                                WHEN decision = 'accepted' THEN 'accepted'
                                WHEN decision = 'declined'
                                     AND new_video_id IS ?
                                THEN 'declined'
                                ELSE 'pending' END
                        WHERE media_type = ? AND tmdb_id = ?
                          AND kind = 'new_theme_available'
                        """,
                        (_nt_vid, _nt_url,
                         record.get("youtube_theme_edited"), sync_ts,
                         _nt_vid, media_type, tmdb_id),
                    )


# ----- v1.12.121 Phase A: snapshot path -----
#
# The remote path makes one HTTP request per item against gh-pages — at
# ~10k items that's the dominant cost of a sync. The snapshot path pulls
# a single tarball of LizardByte/ThemerrDB's `database` branch from
# codeload.github.com, extracts it to a temp dir, and resolves every
# item from the local tree. Same JSON shape, same upsert path; ~1000×
# fewer HTTP requests + free differential sync once Phase B lands.
#
# Failure here is non-fatal — `run_sync` falls through to the remote
# path and flags `op_progress.detail.fallback_active=True` so the
# status bar can surface "last sync used the fallback".

import os
import tarfile
import tempfile


class _SnapshotError(RuntimeError):
    """Snapshot acquisition (download/extract/validate) failed.
    Caller should fall back to the remote path."""


class _SnapshotUnchanged(Exception):
    """Codeload returned 304 Not Modified — the tarball at HEAD of
    the database branch is byte-identical to what we last pulled.
    NOT a failure: caller should skip the entire upsert pipeline
    (no need to re-walk 5k items whose data hasn't changed) and
    proceed to the prune sweeps. v1.12.126 Phase A.5."""


# Hard-coded ceiling on the extracted tree. Catches a runaway tarball
# (e.g. zip-bomb-style nested archives that decompress into hundreds of
# GB) before it fills the disk. The real database branch sits at ~52
# MB extracted; a 500 MB ceiling gives 10× headroom and still flags
# anything pathological. v1.12.121.
_SNAPSHOT_MAX_BYTES = 500 * 1024 * 1024
# Reject archives with absurd member counts. Real branch has ~30k
# members; 200k catches a degenerate millions-of-empty-files tarball
# without false-positiving on legitimate growth.
_SNAPSHOT_MAX_MEMBERS = 200_000


class _DatabaseSnapshot:
    """Codeload tarball view of the ThemerrDB `database` branch.

    Lifecycle:
      snap = _DatabaseSnapshot(db_path, op_id, url, cancel_check)
      snap.acquire(client)              # download + extract + validate
      try:
          idx = snap.index("movies")    # local read of pages.json + all_page_*.json
          rec = snap.fetch_item("movies", imdb_id="tt...", tmdb_id=123)
      finally:
          snap.release()                # rmtree the temp dir

    Cancel check is polled between download chunks and between
    extracted members so a // CANCEL during snapshot acquisition
    bails within ~1MB / ~1k files.

    The cached tarball lives at <db_dir>/cache/themerrdb-database.tar.gz.
    On a successful pull it survives so a future sync that fails to
    re-download can fall back to the prior snapshot (Phase B will
    use it as the diff baseline).
    """

    def __init__(
        self,
        db_path: Path,
        op_id: str,
        tar_url: str,
        cancel_check,
    ):
        self.db_path = Path(db_path)
        self.op_id = op_id
        self.tar_url = tar_url
        self._cancel_check = cancel_check
        # Where the tarball is cached. Lives next to motif.db, not under
        # themes_dir — it's operational state, not user content.
        self.cache_dir = self.db_path.parent / "cache"
        self.tar_path = self.cache_dir / "themerrdb-database.tar.gz"
        # v1.12.126 Phase A.5: conditional-GET state lives next to the
        # cached tarball. JSON file: {"etag": "...", "last_modified": "..."}.
        # On the next sync we send these as If-None-Match + If-
        # Modified-Since; codeload returns 304 if the database branch
        # head hasn't moved, and we skip the whole upsert pipeline.
        self.meta_path = self.cache_dir / "themerrdb-database.tar.gz.meta"
        # Populated on acquire().
        self._tmpdir: tempfile.TemporaryDirectory | None = None
        self.root: Path | None = None
        # v1.12.126: True when codeload returned 304 (no upstream change).
        # Caller checks via is_unchanged() and skips index/fetch.
        self._unchanged = False

    # -- acquisition --

    def is_unchanged(self) -> bool:
        return self._unchanged

    def acquire(self, client: httpx.Client) -> None:
        """Download + extract + validate. Raises _SnapshotError on
        any failure that should trigger fallback to the remote path.

        v1.12.126 Phase A.5: when codeload returns 304 Not Modified
        the download short-circuits via _SnapshotUnchanged, which
        acquire() catches and translates into self._unchanged=True.
        Caller checks via is_unchanged() and skips the upsert
        pipeline. NOT a failure — the data is still fresh, just
        identical to what we already have.

        Cancellation propagates through unchanged: a // CANCEL during
        snapshot acquisition raises _JobCancelled, which is the
        sync-wide bail signal — wrapping it as _SnapshotError would
        spuriously trigger the fallback path (and stick the warn-tone
        pill on the idle indicator) instead of ending the run cleanly
        as 'cancelled'.
        """
        # Imported lazily to keep the worker→sync→worker cycle tractable.
        from .worker import _JobCancelled
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._download(client)
            self._extract()
            self._validate()
        except _SnapshotUnchanged:
            # 304 short-circuit. Don't release — there's no tmpdir
            # to clean (we never extracted). is_unchanged() now True.
            self._unchanged = True
            return
        except _JobCancelled:
            self.release()
            raise
        except _SnapshotError:
            self.release()
            raise
        except Exception as e:
            self.release()
            raise _SnapshotError(
                f"snapshot acquire failed: {type(e).__name__}: {e}"
            ) from e

    def release(self) -> None:
        if self._tmpdir is not None:
            try:
                self._tmpdir.cleanup()
            except Exception as e:
                log.warning("snapshot tmpdir cleanup failed: %s", e)
            self._tmpdir = None
            self.root = None

    # -- stages --

    def _download(self, client: httpx.Client) -> None:
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage="snapshot_download",
            stage_label="Downloading ThemerrDB snapshot",
            stage_current=0, stage_total=0,
            activity=_redact_url_credentials(f"GET {self.tar_url}"),  # v0.51.75: redact URL creds
        )
        # v1.12.126 Phase A.5: conditional GET. If we have ETag /
        # Last-Modified from the prior successful download AND the
        # cached tarball is still on disk, send the If-None-Match /
        # If-Modified-Since headers. codeload returns 304 if the
        # tree hasn't changed — at which point we short-circuit
        # the entire sync (skip extract + index + fetch + flush).
        # Headers are only useful when we'd actually use the cache,
        # so gate on tar_path existing too — protects against the
        # case where the user wiped the cache dir but the meta file
        # somehow survived.
        cond_headers: dict[str, str] = {}
        if self.tar_path.is_file() and self.meta_path.is_file():
            try:
                meta = json.loads(self.meta_path.read_text())
                if meta.get("etag"):
                    cond_headers["If-None-Match"] = meta["etag"]
                if meta.get("last_modified"):
                    cond_headers["If-Modified-Since"] = meta["last_modified"]
            except (OSError, ValueError) as e:
                log.debug("snapshot meta unreadable: %s", e)
        # Stream to a sibling .partial file then atomic-rename. Crash
        # mid-download leaves only the .partial; the next run discards
        # it and starts fresh.
        partial = self.tar_path.with_suffix(self.tar_path.suffix + ".partial")
        try:
            with client.stream("GET", self.tar_url, headers=cond_headers) as r:
                if r.status_code == 304:
                    # Tree unchanged. Update mtime on the meta file so
                    # future "is the cache fresh" checks (Phase B may
                    # add one) see the latest verification time.
                    op_progress.update_progress(
                        self.db_path, self.op_id,
                        activity="Snapshot unchanged (304) — skipping pipeline",
                    )
                    log.info(
                        "snapshot: codeload returned 304 (tree unchanged); "
                        "skipping extract + upsert"
                    )
                    raise _SnapshotUnchanged()
                if r.status_code != 200:
                    raise _SnapshotError(
                        f"codeload returned HTTP {r.status_code}"
                    )
                # Capture validators for the next conditional GET. Save
                # them only after the body streams successfully (below)
                # — a corrupted/truncated download must NOT advance
                # the meta or we'd 304 onto a half-broken cached tarball
                # next time.
                fresh_etag = r.headers.get("etag")
                fresh_last_mod = r.headers.get("last-modified")
                total = int(r.headers.get("content-length") or 0)
                if total > _SNAPSHOT_MAX_BYTES:
                    raise _SnapshotError(
                        f"snapshot Content-Length {total} exceeds ceiling "
                        f"{_SNAPSHOT_MAX_BYTES}"
                    )
                op_progress.update_progress(
                    self.db_path, self.op_id,
                    stage_current=0, stage_total=total,
                )
                received = 0
                last_emit = time.monotonic()
                with partial.open("wb") as fp:
                    for chunk in r.iter_bytes(chunk_size=64 * 1024):
                        if self._cancel_check():
                            from .worker import _JobCancelled
                            raise _JobCancelled()
                        fp.write(chunk)
                        received += len(chunk)
                        if received > _SNAPSHOT_MAX_BYTES:
                            raise _SnapshotError(
                                f"snapshot download exceeded ceiling "
                                f"{_SNAPSHOT_MAX_BYTES} bytes"
                            )
                        if (time.monotonic() - last_emit) > 0.5:
                            op_progress.update_progress(
                                self.db_path, self.op_id,
                                stage_current=received,
                                stage_total=total,
                            )
                            last_emit = time.monotonic()
            os.replace(partial, self.tar_path)
            # v1.12.126 Phase A.5: persist conditional-GET validators
            # only after the body lands. A truncated download must
            # not advance the meta — next run would otherwise 304
            # against a half-broken cached tarball.
            if fresh_etag or fresh_last_mod:
                meta_payload: dict[str, str] = {}
                if fresh_etag:
                    meta_payload["etag"] = fresh_etag
                if fresh_last_mod:
                    meta_payload["last_modified"] = fresh_last_mod
                try:
                    self.meta_path.write_text(json.dumps(meta_payload))
                except OSError as e:
                    log.warning("snapshot meta write failed: %s", e)
            op_progress.update_progress(
                self.db_path, self.op_id,
                stage_current=received, stage_total=received,
                activity=f"Snapshot downloaded ({received} bytes)",
            )
        except _SnapshotError:
            partial.unlink(missing_ok=True)
            raise
        except _SnapshotUnchanged:
            # 304 path — no partial to clean up, no error to wrap.
            partial.unlink(missing_ok=True)
            raise
        except httpx.HTTPError as e:
            partial.unlink(missing_ok=True)
            raise _SnapshotError(f"download HTTP error: {e}") from e
        except BaseException:
            # Catches _JobCancelled (via Exception) and KeyboardInterrupt
            # so the half-written .partial doesn't linger across runs.
            # Re-raises untouched — acquire() decides how to dispatch.
            partial.unlink(missing_ok=True)
            raise

    def _extract(self) -> None:
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage="snapshot_extract",
            stage_label="Extracting snapshot",
            stage_current=0, stage_total=0,
            activity="Extracting tarball",
        )
        self._tmpdir = tempfile.TemporaryDirectory(prefix="motif-tdb-")
        dest = Path(self._tmpdir.name)
        # Strict member filter: reject anything that isn't a regular
        # file or directory under <prefix>/(movies|tv_shows)/, and
        # refuse symlinks/hardlinks/devices outright. Path components
        # are checked for `..` and absolute roots.
        # v1.18.0: movie_collections/ also extracted so the snapshot
        # path can sync collection themes.
        allowed_subdirs = ("movies", "tv_shows", "movie_collections")
        try:
            with tarfile.open(self.tar_path, mode="r:gz") as tf:
                # Enumerate first so we can report stage_total.
                members = tf.getmembers()
                if len(members) > _SNAPSHOT_MAX_MEMBERS:
                    raise _SnapshotError(
                        f"snapshot has {len(members)} members "
                        f"(ceiling {_SNAPSHOT_MAX_MEMBERS})"
                    )
                op_progress.update_progress(
                    self.db_path, self.op_id,
                    stage_current=0, stage_total=len(members),
                )
                # Detect the prefix dir (e.g. "ThemerrDB-database/").
                # codeload tarballs always have a single top-level
                # directory named "<repo>-<branch>".
                prefix: str | None = None
                for m in members:
                    head = m.name.split("/", 1)[0]
                    if head and not head.startswith("."):
                        prefix = head
                        break
                if prefix is None:
                    raise _SnapshotError("snapshot has no top-level directory")

                extracted = 0
                last_emit = time.monotonic()
                total_bytes = 0
                for i, m in enumerate(members):
                    if i % 256 == 0 and self._cancel_check():
                        from .worker import _JobCancelled
                        raise _JobCancelled()
                    if not (m.isreg() or m.isdir()):
                        # symlink, hardlink, fifo, device, char — reject.
                        continue
                    # Strip the codeload prefix.
                    if not m.name.startswith(prefix + "/") and m.name != prefix:
                        continue
                    rel = m.name[len(prefix) + 1:] if m.name != prefix else ""
                    if not rel:
                        continue
                    # Tar-slip defenses.
                    if rel.startswith("/") or ".." in rel.split("/"):
                        raise _SnapshotError(
                            f"snapshot member has unsafe path: {m.name!r}"
                        )
                    # v1.18.0: Only keep movies/, tv_shows/, and
                    # movie_collections/. Everything else
                    # (root README.md, .github/, etc.) is dropped.
                    if not any(
                        rel == sub or rel.startswith(sub + "/")
                        for sub in allowed_subdirs
                    ):
                        continue
                    target = dest / rel
                    # Final paranoia: resolved target must stay under dest.
                    if not str(target.resolve()).startswith(
                        str(dest.resolve()) + os.sep
                    ) and target.resolve() != dest.resolve():
                        raise _SnapshotError(
                            f"snapshot member resolves outside dest: {m.name!r}"
                        )
                    if m.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                    else:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        f = tf.extractfile(m)
                        if f is None:
                            continue
                        data = f.read()
                        total_bytes += len(data)
                        if total_bytes > _SNAPSHOT_MAX_BYTES:
                            raise _SnapshotError(
                                f"extracted size exceeds ceiling "
                                f"{_SNAPSHOT_MAX_BYTES}"
                            )
                        with target.open("wb") as fp:
                            fp.write(data)
                    extracted += 1
                    if (time.monotonic() - last_emit) > 0.5:
                        op_progress.update_progress(
                            self.db_path, self.op_id,
                            stage_current=extracted,
                            stage_total=len(members),
                        )
                        last_emit = time.monotonic()
                op_progress.update_progress(
                    self.db_path, self.op_id,
                    stage_current=extracted, stage_total=len(members),
                    activity=f"Extracted {extracted} files ({total_bytes} bytes)",
                )
        except tarfile.TarError as e:
            raise _SnapshotError(f"tar extraction failed: {e}") from e
        # v1.23.67 (audit #14): require EVERY expected subtree's pages.json, not
        # just ANY subdir. Pre-fix a tarball truncated to only movies/ (tv_shows/
        # + movie_collections/ absent — an interrupted/edge-corrupt codeload that
        # still extracts without a TarError) passed the any()-subdir check, then
        # index('tv_shows') hit `pages.json missing → return [], 0` with
        # failed_pages=0 → the run reported SUCCESS having synced ZERO tv/
        # collection themes (silent fake-success; the v1.22.* media_types_seen
        # gate only stopped the catastrophic mass-drop, not the zero-sync). A
        # complete codeload always ships all three subtrees, so a missing one is
        # truncation: raise here → run_sync falls through to the git/remote tier
        # instead of trusting a partial catalog.
        missing = [
            sub for sub in allowed_subdirs
            if not (dest / sub / "pages.json").is_file()
        ]
        if missing:
            raise _SnapshotError(
                f"snapshot incomplete — missing pages.json for {missing} "
                f"(expected all of {list(allowed_subdirs)}); refusing a "
                f"truncated snapshot"
            )
        self.root = dest

    def _validate(self) -> None:
        """Smoke check: read pages.json + one per-item file from the
        movies tree. Confirm the JSON shape still has `youtube_theme_url`
        addressable on a record. Catches schema drift before we
        upsert thousands of malformed rows."""
        if self.root is None:
            raise _SnapshotError("validate before extract")
        movies_dir = self.root / "movies"
        pages = movies_dir / "pages.json"
        if not pages.is_file():
            raise _SnapshotError("snapshot missing movies/pages.json")
        try:
            meta = json.loads(pages.read_text())
        except (OSError, ValueError) as e:
            raise _SnapshotError(f"pages.json unreadable: {e}") from e
        if not isinstance(meta, dict) or "pages" not in meta:
            raise _SnapshotError("pages.json shape unexpected")
        # Read first page; sample one record.
        first_page = movies_dir / "all_page_1.json"
        if first_page.is_file():
            try:
                entries = json.loads(first_page.read_text())
            except (OSError, ValueError) as e:
                raise _SnapshotError(f"all_page_1.json unreadable: {e}") from e
            if entries and isinstance(entries, list):
                sample = entries[0]
                tmdb_id = sample.get("id")
                imdb_id = sample.get("imdb_id")
                rec = self._read_item("movies", imdb_id=imdb_id, tmdb_id=tmdb_id)
                if rec is None:
                    raise _SnapshotError(
                        f"sample item not resolvable in snapshot "
                        f"(tmdb={tmdb_id}, imdb={imdb_id})"
                    )
                # Don't require a value (may legitimately be null);
                # do require the key be present so the upsert path
                # can read it.
                if "youtube_theme_url" not in rec:
                    raise _SnapshotError(
                        "sample item missing youtube_theme_url field "
                        "(schema drift?)"
                    )

    # -- read-side, mirrors the remote helpers --

    def index(self, media_path: str) -> tuple[list[dict], int]:
        # v1.21.38: returns (items, failed_pages) — see _fetch_index. An
        # unreadable page leaves its items out of the walk; the caller
        # folds failed_pages into stats.errors to block the drop sweep.
        if self.root is None:
            raise _SnapshotError("index before acquire")
        media_dir = self.root / media_path
        pages_file = media_dir / "pages.json"
        if not pages_file.is_file():
            return [], 0
        try:
            meta = json.loads(pages_file.read_text())
        except (OSError, ValueError) as e:
            raise _SnapshotError(f"{pages_file}: {e}") from e
        total_pages = int(meta.get("pages", 0))
        items: list[dict] = []
        failed_pages = 0
        for n in range(1, total_pages + 1):
            page_file = media_dir / f"all_page_{n}.json"
            try:
                body = json.loads(page_file.read_text())
                # v1.22.29 (audit): same non-list/empty-body guard as the git/
                # remote _fetch_index — a truncated/empty all_page_N.json must
                # count as a failed page so index_incomplete suppresses the
                # drop sweep, not silently shrink the catalog.
                if not isinstance(body, list) or not body:
                    raise ValueError("page body empty or not a list")
                items.extend(body)
            except (OSError, ValueError) as e:
                failed_pages += 1
                log.warning("snapshot page %s unreadable: %s", page_file, e)
        return items, failed_pages

    def fetch_item(
        self, media_path: str, *,
        imdb_id: str | None, tmdb_id: int,
    ) -> dict | None:
        return self._read_item(media_path, imdb_id=imdb_id, tmdb_id=tmdb_id)

    def _read_item(
        self, media_path: str, *,
        imdb_id: str | None, tmdb_id: int | None,
    ) -> dict | None:
        if self.root is None:
            return None
        media_dir = self.root / media_path
        candidates = []
        if imdb_id:
            candidates.append(media_dir / "imdb" / f"{imdb_id}.json")
        if tmdb_id is not None:
            candidates.append(media_dir / "themoviedb" / f"{tmdb_id}.json")
        for path in candidates:
            try:
                if path.is_file():
                    return json.loads(path.read_text())
            except (OSError, ValueError) as e:
                log.debug("snapshot item %s unreadable: %s", path, e)
                continue
        return None


# ----- v1.13.0 Phase B: git differential -----
#
# Phase A pulls the full database tarball every sync regardless of how
# much actually changed. On a quiet day TDB might publish 5-50 changed
# items; we still re-walk all 5,178. Phase B keeps a bare git mirror
# of the LizardByte/ThemerrDB `database` branch via dulwich and
# fetches only the delta. First run shallow-clones (~30 MB pack);
# subsequent runs fetch ~tens-of-KB pack-deltas.
#
# Failure here is non-fatal — run_sync falls through to Phase A
# (snapshot tarball) and flags fallback_active accordingly. If the
# snapshot path also fails, the existing fallback continues to remote.


class _GitMirrorError(RuntimeError):
    """Git mirror acquisition failed (network, dulwich error, repo
    corruption). Caller should fall back to the snapshot/remote
    path."""


# Hard-coded ceilings on the bare repo. Catches a repo that has
# grown unreasonably large (fetched megabytes of binary blobs we
# don't actually want) before it fills disk.
_GIT_MIRROR_MAX_BYTES = 500 * 1024 * 1024
# Max number of changed paths we'll process in one run. Real branch
# changes are typically a handful per day; tens of thousands would be
# a re-import event we should fall back from rather than try to
# digest in one transaction. Phase A's snapshot path can replay the
# whole tree if we land in this case.
_GIT_MIRROR_MAX_CHANGES = 50_000


class _GitMirror:
    """Bare-git-repo view of the ThemerrDB `database` branch.

    Lifecycle:
      mirror = _GitMirror(db_path, op_id, repo_url, branch, cancel_check)
      mirror.acquire(client)        # clone (first run) or fetch (subsequent)
      if mirror.is_unchanged():
          # remote HEAD didn't move; skip upsert, run prune only.
          ...
      else:
          changes = mirror.list_changes()    # ChangeSet
          for media_type, path in changes.changed:
              record = mirror.read_json(path)
              ...
          mirror.commit_sync_ok()           # advance MOTIF_LAST_SYNC

    The repo lives at <db_dir>/cache/themerrdb-database.git/. Last-
    known-synced HEAD is persisted at .../MOTIF_LAST_SYNC alongside
    the repo files (single-line text: 40-char hex SHA). On any
    inconsistency (missing file, SHA not present in repo) we treat
    the next acquire as a first run so we can re-establish a known
    baseline.
    """

    def __init__(
        self,
        db_path: Path,
        op_id: str,
        repo_url: str,
        branch: str,
        cancel_check,
    ):
        self.db_path = Path(db_path)
        self.op_id = op_id
        self.repo_url = repo_url
        self.branch = branch
        self.branch_ref = f"refs/heads/{branch}".encode()
        self._cancel_check = cancel_check
        self.cache_dir = self.db_path.parent / "cache"
        self.repo_path = self.cache_dir / "themerrdb-database.git"
        self.last_sync_path = self.repo_path / "MOTIF_LAST_SYNC"
        # Populated by acquire().
        self._repo = None  # type: ignore[assignment]
        self._old_head: bytes | None = None
        self._new_head: bytes | None = None
        self._unchanged = False
        # v1.20.21 (sync audit S4): memoize the computed _ChangeSet.
        # list_changes() runs the full dulwich tree_changes() diff and
        # is called TWICE per git sync (the upsert pass + the drop-
        # detection pass) on this same per-run instance. The heads are
        # fixed after acquire(), so the diff is deterministic — cache it
        # so the second caller doesn't re-walk the object store.
        self._changeset_cache = None
        # v1.23.27: memoize TREE objects across read_json calls. The walk
        # re-resolves each changed path from the root tree; ThemerrDB's big
        # shared directory trees (themoviedb/movie ≈ thousands of entries)
        # were re-decompressed once PER changed path — the dominant cost of a
        # large diff (the user's 10,420-path apply). dulwich doesn't cache
        # decompressed objects, so a tiny per-sha dict turns N re-decompresses
        # of the same dir tree into one. Heads are fixed post-acquire(), so no
        # invalidation; blobs (unique per path) are NOT cached.
        self._tree_cache: dict[bytes, object] = {}

    # -- public API --

    def is_unchanged(self) -> bool:
        return self._unchanged

    def is_baseline_reset(self) -> bool:
        # v0.51.14 (audit #4): True on a first-run / baseline-reset walk (no
        # prior MOTIF_LAST_SYNC to diff against) — list_changes() then returns
        # the WHOLE tree as 'added' with removed=[], so the changeset drop
        # detector is structurally blind; run_sync routes those runs to the
        # last_seen-based full-walk detector instead.
        return self._old_head is None

    def baseline_key(self) -> str:
        # v0.51.14 (audit #5): stable identity of the diff baseline, for the
        # chronic-read-failure escape (compare failures across runs pinned at
        # the SAME baseline).
        h = self._old_head
        return h.decode("ascii", "replace") if isinstance(h, bytes) else str(h)

    def acquire(self, _client_unused=None) -> None:
        """Clone (first run) or fetch (subsequent). On fetch with no
        new commits, sets the _unchanged flag (read via is_unchanged()).

        The httpx client param is accepted-but-ignored for parity with
        _DatabaseSnapshot.acquire's signature; dulwich uses its own
        HTTP transport.

        Raises _GitMirrorError on any failure that should trigger
        fallback to the snapshot path.
        """
        from .worker import _JobCancelled
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            if self._repo_is_initialized():
                self._fetch()
            else:
                self._clone()
            self._post_acquire_validate()
        except _JobCancelled:
            self._close()
            raise
        except _GitMirrorError:
            self._close()
            raise
        except Exception as e:
            self._close()
            raise _GitMirrorError(
                f"git mirror acquire failed: {type(e).__name__}: {e}"
            ) from e

    def list_changes(self):
        """Compute the diff between MOTIF_LAST_SYNC (or empty) and
        new HEAD. Returns _ChangeSet with three lists of relative
        paths under the branch root.

        On a first run (no prior MOTIF_LAST_SYNC), returns every
        path in the new tree as 'added' — caller treats it as a
        full-index walk equivalent to Phase A.
        """
        from dulwich.diff_tree import tree_changes
        if self._repo is None or self._new_head is None:
            raise _GitMirrorError("list_changes before acquire")
        # v1.20.21 (S4): return the memoized diff on the second caller
        # (drop-detection) so the tree_changes() walk runs once per sync.
        if self._changeset_cache is not None:
            return self._changeset_cache
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage="git_diff", stage_label="Computing git diff",
            stage_current=0, stage_total=0,
            activity=f"diff {self._summary_commit(self._old_head)}"
                     f" → {self._summary_commit(self._new_head)}",
        )
        new_commit = self._repo[self._new_head]
        if self._old_head is None:
            # First run — every path is "new". Walk the new tree to
            # collect all blob paths so the upsert path knows what
            # to read. Same scope as Phase A's full-index walk.
            self._changeset_cache = _ChangeSet(
                added=self._walk_tree_paths(new_commit.tree),
                modified=[],
                removed=[],
            )
            return self._changeset_cache
        old_commit = self._repo[self._old_head]
        added: list[str] = []
        modified: list[str] = []
        removed: list[str] = []
        n = 0
        for ch in tree_changes(self._repo.object_store, old_commit.tree, new_commit.tree):
            if n >= _GIT_MIRROR_MAX_CHANGES:
                # v1.15.36: clarify "AT LEAST N" wording. The
                # iterator break-out means we know we've hit the
                # threshold but the actual total is unknown
                # (could be 50K+1 or 5M); pre-fix `> N` read like
                # we'd counted past the threshold which we
                # haven't. Operator can now tell "this is a
                # safety bail at exactly N, not a measured
                # count" from the error string.
                raise _GitMirrorError(
                    f"git diff produced AT LEAST "
                    f"{_GIT_MIRROR_MAX_CHANGES} changed paths "
                    f"(threshold reached, actual total unknown) "
                    f"— falling back"
                )
            n += 1
            old_path = (ch.old.path.decode() if ch.old and ch.old.path else None)
            new_path = (ch.new.path.decode() if ch.new and ch.new.path else None)
            if ch.type == "add":
                if new_path:
                    added.append(new_path)
            elif ch.type == "modify":
                if new_path:
                    modified.append(new_path)
            elif ch.type == "delete":
                if old_path:
                    removed.append(old_path)
            elif ch.type == "rename":
                # Treat rename as remove + add for our purposes.
                if old_path:
                    removed.append(old_path)
                if new_path:
                    added.append(new_path)
        self._changeset_cache = _ChangeSet(
            added=added, modified=modified, removed=removed)
        return self._changeset_cache

    def _cached_obj(self, sha: bytes):
        """v1.23.27: fetch an object, memoizing TREE objects by sha. Trees are
        shared across every path under a directory, so caching them turns the
        big themoviedb/movie tree from N decompresses (one per changed path)
        into one. Blobs are leaf data read once per path — caching them would
        bloat memory with no reuse, so they fall through uncached. Raises
        KeyError exactly like self._repo[sha] when the object is absent."""
        obj = self._tree_cache.get(sha)
        if obj is not None:
            return obj
        obj = self._repo[sha]
        from dulwich.objects import Tree as DTree
        if isinstance(obj, DTree):
            self._tree_cache[sha] = obj
        return obj

    def read_json(self, rel_path: str) -> dict | None:
        """Read a file from the new HEAD's tree, parse as JSON.
        Returns None if the path doesn't exist in the tree (caller
        should treat as "ThemerrDB no longer publishes this item")."""
        from dulwich.objects import Tree as DTree
        if self._repo is None or self._new_head is None:
            return None
        commit = self._repo[self._new_head]
        try:
            # v1.23.27: cached — the root + intermediate dir trees are shared
            # by every changed path, so decompress each once per run.
            cur = self._cached_obj(commit.tree)
        except KeyError:
            return None
        for part in rel_path.encode().split(b"/"):
            if not isinstance(cur, DTree):
                return None
            try:
                _mode, sha = cur[part]
            except KeyError:
                return None
            cur = self._cached_obj(sha)
        if isinstance(cur, DTree):
            return None
        try:
            return json.loads(cur.as_raw_string())
        except (ValueError, AttributeError) as e:
            # v1.17.11: class-9 fix — pre-fix the silent `return
            # None` was indistinguishable from "tree path missing"
            # (lines 1544/1551), so a malformed JSON blob in
            # ThemerrDB silently dropped the record per sync.
            # Once-per-process warn + debug after, same pattern as
            # normalize_title in slow_path_upsert above.
            global _GIT_MIRROR_READ_JSON_WARNED
            if not _GIT_MIRROR_READ_JSON_WARNED:
                log.warning(
                    "_GitMirror.read_json: malformed blob at %r "
                    "(%s: %s) — returning None. Subsequent decode "
                    "failures in this process stay at debug.",
                    rel_path, type(e).__name__, e,
                )
                _GIT_MIRROR_READ_JSON_WARNED = True
            else:
                log.debug(
                    "_GitMirror.read_json: malformed blob at %r: %s",
                    rel_path, e,
                )
            return None

    def path_exists_in_tree(self, rel_path: str) -> bool:
        """v1.24.0: does a FILE blob exist at rel_path in the new HEAD's
        tree, regardless of whether it parses as JSON?

        Drop detection must fail SAFE. read_json returns None for BOTH a
        missing tree path AND a present-but-malformed blob (the
        _GIT_MIRROR_READ_JSON_WARNED path above), so probing survivorship
        with `read_json(...) is not None` reads a corrupt-but-present
        ThemerrDB entry as 'gone' → stamps a false tdb_dropped_at +
        deletes the pending_update, and commit_sync_ok advances the
        baseline so it never self-corrects. This probes object EXISTENCE
        only — a survivor with a temporarily-broken blob counts as
        present, never as dropped."""
        from dulwich.objects import Tree as DTree
        if self._repo is None or self._new_head is None:
            return False
        commit = self._repo[self._new_head]
        try:
            cur = self._cached_obj(commit.tree)
        except KeyError:
            return False
        for part in rel_path.encode().split(b"/"):
            if not isinstance(cur, DTree):
                return False
            try:
                _mode, sha = cur[part]
            except KeyError:
                return False
            try:
                cur = self._cached_obj(sha)
            except KeyError:
                return False
        # A tree at the leaf path is a directory, not a theme file.
        return not isinstance(cur, DTree)

    def commit_sync_ok(self) -> None:
        """Persist the new HEAD as MOTIF_LAST_SYNC and advance the
        local tracking ref atomically. Call only after the upsert
        pipeline completes successfully — committing too early would
        either skip items (if MOTIF_LAST_SYNC advances) or
        misclassify the next run as is_unchanged (if the ref
        advances but MOTIF_LAST_SYNC doesn't and the prior SHA gets
        GC'd from the shallow object store).

        v1.13.6: ref-write moved here from _fetch so a crash mid-
        pipeline leaves MOTIF_LAST_SYNC, the local ref, AND the
        previous successful HEAD all in sync. The new objects from
        the fetch are kept in dulwich's object store regardless of
        ref state, so list_changes / read_json continue to work for
        the rest of THIS run.
        """
        if self._new_head is None:
            return
        # Advance the local ref first so the file write below is the
        # last commit-point — if writing the file fails the ref still
        # advances, and on the next run _read_last_sync()'s soft
        # fallback to the ref (line ~1488) keeps motif behavior
        # equivalent to a successful write.
        if self._repo is not None:
            try:
                self._repo.refs[self.branch_ref] = self._new_head
            except Exception as e:
                log.warning(
                    "git mirror: ref %s write failed: %s",
                    self.branch_ref, e)
        try:
            self.last_sync_path.write_text(self._new_head.decode())
        except OSError as e:
            log.warning("git mirror: MOTIF_LAST_SYNC write failed: %s", e)

    def release(self) -> None:
        """Close repo handles. Repo files stay on disk — they're
        the cache for the next sync."""
        self._close()

    def _close(self) -> None:
        if self._repo is not None:
            try:
                self._repo.close()
            except Exception as e:
                # v1.15.35: log close failures at WARNING. Pre-fix
                # bare `pass` hid corrupted-HEAD / permission-denied
                # close errors; the repo would be left half-closed
                # and subsequent _clone() calls could see stale
                # state in obscure ways. Surfacing the close error
                # lets the operator correlate later sync failures
                # with the upstream cleanup miss.
                log.warning("git mirror close failed: %s", e)
            self._repo = None

    # v1.14.98: re-clone threshold for opportunistic compaction.
    # Fresh ThemerrDB clone is ~30 MB (dulwich's depth=1). the user's
    # repro hit 334 MB before this fix — months of orphaned packs
    # accumulating from daily fetches. 150 MB gives ~5x headroom
    # over the typical fresh size before triggering, so the
    # compaction fires roughly once per quarter on a healthy
    # install + much sooner on a runaway one.
    _COMPACT_THRESHOLD_BYTES = 150 * 1024 * 1024

    def _compact_if_oversized(self) -> None:
        """v1.14.98: drop and re-clone the mirror when it grows
        past _COMPACT_THRESHOLD_BYTES. Safe whenever the baseline ref
        (MOTIF_LAST_SYNC) has advanced to the new HEAD: the _unchanged
        path (old == new), OR — v1.23.80 — after a changed sync's
        commit_sync_ok() retired the old HEAD. NOT safe mid-diff (the
        re-clone drops the old HEAD the diff still needs).

        Background: shallow fetch with depth=1 keeps each fetch's
        pack file alongside the prior packs. Old objects become
        unreachable as the shallow line advances, but dulwich has
        no reliable gc API. Re-cloning produces a single pack with
        only the current HEAD's objects.

        MOTIF_LAST_SYNC is preserved across the re-clone so the
        next sync's `old_head == new_head` short-circuit still
        fires (we just verified _unchanged so the SHA matches).
        """
        try:
            total = sum(
                f.stat().st_size
                for f in self.repo_path.rglob("*")
                if f.is_file()
            )
        except OSError as e:
            log.warning("git mirror size check failed: %s", e)
            return
        if total <= self._COMPACT_THRESHOLD_BYTES:
            return
        log.info(
            "git mirror at %s grew to %.0f MB; compacting via re-clone",
            self.repo_path, total / (1024 * 1024),
        )
        # Preserve MOTIF_LAST_SYNC so the next sync's fast-path
        # short-circuit still fires.
        last_sync_bytes: bytes | None = None
        try:
            if self.last_sync_path.is_file():
                last_sync_bytes = self.last_sync_path.read_bytes()
        except OSError as e:
            log.warning(
                "could not preserve MOTIF_LAST_SYNC for compaction: %s", e,
            )
        self._close()
        import shutil
        # v1.15.34: drop ignore_errors=True so rmtree failures
        # surface in the logs. Pre-fix the silent absorb meant
        # "compaction succeeded" was reported even when the old
        # mirror directory stayed on disk (in-use by another
        # process, permissions, broken symlink). Operator saw
        # the success log but disk free didn't budge — silent
        # storage bloat. Now: try to remove, log + degrade
        # gracefully on failure (the re-clone below still tries,
        # which may collide with the leftover dir but at least
        # the operator can see what happened).
        try:
            shutil.rmtree(self.repo_path)
        except OSError as e:
            log.warning(
                "git mirror compaction: rmtree %s failed: %s "
                "(old mirror dir may persist on disk; check "
                "permissions / running processes)",
                self.repo_path, e,
            )
        # Re-clone fresh — same shape as _clone().
        from dulwich.porcelain import clone
        try:
            clone(
                self.repo_url,
                str(self.repo_path),
                bare=True,
                branch=self.branch.encode(),
                depth=1,
            )
        except Exception as e:
            log.warning(
                "git mirror re-clone after compaction failed: %s; "
                "next sync will treat as first-run", e,
            )
            return
        if last_sync_bytes is not None:
            try:
                self.last_sync_path.write_bytes(last_sync_bytes)
            except OSError as e:
                log.warning(
                    "could not restore MOTIF_LAST_SYNC after compaction: %s",
                    e,
                )
        self._open_repo()
        try:
            new_total = sum(
                f.stat().st_size
                for f in self.repo_path.rglob("*")
                if f.is_file()
            )
            log.info(
                "git mirror compacted: %.0f MB → %.0f MB",
                total / (1024 * 1024), new_total / (1024 * 1024),
            )
        except OSError as e:
            # v1.17.1 (class 9): pre-fix this swallow hid disk-full /
            # permission failures during opportunistic mirror
            # compact. The op isn't critical (next sync still
            # works without compaction), but the operator wants
            # to know if /config is running out of inodes/space —
            # log.debug surfaces it without spamming at INFO.
            log.debug("git mirror compact suppressed: %s", e)

    # -- stages --

    def _repo_is_initialized(self) -> bool:
        return (self.repo_path / "config").is_file() or (self.repo_path / "HEAD").is_file()

    def _clone(self) -> None:
        from dulwich.porcelain import clone
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage="git_fetch",
            stage_label="Cloning ThemerrDB mirror (first run)",
            stage_current=0, stage_total=0,
            activity=_redact_url_credentials(  # v0.51.75: redact URL creds
                f"git clone --bare --depth 1 -b {self.branch} {self.repo_url}"),
        )
        # Wipe any partial state from a previous failed clone so we
        # don't try to fetch into a half-initialized dir.
        if self.repo_path.exists():
            import shutil
            shutil.rmtree(self.repo_path, ignore_errors=True)
        # Shallow clone keeps history bounded. dulwich's clone is
        # synchronous; cancel-check fires after it returns.
        clone(
            self.repo_url,
            str(self.repo_path),
            bare=True,
            branch=self.branch.encode(),
            depth=1,
        )
        if self._cancel_check():
            from .worker import _JobCancelled
            raise _JobCancelled()
        self._open_repo()
        self._old_head = None  # first run: no previous baseline
        new_head = self._repo.refs.as_dict().get(self.branch_ref)
        if new_head is None:
            raise _GitMirrorError(
                f"clone succeeded but branch {self.branch!r} ref missing"
            )
        self._new_head = new_head
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage_current=1, stage_total=1,
            activity=f"Cloned {self.branch} at "
                     f"{self._summary_commit(new_head)}",
        )

    def _fetch(self) -> None:
        from dulwich.porcelain import fetch
        import re as _re
        import time as _t
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage="git_fetch",
            stage_label="Fetching ThemerrDB delta",
            stage_current=0, stage_total=0,
            activity=_redact_url_credentials(f"git fetch {self.repo_url}"),  # v0.51.75: redact URL creds
        )
        self._open_repo()
        # v1.13.19: parse dulwich's sideband progress so the topbar
        # mini-bar fills with a real % during the fetch instead of
        # sitting at indeterminate-shimmer for ~10s of "is this even
        # working?". Dulwich's progress callback receives byte chunks
        # like b"Receiving objects: 42% (1000/2380)\r"; we extract
        # the (current, total) pair from the "(N/M)" suffix and
        # rate-limit emits to ~5/sec so we don't hammer op_progress.
        _last_emit = [0.0]
        _progress_re = _re.compile(
            rb"(Receiving objects|Resolving deltas|Counting objects|"
            rb"Compressing objects):\s*\d+%?\s*\((\d+)/(\d+)\)"
        )
        _phase_label = {
            b"Counting objects":   "counting",
            b"Compressing objects": "compressing",
            b"Receiving objects":  "receiving",
            b"Resolving deltas":   "resolving",
        }

        def _on_fetch_progress(chunk: bytes) -> None:
            # v1.13.29: buffer leftover bytes between calls. Pre-fix
            # dulwich's chunk boundaries could split a sample mid-line
            # ("Receiving objects: 95% (1000/" + "2380)\r") — neither
            # piece matched the regex and the sample dropped. The
            # final cur==total emit landing on a boundary was the
            # most user-visible miss (bar stuck near 95% until the
            # next stage took over). Buffer holds the unterminated
            # tail; on the next chunk we prepend before splitting.
            try:
                buf = _on_fetch_progress._buf  # type: ignore[attr-defined]
            except AttributeError:
                buf = b""
            data = buf + chunk
            pieces = data.split(b"\r")
            # Last piece is the (possibly empty) trailing fragment
            # after the last \r — keep it for the next call.
            _on_fetch_progress._buf = pieces[-1]  # type: ignore[attr-defined]
            try:
                for piece in pieces[:-1]:
                    m = _progress_re.search(piece)
                    if not m:
                        continue
                    phase_b = m.group(1)
                    cur = int(m.group(2))
                    total = int(m.group(3))
                    if total <= 0:
                        continue
                    now = _t.monotonic()
                    # Always emit on the final 100% sample; otherwise
                    # rate-limit to ~5 emits/sec.
                    if cur < total and (now - _last_emit[0]) < 0.2:
                        continue
                    _last_emit[0] = now
                    op_progress.update_progress(
                        self.db_path, self.op_id,
                        stage="git_fetch",
                        stage_label=(f"Fetching ThemerrDB delta "
                                     f"({_phase_label.get(phase_b, '…')})"),
                        stage_current=cur,
                        stage_total=total,
                    )
            except Exception as e:
                # A parse hiccup must never fail the fetch.
                # v1.15.35: log at DEBUG with phase context. Pre-fix
                # bare `pass` hid progress-meter parse failures —
                # the operator saw the bar appear stuck mid-fetch
                # and may have force-killed the sync. Now the
                # debug log gives diagnostic context if the operator
                # turns DEBUG on to investigate "frozen progress".
                log.debug(
                    "git fetch: progress parse hiccup (phase=%r): %s",
                    phase_b, e,
                )

        # Read prior HEAD from MOTIF_LAST_SYNC. Missing or out-of-store
        # → treat as first run (old_head=None) so list_changes returns
        # the full tree as 'added'. This is also the crash-recovery
        # path: if a prior run advanced the local ref via fetch but
        # crashed before commit_sync_ok wrote MOTIF_LAST_SYNC, we
        # must NOT diff from the ref (would give is_unchanged on the
        # next run with no new commits, silently dropping the
        # never-processed delta). Pre-v1.13.6 the soft fallback to
        # refs/heads/<branch> did exactly that — replaced now with a
        # full-walk recovery so the crashed-mid-pipeline state can be
        # safely retried.
        old_head = self._read_last_sync()
        self._old_head = old_head
        # The actual fetch.
        try:
            res = fetch(str(self.repo_path), self.repo_url, depth=1,
                        progress=_on_fetch_progress)
        except TypeError as e:
            # v1.13.29: only fall back when the TypeError is about the
            # `progress` kwarg specifically. Older dulwich (pre-0.21)
            # didn't accept it and raised "unexpected keyword argument".
            # Catching any TypeError masked dulwich-internal errors
            # (e.g., bytes/str confusion in fetch) as "old dulwich" and
            # silently retried, which would just fail again less
            # informatively.
            if "progress" not in str(e):
                raise _GitMirrorError(f"dulwich fetch failed: {e}") from e
            try:
                res = fetch(str(self.repo_path), self.repo_url, depth=1)
            except Exception as e2:
                raise _GitMirrorError(f"dulwich fetch failed: {e2}") from e2
        except Exception as e:
            raise _GitMirrorError(f"dulwich fetch failed: {e}") from e
        if self._cancel_check():
            from .worker import _JobCancelled
            raise _JobCancelled()
        new_head = res.refs.get(self.branch_ref)
        if new_head is None:
            raise _GitMirrorError(
                f"fetch did not return branch {self.branch!r}; refs="
                f"{list(res.refs.keys())[:5]}"
            )
        # v1.13.6: do NOT advance the local tracking ref here. Wait for
        # commit_sync_ok() to write both MOTIF_LAST_SYNC and the ref
        # atomically, so a crash between fetch and upsert keeps both
        # at the prior good SHA. Pre-fix the ref advanced immediately
        # but MOTIF_LAST_SYNC didn't; on the next run _read_last_sync()
        # would fail (the prior SHA was GC'd from the shallow store)
        # and fall back to the now-advanced ref, treating the
        # unprocessed delta as is_unchanged and silently dropping it.
        # The new objects from this fetch are kept in the object store
        # by dulwich whether or not the ref points at them, so
        # list_changes / read_json work fine for the rest of this run.
        self._new_head = new_head
        if old_head is not None and old_head == new_head:
            # No new commits. is_unchanged() True; caller skips upsert.
            self._unchanged = True
            op_progress.update_progress(
                self.db_path, self.op_id,
                activity=f"No new commits since {self._summary_commit(old_head)}",
            )
            # v1.14.98: opportunistic compaction on the no-changes
            # path. Shallow fetches leave orphaned pack files
            # behind (dulwich has no reliable gc API), so the
            # mirror grows monotonically over time — the user's
            # repro: 334 MB after months of daily syncs. Compacting
            # here is safe because there's no delta to process from
            # this fetch.
            self._compact_if_oversized()
            return
        op_progress.update_progress(
            self.db_path, self.op_id,
            stage_current=1, stage_total=1,
            activity=f"Fetched {self._summary_commit(old_head) if old_head else 'first run'}"
                     f" → {self._summary_commit(new_head)}",
        )

    def _post_acquire_validate(self) -> None:
        """Defense in depth: confirm the new HEAD's tree carries the
        expected media subdirs (movies/, tv_shows/) before we let
        list_changes loose. Catches a branch that's been renamed or
        repurposed without breaking the sync silently."""
        if self._new_head is None or self._unchanged:
            return
        from dulwich.objects import Tree as DTree
        try:
            commit = self._repo[self._new_head]
            tree = self._repo[commit.tree]
        except KeyError as e:
            raise _GitMirrorError(f"new HEAD tree missing: {e}") from e
        if not isinstance(tree, DTree):
            raise _GitMirrorError("new HEAD tree is not a tree object")
        names = {entry.path for entry in tree.iteritems()}
        if b"movies" not in names and b"tv_shows" not in names:
            raise _GitMirrorError(
                "branch HEAD missing both movies/ and tv_shows/ — "
                "schema drift or wrong branch"
            )

    # -- helpers --

    def _open_repo(self) -> None:
        from dulwich.repo import Repo
        if self._repo is None:
            self._repo = Repo(str(self.repo_path))

    def _read_last_sync(self) -> bytes | None:
        if not self.last_sync_path.is_file():
            return None
        try:
            txt = self.last_sync_path.read_text().strip()
        except OSError as e:
            # v1.13.10 (#16): an unreadable MOTIF_LAST_SYNC silently
            # triggers full-walk recovery, which is correct behavior
            # but invisible to the operator. Surface it once at WARN
            # so a corrupt FS / permissions issue is debuggable.
            log.warning(
                "git mirror: MOTIF_LAST_SYNC unreadable (%s) — "
                "falling back to full-walk recovery", e)
            return None
        if not txt:
            log.warning(
                "git mirror: MOTIF_LAST_SYNC is empty — falling back "
                "to full-walk recovery")
            return None
        if len(txt) != 40 or any(c not in "0123456789abcdef" for c in txt.lower()):
            log.warning(
                "git mirror: MOTIF_LAST_SYNC contents %r are not a "
                "valid SHA — falling back to full-walk recovery",
                txt[:64])
            return None
        sha = txt.encode()
        # Verify the SHA is actually present in the repo. A leftover
        # MOTIF_LAST_SYNC from a partially-deleted cache would
        # otherwise cause diff_tree to KeyError.
        try:
            _ = self._repo[sha]
        except KeyError:
            log.warning(
                "git mirror: MOTIF_LAST_SYNC %s missing from repo "
                "object store — treating as first run", txt[:8])
            return None
        return sha

    def _walk_tree_paths(self, tree_sha: bytes, prefix: bytes = b"") -> list[str]:
        """Yield every blob path under the tree, relative to the
        branch root. Used on first-run when there's no diff
        baseline."""
        from dulwich.objects import Tree as DTree
        out: list[str] = []
        try:
            tree = self._repo[tree_sha]
        except KeyError:
            return out
        if not isinstance(tree, DTree):
            return out
        for entry in tree.iteritems():
            full = (prefix + b"/" + entry.path) if prefix else entry.path
            obj = self._repo[entry.sha]
            if isinstance(obj, DTree):
                out.extend(self._walk_tree_paths(entry.sha, full))
            else:
                out.append(full.decode())
        return out

    @staticmethod
    def _summary_sha(sha: bytes | None) -> str:
        if sha is None:
            return "(none)"
        return sha[:8].decode() if isinstance(sha, bytes) else str(sha)[:8]

    def _summary_commit(self, sha: bytes | None) -> str:
        """v1.13.26: human-readable commit summary for the activity
        feed and any other surface where the SHA was previously shown
        raw. Returns 'YYYY-MM-DD HH:MM UTC' when the commit can be
        loaded from the local mirror, falling back to the 8-char
        SHA when the object isn't available (e.g., MOTIF_LAST_SYNC
        pointing at a pruned object). Pre-fix activity messages
        like 'No new commits since 06979829' showed raw SHAs that
        were meaningless to users; now they read 'No new commits
        since 2026-05-07 14:30 UTC' (drawer info, dashboard sync
        history, settings page).
        """
        if sha is None:
            return "(none)"
        try:
            commit = self._repo[sha]
            from datetime import datetime as _dt, timezone as _tz
            t = _dt.fromtimestamp(commit.commit_time, tz=_tz.utc)
            return t.strftime("%Y-%m-%d %H:%M UTC")
        except (KeyError, AttributeError, TypeError, ValueError):
            return self._summary_sha(sha)


@dataclass
class _ChangeSet:
    """Three lists of repo-relative paths produced by _GitMirror.list_changes."""
    added: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)

    @property
    def changed(self) -> list[str]:
        """Convenience: paths that need to be re-read (added + modified)."""
        return self.added + self.modified

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.modified or self.removed)

    @property
    def total(self) -> int:
        return len(self.added) + len(self.modified) + len(self.removed)


_GIT_PATH_UNCLASSIFIED_WARNED = False


def _classify_git_path(rel_path: str) -> tuple[str, str | None, int | None] | None:
    """Translate a `database`-branch-relative path into the
    (media_type, imdb_id, tmdb_id) it represents. Returns None for
    paths we don't process (pages.json, all_page_*.json, README, etc.).

    v1.13.0 (Phase B): only the per-item JSON files under
    movies/{imdb,themoviedb}/ and tv_shows/{imdb,themoviedb}/ feed
    the upsert path. The pages.json + all_page_*.json files are
    Phase A's index-walk artifacts; we don't need them in the
    differential path.

    v1.18.0: movie_collections/themoviedb/<tmdb_collection_id>.json
    also feeds the upsert path with media_type='collection'.
    Collections have NO imdb/ subtree — TMDB collection IDs are
    the only identifier space ThemerrDB tracks for them.
    """
    parts = rel_path.split("/")
    if len(parts) != 3:
        return None
    top, kind, leaf = parts
    if not leaf.endswith(".json"):
        return None
    if top == "movies":
        media_type = "movie"
    elif top == "tv_shows":
        media_type = "tv"
    elif top == "movie_collections":
        # v1.18.0: collections only have themoviedb/ keyed records.
        # An imdb/ subtree path (if ever introduced upstream) would
        # be ignored — fall through to the kind=='imdb' branch
        # returning the imdb_id, but the caller's downstream code
        # would treat it as a missing-tmdb error.
        media_type = "collection"
    else:
        return None
    stem = leaf[:-5]  # strip .json
    if kind == "imdb":
        return (media_type, stem, None)
    if kind == "themoviedb":
        try:
            return (media_type, None, int(stem))
        except ValueError:
            pass  # non-int leaf under a known tree — breadcrumb below
    # v1.21.43 (audit L3): a path with a KNOWN top-level dir + a .json
    # leaf but an unrecognized `kind` subdir (or a non-int themoviedb
    # leaf) looks like a per-item record we couldn't parse. A TDB
    # database-branch layout restructure would land EVERY item path here,
    # and the differential sync would silently report 0-new/0-updated —
    # a quiet upstream day is indistinguishable from a broken classifier.
    # Warn once (then debug — hot path) so the operator sees it. Truly-
    # unrelated paths (README, pages.json, an unknown TOP-level dir)
    # returned None earlier WITHOUT this breadcrumb — those are expected.
    global _GIT_PATH_UNCLASSIFIED_WARNED
    if not _GIT_PATH_UNCLASSIFIED_WARNED:
        _GIT_PATH_UNCLASSIFIED_WARNED = True
        log.warning(
            "git path %r: known top-level dir but unrecognized kind/leaf "
            "— skipping. TDB database-branch layout may have changed; "
            "differential sync will under-apply. Further occurrences log "
            "at debug.", rel_path,
        )
    else:
        log.debug(
            "git path %r unclassified (known top, unknown kind/leaf)",
            rel_path,
        )
    return None


def _run_git_differential_upsert(
    db_path,
    git_mirror: "_GitMirror",
    stats: SyncStats,
    *,
    sync_ts: str,
    enqueue_downloads: bool,
    auto_place_override: bool | None,
    auto_download_new_themes: bool,
    cancel_check,
    errored_by_mt: dict[str, set[int]] | None = None,
) -> tuple[list[str], int]:
    """Walk the git mirror's change set, read each changed item's
    JSON from the new HEAD's tree, and feed (media_type, tmdb_id,
    record, upstream_source) tuples into _flush_sync_batch — the
    same plumbing Phase A's full-index walk uses, but scoped to the
    delta. Updates stats.movies_seen / tv_seen / new_count /
    updated_count / errors as it goes.

    Removes are NOT processed yet — Phase A's "items in DB that
    didn't appear in this sync are NOT deleted" rule still holds;
    a true tdb-removed UI affordance is Phase C.

    v1.22.74: no longer advances MOTIF_LAST_SYNC here — run_sync
    commits the baseline AFTER drop detection consumed the changeset
    (pre-fix a detection failure lost the removed paths forever: the
    next run diffed from the NEW head and the drops never
    reappeared). The empty-changeset early return below still
    commits (nothing to lose). If the upsert raises we deliberately
    do NOT advance — the next sync re-processes the same delta,
    which is idempotent (themes upsert is keyed on
    (media_type, tmdb_id)).
    """
    changes = git_mirror.list_changes()
    op_progress.update_progress(
        db_path, "tdb_sync",
        stage="git_apply",
        stage_label=f"Applying {changes.total} changed paths",
        stage_current=0,
        stage_total=len(changes.changed),
        activity=(f"git diff: {len(changes.added)} added, "
                  f"{len(changes.modified)} modified, "
                  f"{len(changes.removed)} removed"),
    )
    if changes.is_empty:
        # Tree changed (e.g. a README edit) but no per-item paths
        # we care about. Skip upsert; commit so next run has the
        # latest baseline.
        git_mirror.commit_sync_ok()
        return [], 0

    # v0.51.14 (audit #4/#5): record WHICH paths fail to read. errored_by_mt
    # gets the resolved (media_type, tmdb_id) so drop detection can EXCLUDE
    # just those ids (the v1.21.44 per-item pattern — their last_seen wasn't
    # refreshed, so a last_seen-based sweep would false-drop them);
    # failed_paths + the unresolved count feed the chronic-pin escape and the
    # baseline-reset routing in run_sync.
    if errored_by_mt is None:
        errored_by_mt = {}
    failed_paths: list[str] = []
    unresolved_failures = 0

    def _note_read_failure(rel_path, media_type, imdb_id, tmdb_id) -> None:
        nonlocal unresolved_failures
        failed_paths.append(rel_path)
        resolved = tmdb_id
        if resolved is None and imdb_id is not None:
            with get_conn(db_path) as conn:
                row = conn.execute(
                    "SELECT tmdb_id FROM themes "
                    "WHERE media_type = ? AND imdb_id = ? "
                    "ORDER BY tmdb_id LIMIT 1",
                    (media_type, imdb_id),
                ).fetchone()
            resolved = row["tmdb_id"] if row else None
        if resolved is None:
            unresolved_failures += 1
        else:
            errored_by_mt.setdefault(media_type, set()).add(int(resolved))

    seen: set[tuple[str, int]] = set()
    batch: list[tuple[str, int, dict, str]] = []
    completed = 0
    last_ui = time.monotonic()
    total = len(changes.changed)
    # v1.23.80: per-phase timing so a slow apply names its own bottleneck —
    # git read_json (object reads) vs DB batch-flush vs the rest of the loop.
    # Logged once at the end. (the user's 9943-path / 4.5-min run had no
    # breakdown; this makes the next one self-explanatory.)
    _apply_t0 = time.monotonic()
    _t_read = 0.0
    _t_flush = 0.0
    for rel_path in changes.changed:
        if cancel_check():
            from .worker import _JobCancelled
            raise _JobCancelled()
        completed += 1
        classification = _classify_git_path(rel_path)
        if classification is None:
            continue
        media_type, imdb_id, tmdb_id = classification
        _r0 = time.monotonic()
        record = git_mirror.read_json(rel_path)
        _t_read += time.monotonic() - _r0
        if record is None:
            stats.errors += 1
            # v1.15.35: log the failed path so the operator can
            # tell network/parse failures from sync's stats.errors
            # counter being a meaningful aggregate vs a single
            # rotted file repeating. Pre-fix the counter ticked
            # silently with no per-item context.
            log.info(
                "sync git: read_json(%s) returned None — skipping",
                rel_path,
            )
            _note_read_failure(rel_path, media_type, imdb_id, tmdb_id)
            continue
        # The record's `id` field is the TMDB id regardless of
        # whether we landed via the imdb/ or themoviedb/ subtree.
        try:
            real_tmdb = int(tmdb_id if tmdb_id is not None
                            else record.get("id"))
        except (TypeError, ValueError) as e:
            stats.errors += 1
            log.info(
                "sync git: bad tmdb_id for %s (record id=%r): %s",
                rel_path, record.get("id"), e,
            )
            _note_read_failure(rel_path, media_type, imdb_id, tmdb_id)
            continue
        # Dedupe: a record can show up via both imdb and themoviedb
        # paths in the same commit (LizardByte mirrors the same
        # JSON). Process once.
        key = (media_type, real_tmdb)
        if key in seen:
            continue
        seen.add(key)
        if media_type == "movie":
            stats.movies_seen += 1
        elif media_type == "collection":
            # v1.18.0: collections counted in their own bucket so
            # the dashboard sparkline can break out collection
            # sync activity from the movies count.
            stats.collections_seen += 1
        else:
            stats.tv_seen += 1
        upstream_source = "imdb" if imdb_id else "themoviedb"
        batch.append((media_type, real_tmdb, record, upstream_source))
        if len(batch) >= _SYNC_BATCH:
            _f0 = time.monotonic()
            _flush_sync_batch(
                db_path, batch, sync_ts=sync_ts,
                enqueue_downloads=enqueue_downloads,
                auto_place_override=auto_place_override,
                auto_download_new_themes=auto_download_new_themes,
                stats=stats,
            )
            _t_flush += time.monotonic() - _f0
            batch.clear()
            time.sleep(0.05)  # yield writer lock briefly
        if (time.monotonic() - last_ui) > 0.3:
            last_ui = time.monotonic()
            op_progress.update_progress(
                db_path, "tdb_sync",
                stage_current=completed,
                stage_total=total,
                processed_total=(stats.movies_seen + stats.tv_seen
                                 + stats.collections_seen),
                error_count=stats.errors,
            )
    if batch:
        # v1.19.91: the final-flush call must pass auto_download_new_themes
        # too — its sibling at the >=_SYNC_BATCH boundary already did, but
        # this tail call was missed when v1.19.71 added the keyword-only
        # param. Crashed every git-differential sync run with a TypeError
        # (only the last partial batch hit it, so small-changeset syncs
        # under _SYNC_BATCH items failed on the FIRST flush).
        _f0 = time.monotonic()
        _flush_sync_batch(
            db_path, batch, sync_ts=sync_ts,
            enqueue_downloads=enqueue_downloads,
            auto_place_override=auto_place_override,
            auto_download_new_themes=auto_download_new_themes,
            stats=stats,
        )
        _t_flush += time.monotonic() - _f0
        batch.clear()
    # v1.23.80: name the bottleneck. read_json = git object reads (slow when
    # the mirror's packs are fragmented — see _compact_if_oversized); flush =
    # DB writes; the remainder is classify/dedup/progress.
    _apply_total = time.monotonic() - _apply_t0
    log.info(
        "sync apply timing: %.1fs total — read_json %.1fs (%d paths), "
        "DB flush %.1fs (%d unique items), loop/other %.1fs",
        _apply_total, _t_read, total, _t_flush, len(seen),
        max(0.0, _apply_total - _t_read - _t_flush),
    )
    op_progress.update_progress(
        db_path, "tdb_sync",
        stage_current=total, stage_total=total,
        processed_total=(stats.movies_seen + stats.tv_seen
                         + stats.collections_seen),
        error_count=stats.errors,
        activity=(f"Applied {len(seen)} unique items "
                  f"({stats.new_count} new, {stats.updated_count} updated)"),
    )
    # v1.22.74: baseline advance moved to run_sync, after drop detection.
    return failed_paths, unresolved_failures


# v1.13.1 (Phase C): drop detection. Catches items LizardByte
# stopped publishing — the per-item JSON disappears from the
# database branch. Sync stamps tdb_dropped_at; the UI surfaces a
# gray TDB◌ pill and ACK DROP / CONVERT TO MANUAL actions in the
# SOURCE menu so the user can decide what to do.
_DROP_SAFETY_PCT = 0.05
_DROP_SAFETY_FLOOR = 50  # never block when total drops are tiny


def _detect_and_stamp_drops_full_walk(
    db_path,
    *,
    sync_ts: str,
    media_types_seen: set[str],
    exclude_by_mt: dict[str, set[int]] | None = None,
) -> int:
    """Snapshot/remote-path drop detection. After a successful
    full-tree walk both transports update last_seen_sync_at on every
    upserted row; anything in themes whose last_seen_sync_at < this
    run's sync_ts AND upstream_source != 'plex_orphan' AND
    tdb_dropped_at IS NULL was implicitly removed upstream.

    Only stamps if the implied drop count stays within
    _DROP_SAFETY_PCT of the surviving catalog (per media_type).
    A LizardByte regression that mass-removes items would otherwise
    cascade: motif would mark everything as dropped, the user
    would either ignore (continuing to use stale data) or panic-
    purge (catastrophic). Bail loud, let the operator review.

    Returns the number of rows stamped across all media_types.
    """
    if not media_types_seen:
        return 0
    exclude_by_mt = exclude_by_mt or {}
    stamped_total = 0
    # v1.24.15 (holistic review): one transaction around the per-mt
    # tdb_dropped_at stamp + its companion pending_updates DELETE. Pre-fix the
    # autocommit connection committed each independently, so a crash between
    # them left a theme stamped-dropped with its now-moot pending_updates row
    # still present (the stale "ACCEPT pointing at a removed URL beside the gray
    # drop pill" state the v1.20.16 clear exists to prevent). Atomic now.
    with get_conn(db_path) as conn, transaction(conn):
        for mt in sorted(media_types_seen):
            # v1.21.44 (review of v1.21.38/H1): EXCLUDE items that errored
            # on their per-item fetch this run (in-index but unfetched →
            # last_seen_sync_at stale but NOT a confirmed drop). Excluding
            # them — rather than disabling the WHOLE sweep on any error
            # (the v1.21.38 permanent-starvation bug: a single chronically
            # -erroring item would skip drop detection forever) — keeps
            # detection working for genuinely-removed items (absent from
            # the index, never fetched, never errored → stamped here).
            excl = sorted(exclude_by_mt.get(mt) or ())
            excl_sql = (" AND tmdb_id NOT IN (%s)" % ",".join("?" * len(excl))
                        if excl else "")
            current_n = conn.execute(
                "SELECT COUNT(*) FROM themes "
                "WHERE media_type = ? AND upstream_source != 'plex_orphan'",
                (mt,),
            ).fetchone()[0] or 0
            implied_n = conn.execute(
                "SELECT COUNT(*) FROM themes "
                "WHERE media_type = ? "
                "  AND upstream_source != 'plex_orphan' "
                "  AND tdb_dropped_at IS NULL "
                "  AND last_seen_sync_at < ?" + excl_sql,
                (mt, sync_ts, *excl),
            ).fetchone()[0] or 0
            if implied_n == 0:
                continue
            cap = max(_DROP_SAFETY_FLOOR,
                      int(current_n * _DROP_SAFETY_PCT))
            if implied_n > cap:
                log.warning(
                    "drop detection: %s implied_drops=%d exceeds cap=%d "
                    "(catalog=%d) — skipping stamp; review upstream",
                    mt, implied_n, cap, current_n,
                )
                continue
            cur = conn.execute(
                "UPDATE themes SET tdb_dropped_at = ? "
                "WHERE media_type = ? "
                "  AND upstream_source != 'plex_orphan' "
                "  AND tdb_dropped_at IS NULL "
                "  AND last_seen_sync_at < ?" + excl_sql,
                (sync_ts, mt, sync_ts, *excl),
            )
            if cur.rowcount:
                # v1.20.16 (L1): a dropped title's pending update is moot
                # — TDB no longer publishes the theme — so clear it. Else
                # the row showed a stale, un-actionable ACCEPT pointing at
                # a removed URL ALONGSIDE the gray TDB◌ drop pill. Re-add
                # re-creates it (the v1.20.13 fast-path clears the drop
                # stamp; sync re-emits on a real change).
                conn.execute(
                    "DELETE FROM pending_updates WHERE media_type = ? "
                    "AND tmdb_id IN (SELECT tmdb_id FROM themes "
                    "  WHERE media_type = ? AND tdb_dropped_at = ?)",
                    (mt, mt, sync_ts),
                )
            stamped_total += cur.rowcount or 0
    return stamped_total


def _clear_url_less_pending_updates(db_path) -> int:
    """v1.22.8: clear undecided pending_updates that propose a TDB theme the
    upstream no longer has. The v1.20.16 cleanup at _detect_and_stamp_drops
    handles TITLE drops (tdb_dropped_at stamped). This complements it for the
    title-PRESENT-but-url-less case: TDB keeps publishing the title but removed
    its youtube_url, so an 'upstream_changed' / 'new_theme_available' pending
    points at a theme that's gone — surfacing as an un-actionable NEEDS WORK /
    !UPD prompt (the user's anime: Witch Hat Atelier, The Beginning After the End).

    urls_match is deliberately NOT swept — it's the legit 'your U == TDB' convert
    prompt and requires TDB to HAVE a url, so a url-less theme can't carry one.
    Safe (v1.18.10 amplifier-sweep rule): only undecided rows, only the two
    TDB-proposing kinds, gated on themes.youtube_url IS NULL — a POSITIVE
    condition that cannot amplify a broken/empty state the way a NOT-EXISTS
    sweep could."""
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM pending_updates "
            "WHERE decision = 'pending' "
            "  AND kind IN ('upstream_changed', 'new_theme_available') "
            "  AND EXISTS (SELECT 1 FROM themes t "
            "              WHERE t.media_type = pending_updates.media_type "
            "                AND t.tmdb_id = pending_updates.tmdb_id "
            "                AND t.youtube_url IS NULL)"
        )
        return cur.rowcount or 0


def _detect_and_stamp_drops_git(
    db_path,
    git_mirror: "_GitMirror",
    *,
    sync_ts: str,
) -> int:
    """Git-path drop detection. The change set's `removed` list is
    the authoritative signal — but a single (media_type, tmdb_id)
    can have BOTH an imdb/<id>.json AND a themoviedb/<id>.json path
    in the tree. Removal of one doesn't necessarily mean TDB
    stopped publishing the item — it might just be a re-key. Only
    stamp dropped if NO surviving path in the new tree references
    this (media_type, tmdb_id).

    Same 5%-of-catalog safety cap as the full-walk path. Returns
    the number of rows stamped.
    """
    changes = git_mirror.list_changes()
    if not changes.removed:
        return 0
    candidates: dict[tuple[str, int], dict] = {}
    for rel_path in changes.removed:
        cls = _classify_git_path(rel_path)
        if cls is None:
            continue
        media_type, imdb_id, tmdb_id = cls
        # When the removed path was an imdb-keyed file we don't yet
        # know the tmdb_id (the JSON is gone). Resolve from themes.
        if tmdb_id is None and imdb_id is not None:
            with get_conn(db_path) as conn:
                row = conn.execute(
                    # v1.20.16 (L2): ORDER BY for determinism — if two
                    # themes rows share an imdb_id (rare data anomaly),
                    # fetchone() otherwise picks an arbitrary tmdb_id.
                    "SELECT tmdb_id FROM themes "
                    "WHERE media_type = ? AND imdb_id = ? "
                    "ORDER BY tmdb_id LIMIT 1",
                    (media_type, imdb_id),
                ).fetchone()
            if row is None:
                # v1.20.21 (sync audit A1): breadcrumb the unresolved
                # removal instead of a silent continue. An imdb-keyed
                # path was removed upstream but no themes row links that
                # imdb_id (NULL/stale imdb_id, or the row was keyed via
                # themoviedb/), so this candidate is dropped from drop-
                # detection on the git path — a genuine drop here would
                # be MISSED (the full-walk path's last_seen signal would
                # still catch it). Logged so "why didn't this row drop?"
                # is diagnosable (class-9 cold-path rule).
                log.info(
                    "_detect_and_stamp_drops_git: removed imdb path %s "
                    "(%s/%s) has no resolvable themes row — skipping "
                    "drop-detection for it this run",
                    rel_path, media_type, imdb_id,
                )
                continue
            tmdb_id = int(row["tmdb_id"])
        if tmdb_id is None:
            continue
        candidates.setdefault((media_type, tmdb_id), {})
    # For each candidate, verify NO surviving path in the new tree
    # still references it. If imdb/<id>.json went away but
    # themoviedb/<id>.json is still there, the item is NOT dropped.
    actually_dropped: list[tuple[str, int]] = []
    for (mt, tmdb_id) in candidates:
        # Look up the imdb_id we'd expect from themes for the
        # imdb-path probe.
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT imdb_id FROM themes "
                "WHERE media_type = ? AND tmdb_id = ?",
                (mt, tmdb_id),
            ).fetchone()
        imdb_id_from_db = row["imdb_id"] if row else None
        # v1.18.0: 'collection' lives under movie_collections/.
        # Collections have no imdb/ subtree, so survives_imdb
        # below is always False (imdb_id_from_db is None for them).
        if mt == "movie":
            media_path = "movies"
        elif mt == "collection":
            media_path = "movie_collections"
        else:
            media_path = "tv_shows"
        # v1.24.0: existence probe, not read_json — a present-but-malformed
        # blob must read as "survives", never as dropped (fail-safe).
        survives_tmdb = git_mirror.path_exists_in_tree(
            f"{media_path}/themoviedb/{tmdb_id}.json")
        survives_imdb = (imdb_id_from_db is not None) and (
            git_mirror.path_exists_in_tree(
                f"{media_path}/imdb/{imdb_id_from_db}.json")
        )
        if not (survives_tmdb or survives_imdb):
            actually_dropped.append((mt, tmdb_id))
    if not actually_dropped:
        return 0
    # Safety cap (per media_type).
    by_type: dict[str, list[int]] = {}
    for mt, tmdb_id in actually_dropped:
        by_type.setdefault(mt, []).append(tmdb_id)
    stamped_total = 0
    # v0.50.89 (audit MEDIUM): one transaction around the per-title
    # tdb_dropped_at stamp + its companion pending_updates DELETE — mirrors
    # the v1.24.15 fix already applied to the full-walk sibling
    # (_detect_and_stamp_drops_full_walk). Pre-fix this git-transport path
    # (the default/primary transport) ran both statements autocommit, so a
    # crash/lock between them could leave a title stamped-dropped with its
    # now-moot pending_updates row still present.
    with get_conn(db_path) as conn, transaction(conn):
        for mt, ids in by_type.items():
            current_n = conn.execute(
                "SELECT COUNT(*) FROM themes "
                "WHERE media_type = ? AND upstream_source != 'plex_orphan'",
                (mt,),
            ).fetchone()[0] or 0
            cap = max(_DROP_SAFETY_FLOOR,
                      int(current_n * _DROP_SAFETY_PCT))
            if len(ids) > cap:
                log.warning(
                    "drop detection (git): %s implied_drops=%d exceeds "
                    "cap=%d (catalog=%d) — skipping stamp",
                    mt, len(ids), cap, current_n,
                )
                continue
            for tmdb_id in ids:
                cur = conn.execute(
                    """UPDATE themes SET tdb_dropped_at = ?
                        WHERE media_type = ? AND tmdb_id = ?
                          AND upstream_source != 'plex_orphan'
                          AND tdb_dropped_at IS NULL""",
                    (sync_ts, mt, tmdb_id),
                )
                if cur.rowcount:
                    # v1.20.16 (L1): clear the now-moot pending update for
                    # the just-dropped title (same as the full-walk path).
                    conn.execute(
                        "DELETE FROM pending_updates "
                        "WHERE media_type = ? AND tmdb_id = ?",
                        (mt, tmdb_id),
                    )
                stamped_total += cur.rowcount or 0
    return stamped_total


def _sweep_orphan_user_overrides(db_path) -> int:
    # v1.12.60: delete user_overrides rows with no theme presence (no
    # local_files, no placements, no Plex sidecar) and no in-flight
    # download/place job. Full rationale at the call site in run_sync.
    # v1.22.61 (audit round 2 #2): extracted from run_sync + the sidecar
    # presence check WIDENED with the theme_id arm — this was the LAST
    # surviving guid_tmdb-only site of the v1.22.17 class. Pre-fix a
    # anime M-row's sidecar (guid_tmdb NULL, linked via
    # pi.theme_id) was invisible to the guard, so a failed/cancelled
    # SET-URL download left the override unprotected and this sweep
    # DELETED it on the next sync (the v1.18.10 amplifier class).
    with get_conn(db_path) as conn:
        return conn.execute(
            """
            DELETE FROM user_overrides
            WHERE NOT EXISTS (
                SELECT 1 FROM local_files lf
                WHERE lf.media_type = user_overrides.media_type
                  AND lf.tmdb_id = user_overrides.tmdb_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM placements p
                WHERE p.media_type = user_overrides.media_type
                  AND p.tmdb_id = user_overrides.tmdb_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM plex_items pi
                LEFT JOIN themes t ON t.id = pi.theme_id
                WHERE pi.media_type = (CASE user_overrides.media_type
                                        WHEN 'tv' THEN 'show'
                                        ELSE user_overrides.media_type END)
                  AND (pi.guid_tmdb = user_overrides.tmdb_id
                       OR (t.tmdb_id = user_overrides.tmdb_id
                           AND t.media_type = user_overrides.media_type))
                  AND pi.local_theme_file = 1
              )
              AND NOT EXISTS (
                SELECT 1 FROM jobs j
                WHERE j.media_type = user_overrides.media_type
                  AND j.tmdb_id = user_overrides.tmdb_id
                  AND j.job_type IN ('download', 'place')
                  AND j.status IN ('pending', 'running')
              )
            """
        ).rowcount or 0


def _prune_stale_pending_updates(db_path) -> int:
    # v1.12.49/.52/.58: delete pending_updates rows that have nothing to
    # update against (no local_files, no override, no placement, no Plex
    # sidecar, no in-flight download/place job). Full rationale at the
    # call site in run_sync.
    # v1.19.77: the 4th NOT EXISTS now anchors a new_theme_available row
    # on plex_items PRESENCE rather than local_theme_file=1. SRC=— and
    # pure-cloud-P rows have no local theme BY DEFINITION, so the pre-fix
    # sweep deleted the v1.19.71 prompt in the SAME run_sync that wrote
    # it — the headline cohort was dead at rest (Opus-4.8 audit HIGH-1).
    # A new-theme row whose title has left Plex (no plex_items row) still
    # prunes, preserving the orphan-cleanup intent.
    with get_conn(db_path) as conn:
        pruned = conn.execute(
            """
            DELETE FROM pending_updates
            WHERE NOT EXISTS (
                SELECT 1 FROM local_files lf
                WHERE lf.media_type = pending_updates.media_type
                  AND lf.tmdb_id = pending_updates.tmdb_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM user_overrides uo
                WHERE uo.media_type = pending_updates.media_type
                  AND uo.tmdb_id = pending_updates.tmdb_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM placements p
                WHERE p.media_type = pending_updates.media_type
                  AND p.tmdb_id = pending_updates.tmdb_id
              )
              AND NOT EXISTS (
                -- v1.22.17: ALSO match theme_id linkage (anime
                -- guid_tmdb NULL) — mirror the sync detection gate so
                -- the prune doesn't reap a pending row whose only
                -- plex_items link is via theme_id, not guid_tmdb.
                SELECT 1 FROM plex_items pi
                LEFT JOIN themes t ON t.id = pi.theme_id
                WHERE pi.media_type = (CASE pending_updates.media_type
                                        WHEN 'tv' THEN 'show'
                                        ELSE pending_updates.media_type END)
                  AND (pi.guid_tmdb = pending_updates.tmdb_id
                       OR (t.tmdb_id = pending_updates.tmdb_id
                           AND t.media_type = pending_updates.media_type))
                  AND (pi.local_theme_file = 1
                       OR pending_updates.kind = 'new_theme_available')
              )
              AND NOT EXISTS (
                SELECT 1 FROM jobs j
                WHERE j.media_type = pending_updates.media_type
                  AND j.tmdb_id = pending_updates.tmdb_id
                  AND j.job_type IN ('download', 'place')
                  AND j.status IN ('pending', 'running')
              )
            """
        ).rowcount or 0
    if pruned:
        log_event(
            db_path, level="INFO", component="sync",
            message=f"Pruned {pruned} stale pending_updates row"
                    f"{'s' if pruned != 1 else ''} (no theme to update against)",
            detail={"pruned": pruned},
        )
    return pruned


def run_sync(db_path, base_url: str, *,
             auto_place_override: bool | None = None,
             enqueue_downloads: bool = True,
             auto_download_new_themes: bool = False,
             cancel_check=lambda: False,
             source: str = "remote",
             database_url: str | None = None,
             git_url: str | None = None,
             git_branch: str | None = None) -> SyncStats:
    """Run a full ThemerrDB sync. Returns stats.

    `auto_place_override=None` lets the worker fall back to the global
    placement.auto_place setting when each download finishes. Setting it to
    True or False stamps that decision onto every download enqueued by this
    run (so a "download only" sync always lands in /pending regardless of
    the global default, and vice versa).

    `enqueue_downloads=False` (v1.7+ default of the dashboard SYNC button)
    runs sync metadata-only: themes rows are upserted but no download jobs
    are enqueued. The user triggers downloads explicitly from /movies, /tv,
    or /coverage. pending_updates rows are still recorded for items whose
    ThemerrDB URL changed since last sync.
    """
    stats = SyncStats()
    sync_ts = now_iso()

    with get_conn(db_path) as conn:
        # v1.13.2 (#1): stamp transport at start so the dashboard
        # sparkline can color this run by which path was requested,
        # even if the run later cascades to a slower tier (the
        # fallback target lands in fallback_reason at finish).
        run_id = conn.execute(
            "INSERT INTO sync_runs (started_at, status, transport) "
            "VALUES (?, 'running', ?)",
            (sync_ts, source),
        ).lastrowid

    log_event(db_path, level="INFO", component="sync", message=f"Sync run #{run_id} started")

    # v1.12.106: live progress surface for the ops side-drawer. One row
    # in op_progress, upserted at every natural checkpoint already
    # iterated by the existing log path. The drawer renders stage
    # timeline + smoothed throughput + ETA from these writes.
    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        # v1.23.32: action-oriented label + a seeded activity line so this
        # opening step (the %-less fetch — git mirror pull, which can take a
        # moment on a big diff) reads as actively working, not a blank step.
        stage="index", stage_label="Fetching latest from ThemerrDB",
        activity="Connecting to ThemerrDB and pulling the latest theme index…",
    )

    # Combined cancel: worker's existing cancel_check (job cancellation
    # via jobs.status='cancelled') OR the user clicking // CANCEL in
    # the ops drawer (op_progress.status='cancelling'). Either flips
    # the worker's cancellation path; we keep _JobCancelled as the
    # bail signal so partial-stats persistence (line 1056+) still
    # fires uniformly.
    def _cancel_check():
        return cancel_check() or op_progress.is_cancelled(db_path, "tdb_sync")

    snapshot: _DatabaseSnapshot | None = None
    git_mirror: _GitMirror | None = None
    try:
        with _make_client() as client:
            # v1.13.0 (Phase B): git mirror path. When sync.source =
            # "git", maintain a bare git mirror of the database
            # branch and fetch only the delta. First run shallow-
            # clones; subsequent runs fetch + diff. On _GitMirrorError
            # we cascade to the snapshot path; on snapshot failure
            # we cascade to remote. Three-tier fallback chain so a
            # single transport failure can't take down the daily sync.
            if source == "git":
                resolved_git_url = git_url or (
                    "https://github.com/LizardByte/ThemerrDB.git"
                )
                resolved_branch = git_branch or "database"
                try:
                    git_mirror = _GitMirror(
                        db_path, "tdb_sync",
                        resolved_git_url, resolved_branch,
                        _cancel_check,
                    )
                    git_mirror.acquire(client)
                    if git_mirror.is_unchanged():
                        log_event(
                            db_path, level="INFO", component="sync",
                            message=f"Sync run #{run_id}: git mirror "
                                    f"unchanged — skipping upsert",
                        )
                        op_progress.set_detail_field(
                            db_path, "tdb_sync", "no_changes", True,
                        )
                    else:
                        log_event(
                            db_path, level="INFO", component="sync",
                            message=f"Sync run #{run_id}: git mirror "
                                    f"acquired from {resolved_git_url}",
                        )
                except _GitMirrorError as e:
                    log_event(
                        db_path, level="WARNING", component="sync",
                        message=f"Sync run #{run_id}: git mirror "
                                f"failed ({e}); cascading to snapshot",
                    )
                    op_progress.set_detail_field(
                        db_path, "tdb_sync", "fallback_active", True,
                    )
                    op_progress.set_detail_field(
                        db_path, "tdb_sync", "fallback_reason",
                        f"git: {e}",
                    )
                    op_progress.update_progress(
                        db_path, "tdb_sync",
                        activity=f"Git mirror unavailable — using "
                                 f"snapshot fallback ({e})",
                    )
                    git_mirror = None
                    source = "database"  # cascade

            # v1.12.121 (Phase A): snapshot path. When sync.source =
            # "database", try to pull a single tarball of LizardByte's
            # database branch up-front. On any failure, log + flag
            # fallback_active and fall through to the remote per-item
            # HTTP path so the run still completes.
            if source == "database":
                tar_url = database_url or (
                    "https://codeload.github.com/LizardByte/ThemerrDB/"
                    "tar.gz/database"
                )
                try:
                    snapshot = _DatabaseSnapshot(
                        db_path, "tdb_sync", tar_url, _cancel_check,
                    )
                    snapshot.acquire(client)
                    if snapshot.is_unchanged():
                        log_event(
                            db_path, level="INFO", component="sync",
                            message=f"Sync run #{run_id}: snapshot "
                                    f"unchanged (304) — skipping upsert",
                        )
                        op_progress.set_detail_field(
                            db_path, "tdb_sync", "no_changes", True,
                        )
                    else:
                        log_event(
                            db_path, level="INFO", component="sync",
                            message=f"Sync run #{run_id}: snapshot acquired "
                                    f"from {tar_url}",
                        )
                except _SnapshotError as e:
                    log_event(
                        db_path, level="WARNING", component="sync",
                        message=f"Sync run #{run_id}: snapshot path "
                                f"failed ({e}); falling back to remote",
                    )
                    op_progress.set_detail_field(
                        db_path, "tdb_sync", "fallback_active", True,
                    )
                    op_progress.set_detail_field(
                        db_path, "tdb_sync", "fallback_reason", str(e),
                    )
                    op_progress.update_progress(
                        db_path, "tdb_sync",
                        activity=f"Snapshot unavailable — using remote "
                                 f"fallback ({e})",
                    )
                    snapshot = None

            # v1.13.0 (Phase B): git differential path. When the git
            # mirror reports new commits, diff the trees and walk
            # ONLY changed paths — typically tens of items vs. the
            # 5k+ full-index walk. Skips the index/fetch loop entirely
            # for both media types; resolve + prune sweeps still run.
            # v0.51.14 (audit #4/#5): hoisted above the git call so the git
            # path can populate the per-item exclusions too (was declared after
            # it, remote-walk-only). git_failed_paths / git_unresolved feed the
            # baseline-reset detection routing + the chronic-pin escape below.
            index_incomplete = False
            errored_by_mt: dict[str, set[int]] = {}
            git_failed_paths: list[str] = []
            git_unresolved = 0
            if (git_mirror is not None
                    and not git_mirror.is_unchanged()):
                git_failed_paths, git_unresolved = _run_git_differential_upsert(
                    db_path, git_mirror, stats, sync_ts=sync_ts,
                    enqueue_downloads=enqueue_downloads,
                    auto_place_override=auto_place_override,
                    auto_download_new_themes=auto_download_new_themes,
                    cancel_check=_cancel_check,
                    errored_by_mt=errored_by_mt,
                )

            # v1.12.126 Phase A.5: skip the per-media-type upsert pipeline
            # entirely when codeload reported 304 (no upstream change).
            # Stats stay zero on this path; the resolve + prune sweeps
            # below still run since they depend on local DB state, not
            # TDB state. is_unchanged is only set for the snapshot path
            # OR the git path; remote-fallback runs always do the full
            # upsert.
            skip_upsert = (
                (snapshot is not None and snapshot.is_unchanged())
                or (git_mirror is not None)
            )
            # v1.18.0: collections walked alongside movies/tv. Same
            # index/all_page shape, same per-item upsert path; only
            # the theme-injection step (Phase 4 worker adapter) is
            # collection-specific.
            media_iter = (() if skip_upsert
                          else (
                              ("movie", "movies"),
                              ("tv", "tv_shows"),
                              ("collection", "movie_collections"),
                          ))
            # v1.21.44 (review of v1.21.38): two DISTINCT incompleteness
            # signals for the drop sweep. index_incomplete = a whole index
            # page failed → items genuinely MISSING from the walk, can't
            # enumerate which → suppress the whole sweep. errored_by_mt =
            # per-item fetch errors (in-index but unfetched) → EXCLUDE just
            # those ids from stamping (don't disable the sweep — a single
            # chronically-erroring item must not permanently starve drop
            # detection). v0.51.14: both now DECLARED above the git-upsert
            # call (the git path populates errored_by_mt too — audit #4).
            # v0.50.45: running total of items COMPLETED (fetched, incl errors)
            # across prior media types — the per-type processed_total stamp adds
            # `completed` to this. Pre-fix the base was stats.X_seen (FLUSHED, so
            # excludes fetch errors), but the stamp added `completed` (fetched), so
            # at each movie→tv→collection boundary the op-wide counter jumped
            # backward by the prior type's fetch-error count. Now base == sum of
            # prior `completed`, so the counter stays monotonic across types.
            _remote_done = 0
            for media_type, media_path in media_iter:
                stage_key = f"index_{media_type}"
                op_progress.update_progress(
                    db_path, "tdb_sync",
                    stage=stage_key,
                    stage_label=f"Fetching {media_type} index",
                    stage_current=0, stage_total=0,
                    activity=f"Fetching {media_type} index from ThemerrDB",
                )
                if snapshot is not None:
                    index, idx_failed_pages = snapshot.index(media_path)
                else:
                    index, idx_failed_pages = _fetch_index(
                        client, base_url, media_path)
                if idx_failed_pages:
                    # v1.21.38: a swallowed index page = items missing from
                    # this walk → their last_seen_sync_at stays stale →
                    # they'd be mis-stamped as TDB-dropped. v1.21.44: flag
                    # index_incomplete (suppresses the WHOLE drop sweep —
                    # we can't enumerate the missing items) + keep the
                    # error count for observability.
                    index_incomplete = True
                    stats.errors += idx_failed_pages
                    log.warning(
                        "sync %s: %d index page(s) failed to load — drop "
                        "detection will be skipped this run (incomplete "
                        "catalog)", media_type, idx_failed_pages,
                    )
                log.info("ThemerrDB %s: %d items in index", media_type, len(index))
                if media_type == "movie":
                    stats.movies_seen = len(index)
                elif media_type == "collection":
                    # v1.18.0: own bucket on the stats record.
                    stats.collections_seen = len(index)
                else:
                    stats.tv_seen = len(index)

                # v1.11.49: parallelize per-item HTTP fetches. Pre-fix the
                # tight loop did a sequential `_fetch_item` per index entry
                # — at ~50ms RTT to GitHub Pages this added up to many
                # minutes for libraries with thousands of titles. The
                # bottleneck was network, not DB; httpx.Client is
                # thread-safe so a small ThreadPoolExecutor over the
                # index yields ~10-20× speedup without any rewrite of
                # the per-item logic.
                #
                # Cancellation: the executor's submission loop checks
                # cancel_check before each submit and bails fast; in-flight
                # workers continue (kill-switch via shutdown(cancel_futures)
                # leaves them queued, the index walk just stops feeding new
                # ones). Since each fetch is ~30s timeout max, the worker
                # pool drains within seconds of cancel.
                batch: list[tuple[str, int, dict, str]] = []
                from .worker import _JobCancelled

                def _do_fetch(entry):
                    tmdb_id = entry.get("id")
                    if tmdb_id is None:
                        return None
                    imdb_id = entry.get("imdb_id") or None
                    if snapshot is not None:
                        record = snapshot.fetch_item(
                            media_path,
                            imdb_id=imdb_id, tmdb_id=int(tmdb_id),
                        )
                    else:
                        record = _fetch_item(
                            client, base_url, media_path,
                            imdb_id=imdb_id, tmdb_id=int(tmdb_id),
                        )
                    if record is None:
                        # v1.15.36: log the failed fetch so the
                        # operator can correlate stats.errors to
                        # specific tmdb_ids. Pre-fix the error
                        # tuple was returned with no per-item
                        # logging — N transient timeouts looked
                        # identical to N upstream-deleted items
                        # in the docker logs.
                        log.info(
                            "sync fetch: %s/%s (imdb=%s) returned "
                            "None — counted as error",
                            media_path, tmdb_id, imdb_id,
                        )
                        return ("error", int(tmdb_id))
                    upstream_source = "imdb" if imdb_id else "themoviedb"
                    return (media_type, int(tmdb_id), record, upstream_source)

                fetch_stage = f"fetch_{media_type}"
                op_progress.update_progress(
                    db_path, "tdb_sync",
                    stage=fetch_stage,
                    stage_label=f"Fetching {media_type} themes",
                    stage_current=0, stage_total=len(index),
                    processed_est=(stats.movies_seen + stats.tv_seen
                                   + stats.collections_seen),
                )
                with ThreadPoolExecutor(
                    max_workers=_FETCH_WORKERS,
                    thread_name_prefix=f"motif-sync-{media_type}",
                ) as ex:
                    futures = []
                    for entry in index:
                        if _cancel_check():
                            ex.shutdown(cancel_futures=True)
                            raise _JobCancelled()
                        futures.append(ex.submit(_do_fetch, entry))
                    # v1.11.76: visible per-item progress so the user can
                    # see the sync is making forward progress (and how
                    # fast). Pre-fix the only sync logs were the
                    # opening 'index' line and the closing 'finished'
                    # line — a slow connection looked indistinguishable
                    # from a hang.
                    completed = 0
                    last_log = time.monotonic()
                    last_ui = time.monotonic()
                    total = len(futures)
                    # v1.18.0: 3-way base so the op-wide processed_total spans all
                    # media types. v0.50.45: base is the running `completed` total
                    # (see _remote_done above), NOT stats.X_seen — the seen counts
                    # exclude fetch errors while the stamp adds `completed` (incl
                    # errors), so the old stats base jumped the counter backward at
                    # each media-type boundary by the prior type's error count.
                    media_processed_base = _remote_done
                    for fut in as_completed(futures):
                        if _cancel_check():
                            ex.shutdown(cancel_futures=True)
                            raise _JobCancelled()
                        result = fut.result()
                        completed += 1
                        # v1.12.124: split log cadence from progress UI
                        # cadence. Pre-fix both fired together at every
                        # 500 items + a 30s wallclock fallback — on a
                        # snapshot run that turns into a ~12% step per
                        # batch, with the bar visibly jumping rather
                        # than ticking. The log line stays at 500-item
                        # milestones (so log files stay terse), but
                        # op_progress now updates every ~300ms so the
                        # bar feels live regardless of items/sec.
                        if (completed == 1 or completed == total
                                or completed % 500 == 0
                                or (time.monotonic() - last_log) > 30.0):
                            log.info(
                                "sync %s progress: %d/%d "
                                "(errors=%d, queued_for_flush=%d)",
                                media_type, completed, total,
                                stats.errors, len(batch),
                            )
                            last_log = time.monotonic()
                        if (completed == 1 or completed == total
                                or (time.monotonic() - last_ui) > 0.3):
                            last_ui = time.monotonic()
                            op_progress.update_progress(
                                db_path, "tdb_sync",
                                stage_current=completed,
                                stage_total=total,
                                processed_total=media_processed_base + completed,
                                error_count=stats.errors,
                            )
                        if result is None:
                            continue
                        if result[0] == "error":
                            stats.errors += 1
                            # v1.21.44: remember which ids errored so the
                            # drop sweep EXCLUDES them (in-index but
                            # unfetched → not a confirmed drop) instead of
                            # disabling the whole sweep.
                            errored_by_mt.setdefault(
                                media_type, set()).add(int(result[1]))
                            continue
                        batch.append(result)
                        if len(batch) >= _SYNC_BATCH:
                            # v1.19.91: pass auto_download_new_themes (in
                            # scope from run_sync) — missed when v1.19.71
                            # made it keyword-only. See the tail-flush note
                            # in _run_git_differential_upsert.
                            _flush_sync_batch(
                                db_path, batch, sync_ts=sync_ts,
                                enqueue_downloads=enqueue_downloads,
                                auto_place_override=auto_place_override,
                                auto_download_new_themes=auto_download_new_themes,
                                stats=stats,
                            )
                            batch.clear()
                            # v1.11.54: yield the writer lock for ~250ms
                            # with jitter between batches. Pre-fix the 50ms
                            # gap left an 80% lock duty cycle — the
                            # general worker's BEGIN IMMEDIATE timed out
                            # at 30s and the v1.11.51 retry path had to
                            # absorb repeated 'database is locked' for
                            # the entire sync. With ~200ms per batch
                            # transaction + 250ms yield, duty cycle drops
                            # to ~45% and other writers (event flusher,
                            # job claim, log_event) reliably get a
                            # window. Total sync wall-clock cost on a
                            # ~5K-item run: ~25s extra; well worth not
                            # wedging the rest of the app.
                            time.sleep(0.25 + random.uniform(0, 0.05))

                if batch:
                    # v1.19.91: tail-flush must pass auto_download_new_themes
                    # too (keyword-only since v1.19.71).
                    _flush_sync_batch(
                        db_path, batch, sync_ts=sync_ts,
                        enqueue_downloads=enqueue_downloads,
                        auto_place_override=auto_place_override,
                        auto_download_new_themes=auto_download_new_themes,
                        stats=stats,
                    )
                    batch.clear()
                # v0.50.45: carry this media type's `completed` (fetched, incl
                # errors) into the base for the next type so processed_total stays
                # monotonic across the movie→tv→collection boundaries.
                _remote_done += completed

        # v1.13.1 (Phase C): drop detection. Stamp tdb_dropped_at on
        # themes whose upstream JSON disappeared between syncs. The
        # snapshot/remote paths use the implicit last_seen_sync_at
        # signal (anything not touched this run is implicitly
        # removed); the git path uses the explicit ChangeSet.removed
        # list with a survivors check (one-of-imdb-or-themoviedb).
        # Both honor a 5%-of-catalog cap so a buggy upstream export
        # can't cascade-drop the whole library.
        #
        # v1.13.7: split the trigger so the git-active-changes path
        # actually runs detection. Pre-fix `if not skip_upsert:`
        # gated everything off skip_upsert which is True for ALL
        # git-mode runs (the differential upsert helper does its
        # work above and sets skip_upsert=True to skip the full-tree
        # walk below). Result: _detect_and_stamp_drops_git was dead
        # code; drops were silently never stamped on source=git.
        ran_git_diff = (git_mirror is not None
                        and not git_mirror.is_unchanged())
        ran_full_walk = not skip_upsert
        if ran_git_diff or ran_full_walk:
            n_dropped = 0
            detection_ok = False
            try:
                if ran_git_diff and git_mirror.is_baseline_reset():
                    # v0.51.14 (audit #4): a first-run / baseline-reset git walk
                    # has removed=[] by construction (no old head to diff), so
                    # the changeset detector below is blind to every upstream
                    # removal across the reset window — while the full-tree
                    # upsert just refreshed last_seen_sync_at on ALL surviving
                    # rows, which is exactly the precondition the full-walk
                    # detector needs. Route there, excluding the failed reads
                    # (v1.21.44 per-item pattern); a failure that couldn't be
                    # mapped to an id makes last_seen unreliable → skip (the
                    # errors!=0 no-advance gate below keeps this a RESET next
                    # run too, so detection retries until a clean walk).
                    if git_unresolved:
                        log.info(
                            "Sync run #%s: baseline-reset drop detection "
                            "skipped — %d read failure(s) could not be mapped "
                            "to an id; retrying next run", run_id,
                            git_unresolved)
                    else:
                        media_types_seen = set()
                        if stats.movies_seen:
                            media_types_seen.add("movie")
                        if stats.tv_seen:
                            media_types_seen.add("tv")
                        if stats.collections_seen:
                            media_types_seen.add("collection")
                        n_dropped = _detect_and_stamp_drops_full_walk(
                            db_path, sync_ts=sync_ts,
                            media_types_seen=media_types_seen,
                            exclude_by_mt=errored_by_mt)
                elif ran_git_diff:
                    # The git-diff sweep keys on the dulwich changeset
                    # (explicit deletions), NOT last_seen_sync_at, so a
                    # partial per-item fetch can't make it see false drops
                    # — it stays ungated.
                    n_dropped = _detect_and_stamp_drops_git(
                        db_path, git_mirror, sync_ts=sync_ts)
                elif index_incomplete:
                    # v1.21.44 (review of v1.21.38): a swallowed INDEX PAGE
                    # means a chunk of items is genuinely MISSING from this
                    # run's catalog and we can't enumerate which — a stale
                    # last_seen_sync_at is then indistinguishable from a real
                    # drop, so skip the whole sweep (rare, usually transient;
                    # caught next clean run). PER-ITEM fetch errors do NOT
                    # land here — they're handled by excluding just those ids
                    # below, so a single chronically-erroring item can't
                    # permanently starve drop detection (the v1.21.38 bug).
                    log.info(
                        "Sync run #%s: skipping drop detection — an index "
                        "page failed this run; the catalog is incomplete and "
                        "a stale last_seen_sync_at would be mis-stamped as a "
                        "drop", run_id,
                    )
                else:
                    media_types_seen: set[str] = set()
                    if stats.movies_seen:
                        media_types_seen.add("movie")
                    if stats.tv_seen:
                        media_types_seen.add("tv")
                    if stats.collections_seen:  # v1.18.0
                        media_types_seen.add("collection")
                    n_dropped = _detect_and_stamp_drops_full_walk(
                        db_path, sync_ts=sync_ts,
                        media_types_seen=media_types_seen,
                        exclude_by_mt=errored_by_mt)
                detection_ok = True
            except Exception as e:
                log.warning("drop detection failed: %s", e)
            # v1.22.74: advance the git baseline only AFTER drop detection
            # consumed the changeset — and only if it SUCCEEDED. Pre-fix
            # commit_sync_ok() ran at the end of the differential upsert, so a
            # detection failure lost the removed paths forever (the next run
            # diffed from the NEW head and the drops never reappeared;
            # full-walk retries naturally via stale last_seen_sync_at, git had
            # no second chance). A cap-skip inside the detector returns
            # normally and still advances — the deliberate amplifier guard.
            # v1.24.0: split out of the detection try (class 9) so a
            # baseline-advance or compaction failure isn't mislabeled "drop
            # detection failed".
            # v1.24.13 (holistic review): also gate on stats.errors == 0. A
            # changed-path read_json() returning None is a `continue` + an
            # `errors += 1` (sync.py:3109/3126) — NOT a raise — so it slipped
            # the v1.22.74 "advance only on success" guard. Advancing the
            # baseline then consumed the changeset, permanently dropping that
            # add/modify (next run diffs from the new HEAD and never re-sees it).
            # In a ran_git_diff run no index walk runs, so stats.errors counts
            # ONLY git read failures. NOT advancing lets the next run re-diff the
            # same delta + retry the failed reads (themes upsert is idempotent).
            if ran_git_diff and detection_ok and stats.errors:
                # v0.51.14 (audit #5): chronic-pin escape. One persistently-
                # malformed upstream blob would otherwise pin the baseline
                # FOREVER — every run re-diffs the same ever-growing delta
                # (the starvation class v1.21.44 fixed per-item for the full
                # walk). If the SAME baseline produced the SAME failed-path
                # set on the previous run too, the failures are persistent,
                # not transient: advance anyway. The skipped paths re-enter
                # the diff whenever upstream actually changes them (their
                # content at the new baseline is still the broken blob).
                _chronic = False
                try:
                    from .runtime import get_runtime_text, set_runtime_text
                    _key = "git_chronic_read_failures"
                    _cur = json.dumps({
                        "baseline": git_mirror.baseline_key(),
                        "paths": sorted(git_failed_paths),
                    }, sort_keys=True)
                    if git_failed_paths and get_runtime_text(
                            db_path, _key) == _cur:
                        _chronic = True
                        set_runtime_text(db_path, _key, "", updated_by="sync")
                    else:
                        set_runtime_text(db_path, _key, _cur,
                                         updated_by="sync")
                except Exception as e:
                    log.warning(
                        "chronic-read-failure tracking failed: %s", e)
                if _chronic:
                    log.warning(
                        "Sync run #%s: %d changed-path read(s) failed on the "
                        "SAME paths as the previous run — chronic upstream "
                        "breakage; advancing the git baseline anyway so it "
                        "can't stay pinned (skipping: %s)",
                        run_id, stats.errors,
                        ", ".join(sorted(git_failed_paths)[:5]))
                    try:
                        git_mirror.commit_sync_ok()
                    except Exception as e:
                        log.warning("git baseline advance failed: %s", e)
                else:
                    log.warning(
                        "Sync run #%s: %d changed-path read(s) failed — NOT "
                        "advancing the git baseline so the next run re-diffs "
                        "and retries (else the add/modify would be permanently "
                        "dropped); a repeat of the SAME failures next run "
                        "triggers the chronic escape above",
                        run_id, stats.errors)
            if ran_git_diff and detection_ok and stats.errors == 0:
                try:
                    git_mirror.commit_sync_ok()
                except Exception as e:
                    log.warning("git baseline advance failed: %s", e)
                else:
                    # v1.23.80: now that commit_sync_ok retired the old HEAD,
                    # compact the mirror if over the size threshold (the
                    # no-change path at sync.py:2755 was the ONLY trigger, so a
                    # run of big-diff days never gave it a window — the user's
                    # mirror reached 168 MB / 6 packs and "applying 9943 paths"
                    # crawled). NOT safe before the HEAD is retired, hence the
                    # else-of-commit gate.
                    try:
                        git_mirror._compact_if_oversized()
                    except Exception as e:
                        log.warning("mirror compaction failed: %s", e)
            if n_dropped:
                try:
                    log_event(
                        db_path, level="INFO", component="sync",
                        message=f"Sync run #{run_id}: stamped "
                                f"{n_dropped} item(s) as TDB-dropped",
                        detail={"dropped": n_dropped},
                    )
                except Exception as e:
                    log.warning("drop-event logging failed: %s", e)

        # v1.22.8: clear undecided pendings that propose a TDB theme the
        # upstream no longer has (title still present, youtube_url went NULL) —
        # otherwise they linger as un-actionable NEEDS WORK / !UPD prompts
        # pointing at a removed theme. Runs every sync (idempotent; only deletes
        # pendings whose theme genuinely has no url, so a partial/errored run
        # can't over-sweep). Complements the v1.20.16 dropped-TITLE cleanup.
        try:
            n_stale_pu = _clear_url_less_pending_updates(db_path)
            if n_stale_pu:
                log_event(
                    db_path, level="INFO", component="sync",
                    message=f"Sync run #{run_id}: cleared {n_stale_pu} stale "
                            f"pending update(s) — upstream theme removed",
                    detail={"cleared_stale_pending": n_stale_pu},
                )
        except Exception as e:
            log.warning("stale pending_updates cleanup failed: %s", e)

        op_progress.update_progress(
            db_path, "tdb_sync",
            stage="resolve",
            stage_label="Resolving theme links",
            stage_current=0, stage_total=0,
            activity="Refreshing plex_items.theme_id",
        )
        # v1.11.26: refresh plex_items.theme_id now that themes has
        # absorbed the new / updated rows from this sync. Keeps the
        # /api/library JOIN's PK lookup column in sync with the latest
        # themes data without waiting for the next plex_enum.
        try:
            from .plex_enum import resolve_theme_ids
            # v1.12.127: route resolve progress through tdb_sync's
            # op_progress row so the ops drawer renders a real %
            # bar during the resolve stage instead of the prior
            # stage_total=0 indeterminate shimmer.
            resolve_theme_ids(db_path, progress_op_id="tdb_sync")
        except Exception as e:
            log.warning("post-sync resolve_theme_ids failed: %s", e)

        # v1.12.60: sweep orphan user_overrides BEFORE the
        # pending_updates sweep below. user_overrides is what
        # makes a row classify as U at download time (worker
        # prefers user_overrides.youtube_url over
        # themes.youtube_url). When the row has no theme presence
        # — no local_files, no placements, no sidecar — and no
        # in-flight job, the override is dangling metadata that
        # silently re-classifies the row as U on the next download.
        # Pre-fix, a sequence like SET URL → PURGE on a pre-v1.12.57
        # build left an orphan override that survived everywhere
        # else and re-applied on the next DOWNLOAD TDB action,
        # confusing users who expected a clean T result. The .57
        # _drop_motif_tracking helper handles the forward path;
        # this sweep handles legacy and edge cases.
        #
        # Same in-flight job guard as the pending_updates sweep —
        # protects a fresh SET URL whose download hasn't started yet
        # (user_overrides exists, local_files doesn't, but there's
        # a queued download job).
        op_progress.update_progress(
            db_path, "tdb_sync",
            stage="prune",
            stage_label="Pruning stale state",
            stage_current=0, stage_total=0,
            activity="Sweeping orphan overrides",
        )
        # v1.22.61: extracted to _sweep_orphan_user_overrides (and the
        # sidecar check widened with the theme_id arm — see the fn).
        stale_overrides = _sweep_orphan_user_overrides(db_path)
        if stale_overrides:
            log_event(
                db_path, level="INFO", component="sync",
                message=f"Pruned {stale_overrides} orphan user_overrides "
                        f"row{'s' if stale_overrides != 1 else ''} "
                        "(no theme presence, no in-flight jobs)",
                detail={"pruned": stale_overrides},
            )

        # v1.12.49: sweep stale pending_updates rows. A pending_updates
        # row is only meaningful when the user actually has a theme to
        # "update against" (motif local file, manual URL override, or
        # Plex-folder sidecar). Rows can become stale when the user
        # drops/forgets a theme after the upstream URL changed: the
        # pending_updates row stays put, lighting the topbar UPD
        # counter and (pre-v1.12.48) the row's blue TDB ↑ pill, even
        # though there's nothing for ACCEPT UPDATE to swap from.
        # v1.12.48 papered over the row pill in JS; this sweep fixes
        # the root cause so the UPD count and the tdb_pills=update
        # filter both stop counting them too.
        #
        # Mirrors the cross-sync match-detection inverse at line 597:
        # rows are *created* when has_local_files OR has_override OR
        # has_sidecar; rows are *deleted* here when none of those hold.
        # v1.12.52: also keep rows that have a placements entry. The
        # creation criteria don't include placements (placement implies
        # local_files at write time) but a placement row can outlive
        # its local_files row — e.g., the canonical was deleted from
        # the themes dir but the hardlink in the Plex folder survives.
        # The user still has the theme playing in Plex, so the
        # pending_update is still actionable. Defensive: if the four
        # criteria all miss, the row is genuinely orphaned.
        # v1.12.58: dropped the decision IN ('pending','declined')
        # filter that was here to protect in-flight ACCEPT UPDATE
        # downloads. Replaced with a NOT EXISTS jobs check that
        # provides the same protection more precisely. Pre-fix,
        # 'accepted' decisions for rows that had since been PURGEd
        # (or where pre-v1.12.57 PURGE failed to clean up — the
        # whole reason v1.12.57 added _drop_motif_tracking) lingered
        # forever, setting the SQL accepted_update flag and
        # blocking REPLACE TDB / DOWNLOAD TDB in the SOURCE menu
        # for the row's afterlife. With the in-flight guard, any
        # decision is fair game once the row has nothing to update
        # against AND no live download/place jobs.
        # v1.19.77: extracted to _prune_stale_pending_updates so the
        # new_theme_available carve-out (Opus-4.8 audit HIGH-1) is
        # unit-testable end-to-end. The rationale above documents WHY
        # the sweep exists; the helper documents the new-kind carve-out.
        _prune_stale_pending_updates(db_path)

        # v1.12.53: detect "U-row with override URL == TDB URL" —
        # the row is classified U but the URL is identical to TDB.
        # Without this synthetic pending_update, the user has no
        # blue-↑-pill prompt to reclassify U → T (cross-sync's
        # is_new and url_changed branches don't fire when both the
        # title and the URL are pre-existing). Writing a synthetic
        # pending_update lights up the blue pill so the user can
        # ACCEPT UPDATE — which (in api.py v1.12.53) flips local_files
        # to T eagerly via the url_match path. Idempotent: ON
        # CONFLICT keeps any existing decision the user already
        # made on this row.
        with get_conn(db_path) as conn:
            # v1.12.99: per-section sweep. Pre-fix the synthetic
            # urls_match row was title-global (one row per title)
            # which lit up the blue pill on every section's row even
            # when the override only existed for one. Now each
            # user_overrides row gets its own section-specific
            # synthetic pending_update so KEEP CURRENT / ACCEPT on
            # one section doesn't bleed to siblings.
            # edition-blind OK (v1.21.94): urls_match detection writes the
            # synthetic pending_update at section granularity (uo.section_id),
            # edition_key=''; per v81 the per-edition ACCEPT/KEEP decision is
            # resolved in api.py against the clicked rk's edition.
            # v1.22.67 (audit round 2): SELECT DISTINCT + ON CONFLICT DO
            # NOTHING. Two same-section overrides differing only in
            # edition_key that both match the TDB URL made the JOIN emit
            # two IDENTICAL rows (uo contributes only section_id; the
            # INSERT writes edition_key='' default) → PK violation →
            # IntegrityError failed the ENTIRE sync, repeatably on every
            # run, until an override was removed. DISTINCT collapses the
            # intra-statement duplicates; the ON CONFLICT also makes the
            # long-standing "ON CONFLICT keeps any existing decision"
            # comment above actually true.
            synthetic = conn.execute(
                """
                INSERT INTO pending_updates (
                    media_type, tmdb_id, section_id,
                    old_video_id, new_video_id,
                    old_youtube_url, new_youtube_url,
                    upstream_edited_at, detected_at, decision, kind
                )
                SELECT DISTINCT t.media_type, t.tmdb_id, uo.section_id,
                       t.youtube_video_id, t.youtube_video_id,
                       NULL, t.youtube_url,
                       t.youtube_edited_at, ?, 'pending', 'urls_match'
                  FROM themes t
                  JOIN user_overrides uo
                    ON uo.media_type = t.media_type
                   AND uo.tmdb_id = t.tmdb_id
                 WHERE t.youtube_url IS NOT NULL
                   AND t.youtube_url <> ''
                   AND TRIM(uo.youtube_url) = TRIM(t.youtube_url)
                   AND NOT EXISTS (
                     SELECT 1 FROM pending_updates pu
                      WHERE pu.media_type = t.media_type
                        AND pu.tmdb_id = t.tmdb_id
                        AND pu.section_id = uo.section_id
                   )
                ON CONFLICT(media_type, tmdb_id, section_id, edition_key)
                DO NOTHING
                """,
                (sync_ts,),
            ).rowcount or 0
        if synthetic:
            log_event(
                db_path, level="INFO", component="sync",
                message=f"Detected {synthetic} U-row"
                        f"{'s' if synthetic != 1 else ''} with override URL "
                        "matching TDB — surfaced as pending updates so the "
                        "user can ACCEPT UPDATE to convert U → T",
                detail={"synthetic": synthetic},
            )

        # v1.13.2 (#1): finalize telemetry. fallback_reason is sourced
        # from op_progress.detail (set by the cascade dispatch); a
        # clean run on the originally-requested transport leaves it
        # NULL. no_changes flips to 1 when either Phase A.5's 304 or
        # Phase B's no-new-commits short-circuit fired.
        finalize_fallback_reason = None
        finalize_no_changes = 0
        try:
            with get_conn(db_path) as conn:
                row = conn.execute(
                    "SELECT detail_json FROM op_progress WHERE op_id = 'tdb_sync'"
                ).fetchone()
            if row is not None and row["detail_json"]:
                _detail = json.loads(row["detail_json"])
                if _detail.get("fallback_active"):
                    finalize_fallback_reason = _detail.get(
                        "fallback_reason") or "unknown"
                if _detail.get("no_changes"):
                    finalize_no_changes = 1
        except Exception as e:
            log.debug("telemetry detail read failed: %s", e)

        with get_conn(db_path) as conn:
            conn.execute(
                """
                UPDATE sync_runs SET
                    finished_at = ?, status = 'success',
                    movies_seen = ?, tv_seen = ?, collections_seen = ?,
                    new_count = ?, updated_count = ?,
                    fallback_reason = ?, no_changes = ?
                WHERE id = ?
                """,
                (now_iso(), stats.movies_seen, stats.tv_seen,
                 stats.collections_seen,
                 stats.new_count, stats.updated_count,
                 finalize_fallback_reason, finalize_no_changes,
                 run_id),
            )

        log_event(
            db_path, level="INFO", component="sync",
            message=f"Sync run #{run_id} finished",
            detail={
                "movies_seen": stats.movies_seen, "tv_seen": stats.tv_seen,
                "collections_seen": stats.collections_seen,  # v1.18.0
                "new": stats.new_count, "updated": stats.updated_count,
                "errors": stats.errors,
            },
        )
        # v1.12.126 Phase A.5: distinct activity line on the no-change
        # path so the ops drawer's "Last Ops" card reads honestly.
        # v1.13.7: four distinct cases — snapshot 304 (skipped),
        # git unchanged (skipped), git active-changes (differential
        # upsert ran above), and full-tree walk (snapshot/remote).
        # The skip_upsert flag overlaps the first three cases (set
        # whenever git_mirror is not None) so we can't gate on it
        # alone; explicitly disambiguate by transport state.
        snapshot_skipped = (snapshot is not None
                            and snapshot.is_unchanged())
        git_unchanged = (git_mirror is not None
                         and git_mirror.is_unchanged())
        if snapshot_skipped:
            done_msg = "Done — no upstream changes (304)"
        elif git_unchanged:
            done_msg = "Done — no new commits"
        else:
            # Full-tree walk OR git differential. Both populate
            # stats.movies_seen / tv_seen / new_count / updated_count.
            # v1.18.0: collections counted alongside movies/tv.
            done_msg = (
                f"Done — {stats.movies_seen + stats.tv_seen + stats.collections_seen} items, "
                f"{stats.new_count} new, {stats.updated_count} updated"
            )
        op_progress.update_progress(
            db_path, "tdb_sync",
            activity=done_msg,
        )
        # v1.13.17: stash the counts in detail_json so the drawer's
        # done-headline can render the full breakdown ("Done — 5177
        # items · 0 new · 0 updated") instead of the generic
        # "Done — N items processed". Mirrors what the docker log
        # line already shows + what the sync_runs row records.
        # v1.21.25: done_summary is now a uniform ordered [{l,v}] list the
        # drawer formats identically for every op kind ("Done — N items ·
        # X new · Y upd"). err part appended only when > 0.
        _ds = [
            {"l": "items", "v": stats.movies_seen + stats.tv_seen
                                + stats.collections_seen},
            {"l": "new", "v": stats.new_count},
            {"l": "upd", "v": stats.updated_count},
        ]
        if stats.errors:
            _ds.append({"l": "err", "v": stats.errors})
        op_progress.set_detail_field(
            db_path, "tdb_sync", "done_summary", _ds,
        )
        op_progress.finish_progress(db_path, "tdb_sync", status="done")
        return stats

    except Exception as e:
        # v1.11.77: persist partial stats on every termination path so
        # the dashboard 'Last Sync' panel + the TDB-pill gating see
        # how much of the upstream we actually captured before we
        # bailed. Pre-fix the exception handler only wrote the
        # status + error, leaving movies_seen / tv_seen / new_count
        # / updated_count at 0 — the user couldn't tell whether a
        # cancellation hit at item 1 or item 4000.
        #
        # _JobCancelled gets a distinct error string so the dashboard
        # can call out 'cancelled' separately from generic failures
        # without changing the sync_runs.status CHECK enum (which
        # would force a DB wipe in dev mode). Status stays 'failed'
        # for any non-clean exit; the error column distinguishes.
        from .worker import _JobCancelled
        err_text = ("cancelled by user"
                    if isinstance(e, _JobCancelled) else str(e))
        # v1.13.2 (#1): on the failure path read fallback_reason out of
        # op_progress.detail so a run that cascaded but THEN crashed
        # still records what the cascade was. Same shape as the
        # success path.
        fail_fallback_reason = None
        try:
            with get_conn(db_path) as conn:
                row = conn.execute(
                    "SELECT detail_json FROM op_progress WHERE op_id = 'tdb_sync'"
                ).fetchone()
            if row is not None and row["detail_json"]:
                _detail = json.loads(row["detail_json"])
                if _detail.get("fallback_active"):
                    fail_fallback_reason = _detail.get(
                        "fallback_reason") or "unknown"
        except Exception as _fr_e:
            # v1.17.1 (class 9): pre-fix swallowed JSON parse / DB
            # read failures during the on-failure fallback-reason
            # lookup. Functional fallback exists (fail_fallback_reason
            # stays None → the UPDATE writes NULL), but a log.debug
            # breadcrumb helps diagnose schema drift on the
            # op_progress.detail_json column.
            # v1.22.67 (audit round 2): renamed from `e` — Python
            # DELETES the except variable when its clause exits, so
            # rebinding `e` here UNBOUND the outer handler's `e`; when
            # this read failed (correlated DB duress), the
            # isinstance(e, _JobCancelled) below raised NameError,
            # masking the real sync error AND skipping finish_progress
            # → the tdb_sync op stuck 'running' forever.
            log.debug("fallback_reason detail_json read failed: %s", _fr_e)
        with get_conn(db_path) as conn:
            conn.execute(
                """UPDATE sync_runs SET
                       finished_at = ?, status = 'failed', error = ?,
                       movies_seen = ?, tv_seen = ?, collections_seen = ?,
                       new_count = ?, updated_count = ?,
                       fallback_reason = ?
                   WHERE id = ?""",
                (now_iso(), err_text,
                 stats.movies_seen, stats.tv_seen, stats.collections_seen,
                 stats.new_count, stats.updated_count,
                 fail_fallback_reason, run_id),
            )
        log_event(
            db_path,
            level="WARNING" if isinstance(e, _JobCancelled) else "ERROR",
            component="sync",
            message=f"Sync run #{run_id} {'cancelled' if isinstance(e, _JobCancelled) else 'failed'}: {err_text}",
            detail={
                "movies_seen": stats.movies_seen, "tv_seen": stats.tv_seen,
                "new": stats.new_count, "updated": stats.updated_count,
                "errors": stats.errors,
            },
        )
        op_progress.finish_progress(
            db_path, "tdb_sync",
            status="cancelled" if isinstance(e, _JobCancelled) else "failed",
            error_message=err_text,
        )
        raise
    finally:
        if snapshot is not None:
            snapshot.release()
        if git_mirror is not None:
            git_mirror.release()
