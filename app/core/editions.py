"""The per-edition discriminator chokepoint (v1.21.53).

Since schema v63 (v1.21.52) motif keys theme state by
(media_type, tmdb_id, section_id, edition_key). edition_key is the
NORMALIZED Plex edition tag from the media folder: {edition-Theatrical}
-> 'theatrical'; no tag -> '' (the standard edition). It is folder-derived
on purpose — stable across Plex's remove+re-add churn (the v1.18.90
reaper mints a fresh rating_key, which would orphan rating_key-keyed
state) and it shares placement.py's matching vocabulary, so state keys
and the placement matcher speak one language.

EVERY write/read of edition_key routes through here. No call site may
default edition_key to '' inline — that is the silent-wrong-classification
class (writing an Extended URL onto Theatrical). The '' for an untagged
folder is returned HERE, deliberately, in one place.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from .normalize import normalize_edition, parse_folder_name

log = logging.getLogger("editions")

# v1.22.1 (code-review): warn-once-then-debug for the rk-miss breadcrumb below.
# edition_key_for_rating_key is called once-per-row inside the bulk-download
# loop (api.py), so an unconditional log.warning floods the log when a batch
# carries N stale rks (the v1.18.90 Plex re-add scenario). First miss warns
# (operator sees it at boot); the rest drop to debug. Resets on process
# restart — matching the cadence at which a deploy fix lands (v1.17.11 rule).
_RK_MISS_WARNED: bool = False


def edition_key_for_basename(folder_basename: str) -> str:
    """Canonical derivation: normalize the {edition-X} tag out of a folder
    BASENAME. Mirrors placement.py FolderEntry.edition_norm and the v62
    placements backfill exactly (same parse + normalize), so state keys and
    the placement matcher agree. '' for an untagged or empty basename."""
    if not folder_basename:
        return ""
    return normalize_edition(parse_folder_name(folder_basename).editions_raw)


def edition_key_for_folder(folder_path: str | None) -> str:
    """edition_key from a media folder PATH (absolute or relative) — takes
    the basename first. '' for None/'' which is the plex_upload / collection
    sentinel (no folder on disk -> standard edition)."""
    if not folder_path:
        return ""
    return edition_key_for_basename(Path(folder_path).name)


# v1.21.76: DISPLAY labels (proper-case, for notifications + the INFO card)
# live in the chokepoint alongside the normalized state KEYS so the
# {edition-X} parse has exactly one home. edition_label_* preserves the
# operator's folder casing ("Sam Takes a Step"); edition_key_* normalizes
# it for state keys ("sam takes a step"). Never mix the two.
_SMALL_WORDS = {
    "a", "an", "and", "as", "at", "but", "by", "for", "from", "in", "nor",
    "of", "on", "or", "the", "to", "vs", "via", "with",
}


def prettify_edition_key(edition_key: str) -> str:
    """Title-case a NORMALIZED edition_key for display, keeping small words
    lowercase (except the first) — 'sam takes a step' -> 'Sam Takes a Step'.
    Only a FALLBACK for when no raw {edition-X} folder label is available
    (the folder casing is always preferred via edition_label_for_folder)."""
    words = edition_key.split()
    out: list[str] = []
    for i, w in enumerate(words):
        if i != 0 and w.lower() in _SMALL_WORDS:
            out.append(w.lower())
        else:
            out.append(w[:1].upper() + w[1:])
    return " ".join(out)


def edition_label_for_basename(folder_basename: str) -> str:
    """The RAW, proper-case edition label(s) for DISPLAY from a folder
    BASENAME — 'Extended Edition', 'Sam Takes a Step'. '' for an untagged
    basename. parse_folder_name already drops GUID-hint {imdb-…}/{tmdb-…}
    tags (they are not editions). Multiple {edition-X} tags join with ' · '."""
    if not folder_basename:
        return ""
    raw = parse_folder_name(folder_basename).editions_raw  # 'edition-X|edition-Y'
    if not raw:
        return ""
    labels = []
    for tag in raw.split("|"):
        labels.append(
            tag[len("edition-"):] if tag.lower().startswith("edition-") else tag
        )
    return " · ".join(lbl for lbl in labels if lbl)


def edition_label_for_folder(folder_path: str | None) -> str:
    """edition_label from a media folder PATH — basename first. '' for
    None/'' (no folder on disk = standard edition)."""
    if not folder_path:
        return ""
    return edition_label_for_basename(Path(folder_path).name)


def edition_key_for_rating_key(
    conn: sqlite3.Connection, rating_key: str | None
) -> str:
    """Resolve a Plex rating_key -> its plex_items.folder_path ->
    edition_key. The UI sends rating_key on every per-row action; this lets
    the backend learn which edition the click targeted without the UI ever
    knowing about edition_key (the churn-proof contract). '' when the rk is
    unknown or its folder is untagged/empty."""
    if not rating_key:
        return ""
    row = conn.execute(
        "SELECT folder_path FROM plex_items WHERE rating_key = ?",
        (rating_key,),
    ).fetchone()
    if row is None:
        # v1.22.0 (line-close audit): a truthy rk that doesn't resolve to a
        # plex_items row falls back to '' (standard/shared scope) — which is
        # indistinguishable from a genuine standard-edition click. Breadcrumb
        # so an edition-scoped action mis-scoped to '' (e.g. after a Plex
        # re-add minted a new rk) is diagnosable, not silent.
        # v1.22.1 (code-review): warn-once (hot-path: per-row in bulk loop).
        global _RK_MISS_WARNED
        if not _RK_MISS_WARNED:
            _RK_MISS_WARNED = True
            log.warning(
                "edition_key_for_rating_key: rk=%s not in plex_items — "
                "defaulting to '' (standard/shared scope). Further misses "
                "this process log at debug.", rating_key,
            )
        else:
            log.debug(
                "edition_key_for_rating_key: rk=%s not in plex_items — "
                "defaulting to ''", rating_key,
            )
        return ""
    return edition_key_for_folder(row[0])
