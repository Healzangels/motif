"""v0.51.271 — an edition replaced by a sibling edition is not a lost theme.

The operator's report: the extended cut of a film is removed and the theatrical
cut put in its place. motif keys placements/local_files/overrides per edition
(v1.21.52), so the old edition's row is reaped and the surviving row has no
theme — producing a "💔 Theme lost … no backup configured" alarm for a title
whose theme is sitting untouched in motif's own store.

Two facts reframe it:

  * The audio was never lost. The canonical lives at
    `<themes_dir>/<subdir>/Title (Year) {edition-<key>}/theme.mp3` — motif's
    store, not the media folder. Only the placement and the edition-keyed
    association break.
  * Edition separation exists to resolve AMBIGUITY (the edition-sibling bleed
    class), and ambiguity needs two editions. When exactly one survives, the
    edition key is a distinction without a difference — there is nothing to
    bleed into.

So this re-keys the theme onto the survivor, under guards that keep every case
edition separation actually protects untouched:

  1. exactly ONE surviving edition row for (media_type, tmdb_id, section_id) —
     two or more is the ambiguous case the brief's self-healing rules put out
     of scope, and motif must not guess;
  2. that survivor carries a DIFFERENT edition_key (else nothing swapped);
  3. the survivor has no theme of its own — never overwrite a theme the
     operator chose for that edition;
  4. the replacement is PRESENT, not merely the old row absent. A real swap
     shows both signals; a transient enumeration gap shows only the absence.
     A positive condition cannot mistake a mid-scan blip for a removal — the
     shape v1.22.8's sweep got wrong (see v0.51.264).

The filesystem move happens OUTSIDE the write transaction, and a failed move
aborts before any row is touched, so a partial run leaves the old edition
intact and the caller falls back to the (now swap-aware) notification.
"""
from __future__ import annotations

import logging
from pathlib import Path

from .canonical import canonical_theme_subdir
from .db import get_conn, transaction
from .events import log_event

log = logging.getLogger(__name__)


def find_surviving_edition(conn, media_type: str, tmdb_id: int,
                           section_id: str, lost_edition_key: str) -> dict | None:
    """The single surviving edition row for this title+section, or None.

    None when: nothing survives (a real removal), more than one survives (the
    ambiguous case), or the only survivor is the edition we just lost."""
    rows = conn.execute(
        "SELECT rating_key, edition_key, folder_path, title, year, has_theme "
        "  FROM plex_items "
        " WHERE media_type = ? AND section_id = ? AND guid_tmdb = ?",
        (_plex_media_type(media_type), section_id, tmdb_id),
    ).fetchall()
    if len(rows) != 1:
        return None                       # guard 1: zero, or ambiguous
    row = dict(rows[0])
    if (row["edition_key"] or "") == (lost_edition_key or ""):
        return None                       # guard 2: nothing actually swapped
    return row


def _plex_media_type(media_type: str) -> str:
    """themes' 'tv' is plex_items' 'show' (the mapping used across plex_enum)."""
    return "show" if media_type == "tv" else media_type


def survivor_already_themed(conn, media_type: str, tmdb_id: int,
                            section_id: str, survivor_edition: str) -> bool:
    """Guard 3. A theme the operator picked for the surviving edition wins."""
    return conn.execute(
        "SELECT 1 FROM local_files "
        " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
        "   AND COALESCE(edition_key, '') = ? "
        "UNION ALL "
        "SELECT 1 FROM user_overrides "
        " WHERE media_type = ? AND tmdb_id = ? AND COALESCE(edition_key, '') = ? "
        "LIMIT 1",
        (media_type, tmdb_id, section_id, survivor_edition,
         media_type, tmdb_id, survivor_edition),
    ).fetchone() is not None


def resolve_edition_swap(db_path: Path, themes_dir: Path, *, media_type: str,
                         tmdb_id: int, section_id: str,
                         lost_edition_key: str) -> dict | None:
    """Re-key a reaped edition's theme onto the single surviving edition.

    Returns a summary dict when it acted, else None (caller keeps its existing
    loss-notification path, which reads the same survivor to explain itself)."""
    with get_conn(db_path) as conn:
        survivor = find_surviving_edition(
            conn, media_type, tmdb_id, section_id, lost_edition_key)
        if survivor is None:
            return None
        new_key = survivor["edition_key"] or ""
        if survivor_already_themed(conn, media_type, tmdb_id, section_id, new_key):
            return None
        lf = conn.execute(
            "SELECT file_path FROM local_files "
            " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
            "   AND COALESCE(edition_key, '') = ?",
            (media_type, tmdb_id, section_id, lost_edition_key or ""),
        ).fetchone()
        if lf is None or not lf["file_path"]:
            return None                   # nothing of ours to carry over

    # Move the canonical FIRST and outside any transaction — a write lock held
    # across filesystem work is the standing prohibition, and a failed move must
    # leave every row untouched.
    # Name the new canonical from the SURVIVOR's own title/year — that is what
    # Plex lists now, and it keeps the folder consistent with what a fresh
    # download for this row would compute.
    title = survivor["title"] or ""
    year = survivor["year"]
    old_rel = lf["file_path"]
    old_abs = themes_dir / old_rel
    new_rel = str(Path(old_rel).parent.parent
                  / canonical_theme_subdir(title, year, new_key)
                  / Path(old_rel).name)
    new_abs = themes_dir / new_rel
    if old_rel != new_rel:
        try:
            if not old_abs.exists():
                return None               # canonical already gone: a real loss
            new_abs.parent.mkdir(parents=True, exist_ok=True)
            if new_abs.exists():
                return None               # never clobber an existing canonical
            old_abs.replace(new_abs)
        except OSError as e:
            log.warning(
                "edition-swap: canonical move failed for %s/%s (%s → %s): %s — "
                "leaving the old edition intact",
                media_type, tmdb_id, old_rel, new_rel, e)
            return None

    with get_conn(db_path) as conn, transaction(conn):
        for table in ("local_files", "placements"):
            conn.execute(
                f"UPDATE {table} SET edition_key = ? "
                " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                "   AND COALESCE(edition_key, '') = ?",
                (new_key, media_type, tmdb_id, section_id, lost_edition_key or ""))
        # user_overrides is title-global on section but still edition-keyed.
        conn.execute(
            "UPDATE user_overrides SET edition_key = ? "
            " WHERE media_type = ? AND tmdb_id = ? "
            "   AND COALESCE(edition_key, '') = ?",
            (new_key, media_type, tmdb_id, lost_edition_key or ""))
        conn.execute(
            "UPDATE local_files SET file_path = ? "
            " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
            "   AND COALESCE(edition_key, '') = ?",
            (new_rel, media_type, tmdb_id, section_id, new_key))
    summary = {
        "media_type": media_type, "tmdb_id": tmdb_id, "section_id": section_id,
        "from_edition": lost_edition_key or "", "to_edition": new_key,
        "rating_key": survivor["rating_key"], "file_path": new_rel,
    }
    log_event(
        db_path, level="INFO", component="plex-enum",
        media_type=media_type, tmdb_id=tmdb_id, section_id=section_id,
        message=(f"Edition swap: carried the theme for {title!r} from "
                 f"{lost_edition_key or 'standard'!r} to "
                 f"{new_key or 'standard'!r} — the replaced edition was the "
                 f"only one in this section, so nothing was lost"),
        detail=summary,
    )
    return summary
