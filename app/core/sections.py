"""
Plex section discovery cache.

On startup and after each sync, motif enumerates Plex library sections
via /library/sections, applies the include/exclude rules from settings,
and caches the result in the plex_sections table. The table is the
single source of truth for "which sections do we manage" — the cached
metadata also drives the /libraries page in the UI.

Sections that disappear from Plex (deleted libraries) are kept in the
cache with stale last_seen_at; the UI shows them dimmed. The user can
manually purge them.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from .db import get_conn
from .plex import PlexClient, PlexSection

log = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def refresh_sections(
    db_path: Path,
    plex: PlexClient,
    *,
    excluded_titles: set[str],
    included_titles: set[str],
) -> list[dict]:
    """Discover sections from Plex, apply include/exclude rules, persist
    to plex_sections, and return the current set of managed sections.

    Rules:
    - If included_titles is non-empty, only sections with matching titles are
      included; everything else is excluded.
    - Otherwise, all sections are included EXCEPT those in excluded_titles.
    - Sections seen in this sync get last_seen_at updated.
    - Sections not seen are left untouched (stale rows kept for UI).
    """
    discovered: list[PlexSection] = plex.discover_sections()
    log.info("Plex returned %d sections", len(discovered))

    now = _now()
    sections_with_state: list[dict] = []

    with get_conn(db_path) as conn:
        for s in discovered:
            if included_titles:
                included = s.title in included_titles
            else:
                included = s.title not in excluded_titles

            existing = conn.execute(
                "SELECT included FROM plex_sections WHERE section_id = ?",
                (s.section_id,),
            ).fetchone()

            if existing is None:
                conn.execute(
                    """INSERT INTO plex_sections
                       (section_id, uuid, title, type, agent, language,
                        location_paths, included, discovered_at, last_seen_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (s.section_id, s.uuid, s.title, s.type, s.agent, s.language,
                     json.dumps(s.location_paths), 1 if included else 0,
                     now, now),
                )
            else:
                # Preserve any user-driven include/exclude override that may
                # have been set in the UI; only update metadata that came
                # from Plex. The env-var rules apply only on FIRST DISCOVERY.
                conn.execute(
                    """UPDATE plex_sections SET
                          uuid = ?, title = ?, type = ?, agent = ?, language = ?,
                          location_paths = ?, last_seen_at = ?
                       WHERE section_id = ?""",
                    (s.uuid, s.title, s.type, s.agent, s.language,
                     json.dumps(s.location_paths), now, s.section_id),
                )

        # Read everything back for return
        rows = conn.execute(
            "SELECT * FROM plex_sections ORDER BY title COLLATE NOCASE"
        ).fetchall()
        sections_with_state = [dict(r) for r in rows]

    return sections_with_state


def get_managed_section_ids(db_path: Path, *, media_type: str | None = None) -> list[str]:
    """Return the section IDs motif should currently manage.
    If media_type is given, filters to that type."""
    with get_conn(db_path) as conn:
        if media_type is None:
            rows = conn.execute(
                "SELECT section_id FROM plex_sections WHERE included = 1"
            ).fetchall()
        else:
            plex_type = "movie" if media_type == "movie" else "show"
            rows = conn.execute(
                "SELECT section_id FROM plex_sections WHERE included = 1 AND type = ?",
                (plex_type,),
            ).fetchall()
    return [r["section_id"] for r in rows]


def list_sections(db_path: Path) -> list[dict]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM plex_sections ORDER BY type, title COLLATE NOCASE"
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["location_paths"] = json.loads(d.get("location_paths") or "[]")
        except (TypeError, ValueError):
            d["location_paths"] = []
        out.append(d)
    return out


def set_section_inclusion(
    db_path: Path, section_id: str, included: bool, *, updated_by: str = "user"
) -> bool:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "UPDATE plex_sections SET included = ? WHERE section_id = ?",
            (1 if included else 0, section_id),
        )
    return cur.rowcount > 0
