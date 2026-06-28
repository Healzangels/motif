"""
NFO file parser — Sonarr/Radarr/Plex-style metadata sidecar files.

motif uses these to identify orphan themes (theme.mp3 files in Plex folders
that don't match any ThemerrDB record). When the .arr stack manages a folder,
it leaves a .nfo file with the canonical TMDB/IMDB/TVDB IDs, which lets us
adopt orphans without hitting an external API.

Movie .nfo example:
    <movie>
      <title>Inception</title>
      <year>2010</year>
      <imdbid>tt1375666</imdbid>
      <tmdbid>27205</tmdbid>
      <uniqueid type="imdb" default="true">tt1375666</uniqueid>
      <uniqueid type="tmdb">27205</uniqueid>
    </movie>

TV .nfo example:
    <tvshow>
      <title>Breaking Bad</title>
      <year>2008</year>
      <imdb_id>tt0903747</imdb_id>
      <tvdbid>81189</tvdbid>
      <uniqueid type="tmdb">1396</uniqueid>
    </tvshow>

We're tolerant of variations: tag names sometimes differ, <uniqueid> blocks
sometimes omit values, files may be malformed. Parsing failures return None
rather than raise — orphans without metadata are handled separately.
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

log = logging.getLogger(__name__)

NfoKind = Literal["movie", "tv"]


@dataclass
class NfoMetadata:
    """Normalized metadata extracted from a .nfo file. Any field can be None."""
    kind: NfoKind
    title: Optional[str] = None
    year: Optional[str] = None
    tmdb_id: Optional[int] = None
    tvdb_id: Optional[int] = None
    imdb_id: Optional[str] = None       # tt1234567 form

    def has_any_id(self) -> bool:
        return any([self.tmdb_id, self.tvdb_id, self.imdb_id])


# Filenames we'll look for in a media folder. Order is preference: a
# `<folder>.nfo` (Radarr default for movies) is preferred over generic
# `movie.nfo`. For TV, `tvshow.nfo` is the only standard.
_MOVIE_NFO_PATTERNS = ["{stem}.nfo", "movie.nfo"]
_TV_NFO_PATTERNS = ["tvshow.nfo"]


def find_nfo(folder: Path, kind: NfoKind) -> Optional[Path]:
    """Locate the most-preferred .nfo file in a media folder. Returns None if
    the folder has no recognizable .nfo file."""
    if not folder.is_dir():
        return None

    if kind == "movie":
        for pattern in _MOVIE_NFO_PATTERNS:
            candidate = folder / pattern.format(stem=folder.name)
            if candidate.is_file():
                return candidate
        # Last resort: any .nfo file in the directory
        for f in folder.iterdir():
            if f.suffix.lower() == ".nfo" and f.is_file():
                return f
    else:  # tv
        for pattern in _TV_NFO_PATTERNS:
            candidate = folder / pattern
            if candidate.is_file():
                return candidate
    return None


_IMDB_ID_RE = re.compile(r"^tt\d{6,}$")


def _coerce_imdb(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if _IMDB_ID_RE.match(s):
        return s
    # Some nfo files have just the digits: '1375666' → 'tt1375666'
    if s.isdigit():
        return f"tt{s}"
    return None


def _coerce_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = s.strip()
    try:
        v = int(s)
        return v if v > 0 else None
    except ValueError:
        return None


def _coerce_year(s: Optional[str]) -> Optional[str]:
    """Year can be '2010' or '2010-07-16' or 'July 2010'. Pull the first 4-digit
    year-like substring."""
    if not s:
        return None
    m = re.search(r"(19\d{2}|20\d{2})", s)
    return m.group(1) if m else None


def parse_nfo(path: Path, kind: NfoKind) -> Optional[NfoMetadata]:
    """Parse an .nfo file. Returns None on any failure (file missing,
    malformed XML, unrecognized root element). Logging is at DEBUG so a
    bad nfo doesn't spam logs at scale."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        log.debug("Cannot read %s: %s", path, e)
        return None

    # Some nfo writers prepend a URL or other text before the XML. ElementTree
    # is strict; clip everything before the first '<'.
    cut = text.find("<")
    if cut > 0:
        text = text[cut:]
    if not text.strip().startswith("<"):
        return None

    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        log.debug("Cannot parse %s as XML: %s", path, e)
        return None

    expected_root = "movie" if kind == "movie" else "tvshow"
    if root.tag.lower() != expected_root:
        # Some Plex bundles use a different root tag at the top, but commonly
        # the expected root is the immediate first child.
        candidate = root.find(expected_root)
        if candidate is not None:
            root = candidate
        else:
            log.debug("Unexpected root tag in %s: %s (wanted %s)",
                      path, root.tag, expected_root)
            return None

    md = NfoMetadata(kind=kind)
    md.title = _text_of(root, "title", "originaltitle")
    md.year = _coerce_year(_text_of(root, "year", "premiered", "releasedate"))

    # IDs from "obvious" tags first
    md.imdb_id = _coerce_imdb(_text_of(root, "imdbid", "imdb_id", "imdb"))
    md.tmdb_id = _coerce_int(_text_of(root, "tmdbid", "tmdb_id", "tmdb"))
    md.tvdb_id = _coerce_int(_text_of(root, "tvdbid", "tvdb_id", "tvdb"))

    # Then <uniqueid type="..."> blocks, which may override or supplement
    for uid in root.findall("uniqueid"):
        t = (uid.get("type") or "").strip().lower()
        v = (uid.text or "").strip()
        if not v:
            continue
        if t in ("imdb",) and not md.imdb_id:
            md.imdb_id = _coerce_imdb(v)
        elif t in ("tmdb", "themoviedb") and not md.tmdb_id:
            md.tmdb_id = _coerce_int(v)
        elif t in ("tvdb", "thetvdb") and not md.tvdb_id:
            md.tvdb_id = _coerce_int(v)

    return md


def _text_of(root: ET.Element, *tags: str) -> Optional[str]:
    """Return the first non-empty text value from any of the given tags."""
    for tag in tags:
        el = root.find(tag)
        if el is not None and el.text:
            t = el.text.strip()
            if t:
                return t
    return None
