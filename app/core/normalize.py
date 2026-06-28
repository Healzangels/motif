"""
Title and edition normalization. Ported faithfully from the user's merge-themes.sh
v4.5.7 script. Behavior must match the Bash version because users have already
organized their library around it.

Key behaviors preserved:
- Edition tags in {edition-...} or any {tag} suffixes
- Title (Year) parsing
- '+' handling with three modes: separator | word | literal
- Roman numeral and word-number → arabic conversion
- 'vs' / 'v' → 'versus'
- '&' → 'and'
- Lowercase + strip non-alphanumeric

The `parse_folder_name` function handles the same input format as the Bash script:
  "Movie Title (2023)"
  "Movie Title (2023) {edition-Director's Cut}"
  "Movie Title (2023) {edition-Theatrical} {edition-IMAX}"
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

PlusMode = Literal["separator", "word", "literal"]


_ROMAN_MAP = {
    "i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6, "vii": 7,
    "viii": 8, "ix": 9, "x": 10, "xi": 11, "xii": 12, "xiii": 13,
    "xiv": 14, "xv": 15, "xvi": 16, "xvii": 17, "xviii": 18, "xix": 19, "xx": 20,
}

_WORDNUM_MAP = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
    "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
    "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16,
    "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
}


@dataclass
class ParsedFolder:
    title: str
    year: str  # "" if absent
    editions_raw: str  # pipe-joined raw tags, "" if none

    @property
    def has_year(self) -> bool:
        return bool(self.year)


# v1.10.25: Plex's folder format also uses {imdb-...}/{tmdb-...}/{tvdb-...}
# tags as GUID hints for its own scanner. They look like edition tags
# but semantically aren't — folders like 'Foo (2024) {imdb-tt12345}'
# are the base edition, not a Director's Cut. Pre-1.10.25 they got
# captured as editions, giving the placement engine edition_norm
# 'imdb tt12345'; ThemerrDB-downloaded themes have empty edition_norm
# so the title/year/edition lookup never matched and placement
# logged 'Skipped placement: no matching folder'.
_GUID_TAG_PREFIXES = ("imdb-", "tmdb-", "tvdb-", "plex-", "agentid-")


def parse_folder_name(folder_basename: str) -> ParsedFolder:
    """Mirrors parse_title_year_edition() in merge-themes.sh."""
    base = folder_basename
    editions: list[str] = []
    # Strip any number of trailing {tag} groups. GUID-style tags are
    # dropped entirely — they aren't editions.
    while True:
        m = re.match(r"^(.*)\{([^}]*)\}\s*$", base)
        if not m:
            break
        tag = m.group(2)
        if not tag.lower().startswith(_GUID_TAG_PREFIXES):
            editions.insert(0, tag)
        base = m.group(1).rstrip()

    m = re.match(r"^(.+)\s+\((\d{4})\)$", base)
    if m:
        title = m.group(1)
        year = m.group(2)
    else:
        title = base
        year = ""

    return ParsedFolder(title=title, year=year, editions_raw="|".join(editions))


def normalize_edition(raw: str) -> str:
    """Mirrors normalize_edition() in merge-themes.sh."""
    e = (raw or "").lower()
    if e.startswith("edition-"):
        e = e[len("edition-"):]
    e = re.sub(r"[^a-z0-9]+", " ", e)
    e = re.sub(r"\s+", " ", e).strip()
    return e


def normalize_title(raw: str, plus_mode: PlusMode = "separator") -> str:
    """Mirrors normalize_title() in merge-themes.sh."""
    t = (raw or "").lower()
    t = t.replace("&", " and ")

    if plus_mode == "separator":
        t = t.replace("+", " ")
    elif plus_mode == "word":
        t = t.replace("+", " plus ")
    elif plus_mode == "literal":
        t = t.replace("+", " __plus__ ")

    # Versus normalization (handle word boundaries)
    t = re.sub(r"\s+vs\.\s+", " versus ", t)
    t = re.sub(r"\s+vs\s+", " versus ", t)
    t = re.sub(r"\s+v\.\s+", " versus ", t)
    t = re.sub(r"\s+v\s+", " versus ", t)

    # Strip everything except alphanumerics and underscore
    t = re.sub(r"[^a-z0-9_]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    out_tokens: list[str] = []
    for tok in t.split():
        if tok in ("vs", "v"):
            out_tokens.append("versus")
            continue
        if tok in _ROMAN_MAP:
            out_tokens.append(str(_ROMAN_MAP[tok]))
            continue
        if tok in _WORDNUM_MAP:
            out_tokens.append(str(_WORDNUM_MAP[tok]))
            continue
        out_tokens.append(tok)

    out = " ".join(out_tokens)
    if plus_mode == "literal":
        out = out.replace("__plus__", "+")
    return out


def titles_equal(a: str, b: str, plus_mode: PlusMode = "separator") -> bool:
    return normalize_title(a, plus_mode) == normalize_title(b, plus_mode)


def editions_equal(a: str, b: str) -> bool:
    return normalize_edition(a) == normalize_edition(b)
