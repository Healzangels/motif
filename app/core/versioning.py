"""v0.51.231 — one definition of "is that release NEWER than what we run".

Two sites answered this question and disagreed. api.py's `/api/release/latest` parsed both
tags into int tuples and compared (`lat_t > cur_t`); scheduler.py's release_available
notification compared the tag STRINGS for inequality. motif's own channel model makes the
running version routinely AHEAD of the latest GitHub *Release* (nightly is the dev branch,
release holds the last shipped tag), so `!=` fired an "upgrade available" push pointing
BACKWARDS — and its tag-keyed dedupe means it fires once per release and never
self-corrects. The topbar correctly stayed silent because it used the other rule.

Shared rather than duplicated because the drift is demonstrated, not hypothetical: this is
the exact mirror-drift class CLAUDE.md's SRC-axis notes describe, and a `!=` reimplementation
is easy to write again by accident.
"""
from __future__ import annotations

import re


def parse_version(v: str) -> tuple[int, ...] | None:
    """'v1.13.8-rc1' -> (1, 13, 8). None when there is no leading numeric part.

    Tolerates the optional 'v' prefix (GitHub tags carry it, __version__ does not) and
    drops any pre-release / build metadata after the first non-numeric character."""
    v = (v or "").strip().lstrip("v")
    m = re.match(r"^(\d+(?:\.\d+)*)", v)
    if not m:
        return None
    try:
        return tuple(int(x) for x in m.group(1).split("."))
    except ValueError:
        return None


def is_newer(candidate: str, running: str) -> bool:
    """True only when `candidate` parses to a STRICTLY greater version than `running`.

    Unparseable input on either side is False — never nag on a tag we cannot read, which
    also covers the tag-format drift case (a release tagged '0.51.227' with no 'v' prefix
    compared unequal to 'v0.51.227' under the old string check)."""
    cur = parse_version(running)
    cand = parse_version(candidate)
    return bool(cur and cand and cand > cur)
