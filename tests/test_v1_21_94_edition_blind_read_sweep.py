"""v1.21.94 — guard against edition-blind (mt, tmdb, section) state reads.

The per-edition isolation arc widened the PK of local_files / user_overrides /
pending_updates / placements with edition_key and routed WRITES through it. But
a READ was missed: worker._do_download resolved the override URL by (media_type,
tmdb_id, section_id) with no edition_key filter, so REPLACE TDB on one edition
pulled a sibling edition's URL (v1.21.92). This is plan risk #3 — a call-site
silently defaulting edition to '' — realized on a read.

This lint walks every SELECT/JOIN of the four edition-keyed state tables in the
worker/sync/scheduler/plex_enum hot paths. Any read that ALSO filters by
section_id (i.e. it's keyed by the (mt, tmdb, section) tuple, the level at which
editions diverge) MUST either carry an edition_key conjunct or an explicit
`edition-blind OK` marker explaining why the blind read is safe (title-global
detection, notify-only, media_folder-discriminated, etc.). A future read that
silently omits edition_key fails this test until it's scoped or justified.

(mt, tmdb)-only reads — no section_id — are title-global by construction and are
not in scope; the marker requirement only attaches to section-keyed reads.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
FILES = [
    "app/core/worker.py",
    "app/core/sync.py",
    "app/core/scheduler.py",
    "app/core/plex_enum.py",
]
STATE_TABLES = ("local_files", "user_overrides", "pending_updates", "placements")
_READ_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+(?:" + "|".join(STATE_TABLES) + r")\b")


def _section_keyed_reads():
    """Yield (file, line_no, window) for every state-table READ whose enclosing
    SQL statement filters by section_id."""
    for rel in FILES:
        lines = (REPO / rel).read_text().split("\n")
        for idx, line in enumerate(lines):
            if not _READ_RE.search(line):
                continue
            if "DELETE FROM" in line:
                continue  # a write, not a read
            # Bound the window to the enclosing execute(...).fetch() statement,
            # plus a few preceding comment lines (where OK-markers live).
            start = idx
            for j in range(idx, max(-1, idx - 25), -1):
                if ".execute(" in lines[j]:
                    start = j
                    break
            end = None
            for j in range(idx, min(len(lines), idx + 50)):
                if ".fetchone()" in lines[j] or ".fetchall(" in lines[j]:
                    end = j
                    break
            if end is None:
                end = min(len(lines) - 1, idx + 24)
            window = "\n".join(lines[max(0, start - 10):end + 1])
            if re.search(r"\bsection_id\b", window):
                yield rel, idx + 1, window


def test_section_keyed_state_reads_carry_edition_key_or_marker():
    offenders = []
    for rel, line_no, window in _section_keyed_reads():
        if "edition_key" in window or "edition-blind OK" in window:
            continue
        offenders.append(f"{rel}:{line_no}")
    assert not offenders, (
        "These (mt, tmdb, section) state-table reads neither carry an "
        "edition_key conjunct nor an `edition-blind OK: <reason>` marker — a "
        "read that defaults edition to '' silently bleeds across editions "
        "(plan risk #3 / v1.21.92). Scope the read to edition_key, or add the "
        "marker if it's genuinely title-global/notify-only:\n  "
        + "\n  ".join(offenders))


def test_sweep_actually_finds_reads():
    """Guard the guard: if a refactor moves all the SQL out of these files (or
    the regex breaks), the lint must not pass vacuously."""
    found = list(_section_keyed_reads())
    assert len(found) >= 5, (
        f"expected several section-keyed state reads, found {len(found)} — "
        "the sweep regex or file list likely drifted")


def test_known_fixed_reads_are_edition_scoped():
    """Anchor the intent: the reads fixed in v1.21.92/.94 must stay scoped."""
    worker = (REPO / "app" / "core" / "worker.py").read_text()
    # v1.21.92: _do_download override resolution.
    dl = worker.index("_do_download: resolve the download's edition") \
        if "_do_download: resolve the download's edition" in worker else None
    # The override SELECT must filter edition_key (hoisted _dl_edition_key).
    assert "AND edition_key = ?" in worker
    # v1.21.94: _do_relink join carries edition_key.
    assert "AND lf.edition_key = p.edition_key" in worker
    # v1.21.94: plex_enum reconciler join carries edition_key.
    enum = (REPO / "app" / "core" / "plex_enum.py").read_text()
    assert "AND pi.edition_key = p.edition_key" in enum


def test_v1_21_94_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
