"""v1.15.101 — instrumentation for the provenance="manual" mystery.

the user's "A Dog's Journey" repro showed local_files.provenance
stuck at "manual" + empty source_video_id after multiple
REPLACE TDB calls. The worker's _do_download SHOULD see
override=None for REPLACE TDB (since the upstream
replace_with_themerrdb() deletes user_overrides for the row's
section before enqueueing the download), so the expected
result is provenance="auto" / source_kind="themerrdb".

The mystery: what override URL did the worker actually see?
Without instrumentation, the only state visible at debug-time
is the DB AFTER the fact. v1.15.101 adds an INFO-level log at
the decision point so the next reproduction has a paper trail.

## What the log line shows

When a download writes `provenance="manual"`, motif now logs:

  _do_download: provenance='manual' for movie/522518 section=1
    (override section=1, url=https://youtube.com/watch?v=...)

The four pieces of info answer:
* WHICH row (media_type/tmdb_id)
* WHICH section's download is running
* WHICH section_id the override came from (could differ from
  the download section — '' fallback path)
* WHICH URL the override carried — verifying it's a real
  user URL and not a stale TDB URL

## Why this matters beyond the user's specific row

The same diagnostic surface helps if ANYONE reports
"provenance is wrong" — they share the log line, we read
the override state, we know whether to chase the worker's
override lookup, the upstream delete, or a different code
path.

## Diagnostic doc

`docs/DIAGNOSTICS.md` carries the full investigation
checklist + 8 SQL queries the user can run if the symptom
reproduces. Standalone doc so the mystery + queries are
discoverable without grepping the journal.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_record_local_file_has_provenance_manual_log():
    """The `_do_download` block in worker.py must log at INFO
    level when provenance is computed as 'manual'. The log
    captures the override's section_id + URL so the next
    repro of the provenance mystery has visible breadcrumbs.

    Pin both the level (info, not debug) and the message
    shape so a future refactor can't silently demote it."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # The instrumentation lives in _do_download — anchor on
    # the v1.15.101 marker so we capture the relevant block.
    anchor = src.index("v1.15.101: log the decision INPUTS")
    block = src[anchor:anchor + 1500]
    assert "log.info" in block, (
        "v1.15.101: the provenance='manual' log must use "
        "log.info, not log.debug. log.debug hides the line in "
        "default production logs (INFO level)."
    )
    assert "provenance='manual'" in block, (
        "v1.15.101: log message must include `provenance='manual'` "
        "as a literal so grep-by-string works."
    )
    assert "override section=" in block, (
        "v1.15.101: log message must include the override's "
        "section_id so the operator can verify the '' fallback "
        "path didn't pick up a stale row."
    )


def test_diagnostics_doc_exists():
    """`docs/DIAGNOSTICS.md` must exist with the provenance
    mystery checklist + queries. This is the operator-facing
    surface for if/when the symptom reproduces."""
    diag = REPO / "docs" / "DIAGNOSTICS.md"
    assert diag.exists(), (
        "v1.15.101: docs/DIAGNOSTICS.md must exist as the "
        "discoverable home for hard-to-repro bug checklists. "
        "Pre-v1.15.101 the provenance mystery was only "
        "documented in DESIGN_SYSTEM.md's gap table — operators "
        "had to grep journal + DESIGN_SYSTEM to find the "
        "diagnostic context."
    )


def test_diagnostics_doc_has_all_queries():
    """The 8 diagnostic SQL queries must each appear in the
    doc. If a query was accidentally removed, root-causing
    a future repro would have to reconstruct it."""
    diag = (REPO / "docs" / "DIAGNOSTICS.md").read_text()
    # Each query has a numbered comment marker.
    for n in range(1, 9):
        marker = f"-- {n}."
        assert marker in diag, (
            f"v1.15.101: docs/DIAGNOSTICS.md missing query #{n} "
            f"(anchor `{marker}` not found). The 8-query "
            f"checklist is the operator's one-shot for "
            f"root-causing the provenance mystery."
        )


def test_diagnostics_doc_pins_a_dogs_journey_tmdb_id():
    """The doc references the specific row (movie/522518) that
    the user reported. Pin so a copy-paste edit that strips the
    tmdb_id by mistake fails fast."""
    diag = (REPO / "docs" / "DIAGNOSTICS.md").read_text()
    assert "522518" in diag, (
        "v1.15.101: docs/DIAGNOSTICS.md should reference the "
        "specific tmdb_id from the user's repro (522518). Future "
        "operators can use it as a copy-paste anchor."
    )
