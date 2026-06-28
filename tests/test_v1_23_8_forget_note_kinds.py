"""v1.23.8 — placement-kind-aware FORGET notification note.

the user on the theme_forgotten notification: "wondering what the
0 placement(s) unlinked from Plex means." His row's only placement
was a plex_upload (theme in Plex's metadata bundle, no sidecar) —
removed via the v1.18.60 API teardown and counted in api_deleted,
which the note never surfaced. The old copy only showed the SIDECAR
unlink counter, so upload-placed rows always read "0 placement(s)
unlinked" even when the theme WAS removed from Plex.

The note now builds from both counters: "N Plex upload(s) removed"
when the API teardown deleted any, "N placement(s) unlinked from
Plex" when sidecar files were deleted, "no placements to remove"
when neither — always followed by the RESTORE-available / TDB-
tracking-preserved tail.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _note_block() -> str:
    i = API_PY.index("v1.23.8: placement-kind-aware note")
    return API_PY[i:i + 1400]


def test_note_surfaces_both_counters():
    block = _note_block()
    assert 'f"{api_deleted} Plex upload(s) removed")' in block
    assert ('f"{unlinked} placement(s) unlinked from Plex")'
            in block)
    assert '"no placements to remove"' in block
    # the RESTORE / TDB tail survives on the joined note.
    assert '" · RESTORE available" if is_orphan' in block


def test_zero_only_copy_retired():
    """The unconditional f-string that always led with the sidecar
    counter (the "0 placement(s) unlinked" confusion on upload-
    placed rows) must not survive anywhere."""
    assert ('extra = (\n                f"{unlinked} placement(s) '
            'unlinked from Plex"') not in API_PY


def test_note_branches_evaluate():
    """Exercise the three note shapes with the same expression the
    endpoint builds (kept in lockstep by the source pin above)."""
    def build(api_deleted, unlinked, is_orphan=True):
        parts = []
        if api_deleted:
            parts.append(f"{api_deleted} Plex upload(s) removed")
        if unlinked:
            parts.append(f"{unlinked} placement(s) unlinked from Plex")
        if not parts:
            parts.append("no placements to remove")
        return " · ".join(parts) + (
            " · RESTORE available" if is_orphan
            else " · TDB tracking preserved")

    assert build(1, 0) == "1 Plex upload(s) removed · RESTORE available"
    assert build(0, 2) == ("2 placement(s) unlinked from Plex "
                           "· RESTORE available")
    assert build(1, 2) == ("1 Plex upload(s) removed · 2 placement(s) "
                           "unlinked from Plex · RESTORE available")
    assert build(0, 0, is_orphan=False) == (
        "no placements to remove · TDB tracking preserved")
