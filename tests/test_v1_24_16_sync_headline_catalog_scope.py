"""v1.24.16 — sync summary headline scoped to "no CATALOG changes".

the user's repro: a Discord sync notification read "Motif sync — no changes
(6194 checked) · ✅ Sync complete" and then, right after, "🎵 Theme added —
DragonHeart (1996)". That looks self-contradictory, but isn't: the headline
reflects ONLY the ThemerrDB catalog diff (new/updated themes), while the
theme_added came from the post-sync plex_enum auto-theming a newly-seen UNTHEMED
Plex row from a PRE-EXISTING catalog theme. The enum is enqueued as separate
jobs that run AFTER the summary dispatch (worker.py), so the headline genuinely
can't know about it at dispatch time — scoping the word to "catalog" makes the
two messages coherent ("the catalog didn't change" + "a theme was placed").
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


def test_no_change_headline_is_catalog_scoped():
    # both the with-count and the bare (zero-catalog / errored) forms.
    # v0.50.72: the count now names its unit — "N ThemerrDB themes checked" — so
    # the creeping number reads as ThemerrDB catalog growth, not a library match.
    assert '({checked:,} ThemerrDB themes checked)"' in WORKER_PY
    assert 'f"Motif sync — no catalog changes "' in WORKER_PY
    assert 'else "Motif sync — no catalog changes")' in WORKER_PY
    # the pre-fix bare "no changes" headline must be gone so it can't silently
    # revert (the changed/updated headline `parts` join is unaffected).
    assert 'f"Motif sync — no changes ({checked} checked)"' not in WORKER_PY
    assert 'else "Motif sync — no changes")' not in WORKER_PY


def test_rationale_marker_present():
    # the WHY breadcrumb (the headline is catalog-scoped because the post-sync
    # enum auto-themes afterward) must survive next to the change.
    assert "v1.24.16" in WORKER_PY
    assert "post-sync plex_enum runs AFTER this dispatch" in WORKER_PY
