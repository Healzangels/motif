"""v1.19.94 — Plex theme-upload ceiling lowered 20MB → 10MB.

the user's 2026-05-29 bulk place into the anime section stuck at
"PLACE INTO PLEX QUEUED · 97%" (31/32). One row — The Demon Sword
Master of Excalibur Academy (rk=451936, 10.5MB) — got HTTP 500 from
Plex on every attempt (multipart AND raw-body fallback both 500),
then took the RuntimeError 3×-retry-with-backoff path because it
fell BELOW the v1.18.70 size-rejection threshold (20MB). That held
the place queue at 97% for ~6min before terminal-failing.

The real Plex theme-upload ceiling is ~10MB, not the ~25MB the
v1.18.68-94 comments cited: in the same bulk place, 9.4MB (Unnamed
Memory) uploaded fine while 10.5MB (Demon Sword) and the v1.18.94
11.7MB (Fate/stay night) both consistently 500'd.

## Fix

`_PLEX_THEME_UPLOAD_CEILING_MB = 10` constant in worker.py, used at
BOTH the size-rejection decision sites:
  - sidecar-fallback gate (movie/TV with a folder → hardlink/copy a
    sidecar instead of raising → row themed via HL/C)
  - _JobPermanentFailure classification (collections / folderless →
    terminal-fail fast, no 3× retry)

A 10.5MB row that 500s now either falls back to a sidecar (themed)
or terminal-fails immediately, so the bulk place completes promptly
instead of holding the queue through backoff cycles.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


# ── the constant ─────────────────────────────────────────────


def test_ceiling_constant_defined_and_value():
    import app.core.worker as w
    assert hasattr(w, "_PLEX_THEME_UPLOAD_CEILING_MB"), (
        "v1.19.94: the ceiling must be a named module constant"
    )
    c = w._PLEX_THEME_UPLOAD_CEILING_MB
    # Must sit ABOVE the largest observed success (9.4MB) and AT/BELOW
    # the smallest observed failure (10.5MB) so a 10.5MB 500 is caught
    # as a size rejection while a 9.4MB upload still gets attempted.
    assert 9.4 < c <= 10.5, (
        f"v1.19.94: ceiling {c} must be in (9.4, 10.5] — above the "
        f"largest observed OK upload, at/below the smallest 500"
    )


# ── both decision sites use the constant (not a literal 20) ──


def _place_collection_body() -> str:
    fn_idx = WORKER_PY.index("def _do_place_collection(")
    nxt = WORKER_PY.find("\n    def ", fn_idx + 1)
    return WORKER_PY[fn_idx:nxt if nxt != -1 else len(WORKER_PY)]


def test_both_sites_reference_the_constant():
    body = _place_collection_body()
    # Sidecar-fallback gate + size_rejection classification.
    assert body.count("_PLEX_THEME_UPLOAD_CEILING_MB") >= 2, (
        "v1.19.94: both the sidecar-fallback gate and the "
        "size_rejection classification must use the constant"
    )


def test_no_stale_20mb_threshold():
    body = _place_collection_body()
    assert "size_mb >= 20" not in body, (
        "v1.19.94: the hardcoded 20MB threshold must be gone — it "
        "let 10-20MB 500s take the 3×-retry path and stick the queue"
    )


# ── classification logic with the real constant ──────────────


def test_size_rejection_classification_with_real_constant():
    """Replicate the size_rejection boolean using the IMPORTED
    constant so the test tracks the real value. A 10.5MB / 11.7MB
    HTTP 500 must classify as a size rejection; a 9.4MB 500 must
    not (could be a transient hiccup — stays retryable)."""
    from app.core.worker import _PLEX_THEME_UPLOAD_CEILING_MB as C

    def is_size_rejection(size_mb, http_status):
        return size_mb >= C and http_status == 500

    assert is_size_rejection(10.5, 500), "Demon Sword (10.5MB) → size rejection"
    assert is_size_rejection(11.7, 500), "Fate/stay night (11.7MB) → size rejection"
    assert not is_size_rejection(9.4, 500), "9.4MB OK-sized → not a size rejection"
    # A 500 below the ceiling stays retryable; a non-500 never
    # classifies as size rejection regardless of size.
    assert not is_size_rejection(50.0, 404), "non-500 never size rejection"


# ── version pin ──────────────────────────────────────────────


def test_v1_19_94_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
