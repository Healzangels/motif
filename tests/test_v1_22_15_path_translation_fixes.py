"""v1.22.15 — host→container path translation on two Unraid-broken sites.

Audit finding: two filesystem checks used Plex's RAW reported folder_path
(`/mnt/user/data/...` on the user's Unraid) without routing through
`_candidate_local_paths`, so inside the container (where only `/data/...`
exists) they silently did the wrong thing:

  #1 worker.py over-ceiling sidecar fallback — `Path(folder_path)/"theme.mp3"`
     + `mkdir(parents=True)` CREATED a phantom container-local tree Plex never
     serves, then logged SUCCESS. The >10MB theme went nowhere. (Pinned in
     tests/test_v1_18_69_*.py::test_fallback_resolves_container_path_via_candidate_local_paths)

  #4 plex_enum.py theme-lost reaper Tier-2 sidecar_fs check —
     `Path(folder_path)/"theme.mp3".exists()` was ALWAYS False on Unraid, so a
     row with a recoverable on-disk sidecar mis-tiered to `no_fallback` and
     fired the wrong "theme lost, no recovery" notification.

Both now route through `_candidate_local_paths` like every sibling sidecar
check. This file pins #4 + the translation contract both fixes depend on.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Contract: _candidate_local_paths translates host → container ─


def test_candidate_local_paths_translates_unraid_host_to_container():
    """The dependency both fixes rely on: a /mnt/user host path must yield the
    /data container view as a candidate (and still offer the original)."""
    from app.core.plex_enum import _candidate_local_paths
    cands = [str(c) for c in _candidate_local_paths(
        "/mnt/user/data/media/movies/Foo (2020)")]
    assert "/data/media/movies/Foo (2020)" in cands, (
        "the Unraid host prefix /mnt/user/data must translate to the "
        "container /data view"
    )
    assert "/mnt/user/data/media/movies/Foo (2020)" in cands, (
        "the original host path must still be a candidate (non-Unraid / "
        "already-container-visible setups)"
    )


# ── #4: reaper Tier-2 sidecar_fs routes through translation ──────


def _reaper_sidecar_block() -> str:
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    # The Tier-2 fs check sets sidecar_fs from disk. v0.51.14: widened
    # 1800→3600 — the check is now a deadline-bounded executor submit
    # (audit #7: it ran inside the reap's BEGIN IMMEDIATE txn).
    i = src.index("sidecar_fs = False")
    return src[i:i + 3600]


def test_reaper_sidecar_fs_resolves_via_candidate_local_paths():
    """The reaper's Tier-2 on-disk sidecar check must translate
    host→container, NOT use a raw Path(folder_path). v1.22.72 routed
    the check through find_theme_sidecar_path — which walks
    _candidate_local_paths internally AND accepts the full
    SIDECAR_AUDIO_EXTS set (the hardcoded theme.mp3 mis-tiered a
    manual theme.flac to no_fallback)."""
    block = _reaper_sidecar_block()
    # v0.51.14 (audit #7): the check became a deadline-bounded executor submit
    # (it ran inside the reap's BEGIN IMMEDIATE txn) — pin the submit form.
    assert "find_theme_sidecar_path, _folder_path" in block, (
        "v1.22.15/v1.22.72: reaper Tier-2 sidecar_fs must translate "
        "host→container (via find_theme_sidecar_path)"
    )
    assert "v1.22.15" in block
    # The raw, untranslated form that was always-False on Unraid must be gone.
    assert 'Path(_folder_path)\n' not in block
    # The single-extension form that missed non-mp3 sidecars must be gone.
    assert '(c / "theme.mp3").exists()' not in block


# ── #1 cross-ref: worker fallback pinned in test_v1_18_69 ────────


def test_worker_over_ceiling_fallback_pin_exists():
    """Sanity: the worker-side fix (#1) is pinned in its home test file."""
    t = (REPO / "tests"
         / "test_v1_18_69_plex_upload_too_large_sidecar_fallback.py").read_text()
    assert "test_fallback_resolves_container_path_via_candidate_local_paths" in t


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
