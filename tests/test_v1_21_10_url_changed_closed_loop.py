"""v1.21.10 — close the last automated-action loop break.

The v1.21.9 closed-loop audit found the TDB-sync `url_changed` branch
auto-downloaded M-rows (manual sidecar) without the has_sidecar check OR
the auto_download_new_themes opt-in that the `is_new` branch has. So an
M-row whose TDB url changed silently re-downloaded TDB bytes on every
cron sync, and a SRC=— url change ignored the toggle.

Fix (mirror is_new): compute has_sidecar in the url_changed branch;
route M-rows into the prompt path (pending_update / blue TDB↑) instead of
auto-download; gate the SRC=— auto-download on auto_download_new_themes.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()


def _url_changed_block() -> str:
    # The branch's else-gate sits at ~offset 11400; slice generously.
    idx = SYNC_PY.index("elif url_changed:")
    return SYNC_PY[idx:idx + 13000]


def test_url_changed_computes_has_sidecar():
    b = _url_changed_block()
    # v1.23.89: has_sidecar now routes through the _plex_title_present helper
    # (index-friendly split-EXISTS) with sidecar_only=True, mirroring the is_new
    # twin. The helper preserves the v1.22.39 theme_id linkage (HAMA guid_tmdb-
    # NULL cohort) AND the local_theme_file=1 sidecar gate.
    assert "has_sidecar = _plex_title_present(" in b
    assert "sidecar_only=True" in b
    helper = SYNC_PY[SYNC_PY.index("def _plex_title_present("):
                     SYNC_PY.index("def _plex_title_present(") + 1600]
    assert "local_theme_file = 1" in helper
    assert "theme_id = ?" in helper


def test_will_enqueue_download_excludes_sidecar_and_requires_toggle():
    """M-rows (sidecar) must be excluded from auto-download, and the
    SRC=— auto-download must require the opt-in — mirroring is_new."""
    b = _url_changed_block()
    assert "not (already_have or has_override or has_sidecar)" in b
    assert "and auto_download_new_themes" in b


def test_m_rows_routed_to_prompt_path():
    """The pending_update prompt block now fires for has_sidecar rows
    too — an M-row whose TDB url changed gets a blue TDB↑ prompt
    instead of a silent download."""
    b = _url_changed_block()
    assert "if already_have or has_override or has_sidecar:" in b


def test_else_branch_auto_download_is_opt_in_gated():
    """The remaining (SRC=—) auto-download in the else branch must be
    gated on auto_download_new_themes."""
    b = _url_changed_block()
    assert "and auto_download_new_themes):" in b


def test_is_new_branch_unchanged_reference():
    """Sanity: the is_new branch's own gate (the pattern we mirrored)
    still exists, so the two branches stay aligned."""
    idx = SYNC_PY.index("if yt_url and in_plex:")
    block = SYNC_PY[idx:idx + 2000]
    assert "not has_content and not plex_supplies" in block
    assert "and auto_download_new_themes" in block


def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
