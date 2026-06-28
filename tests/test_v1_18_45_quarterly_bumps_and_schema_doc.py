"""v1.18.45 — quarterly dep bumps + local_files.file_path schema doc.

Two small hygiene tasks bundled:

  1. Apprise floor bumped 1.7.0 → 1.10.0 (latest stable as of
     2026-04-26). Three minor releases worth of new service
     handlers + transport-URL evolution. Per the CLAUDE.md
     quarterly review convention.

     yt-dlp NOT bumped — latest stable is still 2026.03.17
     (the v1.15.16 floor). Re-verified the date marker in
     requirements.txt.

  2. local_files.file_path schema comment. Documents the
     relative-to-themes_dir convention that bit v1.18.36 →
     v1.18.39. Defense-in-depth: the comment appears in
     schema dumps + `PRAGMA table_info` so future contributors
     reading the schema directly (not via CLAUDE.md) see the
     convention at the column definition.
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
REQUIREMENTS = REPO / "requirements.txt"
DB_PY = REPO / "app" / "core" / "db.py"


# ── Quarterly dep bumps ──────────────────────────────────────


def test_apprise_floor_bumped_to_1_10_0():
    """Apprise floor — v1.22.24 bumped it 1.10.0 → 1.11.0 (latest stable
    2026-05-29) on the quarterly review."""
    req = REQUIREMENTS.read_text()
    assert "apprise>=1.11.0" in req, (
        "v1.22.24: apprise floor must be bumped to 1.11.0 (latest stable "
        "2026-05-29)"
    )
    # Superseded floors must NOT survive in the LIVE require
    # (comments referencing them for history are fine).
    live_lines = [
        line for line in req.splitlines()
        if not line.lstrip().startswith("#")
    ]
    joined = "\n".join(live_lines)
    assert "apprise>=1.7.0" not in joined
    assert "apprise>=1.10.0" not in joined


def test_yt_dlp_floor_unchanged_but_verified():
    """v1.24.42 (security audit): yt-dlp floor bumped 2026.3.17 → 2026.6.9 (the
    release fixing CVE-2026-50023/50574/50019 + GHSA-69qj). The comment must
    still record the quarterly re-verification lineage."""
    req = REQUIREMENTS.read_text()
    assert "yt-dlp>=2026.6.9" in req
    assert "re-verified 2026-06-06" in req, (
        "v1.22.24: yt-dlp comment must record the quarterly "
        "re-verification date even when no bump was needed"
    )


# ── Schema column comment ────────────────────────────────────


def test_local_files_file_path_has_relative_convention_comment():
    """The CREATE TABLE local_files block must include a SQL
    comment on the file_path column explaining the relative-
    to-themes_dir convention."""
    src = DB_PY.read_text()
    idx = src.index("CREATE TABLE IF NOT EXISTS local_files")
    block = src[idx:idx + 1500]
    # The comment must reference both the convention + the
    # bug-cycle that motivated documenting it.
    assert "RELATIVE to settings.themes_dir" in block, (
        "v1.18.45: file_path column must have a SQL comment "
        "documenting the relative-to-themes_dir convention"
    )
    assert "themes_dir / file_path" in block, (
        "v1.18.45: comment must show the absolute-path "
        "construction recipe"
    )
    # The comment must point at CLAUDE.md so future readers
    # who want the longer-form rationale know where to look.
    assert "CLAUDE.md" in block
