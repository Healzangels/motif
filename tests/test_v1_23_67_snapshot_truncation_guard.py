"""v1.23.67 — snapshot-tarball truncation guard (audit #14).

A ThemerrDB codeload tarball truncated to only the movies/ subtree (tv_shows/ +
movie_collections/ absent — an interrupted/edge-corrupt download that still
extracts without a TarError) passed _extract's old any()-subdir sanity check,
then index('tv_shows') hit `pages.json missing → return [], 0` (failed_pages=0)
so the run reported SUCCESS having synced ZERO tv/collection themes — a silent
fake-success (the v1.22.* media_types_seen gate only stopped the catastrophic
mass-drop, not the zero-sync). _extract now requires EVERY subtree's pages.json
so a truncated tarball raises _SnapshotError → run_sync falls through to the
git/remote tier.

The behavioral coverage (a movies-only tarball raises) lives in
test_sync_database.py::test_snapshot_rejects_truncated_tarball, alongside the
snapshot infra + the tar-slip rejection siblings. These are the source pins.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
DB_TEST = (REPO / "tests" / "test_sync_database.py").read_text()


def test_extract_requires_all_subtrees_pages_json():
    # the lenient any()-subdir gate must be gone from the module entirely.
    assert "any((dest / sub).is_dir()" not in SYNC_PY, (
        "v1.23.67: the lenient any()-subdir sanity check must be gone"
    )
    # replaced by an all-subtrees pages.json completeness check.
    assert 'sub / "pages.json"' in SYNC_PY, (
        "v1.23.67: _extract must require each subtree's pages.json"
    )
    assert "truncated snapshot" in SYNC_PY, (
        "v1.23.67: _extract must raise a truncation-specific _SnapshotError"
    )
    # the check must reference the canonical subtree list it iterates.
    assert "for sub in allowed_subdirs" in SYNC_PY


def test_fixture_builder_backfills_all_three_subtrees():
    # the canonical builder must satisfy the new gate even for minimal fixtures.
    assert 'for _sub in ("movies", "tv_shows", "movie_collections"):' in DB_TEST
    assert "records.setdefault(_sub, {})" in DB_TEST
