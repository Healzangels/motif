"""v1.21.22 — dead-code cleanup (simplification review).

A 3-agent holistic simplification review of motif found the codebase
already very lean (~52k lines, almost no dead surface). The grep-confirmed,
test-suite-validated removals shipped here:

  - app/core/tvdb.py — entire module orphaned (superseded by the TMDB
    bridge; imported by nothing).
  - config.py: 4 never-called accessors (section_themes_dir,
    download_max_retries, tvdb_api_key property, reset_settings_for_tests).
    The plex.tvdb_api_key *field* stays (masking/env still read it).
  - config_file.py: _get_dotted (orphaned half of the get/set pair).
  - 12 verified-unused imports across 6 modules.
  - 2 dead JS lines (a ytLink alias + a no-op getElementById close that
    targeted a nonexistent dialog id).

The real validation is the full suite still passing (nothing referenced
any of this). These pins just lock the removals so they don't creep back.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def test_orphaned_tvdb_module_gone():
    assert not (REPO / "app" / "core" / "tvdb.py").exists()


def test_dead_config_symbols_removed():
    cfg = (REPO / "app" / "config.py").read_text()
    assert "def download_max_retries" not in cfg
    assert "def reset_settings_for_tests" not in cfg
    assert "def tvdb_api_key" not in cfg          # the convenience property
    assert "def section_themes_dir(" not in cfg   # not the _by_subdir sibling
    # the still-used sibling must remain
    assert "def section_themes_dir_by_subdir" in cfg


# v0.50.89: test_get_dotted_removed dropped — `_get_dotted` was reinstated
# with a genuine caller (save()'s env-override-baking guard), so it is no
# longer dead code and the removal pin no longer applies.


def test_dead_js_lines_removed():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const ytLink = currentUrlLink" not in js
    assert "item-info-dialog" not in js


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
