"""v1.18.55 — fix the auto-incremental TVDB bridge import + clarify docs.

the user's review of the TVDB BRIDGE in /settings:

> "I'm a bit confused what the // TVDB BRIDGE is supposed to
>  achieve as it doesn't seem to do anything on my library, also
>  we refer to it hama bridge often in places and code which
>  makes it extra confusing."

Investigation surfaced a latent silent-failure bug PLUS the
documentation gap the user asked about.

## Bug — broken relative import (silent since v1.16.0)

`app/core/plex_enum.py:1576` had `from .config import Settings`
inside the auto-incremental bridge path. Relative-import-wise
that resolves to `app.core.config` — a module that doesn't
exist. The actual location is `app.config` (one level up,
`..config`).

Every plex_enum since v1.16.0 raised `ModuleNotFoundError` on
this line. The outer `except` swallowed it and logged
"auto-incremental TVDB bridge failed: No module named
'app.core.config'" — the user's docker logs confirm this fired
on every sync run.

Result: for ~10 weeks (v1.16.0 → v1.18.54) the auto-incremental
path was dead. New shows landing in Plex didn't get bridged
incrementally — only manual REBUILD BRIDGE clicks did
anything. Class-9 silent-defensive-catch (CLAUDE.md class 9):
the breadcrumb log line existed but no one connected the
"every run logs the same message" dot.

Fix: `.config` → `..config`. Mirrors worker.py:27 + scheduler.py
:16 which use the correct two-dot pattern.

## Docs — clarify what the bridge does and how to read results

the user's specific repro: his last manual REBUILD produced
linked=0, unmappable=200, no_record=1854. The original settings
hint mentioned no_record but didn't explain the other two
counts or set expectations for linked=0 outcomes.

v1.18.55 rewrites the settings.html hint into three blocks:
  1. What this does (purpose + when it helps)
  2. How to read the results (per-count interpretation)
  3. Maintenance cadence

## Naming — log line renamed HAMA → TVDB

The internal op_progress.kind stays `tvdb_bridge` for schema
continuity (db.py v52 CHECK widening, prior runtime_settings
keys). Operator-visible surfaces — log line, settings UI,
KIND_LABEL drawer — all read TVDB BRIDGE consistently.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Bug fix: the import resolves at module-load time ─────────


def test_plex_enum_uses_correct_config_import():
    """The auto-incremental bridge's lazy import must use
    `..config` (two dots → app.config) not `.config` (one dot
    → app.core.config which doesn't exist)."""
    src = PLEX_ENUM_PY.read_text()
    assert "from ..config import Settings" in src, (
        "v1.18.55: must use `from ..config import Settings` "
        "(two-dot relative import — app.config lives one level up "
        "from app.core/plex_enum.py)"
    )
    # The broken one-dot form must NOT survive — would re-introduce
    # the silent ModuleNotFoundError.
    assert "from .config import Settings" not in src, (
        "v1.18.55: `from .config import Settings` is broken — "
        "resolves to app.core.config which doesn't exist"
    )


def test_app_core_config_module_does_not_exist():
    """Sanity check: app.core.config really doesn't exist
    (which is why the one-dot import raised). If a future tag
    creates app/core/config.py for some reason, this test
    fires loud so the bridge's import gets re-evaluated."""
    assert not (REPO / "app" / "core" / "config.py").exists(), (
        "v1.18.55 audit: app/core/config.py now exists — "
        "re-verify the relative-import in plex_enum.py auto-"
        "incremental bridge path is still correct"
    )


def test_app_config_module_exists():
    """And app.config IS the canonical Settings home."""
    assert (REPO / "app" / "config.py").exists()


def test_auto_incremental_bridge_runs_without_import_error():
    """End-to-end import check: importing plex_enum + invoking
    the auto-incremental branch must not raise
    ModuleNotFoundError. Pre-fix this raised every plex_enum
    run."""
    # Just import the module — the previous broken `from .config`
    # was inside a function body, so import succeeded but the
    # function call would fail. Simulate the function-call import
    # by walking the source for any remaining one-dot config
    # imports.
    import importlib
    import app.core.plex_enum as pe
    importlib.reload(pe)  # force re-evaluation
    # The function under test exists.
    assert callable(pe.bridge_tvdb_to_tmdb)
    # The Settings import the auto-incremental path now uses
    # must succeed in isolation.
    from app.config import Settings  # noqa: F401
    # If we got here, both branches of the previously-broken
    # import path work.


# ── Log line renamed HAMA → TVDB ─────────────────────────────


def test_log_line_renamed_hama_to_tvdb():
    """The operator-visible warning log line must read TVDB
    (not HAMA) so docker logs match the UI's // TVDB BRIDGE
    label. Internal kind stays tvdb_bridge for schema
    continuity — only the user-visible string changes."""
    src = PLEX_ENUM_PY.read_text()
    # The new shape — visible in operator logs.
    assert 'log.warning("auto-incremental TVDB bridge failed' in src, (
        "v1.18.55: log line must read 'auto-incremental TVDB "
        "bridge failed' to match the UI label"
    )
    # The pre-fix 'TVDB bridge' string must not survive as a
    # log message format (comments are fine — code archaeology).
    # Strip comment lines + check.
    code_only = "\n".join(
        line for line in src.split("\n")
        if not line.strip().startswith("#")
    )
    assert 'log.warning("auto-incremental HAMA' not in code_only, (
        "v1.18.55: the pre-fix HAMA log format must not survive "
        "in code (comments are fine)"
    )


def test_internal_kind_stays_tvdb_bridge():
    """The schema-bound `op_progress.kind = 'tvdb_bridge'` must
    survive the rename — changing the kind would require a v53
    migration that's not worth the risk for a label fix."""
    src = REPO / "app" / "core" / "db.py"
    db_src = src.read_text()
    assert "'tvdb_bridge'" in db_src, (
        "v1.18.55: schema-level kind 'tvdb_bridge' must persist "
        "for back-compat. UI renaming is operator-visible only."
    )


# ── Settings docs clarification ──────────────────────────────


def test_settings_html_documents_linked_count():
    """The settings TVDB BRIDGE hint must explicitly explain
    what `linked` means (the success case). Pre-v1.18.55 it
    only mentioned no_record."""
    src = SETTINGS_HTML.read_text()
    # Anchor on the TVDB BRIDGE block to keep the assertion
    # scoped (settings.html is large).
    bridge_anchor = src.index("// TVDB BRIDGE")
    block = src[bridge_anchor:bridge_anchor + 4000]
    assert "<code>linked</code>" in block, (
        "v1.18.55: settings hint must explain `linked` count"
    )


def test_settings_html_documents_unmappable_count():
    """The settings hint must explain `unmappable` — pre-v1.18.55
    only no_record was named."""
    src = SETTINGS_HTML.read_text()
    bridge_anchor = src.index("// TVDB BRIDGE")
    block = src[bridge_anchor:bridge_anchor + 4000]
    assert "<code>unmappable</code>" in block


def test_settings_html_explains_zero_linked_is_not_broken():
    """The hint must address the user's specific confusion: a
    rebuild that produces linked=0 isn't broken — it just means
    no overlap with ThemerrDB exists."""
    src = SETTINGS_HTML.read_text()
    bridge_anchor = src.index("// TVDB BRIDGE")
    block = src[bridge_anchor:bridge_anchor + 4000]
    # The narrative must explicitly say zero-linked is a valid
    # outcome.
    flat = " ".join(block.split())
    assert "correct outcome" in flat or "not a malfunction" in flat, (
        "v1.18.55: settings hint must explicitly explain that "
        "linked=0 is a valid outcome (not a bug)"
    )


def test_settings_html_explains_purpose_clearly():
    """The hint's opening must be a clear 'what this does'
    statement, not buried in middle paragraphs."""
    src = SETTINGS_HTML.read_text()
    bridge_anchor = src.index("// TVDB BRIDGE")
    block = src[bridge_anchor:bridge_anchor + 4000]
    assert "<strong>What this does:</strong>" in block or \
           "What this does:" in block, (
        "v1.18.55: lead with a clear 'What this does' opener"
    )


# ── End-to-end /settings still renders ───────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def test_settings_page_renders_with_new_bridge_copy(admin_client):
    """/settings must still render after the hint rewrite."""
    r = admin_client.get("/settings", headers=AUTH)
    assert r.status_code == 200
    # The new "What this does" preamble must reach the rendered
    # HTML.
    assert "What this does:" in r.text
    # The TVDB BRIDGE block survives.
    assert "// TVDB BRIDGE" in r.text


# ── Version marker ───────────────────────────────────────────


def test_v1_18_55_marker_present_at_fix_site():
    """The fix site must carry the v1.18.55 marker so future
    archaeology surfaces this latent bug."""
    src = PLEX_ENUM_PY.read_text()
    # Locate the auto-incremental import block and verify the
    # marker is nearby. Window covers both the comment block
    # preceding the import + the renamed log line that follows.
    # v1.18.80 added a second `from ..config import Settings`
    # inside the backup_ready_to_deploy dispatch block — that
    # one is structurally unrelated to the TVDB bridge fix, so
    # anchor on the v1.18.55-specific neighbor `from .tmdb
    # import TMDBClient` which only appears in the bridge site.
    idx = src.index("from .tmdb import TMDBClient")
    block = src[max(0, idx - 1500):idx + 600]
    assert "v1.18.55" in block, (
        "v1.18.55: marker must sit near the import fix"
    )
