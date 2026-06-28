"""v1.17.9 — hygiene audit rollover.

Three-agent audit (silent-catch sweep, dead-code sweep, DB hygiene
sweep) surfaced a Tier-A punch list of low-risk wins:

## Dead code retired

* `@app.get("/api/debug/stat-folder")` — diagnostic endpoint from
  v1.11.76 for the M-vs-P false-positive on Unraid. The class was
  fixed multiple times since (v1.14.50 owner-stamp, v1.15.117
  _safe_link_or_copy pre-clean); the endpoint had zero callers and
  no UI surface.
* `get_managed_section_ids` (`app/core/sections.py`) + its import
  in `app/web/api.py:59` — orphan pair; all callsites query
  `plex_sections.included = 1` directly.
* `get_all_runtime` (`app/core/runtime.py`) — zero callers in the
  whole tree; the only runtime bool we care about is `dry_run`
  with its own dedicated accessors.
* JS: `relinkItem`, `urlSourceLabel`, `activePlexEnumScopeLabel`
  in `app/web/static/app.js` — defined but never invoked / no
  template `onclick=` wiring.

## Class-9 silent-catch breadcrumbs added

Every defensive `except` that swallowed a failure mode without a
log line is a bug — surfacing as silent state drift weeks later
(canonical: v1.14.52 H5, v1.15.0, v1.15.34, v1.15.117). This
ship closes four more:

* `config_file.py` `chmod 0600` on motif.yaml — the file may carry
  the Plex token + apprise URLs (which can include service-side
  credentials). Silent chmod failure on a multi-tenant host left
  those readable to other local users with no breadcrumb.
* `main.py` `scheduler.shutdown(wait=False)` — silent exception
  here left apscheduler threads alive through the rest of teardown
  while the next "motif shutting down" log line lied.
* `api.py` `_compute_next_cron_fire` apscheduler import — silent
  ImportError nuked the topbar next-sync pill on every poll;
  operator saw a blank field, not "apscheduler missing."
* `plex.py` `get_item_paths` JSON decode — twin sites at lines
  288/346/744/990 already log; this one silently returned `[]` so
  the placement pipeline lost the media folder with no breadcrumb.

## DB hygiene: schema v53 + TTL sweeps

* Schema v53 drops `plex_items.motif_unplaced_at` — dead column
  since v1.12.111 (replaced by `plex_theme_verified_ok`). The v34
  migration docstring explicitly anticipated this drop ("future
  migrations may drop it").
* New scheduler job `tvdb_lookup_cache_prune` daily at 03:05 UTC
  removes expired rows. Pre-fix the table was append-only despite
  `expires_at` — read paths already skipped expired rows but
  nothing deleted them.
* New scheduler job `events_prune` daily at 03:10 UTC removes
  `events` rows older than 30 days. The 30-day cap matches the
  `/events?since=` query cap (api.py:14735); audit_events stays
  long-lived per design.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
SECTIONS_PY = REPO / "app" / "core" / "sections.py"
RUNTIME_PY = REPO / "app" / "core" / "runtime.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
CONFIG_FILE_PY = REPO / "app" / "core" / "config_file.py"
MAIN_PY = REPO / "app" / "main.py"
PLEX_PY = REPO / "app" / "core" / "plex.py"
DB_PY = REPO / "app" / "core" / "db.py"
SCHEDULER_PY = REPO / "app" / "core" / "scheduler.py"
APP_INIT = REPO / "app" / "__init__.py"


# ── Dead code retired ──────────────────────────────────────────


def test_stat_folder_endpoint_retired():
    """The /api/debug/stat-folder route (v1.11.76) had zero
    callers and was deleted in the v1.17.9 hygiene audit."""
    src = API_PY.read_text()
    assert '@app.get("/api/debug/stat-folder")' not in src, (
        "v1.17.9: /api/debug/stat-folder must be retired."
    )
    assert "async def api_debug_stat_folder" not in src, (
        "v1.17.9: api_debug_stat_folder handler must be deleted."
    )


def test_get_managed_section_ids_retired():
    """`get_managed_section_ids` was imported by api.py but had
    no callers. The orphan pair (import + def) is deleted."""
    api_src = API_PY.read_text()
    sec_src = SECTIONS_PY.read_text()
    assert "get_managed_section_ids" not in api_src, (
        "v1.17.9: api.py must drop the get_managed_section_ids "
        "import — it was unused."
    )
    assert "def get_managed_section_ids" not in sec_src, (
        "v1.17.9: sections.py must drop get_managed_section_ids — "
        "no callers in tree."
    )


def test_get_all_runtime_retired():
    """`get_all_runtime` had no callers; deleted in v1.17.9."""
    src = RUNTIME_PY.read_text()
    assert "def get_all_runtime" not in src, (
        "v1.17.9: runtime.py must drop get_all_runtime — "
        "no callers in tree."
    )


def test_dead_js_helpers_retired():
    """`relinkItem`, `urlSourceLabel`, `activePlexEnumScopeLabel`
    were JS-side dead functions deleted in v1.17.9."""
    src = APP_JS.read_text()
    for name in ("activePlexEnumScopeLabel", "urlSourceLabel",
                 "relinkItem"):
        # `function <name>(` would catch the definition; we want
        # neither def nor reference to remain.
        assert f"function {name}(" not in src, (
            f"v1.17.9: JS function `{name}` must be deleted — "
            f"hygiene audit found zero callers."
        )
        assert f"async function {name}(" not in src, (
            f"v1.17.9: JS function `{name}` must be deleted."
        )


# ── Class-9 silent-catch breadcrumbs ───────────────────────────


def test_config_chmod_logs_on_failure():
    """v1.17.9 class-9: the chmod 0o600 swallow on motif.yaml
    must now log a warning. The file can carry the Plex token +
    apprise URLs (which may include credentials), so a silent
    chmod failure on a multi-tenant host needs a breadcrumb."""
    src = CONFIG_FILE_PY.read_text()
    # Locate the chmod block.
    idx = src.index("os.chmod(self.path, 0o600)")
    window = src[idx:idx + 600]
    assert "except OSError as e:" in window, (
        "v1.17.9: config_file.py chmod must bind the exception "
        "and log it."
    )
    assert "log.warning(" in window, (
        "v1.17.9: chmod failure must log.warning (security-"
        "adjacent — file holds Plex token + notification URLs)."
    )


def test_main_shutdown_logs_scheduler_failure():
    """v1.17.9 class-9: `scheduler.shutdown(wait=False)` in
    main.shutdown() must log on exception so postmortem can
    distinguish hung shutdown from clean shutdown."""
    src = MAIN_PY.read_text()
    idx = src.index("scheduler.shutdown(wait=False)")
    window = src[idx:idx + 400]
    assert "except Exception as e:" in window, (
        "v1.17.9: scheduler.shutdown failure must bind the "
        "exception."
    )
    assert "log.warning(" in window, (
        "v1.17.9: scheduler.shutdown failure must log.warning."
    )


def test_cron_fire_apscheduler_import_logs_on_failure():
    """v1.17.9 class-9: apscheduler import failure inside
    _compute_next_cron_fire must log a warning. A broken venv
    silently blanked the topbar pill on every poll pre-fix."""
    src = API_PY.read_text()
    idx = src.index("def _compute_next_cron_fire(")
    window = src[idx:idx + 1500]
    assert "from apscheduler.triggers.cron import CronTrigger" in window
    assert "except Exception as e:" in window, (
        "v1.17.9: apscheduler import block must bind the exception."
    )
    assert "log.warning(" in window, (
        "v1.17.9: apscheduler import failure must log.warning "
        "(topbar pill silently blanks pre-fix)."
    )


def test_get_item_paths_logs_malformed_json():
    """v1.17.9 class-9: `get_item_paths` JSON decode failure must
    log a warning. Twin sites in this file already log; the
    silent `[]` here lost the placement pipeline's media folder
    with no breadcrumb."""
    src = PLEX_PY.read_text()
    idx = src.index("def get_item_paths(self, rating_key:")
    window = src[idx:idx + 1500]
    assert "except (ValueError, json.JSONDecodeError) as e:" in window, (
        "v1.17.9: get_item_paths JSON decode must bind the "
        "exception."
    )
    assert "log.warning(" in window, (
        "v1.17.9: get_item_paths malformed JSON must log.warning."
    )


# ── DB hygiene: v53 + TTL sweeps ───────────────────────────────


def test_schema_version_bumped_to_v53():
    """v1.17.9 bumps schema v52 → v53 to drop the dead
    motif_unplaced_at column."""
    src = DB_PY.read_text()
    m = re.search(r"CURRENT_SCHEMA_VERSION\s*=\s*(\d+)", src)
    assert m, "CURRENT_SCHEMA_VERSION must be present"
    assert int(m.group(1)) >= 53, (
        f"v1.17.9: CURRENT_SCHEMA_VERSION must be >= 53 "
        f"(found {m.group(1)})."
    )


def test_v52_to_v53_migration_drops_motif_unplaced_at():
    """v1.17.9: the new migration must DROP COLUMN
    plex_items.motif_unplaced_at — dead since v1.12.111."""
    src = DB_PY.read_text()
    assert "def _migrate_v52_to_v53(" in src, (
        "v1.17.9: _migrate_v52_to_v53 must exist."
    )
    idx = src.index("def _migrate_v52_to_v53(")
    body = src[idx:idx + 2000]
    assert "DROP COLUMN motif_unplaced_at" in body, (
        "v1.17.9: migration must DROP COLUMN motif_unplaced_at."
    )
    # Chain wiring — make sure init_db's migration ladder calls it.
    assert "elif current == 52:" in src and "_migrate_v52_to_v53(conn)" in src, (
        "v1.17.9: init_db migration chain must include "
        "_migrate_v52_to_v53 at the v52 step."
    )


def test_schema_definition_drops_motif_unplaced_at_column():
    """v1.17.9: the SCHEMA string (used for fresh installs) must
    no longer declare the dead column. The migration handles
    existing installs; fresh installs skip the column entirely."""
    src = DB_PY.read_text()
    # The actual column declaration line — distinguished from
    # narrative comments that may still reference the historical
    # column name.
    bad = re.search(
        r"^\s+motif_unplaced_at\s+TEXT,?\s*$",
        src, re.MULTILINE,
    )
    assert bad is None, (
        "v1.17.9: SCHEMA must not declare `motif_unplaced_at TEXT` "
        "as a column on plex_items — column dropped in v53."
    )


def test_tvdb_lookup_cache_prune_job_registered():
    """v1.17.9: scheduler must wire a daily prune for expired
    tvdb_lookup_cache rows. Pre-fix the table grew unbounded."""
    src = SCHEDULER_PY.read_text()
    assert "def _prune_tvdb_lookup_cache(" in src, (
        "v1.17.9: _prune_tvdb_lookup_cache must exist."
    )
    idx = src.index("def _prune_tvdb_lookup_cache(")
    body = src[idx:idx + 2000]
    assert "DELETE FROM tvdb_lookup_cache" in body, (
        "v1.17.9: prune must DELETE expired rows."
    )
    assert "expires_at" in body, (
        "v1.17.9: prune must key on expires_at."
    )
    # Job registered with a CronTrigger.
    assert 'id="tvdb_lookup_cache_prune"' in src, (
        "v1.17.9: scheduler.start_scheduler must register the "
        "tvdb_lookup_cache_prune job with a stable id."
    )


def test_events_prune_job_registered():
    """v1.17.9: scheduler must wire a daily prune for `events`
    rows older than 30 days. Comments call the table "rotating"
    but pre-fix nothing actually rotated it."""
    src = SCHEDULER_PY.read_text()
    assert "def _prune_events(" in src, (
        "v1.17.9: _prune_events must exist."
    )
    idx = src.index("def _prune_events(")
    body = src[idx:idx + 2000]
    assert "DELETE FROM events" in body, (
        "v1.17.9: prune must DELETE events rows."
    )
    assert "-30 days" in body, (
        "v1.17.9: events prune must use the 30-day retention "
        "matching the /events?since= UI cap."
    )
    assert 'id="events_prune"' in src, (
        "v1.17.9: scheduler.start_scheduler must register the "
        "events_prune job with a stable id."
    )


# ── Version pin (soft floor) ───────────────────────────────────


def test_version_pinned_at_or_above_1_17_9():
    """v1.17.9: app/__init__.py __version__ must be >= 1.17.9.
    Soft-floor so subsequent tag bumps don't break this pin."""
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m, "__version__ must be a 3-part semver string"
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 9), (
        f"v1.17.9: __version__ must be at or above 1.17.9 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
