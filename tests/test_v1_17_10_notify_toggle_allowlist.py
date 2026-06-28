"""v1.17.10 — notification toggle allowlist parity.

## The bug

the user reported: checking the bottom 6 toggles in
/settings → NOTIFICATIONS (cookies_needed, disk_low,
worker_restarted, theme_added, theme_deleted,
release_available) + clicking SAVE EVENTS auto-unchecks them
on next render. Silent failure — no console error, no log line.

## The cause

Two-step drift between v1.17.0 and v1.17.4:

1. v1.17.0 added `_DEFAULT_NOTIFY_EVENTS` in `config_file.py`
   with 4 keys (sync_completed, sync_failed,
   bulk_action_completed, themes_added_by_sync) plus a
   "Phase 2 (v1.17.1+)" comment block listing 7 deferred
   events that *would* be added when their hook sites landed.
2. v1.17.1 added a closed-set filter at `api.py:218`:
   `if sub_k not in allowed_keys: continue` — silently drops
   any PATCH key that isn't in `_DEFAULT_NOTIFY_EVENTS`.
3. v1.17.4 added the hook sites + `notify_dedupe` primitive
   + UI toggles for 6 of those 7 deferred events. But
   `_DEFAULT_NOTIFY_EVENTS` was never extended — the comment
   block in `config_file.py` still claimed they were deferred.

Result: the closed-set filter rejected every PATCH from the
6 new toggles, the in-memory `cfg.notifications.events` never
changed, the next page render read False from the unchanged
state, and the checkbox flipped back to unchecked.

Class-9 silent-defensive-catch by another name: the
contract-mismatch drop at api.py:220 had no log breadcrumb,
so the drift between UI surface and allowlist accumulated
across two tag cycles without surfacing.

## The fix

* `_DEFAULT_NOTIFY_EVENTS` extended to include all 7
  v1.17.4-wired event kinds. ON-by-default for critical /
  rare events (cookies_needed, disk_low, worker_restarted —
  all dedupe-rate-limited so they can't spam); OFF for opt-
  in (theme_added, theme_deleted, release_available).
* `new_tdb_theme_available` also added so manual PATCH
  experimentation works per the v1.17.0 README note.
* api.py:220 drop now `log.debug`s the rejected key so the
  next contract drift leaves a breadcrumb.

## The regression test

The structural fix above closes the user's bug. The parity
test (`test_ui_toggles_in_allowlist`) prevents the same
class from recurring: any new toggle added to settings.html
that isn't also added to `_DEFAULT_NOTIFY_EVENTS` will fail
this test before it can ship as a silent-drop bug.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
CONFIG_FILE_PY = REPO / "app" / "core" / "config_file.py"
API_PY = REPO / "app" / "web" / "api.py"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


def _parse_default_notify_events() -> dict[str, bool]:
    """Pull the literal dict body out of config_file.py."""
    src = CONFIG_FILE_PY.read_text()
    m = re.search(
        r"_DEFAULT_NOTIFY_EVENTS:\s*dict\[str,\s*bool\]\s*=\s*\{(.*?)^\}",
        src, re.DOTALL | re.MULTILINE,
    )
    assert m, "Couldn't locate _DEFAULT_NOTIFY_EVENTS literal"
    body = m.group(1)
    out: dict[str, bool] = {}
    for key, value in re.findall(
        r'"([a-z_]+)"\s*:\s*(True|False)', body
    ):
        out[key] = value == "True"
    return out


def _ui_event_toggles() -> set[str]:
    """Every `data-cfg-field="notifications.events.X"` toggle in
    the settings template."""
    src = SETTINGS_HTML.read_text()
    keys = set(re.findall(
        r'data-cfg-field="notifications\.events\.([a-z_]+)"',
        src,
    ))
    assert keys, (
        "settings.html should declare at least one "
        "notifications.events.X toggle"
    )
    return keys


# ── Core regression test ──────────────────────────────────────


def test_ui_toggles_in_allowlist():
    """Every UI-surfaced toggle in settings.html must also live in
    `_DEFAULT_NOTIFY_EVENTS` — otherwise the api.py:220 closed-
    set filter silently drops the PATCH and the toggle auto-
    unchecks on save."""
    ui = _ui_event_toggles()
    allowlist = set(_parse_default_notify_events().keys())
    missing = sorted(ui - allowlist)
    assert not missing, (
        "v1.17.10: every UI toggle in settings.html "
        "(`data-cfg-field=\"notifications.events.X\"`) must have "
        "a matching key in app/core/config_file.py "
        "`_DEFAULT_NOTIFY_EVENTS`. The api.py:218 closed-set "
        "filter silently drops PATCHes for keys not in this dict, "
        "and SAVE EVENTS will auto-uncheck them on the next "
        "render. Missing: " + ", ".join(missing)
    )


# ── Specific keys pinned (defense-in-depth) ───────────────────


def test_phase_2_events_in_allowlist_with_correct_defaults():
    """The 6 events wired in v1.17.4 + their defaults."""
    events = _parse_default_notify_events()
    expected = {
        # ON-by-default: critical events, all dedupe-rate-limited.
        "cookies_needed":     True,
        "disk_low":           True,
        "worker_restarted":   True,
        # OFF-by-default: opt-in (per-row or new-release ping).
        "theme_added":        False,
        "theme_deleted":      False,
        "release_available":  False,
    }
    for key, default in expected.items():
        assert key in events, (
            f"v1.17.10: `{key}` must be in _DEFAULT_NOTIFY_EVENTS "
            f"(UI exposes a SAVE-able toggle for it)."
        )
        assert events[key] == default, (
            f"v1.17.10: `{key}` default should be {default}; "
            f"found {events[key]}."
        )


def test_phase_1_defaults_unchanged():
    """v1.17.0 defaults must be preserved verbatim — flipping any
    of these would silently change behavior on existing installs
    on first restart."""
    events = _parse_default_notify_events()
    assert events.get("sync_completed") is True
    assert events.get("sync_failed") is True
    assert events.get("bulk_action_completed") is True
    assert events.get("themes_added_by_sync") is False


def test_new_tdb_theme_available_in_allowlist_off():
    """The deferred-UI event still belongs in the allowlist so
    manual /api/config PATCH experimentation works per the
    v1.17.0 README note ("reachable via /api/config PATCH
    today"). Default OFF — it'll be a hot-path firehose if
    ever wired without batching."""
    events = _parse_default_notify_events()
    assert events.get("new_tdb_theme_available") is False, (
        "v1.17.10: new_tdb_theme_available must be allowlisted "
        "(default OFF) so manual PATCH stays reachable."
    )


# ── Class-9 breadcrumb on dropped keys ────────────────────────


def test_dropped_event_key_logs_debug():
    """The api.py closed-set drop must log at debug so the next
    UI/allowlist drift is diagnosable. Pre-fix the drop was
    fully silent — the user's bug accumulated across v1.17.4 +
    v1.17.5-9 with zero breadcrumb."""
    src = API_PY.read_text()
    # Locate the drop site by its closed-set guard.
    idx = src.index("if allowed_keys is not None and sub_k not in allowed_keys:")
    window = src[idx:idx + 1200]
    assert "log.debug(" in window, (
        "v1.17.10: the closed-set drop in _apply_partial_config "
        "must log.debug the dropped key so contract drift leaves "
        "a breadcrumb."
    )
    assert "_DEFAULT_NOTIFY_EVENTS" in window, (
        "v1.17.10: the breadcrumb should name _DEFAULT_NOTIFY_EVENTS "
        "so a grep on the symbol lands at the drop site."
    )


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_10():
    src = (REPO / "app" / "__init__.py").read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m, "__version__ must be a 3-part semver string"
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 10), (
        f"v1.17.10: __version__ must be at or above 1.17.10 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
