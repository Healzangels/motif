"""v1.21.21 — gray out the cron-only sub-toggle when the parent is on.

Follow-up to v1.21.19: the cron-only post-sync-enum sub-checkbox
(sync.auto_enum_after_cron_sync) has no effect when the main toggle
(sync.auto_enum_after_sync) is on, since main already covers cron. v1.21.19
left it always-clickable with explanatory help text; the user asked for the
gray-out. Now applyCronEnumDependency() disables + dims the sub whenever
the main toggle is checked (env-locked subs stay disabled regardless).

No JS runtime here — source-pin the wiring + the CSS. (The behavior leans
on collectFieldsForTab already skipping disabled inputs, so the grayed
sub's stored value is preserved, not overwritten.)
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_dependency_function_defined():
    idx = APP_JS.index("function applyCronEnumDependency(")
    body = APP_JS[idx:idx + 600]
    assert 'data-cfg-field="sync.auto_enum_after_sync"' in body      # main
    assert 'data-cfg-field="sync.auto_enum_after_cron_sync"' in body  # sub
    # disabled = env-locked OR main checked
    assert "const disabled = envLocked || main.checked;" in body
    assert "sub.disabled = disabled;" in body
    assert "classList.toggle('is-disabled', disabled)" in body


def test_dependency_wired_and_applied_on_config_load():
    # The change listener is wired once (guard flag) and applied after load.
    assert "cronMain.addEventListener('change', applyCronEnumDependency)" in APP_JS
    assert "cronMain.dataset.cronDepWired = '1'" in APP_JS
    assert "applyCronEnumDependency();" in APP_JS


def test_env_lock_preserved():
    # An env-locked sub stays disabled regardless of the main toggle.
    idx = APP_JS.index("function applyCronEnumDependency(")
    body = APP_JS[idx:idx + 600]
    assert "sub.dataset.envLocked === '1'" in body
    assert "cronSub.dataset.envLocked = cronSub.disabled ? '1' : '0'" in APP_JS


def test_css_disabled_rule_present():
    assert ".form-checkbox-sub.is-disabled" in APP_CSS
    idx = APP_CSS.index(".form-checkbox-sub.is-disabled")
    assert "opacity" in APP_CSS[idx:idx + 120]


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
