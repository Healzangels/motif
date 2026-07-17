"""v0.51.188 — every MotifConfig section must be settable from the UI.

_ALLOWED_TOP_LEVEL is a closed set, and a section missing from it makes /api/config 400
with "unknown config section" — the control renders, the user toggles it, the save fails.
This has now bitten TWICE:

  - v1.13.26: `placement` was absent, so the PLACEMENT MODE block's // SAVE button 400'd
    every time. Users could not disable auto_place from the UI at all.
  - v1.17.10: the same shape one level down — a closed-set filter on notifications.events
    dropped newly-added event keys for SIX TAGS, silently auto-unchecking them.

Both were found in the field rather than by a test, because a closed set fails only when
someone tries the thing. So: walk the dataclass and assert, instead of waiting.
"""
from __future__ import annotations

import dataclasses

from app.core.config_file import MotifConfig
from app.web.api import _ALLOWED_TOP_LEVEL

# schema_version is metadata, not a user-settable section.
_NOT_A_SECTION = {"schema_version"}


def test_every_config_section_is_settable():
    sections = {
        f.name for f in dataclasses.fields(MotifConfig)
        if f.name not in _NOT_A_SECTION and dataclasses.is_dataclass(f.type)
        or (f.name not in _NOT_A_SECTION and hasattr(getattr(MotifConfig(), f.name),
                                                     "__dataclass_fields__"))
    }
    missing = sections - _ALLOWED_TOP_LEVEL
    assert not missing, (
        f"config section(s) {sorted(missing)} exist on MotifConfig but are not in "
        f"_ALLOWED_TOP_LEVEL — /api/config will 400 'unknown config section' on every "
        f"save of them. This is the v1.13.26 (placement) bug verbatim."
    )


def test_allowed_set_has_no_phantoms():
    """The reverse drift: a name here that no longer exists on MotifConfig would be a
    section nobody can save because it isn't real."""
    sections = {f.name for f in dataclasses.fields(MotifConfig)}
    phantom = _ALLOWED_TOP_LEVEL - sections
    assert not phantom, f"_ALLOWED_TOP_LEVEL names non-existent section(s): {sorted(phantom)}"


def test_loudness_is_settable():
    """The section this lint was written alongside — normalize-at-download's toggle."""
    assert "loudness" in _ALLOWED_TOP_LEVEL


# ── the toggle must actually SAVE, not just render ───────────────────────
# The v1.18.81 phantom shape: a control that renders and does nothing.

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_every_cfg_field_is_collected_by_some_save_button():
    """collectFieldsForTab selects [data-cfg-field^="<tab>."], so a field whose section is
    not named by ANY data-save button renders, accepts input, and silently never saves.
    That is the v1.13.26 placement bug wearing a different hat, one layer up."""
    import re
    fields = set(re.findall(r'data-cfg-field="([^".]+)\.', HTML))
    saved = set()
    for spec in re.findall(r'data-save="([^"]+)"', HTML):
        saved.update(spec.split())
    orphans = fields - saved
    assert not orphans, (
        f"config section(s) {sorted(orphans)} have controls in settings.html but no "
        f"data-save button collects them — the controls would render and never save."
    )


def test_loudness_controls_are_collected_by_the_downloads_save():
    assert 'data-cfg-field="loudness.normalize_on_download"' in HTML
    assert 'data-cfg-field="loudness.target_lufs"' in HTML
    assert 'data-save="downloads loudness"' in HTML


def test_collect_still_supports_multi_section_specs():
    """The mechanism the fix relies on (v1.21.12). If this regressed to single-section,
    the loudness controls would silently stop saving."""
    i = APP_JS.index("function collectFieldsForTab")
    src = APP_JS[i:i + 700]
    assert ".split(/\\s+/)" in src, "multi-section save specs must still be supported"
