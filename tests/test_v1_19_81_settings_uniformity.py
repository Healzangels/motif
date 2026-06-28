"""v1.19.81 — settings design-uniformity audit follow-ups.

A holistic style review of every /settings section (follow-on to
the v1.18.57 header-contract audit) surfaced four drifts. the user
approved all four for this tag:

  #1 (HIGH, spacing) — // DEFAULT PLACEMENT METHOD nested its
     `.form-actions` INSIDE `.form-grid`. Every other section makes
     form-actions a sibling AFTER form-grid. Because `.form-grid`
     has `gap: var(--gap-6)` (24px) AND `.form-actions` carries its
     own `margin-top: var(--gap-6)` (24px), nesting stacked them →
     a ~48px gap above SAVE PLACEMENT vs ~24px everywhere else.
     Un-nested.

  #2 (MED, label) — // LIBRARY SECTIONS' save button was the lone
     un-scoped `// SAVE`; every other names its scope (// SAVE PATHS
     … // SAVE EVENTS). Scoped to `// SAVE SECTIONS`. The live label
     is JS-driven (updateLibrariesSaveButton at app.js), so BOTH the
     SSR default in settings.html AND the JS string were updated.

  #3 (MED, convention) — // TVDB BRIDGE was a fake `form-label`
     carrying the `//` prefix inside a `form-label-text`. `//` is
     reserved for `block-title` + `form-subhead`; this block is an
     action+stats surface, not a labelled field. Restructured to a
     `form-subhead` subsection (peer of // CONNECTION / // TMDB
     INTEGRATION in the same panel) + a plain wrapper div that keeps
     the `id="tvdb-bridge-section"` gate the JS binds on.

  #4 (MED, status drift) — #tmdb-test-result + #tvdb-bridge-result
     used `class="muted small"` + inline `style.color`, while their
     sibling test buttons (TEST COOKIES / PROBE TRANSPORT / RUN
     PROBE) use `.form-status` + the shared `.form-status-ok/fail`
     tone palette. Migrated both spans to `.form-status` and their
     JS to set tone via `classList`, not inline color.

  #5 (LOW, bundled with #2) — #libraries-save-status was the only
     status span combining `form-status muted small`. Dropped to
     bare `.form-status` like every other save-status span.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"

SRC = SETTINGS_HTML.read_text()
JS = APP_JS.read_text()


def _section(title: str) -> str:
    """Return the enclosing <section>…</section> for a title."""
    t = SRC.index(title)
    start = SRC.rfind("<section", 0, t)
    end = SRC.index("</section>", t)
    return SRC[start:end]


def _matching_div_close(s: str, open_idx: int) -> int:
    """Index just past the </div> that closes the <div at open_idx.
    Balanced-tag walk so the assertion is structural, not
    whitespace-fragile."""
    depth = 0
    for m in re.finditer(r"<div\b|</div>", s[open_idx:]):
        if m.group() == "</div>":
            depth -= 1
            if depth == 0:
                return open_idx + m.end()
        else:
            depth += 1
    return -1


# ── #1 DEFAULT PLACEMENT METHOD form-actions un-nested ──────────


def test_placement_form_actions_is_sibling_not_nested():
    """The form-actions row must sit AFTER the form-grid closes
    (sibling), not inside it. Balanced-div walk proves it."""
    section = _section("// DEFAULT PLACEMENT METHOD")
    grid_open = section.index(
        '<div class="form-grid" data-config-form="placement">'
    )
    grid_close = _matching_div_close(section, grid_open)
    assert grid_close != -1, "could not find form-grid close"
    actions = section.index('<div class="form-actions">')
    assert actions >= grid_close, (
        "v1.19.81: // DEFAULT PLACEMENT METHOD form-actions must be a "
        "sibling AFTER the form-grid (was nested inside → double gap)"
    )


def test_placement_nested_form_actions_shape_gone():
    """Regression guard for the pre-fix nested shape — form-actions
    immediately after </label> with no intervening form-grid close."""
    section = _section("// DEFAULT PLACEMENT METHOD")
    bad = re.search(
        r'</label>\s*<div class="form-actions">\s*'
        r'<button[^>]*data-save="placement"',
        section,
    )
    assert bad is None, (
        "v1.19.81: the pre-fix nested form-actions would re-introduce "
        "the ~48px double-gap above SAVE PLACEMENT"
    )


# ── #2 + #5 LIBRARY SECTIONS save label + status span ───────────


def test_libraries_save_button_scoped_html():
    """SSR default label must be the scoped // SAVE SECTIONS."""
    assert "<button class=\"btn btn-warn\" id=\"libraries-save-btn\" " \
           "type=\"button\" disabled>// SAVE SECTIONS</button>" in SRC, (
        "v1.19.81: libraries SAVE button SSR default must read "
        "// SAVE SECTIONS"
    )
    # The bare un-scoped label must be gone for this button.
    assert 'id="libraries-save-btn" type="button" disabled>// SAVE<' \
        not in SRC, (
        "v1.19.81: bare un-scoped // SAVE label must be gone"
    )


def test_libraries_save_button_scoped_js():
    """The JS-driven label (updateLibrariesSaveButton) must also use
    the scoped text — otherwise it overwrites the SSR default on
    first render."""
    assert "'// SAVE SECTIONS'" in JS
    assert "// SAVE SECTIONS (${n})" in JS
    # The pre-fix bare ternary must be gone.
    assert "? '// SAVE' :" not in JS, (
        "v1.19.81: the JS label setter must use // SAVE SECTIONS, "
        "not the old bare // SAVE"
    )


def test_libraries_save_status_bare_form_status():
    """#libraries-save-status must drop its lone `muted small`
    combo — every other save-status span is bare .form-status."""
    assert '<span class="form-status" id="libraries-save-status">' in SRC
    assert 'class="form-status muted small" id="libraries-save-status"' \
        not in SRC, (
        "v1.19.81: libraries-save-status must be bare .form-status"
    )


# ── #3 TVDB BRIDGE → form-subhead subsection ────────────────────


def test_tvdb_bridge_uses_form_subhead():
    """// TVDB BRIDGE must render as a form-subhead subsection, not
    a // prefix smuggled into a form-label-text."""
    assert '<p class="form-subhead">// TVDB BRIDGE</p>' in SRC, (
        "v1.19.81: // TVDB BRIDGE must be a form-subhead (peer of "
        "// CONNECTION / // TMDB INTEGRATION)"
    )
    assert '<span class="form-label-text">// TVDB BRIDGE</span>' not in SRC, (
        "v1.19.81: the // prefix must no longer live in a "
        "form-label-text"
    )


def test_tvdb_bridge_js_gate_and_button_survive():
    """The restructure must preserve the id the JS binds on and the
    rebuild button markup the v1.17.5 audit pins."""
    assert 'id="tvdb-bridge-section"' in SRC
    assert 'id="tvdb-bridge-rebuild-btn"' in SRC
    # v1.17.5 design-audit pin: exact button class string.
    assert 'class="btn btn-tiny btn-warn" id="tvdb-bridge-rebuild-btn"' \
        in SRC


def test_no_slash_prefix_in_any_form_label_text():
    """`//` is reserved for block-title + form-subhead. No
    form-label-text anywhere in settings may carry it (TVDB BRIDGE
    was the only offender)."""
    bad = re.findall(r'<span class="form-label-text">\s*//', SRC)
    assert not bad, (
        f"v1.19.81: // prefix found in a form-label-text "
        f"({len(bad)} site(s)) — reserve // for block-title / "
        f"form-subhead"
    )


# ── #4 test-result spans share the .form-status tone palette ────


def test_test_result_spans_use_form_status():
    assert '<span class="form-status" id="tmdb-test-result">' in SRC
    assert '<span class="form-status" id="tvdb-bridge-result">' in SRC
    assert 'class="muted small" id="tmdb-test-result"' not in SRC
    assert 'class="muted small" id="tvdb-bridge-result"' not in SRC


def test_test_result_tone_via_classlist():
    """The JS must set tone via .form-status-ok/fail classes, not
    inline style.color."""
    assert "result.classList.add('form-status-ok')" in JS
    assert "result.classList.add('form-status-fail')" in JS
    assert "resultEl.classList.add('form-status-ok')" in JS


def test_test_result_inline_color_writes_gone():
    """The pre-fix inline-color writes for these two result spans
    must be gone."""
    assert "result.style.color = 'var(--green)'" not in JS, (
        "v1.19.81: tmdb-test-result must use form-status-ok class, "
        "not inline green"
    )
    assert "result.style.color = 'var(--red)'" not in JS, (
        "v1.19.81: tmdb-test-result must use form-status-fail class, "
        "not inline red"
    )
    assert "resultEl.style.color = 'var(--green)'" not in JS, (
        "v1.19.81: tvdb-bridge-result must use form-status-ok class, "
        "not inline green"
    )


# ── End-to-end render ───────────────────────────────────────────


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


def test_settings_page_renders_with_uniformity_fixes(admin_client):
    """/settings must still render after the v1.19.81 changes."""
    r = admin_client.get("/settings", headers=AUTH)
    assert r.status_code == 200
    assert "// SAVE SECTIONS" in r.text
    assert '<p class="form-subhead">// TVDB BRIDGE</p>' in r.text
    assert "// DEFAULT PLACEMENT METHOD" in r.text
    # The TVDB bridge hint copy (v1.18.55) must survive the
    # restructure.
    assert "What this does:" in r.text
