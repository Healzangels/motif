"""v1.15.25 — flash follow-ups: SSR Settings ENV badges + library
resolution chips.

the user (after v1.15.21/22 nav + topbar SSR): "lets check
everywhere make sure this bug doesn't exist anywhere else"

## Audit + this tag's scope

The v1.15.22 audit identified five remaining flash surfaces:
- UPD chip (deferred — needs updates_pending SQL extraction)
- Dashboard cards (deferred — ~10 individually-conditional)
- Settings ENV badges (this tag)
- Library resolution chips (this tag)
- Settings tab panels (deferred — URL hash isn't server-visible)

## Fixes

### 1. Settings ENV badges

Pre-fix every `<span class="form-env-badge" data-env-badge="X"
style="display:none">// ENV OVERRIDE</span>` was hidden by
default + JS reveal post-/api/config (v1.15.16 startup log shows
the user has 8 env overrides). Visible flicker on every /settings
load for env-driven deploys.

Fix: register `env_overrides()` Jinja global returning
`settings.env_overrides()` dict. settings.html badges add Jinja
conditional `{% if "X" not in env_overrides() %} style="display:
none"{% endif %}` so HTML received already has correct
visibility.

### 2. Library resolution chips

Pre-fix `<div class="chips" role="tablist" aria-label="resolution"
style="display:none">` + JS adaptLibraryFourkToggle reveal
post-/api/stats. Flash on every library page load (every nav to
MOVIES/TV/ANIME).

Fix: new `library_resolution_state(tab)` Jinja global runs the
same EXISTS query nav_tab_availability uses but per-variant
(standard/fourk). library.html applies Jinja conditionals on:
- the chip set wrapper (hide when neither variant exists)
- each individual chip (hide when that variant doesn't exist)
- chip-active class (route handler already parses ?fourk URL
  param, template uses it directly)
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"


# ── Settings ENV badges SSR ───────────────────────────────────


def test_env_overrides_jinja_global_registered():
    """The helper must be registered as a Jinja env global so
    settings.html can call `env_overrides()` directly."""
    src = API_PY.read_text()
    assert "def _env_overrides_for_template(" in src
    assert 'templates.env.globals["env_overrides"]' in src


def test_env_overrides_helper_returns_safe_default_on_failure():
    """If settings.env_overrides() raises (corrupt config /
    edge-case state), helper must return empty dict so badges
    just stay hidden — JS recovers when /api/config lands.
    Same fallback shape as v1.15.21/22 SSR helpers."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _env_overrides_for_template(")
    fn_end = src.index("templates.env.globals", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    assert "except Exception:" in fn_body
    assert "return {}" in fn_body


def test_settings_template_env_badges_use_ssr_conditional():
    """All form-env-badge spans must apply the SSR Jinja
    conditional. Pin via substring count: every badge has the
    `{% if "X" not in env_overrides() %} style="display:none"
    {% endif %}` shape after each `data-env-badge="X"`."""
    src = SETTINGS_HTML.read_text()
    badge_count = src.count('class="form-env-badge"')
    conditional_count = src.count("not in env_overrides()")
    assert badge_count > 0, "no form-env-badge spans found"
    assert conditional_count == badge_count, (
        f"v1.15.25: expected every form-env-badge ({badge_count}) "
        f"to have an env_overrides() Jinja conditional; got "
        f"{conditional_count} conditionals"
    )


def test_settings_template_no_unconditional_env_badge_display_none():
    """Defensive guard: the unconditional `style="display:none"`
    on form-env-badge spans must be gone. Pre-fix pattern was
    `<span class="form-env-badge" data-env-badge="X" style="display:
    none">// ENV OVERRIDE</span>`. Post-fix any display:none lives
    inside a Jinja conditional."""
    src = SETTINGS_HTML.read_text()
    # Look for the bare pattern (no Jinja conditional preceding
    # style="display:none") inside an env-badge span.
    bad_pattern = 'class="form-env-badge" data-env-badge="'
    for match_start in [
        i for i in range(len(src))
        if src.startswith(bad_pattern, i)
    ]:
        # Walk to end of span open tag.
        tag_end = src.index(">", match_start)
        tag = src[match_start:tag_end]
        assert "{% if" in tag or "display:none" not in tag, (
            f"v1.15.25: form-env-badge with unconditional "
            f"display:none found: {tag!r}"
        )


# ── Library resolution chips SSR ──────────────────────────────


def test_library_resolution_state_jinja_global_registered():
    """Helper for the library page's STANDARD/4K toggle.
    Registered as Jinja env global so library.html can call it
    with the current `tab` value."""
    src = API_PY.read_text()
    assert "def _library_resolution_state(" in src
    assert 'templates.env.globals["library_resolution_state"]' in src


def test_library_resolution_helper_handles_anime_separately():
    """Anime sections are flagged via is_anime=1 (movie/show
    type doesn't distinguish them). Helper must handle the
    anime tab via the is_anime flag, not type='movie'/'show'."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _library_resolution_state(")
    fn_end = src.index("templates.env.globals", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    assert 'tab == "anime"' in fn_body
    assert "is_anime=1" in fn_body
    # Movies/TV branch must use type+is_anime=0 to exclude anime.
    assert "is_anime=0" in fn_body


def test_library_resolution_helper_returns_per_variant_flags():
    """Returns has_standard / has_fourk + a show_chips
    convenience boolean. Pin all three keys so library.html's
    template can rely on them."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _library_resolution_state(")
    fn_end = src.index("templates.env.globals", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    assert '"has_standard"' in fn_body
    assert '"has_fourk"' in fn_body
    assert '"show_chips"' in fn_body


def test_library_resolution_helper_falls_back_safely():
    """DB-error fallback: all flags False (chips hidden) so the
    page still renders. JS recovers from /api/stats on the next
    poll.
    v1.15.35: bare `except Exception:` was upgraded to
    `except Exception as e:` + log.warning so DB hiccups become
    visible (pre-fix the silent absorb hid the 4K/Standard
    chips with zero operator visibility). The fallback behavior
    (safe defaults) is unchanged — the upgrade is logging-only."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _library_resolution_state(")
    fn_end = src.index("templates.env.globals", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    assert "except Exception as e:" in fn_body
    assert "log.warning(" in fn_body


def test_library_template_uses_ssr_resolution_state():
    """library.html must call the helper + apply Jinja
    conditionals. Pin the call site + the show_chips wrapper +
    per-chip variant gates."""
    src = LIBRARY_HTML.read_text()
    assert "library_resolution_state(tab)" in src
    # Chip wrapper hides when neither variant exists.
    assert "_res.show_chips" in src
    # Per-chip variant gates.
    assert "_res.has_standard" in src
    assert "_res.has_fourk" in src


def test_library_template_chip_active_reflects_url_fourk():
    """The chip-active class must render based on the route
    handler's `fourk` variable (parsed from ?fourk URL param).
    Pre-fix the chip-active was always on // STANDARD; JS
    flipped after /api/stats. SSR matches the URL state on
    first paint."""
    src = LIBRARY_HTML.read_text()
    chip_anchor = src.index('aria-label="resolution"')
    chip_end = src.index("</div>", chip_anchor)
    chip_block = src[chip_anchor:chip_end]
    # Both buttons reference the `fourk` template var.
    assert "{% if not fourk %} chip-active" in chip_block
    assert "{% if fourk %} chip-active" in chip_block


def test_library_template_no_unconditional_resolution_display_none():
    """Defensive: the wrapper div's `style="display:none"` must
    only fire inside the Jinja conditional. Pre-fix the
    unconditional display:none was the source of the flash."""
    src = LIBRARY_HTML.read_text()
    chip_anchor = src.index('aria-label="resolution"')
    chip_open_end = src.index(">", chip_anchor)
    chip_open_tag = src[chip_anchor:chip_open_end]
    # Should NOT have a bare `style="display:none"`. If style
    # exists at all, must be inside `{% if ... %}`.
    if 'style="display:none"' in chip_open_tag:
        assert "{% if" in chip_open_tag, (
            "v1.15.25: resolution chip wrapper must guard its "
            "display:none behind a Jinja conditional"
        )
