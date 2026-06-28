"""v1.14.3 — oEmbed proxy routes by source (YouTube + SoundCloud).

Pre-fix the proxy was YouTube-only:

  - Endpoint named `/api/youtube/oembed`
  - Hardcoded `https://www.youtube.com/oembed` provider URL
  - 400 "not a YouTube URL" on any non-YT host

The diff tile in the info dialog (renderPendingUpdateDiff →
hydrateDiffTitles) feeds whatever URL the row has — once the
SoundCloud rollout (v1.14.0-2) made SC URLs first-class, those
URLs were 400'ing on the title hydration call and the placeholder
text never resolved.

v1.14.3 introduces a source-aware proxy:

  - New canonical endpoint `/api/source/oembed`
  - Routes by host: YouTube → youtube.com/oembed, SoundCloud →
    soundcloud.com/oembed
  - Identical {title, author_name, author_url, thumbnail_url}
    response shape so the JS render block didn't have to branch
  - Legacy `/api/youtube/oembed` kept as alias so a browser holding
    a cached app.js (pre-bump) doesn't 404 after deploy

Tests pin via static-text guards on api.py + app.js (consistent
with the v1.14.0-2 pattern). Behavioral coverage of the actual
HTTP roundtrip would need a real httpx mock harness the project
hasn't adopted — deferring per CLAUDE.md test conventions.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── New canonical endpoint exists ─────────────────────────────


def test_source_oembed_endpoint_defined():
    """The v1.14.3 canonical endpoint must exist at /api/source/oembed.
    Naming intentionally drops the 'youtube' prefix so future
    sources (Bandcamp, Vimeo, …) slot in without another rename."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert '@app.get("/api/source/oembed")' in api
    assert "async def api_source_oembed" in api


def test_legacy_youtube_endpoint_preserved_as_alias():
    """The pre-v1.14.3 endpoint must still be reachable so a browser
    with a stale app.js cache doesn't 404 on the diff tile hydration
    immediately after a deploy. The alias forwards through the same
    routing helper — no behavioral drift from the canonical path."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert '@app.get("/api/youtube/oembed")' in api
    assert "async def api_youtube_oembed" in api


# ── Source routing ────────────────────────────────────────────


def test_oembed_providers_table_lists_youtube_and_soundcloud():
    """The provider lookup table is the routing source-of-truth.
    Pin both entries so a refactor doesn't silently drop one."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    anchor = api.index("_OEMBED_PROVIDERS = {")
    block = api[anchor:anchor + 400]
    assert '"youtube": "https://www.youtube.com/oembed"' in block
    assert '"soundcloud": "https://soundcloud.com/oembed"' in block


def test_oembed_source_helper_recognizes_youtube_hosts():
    """_oembed_source_for must recognize YouTube + SoundCloud hosts.
    v1.20.43 switched from a bare substring check to parsed-hostname
    matching (closing an SSRF where instagram.com-in-the-path passed),
    so the helper now keys on _host_is(<domain>)."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    anchor = api.index("def _oembed_source_for(url: str)")
    body = api[anchor:anchor + 1700]
    assert "urlparse(url).hostname" in body
    assert '_host_is("youtube.com")' in body
    assert '_host_is("youtu.be")' in body
    assert '_host_is("soundcloud.com")' in body


def test_fetch_oembed_routes_via_provider_table():
    """The fetch helper must look the provider URL up via the source
    table — pre-fix it hardcoded the YT URL inline, which is what
    we're regressing against."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    anchor = api.index("def _fetch_oembed(url: str)")
    body = api[anchor:anchor + 1500]
    # Routing path: source lookup → provider table → request.
    assert "source = _oembed_source_for(url)" in body
    assert "provider = _OEMBED_PROVIDERS[source]" in body
    # Provider URL is no longer hardcoded inline at the call site.
    assert 'c.get(\n                    provider,' in body or \
           'c.get(provider,' in body or \
           'c.get(\n                    provider' in body


def test_no_hardcoded_youtube_oembed_url_at_fetch_call_site():
    """Regression guard: pre-v1.14.3 the YT oembed URL was inline
    at the httpx.get call. The whole point of v1.14.3 is to delete
    that hardcode — if it reappears at the fetch call site we've
    regressed."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    # The string still appears in the providers table (that's where
    # it lives now). Restrict the search to the _fetch_oembed body.
    anchor = api.index("def _fetch_oembed(url: str)")
    end = api.index("def ", anchor + 10)
    body = api[anchor:end]
    assert '"https://www.youtube.com/oembed"' not in body, (
        "v1.14.3: the YT oembed URL must come from _OEMBED_PROVIDERS, "
        "not be hardcoded inside _fetch_oembed"
    )


# ── Endpoint behavior contracts ───────────────────────────────


def test_source_endpoint_400s_on_unrecognized_host():
    """An unknown host (e.g. vimeo.com, example.com) must 400 — not
    silently fall through to a generic provider, not 404 (which the
    UI interprets as 'private/removed/geo-blocked' and would show
    misleading text)."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    anchor = api.index('@app.get("/api/source/oembed")')
    body = api[anchor:anchor + 1500]
    assert "status_code=400" in body
    assert "not a recognized source URL" in body


def test_source_endpoint_404s_on_lookup_failure():
    """Distinct from the 400: a recognized host whose oEmbed call
    fails (private/removed/geo-blocked) must 404 so the JS
    fall-through (catch + leave placeholder) fires correctly."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    anchor = api.index('@app.get("/api/source/oembed")')
    body = api[anchor:anchor + 1500]
    assert "status_code=404" in body
    assert "oEmbed lookup failed" in body


def test_legacy_alias_now_accepts_soundcloud_too():
    """The alias mustn't drift behaviorally from the canonical
    endpoint — both call _oembed_source_for and accept whatever it
    accepts. Pre-fix the YT-only validation lived inside this
    handler; v1.14.3 moves the gate to the shared helper so the
    alias inherits SC support automatically."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    anchor = api.index('@app.get("/api/youtube/oembed")')
    body = api[anchor:anchor + 1500]
    # The alias now uses _oembed_source_for too.
    assert "_oembed_source_for(url)" in body
    # And it must NOT contain the old hardcoded YT-only string-match
    # rejection ("not a YouTube URL"), which would re-narrow it.
    assert '"not a YouTube URL"' not in body
    assert "'not a YouTube URL'" not in body


def test_legacy_alias_no_longer_uses_youtube_only_substring_check():
    """Regression guard against the pre-v1.14.3 line:

        if not url or "youtube.com" not in url and "youtu.be" not in url:
            raise HTTPException(status_code=400, detail="not a YouTube URL")

    A future revert that accidentally restores this would silently
    re-break SoundCloud diff tiles via the alias path."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert 'detail="not a YouTube URL"' not in api


# ── JS callers updated ────────────────────────────────────────


def test_diff_tile_hydration_uses_source_endpoint():
    """hydrateDiffTitles is the SC-affecting call site — the diff
    tile shows whatever URL the row has, so it must hit the
    source-aware endpoint to get SC titles back."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function hydrateDiffTitles(root)")
    body = js[fn_anchor:fn_anchor + 2000]
    assert "/api/source/oembed?url=" in body
    # The legacy path must NOT linger here — the alias is for cached
    # browser app.js only, not for fresh code paths.
    assert "/api/youtube/oembed?url=" not in body


def test_manual_url_preview_uses_source_endpoint():
    """The manual-url dialog preview also routes through the
    source-aware endpoint for consistency.

    v1.15.129 refactor: the preview is now source-aware (YT
    AND SC) so the previewLastVid race-guard variable was
    renamed to previewLastKey (vid-or-url key). Anchor on the
    new variable; the /api/source/oembed call must still be
    reachable from the preview body."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    anchor = js.index("previewLastKey = key;")
    body = js[anchor:anchor + 2500]
    assert "/api/source/oembed?url=" in body


def test_no_app_js_calls_remain_on_legacy_youtube_endpoint():
    """No live JS code path should reference the legacy alias — the
    alias is for browsers with a cached app.js from BEFORE this
    deploy. Comments mentioning the path are fine."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Strip line comments so a documentation comment doesn't trip us.
    stripped = "\n".join(
        line for line in js.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "/api/youtube/oembed" not in stripped, (
        "v1.14.3: live JS calls must use /api/source/oembed; the "
        "/api/youtube/oembed alias is for stale browser caches only"
    )
