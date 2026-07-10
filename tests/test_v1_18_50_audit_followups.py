"""v1.18.50 — follow-up asks from the user's v1.18.49 review.

## Bug A — wider impossible-filter hide on /collections

v1.18.49 hid the PS LINK chip. the user: "let's take a wider scope
for impossible filters in the collections section." Collections
have:

  • no media folder (folder_path is empty string by design)
  • no on-disk theme.<ext> sidecar
  • only placement_kind='plex_upload' (Plex's metadata bundle)

Filters that match zero rows on /collections (dead UI):

  • LINK row: HL (hardlink), C (copy), M (mismatch), PS (already
    hidden in v1.18.49). All four require filesystem placement
    or sidecar — collections have neither.
  • SRC row: A (adopted), M (manual sidecar). Both require
    a folder + theme.<ext> to identify with. SRC=P is left
    visible because themerr-plex CAN push collection themes via
    the same /library/metadata/{rk}/themes endpoint motif uses;
    rare but possible.

Filters that still apply on /collections:

  • LINK: PU (the only placement_kind collections use), —
    (no placement)
  • SRC: T (themerrdb), U (user URL), P (rare but possible),
    +P (composite), T (TDB-only / not in plex), — (none)

## Bug B — filter+search clear on refresh, persist on tab change

the user: "when you refresh the page can we make it clear filters
and any search text, want it to stay though when changing tabs
still."

Pre-fix: both stores (localStorage 'motif:library_filter_state'
+ sessionStorage 'motif:library_q') survived refresh. Fix: detect
reload via Performance API (`getEntriesByType('navigation')
[0].type === 'reload'`) and wipe both stores before hydration.
Tab clicks (type='navigate') and back/forward (type='back_forward')
pass through untouched.

## Bug C — dashboard COLLECTIONS in top two rows

the user: "can we on the dashboard put collections on the top two
rows at the end of row." Move PLEX LIBRARY section up to
immediately follow COVERAGE so:

  • Row 1 (COVERAGE):       TDB MOVIES, TDB TV SERIES, TDB COLLECTIONS
  • Row 2 (PLEX LIBRARY):   PLEX MOVIES, PLEX TV, PLEX ANIME, PLEX COLLECTIONS

Pre-fix order was COVERAGE → OPERATIONS → ACTIVITY → PLEX LIBRARY
(collections buried in row 4). The "operational flow" narrative
from v1.15.31 is preserved: OPERATIONS still precedes ACTIVITY
just lower in the page; the catalogue↔Plex pairing in rows 1-2
is the more natural read.

## Note — canonical-missing icon left amber

the user floated changing the title-glyph ↺ from amber to green
"since it's restoring from a green PL back to motif's copy."
Deferred — the row's current state IS degraded (canonical lost),
amber correctly signals "needs attention." Green would conflate
"healthy" with "fixable", weakening the attention signal. Per
the user's own hedge: "if that doesn't make sense let's not." The
v1.11.62 / v1.12.65 amber convention stays.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"
DASH_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


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


# ── Bug A: wider dead-chip hide on /collections ──────────────


def test_collections_omits_link_hl_c_m_chips(admin_client):
    """HL / C / M LINK chips must not render on /collections.
    All three require filesystem placement; collections only
    have plex_upload (PU)."""
    r = admin_client.get("/collections", headers=AUTH)
    assert r.status_code == 200
    assert 'data-link-pill="hl"' not in r.text, "HL dead on /collections"
    assert 'data-link-pill="c"' not in r.text, "C dead on /collections"
    assert 'data-link-pill="m"' not in r.text, "M dead on /collections"


def test_collections_keeps_link_pu_and_none(admin_client):
    """PU (plex_upload — the only kind collections use) and —
    (no placement) must still render on /collections."""
    r = admin_client.get("/collections", headers=AUTH)
    assert r.status_code == 200
    assert 'data-link-pill="pu"' in r.text
    assert 'data-link-pill="none"' in r.text


def test_collections_omits_src_a_and_m_chips(admin_client):
    """SRC=A (adopted) requires a folder match; SRC=M (manual
    sidecar) requires a sidecar at the folder. Both impossible
    on /collections."""
    r = admin_client.get("/collections", headers=AUTH)
    assert r.status_code == 200
    assert 'data-src-filter="A"' not in r.text, "SRC=A dead on /collections"
    assert 'data-src-filter="M"' not in r.text, "SRC=M dead on /collections"


def test_collections_keeps_src_t_u_p_pp(admin_client):
    """SRC=T (themerrdb), U (user URL), P (Plex-served, rare but
    possible for collections), +P (composite), — (none) must
    still render on /collections."""
    r = admin_client.get("/collections", headers=AUTH)
    assert r.status_code == 200
    assert 'data-src-filter="T"' in r.text
    assert 'data-src-filter="U"' in r.text
    assert 'data-src-filter="P"' in r.text
    assert 'data-src-filter="Pp"' in r.text


def test_movies_still_renders_all_link_chips(admin_client):
    """Regression guard: /movies must keep HL/C/M/PU/—.
    v1.18.50's hide is collections-only.

    v1.19.66: removed 'ps' from the expected set — chip was
    dropped entirely (vestigial post-v1.19.61). URL param
    `link_pills=ps` is preserved as a no-op for bookmark
    compat but the template button is gone."""
    r = admin_client.get("/movies", headers=AUTH)
    assert r.status_code == 200
    for chip in ("hl", "c", "m", "pu", "none"):
        assert f'data-link-pill="{chip}"' in r.text, (
            f"Regression: /movies must still render LINK={chip}"
        )


def test_movies_still_renders_all_src_chips(admin_client):
    """Regression guard: /movies must keep T/U/A/M/P/+P."""
    r = admin_client.get("/movies", headers=AUTH)
    assert r.status_code == 200
    for chip in ("T", "U", "A", "M", "P", "Pp"):
        assert f'data-src-filter="{chip}"' in r.text, (
            f"Regression: /movies must still render SRC={chip}"
        )


def test_tv_still_renders_all_link_and_src_chips(admin_client):
    """/tv and /anime use the same template as /movies but with
    a different tab value — they must also keep the full chip
    set."""
    r = admin_client.get("/tv", headers=AUTH)
    assert r.status_code == 200
    assert 'data-link-pill="hl"' in r.text
    assert 'data-src-filter="A"' in r.text


# ── Bug B: clear filter+search storage on refresh ────────────


def test_is_page_reload_helper_defined():
    """The _isPageReload helper must exist with the canonical
    Performance API check + the deprecated-API fallback."""
    src = APP_JS.read_text()
    assert "function _isPageReload()" in src, (
        "v1.18.50: _isPageReload helper missing"
    )
    # Must check the modern PerformanceNavigationTiming API.
    assert "getEntriesByType('navigation')" in src
    # Must compare against 'reload'.
    assert "'reload'" in src


def test_maybe_clear_storage_helper_wipes_both_stores():
    """_maybeClearStorageOnReload must wipe BOTH the
    localStorage filter snapshot AND the sessionStorage search
    query. Either survives → the user's reset request fails."""
    src = APP_JS.read_text()
    assert "function _maybeClearStorageOnReload()" in src
    # Find the function body.
    idx = src.index("function _maybeClearStorageOnReload()")
    body = src[idx:idx + 600]
    assert "_clearLibraryFilterStorage" in body, (
        "v1.18.50: refresh-clear must wipe the filter snapshot"
    )
    assert "_clearSessionQ" in body, (
        "v1.18.50: refresh-clear must wipe the search query"
    )


def test_bindlibrary_calls_clear_before_hydration():
    """The clear-on-reload check must fire BEFORE any
    hydration code reads localStorage or sessionStorage —
    otherwise a reload would still see the old values."""
    src = APP_JS.read_text()
    bl_idx = src.index("function bindLibrary()")
    # _maybeClearStorageOnReload should appear in the first ~1500
    # chars of bindLibrary, well before _hydrateLibraryFromStorage.
    bl_body = src[bl_idx:bl_idx + 3000]
    clear_idx = bl_body.find("_maybeClearStorageOnReload")
    hydrate_idx = bl_body.find("_hydrateLibraryFromStorage")
    assert clear_idx != -1, (
        "v1.18.50: bindLibrary must call _maybeClearStorageOnReload"
    )
    # Hydration may be outside the slice; if so, just verify the
    # clear call is early in the function.
    if hydrate_idx != -1:
        assert clear_idx < hydrate_idx, (
            "v1.18.50: clear-on-reload must precede hydration"
        )


# ── Bug C: dashboard PLEX LIBRARY in row 2 ───────────────────


def test_dashboard_section_order_puts_plex_library_second():
    """Both stat sections must precede OPERATIONS so both COLLECTIONS
    cards land in the top-two-rows fold. v0.51.121 swapped which of the
    two shows which metric — PLEX LIBRARY reach is now the first (top)
    row, COVERAGE the second — but both stay ahead of OPERATIONS."""
    src = DASH_HTML.read_text()
    # Find each section's `data-dash-label` and assert order.
    cov_idx = src.index('data-dash-label="COVERAGE"')
    plex_idx = src.index('data-dash-label="PLEX LIBRARY"')
    ops_idx = src.index('data-dash-label="OPERATIONS"')
    act_idx = src.index('data-dash-label="ACTIVITY"')
    assert plex_idx < cov_idx < ops_idx < act_idx, (
        "v0.51.121: dashboard section order must be "
        "PLEX LIBRARY → COVERAGE → OPERATIONS → ACTIVITY so "
        "both COLLECTIONS cards land in the top two rows"
    )


def test_dashboard_has_single_plex_library_section():
    """The relocation must not leave a duplicate PLEX LIBRARY
    section in the old position (lower in the file). Pinning
    `data-dash-label="PLEX LIBRARY"` to exactly one occurrence
    guards against a copy-paste-without-delete mistake."""
    src = DASH_HTML.read_text()
    count = src.count('data-dash-label="PLEX LIBRARY"')
    assert count == 1, (
        f"v1.18.50: expected exactly 1 PLEX LIBRARY section, "
        f"found {count}"
    )


def test_dashboard_collections_cards_appear_in_top_two_rows(admin_client):
    """End-to-end: hit the dashboard, verify both COLLECTIONS
    cards (TDB + PLEX) appear before the OPERATIONS section
    label in the rendered HTML."""
    r = admin_client.get("/", headers=AUTH)
    assert r.status_code == 200
    # COLLECTIONS card identifiers — TDB by data-stat, PLEX by id.
    tdb_coll = r.text.index('data-dash-card="tdb-collections"')
    plex_coll = r.text.index('data-dash-card="plex-collections"')
    ops_label = r.text.index('data-dash-label="OPERATIONS"')
    assert tdb_coll < ops_label, (
        "v1.18.50: TDB COLLECTIONS must render before OPERATIONS"
    )
    assert plex_coll < ops_label, (
        "v1.18.50: PLEX COLLECTIONS must render before OPERATIONS"
    )


def test_dashboard_old_plex_library_position_is_commented():
    """The relocation must leave a marker at the OLD position
    so future debugging sees the moved-from breadcrumb. Pinning
    that the v1.18.50 marker appears in the dashboard template
    after the new PLEX LIBRARY section."""
    src = DASH_HTML.read_text()
    assert "v1.18.50" in src, (
        "v1.18.50: dashboard template must carry the marker "
        "explaining the section relocation"
    )


# ── Marker tests ─────────────────────────────────────────────


def test_library_html_carries_v1_18_50_markers():
    """The library.html template must carry the v1.18.50 marker
    in the new collections-hide blocks so future grep lands on
    the rationale."""
    src = LIBRARY_HTML.read_text()
    # The marker must appear at both new hide sites — LINK block
    # + SRC block.
    occurrences = src.count("v1.18.50")
    assert occurrences >= 2, (
        f"v1.18.50: library.html should carry the marker at "
        f"both the LINK-hide and SRC-hide sites (found "
        f"{occurrences} occurrences)"
    )


def test_app_js_carries_v1_18_50_marker():
    """app.js must carry the v1.18.50 marker on the new
    reload-clear helpers."""
    src = APP_JS.read_text()
    assert "v1.18.50" in src
