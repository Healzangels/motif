"""v1.18.23 — Opt-in API push for movies/TV.

the user's full architectural pass continued. v1.18.22 built the
foundation (PU chip, +P suppression for plex_upload). This tag
lets users actually choose API push for movie/TV rows via the
PLACE menu, plus a SWITCH PLACEMENT action to flip between
sidecar and API on already-placed rows.

## What ships in v1.18.23

  * Config: new `placement.default_method` setting ('file' or
    'api') + `MOTIF_DEFAULT_PLACEMENT_METHOD` env override.
    Default 'file'. Collections IGNORE this and always use 'api'.
  * Worker: `_do_place` reads job payload `kind`. Movie/TV rows
    with `kind='api'` route through `_do_place_collection` (the
    helper is media_type-agnostic at the SQL/HTTP level — same
    code path collections use). Absent payload kind falls back
    to the global setting.
  * Plex client: new `upload_theme` / `delete_theme` aliases
    for the collection-specific functions, so the misleading
    name doesn't leak into movie/TV call sites later.
  * API: `/api/items/{mt}/{tmdb}/replace` accepts a JSON body
    `{"kind": "file"|"api"}` that scopes the placement method.
    NEW `/api/items/{mt}/{tmdb}/switch-placement` endpoint
    inverts the current placement_kind (sidecar↔api).
  * Frontend: PLACE menu on movie/TV unplaced rows splits to
    `PUSH TO PLEX (FILE)` and `PUSH TO PLEX (API)`. Already-
    placed movie/TV rows get a `SWITCH TO SIDECAR` /
    `SWITCH TO API` action below RE-PUSH. Collections menu
    unchanged (no folder option exists).
  * Replace endpoint also DELETES any existing placements row
    when the caller passes a `kind`. Decision-locked: one
    placement per row (no coexistence of sidecar + API upload).

The settings page UI (radio in the Plex block) is deferred to
v1.18.24 — the config key + env override are sufficient for the
backend to operate; users can flip the default via env or yaml
edit while the UI lands later.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

CONFIG_FILE_PY = REPO / "app" / "core" / "config_file.py"
CONFIG_PY = REPO / "app" / "config.py"
WORKER_PY = REPO / "app" / "core" / "worker.py"
PLEX_PY = REPO / "app" / "core" / "plex.py"
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Config: placement.default_method ──────────────────────────


def test_placement_config_has_default_method_field():
    """PlacementConfig must declare `default_method: str = 'file'`."""
    src = CONFIG_FILE_PY.read_text()
    assert "default_method: str = \"file\"" in src


def test_env_override_maps_default_placement_method():
    """MOTIF_DEFAULT_PLACEMENT_METHOD env var must be in the
    config_file env-override map."""
    src = CONFIG_FILE_PY.read_text()
    assert "MOTIF_DEFAULT_PLACEMENT_METHOD" in src
    assert "placement.default_method" in src


def test_settings_exposes_default_placement_method():
    """`Settings.default_placement_method` property must return
    'file' or 'api', validated against the raw config value."""
    src = CONFIG_PY.read_text()
    assert "def default_placement_method(self)" in src
    fn_idx = src.index("def default_placement_method(self)")
    body = src[fn_idx:fn_idx + 700]
    # Validation: garbage falls back to 'file'.
    assert 'in ("file", "api")' in body


# ── Worker: kind-aware dispatch ───────────────────────────────


def test_worker_reads_payload_kind_before_dispatch():
    """`_do_place` must parse the job payload's `kind` field and
    route to `_do_place_collection` (the API-upload helper) when
    kind=='api'. Movie/TV rows with kind=='file' (or absent +
    default='file') stay on the legacy sidecar path."""
    src = WORKER_PY.read_text()
    fn_idx = src.index("def _do_place(self, job:")
    body = src[fn_idx:fn_idx + 6000]
    assert "payload_kind" in body
    assert "effective_kind" in body
    assert "default_placement_method" in body
    # When kind=='api', delegate to _do_place_collection.
    assert 'effective_kind == "api":' in body


def test_worker_fallback_resolves_pi_media_type_for_movie_tv():
    """`_do_place_collection` fallback query previously hardcoded
    `media_type = 'collection'`. v1.18.23 generalizes it so the
    helper works when invoked for movie/TV (`'tv'` themes map
    to `'show'` plex_items)."""
    src = WORKER_PY.read_text()
    # Find the fallback in _do_place_collection.
    fn_idx = src.index("def _do_place_collection(")
    body = src[fn_idx:fn_idx + 8000]
    assert "pi_media_type_fallback" in body
    # The mapping logic: tv → show, everything else unchanged.
    assert "\"show\" if media_type == \"tv\" else media_type" in body
    # Hardcoded 'collection' must be GONE from the fallback.
    assert (
        "AND media_type = 'collection' "
        "  AND section_id = ? LIMIT 1"
        not in body
    )


# ── Plex client: upload_theme alias ───────────────────────────


def test_plex_client_upload_theme_alias_exists():
    """PlexClient.upload_theme (alias for upload_collection_theme)
    must be defined so v1.18.23+ call sites don't have to use the
    collection-specific name."""
    src = PLEX_PY.read_text()
    assert "def upload_theme(" in src
    # Delegates unchanged to the collection function.
    fn_idx = src.index("def upload_theme(")
    body = src[fn_idx:fn_idx + 500]
    assert "self.upload_collection_theme(" in body


def test_plex_client_delete_theme_alias_exists():
    """Mirror alias for the delete path."""
    src = PLEX_PY.read_text()
    assert "def delete_theme(self" in src


# ── API: /replace accepts kind ────────────────────────────────


def test_replace_endpoint_parses_kind_body():
    """/replace must parse a JSON body `{kind: 'file'|'api'}`
    and validate against the two allowed values."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_replace_item(")
    # v1.18.60: widened 4000 → 6000. The REPLACE function body
    # grew ~1500 chars when the v1.18.60 audit added the
    # _teardown_plex_api_artifacts_for_placements call before
    # the DELETE FROM placements row drop.
    # v1.18.71: widened 6000 → 10000. REPLACE [kind=file] now
    # collects per-section rks into a dict instead of calling the
    # sync helper (atomic-teardown audit rollover) — ~1500 more
    # chars before the place-job INSERT.
    body = src[fn_idx:fn_idx + 10000]
    assert "raw_kind = (body or {}).get(\"kind\")" in body
    assert 'in ("file", "api"):' in body
    # Payload includes the kind key when set.
    assert '"kind"' in body
    assert 'payload_obj["kind"] = kind' in body


def test_replace_endpoint_deletes_existing_placements_on_kind_switch():
    """When `kind` is provided, /replace must DELETE existing
    placements rows before enqueuing the new job — decision-
    locked one-placement-per-row contract."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_replace_item(")
    # v1.18.60: widened 4000 → 6000. The REPLACE function body
    # grew ~1500 chars when the v1.18.60 audit added the
    # _teardown_plex_api_artifacts_for_placements call before
    # the DELETE FROM placements row drop.
    # v1.18.71: widened 6000 → 10000. REPLACE [kind=file] now
    # collects per-section rks into a dict instead of calling the
    # sync helper (atomic-teardown audit rollover) — ~1500 more
    # chars before the place-job INSERT.
    body = src[fn_idx:fn_idx + 10000]
    # The conditional DELETE keyed on `kind` being set.
    assert "if kind:" in body
    assert "DELETE FROM placements " in body


# ── API: /switch-placement endpoint ───────────────────────────


def test_switch_placement_endpoint_exists():
    """/switch-placement must be a registered POST endpoint."""
    src = API_PY.read_text()
    assert (
        '@app.post(\n        "/api/items/{media_type}/{tmdb_id}/switch-placement"\n    )'
        in src
        or '"/api/items/{media_type}/{tmdb_id}/switch-placement"' in src
    )
    assert "async def api_switch_placement(" in src


def test_switch_placement_rejects_collections():
    """Collections always use API push (no folder exists).
    Switching makes no sense — endpoint must reject with 400."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_switch_placement(")
    body = src[fn_idx:fn_idx + 3000]
    assert 'if media_type == "collection":' in body
    assert "status_code=400" in body


def test_switch_placement_inverts_existing_kind():
    """The endpoint must read the current placement_kind set
    and target the inverse (plex_upload → file; anything else
    → api)."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_switch_placement(")
    # v1.18.60: widened 4000 → 6000. The REPLACE function body
    # grew ~1500 chars when the v1.18.60 audit added the
    # _teardown_plex_api_artifacts_for_placements call before
    # the DELETE FROM placements row drop.
    # v1.18.71: widened 6000 → 10000. REPLACE [kind=file] now
    # collects per-section rks into a dict instead of calling the
    # sync helper (atomic-teardown audit rollover) — ~1500 more
    # chars before the place-job INSERT.
    body = src[fn_idx:fn_idx + 10000]
    assert 'target_kind = "file"' in body
    assert 'target_kind = "api"' in body
    # The inversion rule.
    assert '"plex_upload" in kinds' in body


# ── Frontend: PLACE menu items ────────────────────────────────


def test_js_menu_has_push_file_and_push_api_for_movie_tv():
    """Movie/TV unplaced rows must surface PUSH TO PLEX (FILE)
    and PUSH TO PLEX (API) as separate menu items. Collections
    keep the single PUSH TO PLEX item (no folder option)."""
    js = APP_JS.read_text()
    assert "'replace-file', 'PUSH TO PLEX (FILE)'" in js
    assert "'replace-api', 'PUSH TO PLEX (API)'" in js
    # Both gated on `isCollectionRow` being false.
    # v1.18.46: anchor-based slicing. Pre-fix this was a fixed
    # backward window (500 → 1200 chars across v1.18.28+). Now
    # walks backward from 'replace-file' to the nearest
    # `if (isCollectionRow)` opening. Survives future comment
    # additions in the collection branch.
    file_idx = js.index("'replace-file'")
    # Walk backward to the nearest isCollectionRow gate.
    branch_idx = js.rfind("if (isCollectionRow)", 0, file_idx)
    assert branch_idx != -1, (
        "v1.18.23: PUSH TO PLEX (FILE)/(API) split must live in "
        "the !isCollectionRow branch — collections keep one button"
    )


def test_js_switch_placement_menu_item_on_placed_rows():
    """Already-placed movie/TV rows get a SWITCH TO SIDECAR /
    SWITCH TO API menu item that inverts the current kind."""
    js = APP_JS.read_text()
    assert "'switch-placement'" in js
    assert "'SWITCH TO SIDECAR'" in js
    assert "'SWITCH TO API'" in js
    # Label must branch on placement_kind === 'plex_upload'.
    # Find the menuItemHtml(...'switch-placement'...) call — NOT
    # the comment that quotes the action key above it.
    push_anchor = "menuItemHtml(\n          'switch-placement'"
    sw_idx = js.index(push_anchor)
    block = js[max(0, sw_idx - 800):sw_idx]
    assert "isPlexUpload" in block, (
        "v1.18.23: switch-placement menu push must use the "
        "isPlexUpload discriminator to pick the SWITCH TO "
        "SIDECAR vs SWITCH TO API label"
    )


def test_js_replace_helper_accepts_kind_param():
    """The replaceTheme JS helper must accept a `kind` arg and
    POST a JSON body when set. Pre-fix it sent no body."""
    js = APP_JS.read_text()
    fn_idx = js.index("async function replaceTheme(")
    body = js[fn_idx:fn_idx + 2000]
    assert "replaceTheme(mediaType, tmdbId, btn, kind)" in body
    assert "JSON.stringify({ kind })" in body


def test_js_dispatch_routes_replace_file_and_replace_api():
    """The action dispatcher must handle 'replace-file' and
    'replace-api' by calling replaceTheme with the explicit
    kind."""
    js = APP_JS.read_text()
    assert "act === 'replace-file'" in js
    assert "act === 'replace-api'" in js
    assert "act === 'switch-placement'" in js
    # The kind argument propagates.
    file_idx = js.index("act === 'replace-file'")
    block = js[file_idx:file_idx + 500]
    assert "'file'" in block
    api_idx = js.index("act === 'replace-api'")
    block = js[api_idx:api_idx + 500]
    assert "'api'" in block


# ── End-to-end: settings default_placement_method validates ───


def test_settings_validates_to_file_on_garbage():
    """Settings.default_placement_method validates input. Any
    value other than 'file' or 'api' falls back to 'file'.
    Uses a lightweight stub instead of constructing a full
    Settings (which requires a yaml file on disk)."""
    from app.core.config_file import PlacementConfig
    from app.config import Settings
    class _Cfg:
        def __init__(self, method):
            self.placement = PlacementConfig(default_method=method)
    # Bind the property to a stub Settings-like object.
    prop = Settings.default_placement_method.fget
    assert prop(type("S", (), {"_cfg": _Cfg("garbage")})()) == "file"
    assert prop(type("S", (), {"_cfg": _Cfg("file")})()) == "file"
    assert prop(type("S", (), {"_cfg": _Cfg("api")})()) == "api"
    # Case-insensitive: "API" should also map to api.
    assert prop(type("S", (), {"_cfg": _Cfg("API")})()) == "api"


def test_settings_accepts_api_value():
    """Settings.default_placement_method accepts 'api' (covered
    above; this is a focused pin for clarity)."""
    from app.core.config_file import PlacementConfig
    from app.config import Settings
    class _Cfg:
        placement = PlacementConfig(default_method="api")
    prop = Settings.default_placement_method.fget
    assert prop(type("S", (), {"_cfg": _Cfg()})()) == "api"
