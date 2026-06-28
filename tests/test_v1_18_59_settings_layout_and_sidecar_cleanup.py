"""v1.18.59 — three follow-ups from the user's v1.18.58 deploy.

> "the settings page for the diag is a bit messed up also still
>  seeing the larger the normal space below probe tdb url it
>  seems. also show the swapped to use api ... had a left over
>  sidecar file even though it should have been removed when
>  changed to api. it surfaced in the orphan scan."

## Bug A — DIAGNOSTICS panel layout broken

The DIAGNOSTICS panel wrapped its OPEN ORPHAN SCAN button in a
`<div class="form-grid"><div class="form-label">` structure
meant for labelled inputs. The form-label primitive adds
internal padding + label-row alignment that fights an action
button, producing the broken-border artifacts the user's
screenshot showed.

Fix: restructured to the canonical `<p class="form-hint"> +
<div class="form-actions"><a class="btn"></a></div>` pattern
that every other settings section uses (PROBE TDB URLS,
REPROBE FAILURES, TVDB BRIDGE).

## Bug B — extra space below // PROBE TDB URLS persists

v1.18.57 tightened `.form-hint-divider` margins from
`18px 0 10px` → `10px 0 6px` which closed most of the gap.
the user reported some still remained.

Root cause: `.block-body` sets `white-space: pre-wrap`
(line 767) to support inline `<code>` blocks. The pre-wrap
preserves the literal newlines + indentation between sibling
elements in the HTML source. Between the form-actions div and
the form-hint-divider, the ~3 lines of source whitespace
rendered as visible vertical space (~1.7em of line-height
inheritance ≈ 24px).

Fix: add `white-space: normal` to `.form-hint-divider` AND
`.form-actions` so they collapse surrounding text whitespace
without affecting the rules' intentional spacing. Mirrors the
v1.14.98 fix that put the same override on `.form-hint`.

## Bug C — leftover sidecar on direct API placement

the user's Titans (2018) row showed `placement_kind='plex_upload'`
but the orphan scan flagged a stranded sidecar at the Plex
media folder. The history confirmed direct API placement (no
SWITCH event — just download + upload).

`_do_place_collection` upserts the placements row + updates
local_files + sets plex_items.has_theme=1, but never touches
any pre-existing sidecar at the Plex media folder. v1.18.36's
SWITCH file→api cleanup only runs when the user explicitly
clicks SWITCH PLACEMENT — direct API placements (initial
deploy, REPLACE TDB with kind=api, automatic via
default_placement_method) didn't get the cleanup.

Fix: mirror the v1.18.36 SWITCH file→api cleanup into
`_do_place_collection`. After the API upload succeeds + DB
state lands, for movie/TV rows (collections skip — no folder),
look up plex_items.folder_path, call
`plex_enum.find_theme_sidecar_path()` (the v1.18.43 helper
that walks host→container candidates), and unlink anything
it finds. Logged at INFO with the same "removed sidecar"
shape the SWITCH path uses.

Best-effort: cleanup failures (transient mount issues, etc.)
log at WARN but don't fail the place job — the orphan scan
already surfaces any sidecar we miss.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
WORKER_PY = REPO / "app" / "core" / "worker.py"


# ── Bug A: DIAGNOSTICS uses form-actions, not form-label ────


def test_diagnostics_panel_uses_form_actions_pattern():
    """The OPEN ORPHAN SCAN button must live inside a
    `<div class="form-actions">` not a `<div class="form-grid">
    <div class="form-label">` structure. The form-label
    wrapper was the source of the broken-border artifact
    the user's screenshot showed."""
    src = SETTINGS_HTML.read_text()
    # Anchor on the DIAGNOSTICS panel header.
    diag_idx = src.index('data-panel="diagnostics"')
    diag_block = src[diag_idx:diag_idx + 3000]
    # The action button must be in a form-actions wrapper.
    assert 'class="form-actions"' in diag_block, (
        "v1.18.59: DIAGNOSTICS panel must use .form-actions "
        "wrapper around the OPEN ORPHAN SCAN button"
    )
    # The pre-fix form-grid > form-label wrapper around the
    # button must NOT survive (form-grid is fine elsewhere
    # for actual labelled inputs, just not around the button).
    # Slice from the orphan-scan link backwards to verify it's
    # NOT inside a form-label.
    link_idx = diag_block.index('href="/admin/orphans"')
    preceding = diag_block[:link_idx]
    # The closest opening tag before the link should be
    # .form-actions, not .form-label.
    last_form_actions = preceding.rfind('class="form-actions"')
    last_form_label = preceding.rfind('class="form-label"')
    assert last_form_actions > last_form_label, (
        "v1.18.59: OPEN ORPHAN SCAN link must be inside a "
        ".form-actions wrapper (not .form-label)"
    )


def test_diagnostics_panel_button_uses_btn_warn():
    """The OPEN ORPHAN SCAN button must use the standard
    `btn btn-warn` class (matching every other action button
    in settings — PROBE TDB URLS, REPROBE FAILURES, REBUILD
    BRIDGE). The pre-fix `btn-tiny lib-source-themerrdb`
    classes were custom one-offs."""
    src = SETTINGS_HTML.read_text()
    diag_idx = src.index('data-panel="diagnostics"')
    diag_block = src[diag_idx:diag_idx + 3000]
    # Locate the OPEN ORPHAN SCAN link element.
    link_idx = diag_block.index('href="/admin/orphans"')
    # Walk back to the opening <a tag.
    a_open = diag_block.rfind('<a ', 0, link_idx)
    link_tag = diag_block[a_open:link_idx + 200]
    assert 'btn-warn' in link_tag, (
        "v1.18.59: OPEN ORPHAN SCAN button must use btn-warn "
        "class for visual consistency with sibling buttons"
    )


# ── Bug B: white-space:normal collapse on form-actions/divider ─


def test_form_actions_overrides_pre_wrap():
    """`.form-actions` must include `white-space: normal` so the
    inherited pre-wrap from .block-body doesn't render source
    newlines as visible vertical space."""
    src = APP_CSS.read_text()
    rule_idx = src.index(".form-actions {")
    rule_end = src.index("}", rule_idx)
    rule = src[rule_idx:rule_end]
    assert "white-space: normal" in rule, (
        "v1.18.59: .form-actions must override block-body's "
        "white-space: pre-wrap"
    )


def test_form_hint_divider_overrides_pre_wrap():
    """`.form-hint-divider` must also collapse surrounding
    whitespace — same rationale as form-actions."""
    src = APP_CSS.read_text()
    rule_idx = src.index(".form-hint-divider {")
    rule_end = src.index("}", rule_idx)
    rule = src[rule_idx:rule_end]
    assert "white-space: normal" in rule, (
        "v1.18.59: .form-hint-divider must override block-body's "
        "white-space: pre-wrap"
    )


# ── Bug C: sidecar cleanup after API place ──────────────────


def test_do_place_collection_calls_find_theme_sidecar_path():
    """`_do_place_collection` must invoke
    `find_theme_sidecar_path` to locate any stranded sidecar
    at the Plex media folder for movie/TV rows after the API
    upload succeeds. Pin the import + call."""
    src = WORKER_PY.read_text()
    # Anchor on the function name to scope the assertion.
    fn_idx = src.index("def _do_place_collection(")
    # v1.18.68: window widened 15000 → 25000 — see header on
    # test_do_place_collection_cleanup_logs_removal for context.
    # v1.18.69: window widened 25000 → 35000 — the size-rejection
    # sidecar fallback added ~100 lines of failure-branch handling
    # PLUS a WARNING/INFO branch on log_event, pushing the
    # `def _do_refresh(` anchor past the old window.
    body = src[fn_idx:src.index("def _do_refresh(", fn_idx) + 20]  # v1.22.15: anchor on fn end
    assert "find_theme_sidecar_path" in body, (
        "v1.18.59: _do_place_collection must call "
        "find_theme_sidecar_path to clean up leftover sidecars"
    )
    # The cleanup branch must skip collections (media_type !=
    # 'collection' guard).
    assert 'media_type != "collection"' in body, (
        "v1.18.59: cleanup must skip collections (they have "
        "no media folder)"
    )


def _flatten_concatenated_strings(body: str) -> str:
    """Python adjacent-string-literal concatenation lets us split
    log messages like `"removed " "leftover sidecar"` across
    lines. Strip the quote+whitespace+quote breaks so substring
    asserts work on the runtime value."""
    import re
    # Collapse `"  "` (close quote → whitespace → open quote)
    return re.sub(r'"\s*"', '', body)


def test_do_place_collection_cleanup_logs_removal():
    """The cleanup path must log at INFO when it removes a
    sidecar so docker logs show the action. Pin the log shape
    so a future refactor can't silently drop the breadcrumb."""
    src = WORKER_PY.read_text()
    fn_idx = src.index("def _do_place_collection(")
    # v1.18.68: window widened 15000 → 25000 — the atomic-sidecar
    # fix added ~80 lines of upload-result handling + payload-driven
    # sidecar removal BEFORE the v1.18.59 cleanup block, pushing it
    # past the old window.
    # v1.21.52: 30000 → 32000 — the edition_key conflict-target widen
    # added chars to the placements upsert, nudging the cleanup log out.
    body = _flatten_concatenated_strings(src[fn_idx:src.index("def _do_refresh(", fn_idx) + 20])
    assert "removed leftover sidecar" in body, (
        "v1.18.59: cleanup must log a 'removed leftover sidecar' "
        "breadcrumb on success"
    )
    # And it's a log.info (not log.warning) since the cleanup
    # is a desired outcome, not a failure.
    assert "log.info" in body[body.index("removed leftover sidecar") - 200:
                                body.index("removed leftover sidecar")]


def test_do_place_collection_cleanup_failure_logs_warn():
    """OSError on unlink must log at WARN but NOT fail the
    place job — cleanup is best-effort. The orphan scan will
    surface any sidecar we miss."""
    src = WORKER_PY.read_text()
    fn_idx = src.index("def _do_place_collection(")
    # v1.18.68: window widened 15000 → 25000 — the atomic-sidecar
    # fix added ~80 lines of upload-result handling + payload-driven
    # sidecar removal BEFORE the v1.18.59 cleanup block, pushing it
    # past the old window.
    # v1.18.94: widened 25000 → 28000 — the plex_rejected stamp
    # added ~26 lines INSIDE the function (between the upload-fail
    # classification and the raise), pushing the cleanup block
    # past the v1.18.68 window.
    # v1.21.8: widened 30000 → 32000 — the M1 transaction() wrap on
    # the collection place-outcome stamp added a comment block ahead
    # of the cleanup section.
    body = _flatten_concatenated_strings(src[fn_idx:src.index("def _do_refresh(", fn_idx) + 20])
    assert "could not unlink leftover sidecar" in body, (
        "v1.18.59: cleanup unlink failure must log a 'could not "
        "unlink leftover sidecar' WARN breadcrumb"
    )
    # And it's a log.warning (not error / info).
    idx = body.index("could not unlink leftover sidecar")
    assert "log.warning" in body[idx - 200:idx]


def test_cleanup_runs_after_plex_items_has_theme_update():
    """Structural pin: the cleanup block must run AFTER the
    plex_items has_theme=1 update (which signals the upload
    landed). Doing it BEFORE would risk deleting a sidecar
    when the upload itself failed."""
    src = WORKER_PY.read_text()
    fn_idx = src.index("def _do_place_collection(")
    # Widen the window — full function body extends past 15k chars
    # with all the comments.
    # v1.18.69: window widened 25000 → 35000 — the size-rejection
    # sidecar fallback added ~100 lines of failure-branch handling
    # PLUS a WARNING/INFO branch on log_event, pushing the
    # `def _do_refresh(` anchor past the old window.
    body = src[fn_idx:src.index("def _do_refresh(", fn_idx) + 20]  # v1.22.15: anchor on fn end
    has_theme_idx = body.index("UPDATE plex_items SET has_theme = 1")
    cleanup_idx = body.index('find_theme_sidecar_path')
    assert has_theme_idx < cleanup_idx, (
        "v1.18.59: cleanup must run AFTER the plex_items "
        "has_theme update (so we only clean up on confirmed "
        "successful upload)"
    )
    # And it must run before the next function definition (so
    # it's actually inside _do_place_collection, not stranded
    # later in the file).
    fn_end_anchor = body.index("def _do_refresh(", cleanup_idx)
    assert cleanup_idx < fn_end_anchor


def test_cleanup_skips_collections():
    """Structural pin: the cleanup branch is gated on
    `media_type != 'collection'`. Collections have no media
    folder, so the cleanup would be a no-op anyway, but the
    explicit guard saves a needless plex_items lookup."""
    src = WORKER_PY.read_text()
    fn_idx = src.index("def _do_place_collection(")
    # v1.18.68: window widened 15000 → 25000 — see header on
    # test_do_place_collection_cleanup_logs_removal for context.
    # v1.18.69: window widened 25000 → 35000 — the size-rejection
    # sidecar fallback added ~100 lines of failure-branch handling
    # PLUS a WARNING/INFO branch on log_event, pushing the
    # `def _do_refresh(` anchor past the old window.
    body = src[fn_idx:src.index("def _do_refresh(", fn_idx) + 20]  # v1.22.15: anchor on fn end
    # The guard must wrap the cleanup block.
    guard_idx = body.index('media_type != "collection" and cached_rk')
    cleanup_idx = body.index('find_theme_sidecar_path')
    assert guard_idx < cleanup_idx, (
        "v1.18.59: media_type != 'collection' guard must "
        "precede the cleanup find_theme_sidecar_path call"
    )


def test_cleanup_outer_except_logs_warn_not_raises():
    """Structural pin: the cleanup block has an outer
    `except Exception` that logs at WARN and continues —
    a transient mount issue or db hiccup during cleanup must
    NOT fail the place job. The orphan scan will surface any
    sidecar we miss."""
    src = WORKER_PY.read_text()
    fn_idx = src.index("def _do_place_collection(")
    # v1.18.69: window widened 25000 → 35000 — the size-rejection
    # sidecar fallback added ~100 lines of failure-branch handling
    # PLUS a WARNING/INFO branch on log_event, pushing the
    # `def _do_refresh(` anchor past the old window.
    body = src[fn_idx:src.index("def _do_refresh(", fn_idx) + 20]  # v1.22.15: anchor on fn end
    cleanup_idx = body.index('find_theme_sidecar_path')
    # Slice from cleanup to the next function definition.
    fn_end_anchor = body.index("def _do_refresh(", cleanup_idx)
    after_cleanup = body[cleanup_idx:fn_end_anchor]
    # The cleanup's outer except must exist.
    assert "except Exception as e:" in after_cleanup
    # Must log at WARN ("sidecar cleanup\n        scan failed"
    # or similar — concatenated literal).
    flat = _flatten_concatenated_strings(after_cleanup)
    assert "sidecar cleanup" in flat
    # And NO raise inside the cleanup's own except handler.
    # (Other raises later in the file outside this block are
    # fine — we only care about the cleanup's local exception
    # handling.) Slice between `except Exception as e:` and the
    # next `log_event` (which marks the end of the cleanup
    # block + start of the success-event report).
    except_idx = after_cleanup.index("except Exception as e:")
    log_event_idx = after_cleanup.index("log_event", except_idx)
    cleanup_except_block = after_cleanup[except_idx:log_event_idx]
    raises_in_except = cleanup_except_block.count("raise ")
    assert raises_in_except == 0, (
        "v1.18.59: cleanup outer except must NOT raise — "
        "place job continues even if cleanup fails"
    )


# ── Version marker ──────────────────────────────────────────


def test_v1_18_59_markers_present():
    """The fix sites must carry v1.18.59 markers."""
    assert "v1.18.59" in WORKER_PY.read_text()
    assert "v1.18.59" in APP_CSS.read_text()
    assert "v1.18.59" in SETTINGS_HTML.read_text()
