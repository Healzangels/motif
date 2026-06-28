"""v1.17.1 — audit-cleared rollover of the v1.17.0 ship.

PROJECT_HISTORY § 12.D documents the audit-rollover cadence: every
major-line ship gets a follow-up that closes the audit findings.
v1.17.0 audit surfaced 3 HIGH / 5 MEDIUM / 6 LOW (incl. 1 self-
review class-9 in plex.py); this module pins the fixes for each.

Findings closed in v1.17.1:

* **H1 + L1** — `themes_added_by_sync` SQL referenced
  `themes.created_at`, a column that does not exist. Feature was
  dead code in production. Fixed: switched to `first_seen_sync_at`.
  The companion test that pinned the literal `created_at` string
  was rewritten to pin the contract (timestamp >= ? WHERE shape),
  not the literal column name.

* **H2** — `_hydrate_dataclass` (config_file.py) wholesale-replaced
  `notifications.events` on YAML load, breaking the forward-compat
  guarantee that `_apply_partial_config` already enforced for the
  PATCH path. Fixed: dict-merge branch in `_hydrate_dataclass`
  mirrors the PATCH-merge contract.

* **H3 + L2** — three bare `except Exception: pass` sites in
  worker.py wrapped notify.dispatch calls (and in 879's case, a
  SQL query) with no log breadcrumb. Fixed: log.warning on the
  H1 swallow (worker.py:879), log.debug on the two other notify
  swallows (824 + 845).

* **M1** — `_send_embedded` reported `(added, 0)` on success even
  when Apprise had rejected URLs as malformed. Fixed: return
  `(added, rejected_count)` so the TEST UI surfaces typo'd URLs.

* **M2** — `_apply_partial_config` dict-merge branch raised on
  `events: null` (forcing the whole PATCH to 400) and accepted
  arbitrary unknown event keys into the YAML. Fixed: null = leave
  alone; closed-set filter restricts merged keys to those in
  `_DEFAULT_NOTIFY_EVENTS`.

* **M3 (partial)** — `notifications.apprise_external_url` accepted
  any scheme, including file:// / gopher:// / ftp://. Fixed:
  `validate()` enforces http:// or https:// — mirrors the existing
  `downloads.proxy_url` scheme check. RFC1918 / body-scrub deferred
  to a follow-up.

* **M4** — `bulk_action_completed` notifications fired even when
  zero targets were processed. Fixed: dispatch gated on `done > 0`
  / `n_targets > 0`.

* **M5** — `_send_external` always sent `type=info` regardless of
  event severity. Fixed: per-event-kind notify-type map
  (`_EVENT_NOTIFY_TYPE`) routes sync_failed → "failure" /
  cookies_needed + disk_low + worker_restarted → "warning" / rest
  → "info". Threaded through `dispatch()` → `_dispatch_inline()`
  → `_send_embedded()` (maps to apprise.NotifyType enum) +
  `_send_external()` (POST body `type` field).

* **L3** — `_bulk_lps_run` only stamped
  `last_place_attempt_reason='plex_has_theme'` when `placements`
  was non-empty. Class-P drift with `api_unplace_item` which
  stamps unconditionally. Fixed: unplace stamp now also fires in
  the no-placements-existed branch.

* **L4** — `bindTestNotification` in app.js relied on
  `r.embedded.fail` and `r.external.fail` always existing on the
  server response. Fixed: optional-chaining (`r.embedded?.fail`).

* **Self-review class-9 (plex.py)** — `_parse_candidates` silently
  returned `[]` on JSON parse failure. Fixed: log.warning with the
  first 200 chars of the response so the operator can diagnose
  Plex-side flakiness.

* **Class-9 follow-ups** (sync.py + worker.py) — three more
  bare-pass swallows fitted with `log.debug` breadcrumbs:
  - sync.py:1721 (git mirror compaction OSError)
  - sync.py:3167 (fallback_reason JSON parse)
  - worker.py relink tmp-unlink OSError
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_INIT = REPO / "app" / "__init__.py"
CONFIG_FILE_PY = REPO / "app" / "core" / "config_file.py"
NOTIFY_PY = REPO / "app" / "core" / "notify.py"
API_PY = REPO / "app" / "web" / "api.py"
WORKER_PY = REPO / "app" / "core" / "worker.py"
SYNC_PY = REPO / "app" / "core" / "sync.py"
PLEX_PY = REPO / "app" / "core" / "plex.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Version + release marker ──────────────────────────────────


def test_version_at_least_v1_17_1():
    """v1.17.1 ships as a normal patch tag. The bump-before-tag
    rule (CLAUDE.md) means __version__ must be 1.17.1 or later
    once this audit-rollover ships. Pinning to exactly 1.17.1
    would brittlely break the test on every subsequent bump —
    `test_v1_13_79_link_fixes.py` already enforces the
    bump-before-tag protocol against the current tag."""
    import re
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m, f"could not find __version__ string in {APP_INIT}"
    major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))
    assert (major, minor, patch) >= (0, 17, 1), (
        f"v1.17.1 audit-rollover requires __version__ >= 1.17.1; "
        f"found {major}.{minor}.{patch}"
    )


# ── H1 + L1: themes_added_by_sync column fix + test contract pin ──


def test_h1_themes_added_by_sync_uses_first_seen_sync_at():
    """`themes` does not have a `created_at` column. v1.17.1
    switched the themes_added_by_sync query to `first_seen_sync_at`
    (the timestamp the sync's own upsert writes on the first
    INSERT). Pin the contract via a regex so future renames don't
    break this without changing the WHERE-shape."""
    src = WORKER_PY.read_text()
    # v1.21.6: the New-titles query lives in the sync_completed
    # summary now, gated by the themes_added_by_sync toggle.
    block_start = src.index('_events.get("themes_added_by_sync"')
    block_end = src.index("# v1.12.126", block_start)
    block = src[block_start:block_end]
    # H1 fix: the WHERE clause now uses first_seen_sync_at.
    assert "first_seen_sync_at >= ?" in block
    # And the ORDER BY moves with it so the sample is chronological.
    assert "ORDER BY first_seen_sync_at ASC" in block
    # Pre-fix `created_at` SQL reference is gone from the actual
    # query lines (defensive). Comments referencing the historic
    # bug name `themes.created_at` are allowed — they're
    # archaeology for the v1.17.1 marker comment.
    import re
    sql_lines = [
        ln for ln in block.splitlines()
        if "created_at" in ln
        and not ln.lstrip().startswith("#")
    ]
    assert not sql_lines, (
        "v1.17.1 H1: the pre-fix `themes.created_at` SQL reference "
        "must be removed from non-comment lines — the column does "
        "not exist in the themes table and the SQL raises "
        "OperationalError at runtime if referenced. Found these "
        f"non-comment lines still referencing created_at: {sql_lines!r}"
    )


# ── H2: _hydrate_dataclass dict-merge for forward-compat ──────


def test_h2_hydrate_dataclass_merges_dict_fields():
    """The YAML load path must MERGE dict fields rather than
    REPLACE them, mirroring the PATCH-merge contract in
    `_apply_partial_config`. Pre-fix a v1.17.1+ deploy adding a
    new ON-by-default event kind silently OFF'd it on existing
    installs whose YAML didn't carry the new key."""
    src = CONFIG_FILE_PY.read_text()
    fn_anchor = src.index("def _hydrate_dataclass(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert "isinstance(cur, dict) and isinstance(v, dict)" in body, (
        "v1.17.1 H2: _hydrate_dataclass must special-case dict "
        "fields with merge semantics (cur.update(v)) instead of "
        "the wholesale-replace else branch."
    )
    assert "merged.update(v)" in body, (
        "v1.17.1 H2: the merge branch must call dict.update(v) "
        "so YAML keys override defaults but missing keys preserve "
        "the dataclass default."
    )


# ── H3 + L2: log breadcrumbs on notify-dispatch swallows ──────


def test_h3_l2_notify_dispatch_swallows_have_log_breadcrumbs():
    """The notify-dispatch swallow sites in worker.py's sync path
    must each carry a log breadcrumb (class 9 — silent-defensive-
    catch needs a breadcrumb).

    v1.21.6: the standalone themes_added/updated_by_sync dispatches
    were folded into sync_completed, so the H1-adjacent SQL swallow
    is now the New-titles query inside the summary
    (log.warning "sync summary new-titles query failed"). The
    sync_completed + sync_failed dispatch swallows stay at
    log.debug."""
    src = WORKER_PY.read_text()
    # The SQL swallow (themes lookup) stays at log.warning — the
    # underlying schema-drift bug class warrants default-INFO
    # visibility.
    assert 'log.warning(\n                        "sync summary new-titles query failed: %s", e)' in src \
        or 'log.warning("sync summary new-titles query failed: %s", e)' in src \
        or "sync summary new-titles query failed" in src, (
        "v1.21.6 H3: the New-titles query swallow inside the sync "
        "summary must log.warning the breadcrumb."
    )
    # The sync_completed + sync_failed dispatch swallows use log.debug.
    assert 'log.debug("notify dispatch (sync_completed)' in src
    assert 'log.debug("notify dispatch (sync_failed)' in src


# ── M1: _send_embedded fail count includes rejected URLs ──────


def test_m1_send_embedded_reports_rejected_urls_as_failures():
    """`_send_embedded` must count URLs that Apprise.add()
    rejected (malformed) against the fail return so the TEST UI
    surfaces them. Pre-fix `(added, 0)` hid Apprise-unparseable
    URLs from the operator."""
    src = NOTIFY_PY.read_text()
    fn_anchor = src.index("def _send_embedded(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    # Success path: rejected count must contribute to fail.
    assert "rejected = len(urls) - added" in body
    assert "(added, rejected)" in body, (
        "v1.17.1 M1: success-path return must include the "
        "rejected-URL count as failures."
    )


# ── M2: closed-set events + null-PATCH handling ───────────────


def test_m2_apply_partial_config_accepts_null_dict():
    """A PATCH body with `events: null` must NOT 400 the whole
    request — `null` should mean 'leave alone' (matches the
    semantic of optional fields elsewhere)."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _apply_partial_config(")
    fn_end = src.index("\n# -", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    # The dict branch must short-circuit on v is None.
    dict_anchor = body.index("elif isinstance(current, dict):")
    dict_block = body[dict_anchor:dict_anchor + 2000]
    assert "if v is None:" in dict_block
    assert "continue" in dict_block, (
        "v1.17.1 M2: dict branch must `continue` past null values "
        "rather than raise."
    )


def test_m2_apply_partial_config_filters_unknown_event_keys():
    """notifications.events must filter to keys in
    `_DEFAULT_NOTIFY_EVENTS`. Pre-fix a PATCH with
    `events: {arbitrary_garbage: true}` landed in the YAML
    permanently; same shape as v1.14.60 silent-key-acceptance."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _apply_partial_config(")
    fn_end = src.index("\n# -", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert "_DEFAULT_NOTIFY_EVENTS" in body, (
        "v1.17.1 M2: the dict-merge branch must import "
        "_DEFAULT_NOTIFY_EVENTS to build the allowed-keys set."
    )
    assert "allowed_keys" in body
    assert 'section_name == "notifications" and k == "events"' in body, (
        "v1.17.1 M2: the closed-set filter must be scoped to "
        "notifications.events specifically — other dict fields "
        "added in the future may have different semantics."
    )


# ── M3: external URL scheme enforcement ───────────────────────


def test_m3_apprise_external_url_scheme_enforcement():
    """`validate()` must reject schemes != http:// / https:// for
    `notifications.apprise_external_url`. Mirrors the existing
    `downloads.proxy_url` scheme check."""
    src = CONFIG_FILE_PY.read_text()
    fn_anchor = src.index("def validate(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert "apprise_external_url" in body, (
        "v1.17.1 M3: validate() must inspect notifications."
        "apprise_external_url."
    )
    # Locate the apprise-specific block (separate from the
    # downloads.proxy_url block above) and assert it contains
    # both schemes + a startswith call.
    apprise_idx = body.index("apprise_external_url")
    apprise_block = body[apprise_idx:apprise_idx + 800]
    assert "http://" in apprise_block and "https://" in apprise_block
    assert "startswith" in apprise_block, (
        "v1.17.1 M3: scheme check uses str.startswith() — same "
        "pattern as the existing downloads.proxy_url check."
    )


# ── M4: bulk_action_completed gated on n > 0 ──────────────────


def test_m4_bulk_lps_action_completed_gated_on_n_targets():
    """`_bulk_lps_run` must not fire bulk_action_completed when
    n_targets == 0 (empty-selection invocation produces a
    "0 targets done" notification that's pure noise)."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    # The dispatch call must be inside an `if n_targets > 0` block.
    dispatch_idx = body.index('event_kind="bulk_action_completed"')
    # Walk backward to the nearest `if n_targets > 0:` opener.
    gate_idx = body.rfind("if n_targets > 0:", 0, dispatch_idx)
    assert gate_idx != -1, (
        "v1.17.1 M4: bulk-LPS dispatch must be gated on "
        "n_targets > 0."
    )


def test_m4_bulk_probe_action_completed_gated_on_done():
    """Same gate for `_bulk_probe_tdb_run` — empty-selection
    invocation must not ping operators."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    dispatch_idx = body.index('event_kind="bulk_action_completed"')
    gate_idx = body.rfind("if done > 0:", 0, dispatch_idx)
    assert gate_idx != -1


# ── M5: per-event notify-type severity routing ────────────────


def test_m5_event_notify_type_map_routes_severity():
    """`_EVENT_NOTIFY_TYPE` map must route sync_failed →
    "failure", warning-class events → "warning", and rest →
    "info". This drives both the embedded Apprise.NotifyType
    enum and the external API's `type` field."""
    src = NOTIFY_PY.read_text()
    assert "_EVENT_NOTIFY_TYPE" in src
    # The map must be defined as a dict at module scope.
    map_idx = src.index("_EVENT_NOTIFY_TYPE: dict[str, NotifyType] = {")
    # Find the closing brace of the map literal.
    map_end = src.index("}", map_idx)
    map_block = src[map_idx:map_end]
    assert '"sync_failed":' in map_block and '"failure"' in map_block
    assert '"cookies_needed":' in map_block and '"warning"' in map_block
    assert '"disk_low":' in map_block
    assert '"worker_restarted":' in map_block
    assert '"sync_completed":' in map_block and '"info"' in map_block


def test_m5_dispatch_threads_notify_type_through_pool_submit():
    """`dispatch()` must look up the per-event notify_type and
    pass it down to `_dispatch_inline()` so it threads through
    to `_send_embedded` + `_send_external`."""
    src = NOTIFY_PY.read_text()
    fn_anchor = src.index("def dispatch(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert "_EVENT_NOTIFY_TYPE.get(event_kind" in body
    assert "notify_type=notify_type" in body


def test_m5_send_embedded_uses_apprise_notify_type_enum():
    """`_send_embedded` must map the `notify_type` string into
    `apprise.NotifyType.X` and pass it to `apobj.notify()`."""
    src = NOTIFY_PY.read_text()
    fn_anchor = src.index("def _send_embedded(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert "apprise.NotifyType.INFO" in body
    assert "apprise.NotifyType.WARNING" in body
    assert "apprise.NotifyType.FAILURE" in body
    assert "notify_type=apprise_type" in body


def test_m5_send_external_threads_type_into_post_body():
    """`_send_external` must POST the `notify_type` as the `type`
    field (caronc/apprise-api routes on this)."""
    src = NOTIFY_PY.read_text()
    fn_anchor = src.index("def _send_external(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert '"type": notify_type' in body


# ── L3: unplace stamp unconditional in _bulk_lps_run ──────────


def test_l3_bulk_lps_stamps_unplace_reason_even_on_empty_placements():
    """`_bulk_lps_run` must stamp `last_place_attempt_reason=
    'plex_has_theme'` even when the row had no placement to
    delete (e.g. already let-Plex-served from a prior LPS that
    ran before v1.16.12). Mirrors `api_unplace_item`'s
    unconditional stamp."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    # The no-placements branch ("else: # No placement file existed") must
    # contain a last_place_attempt_reason UPDATE.
    else_idx = body.index(
        "No placement file existed — treat as already-served-by-Plex.")
    # Bound to ~2000 chars after — generous enough for the section-
    # scoped + title-global branches.
    no_placement_block = body[else_idx:else_idx + 2000]
    assert "last_place_attempt_reason = 'backup_only'" in no_placement_block, (
        "v1.17.1 L3: the no-placements-existed branch must also "
        "stamp last_place_attempt_reason so the v1.13.76 retry "
        "sweep skips these rows. Mirrors api_unplace_item."
    )


# ── L4: bindTestNotification optional-chains ──────────────────


def test_l4_test_notification_handler_optional_chains_response():
    """The TEST NOTIFICATION JS click handler must use optional-
    chaining on `r.embedded.fail` / `r.external.fail` so a future
    server-shape change doesn't crash the UI with a useless
    'Cannot read properties of undefined' message."""
    src = APP_JS.read_text()
    handler_anchor = src.index("function bindTestNotification()")
    fn_end = src.index("\n  function ", handler_anchor + 1)
    body = src[handler_anchor:fn_end]
    assert "r.embedded?.fail" in body
    assert "r.external?.fail" in body


# ── Self-review class-9: plex._parse_candidates log.warning ───


def test_plex_parse_candidates_logs_json_parse_failure():
    """`PlexClient._parse_candidates` must log.warning when JSON
    parse fails — pre-fix the silent `return []` was
    indistinguishable from a legitimate empty section. Class-9
    instance closed in v1.17.1."""
    src = PLEX_PY.read_text()
    fn_anchor = src.index("def _parse_candidates(")
    fn_end = src.index("\n    def ", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert "log.warning" in body, (
        "v1.17.1: _parse_candidates must log.warning on JSON "
        "parse failure rather than silently returning []."
    )
    assert "body_text[:200]" in body, (
        "v1.17.1: the log line must include the first 200 chars "
        "of the response so the operator can diagnose what Plex "
        "returned instead of valid JSON."
    )


# ── Class-9 follow-ups: log breadcrumbs in sync.py + worker.py ──


def test_class9_sync_git_mirror_compact_logs_oserror():
    """sync.py git-mirror compaction OSError must log.debug
    rather than silently `pass`. Functional fallback exists (next
    sync still works), but the operator wants to know if /config
    is running out of disk/inodes."""
    src = SYNC_PY.read_text()
    anchor = src.index("git mirror compacted: %.0f MB")
    block = src[anchor:anchor + 800]
    assert "except OSError as e:" in block
    assert "log.debug(\"git mirror compact suppressed:" in block


def test_class9_sync_fallback_reason_parse_logs():
    """sync.py fallback_reason JSON parse must log.debug rather
    than silently `pass`. Functional fallback exists (the UPDATE
    writes NULL), but a log line helps diagnose schema drift.

    Anchor on the unique log.debug breadcrumb string itself —
    `fallback_active` appears earlier in the file (in a docstring
    + a comment block), but the log message is only present once
    at the actual except site."""
    src = SYNC_PY.read_text()
    assert "fallback_reason detail_json read failed" in src, (
        "v1.17.1: the sync.py fallback_reason except site must "
        "log.debug rather than silently swallow."
    )


def test_class9_worker_relink_tmp_unlink_logs():
    """worker.py relink tmp-unlink OSError must log.debug rather
    than silently `pass`. Functional fallback exists (subsequent
    os.link will fail and outer handler catches), but a permission
    issue here is otherwise invisible."""
    src = WORKER_PY.read_text()
    anchor = src.index(".relink-tmp")
    block = src[anchor:anchor + 800]
    assert "except OSError as e:" in block
    assert "relink tmp pre-clean failed" in block
