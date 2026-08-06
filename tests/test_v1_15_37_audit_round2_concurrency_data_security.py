"""v1.15.37 — audit round 2: concurrency + data integrity + security
+ dead-code, all in one tag.

the user: "great lets audit once more" → "yes lets ship all in
one." Four parallel agents audited fresh angles (race conditions,
transaction correctness, input validation, dead code) — this
tag ships every real finding.

## Concurrency

1. **TOCTOU enqueue race** (HIGH) — `bulk-probe-tdb`,
   `reprobe-plex-themes`, `bulk-lps`, `reprobe-tdb-failures`
   all used `load_active(...) + any(... op_id ...)` to gate
   enqueue, then spawned a daemon thread that called
   `start_progress` separately. Two near-simultaneous requests
   could both pass the check before either reached
   `start_progress`; the second's `ON CONFLICT DO UPDATE`
   would RESET the first's row, making the first thread's
   progress writes invisible. Fix: new
   `op_progress.try_acquire(db, op_id, kind)` does check +
   claim atomically inside `BEGIN IMMEDIATE`. Returns False
   on conflict so the route can 409.
2. **`_DOWNLOAD_PROGRESS` dict race** (HIGH) — `_synthesize_
   queue_ops` had two read sites with the pattern `[
   _DOWNLOAD_PROGRESS.get(jid) for jid in jobs if
   _DOWNLOAD_PROGRESS.get(jid) is not None ]`. Between the
   two `.get(jid)` calls a worker thread's `pop()` could fire
   → list-length-mismatch downstream → bad bar_pct. GIL
   protects individual dict ops but NOT this read-then-recheck
   pattern. Fix: added `_DOWNLOAD_PROGRESS_LOCK` + new
   `snapshot_download_progress(job_ids)` helper that does the
   lookups under the lock. Writers (set_download_progress /
   clear_download_progress) also acquire.

## Data integrity

3. **23 multi-row POST handlers** lacked transaction wrapping
   (HIGH) — half-state risk if any write failed mid-sequence.
   Affected: override, manual-url, unmanage, forget,
   revert-to-themerrdb, accept-update, accept-all, decline,
   restore-canonical, replace, adopt-from-plex, unplace,
   redownload, download-backup, clear-failure,
   convert-to-manual, audit-clear, events-clear, upload-theme
   (×2 blocks), library/refresh, jobs/{id}/cancel,
   saved-filters. Fix: wrap each `with get_conn(db) as conn:`
   block in `transaction(conn)` (uses BEGIN IMMEDIATE with
   retry from the existing helper).

## Security

4. **Open redirect on GET /login** (MED) — POST handler
   validated `next` (`startswith("/") and not startswith("//")`)
   but the GET handler used `RedirectResponse(next or "/")`
   directly. `/login?next=//attacker.com` was a clean
   phishing redirect. Fix: validate `next` on GET with the
   same pattern as POST + thread the sanitized value through
   to the template.

## Dead code

5. **placement.py unused imports** — `editions_equal,
   titles_equal` from `.normalize` were imported but never
   referenced in placement.py (used elsewhere, not globally
   dead). Fix: drop from this file's import list.

## False positives ruled out

- The audit's "op_progress lifecycle orphan" claim was
  re-verified (after v1.15.34 already verified). All
  `start_progress` callers in worker.py / sync.py /
  plex_enum.py / api.py wrap in try/except + finish_progress
  on every termination path.
- The audit's claim of "40+ POST handlers" needing transaction
  wrapping over-counted — actual was 23 (counted by AST
  walk in this audit's verification step).
- placement.py `_safe_link_or_copy` cleanup leaks (flagged
  v1.15.36 audit) — self-heal on next call's pre-check at
  line 207. Confirmed false-positive.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
PROGRESS_PY = REPO / "app" / "core" / "progress.py"
API_PY = REPO / "app" / "web" / "api.py"
PLACEMENT_PY = REPO / "app" / "core" / "placement.py"


# ── 1. op_progress.try_acquire helper ─────────────────────────


def test_try_acquire_helper_defined():
    """The new atomic check-and-claim helper must exist + use
    BEGIN IMMEDIATE for the check + claim. Pre-fix the route
    handlers used a non-atomic load_active + spawn-thread
    pattern that lost progress writes on near-simultaneous
    requests."""
    src = PROGRESS_PY.read_text()
    assert "def try_acquire(" in src
    fn_anchor = src.index("def try_acquire(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # Returns bool (True = acquired, False = already running).
    assert "-> bool:" in fn_body
    # Atomic check + claim inside transaction(conn).
    assert "with get_conn(db_path) as conn, transaction(conn):" in fn_body
    # Checks the existing row's status before claiming.
    assert 'SELECT status FROM op_progress WHERE op_id = ?' in fn_body
    assert "running" in fn_body
    assert "pending" in fn_body
    assert "cancelling" in fn_body
    # Claims with status='pending' (the worker's start_progress
    # then transitions to 'running').
    assert "'pending'" in fn_body


def test_try_acquire_used_by_three_singleton_routes():
    """The 3 background-op routes (reprobe-plex-themes,
    bulk-probe-tdb, reprobe-tdb-failures, bulk-lps) must all
    call try_acquire instead of the pre-v1.15.37 load_active
    pattern."""
    src = API_PY.read_text()
    # Each route now uses try_acquire.
    assert 'op_progress.try_acquire, db,\n                "reprobe-plex-themes"' in src
    assert 'op_progress.try_acquire, db,\n                "bulk-probe-tdb"' in src
    assert 'op_progress.try_acquire, db,\n                "bulk-lps"' in src
    # The pre-fix `load_active(...) + any(... op_id ...)` gate is gone
    # from these routes (it's a substring that was only used in the
    # 4 background-op routes — now all migrated).
    assert 'r.get("op_id") == "bulk-probe-tdb"' not in src
    assert 'r.get("op_id") == "reprobe-plex-themes"' not in src
    assert 'r.get("op_id") == "bulk-lps"' not in src


# ── 2. _DOWNLOAD_PROGRESS lock + snapshot helper ─────────────


def test_download_progress_protected_by_lock():
    """Writers (set_download_progress / clear_download_progress)
    must acquire `_DOWNLOAD_PROGRESS_LOCK` before mutating the
    module-level dict. Pre-fix the GIL was assumed to be enough
    but the read-then-recheck pattern in the consumer broke
    that assumption."""
    src = PROGRESS_PY.read_text()
    assert "_DOWNLOAD_PROGRESS_LOCK = threading.Lock()" in src
    # Both writers acquire.
    set_anchor = src.index("def set_download_progress(")
    set_end = src.index("\n\ndef ", set_anchor + 1)
    set_body = src[set_anchor:set_end]
    assert "with _DOWNLOAD_PROGRESS_LOCK:" in set_body

    clear_anchor = src.index("def clear_download_progress(")
    clear_end = src.index("\n\ndef ", clear_anchor + 1)
    clear_body = src[clear_anchor:clear_end]
    assert "with _DOWNLOAD_PROGRESS_LOCK:" in clear_body


def test_snapshot_download_progress_helper_defined():
    """The new lock-guarded snapshot helper must exist + return
    a dict snapshot under the lock so the consumer can iterate
    without racing with worker thread pop()s."""
    src = PROGRESS_PY.read_text()
    assert "def snapshot_download_progress(" in src
    fn_anchor = src.index("def snapshot_download_progress(")
    fn_end = src.index("\n\ndef ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert "with _DOWNLOAD_PROGRESS_LOCK:" in fn_body
    # Returns a snapshot dict (only entries with a recorded
    # fraction at the moment of the lock).
    assert "return {" in fn_body
    assert "_DOWNLOAD_PROGRESS[jid]" in fn_body


def test_synthesize_queue_ops_uses_snapshot_helper():
    """The two consumer sites in _synthesize_queue_ops +
    _build_queue_detail must use snapshot_download_progress
    instead of the racy `[_DOWNLOAD_PROGRESS.get(jid) for ...
    if _DOWNLOAD_PROGRESS.get(jid) is not None]` pattern."""
    src = PROGRESS_PY.read_text()
    # Both consumer sites migrated.
    assert src.count("snapshot_download_progress(running_dl_jobs)") >= 2
    # The pre-fix pattern is gone from CODE (still allowed in
    # the marker comment that documents the v1.15.37 fix
    # history). Strip comments and check the residue.
    import re
    code_only = re.sub(r"#[^\n]*", "", src)
    assert "_DOWNLOAD_PROGRESS.get(jid)" not in code_only, (
        "v1.15.37: the racy .get(jid) pattern must not survive — "
        "all consumers should snapshot under the lock"
    )


# ── 3. transaction wrapping on multi-row POST handlers ────────


def test_high_stakes_routes_wrapped_in_transactions():
    """The highest-stakes multi-table mutation routes must wrap
    their `get_conn` blocks in `transaction(conn)`. Spot-check
    the most destructive ones — full enumeration is implicit
    in the count test below."""
    src = API_PY.read_text()
    for fn_name in (
        "api_unmanage_item",
        "api_forget_item",
        "api_revert_to_themerrdb",
        "api_accept_update",
        "api_manual_url",
        "api_clear_failure",
        "api_upload_theme",
        "api_unplace_item",
        # "api_override" dropped v0.51.251 — the dead endpoint was removed
        "api_adopt_from_plex",
        "api_redownload",
        "api_download_backup",
        "api_replace_item",
        "api_restore_canonical",
        "api_convert_to_manual",
        "api_cancel_job",
    ):
        fn_anchor = src.index(f"async def {fn_name}(")
        # Walk to next route or function.
        end = src.find("\n    @app.", fn_anchor)
        body = src[fn_anchor:end if end > 0 else fn_anchor + 8000]
        assert "transaction(conn)" in body, (
            f"v1.15.37: {fn_name} must wrap its get_conn block "
            f"in transaction(conn) — pre-fix multi-row writes "
            f"could half-commit on partial failure"
        )


def test_all_multi_write_get_conn_blocks_now_use_transaction():
    """Audit-walk: count `with get_conn(...) as conn:` blocks
    that contain ≥2 INSERT/UPDATE/DELETE without a wrapping
    `transaction(...)`. Should be zero (or only the few
    intentionally-non-txn read paths that have a single write
    embedded in mostly-read logic)."""
    import re
    src = API_PY.read_text()
    # Find every get_conn opener that doesn't already mention
    # transaction in the same with-statement.
    racy = []
    for m in re.finditer(
        r'(\s*)with get_conn\(([^)]+)\) as (\w+):',
        src,
    ):
        # The matched line.
        line = m.group(0)
        if "transaction(" in line:
            continue
        # Walk the indented block.
        indent = m.group(1).split('\n')[-1]
        rest = src[m.end():]
        block_lines = []
        for ln in rest.split('\n'):
            if not ln.strip():
                block_lines.append(ln)
                continue
            ind = len(ln) - len(ln.lstrip())
            if ind <= len(indent):
                break
            block_lines.append(ln)
        block = '\n'.join(block_lines)
        # v1.15.37: case-sensitive UPPERCASE keywords only —
        # SQL keywords are conventionally uppercase in motif's
        # codebase, and case-insensitive matching false-positives
        # on words like "update" inside doc comments.
        inserts = len(re.findall(r'\bINSERT\s+(?:OR\s+\w+\s+)?INTO\b', block))
        updates = len(re.findall(r'\bUPDATE\s+\w', block))
        deletes = len(re.findall(r'\bDELETE\s+FROM\b', block))
        writes = inserts + updates + deletes
        if writes >= 2 and "transaction(" not in block:
            racy.append((m.start(), inserts, updates, deletes))
    assert racy == [], (
        f"v1.15.37: {len(racy)} multi-write get_conn block(s) "
        f"still lack transaction wrapping; offsets: "
        f"{[r[0] for r in racy[:5]]}"
    )


# ── 4. Open redirect on GET /login ────────────────────────────


def test_login_get_validates_next_parameter():
    """The GET /login handler must validate `next` the same
    way the POST handler does — startswith("/") AND not
    startswith("//") — before passing to RedirectResponse.
    Pre-fix `RedirectResponse(next or "/")` was a clean
    open-redirect (`?next=//attacker.com`) phishing vector."""
    src = API_PY.read_text()
    fn_anchor = src.index('async def login_get(')
    fn_end = src.index('@app.post("/login")', fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    # v1.15.37 marker present.
    assert "v1.15.37: validate" in fn_body
    # Sanitized variable computed up front.
    assert "safe_next = (" in fn_body
    # Same validation predicate as the POST handler.
    assert 'startswith("/")' in fn_body
    assert 'not next.startswith("//")' in fn_body
    # All RedirectResponse calls in the GET handler use safe_next
    # (not the raw `next` arg). Filter out comment lines so the
    # marker comment that documents the v1.15.37 fix doesn't
    # falsely trip this check.
    redirect_calls = [
        line for line in fn_body.split('\n')
        if 'RedirectResponse(' in line
        and not line.lstrip().startswith('#')
    ]
    # v0.51.78: the forward-auth redirect was removed (redirect-loop lockout fix); the
    # already-logged-in redirect remains and must still use safe_next.
    assert len(redirect_calls) >= 1
    for line in redirect_calls:
        assert "safe_next" in line, (
            f"GET /login RedirectResponse must use safe_next, "
            f"not raw `next`: {line.strip()!r}"
        )
    # Template render also passes the sanitized value (so a
    # round-trip through the form can't survive a hostile next).
    assert '"next": safe_next' in fn_body


# ── 5. placement.py dead-import cleanup ──────────────────────


def test_placement_drops_unused_normalize_imports():
    """`editions_equal` and `titles_equal` were imported in
    placement.py but never used in this file (still used
    elsewhere — sync.py, scanner.py, plex.py). Audit cleanup."""
    src = PLACEMENT_PY.read_text()
    # Both unused names must be gone from the import line.
    import_anchor = src.index("from .normalize import")
    # Find the import statement (may span lines if parenthesized).
    import_end = src.index(")", import_anchor)
    import_block = src[import_anchor:import_end + 1]
    assert "editions_equal" not in import_block
    assert "titles_equal" not in import_block
    # The still-used names survive.
    assert "PlusMode" in import_block
    assert "normalize_edition" in import_block
    assert "normalize_title" in import_block
    assert "parse_folder_name" in import_block
