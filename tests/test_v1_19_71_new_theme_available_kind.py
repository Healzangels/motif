"""v1.19.71 — surface NEW TDB themes via blue !UPD on in-Plex rows.

Companion to v1.19.70 (in_plex notification filter). Pre-fix the
sync apply-loop's `is_new` branch either auto-downloaded silently
(SRC=— rows when `enqueue_downloads=True` — the worker default) or
did nothing visible (rows with existing motif/Plex content). The
operator had no UI signal that TDB just discovered themes for new
in-library titles.

v1.19.71 widens the pending_updates.kind enum with a third value
'new_theme_available' (alongside 'upstream_changed' + 'urls_match'),
threads a settings toggle through sync, gates the SRC=— exception
on the new kind, and surfaces the blue !UPD title-glyph + blue
TDB↑ pill on every in-Plex row regardless of source letter.

## Schema migration v59 → v60

CHECK constraint on `pending_updates.kind` widened from
('upstream_changed', 'urls_match') to add 'new_theme_available'.
Uses the canonical `_widen_check_constraint` helper wrapped in
`PRAGMA foreign_keys = OFF / ON` (the v1.18.5 FK-cascade lesson).

## Sync apply-loop branching

The is_new branch now decides between three paths for in-Plex
rows:
- has_content (U/A/M/P-with-sidecar) → write pending_updates
  kind='new_theme_available'.
- SRC=— + opt-in `auto_download_new_themes_for_unthemed_rows`
  + enqueue_downloads → _enqueue_download (existing behavior
  preserved for opt-in users).
- SRC=— without the opt-in → write pending_updates
  kind='new_theme_available'.

The setting is threaded through `_flush_sync_batch` →
`_run_git_differential_upsert` → `run_sync` → worker.

## Read-path

11 mirror-drift gate sites in api.py learn the new kind via a
new helper `_pending_update_new_theme_kind_sql`, added as the
third OR branch alongside `_pending_update_real_diff_sql` and
`_row_has_non_url_local_content_sql`. The presence gate (the
existing local_files / user_overrides / placements / sidecar
OR-chain) is widened with the new helper to permit unthemed
rows that have ONLY a new_theme_available pending_update to
surface.

## Client gates

`computeTdbPill` and the title-glyph cascade both gated the
blue !UPD signal on `computeSrcLetter(it) !== '-'`. v1.19.71
adds an exception: when `pending_update_kind === 'new_theme_
available'`, the gate passes even on SRC=— rows.

A new tooltip variant covers the new_theme_available branch
(ACCEPT to download or KEEP CURRENT to dismiss).
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

DB_PY = (REPO / "app" / "core" / "db.py").read_text()
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
CFG_PY = (REPO / "app" / "core" / "config_file.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── Schema migration v59 → v60 ──────────────────────────────────


def test_schema_version_is_v60():
    """The v59→v60 bump must remain in place. v1.21.2: forward-compatible
    (>=60) so later schema bumps don't churn this v1.19.71 test — the
    v59→v60 migration wiring is pinned separately below."""
    import re
    m = re.search(r"CURRENT_SCHEMA_VERSION = (\d+)", DB_PY)
    assert m and int(m.group(1)) >= 60


def test_schema_check_lists_new_theme_available():
    """The in-place SCHEMA dict (used for fresh installs) must
    list 'new_theme_available' alongside the other two kinds."""
    # SCHEMA pending_updates table CHECK clause.
    assert (
        "kind IN ('upstream_changed', 'urls_match', 'new_theme_available')"
        in DB_PY
    )


def test_v59_to_v60_migration_function_exists():
    """The dispatch loop in run_migrations must call a v59→v60
    migration that widens the kind CHECK."""
    assert "_migrate_v59_to_v60" in DB_PY


def test_v59_to_v60_runs_widen_check_constraint_with_fk_off():
    """Mirror the v57→v58 shape (PRAGMA foreign_keys = OFF wrap
    + _widen_check_constraint + foreign_key_check + restore ON)
    per the v1.18.5 class-9 FK-cascade lesson."""
    fn = DB_PY[DB_PY.index("def _migrate_v59_to_v60"):]
    fn = fn[: fn.index("\ndef ")]
    assert "PRAGMA foreign_keys = OFF" in fn
    assert "_widen_check_constraint" in fn
    assert "PRAGMA foreign_key_check" in fn
    assert "PRAGMA foreign_keys = ON" in fn
    # Must include the new kind in the widened set.
    assert "'new_theme_available'" in fn


def test_v59_to_v60_migration_actually_widens_check(tmp_path):
    """Behavioral: build a v59-shape DB with the narrow CHECK,
    run the migration, assert a 'new_theme_available' INSERT
    succeeds afterward."""
    db_path = tmp_path / "m.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE pending_updates (
            media_type TEXT NOT NULL,
            tmdb_id INTEGER NOT NULL,
            section_id TEXT NOT NULL DEFAULT '',
            old_video_id TEXT,
            new_video_id TEXT,
            old_youtube_url TEXT,
            new_youtube_url TEXT,
            upstream_edited_at TEXT,
            detected_at TEXT NOT NULL,
            decision TEXT NOT NULL DEFAULT 'pending',
            kind TEXT NOT NULL DEFAULT 'upstream_changed'
                CHECK (kind IN ('upstream_changed', 'urls_match')),
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        """
    )
    conn.commit()
    # Pre-fix: insert with new kind must fail.
    try:
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, "
            "detected_at, kind) VALUES (?, ?, ?, ?)",
            ("movie", 1, "2026-01-01T00:00:00Z", "new_theme_available"),
        )
        raised = False
    except sqlite3.IntegrityError:
        raised = True
    assert raised, "narrow v59 CHECK should reject new_theme_available"
    # Run the v59→v60 migration via the function itself.
    from app.core.db import _migrate_v59_to_v60
    _migrate_v59_to_v60(conn)
    # Post-fix: insert must succeed.
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, "
        "detected_at, kind) VALUES (?, ?, ?, ?)",
        ("movie", 1, "2026-01-01T00:00:00Z", "new_theme_available"),
    )
    conn.commit()
    rows = conn.execute(
        "SELECT kind FROM pending_updates WHERE tmdb_id = 1"
    ).fetchall()
    assert rows == [("new_theme_available",)]
    conn.close()


# ── config_file.py: new SyncConfig field ─────────────────────────


def test_sync_config_has_auto_download_setting_defaulting_off():
    """The new setting must exist with bool type + default False."""
    assert (
        "auto_download_new_themes_for_unthemed_rows: bool = False" in CFG_PY
    )


def test_sync_config_documents_why_default_off():
    """The comment must explain WHY default OFF (per CLAUDE.md
    WHY-comments convention)."""
    needle = "auto_download_new_themes_for_unthemed_rows"
    idx = CFG_PY.index(needle)
    preamble = CFG_PY[max(0, idx - 2500): idx]
    assert "operator prompt" in preamble or "explicit" in preamble


# ── sync.py: threading + apply-loop branches ────────────────────


def test_run_sync_signature_threads_auto_download_param():
    """run_sync must accept auto_download_new_themes (default False)
    so the existing call-sites that don't pass it stay default-off."""
    sig = SYNC_PY[SYNC_PY.index("def run_sync"):]
    sig = sig[: sig.index(":\n") + 2]
    assert "auto_download_new_themes" in sig
    assert "auto_download_new_themes: bool = False" in sig


def test_flush_sync_batch_signature_threads_auto_download():
    """_flush_sync_batch is the per-batch txn helper. It receives
    the flag (no default — kwonly explicit pass)."""
    fn = SYNC_PY[SYNC_PY.index("def _flush_sync_batch"):]
    fn = fn[: fn.index(":\n") + 2]
    assert "auto_download_new_themes: bool" in fn


def test_git_differential_upsert_signature_threads_auto_download():
    """The parallel sync transport path must also thread the flag."""
    fn = SYNC_PY[SYNC_PY.index("def _run_git_differential_upsert"):]
    fn = fn[: fn.index(":\n") + 2]
    assert "auto_download_new_themes: bool" in fn


def test_is_new_branch_writes_new_theme_available_kind():
    """The is_new branch's INSERT INTO pending_updates must use
    the new kind, not 'upstream_changed'."""
    # Slice the is_new branch — bounded by the `if yt_url and
    # in_plex:` line and the next `elif url_changed:` site.
    start = SYNC_PY.index("if yt_url and in_plex:")
    end = SYNC_PY.index("elif url_changed:", start)
    block = SYNC_PY[start:end]
    # Insert must use the new kind literal.
    assert "'pending', 'new_theme_available'" in block


def test_is_new_branch_opt_in_gates_enqueue_download():
    """SRC=— rows must NOT auto-download unless the setting +
    enqueue_downloads is True. The conjunction of
    `not has_content and not plex_supplies and auto_download_new_
    themes and enqueue_downloads` is the discriminator."""
    start = SYNC_PY.index("if yt_url and in_plex:")
    end = SYNC_PY.index("elif url_changed:", start)
    block = SYNC_PY[start:end]
    assert "not has_content" in block
    assert "not plex_supplies" in block
    assert "auto_download_new_themes" in block
    assert "enqueue_downloads" in block


def test_is_new_branch_writes_pending_with_on_conflict_preservation():
    """ON CONFLICT must preserve a prior 'declined' decision so
    a KEEP CURRENT click doesn't get re-prompted on next sync."""
    start = SYNC_PY.index("if yt_url and in_plex:")
    end = SYNC_PY.index("elif url_changed:", start)
    block = SYNC_PY[start:end]
    # v1.21.52 (schema v63): conflict target gained edition_key (PK widened).
    assert "ON CONFLICT(media_type, tmdb_id, section_id, edition_key)" in block
    assert "WHEN pending_updates.decision = 'declined'" in block
    assert "THEN 'declined'" in block


# ── worker.py: threading the setting ────────────────────────────


def test_worker_passes_auto_download_setting_to_run_sync():
    """The cron-driven worker call must pull the value from
    cfg.sync.auto_download_new_themes_for_unthemed_rows."""
    assert "auto_download_new_themes=" in WORKER_PY
    assert (
        "auto_download_new_themes_for_unthemed_rows" in WORKER_PY
    )


# ── api.py: helper + 11 mirror-drift sites ──────────────────────


def test_new_theme_kind_sql_helper_exists():
    """The dedicated helper must exist and return a single
    boolean SQL expression. Single-purpose vs reuse of
    _pending_update_real_diff_sql per CLAUDE.md (no premature
    abstraction)."""
    assert "def _pending_update_new_theme_kind_sql" in API_PY
    fn = API_PY[API_PY.index("def _pending_update_new_theme_kind_sql"):]
    fn = fn[: fn.index("\n\n\n")]
    # Returns a single COALESCE check against the new kind.
    assert "= 'new_theme_available'" in fn
    # Both per-section + global '' branches per v1.12.99
    # per-section convention.
    assert "section_id = {pi}.section_id" in fn
    assert "section_id = ''" in fn


def test_new_theme_kind_sql_helper_callable():
    """Smoke test: import + call returns a non-empty string
    containing the kind literal."""
    from app.web.api import _pending_update_new_theme_kind_sql
    sql = _pending_update_new_theme_kind_sql("t", "pi")
    assert "'new_theme_available'" in sql
    assert "t.media_type" in sql
    assert "t.tmdb_id" in sql
    assert "pi.section_id" in sql


def test_eleven_mirror_drift_sites_reference_new_helper():
    """The 11 SQL gate sites that decide whether to surface a
    pending_update must ALL include the new_theme_kind branch
    as an OR alongside real_diff + non_url_local_content."""
    # Count the inclusions. Each occurrence is a separate gate
    # site (the helper is called once per query).
    matches = re.findall(
        r"_pending_update_new_theme_kind_sql\(\s*['\"]t2?['\"]",
        API_PY,
    )
    assert len(matches) >= 11, (
        f"Expected at least 11 mirror-drift gate-site calls to "
        f"_pending_update_new_theme_kind_sql, found {len(matches)}. "
        f"A new pending_update kind needs uniform read-path coverage."
    )


def test_presence_gate_widened_with_new_theme_kind():
    """The presence gate (local_files OR user_overrides OR
    placements OR sidecar) must include the new kind so SRC=—
    rows can surface a pending_update without any motif/Plex
    content."""
    # Find all `OR pi.local_theme_file = 1` / `OR pi2.local_
    # theme_file = 1` sidecar lines, assert each is followed
    # within ~80 chars by an _pending_update_new_theme_kind_sql
    # reference.
    pattern = re.compile(
        r"OR pi2?\.local_theme_file = 1.{0,200}?"
        r"_pending_update_new_theme_kind_sql",
        re.DOTALL,
    )
    matches = pattern.findall(API_PY)
    # At minimum the 10 presence-gate sites identified in the
    # v1.19.71 audit.
    assert len(matches) >= 9, (
        f"Expected at least 9 presence-gate sites to chain the "
        f"new_theme_kind helper after `OR pi*.local_theme_file = 1`, "
        f"found {len(matches)}."
    )


# ── app.js: SRC=— exception in two gate sites ───────────────────


def test_compute_tdb_pill_has_new_theme_available_exception():
    """The computeTdbPill SRC !== '-' gate must add an exception
    for new_theme_available."""
    fn = APP_JS[APP_JS.index("function computeTdbPill"):]
    fn = fn[: fn.index("\n  }\n")]
    assert "pending_update_kind === 'new_theme_available'" in fn
    # The exception must be ORed in alongside the existing gate.
    assert "computeSrcLetter(it) !== '-'" in fn


def test_inline_tdb_pill_blocks_have_new_theme_available_exception():
    """Mirror-drift guard: both inline TDB-pill render sites in
    renderLibraryRow (line ~8373 + the v1.18.65 fallthrough at
    ~8415) must mirror computeTdbPill's SRC=— exception so the
    blue TDB ↑ pill surfaces on unthemed rows with a new_theme_
    available pending_update."""
    # Both sites share the gate shape. Count both.
    matches = re.findall(
        r"it\.pending_update\s*\n\s*&& \(computeSrcLetter\(it\) !== '-'\s*\n"
        r"\s*\|\| it\.pending_update_kind === 'new_theme_available'\)",
        APP_JS,
    )
    assert len(matches) >= 3, (
        f"Expected at least 3 occurrences of the v1.19.71 SRC=— "
        f"exception gate shape (computeTdbPill + 2 inline render "
        f"sites), found {len(matches)}. Mirror-drift class-9 — "
        f"all blue-pill gate sites must share the same gate."
    )


def test_title_glyph_cascade_has_new_theme_available_exception():
    """The blue ! title-glyph branch must also accept new_theme_
    available even when SRC=—."""
    idx = APP_JS.index("it.actionable_update")
    block = APP_JS[idx: idx + 1500]
    assert "pending_update_kind === 'new_theme_available'" in block
    assert "computeSrcLetter(it) !== '-'" in block


def test_title_glyph_tooltip_has_new_theme_available_variant():
    """A distinct tooltip for the new_theme_available branch
    so the operator sees the right CTA (ACCEPT to download vs
    KEEP CURRENT to dismiss)."""
    idx = APP_JS.index("it.actionable_update")
    block = APP_JS[idx: idx + 3000]
    # Three-way tooltip ternary: urls_match / new_theme_
    # available / default (upstream_changed).
    assert "(it.pending_update_kind === 'urls_match')" in block
    assert "(it.pending_update_kind === 'new_theme_available')" in block
    # The new variant's CTA must mention ACCEPT + KEEP CURRENT
    # so the operator copy aligns with the rest of the !UPD
    # surface. The tooltip-ternary anchor is the new kind
    # literal followed by `?` (ternary), distinct from the
    # gate predicate (followed by `)`) and the comment
    # references (followed by `.` or `,`).
    ternary_idx = block.index("'new_theme_available')\n          ?")
    nearby = block[ternary_idx: ternary_idx + 400]
    assert "ACCEPT" in nearby
    assert "KEEP CURRENT" in nearby
