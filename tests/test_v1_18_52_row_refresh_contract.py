"""v1.18.52 — row-refresh contract guard.

the user's review of v1.18.51:

> "Letter A fixes we just did could we widen the scope to include
>  any other type of action where we see different SRC, DL, PL,
>  LINK status on the row they are all responsive to changed
>  states. maybe worth making a rule or test so we keep this
>  constant."

v1.18.51 added transition-detection for plex_enum only
(myTabBusy / globalEnumActive). But other backend ops also mutate
row state without per-row job_in_flight markers:

  - tdb_sync           → themes table writes → SRC, +P, ATTN axes
  - bulk_probe_tdb     → failure_kind → STATUS axis
  - bulk_lps           → +P stamp, has_theme → PL, LINK axes
  - tvdb_bridge        → plex_items.theme_id → SRC for anime
  - reprobe_plex_themes → has_theme on many rows → PL axis

Pre-v1.18.52 those ops mutated rows invisibly: libraryRapidPoll
auto-stopped because no per-row signal existed, no global signal
fed back to the rapid-poll auto-stop heuristic, and rows lagged
the 30s background tick.

v1.18.52 introduces a single `anyMutatingOpActive` signal that
unions all three sources:

  1. themerrdbBusy            (job_type='sync')
  2. plexEnumBusy             (job_type='plex_enum')
  3. q.op_progress_running    (NEW — catches every kind accepted
                               by op_progress.kind CHECK)

False→true → kick libraryRapidPoll. True→false → one-shot
loadLibrary. The unified signal also drives rapid-poll's
auto-stop "still busy" check.

The catch-all `op_progress_running` count means future ops
automatically inherit the contract. If a future kind is added
to the CHECK that does NOT mutate row state, an explicit
exclusion needs to land in the SQL — this test fires loud when
that gap appears.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"
DB_PY = REPO / "app" / "core" / "db.py"
CLAUDE_MD = REPO / "CLAUDE.md"


# Kinds we INTENTIONALLY count toward op_progress_running because
# they mutate visible library-row state. Audit guard below verifies
# this list matches every kind accepted by op_progress.kind CHECK in
# db.py — adding a new kind requires deciding whether it mutates
# rows. If yes, append it here. If no, it must be filtered out in
# the /api/stats SQL.
ROW_MUTATING_KINDS = {
    "tdb_sync",
    "plex_enum",
    "reprobe_plex_themes",
    "bulk_probe_tdb",
    "bulk_lps",
    "tvdb_bridge",
    # v1.19.45: cloud_themes_backup is the v1.19.42 walker that
    # INSERTs local_files rows (source_kind='plex_cloud') per
    # successful C1 download. Row chips change: LINK column
    # transitions from `—` to B (cloud-backup). Mutates row
    # state → libraryRapidPoll must fire on the false→true
    # transition + a one-shot loadLibrary on true→false so the
    # post-op state lands without waiting for the 30s background
    # tick.
    "cloud_themes_backup",
    # v0.51.195: bulk_normalize mp3gain-levels each eligible row and stamps
    # local_files.norm_state='normalized' + re-uploads to Plex. Row chips change
    # (the v0.51.192 leveled marker appears), so it MUST fire the row-refresh
    # contract like every other mutating op — it is NOT filtered from /api/stats.
    "bulk_normalize",
}


def _check_kinds_from_db_py() -> set[str]:
    """Parse the op_progress.kind CHECK clause from db.py."""
    src = DB_PY.read_text()
    # Pattern: the table-creation CHECK has the canonical list; the
    # migration helpers (v47/v49/v52) widen by re-creating with the
    # current full list, but parsing the live CREATE TABLE block is
    # the source of truth.
    match = re.search(
        r"CREATE TABLE IF NOT EXISTS op_progress.*?"
        r"CHECK \(kind IN \(([^)]+)\)\)",
        src,
        re.DOTALL,
    )
    assert match, "Could not locate op_progress.kind CHECK in db.py"
    return set(re.findall(r"'([a-z_]+)'", match.group(1)))


# ── Contract: every op_progress.kind is accounted for ────────


def test_every_op_progress_kind_decided_for_row_refresh():
    """Walk the op_progress.kind CHECK in db.py. Every entry must
    appear in our ROW_MUTATING_KINDS set. When a future kind is
    added to the CHECK, the dev MUST either:
      a) confirm it mutates row state → add to ROW_MUTATING_KINDS
         here (test passes because the catch-all count picks it
         up automatically), OR
      b) confirm it does NOT mutate row state → add an explicit
         kind filter to the SQL in api.py's /api/stats handler
         AND update ROW_MUTATING_KINDS to exclude it. Document
         the why-not-mutating reasoning inline.
    This is the v1.18.52 contract that motivated the audit guard."""
    declared = _check_kinds_from_db_py()
    missing_from_audit = declared - ROW_MUTATING_KINDS
    extra_in_audit = ROW_MUTATING_KINDS - declared
    assert not missing_from_audit, (
        f"v1.18.52 contract: new op_progress.kind(s) {missing_from_audit} "
        f"added to db.py CHECK but not classified in this test's "
        f"ROW_MUTATING_KINDS. Decide whether they mutate row state, "
        f"then either add them here (catch-all picks them up) OR add "
        f"an explicit filter in api.py /api/stats SQL."
    )
    assert not extra_in_audit, (
        f"v1.18.52 audit-side has kinds {extra_in_audit} not present "
        f"in db.py CHECK. Either re-add to CHECK or drop from the audit."
    )


def test_api_stats_exposes_op_progress_running():
    """`/api/stats` must surface an `op_progress_running` count
    so refreshTopbarStatus can fold it into anyMutatingOpActive."""
    src = API_PY.read_text()
    # The SQL alias.
    assert "AS op_progress_running" in src, (
        "v1.18.52: /api/stats SQL must compute op_progress_running"
    )
    # The response field.
    assert '"op_progress_running": row["op_progress_running"]' in src, (
        "v1.18.52: /api/stats JSON must include op_progress_running"
    )


# ── JS contract: unified anyMutatingOpActive signal ──────────


def test_app_js_defines_any_mutating_op_active():
    """refreshTopbarStatus must compute anyMutatingOpActive from
    the three signal sources (themerrdbBusy / plexEnumBusy /
    op_progress_running)."""
    src = APP_JS.read_text()
    assert "anyMutatingOpActive" in src, (
        "v1.18.52: app.js must define anyMutatingOpActive"
    )
    # The computation must union all three signal sources.
    idx = src.index("anyMutatingOpActive = ")
    block = src[idx:idx + 400]
    assert "themerrdbBusy" in block, (
        "v1.18.52: anyMutatingOpActive must include themerrdbBusy"
    )
    assert "plexEnumBusy" in block, (
        "v1.18.52: anyMutatingOpActive must include plexEnumBusy"
    )
    assert "opProgressRunning" in block or "op_progress_running" in block, (
        "v1.18.52: anyMutatingOpActive must include op_progress_running"
    )


def test_app_js_consumes_op_progress_running_from_stats():
    """app.js must read q.op_progress_running so the new
    /api/stats field actually drives the signal."""
    src = APP_JS.read_text()
    assert "q.op_progress_running" in src, (
        "v1.18.52: app.js must read q.op_progress_running"
    )


def test_app_js_kicks_rapid_poll_on_any_mutating_transition():
    """False→true transition of anyMutatingOpActive must kick
    libraryRapidPoll. True→false must one-shot loadLibrary.
    The same pattern v1.18.51 established for plex_enum, now
    unified for all row-mutating ops."""
    src = APP_JS.read_text()
    assert "prevAnyMutatingActive" in src
    idx = src.index("prevAnyMutatingActive")
    block = src[idx:idx + 1500]
    assert "libraryRapidPoll()" in block, (
        "v1.18.52: false→true transition must kick rapid-poll"
    )
    assert "loadLibrary()" in block, (
        "v1.18.52: true→false transition must one-shot loadLibrary"
    )


def test_rapid_poll_auto_stop_unified_signal():
    """libraryRapidPoll's auto-stop heuristic must consult the
    unified anyMutatingOpActive signal — not the legacy
    per-axis flags. Without this the rapid-poll terminates while
    e.g. a bulk_lps is still mutating rows in the background."""
    src = APP_JS.read_text()
    fn_idx = src.index("function libraryRapidPoll(")
    body = src[fn_idx:fn_idx + 3500]
    assert "anyMutatingOpActive" in body, (
        "v1.18.52: rapid-poll auto-stop must check anyMutatingOpActive"
    )


# ── Documentation: rule landed in CLAUDE.md ──────────────────


def test_claude_md_documents_row_refresh_contract():
    """The contract narrative must live in CLAUDE.md so future
    sessions reading the conventions see the rule. Per the user's
    ask: 'maybe worth making a rule or test so we keep this
    constant.' (Both: rule in CLAUDE.md AND tests here.)"""
    src = CLAUDE_MD.read_text()
    flat = " ".join(src.split())
    assert "Row-refresh contract" in flat, (
        "v1.18.52: CLAUDE.md must include the Row-refresh contract section"
    )
    # Key concepts the section must reference so a quick grep
    # surfaces the rule.
    assert "anyMutatingOpActive" in flat
    assert "op_progress_running" in flat
    # Acceptance criteria: the catch-all rule for adding new kinds.
    assert "Adding a new row-mutating op kind" in flat \
        or "Adding a new row-mutating" in flat


# ── End-to-end /api/stats response ───────────────────────────


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


def test_api_stats_returns_op_progress_running_field(admin_client):
    """Hit /api/stats on a fresh install — the new field must
    be present in the `queue` block even with 0 ops running."""
    r = admin_client.get("/api/stats", headers=AUTH)
    assert r.status_code == 200
    data = r.json()
    assert "queue" in data, "/api/stats must have a 'queue' block"
    assert "op_progress_running" in data["queue"], (
        "v1.18.52: /api/stats `queue` block must include "
        "op_progress_running"
    )
    # No ops on a fresh install.
    assert data["queue"]["op_progress_running"] == 0


def test_api_stats_op_progress_running_reflects_active_op(admin_client, tmp_path):
    """When an op_progress row is in status='running',
    /api/stats must count it. Pin the end-to-end mapping."""
    from app.core.db import get_conn
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    with get_conn(settings.db_path) as conn:
        conn.execute(
            "INSERT INTO op_progress (op_id, kind, status, "
            "                          started_at, updated_at) "
            "VALUES ('test_op', 'tdb_sync', 'running', "
            "        '2026-05-21T00:00:00Z', '2026-05-21T00:00:00Z')"
        )
        conn.commit()

    # /api/stats has a 1s cache TTL (CLAUDE.md class 7). Pass the
    # cache buster by waiting briefly or by hitting twice — easier
    # to just import and bust the cache directly if exposed.
    r = admin_client.get("/api/stats", headers=AUTH)
    assert r.status_code == 200
    data = r.json()
    # First read may be cached as 0; if so, hit again past the TTL.
    if data["queue"]["op_progress_running"] == 0:
        import time
        time.sleep(1.2)
        r = admin_client.get("/api/stats", headers=AUTH)
        data = r.json()
    assert data["queue"]["op_progress_running"] >= 1, (
        f"v1.18.52: expected op_progress_running >= 1, got "
        f"{data['queue']['op_progress_running']}"
    )


# ── Backwards-compat stash for legacy callers ────────────────


def test_legacy_per_axis_stashes_preserved():
    """Pre-v1.18.52 callers read myTabEnumBusy / globalEnumActive
    directly (e.g. per-tab REFRESH button lock). The v1.18.52
    refactor unified the row-refresh signal but the legacy
    stashes must stay in place — they're consumed by other UI
    code paths. Pinning to catch a future refactor that
    accidentally drops them."""
    src = APP_JS.read_text()
    assert "__motif_queue.myTabEnumBusy" in src, (
        "v1.18.52: legacy myTabEnumBusy stash must survive the unification"
    )
    assert "__motif_queue.globalEnumActive" in src, (
        "v1.18.52: legacy globalEnumActive stash must survive"
    )
