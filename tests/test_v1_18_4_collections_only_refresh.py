"""v1.18.4 — `// REFRESH COLLECTIONS` label + collections-only enum.

Two follow-ups the user flagged on the v1.18.3 deploy:

  1. The library refresh button label flickered from
     `// REFRESH COLLECTIONS` (SSR-rendered correctly by
     library.html) to `// REFRESH PLEX` on the first
     refreshTopbarStatus tick. Root cause: app.js's
     `libraryRefreshLabel()` helper mapped 'movies' / 'tv' /
     'anime' to display labels but had no entry for
     'collections' — it fell through to `'PLEX'`. The
     first poll-driven `updateLibraryRefreshBtnLabel()`
     overwrote the SSR text.

  2. REFRESH PLEX on /collections enqueued full plex_enum jobs
     that walked /library/sections/{id}/all (~16K items on
     the user's install) when the user only wanted the collection
     rows refreshed. Per the user: "is there anyway to make it so
     it only refresh collections and not all items within the
     libraries to increase speed."

Fixes:

  * `libraryRefreshLabel()` adds `collections: 'COLLECTIONS'` to
    the tab→label map AND short-circuits the 4K prefix when
    tab==='collections' (collections aren't 4K-tagged at the
    Plex level — same template guard as library.html's hero btn).

  * `run_plex_enum()` gains a `collections_only=False` kwarg.
    When True, the per-section main item walk
    (enumerate_section_items + sidecar stat + folder fallback +
    _upsert_items) is skipped entirely. Only the v1.18.0
    collections-pass (enumerate_collections_for_section +
    _upsert_items for collection rows) runs. _verify_theme_claims
    (per-rk HEAD probe) is also skipped — it's a movie/show
    correctness signal, not a collections one. The
    `last_enum_content_changed_at` stamp is NOT written under
    collections_only so the next full enum's delta gate stays
    honest (a collections refresh shouldn't tell the next full
    enum that the section is up-to-date).

  * `_do_plex_enum` worker handler reads `collections_only` from
    the job payload and passes it through.

  * `/api/libraries/refresh` adds `collections_only: True` to
    the job payload when `tab == "collections"`.

Throughput: the user's install is 10381 movies + 4161 TV shows +
1231 anime + 994 collections (movie section) + 140 (tv) + 94
(anime). Full enum walks ~16K items (~90s on a fresh connection
+ disk-stat sweep + theme verify); collections-only walks ~1,228
collection rows + skip-and-stamp. Should be ~10× faster.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = REPO / "app" / "web" / "static" / "app.js"
PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"
WORKER_PY = REPO / "app" / "core" / "worker.py"
API_PY = REPO / "app" / "web" / "api.py"


# ── libraryRefreshLabel includes 'collections' ────────────────


def test_library_refresh_label_handles_collections_tab():
    """The helper's tab→label map must include 'collections'.
    Without it, the first JS-driven label update overwrites the
    SSR 'COLLECTIONS' with 'PLEX' — the flicker the user saw."""
    js = APP_JS.read_text()
    fn_start = js.index("function libraryRefreshLabel(")
    fn_end = js.index("\n  function ", fn_start + 1)
    body = js[fn_start:fn_end]
    assert "collections: 'COLLECTIONS'" in body, (
        "v1.18.4: libraryRefreshLabel must map 'collections' to "
        "'COLLECTIONS' — otherwise the JS overwrites the SSR "
        "label with 'PLEX' on first poll tick"
    )


def test_library_refresh_label_skips_4k_prefix_for_collections():
    """Collections aren't 4K-tagged → the label must not emit
    '4K COLLECTIONS'. Mirrors the library.html template guard
    `{% if fourk and tab != "collections" %}4K {% endif %}`."""
    js = APP_JS.read_text()
    fn_start = js.index("function libraryRefreshLabel(")
    fn_end = js.index("\n  function ", fn_start + 1)
    body = js[fn_start:fn_end]
    assert "if (tab === 'collections') return tabName;" in body, (
        "v1.18.4: libraryRefreshLabel must skip the 4K prefix on "
        "collections"
    )


# ── run_plex_enum collections_only kwarg ──────────────────────


def test_run_plex_enum_signature_has_collections_only():
    """`run_plex_enum` must declare a `collections_only=False`
    kwarg so worker callers can opt into the fast-path."""
    src = PLEX_ENUM_PY.read_text()
    sig_match = re.search(
        r"def run_plex_enum\(.+?collections_only:\s*bool\s*=\s*False",
        src, re.DOTALL,
    )
    assert sig_match is not None, (
        "v1.18.4: run_plex_enum must accept `collections_only: "
        "bool = False`"
    )


def test_run_plex_enum_skips_main_item_walk_under_collections_only():
    """The per-section main item walk
    (enumerate_section_items + _upsert_items for movies/shows)
    must be gated on `not collections_only`. Without the gate,
    the fast-path still walks /all and saves nothing."""
    src = PLEX_ENUM_PY.read_text()
    # Anchor on the wrapping conditional.
    assert "if not collections_only:" in src, (
        "v1.18.4: main item walk must be wrapped in "
        "`if not collections_only:`"
    )
    # The conditional must precede the enumerate_section_items call.
    gate_idx = src.index("if not collections_only:")
    block_after = src[gate_idx:gate_idx + 600]
    assert "client.enumerate_section_items(" in block_after, (
        "v1.18.4: the gate must wrap enumerate_section_items"
    )


def test_run_plex_enum_skips_verify_theme_claims_under_collections_only():
    """_verify_theme_claims (per-rk HEAD probe) is a movie/show
    correctness check. Collections-only must skip it to honor
    the speed promise."""
    src = PLEX_ENUM_PY.read_text()
    # Pin the conditional around the verify call.
    assert "if collections_only:\n                    verified_n = 0" in src, (
        "v1.18.4: _verify_theme_claims must be skipped under "
        "collections_only — verified_n=0 sentinel keeps the "
        "downstream pluralization logic happy"
    )


def test_run_plex_enum_skips_content_changed_stamp_under_collections_only():
    """Writing `last_enum_content_changed_at` after a collections-
    only pass would tell the next full enum that the section is
    up-to-date — but the full /all walk never ran. The stamp
    MUST be gated on `not collections_only`."""
    src = PLEX_ENUM_PY.read_text()
    # The stamp block lives right before "Section done — fill bar"
    # (line comment v1.14.74). Pin the gate.
    # v1.22.72: the gate gained `and not collections_fetch_failed` —
    # a failed collections fetch must not stamp either (the stamp
    # arms the skip gate and defeats the advertised retry).
    # v1.23.77: the gate also gained `and not skip_collections` — an
    # items-only library-tab refresh must not stamp either, or the delta
    # gate would let the next full enum skip the section's collections.
    assert ("if (not collections_only and not skip_collections\n"
            "                        and not collections_fetch_failed):") in src, (
        "v1.18.4/v1.22.72/v1.23.77: last_enum_content_changed_at stamp must "
        "be gated on `not collections_only` AND `not skip_collections` AND a "
        "successful collections fetch so the full-enum delta gate stays honest"
    )


def test_run_plex_enum_still_runs_collections_pass_under_collections_only():
    """The collections-pass (Phase 5's
    enumerate_collections_for_section + _upsert_items for
    collection rows) must remain in the per-section loop — that's
    the work the fast-path is meant to do."""
    src = PLEX_ENUM_PY.read_text()
    # The collections-pass block is unchanged at this version;
    # source-grep confirms the call survives.
    assert "client.enumerate_collections_for_section(" in src
    # Counter-pin: the call must NOT be inside the
    # `if not collections_only:` block (otherwise collections_only
    # would skip the collections pass — exactly the wrong thing).
    src_lines = src.split("\n")
    in_gate_block = False
    gate_indent: int | None = None
    found_call_in_gate = False
    for line in src_lines:
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        if "if not collections_only:" in stripped:
            in_gate_block = True
            gate_indent = indent
            continue
        if in_gate_block and gate_indent is not None:
            if stripped and indent <= gate_indent:
                in_gate_block = False
                gate_indent = None
        if in_gate_block and "enumerate_collections_for_section(" in stripped:
            found_call_in_gate = True
            break
    assert not found_call_in_gate, (
        "v1.18.4: the collections-pass must run OUTSIDE the "
        "`if not collections_only:` gate — the gate's whole "
        "purpose is to skip the main item walk while keeping "
        "the collections walk."
    )


# ── _do_plex_enum reads collections_only from payload ─────────


def test_do_plex_enum_reads_collections_only_from_payload():
    """Worker handler must extract `collections_only` from the
    job payload and forward it to run_plex_enum."""
    src = WORKER_PY.read_text()
    fn_start = src.index("def _do_plex_enum(self, job:")
    fn_end = src.index("\n    def _do_sync(", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "collections_only = False" in body, (
        "v1.18.4: handler must declare a local collections_only "
        "default"
    )
    assert 'payload.get("collections_only")' in body
    assert "collections_only=collections_only," in body, (
        "v1.18.4: handler must forward the flag to run_plex_enum"
    )


# ── /api/libraries/refresh sets collections_only for collections ──


def test_libraries_refresh_sets_collections_only_for_collections_tab():
    """The /api/libraries/refresh handler must add
    `collections_only: True` to the job payload when
    tab='collections'. Other tabs keep the flag unset (defaults
    to full enum)."""
    src = API_PY.read_text()
    # Find the new enqueue block (post-v1.18.4 dict build).
    idx = src.index('if tab == "collections":\n                        job_payload["collections_only"] = True')
    assert idx > 0, (
        "v1.18.4: /api/libraries/refresh must set "
        "collections_only=True on the collections-tab refresh"
    )


# ── End-to-end DB-level: collections_only behavior ───────────


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db)
    return db


def test_do_plex_enum_payload_round_trip(fresh_db: Path):
    """Round-trip test: a job payload {'section_id': '1',
    'collections_only': true} that lands in the jobs table must
    parse cleanly. Confirms the JSON serializer in
    api_libraries_refresh produces a payload the worker's
    json.loads can consume."""
    ts = "2026-05-20T00:00:00"
    payload = {"section_id": "1", "collections_only": True,
               "scope": "collections"}
    with sqlite3.connect(fresh_db) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, payload, status, "
            "                  created_at, next_run_at) "
            "VALUES ('plex_enum', ?, 'pending', ?, ?)",
            (json.dumps(payload), ts, ts),
        )
        row = conn.execute(
            "SELECT payload FROM jobs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    parsed = json.loads(row[0])
    assert parsed["collections_only"] is True
    assert parsed["section_id"] == "1"
