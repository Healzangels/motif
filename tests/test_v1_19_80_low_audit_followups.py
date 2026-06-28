"""v1.19.80 — LOW batch from the Opus-4.8 audit.

Three low-severity follow-ups, all class-9 / contract-drift
cleanups with no happy-path behavior change:

1. worker.py — the two `theme_pushed` discriminator sites in
   `_do_place` / `_do_place_collection` parsed `job["payload"]`
   with a bare `except (TypeError, ValueError): _p = {}` swallow
   instead of the canonical `_safe_payload_parse` breadcrumb
   helper (v1.18.98). A corrupt payload could silently mislabel a
   notification (`theme_added` vs `theme_pushed`). Routed both
   through the helper.

2. plex_enum.py — the `_upsert_items` normalize_title swallow had
   no `_FOO_WARNED` breadcrumb, unlike its identical twin at
   sync.py:483 which got `_SYNC_NORMALIZE_TITLE_WARNED` in
   v1.17.11. A broken normalize_title would silently degrade
   title-matching library-wide. Added
   `_PLEX_ENUM_NORMALIZE_TITLE_WARNED` with the warn-once shape.

3. cloud_theme_backup.py — (a) the docstring claimed "stream-hash"
   but the code buffers the whole body via `r.content`. Aligned
   the contract to the buffered reality (bounded ≤~9MB; streaming
   would be a premature abstraction). (b) the walker re-called
   `cancel_check()` at walk completion — redundant (a clean loop
   exit already means "not cancelled") and a TOCTOU window. Now
   tracked in a local `cancelled` flag.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from unittest.mock import MagicMock


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WORKER = (REPO / "app" / "core" / "worker.py").read_text()
PLEX_ENUM = (REPO / "app" / "core" / "plex_enum.py").read_text()
CTB = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()


# ── 1. worker payload-parse helper routing ─────────────────────


def test_do_place_discriminator_uses_safe_payload_parse():
    """The _do_place theme_pushed discriminator must route through
    _safe_payload_parse (the v1.18.98 breadcrumb helper), not a
    bare except-swallow."""
    assert 'where="_do_place (theme_pushed discriminator)"' in WORKER, (
        "v1.19.80: _do_place theme_pushed discriminator must use "
        "_safe_payload_parse with a descriptive where= label"
    )


def test_do_place_collection_discriminator_uses_safe_payload_parse():
    """Same for _do_place_collection's discriminator."""
    assert (
        'where="_do_place_collection (theme_pushed discriminator)"'
        in WORKER
    ), (
        "v1.19.80: _do_place_collection theme_pushed discriminator "
        "must use _safe_payload_parse"
    )


def test_payload_parse_helper_count_grew_by_two():
    """v1.18.98 wired 9 sites (1 def + 9 calls = 10 occurrences).
    v1.19.80 adds the 2 discriminator sites → 12 occurrences.
    Pins the additions so a regression to bare json.loads fires."""
    assert WORKER.count("_safe_payload_parse(") >= 12, (
        "v1.19.80: expected >= 12 _safe_payload_parse occurrences "
        "(1 def + 11 calls) after wiring the 2 discriminator sites"
    )


def test_no_bare_payload_swallow_in_discriminators():
    """The pre-fix shape — `_p = json.loads(...)` guarded by a bare
    `except (TypeError, ValueError): _p = {}` — must be GONE."""
    import re
    bad = re.findall(
        r"_p = json\.loads\([^\n]*\n\s*except \(TypeError, ValueError\):"
        r"\s*\n\s*_p = \{\}",
        WORKER,
    )
    assert not bad, (
        f"v1.19.80: remaining bare json.loads payload swallow in a "
        f"discriminator site — use _safe_payload_parse. Matches: {bad[:2]}"
    )


def test_helper_returns_empty_dict_still_drives_discriminator():
    """Behavioral: a corrupt payload through the helper yields {},
    so the discriminator computes _is_pushed=False (theme_added,
    not theme_pushed) — the safe default. Confirms the helper's
    contract the discriminator now relies on."""
    from app.core import worker
    worker._PAYLOAD_PARSE_WARNED = set()
    p = worker._safe_payload_parse("{corrupt", where="t")
    assert p == {}
    # The discriminator's logic: _is_pushed iff force/force_place/reason.
    is_pushed = bool(p.get("force") or p.get("force_place") or p.get("reason"))
    assert is_pushed is False


# ── 2. plex_enum normalize_title breadcrumb ────────────────────


def test_plex_enum_declares_normalize_title_warned_flag():
    """plex_enum.py must declare the module-level warn-once flag,
    mirroring sync.py's _SYNC_NORMALIZE_TITLE_WARNED (v1.17.11)."""
    assert "_PLEX_ENUM_NORMALIZE_TITLE_WARNED" in PLEX_ENUM, (
        "v1.19.80: plex_enum.py must declare "
        "_PLEX_ENUM_NORMALIZE_TITLE_WARNED for the once-per-process "
        "breadcrumb (twin of sync.py:483)"
    )
    assert "global _PLEX_ENUM_NORMALIZE_TITLE_WARNED" in PLEX_ENUM


def test_plex_enum_normalize_title_logs_once_per_process():
    """The _upsert_items normalize_title swallow must warn on first
    hit, debug after — same shape as the sync.py twin. Locate the
    swallow on the second normalize_title import in the file (the
    first is in a docstring/comment; the live one is in the loop)."""
    # The live import sits inside _upsert_items' per-row loop.
    idx = PLEX_ENUM.index("tn = normalize_title(")
    window = PLEX_ENUM[idx:idx + 1500]
    assert "log.warning(" in window, (
        "v1.19.80: first occurrence must log.warning so the operator "
        "sees a broken normalize_title at boot"
    )
    assert "log.debug(" in window, (
        "v1.19.80: subsequent occurrences must drop to log.debug to "
        "avoid drowning the hot-path log"
    )
    assert "_PLEX_ENUM_NORMALIZE_TITLE_WARNED" in window


def test_plex_enum_normalize_title_no_longer_bare_swallow():
    """The pre-fix `except Exception:\\n tn = (it.title or '').lower()`
    bare swallow must be gone (the except now binds the exception
    and logs)."""
    import re
    bad = re.search(
        r"except Exception:\s*\n\s*tn = \(it\.title or \"\"\)\.lower\(\)",
        PLEX_ENUM,
    )
    assert bad is None, (
        "v1.19.80: plex_enum normalize_title swallow must bind the "
        "exception and log a breadcrumb, not swallow silently"
    )


# ── 3. cloud_theme_backup contract + cancelled flag ────────────


def test_ctb_docstring_no_longer_claims_streaming():
    """The 'stream-hash' / 'streaming sha256' wording was inaccurate
    (the code buffers r.content). Contract aligned to buffered
    reality."""
    assert "streaming sha256" not in CTB, (
        "v1.19.80: drop the misleading 'streaming sha256' wording — "
        "the body is buffered (bounded ≤~9MB)"
    )
    assert "Stream-hash bytes" not in CTB, (
        "v1.19.80: drop the 'Stream-hash bytes' docstring step — "
        "the code buffers via r.content"
    )
    assert "buffered body" in CTB or "Buffer the body" in CTB


def test_ctb_uses_local_cancelled_flag_not_second_cancel_call():
    """The walk-completion cursor-clear must key off a local
    `cancelled` flag, not a second cancel_check() call (redundant +
    TOCTOU)."""
    assert "if use_cursor and not cancelled:" in CTB, (
        "v1.19.80: clear cursor on `not cancelled`, not a re-call "
        "of cancel_check() at completion"
    )
    assert "cancel_check is None or not cancel_check()" not in CTB, (
        "v1.19.80: the redundant completion-time cancel_check() call "
        "must be gone"
    )


def _seed_prow(conn, *, rk, tmdb_id, section_id="1"):
    """Minimal P-row: has_theme=1, guid_tmdb set, no local_file, no
    placement — the walker's candidate shape."""
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, "
        "   themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, "
        "        '2026-05-27', '2026-05-27')",
        (section_id,),
    )
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, guid_tmdb, title, "
        "   year, has_theme, first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'movie', ?, ?, 2020, 1, "
        "        '2026-05-27', '2026-05-27')",
        (rk, section_id, tmdb_id, f"Movie {rk}"),
    )


def _mock_plex():
    plex = MagicMock()

    def _get_themes(*, rating_key):
        return {
            "ok": True, "http_status": 200, "error": None,
            "body": {"MediaContainer": {"Metadata": [
                {"ratingKey": "metadata://themes/" + "a" * 40}
            ]}},
        }

    plex.get_themes.side_effect = _get_themes
    return plex


def test_clean_walk_clears_cursor_without_recalling_cancel_check(tmp_path):
    """The TOCTOU fix: a cancel signal arriving AFTER the loop
    completes must NOT block the cursor-clear. With the old code, a
    completion-time cancel_check() returning True wrongly preserved
    the cursor on a fully-completed walk. The new `cancelled` flag
    means cancel_check is never re-called at completion."""
    db_path = tmp_path / "test.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        for i in range(3):
            _seed_prow(conn, rk=f"rk-{i:03d}", tmdb_id=900 + i)
        conn.commit()
        from app.core.cloud_theme_backup import identify_c1_rows, CURSOR_KEY

        # False for the in-loop calls (2 batches), True afterward.
        # Old code would call a 3rd time at completion → True →
        # cursor NOT cleared. New code never makes that 3rd call.
        calls = {"n": 0}

        def cancel_check():
            calls["n"] += 1
            return calls["n"] > 2

        identify_c1_rows(
            conn, _mock_plex(), inter_call_sleep_s=0,
            batch_size=2, use_cursor=True, cancel_check=cancel_check,
        )
        # cancel_check called exactly twice (once per batch), NOT a
        # third time at completion.
        assert calls["n"] == 2, (
            f"v1.19.80: cancel_check must NOT be re-called at walk "
            f"completion; got {calls['n']} calls (expected 2)"
        )
        # Clean walk → cursor cleared despite the post-loop cancel.
        row = conn.execute(
            "SELECT value FROM runtime_settings WHERE key = ?",
            (CURSOR_KEY,),
        ).fetchone()
        assert row is None, (
            "v1.19.80: a fully-completed walk must clear the cursor "
            "even if a cancel would have fired after the last batch"
        )


def test_midloop_cancel_preserves_cursor(tmp_path):
    """A genuine mid-walk cancel must PRESERVE the resume cursor so
    the next run picks up where it left off."""
    db_path = tmp_path / "test.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        for i in range(3):
            _seed_prow(conn, rk=f"rk-{i:03d}", tmdb_id=950 + i)
        from app.core.events import now_iso
        from app.core.cloud_theme_backup import identify_c1_rows, CURSOR_KEY
        # Pre-seed the cursor at rk-000 → walk scopes to rk-001/002.
        conn.execute(
            "INSERT INTO runtime_settings "
            "(key, value, updated_at, updated_by) "
            "VALUES (?, 'rk-000', ?, 'test')",
            (CURSOR_KEY, now_iso()),
        )
        conn.commit()

        identify_c1_rows(
            conn, _mock_plex(), inter_call_sleep_s=0,
            batch_size=2, use_cursor=True,
            cancel_check=lambda: True,  # cancel immediately
        )
        row = conn.execute(
            "SELECT value FROM runtime_settings WHERE key = ?",
            (CURSOR_KEY,),
        ).fetchone()
        assert row is not None and row[0] == "rk-000", (
            "v1.19.80: a mid-walk cancel must preserve the resume "
            f"cursor; got {row[0] if row else None!r}"
        )
