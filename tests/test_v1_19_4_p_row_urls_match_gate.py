"""v1.19.4 — exclude P rows from the urls_match update prompt.

the user's 2026-05-24 repro: a P row (Back to the Future 1985 in
4K Movies) showed the !UPD attention indicator because the user
override URL happened to match the new TDB URL (kind=urls_match
pending_update). The prompt was meaningless — SRC=P means Plex
serves + motif owns no placement → ACCEPT / KEEP CURRENT have
no effect on the user-visible theme.

## Diagnostic confirmed the narrow scope

Investigation query against the live install showed ONE P-row
with a pending update, kind=urls_match. Zero P-rows with
upstream_changed. So the narrow fix (skip urls_match on P rows
specifically) closes the user's bug without touching the rarer
upstream_changed path.

## Fix

11 user-facing SQL sites had identical `kind = 'urls_match' OR
new_url != applied` predicates feeding the !UPD pill / row
indicator / topbar count / bulk-action gate. Each one needed
a P-row exclusion on the urls_match branch.

Helper: `_not_p_row_sql(t, pi)` at module scope returns the
SQL fragment evaluating to TRUE when the row is NOT a pure
SRC=P row. Uses an inline NOT EXISTS for the placement check
so it works in subqueries without a placements JOIN. Two
alias parameters (`t`/`pi` for the library main FROM,
`t2`/`pi2` for count subqueries) cover all 11 sites.

Each site's predicate now reads:

  AND (
    (kind = 'urls_match' AND _not_p_row_sql)
    OR new_url != applied
  )

SQL operator precedence (AND binds tighter than OR) makes the
new gate scope cleanly to the urls_match branch only. The
upstream-changed branch (new_url differs) fires unchanged.

## What's pinned

- Helper defined at module scope with both alias params
- All 11 user-facing urls_match gate sites invoke the helper
- No remaining ungated urls_match clauses in api.py (audit)
- End-to-end: seed a P-row with urls_match pending_update;
  verify /api/library returns it WITHOUT pending_update=1
- End-to-end: seed a non-P (U) row with urls_match; verify
  pending_update=1 still set (other row classes unaffected)
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── Source pins ──────────────────────────────────────────────


def test_helper_defined_at_module_scope():
    """`_not_p_row_sql` must live at module scope so the 11
    SQL sites can interpolate it via f-string."""
    src = API_PY.read_text()
    assert "def _not_p_row_sql(" in src
    # Top-level (no leading indent).
    assert "\ndef _not_p_row_sql(" in src, (
        "v1.19.4: helper must be module-scope"
    )


def test_helper_takes_alias_params():
    """Helper must accept t_alias + pi_alias so it can serve
    both the library main FROM (t/pi) AND count subqueries
    (t2/pi2)."""
    src = API_PY.read_text()
    import re
    sig_match = re.search(
        r"def _not_p_row_sql\([^)]*\)", src,
    )
    assert sig_match
    sig = sig_match.group(0)
    assert "t" in sig and "pi" in sig, (
        "v1.19.4: helper signature must accept t_alias + pi_alias"
    )


def test_helper_marker_explains_rationale():
    """The helper's docstring must explain WHY P rows skip the
    urls_match prompt — so future readers don't 'optimize' the
    gate away."""
    src = API_PY.read_text()
    idx = src.index("def _not_p_row_sql(")
    block = src[max(0, idx - 2000):idx + 500]
    assert "v1.19.4" in block
    # Must reference the user-visible semantic.
    assert ("Plex serves" in block
            or "Plex" in block and "placement" in block), (
        "v1.19.4: helper marker must explain SRC=P semantic"
    )
    # Must reference the urls_match path.
    assert "urls_match" in block


def test_p_row_gate_lives_in_actionable_helper():
    """v1.19.4 originally spread the P-row exclusion across 11 inline
    urls_match gate sites. v1.22.10 CONSOLIDATED all of them into one
    `_pending_update_actionable_sql` helper, which is now invoked at every
    pill/filter/sort surface. So the P-row exclusion lives in exactly that one
    place — a stronger guarantee than the old 11-site spread. Pin: the helper
    exists, contains the `_not_p_row_sql` P-skip on the urls_match branch, and
    gates ≥11 surfaces."""
    src = API_PY.read_text()
    assert "def _pending_update_actionable_sql(" in src, (
        "v1.22.10: the consolidated actionable-gate helper must exist"
    )
    idx = src.index("def _pending_update_actionable_sql(")
    # v1.22.14: anchor the window on the NEXT def so it can't overshoot into an
    # unrelated function as the helper's docstring grows (markers stack).
    body = src[idx:src.index("def _pending_update_detected_sql(", idx)]
    assert "= 'urls_match'" in body and "_not_p_row_sql(t, pi)" in body, (
        "v1.19.4/v1.22.10: the urls_match branch must keep its P-row exclusion"
    )
    assert src.count("_pending_update_actionable_sql('") >= 11, (
        "v1.22.10: the helper must gate every pending-update surface "
        "(pill columns, tdb/attn filters, NEEDS WORK sort, count subqueries)"
    )


def test_no_ungated_urls_match_in_gate_predicates():
    """Audit guard: any `= 'urls_match'` in a gate predicate
    context (followed by `OR COALESCE` typically) must be
    paired with a _not_p_row_sql call within ~200 chars. If a
    future patch adds a new ungated urls_match gate, this
    fires."""
    src = API_PY.read_text()
    import re
    # Find every `= 'urls_match'` occurrence with a snippet
    # of surrounding context.
    for m in re.finditer(r"= 'urls_match'", src):
        idx = m.start()
        # Skip comments + write-sites (INSERT with literal).
        lookback = src[max(0, idx - 200):idx]
        if "--" in lookback.split("\n")[-1]:
            continue  # SQL comment
        if "kind != 'urls_match'" in src[idx - 30:idx + 30]:
            continue  # the kind != exclusion (different semantic)
        if "'pending', 'urls_match'" in src[idx - 50:idx + 30]:
            continue  # INSERT writing literal
        if "#" in src[max(0, idx - 80):idx].split("\n")[-1]:
            continue  # Python comment
        # Pin: helper must appear within 400 chars after.
        lookahead = src[idx:idx + 400]
        assert "_not_p_row_sql" in lookahead, (
            f"v1.19.4: ungated urls_match gate detected at "
            f"offset {idx}. Context: ...{src[idx:idx+100]}... "
            f"Either add _not_p_row_sql() OR add an explicit "
            f"exclusion (comment / kind != / write-site)."
        )


# ── End-to-end behavioral ────────────────────────────────────


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


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    ).db_path


def _seed_section(conn, section_id="1", title="Movies"):
    conn.execute(
        "INSERT INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, "
        "   themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES (?, ?, 'movie', 0, 0, "
        "        'movies', 1, '2026-05-24T00:00:00', "
        "        '2026-05-24T00:00:00')",
        (section_id, title),
    )


def _seed_theme(conn, theme_id=1, tmdb_id=100,
                youtube_url="https://www.youtube.com/watch?v=ORIG"):
    now = "2026-05-24T00:00:00"
    conn.execute(
        "INSERT INTO themes "
        "  (id, media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
        "VALUES (?, 'movie', ?, 'Test Movie', 'imdb', ?, ?, ?)",
        (theme_id, tmdb_id, now, now, youtube_url),
    )


def _seed_plex_item(conn, *, rk, theme_id, section_id="1",
                     has_theme=1, verified=1):
    """Plex item where motif has resolved theme_id linkage."""
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, theme_id, "
        "   guid_imdb, guid_tmdb, title, year, has_theme, "
        "   local_theme_file, folder_path, "
        "   plex_independent_theme, plex_theme_verified_ok, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'movie', ?, "
        "        'tt100', NULL, 'Test', 2020, ?, 0, "
        "        '/data/movies/Test', 0, ?, "
        "        '2026-05-24T00:00:00', "
        "        '2026-05-24T00:00:00')",
        (rk, section_id, theme_id, has_theme, verified),
    )


def _seed_user_override(conn, tmdb_id, section_id="1",
                         url="https://www.youtube.com/watch?v=NEW"):
    conn.execute(
        "INSERT INTO user_overrides "
        "  (media_type, tmdb_id, section_id, youtube_url, "
        "   set_at, set_by) "
        "VALUES ('movie', ?, ?, ?, '2026-05-24T00:00:00', 'admin')",
        (tmdb_id, section_id, url),
    )


def _seed_pending_update(conn, tmdb_id, section_id="1", kind="urls_match",
                          new_url="https://www.youtube.com/watch?v=NEW",
                          old_url=None):
    # v1.19.60: sync now binds old_youtube_url for the read-path
    # gate's real-diff check. Seed it to mirror what real sync
    # writes; defaults to None for callers that don't care.
    conn.execute(
        "INSERT INTO pending_updates "
        "  (media_type, tmdb_id, section_id, "
        "   new_video_id, new_youtube_url, "
        "   old_video_id, old_youtube_url, "
        "   detected_at, decision, kind) "
        "VALUES ('movie', ?, ?, 'NEW', ?, "
        "        'ORIG', ?, "
        "        '2026-05-24T00:00:00', 'pending', ?)",
        (tmdb_id, section_id, new_url, old_url, kind),
    )


def test_p_row_with_urls_match_no_pending_update(
    admin_client, tmp_path,
):
    """the user's exact repro: a P row (has_theme=1, verified, no
    placement) with a urls_match pending_update should NOT show
    as pending_update=1 in /api/library."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn, theme_id=1, tmdb_id=100)
        # P-row: has_theme=1, verified=1, NO placements row.
        _seed_plex_item(
            conn, rk="rk-p", theme_id=1,
            has_theme=1, verified=1,
        )
        # User override matches the pending update's new_url.
        _seed_user_override(
            conn, tmdb_id=100,
            url="https://www.youtube.com/watch?v=NEW",
        )
        _seed_pending_update(
            conn, tmdb_id=100, kind="urls_match",
            new_url="https://www.youtube.com/watch?v=NEW",
        )
        conn.commit()

    r = admin_client.get("/api/library?tab=movies", headers=AUTH)
    assert r.status_code == 200, r.text
    items = r.json().get("items", [])
    # Find our seeded row.
    matching = [it for it in items if it.get("rating_key") == "rk-p"]
    assert matching, f"seeded row missing: {[it.get('rating_key') for it in items]}"
    row = matching[0]
    assert row.get("pending_update") in (0, None, False), (
        f"v1.19.4: P-row with urls_match must NOT have "
        f"pending_update set; got {row.get('pending_update')}"
    )


def test_non_p_row_with_urls_match_still_pending(
    admin_client, tmp_path,
):
    """The fix must be SCOPED to P rows. A U row (user override
    + motif placement) with urls_match should still get
    pending_update=1 — that's the U→T conversion prompt the
    urls_match kind exists for."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn, theme_id=2, tmdb_id=200)
        # NOT-P row: has_theme=0 (Plex doesn't serve), with a
        # placement (motif owns it).
        _seed_plex_item(
            conn, rk="rk-u", theme_id=2,
            has_theme=0, verified=0,
        )
        # Placement so SRC reads as something other than P.
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, "
            "   media_folder, placed_at, placement_kind, "
            "   plex_rating_key, plex_refreshed, provenance) "
            "VALUES ('movie', 200, '1', "
            "        '/data/movies/U Row', "
            "        '2026-05-24T00:00:00', 'hardlink', "
            "        'rk-u', 1, 'manual')"
        )
        # Local file so the has-something gate passes.
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   source_kind, source_video_id, downloaded_at) "
            "VALUES ('movie', 200, '1', 'u.mp3', "
            "        'url', 'NEW', '2026-05-24T00:00:00')"
        )
        _seed_user_override(
            conn, tmdb_id=200,
            url="https://www.youtube.com/watch?v=NEW",
        )
        _seed_pending_update(
            conn, tmdb_id=200, kind="urls_match",
            new_url="https://www.youtube.com/watch?v=NEW",
        )
        conn.commit()

    r = admin_client.get("/api/library?tab=movies", headers=AUTH)
    items = r.json().get("items", [])
    matching = [it for it in items if it.get("rating_key") == "rk-u"]
    assert matching, "seeded U-row missing"
    row = matching[0]
    assert row.get("pending_update") == 1, (
        f"v1.19.4: non-P (U) row with urls_match must STILL "
        f"have pending_update=1 (the U→T conversion prompt); "
        f"got {row.get('pending_update')}"
    )


def test_p_row_with_upstream_changed_still_pending(
    admin_client, tmp_path,
):
    """The fix is narrow: skip urls_match on P rows ONLY.
    A P row with kind=upstream_changed (TDB URL actually
    differs from applied) should still flag — the prompt is
    meaningful there (refresh the downloaded file ahead of
    a possible future P→T switch)."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(
            conn, theme_id=3, tmdb_id=300,
            youtube_url="https://www.youtube.com/watch?v=ORIG",
        )
        # P-row.
        _seed_plex_item(
            conn, rk="rk-p2", theme_id=3,
            has_theme=1, verified=1,
        )
        # Local file for has-something check.
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   source_kind, source_video_id, downloaded_at) "
            "VALUES ('movie', 300, '1', 'p2.mp3', "
            "        'themerrdb', 'ORIG', '2026-05-24T00:00:00')"
        )
        # upstream_changed kind: new_url DIFFERS from applied.
        # v1.19.60: seed old_url too so the v1.19.60 real-diff
        # gate fires (pre-fix the gate compared against
        # themes.youtube_url, which the test seeded to ORIG).
        _seed_pending_update(
            conn, tmdb_id=300, kind="upstream_changed",
            new_url="https://www.youtube.com/watch?v=DIFFERENT",
            old_url="https://www.youtube.com/watch?v=ORIG",
        )
        conn.commit()

    r = admin_client.get("/api/library?tab=movies", headers=AUTH)
    items = r.json().get("items", [])
    matching = [it for it in items if it.get("rating_key") == "rk-p2"]
    assert matching, "seeded P-row (upstream_changed) missing"
    row = matching[0]
    # upstream_changed branch still fires regardless of SRC.
    assert row.get("pending_update") == 1, (
        f"v1.19.4: P-row with upstream_changed (URLs actually "
        f"differ) must STILL have pending_update=1 — the fix "
        f"only suppresses urls_match, not real diff; got "
        f"{row.get('pending_update')}"
    )
