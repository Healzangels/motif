"""v1.19.70 — sync notifications filter to titles in user's Plex library.

the user's 2026-05-29 sample sync notification listed "The Family
Circumstances of the Irregular Witch (2023)" among 43 new themes,
but searching for it in every library tab surfaced it as a
"// RESULTS · 1 ThemerrDB-only items" row labeled "(not in your
Plex library)". TDB tracks every theme upstream publishes; motif
should only notify the user about themes for content they actually
own. The faded T-only filter chip still lets the operator discover
TDB-only titles in the library UI when curious; the Discord/Slack
notification just stops pushing them.

## Changes

**`app/core/sync.py`** — the per-row apply loop now computes
`in_plex` once at the top of each row's processing (via a
single EXISTS query against plex_items, mapping themes
`tv` → plex `show`). Both the `is_new` branch's `new_count`
increment and the `url_changed` branch's `updated_count` +
`updated_titles.append` are gated on `in_plex`. Items not
in the user's library never contribute to the sync_completed
title summary OR the title-list events.

Side cleanup: the `is_new` branch's old local `plex_mt_for_
sidecar` if/elif/else block (used only for the has_sidecar
check) was deleted; both consumers now share the outer
`_plex_mt` variable.

**`app/core/worker.py`** — `themes_added_by_sync` query
extended with an EXISTS plex_items clause. Pre-fix the
query was `SELECT title, year FROM themes WHERE
first_seen_sync_at >= ?` — picked up every new TDB title,
in-library or not. Post-fix the same query joins against
plex_items via guid_tmdb + media_type mapping
(plex 'show' ↔ themes 'tv'), filtering to in-library only.

## URL-deep-link / surface preservation

- The themes table still receives every TDB row (sync's
  upsert logic unchanged). Out-of-library titles are still
  searchable via the library's faded T-only chip + the
  search box.
- `sync_stats.new_count` semantic changes: it's now the
  count of in-library new themes (was: total new themes
  regardless of library presence). This drives the sync
  notification's title text ("Motif sync — N new") and the
  themes_added_by_sync event title.

## Next tag (B / v1.19.71)

the user's companion ask — surface the blue !UPD glyph on
in-library rows when TDB adds a new theme (so the operator
can take action vs only seeing it in notifications) — is
deferred to v1.19.71. That's a bigger design surface (new
pending_update.kind, settings toggle for auto-download on
SRC=— rows, JS gate work) and worth its own tag.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


# ── sync.py: in_plex gate on count + title-list ──────────────


def test_sync_computes_in_plex_predicate():
    """sync.py's apply loop must compute an `in_plex` predicate over
    plex_items, hoisted to the outer scope so both is_new + url_changed
    branches can use it. v1.23.89: the predicate moved into the
    _plex_title_present helper (index-friendly split-EXISTS) — in_plex is
    now its return value, still computed once in the shared per-item scope."""
    assert "in_plex = _plex_title_present(conn, _plex_mt, tmdb_id, _theme_row_id)" in SYNC_PY
    # The helper resolves presence over plex_items by guid_tmdb (and theme_id).
    idx = SYNC_PY.index("def _plex_title_present(")
    block = SYNC_PY[idx:idx + 1600]
    assert "FROM plex_items" in block
    assert "guid_tmdb = ?" in block


def test_new_count_gated_on_in_plex():
    """The is_new branch's `stats.new_count += 1` must be
    nested under `if in_plex:`."""
    new_idx = SYNC_PY.index("stats.new_count += 1")
    pre = SYNC_PY[max(0, new_idx - 200):new_idx]
    # The increment must be preceded by an `if in_plex:` clause.
    assert "if in_plex:" in pre, (
        "v1.19.70: new_count increment must be gated on in_plex"
    )


def test_updated_count_gated_on_in_plex_and_actionable():
    """The url_changed branch's `stats.updated_count += 1` must
    be gated on BOTH `is_actionable` AND `in_plex`."""
    upd_idx = SYNC_PY.index("stats.updated_count += 1")
    pre = SYNC_PY[max(0, upd_idx - 400):upd_idx]
    # The gate must include both is_actionable AND in_plex.
    assert "is_actionable and in_plex" in pre, (
        "v1.19.70: updated_count must require both "
        "is_actionable AND in_plex"
    )


def test_v1_19_70_marker_documents_in_plex_gate():
    """v1.19.70 marker explains the in_plex motivation."""
    assert "v1.19.70" in SYNC_PY
    # Either branch's marker should mention 'in_plex' rationale.
    in_plex_marker_idx = SYNC_PY.index("v1.19.70:")
    block = SYNC_PY[in_plex_marker_idx:in_plex_marker_idx + 800]
    assert "Plex library" in block or "plex_items" in block


# ── sync.py side-cleanup: plex_mt_for_sidecar consolidated ──


def test_plex_mt_for_sidecar_consolidated_into_outer_scope():
    """The is_new branch had its own if/elif/else mapping
    media_type → plex_mt_for_sidecar (for the has_sidecar
    EXISTS check). v1.19.70 hoisted this mapping to the outer
    scope (as `_plex_mt`) so both is_new + url_changed branches
    can share the in_plex query. The local plex_mt_for_sidecar
    variable should be GONE."""
    assert "plex_mt_for_sidecar = " not in SYNC_PY, (
        "v1.19.70: plex_mt_for_sidecar local variable should be "
        "consolidated into outer _plex_mt"
    )
    # The outer-scope mapping must exist.
    assert "_plex_mt = " in SYNC_PY


# ── worker.py: themes_added_by_sync query filters to in_plex ──


def test_themes_added_query_joins_plex_items():
    """The `themes_added_by_sync` query in worker.py must JOIN
    or EXISTS against plex_items so out-of-library titles don't
    surface in the title-list notification."""
    # v1.21.6: the New-titles query moved into the sync_completed
    # summary, gated by the themes_added_by_sync toggle.
    idx = WORKER_PY.index('_events.get("themes_added_by_sync"')
    end = WORKER_PY.index("# v1.12.126", idx)
    block = WORKER_PY[idx:end]
    # The query block must reference plex_items via EXISTS.
    assert "EXISTS" in block
    assert "FROM plex_items" in block
    # The media_type aliasing (themes 'tv' ↔ plex 'show') must
    # be present.
    assert "'show' THEN 'tv'" in block, (
        "v1.19.70: query must alias plex_items.media_type='show' "
        "to themes.media_type='tv' (sync's media_type values "
        "are themes-side ('tv'), plex_items uses Plex strings)"
    )


def test_themes_added_query_preserves_first_seen_sync_at_filter():
    """v1.17.1's H1 fix (column rename) must survive — the
    WHERE clause still uses first_seen_sync_at, the v1.19.70
    fix only adds an EXISTS plex_items conjunct."""
    # v1.21.6: the New-titles query moved into the sync_completed
    # summary, gated by the themes_added_by_sync toggle.
    idx = WORKER_PY.index('_events.get("themes_added_by_sync"')
    end = WORKER_PY.index("# v1.12.126", idx)
    block = WORKER_PY[idx:end]
    assert "first_seen_sync_at >= ?" in block
    assert "ORDER BY first_seen_sync_at ASC" in block


def test_themes_added_query_preserves_limit_11():
    """LIMIT 11 (10 + 1 for "and N more" detection) preserved."""
    # v1.21.6: the New-titles query moved into the sync_completed
    # summary, gated by the themes_added_by_sync toggle.
    idx = WORKER_PY.index('_events.get("themes_added_by_sync"')
    end = WORKER_PY.index("# v1.12.126", idx)
    block = WORKER_PY[idx:end]
    assert "LIMIT 11" in block


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_70_version_pin():
    """Loose prefix — later tags continue the v1.19.x line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
