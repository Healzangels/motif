"""v1.18.58 — fix Apprise "Source: Adopted" for UPLOAD MP3 path.

the user's Discord screenshot:

> "Theme added — Bastard!! (1992) / Source: Adopted"

But the // MOTIF INFO history showed:

> "Manual upload by admin: 5724736 bytes"
> "Uploaded collection theme for 'Bastard!!' (rk=438837)"

So motif sent the user an "Adopted" notification for an action
that was actually a manual MP3 upload. Wrong source label.

## Root cause

`notify_content.enrich_item` had a short-circuit at line 132-135:

```python
if row["upstream_source"] == "plex_orphan":
    ctx["provenance"] = "adopted"
else:
    ctx["provenance"] = "themerrdb"
```

For the user's BASTARD!! row:
  - themes table has it (synthesized orphan)
  - upstream_source = 'plex_orphan' ✓
  - → ctx["provenance"] = "adopted"

The local_files check at line 173+ only fired when
`"title" not in ctx` — and the themes row HAD set a title, so
local_files (which had source_kind='upload' from the UPLOAD MP3
path at api.py:9819) was never consulted. The 'adopted' default
leaked through.

## Fix

Walk local_files unconditionally. Use the explicit source_kind
→ provenance mapping to override the upstream-derived default:

  source_kind='upload'    → 'user_upload'  (NEW)
  source_kind='url'       → 'user_url'
  source_kind='themerrdb' → 'themerrdb'
  source_kind='adopt'     → 'adopted'
  provenance='manual' (no source_kind) → 'manual'
  else: preserve step-1 upstream default

Added new ProvenanceKind = 'user_upload' + label "User upload"
so Discord/Apprise reads "Source: User upload" for the
UPLOAD MP3 path.

Also fixes a latent class-9-ish issue: any plex_orphan row that
got its theme via a non-adopt path (URL download → cleared
user_overrides, manual upload) would have been mislabeled the
same way. v1.18.58 covers all those paths via the explicit
source_kind dispatch.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
NOTIFY_CONTENT_PY = REPO / "app" / "core" / "notify_content.py"


# ── enrich_item walk + source_kind override ─────────────────


def _seed_db(tmp_path: Path):
    """Materialise the schema + return the db path."""
    from app.core.db import init_db
    db = tmp_path / "test.db"
    init_db(db)
    ts = "2026-05-22T00:00:00Z"
    from app.core.db import get_conn
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "                            included, discovered_at, "
            "                            last_seen_at) "
            "VALUES ('3', 'Anime', 'show', 1, ?, ?)",
            (ts, ts),
        )
    return db


def _add_theme(db, *, media_type, tmdb_id, upstream_source, title,
               year=None):
    from app.core.db import get_conn
    ts = "2026-05-22T00:00:00Z"
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "                     upstream_source, last_seen_sync_at, "
            "                     first_seen_sync_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (media_type, tmdb_id, title, str(year) if year else None,
             upstream_source, ts, ts),
        )


def _add_local_file(db, *, media_type, tmdb_id, section_id,
                    source_kind, provenance="manual",
                    source_video_id=""):
    from app.core.db import get_conn
    ts = "2026-05-22T00:00:00Z"
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            "                          file_path, downloaded_at, "
            "                          source_video_id, provenance, "
            "                          source_kind) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (media_type, tmdb_id, section_id,
             f"anime/{tmdb_id}/theme.mp3", ts,
             source_video_id, provenance, source_kind),
        )


# ── Bug fix: UPLOAD MP3 → "User upload" not "Adopted" ───────


def test_upload_mp3_provenance_is_user_upload(tmp_path):
    """the user's BASTARD!! repro: plex_orphan theme + local_files
    with source_kind='upload' must produce provenance='user_upload'
    → Discord shows "Source: User upload". Pre-v1.18.58 the
    upstream_source='plex_orphan' branch short-circuited to
    'adopted' before local_files was consulted."""
    db = _seed_db(tmp_path)
    _add_theme(db, media_type='tv', tmdb_id=-69,
               upstream_source='plex_orphan',
               title='Bastard!!', year=1992)
    _add_local_file(db, media_type='tv', tmdb_id=-69,
                    section_id='3', source_kind='upload',
                    provenance='manual')

    from app.core.notify_content import enrich_item
    ctx = enrich_item(db, media_type='tv', tmdb_id=-69, section_id='3')
    assert ctx['provenance'] == 'user_upload', (
        f"v1.18.58: UPLOAD MP3 must set provenance='user_upload', "
        f"got {ctx['provenance']!r} — the plex_orphan upstream "
        f"branch is still leaking through"
    )


def test_adopt_provenance_still_correct(tmp_path):
    """Regression guard: a true adopt (source_kind='adopt')
    must still produce provenance='adopted'. The fix narrows
    WHEN 'adopted' fires, not its semantics."""
    db = _seed_db(tmp_path)
    _add_theme(db, media_type='tv', tmdb_id=-70,
               upstream_source='plex_orphan',
               title='Adopted Show', year=2024)
    _add_local_file(db, media_type='tv', tmdb_id=-70,
                    section_id='3', source_kind='adopt',
                    provenance='manual',
                    source_video_id='deadbeef' * 5)  # hash-derived
    from app.core.notify_content import enrich_item
    ctx = enrich_item(db, media_type='tv', tmdb_id=-70, section_id='3')
    assert ctx['provenance'] == 'adopted'


def test_user_url_provenance_when_source_kind_url(tmp_path):
    """source_kind='url' on local_files maps to 'user_url' even
    if user_overrides was cleared after the download."""
    db = _seed_db(tmp_path)
    _add_theme(db, media_type='tv', tmdb_id=-71,
               upstream_source='plex_orphan',
               title='User URL Show', year=2024)
    _add_local_file(db, media_type='tv', tmdb_id=-71,
                    section_id='3', source_kind='url',
                    provenance='manual',
                    source_video_id='dQw4w9WgXcQ')
    from app.core.notify_content import enrich_item
    ctx = enrich_item(db, media_type='tv', tmdb_id=-71, section_id='3')
    assert ctx['provenance'] == 'user_url'


def test_themerrdb_provenance_when_source_kind_themerrdb(tmp_path):
    """source_kind='themerrdb' must produce 'themerrdb' even on
    a plex_orphan row (unusual but possible — e.g., an orphan
    with a manually-curated themerrdb-style download)."""
    db = _seed_db(tmp_path)
    _add_theme(db, media_type='tv', tmdb_id=-72,
               upstream_source='plex_orphan',
               title='TDB on Orphan', year=2024)
    _add_local_file(db, media_type='tv', tmdb_id=-72,
                    section_id='3', source_kind='themerrdb',
                    provenance='auto',
                    source_video_id='dQw4w9WgXcQ')
    from app.core.notify_content import enrich_item
    ctx = enrich_item(db, media_type='tv', tmdb_id=-72, section_id='3')
    assert ctx['provenance'] == 'themerrdb'


def test_themerrdb_upstream_default_preserved(tmp_path):
    """When there's no local_files row, the upstream-based
    default (themerrdb for non-orphan) must still apply."""
    db = _seed_db(tmp_path)
    _add_theme(db, media_type='movie', tmdb_id=42,
               upstream_source='themoviedb',
               title='TDB Movie', year=2024)
    # No local_files row.
    from app.core.notify_content import enrich_item
    ctx = enrich_item(db, media_type='movie', tmdb_id=42, section_id='1')
    assert ctx['provenance'] == 'themerrdb'


def test_plex_orphan_with_no_local_file_falls_back_to_adopted(tmp_path):
    """Edge case: plex_orphan theme exists but no local_files row.
    The upstream-derived default ('adopted') stays. Reasonable —
    if motif tracked an orphan and there's no current local file,
    the most likely path was an adopt that lost its local_file
    row somewhere."""
    db = _seed_db(tmp_path)
    _add_theme(db, media_type='tv', tmdb_id=-73,
               upstream_source='plex_orphan',
               title='Orphan No LF', year=2024)
    from app.core.notify_content import enrich_item
    ctx = enrich_item(db, media_type='tv', tmdb_id=-73, section_id='3')
    assert ctx['provenance'] == 'adopted'


# ── Label map ───────────────────────────────────────────────


def test_user_upload_label_added():
    """The _PROVENANCE_LABEL map must include 'user_upload':
    'User upload' so Discord reads correctly."""
    from app.core.notify_content import _PROVENANCE_LABEL
    assert 'user_upload' in _PROVENANCE_LABEL
    assert _PROVENANCE_LABEL['user_upload'] == 'User upload'


def test_user_upload_label_distinct_from_user_url():
    """'User upload' (MP3) and 'User URL' (URL→download) must
    be visually distinct in notifications so the operator can
    tell which path the theme came from."""
    from app.core.notify_content import _PROVENANCE_LABEL
    assert (_PROVENANCE_LABEL['user_upload']
            != _PROVENANCE_LABEL['user_url'])


def test_provenance_kind_literal_includes_user_upload():
    """The ProvenanceKind Literal must include 'user_upload'
    so static type-checkers + future code see it as a valid
    value."""
    src = NOTIFY_CONTENT_PY.read_text()
    # The Literal definition must list 'user_upload'.
    lit_idx = src.index("ProvenanceKind = Literal[")
    lit_end = src.index("]", lit_idx)
    lit_block = src[lit_idx:lit_end]
    assert '"user_upload"' in lit_block, (
        "v1.18.58: ProvenanceKind Literal must include "
        "'user_upload'"
    )


# ── End-to-end format_theme_added_body ──────────────────────


def test_theme_added_body_reads_user_upload_for_upload_mp3(tmp_path):
    """End-to-end: format_theme_added_body on the BASTARD!!
    scenario must produce 'Source: User upload' (not 'Source:
    Adopted'). Pin the user-visible string the user sees on
    Discord."""
    db = _seed_db(tmp_path)
    _add_theme(db, media_type='tv', tmdb_id=-69,
               upstream_source='plex_orphan',
               title='Bastard!!', year=1992)
    _add_local_file(db, media_type='tv', tmdb_id=-69,
                    section_id='3', source_kind='upload',
                    provenance='manual')

    from app.core.notify_content import (
        enrich_item, format_theme_added_body,
    )
    ctx = enrich_item(db, media_type='tv', tmdb_id=-69, section_id='3')
    body = format_theme_added_body(ctx)
    assert "Source: 🟣 User upload" in body, (
        f"v1.18.58: body must read 'Source: User upload' for "
        f"the UPLOAD MP3 path. Got:\n{body}"
    )
    assert "Source: Adopted" not in body, (
        "v1.18.58: the pre-fix 'Source: Adopted' string must "
        "not appear for upload-MP3 rows"
    )


# ── Version marker ──────────────────────────────────────────


def test_v1_18_58_marker_present():
    src = NOTIFY_CONTENT_PY.read_text()
    assert "v1.18.58" in src
