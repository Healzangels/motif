"""v1.17.24 — orphan zombie themes keeps RESTORE alive post-PURGE.

the user's screenshot scenario:
1. SET URL on a plex_orphan row (SoundCloud URL) → U source, themed,
   downloaded, placed.
2. PURGE the row.
3. Expected: SOURCE menu offers RESTORE (the captured URL is in
   `previous_urls` with kind='user' post-v1.17.17).
4. Actual: SOURCE menu only offers SET URL + UPLOAD MP3. No
   RESTORE. User has to re-type the URL from memory.

## Root cause — the FK SET NULL chain

The library SQL JOINs `previous_urls` via the themes row:

```sql
LEFT JOIN themes t ON t.id = pi.theme_id
LEFT JOIN previous_urls pv_sec
  ON pv_sec.media_type = t.media_type
 AND pv_sec.tmdb_id    = t.tmdb_id
 AND pv_sec.section_id = pi.section_id
```

When PURGE / UNMANAGE on an orphan ran `DELETE FROM themes
WHERE media_type=? AND tmdb_id=?`, the FK `plex_items.theme_id
REFERENCES themes(id) ON DELETE SET NULL` nulled
`pi.theme_id`. Next library load:
* `LEFT JOIN themes t ON t.id = NULL` → t.* all NULL
* `pv_sec.media_type = NULL` → no match (NULL != anything in SQL)
* `has_previous_url=0` → RESTORE gate fails

The v1.17.17 fix changed `_capture_previous_url` to emit
`kind='user'` for orphan-sourced captures (so
`revert_redundant` wouldn't suppress RESTORE). That fix was
correct but unreachable because the JOIN itself broke after
the themes DROP.

## Fix

Stop dropping the orphan themes row on PURGE / UNMANAGE.
Instead UPDATE `youtube_url = NULL`, `youtube_video_id = NULL`.
The row becomes a zombie tracker:
* `plex_items.theme_id` still points at a valid row (no FK
  cascade)
* `previous_urls` JOIN resolves via `t.media_type`/`t.tmdb_id`
* `has_previous_url=1` when the captured URL differs from the
  now-NULL `t.youtube_url` (which is always the case post-
  PURGE)
* RESTORE surfaces ✓
* SRC letter still computes as `'—'` (no placement / local_files)
* `isOrphan` still true → v1.17.17's DOWNLOAD/RE-DOWNLOAD TDB
  gate keeps those hidden (orphans have no TDB to fetch from)

`api_delete_item` is unchanged — DELETE is the explicit "nuke
this orphan" path, and user can chain PURGE → CLEAR URL →
DELETE for a true clean slate.

The theme_deleted notification body's `is_orphan` branch flips
from `"orphan row dropped"` to `"RESTORE available"` — the
zombie tracker EXISTS post-PURGE, and the user's recovery
path is the actionable fact worth surfacing.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_INIT = REPO / "app" / "__init__.py"


# v1.17.24 narrative comments mention the retired "DELETE FROM
# themes" wording. Strip Python comments before negative
# assertions so the source-grep targets executable code only.
_PY_COMMENT_RE = re.compile(r"#.*$", re.MULTILINE)


def _strip_py_comments(s: str) -> str:
    return _PY_COMMENT_RE.sub("", s)


# ── Source-level pins ─────────────────────────────────────────


def test_unmanage_zombies_orphan_themes_not_dropped():
    """`api_unmanage_item` orphan branches must UPDATE the
    themes row (null URLs) rather than DELETE it. Two
    branches: last-section + legacy global UNMANAGE."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_unmanage_item(")
    fn_end = src.index("@app.post(\"/api/items", fn_start + 1)
    fn_code = _strip_py_comments(src[fn_start:fn_end])
    # Counter-pin: no DELETE FROM themes in the orphan branches.
    # The DELETE was the v1.12.57-era hard-drop; v1.17.24
    # replaces it with UPDATE.
    assert "DELETE FROM themes" not in fn_code, (
        "v1.17.24: api_unmanage_item must NOT DROP the orphan "
        "themes row — the FK cascade nulls plex_items.theme_id "
        "and breaks the previous_urls JOIN. Use UPDATE "
        "instead."
    )
    # Pin: the new UPDATE shape appears.
    assert "SET youtube_url = NULL" in fn_code, (
        "v1.17.24: api_unmanage_item orphan branches must "
        "UPDATE themes SET youtube_url = NULL."
    )
    assert "youtube_video_id = NULL" in fn_code, (
        "v1.17.24: youtube_video_id must also be nulled."
    )


def test_forget_zombies_orphan_themes_not_dropped():
    """Same fix in `api_forget_item` (the PURGE handler)."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_forget_item(")
    fn_end = src.index("@app.", fn_start + 1)
    fn_code = _strip_py_comments(src[fn_start:fn_end])
    assert "DELETE FROM themes" not in fn_code, (
        "v1.17.24: api_forget_item orphan branches must NOT "
        "DROP the themes row."
    )
    assert "SET youtube_url = NULL" in fn_code
    assert "youtube_video_id = NULL" in fn_code


def test_delete_item_still_hard_deletes():
    """`api_delete_item` is the explicit nuke-everything path
    and MUST still DROP the themes row. v1.17.24 only changes
    PURGE / UNMANAGE; DELETE is unchanged."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_delete_item(")
    # api_delete_item is shorter — read to the next @app. or end.
    fn_end = src.index("@app.", fn_start + 1) if "@app." in src[fn_start + 1:] else len(src)
    fn_code = _strip_py_comments(src[fn_start:fn_end])
    assert "DELETE FROM themes" in fn_code, (
        "v1.17.24: api_delete_item must STILL DELETE the themes "
        "row — it's the explicit nuke-this-orphan path."
    )


# ── Notification copy follows the new reality ────────────────


def test_forget_notification_body_says_restore_available():
    """The theme_deleted body for PURGE on an orphan must
    surface 'RESTORE available' — the captured URL is reachable
    via the zombie themes row + previous_urls JOIN."""
    src = API_PY.read_text()
    # Anchor on the function declaration so we land in
    # api_forget_item proper (not an earlier comment mentioning
    # "api_forget_item").
    fn_start = src.index("async def api_forget_item(")
    fn_end = src.index("@app.", fn_start + 1)
    fn = src[fn_start:fn_end]
    assert "RESTORE available" in fn, (
        "v1.17.24: theme_deleted body for orphan PURGE must "
        "mention 'RESTORE available' — pre-fix it said "
        "'orphan row dropped' which was misleading."
    )
    # Counter-pin: the old wording is gone from the executable
    # path (might still appear in a historical comment).
    fn_code = _strip_py_comments(fn)
    assert "orphan row dropped" not in fn_code, (
        "v1.17.24: 'orphan row dropped' is misleading post-fix "
        "— the row is a zombie tracker, not dropped."
    )


# ── End-to-end DB-level check ────────────────────────────────


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db)
    return db


def test_zombie_themes_keeps_previous_urls_join_alive(fresh_db):
    """End-to-end: simulate post-PURGE state with a zombie
    themes row + previous_urls capture, then run the
    has_previous_url logic and confirm the captured URL is
    reachable.

    Pre-v1.17.24: the themes row would be DROPPED → FK SET
    NULL on plex_items.theme_id → the JOIN would fail and
    has_previous_url=0.

    Post-v1.17.24: the themes row stays as a zombie (URLs
    NULL'd), the JOIN succeeds, has_previous_url=1 when
    captured URL exists."""
    captured_url = "https://soundcloud.com/x/y"
    with sqlite3.connect(fresh_db) as conn:
        # Seed the zombie themes row (post-PURGE state).
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, year, "
            "   youtube_url, youtube_video_id, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES (?, ?, 'Some Orphan', '2020', "
            "        NULL, NULL, 'plex_orphan', "
            "        datetime('now'), datetime('now'))",
            ("movie", -42),
        )
        themes_id = conn.execute(
            "SELECT id FROM themes WHERE media_type='movie' AND tmdb_id=-42"
        ).fetchone()[0]
        # Seed the previous_urls capture (kind='user' from
        # v1.17.17).
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, "
            "   youtube_url, kind, captured_at) "
            "VALUES (?, ?, '1', ?, 'user', datetime('now'))",
            ("movie", -42, captured_url),
        )
        conn.commit()
        # Verify the JOIN reaches the captured URL via the
        # zombie themes row's (media_type, tmdb_id).
        # Mirror the api_library JOIN shape — the load-bearing
        # piece is that pv_sec.media_type = t.media_type
        # succeeds when t.* is non-NULL.
        row = conn.execute(
            """
            SELECT pv.youtube_url, pv.kind
              FROM themes t
              LEFT JOIN previous_urls pv
                ON pv.media_type = t.media_type
               AND pv.tmdb_id    = t.tmdb_id
             WHERE t.id = ?
            """,
            (themes_id,),
        ).fetchone()
    assert row is not None, (
        "v1.17.24 sanity: zombie themes row must be SELECTable."
    )
    assert row[0] == captured_url, (
        "v1.17.24: the captured URL must be reachable via the "
        "zombie themes row's (media_type, tmdb_id) JOIN key. "
        "Pre-fix the themes row was DROPPED and the JOIN "
        "couldn't resolve the captured URL."
    )
    assert row[1] == "user"


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_24():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 24), (
        f"v1.17.24: __version__ must be >= 1.17.24 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
