"""v0.51.75 — op_progress activity strings never store URL credentials.

Security-audit finding (medium). If the operator points motif at a PRIVATE
authenticated ThemerrDB mirror by embedding credentials in the sync URL userinfo
(a supported, masked config field), the raw URL leaked: _GitMirror interpolates
repo_url/tar_url verbatim into op_progress activity strings (sync.py GET/git
clone/git fetch), progress stored them WITHOUT scrubbing, and GET /api/progress
returns them to a READ-scoped API token (only is_authenticated — unlike the
admin-gated GET /api/events). So a lower-privileged token (the kind handed to
Homepage/dashboards) could read the git PAT / basic-auth password in cleartext.

v0.51.75 scrubs URL credentials on BOTH progress write paths (start_progress +
update_progress) via events._redact_url_credentials, and redacts at the three
sync.py call-sites too (belt-and-suspenders). The /api/progress auth level is
deliberately left unchanged (read tokens are handed to external dashboards).

This guards the write-path scrub — the robust catch-all — behaviorally.
"""
from __future__ import annotations

import json
import sqlite3

from app.core import progress
from app.core.db import init_db

SECRET = "gh_pat_SUPERSECRET1234567890"


def _stored_activity(db) -> str:
    with sqlite3.connect(db) as c:
        row = c.execute(
            "SELECT detail_json FROM op_progress WHERE op_id = 'tdb_sync'").fetchone()
    return " ".join(a["msg"] for a in json.loads(row[0]).get("activity", []))


def test_start_and_update_scrub_url_credentials(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    # start path stores an activity...
    progress.start_progress(
        db, "tdb_sync", "tdb_sync",
        activity=f"git clone --bare https://user:{SECRET}@github.com/x/y")
    # ...and the update path pushes more.
    progress.update_progress(
        db, "tdb_sync",
        activity=f"git fetch https://user:{SECRET}@github.com/x/y")
    joined = _stored_activity(db)
    assert SECRET not in joined, f"credential leaked into op_progress activity: {joined!r}"
    assert "***@github.com" in joined, f"userinfo not redacted: {joined!r}"
    # host + scheme preserved for diagnostic value.
    assert "https://" in joined and "github.com" in joined


def test_non_credential_activity_is_unchanged(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    progress.start_progress(db, "tdb_sync", "tdb_sync")
    progress.update_progress(db, "tdb_sync", activity="Downloading theme.mp3 45%")
    assert "Downloading theme.mp3 45%" in _stored_activity(db)
