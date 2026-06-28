"""v1.22.77 (audit round 2, Batch C #6) — cookies write-back race +
non-ASCII login lockout.

(1) Both yt-dlp opts builders pointed cookiefile at the shared
/config/cookies.txt. yt-dlp's close() truncate-rewrites that file, so
with concurrent downloads (the user runs CONCURRENCY 2-3) + threaded
probes (v1.22.58), the LAST closer wrote a stale jar back — YouTube
rotates session cookies mid-run, the stale write-back invalidated the
session, and every row failed COOKIES_EXPIRED in a way that read as
"my cookies aged out". A kill mid-save could also truncate the
operator's file. Each invocation now gets a per-thread throwaway copy
(_cookiefile_snapshot); rotations are deliberately not persisted —
the operator owns cookies.txt.

(2) authenticate_password compared usernames with
hmac.compare_digest(str, str), which raises TypeError on non-ASCII —
an admin created with a non-ASCII username (create_admin only checks
length) hit an unhandled 500 on EVERY login (permanent lockout, no
breadcrumb), and the 500 fired before record_login_failure so such
probes never counted toward the v1.21.18 rate limit. Now compares
UTF-8 bytes.
"""
from __future__ import annotations

from pathlib import Path

from app.core.auth import (authenticate_password, create_admin,
                           init_auth_schema)
from app.core.downloader import _cookiefile_snapshot

REPO = Path(__file__).resolve().parent.parent
DL_PY = (REPO / "app" / "core" / "downloader.py").read_text()


# ── (1) cookie snapshot ──────────────────────────────────────


def test_snapshot_is_a_distinct_throwaway_copy(tmp_path):
    jar = tmp_path / "cookies.txt"
    jar.write_text("# Netscape HTTP Cookie File\nyt\tTRUE\t/\t1\n")
    snap = _cookiefile_snapshot(jar)
    assert snap != str(jar)
    assert Path(snap).read_text() == jar.read_text()
    # Per-thread name → a second call REUSES (overwrites) the same
    # path, so /tmp stays bounded.
    jar.write_text("rotated\n")
    snap2 = _cookiefile_snapshot(jar)
    assert snap2 == snap
    assert Path(snap2).read_text() == "rotated\n"


def test_both_opts_builders_route_through_snapshot():
    assert DL_PY.count(
        'opts["cookiefile"] = _cookiefile_snapshot(cookies_file)') == 2, (
        "v1.22.77: the probe + download opts builders must both use "
        "the throwaway copy"
    )
    assert 'opts["cookiefile"] = str(cookies_file)' not in DL_PY, (
        "the shared-jar form would reintroduce the write-back race"
    )


# ── (2) non-ASCII username login ─────────────────────────────


def test_non_ascii_admin_can_log_in(tmp_path):
    db = tmp_path / "auth.db"
    init_auth_schema(db)
    create_admin(db, username="cønnor", password="hunter2hunter2")
    # Pre-fix: TypeError ("comparing strings with non-ASCII
    # characters is not supported") → unhandled 500 → lockout.
    assert authenticate_password(
        db, username="cønnor", password="hunter2hunter2") is True
    assert authenticate_password(
        db, username="cønnor", password="wrong-password") is False


def test_non_ascii_probe_against_ascii_admin_returns_false(tmp_path):
    """An attacker posting non-ASCII usernames must get a clean False
    (which the route then counts toward the rate limit), not a 500."""
    db = tmp_path / "auth.db"
    init_auth_schema(db)
    create_admin(db, username="admin", password="hunter2hunter2")
    assert authenticate_password(
        db, username="ädmin‮", password="x" * 14) is False
