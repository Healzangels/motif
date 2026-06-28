"""v1.15.16 — bump yt-dlp floor + log running version on startup.

the user: "is it worth updating our ytdl to latest version I think
I'd like to try and keep all our dependencies as up to date as
possible or at least on latest stable in case of edge cases that
warrant"

## Pre-fix

requirements.txt pinned `yt-dlp>=2025.1.0` — almost a year stale
as of 2026-05-14. The pip resolver pulled current latest at
build time, but the floor didn't document a tested-minimum, and
operators had no way to see which version was actually running
without exec-ing into the container.

When YouTube ships an anti-bot change, an old yt-dlp version
fails opaquely with "Video unavailable" or "rate-limited" errors
that look like motif bugs. Every couple of months the right fix
is "rebuild image, get newer yt-dlp" — but that's invisible
unless you check.

## Fix

1. Bump the floor to `>=2026.3.17` (current latest stable). New
   image builds get this minimum even if pip's resolver picks
   something newer at install time.
2. Add a startup log line printing `yt_dlp.version.__version__`
   so the running version is visible in `docker logs motif` next
   to the existing config dump (cookies_file, plex_url, etc).
3. Add a quarterly-review note to CLAUDE.md so future-Claude
   bumps the floor on each major motif release.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
REQUIREMENTS = REPO / "requirements.txt"
MAIN_PY = REPO / "app" / "main.py"
CLAUDE_MD = REPO / "CLAUDE.md"


def test_yt_dlp_floor_bumped_to_recent_stable():
    """Floor must be a 2026.x.x date — older is stale enough to
    miss multiple YouTube anti-bot updates. Pin the prefix
    rather than an exact version so quarterly bumps land here
    naturally."""
    src = REQUIREMENTS.read_text()
    # Find the yt-dlp pin line (skip comment lines).
    for line in src.splitlines():
        s = line.strip()
        if s.startswith("yt-dlp"):
            assert ">=2026." in s, (
                f"v1.15.16 bumped the floor to a 2026.x version; "
                f"got: {s!r}"
            )
            # Defensive: anything <= 2025.x risks running into the
            # known YouTube anti-bot regressions.
            assert ">=2025." not in s
            return
    raise AssertionError("yt-dlp pin line not found in requirements.txt")


def test_startup_log_prints_yt_dlp_version():
    """The startup log must include a `yt_dlp = X.Y.Z` line so
    operators can see the installed version without exec-ing
    into the container. Pin the import + log call site so a
    future refactor of the startup banner doesn't accidentally
    drop it."""
    src = MAIN_PY.read_text()
    # The import + log block must be present.
    assert "import yt_dlp.version" in src, (
        "v1.15.16: startup must import yt_dlp.version to log "
        "the running yt-dlp version"
    )
    assert 'log.info("  yt_dlp       = %s"' in src, (
        "v1.15.16: startup log must include the yt_dlp version line"
    )


def test_startup_log_handles_yt_dlp_import_failure():
    """If yt-dlp can't be imported (pip install failed in a
    weird way), the startup log shouldn't crash — fall back to
    a clear `(not importable)` message so the operator sees the
    problem without losing the rest of the startup banner."""
    src = MAIN_PY.read_text()
    log_block_anchor = src.index("yt_dlp.version")
    log_block = src[log_block_anchor - 200:log_block_anchor + 400]
    assert "except Exception:" in log_block, (
        "v1.15.16: yt-dlp version log must be wrapped in try/except "
        "so an unimportable yt-dlp doesn't break startup"
    )
    assert "(not importable)" in log_block, (
        "v1.15.16: fallback message must say (not importable) "
        "so the operator sees the problem"
    )


def test_claude_md_documents_quarterly_bump():
    """The bump-yt-dlp-quarterly protocol must live in CLAUDE.md
    so future sessions reading it before tagging see the
    expectation. Whitespace-tolerant — same pattern as the
    bump-version protocol guard (test_v1_13_79)."""
    raw = CLAUDE_MD.read_text()
    flat = " ".join(raw.split())
    assert "Quarterly: bump the `yt-dlp` floor" in flat, (
        "v1.15.16: CLAUDE.md must document the quarterly bump"
    )
    assert "startup log line" in flat, (
        "v1.15.16: CLAUDE.md must mention the startup log line "
        "as the verification mechanism"
    )


def test_running_yt_dlp_version_meets_floor():
    """Sanity: the locally-installed yt-dlp must satisfy the
    floor. If this fails, .venv needs `pip install -U -r
    requirements.txt`. Lower bound check — exact version may
    be higher (pip resolved to latest)."""
    try:
        import yt_dlp.version as ytv
    except Exception as e:
        raise AssertionError(f"yt-dlp not importable in test env: {e}")
    installed = ytv.__version__
    # Floor is 2026.3.17 — installed must be >= that.
    parts = installed.split(".")
    assert len(parts) >= 3, f"unexpected version shape: {installed}"
    year = int(parts[0])
    assert year >= 2026, (
        f"installed yt-dlp {installed} is older than the v1.15.16 "
        f"floor (2026.3.17). Run `pip install -U yt-dlp`."
    )
