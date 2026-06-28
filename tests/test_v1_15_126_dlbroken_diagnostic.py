"""v1.15.126 — detailed dlBroken diagnostic for audio endpoint.

the user on v1.15.125 visited the audio URL directly and got:

    {"detail":"canonical recorded but missing on disk (dlBroken)"}

— but his Unraid file manager confirmed the file is at the path
the UI shows (/data/media/themes/tv/Watchmen (2019)/theme.mp3,
7.74 MB). So the API endpoint's `full.is_file()` is returning
False on a file that DOES exist on the host.

## The mismatch

The UI computes `lf.abs_path` as a string-only concatenation
`settings.themes_dir / file_path` — no `.resolve()`, no stat
(api.py:10542).

The audio endpoint does:
  full = settings.themes_dir / row["file_path"]
  full = full.resolve()                    # follows symlinks
  if themes_root not in full.parents:      # path-traversal guard
      raise 400
  if not full.is_file():                   # file existence check
      raise 410

the user hit the 410. So either:
  - .resolve() changed `full` to a path that doesn't exist
  - Some directory in the chain has a permission issue stopping stat
  - The path the user sees in the UI (`/data/media/themes/tv/Watchmen
    (2019)/theme.mp3`) and what the container resolves are different

## The fix (this tag)

Add detailed diagnostic logging on the 410 branch:

  - themes_dir + file_path (raw inputs)
  - resolved_full
  - full.exists() (separate from is_file — could be a directory,
    a broken symlink, etc.)
  - parent.exists() + parent.is_dir()
  - parent contents listing (first 20 entries) so case-mismatch,
    encoding, or "directory empty" cases all surface in one line

The 410 response stays the same (cosmetic + user-side); the log
line gives the user (and future operators) the forensics to
diagnose without code-instrumentation gymnastics.

Once the user re-hits the URL on v1.15.126, the next iteration
can fix the actual mismatch.

## Tests

  - log.warning call in the dlBroken branch
  - the log line references the right fields (themes_dir,
    file_path, full, parent, parent_contents)
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def _audio_endpoint_body() -> str:
    src = API_PY.read_text()
    fn_start = src.index("def api_item_theme_audio(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    return src[fn_start:fn_end]


def test_dlbroken_logs_warning_with_diagnostic_context():
    """The 410 branch must call log.warning before raising, with
    the resolved path + parent state + file_path so the operator
    can diagnose from a single log line."""
    body = _audio_endpoint_body()
    # The log.warning call must precede the HTTPException raise.
    raise_idx = body.index("status_code=410,")
    walk_back = body[max(0, raise_idx - 2000):raise_idx]
    assert "log.warning(" in walk_back, (
        "v1.15.126: the dlBroken branch must log.warning the "
        "diagnostic context before raising the 410. Pre-fix the "
        "endpoint just returned a 410 with no forensics."
    )


def test_dlbroken_diagnostic_includes_required_fields():
    """The log line must include themes_dir, file_path, resolved
    full path, and parent state — these are the fields needed to
    diagnose the mismatch."""
    body = _audio_endpoint_body()
    # Locate the log.warning block specifically near the 410.
    raise_idx = body.index("status_code=410,")
    log_window = body[max(0, raise_idx - 2000):raise_idx]
    required = [
        "themes_dir",
        "file_path",
        "resolved_full",
        "parent",
        "parent_contents",
    ]
    missing = [f for f in required if f not in log_window]
    assert not missing, (
        f"v1.15.126: dlBroken log line missing fields: {missing}. "
        "All are needed to forensically diagnose the path mismatch."
    )


def test_dlbroken_diagnostic_handles_parent_stat_errors():
    """If the parent directory itself can't be stat'd (perm
    denied / mount missing), the diagnostic must still emit
    rather than masking the underlying OSError under a new
    crash."""
    body = _audio_endpoint_body()
    raise_idx = body.index("status_code=410,")
    log_window = body[max(0, raise_idx - 2000):raise_idx]
    assert "OSError" in log_window, (
        "v1.15.126: the parent-stat block must catch OSError so "
        "the log line emits even when the directory chain is "
        "unreachable."
    )
