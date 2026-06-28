"""v0.50.35 — INFO card: dead themerrdb URL turns red + a compact backup line.

Two INFO-card asks (the user):
  1. A dead ThemerrDB URL (failure_kind ∈ the dead set) renders RED so it reads as
     dead at a glance, reverting to green when a reprobe clears the failure. A
     pending NEW TDB URL means upstream already fixed it → never red.
  2. Every row with a local theme.mp3 on disk shows a compact "✓ theme.mp3 on disk"
     backup line (with the source URL), so a recoverable backup is visible on HL /
     placed / dead-url rows — not just the backup-only rows that already relabel
     "applied url" → "backup url". One compact line, no second full url field.

Pure client-render off fields api_item already returns (t.failure_kind,
lf.source_video_id), so these are source-shape pins.
"""
from __future__ import annotations

from pathlib import Path

JS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.js").read_text()


def test_dead_tdb_url_renders_red():
    # the dead flag is the shared dead-set + not-pending guard
    assert "const tdbUrlDead = !_pendingTdbUrl" in JS
    assert "TDB_DEAD_FAILURES_GLOBAL.has(t.failure_kind)" in JS
    # the url link colour switches to red when dead, green otherwise
    assert "linkOrDash(tdbUrl, tdbUrlDead ? 'var(--red)' : 'var(--green-bright)')" in JS
    # a small "dead" tag on the label, also red
    assert "const tdbDeadTag = tdbUrlDead" in JS
    assert "${tdbSrcTag}${tdbDeadTag}" in JS


def test_compact_backup_line_on_rows_with_a_local_file():
    assert "const hasBackupFile = !!(lf && lf.file_path);" in JS
    # backup-only rows already relabel applied→backup, so the compact line skips them
    assert "const backupBlock = (hasBackupFile && !lfIsBackupOnly)" in JS
    assert "✓ theme.mp3 on disk" in JS
    # reconstruct the YouTube source URL from source_video_id (skip fb/ig/sc ids)
    assert "!/^(fb-|ig-|sc-)/.test(lf.source_video_id)" in JS
    assert "https://www.youtube.com/watch?v=${lf.source_video_id}" in JS
    # rendered in the dl grid, right after the downloaded row
    assert "${dlBlock}\n        ${backupBlock}" in JS
