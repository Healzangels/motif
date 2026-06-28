"""v1.20.56 — status auto-dismiss + emoji doc/lint drift (audit fix 4/4).

Design-system audit (2026-05-31):
1. watchTvdbBridgeCompletion stamped a `✓ processed N rows` status on the
   done path and never auto-dismissed it (DESIGN_SYSTEM §3 status-text
   auto-dismiss), unlike every sibling diagnostic status.
2. The §3 emoji table was missing 4 shipped event kinds (📤 theme_pushed,
   🔄 themes_updated_by_sync, 🎯 backup_ready_to_deploy, 💔 theme_lost*),
   and the central emoji lint didn't scan notify_content.py where those
   title formatters live.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
DESIGN = (REPO / "docs" / "DESIGN_SYSTEM.md").read_text()
NOTIFY_CONTENT = (REPO / "app" / "core" / "notify_content.py").read_text()
WORKER = (REPO / "app" / "core" / "worker.py").read_text()


def test_tvdb_bridge_done_status_auto_dismisses():
    anchor = APP_JS.index("async function watchTvdbBridgeCompletion(")
    body = APP_JS[anchor:anchor + 2000]
    # The ✓ done status must be followed by an auto-dismiss call.
    assert "processed ${op.stage_current ?? '?'} rows" in body
    assert "_autoDismissOpStatus(resultEl, 6000)" in body


# ── emoji doc table now lists the 4 newer kinds ──────────────


def test_emoji_table_lists_newer_kinds():
    # Find the §3 emoji table.
    table = DESIGN[DESIGN.index("| Event | Emoji | Subject shape |"):]
    table = table[:table.index("Rules:")]
    for kind, emoji in (
        ("theme_pushed", "📤"),
        ("themes_updated_by_sync", "🔄"),
        ("backup_ready_to_deploy", "🎯"),
        ("plex_theme_lost", "💔"),
    ):
        assert kind in table, f"§3 emoji table missing {kind}"
        assert emoji in table, f"§3 emoji table missing {emoji}"


def test_newer_kinds_carry_documented_emoji_in_code():
    """Positive doc↔code pin: the title formatters use the emoji the §3
    table documents (so the table can't silently drift from the code)."""
    assert 'return f"📤' in NOTIFY_CONTENT  # theme_pushed
    assert 'return f"🎯' in NOTIFY_CONTENT  # backup_ready
    assert 'return f"💔' in NOTIFY_CONTENT  # theme_lost*
    # v1.21.6: themes_updated_by_sync folded into sync_completed as a
    # plain "🔄 Updated:" section header (no longer an f-string title).
    assert '"🔄 Updated:"' in WORKER         # sync updated-titles section


def test_emoji_lint_scans_notify_content():
    lint = (REPO / "tests" / "test_v1_17_19_notification_emoji.py").read_text()
    anchor = lint.index("def test_no_lingering_motif_colon_titles(")
    body = lint[anchor:anchor + 700]
    assert "NOTIFY_CONTENT_PY" in body, (
        "the central motif:-prefix lint must include notify_content.py "
        "in its targets"
    )


def test_v1_20_56_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
