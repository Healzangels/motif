"""v1.23.75 — notification-audit consistency fixes.

Three gaps the notification audit surfaced (patterns built recently but not
applied uniformly):

#1 FB thumbnail attachment (v1.22.94) was worker-only. The sibling per-item
   dispatches that can also carry a Facebook-sourced theme (backup_ready,
   the theme-lost reaper tiers, theme_deleted ×3, the test-trigger) omitted
   `attach_url`, so a fbcdn-backed theme rendered no image there.
#2 Section/library grouping (v1.22.45) was sync-only. The coalesced bulk
   batches (theme_pushed / theme_added / theme_backed_up) rendered a flat
   list; they now group by Plex library when a bulk spans ≥2 of them.
#3 format_backup_ready_body omitted the provenance line, so it didn't say
   whether the backup was a User URL or a Plex-cloud copy.
"""
from __future__ import annotations

from pathlib import Path

from app.core import notify_content as nc

REPO = Path(__file__).resolve().parent.parent
PLEX_ENUM = (REPO / "app" / "core" / "plex_enum.py").read_text()
API = (REPO / "app" / "web" / "api.py").read_text()


# ── #1 FB thumb attachment at the non-worker dispatch sites ──


def test_attach_url_at_backup_ready_and_reaper():
    # backup_ready_to_deploy + the four-way theme-lost reaper now attach.
    # v0.51.151: windows widened (700→820 / 500→600) after the item_ctx=_ctx
    # click-through line landed between event_kind and attach_url in both calls.
    bi = PLEX_ENUM.index('event_kind="backup_ready_to_deploy"')
    assert "attach_url=_nc.attachment_thumb_url(" in PLEX_ENUM[bi:bi + 820]
    # v0.51.288: the reaper call became dispatch_coalesced — the thumb rides
    # single_attach_url now — and the fixed 600-char window rotted (the .261
    # bug class). Anchor to the statement right after the call instead.
    ri = PLEX_ENUM.index("event_kind=_event_kind")
    reaper_call = PLEX_ENUM[ri:PLEX_ENUM.index("_ndedupe.record_fire", ri)]
    assert "single_attach_url=_nc.attachment_thumb_url(_ctx)" in reaper_call


def test_attach_url_at_theme_deleted_and_test_trigger():
    # all three theme_deleted dispatches + the test-trigger attach the thumb.
    assert API.count("attach_url=_nc.attachment_thumb_url(_notify_ctx)") >= 3
    ti = API.index("event_kind=_event_kind")
    assert "attach_url=_nc.attachment_thumb_url(_ctx)" in API[ti:ti + 500]


# ── #2 section grouping on bulk batches ──


def test_batch_body_groups_when_two_libraries():
    labels = ["A (2020)", "B (2021)", "C (2022)"]
    buckets = {"Movies": ["A (2020)", "B (2021)"], "Anime": ["C (2022)"]}
    body = nc.format_theme_pushed_batch_body(labels, buckets)
    # bold library headers, items nested beneath, alphabetical sections.
    assert "**Anime**" in body and "**Movies**" in body
    assert body.index("**Anime**") < body.index("**Movies**")
    assert "* C (2022)" in body and "* A (2020)" in body


def test_batch_body_stays_flat_for_single_library():
    labels = ["A (2020)", "B (2021)"]
    buckets = {"Movies": ["A (2020)", "B (2021)"]}
    body = nc.format_theme_added_batch_body(labels, buckets)
    assert "**Movies**" not in body, "one library → no redundant header"
    assert body == "* A (2020)\n* B (2021)"


def test_batch_body_flat_without_buckets():
    # legacy 1-arg call (e.g. the new_tdb batch) still works → flat list.
    body = nc.format_theme_backed_up_batch_body(["X (2024)", "Y (2024)"])
    assert body == "* X (2024)\n* Y (2024)"


def test_grouped_overflow_caps_across_sections():
    labels = [f"M{i}" for i in range(12)] + [f"A{i}" for i in range(8)]
    buckets = {"Movies": [f"M{i}" for i in range(12)],
               "Anime": [f"A{i}" for i in range(8)]}
    body = nc.format_theme_pushed_batch_body(labels, buckets)
    assert "…and 5 more" in body, "20 items, cap 15 → 5 overflow"


def test_dispatch_batch_groups_by_section(monkeypatch):
    from app.core import notify as n

    sent = {}
    monkeypatch.setattr(n, "dispatch",
                        lambda *a, **k: sent.update(k))

    items = [
        {"label": "A (2020)", "section": "Movies",
         "title": "t", "body": "b", "body_format": "markdown",
         "attach_url": None,
         "batch_title_fn": nc.format_theme_pushed_batch_title,
         "batch_body_fn": nc.format_theme_pushed_batch_body},
        {"label": "B (2021)", "section": "Anime",
         "title": "t", "body": "b", "body_format": "markdown",
         "attach_url": None,
         "batch_title_fn": nc.format_theme_pushed_batch_title,
         "batch_body_fn": nc.format_theme_pushed_batch_body},
    ]
    n._dispatch_batch(Path("/x"), object(), "theme_pushed", items)
    assert "**Movies**" in sent["body"] and "**Anime**" in sent["body"]


def test_dispatch_coalesced_accepts_section():
    # source pin: the coalesce API + buffer carry `section`.
    src = (REPO / "app" / "core" / "notify.py").read_text()
    assert "section: str = \"\"," in src
    assert '"section": section,' in src


# ── #3 backup_ready provenance line ──


def test_backup_ready_body_includes_provenance():
    ctx: nc.ItemContext = {"title": "X", "year": "2020",
                           "provenance": "user_url"}
    body = nc.format_backup_ready_body(ctx, override_url="https://y/1")
    assert "User URL" in body, "backup source named so user-URL ≠ cloud copy"
