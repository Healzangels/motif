"""v1.17.16 — notification audit pass.

the user's feedback on v1.17.14:

> adding looks better, I think maybe we could remove the
> duplication of the title
> issue with purging however [shows "movie/-26" instead of
> "Everything Is Illuminated (2005)"]
> can we audit all of them to reduce having to test making sure
> titles are being displayed etc, also lets make sure we're not
> duplicating the titles all over the place in the notifications.
> Also the adding theme is nice but because we get the embedded
> theme which I like the notification takes up a lot of space on
> the discord window if there is any way to make it smaller

Three concrete issues:

## 1. Title duplication

The notification subject (`motif: theme added — Everything Is
Illuminated (2005)`) already includes the human title. The body
opened with `**Everything Is Illuminated (2005)**` as line 1 —
pure duplication. On Discord the bold title showed BELOW the
subject line where Discord already renders it big.

Fix: drop the bold title line from `format_theme_added_body` and
`format_theme_deleted_body`. The subject line is the canonical
title surface; the body is structural metadata + URL.

## 2. Purge shows "movie/-26"

`api_forget_item` and `api_delete_item` ran `enrich_item(...)`
AFTER the destructive transaction. For orphan-drop paths
(FORGET on a `plex_orphan` upstream_source, or any DELETE) the
themes row + all FK'd children are gone by then — enrich_item
finds nothing, falls back to the bare-ID `display_title` like
"movie/-26", and the user sees that in the notification.

Fix: capture the `ItemContext` BEFORE the transaction in all
three theme_deleted handlers. The context is captured at the
top of the handler, just after `_require_admin(...)`, and
threaded into `notify.dispatch` after the destructive op
completes. `api_unmanage_item` doesn't strictly need this
(theme row may survive UNMANAGE) but uses the same pattern
for consistency.

## 3. Body footprint

The text body lost one line (the bold title). Discord's YouTube
auto-embed itself is the main vertical-space contributor, but
the embed is what the user explicitly likes ("the embedded theme
which I like") — it's the size cost of the feature. Body
shape now:

    Source: ThemerrDB · YouTube
    https://www.youtube.com/watch?v=...
    [YouTube embed card]

Two lines of text + the embed. Minimally compact while still
informative.

For theme_deleted, action + extra-note collapse onto one line
when both present:

    Forgotten by admin · 1 placement(s) unlinked · orphan row dropped
    Source: ThemerrDB · YouTube
    Previous theme: <https://www.youtube.com/...>
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
NOTIFY_CONTENT_PY = REPO / "app" / "core" / "notify_content.py"
API_PY = REPO / "app" / "web" / "api.py"
APP_INIT = REPO / "app" / "__init__.py"


# ── Title-duplication retirement ──────────────────────────────


def test_added_body_does_not_include_bold_title():
    """Pin: format_theme_added_body must NOT emit the bold
    display_title — the subject line already has it."""
    from app.core.notify_content import format_theme_added_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 42,
        "display_title": "Everything Is Illuminated (2005)",
        "theme_url": "https://www.youtube.com/watch?v=9wX8Es4MUMo",
        "thumb_url": (
            "https://i.ytimg.com/vi/9wX8Es4MUMo/hqdefault.jpg"
        ),
        "provenance": "themerrdb",
    }
    body = format_theme_added_body(ctx)
    assert "**Everything Is Illuminated (2005)**" not in body, (
        "v1.17.16: the bold title must not appear in the body "
        "— it duplicates the subject line."
    )
    # And the body must NOT start with a bold marker at all
    # (defensive against any future "let's bring back the title
    # with a different prefix" edit).
    assert not body.startswith("**"), (
        "v1.17.16: body must not start with bold markdown — "
        "no title duplication of any shape."
    )


def test_deleted_body_does_not_include_bold_title():
    """Same constraint for delete events."""
    from app.core.notify_content import format_theme_deleted_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": -26,
        "display_title": "Everything Is Illuminated (2005)",
        "theme_url": "https://youtu.be/9wX8Es4MUMo",
        "thumb_url": None,
        "provenance": "themerrdb",
    }
    body = format_theme_deleted_body(
        ctx, action="forgotten", actor="admin",
        extra_note="1 placement(s) unlinked · orphan row dropped",
    )
    assert "**Everything Is Illuminated (2005)**" not in body
    assert not body.startswith("**")


def test_deleted_body_collapses_action_and_extra_when_present():
    """Action + extra-note now fit on one line when both
    present. Body is 3 lines (was 4 in v1.17.14)."""
    from app.core.notify_content import format_theme_deleted_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 42,
        "display_title": "Everything Is Illuminated (2005)",
        "theme_url": "https://youtu.be/9wX8Es4MUMo",
        "thumb_url": None,
        "provenance": "themerrdb",
    }
    body = format_theme_deleted_body(
        ctx, action="forgotten", actor="admin",
        extra_note="1 placement(s) unlinked · orphan row dropped",
    )
    # v1.19.92: body leads with the actor, not the action verb — the
    # title carries "Theme forgotten", so the verb was dropped here.
    assert (
        "By admin · 1 placement(s) unlinked "
        "· orphan row dropped"
    ) in body, (
        "v1.17.16/v1.19.92: actor + extra-note collapse onto one "
        "line with `·` separators (no leading action verb)."
    )


def test_deleted_body_without_extra_omits_separator():
    """When extra_note is empty, the action line stands alone
    (no trailing `·`)."""
    from app.core.notify_content import format_theme_deleted_body
    ctx = {
        "media_type": "movie",
        "tmdb_id": 42,
        "display_title": "Some Title",
        "theme_url": None,
        "thumb_url": None,
        "provenance": "unknown",
    }
    body = format_theme_deleted_body(
        ctx, action="unmanaged", actor="user",
    )
    # v1.19.92: actor-led line (verb lives in the title).
    assert "By user" in body
    # The action line must not end with a dangling separator.
    first_line = body.split("\n", 1)[0]
    assert not first_line.rstrip().endswith("·"), (
        "v1.17.16: no dangling `·` when extra_note is empty."
    )


# ── Pre-op enrichment capture ─────────────────────────────────


def _find_handler_block(src: str, handler_name: str) -> str:
    """Return the source span from `async def <handler>` to the
    next `@app.` decorator or end of file."""
    start = src.index(f"async def {handler_name}(")
    after = src.find("\n    @app.", start)
    return src[start:after if after != -1 else len(src)]


def test_unmanage_captures_enrichment_before_txn():
    """api_unmanage_item must call enrich_item BEFORE the
    `with get_conn(db) as conn, transaction(conn):` block.
    Pre-fix the call ran AFTER the txn and orphan-drop paths
    lost the title."""
    src = API_PY.read_text()
    block = _find_handler_block(src, "api_unmanage_item")
    # The enrich_item call site.
    nc_idx = block.index("_nc.enrich_item(")
    # The transaction-open site.
    txn_idx = block.index("with get_conn(db) as conn, transaction(conn):")
    assert nc_idx < txn_idx, (
        "v1.17.16: api_unmanage_item must call enrich_item "
        "BEFORE opening the transaction so the title lookup "
        "happens against live data."
    )
    # And the dispatch site at the bottom uses _notify_ctx.
    assert "_notify_ctx, action=\"unmanaged\"" in block, (
        "v1.17.16: api_unmanage_item dispatch must use the "
        "pre-captured _notify_ctx (not a fresh enrich_item)."
    )


def test_forget_captures_enrichment_before_txn():
    """api_forget_item — the one the user's screenshot caught.
    FORGET drops the orphan themes row, so post-op enrichment
    finds nothing."""
    src = API_PY.read_text()
    block = _find_handler_block(src, "api_forget_item")
    nc_idx = block.index("_nc.enrich_item(")
    txn_idx = block.index("with get_conn(db) as conn:")
    assert nc_idx < txn_idx, (
        "v1.17.16: api_forget_item must capture enrichment "
        "before opening the connection — the orphan themes row "
        "may be dropped inside the conn block, after which the "
        "lookup returns the bare-ID fallback (the user's "
        "'movie/-26' screenshot)."
    )
    assert "_notify_ctx, action=\"forgotten\"" in block


def test_delete_captures_enrichment_before_txn():
    """api_delete_item — always drops the themes row + FK
    children, so pre-op capture is mandatory."""
    src = API_PY.read_text()
    block = _find_handler_block(src, "api_delete_item")
    nc_idx = block.index("_nc.enrich_item(")
    txn_idx = block.index("with get_conn(db) as conn:")
    assert nc_idx < txn_idx, (
        "v1.17.16: api_delete_item must capture enrichment "
        "before opening the connection."
    )
    assert "_notify_ctx, action=\"deleted\"" in block


def test_no_fresh_enrich_item_in_dispatch_blocks():
    """Counter-pin: after v1.17.16, the dispatch blocks must
    NOT call enrich_item again — they must use _notify_ctx
    captured up top. If a future edit re-introduces a fresh
    enrich_item inside the dispatch block, the orphan-drop
    bug returns silently."""
    src = API_PY.read_text()
    for handler in ("api_unmanage_item", "api_forget_item",
                    "api_delete_item"):
        block = _find_handler_block(src, handler)
        # There must be exactly ONE enrich_item call per handler
        # (the pre-op capture). Zero or two-plus means a
        # regression somewhere.
        n = block.count("_nc.enrich_item(")
        assert n == 1, (
            f"v1.17.16: {handler} must have exactly one "
            f"_nc.enrich_item call (the pre-op capture). Found "
            f"{n}."
        )


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_16():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 16), (
        f"v1.17.16: __version__ must be >= 1.17.16 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
