"""v1.21.76 — edition DISPLAY-label chokepoint + regex de-dup.

Code-review (v1.21.75) altitude finding: enrich_item inlined a third copy of
the `{edition-X}` parse regex (api_item's edition_pattern + app.js
parseEditionFromFolderPath being the other two). The RAW proper-case display
label now lives in the editions.py chokepoint as edition_label_for_folder /
edition_label_for_basename, with prettify_edition_key as the no-folder
fallback; enrich_item + api_item route through it.
"""
from __future__ import annotations

from pathlib import Path

from app.core.editions import (
    edition_label_for_basename, edition_label_for_folder,
    edition_key_for_folder, prettify_edition_key,
)


REPO = Path(__file__).resolve().parent.parent


# ── raw display labels ───────────────────────────────────────────

def test_label_preserves_folder_casing():
    assert edition_label_for_folder(
        "/data/Movies/X (2009) {edition-Extended Edition}") == "Extended Edition"
    assert edition_label_for_folder(
        "/data/Movies/X (2001) {edition-Sam Takes a Step}") == "Sam Takes a Step"
    assert edition_label_for_folder(
        "/data/Movies/Y (2009) {edition-Director's Cut}") == "Director's Cut"
    # RAW case is preserved verbatim — a lowercase folder tag stays lowercase
    # (the label is for display; only edition_key_for_* normalizes).
    assert edition_label_for_folder(
        "/data/Movies/X (2001) {edition-sam takes a step}") == "sam takes a step"


def test_label_empty_for_untagged_or_none():
    assert edition_label_for_folder("/data/Movies/Z (2001)") == ""
    assert edition_label_for_folder("") == ""
    assert edition_label_for_folder(None) == ""


def test_label_joins_multiple_edition_tags():
    assert edition_label_for_basename(
        "X (2008) {edition-Theatrical} {edition-IMAX}") == "Theatrical · IMAX"


def test_label_drops_guid_hint_tags():
    # {imdb-…} is a Plex GUID hint, NOT an edition — parse_folder_name drops it.
    assert edition_label_for_basename("X (2008) {imdb-tt1234567}") == ""


# ── pretty fallback (no folder match) ────────────────────────────

def test_prettify_keeps_small_words_lower():
    assert prettify_edition_key("sam takes a step") == "Sam Takes a Step"
    assert prettify_edition_key("extended edition") == "Extended Edition"
    assert prettify_edition_key("theatrical") == "Theatrical"
    # First word is always capitalized even if it's a small word.
    assert prettify_edition_key("the final cut") == "The Final Cut"


# ── parity with the state-key derivation ─────────────────────────

def test_label_and_key_agree_on_presence():
    """A folder has a display label iff it has a normalized edition_key —
    the two derivations must never disagree on whether an edition exists."""
    for folder in (
        "/m/X (2009) {edition-Extended Edition}",
        "/m/X (2001) {edition-Sam Takes a Step}",
        "/m/Z (2001)",
        "",
    ):
        assert bool(edition_label_for_folder(folder)) == \
            bool(edition_key_for_folder(folder)), folder


# ── de-dup guard: the inline regex must not come back ────────────

def test_no_inline_edition_regex_in_notify_content():
    src = (REPO / "app" / "core" / "notify_content.py").read_text()
    # Guard the regex string itself (comments may mention {edition-X}).
    assert r"\{edition-" not in src, (
        "notify_content.py must route edition-label parsing through "
        "editions.edition_label_for_folder, not an inline {edition-X} regex")
    assert "edition_label_for_folder" in src


def test_api_item_uses_chokepoint_helper():
    src = (REPO / "app" / "web" / "api.py").read_text()
    # The api_item INFO-card block uses the helper, not a local re.compile.
    assert "edition_label_for_folder" in src
    assert 'edition_pattern = _re.compile' not in src, (
        "api_item must use edition_label_for_folder, not its own "
        "edition_pattern regex (the 2nd Python copy)")
