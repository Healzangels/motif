"""v1.18.79 — intent-aware WARNING breadcrumbs.

v1.18.77/.78 landed the user_overrides.intent foundation
(backup vs replace) and the info-card UI. This tag adds the
two log breadcrumbs that complete the silent-failure surface:

1. **Worker WARNING on intent='replace' + placement skipped.**
   When the place worker skips because plex_has_theme=1, and
   the row's user_override has intent='replace', the user
   EXPLICITLY chose to replace Plex's theme and motif silently
   refused. Pre-v1.18.79 this fired at INFO indistinguishably
   from the legitimate intent='backup' skip. Now: WARNING +
   structured note pointing the operator at the // PROMOTE
   TO ACTIVE / // MARK AS BACKUP actions.

2. **plex_enum WARNING on has_theme 1→0 transition for a
   backup-intent row.** Plex stopped serving its own theme;
   the user's backup is now the only candidate. Logged as
   'backup_ready_to_deploy' so the operator can `docker logs
   motif | grep backup_ready` and see which rows are eligible
   to PROMOTE.

v1.18.80 will add the Apprise notification dispatch on the
same plex_enum transition; v1.18.79's log is the foundation
so we can verify the detection logic in production before
adding user-facing notifications.

## Deferred

- UPLOAD MP3 intent parity (UPLOAD MP3 writes local_files,
  not user_overrides — needs a parallel mechanism).
- Apprise notification on backup_ready_to_deploy (v1.18.80
  adds the notify.py event_kind, notify_content formatter,
  config_file default).
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Worker WARNING on intent='replace' + skipped placement ──


def _do_place_body() -> str:
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_idx = src.index("    def _do_place(self,")
    fn_end = src.index("    def _do_place_collection(", fn_idx + 1)
    return src[fn_idx:fn_end]


def test_worker_reads_override_intent_on_plex_has_theme_skip():
    """When the place outcome reason is 'plex_has_theme' (skipped
    because Plex has its own theme), the worker must read the
    row's user_overrides.intent to decide whether to log at
    INFO (backup-intent — expected outcome) or WARNING
    (replace-intent — user's intent silently denied).

    Anchor on the v1.18.79-specific `_intent_for_log` local
    (unique to this block) — the bare `outcome.reason ==
    "plex_has_theme"` substring appears earlier at line ~2177
    in the v1.18.0 placement skip-reason sweep."""
    body = _do_place_body()
    intent_idx = body.index("_intent_for_log: str | None = None")
    block = body[intent_idx:intent_idx + 3000]
    # The skip-condition test inside the v1.18.79 block.
    assert 'outcome.reason == "plex_has_theme"' in block
    assert "SELECT intent FROM user_overrides" in block, (
        "v1.18.79: worker must read user_overrides.intent on "
        "plex_has_theme skip"
    )
    # Per-section first, '' global fallback (mirrors the
    # established pattern).
    assert "section_id = ?" in block
    assert "section_id = ''" in block


def test_worker_upgrades_to_warning_on_intent_replace_skip():
    """`_place_log_level` must flip to 'WARNING' when the skip
    is plex_has_theme AND the row's intent is 'replace'.
    Otherwise stays 'INFO' (the legitimate backup-intent skip)."""
    body = _do_place_body()
    assert '_place_log_level = "INFO"' in body
    assert '_place_log_level = "WARNING"' in body
    # And the upgrade conditional checks intent='replace'.
    body_flat = re.sub(r"\s+", " ", body)
    assert (
        'if _intent_for_log == "replace": _place_log_level = "WARNING"'
        in body_flat
        or 'if (_intent_for_log == "replace"): _place_log_level = "WARNING"'
        in body_flat
    ), (
        "v1.18.79: WARNING upgrade only fires on intent='replace'"
    )


def test_worker_warning_message_points_at_recovery_buttons():
    """The WARNING message must name the // PROMOTE TO ACTIVE
    and // MARK AS BACKUP actions so the operator reading the
    log knows the resolution path. Pre-v1.18.79 the user had
    no signal that anything was wrong."""
    body = _do_place_body()
    body_flat = re.sub(r'"\s*"', "", body)
    assert "PROMOTE TO ACTIVE" in body_flat
    assert "MARK AS BACKUP" in body_flat


def test_worker_intent_lookup_is_class_9_safe():
    """The intent lookup must wrap the DB call in try/except +
    log.debug on failure. v1.17.11's hot-path pattern: a
    breadcrumb fix shouldn't itself become a silent class-9
    trap if the new code path raises."""
    body = _do_place_body()
    body_flat = re.sub(r"\s+", " ", body)
    # The intent-lookup block has its own try/except.
    intent_idx = body_flat.index("SELECT intent FROM user_overrides")
    pre_intent = body_flat[max(0, intent_idx - 200):intent_idx]
    # `try:` precedes the SELECT.
    assert "try:" in pre_intent
    # The except wraps the entire lookup + logs at debug.
    assert "log.debug(" in body_flat[intent_idx:intent_idx + 1500]


def test_worker_detail_payload_includes_override_intent():
    """When the WARNING fires, the log_event detail must include
    `override_intent` so downstream analytics (event-log filter,
    /logs panel rendering, future grafana boards) can target
    the affected population."""
    body = _do_place_body()
    body_flat = re.sub(r"\s+", " ", body)
    # The detail dict gets the field via assignment syntax, not
    # dict-literal: `_place_detail["override_intent"] = _intent_for_log`.
    assert (
        '_place_detail["override_intent"] = _intent_for_log'
        in body_flat
    )


def test_worker_v1_18_79_marker_present():
    """v1.18.79 marker required so a future refactor knows WHY
    the worker reads intent on plex_has_theme skips."""
    body = _do_place_body()
    assert "v1.18.79" in body
    body_flat = " ".join(body.split())
    assert ("Class-9" in body_flat or "class-9" in body_flat.lower()
            or "intent denied" in body_flat
            or "silently refused" in body_flat), (
        "v1.18.79: marker should reference class-9 silent-failure "
        "pattern or the 'intent denied' framing"
    )


# ── plex_enum WARNING on has_theme 1→0 transition ───────────


def _plex_enum_text() -> str:
    return (REPO / "app" / "core" / "plex_enum.py").read_text()


def test_plex_enum_fetches_prior_has_theme():
    """The plex_items SELECT must include has_theme (alongside
    plex_theme_uri) so the upsert logic can compare prior to
    new. Pre-v1.18.79 only plex_theme_uri was read."""
    src = _plex_enum_text()
    # The query that grabs prior state.
    assert "SELECT plex_theme_uri, has_theme" in src, (
        "v1.18.79: must SELECT has_theme alongside plex_theme_uri "
        "so the 1→0 transition can be detected"
    )
    # And guid_tmdb + media_type so we can look up user_overrides
    # without a second query.
    assert "guid_tmdb" in src
    assert "media_type" in src


def test_plex_enum_detects_has_theme_one_to_zero():
    """The transition detection: prior=true + new=false. Pin
    the condition shape so a future refactor can't accidentally
    invert it."""
    src = _plex_enum_text()
    src_flat = re.sub(r"\s+", " ", src)
    assert (
        "if _prior_has_theme and not it.has_theme:"
        in src_flat
    ), (
        "v1.18.79: transition condition must be prior=1 AND new=0"
    )


def test_plex_enum_filters_to_backup_intent_rows():
    """The WARNING fires only when the affected row has a
    user_override with intent='backup'. Other rows' transitions
    aren't actionable (no backup to deploy).

    v1.19.41: widened slice 2000 → 4500 chars. The v1.19.41
    Bug-A fallback (resolve tmdb via theme_id when guid_tmdb
    IS NULL) + Bug-C transition INFO breadcrumb added ~1500
    chars BETWEEN the transition check and the
    `intent = 'backup'` SQL."""
    src = _plex_enum_text()
    transition_idx = src.index("_prior_has_theme and not it.has_theme")
    block = src[transition_idx:transition_idx + 4500]
    assert "intent = 'backup'" in block
    # Per-section first ordering.
    assert "ORDER BY (section_id = ?)" in block, (
        "v1.18.79: prefer the section-specific override row "
        "over the '' global fallback when both exist"
    )


def test_plex_enum_warning_uses_backup_ready_to_deploy_keyword():
    """The WARNING message must contain the exact string
    'backup_ready_to_deploy' so operators can grep for it.
    Same string will be the event_kind in v1.18.80's
    notification dispatch."""
    src = _plex_enum_text()
    assert "backup_ready_to_deploy" in src


def test_plex_enum_warning_names_recovery_action():
    """Operator reading the WARNING in docker logs needs to know
    the resolution path — name // PROMOTE TO ACTIVE explicitly."""
    src = _plex_enum_text()
    src_flat = re.sub(r'"\s*"', "", src)
    assert "PROMOTE TO ACTIVE" in src_flat


def test_plex_enum_handles_media_type_translation():
    """plex_items.media_type uses Plex's strings ('show',
    'collection', 'movie') — user_overrides uses motif's
    ('tv', 'collection', 'movie'). The lookup must translate
    'show' → 'tv' before matching."""
    src = _plex_enum_text()
    transition_idx = src.index("_prior_has_theme and not it.has_theme")
    block = src[transition_idx:transition_idx + 2000]
    # The translation check.
    src_flat = re.sub(r"\s+", " ", block)
    assert (
        'existing["media_type"] == "show"' in src_flat
        and '"tv"' in src_flat
    ), (
        "v1.18.79: must translate plex's 'show' → motif's 'tv' "
        "for the user_overrides lookup"
    )


def test_plex_enum_v1_18_79_marker_explains_breadcrumb_only_scope():
    """v1.18.79 marker should explicitly say this is breadcrumb-
    only — v1.18.80 will add the Apprise dispatch. Documents
    the staged rollout."""
    src = _plex_enum_text()
    assert "v1.18.79" in src
    src_flat = " ".join(src.split())
    assert ("v1.18.80" in src_flat or "breadcrumb" in src_flat.lower()), (
        "v1.18.79: marker should reference the v1.18.80 dispatch "
        "follow-up so future authors know the staging plan"
    )
