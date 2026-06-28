"""v1.18.98 — silent-fail audit: worker payload-parse breadcrumb.

Follow-up to v1.18.97's audit. The HIGH-severity finding shipped
in v1.18.97 (scanner stat() OSError). This tag closes the MED-
severity finding: 9 sites in worker.py had identical
`except (TypeError, ValueError): pass` swallows around
`json.loads(job["payload"] or "{}")`.

## Why it matters

Each swallow silently dropped a payload-derived flag:

  - _do_plex_enum: section_id, collections_only (sync runs
    unscoped → whole library instead of one section)
  - _do_sync: auto_place, enqueue_downloads (default behavior
    fires unexpectedly)
  - _do_download: user_initiated_mismatch (mismatch-flow logic
    silently inactive)
  - _record_local_file: user_initiated_mismatch (same)
  - _do_place (kind): falls back to default_placement_method
    instead of the explicitly-requested kind
  - _do_place (force): force_overwrite stays False (won't
    replace existing theme)
  - _do_place (clear_plex_api_rks): stranded Plex API entries
    not cleaned up
  - _do_place_collection (force): same as _do_place force
  - _do_place_collection (sidecar_cleanup): leftover sidecars
    not removed post-upload

Payload writes are owned by motif — we json.dumps everything
we INSERT — so a parse failure here suggests DB corruption, a
writer bug, or hand-edited jobs.payload. None should happen
silently.

## Fix

Module-level helper + warn-once flag per v1.17.11 hot-path
sub-pattern:

  _PAYLOAD_PARSE_WARNED: bool = False

  def _safe_payload_parse(raw, *, where) -> dict:
      try: parsed = json.loads(raw or "{}")
      except (TypeError, ValueError) as e:
          if not _PAYLOAD_PARSE_WARNED:
              log.warning(...)        # first occurrence
              _PAYLOAD_PARSE_WARNED = True
          else:
              log.debug(...)          # subsequent
          return {}
      return parsed if isinstance(parsed, dict) else {}

Each call site collapses from ~6 lines to 1:

  payload = _safe_payload_parse(job["payload"], where="_do_X")

The `where=` label identifies the source so the WARNING gives
operators enough context to find the corrupt job.

## What's pinned

- Helper exists at module scope with the warn-once flag
- All 9 call sites use the helper (no remaining bare
  except: pass on json.loads patterns in worker.py)
- First call with bad payload logs.warning + sets flag
- Subsequent calls log.debug (no docker-log drowning)
- Helper returns {} on parse failure (not None, not non-dict)
- Marker references v1.17.11 sub-pattern + class 9
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = REPO / "app" / "core" / "worker.py"


# ── Source pins ──────────────────────────────────────────────


def test_helper_defined_at_module_scope():
    """_safe_payload_parse must live at module scope (not nested)
    so it's importable + testable."""
    src = WORKER_PY.read_text()
    assert "def _safe_payload_parse(" in src
    # Must be top-level (no leading indent).
    assert "\ndef _safe_payload_parse(" in src, (
        "v1.18.98: _safe_payload_parse must be module-scope"
    )


def test_warn_once_flag_exists():
    """The module-level _PAYLOAD_PARSE_WARNED flag must exist —
    v1.17.11 hot-path sub-pattern. First occurrence (per call-site)
    warns, rest drop to debug so docker logs don't drown.
    v1.23.62 (audit #18): a SET keyed by call-site, not a single
    process-wide bool — so a corrupt payload in one handler no longer
    mutes the warning for every other handler."""
    src = WORKER_PY.read_text()
    assert "_PAYLOAD_PARSE_WARNED: set[str] = set()" in src, (
        "v1.23.62: warn-once flag must be a per-call-site set"
    )
    assert "if where not in _PAYLOAD_PARSE_WARNED:" in src
    assert "_PAYLOAD_PARSE_WARNED.add(where)" in src


def test_marker_references_class_9_and_v1_17_11():
    """The helper's docstring/comment block must reference both
    class 9 (silent-defensive-catch) and v1.17.11 (hot-path
    sub-pattern) for catalog discoverability."""
    src = WORKER_PY.read_text()
    # Anchor on the helper definition.
    idx = src.index("def _safe_payload_parse(")
    block = src[max(0, idx - 1500):idx + 1500]
    assert "v1.17.11" in block, (
        "v1.18.98: marker should reference v1.17.11 hot-path "
        "sub-pattern for catalog lineage"
    )
    assert "class-9" in block.lower() or "class 9" in block.lower(), (
        "v1.18.98: marker should reference CLAUDE.md class-9 "
        "silent-defensive-catch"
    )


def test_no_remaining_silent_json_loads_swallows():
    """The hostile pattern — `except (TypeError, ValueError): pass`
    on a `json.loads(job["payload"]...)` — must be GONE from
    worker.py. If a future patch re-introduces it, this test
    fires."""
    src = WORKER_PY.read_text()
    # Find all `json.loads(job[` or `json.loads(job_payload` patterns
    # and verify NONE of them are followed by a `pass`-only except.
    import re
    # Pattern: try-block containing json.loads(job(_payload)?[..]) +
    # except (TypeError, ValueError) ending in `pass`.
    bad = re.findall(
        r"try:\s*\n\s*[^\n]*json\.loads\([^)]*job(?:_payload|\[\"payload\"\])"
        r"[^\n]*\n[^\n]*\n\s*except \(TypeError, ValueError\):\s*\n\s*pass",
        src, flags=re.MULTILINE,
    )
    assert not bad, (
        f"v1.18.98: remaining silent json.loads swallow on "
        f"job payload — should use _safe_payload_parse helper. "
        f"Matches: {bad[:2]}"
    )


def test_all_call_sites_use_helper():
    """Every previously-silent site now uses _safe_payload_parse.
    Pin the call count so a future refactor doesn't accidentally
    re-introduce the bare json.loads pattern."""
    src = WORKER_PY.read_text()
    count = src.count("_safe_payload_parse(")
    # 1 definition + 9 call sites = 10 occurrences.
    assert count >= 10, (
        f"v1.18.98: expected at least 10 occurrences of "
        f"_safe_payload_parse (1 def + 9 calls); found {count}. "
        f"A site may have regressed to bare json.loads."
    )


def test_helper_call_sites_have_descriptive_where_labels():
    """Each call site's `where=` arg must carry a non-trivial
    label so the WARNING includes enough context to find the
    corrupt job. Pin to catch a future patch that copy-pastes
    a site with a stale or generic label."""
    src = WORKER_PY.read_text()
    import re
    # Match the call sites' where= kwarg values.
    labels = re.findall(
        r'_safe_payload_parse\([^)]*where="([^"]+)"',
        src,
    )
    # 9 expected call sites with where= labels.
    assert len(labels) >= 9, (
        f"v1.18.98: expected at least 9 labeled call sites; "
        f"found {len(labels)}: {labels}"
    )
    # Every label must reference a real method/context (no
    # placeholders, no generic strings).
    for label in labels:
        assert label.startswith("_do_") or label.startswith("_record_"), (
            f"v1.18.98: where= label '{label}' should name a "
            f"worker method (_do_X / _record_X) so the warning "
            f"identifies the source"
        )


# ── Behavioral ──────────────────────────────────────────────


def test_helper_returns_empty_dict_on_parse_failure(caplog):
    """Malformed JSON → returns {} + emits log.warning on first
    occurrence."""
    from app.core import worker
    # Reset the flag so this test always exercises the warn path.
    worker._PAYLOAD_PARSE_WARNED = set()
    caplog.set_level(logging.WARNING)
    result = worker._safe_payload_parse(
        '{"not valid', where="test_first",
    )
    assert result == {}
    matching = [
        rec for rec in caplog.records
        if rec.levelno >= logging.WARNING
        and "test_first" in rec.message
        and "payload" in rec.message
    ]
    assert matching, (
        f"v1.18.98: first parse failure must emit warning; got "
        f"records: {[(r.levelname, r.message[:80]) for r in caplog.records]}"
    )


def test_helper_warns_once_then_debugs(caplog):
    """Same call-site twice: first warns, second debugs (v1.17.11 drown-
    prevention). v1.23.62: keyed PER call-site, so a DIFFERENT site warns again
    instead of being muted by an unrelated earlier corruption."""
    from app.core import worker
    worker._PAYLOAD_PARSE_WARNED = set()
    caplog.set_level(logging.DEBUG)

    def _warns():
        return sum(1 for r in caplog.records
                   if r.levelno >= logging.WARNING and "payload" in r.message)

    # First hit at site A → warn.
    worker._safe_payload_parse("garbage1", where="site_a")
    assert _warns() == 1
    # Second hit at the SAME site A → debug, not warn.
    worker._safe_payload_parse("garbage2", where="site_a")
    assert _warns() == 1, "same call-site must not warn twice (drown-prevention)"
    assert [r for r in caplog.records
            if r.levelno == logging.DEBUG and "site_a" in r.message], (
        "v1.17.11: second occurrence at the same site must log.debug"
    )
    # A DIFFERENT call-site → warns again (the v1.23.62 fix — a corruption in one
    # handler no longer mutes another handler's first warning).
    worker._safe_payload_parse("garbage3", where="site_b")
    assert _warns() == 2, "v1.23.62: a different call-site gets its own first warning"


def test_helper_returns_dict_on_success():
    """Well-formed JSON dict → returns the parsed dict unchanged."""
    from app.core import worker
    result = worker._safe_payload_parse(
        '{"section_id": "1", "force": true}',
        where="test_success",
    )
    assert result == {"section_id": "1", "force": True}


def test_helper_empty_input_returns_empty_dict():
    """None / "" input → returns {} (defaults to no-payload
    behavior, matches the legacy `or '{}'` pattern)."""
    from app.core import worker
    assert worker._safe_payload_parse(None, where="empty1") == {}
    assert worker._safe_payload_parse("", where="empty2") == {}


def test_helper_non_dict_json_returns_empty_dict(caplog):
    """If the payload parses but isn't a dict (e.g. a list or a
    number from a writer bug), return {} so call sites can use
    .get() unconditionally. NOT a warn case — the JSON parsed
    fine, it just wasn't the expected shape."""
    from app.core import worker
    worker._PAYLOAD_PARSE_WARNED = set()
    caplog.set_level(logging.WARNING)
    # JSON list, parses fine but isn't a dict.
    assert worker._safe_payload_parse("[1, 2, 3]", where="t") == {}
    # JSON literal int.
    assert worker._safe_payload_parse("42", where="t") == {}
    # The non-dict case should NOT trigger the warn flag (it's
    # not a parse failure).
    assert worker._PAYLOAD_PARSE_WARNED == set()
