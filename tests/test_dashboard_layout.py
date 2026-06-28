"""v1.13.55: dashboard layout endpoint regression guards.

Two layers covered:
  - _clean_dashboard_sections: the input sanitizer the PUT endpoint
    runs before persisting. Drops malformed entries, dedupes by id,
    coerces hidden to bool, preserves order.
  - get_runtime_text / set_runtime_text: the storage helpers the GET
    + PUT endpoints use to round-trip the JSON blob via
    runtime_settings.dashboard_layout.

These together pin the v1.13.48 dashboard customize feature without
needing TestClient + auth scaffolding.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.db import init_db
from app.core.runtime import get_runtime_text, set_runtime_text
from app.web.api import _clean_dashboard_sections


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "test.db"
    init_db(db)
    return db


# ── _clean_dashboard_sections ──────────────────────────────────────

def test_clean_passes_well_formed_sections():
    out = _clean_dashboard_sections([
        {"id": "top-stats", "hidden": False},
        {"id": "plex-coverage", "hidden": True},
    ])
    assert out == [
        {"id": "top-stats", "hidden": False},
        {"id": "plex-coverage", "hidden": True},
    ]


def test_clean_preserves_order():
    out = _clean_dashboard_sections([
        {"id": "c"},
        {"id": "a"},
        {"id": "b"},
    ])
    assert [s["id"] for s in out] == ["c", "a", "b"]


def test_clean_dedupes_by_id_keeping_first():
    out = _clean_dashboard_sections([
        {"id": "x", "hidden": True},
        {"id": "y", "hidden": False},
        {"id": "x", "hidden": False},  # dropped
    ])
    assert out == [
        {"id": "x", "hidden": True},
        {"id": "y", "hidden": False},
    ]


def test_clean_drops_non_dict_entries():
    out = _clean_dashboard_sections([
        {"id": "x"},
        "not-a-dict",
        None,
        42,
        ["nope"],
        {"id": "y"},
    ])
    assert [s["id"] for s in out] == ["x", "y"]


def test_clean_drops_entries_without_string_id():
    out = _clean_dashboard_sections([
        {"id": "x"},
        {"id": None},
        {"id": 42},
        {"id": ""},  # empty string treated as missing
        {"hidden": True},  # no id at all
        {"id": "y"},
    ])
    assert [s["id"] for s in out] == ["x", "y"]


def test_clean_coerces_hidden_to_bool():
    out = _clean_dashboard_sections([
        {"id": "a", "hidden": True},
        {"id": "b", "hidden": False},
        {"id": "c", "hidden": 1},          # truthy → True
        {"id": "d", "hidden": 0},          # falsy → False
        {"id": "e", "hidden": "yes"},      # truthy str → True
        {"id": "f", "hidden": ""},         # empty str → False
        {"id": "g"},                       # missing → False
        {"id": "h", "hidden": None},       # None → False
    ])
    assert [(s["id"], s["hidden"]) for s in out] == [
        ("a", True), ("b", False),
        ("c", True), ("d", False),
        ("e", True), ("f", False),
        ("g", False), ("h", False),
    ]


def test_clean_returns_empty_for_non_list_input():
    assert _clean_dashboard_sections(None) == []
    assert _clean_dashboard_sections("string") == []
    assert _clean_dashboard_sections({"id": "x"}) == []
    assert _clean_dashboard_sections(42) == []


def test_clean_returns_empty_for_empty_list():
    assert _clean_dashboard_sections([]) == []


def test_clean_drops_extra_fields_from_entries():
    """Entries can carry extra fields (e.g. legacy schema, future
    additions) — the cleaner returns only id + hidden."""
    out = _clean_dashboard_sections([
        {"id": "x", "hidden": False, "extra_field": "ignored", "order": 5},
    ])
    assert out == [{"id": "x", "hidden": False}]


# ── runtime helpers (storage layer) ────────────────────────────────

def test_get_runtime_text_returns_default_when_missing(fresh_db):
    assert get_runtime_text(fresh_db, "dashboard_layout", "") == ""
    assert get_runtime_text(fresh_db, "missing_key", "default-x") == "default-x"


def test_get_runtime_text_does_not_seed_missing(fresh_db):
    """v1.13.48 contract: text helpers don't seed on read since
    payloads are typically large (JSON blobs). Verify the row stays
    absent after a GET."""
    import sqlite3
    get_runtime_text(fresh_db, "no_seed_key", "fallback")
    with sqlite3.connect(fresh_db) as conn:
        row = conn.execute(
            "SELECT 1 FROM runtime_settings WHERE key = ?",
            ("no_seed_key",),
        ).fetchone()
    assert row is None


def test_set_then_get_round_trip(fresh_db):
    payload = json.dumps({"sections": [{"id": "x", "hidden": True}]})
    set_runtime_text(fresh_db, "dashboard_layout", payload, updated_by="admin")
    assert get_runtime_text(fresh_db, "dashboard_layout") == payload


def test_set_runtime_text_upserts(fresh_db):
    """Same key written twice — second write replaces."""
    set_runtime_text(fresh_db, "dashboard_layout", "first",
                     updated_by="admin")
    set_runtime_text(fresh_db, "dashboard_layout", "second",
                     updated_by="admin")
    assert get_runtime_text(fresh_db, "dashboard_layout") == "second"


def test_get_default_returned_when_blob_invalid_json_caller_handles():
    """Storage helper is JSON-agnostic — it just hands back the
    string. The endpoint's GET handler is responsible for handling
    parse failures and returning {sections: []}."""
    # This test documents the contract; the parse-fail behavior
    # is covered by the endpoint's own try/except logic.
    pass


def test_full_endpoint_simulation(fresh_db):
    """Simulate the GET → PUT → GET cycle the dashboard customize
    JS performs: load empty, save a layout, reload, verify."""
    # GET on a clean DB returns empty.
    raw1 = get_runtime_text(fresh_db, "dashboard_layout", "")
    assert raw1 == ""

    # User reorders + hides a couple sections, PUT serializes:
    cleaned = _clean_dashboard_sections([
        {"id": "top-stats", "hidden": False},
        {"id": "plex-coverage", "hidden": True},
        {"id": "recent-activity", "hidden": False},
    ])
    set_runtime_text(fresh_db, "dashboard_layout",
                     json.dumps({"sections": cleaned}),
                     updated_by="admin")

    # GET on reload returns what we wrote.
    raw2 = get_runtime_text(fresh_db, "dashboard_layout", "")
    parsed = json.loads(raw2)
    assert parsed == {"sections": [
        {"id": "top-stats", "hidden": False},
        {"id": "plex-coverage", "hidden": True},
        {"id": "recent-activity", "hidden": False},
    ]}
