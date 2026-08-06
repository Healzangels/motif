"""v0.51.251 — remove the dead /override endpoint pair + has_year, mark
saved_reason forensic-only.

The /api/items/{mt}/{id}/override POST+DELETE pair had no UI caller since the
v1.19.87 dialog removal and no API-token caller (zero live tokens on the
operator install), yet it kept absorbing contract fixes while dead (v1.21.89
edition scope, v1.22.31 txn hardening) — and the DELETE's edition fan-out
would have deleted sibling editions' overrides for any token holder who found
it. The live flows are manual-url + clear-url; this pins the pair's removal
by ROUTE INTROSPECTION, not source grep, because the removal breadcrumb in
api.py legitimately names the path (the comment-trap class — six instances
this session).

ParsedFolder.has_year was write-only convenience with one mirror assert as
its only reader. saved_reason is real data with zero readers — kept (it is
forensic history the operator may query by hand) but its schema lines must
SAY nothing reads it, so nobody builds on it believing restore honors it.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _routes(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.db import init_db
    from app.core.auth import init_auth_schema
    from app.web.api import create_app
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    app = create_app(s)
    return [(r.path, tuple(sorted(getattr(r, "methods", ()) or ())))
            for r in app.routes]


def test_the_override_route_pair_is_gone(tmp_path, monkeypatch):
    """No registered route may end in }/override — POST or DELETE. Route
    introspection so the api.py removal breadcrumb can't satisfy or defeat
    this by naming the path in prose."""
    left = [(p, m) for p, m in _routes(tmp_path, monkeypatch)
            if p.endswith("/override")]
    assert not left, (
        f"the dead /override endpoint pair is back: {left} — it was removed "
        "in v0.51.251 (no caller; DELETE fan-out was a cross-edition "
        "override wipe for any token holder)")


def test_the_live_flows_survive(tmp_path, monkeypatch):
    """The removal argument rests on manual-url + clear-url being the live
    replacements — so their disappearance must fail THIS test, not just the
    UI."""
    routes = _routes(tmp_path, monkeypatch)
    assert ("/api/items/{media_type}/{tmdb_id}/clear-url", ("POST",)) in routes
    assert ("/api/plex_items/{rating_key}/manual-url", ("POST",)) in routes


def test_has_year_is_gone_everywhere():
    """Removed with its sole reader (a mirror assert on the same literal —
    the pin-tests-that-mirror class). Comment-stripped so a rationale
    comment can't resurrect the name."""
    offenders = []
    for f in list((REPO / "app").rglob("*.py")) + list((REPO / "tests").glob("*.py")):
        if f.name == Path(__file__).name:
            continue
        live = "\n".join(ln for ln in f.read_text().splitlines()
                         if not ln.lstrip().startswith("#"))
        if "has_year" in live:
            offenders.append(str(f.relative_to(REPO)))
    assert not offenders, f"has_year resurrected in: {offenders}"


def test_saved_reason_is_marked_forensic_only():
    """Both schema sites (base CREATE + the migration copy) must carry the
    forensic-only annotation. The column stays — it's real audit data — but
    a reader must learn at the declaration that NOTHING consumes it and
    restore ignores it."""
    src = (REPO / "app" / "core" / "db.py").read_text()
    marked = [ln for ln in src.splitlines()
              if "saved_reason" in ln and "forensic-only" in ln]
    assert len(marked) == 2, (
        f"expected both saved_reason schema lines annotated forensic-only, "
        f"found {len(marked)} — if a reader was ADDED, delete the annotation "
        "instead of this test")
