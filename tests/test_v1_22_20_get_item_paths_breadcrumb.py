"""v1.22.20 — get_item_paths logs a breadcrumb on a non-200 Plex response.

Audit class-9 (silent-defensive-catch). PlexClient.get_item_paths backs the
placement pipeline's media-folder lookup. Its early return on a non-200 status
(`if r is None or r.status_code != 200: return []`) was SILENT — a Plex 404 /
500 / auth response lost the media folder with no breadcrumb, so a placement
quietly skipped the item. The adjacent malformed-JSON branch was already given a
WARNING by the v1.17.9 hygiene audit; _get only logs TRANSPORT errors (it
returns None + WARNING), never HTTP error STATUSES. This was the last silent
[]-return in the folder lookup. Now a non-200 logs WARNING before returning [].
"""
from __future__ import annotations

import logging
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
PLEX_PY = (REPO / "app" / "core" / "plex.py").read_text()


def _plex():
    from app.core.plex import PlexClient, PlexConfig
    cfg = PlexConfig(
        url="http://plex.test:32400", token="testtoken",
        movie_section="1", tv_section="2", enabled=True,
    )
    return PlexClient(cfg)


class _Resp:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


def test_non_200_logs_warning_and_returns_empty(monkeypatch, caplog):
    plex = _plex()
    monkeypatch.setattr(plex, "_get", lambda path: _Resp(404))
    with caplog.at_level(logging.WARNING, logger="app.core.plex"):
        out = plex.get_item_paths("rk-404")
    assert out == []
    assert any(
        "get_item_paths" in r.message and "404" in r.message
        for r in caplog.records
    ), "v1.22.20: a non-200 must log a WARNING breadcrumb (got: %r)" % (
        [r.message for r in caplog.records],
    )


def test_transport_none_does_not_double_log(monkeypatch, caplog):
    """When _get returns None it ALREADY logged the transport failure — the
    get_item_paths None branch must NOT add a second (misleading) warning."""
    plex = _plex()
    monkeypatch.setattr(plex, "_get", lambda path: None)
    with caplog.at_level(logging.WARNING, logger="app.core.plex"):
        out = plex.get_item_paths("rk-none")
    assert out == []
    assert not any("get_item_paths" in r.message for r in caplog.records), (
        "v1.22.20: the None branch must stay quiet (_get already warned)"
    )


def test_200_still_parses_paths(monkeypatch):
    """Regression: a healthy 200 still returns the parsed file paths."""
    plex = _plex()
    body = (
        '{"MediaContainer":{"Metadata":[{"Media":[{"Part":'
        '[{"file":"/data/m/Movie/movie.mkv"}]}]}]}}'
    )
    monkeypatch.setattr(plex, "_get", lambda path: _Resp(200, body))
    out = plex.get_item_paths("rk-200")
    assert out == ["/data/m/Movie/movie.mkv"]


def test_source_pin_non_200_branch_warns():
    i = PLEX_PY.index("def get_item_paths(")
    j = PLEX_PY.index("\n    def ", i + 10)
    body = PLEX_PY[i:j]
    assert "if r.status_code != 200:" in body
    seg = body[body.index("if r.status_code != 200:"):]
    assert "log.warning(" in seg[:800]
    assert "v1.22.20" in body
