"""v1.21.39 — UNPLACE inline-verify uses the TRISTATE verify_theme_claim.

Silent-failure audit finding H2: api_unplace_item's inline HEAD-verify
branched on True/False/None (a tristate) but called plex.item_has_theme,
which is `-> bool` — it collapses a transient error (network/timeout/5xx)
to False. So a Plex hiccup mid-unplace wrote has_theme=0/verified_ok=0 on
a row that may still serve a theme; and since _verify_theme_claims only
re-checks has_theme=1 rows, the row stayed falsely themeless (SRC off P)
until the next FULL plex_enum. The `# None → transient` branch was dead.

Fix: call the purpose-built tristate verify_theme_claim (True=200,
False=404, None=transient) so a transient leaves verified_ok=NULL.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _plex():
    from app.core.plex import PlexClient, PlexConfig
    cfg = PlexConfig(
        url="http://plex.test:32400", token="testtoken",
        movie_section="1", tv_section="2", enabled=True,
    )
    return PlexClient(cfg)


# ── the contract difference the fix hinges on ────────────────


def test_transient_diverges_item_has_theme_vs_verify_claim(monkeypatch):
    """On a transient (status None, metadata GET None), item_has_theme
    returns False (would wrongly zero the row) while verify_theme_claim
    returns None (leaves verified_ok=NULL — the safe outcome)."""
    plex = _plex()
    monkeypatch.setattr(plex, "_head_or_get_status", lambda path: None)
    monkeypatch.setattr(plex, "_get", lambda path: None)
    assert plex.item_has_theme("rk1") is False, (
        "item_has_theme collapses a transient to False — the bug")
    assert plex.verify_theme_claim("rk1") is None, (
        "verify_theme_claim returns None on a transient — the safe value")


def test_verify_claim_tristate_200_404(monkeypatch):
    plex = _plex()
    monkeypatch.setattr(plex, "_head_or_get_status", lambda path: 200)
    assert plex.verify_theme_claim("rk") is True
    monkeypatch.setattr(plex, "_head_or_get_status", lambda path: 404)
    assert plex.verify_theme_claim("rk") is False


# ── the unplace call site uses the tristate + handles None ───


def test_unplace_inline_verify_uses_verify_theme_claim():
    fn = API_PY[API_PY.index("async def api_unplace_item("):]
    fn = fn[:fn.index("\n    @app.", 1)]
    # v1.22.58: offloaded via run_in_threadpool (event-loop lint) — the
    # tristate method now rides as an argument to the threadpool call.
    assert "verified = await run_in_threadpool(" in fn, (
        "v1.21.39/v1.22.58: unplace inline-verify must use the tristate "
        "verify_theme_claim, offloaded via run_in_threadpool")
    assert "plex.verify_theme_claim, rk)" in fn, (
        "v1.21.39: the verify must be the tristate verify_theme_claim")
    assert "plex.item_has_theme" not in fn, (
        "the bool item_has_theme must no longer be used here")
    # All three tristate branches present, incl. the now-live None path.
    assert "if verified is True:" in fn
    assert "elif verified is False:" in fn
    assert "None → transient; verified_ok stays NULL" in fn
