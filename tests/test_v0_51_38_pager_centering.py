"""v0.51.38 — centre the // RESULTS action group (LEGEND / NEEDS WORK / pager) on
a phone (the user: "get last on the same row as the other options or if not do
centering similar to what we did in the filter section above ... same for legend
and needs work"). Mirrors the v0.51.27 filterbar treatment. All scoped to
@media (max-width: 600px).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _mobile_block() -> str:
    i = APP_CSS.index("@media (max-width: 600px) {")
    j = i + APP_CSS[i:].index("{")
    depth = 0
    for k in range(j, len(APP_CSS)):
        if APP_CSS[k] == "{":
            depth += 1
        elif APP_CSS[k] == "}":
            depth -= 1
            if depth == 0:
                return APP_CSS[j:k + 1]
    raise AssertionError("unterminated @media (max-width: 600px)")


MOBILE = _mobile_block()


def test_results_action_group_centres_on_mobile():
    # width:100% so justify-content centres the group ON SCREEN rather than inside
    # the left-hugging shrink box the space-between .block-head packs it into (the
    # same reason the v0.51.27 resolution chips needed width:100%).
    assert ".block-head-actions { flex-wrap: wrap; width: 100%; justify-content: center; }" in MOBILE
    # the pager centres its own buttons so a `last »` that can't share the row
    # centres as its own line instead of stranding left.
    assert ".pager { flex-wrap: wrap; justify-content: center; }" in MOBILE


def test_block_head_actions_is_library_only():
    # The centering is safe to apply unconditionally on mobile ONLY because
    # .block-head-actions is used by the library RESULTS header alone — the
    # LOGS / queue block-heads use a bare .block-head with no action group, so
    # this never centres their toggle chips. If a future page adopts the class,
    # this pin fails and forces a re-scope decision.
    templates = (REPO / "app" / "web" / "templates")
    users = [p.name for p in templates.glob("*.html")
             if "block-head-actions" in p.read_text()]
    assert users == ["library.html"], (
        f"block-head-actions leaked to {users} — re-scope the v0.51.38 "
        "mobile centering before it silently reflows another page's header")
