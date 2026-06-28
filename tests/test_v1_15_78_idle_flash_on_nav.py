"""v1.15.78 — kill the IDLE flash on cross-tab nav while an op runs.

the user: "when clicking between tabs and a job is running in the
status bar idle is briefly displayed before disappearing if we
can make it so it doesn't would look cleaner."

Root cause: v1.15.55 SSR-baked the #op-mini bar's visibility so
it persisted across navs (gated on `_ssr_top.op_running`). But
the sibling #op-status-idle pill was always rendered unhidden —
SSR didn't mirror the gate. Result: when an op was running:

  Frame 1 (SSR paint): #op-mini visible + #op-status-idle visible
  Frame 2 (JS reconciles): JS sets idle.hidden = true

The flash was that one-frame gap. Fix: SSR now hides
#op-status-idle when `_ssr_top.op_running` is true, mirroring
the v1.15.55 op-mini gate so the initial paint matches the
JS-reconciled state.

JS still owns live updates — when an op finishes the next
/api/progress poll calls `idle.hidden = false`. SSR just
provides a non-flashing starting frame for the running case.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"
API_PY = REPO / "app" / "web" / "api.py"


def test_idle_pill_ssr_hidden_when_op_running():
    """The #op-status-idle anchor must carry a Jinja
    `{% if _ssr_top.op_running %}hidden{% endif %}` directive so
    its initial visibility matches the JS-reconciled state."""
    html = BASE_HTML.read_text()
    pos = html.index('id="op-status-idle"')
    # Walk back to the <a tag.
    tag_start = html.rfind("<a ", 0, pos)
    tag_end = html.index(">", pos)
    tag = html[tag_start:tag_end + 1]
    assert "_ssr_top.op_running" in tag, (
        "v1.15.78: #op-status-idle must check _ssr_top.op_running "
        "in its open tag so SSR renders it hidden when an op is "
        "running — otherwise the user sees a one-frame IDLE flash "
        "on every cross-tab nav while the op is in flight"
    )
    assert "hidden" in tag, (
        "v1.15.78: the conditional must render the `hidden` "
        "attribute when op_running is true"
    )


def test_idle_pill_ssr_mirror_matches_op_mini_pattern():
    """The IDLE pill's SSR gate must be the inverse of #op-mini's
    SSR gate. v1.15.55's op-mini hides itself when NOT op_running;
    v1.15.78's IDLE hides itself WHEN op_running. The two gates
    must always disagree so exactly one pill is visible at SSR-
    paint time."""
    html = BASE_HTML.read_text()
    # op-mini gate.
    mini_idx = html.index('id="op-mini"')
    mini_tag_end = html.index(">", mini_idx)
    mini_tag = html[html.rfind("<a ", 0, mini_idx):mini_tag_end + 1]
    # IDLE gate.
    idle_idx = html.index('id="op-status-idle"')
    idle_tag_end = html.index(">", idle_idx)
    idle_tag = html[html.rfind("<a ", 0, idle_idx):idle_tag_end + 1]
    assert "not _ssr_top.op_running" in mini_tag, (
        "Precondition: #op-mini must hide when NOT op_running "
        "(v1.15.55 baseline)"
    )
    # IDLE must hide when op_running (the inverse).
    # Accept either `_ssr_top.op_running` alone (truthy) or
    # `if _ssr_top.op_running` — the meaningful check is that
    # the condition does NOT carry a `not` for op_running.
    assert "not _ssr_top.op_running" not in idle_tag, (
        "v1.15.78: #op-status-idle gate must be the INVERSE of "
        "#op-mini's (hide when op_running, not when not). "
        "Otherwise both pills paint visible together and the "
        "v1.15.78 fix doesn't actually fix the flash."
    )


def test_topbar_ssr_state_returns_op_running_flag():
    """Precondition for the gate: the SSR endpoint must populate
    `op_running` from op_progress. Pinning so a future refactor
    of _topbar_ssr_state doesn't quietly drop the field and
    re-introduce the flash."""
    src = API_PY.read_text()
    func_start = src.index("def _topbar_ssr_state(")
    # Anchor on the next top-level def to bound the slice.
    func_end = src.index("def _dashboard_ssr_state(", func_start)
    body = src[func_start:func_end]
    assert '"op_running":' in body, (
        "v1.15.78: _topbar_ssr_state must initialize op_running "
        "in the state dict so base.html can branch on it"
    )
    assert 'state["op_running"] = True' in body, (
        "v1.15.78: _topbar_ssr_state must set op_running=True "
        "when op_progress has a running row — otherwise SSR "
        "always renders IDLE-visible"
    )
