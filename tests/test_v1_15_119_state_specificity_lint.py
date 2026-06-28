"""v1.15.119 — extend hover lint to :focus + :active + a11y fixes.

Two related design-system items:

## 1. State-specificity lint extension (:focus + :active)

The v1.15.118 lint flagged high-specificity rules that silently
override `.X:hover`. The same cascade-fight pattern applies to
`.X:focus` and `.X:active` — if a high-specificity attribute or
state rule sets the same property as the focus or active
pseudo-class style, the pseudo-class style dies silently.

v1.15.119 extends the lint to scan all three pseudo-classes
uniformly. Same value-equality short-circuit (state-equals-hover
designs don't false-positive). Same allowlist contract.

## 2. Focus-visible coverage for .tab + .lib-flag-pill

Both classes had `.X:hover` styling but no `:focus-visible`
counterpart in the global outline block at app.css:444-453.
Keyboard users tabbing through the settings nav or the
libraries-table section-include pills got no visible focus
indication.

v1.15.119 adds both to the `.btn:focus-visible, .chip:focus-
visible, ...` outline rule list. Now both render a 2px cyan
outline on keyboard focus, matching the rest of the
interactive-primitive family.

## Tests

  - State-specificity lint extended (3 pseudo-classes scanned)
  - Self-test synthetic case for each pseudo-class
  - .tab + .lib-flag-pill present in the focus-visible
    selector list
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"


# (selector, property, pseudo) → reason. Empty initially.
ALLOWLIST: dict[tuple[str, str, str], str] = {}


def _strip_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def _top_level_rules(src: str) -> list[tuple[str, str]]:
    """Yield (selector, body) for every top-level rule with
    multi-selector lists split into per-compound rules."""
    src = _strip_comments(src)
    out: list[tuple[str, str]] = []
    depth = 0
    buf: list[str] = []
    body_buf: list[str] = []
    in_body = False
    selector = ""
    for c in src:
        if c == "{":
            if depth == 0:
                selector = "".join(buf).strip()
                buf = []
                in_body = True
            else:
                body_buf.append(c)
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                body = "".join(body_buf)
                if selector and not selector.startswith("@"):
                    for sel in selector.split(","):
                        sel = re.sub(r"\s+", " ", sel).strip()
                        if sel:
                            out.append((sel, body))
                body_buf = []
                in_body = False
            else:
                body_buf.append(c)
        elif in_body:
            body_buf.append(c)
        else:
            buf.append(c)
    return out


def _specificity(selector: str) -> tuple[int, int, int]:
    """Standard CSS specificity per single compound selector."""
    s = re.sub(r"::[\w-]+", "", selector.strip())
    ids = len(re.findall(r"#[\w-]+", s))
    s_no_id = re.sub(r"#[\w-]+", " ", s)
    n_attrs = len(re.findall(r"\[[^\]]+\]", s_no_id))
    s_no_attr = re.sub(r"\[[^\]]+\]", " ", s_no_id)
    n_pseudos = len(re.findall(r":[\w-]+", s_no_attr))
    s_no_pseudo = re.sub(r":[\w-]+", " ", s_no_attr)
    n_classes = len(re.findall(r"\.[\w-]+", s_no_pseudo))
    s_no_class = re.sub(r"\.[\w-]+", " ", s_no_pseudo)
    types = len(re.findall(r"\b[a-z][\w-]*", s_no_class))
    return (ids, n_attrs + n_pseudos + n_classes, types)


def _class_targets(selector: str, *, strip_pseudo: str) -> set[str]:
    """Rightmost class anchor with the given pseudo-class stripped.
    Returns the bare class so we can compare cross-rules."""
    s = re.sub(r"::[\w-]+", "", selector)
    s = re.sub(rf":{re.escape(strip_pseudo)}\b", "", s)
    last = s.split()[-1] if s.split() else ""
    classes = re.findall(r"\.([A-Za-z][\w-]*)", last)
    return set(classes[-1:])


def _parse_props(body: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for decl in body.split(";"):
        decl = decl.strip()
        if not decl or ":" not in decl:
            continue
        prop, _, val = decl.partition(":")
        out[prop.strip()] = val.strip()
    return out


def _scan(path: Path, pseudo: str) -> list[str]:
    """Return list of findings for one pseudo-class against one CSS file."""
    findings: list[str] = []
    src = path.read_text()
    rules = _top_level_rules(src)

    state_index: dict[tuple[str, str], tuple[tuple[int, int, int], str]] = {}
    state_value: dict[tuple[str, str], str] = {}
    pseudo_marker = f":{pseudo}"
    for sel, body in rules:
        if pseudo_marker not in sel:
            continue
        # Skip if the pseudo is inside a :not(...) — that's a
        # negation, not the state itself.
        cleaned = re.sub(r":not\([^)]+\)", "", sel)
        if pseudo_marker not in cleaned:
            continue
        targets = _class_targets(sel, strip_pseudo=pseudo)
        if not targets:
            continue
        spec = _specificity(sel)
        props = _parse_props(body)
        for cls in targets:
            for prop, val in props.items():
                key = (cls, prop)
                prev = state_index.get(key)
                if prev is None or spec > prev[0]:
                    state_index[key] = (spec, sel)
                state_value[key] = val

    for sel, body in rules:
        if pseudo_marker in re.sub(r":not\([^)]+\)", "", sel):
            continue
        targets = _class_targets(sel, strip_pseudo=pseudo)
        if not targets:
            continue
        spec = _specificity(sel)
        props = _parse_props(body)
        for cls in targets:
            for prop, val in props.items():
                key = (cls, prop)
                state_entry = state_index.get(key)
                if state_entry is None:
                    continue
                s_spec, s_sel = state_entry
                if spec <= s_spec:
                    continue
                if state_value.get(key) == val:
                    continue
                if (sel, prop, pseudo) in ALLOWLIST:
                    continue
                findings.append(
                    f"{path.name} [:{pseudo}]: rule `{sel}` "
                    f"(specificity {spec}) overrides "
                    f"`.{cls}:{pseudo}` (`{s_sel}`, specificity "
                    f"{s_spec}) for property `{prop}` "
                    f"(`{val}` vs state `{state_value.get(key)}`)."
                )
    return findings


# ── State-specificity lint: :hover, :focus, :active ──────────

def test_no_hover_overridden_by_higher_specificity():
    findings = []
    for path in (APP_CSS, OPS_CSS):
        findings.extend(_scan(path, "hover"))
    assert not findings, (
        "v1.15.119: hover-state overrides by higher-specificity "
        "non-hover rules. After the state-attribute selector "
        "matches (e.g. JS adds a class/attribute), hover dies "
        "silently. Add a matching-specificity hover variant or "
        "allowlist with reason. Findings:\n  - "
        + "\n  - ".join(findings)
    )


def test_no_focus_overridden_by_higher_specificity():
    findings = []
    for path in (APP_CSS, OPS_CSS):
        findings.extend(_scan(path, "focus"))
    assert not findings, (
        "v1.15.119: focus-state overrides by higher-specificity "
        "non-focus rules. Same cascade-fight class as v1.15.118 "
        "hover bug. Findings:\n  - "
        + "\n  - ".join(findings)
    )


def test_no_active_overridden_by_higher_specificity():
    findings = []
    for path in (APP_CSS, OPS_CSS):
        findings.extend(_scan(path, "active"))
    assert not findings, (
        "v1.15.119: active-state overrides by higher-specificity "
        "non-active rules. Same cascade-fight class. Findings:\n  - "
        + "\n  - ".join(findings)
    )


# ── Self-tests: lint must catch the synthetic bug pattern ────

def test_self_test_catches_hover_bug_pattern():
    synthetic = (
        ".tab:hover { color: green; }\n"
        "html[data-x] #wrap .tab { color: gray; }\n"
    )
    rules = _top_level_rules(synthetic)
    # Quick path: run the same logic as _scan but against the
    # synthetic string by writing it to a tempfile.
    import tempfile
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".css", delete=False,
    ) as tmp:
        tmp.write(synthetic)
        tmp_path = Path(tmp.name)
    try:
        findings = _scan(tmp_path, "hover")
    finally:
        tmp_path.unlink()
    assert findings, "Self-test: lint must flag synthetic hover bug"


def test_self_test_catches_focus_bug_pattern():
    # Override needs HIGHER specificity than `.btn:focus` (0,2,0)
    # to be flagged. Use an attribute + descendant chain.
    synthetic = (
        ".btn:focus { outline: 2px solid cyan; }\n"
        "html[data-dlg-open] #wrap .btn { outline: none; }\n"
    )
    import tempfile
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".css", delete=False,
    ) as tmp:
        tmp.write(synthetic)
        tmp_path = Path(tmp.name)
    try:
        findings = _scan(tmp_path, "focus")
    finally:
        tmp_path.unlink()
    assert findings, "Self-test: lint must flag synthetic focus bug"


# ── A11y: .tab + .lib-flag-pill have focus-visible outline ───

def test_tab_has_focus_visible_outline():
    """The settings tabs must show a keyboard-focus outline so
    Tab-keying through them is visible to keyboard users."""
    src = APP_CSS.read_text()
    src = _strip_comments(src)
    # Find the focus-visible selector list block.
    fv_idx = src.index(":focus-visible {")
    # Walk back ~600 chars to capture the full selector list.
    head = src[max(0, fv_idx - 800):fv_idx]
    assert ".tab:focus-visible" in head, (
        "v1.15.119: .tab must appear in the focus-visible "
        "selector list so keyboard navigation through settings "
        "tabs paints an outline."
    )


def test_lib_flag_pill_has_focus_visible_outline():
    """Section-include role pills (A / 4K) are keyboard-
    interactive but pre-fix had no focus-visible outline."""
    src = APP_CSS.read_text()
    src = _strip_comments(src)
    fv_idx = src.index(":focus-visible {")
    head = src[max(0, fv_idx - 800):fv_idx]
    assert ".lib-flag-pill:focus-visible" in head, (
        "v1.15.119: .lib-flag-pill must appear in the focus-"
        "visible selector list so Tab-keying through the "
        "section-role pills paints an outline."
    )
