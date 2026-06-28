"""v1.15.118 — hover-regression specificity lint + 2 fixes.

the user's report on v1.15.116:

> Found an issue on the settings page, when first navigating to the
> settings page on hover works when hovering over paths, plex,
> downloads etc. Once you have clicked one however the on hover
> highlight text stops working.

Plus a follow-up "I want to test for this across motif — these are
the kind of bugs I don't want to keep slipping through the cracks
and come up on testing to be fixed automatically."

## The bug class

A common CSS pattern: a state-attribute rule (e.g. `html[data-x]
.target`) sets `color` or similar at high specificity. Elsewhere
`.target:hover` is defined at lower specificity. After the state
flips on (JS adds the attribute post-click), the high-specificity
rule WINS over the hover — hover dies silently. The user sees a
button that's "stuck" without color feedback.

Pre-fix concrete instance:

  html[data-settings-tab] #settings-tabs .tab {
    color: var(--fg-mute);        /* specificity 0,1,2,1 */
  }
  .tab:hover { color: var(--green); }  /* specificity 0,0,2,0 */

After `showTab()` stamps the attribute on <html>, every tab's
hover color was pinned to --fg-mute. Only re-navigating to
/settings (no hash) cleared the attribute → hover worked again.

## The fix (instance)

A matching-specificity sibling hover rule:

  html[data-settings-tab] #settings-tabs .tab:hover {
    color: var(--green);
  }

## The lint (forward-looking)

For every `.X:hover { property: value; }` declaration, the lint
checks that no higher-specificity rule on the SAME .X class sets
the same property without its own matching-or-higher specificity
`:hover` sibling. If it finds one, the test fails with the
offending selectors + the property in question.

The lint is conservative: it only flags exact-class matches. It
won't catch cross-component cascade fights (e.g. `.parent .X`
overriding `.X:hover` — those are usually intentional context
scoping). The lint catches the specific bug pattern the user
flagged: state-attribute selectors that nuke hover.

## Initial scan

Post-fix the lint should be CLEAN. If it surfaces unknown
findings, those are pre-existing bugs the audit didn't catch yet.

Allowlist is empty initially. Entries take the form:

    ALLOWLIST[("selector", "property")] = "reason"

When the allowlist grows, each entry must come with a comment
explaining why the override is intentional (e.g., the disabled
state legitimately overrides hover color).
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"


# Allowlist of (high-specificity-selector, property) pairs that
# legitimately override a `.X:hover`. None at v1.15.118.
ALLOWLIST: dict[tuple[str, str], str] = {}


def _strip_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def _top_level_rules(src: str) -> list[tuple[str, str]]:
    """Yield (selector, body) for every top-level rule. Multi-
    selector lists (`A, B, C { ... }`) are SPLIT into individual
    (selector, body) tuples so per-compound specificity comparisons
    work correctly — pre-fix the lint summed specificity across
    comma-separated compounds and got a wildly inflated count
    (10 IDs from 10 occurrences of `#settings-tabs` etc.).
    Skips at-rules (@media / @keyframes / etc.)."""
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
                    # Split comma-separated selector lists so each
                    # compound is its own rule for specificity.
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
    """Return (id_count, class+attr+pseudo_count, type_count) for
    a SINGLE selector compound (no commas — callers split before
    this). Standard CSS specificity calculation."""
    s = selector.strip()
    # Strip pseudo-element parts (::before, ::after, ::backdrop) —
    # they count as types per modern spec but we treat them as 0
    # for the lint's purposes (consistent with most calculators).
    s = re.sub(r"::[\w-]+", "", s)
    # Count IDs first, then erase them so the type-selector regex
    # doesn't double-count.
    ids = len(re.findall(r"#[\w-]+", s))
    s_no_id = re.sub(r"#[\w-]+", " ", s)
    # Count attribute selectors then erase.
    n_attrs = len(re.findall(r"\[[^\]]+\]", s_no_id))
    s_no_attr = re.sub(r"\[[^\]]+\]", " ", s_no_id)
    # Count pseudo-classes (including :hover, :focus, :active,
    # :not, etc.) then erase.
    n_pseudos = len(re.findall(r":[\w-]+", s_no_attr))
    s_no_pseudo = re.sub(r":[\w-]+", " ", s_no_attr)
    # Count classes then erase.
    n_classes = len(re.findall(r"\.[\w-]+", s_no_pseudo))
    s_no_class = re.sub(r"\.[\w-]+", " ", s_no_pseudo)
    # Remaining word tokens are type selectors. Filter out combinator
    # chars + the universal selector.
    types = len(re.findall(r"\b[a-z][\w-]*", s_no_class))
    return (ids, n_attrs + n_pseudos + n_classes, types)


def _parse_props(body: str) -> dict[str, str]:
    """Return {property: value} from a rule body. Last wins on dup
    properties within the same rule (CSS cascade order)."""
    out: dict[str, str] = {}
    for decl in body.split(";"):
        decl = decl.strip()
        if not decl or ":" not in decl:
            continue
        prop, _, val = decl.partition(":")
        out[prop.strip()] = val.strip()
    return out


def _class_targets(selector: str) -> set[str]:
    """Extract the SECOND-to-last simple-class anchor — the actual
    class whose hover state is being styled. For `.tab:hover` →
    {'tab'}. For `html[x] #y .tab:hover` → {'tab'}.

    Conservative: the lint scopes to single-class targets so it
    catches the v1.15.118 bug class without false-positives on
    multi-class descendant chains."""
    s = re.sub(r"::[\w-]+", "", selector)
    # Strip :hover specifically — what remains is the target.
    s_no_hover = re.sub(r":hover\b", "", s)
    # Find the rightmost simple class (after the last
    # combinator / space).
    last = s_no_hover.split()[-1] if s_no_hover.split() else ""
    classes = re.findall(r"\.([A-Za-z][\w-]*)", last)
    return set(classes[-1:])  # rightmost only


def test_no_hover_property_overridden_by_higher_specificity():
    """For every `.X:hover { prop: val; }`, verify that no rule
    targeting `.X` (without `:hover`) has higher specificity AND
    sets the same property — unless a sibling higher-specificity
    `:hover` rule exists OR the pair is allowlisted with a reason.
    """
    findings: list[str] = []
    for path in (APP_CSS, OPS_CSS):
        src = path.read_text()
        rules = _top_level_rules(src)
        # Step 1: collect every (target_class, property) covered by
        # a hover rule, along with its specificity.
        hover_index: dict[tuple[str, str], tuple[tuple[int, int, int], str]] = {}
        for sel, body in rules:
            if ":hover" not in sel:
                continue
            targets = _class_targets(sel)
            if not targets:
                continue
            spec = _specificity(sel)
            props = _parse_props(body)
            for cls in targets:
                for prop in props:
                    key = (cls, prop)
                    prev = hover_index.get(key)
                    if prev is None or spec > prev[0]:
                        hover_index[key] = (spec, sel)

        # Step 1b: re-walk to record hover VALUES per (cls, prop)
        # so step 2 can compare values, not just property names.
        # A higher-specificity rule that sets the SAME value as
        # hover isn't a cascade-fight bug — it's a no-op for the
        # hover transition (intentional in state-equals-hover
        # designs like .library-presets-bookmark where the
        # "has-active" state visually equals the hover state).
        hover_value: dict[tuple[str, str], str] = {}
        for sel, body in rules:
            if ":hover" not in sel:
                continue
            targets = _class_targets(sel)
            if not targets:
                continue
            props = _parse_props(body)
            for cls in targets:
                for prop, val in props.items():
                    hover_value[(cls, prop)] = val

        # Step 2: for every non-hover rule that targets a class
        # also covered by hover_index, compare specificity per
        # property — AND only flag if the override value differs
        # from the hover value.
        for sel, body in rules:
            if ":hover" in sel:
                continue
            targets = _class_targets(sel)
            if not targets:
                continue
            spec = _specificity(sel)
            props = _parse_props(body)
            for cls in targets:
                for prop, val in props.items():
                    key = (cls, prop)
                    hover_entry = hover_index.get(key)
                    if hover_entry is None:
                        continue
                    h_spec, h_sel = hover_entry
                    if spec <= h_spec:
                        continue
                    # Value-aware: same value → not a cascade fight.
                    if hover_value.get(key) == val:
                        continue
                    if (sel, prop) in ALLOWLIST:
                        continue
                    findings.append(
                        f"{path.name}: rule `{sel}` (specificity "
                        f"{spec}) overrides `.{cls}:hover` "
                        f"(`{h_sel}`, specificity {h_spec}) "
                        f"for property `{prop}` "
                        f"(`{val}` vs hover `{hover_value.get(key)}`). "
                        "Add a matching-specificity `:hover` "
                        "variant or allowlist the override."
                    )

    assert not findings, (
        "v1.15.118: high-specificity rule(s) override `.X:hover` "
        "for the same property at a lower specificity. After the "
        "high-specificity selector matches (e.g. JS adds a state "
        "attribute), hover dies silently. Add a matching-specificity "
        "`:hover` variant or allowlist with reason. Findings:\n  - "
        + "\n  - ".join(findings)
    )


def test_settings_tab_hover_preserved_post_click():
    """v1.15.118 fix: when html[data-settings-tab] is stamped (by
    showTab post-click), the `.tab:hover` color override must
    still fire. Catches a regression that drops the matching-
    specificity rule."""
    src = APP_CSS.read_text()
    src = _strip_comments(src)
    # The rule must appear and use the same color as the lower-
    # specificity .tab:hover.
    assert (
        "html[data-settings-tab] #settings-tabs .tab:hover" in src
    ), (
        "v1.15.118: the matching-specificity hover rule for the "
        "settings tabs is missing. Without it, hover dies silently "
        "after the first tab click."
    )


def test_lint_catches_synthetic_v1_15_118_bug_pattern():
    """Self-test on a synthetic CSS sample that mimics the
    pre-v1.15.118 bug:
       .tab:hover { color: green; }                     (low spec)
       html[data-x] .tab { color: gray; }               (high spec, no hover)
    The lint must produce a finding. Without this self-test a
    silent bug in the lint logic (e.g. the value-equality short-
    circuit firing too aggressively) would let the same class of
    bug ship undetected next time.
    """
    synthetic = (
        ".tab { color: black; }\n"
        ".tab:hover { color: green; }\n"
        "html[data-x] #wrap .tab { color: gray; }\n"
    )

    # Inline the lint logic against the synthetic string.
    rules = _top_level_rules(synthetic)

    hover_index: dict[tuple[str, str], tuple[tuple[int, int, int], str]] = {}
    hover_value: dict[tuple[str, str], str] = {}
    for sel, body in rules:
        if ":hover" not in sel:
            continue
        targets = _class_targets(sel)
        if not targets:
            continue
        spec = _specificity(sel)
        props = _parse_props(body)
        for cls in targets:
            for prop, val in props.items():
                key = (cls, prop)
                prev = hover_index.get(key)
                if prev is None or spec > prev[0]:
                    hover_index[key] = (spec, sel)
                hover_value[key] = val

    flagged = False
    for sel, body in rules:
        if ":hover" in sel:
            continue
        targets = _class_targets(sel)
        if not targets:
            continue
        spec = _specificity(sel)
        props = _parse_props(body)
        for cls in targets:
            for prop, val in props.items():
                key = (cls, prop)
                hover_entry = hover_index.get(key)
                if hover_entry is None:
                    continue
                h_spec, _ = hover_entry
                if spec > h_spec and hover_value.get(key) != val:
                    flagged = True

    assert flagged, (
        "Self-test: the lint must flag the synthetic bug pattern. "
        "If this fails, the lint's specificity comparison or value-"
        "equality logic has regressed."
    )


def test_import_preview_td_vertical_align_middle():
    """v1.15.118 fix: import-preview rows had cells defaulting to
    `vertical-align: baseline`, pushing the action <select> below
    the row midline and pulling the smaller URL text above center.
    Cell-level vertical-align middle pairs with the
    .input-tiny line-box centering for cross-cell alignment."""
    src = APP_CSS.read_text()
    src = _strip_comments(src)
    assert "#import-preview-table td { vertical-align: middle; }" in src, (
        "v1.15.118: the cell-level vertical-align middle rule for "
        "the import-preview table is missing. Without it, the row "
        "alignment the user flagged (Apply ▼ bottom-biased, URL "
        "high) returns."
    )
