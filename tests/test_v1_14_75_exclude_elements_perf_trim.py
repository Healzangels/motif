"""v1.14.75 — Plex /library/sections/{id}/all `excludeElements` payload trim.

the user: "lets do it" (referring to perf research item #2 from
the v1.14.71 conversation).

The default Plex response for /library/sections/{id}/all carries
~80 lines per item — Genre/Writer/Director/Country/Role/Producer/
Similar/Collection and other tag-list children. Motif reads NONE
of these (only ratingKey/title/year/theme attributes + <Guid>
/<Part>/<Location> children for IDs and folder paths). For a
4126-item TV section that's megabytes of XML per page wasted on
parse + immediate discard.

The Plex API's "Response Customization" spec lets a client send
`excludeElements=...` to skip those children server-side. Per
the spec it's "treated as a request, not a guarantee" — Plex
builds that ignore it just return the full payload, so this is
a pure win when honored and a no-op otherwise.

## What this test pins

  - The `excludeElements` param is present in the params dict
    that enumerate_section_items sends.
  - It excludes the metadata-tag children we know motif never
    reads (Genre/Writer/Director/Country/Role/Producer/Similar/
    Collection/Field/Label/Mood/Style/Image/Review).
  - It does NOT exclude Media. <Part> (which carries the file
    path for movie folder_path extraction) is nested inside
    <Media>; excluding Media would drop Part too and break
    movie folder discovery. This is the "Media stays" regression
    guard.
  - The v1.14.75 marker explains the rationale at the callsite.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
PLEX_PY = REPO / "app" / "core" / "plex.py"


def _params_block() -> str:
    """Slice the params dict literal in enumerate_section_items
    so test assertions stay scoped to the actual request site
    (not e.g. a comment that mentions excludeElements somewhere
    else)."""
    src = PLEX_PY.read_text()
    fn_start = src.index("def enumerate_section_items(")
    # The params dict opens with `params = {`.
    params_idx = src.index("params = {", fn_start)
    # Slice through the closing brace + a small buffer so the
    # match is unambiguous.
    end_idx = src.index("}", params_idx)
    return src[params_idx:end_idx + 1]


# ── excludeElements is present + correctly populated ──────────


def test_exclude_elements_param_present_in_request():
    """The params dict must include `excludeElements`. Pre-fix
    every page response carried the full per-item Genre/Writer/
    Director/etc. tag lists motif then discarded."""
    block = _params_block()
    assert '"excludeElements"' in block


def test_exclude_elements_lists_known_unused_tag_types():
    """The exclude list must cover the metadata-tag children
    motif never reads. Pin each name so a future change can't
    silently drop one (which would reintroduce that element's
    payload bloat)."""
    block = _params_block()
    must_exclude = (
        "Genre", "Writer", "Director", "Country", "Role",
        "Producer", "Similar", "Collection", "Field", "Label",
        "Mood", "Style", "Image", "Review",
    )
    for name in must_exclude:
        assert name in block, (
            f"excludeElements is missing {name!r} — payload bloat "
            "from this element type would return."
        )


# ── Media MUST NOT be excluded (regression guard) ─────────────


def test_media_element_must_not_be_excluded():
    """`<Part>` (which carries the movie file path that
    `_extract_folder_path` needs for folder_path) is nested
    inside `<Media>`. Excluding Media would drop Part too and
    silently break movie folder_path discovery. The agent's
    perf research note suggested excluding Media but the v1.14.75
    impl explicitly KEEPS it for this reason. Pin so a future
    "let's exclude Media too" change fails here first."""
    block = _params_block()
    # Look for Media as a comma-bounded token (not as a substring
    # of "MediaContainer" or similar). The exclude list is a
    # comma-separated string inside the dict value.
    assert "Media," not in block
    assert ",Media" not in block
    # Stricter shape: Media doesn't appear inside the literal
    # `excludeElements` value. Use a narrow window around the
    # value.
    val_start = block.index('"excludeElements"')
    val_end = block.index(")", val_start)
    val_block = block[val_start:val_end]
    # Allow the literal "MediaContainer" or "Media-Foo" substrings
    # if they ever appear; but the bare token "Media" between
    # commas (or at edge) must not.
    tokens = (
        val_block.replace('"', '')
                 .replace("(", "")
                 .replace(")", "")
                 .replace(" ", "")
    )
    parts = tokens.split(",")
    assert "Media" not in parts, (
        "v1.14.75 explicitly KEEPS Media in the response — "
        "<Part> children (file paths for movie folder discovery) "
        "are nested inside it. Excluding Media silently breaks "
        "_extract_folder_path for movies."
    )


# ── Existing params survive (no over-edit regression guard) ───


def test_existing_request_params_still_present():
    """v1.14.75 only ADDS excludeElements — none of the existing
    params should disappear in the same edit. Pin the load-bearing
    ones (the v1.11.82 includeLocations + the pagination keys)."""
    block = _params_block()
    assert '"includeGuids"' in block
    assert '"includeLocations"' in block
    assert '"X-Plex-Container-Start"' in block
    assert '"X-Plex-Container-Size"' in block
    assert '"type"' in block


# ── v1.14.75 marker pinned at callsite ────────────────────────


def test_v1_14_75_marker_explains_rationale():
    """The v1.14.75 marker explaining why excludeElements is
    safe (and why Media stays) must sit at the callsite so a
    future reader who notices the param can grep here for the
    rationale + caveats."""
    src = PLEX_PY.read_text()
    fn_start = src.index("def enumerate_section_items(")
    fn_body = src[fn_start:fn_start + 6000]
    # Marker text.
    assert "v1.14.75: trim per-item child elements" in fn_body
    # The "treated as a request, not a guarantee" caveat. Comment
    # wraps across multiple lines AND each wrap-line starts with
    # `# ` (Python comment leader), so strip both before
    # collapsing whitespace.
    flat = " ".join(
        line.lstrip("# ").lstrip()
        for line in fn_body.splitlines()
    )
    assert "treated as a request, not a guarantee" in flat
    # The Media-must-stay reasoning.
    assert "Media" in fn_body and "Part" in fn_body


# ── Cross-ref: motif's parsers don't read the excluded types ──


def test_extractors_do_not_reference_excluded_element_types():
    """Belt-and-suspenders: confirm motif's two element-iter
    parsers (`_extract_guids`, `_extract_folder_path`) don't
    iterate any of the excluded child types. If a future patch
    adds e.g. `el.iter("Genre")` to one of them, the exclude
    list would silently break it — this test catches that
    drift class."""
    src = PLEX_PY.read_text()
    # Slice the two extractor function bodies.
    g_start = src.index("def _extract_guids(")
    g_body = src[g_start:src.index("\n\n", g_start)]
    f_start = src.index("def _extract_folder_path(")
    f_body = src[f_start:src.index("\n\n", f_start)]
    excluded = (
        "Genre", "Writer", "Director", "Country", "Role",
        "Producer", "Similar", "Collection", "Field", "Label",
        "Mood", "Style", "Image", "Review",
    )
    for tag in excluded:
        assert f'iter("{tag}")' not in g_body
        assert f'iter("{tag}")' not in f_body
