"""v1.21.57 — per-edition theme isolation, Phase C1 prep.

Parameterizes _SRC_LETTER_SQL (the SRC-letter CASE) into a builder
_src_letter_sql(...) so the per-edition library query (v1.21.58) can pass
edition-aware COALESCE(p_e.x, p_g.x) column expressions, while every
existing call site keeps the bare-alias form via the default args.

This is a PURE refactor: the default output must be byte-identical to the
pre-v1.21.57 constant, so all ~15 call sites (filters, sort, stats, donut,
library rows) are unchanged. This test pins that equivalence so a future
edit to the builder can't silently drift the SRC classification.
"""
from __future__ import annotations

from app.web.api import _SRC_LETTER_SQL, _src_letter_sql


# The exact pre-v1.21.57 constant value. If the builder's default output
# ever diverges from this, every SRC classification across the app shifts.
_FROZEN_DEFAULT = (
    "CASE "
    "WHEN p.media_folder IS NOT NULL AND lf.source_kind = 'themerrdb' THEN 'T' "
    "WHEN p.media_folder IS NOT NULL AND lf.source_kind = 'adopt' THEN 'A' "
    "WHEN p.media_folder IS NOT NULL AND lf.source_kind IN ('url','upload') THEN 'U' "
    "WHEN p.media_folder IS NOT NULL AND lf.source_kind = 'plex_cloud' THEN 'P' "
    "WHEN p.media_folder IS NOT NULL AND p.provenance = 'auto' THEN 'T' "
    "WHEN p.media_folder IS NOT NULL AND p.provenance = 'manual' AND ("
        "t.upstream_source != 'plex_orphan' "
        "OR COALESCE(lf.source_video_id, '') = '' "
        "OR length(lf.source_video_id) = 11 "
        "OR lf.source_video_id LIKE 'sc-%' "
        "OR lf.source_video_id LIKE 'ig-%' "
        "OR lf.source_video_id LIKE 'fb-%'"
    ") THEN 'U' "
    "WHEN p.media_folder IS NOT NULL AND p.provenance = 'manual' THEN 'A' "
    "WHEN p.media_folder IS NULL AND pi.local_theme_file = 1 THEN 'M' "
    "WHEN p.media_folder IS NULL AND pi.has_theme = 1 "
        "AND COALESCE(pi.plex_theme_verified_ok, 1) = 1 THEN 'P' "
    "ELSE '-' END"
)


def test_default_builder_matches_frozen_constant():
    assert _src_letter_sql() == _FROZEN_DEFAULT


def test_module_constant_still_uses_default():
    # The shared _SRC_LETTER_SQL constant (used by ~15 call sites) must
    # remain the default form — nothing edition-aware leaks into it.
    assert _SRC_LETTER_SQL == _FROZEN_DEFAULT


def test_edition_aware_form_substitutes_all_p_lf_columns():
    """When called with COALESCE expressions, NO bare p./lf. placement or
    local_files column survives (else the library query, which renames the
    aliases to p_e/p_g/lf_e/lf_g, would reference a non-existent alias)."""
    ed = _src_letter_sql(
        "COALESCE(p_e.media_folder, p_g.media_folder)",
        "COALESCE(p_e.provenance, p_g.provenance)",
        "COALESCE(lf_e.source_kind, lf_g.source_kind)",
        "COALESCE(lf_e.source_video_id, lf_g.source_video_id)")
    assert "p.media_folder" not in ed
    assert "p.provenance" not in ed
    assert "lf.source_kind" not in ed
    assert "lf.source_video_id" not in ed
    assert "COALESCE(p_e.media_folder, p_g.media_folder)" in ed
    # pi.*/t.* are NOT edition-split — they must remain untouched.
    assert "pi.local_theme_file = 1" in ed
    assert "pi.has_theme = 1" in ed
    assert "t.upstream_source != 'plex_orphan'" in ed
