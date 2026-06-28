"""v1.18.77 — user_overrides.intent: backup-vs-replace foundation.

the user's report: two rows in apparently-similar U-with-dead-TDB
states (1941 and Pokémon) showed different banners ("PLEX SERVES
— OPTIONAL UPGRADES" vs "RESOLVED VIA URL"). Investigation found
they're actually in different states — Pokémon's placement landed,
1941's placement was silently skipped due to plex_has_theme.

Pre-v1.18.77 the system couldn't distinguish two different user
intents that both landed in the "downloaded but not placed" state:
  (a) "Plex serves theme; my URL is intentionally a backup."
  (b) "I wanted to replace Plex's theme but motif silently skipped."

Both looked identical post-fact and the UI suggested PUSH TO PLEX
for both — wrong for case (a) where the user explicitly chose
backup.

v1.18.77 promotes the user's choice to a first-class column on
user_overrides:
  - `intent='replace'` → motif's URL should win; force placement
  - `intent='backup'`  → defer to Plex; URL is a safety net

The existing `download_only` checkbox in the SET URL dialog
drives the intent at row creation. Migration v55→v56 backfills
existing rows: 'replace' when a placements row exists for the
(mt, tmdb, section_id); 'backup' otherwise (the user's "default
to backup" choice for ambiguous rows).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Schema + migration ──────────────────────────────────────


def test_schema_version_bumped_to_56():
    """v1.18.77's pin: schema must have included v56. After
    subsequent migrations (v1.19.12 → v57), the constant
    moves but the v56 migration must still exist + be
    reachable from the migration chain."""
    db_py = (REPO / "app" / "core" / "db.py").read_text()
    # CURRENT_SCHEMA_VERSION is now ≥ 56 (subsequent tags may
    # bump further). What matters for v1.18.77's contract is
    # that the v55→v56 migration exists.
    assert "_migrate_v55_to_v56(" in db_py, (
        "v1.18.77: the v55→v56 migration must remain defined"
    )
    # And the migration chain must reach v56 (which it does
    # implicitly if CURRENT_SCHEMA_VERSION >= 56).
    import re
    m = re.search(r"CURRENT_SCHEMA_VERSION\s*=\s*(\d+)", db_py)
    assert m
    assert int(m.group(1)) >= 56, (
        f"v1.18.77: CURRENT_SCHEMA_VERSION must be >= 56; "
        f"got {m.group(1)}"
    )


def test_user_overrides_schema_includes_intent_column():
    """The SCHEMA constant's user_overrides definition must include
    the intent column for fresh installs."""
    db_py = (REPO / "app" / "core" / "db.py").read_text()
    create_idx = db_py.index(
        "CREATE TABLE IF NOT EXISTS user_overrides")
    create_end = db_py.index(");", create_idx)
    create_block = db_py[create_idx:create_end]
    assert "intent" in create_block, (
        "v1.18.77: SCHEMA must declare intent column"
    )
    assert "DEFAULT 'replace'" in create_block, (
        "v1.18.77: default value must be 'replace'"
    )
    assert "CHECK (intent IN ('replace', 'backup'))" in create_block, (
        "v1.18.77: CHECK constraint must pin the two allowed values"
    )


def test_migration_v55_to_v56_exists():
    """The migration helper must exist + be wired into the
    dispatch chain in `init_db`."""
    db_py = (REPO / "app" / "core" / "db.py").read_text()
    assert "def _migrate_v55_to_v56(" in db_py, (
        "v1.18.77: migration helper required"
    )
    # Dispatch entry.
    assert "_migrate_v55_to_v56(conn)" in db_py
    assert "elif current == 55:" in db_py


def test_migration_v55_to_v56_uses_alter_table_add_column():
    """SQLite supports ALTER TABLE ADD COLUMN with default — no table rebuild
    needed (avoids the v1.18.0-class rebuild risks). v1.24.50: the ADD COLUMN goes
    through the idempotent _add_column helper so a crash-then-reboot re-run is a
    safe no-op instead of a 'duplicate column name' crash-loop."""
    db_py = (REPO / "app" / "core" / "db.py").read_text()
    fn_idx = db_py.index("def _migrate_v55_to_v56(")
    fn_end = db_py.index("\ndef ", fn_idx + 1)
    body = db_py[fn_idx:fn_end]
    assert '_add_column(conn, "user_overrides", "intent"' in body


def test_migration_backfills_backup_for_rows_without_placement():
    """Per the user's design choice ('default to backup'), existing
    user_overrides rows WITHOUT a corresponding placement get
    intent='backup'. Rows WITH a placement keep the schema default
    'replace'."""
    db_py = (REPO / "app" / "core" / "db.py").read_text()
    fn_idx = db_py.index("def _migrate_v55_to_v56(")
    fn_end = db_py.index("\ndef ", fn_idx + 1)
    body = db_py[fn_idx:fn_end]
    # Two backfill UPDATEs — section-scoped + global.
    assert body.count(
        "UPDATE user_overrides\n           SET intent = 'backup'"
    ) == 2, (
        "v1.18.77: migration must run TWO backfill UPDATEs — "
        "one for section-scoped overrides, one for the '' global "
        "fallback. Pre-v1.18.77 the global override had no per-"
        "section placement to check against, so it needs its own "
        "ANY-section heuristic."
    )
    # Section-scoped backfill: placement at same section_id.
    assert "AND p.section_id = user_overrides.section_id" in body, (
        "v1.18.77: per-section backfill checks placement at the "
        "same section as the override"
    )
    # Global ('') backfill: placement at ANY section.
    assert "WHERE section_id = ''" in body


# ── SET URL persists intent ─────────────────────────────────


def test_manual_url_endpoint_persists_intent():
    """api_manual_url must persist intent from the download_only
    body field. download_only=true → intent='backup'; else
    intent='replace'."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = api_py.index("async def api_manual_url(")
    fn_end = api_py.index("@app.post", fn_idx + 1)
    body = api_py[fn_idx:fn_end]
    # The intent calculation.
    assert '_intent = (' in body
    assert '"backup"' in body
    assert '"replace"' in body
    # The INSERT includes intent column.
    assert "intent" in body
    # INSERT VALUES uses the _intent local.
    import re
    body_flat = re.sub(r"\s+", " ", body)
    # v1.21.60: _edition_key threads between section_id_for_override and
    # _intent (per-edition override scoping).
    assert "section_id_for_override, _edition_key, _intent" in body_flat, (
        "v1.18.77: VALUES tuple must include the computed _intent"
    )


def test_manual_url_endpoint_upsert_updates_intent_on_conflict():
    """When the override already exists, the ON CONFLICT branch
    must UPDATE intent too (otherwise toggling download_only on a
    re-SET URL wouldn't propagate)."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = api_py.index("async def api_manual_url(")
    fn_end = api_py.index("@app.post", fn_idx + 1)
    body = api_py[fn_idx:fn_end]
    assert "intent = excluded.intent" in body, (
        "v1.18.77: ON CONFLICT must propagate the new intent"
    )


# ── /intent endpoint ────────────────────────────────────────


def test_intent_endpoint_exists():
    """POST /api/items/{mt}/{id}/intent must be a registered
    endpoint. Flips intent between 'backup' and 'replace'."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert (
        '@app.post("/api/items/{media_type}/{tmdb_id}/intent")'
        in api_py
    )
    assert "async def api_set_override_intent(" in api_py


def test_intent_endpoint_validates_intent_value():
    """Body must specify `intent` in ('backup', 'replace');
    otherwise 400."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = api_py.index("async def api_set_override_intent(")
    fn_end = api_py.index("@app.post", fn_idx + 1)
    body = api_py[fn_idx:fn_end]
    assert 'new_intent not in ("backup", "replace")' in body
    assert "status_code=400" in body


def test_intent_endpoint_409s_when_no_override():
    """If no user_override exists for the row (per-section + ''
    fallback), 409. The endpoint can't create an override; it
    can only flip an existing one."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = api_py.index("async def api_set_override_intent(")
    fn_end = api_py.index("@app.post", fn_idx + 1)
    body = api_py[fn_idx:fn_end]
    assert "status_code=409" in body
    assert "no user_override to flip intent on" in body


def test_intent_endpoint_records_audit():
    """Every intent flip must record an audit entry. Lets the
    user trace state-change history."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = api_py.index("async def api_set_override_intent(")
    fn_end = api_py.index("@app.post", fn_idx + 1)
    body = api_py[fn_idx:fn_end]
    assert 'action="set_intent"' in body
    assert "_record_audit(" in body


def test_intent_endpoint_promote_enqueues_force_place():
    """backup → replace must enqueue a force-place job per
    section with staged content. The user expressed intent to
    deploy the backup; motif must act on it."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = api_py.index("async def api_set_override_intent(")
    fn_end = api_py.index("@app.post", fn_idx + 1)
    body = api_py[fn_idx:fn_end]
    # The promote-to-active branch.
    assert 'new_intent == "replace"' in body
    # Force-place payload.
    assert '"force_place": True' in body
    assert '"reason": "promote_backup_to_active"' in body
    # And cancels prior jobs first (defensive — backup row
    # shouldn't have a queued place job, but if it does, cancel
    # before re-enqueueing).
    assert "UPDATE jobs SET status = 'cancelled'" in body


def test_intent_endpoint_demote_does_not_cancel_running_jobs():
    """replace → backup must NOT cancel a currently-running
    place job mid-flight (that's a destructive op the user
    didn't ask for). The NEXT post-download flow respects
    the new 'backup' intent; in-flight work completes.

    v1.19.35: this function now has TWO cancel-jobs blocks —
    the original v1.18.77 one (gated on `new_intent == "replace"`)
    and the new BK-no-override branch (gated on
    `new_intent != "replace"` raising 409 BEFORE the cancel).
    Pin every cancel-block as preceded by a replace-direction
    gate, not the first-occurrence shape that v1.19.35's BK
    branch invalidated."""
    import re
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    fn_idx = api_py.index("async def api_set_override_intent(")
    fn_end = api_py.index("@app.post", fn_idx + 1)
    body = api_py[fn_idx:fn_end]
    # Find every cancel-block offset in the function body.
    cancel_offsets = [
        m.start()
        for m in re.finditer(
            r"UPDATE jobs SET status = 'cancelled'", body,
        )
    ]
    assert cancel_offsets, (
        "expected at least one cancel-jobs block in the intent "
        "endpoint body"
    )
    # For each cancel-block, search the 600 chars preceding it for
    # a replace-direction gate — either `new_intent == "replace"`
    # (the legacy v1.18.77 promote branch) OR
    # `new_intent != "replace": raise` (the v1.19.35 BK branch's
    # 409 guard that means "everything past this is replace-only").
    for offset in cancel_offsets:
        # v1.19.35: widened from 600 → 1600 chars to accommodate
        # the v1.19.35 BK branch's longer comment block between
        # the `new_intent != "replace"` raise-409 gate and the
        # cancel-jobs SQL.
        # v1.19.44: widened 1600 → 2400 chars. The v1.19.44
        # BK-no-override widening (SELECT source_kind + file_path
        # + ~700 chars of WHY comments documenting the v1.18.36
        # re-upload trick rationale) pushed the gate further from
        # the cancel block. Test's intent unchanged: every
        # cancel-jobs block must be gated on a replace-direction
        # check so DEMOTE doesn't cancel running jobs.
        # v1.22.75: widened 2400 → 3200 — the edition-scope comment
        # added between the 409 gate and the no-override cancel.
        window = body[max(0, offset - 3200):offset]
        ok = (
            'new_intent == "replace"' in window
            or 'new_intent != "replace"' in window
        )
        assert ok, (
            f"v1.18.77 (+ v1.19.35): cancel-jobs block at offset "
            f"{offset} is not gated on a replace-direction check. "
            f"demote (replace → backup) must NOT cancel running "
            f"jobs."
        )


# ── Frontend banner distinction ─────────────────────────────


def test_frontend_distinguishes_backup_banner():
    """The recovery banner must show different text for
    intent='backup' rows. Pre-v1.18.77 both intents fell into
    'PLEX SERVES — OPTIONAL UPGRADES'; post-v1.18.77 the
    backup-intent rows get 'BACKUP READY — DEFERRING TO PLEX'."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "BACKUP READY — DEFERRING TO PLEX" in js, (
        "v1.18.77: new banner text for intent='backup' + "
        "plex_serves required"
    )
    # The existing 'PLEX SERVES' branch must still survive for
    # rows where intent='replace' but placement was blocked.
    assert "PLEX SERVES — OPTIONAL UPGRADES" in js


def test_frontend_reads_intent_from_override_payload():
    """The frontend must read intent from data.override.intent —
    the existing api_item endpoint returns the full user_overrides
    row via SELECT *, so intent is automatically present in the
    payload."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert (
        "data.override && data.override.intent"
        in js
    ), (
        "v1.18.77: frontend must derive overrideIntent from "
        "data.override.intent"
    )


def test_frontend_intent_flip_buttons_present():
    """When the row has a user override + Plex is serving,
    show one of two conversion buttons:
      - intent='backup' → // PROMOTE TO ACTIVE
      - intent='replace' → // MARK AS BACKUP

    Mutually exclusive (a row has exactly one intent value)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert '// PROMOTE TO ACTIVE' in js
    assert '// MARK AS BACKUP' in js
    # Data-act values for the click handlers.
    assert 'data-act="promote-to-active"' in js
    assert 'data-act="mark-as-backup"' in js


def test_frontend_intent_flip_buttons_hit_correct_endpoint():
    """The click handlers must POST to
    /api/items/{mt}/{id}/intent with the right intent body."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Endpoint path.
    assert "/api/items/${mt}/${id}/intent" in js
    # Both intents are wired.
    assert "_wireIntentFlip(" in js


# ── SET URL dialog label updated ────────────────────────────


def test_set_url_dialog_checkbox_label_says_backup():
    """The SET URL dialog's checkbox was previously labeled
    'DOWNLOAD ONLY' which was ambiguous. v1.18.77 renamed it
    to 'KEEP AS BACKUP' — explicit about the downstream
    intent. v1.18.86 dropped the `// ` prefix (per
    DESIGN_SYSTEM §3 the prefix belongs on buttons + section
    titles only, not checkbox labels)."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    assert "KEEP AS BACKUP (Plex keeps serving)" in html
    # The `// ` prefix must NOT lead the label — v1.18.86 fix.
    assert "// KEEP AS BACKUP" not in html
    # v1.18.77 marker comment in the template survives.
    assert "v1.18.77" in html


def test_set_url_dialog_hint_explains_backup_semantics():
    """The hint text under the checkbox must explain what
    'backup' means downstream — Plex keeps serving, motif holds
    the URL as a safety net, can promote later via // PROMOTE
    TO ACTIVE."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    # v1.19.63: BU LINK tooltip now also mentions "KEEP AS BACKUP"
    # as part of its sources list. Skip past the first match to
    # land on the actual SET URL dialog hint block.
    first_hit = html.index("KEEP AS BACKUP")
    hint_idx = html.index("KEEP AS BACKUP", first_hit + 1)
    block = html[hint_idx:hint_idx + 2000]
    # Key phrases.
    assert "backup" in block.lower()
    assert "Plex" in block
    assert "PROMOTE TO ACTIVE" in block
