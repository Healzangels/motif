"""v0.50.89 — holistic-audit Batch 6: topbar SSR op-kind coverage.

A fresh read of the 4 originally-listed Batch 6 findings confirmed only
ONE was real; the other three did not reproduce against current code and
were dropped:

  - notify.py "dedupe timing bug": NO dedupe mechanism exists in notify.py
    at all (dispatch is pure fire-and-forget; dispatch_coalesced batches
    distinct events, it does not de-duplicate the same event). Stale.
  - plex_enum.py has_plex_upload guard "inert for new rows": the guard
    computes indep_observation and correctly returns False for a genuinely
    new rating_key (a new row cannot have a pre-existing plex_upload
    placement under its own rk) while still firing via the JOIN when a
    placement exists from a prior rk. Behaves as designed.
  - worker.py "stranded plex_upload placement row on fallback": the
    placements upsert is written exactly ONCE, after the fallback decision
    is already resolved (_placed_kind = fell_back_kind or 'plex_upload'),
    so no half-created plex_upload row is left behind. Does not reproduce.

The real finding:

  `_topbar_ssr_state` (api.py) — the server-side-rendered topbar op-mini
  picker only ranked/labelled 4 of the 7 real op_progress kinds. bulk_lps,
  tvdb_bridge, and cloud_themes_backup fell to the ELSE-99 tier with no
  tone/label mapping, so an SSR paint while one of them ran alone showed a
  generic tone + "…" placeholder until the first ops.js poll corrected it.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
DB_PY = (REPO / "app" / "core" / "db.py").read_text()


def _ssr_body() -> str:
    anchor = API_PY.index("def _topbar_ssr_state(")
    end = API_PY.index('templates.env.globals["topbar_ssr_state"]', anchor)
    return API_PY[anchor:end]


def _real_op_progress_kinds() -> set[str]:
    m = re.search(
        r"CHECK\s*\(kind\s+IN\s*\((.*?)\)\)", DB_PY, re.DOTALL,
    )
    assert m, "could not locate op_progress.kind CHECK in db.py"
    return set(re.findall(r"'([a-z_]+)'", m.group(1)))


# ── 1. every real op_progress kind is covered by the SSR picker ─────────


def test_ssr_picker_covers_every_real_op_progress_kind():
    """The picker CASE, tone_by_kind, and kind_label must each name every
    real op_progress kind — no kind may silently fall to ELSE 99 with a
    generic tone/label, since that's exactly the drift this fixes."""
    body = _ssr_body()
    kinds = _real_op_progress_kinds()
    # the picker CASE + both maps live in the SSR body
    picker = body[body.index("CASE kind"):body.index("tone_by_kind = {")]
    for kind in kinds:
        assert f"'{kind}'" in picker, (
            f"v0.50.89: op_progress kind {kind!r} missing from the SSR "
            f"picker CASE — it would fall to ELSE 99"
        )
    tone_block = body[body.index("tone_by_kind = {"):body.index("kind_label = {")]
    label_block = body[body.index("kind_label = {"):body.index('state["op_running"]')]
    for kind in kinds:
        assert f'"{kind}"' in tone_block, (
            f"v0.50.89: kind {kind!r} missing a tone_by_kind entry"
        )
        assert f'"{kind}"' in label_block, (
            f"v0.50.89: kind {kind!r} missing a kind_label entry"
        )


def test_ssr_new_kinds_tone_and_label_match_ops_js():
    """The three newly-added kinds must carry the same tone + label that
    ops.js uses, or the SSR paint disagrees with the first JS poll."""
    body = _ssr_body()
    for kind, tone, label in [
        ("bulk_lps", "plex", "BULK LET PLEX SERVE"),
        ("tvdb_bridge", "tdb", "TVDB BRIDGE"),
        ("cloud_themes_backup", "plex", "DOWNLOAD PLEX BACKUP"),
    ]:
        assert f'"{kind}": "{tone}"' in body, (
            f"v0.50.89: {kind} tone must be {tone} (mirror ops.js)"
        )
        assert f'"{kind}": "{label}"' in body, (
            f"v0.50.89: {kind} label must be {label!r} (mirror ops.js)"
        )
    # cross-source pin: ops.js still binds those tones/labels
    ops = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    assert "bulk_lps:       'plex'" in ops
    assert "cloud_themes_backup: 'plex'" in ops
    assert "tvdb_bridge:    'tdb'" in ops


# ── 2. behavioral: an SSR paint while bulk_lps runs shows its label ─────


def test_ssr_paints_bulk_lps_label_when_running(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db, get_conn
    from app.core.events import now_iso
    from app.web.api import create_app
    from fastapi.testclient import TestClient

    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")

    now = now_iso()
    with get_conn(settings.db_path) as conn:
        conn.execute(
            "INSERT INTO op_progress (op_id, kind, status, started_at, "
            "updated_at, stage_label) VALUES "
            "('op1', 'bulk_lps', 'running', ?, ?, NULL)",
            (now, now),
        )

    client = TestClient(create_app(settings))
    r = client.get("/", headers={"X-Authentik-Username": "testadmin"})
    assert r.status_code == 200
    assert "BULK LET PLEX SERVE" in r.text, (
        "v0.50.89: a running bulk_lps op must SSR its real label, not the "
        "'…' fallback"
    )
    assert "op-tone-plex" in r.text
