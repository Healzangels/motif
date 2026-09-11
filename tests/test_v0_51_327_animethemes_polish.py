"""v0.51.327 — ANIME THEMES tag 5: polish; the series is complete.

1. `refresh_bridge` + the weekly `animethemes_bridge_refresh` scheduler job:
   ETag-conditional, and a no-op until the operator has used the feature
   (no cache file → no fetch — spec §3.7).
2. One `bulk_action_completed` digest per APPLY SELECTED run: the page reports
   the batch to POST /api/admin/animethemes-sweep/digest; motif logs it and
   notifies once — never N per-row pings.
3. README section with attribution (spec §6 decision 4), CLAUDE.md map, the
   settings hint and the README notification tables name the digest.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import httpx
import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from app.core import animethemes as at  # noqa: E402

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
SCHEDULER = (REPO / "app" / "core" / "scheduler.py").read_text()
SETTINGS = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
README = (REPO / "README.md").read_text()
CLAUDE = (REPO / "CLAUDE.md").read_text()
SPEC = (REPO / "docs" / "specs" / "ANIMETHEMES_SPEC.md").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


# ── 1. the bridge refresh ─────────────────────────────────────


def _http(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_refresh_is_skipped_until_the_feature_has_been_used(tmp_path):
    calls = []
    hc = _http(lambda r: (calls.append(r), httpx.Response(200, json=[]))[1])
    assert at.refresh_bridge(tmp_path / "animethemes", client=hc) == "skipped"
    assert calls == [], "no cache file → no fetch (spec §3.7: never a fetch the operator didn't cause)"


def test_refresh_reports_unchanged_on_304_and_restarts_the_ttl(tmp_path):
    d = tmp_path / "animethemes"; d.mkdir()
    p = d / at.BRIDGE_FILENAME
    p.write_text("[]")
    (d / (at.BRIDGE_FILENAME + ".etag")).write_text('"abc"')
    old = time.time() - 30 * 86400
    os.utime(p, (old, old))
    seen = {}
    hc = _http(lambda r: (seen.update(inm=r.headers.get("if-none-match")), httpx.Response(304))[1])
    assert at.refresh_bridge(d, client=hc) == "unchanged"
    assert seen["inm"] == '"abc"', "the conditional fetch carries the stored ETag"
    assert time.time() - p.stat().st_mtime < 60, "mtime touched → load_bridge's 7-day TTL restarts"


def test_refresh_reports_refreshed_and_replaces_the_file_atomically(tmp_path):
    d = tmp_path / "animethemes"; d.mkdir()
    p = d / at.BRIDGE_FILENAME
    p.write_text("[]")
    payload = [{"anidb_id": i, "thetvdb_id": 1000 + i} for i in range(1200)]
    hc = _http(lambda r: httpx.Response(200, json=payload, headers={"etag": '"new"'}))
    assert at.refresh_bridge(d, client=hc) == "refreshed"
    assert len(__import__("json").loads(p.read_text())) == 1200
    assert (d / (at.BRIDGE_FILENAME + ".etag")).read_text() == '"new"'
    assert not p.with_suffix(".tmp").exists()


def test_refresh_raises_so_the_job_can_log_it(tmp_path):
    d = tmp_path / "animethemes"; d.mkdir()
    (d / at.BRIDGE_FILENAME).write_text("[]")
    hc = _http(lambda r: httpx.Response(503))
    with pytest.raises(at.AnimeThemesError):
        at.refresh_bridge(d, client=hc)


def test_weekly_job_registered_and_logs_only_what_matters(tmp_path, monkeypatch):
    job = slice_between(SCHEDULER, "    scheduler.add_job(\n        _refresh_animethemes_bridge", "    )\n")
    assert 'id="animethemes_bridge_refresh"' in job and 'day_of_week="sun"' in job and 'hour="3"' in job
    assert "anime-lists bridge refresh Sun 03:20" in SCHEDULER, "the boot log line names it"
    from app.core import scheduler as sch
    logged = []
    monkeypatch.setattr(sch, "log_event", lambda db, **kw: logged.append((kw["level"], kw["component"], kw["message"])))

    class S:  # the two settings fields the job reads
        config_dir = tmp_path
        db_path = tmp_path / "motif.db"
    outcomes = iter(["skipped", "unchanged", "refreshed"])
    monkeypatch.setattr("app.core.animethemes.refresh_bridge", lambda cache_dir: next(outcomes))
    for _ in range(3):
        sch._refresh_animethemes_bridge(S())
    assert logged == [("INFO", "scheduler", "AnimeThemes bridge refreshed (Fribb/anime-lists)")], (
        "skipped + unchanged stay silent; a refresh is one INFO line")

    def boom(cache_dir):
        raise at.AnimeThemesError(503, "x")
    monkeypatch.setattr("app.core.animethemes.refresh_bridge", boom)
    sch._refresh_animethemes_bridge(S())  # never raises
    assert logged[-1][0] == "WARNING" and "bridge refresh failed" in logged[-1][2]


# ── 2. the apply digest ───────────────────────────────────────


@pytest.fixture
def app_env(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path); init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s


def test_digest_notifies_once_with_the_batch(app_env, monkeypatch):
    client, s = app_env
    from app.core import notify
    from app.web import api as api_mod
    sent, logged = [], []
    monkeypatch.setattr(notify, "dispatch", lambda db, cfg, **kw: sent.append(kw))
    monkeypatch.setattr(api_mod, "log_event", lambda db, **kw: logged.append(kw))
    r = client.post("/api/admin/animethemes-sweep/digest", headers=AUTH,
                    json={"queued": 3, "failed": 1, "backups": 2, "titles": ["Bleach", "Chainsaw Man", "Frieren"], "total_titles": 3})
    assert r.status_code == 200 and r.json() == {"ok": True, "notified": True}
    assert len(sent) == 1, "one digest, never per-row pings"
    kw = sent[0]
    assert kw["event_kind"] == "bulk_action_completed"
    assert kw["title"] == "✅ Bulk ANIME THEMES done — 3 queued"
    assert "3 openings queued" in kw["body"] and "2 as backups" in kw["body"] and "1 failed" in kw["body"]
    assert "🎵 Queued: Bleach · Chainsaw Man · Frieren" in kw["body"]
    ev = [x for x in logged if x["message"].startswith("ANIME THEMES apply by testadmin")]
    assert len(ev) == 1 and "3 queued (2 as backups), 1 failed" in ev[0]["message"]
    assert ev[0]["detail"]["titles"] == ["Bleach", "Chainsaw Man", "Frieren"]


def test_digest_is_silent_for_an_empty_batch_and_caps_titles(app_env, monkeypatch):
    client, _ = app_env
    from app.core import notify
    from app.web import api as api_mod
    sent, logged = [], []
    monkeypatch.setattr(notify, "dispatch", lambda db, cfg, **kw: sent.append(kw))
    monkeypatch.setattr(api_mod, "log_event", lambda db, **kw: logged.append(kw))
    assert client.post("/api/admin/animethemes-sweep/digest", headers=AUTH, json={"queued": 0}).json() == {"ok": True, "notified": False}
    assert sent == [] and logged == [], "an empty batch leaves no event and no notification"
    client.post("/api/admin/animethemes-sweep/digest", headers=AUTH,
                json={"queued": 12, "titles": [f"T{i}" for i in range(12)], "total_titles": 12})
    import re
    assert len(re.findall(r"\bT\d+", sent[-1]["body"])) == 10 and "(+2 more)" in sent[-1]["body"]
    assert client.post("/api/admin/animethemes-sweep/digest", json={"queued": 1}).status_code in (401, 403)


def test_page_reports_the_batch_once_after_the_loop():
    b = slice_between(APP_JS, "  function bindAnimeThemesSweep() {", "  function bindLoudnessAudit() {")
    apply = slice_between(b, "applyBtn.addEventListener('click'", "\n    });")
    assert apply.index("for (const r of targets)") < apply.index("/api/admin/animethemes-sweep/digest"), "after the loop"
    assert "queued: ok, failed, backups: done.filter((r) => r.plex_has_theme).length" in apply
    assert "titles: done.slice(0, 10).map((r) => r.title), total_titles: done.length" in apply
    assert "if (ok || failed) {" in apply
    assert API_PY.count('event_kind="bulk_action_completed"') == 5


# ── 3. the words ──────────────────────────────────────────────


def test_settings_hint_readme_and_claude_md_name_the_feature():
    hint = slice_between(SETTINGS, 'data-cfg-field="notifications.events.bulk_action_completed"', "</label>")
    assert "ANIME THEMES apply run" in hint
    assert "## Anime themes (AnimeThemes.moe)" in README
    sec = slice_between(README, "## Anime themes (AnimeThemes.moe)", "## Plex Scans")
    assert "https://animethemes.moe" in sec and "https://github.com/Fribb/anime-lists" in sec
    assert "**Attribution.**" in sec and "non-commercial" in sec and "no licence" in sec, "spec §6 decision 4"
    assert "/admin/anime-themes" in sec and "Sunday 03:20 UTC" in sec
    assert "`✅ Bulk ANIME THEMES done — N queued`" in README
    assert "* `/admin/anime-themes` —" in README
    assert "## AnimeThemes source (v0.51.314–.327)" in CLAUDE
    cl = slice_between(CLAUDE, "## AnimeThemes source", "## Commit + release conventions")
    assert "refresh_bridge" in cl and "animethemes_bridge_refresh" in cl and "decision 5" in cl
    assert "pi.theme_id → themes" in cl, "the orphan-linkage trap is written down"
    assert "shipped as v0.51.327" in SPEC and "DONE v0.51.327" in SPEC


def test_v0_51_327_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.327: ANIME THEMES tag 5" in init_py
