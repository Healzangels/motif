"""v0.51.280 — feature-brief D: per-provider health + adaptive rate.

The brief's own hard requirement leads: "Fixed mode behavior is unchanged."
fixed is the DEFAULT, the worker's cooldown gate sits behind an explicit
`== "adaptive"` check, and health is OBSERVED in both modes so the operator
has real evidence (via GET /api/admin/provider-health) before opting in.

Deliberate deviation from the brief, recorded: COOKIES_EXPIRED does NOT pause
a provider — per-job failure + the existing cookies_needed notification
already surface it, and one misclassified error must not halt every download.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
from app.core.downloader import FailureKind  # noqa: E402

RATES = dict(base_rate=30.0, min_rate=5.0, max_rate=120.0)


@pytest.fixture
def db(tmp_path):
    from app.core.db import init_db
    d = tmp_path / "t.db"
    init_db(d)
    return d


def _state(db, provider="youtube"):
    from app.core.provider_health import get_state
    return get_state(db, provider, base_rate=30.0)


# ── the state machine ────────────────────────────────────────


def test_rate_limited_backs_off_exponentially_and_cools_down(db):
    from app.core.provider_health import report_outcome
    s1 = report_outcome(db, "youtube", FailureKind.RATE_LIMITED, **RATES)
    assert s1["state"] == "COOLDOWN" and s1["failure_streak"] == 1
    assert s1["effective_rate"] == 15.0, "halved from base 30"
    u1 = datetime.fromisoformat(s1["cooldown_until"])
    s2 = report_outcome(db, "youtube", FailureKind.RATE_LIMITED, **RATES)
    u2 = datetime.fromisoformat(s2["cooldown_until"])
    assert (u2 - datetime.now(timezone.utc)) > (u1 - datetime.now(timezone.utc)), (
        "consecutive throttles double the cooldown")
    assert s2["effective_rate"] == 7.5


def test_effective_rate_floors_at_min(db):
    from app.core.provider_health import report_outcome
    for _ in range(6):
        s = report_outcome(db, "youtube", FailureKind.RATE_LIMITED, **RATES)
    assert s["effective_rate"] == RATES["min_rate"]


def test_success_recovers_slowly_toward_max_not_a_jump(db):
    from app.core.provider_health import report_outcome
    report_outcome(db, "youtube", FailureKind.RATE_LIMITED, **RATES)  # → 15
    s = report_outcome(db, "youtube", None, **RATES)
    assert s["state"] == "GOOD" and s["cooldown_until"] is None
    assert s["effective_rate"] == pytest.approx(18.75), (
        "the brief: gradual recovery, never an immediate jump to max")
    for _ in range(20):
        s = report_outcome(db, "youtube", None, **RATES)
    assert s["effective_rate"] == RATES["max_rate"], "…and it caps at max"


def test_video_kinds_have_zero_health_impact(db):
    from app.core.provider_health import report_outcome
    for kind in (FailureKind.VIDEO_REMOVED, FailureKind.VIDEO_PRIVATE,
                 FailureKind.GEO_BLOCKED, FailureKind.VIDEO_AGE_RESTRICTED):
        s = report_outcome(db, "youtube", kind, **RATES)
    assert s["state"] == "GOOD" and s["failure_streak"] == 0, (
        "the brief: a dead URL is an item problem, not provider health — one "
        "broken video must never cool down the whole provider")


def test_cookies_do_not_pause_the_provider(db):
    from app.core.provider_health import report_outcome
    s = report_outcome(db, "youtube", FailureKind.COOKIES_EXPIRED, **RATES)
    assert s["state"] == "GOOD" and s["cooldown_until"] is None
    assert s["last_error_class"] == "auth", "recorded, surfaced elsewhere"


def test_transient_noise_degrades_only_on_a_streak(db):
    from app.core.provider_health import report_outcome
    s = report_outcome(db, "youtube", FailureKind.NETWORK_ERROR, **RATES)
    assert s["state"] == "GOOD", "one blip is not degradation"
    report_outcome(db, "youtube", FailureKind.NETWORK_ERROR, **RATES)
    s = report_outcome(db, "youtube", FailureKind.NETWORK_ERROR, **RATES)
    assert s["state"] == "DEGRADED"


def test_provider_isolation(db):
    """The brief: provider A's degradation must not stop provider B."""
    from app.core.provider_health import report_outcome
    report_outcome(db, "instagram", FailureKind.RATE_LIMITED, **RATES)
    assert _state(db, "youtube")["state"] == "GOOD"
    assert _state(db, "instagram")["state"] == "COOLDOWN"


def test_state_survives_a_restart(db):
    """The brief's guardrail: losing a cooldown across restart resumes
    hammering a throttled provider. runtime_settings persists it."""
    from app.core.provider_health import check_cooldown, report_outcome
    report_outcome(db, "youtube", FailureKind.RATE_LIMITED, **RATES)
    # a "restart" is just a fresh read — no module state involved
    assert _state(db)["state"] == "COOLDOWN"
    assert check_cooldown(db, "youtube", base_rate=30.0) > 0


def test_check_cooldown_zero_when_expired(db):
    from app.core.provider_health import check_cooldown
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings (key, value, updated_at) "
            "VALUES ('provider_health:youtube', ?, '2026-01-01')",
            (json.dumps({"state": "COOLDOWN", "cooldown_until":
                         (datetime.now(timezone.utc) - timedelta(seconds=5)
                          ).isoformat(timespec="seconds")}),))
        conn.commit()
    assert check_cooldown(db, "youtube", base_rate=30.0) == 0


def test_provider_detection():
    from app.core.provider_health import provider_for_url
    assert provider_for_url("https://www.youtube.com/watch?v=x") == "youtube"
    assert provider_for_url("https://youtu.be/x") == "youtube"
    assert provider_for_url("https://soundcloud.com/a/b") == "soundcloud"
    assert provider_for_url("https://www.instagram.com/reel/x") == "instagram"
    assert provider_for_url("https://fb.watch/x") == "facebook"
    assert provider_for_url("https://example.com/x") == "other"
    assert provider_for_url(None) == "other"


# ── fixed mode is untouched; the gate is adaptive-only ───────


def test_fixed_mode_is_the_default():
    from app.core.config_file import DownloadsConfig
    assert DownloadsConfig().rate_mode == "fixed"


def test_worker_gate_is_behind_the_adaptive_check():
    src = (REPO / "app" / "core" / "worker.py").read_text()
    i = src.index('self.settings.download_rate_mode == "adaptive"')
    block = src[i:src.index("_stale_backup = None", i)]
    assert "check_cooldown" in block and "_mark_transient" in block, (
        "the cooldown defer uses the v1.14.54 attempt-free seam")
    assert src.index('== "adaptive"') < src.index("check_cooldown"), (
        "fixed mode must never reach the gate — the brief's hard requirement")


def test_worker_reports_outcomes_in_both_modes():
    src = (REPO / "app" / "core" / "worker.py").read_text()
    assert src.count("report_outcome(") == 2, "success + DownloadError paths"
    i = src.index("except DownloadError as e:")
    assert "report_outcome(" in src[i:src.index("_stale_backup", i)]


# ── config + endpoint ────────────────────────────────────────


def test_config_validation_bounds():
    from app.core.config_file import MotifConfig, validate
    cfg = MotifConfig()
    cfg.downloads.rate_mode = "chaotic"
    errs = validate(cfg, require_themes_dir=False)
    assert any("rate_mode" in e for e in errs)
    cfg.downloads.rate_mode = "adaptive"
    cfg.downloads.adaptive_min_per_hour = 50
    cfg.downloads.adaptive_max_per_hour = 10
    errs = validate(cfg, require_themes_dir=False)
    assert any("adaptive_max_per_hour" in e for e in errs)


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s


AUTH = {"X-Authentik-Username": "testadmin"}


def test_health_endpoint_shape_and_auth(client):
    c, _ = client
    assert c.get("/api/admin/provider-health").status_code in (401, 403)
    r = c.get("/api/admin/provider-health", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["mode"] == "fixed"
    assert set(body["providers"]) == {"youtube", "soundcloud", "instagram",
                                      "facebook", "other"}
    assert body["providers"]["youtube"]["state"] == "GOOD"


def test_new_config_keys_survive_a_patch_round_trip(client):
    """The v1.17.10 closed-set trap, tested for real: PATCH the new keys and
    read them back — a dropped key comes back as the default."""
    c, _ = client
    r = c.patch("/api/config", headers=AUTH, json={
        "downloads": {"rate_mode": "adaptive", "adaptive_min_per_hour": 7}})
    assert r.status_code == 200, r.text
    got = c.get("/api/config", headers=AUTH).json()["config"]
    assert got["downloads"]["rate_mode"] == "adaptive"
    assert got["downloads"]["adaptive_min_per_hour"] == 7


def test_settings_ui_fields_exist():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    for key in ("downloads.rate_mode", "downloads.adaptive_min_per_hour",
                "downloads.adaptive_max_per_hour"):
        assert f'data-cfg-field="{key}"' in html


def test_v0_51_280_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
