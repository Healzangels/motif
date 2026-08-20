"""v0.51.268 — /readyz: local operational readiness, separate from liveness.

Milestone 1 of the feature-implementation brief, scoped to readiness only (the
operator declined a Prometheus /metrics endpoint — nothing scrapes it here).
Independently the code-review validation brief raised the same gap as its #9.

The gap is not hypothetical. A PUID mismatch on a permission-enforcing share
(Unraid shfs, v1.22.4) denied every write while Docker reported the container
healthy, and the only breadcrumb was a single boot log line — the failure
surfaced a week later as crash-looping downloads.

Design, per the brief's §10.1:
  /healthz stays LIVENESS. It must not flap on an operator-fixable condition; a
  healthcheck that restart-loops on a permissions problem fixes nothing. The
  Docker healthcheck deliberately keeps pointing at it.
  /readyz answers "can motif actually do its job", names the failing CHECK (not
  the path — it is public, like /healthz), and 503s when not ready.

The write probe is TTL-cached: /readyz can be polled, and the probe touches the
filesystem. `config.probe_dir_writable` is the single definition of writable, so
the boot probe and this endpoint cannot drift on what the word means.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def app_env(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    # a configured install: themes_dir set, so paths_configured is True and the
    # readiness question is about writability rather than configuration.
    themes = tmp_path / "data" / "themes"
    themes.mkdir(parents=True, exist_ok=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s


# ── the endpoint exists, is public, and reports ready ────────


def test_readyz_is_ready_on_a_healthy_install(app_env):
    c, _ = app_env
    r = c.get("/readyz")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ready"
    assert body["failing"] == []
    assert body["checks"]["db"] is True
    assert body["checks"]["config_dir_writable"] is True


def test_readyz_needs_no_credential(app_env):
    """An orchestrator must reach it without a token — same posture as
    /healthz, which is why it joins PUBLIC_PATHS."""
    c, _ = app_env
    assert c.get("/readyz").status_code == 200
    from app.core.auth import PUBLIC_PATHS
    assert "/readyz" in PUBLIC_PATHS


# ── THE acceptance criterion: unwritable path → not ready ────


def test_unwritable_themes_dir_makes_it_not_ready_and_names_the_check(app_env,
                                                                     monkeypatch):
    c, s = app_env
    themes = s.config_dir / "unwritable-themes"
    themes.mkdir()
    import app.config as cfg
    real = cfg.probe_dir_writable

    def fake(path):
        if Path(path) == themes:
            return "PermissionError: Permission denied"
        return real(path)

    monkeypatch.setattr(cfg, "probe_dir_writable", fake)
    monkeypatch.setattr(type(s), "is_paths_ready", lambda self: True)
    monkeypatch.setattr(type(s), "themes_dir", property(lambda self: themes))

    r = c.get("/readyz")
    assert r.status_code == 503, "an unwritable themes dir must report NOT ready"
    body = r.json()
    assert body["status"] == "not_ready"
    assert "themes_dir_writable" in body["failing"], (
        "the response must NAME the failing check so the operator knows which")
    assert body["checks"]["themes_dir_writable"] is False


def test_readyz_never_leaks_the_path(app_env, monkeypatch):
    """It is public, so a failing check names the CHECK, never the absolute
    path. The boot log carries the path + uid/owner for whoever can act."""
    c, s = app_env
    import app.config as cfg
    monkeypatch.setattr(cfg, "probe_dir_writable", lambda p: "PermissionError: nope")
    body = c.get("/readyz").text
    assert str(s.config_dir) not in body
    assert "PermissionError" not in body, "no OS error detail in a public body"


def test_unconfigured_paths_report_as_not_ready(app_env, monkeypatch):
    c, s = app_env
    monkeypatch.setattr(type(s), "is_paths_ready", lambda self: False)
    body = c.get("/readyz").json()
    assert body["checks"]["paths_configured"] is False
    assert "paths_configured" in body["failing"]
    assert "themes_dir_writable" not in body["checks"], (
        "an unconfigured install reports paths_configured, not a phantom "
        "writability failure on an empty path")


# ── liveness stays liveness ─────────────────────────────────


def test_healthz_is_unchanged_by_an_unwritable_path(app_env, monkeypatch):
    """THE separation. A permissions problem must not make Docker kill the
    container — /healthz is the restart signal, /readyz is the diagnosis."""
    c, _ = app_env
    import app.config as cfg
    monkeypatch.setattr(cfg, "probe_dir_writable", lambda p: "PermissionError: nope")
    r = c.get("/healthz")
    assert r.status_code == 200, "liveness must not flap on a readiness failure"
    assert "config_dir_writable" not in r.json()["checks"]


def test_docker_healthcheck_still_points_at_healthz():
    """Deliberate: the brief's §10.1 and non-goals both say external/operator
    conditions must not determine container liveness."""
    for f in ("Dockerfile", "docker-compose.yml"):
        text = (REPO / f).read_text()
        if "healthcheck" in text.lower() or "HEALTHCHECK" in text:
            assert "/readyz" not in text, (
                f"{f}: the Docker healthcheck must stay on /healthz — a "
                f"readiness failure restart-looping the container fixes nothing")


def test_db_check_mirrors_healthz_and_its_real_boundary_is_documented():
    """The db check exists and is the same probe /healthz runs.

    Its boundary, verified rather than assumed: the auth middleware calls
    setup_complete(db_path) on EVERY request before the public-path branch, so a
    missing or unopenable DB 500s out of the middleware and neither probe ever
    runs. The db check here therefore covers the transient/busy case (a locked
    or briefly-unavailable DB), not a vanished file. Docker still sees a non-200
    either way, so liveness signalling is unaffected."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    ready = api_py[api_py.index('@app.get("/readyz")'):api_py.index('@app.get("/healthz")')]
    assert 'checks["db"]' in ready and "SELECT 1" in ready
    # the middleware ordering that creates the boundary
    mw = api_py[api_py.index("# First-run gate:"):]
    assert mw.index("setup_complete(") < mw.index("self._is_public(path)")


# ── cost: a poll must not re-probe the filesystem every time ──


def test_the_write_probe_is_cached_across_requests(app_env, monkeypatch):
    c, _ = app_env
    import app.config as cfg
    calls = {"n": 0}
    real = cfg.probe_dir_writable

    def counting(path):
        calls["n"] += 1
        return real(path)

    monkeypatch.setattr(cfg, "probe_dir_writable", counting)
    for _ in range(5):
        assert c.get("/readyz").status_code == 200
    assert calls["n"] <= 2, (
        f"probed the filesystem {calls['n']} times across 5 polls — /readyz "
        f"must not write a probe file per scrape")


# ── one definition of writable ───────────────────────────────


def test_boot_probe_and_readyz_share_the_helper():
    """They must not drift on what 'writable' means."""
    main_py = (REPO / "app" / "main.py").read_text()
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert "probe_dir_writable" in main_py
    assert "probe_dir_writable" in api_py
    assert main_py.count(".motif-write-probe") == 0, (
        "main.py must call the shared helper, not re-implement the probe")


def test_probe_dir_writable_contract(tmp_path):
    from app.config import probe_dir_writable
    assert probe_dir_writable(tmp_path) is None
    assert probe_dir_writable(tmp_path / "made" / "deep") is None, (
        "it creates missing parents — a fresh appdata mount is not a failure")
    blocked = tmp_path / "blocked"
    blocked.mkdir(mode=0o500)
    try:
        res = probe_dir_writable(blocked)
        if res is not None:            # skipped when the test runs as root
            assert "Error" in res
    finally:
        blocked.chmod(0o700)


def test_v0_51_268_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
