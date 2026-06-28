"""v1.23.57 — the collections filter panel reserves the ED row's height.

Collections have no editions, so the ED filter row is dropped — but that left the
file-axis column (DL / PL / LINK / ED) one row shorter, shrinking the whole filter
panel so it jumped size when switching between a library tab and collections
(the user). A spacer now holds the ED row's place on collections so both panels are
the same height.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


def test_collections_has_spacer_not_ed_row(admin_client):
    html = admin_client.get("/collections", headers=AUTH).text
    assert "pill-filter-spacer" in html, "collections must reserve the ED row's height"
    assert 'aria-label="EDITION pill filter"' not in html, "collections have no editions"


def test_library_tab_has_ed_row_not_spacer(admin_client):
    html = admin_client.get("/movies", headers=AUTH).text
    assert 'aria-label="EDITION pill filter"' in html, "library tabs keep the ED row"
    assert "pill-filter-spacer" not in html, "a library tab doesn't need the spacer"


def test_spacer_reserves_one_ed_row_height():
    # v1.23.59: the spacer matches the ED row's IN-FILTER-ROW chip height (20px,
    # the v1.12.48 shared height), not the standalone .ed-pill-btn's 22px — else
    # collections renders ~2px taller than the library filter panel.
    i = APP_CSS.index(".pill-filter-spacer {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "min-height: 20px" in block
