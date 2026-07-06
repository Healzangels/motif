"""v0.51.90 — DASHBOARD // DOWNLOAD ACTIVITY date alignment (client UTC + server).

The 30-slot bar axis was built with LOCAL date arithmetic (`d.setDate(...)`) but
keyed as UTC (`d.toISOString().slice(0,10)`), while the server buckets on UTC
`DATE(created_at)`. In any negative-UTC-offset zone (e.g. US) in the evening the
last slot landed on tomorrow's UTC date → the "today" bar was mislabeled/dropped.
Fixed the client to iterate in UTC. Also aligned the server window to exactly the
30 UTC calendar dates the client renders (was a rolling 30x24h window spanning a
31st partial date with no client bucket → that day's downloads were dropped).

Source guards (a wall-clock/timezone property of a real browser — can't be
exercised headless; the server side is a query-shape pin).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _activity_loop() -> str:
    i = APP_JS.index("const have = new Map(rows.map((r) => [r.day, r.count]));")
    return APP_JS[i:i + 700]


def test_client_iterates_dates_in_utc():
    block = _activity_loop()
    assert "d.setUTCDate(d.getUTCDate() - i)" in block, (
        "v0.51.90: the axis must iterate in UTC to match the server's UTC "
        "DATE(created_at) keys")
    # the local form (the bug) must be gone from this loop.
    assert "d.setDate(d.getDate() - i)" not in block


def test_server_window_is_date_aligned_to_30_calendar_days():
    # scope to the daily-downloads chart query — has_insight_downloads (a
    # separate 30-day EXISTS gate) legitimately keeps the rolling window.
    i = API_PY.index("SELECT DATE(created_at) AS day, COUNT(*) AS n")
    q = API_PY[i:i + 700]  # spans the WHERE + its explanatory comment
    assert "DATE(created_at) >= DATE('now', '-29 days')" in q, (
        "v0.51.90: the download-activity chart window must be DATE()-aligned to "
        "the 30 UTC calendar dates the client renders")
    # the old rolling-hours form must be gone from THIS query.
    assert "datetime(created_at) >= datetime('now', '-30 days')" not in q
