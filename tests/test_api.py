"""API tests with a fake DB connection: they check routing and that user input is
always passed as bound parameters, never formatted into SQL."""

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from src.api.main import app, get_connection


class FakeResult:
    def __init__(self, rows):
        self._rows = [SimpleNamespace(_mapping=r) for r in rows]

    def __iter__(self):
        return iter(self._rows)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeConnection:
    def __init__(self, rows=None):
        self.rows = rows if rows is not None else []
        self.calls = []

    def execute(self, query, params=None):
        self.calls.append((str(query), params or {}))
        return FakeResult(self.rows)


@pytest.fixture
def conn():
    fake = FakeConnection()
    app.dependency_overrides[get_connection] = lambda: fake
    yield fake
    app.dependency_overrides.clear()


client = TestClient(app)


def test_search_route_is_not_shadowed_by_mmsi_route(conn):
    resp = client.get("/vessels/search", params={"name": "MAERSK"})
    assert resp.status_code == 200
    sql, params = conn.calls[0]
    assert "ILIKE :name" in sql
    assert params["name"] == "%MAERSK%"


def test_search_input_is_bound_not_interpolated(conn):
    payload = "x' OR 1=1 --"
    resp = client.get("/vessels/search", params={"name": payload, "vessel_type": 70})
    assert resp.status_code == 200
    sql, params = conn.calls[0]
    assert payload not in sql
    assert params["name"] == f"%{payload}%"
    assert params["vessel_type"] == 70


def test_unknown_vessel_returns_404(conn):
    assert client.get("/vessels/219018671").status_code == 404


def test_vessels_pagination_is_validated(conn):
    assert client.get("/vessels", params={"limit": 5000}).status_code == 422
    client.get("/vessels", params={"limit": 10, "offset": 20})
    assert conn.calls[-1][1] == {"limit": 10, "offset": 20}


def test_statistics_do_not_join_positions(conn):
    conn.rows = [{"total_vessels": 0, "total_positions": 0}]
    client.get("/statistics")
    assert all("JOIN" not in sql.upper() for sql, _ in conn.calls)
