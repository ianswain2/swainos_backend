from __future__ import annotations

from src.core.supabase import SupabaseClient


def test_health_check(client):
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["status"] == "ok"
    assert payload["pagination"] is None
    assert payload["meta"]["source"] == "system"


def test_readiness_check_success(client, monkeypatch):
    def _ok_select(self, table, select, filters=None, limit=None, offset=None, order=None, count=False):
        _ = table, select, filters, limit, offset, order, count
        return [{"id": "ok"}], 1

    monkeypatch.setattr(SupabaseClient, "select", _ok_select)
    response = client.get("/api/v1/health/ready")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["status"] == "ready"


def test_readiness_check_failure(client, monkeypatch):
    def _failing_select(self, table, select, filters=None, limit=None, offset=None, order=None, count=False):
        _ = table, select, filters, limit, offset, order, count
        raise RuntimeError("supabase unavailable")

    monkeypatch.setattr(SupabaseClient, "select", _failing_select)
    response = client.get("/api/v1/health/ready")
    assert response.status_code == 503
    payload = response.json()
    assert payload["error"]["code"] == "dependency_unavailable"
