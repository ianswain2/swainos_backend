from __future__ import annotations


def test_health_check(client):
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["status"] == "ok"
    assert payload["pagination"] is None
    assert payload["meta"]["source"] == "system"
