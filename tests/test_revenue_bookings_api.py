from __future__ import annotations


def test_list_revenue_bookings(client):
    response = client.get("/api/v1/revenue-bookings")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["bookingNumber"] == "BK-1001"
    assert payload["pagination"]["totalItems"] == 1
    assert payload["meta"]["calculationVersion"] == "v1"


def test_get_revenue_booking_detail(client):
    response = client.get("/api/v1/revenue-bookings/booking-1")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["confirmationNumber"] == "CONF-1"
    assert payload["meta"]["timeWindow"] == "na"