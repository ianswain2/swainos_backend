from __future__ import annotations


def test_cashflow_summary(client):
    response = client.get("/api/v1/cash-flow/summary")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["netCashTotal"] == 400
    assert payload["pagination"]["totalItems"] == 1
    assert payload["meta"]["currency"] is None


def test_cashflow_timeseries(client):
    response = client.get("/api/v1/cash-flow/timeseries")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["netCash"] == 400
    assert payload["pagination"]["totalItems"] == 1


def test_deposits_summary(client):
    response = client.get("/api/v1/deposits/summary")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["totalDeposits"] == 1000
    assert payload["pagination"]["totalItems"] == 1


def test_payments_out_summary(client):
    response = client.get("/api/v1/payments-out/summary")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["outstandingAmount"] == 400
    assert payload["pagination"]["totalItems"] == 1


def test_booking_forecasts(client):
    response = client.get("/api/v1/booking-forecasts")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["projectedBookings"] == 5
    assert payload["pagination"]["totalItems"] == 1


def test_invalid_time_window_returns_error_envelope(client):
    response = client.get("/api/v1/cash-flow/summary?time_window=bad")
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"]["code"] == "bad_request"


def test_validation_error_returns_error_envelope(client):
    response = client.get("/api/v1/booking-forecasts?lookback_months=1")
    assert response.status_code == 422
    payload = response.json()
    assert payload["error"]["code"] == "validation_error"