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


def test_cashflow_forecast_3m_returns_monthly_points(client):
    response = client.get("/api/v1/cash-flow/forecast?time_window=3m")
    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["timeWindow"] == "3m"
    assert payload["data"]
    first_currency = payload["data"][0]
    assert first_currency["timeWindow"] == "3m"
    assert first_currency["points"]
    first_point = first_currency["points"][0]
    # The forecast contract is month-bucketed date ranges, not weekly buckets.
    assert len(first_point["periodStart"]) == 10
    assert len(first_point["periodEnd"]) == 10


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
    assert payload["data"][0]["totalOutstandingAmount"] == 400
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
