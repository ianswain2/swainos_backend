from __future__ import annotations

from src.integrations.salesforce_bulk_client import SalesforceApiBudget, SalesforceBulkReadOnlyClient


def _build_client() -> SalesforceBulkReadOnlyClient:
    client = SalesforceBulkReadOnlyClient(
        login_base_url="https://login.salesforce.com",
        client_id="client",
        client_secret="secret",
        api_version="v61.0",
        timeout_seconds=10.0,
        budget=SalesforceApiBudget(max_jobs_per_run=2, max_polls_per_run=10, max_result_pages_per_job=5),
    )
    # Set auth state directly so allowlist checks can be evaluated without network.
    client._access_token = "token"  # type: ignore[attr-defined]
    client._instance_url = "https://example.my.salesforce.com"  # type: ignore[attr-defined]
    return client


def test_allowlist_accepts_bulk_query_paths() -> None:
    client = _build_client()
    client._assert_allowed_url(  # type: ignore[attr-defined]
        "https://example.my.salesforce.com/services/data/v61.0/jobs/query", "POST"
    )
    client._assert_allowed_url(  # type: ignore[attr-defined]
        "https://example.my.salesforce.com/services/data/v61.0/jobs/query/750XX0000000001/results",
        "GET",
    )


def test_allowlist_blocks_non_bulk_paths() -> None:
    client = _build_client()
    try:
        client._assert_allowed_url(  # type: ignore[attr-defined]
            "https://example.my.salesforce.com/services/data/v61.0/sobjects/Account/001XX0000000001",
            "PATCH",
        )
    except RuntimeError as exc:
        assert "Blocked non-allowlisted Salesforce path" in str(exc) or "Disallowed Salesforce method" in str(
            exc
        )
    else:
        raise AssertionError("Expected Salesforce path allowlist to reject mutation path")

