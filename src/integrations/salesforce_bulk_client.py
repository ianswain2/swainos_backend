from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx


@dataclass(frozen=True)
class SalesforceApiBudget:
    max_jobs_per_run: int
    max_polls_per_run: int
    max_result_pages_per_job: int


@dataclass(frozen=True)
class SalesforceApiCounters:
    jobs_created: int = 0
    polls_made: int = 0
    result_pages_read: int = 0


@dataclass(frozen=True)
class SalesforceCursor:
    last_systemmodstamp: Optional[str]
    last_id: Optional[str]


class SalesforceBulkReadOnlyClient:
    """Read-only Salesforce client for Bulk API 2.0 query/queryAll.

    Guardrails:
    - No mutation endpoints are exposed.
    - No automatic retries.
    - Request path allowlist enforcement.
    """

    def __init__(
        self,
        login_base_url: str,
        client_id: str,
        client_secret: str,
        api_version: str,
        timeout_seconds: float,
        budget: SalesforceApiBudget,
    ) -> None:
        self.login_base_url = login_base_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_version = api_version
        self.timeout_seconds = timeout_seconds
        self.budget = budget
        self._http = httpx.Client(timeout=timeout_seconds)
        self._access_token: Optional[str] = None
        self._instance_url: Optional[str] = None
        self._counters = SalesforceApiCounters()

    @property
    def counters(self) -> SalesforceApiCounters:
        return self._counters

    @property
    def instance_url(self) -> str:
        if not self._instance_url:
            raise RuntimeError("Salesforce instance URL is unavailable before token exchange")
        return self._instance_url

    def authenticate(self) -> None:
        token_url = f"{self.login_base_url}/services/oauth2/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        response = self._http.post(token_url, data=payload)
        response.raise_for_status()
        body = response.json()
        access_token = body.get("access_token")
        instance_url = body.get("instance_url")
        if not access_token or not instance_url:
            raise RuntimeError("Salesforce auth response missing access_token or instance_url")
        self._access_token = str(access_token)
        self._instance_url = str(instance_url).rstrip("/")

    def create_query_job(self, soql: str, operation: str = "queryAll") -> str:
        if operation not in {"query", "queryAll"}:
            raise ValueError(f"Unsupported operation `{operation}`; expected query or queryAll")
        self._guard_job_budget()
        path = f"/services/data/{self.api_version}/jobs/query"
        body = {"operation": operation, "query": soql, "contentType": "CSV", "lineEnding": "LF"}
        response = self._request("POST", path, json=body)
        job_id = response.json().get("id")
        if not job_id:
            raise RuntimeError("Salesforce query job response missing id")
        self._counters = SalesforceApiCounters(
            jobs_created=self._counters.jobs_created + 1,
            polls_made=self._counters.polls_made,
            result_pages_read=self._counters.result_pages_read,
        )
        return str(job_id)

    def wait_for_job(
        self,
        job_id: str,
        poll_interval_seconds: int,
        max_polls_per_job: int,
    ) -> Dict[str, Any]:
        path = f"/services/data/{self.api_version}/jobs/query/{job_id}"
        for _ in range(max_polls_per_job):
            self._guard_poll_budget()
            response = self._request("GET", path)
            self._counters = SalesforceApiCounters(
                jobs_created=self._counters.jobs_created,
                polls_made=self._counters.polls_made + 1,
                result_pages_read=self._counters.result_pages_read,
            )
            payload = response.json()
            state = str(payload.get("state") or "")
            if state in {"JobComplete", "Aborted", "Failed"}:
                return payload
            self._sleep_seconds(poll_interval_seconds)
        raise RuntimeError(f"Salesforce job {job_id} did not complete within {max_polls_per_job} polls")

    def get_all_result_rows(self, job_id: str) -> List[Dict[str, str]]:
        next_path = f"/services/data/{self.api_version}/jobs/query/{job_id}/results"
        rows: List[Dict[str, str]] = []
        pages_read = 0
        while next_path:
            if pages_read >= self.budget.max_result_pages_per_job:
                raise RuntimeError(
                    f"Result-page budget exceeded for job {job_id} "
                    f"(max={self.budget.max_result_pages_per_job})"
                )
            response = self._request("GET", next_path)
            page_text = response.text
            if page_text:
                reader = csv.DictReader(io.StringIO(page_text))
                rows.extend([dict(row) for row in reader])
            locator = response.headers.get("Sforce-Locator", "")
            pages_read += 1
            self._counters = SalesforceApiCounters(
                jobs_created=self._counters.jobs_created,
                polls_made=self._counters.polls_made,
                result_pages_read=self._counters.result_pages_read + 1,
            )
            if locator and locator.lower() != "null":
                next_path = (
                    f"/services/data/{self.api_version}/jobs/query/{job_id}/results?locator={locator}"
                )
            else:
                next_path = ""
        return rows

    def build_incremental_soql(
        self,
        object_name: str,
        select_fields: List[str],
        cursor: SalesforceCursor,
        upper_bound: datetime,
        include_is_deleted: bool = True,
    ) -> str:
        required = {"Id", "SystemModstamp"}
        if include_is_deleted:
            required.add("IsDeleted")
        fields = list(dict.fromkeys(select_fields + sorted(required)))
        upper = upper_bound.astimezone(timezone.utc).replace(microsecond=0).isoformat()
        where_fragments = [f"SystemModstamp < {self._soql_datetime_literal(upper)}"]
        if cursor.last_systemmodstamp and cursor.last_id:
            stamp = self._soql_datetime_literal(cursor.last_systemmodstamp)
            last_id = self._soql_string_literal(cursor.last_id)
            where_fragments.append(
                f"(SystemModstamp > {stamp} OR (SystemModstamp = {stamp} AND Id > {last_id}))"
            )
        elif cursor.last_systemmodstamp:
            stamp = self._soql_datetime_literal(cursor.last_systemmodstamp)
            where_fragments.append(f"SystemModstamp > {stamp}")
        where_sql = " AND ".join(where_fragments)
        return (
            f"SELECT {', '.join(fields)} "
            f"FROM {object_name} "
            f"WHERE {where_sql} "
            "ORDER BY SystemModstamp, Id"
        )

    @staticmethod
    def default_upper_bound(lag_minutes: int) -> datetime:
        return datetime.now(timezone.utc) - timedelta(minutes=lag_minutes)

    def _request(self, method: str, path: str, json: Optional[Dict[str, Any]] = None) -> httpx.Response:
        method = method.upper()
        if not self._access_token:
            raise RuntimeError("Salesforce client is not authenticated")
        if method not in {"GET", "POST"}:
            raise RuntimeError(f"Disallowed Salesforce method `{method}`")

        absolute_url = self._resolve_absolute_url(path)
        self._assert_allowed_url(absolute_url, method)

        headers = {"Authorization": f"Bearer {self._access_token}"}
        if json is not None:
            headers["Content-Type"] = "application/json"
            return self._http.request(method, absolute_url, headers=headers, json=json)
        return self._http.request(method, absolute_url, headers=headers)

    def _resolve_absolute_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            raise RuntimeError(f"Expected absolute Salesforce API path, got `{path}`")
        return f"{self.instance_url}{path}"

    def _assert_allowed_url(self, absolute_url: str, method: str) -> None:
        parsed = urlparse(absolute_url)
        instance_host = urlparse(self.instance_url).netloc
        login_host = urlparse(self.login_base_url).netloc
        if parsed.netloc not in {instance_host, login_host}:
            raise RuntimeError(f"Blocked Salesforce request host `{parsed.netloc}`")

        path = parsed.path
        token_path = "/services/oauth2/token"
        jobs_base = f"/services/data/{self.api_version}/jobs/query"
        allowed = (
            (method == "POST" and path == token_path)
            or path == jobs_base
            or path.startswith(f"{jobs_base}/")
        )
        if not allowed:
            raise RuntimeError(f"Blocked non-allowlisted Salesforce path `{path}`")

    def _guard_job_budget(self) -> None:
        if self._counters.jobs_created >= self.budget.max_jobs_per_run:
            raise RuntimeError(
                f"Salesforce job budget exceeded (max_jobs_per_run={self.budget.max_jobs_per_run})"
            )

    def _guard_poll_budget(self) -> None:
        if self._counters.polls_made >= self.budget.max_polls_per_run:
            raise RuntimeError(
                f"Salesforce poll budget exceeded (max_polls_per_run={self.budget.max_polls_per_run})"
            )

    @staticmethod
    def _soql_datetime_literal(value: str) -> str:
        sanitized = value.replace("'", "").strip()
        # SOQL datetime literals are expected in UTC; normalize +00:00 suffix to Z.
        if sanitized.endswith("+00:00"):
            sanitized = sanitized[:-6] + "Z"
        return sanitized

    @staticmethod
    def _soql_string_literal(value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    @staticmethod
    def _sleep_seconds(seconds: int) -> None:
        # Import is local to keep this module import side-effects minimal.
        import time

        time.sleep(seconds)

