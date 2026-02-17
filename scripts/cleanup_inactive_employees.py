from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib import error, request
from urllib.parse import urlencode


def load_env_file(env_path: str) -> None:
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key, value)


def fetch_rows(
    base_url: str,
    api_key: str,
    table: str,
    select: str,
    filters: Optional[List[Tuple[str, str]]] = None,
    limit: int = 1000,
    order: Optional[str] = None,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    offset = 0
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    while True:
        params: List[Tuple[str, str]] = [("select", select), ("limit", str(limit)), ("offset", str(offset))]
        if filters:
            params.extend(filters)
        if order:
            params.append(("order", order))
        endpoint = f"{base_url}/{table}?{urlencode(params, doseq=True)}"
        req = request.Request(endpoint, method="GET", headers=headers)
        try:
            with request.urlopen(req, timeout=90) as response:
                payload = response.read().decode("utf-8")
        except error.HTTPError as exc:
            details = exc.read().decode("utf-8")
            raise RuntimeError(f"Failed to fetch {table}: HTTP {exc.code} {details}") from exc

        batch = json.loads(payload) if payload else []
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return rows


def delete_employee_by_id(base_url: str, api_key: str, employee_id: str) -> None:
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Prefer": "return=minimal",
    }
    endpoint = f"{base_url}/employees?id=eq.{employee_id}"
    req = request.Request(endpoint, method="DELETE", headers=headers)
    try:
        with request.urlopen(req, timeout=90):
            return
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8")
        raise RuntimeError(f"Failed deleting employee {employee_id}: HTTP {exc.code} {details}") from exc


def collect_active_employee_ids(base_url: str, api_key: str, cutoff_iso: str) -> Set[str]:
    activity_filters = [
        [("employee_id", "not.is.null"), ("created_at", f"gte.{cutoff_iso}")],
        [("employee_id", "not.is.null"), ("close_date", f"gte.{cutoff_iso}")],
        [("employee_id", "not.is.null"), ("travel_start_date", f"gte.{cutoff_iso}")],
        [("employee_id", "not.is.null"), ("travel_end_date", f"gte.{cutoff_iso}")],
    ]
    active_ids: Set[str] = set()
    for filters in activity_filters:
        rows = fetch_rows(
            base_url=base_url,
            api_key=api_key,
            table="itineraries",
            select="employee_id",
            filters=filters,
            limit=1000,
        )
        for row in rows:
            employee_id = row.get("employee_id")
            if employee_id:
                active_ids.add(str(employee_id))
    return active_ids


def build_display_row(employee: Dict[str, object]) -> str:
    first = str(employee.get("first_name") or "").strip()
    last = str(employee.get("last_name") or "").strip()
    email = str(employee.get("email") or "").strip()
    external_id = str(employee.get("external_id") or "").strip()
    employee_id = str(employee.get("id") or "").strip()
    full_name = f"{first} {last}".strip() or "Unknown"
    return f"{full_name} | {email or 'no-email'} | external_id={external_id or 'n/a'} | id={employee_id}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete employees with no itinerary activity in the past N years."
    )
    parser.add_argument(
        "--years",
        type=int,
        default=2,
        help="Lookback window in years for itinerary activity (default: 2)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete matched employees. Without this flag, script runs in dry-run mode.",
    )
    parser.add_argument(
        "--preview-limit",
        type=int,
        default=50,
        help="How many candidate rows to print in dry-run mode (default: 50)",
    )
    parser.add_argument(
        "--env-file",
        default=os.path.join(os.path.dirname(__file__), "..", ".env"),
        help="Path to .env file",
    )
    args = parser.parse_args()

    load_env_file(os.path.abspath(args.env_file))
    supabase_url = os.environ.get("SUPABASE_URL")
    service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")

    rest_base_url = f"{supabase_url.rstrip('/')}/rest/v1"
    cutoff = date.today() - timedelta(days=365 * args.years)
    cutoff_iso = cutoff.isoformat()

    employees = fetch_rows(
        base_url=rest_base_url,
        api_key=service_role_key,
        table="employees",
        select="id,external_id,first_name,last_name,email",
        limit=1000,
        order="last_name.asc",
    )
    employee_by_id = {str(row.get("id")): row for row in employees if row.get("id")}
    all_employee_ids = set(employee_by_id.keys())

    active_employee_ids = collect_active_employee_ids(
        base_url=rest_base_url,
        api_key=service_role_key,
        cutoff_iso=cutoff_iso,
    )
    inactive_employee_ids = sorted(all_employee_ids - active_employee_ids)

    print(f"Lookback cutoff: {cutoff_iso}")
    print(f"Total employees: {len(all_employee_ids)}")
    print(f"Employees with itinerary activity in last {args.years} year(s): {len(active_employee_ids)}")
    print(f"Employees with NO recent itinerary activity: {len(inactive_employee_ids)}")

    if not inactive_employee_ids:
        print("No inactive employees matched. Nothing to do.")
        return

    if not args.apply:
        print(f"\nDry run only. Showing up to {args.preview_limit} candidates:\n")
        for employee_id in inactive_employee_ids[: args.preview_limit]:
            row = employee_by_id.get(employee_id)
            if row:
                print(f"- {build_display_row(row)}")
        if len(inactive_employee_ids) > args.preview_limit:
            print(f"... plus {len(inactive_employee_ids) - args.preview_limit} more")
        print("\nRe-run with --apply to delete these employees.")
        return

    print("\nApplying deletions...")
    deleted = 0
    for employee_id in inactive_employee_ids:
        delete_employee_by_id(rest_base_url, service_role_key, employee_id)
        deleted += 1
    print(f"Deleted employees: {deleted}")


if __name__ == "__main__":
    main()
