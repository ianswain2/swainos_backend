from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib import error, request


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


def pick(row: Dict[str, str], *keys: str) -> Optional[str]:
    for key in keys:
        if key in row:
            value = row.get(key)
            if value is not None and value != "":
                return value
    return None


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def normalize_float(value: Optional[str]) -> Optional[float]:
    value = normalize_text(value)
    if value is None:
        return None
    value = value.replace(",", "")
    try:
        return float(value)
    except ValueError:
        return None


def normalize_datetime(value: Optional[str]) -> Optional[str]:
    value = normalize_text(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return value


def chunk_rows(rows: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def filter_employee_rows(rows: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    seen_emails: set[str] = set()
    duplicate_email_count = 0
    for row in rows:
        if not row:
            continue
        first_name = row.get("first_name")
        last_name = row.get("last_name")
        email = row.get("email")
        if not first_name or not last_name or not email:
            continue
        email_normalized = str(email).strip().lower()
        if email_normalized in seen_emails:
            duplicate_email_count += 1
            continue
        seen_emails.add(email_normalized)
        yield row
    if duplicate_email_count:
        print(f"Skipped {duplicate_email_count} duplicate email row(s) during employee import")


def build_employee_payload(row: Dict[str, str]) -> Dict[str, Any]:
    external_id = normalize_text(pick(row, "external_id", "Id"))
    if not external_id:
        return {}

    payload: Dict[str, Any] = {
        "external_id": external_id,
        "first_name": normalize_text(pick(row, "first_name", "FirstName")),
        "last_name": normalize_text(pick(row, "last_name", "LastName")),
        "email": normalize_text(pick(row, "email", "Email", "\ufeffemail")),
        # salary is stored as annual salary amount for compensation rollups.
        "salary": normalize_float(pick(row, "salary", "Salary__c")),
    }
    commission_rate = normalize_float(
        pick(row, "commission_rate", "Commission_Rate__c", "commission_percent")
    )
    if commission_rate is not None:
        payload["commission_rate"] = commission_rate
    updated_at = normalize_datetime(pick(row, "updated_at", "LastModifiedDate"))
    if updated_at is not None:
        payload["updated_at"] = updated_at
    return payload


def post_batch(url: str, headers: Dict[str, str], payload: List[Dict[str, Any]]) -> None:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=body, method="POST", headers=headers)
    try:
        with request.urlopen(req, timeout=90) as response:
            if response.status not in {200, 201, 204}:
                raise RuntimeError(f"Unexpected response: {response.status}")
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8")
        raise RuntimeError(f"HTTP {exc.code}: {details}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Upsert employees via Supabase REST API.")
    parser.add_argument("csv_path", help="Path to employees CSV file")
    parser.add_argument("--batch-size", type=int, default=300, help="Rows per request batch")
    parser.add_argument(
        "--start-row",
        type=int,
        default=0,
        help="Row index to resume from (0-based, excluding header)",
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

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/employees?on_conflict=external_id"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    uploaded_rows = 0
    with open(args.csv_path, "r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for _ in range(args.start_row):
            next(reader, None)
        rows = (build_employee_payload(row) for row in reader)
        rows = filter_employee_rows(rows)
        for index, batch in enumerate(chunk_rows(rows, args.batch_size), start=1):
            post_batch(endpoint, headers, batch)
            uploaded_rows += len(batch)
            print(f"Uploaded batch {index} ({len(batch)} rows)")
    print(f"Employee upsert complete. Rows uploaded: {uploaded_rows}")


if __name__ == "__main__":
    main()
