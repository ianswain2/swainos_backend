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


def normalize_bool(value: Optional[str]) -> Optional[bool]:
    value = normalize_text(value)
    if value is None:
        return None
    normalized = value.lower()
    if normalized in {"true", "t", "1", "yes", "y"}:
        return True
    if normalized in {"false", "f", "0", "no", "n"}:
        return False
    return None


def normalize_int(value: Optional[str]) -> Optional[int]:
    value = normalize_text(value)
    if value is None:
        return None
    value = value.replace(",", "")
    try:
        return int(float(value))
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


def build_supplier_payload(row: Dict[str, str]) -> Dict[str, Any]:
    external_id = normalize_text(pick(row, "external_id", "Id"))
    if not external_id:
        return {}

    return {
        "external_id": external_id,
        "supplier_name": normalize_text(pick(row, "supplier_name", "Name")),
        "supplier_code": normalize_text(pick(row, "supplier_code", "IATA_Number__c")),
        "supplier_type": normalize_text(pick(row, "supplier_type")),
        "default_currency": normalize_text(
            pick(row, "default_currency", "KaptioTravel__AccountCurrency__c")
        ),
        "payment_terms_days": normalize_int(pick(row, "payment_terms_days")),
        "contact_email": normalize_text(pick(row, "contact_email", "Account_Email__c")),
        "contact_phone": normalize_text(pick(row, "contact_phone", "Phone")),
        "address_country": normalize_text(pick(row, "address_country")),
        "is_active": normalize_bool(pick(row, "is_active", "KaptioTravel__IsActive__c")),
        "created_at": normalize_datetime(pick(row, "created_at", "CreatedDate")),
        "updated_at": normalize_datetime(pick(row, "updated_at", "LastModifiedDate")),
    }


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
    parser = argparse.ArgumentParser(description="Upsert suppliers via Supabase REST API.")
    parser.add_argument("csv_path", help="Path to suppliers CSV file")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per request batch")
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

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/suppliers?on_conflict=external_id"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    uploaded_rows = 0
    with open(args.csv_path, "r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for _ in range(args.start_row):
            next(reader, None)
        rows = (build_supplier_payload(row) for row in reader)
        rows = (row for row in rows if row)
        for index, batch in enumerate(chunk_rows(rows, args.batch_size), start=1):
            post_batch(endpoint, headers, batch)
            uploaded_rows += len(batch)
            print(f"Uploaded batch {index} ({len(batch)} rows)")

    print(f"Supplier upsert complete. Rows uploaded: {uploaded_rows}")


if __name__ == "__main__":
    main()
