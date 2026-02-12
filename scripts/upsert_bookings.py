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


def normalize_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return value


def normalize_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    value = value.strip().lower()
    if value in {"true", "t", "1", "yes", "y"}:
        return True
    if value in {"false", "f", "0", "no", "n"}:
        return False
    return None


def normalize_uuid(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def normalize_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def chunk_rows(rows: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def build_booking_payload(row: Dict[str, str]) -> Dict[str, Any]:
    external_id = row.get("external_id")
    if not external_id or not external_id.strip():
        return {}

    return {
        "external_id": external_id,
        "itinerary_id": normalize_uuid(row.get("itinerary_id")),
        "supplier_id": normalize_uuid(row.get("supplier_id")),
        "booking_number": row.get("booking_number"),
        "service_name": row.get("service_name"),
        "service_start_date": normalize_date(row.get("service_start_date")),
        "service_end_date": normalize_date(row.get("service_end_date")),
        "currency_code": row.get("currency_code"),
        "gross_amount": normalize_float(row.get("gross_amount")),
        "net_amount": normalize_float(row.get("net_amount")),
        "commission_amount": normalize_float(row.get("commission_amount")),
        "is_deleted": normalize_bool(row.get("is_deleted")),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def post_batch(url: str, headers: Dict[str, str], payload: List[Dict[str, Any]]) -> None:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=body, method="POST", headers=headers)
    try:
        with request.urlopen(req, timeout=60) as response:
            if response.status not in {200, 201, 204}:
                raise RuntimeError(f"Unexpected response: {response.status}")
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8")
        raise RuntimeError(f"HTTP {exc.code}: {details}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Upsert bookings via Supabase REST API.")
    parser.add_argument("csv_path", help="Path to bookings CSV file")
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

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/bookings?on_conflict=external_id"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    with open(args.csv_path, "r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for _ in range(args.start_row):
            next(reader, None)
        rows = (build_booking_payload(row) for row in reader)
        rows = (row for row in rows if row)
        for index, batch in enumerate(chunk_rows(rows, args.batch_size), start=1):
            post_batch(endpoint, headers, batch)
            print(f"Uploaded batch {index} ({len(batch)} rows)")


if __name__ == "__main__":
    main()
