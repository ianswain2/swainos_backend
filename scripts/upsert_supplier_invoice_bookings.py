from __future__ import annotations

import argparse
import csv
import json
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib import error, request
from urllib.parse import quote


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


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def normalize_uuid(value: Optional[str]) -> Optional[str]:
    value = normalize_text(value)
    if not value:
        return None
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError):
        return None


def normalize_float(value: Optional[str]) -> Optional[float]:
    value = normalize_text(value)
    if value is None:
        return None
    value = value.replace(",", "")
    try:
        return float(value)
    except ValueError:
        return None


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


def chunk_values(values: List[str], size: int) -> Iterable[List[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def collect_external_reference_values(
    csv_path: str, start_row: int, max_rows: Optional[int]
) -> Tuple[Set[str], Set[str]]:
    itinerary_external_ids: Set[str] = set()
    supplier_external_ids: Set[str] = set()
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for _ in range(start_row):
            next(reader, None)
        processed = 0
        for row in reader:
            if max_rows is not None and processed >= max_rows:
                break
            processed += 1
            itinerary_external_id = normalize_text(row.get("itinerary_external_id"))
            supplier_external_id = normalize_text(row.get("supplier_external_id"))
            if itinerary_external_id:
                itinerary_external_ids.add(itinerary_external_id)
            if supplier_external_id:
                supplier_external_ids.add(supplier_external_id)
    return itinerary_external_ids, supplier_external_ids


def fetch_external_id_map(
    table: str,
    external_ids: Set[str],
    supabase_url: str,
    service_role_key: str,
    batch_size: int = 200,
) -> Dict[str, str]:
    if not external_ids:
        return {}

    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Accept": "application/json",
    }
    mapping: Dict[str, str] = {}
    for id_batch in chunk_values(sorted(external_ids), batch_size):
        encoded_values = ",".join(quote(value, safe="") for value in id_batch)
        endpoint = (
            f"{supabase_url.rstrip('/')}/rest/v1/{table}"
            f"?select=id,external_id&external_id=in.({encoded_values})"
        )
        req = request.Request(endpoint, method="GET", headers=headers)
        try:
            with request.urlopen(req, timeout=60) as response:
                body = response.read().decode("utf-8")
                rows = json.loads(body) if body else []
        except error.HTTPError as exc:
            details = exc.read().decode("utf-8")
            raise RuntimeError(f"Failed resolving {table} external IDs: HTTP {exc.code} {details}") from exc
        for row in rows:
            external_id = row.get("external_id")
            row_id = row.get("id")
            if external_id and row_id:
                mapping[str(external_id)] = str(row_id)
    return mapping


def export_unresolved_external_ids(
    output_path: str,
    unresolved_itinerary_ids: Set[str],
    unresolved_supplier_ids: Set[str],
) -> None:
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["entity_type", "external_id"])
        writer.writeheader()
        for external_id in sorted(unresolved_itinerary_ids):
            writer.writerow({"entity_type": "itinerary", "external_id": external_id})
        for external_id in sorted(unresolved_supplier_ids):
            writer.writerow({"entity_type": "supplier", "external_id": external_id})


def build_booking_payload(
    row: Dict[str, str],
    itinerary_id_map: Dict[str, str],
    supplier_id_map: Dict[str, str],
    strict_fk_resolver: bool,
) -> Dict[str, Any]:
    external_id = normalize_text(row.get("external_id"))
    if not external_id:
        return {}

    itinerary_external_id = normalize_text(row.get("itinerary_external_id"))
    supplier_external_id = normalize_text(row.get("supplier_external_id"))

    itinerary_id = normalize_uuid(row.get("itinerary_id"))
    supplier_id = normalize_uuid(row.get("supplier_id"))
    if strict_fk_resolver:
        itinerary_id = itinerary_id_map.get(itinerary_external_id) if itinerary_external_id else None
        supplier_id = supplier_id_map.get(supplier_external_id) if supplier_external_id else None
    else:
        if not itinerary_id and itinerary_external_id:
            itinerary_id = itinerary_id_map.get(itinerary_external_id)
        if not supplier_id and supplier_external_id:
            supplier_id = supplier_id_map.get(supplier_external_id)

    return {
        "external_id": external_id,
        "booking_name": normalize_text(row.get("booking_name")),
        "balance_due": normalize_float(row.get("balance_due")),
        "commission_balance_due": normalize_float(row.get("commission_balance_due")),
        "commission_due": normalize_float(row.get("commission_due")),
        "commission_invoiced": normalize_float(row.get("commission_invoiced")),
        "cost_due": normalize_float(row.get("cost_due")),
        "cost_invoiced": normalize_float(row.get("cost_invoiced")),
        "currency_code": normalize_text(row.get("currency_code")),
        "is_complete": normalize_bool(row.get("is_complete")),
        "is_deleted": normalize_bool(row.get("is_deleted")),
        "created_at": normalize_datetime(row.get("created_at")),
        "updated_at": normalize_datetime(row.get("updated_at")),
        "synced_at": normalize_datetime(row.get("synced_at")),
        "itinerary_external_id": itinerary_external_id,
        "supplier_external_id": supplier_external_id,
        "itinerary_id": itinerary_id,
        "supplier_id": supplier_id,
    }


def post_batch(url: str, headers: Dict[str, str], payload: List[Dict[str, Any]]) -> None:
    body = json.dumps(payload).encode("utf-8")
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        req = request.Request(url, data=body, method="POST", headers=headers)
        try:
            with request.urlopen(req, timeout=60) as response:
                if response.status not in {200, 201, 204}:
                    raise RuntimeError(f"Unexpected response: {response.status}")
                return
        except error.HTTPError as exc:
            details = exc.read().decode("utf-8")
            retryable = exc.code in {429, 502, 503, 504}
            if retryable and attempt < max_attempts:
                delay_seconds = min(2**attempt, 20)
                print(
                    f"Retryable HTTP {exc.code} on batch post (attempt {attempt}/{max_attempts}); "
                    f"retrying in {delay_seconds}s"
                )
                time.sleep(delay_seconds)
                continue
            raise RuntimeError(f"HTTP {exc.code}: {details}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Upsert supplier invoice bookings via Supabase REST API.")
    parser.add_argument("csv_path", help="Path to supplier invoice bookings CSV file")
    parser.add_argument("--batch-size", type=int, default=200, help="Rows per request batch")
    parser.add_argument(
        "--strict-fk-resolver",
        action="store_true",
        help="Resolve FK IDs from external IDs only and ignore incoming UUID values",
    )
    parser.add_argument(
        "--skip-unresolved-fks",
        action="store_true",
        help="Skip rows where strict resolver cannot resolve required FK IDs",
    )
    parser.add_argument(
        "--export-unresolved-csv",
        default=None,
        help="Optional path to export unresolved external IDs",
    )
    parser.add_argument("--start-row", type=int, default=0, help="Row index to resume from (0-based)")
    parser.add_argument("--max-rows", type=int, default=None, help="Optional maximum rows to process")
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

    itinerary_id_map: Dict[str, str] = {}
    supplier_id_map: Dict[str, str] = {}
    if args.strict_fk_resolver:
        itinerary_external_ids, supplier_external_ids = collect_external_reference_values(
            args.csv_path, args.start_row, args.max_rows
        )
        itinerary_id_map = fetch_external_id_map(
            "itineraries", itinerary_external_ids, supabase_url, service_role_key
        )
        supplier_id_map = fetch_external_id_map("suppliers", supplier_external_ids, supabase_url, service_role_key)
        unresolved_itinerary_ids = itinerary_external_ids - set(itinerary_id_map.keys())
        unresolved_supplier_ids = supplier_external_ids - set(supplier_id_map.keys())
        print(
            "Resolved foreign keys: "
            f"itineraries={len(itinerary_id_map)}/{len(itinerary_external_ids)} "
            f"(unresolved={len(unresolved_itinerary_ids)}), "
            f"suppliers={len(supplier_id_map)}/{len(supplier_external_ids)} "
            f"(unresolved={len(unresolved_supplier_ids)})"
        )
        if args.export_unresolved_csv:
            export_unresolved_external_ids(
                args.export_unresolved_csv, unresolved_itinerary_ids, unresolved_supplier_ids
            )
            print(f"Exported unresolved external IDs to {args.export_unresolved_csv}")
        if unresolved_supplier_ids and not args.skip_unresolved_fks:
            raise RuntimeError(
                "Strict FK resolver found unresolved supplier external IDs. "
                "Use --skip-unresolved-fks to continue while skipping unresolved rows."
            )
        if unresolved_itinerary_ids:
            print(
                "Warning: strict resolver found unresolved optional itinerary external IDs "
                f"(count={len(unresolved_itinerary_ids)})."
            )

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/supplier_invoice_bookings?on_conflict=external_id"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    processed_rows = 0
    skipped_unresolved_rows = 0
    uploaded_rows = 0
    with open(args.csv_path, "r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for _ in range(args.start_row):
            next(reader, None)

        def payload_rows() -> Iterable[Dict[str, Any]]:
            nonlocal processed_rows, skipped_unresolved_rows
            for row in reader:
                if args.max_rows is not None and processed_rows >= args.max_rows:
                    break
                processed_rows += 1
                payload = build_booking_payload(row, itinerary_id_map, supplier_id_map, args.strict_fk_resolver)
                if not payload:
                    continue
                if args.strict_fk_resolver and args.skip_unresolved_fks:
                    unresolved_supplier = bool(
                        payload.get("supplier_external_id") and payload.get("supplier_id") is None
                    )
                    unresolved_itinerary = bool(
                        payload.get("itinerary_external_id") and payload.get("itinerary_id") is None
                    )
                    if unresolved_supplier or unresolved_itinerary:
                        skipped_unresolved_rows += 1
                        continue
                yield payload

        for index, batch in enumerate(chunk_rows(payload_rows(), args.batch_size), start=1):
            post_batch(endpoint, headers, batch)
            uploaded_rows += len(batch)
            print(f"Uploaded batch {index} ({len(batch)} rows)")

    print(
        "Supplier invoice bookings upsert complete. "
        f"Processed={processed_rows}, Uploaded={uploaded_rows}, "
        f"SkippedUnresolved={skipped_unresolved_rows}"
    )


if __name__ == "__main__":
    main()
