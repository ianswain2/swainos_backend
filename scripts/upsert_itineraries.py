from __future__ import annotations

import argparse
import csv
import json
import os
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


def normalize_date(value: Optional[str]) -> Optional[str]:
    value = normalize_text(value)
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
        return None


def normalize_datetime(value: Optional[str]) -> Optional[str]:
    value = normalize_text(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return None


def normalize_int(value: Optional[str]) -> Optional[int]:
    value = normalize_text(value)
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError:
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


def normalize_uuid(value: Optional[str]) -> Optional[str]:
    value = normalize_text(value)
    if not value:
        return None
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError):
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


def chunk_values(values: List[str], size: int) -> Iterable[List[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def collect_external_reference_values(
    csv_path: str, start_row: int
) -> Tuple[Set[str], Set[str], Set[str]]:
    agency_external_ids: Set[str] = set()
    contact_external_ids: Set[str] = set()
    owner_external_ids: Set[str] = set()

    with open(csv_path, "r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for _ in range(start_row):
            next(reader, None)
        for row in reader:
            agency_external_id = normalize_text(
                pick(row, "agency_external_id", "KaptioTravel__Account__c")
            )
            contact_external_id = normalize_text(
                pick(row, "primary_contact_external_id", "KaptioTravel__Primary_Contact__c")
            )
            if agency_external_id:
                agency_external_ids.add(agency_external_id)
            if contact_external_id:
                contact_external_ids.add(contact_external_id)
            owner_external_id = normalize_text(pick(row, "owner_external_id", "OwnerId"))
            if owner_external_id:
                owner_external_ids.add(owner_external_id)

    return agency_external_ids, contact_external_ids, owner_external_ids


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
    sorted_ids = sorted(external_ids)
    for id_batch in chunk_values(sorted_ids, batch_size):
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


def build_itinerary_payload(
    row: Dict[str, str],
    agency_id_map: Dict[str, str],
    contact_id_map: Dict[str, str],
    employee_id_map: Dict[str, str],
) -> Dict[str, Any]:
    external_id = normalize_text(pick(row, "external_id", "Id"))
    if not external_id:
        return {}

    agency_external_id = normalize_text(pick(row, "agency_external_id", "KaptioTravel__Account__c"))
    primary_contact_external_id = normalize_text(
        pick(row, "primary_contact_external_id", "KaptioTravel__Primary_Contact__c")
    )
    owner_external_id = normalize_text(pick(row, "owner_external_id", "OwnerId"))
    agency_id = normalize_uuid(pick(row, "agency_id"))
    primary_contact_id = normalize_uuid(pick(row, "primary_contact_id"))
    employee_id = normalize_uuid(pick(row, "employee_id"))

    if not agency_id and agency_external_id:
        agency_id = agency_id_map.get(agency_external_id)
    if not primary_contact_id and primary_contact_external_id:
        primary_contact_id = contact_id_map.get(primary_contact_external_id)
    if not employee_id and owner_external_id:
        employee_id = employee_id_map.get(owner_external_id)

    return {
        "external_id": external_id,
        "itinerary_number": normalize_text(
            pick(row, "itinerary_number", "KaptioTravel__BookingNumber__c")
        ),
        "itinerary_name": normalize_text(pick(row, "itinerary_name")),
        "itinerary_status": normalize_text(
            pick(row, "itinerary_status", "KaptioTravel__Status__c")
        ),
        "travel_start_date": normalize_date(
            pick(row, "travel_start_date", "KaptioTravel__Start_Date__c")
        ),
        "travel_end_date": normalize_date(pick(row, "travel_end_date", "KaptioTravel__End_Date__c")),
        "primary_country": normalize_text(pick(row, "primary_country", "Itinerary_Countries__c")),
        "primary_region": normalize_text(pick(row, "primary_region")),
        "primary_city": normalize_text(pick(row, "primary_city")),
        "primary_latitude": normalize_float(pick(row, "primary_latitude")),
        "primary_longitude": normalize_float(pick(row, "primary_longitude")),
        "pax_count": normalize_int(pick(row, "pax_count", "KaptioTravel__Group_Size__c")),
        "adult_count": normalize_int(pick(row, "adult_count")),
        "child_count": normalize_int(pick(row, "child_count")),
        "gross_amount": normalize_float(
            pick(row, "gross_amount", "KaptioTravel__Itinerary_Amount__c")
        ),
        "net_amount": normalize_float(
            pick(row, "net_amount", "KaptioTravel__TotalAmountNet__c")
        ),
        "commission_amount": normalize_float(
            pick(row, "commission_amount", "KaptioTravel__CommissionTotal__c")
        ),
        "deposit_received": normalize_float(
            pick(row, "deposit_received", "KaptioTravel__DepositAmount__c", "KaptioTravel__TotalDepositPaid__c")
        ),
        "balance_due": normalize_float(pick(row, "balance_due")),
        "currency_code": normalize_text(pick(row, "currency_code", "CurrencyIsoCode")),
        "agency_id": agency_id,
        "agency_external_id": agency_external_id,
        "primary_contact_id": primary_contact_id,
        "primary_contact_external_id": primary_contact_external_id,
        "primary_contact_type": normalize_text(pick(row, "primary_contact_type")),
        "employee_id": employee_id,
        "close_date": normalize_date(pick(row, "close_date", "CloseDateOutput__c")),
        "trade_commission_due_date": normalize_date(
            pick(row, "trade_commission_due_date", "Commission_Due_Date__c")
        ),
        "trade_commission_status": normalize_text(
            pick(row, "trade_commission_status", "Commission_Status__c")
        ),
        "consortia": normalize_text(pick(row, "consortia", "Consortia__c")),
        "final_payment_date": normalize_date(
            pick(row, "final_payment_date", "KaptioTravel__FinalPaymentExpectedDate__c")
        ),
        "gross_profit": normalize_float(pick(row, "gross_profit", "KaptioTravel__GrossProfit__c")),
        "cost_amount": normalize_float(pick(row, "cost_amount", "KaptioTravel__Itinerary_Cost__c")),
        "number_of_days": normalize_int(pick(row, "number_of_days", "KaptioTravel__No_of_days__c")),
        "number_of_nights": normalize_int(
            pick(row, "number_of_nights", "KaptioTravel__No_of_nights__c")
        ),
        "trade_commission_amount": normalize_float(
            pick(row, "trade_commission_amount", "KaptioTravel__ResellerCommissionTotal__c")
        ),
        "outstanding_balance": normalize_float(
            pick(row, "outstanding_balance", "KaptioTravel__Outstanding__c")
        ),
        "owner_external_id": owner_external_id,
        "lost_date": normalize_date(pick(row, "lost_date", "Lost_Date__c")),
        "lost_comments": normalize_text(pick(row, "lost_comments", "Lost_Reason_Description__c")),
        "created_at": normalize_datetime(pick(row, "created_at", "CreatedDate")),
        "updated_at": normalize_datetime(pick(row, "updated_at", "LastModifiedDate")),
        "synced_at": normalize_datetime(pick(row, "synced_at")),
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
    parser = argparse.ArgumentParser(description="Upsert itineraries via Supabase REST API.")
    parser.add_argument("csv_path", help="Path to itineraries CSV file")
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
    parser.add_argument(
        "--skip-fk-resolver",
        action="store_true",
        help="Skip resolving agency/contact/employee external IDs to UUID foreign keys",
    )
    parser.add_argument(
        "--fail-on-unresolved-employees",
        action="store_true",
        help="Fail import when owner_external_id values cannot resolve to employees",
    )
    args = parser.parse_args()

    load_env_file(os.path.abspath(args.env_file))

    supabase_url = os.environ.get("SUPABASE_URL")
    service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/itineraries?on_conflict=external_id"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    agency_id_map: Dict[str, str] = {}
    contact_id_map: Dict[str, str] = {}
    employee_id_map: Dict[str, str] = {}
    if not args.skip_fk_resolver:
        agency_external_ids, contact_external_ids, owner_external_ids = collect_external_reference_values(
            args.csv_path, args.start_row
        )
        agency_id_map = fetch_external_id_map(
            table="agencies",
            external_ids=agency_external_ids,
            supabase_url=supabase_url,
            service_role_key=service_role_key,
        )
        contact_id_map = fetch_external_id_map(
            table="contacts",
            external_ids=contact_external_ids,
            supabase_url=supabase_url,
            service_role_key=service_role_key,
        )
        employee_id_map = fetch_external_id_map(
            table="employees",
            external_ids=owner_external_ids,
            supabase_url=supabase_url,
            service_role_key=service_role_key,
        )
        unresolved_agencies = len(agency_external_ids - set(agency_id_map.keys()))
        unresolved_contacts = len(contact_external_ids - set(contact_id_map.keys()))
        unresolved_employees = len(owner_external_ids - set(employee_id_map.keys()))
        print(
            "Resolved foreign keys: "
            f"agencies={len(agency_id_map)}/{len(agency_external_ids)} "
            f"(unresolved={unresolved_agencies}), "
            f"contacts={len(contact_id_map)}/{len(contact_external_ids)} "
            f"(unresolved={unresolved_contacts}), "
            f"employees={len(employee_id_map)}/{len(owner_external_ids)} "
            f"(unresolved={unresolved_employees})"
        )
        if unresolved_employees > 0:
            unresolved_owner_ids = sorted(owner_external_ids - set(employee_id_map.keys()))
            preview = unresolved_owner_ids[:10]
            print(f"Unresolved owner_external_id sample ({len(preview)}): {preview}")
            if args.fail_on_unresolved_employees:
                raise RuntimeError(
                    f"Cannot resolve {unresolved_employees} owner_external_id values to employees"
                )

    with open(args.csv_path, "r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for _ in range(args.start_row):
            next(reader, None)
        rows = (
            build_itinerary_payload(row, agency_id_map, contact_id_map, employee_id_map)
            for row in reader
        )
        rows = (row for row in rows if row)
        for index, batch in enumerate(chunk_rows(rows, args.batch_size), start=1):
            post_batch(endpoint, headers, batch)
            print(f"Uploaded batch {index} ({len(batch)} rows)")


if __name__ == "__main__":
    main()
