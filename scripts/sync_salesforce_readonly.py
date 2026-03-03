from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from src.integrations.salesforce_bulk_client import (
    SalesforceApiBudget,
    SalesforceBulkReadOnlyClient,
    SalesforceCursor,
)
from src.repositories.salesforce_sync_repository import SalesforceSyncRepository


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


def parse_csv_list(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def normalize_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "t", "1", "yes", "y"}:
        return True
    if normalized in {"false", "f", "0", "no", "n"}:
        return False
    return None


def row_is_deleted(row: Dict[str, str]) -> bool:
    return normalize_bool(row.get("IsDeleted")) is True


def as_sorted_cursor(rows: Iterable[Dict[str, str]]) -> Tuple[Optional[str], Optional[str]]:
    sortable: List[Tuple[str, str]] = []
    for row in rows:
        stamp = str(row.get("SystemModstamp") or "").strip()
        record_id = str(row.get("Id") or "").strip()
        if not stamp or not record_id:
            continue
        sortable.append((stamp, record_id))
    if not sortable:
        return None, None
    sortable.sort()
    return sortable[-1]


def write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[Dict[str, object]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def run_script(script_path: Path, csv_path: Path, extra_args: Optional[List[str]] = None) -> None:
    args = [sys.executable, str(script_path), str(csv_path)]
    if extra_args:
        args.extend(extra_args)
    subprocess.run(args, check=True)


@dataclass(frozen=True)
class SyncConfig:
    login_base_url: str
    client_id: str
    client_secret: str
    api_version: str
    poll_interval_seconds: int
    max_polls_per_job: int
    upper_bound_lag_minutes: int
    budget: SalesforceApiBudget
    supplier_record_types: set[str]
    supplier_account_types: set[str]
    account_object: str
    user_object: str
    itinerary_object: str
    itinerary_item_object: str
    itinerary_item_field_map: Dict[str, str]
    unresolved_dir: Path


class FileLock:
    def __init__(self, lock_file: Path) -> None:
        self.lock_file = lock_file

    def __enter__(self) -> "FileLock":
        if self.lock_file.exists():
            raise RuntimeError(f"Sync lock exists at {self.lock_file}; another run may be active")
        self.lock_file.write_text(str(os.getpid()), encoding="utf-8")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        try:
            if self.lock_file.exists():
                self.lock_file.unlink()
        except OSError:
            pass


def transform_account_for_agencies(row: Dict[str, str]) -> Dict[str, object]:
    active = normalize_bool(row.get("KaptioTravel__IsActive__c"))
    if row_is_deleted(row):
        active = False
    return {
        "Id": row.get("Id"),
        "Name": row.get("Name"),
        "IATA_Number__c": row.get("IATA_Number__c"),
        "Account_Email__c": row.get("Account_Email__c"),
        "KaptioTravel__IsActive__c": active,
        "CreatedDate": row.get("CreatedDate"),
        "LastModifiedDate": row.get("LastModifiedDate"),
        "Consortia__c": row.get("Consortia__c"),
    }


def transform_account_for_suppliers(row: Dict[str, str]) -> Dict[str, object]:
    active = normalize_bool(row.get("KaptioTravel__IsActive__c"))
    if row_is_deleted(row):
        active = False
    return {
        "Id": row.get("Id"),
        "Name": row.get("Name"),
        "IATA_Number__c": row.get("IATA_Number__c"),
        "KaptioTravel__AccountCurrency__c": row.get("KaptioTravel__AccountCurrency__c"),
        "Account_Email__c": row.get("Account_Email__c"),
        "Phone": row.get("Phone"),
        "KaptioTravel__IsActive__c": active,
        "CreatedDate": row.get("CreatedDate"),
        "LastModifiedDate": row.get("LastModifiedDate"),
    }


def is_supplier_account(row: Dict[str, str], config: SyncConfig) -> bool:
    record_type = str(row.get("RecordType.DeveloperName") or "").strip().lower()
    account_type = str(row.get("Type") or "").strip().lower()
    if record_type and record_type in config.supplier_record_types:
        return True
    if account_type and account_type in config.supplier_account_types:
        return True
    return "supplier" in record_type or "supplier" in account_type or "vendor" in account_type


def transform_user_for_employees(row: Dict[str, str]) -> Dict[str, object]:
    return {
        "Id": row.get("Id"),
        "FirstName": row.get("FirstName"),
        "LastName": row.get("LastName"),
        "Email": row.get("Email"),
        "Salary__c": row.get("Salary__c"),
        "Commission_Rate__c": row.get("Commission_Rate__c"),
        "LastModifiedDate": row.get("LastModifiedDate"),
    }


def transform_itinerary(row: Dict[str, str]) -> Dict[str, object]:
    return {
        "Id": row.get("Id"),
        "KaptioTravel__BookingNumber__c": row.get("KaptioTravel__BookingNumber__c"),
        "KaptioTravel__Status__c": row.get("KaptioTravel__Status__c"),
        "KaptioTravel__Start_Date__c": row.get("KaptioTravel__Start_Date__c"),
        "KaptioTravel__End_Date__c": row.get("KaptioTravel__End_Date__c"),
        "Itinerary_Countries__c": row.get("Itinerary_Countries__c"),
        "KaptioTravel__Group_Size__c": row.get("KaptioTravel__Group_Size__c"),
        "KaptioTravel__Itinerary_Amount__c": row.get("KaptioTravel__Itinerary_Amount__c"),
        "KaptioTravel__TotalAmountNet__c": row.get("KaptioTravel__TotalAmountNet__c"),
        "KaptioTravel__CommissionTotal__c": row.get("KaptioTravel__CommissionTotal__c"),
        "KaptioTravel__DepositAmount__c": row.get("KaptioTravel__DepositAmount__c"),
        "KaptioTravel__TotalDepositPaid__c": row.get("KaptioTravel__TotalDepositPaid__c"),
        "CurrencyIsoCode": row.get("CurrencyIsoCode"),
        "KaptioTravel__Account__c": row.get("KaptioTravel__Account__c"),
        "KaptioTravel__Primary_Contact__c": row.get("KaptioTravel__Primary_Contact__c"),
        "OwnerId": row.get("OwnerId"),
        "CloseDateOutput__c": row.get("CloseDateOutput__c"),
        "Commission_Due_Date__c": row.get("Commission_Due_Date__c"),
        "Commission_Status__c": row.get("Commission_Status__c"),
        "Consortia__c": row.get("Consortia__c"),
        "KaptioTravel__FinalPaymentExpectedDate__c": row.get("KaptioTravel__FinalPaymentExpectedDate__c"),
        "KaptioTravel__GrossProfit__c": row.get("KaptioTravel__GrossProfit__c"),
        "KaptioTravel__Itinerary_Cost__c": row.get("KaptioTravel__Itinerary_Cost__c"),
        "KaptioTravel__No_of_days__c": row.get("KaptioTravel__No_of_days__c"),
        "KaptioTravel__No_of_nights__c": row.get("KaptioTravel__No_of_nights__c"),
        "KaptioTravel__ResellerCommissionTotal__c": row.get("KaptioTravel__ResellerCommissionTotal__c"),
        "KaptioTravel__Outstanding__c": row.get("KaptioTravel__Outstanding__c"),
        "Lost_Date__c": row.get("Lost_Date__c"),
        "Lost_Reason_Description__c": row.get("Lost_Reason_Description__c"),
        "CreatedDate": row.get("CreatedDate"),
        "LastModifiedDate": row.get("LastModifiedDate"),
    }


def transform_itinerary_item(row: Dict[str, str], field_map: Dict[str, str]) -> Dict[str, object]:
    transformed: Dict[str, object] = {}
    for destination_field, source_field in field_map.items():
        if source_field == "IsDeleted":
            transformed[destination_field] = row.get("IsDeleted")
        else:
            transformed[destination_field] = row.get(source_field)
    return transformed


def load_sync_config(args: argparse.Namespace) -> SyncConfig:
    client_id = os.environ.get("SALESFORCE_CLIENT_ID")
    client_secret = os.environ.get("SALESFORCE_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("SALESFORCE_CLIENT_ID and SALESFORCE_CLIENT_SECRET are required")

    itinerary_item_field_map = {
        "external_id": os.environ.get("SF_ITEM_EXTERNAL_ID_FIELD", "Id"),
        "itinerary_external_id": os.environ.get(
            "SF_ITEM_ITINERARY_EXTERNAL_ID_FIELD", "KaptioTravel__Itinerary__c"
        ),
        "supplier_external_id": os.environ.get(
            "SF_ITEM_SUPPLIER_EXTERNAL_ID_FIELD", "KaptioTravel__Supplier__c"
        ),
        "item_name": os.environ.get("SF_ITEM_NAME_FIELD", "Name"),
        "description": os.environ.get("SF_ITEM_DESCRIPTION_FIELD", "KaptioTravel__Description__c"),
        "date_from": os.environ.get("SF_ITEM_DATE_FROM_FIELD", "KaptioTravel__Date_From__c"),
        "date_to": os.environ.get("SF_ITEM_DATE_TO_FIELD", "KaptioTravel__Date_To__c"),
        "destination_country": os.environ.get(
            "SF_ITEM_DESTINATION_COUNTRY_FIELD", "KaptioTravel__DestinationCountry__c"
        ),
        "location": os.environ.get("SF_ITEM_LOCATION_FIELD", "KaptioTravel__Location__c"),
        "quantity": os.environ.get("SF_ITEM_QUANTITY_FIELD", "KaptioTravel__Quantity__c"),
        "unit_cost": os.environ.get("SF_ITEM_UNIT_COST_FIELD", "KaptioTravel__UnitCost__c"),
        "total_cost": os.environ.get("SF_ITEM_TOTAL_COST_FIELD", "KaptioTravel__TotalCost__c"),
        "full_service_name": os.environ.get(
            "SF_ITEM_FULL_SERVICE_NAME_FIELD", "KaptioTravel__FullServiceName__c"
        ),
        "unit_price": os.environ.get("SF_ITEM_UNIT_PRICE_FIELD", "KaptioTravel__UnitPrice__c"),
        "total_price": os.environ.get("SF_ITEM_TOTAL_PRICE_FIELD", "KaptioTravel__TotalPrice__c"),
        "subtotal_price": os.environ.get(
            "SF_ITEM_SUBTOTAL_PRICE_FIELD", "KaptioTravel__SubtotalPrice__c"
        ),
        "subtotal_cost": os.environ.get("SF_ITEM_SUBTOTAL_COST_FIELD", "KaptioTravel__SubtotalCost__c"),
        "gross_margin": os.environ.get("SF_ITEM_GROSS_MARGIN_FIELD", "KaptioTravel__GrossMargin__c"),
        "profit_margin_percent": os.environ.get(
            "SF_ITEM_PROFIT_MARGIN_PERCENT_FIELD", "KaptioTravel__ProfitMarginPercent__c"
        ),
        "is_cancelled": os.environ.get("SF_ITEM_IS_CANCELLED_FIELD", "KaptioTravel__IsCancelled__c"),
        "cancelled_date": os.environ.get(
            "SF_ITEM_CANCELLED_DATE_FIELD", "KaptioTravel__CancelledDate__c"
        ),
        "is_invoiced": os.environ.get("SF_ITEM_IS_INVOICED_FIELD", "KaptioTravel__IsInvoiced__c"),
        "is_deleted": "IsDeleted",
        "voucher_title": os.environ.get("SF_ITEM_VOUCHER_TITLE_FIELD", "KaptioTravel__VoucherTitle__c"),
        "destination_continent": os.environ.get(
            "SF_ITEM_DESTINATION_CONTINENT_FIELD", "KaptioTravel__DestinationContinent__c"
        ),
        "currency_code": os.environ.get("SF_ITEM_CURRENCY_FIELD", "CurrencyIsoCode"),
        "voucher_reference": os.environ.get(
            "SF_ITEM_VOUCHER_REFERENCE_FIELD", "KaptioTravel__VoucherReference__c"
        ),
        "confirmation_status": os.environ.get(
            "SF_ITEM_CONFIRMATION_STATUS_FIELD", "KaptioTravel__ConfirmationStatus__c"
        ),
        "created_at": os.environ.get("SF_ITEM_CREATED_AT_FIELD", "CreatedDate"),
        "updated_at": os.environ.get("SF_ITEM_UPDATED_AT_FIELD", "LastModifiedDate"),
    }

    return SyncConfig(
        login_base_url=os.environ.get("SALESFORCE_LOGIN_URL", "https://login.salesforce.com"),
        client_id=client_id,
        client_secret=client_secret,
        api_version=os.environ.get("SALESFORCE_API_VERSION", "v61.0"),
        poll_interval_seconds=int(os.environ.get("SF_POLL_INTERVAL_SECONDS", str(args.poll_interval))),
        max_polls_per_job=int(
            os.environ.get("SF_MAX_POLLS_PER_JOB", str(args.max_polls_per_job))
        ),
        upper_bound_lag_minutes=int(
            os.environ.get("SF_UPPER_BOUND_LAG_MINUTES", str(args.upper_bound_lag_minutes))
        ),
        budget=SalesforceApiBudget(
            max_jobs_per_run=int(os.environ.get("SF_MAX_JOBS_PER_RUN", str(args.max_jobs_per_run))),
            max_polls_per_run=int(
                os.environ.get("SF_MAX_POLLS_PER_RUN", str(args.max_polls_per_run))
            ),
            max_result_pages_per_job=int(
                os.environ.get("SF_MAX_RESULT_PAGES_PER_JOB", str(args.max_result_pages_per_job))
            ),
        ),
        supplier_record_types={
            value.lower()
            for value in parse_csv_list(
                os.environ.get("SF_SUPPLIER_RECORD_TYPE_DEVELOPER_NAMES", "")
            )
        },
        supplier_account_types={
            value.lower()
            for value in parse_csv_list(
                os.environ.get("SF_SUPPLIER_ACCOUNT_TYPES", "supplier,vendor")
            )
        },
        account_object=os.environ.get("SF_ACCOUNT_OBJECT", "Account"),
        user_object=os.environ.get("SF_USER_OBJECT", "User"),
        itinerary_object=os.environ.get("SF_ITINERARY_OBJECT", "KaptioTravel__Itinerary__c"),
        itinerary_item_object=os.environ.get(
            "SF_ITINERARY_ITEM_OBJECT", "KaptioTravel__Itinerary_Item__c"
        ),
        itinerary_item_field_map=itinerary_item_field_map,
        unresolved_dir=Path(
            os.environ.get(
                "SF_UNRESOLVED_EXPORT_DIR",
                str(Path(__file__).resolve().parents[1] / "tmp" / "salesforce-unresolved"),
            )
        ),
    )


def extract_object_rows(
    client: SalesforceBulkReadOnlyClient,
    object_name: str,
    fields: List[str],
    cursor: SalesforceCursor,
    config: SyncConfig,
    include_is_deleted: bool = True,
) -> Tuple[List[Dict[str, str]], Tuple[Optional[str], Optional[str]]]:
    upper_bound = SalesforceBulkReadOnlyClient.default_upper_bound(config.upper_bound_lag_minutes)
    soql = client.build_incremental_soql(
        object_name=object_name,
        select_fields=fields,
        cursor=cursor,
        upper_bound=upper_bound,
        include_is_deleted=include_is_deleted,
    )
    job_id = client.create_query_job(soql=soql, operation="queryAll")
    job_payload = client.wait_for_job(
        job_id=job_id,
        poll_interval_seconds=config.poll_interval_seconds,
        max_polls_per_job=config.max_polls_per_job,
    )
    state = str(job_payload.get("state") or "")
    if state != "JobComplete":
        message = job_payload.get("errorMessage") or f"Salesforce job {job_id} ended as {state}"
        raise RuntimeError(str(message))
    rows = client.get_all_result_rows(job_id)
    return rows, as_sorted_cursor(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scheduled read-only Salesforce -> Supabase sync via Bulk API 2.0."
    )
    parser.add_argument(
        "--objects",
        default="agencies,suppliers,employees,itineraries,itinerary_items",
        help="Comma-separated object loaders to execute",
    )
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--max-polls-per-job", type=int, default=120)
    parser.add_argument("--upper-bound-lag-minutes", type=int, default=3)
    parser.add_argument("--max-jobs-per-run", type=int, default=10)
    parser.add_argument("--max-polls-per-run", type=int, default=1000)
    parser.add_argument("--max-result-pages-per-job", type=int, default=200)
    parser.add_argument(
        "--lock-file",
        default="/tmp/swainos-salesforce-sync.lock",
        help="Local lock file path used to prevent overlapping runs",
    )
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).resolve().parents[1] / ".env"),
        help="Path to .env file",
    )
    args = parser.parse_args()

    load_env_file(os.path.abspath(args.env_file))
    selected = set(parse_csv_list(args.objects))
    config = load_sync_config(args)
    sync_repo = SalesforceSyncRepository()

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    upper_bound = SalesforceBulkReadOnlyClient.default_upper_bound(
        config.upper_bound_lag_minutes
    ).isoformat()
    metrics: Dict[str, Dict[str, int]] = {}

    sync_repo.create_run(
        {
            "run_id": run_id,
            "status": "running",
            "started_at": started_at,
            "upper_bound": upper_bound,
            "object_scope": sorted(selected),
            "object_metrics": metrics,
        }
    )

    client = SalesforceBulkReadOnlyClient(
        login_base_url=config.login_base_url,
        client_id=config.client_id,
        client_secret=config.client_secret,
        api_version=config.api_version,
        timeout_seconds=45.0,
        budget=config.budget,
    )

    script_root = Path(__file__).resolve().parent
    agencies_script = script_root / "upsert_agencies.py"
    suppliers_script = script_root / "upsert_suppliers.py"
    employees_script = script_root / "upsert_employees.py"
    itineraries_script = script_root / "upsert_itineraries.py"
    items_script = script_root / "upsert_itinerary_items.py"

    try:
        with FileLock(Path(args.lock_file)):
            client.authenticate()
            with tempfile.TemporaryDirectory(prefix="swainos-salesforce-sync-") as tmp_dir:
                tmp_path = Path(tmp_dir)

                account_selected = bool({"agencies", "suppliers"} & selected)
                account_cursor = sync_repo.get_cursor("Account")
                if account_selected:
                    account_rows, account_max_cursor = extract_object_rows(
                        client=client,
                        object_name=config.account_object,
                        fields=[
                            "Id",
                            "Name",
                            "IATA_Number__c",
                            "Account_Email__c",
                            "KaptioTravel__IsActive__c",
                            "CreatedDate",
                            "LastModifiedDate",
                            "Consortia__c",
                            "KaptioTravel__AccountCurrency__c",
                            "Phone",
                            "RecordType.DeveloperName",
                            "Type",
                        ],
                        cursor=SalesforceCursor(
                            last_systemmodstamp=account_cursor["last_systemmodstamp"],
                            last_id=account_cursor["last_id"],
                        ),
                        config=config,
                    )

                    agencies_rows = [
                        transform_account_for_agencies(row)
                        for row in account_rows
                        if not is_supplier_account(row, config)
                    ]
                    suppliers_rows = [
                        transform_account_for_suppliers(row)
                        for row in account_rows
                        if is_supplier_account(row, config)
                    ]

                    if "agencies" in selected:
                        agencies_csv = tmp_path / "agencies.csv"
                        loaded = write_csv(
                            agencies_csv,
                            [
                                "Id",
                                "Name",
                                "IATA_Number__c",
                                "Account_Email__c",
                                "KaptioTravel__IsActive__c",
                                "CreatedDate",
                                "LastModifiedDate",
                                "Consortia__c",
                            ],
                            agencies_rows,
                        )
                        run_script(agencies_script, agencies_csv)
                        metrics["agencies"] = {
                            "extracted": len(agencies_rows),
                            "loaded": loaded,
                            "deleted_flagged": sum(
                                1 for row in account_rows if row_is_deleted(row) and not is_supplier_account(row, config)
                            ),
                        }

                    if "suppliers" in selected:
                        suppliers_csv = tmp_path / "suppliers.csv"
                        loaded = write_csv(
                            suppliers_csv,
                            [
                                "Id",
                                "Name",
                                "IATA_Number__c",
                                "KaptioTravel__AccountCurrency__c",
                                "Account_Email__c",
                                "Phone",
                                "KaptioTravel__IsActive__c",
                                "CreatedDate",
                                "LastModifiedDate",
                            ],
                            suppliers_rows,
                        )
                        run_script(suppliers_script, suppliers_csv)
                        metrics["suppliers"] = {
                            "extracted": len(suppliers_rows),
                            "loaded": loaded,
                            "deleted_flagged": sum(
                                1 for row in account_rows if row_is_deleted(row) and is_supplier_account(row, config)
                            ),
                        }

                    if account_max_cursor[0] and account_max_cursor[1]:
                        sync_repo.upsert_cursor(
                            object_name="Account",
                            last_systemmodstamp=account_max_cursor[0],
                            last_id=account_max_cursor[1],
                        )

                if "employees" in selected:
                    user_cursor = sync_repo.get_cursor("User")
                    user_rows, user_max_cursor = extract_object_rows(
                        client=client,
                        object_name=config.user_object,
                        fields=[
                            "Id",
                            "FirstName",
                            "LastName",
                            "Email",
                            "Salary__c",
                            "Commission_Rate__c",
                            "LastModifiedDate",
                            "IsActive",
                        ],
                        cursor=SalesforceCursor(
                            last_systemmodstamp=user_cursor["last_systemmodstamp"],
                            last_id=user_cursor["last_id"],
                        ),
                        config=config,
                        include_is_deleted=False,
                    )
                    employee_rows = [transform_user_for_employees(row) for row in user_rows]
                    employees_csv = tmp_path / "employees.csv"
                    loaded = write_csv(
                        employees_csv,
                        [
                            "Id",
                            "FirstName",
                            "LastName",
                            "Email",
                            "Salary__c",
                            "Commission_Rate__c",
                            "LastModifiedDate",
                        ],
                        employee_rows,
                    )
                    run_script(employees_script, employees_csv)
                    metrics["employees"] = {
                        "extracted": len(employee_rows),
                        "loaded": loaded,
                        "deleted_flagged": 0,
                    }
                    if user_max_cursor[0] and user_max_cursor[1]:
                        sync_repo.upsert_cursor(
                            object_name="User",
                            last_systemmodstamp=user_max_cursor[0],
                            last_id=user_max_cursor[1],
                        )

                if "itineraries" in selected:
                    itinerary_cursor = sync_repo.get_cursor("Itinerary")
                    itinerary_rows, itinerary_max_cursor = extract_object_rows(
                        client=client,
                        object_name=config.itinerary_object,
                        fields=[
                            "Id",
                            "KaptioTravel__BookingNumber__c",
                            "KaptioTravel__Status__c",
                            "KaptioTravel__Start_Date__c",
                            "KaptioTravel__End_Date__c",
                            "Itinerary_Countries__c",
                            "KaptioTravel__Group_Size__c",
                            "KaptioTravel__Itinerary_Amount__c",
                            "KaptioTravel__TotalAmountNet__c",
                            "KaptioTravel__CommissionTotal__c",
                            "KaptioTravel__DepositAmount__c",
                            "KaptioTravel__TotalDepositPaid__c",
                            "CurrencyIsoCode",
                            "KaptioTravel__Account__c",
                            "KaptioTravel__Primary_Contact__c",
                            "OwnerId",
                            "CloseDateOutput__c",
                            "Commission_Due_Date__c",
                            "Commission_Status__c",
                            "Consortia__c",
                            "KaptioTravel__FinalPaymentExpectedDate__c",
                            "KaptioTravel__GrossProfit__c",
                            "KaptioTravel__Itinerary_Cost__c",
                            "KaptioTravel__No_of_days__c",
                            "KaptioTravel__No_of_nights__c",
                            "KaptioTravel__ResellerCommissionTotal__c",
                            "KaptioTravel__Outstanding__c",
                            "Lost_Date__c",
                            "Lost_Reason_Description__c",
                            "CreatedDate",
                            "LastModifiedDate",
                        ],
                        cursor=SalesforceCursor(
                            last_systemmodstamp=itinerary_cursor["last_systemmodstamp"],
                            last_id=itinerary_cursor["last_id"],
                        ),
                        config=config,
                    )
                    transformed = [transform_itinerary(row) for row in itinerary_rows]
                    itineraries_csv = tmp_path / "itineraries.csv"
                    loaded = write_csv(
                        itineraries_csv,
                        list(transformed[0].keys()) if transformed else [
                            "Id",
                            "KaptioTravel__BookingNumber__c",
                            "KaptioTravel__Status__c",
                            "KaptioTravel__Start_Date__c",
                            "KaptioTravel__End_Date__c",
                            "Itinerary_Countries__c",
                            "KaptioTravel__Group_Size__c",
                            "KaptioTravel__Itinerary_Amount__c",
                            "KaptioTravel__TotalAmountNet__c",
                            "KaptioTravel__CommissionTotal__c",
                            "KaptioTravel__DepositAmount__c",
                            "KaptioTravel__TotalDepositPaid__c",
                            "CurrencyIsoCode",
                            "KaptioTravel__Account__c",
                            "KaptioTravel__Primary_Contact__c",
                            "OwnerId",
                            "CloseDateOutput__c",
                            "Commission_Due_Date__c",
                            "Commission_Status__c",
                            "Consortia__c",
                            "KaptioTravel__FinalPaymentExpectedDate__c",
                            "KaptioTravel__GrossProfit__c",
                            "KaptioTravel__Itinerary_Cost__c",
                            "KaptioTravel__No_of_days__c",
                            "KaptioTravel__No_of_nights__c",
                            "KaptioTravel__ResellerCommissionTotal__c",
                            "KaptioTravel__Outstanding__c",
                            "Lost_Date__c",
                            "Lost_Reason_Description__c",
                            "CreatedDate",
                            "LastModifiedDate",
                        ],
                        transformed,
                    )
                    run_script(itineraries_script, itineraries_csv)
                    metrics["itineraries"] = {
                        "extracted": len(transformed),
                        "loaded": loaded,
                        "deleted_flagged": sum(1 for row in itinerary_rows if row_is_deleted(row)),
                    }
                    if itinerary_max_cursor[0] and itinerary_max_cursor[1]:
                        sync_repo.upsert_cursor(
                            object_name="Itinerary",
                            last_systemmodstamp=itinerary_max_cursor[0],
                            last_id=itinerary_max_cursor[1],
                        )

                if "itinerary_items" in selected:
                    item_cursor = sync_repo.get_cursor("ItineraryItem")
                    select_fields = list(set(config.itinerary_item_field_map.values()))
                    item_rows, item_max_cursor = extract_object_rows(
                        client=client,
                        object_name=config.itinerary_item_object,
                        fields=select_fields,
                        cursor=SalesforceCursor(
                            last_systemmodstamp=item_cursor["last_systemmodstamp"],
                            last_id=item_cursor["last_id"],
                        ),
                        config=config,
                    )
                    transformed = [
                        transform_itinerary_item(row, config.itinerary_item_field_map) for row in item_rows
                    ]
                    items_csv = tmp_path / "itinerary_items.csv"
                    loaded = write_csv(
                        items_csv,
                        list(config.itinerary_item_field_map.keys()),
                        transformed,
                    )
                    unresolved_csv = config.unresolved_dir / f"itinerary-items-unresolved-{run_id}.csv"
                    run_script(
                        items_script,
                        items_csv,
                        extra_args=[
                            "--strict-fk-resolver",
                            "--skip-unresolved-fks",
                            "--export-unresolved-csv",
                            str(unresolved_csv),
                        ],
                    )
                    metrics["itinerary_items"] = {
                        "extracted": len(transformed),
                        "loaded": loaded,
                        "deleted_flagged": sum(1 for row in item_rows if row_is_deleted(row)),
                    }
                    if item_max_cursor[0] and item_max_cursor[1]:
                        sync_repo.upsert_cursor(
                            object_name="ItineraryItem",
                            last_systemmodstamp=item_max_cursor[0],
                            last_id=item_max_cursor[1],
                        )

        sync_repo.finalize_run(
            run_id=run_id,
            status="success",
            finished_at=datetime.now(timezone.utc).isoformat(),
            error_message=None,
            object_metrics=metrics,
            counters={
                "jobs_created": client.counters.jobs_created,
                "polls_made": client.counters.polls_made,
                "result_pages_read": client.counters.result_pages_read,
            },
        )
        print("Salesforce read-only sync completed successfully")
    except Exception as exc:
        sync_repo.finalize_run(
            run_id=run_id,
            status="failed",
            finished_at=datetime.now(timezone.utc).isoformat(),
            error_message=str(exc),
            object_metrics=metrics,
            counters={
                "jobs_created": client.counters.jobs_created,
                "polls_made": client.counters.polls_made,
                "result_pages_read": client.counters.result_pages_read,
            },
        )
        raise


if __name__ == "__main__":
    main()

