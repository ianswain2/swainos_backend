from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, List

from src.integrations.salesforce_bulk_client import SalesforceApiBudget, SalesforceBulkReadOnlyClient


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


def build_smoke_soql(object_name: str, fields: List[str], include_is_deleted: bool) -> str:
    required = {"Id", "SystemModstamp"}
    if include_is_deleted:
        required.add("IsDeleted")
    deduped_fields = list(dict.fromkeys(fields + sorted(required)))
    return (
        f"SELECT {', '.join(deduped_fields)} "
        f"FROM {object_name} "
        "ORDER BY SystemModstamp DESC, Id DESC "
        "LIMIT 1"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate read-only Salesforce object/field access for SwainOS ingestion."
    )
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).resolve().parents[1] / ".env"),
        help="Path to .env file",
    )
    args = parser.parse_args()
    load_env_file(os.path.abspath(args.env_file))

    client_id = os.environ.get("SALESFORCE_CLIENT_ID")
    client_secret = os.environ.get("SALESFORCE_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("SALESFORCE_CLIENT_ID and SALESFORCE_CLIENT_SECRET are required")

    client = SalesforceBulkReadOnlyClient(
        login_base_url=os.environ.get("SALESFORCE_LOGIN_URL", "https://login.salesforce.com"),
        client_id=client_id,
        client_secret=client_secret,
        api_version=os.environ.get("SALESFORCE_API_VERSION", "v61.0"),
        timeout_seconds=30.0,
        budget=SalesforceApiBudget(max_jobs_per_run=10, max_polls_per_run=100, max_result_pages_per_job=5),
    )
    client.authenticate()

    checks: Dict[str, Dict[str, object]] = {
        "Account": {
            "object": os.environ.get("SF_ACCOUNT_OBJECT", "Account"),
            "fields": [
                "Name",
                "IATA_Number__c",
                "Account_Email__c",
                "KaptioTravel__IsActive__c",
                "CreatedDate",
                "LastModifiedDate",
                "Consortia__c",
                "KaptioTravel__AccountCurrency__c",
                "Phone",
            ],
            "include_is_deleted": True,
        },
        "User": {
            "object": os.environ.get("SF_USER_OBJECT", "User"),
            "fields": ["FirstName", "LastName", "Email", "Salary__c", "Commission_Rate__c", "IsActive"],
            "include_is_deleted": False,
        },
        "Itinerary": {
            "object": os.environ.get("SF_ITINERARY_OBJECT", "KaptioTravel__Itinerary__c"),
            "fields": [
                "KaptioTravel__BookingNumber__c",
                "KaptioTravel__Status__c",
                "KaptioTravel__Start_Date__c",
                "KaptioTravel__End_Date__c",
                "KaptioTravel__Account__c",
                "OwnerId",
                "LastModifiedDate",
            ],
            "include_is_deleted": True,
        },
        "ItineraryItem": {
            "object": os.environ.get("SF_ITINERARY_ITEM_OBJECT", "KaptioTravel__Itinerary_Item__c"),
            "fields": parse_csv_list(
                os.environ.get(
                    "SF_ITEM_PERMISSION_SMOKE_FIELDS",
                    "KaptioTravel__Itinerary__c,KaptioTravel__Supplier__c,CreatedDate,LastModifiedDate",
                )
            ),
            "include_is_deleted": True,
        },
    }

    for label, check in checks.items():
        object_name = str(check["object"])
        fields = [str(field) for field in check["fields"]]  # type: ignore[index]
        include_is_deleted = bool(check["include_is_deleted"])
        soql = build_smoke_soql(
            object_name=object_name,
            fields=fields,
            include_is_deleted=include_is_deleted,
        )
        job_id = client.create_query_job(soql=soql, operation="queryAll")
        job = client.wait_for_job(job_id=job_id, poll_interval_seconds=3, max_polls_per_job=20)
        if str(job.get("state") or "") != "JobComplete":
            raise RuntimeError(f"{label} permission smoke failed: {job.get('errorMessage') or job}")
        print(f"{label} permission smoke passed for object `{object_name}`")

    print("Salesforce read-only permission validation passed")


if __name__ == "__main__":
    main()

