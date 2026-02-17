from __future__ import annotations

import argparse
import json
import os
from typing import Dict, List, Tuple
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


def fetch_count(base_url: str, api_key: str, table: str) -> int:
    params: List[Tuple[str, str]] = [("select", "id"), ("limit", "1")]
    endpoint = f"{base_url}/{table}?{urlencode(params, doseq=True)}"
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Prefer": "count=exact",
        "Accept": "application/json",
    }
    req = request.Request(endpoint, method="GET", headers=headers)
    try:
        with request.urlopen(req, timeout=90) as response:
            content_range = response.headers.get("Content-Range", "")
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8")
        raise RuntimeError(f"Failed counting {table}: HTTP {exc.code} {details}") from exc

    if "/" not in content_range:
        return 0
    total = content_range.split("/")[-1].strip()
    return int(total) if total.isdigit() else 0


def delete_all_rows(base_url: str, api_key: str, table: str) -> int:
    # PostgREST requires a filter for DELETE; this targets all rows.
    endpoint = f"{base_url}/{table}?id=not.is.null"
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Prefer": "return=representation",
    }
    req = request.Request(endpoint, method="DELETE", headers=headers)
    try:
        with request.urlopen(req, timeout=90) as response:
            payload = response.read().decode("utf-8")
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8")
        raise RuntimeError(f"Failed deleting from {table}: HTTP {exc.code} {details}") from exc

    data = json.loads(payload) if payload else []
    return len(data) if isinstance(data, list) else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Purge AI insights output tables so insights can be regenerated cleanly."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete rows. Without this flag, script runs in dry-run mode.",
    )
    parser.add_argument(
        "--env-file",
        default=os.path.join(os.path.dirname(__file__), "..", ".env"),
        help="Path to .env file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env_file(os.path.abspath(args.env_file))

    supabase_url = os.environ.get("SUPABASE_URL")
    service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")

    base_url = f"{supabase_url.rstrip('/')}/rest/v1"
    tables = [
        "ai_recommendation_queue",
        "ai_insight_events",
        "ai_briefings_daily",
    ]

    pre_counts: Dict[str, int] = {table: fetch_count(base_url, service_role_key, table) for table in tables}
    print("Current AI table row counts:")
    for table in tables:
        print(f"- {table}: {pre_counts[table]}")

    if not args.apply:
        print("\nDry run only. Re-run with --apply to purge these tables.")
        return

    print("\nPurging AI tables...")
    deleted_counts: Dict[str, int] = {}
    for table in tables:
        deleted_counts[table] = delete_all_rows(base_url, service_role_key, table)
        print(f"- deleted {deleted_counts[table]} row(s) from {table}")

    post_counts: Dict[str, int] = {table: fetch_count(base_url, service_role_key, table) for table in tables}
    print("\nPost-purge AI table row counts:")
    for table in tables:
        print(f"- {table}: {post_counts[table]}")


if __name__ == "__main__":
    main()
