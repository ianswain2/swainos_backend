from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict
from urllib import error, request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate AI insights from existing SwainOS context views.")
    parser.add_argument(
        "--trigger",
        default="manual_cli",
        help="Run trigger label (default: manual_cli).",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Environment file path (default: .env).",
    )
    parser.add_argument(
        "--refresh-rollups",
        action="store_true",
        help="Refresh consultant and AI context materialized views before generation.",
    )
    return parser.parse_args()


def run_generation(trigger: str) -> Dict[str, Any]:
    from src.api.dependencies import get_ai_orchestration_service

    orchestration_service = get_ai_orchestration_service()
    return orchestration_service.generate_insights(trigger=trigger)


def refresh_rollups_if_requested(should_refresh: bool) -> None:
    if not should_refresh:
        return
    supabase_url = os.environ.get("SUPABASE_URL")
    service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/rpc/refresh_consultant_ai_rollups_v1"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    req = request.Request(endpoint, method="POST", data=b"{}", headers=headers)
    try:
        with request.urlopen(req, timeout=180):
            return
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8")
        raise RuntimeError(
            f"Failed to refresh rollups before generation: HTTP {exc.code} {details}. "
            "Ensure refresh-rollup migrations (0043/0045/0046) have been applied."
        ) from exc


def main() -> None:
    args = parse_args()
    load_env_file(args.env_file)
    refresh_rollups_if_requested(args.refresh_rollups)
    result = run_generation(trigger=args.trigger)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

