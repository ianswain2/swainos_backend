from __future__ import annotations

import argparse
import json
import os
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


def call_refresh_rpc(base_url: str, api_key: str) -> dict:
    endpoint = f"{base_url}/rpc/refresh_consultant_ai_rollups_v1"
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    req = request.Request(endpoint, method="POST", data=b"{}", headers=headers)
    try:
        with request.urlopen(req, timeout=180) as response:
            payload = response.read().decode("utf-8")
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8")
        raise RuntimeError(
            f"Failed refreshing rollups: HTTP {exc.code} {details}. "
            "Ensure refresh-rollup migrations (0043/0045/0046) have been applied."
        ) from exc

    if not payload:
        return {"status": "ok"}
    data = json.loads(payload)
    if isinstance(data, dict):
        return data
    return {"status": "ok", "result": data}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh itinerary, travel consultant, and AI context materialized views."
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
    result = call_refresh_rpc(base_url, service_role_key)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
