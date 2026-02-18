from __future__ import annotations

import argparse
import json
import os
import sys

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
    parser = argparse.ArgumentParser(description="Pull and persist FX rates from the configured provider.")
    parser.add_argument(
        "--env-file",
        default=os.path.join(PROJECT_ROOT, ".env"),
        help="Path to .env file.",
    )
    parser.add_argument(
        "--run-type",
        default="scheduled",
        choices=["scheduled", "manual", "on_demand"],
        help="Run type label.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env_file(os.path.abspath(args.env_file))

    from src.api.dependencies import get_fx_service
    from src.schemas.fx import FxRatePullRunRequest

    service = get_fx_service()
    result = service.pull_rates(FxRatePullRunRequest(run_type=args.run_type))
    print(json.dumps(result.model_dump(by_alias=True), indent=2, default=str))


if __name__ == "__main__":
    main()
