from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import httpx

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
    parser = argparse.ArgumentParser(
        description="Backfill historical daily FX rates for configured USD target pairs."
    )
    parser.add_argument(
        "--env-file",
        default=os.path.join(PROJECT_ROOT, ".env"),
        help="Path to .env file.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=120,
        help="Number of daily points to request per pair (max 5000).",
    )
    parser.add_argument(
        "--interval",
        default="1day",
        choices=["1day", "1week", "1month"],
        help="Twelve Data interval to request for backfill.",
    )
    return parser.parse_args()


def parse_rate_timestamp(raw: str) -> str:
    value = raw.strip()
    if not value:
        return datetime.now(timezone.utc).isoformat()
    # TwelveData can return date-only or datetime strings.
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(value, pattern).replace(tzinfo=timezone.utc)
            return parsed.isoformat()
        except ValueError:
            continue
    return datetime.now(timezone.utc).isoformat()


def extract_series_values(payload: dict[str, Any]) -> list[dict[str, Any]]:
    values = payload.get("values")
    if not isinstance(values, list):
        return []
    parsed: list[dict[str, Any]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        dt = item.get("datetime")
        close = item.get("close")
        if not dt or close in {None, ""}:
            continue
        parsed.append(
            {
                "rate_timestamp": parse_rate_timestamp(str(dt)),
                "mid_rate": str(close),
                "bid_rate": None,
                "ask_rate": None,
                "source": "twelve_data",
            }
        )
    return parsed


def main() -> None:
    args = parse_args()
    load_env_file(os.path.abspath(args.env_file))

    from src.api.dependencies import get_fx_service
    from src.repositories.fx_repository import FxRepository

    fx_service = get_fx_service()
    repository = FxRepository()

    api_key = fx_service.settings.fx_primary_api_key
    if not api_key:
        raise RuntimeError("FX_PRIMARY_API_KEY is required for historical backfill.")

    base_url = fx_service.settings.fx_primary_base_url.rstrip("/")
    target_currencies = fx_service._target_currencies()  # noqa: SLF001
    if not target_currencies:
        raise RuntimeError("FX_TARGET_CURRENCIES must include AUD, NZD, or ZAR.")

    outputsize = min(max(args.days, 1), 5000)
    rows_to_upsert: list[dict[str, Any]] = []
    request_errors: list[str] = []

    with httpx.Client(timeout=30.0) as client:
        for target in target_currencies:
            pair = f"{fx_service.settings.fx_base_currency}/{target}"
            response = client.get(
                f"{base_url}/time_series",
                params={
                    "symbol": pair,
                    "interval": args.interval,
                    "outputsize": outputsize,
                    "order": "desc",
                    "apikey": api_key,
                },
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("status") == "error":
                request_errors.append(f"{pair}: {payload.get('message') or 'unknown provider error'}")
                continue
            series_values = extract_series_values(payload)
            for row in series_values:
                row["currency_pair"] = pair
                rows_to_upsert.append(row)

    inserted = repository.upsert_rates(rows_to_upsert)
    refresh = repository.refresh_fx_exposure()
    result = {
        "pairsRequested": [f"{fx_service.settings.fx_base_currency}/{target}" for target in target_currencies],
        "interval": args.interval,
        "daysRequested": outputsize,
        "requestErrors": request_errors,
        "rowsPrepared": len(rows_to_upsert),
        "rowsUpserted": len(inserted),
        "exposureRefresh": refresh,
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
