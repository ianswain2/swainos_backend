from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from src.services.job_runners.base import RunnerResult


class SubprocessScriptRunner:
    def __init__(
        self,
        *,
        runner_key: str,
        script_relative_path: str,
        extra_args: list[str] | None = None,
    ) -> None:
        self.runner_key = runner_key
        self.script_relative_path = script_relative_path
        self.extra_args = extra_args or []
        self.project_root = Path(__file__).resolve().parents[3]

    def run(self, job_key: str, run_id: str, metadata: dict[str, Any]) -> RunnerResult:
        script_path = self.project_root / self.script_relative_path
        if not script_path.exists():
            return RunnerResult(
                status="failed",
                message=f"Script not found for {self.runner_key}: {script_path}",
                output={"scriptPath": str(script_path)},
            )

        command = [sys.executable, str(script_path), *self.extra_args]
        completed = subprocess.run(
            command,
            cwd=str(self.project_root),
            capture_output=True,
            text=True,
            check=False,
        )
        output: dict[str, Any] = {
            "returnCode": completed.returncode,
            "stdout": completed.stdout[-4000:],
            "stderr": completed.stderr[-4000:],
            "command": command,
        }
        parsed_json = self._try_parse_json(completed.stdout)
        if parsed_json is not None:
            output["parsed"] = parsed_json

        if completed.returncode != 0:
            return RunnerResult(
                status="failed",
                message=f"{self.runner_key} failed with exit code {completed.returncode}",
                output=output,
            )
        return RunnerResult(
            status="success",
            message=f"{self.runner_key} completed",
            output=output,
        )

    def _try_parse_json(self, value: str) -> Any | None:
        text = (value or "").strip()
        if not text:
            return None
        candidates = [text]
        lines = text.splitlines()
        if lines:
            candidates.append(lines[-1])
        for candidate in candidates:
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue
        return None
