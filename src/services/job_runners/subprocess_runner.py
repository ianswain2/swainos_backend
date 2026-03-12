from __future__ import annotations

import json
import os
import signal
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

    def run(
        self,
        job_key: str,
        run_id: str,
        metadata: dict[str, Any],
        max_runtime_seconds: int | None = None,
    ) -> RunnerResult:
        _ = job_key, run_id, metadata
        script_path = self.project_root / self.script_relative_path
        if not script_path.exists():
            return RunnerResult(
                status="failed",
                message=f"Script not found for {self.runner_key}: {script_path}",
                output={"scriptPath": str(script_path)},
            )

        command = [sys.executable, str(script_path), *self.extra_args]
        process = subprocess.Popen(
            command,
            cwd=str(self.project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        timed_out = False
        try:
            stdout, stderr = process.communicate(timeout=max_runtime_seconds)
            return_code = process.returncode or 0
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            stdout = exc.output or ""
            stderr = exc.stderr or ""
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except Exception:
                process.kill()
            process.wait()
            return_code = process.returncode or -9
        output: dict[str, Any] = {
            "returnCode": return_code,
            "stdout": stdout[-4000:],
            "stderr": stderr[-4000:],
            "command": command,
            "timedOut": timed_out,
            "maxRuntimeSeconds": max_runtime_seconds,
        }
        parsed_json = self._try_parse_json(stdout)
        if parsed_json is not None:
            output["parsed"] = parsed_json

        if timed_out:
            return RunnerResult(
                status="failed",
                message=(
                    f"{self.runner_key} exceeded max runtime ({max_runtime_seconds}s) "
                    "and was terminated"
                ),
                output=output,
            )
        if return_code != 0:
            return RunnerResult(
                status="failed",
                message=f"{self.runner_key} failed with exit code {return_code}",
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
