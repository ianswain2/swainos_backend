from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class RunnerResult:
    status: str
    message: str
    output: dict[str, Any] = field(default_factory=dict)
    steps: list[dict[str, Any]] = field(default_factory=list)


class DataJobRunner(Protocol):
    runner_key: str

    def run(self, job_key: str, run_id: str, metadata: dict[str, Any]) -> RunnerResult:
        ...
