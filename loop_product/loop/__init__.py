"""Single-node LOOP helpers."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import Any

__all__ = [
    "build_evaluator_submission_for_endpoint_clarification",
    "build_evaluator_submission_for_frozen_task",
    "build_evaluator_submission_for_smoke_round",
    "ensure_evaluator_supervision_running",
    "materialize_evaluator_request",
    "run_child_smoke_loop",
    "run_evaluator_node",
    "run_evaluator_node_until_terminal",
    "run_submission_under_runtime_supervision",
    "supervise_submission_until_terminal",
]

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "build_evaluator_submission_for_endpoint_clarification": (".evaluator_client", "build_evaluator_submission_for_endpoint_clarification"),
    "build_evaluator_submission_for_frozen_task": (".evaluator_client", "build_evaluator_submission_for_frozen_task"),
    "build_evaluator_submission_for_smoke_round": (".evaluator_client", "build_evaluator_submission_for_smoke_round"),
    "materialize_evaluator_request": (".evaluator_client", "materialize_evaluator_request"),
    "run_evaluator_node": (".evaluator_client", "run_evaluator_node"),
    "run_evaluator_node_until_terminal": (".evaluator_client", "run_evaluator_node_until_terminal"),
    "ensure_evaluator_supervision_running": (".evaluator_supervision", "ensure_evaluator_supervision_running"),
    "run_submission_under_runtime_supervision": (".evaluator_supervision", "run_submission_under_runtime_supervision"),
    "supervise_submission_until_terminal": (".evaluator_supervision", "supervise_submission_until_terminal"),
    "run_child_smoke_loop": (".runner", "run_child_smoke_loop"),
}


def __getattr__(name: str) -> Any:
    export = _LAZY_EXPORTS.get(str(name))
    if export is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = export
    module: ModuleType = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
