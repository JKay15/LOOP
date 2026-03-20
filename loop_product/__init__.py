"""Shared standalone LOOP product surfaces."""

from importlib import import_module

_LAZY_EXPORTS = {
    "KernelState": (".kernel.state", "KernelState"),
    "bootstrap_first_implementer_node": (".runtime", "bootstrap_first_implementer_node"),
    "bootstrap_first_implementer_from_endpoint": (".runtime", "bootstrap_first_implementer_from_endpoint"),
    "child_runtime_status_from_launch_result_ref": (".runtime", "child_runtime_status_from_launch_result_ref"),
    "publish_external_from_implementer_result": (".runtime", "publish_external_from_implementer_result"),
    "recover_orphaned_active_node": (".runtime", "recover_orphaned_active_node"),
    "supervise_child_until_settled": (".runtime", "supervise_child_until_settled"),
    "apply_endpoint_clarification_decision": (
        ".endpoint.clarification",
        "apply_endpoint_clarification_decision",
    ),
    "continue_endpoint_clarification": (
        ".endpoint.clarification",
        "continue_endpoint_clarification",
    ),
    "endpoint_clarification_main": (".endpoint.clarification", "main"),
    "handle_endpoint_clarification_turn": (
        ".endpoint.clarification",
        "handle_endpoint_clarification_turn",
    ),
    "initialize_evaluator_runtime": (".runtime", "initialize_evaluator_runtime"),
    "load_endpoint_artifact": (".endpoint.clarification", "load_endpoint_artifact"),
    "start_endpoint_clarification": (".endpoint.clarification", "start_endpoint_clarification"),
    "build_ai_user_prompt": (".evaluator.prototype", "build_ai_user_prompt"),
    "build_checker_prompt": (".evaluator.prototype", "build_checker_prompt"),
    "build_endpoint_clarification_part1_input": (
        ".evaluator.prototype",
        "build_endpoint_clarification_part1_input",
    ),
    "build_endpoint_clarification_part1_prototype_input": (
        ".evaluator.prototype",
        "build_endpoint_clarification_part1_prototype_input",
    ),
    "build_evaluator_prototype_self_eval_input": (
        ".evaluator.prototype",
        "build_evaluator_prototype_self_eval_input",
    ),
    "evaluator_simple_prototype_main": (".evaluator.simple", "main"),
    "build_reviewer_prompt": (".evaluator.prototype", "build_reviewer_prompt"),
    "build_test_designer_prompt": (".evaluator.prototype", "build_test_designer_prompt"),
    "evaluator_prototype_main": (".evaluator.prototype", "main"),
    "kernel_main": (".kernel.entry", "main"),
    "run_evaluator_prototype": (".evaluator.prototype", "run_evaluator_prototype"),
    "run_evaluator_prototype_supervised": (
        ".evaluator.prototype",
        "run_evaluator_prototype_supervised",
    ),
    "run_evaluator_simple_prototype": (
        ".evaluator.simple",
        "run_evaluator_simple_prototype",
    ),
    "run_evaluator": (".evaluator.simple", "run_evaluator"),
    "run_kernel_smoke": (".kernel.entry", "run_kernel_smoke"),
}

__all__ = [
    "KernelState",
    "apply_endpoint_clarification_decision",
    "bootstrap_first_implementer_from_endpoint",
    "bootstrap_first_implementer_node",
    "child_runtime_status_from_launch_result_ref",
    "publish_external_from_implementer_result",
    "recover_orphaned_active_node",
    "supervise_child_until_settled",
    "build_ai_user_prompt",
    "build_checker_prompt",
    "build_endpoint_clarification_part1_input",
    "build_endpoint_clarification_part1_prototype_input",
    "build_evaluator_prototype_self_eval_input",
    "build_reviewer_prompt",
    "build_test_designer_prompt",
    "continue_endpoint_clarification",
    "endpoint_clarification_main",
    "evaluator_simple_prototype_main",
    "evaluator_prototype_main",
    "handle_endpoint_clarification_turn",
    "initialize_evaluator_runtime",
    "kernel_main",
    "load_endpoint_artifact",
    "run_evaluator",
    "run_evaluator_prototype",
    "run_evaluator_simple_prototype",
    "run_evaluator_prototype_supervised",
    "run_kernel_smoke",
    "start_endpoint_clarification",
]


def __getattr__(name: str):
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    value = getattr(import_module(module_name, __name__), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
