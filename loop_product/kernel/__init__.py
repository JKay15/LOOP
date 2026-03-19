"""Kernel surfaces for the first complete LOOP repo wave."""

from importlib import import_module

from .state import KernelState, ensure_runtime_tree, query_kernel_state

__all__ = [
    "KernelState",
    "ensure_runtime_tree",
    "query_kernel_state",
    "query_authority_view",
    "query_recent_audit_events",
    "route_user_requirements",
    "run_kernel_smoke",
    "submit_control_envelope",
    "submit_topology_mutation",
]


def __getattr__(name: str):
    if name == "run_kernel_smoke":
        value = getattr(import_module(".entry", __name__), name)
    elif name == "query_authority_view":
        value = getattr(import_module(".query", __name__), name)
    elif name == "query_recent_audit_events":
        value = getattr(import_module(".audit", __name__), name)
    elif name == "route_user_requirements":
        value = getattr(import_module(".requirements", __name__), name)
    elif name in {"submit_control_envelope", "submit_topology_mutation"}:
        value = getattr(import_module(".submit", __name__), name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value
