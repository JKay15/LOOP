"""Kernel surfaces for the first complete LOOP repo wave."""

from importlib import import_module

from .state import (
    KernelState,
    ensure_runtime_tree,
    query_kernel_state,
    query_kernel_state_object,
    rebuild_kernel_projections,
    rebuild_node_projection,
)

__all__ = [
    "KernelState",
    "ensure_runtime_tree",
    "query_kernel_state",
    "query_kernel_state_object",
    "rebuild_kernel_projections",
    "rebuild_node_projection",
    "query_authority_view",
    "query_heavy_object_authority_gap_audit_trace_view",
    "query_heavy_object_authority_gap_inventory_view",
    "query_heavy_object_authority_gap_repo_remediation_trace_view",
    "query_heavy_object_authority_gap_remediation_inventory_view",
    "query_heavy_object_authority_gap_remediation_trace_view",
    "query_heavy_object_authority_gap_trace_view",
    "query_commit_state_view",
    "query_command_parity_debugger_view",
    "query_event_journal_status_view",
    "query_event_journal_mixed_version_trace_view",
    "query_heavy_object_gc_candidate_inventory_view",
    "query_heavy_object_dedup_inventory_view",
    "query_evaluator_result_trace_view",
    "query_heavy_object_discovery_inventory_view",
    "query_heavy_object_discovery_trace_view",
    "query_heavy_object_gc_eligibility_trace_view",
    "query_heavy_object_observation_trace_view",
    "query_heavy_object_observation_view",
    "query_heavy_object_pin_release_trace_view",
    "query_heavy_object_pin_trace_view",
    "query_heavy_object_reference_inventory_view",
    "query_heavy_object_reference_release_trace_view",
    "query_heavy_object_reference_trace_view",
    "query_heavy_object_reclamation_trace_view",
    "query_heavy_object_registration_trace_view",
    "query_heavy_object_retention_view",
    "query_heavy_object_sharing_inventory_view",
    "query_heavy_object_sharing_view",
    "query_heavy_object_supersession_trace_view",
    "query_lineage_parity_debugger_view",
    "query_launch_lineage_trace_view",
    "query_lease_fencing_matrix_view",
    "query_lease_lineage_trace_view",
    "query_node_lineage_trace_view",
    "query_runtime_identity_trace_view",
    "query_runtime_identity_parity_debugger_view",
    "query_operational_hygiene_view",
    "query_repo_tree_cleanup_trace_view",
    "query_runtime_liveness_view",
    "query_projection_explanation_view",
    "query_projection_crash_consistency_matrix_view",
    "query_projection_consistency_view",
    "query_projection_replay_trace_view",
    "query_real_root_recovery_action_safety_matrix_view",
    "query_real_root_accepted_audit_mirror_repair_trace_view",
    "query_real_root_repo_service_cleanup_retirement_repair_trace_view",
    "query_real_root_projection_adoption_repair_trace_view",
    "query_real_root_recovery_continuation_matrix_view",
    "query_real_root_recovery_rehearsal_trace_view",
    "query_real_root_recovery_rehearsal_view",
    "query_terminal_result_trace_view",
    "query_topology_command_trace_view",
    "query_recent_audit_events",
    "route_user_requirements",
    "run_kernel_smoke",
    "submit_control_envelope",
    "submit_heavy_object_authority_gap_audit_request",
    "submit_heavy_object_authority_gap_remediation_request",
    "submit_heavy_object_authority_gap_repo_remediation_request",
    "submit_heavy_object_discovery_request",
    "submit_heavy_object_gc_eligibility_request",
    "submit_heavy_object_observation_request",
    "submit_heavy_object_pin_release_request",
    "submit_heavy_object_pin_request",
    "submit_heavy_object_reference_release_request",
    "submit_heavy_object_reference_request",
    "submit_heavy_object_reclamation_request",
    "submit_heavy_object_registration_request",
    "submit_heavy_object_supersession_request",
    "submit_repo_tree_cleanup_request",
    "submit_topology_mutation",
]


def __getattr__(name: str):
    if name == "run_kernel_smoke":
        value = getattr(import_module(".entry", __name__), name)
    elif name in {"query_authority_view", "query_heavy_object_authority_gap_audit_trace_view", "query_heavy_object_authority_gap_inventory_view", "query_heavy_object_authority_gap_repo_remediation_trace_view", "query_heavy_object_authority_gap_remediation_inventory_view", "query_heavy_object_authority_gap_remediation_trace_view", "query_heavy_object_authority_gap_trace_view", "query_commit_state_view", "query_command_parity_debugger_view", "query_event_journal_status_view", "query_event_journal_mixed_version_trace_view", "query_heavy_object_gc_candidate_inventory_view", "query_heavy_object_dedup_inventory_view", "query_evaluator_result_trace_view", "query_heavy_object_discovery_inventory_view", "query_heavy_object_discovery_trace_view", "query_heavy_object_gc_eligibility_trace_view", "query_heavy_object_observation_trace_view", "query_heavy_object_observation_view", "query_heavy_object_pin_release_trace_view", "query_heavy_object_pin_trace_view", "query_heavy_object_reference_inventory_view", "query_heavy_object_reference_release_trace_view", "query_heavy_object_reference_trace_view", "query_heavy_object_reclamation_trace_view", "query_heavy_object_registration_trace_view", "query_heavy_object_retention_view", "query_heavy_object_sharing_inventory_view", "query_heavy_object_sharing_view", "query_heavy_object_supersession_trace_view", "query_lineage_parity_debugger_view", "query_launch_lineage_trace_view", "query_lease_fencing_matrix_view", "query_lease_lineage_trace_view", "query_node_lineage_trace_view", "query_real_root_accepted_audit_mirror_repair_trace_view", "query_real_root_repo_service_cleanup_retirement_repair_trace_view", "query_real_root_projection_adoption_repair_trace_view", "query_real_root_recovery_action_safety_matrix_view", "query_real_root_recovery_continuation_matrix_view", "query_real_root_recovery_rehearsal_trace_view", "query_real_root_recovery_rehearsal_view", "query_runtime_identity_trace_view", "query_runtime_identity_parity_debugger_view", "query_operational_hygiene_view", "query_repo_tree_cleanup_trace_view", "query_runtime_liveness_view", "query_projection_consistency_view", "query_projection_crash_consistency_matrix_view", "query_projection_explanation_view", "query_projection_replay_trace_view", "query_topology_command_trace_view", "query_terminal_result_trace_view"}:
        value = getattr(import_module(".query", __name__), name)
    elif name == "query_recent_audit_events":
        value = getattr(import_module(".audit", __name__), name)
    elif name == "route_user_requirements":
        value = getattr(import_module(".requirements", __name__), name)
    elif name in {"submit_control_envelope", "submit_heavy_object_authority_gap_audit_request", "submit_heavy_object_authority_gap_remediation_request", "submit_heavy_object_authority_gap_repo_remediation_request", "submit_heavy_object_discovery_request", "submit_heavy_object_gc_eligibility_request", "submit_heavy_object_observation_request", "submit_heavy_object_pin_release_request", "submit_heavy_object_pin_request", "submit_heavy_object_reference_release_request", "submit_heavy_object_reference_request", "submit_heavy_object_reclamation_request", "submit_heavy_object_registration_request", "submit_heavy_object_supersession_request", "submit_repo_tree_cleanup_request", "submit_topology_mutation"}:
        value = getattr(import_module(".submit", __name__), name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value
