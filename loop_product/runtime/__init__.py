"""Runtime helpers exposed through lazy imports to avoid package import cycles."""

from __future__ import annotations

from importlib import import_module


_LAZY_EXPORTS = {
    "RuntimeContextMissingError": (".lifecycle", "RuntimeContextMissingError"),
    "backfill_authoritative_terminal_results": (".lifecycle", "backfill_authoritative_terminal_results"),
    "bootstrap_first_implementer_from_endpoint": (".lifecycle", "bootstrap_first_implementer_from_endpoint"),
    "bootstrap_first_implementer_node": (".lifecycle", "bootstrap_first_implementer_node"),
    "child_progress_snapshot_from_launch_result_ref": (".lifecycle", "child_progress_snapshot_from_launch_result_ref"),
    "child_runtime_status_from_launch_result_ref": (".lifecycle", "child_runtime_status_from_launch_result_ref"),
    "initialize_evaluator_runtime": (".lifecycle", "initialize_evaluator_runtime"),
    "launch_child_from_result_ref": (".lifecycle", "launch_child_from_result_ref"),
    "materialize_runtime_liveness_lease_expiry_events": (".lifecycle", "materialize_runtime_liveness_lease_expiry_events"),
    "default_real_root_recovery_checkpoint_ref": (".rehearsal", "default_real_root_recovery_checkpoint_ref"),
    "publish_external_from_implementer_result": (".lifecycle", "publish_external_from_implementer_result"),
    "recover_orphaned_active_node": (".lifecycle", "recover_orphaned_active_node"),
    "synchronize_authoritative_state": (".lifecycle", "synchronize_authoritative_state"),
    "supervise_child_until_settled": (".lifecycle", "supervise_child_until_settled"),
    "build_runtime_loss_signal": (".liveness", "build_runtime_loss_signal"),
    "runtime_attachment_state": (".liveness", "runtime_attachment_state"),
    "cleanup_test_runtime_root": (".gc", "cleanup_test_runtime_root"),
    "ensure_housekeeping_reap_controller_running": (".gc", "ensure_housekeeping_reap_controller_running"),
    "ensure_housekeeping_reap_controller_service_running": (".gc", "ensure_housekeeping_reap_controller_service_running"),
    "ensure_housekeeping_reap_controller_service_for_runtime_root": (".gc", "ensure_housekeeping_reap_controller_service_for_runtime_root"),
    "live_housekeeping_reap_controller_runtime": (".gc", "live_housekeeping_reap_controller_runtime"),
    "reconcile_housekeeping_reap_requests": (".gc", "reconcile_housekeeping_reap_requests"),
    "run_housekeeping_reap_controller_loop": (".gc", "run_housekeeping_reap_controller_loop"),
    "run_housekeeping_reap_controller_once": (".gc", "run_housekeeping_reap_controller_once"),
    "submit_stopped_run_reap_request": (".gc", "submit_stopped_run_reap_request"),
    "apply_accepted_recovery_mutation": (".recover", "apply_accepted_recovery_mutation"),
    "build_relaunch_request": (".recover", "build_relaunch_request"),
    "build_resume_request": (".recover", "build_resume_request"),
    "build_retry_request": (".recover", "build_retry_request"),
    "review_recovery_request": (".recover", "review_recovery_request"),
    "execute_real_root_recovery_rehearsal": (".rehearsal", "execute_real_root_recovery_rehearsal"),
    "repair_real_root_accepted_audit_mirror": (".rehearsal", "repair_real_root_accepted_audit_mirror"),
    "repair_real_root_projection_adoption": (".rehearsal", "repair_real_root_projection_adoption"),
    "load_real_root_recovery_checkpoint": (".rehearsal", "load_real_root_recovery_checkpoint"),
    "materialize_real_root_recovery_rehearsal": (".rehearsal", "materialize_real_root_recovery_rehearsal"),
    "ensure_repo_control_plane_running": (".control_plane", "ensure_repo_control_plane_running"),
    "ensure_repo_control_plane_services_running": (".control_plane", "ensure_repo_control_plane_services_running"),
    "ensure_repo_reactor_running": (".control_plane", "ensure_repo_reactor_running"),
    "ensure_repo_reactor_residency_guard_running": (".control_plane", "ensure_repo_reactor_residency_guard_running"),
    "cleanup_repo_services": (".control_plane", "cleanup_repo_services"),
    "cleanup_repo_tree_services": (".control_plane", "cleanup_repo_tree_services"),
    "cleanup_test_repo_services": (".control_plane", "cleanup_test_repo_services"),
    "execute_accepted_heavy_object_authority_gap_audit": (".control_plane", "execute_accepted_heavy_object_authority_gap_audit"),
    "execute_accepted_heavy_object_authority_gap_repo_remediation": (".control_plane", "execute_accepted_heavy_object_authority_gap_repo_remediation"),
    "execute_accepted_heavy_object_discovery": (".control_plane", "execute_accepted_heavy_object_discovery"),
    "heavy_object_authority_gap_audit_receipt_ref": (".control_plane", "heavy_object_authority_gap_audit_receipt_ref"),
    "heavy_object_authority_gap_repo_remediation_receipt_ref": (".control_plane", "heavy_object_authority_gap_repo_remediation_receipt_ref"),
    "execute_accepted_heavy_object_reclamation": (".control_plane", "execute_accepted_heavy_object_reclamation"),
    "heavy_object_discovery_receipt_ref": (".control_plane", "heavy_object_discovery_receipt_ref"),
    "execute_accepted_repo_tree_cleanup": (".control_plane", "execute_accepted_repo_tree_cleanup"),
    "heavy_object_reclamation_receipt_ref": (".control_plane", "heavy_object_reclamation_receipt_ref"),
    "live_repo_control_plane_runtime": (".control_plane", "live_repo_control_plane_runtime"),
    "live_repo_reactor_runtime": (".control_plane", "live_repo_reactor_runtime"),
    "live_repo_reactor_residency_guard_runtime": (".control_plane", "live_repo_reactor_residency_guard_runtime"),
    "run_repo_control_plane_loop": (".control_plane", "run_repo_control_plane_loop"),
    "run_repo_reactor_loop": (".control_plane", "run_repo_reactor_loop"),
    "run_repo_reactor_residency_guard_loop": (".control_plane", "run_repo_reactor_residency_guard_loop"),
    "repo_tree_cleanup_receipt_ref": (".control_plane", "repo_tree_cleanup_receipt_ref"),
    "summarize_real_root_recovery_rebasing": (".rehearsal", "summarize_real_root_recovery_rebasing"),
}

__all__ = list(_LAZY_EXPORTS)


def __getattr__(name: str):
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    value = getattr(import_module(module_name, __name__), attr_name)
    globals()[name] = value
    return value
