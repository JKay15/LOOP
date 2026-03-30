# LOOP System Event Journal Milestone 3 Closeout

This document freezes the required deliverables, acceptance, and evidence surfaces for Milestone 3 of the LOOP event-journal migration.

Milestone 3 covers the **canonical lease/liveness/reap control plane**. It does **not** claim completion of the command-migration/parity-debugger work from Milestone 4, and it does **not** claim completion of the canonical heavy-object lifecycle/share/retention model from Milestone 5.

## Deliverables

- launch/supervision/heartbeat/exit/lease-expiry canonical facts
  - evidence:
    - `tests/check_loop_system_repo_canonical_launch_started_event.py`
    - `tests/check_loop_system_repo_canonical_launch_reused_existing_event.py`
    - `tests/check_loop_system_repo_canonical_process_identity_confirmed_event.py`
    - `tests/check_loop_system_repo_canonical_supervision_attached_event.py`
    - `tests/check_loop_system_repo_canonical_heartbeat_observed_event.py`
    - `tests/check_loop_system_repo_supervision_exit_events.py`
    - `tests/check_loop_system_repo_runtime_loss_confirmed_event.py`
    - `tests/check_loop_system_repo_canonical_lease_expired_event.py`
    - `tests/check_loop_system_repo_canonical_supervision_detached_event.py`
- lease-aware runtime projection and truthful ACTIVE downgrade
  - evidence:
    - `tests/check_loop_system_repo_lease_expiry_materialization.py`
    - `tests/check_loop_system_repo_lease_aware_authority_view.py`
    - `tests/check_loop_system_repo_state_query.py`
- lease epoch / fencing model and stale-observer rejection
  - evidence:
    - `tests/check_loop_system_repo_event_journal_liveness_observations.py`
    - `tests/check_loop_system_repo_event_journal_liveness_freshness.py`
    - `tests/check_loop_system_repo_event_journal_liveness_epoch_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_writer_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_owner_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_pid_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_fingerprint_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_launch_identity_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_start_evidence_guard.py`
    - `tests/check_loop_system_repo_runtime_status_liveness_mirror.py`
    - `tests/check_loop_system_repo_child_supervision_liveness_mirror.py`
    - `tests/check_loop_system_repo_child_supervision_heartbeat_mirror.py`
    - `tests/check_loop_system_repo_sidecar_cleanup_authority_guard.py`
- `codex exec` capability-parity harness for the lease model
  - evidence:
    - `tests/check_loop_system_repo_codex_exec_capability_parity_foundation.py`
- event-driven reap/defer/reap-confirmed model for orphaned sidecars, supervisors, and abandoned runtime stacks
  - evidence:
    - `tests/check_loop_system_repo_event_reap_command_foundation.py`
    - `tests/check_loop_system_repo_canonical_reap_vocabulary.py`
    - `tests/check_loop_system_repo_reap_reconciler_foundation.py`
    - `tests/check_loop_system_repo_housekeeping_reap_controller.py`
    - `tests/check_loop_system_repo_housekeeping_reap_controller_loop.py`
    - `tests/check_loop_system_repo_housekeeping_reap_request_autobootstrap.py`
    - `tests/check_loop_system_repo_housekeeping_controller_service_mode.py`
    - `tests/check_loop_system_repo_housekeeping_controller_authority_mirror.py`
    - `tests/check_loop_system_repo_housekeeping_controller_authority_reuse.py`
    - `tests/check_loop_system_repo_housekeeping_controller_authoritative_exit.py`
    - `tests/check_loop_system_repo_housekeeping_controller_lease_fencing.py`
    - `tests/check_loop_system_repo_housekeeping_controller_proactive_bootstrap.py`
    - `tests/check_loop_system_repo_housekeeping_controller_host_supervisor_bootstrap.py`
    - `tests/check_loop_system_repo_housekeeping_controller_repo_lifecycle_bootstrap.py`
    - `tests/check_loop_system_repo_host_supervisor_authority_mirror.py`
    - `tests/check_loop_system_repo_host_supervisor_authority_reuse.py`
    - `tests/check_loop_system_repo_host_supervisor_authoritative_exit.py`
    - `tests/check_loop_system_repo_host_supervisor_lease_fencing.py`
    - `tests/check_loop_system_repo_repo_control_plane_bootstrap.py`
    - `tests/check_loop_system_repo_repo_control_plane_service_loop.py`
    - `tests/check_loop_system_repo_repo_control_plane_authority_mirror.py`
    - `tests/check_loop_system_repo_repo_control_plane_authority_reuse.py`
    - `tests/check_loop_system_repo_repo_control_plane_authoritative_exit.py`
    - `tests/check_loop_system_repo_repo_control_plane_lease_fencing.py`
    - `tests/check_loop_system_repo_repo_control_plane_repo_lifecycle_bootstrap.py`
    - `tests/check_loop_system_repo_repo_control_plane_runtime_tree_bootstrap.py`
    - `tests/check_loop_system_repo_repo_control_plane_first_child_bootstrap.py`
    - `tests/check_loop_system_repo_repo_control_plane_authority_query_bootstrap.py`
    - `tests/check_loop_system_repo_repo_control_plane_status_query_bootstrap.py`
    - `tests/check_loop_system_repo_repo_control_plane_requirement_event.py`
    - `tests/check_loop_system_repo_repo_control_plane_host_watchdog_restore.py`
    - `tests/check_loop_system_repo_repo_control_plane_housekeeping_watchdog_restore.py`
    - `tests/check_loop_system_repo_repo_control_plane_upper_watchdog_restore.py`
    - `tests/check_loop_system_repo_repo_global_reactor_restore.py`
    - `tests/check_loop_system_repo_repo_global_reactor_full_chain_restore.py`
    - `tests/check_loop_system_repo_repo_global_housekeeping_chain_restore.py`
    - `tests/check_loop_system_repo_repo_reactor_residency_guard_restore.py`
    - `tests/check_loop_system_repo_repo_service_cleanup_authority.py`
    - `tests/check_loop_system_repo_repo_service_cleanup_orphan_fallback.py`
    - `tests/check_loop_system_repo_repo_service_cleanup_convergence_guardrails.py`
    - `tests/check_loop_system_repo_process_hygiene.py`

## Acceptance

- dead process + expired lease cannot remain durably ATTACHED/ACTIVE
  - evidence:
    - `tests/check_loop_system_repo_event_journal_liveness_freshness.py`
    - `tests/check_loop_system_repo_lease_expiry_materialization.py`
    - `tests/check_loop_system_repo_lease_aware_authority_view.py`
    - `tests/check_loop_system_repo_state_query.py`
- pid reuse cannot impersonate an old live child without full identity match
  - evidence:
    - `tests/check_loop_system_repo_launch_reuse_pid_identity.py`
    - `tests/check_loop_system_repo_event_journal_liveness_pid_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_fingerprint_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_launch_identity_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_start_evidence_guard.py`
- every scenario currently recoverable for `codex exec` remains recoverable after the lease model is enabled
  - evidence:
    - `tests/check_loop_system_repo_codex_exec_capability_parity_foundation.py`
    - `tests/check_loop_system_repo_retryable_recovery_status_refresh.py`
    - `tests/check_loop_system_repo_host_child_launch_supervisor.py`
    - `tests/check_loop_system_repo_child_runtime_status_helper.py`
- stale sidecars/observers cannot regain authority after supersession
  - evidence:
    - `tests/check_loop_system_repo_event_journal_liveness_epoch_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_writer_guard.py`
    - `tests/check_loop_system_repo_event_journal_liveness_owner_guard.py`
    - `tests/check_loop_system_repo_housekeeping_controller_lease_fencing.py`
    - `tests/check_loop_system_repo_host_supervisor_lease_fencing.py`
    - `tests/check_loop_system_repo_repo_control_plane_lease_fencing.py`
- LOOP-owned garbage processes cannot accumulate indefinitely outside a lease or durable defer reason
  - evidence:
    - `tests/check_loop_system_repo_event_reap_command_foundation.py`
    - `tests/check_loop_system_repo_reap_reconciler_foundation.py`
    - `tests/check_loop_system_repo_process_reap_gap_characterization.py`
    - `tests/check_loop_system_repo_repo_service_cleanup_authority.py`
    - `tests/check_loop_system_repo_repo_service_cleanup_orphan_fallback.py`
    - `tests/check_loop_system_repo_repo_service_cleanup_convergence_guardrails.py`
    - `tests/check_loop_system_repo_process_hygiene.py`
- top-level repo-root `.loop` initialization is a valid ambient residency entrypoint for the fully lease-owned repo-global service stack, and ordinary authority/status queries remain read-only
  - evidence:
    - `tests/check_loop_system_repo_repo_control_plane_runtime_tree_bootstrap.py`
    - `tests/check_loop_system_repo_repo_control_plane_authority_query_bootstrap.py`
    - `tests/check_loop_system_repo_repo_control_plane_status_query_bootstrap.py`
    - `tests/check_loop_system_repo_repo_global_reactor_restore.py`
    - `tests/check_loop_system_repo_repo_reactor_residency_guard_restore.py`
- once the repo-root `.loop` boundary exists, surviving lease-owned repo-global services can re-form the full repo-global stack through committed requirement facts alone, without query side effects or child-runtime bootstrap dependence
  - evidence:
    - `tests/check_loop_system_repo_repo_control_plane_host_watchdog_restore.py`
    - `tests/check_loop_system_repo_repo_control_plane_housekeeping_watchdog_restore.py`
    - `tests/check_loop_system_repo_repo_control_plane_upper_watchdog_restore.py`
    - `tests/check_loop_system_repo_repo_global_reactor_restore.py`
    - `tests/check_loop_system_repo_repo_global_reactor_full_chain_restore.py`
    - `tests/check_loop_system_repo_repo_global_housekeeping_chain_restore.py`
    - `tests/check_loop_system_repo_repo_reactor_residency_guard_restore.py`
- frozen Milestone 0 parity matrix still passes
  - evidence:
    - `tests/check_loop_system_repo_milestone0_inventory_guardrails.py`

## Explicit non-claims

- Milestone 3 does not complete the command-migration/parity-debugger work from Milestone 4.
- Milestone 3 does not complete the canonical heavy-object lifecycle/share/retention model from Milestone 5.
- Milestone 3 does not claim OS-level auto-start before the repo-root `.loop` control-plane boundary exists.
- The repo-root `.loop` boundary itself is now the legal ambient residency entrypoint for repo-global services; once that durable boundary exists, committed lease/reap authority governs the fully lease-owned repo-global stack without relying on query side effects or child-runtime bootstrap triggers.
- Milestone 3 now additionally freezes the narrower event-driven repo-control-plane requirement semantics:
  - trusted explicit bootstrap and repo-root ambient-init entrypoints may publish canonical `repo_control_plane_required` facts ahead of convergence
  - ordinary authority/status queries remain read-only and do not create repo-global requirement facts as a side effect
  - repo-global residency is anchored at the repo-root `.loop` boundary rather than any child runtime lifecycle
  - surviving lease-owned repo-global services may restore the next layer up through canonical requirement facts (`repo_control_plane_required`, `repo_reactor_required`, `repo_reactor_residency_required`) until the full stack is re-formed

## Closeout rule

- Milestone 3 is not complete unless:
  - all evidence tests above are green
  - `python tests/run.py --profile core` is green except for explicitly pre-existing unrelated failures already recorded before the Milestone 3 closeout attempt

## Outcomes and retrospective

- Verification summary:
  - `/Users/xiongjiangkai/xjk_papers/leanatlas/artifacts/verification/20260325_event_journal_milestone3_closeout_verify.md`
