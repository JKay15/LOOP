# LOOP System Historical Bug Matrix

This matrix freezes the historically observed bug families that must remain permanent regression targets.

## Bug families

### BUG-001. Split-brain / stale-state drift
- Symptom: `result.json`, `state/<node>.json`, and `kernel_state.json` disagree about the same fact.
- Current regression coverage:
  - `tests/check_loop_system_repo_evaluator_authority_status_sync.py`
  - `tests/check_loop_system_repo_state_query.py`
  - `tests/check_loop_system_repo_state_surface_guardrails.py`
- Final-state invariant: one authoritative fact, many rebuildable projections.

### BUG-002. Replay / idempotence failures
- Symptom: same accepted fact or same logical mutation lands multiple times.
- Current regression coverage:
  - `tests/check_loop_system_repo_kernel_envelope_idempotence.py`
- Final-state invariant: replay may reappear physically, never semantically.

### BUG-003. Identity aliasing
- Symptom: wrong pid, wrong process, or stale `started_existing` alias is treated as the live child.
- Current regression coverage:
  - `tests/check_loop_system_repo_launch_reuse_pid_identity.py`
  - `tests/check_loop_system_repo_child_runtime_status_helper.py`
  - `tests/check_loop_system_repo_host_child_launch_supervisor.py`
- Final-state invariant: launch identity, process identity, and supervision identity must all match before live truth is granted.

### BUG-004. Lifecycle not updating
- Symptom: process dead but node still looks `ACTIVE`, `ATTACHED`, or `pid_alive=true`.
- Current regression coverage:
  - `tests/check_loop_system_repo_retryable_recovery_status_refresh.py`
  - `tests/check_loop_system_repo_child_runtime_status_helper.py`
  - `tests/check_loop_system_repo_host_child_runtime_status_supervisor.py`
- Final-state invariant: stale liveness must converge to truthful non-live state.

### BUG-005. Retry / recovery churn
- Symptom: retries keep spawning or replaying without one governed recovery sequence.
- Current regression coverage:
  - `tests/check_loop_system_repo_runtime_recovery.py`
  - `tests/check_loop_system_repo_child_supervision_spawn_dedupe.py`
  - `tests/check_loop_system_repo_child_supervision_helper.py`
- Final-state invariant: one recovery intent becomes one governed sequence of committed facts.

### BUG-006. Observation pipeline fragility
- Symptom: observer/sidecar failure freezes old runtime status as if it were durable truth.
- Current regression coverage:
  - `tests/check_loop_system_repo_child_supervision_helper.py`
  - `tests/check_loop_system_repo_child_runtime_status_helper.py`
  - `tests/check_loop_system_repo_host_child_runtime_status_supervisor.py`
- Final-state invariant: observation loss is itself modeled and cannot masquerade as fresh truth.

### BUG-007. Topology / ownership drift
- Symptom: parent/child ownership, split responsibility, or local-repair semantics drift away from authority state.
- Current regression coverage:
  - `tests/check_loop_system_repo_runtime_recovery.py`
  - `tests/check_loop_system_repo_state_query.py`
  - `tests/check_loop_system_repo_kernel_envelope_idempotence.py`
- Final-state invariant: topology authority and ownership transitions are committed, queryable facts.

### BUG-008. Runtime artifact topology drift
- Symptom: runtime-owned heavy trees or publish staging bypass invariants and later pollute truth/recovery.
- Current regression coverage:
  - `tests/check_loop_system_repo_publish_staging_hygiene.py`
  - `tests/check_loop_system_repo_child_runtime_status_helper.py`
- Final-state invariant: runtime-owned artifact topology is explicit, bounded, and never confused with authority state.

### BUG-009. Operational hygiene outside authority
- Symptom: orphaned sidecars, supervisors, or stopped-run leftovers continue existing without a committed cleanup/reap/defer fact.
- Current regression coverage:
  - `tests/check_loop_system_repo_process_hygiene.py`
  - `tests/check_loop_system_repo_stopped_run_gc.py`
  - `tests/check_loop_system_repo_cleanup_authority_runtime_identity_guard.py`
  - `tests/check_loop_system_repo_housekeeping_controller_proactive_bootstrap.py`
  - `tests/check_loop_system_repo_housekeeping_controller_host_supervisor_bootstrap.py`
  - `tests/check_loop_system_repo_host_supervisor_authority_mirror.py`
  - `tests/check_loop_system_repo_host_supervisor_authority_reuse.py`
  - `tests/check_loop_system_repo_host_supervisor_authoritative_exit.py`
  - `tests/check_loop_system_repo_host_supervisor_lease_fencing.py`
  - `tests/check_loop_system_repo_housekeeping_controller_repo_lifecycle_bootstrap.py`
  - `tests/check_loop_system_repo_repo_control_plane_bootstrap.py`
  - `tests/check_loop_system_repo_repo_control_plane_service_loop.py`
  - `tests/check_loop_system_repo_repo_control_plane_authority_mirror.py`
  - `tests/check_loop_system_repo_repo_control_plane_authority_reuse.py`
  - `tests/check_loop_system_repo_repo_control_plane_repo_lifecycle_bootstrap.py`
  - `tests/check_loop_system_repo_repo_control_plane_runtime_tree_bootstrap.py`
  - `tests/check_loop_system_repo_repo_control_plane_first_child_bootstrap.py`
  - `tests/check_loop_system_repo_repo_control_plane_authority_query_bootstrap.py`
  - `tests/check_loop_system_repo_repo_control_plane_host_watchdog_restore.py`
  - `tests/check_loop_system_repo_repo_control_plane_housekeeping_watchdog_restore.py`
  - `tests/check_loop_system_repo_repo_global_reactor_restore.py`
  - `tests/check_loop_system_repo_repo_reactor_residency_guard_restore.py`
  - `tests/check_loop_system_repo_repo_control_plane_authoritative_exit.py`
  - `tests/check_loop_system_repo_repo_control_plane_lease_fencing.py`
  - `tests/check_loop_system_repo_repo_service_cleanup_authority.py`
  - `tests/check_loop_system_repo_repo_service_cleanup_orphan_fallback.py`
  - `tests/check_loop_system_repo_housekeeping_controller_service_mode.py`
  - `tests/check_loop_system_repo_housekeeping_controller_authoritative_exit.py`
  - `tests/check_loop_system_repo_housekeeping_controller_lease_fencing.py`
  - `tests/check_loop_system_repo_housekeeping_controller_authority_reuse.py`
  - `tests/check_loop_system_repo_housekeeping_controller_authority_mirror.py`
  - `tests/check_loop_system_repo_housekeeping_reap_controller.py`
  - `tests/check_loop_system_repo_housekeeping_reap_controller_loop.py`
  - `tests/check_loop_system_repo_housekeeping_reap_request_autobootstrap.py`
- `tests/check_loop_system_repo_reap_reconciler_foundation.py`
- `tests/check_loop_system_repo_sidecar_cleanup_authority_guard.py`
- `tests/check_loop_system_repo_process_reap_gap_characterization.py`
- `tests/check_loop_system_repo_sidecar_runtime_context_missing_exit.py`
- Characterization baseline only: `check_loop_system_repo_process_reap_gap_characterization.py` remains the explicit known-gap baseline for the remaining repo-root resident cleanup/reap surface; it confirms the current convergence shape without claiming that every future process-reclamation path is already fully shipped.
- Current capability note: `check_loop_system_repo_process_reap_gap_characterization.py` now verifies repo-root resident cleanup convergence directly: committed reap intent retires the stopped runtime root, terminates lingering owned processes, and leaves the housekeeping service resident afterward. Top-level repo runtime-tree initialization at the repo-root `.loop` boundary ambiently converges the full repo-global service stack, while trusted authority/status queries remain read-only and do not act as hidden bootstrap surfaces.
- Final-state invariant: cleanup/reap/defer is authoritative and explainable, not an out-of-band shell side effect.

### BUG-010. Heavy-object duplication and unmanaged retention
- Symptom: `.lake`, mathlib packs, evaluator staging trees, or publish staging roots duplicate and accumulate outside an authority-tracked sharing/retention model.
- Current regression coverage:
  - `tests/check_loop_system_repo_publish_staging_hygiene.py`
  - `tests/check_loop_system_repo_heavy_object_gap_characterization.py`
  - `tests/check_loop_system_repo_heavy_object_nested_runtime_authority_bridge.py`
- Graduated from characterization-only evidence: `check_loop_system_repo_heavy_object_gap_characterization.py` now runs as a standard `core` regression because nested runtime heavy-object gaps converge through repo-anchor authoritative registration/discovery/remediation truth instead of remaining a frozen known-gap baseline.
- Current capability note: nested runtime roots now bridge heavy-object authority queries through the repo-anchor `.loop` runtime when the local runtime carries no heavy-object lifecycle facts of its own, so repo-global auto-audit/auto-registration convergence is visible through the standard read-only status/commit/projection surfaces without adding a second writer or query-side bootstrap.
- Final-state invariant: heavyweight runtime objects are shared, pinned, superseded, and reclaimed through authoritative object lifecycle facts.
