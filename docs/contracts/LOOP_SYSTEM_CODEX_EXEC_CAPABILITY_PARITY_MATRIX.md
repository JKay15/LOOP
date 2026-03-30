# LOOP System `codex exec` Capability-Parity Matrix

This matrix freezes the currently trusted `codex exec` recovery behaviors that the final architecture must preserve or improve.

Current focused deterministic harness:
- `tests/check_loop_system_repo_codex_exec_capability_parity_foundation.py`
- This harness replays the currently frozen evidence set for the Milestone 3 parity scenarios below; it is a gate, not a replacement for the underlying scenario tests.

## Capability scenarios

### CE-001. Quota/provider interruption resumes from durable checkpoint
- Current trusted behavior: same logical node can be resumed or retried from durable handoff/result context after interruption.
- Current evidence:
  - `tests/check_loop_system_repo_runtime_recovery.py`
  - `tests/check_loop_system_repo_retryable_recovery_status_refresh.py`
- Final parity gate: no loss of successful recovery semantics after journal migration.

### CE-002. Sidecar loss does not strand fake-live state forever
- Current trusted behavior: hardened paths can degrade fake live truth and continue recovery instead of freezing forever.
- Current evidence:
  - `tests/check_loop_system_repo_runtime_loss_confirmed_event.py`
  - `tests/check_loop_system_repo_host_child_runtime_status_supervisor.py`
- Final parity gate: sidecar loss becomes a governed lease/liveness transition, not a stale `ACTIVE`.

### CE-003. `started_existing` reuse requires identity continuity
- Current trusted behavior: hardened checks reject several stale/wrong reuse cases.
- Current evidence:
  - `tests/check_loop_system_repo_launch_reuse_pid_identity.py`
  - `tests/check_loop_system_repo_host_child_launch_supervisor.py`
- Final parity gate: no live reuse without launch-lineage and process-identity confirmation.

### CE-004. Wrong pid / wrong wrapper cannot satisfy live truth
- Current trusted behavior: wrong-process attachment is rejected in key helper paths.
- Current evidence:
  - `tests/check_loop_system_repo_launch_reuse_pid_identity.py`
- Final parity gate: lease + identity + fencing prevent wrong-process live projection everywhere.

### CE-005. Evaluator/result context survives attempt rollover
- Current trusted behavior: repair and rerun can preserve evaluator/result authority across attempts.
- Current evidence:
  - `tests/check_loop_system_repo_evaluator_authority_status_sync.py`
  - `tests/check_loop_system_repo_retryable_recovery_status_refresh.py`
- Final parity gate: attempt changes preserve durable evaluator/result/repair context by construction.

### CE-006. Duplicate side effects are blocked during retry/resume churn
- Current trusted behavior: replayed accepted envelopes and duplicate supervision spawns are now bounded on hardened paths.
- Current evidence:
  - `tests/check_loop_system_repo_kernel_envelope_idempotence.py`
  - `tests/check_loop_system_repo_child_supervision_spawn_dedupe.py`
- Final parity gate: command replay and retry churn remain logically exact-once.

### CE-007. Child death without exact exit observation still converges to truthful non-live state
- Current trusted behavior: several hardened paths can self-heal stale live snapshots into truthful loss/terminal state.
- Current evidence:
  - `tests/check_loop_system_repo_runtime_loss_confirmed_event.py`
  - `tests/check_loop_system_repo_host_child_runtime_status_supervisor.py`
- Final parity gate: lease expiry / runtime-loss confirmation guarantee eventual truthful downgrade.

### CE-008. Mixed-version coexistence does not corrupt authority
- Current trusted behavior: this is not yet fully guaranteed and is therefore a migration-critical gap.
- Current evidence:
  - planned only in Milestone 0; no full direct regression yet
- Final parity gate: old and new writers may coexist temporarily without conflicting durable truth.

### CE-009. Real-root recovery of `r59`
- Current trusted behavior: `r59` is preserved as a real historically messy runtime root.
- Current evidence:
  - `/Users/xiongjiangkai/xjk_papers/leanatlas/.cache/leanatlas/tmp/r59_golden_recovery_checkpoint_20260324T002141.json`
- Final parity gate: final architecture must recover `r59` and drive it to truthful bug-free completion.

### CE-010. Orphaned runtime stacks are reclaimed without weakening recovery
- Current trusted behavior: the repo can sometimes kill stale stopped-run descendants or idle supervisors, but this is not yet part of one authoritative recovery model.
- Current evidence:
  - `tests/check_loop_system_repo_process_hygiene.py`
- `tests/check_loop_system_repo_stopped_run_gc.py`
- `tests/check_loop_system_repo_housekeeping_controller_authority_reuse.py`
- `tests/check_loop_system_repo_housekeeping_controller_authority_mirror.py`
- `tests/check_loop_system_repo_process_reap_gap_characterization.py`
- Characterization baseline only: `check_loop_system_repo_process_reap_gap_characterization.py` remains the explicit known-gap baseline for the repo-root resident cleanup/reap surface; it confirms the current authoritative convergence shape without claiming that every recovery-owned reclamation path is already complete.
- Current evidence now also includes repo-root resident cleanup convergence: `check_loop_system_repo_process_reap_gap_characterization.py` verifies committed reap intent retires the stopped runtime root, terminates lingering owned processes, and leaves the housekeeping service resident afterward.
- Final parity gate: process cleanup becomes event-driven without losing any currently working recovery or resume path for `codex exec`-backed nodes.

### CE-011. Heavy runtime content remains recoverable while becoming shareable
- Current trusted behavior: runtime roots can recover from large artifact trees today, but `.lake`/mathlib duplication is still unmanaged and expensive.
- Current evidence:
  - `tests/check_loop_system_repo_publish_staging_hygiene.py`
  - `tests/check_loop_system_repo_heavy_object_gap_characterization.py`
- `tests/check_loop_system_repo_heavy_object_nested_runtime_authority_bridge.py`
- Graduated from characterization-only evidence: `check_loop_system_repo_heavy_object_gap_characterization.py` now runs as a standard `core` regression because nested runtime heavy-object authority queries converge through repo-anchor lifecycle truth instead of remaining a frozen known-gap-only characterization.
- Current evidence now also includes repo-anchor heavy-object authority takeover for nested runtime roots: `check_loop_system_repo_heavy_object_gap_characterization.py` and `check_loop_system_repo_heavy_object_nested_runtime_authority_bridge.py` verify that child runtime queries converge through repo-anchor registration/discovery/remediation truth instead of leaving unmanaged duplicate trees invisible to generic read-only surfaces.
- Final parity gate: the final architecture shares heavyweight runtime content without making `codex exec` resume, evaluator rerun, or artifact recovery weaker than today.
