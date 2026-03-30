# LOOP System Event Journal Milestone 4 Targeted Closeout

This document freezes the **targeted-closeout** deliverables, acceptance, and evidence surfaces for Milestone 4 of the LOOP event-journal migration.

Milestone 4 covers the **command migration and observability** layer on top of the Milestone 3 lease/reap control plane. It freezes the currently landed command-event, read-only trace, lineage, and parity-debugger surfaces. It does **not** claim formal closeout yet, because the final `core` gate remains deferred.

## Deliverables

- topology-family command migration onto canonical accepted command facts plus read-only trace, including nested visible node-lineage reuse for affected source/target nodes
  - evidence:
    - `tests/check_loop_system_repo_event_journal_topology_command_event.py`
    - `tests/check_loop_system_repo_topology_command_trace.py`
    - `tests/check_loop_system_repo_event_journal_activate_command_event.py`
    - `tests/check_loop_system_repo_activate_command_trace.py`
    - `tests/check_loop_system_repo_event_journal_recovery_command_event.py`
    - `tests/check_loop_system_repo_recovery_command_trace.py`
    - `tests/check_loop_system_repo_event_journal_relaunch_command_event.py`
    - `tests/check_loop_system_repo_relaunch_command_trace.py`
- evaluator/terminal result migration onto canonical facts plus read-only trace
  - evidence:
    - `tests/check_loop_system_repo_event_journal_evaluator_result_event.py`
    - `tests/check_loop_system_repo_evaluator_result_trace.py`
    - `tests/check_loop_system_repo_event_journal_terminal_result_event.py`
    - `tests/check_loop_system_repo_terminal_result_trace.py`
  - note:
    - the evaluator/terminal trace surfaces now reuse the visible source-node lineage view instead of exposing only flat source-node summaries
- node / launch / lease / runtime-identity read-only trace tooling, with node-lineage carrying the nested runtime-identity surface
  - evidence:
    - `tests/check_loop_system_repo_launch_lineage_trace.py`
    - `tests/check_loop_system_repo_lease_lineage_trace.py`
    - `tests/check_loop_system_repo_node_lineage_trace.py`
    - `tests/check_loop_system_repo_runtime_identity_trace.py`
- migration parity debugger surfaces
  - evidence:
    - `tests/check_loop_system_repo_command_parity_debugger.py`
    - `tests/check_loop_system_repo_topology_command_family_parity.py`
    - `tests/check_loop_system_repo_runtime_identity_parity_debugger.py`
    - `tests/check_loop_system_repo_lineage_parity_debugger.py`
- repo-tree cleanup command+trace+parity promoted to a formal command surface
  - evidence:
    - `tests/check_loop_system_repo_event_journal_repo_tree_cleanup_event.py`
    - `tests/check_loop_system_repo_repo_tree_cleanup_trace.py`
    - `tests/check_loop_system_repo_repo_tree_cleanup_parity_debugger.py`

## Acceptance

- `split/activate/retry/resume/evaluator/terminal` mutations travel through explicit command -> event -> projection / trace surfaces
  - evidence:
    - `tests/check_loop_system_repo_event_journal_topology_command_event.py`
    - `tests/check_loop_system_repo_event_journal_activate_command_event.py`
    - `tests/check_loop_system_repo_event_journal_recovery_command_event.py`
    - `tests/check_loop_system_repo_event_journal_relaunch_command_event.py`
    - `tests/check_loop_system_repo_event_journal_evaluator_result_event.py`
    - `tests/check_loop_system_repo_event_journal_terminal_result_event.py`
- humans can audit one command or one runtime identity through read-only causal history without mutating authority
  - evidence:
    - `tests/check_loop_system_repo_topology_command_trace.py`
    - `tests/check_loop_system_repo_activate_command_trace.py`
    - `tests/check_loop_system_repo_recovery_command_trace.py`
    - `tests/check_loop_system_repo_relaunch_command_trace.py`
    - `tests/check_loop_system_repo_evaluator_result_trace.py`
    - `tests/check_loop_system_repo_terminal_result_trace.py`
    - `tests/check_loop_system_repo_launch_lineage_trace.py`
    - `tests/check_loop_system_repo_lease_lineage_trace.py`
    - `tests/check_loop_system_repo_node_lineage_trace.py`
    - `tests/check_loop_system_repo_runtime_identity_trace.py`
- migration parity remains explicit rather than silently normalizing divergence away
  - evidence:
    - `tests/check_loop_system_repo_command_parity_debugger.py`
    - `tests/check_loop_system_repo_topology_command_family_parity.py`
    - `tests/check_loop_system_repo_runtime_identity_parity_debugger.py`
    - `tests/check_loop_system_repo_lineage_parity_debugger.py`
    - `tests/check_loop_system_repo_repo_tree_cleanup_parity_debugger.py`
- historical bug regressions remain green while M4 is in progress
  - evidence:
    - `docs/contracts/LOOP_SYSTEM_HISTORICAL_BUG_MATRIX.md`
- all exact non-core profiles remain green while `core` stays deferred
  - evidence:
    - `tests/run.py --profile nightly-only (exact selection)`
    - `tests/run.py --profile soak-only (exact selection)`

## Explicit non-claims

- Milestone 4 targeted closeout does not replace the formal `core` gate.
- Milestone 4 targeted closeout does not complete Milestone 5 heavy-object lifecycle/share/retention work.
- Milestone 4 targeted closeout does not claim that every possible future command family has already been migrated; it freezes only the command families listed above.

## Closeout rule

- Milestone 4 targeted closeout is not complete unless:
  - all evidence tests above are green
  - the full historical bug matrix is green
  - the exact non-core profiles (`nightly-only`, `soak-only`) are green
  - LOOP process residue converges back to zero after verification
- Milestone 4 formal closeout is not complete until:
  - `python tests/run.py --profile core` is green under the Milestone 4 closeout attempt

## Outcomes and retrospective

- Targeted-closeout verification summary:
  - `/Users/xiongjiangkai/xjk_papers/leanatlas/artifacts/verification/20260325_event_journal_milestone4_targeted_closeout_verify.md`
- Primary strict-gate log:
  - `/Users/xiongjiangkai/xjk_papers/leanatlas/artifacts/verification/20260325_milestone4_strict_noncore_gate_rerun_result_trace_lineage_integration.log`
- Verified status on 2026-03-25:
  - historical bug matrix `52/52` green
  - exact `nightly-only` `3/3` green
  - exact `soak-only` `11/11` green
  - hygiene/post checks `10/10` green
  - residue converged to zero after every phase
