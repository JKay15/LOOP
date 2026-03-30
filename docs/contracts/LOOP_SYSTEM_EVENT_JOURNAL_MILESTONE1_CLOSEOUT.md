# LOOP System Event Journal Milestone 1 Closeout

This document freezes the required deliverables, acceptance, and evidence surfaces for Milestone 1 of the LOOP event-journal migration.

## Deliverables

- canonical event schema
  - evidence:
    - `loop_product/event_journal/store.py`
    - `tests/check_loop_system_repo_event_journal_foundation.py`
- embedded journal store
  - evidence:
    - `loop_product/event_journal/store.py`
    - `tests/check_loop_system_repo_event_journal_meta.py`
    - `tests/check_loop_system_repo_event_journal_meta_guardrails.py`
- atomic commit API returning global `seq`
  - evidence:
    - `tests/check_loop_system_repo_event_journal_foundation.py`
- command id / event id / causal ref model
  - evidence:
    - `tests/check_loop_system_repo_event_journal_foundation.py`
- compatibility shim proving old trusted writer outcomes can be mirrored into the journal without changing observable recovery semantics
  - evidence:
    - `tests/check_loop_system_repo_event_journal_submit_mirror.py`
    - `tests/check_loop_system_repo_evaluator_authority_status_sync.py`
    - `tests/check_loop_system_repo_retryable_recovery_status_refresh.py`
- schema-versioned event envelope format
  - evidence:
    - `tests/check_loop_system_repo_event_journal_schema_guardrails.py`
    - `tests/check_loop_system_repo_event_journal_compatibility_status.py`
- commit-state model documenting crash points and recovery semantics
  - evidence:
    - `tests/check_loop_system_repo_event_journal_commit_state.py`
    - `tests/check_loop_system_repo_event_journal_projection_reconcile.py`
    - `tests/check_loop_system_repo_event_journal_projection_rebuild.py`
    - `tests/check_loop_system_repo_event_journal_projection_rebuild_blocked.py`

## Acceptance

- a committed event can be appended exactly once
  - evidence:
    - `tests/check_loop_system_repo_event_journal_foundation.py`
- replay with same command/event identity is idempotent
  - evidence:
    - `tests/check_loop_system_repo_event_journal_foundation.py`
    - `tests/check_loop_system_repo_kernel_envelope_idempotence.py`
- no regression in current `codex exec` recovery behavior under mirrored writes
  - evidence:
    - `tests/check_loop_system_repo_evaluator_authority_status_sync.py`
    - `tests/check_loop_system_repo_launch_reuse_pid_identity.py`
    - `tests/check_loop_system_repo_retryable_recovery_status_refresh.py`
    - `tests/check_loop_system_repo_child_supervision_spawn_dedupe.py`
    - `tests/check_loop_system_repo_host_child_launch_supervisor.py`
    - `tests/check_loop_system_repo_child_runtime_status_helper.py`
- frozen Milestone 0 parity matrix still passes
  - evidence:
    - `tests/check_loop_system_repo_milestone0_inventory_guardrails.py`
- old-vs-new mixed writes cannot silently reinterpret committed truth
  - evidence:
    - `tests/check_loop_system_repo_event_journal_schema_guardrails.py`
    - `tests/check_loop_system_repo_event_journal_surface_guardrails.py`
    - `tests/check_loop_system_repo_event_journal_compatibility_status.py`

## Closeout rule

- Milestone 1 is not complete unless:
  - all evidence tests above are green
  - `python tests/run.py --profile core` is green except for explicitly pre-existing unrelated failures already recorded before the Milestone 1 closeout attempt
