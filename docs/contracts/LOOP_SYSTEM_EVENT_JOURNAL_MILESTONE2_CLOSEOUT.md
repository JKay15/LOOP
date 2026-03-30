# LOOP System Event Journal Milestone 2 Closeout

This document freezes the required deliverables, acceptance, and evidence surfaces for Milestone 2 of the LOOP event-journal migration.

Milestone 2 covers the **projection engine and authoritative observation/debug surfaces**. It does **not** claim completion of the canonical lease/liveness model from Milestone 3, and it does **not** claim completion of the canonical heavy-object lifecycle/share model from Milestone 5.

## Deliverables

- projection metadata on kernel/node projections
  - evidence:
    - `tests/check_loop_system_repo_event_journal_projection_metadata.py`
- commit-state and crash-boundary visibility
  - evidence:
    - `tests/check_loop_system_repo_event_journal_commit_state.py`
    - `tests/check_loop_system_repo_event_journal_projection_reconcile.py`
- projection consistency/debug status view
  - evidence:
    - `tests/check_loop_system_repo_event_journal_projection_status.py`
    - `tests/check_loop_system_repo_event_journal_status.py`
- projection manifest and atomic bundle visibility
  - evidence:
    - `tests/check_loop_system_repo_event_journal_projection_manifest.py`
- projection explanation/debug surface
  - evidence:
    - `tests/check_loop_system_repo_event_journal_projection_explanation.py`
- explicit rebuild boundary
  - evidence:
    - `tests/check_loop_system_repo_event_journal_projection_rebuild.py`
    - `tests/check_loop_system_repo_event_journal_projection_rebuild_blocked.py`
    - `tests/check_loop_system_repo_event_journal_projector_boundary.py`
    - `tests/check_loop_system_repo_event_journal_node_projection_rebuild.py`
- projection provenance
  - evidence:
    - `tests/check_loop_system_repo_event_journal_projection_provenance.py`
- authoritative hygiene/heavy-object observation foundation
  - evidence:
    - `tests/check_loop_system_repo_event_journal_hygiene_observations.py`
    - `tests/check_loop_system_repo_process_reap_gap_characterization.py`
    - `tests/check_loop_system_repo_heavy_object_gap_characterization.py`

## Acceptance

- stale projections are detectable by committed seq vs projection floor
  - evidence:
    - `tests/check_loop_system_repo_event_journal_projection_status.py`
    - `tests/check_loop_system_repo_state_query.py`
- committed-but-unprojected truth can be reconciled or truthfully reported as blocked
  - evidence:
    - `tests/check_loop_system_repo_event_journal_projection_reconcile.py`
    - `tests/check_loop_system_repo_event_journal_projection_rebuild.py`
    - `tests/check_loop_system_repo_event_journal_projection_rebuild_blocked.py`
- visible projection bundles can explain their source journal/bundle provenance
  - evidence:
    - `tests/check_loop_system_repo_event_journal_projection_explanation.py`
    - `tests/check_loop_system_repo_event_journal_projection_provenance.py`
- node-scoped repair cannot rewrite truth from a stale global floor
  - evidence:
    - `tests/check_loop_system_repo_event_journal_node_projection_rebuild.py`
- operational hygiene and heavy-object pressure are now visible to authority query surfaces
  - evidence:
    - `tests/check_loop_system_repo_event_journal_hygiene_observations.py`
    - `tests/check_loop_system_repo_process_reap_gap_characterization.py`
    - `tests/check_loop_system_repo_heavy_object_gap_characterization.py`
- no regression in current trusted `codex exec` / recovery behavior while the projection engine is enabled
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

## Explicit non-claims

- Milestone 2 does not complete the lease/heartbeat/fencing model.
- Milestone 2 does not complete the canonical reap model.
- Milestone 2 does not complete the canonical heavy-object lifecycle/share/retention model.
- Any existing `runtime_reap_*` compatibility events are scaffolding only and do not by themselves satisfy Milestone 3 closeout.

## Closeout rule

- Milestone 2 is not complete unless:
  - all evidence tests above are green
  - `python tests/run.py --profile core` is green except for explicitly pre-existing unrelated failures already recorded before the Milestone 2 closeout attempt

## Outcomes and retrospective

- Milestone 2 deliverables are implemented and the targeted evidence matrix is green.
- Full `core` verification on the Milestone 2 closeout attempt produced:
  - `158` total
  - `155` passed
  - `1` failed (`english_only_policy`, pre-existing and unrelated)
  - `2` timed out
- The timed-out tests were:
  - `loop_evaluator_prototype`
  - `loop_system_wave_simple_eval`
- Both timed-out tests passed on isolated rerun, and both have pre-existing runtime-budget drift evidence that predates this closeout attempt.
- Verification summary:
  - `/Users/xiongjiangkai/xjk_papers/leanatlas/artifacts/verification/20260324_event_journal_milestone2_closeout_verify.md`

Closeout decision:

- Milestone 2 is accepted as complete.
- The remaining `core` red items are classified as pre-existing unrelated failure/budget-pressure items, not Milestone 2 projection-engine regressions.
