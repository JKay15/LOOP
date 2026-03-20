# LOOP System Node Contract

This contract freezes `IF-2` for materialized LOOP nodes.

## Purpose

Every LOOP execution unit is a `node`, and child materialization must pass through `child-dispatch`.

## Runtime anchors

- node schema: `docs/schemas/LoopSystemNodeSpec.schema.json`
- first-child bootstrap request schema: `docs/schemas/LoopFirstImplementerBootstrapRequest.schema.json`
- first-child bootstrap result schema: `docs/schemas/LoopFirstImplementerBootstrapResult.schema.json`
- orphaned-active recovery request schema: `docs/schemas/LoopOrphanedActiveRecoveryRequest.schema.json`
- orphaned-active recovery result schema: `docs/schemas/LoopOrphanedActiveRecoveryResult.schema.json`
- child progress snapshot schema: `docs/schemas/LoopChildProgressSnapshot.schema.json`
- child materialization: `loop_product/dispatch/child_dispatch.py`
- first-child bootstrap helper: `loop_product.runtime.bootstrap_first_implementer_node(...)`
- endpoint-driven first-child bootstrap helper: `loop_product.runtime.bootstrap_first_implementer_from_endpoint(...)`
- orphaned-active recovery helper: `loop_product.runtime.recover_orphaned_active_node(...)`
- child runtime status helper: `loop_product.runtime.child_runtime_status_from_launch_result_ref(...)`
- child progress snapshot helper: `loop_product.runtime.child_progress_snapshot_from_launch_result_ref(...)`
- child supervision result schema: `docs/schemas/LoopChildSupervisionResult.schema.json`
- child supervision helper: `loop_product.runtime.supervise_child_until_settled(...)`
- durable node state: `.loop/state/<node_id>.json`
- frozen delegation artifact: `.loop/state/delegations/<node_id>.json`

## Required node fields

- `node_id`
- `parent_node_id`
- `generation`
- `round_id`
- `node_kind`
- `goal_slice`
- `execution_policy`
- `reasoning_profile`
- `budget_profile`
- `allowed_actions`
- `workspace_root`
- `depends_on_node_ids`
- `activation_condition`
- `runtime_state`
- `delegation_ref`
- `result_sink_ref`
- `lineage_ref`

## Required behavior

- kernel freezes delegation before the child starts
- the serialized node protocol must carry execution-strategy data explicitly: `execution_policy.agent_provider`, `execution_policy.sandbox_mode`, and `reasoning_profile.thinking_budget` cannot stay only in prose
- child-dispatch materializes the child from frozen inputs
- the committed endpoint-driven first-child bootstrap helper and wrapper should derive the normal first-child bootstrap request from the clarified endpoint artifact so root-chat does not need to probe `--help`, inspect request schemas, rediscover helper internals, or pre-create guessed workspace folders during the ordinary path
- when the clarified endpoint already names exact existing local files or directories, the committed endpoint-driven first-child bootstrap helper should preserve those explicit local refs in the derived `context_refs` instead of forcing the child to rediscover them from prose
- the committed first-child bootstrap helper must deterministically render `FrozenHandoff.json`, `FROZEN_HANDOFF.md`, and `CHILD_PROMPT.md` from frozen inputs instead of relying on root-chat freeform prose
- the committed first-child bootstrap helper must also pre-materialize a task-scoped exact evaluator submission artifact, task-scoped evaluator manual/final-effects refs, and an exact evaluator runner for that node instead of forcing the implementer to infer evaluator wrapper arguments in chat
- implementer child nodes must materialize one explicit project folder under `workspace/` and serialize that folder as `workspace_root`
- node serialization must also carry a separate `runtime_state` so authoritative lifecycle does not have to pretend that a detached child runtime is still live
- child-dispatch must expose child-local context as the materialized node snapshot plus the frozen delegation artifact, without requiring the raw parent transcript
- child reports heartbeat, local control, and terminal status upward
- if a node proposes a split, that proposal must remain a proposal until kernel accepts it; only then may child-dispatch materialize the accepted split children from frozen delegation data
- accepted split children must match the reviewed `target_node_ids` exactly and must inherit the kernel-owned `parent generation + 1` step rather than proposer-supplied generation drift
- accepted deferred split children must persist `depends_on_node_ids` and `activation_condition` in both node state and frozen delegation, and must land as `PLANNED` rather than `ACTIVE`
- accepted deferred activation must promote the already-materialized `PLANNED` child in place to `ACTIVE` without rewriting its frozen dependency metadata or rematerializing a duplicate node
- if a blocked parallel source node or a completed deferred source node proposes a merge, that proposal must remain a proposal until kernel accepts it; accepted merge reactivates the source node only after its declared child branches are already completed
- accepted deferred merge must preserve completed child evidence while resetting the reactivated source node's runtime state truthfully for the next active segment
- terminal non-root nodes may be reaped only after kernel accepts a reap request and archives the retired node outside the live authoritative graph
- if a node proposes `resume`, `retry`, or `relaunch`, that recovery proposal must remain a proposal until kernel accepts it
- implementer child nodes must carry recovery permissions needed for kernel-owned `resume`, `retry`, and `relaunch` instead of shipping as non-recoverable minimal workers
- accepted `relaunch` must materialize a replacement child from durable delegation data under the original parent rather than mutating the source node in place and pretending no restart happened
- accepted same-node orphaned-active retry must preserve the same `workspace_root`, reuse the existing `CHILD_PROMPT.md`, and rebuild the canonical launch spec from serialized node policy instead of inventing launch variants or hand-authored recovery request JSON in chat
- accepted same-node orphaned-active retry must also record which committed child launch result and fresh runtime-status result justified the recovery, so later audit can distinguish a truly detached child from a patient supervision window
- committed child runtime surfaces must also expose enough durable evidence for root-side progress snapshots to summarize live state without reconstructing status from chat memory alone
- workspace-local relaunch context should be persisted through one committed `RECOVERY_CONTEXT.md` stop-point file so the resumed implementer can read the latest kernel-approved recovery reason without reconstructing state from chat
- implementer handoff/prompt generation must keep the primary workspace layout and local-input discovery path explicit: durable implementation files normally belong inside the materialized workspace, while endpoint-required repo-local or user-local input discovery may inspect exact external paths when the needed file was not already materialized there
- when endpoint-required repo-local or user-local file discovery is still needed, the committed handoff/prompt surface should prefer a repo-shipped filename/path discovery helper rather than nudging the implementer toward ad hoc content grep
- child/evaluator document-observation for supervision should come from committed launch/evaluator logs and snapshot helpers, not from bespoke child-authored “I read these docs” side reports
- implementer nodes must not end their own conversation or present unfinished recoverable work as done; they should continue the loop or leave explicit recovery evidence for same-node continuation
- when the frozen endpoint requires local/offline user assets, the committed handoff/prompt surface should keep sourcing local-first: search canonical local roots such as Desktop, Music, and Downloads before inventing narrower ad hoc roots, and do not browse/download remote substitutes unless the frozen handoff explicitly allows remote sourcing
- `execution_policy`, `reasoning_profile`, and `budget_profile` must be serialized in the node contract, not left only in prose
- `workspace_root` must be serialized in the node contract, not left only in prose
- child-local execution consumes the frozen delegation artifact and node-local context instead of the parent chat transcript

## Forbidden

- child nodes declaring topology changes accepted
- child nodes declaring runtime recovery accepted
- execution policy or provider/sandbox selection existing only in prose instead of the node contract
- materializing a child without also persisting its delegation artifact
