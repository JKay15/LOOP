# LOOP System Runtime Recovery Contract

This contract freezes the first `runtime-recovery` surface for the standalone LOOP product repo.

## Purpose

Runtime recovery exists to organize `resume`, `retry`, and `relaunch` proposals after a node becomes blocked or failed, and also to recover an orphaned `ACTIVE` node whose backing runtime has detached, while preserving kernel as the only authority that can accept those proposals.

## Runtime anchors

- recovery proposal builders / reviewers: `loop_product/runtime/recover.py`
- committed orphaned-active recovery helper: `loop_product.runtime.recover_orphaned_active_node(...)`
- committed child launch helper: `loop_product.runtime.launch_child_from_result_ref(...)`
- committed child runtime status helper: `loop_product.runtime.child_runtime_status_from_launch_result_ref(...)`
- orphaned-active recovery request schema: `docs/schemas/LoopOrphanedActiveRecoveryRequest.schema.json`
- orphaned-active recovery result schema: `docs/schemas/LoopOrphanedActiveRecoveryResult.schema.json`
- kernel authority boundary: `loop_product/kernel/submit.py`
- kernel topology routing: `loop_product/kernel/topology.py`
- durable node state: `.loop/state/<node_id>.json`
- durable delegation state: `.loop/state/delegations/<node_id>.json`

## Required behavior

- local runtime may build recovery proposals, but it may not apply them directly
- accepted `resume` may reactivate the same blocked node only after kernel review confirms the blocker is resolved
- accepted `retry` may reactivate the same blocked or failed node only after explicit self-attribution and self-repair evidence is recorded
- accepted `relaunch` must materialize a replacement child node from durable delegation inputs and must not rely on hidden host-specific paths or source-workspace assumptions
- accepted `retry` and `relaunch` may also recover an `ACTIVE` node when explicit runtime-loss evidence proves the backing child runtime is gone even though the durable lifecycle has not yet been reclassified
- runtime-loss evidence for orphaned `ACTIVE` recovery must be structured, queryable, and durable enough for audit/experience review rather than existing only as transient chat prose
- before same-node orphaned-active recovery, supervision should query the committed child runtime status helper so a quiet child with a still-live PID remains supervision work rather than being escalated into recovery too early
- same-node orphaned-active retry should prefer the committed `recover_orphaned_active_node(...)` surface so kernel can accept retry and rebuild the canonical launch spec without hand-assembling chat-local launch mutations or hand-writing recovery request JSON in chat
- once same-node orphaned-active retry is accepted and the canonical launch spec exists again, relaunch should prefer the committed `launch_child_from_result_ref(...)` surface rather than a chat-local shell reconstruction of `codex exec`; retryable early provider transport/capacity launch failures should normally be retried there inside the helper's startup-health window before escalating into another recovery turn, and a still-live exact same-node child should be reused instead of starting a duplicate launch
- evaluator recovery inside the same evaluator run must preserve `EvaluatorRunState.json`
- retryable evaluator-owned `RECOVERY_REQUIRED` outcomes remain recovery work rather than terminal closure for the node or task
- same evaluator run recovery must preserve the frozen checker graph and lane plan instead of re-running checker by default
- rejected recovery requests remain rejected audit facts and must not mutate authoritative node state
- self-attribution and self-repair conclusions attached to accepted recovery requests must be queryable through the audit/experience surface

## Forbidden

- local runtime silently resuming or relaunching work without kernel acceptance
- treating host conversation resume, long-term wake-up, or global scheduling as part of this repo-local recovery surface
- hardcoding absolute paths, source-workspace virtualenv assumptions, or repo-specific environment hacks into recovery contracts
