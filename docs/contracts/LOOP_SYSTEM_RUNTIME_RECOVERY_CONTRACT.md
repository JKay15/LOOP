# LOOP System Runtime Recovery Contract

This contract freezes the first `runtime-recovery` surface for the standalone LOOP product repo.

## Purpose

Runtime recovery exists to organize `resume`, `retry`, and `relaunch` proposals after a node becomes blocked or failed, and also to recover an orphaned `ACTIVE` node whose backing runtime has detached, while preserving kernel as the only authority that can accept those proposals.

## Runtime anchors

- recovery proposal builders / reviewers: `loop_product/runtime/recover.py`
- committed orphaned-active recovery helper: `loop_product.runtime.recover_orphaned_active_node(...)`
- committed child launch helper: `loop_product.runtime.launch_child_from_result_ref(...)`
- committed child runtime status helper: `loop_product.runtime.child_runtime_status_from_launch_result_ref(...)`
- committed child progress snapshot helper: `loop_product.runtime.child_progress_snapshot_from_launch_result_ref(...)`
- committed child supervision helper: `loop_product.runtime.supervise_child_until_settled(...)`
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
- while supervision is still active, root must not end the root conversation or accept task closeout before the child settles; the committed child progress snapshot helper is an optional observation surface when root needs live state, evaluator state, or doc-usage evidence
- if a child stays live but committed progress snapshots remain placeholder-only and unchanged across a bounded no-substantive-progress window, the committed child supervision helper may return a truthful `no_substantive_progress` stop point instead of waiting forever or pretending recovery applies
- that `no_substantive_progress` stop point must be real, not cosmetic: before returning it, the committed supervision helper must terminate the runtime-owned live child and refresh runtime status so root does not stop supervising while the child is still running
- fresh implementer startup should also get one bounded grace window for slow high-effort planning, and that startup grace may scale with the frozen reasoning budget rather than using one fixed cap for every node; but if the frozen required workspace mirror or workspace-local result path stays completely empty throughout that startup window, repeated “writing now” or planning prose must not keep the node truthfully `ACTIVE`; the committed child supervision helper may settle it as `no_substantive_progress`
- that committed child runtime status helper must treat signal-probe permission denial as inconclusive rather than as proof of death, and should fall back to a host-observable pid check such as `ps` before reporting `pid_alive=false`
- that committed child runtime status helper must distinguish the live build root from the publish root: direct mutation of `deliverables/primary_artifact` without a matching runtime-owned publication receipt is a publication violation
- that committed child runtime status helper must first absorb any authoritative child result already present at the committed result sink through a trusted runtime-owned sync path before reporting lifecycle or recovery eligibility, including host-side runtime-status supervision
- when the frozen live build root is external to the workspace, the committed child runtime status helper must also treat any workspace-local `.tmp_primary_artifact*`, root-level `.lake`/`.git`/`.venv`/`.uv-cache`, or equivalent runtime-owned heavy tree as a recovery-worthy hygiene violation rather than acceptable local scratch state
- when such a publication violation appears while the child is still live, the committed runtime-status/helper path may terminate the live child and route same-node recovery through the existing kernel-reviewed recovery surface instead of silently scrubbing the publish root and pretending nothing happened
- a single `pid_alive=false` observation for an `ACTIVE` child is not enough by itself to trigger orphaned-active recovery when launch logs are still recent; the committed status helper must keep that node under supervision until runtime-loss is confirmed by a quiet window, explicit exit evidence, or persisted lost attachment state
- an authoritative terminal `implementer_result.json` for implementer supervision requires evaluator-backed closure evidence, not just `status=COMPLETED`; at minimum the durable result must carry a terminal evaluator verdict plus a resolvable terminal evaluator report ref
- if such an authoritative terminal `implementer_result.json` already exists for that node, the committed status helper must suppress orphaned-active recovery even when stale node state still says `ACTIVE`, and should report truthful terminal closeout instead of continuing recovery churn
- a hand-written or incomplete `implementer_result.json` that claims `COMPLETED` without terminal evaluator evidence must remain recovery work after the child stops; supervision must not normalize that file into authoritative closeout or let it suppress same-node recovery
- once a child node already reconciles to a terminal lifecycle such as `BLOCKED`, `FAILED`, or evaluator-backed `COMPLETED` with terminal runtime attachment, the committed child supervision helper must settle that authoritative result instead of classifying it as an incomplete terminal and forcing same-node recovery
- same-node orphaned-active retry should prefer the committed `recover_orphaned_active_node(...)` surface so kernel can accept retry and rebuild the canonical launch spec without hand-assembling chat-local launch mutations or hand-writing recovery request JSON in chat
- that committed orphaned-active recovery helper must prefer an exact caller-confirmed launch/status pair when supervision has already persisted one, and otherwise perform its own fresh runtime-status recheck against a committed child launch result; in either case it must reject recovery if the exact same-node child is still live or runtime loss is not yet confirmed
- once same-node orphaned-active retry is accepted and the canonical launch spec exists again, relaunch should prefer the committed `launch_child_from_result_ref(...)` surface rather than a chat-local shell reconstruction of `codex exec`; retryable early provider transport/capacity launch failures should normally be retried there inside the helper's startup-health window before escalating into another recovery turn, that helper should scrub parent `CODEX_*` session/transport env vars plus disable OTEL exporters before nested child startup, should preserve terminal-compatible Codex stdout/stderr semantics instead of binding the child directly to regular-file stdio capture, and a still-live exact same-node child should be reused instead of starting a duplicate launch
- if root-side supervision later observes retryable launch/runtime loss for the same child again, it should apply a bounded issue-aware cooldown before same-node recovery/relaunch instead of turning transient provider churn into a rapid attempt storm; persisted launch-result evidence such as `retryable_failure_kind=provider_capacity` is authoritative input for that cooldown
- evaluator recovery inside the same evaluator run must preserve `EvaluatorRunState.json`
- retryable evaluator-owned `RECOVERY_REQUIRED` outcomes remain recovery work rather than terminal closure for the node or task
- if a delegated evaluator role already classified a failure as a retryable provider issue during bounded local retries, later exception/report materialization must preserve that issue kind across retry exhaustion so same-run recovery stays available instead of collapsing into terminal untyped runtime failure
- same evaluator run recovery must preserve the frozen checker graph and lane plan instead of re-running checker by default
- if a child stops after a retryable terminal evaluator result, root-side supervision should continue through the committed child supervision helper rather than treating that stop as settled completion
- nested host-side child-launch supervision must stay visible through a committed runtime marker and may exit after bounded idle once no pending launch/runtime-status requests remain, so helper processes do not survive silently after the run has settled
- forcing host-side direct child-launch mode is a recursion guard for the request path, not a license to turn off the safer terminal-compatible capture bridge for the actual Codex subprocess
- any claim that a child or evaluator actually read repo documentation during that live period should come from document refs observed in committed logs, as surfaced by the child progress snapshot helper, rather than from free-form supervisor guesswork
- rejected recovery requests remain rejected audit facts and must not mutate authoritative node state
- self-attribution and self-repair conclusions attached to accepted recovery requests must be queryable through the audit/experience surface

## Forbidden

- local runtime silently resuming or relaunching work without kernel acceptance
- treating host conversation resume, long-term wake-up, or global scheduling as part of this repo-local recovery surface
- hardcoding absolute paths, source-workspace virtualenv assumptions, or repo-specific environment hacks into recovery contracts
