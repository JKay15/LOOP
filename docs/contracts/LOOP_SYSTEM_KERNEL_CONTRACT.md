# LOOP System Kernel Contract

This contract freezes `IF-1` for the standalone LOOP product repo.

## Purpose

The `kernel` is the root supervisory authority for the complete LOOP repo. It owns the authoritative state under `.loop/state/` and is the only surface allowed to turn a control proposal/report into accepted fact.

## Runtime anchors

- state snapshot schema: `docs/schemas/LoopSystemKernelState.schema.json`
- accepted control envelope schema: `docs/schemas/LoopSystemControlEnvelope.schema.json`
- authoritative state helpers: `loop_product/kernel/state.py`
- kernel submission boundary: `loop_product/kernel/submit.py`
- kernel topology review / application helpers: `loop_product/kernel/topology.py`
- public kernel query surface: `loop_product.kernel.query_authority_view(...)`
- public kernel audit surface: `loop_product.kernel.query_recent_audit_events(...)`
- public requirement-routing surface: `loop_product.kernel.route_user_requirements(...)`
- public trusted runtime bootstrap surface: `loop_product.runtime.initialize_evaluator_runtime(...)`
- public trusted first-child bootstrap surface: `loop_product.runtime.bootstrap_first_implementer_node(...)`
- public trusted endpoint-driven first-child bootstrap surface: `loop_product.runtime.bootstrap_first_implementer_from_endpoint(...)`
- public trusted orphaned-active recovery surface: `loop_product.runtime.recover_orphaned_active_node(...)`
- public trusted child launch surface: `loop_product.runtime.launch_child_from_result_ref(...)`
- public trusted child runtime status surface: `loop_product.runtime.child_runtime_status_from_launch_result_ref(...)`
- public trusted child progress snapshot surface: `loop_product.runtime.child_progress_snapshot_from_launch_result_ref(...)`
- public trusted child supervision surface: `loop_product.runtime.supervise_child_until_settled(...)`
- public trusted external publication surface: `loop_product.runtime.publish_external_from_implementer_result(...)`
- ordinary first-child bootstrap in this repo should rely on the repo-local kernel/node/child-dispatch surfaces rather than detouring through generic `loop-mainline` or LeanAtlas mainline workflow docs
- ordinary first-child bootstrap in this repo should consume the already-persisted endpoint artifact produced by the committed clarification wrappers instead of rereading clarification manuals in `kernel mode`

## Minimum surface

- read the current root task and active node graph
- submit `split`, `activate`, `merge`, `resume`, `retry`, `relaunch`, and `reap` proposals
- query node budget and status
- read recent audit events
- route new user requirements into the graph
- bootstrap a trusted `.loop/...` runtime for an ordinary implementer evaluator run without exposing raw kernel authority
- bootstrap the first implementer node for a clarified task through one committed helper call (preferably the endpoint-driven wrapper `scripts/bootstrap_first_implementer_from_endpoint.sh`) instead of probing `--help`, using bare `python -m`, repository rediscovery, request-schema inspection, pre-creating guessed workspace folders, or hand-assembling frozen handoff files, child prompts, and launch glue in the root chat
- the kernel may materialize additional child nodes only after an implementer-authored split proposal passes kernel review; one initial implementer child is the default entry path, not immediate root fanout
- first-child bootstrap should pre-materialize a task-scoped exact evaluator submission artifact plus an exact evaluator runner for the implementer node so the child can call the committed evaluator path directly instead of inferring wrapper arguments from docs in chat
- recover a detached but still-authoritatively-`ACTIVE` child through one committed helper call (preferably the wrapper `scripts/recover_orphaned_active.sh`) that accepts same-node orphaned-active retry from direct structured flags and rebuilds the canonical launch spec from durable node state
- launch or relaunch a materialized child through one committed helper call (preferably the wrapper `scripts/launch_child_from_result.sh`) using the exact bootstrap/recovery result ref instead of translating `launch_spec` into a fresh ad hoc shell command in the root chat; that committed helper should own bounded startup-health observation plus retry for retryable early provider transport/capacity failures, should scrub parent `CODEX_*` session/transport env vars before child startup, should disable OTEL exporters for the nested child launch, should preserve terminal-compatible Codex stdout/stderr semantics instead of attaching the child directly to regular-file stdio capture, and should reuse an already-live exact same-node child instead of starting a duplicate launch
- after launch, root-side supervision should prefer one committed helper call (preferably `scripts/supervise_child_from_launch.sh`) that keeps the same node under liveness/recovery supervision until it settles instead of relying on ad hoc polling loops in chat; when persisted launch or evaluator evidence classifies retryable provider/runtime churn, that supervision should apply bounded issue-aware cooldown before same-node recovery/relaunch rather than turning transient upstream pressure into a rapid attempt storm
- while that supervision remains active, root must not end the root conversation or treat the task as settled before the child actually settles; `scripts/child_progress_snapshot.sh` is an optional committed observation surface when root needs live state, evaluator state, or doc-usage evidence without ad hoc manual log reading
- product closeout for an auto-split-capable run must state whether split was proposed, accepted, and useful; if split was rejected or never requested, that must also be reported explicitly
- before same-node orphaned-active recovery, root supervision should query one committed child-runtime status helper (preferably `scripts/check_child_runtime_status.sh`) so a quiet child with a still-live PID stays in supervision rather than being recovered too early
- create the first implementer project folder under `workspace/` and pin that node's `workspace_root` plus launch `cwd` / `-C` to the same folder before launching it
- while an implementer is still in flight, root-kernel supervision is limited to liveness, status, and runtime-recovery decisions until a terminal evaluator-backed result exists
- for implementer nodes, a terminal evaluator-backed result requires more than `implementer_result.status=COMPLETED`; the durable result must include terminal evaluator evidence such as a verdict and a resolvable evaluator report ref before kernel may treat it as authoritative closeout
- once such an evaluator-backed terminal result exists, the committed closeout path must also normalize the authoritative node graph itself through kernel-owned terminal facts; leaving `kernel_state.json` or `state/<node>.json` at `ACTIVE` after terminal evaluator closure is a contract violation
- a manual or incomplete `implementer_result.json` that omits terminal evaluator evidence remains unfinished repair work and must not be normalized into root-side completion, publication readiness, or supervision shutdown
- retryable evaluator-owned `RECOVERY_REQUIRED` outcomes are not terminal evaluator-backed results for task closure or publication
- a retryable terminal evaluator `FAIL` is also unfinished repair work by default and must not be normalized into root-side completion just because an authoritative `implementer_result.json` exists
- when a frozen final effect includes an external publish target outside the implementer project folder, root-kernel publication may mechanically copy the exact workspace mirror artifact to that external target after any terminal evaluator-backed result; publication must not be treated as root acceptance and must preserve the evaluator verdict honestly
- that external publication step should prefer the committed helper `publish_external_from_implementer_result(...)` or the wrapper `scripts/publish_external_from_result.sh` instead of ad hoc `cp`, `mv`, or Finder automation in chat
- the committed external publication helper may deterministically normalize an authoritative `implementer_result.json` from already-present terminal evaluator evidence (for example nested evaluator refs/verdicts) before publication, instead of requiring the implementer to hand-author duplicate publication-ready projection fields
- first-child bootstrap should freeze a kernel-visible authoritative implementer result sink under `.loop/...` and may also freeze a separate workspace-local mirror sink instead of asking the implementer to improvise result locations in chat
- root-kernel bootstrap should freeze an external publish target path without inspecting existing external-target contents, local asset files, or preflighting writability during first-child bootstrap; publication-environment checks belong to the later root publication step
- when evaluator recovery remains inside the same evaluator run, kernel-side supervision must read `EvaluatorRunState.json` as the durable recovery ledger, preserve the frozen checker graph, and avoid silently restarting a fresh checker pass

## Current authoritative facts

- `kernel_state.json` is the durable root snapshot under `.loop/state/`
- `accepted_envelopes.json` is the accepted-fact sidecar under `.loop/state/`
- every runtime `state_root` passed to kernel/gateway/runtime helpers must resolve inside a `.loop/` boundary; roots outside `.loop/` must be rejected before any durable write happens
- accepted split proposals are not merely audit comments; once kernel accepts them they materialize durable child delegation/state artifacts and block the source node with an auditable reason
- split acceptance must be based on kernel-reviewed normalized child data; malformed child payloads, target-id drift, or generation drift must be rejected before acceptance rather than partially mutating `.loop`
- deferred split is a separate accepted mode: the source remains `ACTIVE`, accepted future children materialize as `PLANNED`, and kernel preserves their dependency/activation metadata for later activation
- accepted deferred activation promotes one `PLANNED` child to `ACTIVE` only after kernel review confirms its dependency set and activation condition are satisfied and the active-node budget still permits activation
- accepted merge proposals may reactivate either a blocked parallel source or a completed deferred source only when all declared child branches are merge-ready and kernel has accepted the convergence request
- accepted deferred merge must reactivate the completed source node truthfully by resetting its runtime attachment state instead of pretending the old completed runtime is still attached
- accepted reap proposals may retire a terminal non-root node from the live authoritative graph only after kernel has archived it under `.loop/quarantine/`
- accepted `resume` and `retry` proposals reactivate the same node only after kernel review passes
- accepted `relaunch` proposals materialize a fresh replacement child from durable delegation inputs while freezing the superseded source node as closed fact
- accepted implementer children must carry an explicit `workspace_root` under `workspace/<project>`; implementer launch policy is selected by that folder's `cwd` / `-C`, not by overriding the default Codex home
- authoritative node recovery must distinguish lifecycle state from runtime attachment so the kernel can recognize an orphaned ACTIVE node whose backing child runtime has disappeared
- orphaned ACTIVE nodes require explicit runtime attachment loss evidence before kernel may accept same-node `retry` or replacement-node `relaunch`
- same-node orphaned-active retry should prefer the committed `recover_orphaned_active_node(...)` surface over chat-local retry-envelope construction or chat-authored recovery request JSON files
- same-node orphaned-active retry should normally be gated by the committed `child_runtime_status_from_launch_result_ref(...)` surface rather than ad hoc log-age guesses in chat, and supervision should carry the exact confirmed launch/status refs into recovery when it already has them
- that committed child-runtime status surface must not downgrade a child to `pid_alive=false` solely because signal probing is permission-denied inside a restricted root session; it should confirm with a host-visible pid check before escalation
- the committed orphaned-active recovery helper itself must perform a fresh recheck against the latest committed child launch result before accepting retry, so kernel cannot accidentally recover a still-live exact same-node child from stale status impressions
- child launch after bootstrap/recovery should prefer the committed `launch_child_from_result_ref(...)` surface over chat-local `codex exec` reconstruction, and retryable early transport/capacity startup failures should normally be absorbed there by the helper's startup-health window before kernel escalates into a broader recovery path
- forcing host-side direct child launch mode is only meant to suppress recursive host-launch requests; it must not disable a safer terminal-compatible bridge/capture path for the actual Codex child process when that capture path is available
- child runtime supervision must be patient: a single no-live-pid observation with still-recent launch evidence should remain supervision work instead of immediately escalating into orphaned-active recovery
- once launch/runtime loss is confirmed and the latest persisted evidence still classifies the failure as retryable provider pressure, root-side supervision should cool down in bounded issue-aware steps before rematerializing another same-node recovery/relaunch cycle
- child progress snapshots should summarize live runtime state, latest evaluator state when present, recent log activity, and document refs observed from committed logs instead of relying on free-form prose claims that a child or evaluator “must have read” some doc
- evaluator-side checker/test_ai/ai_user/reviewer launches should prefer the committed evaluator role-launch runtime over ad hoc `bash -c "codex exec ..."` reconstruction
- evaluator recovery within the same evaluator run must preserve `EvaluatorRunState.json` plus the frozen checker graph so lane resume does not mutate the already-accepted evaluation decomposition mid-run
- accepted terminal node facts must keep `kernel_state.json` and the per-node snapshot under `.loop/state/<node>.json` in sync on terminal status and runtime attachment state rather than updating only the aggregate kernel graph
- `query_kernel_state(...)` must expose:
  - active node graph
  - lifecycle state
  - current effective requirement set
  - delegation map
  - active evaluator lanes
  - accepted control facts
  - blocked reasons
  - budget / complexity view

## Forbidden

- local nodes bypassing the kernel to mutate graph fact
- scripts or evaluator lanes writing `.loop/state/` directly as authority
- ordinary child callers directly invoking `kernel_internal_authority()` or `persist_kernel_state(...)` instead of using the exported trusted runtime bootstrap surface
- treating gateway normalization as acceptance; normalization is only pre-acceptance shaping
- letting `split-review` or `child-dispatch` directly mutate topology fact without a kernel acceptance decision
- letting local runtime helpers silently resume or relaunch work without a kernel acceptance decision
- root-kernel acceptance/testing/inspection that duplicates evaluator work before the implementer has produced a terminal evaluator-backed result
- rewriting a blocked or bug-owned evaluator outcome into an implied PASS merely because the root kernel published the exact workspace mirror artifact to the external target
