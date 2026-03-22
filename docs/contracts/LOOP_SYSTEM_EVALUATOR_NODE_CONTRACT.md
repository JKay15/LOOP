# LOOP System Evaluator Node Contract

This contract freezes `IF-3` for evaluator-as-node behavior.

## Purpose

The evaluator is a graph participant, not an oracle outside the graph.

## Runtime anchors

- evaluator result schema: `docs/schemas/LoopSystemEvaluatorResult.schema.json`
- evaluator submission schema: `docs/schemas/LoopSystemEvaluatorSubmission.schema.json`
- accepted transport envelope: `docs/schemas/LoopSystemControlEnvelope.schema.json`
- evaluator result transport: `envelope_type="evaluator_result"`
- self-diagnostics field: `payload.diagnostics`
- node adapter runtime: `loop_product/loop/evaluator_client.py`

## Required evaluator lanes

- `checker`
- `ordinary-test`
- `AI-as-User`
- `reviewer`

## Required verdicts

- `PASS`
- `FAIL`
- `structured-exception`
- `BUG`
- `STUCK`
- `ERROR`

## Required behavior

- evaluator results return through the gateway
- AI-as-User must actually use the product
- evaluator lanes must check self-caused execution mistakes before blaming the implementation
- ordinary implementer evaluator submissions must be task-scoped to the frozen workspace mirror artifact and must not silently reuse endpoint-clarification manuals/final-effects from another product surface
- each evaluator AI role must run from a dedicated role-specific `cwd`
- per-run artifacts must live under `run_root/.loop/<role>/runs/`
- evaluator role guidance must come from committed prompt/runtime surfaces, not from transient runtime `AGENTS.md` / `AGENTS.override.md` files under `.loop/**`
- those committed role rules should keep delegated evaluator roles on bounded non-interactive probes by default instead of headed or long-lived interactive sessions
- delegated evaluator roles must not leave auxiliary browser/server/watcher/helper processes running once decisive evidence for the current evaluation unit already exists
- once decisive evidence exists for the current evaluation unit, delegated evaluator roles should emit terminal output instead of continuing open-ended exploration for nicer evidence
- when a role workspace is copied from the source workspace, runtime-owned/generated directories such as `.loop/`, `.loop_runtime/`, `workspace/`, and `.uv-cache/` must stay excluded by default instead of being copied into the delegated evaluator workspace
- evaluator role launches must not override the default Codex home merely to select role-local rules; auth/config remain on the default host home
- when a repo-shipped Python role agent is launched from that dedicated `cwd`, the launch must still preserve repo importability, for example by prepending the product repo root to `PYTHONPATH`
- evaluator role launches must scrub parent `CODEX_*` session/transport env vars such as `CODEX_THREAD_ID` and `CODEX_INTERNAL_ORIGINATOR_OVERRIDE` before nested agent execution begins
- evaluator role launches should prefer committed argv-based launch specs over ad hoc `bash -c "codex exec ..."` reconstruction so prompt transport, cwd, retry metadata, and stderr classification stay stable
- when a delegated evaluator role has already produced a non-empty authoritative terminal response artifact and a committed provider terminal marker has also been observed on the configured launch streams, the runtime may settle that role from the artifact instead of waiting indefinitely for provider-side exit cleanup
- response-file stability alone is not terminal success; post-response non-zero exits without that terminal marker must still translate to evaluator `ERROR`
- evaluator node runtime must still fail closed when no authoritative terminal response artifact exists; stderr/stdout chatter alone is not terminal success
- generic evaluator-node submissions should default every delegated evaluator AI role (`checker`, ordinary-test, `AI-as-User`, and `reviewer`) to `high` unless the caller explicitly overrides it
- retryable provider-side evaluator failures must stay distinct from path/config mistakes; `provider_transport`, `provider_capacity`, and startup-level `provider_runtime` failures are evaluator-owned diagnostics and should receive bounded lane retry before the evaluator settles on a recovery-required stop
- provider quota exhaustion must surface explicitly as `provider_quota`; it may require waiting for a reset window rather than immediate bounded lane retry, but it still remains evaluator-owned recovery work instead of generic path or implementation failure
- bounded lane retry must preserve retry history in the delegated role artifacts instead of silently discarding the first failed attempt
- same-run evaluator recovery must persist `EvaluatorRunState.json`
- same-run evaluator recovery must surface `status = RECOVERY_REQUIRED` plus machine-readable recovery metadata instead of collapsing retryable evaluator-owned interruptions into terminal `ERROR`
- evaluator recovery must freeze the checker-produced lane plan once checker normalization has completed for the same evaluator run
- checker-normalized requirements may classify evaluator-owned closure obligations as `runtime_closure` instead of ordinary product-effect lanes
- `runtime_closure` requirements must not become delegated reviewer/test_ai/ai_user lanes; they are satisfied only by this evaluator run itself reaching terminal completion
- if runtime-closure filtering leaves no delegated product-effect lanes, reviewer may be `SKIPPED` and evaluator-node translation must treat terminal completion as an honest `PASS`, not reviewer-absence `ERROR`
- evaluator recovery must skip already-completed lanes instead of replaying them by default
- reviewer only after every lane is terminal
- whole-paper benchmark submissions must fail closed before evaluator launch unless the delivered artifact includes structured terminal evidence in `WHOLE_PAPER_STATUS.json`; extraction/partition/intermediate ledgers alone are not evaluator-ready whole-paper closure
- split-child or slice-level evaluator submissions that declare required outputs must also fail closed before evaluator launch when any required outputs are still missing; evaluator entry must not trust a child-authored blocked status to excuse unrelated unsatisfied required outputs
- whole-paper evaluator surfaces are reserved for the root whole-paper node or a dedicated final integration / closeout node; ordinary split children must fail closed if they arrive with whole-paper final-effects text merely because their slice prose mentions `whole-paper` dependencies or ledger boundaries
- whole-paper final-integration submissions must also fail closed before evaluator launch when any declared `depends_on_node_ids` dependency has not yet reached a terminal-ready authoritative outcome
- child prompt and workspace rules must treat deferred split follow-up activation as separate topology work; evaluator launch must not be used as a substitute for activate submission
- evaluator node adapters must translate retryable `RECOVERY_REQUIRED` outcomes into `retryable=true` evaluator results instead of treating them as final evaluator closure
- evaluator node adapters must translate terminal simple-evaluator `ERROR` reports into evaluator verdict `ERROR`; reserve evaluator verdict `BUG` for adapter/runtime corruption, transport failure, or other unknown terminal states that cannot be honestly classified as plain evaluator error
- evaluator node adapters should expose a committed helper such as `run_evaluator_node_until_terminal(...)` so same-run recovery can continue to terminal without ad hoc rerun glue in the implementer
- `diagnostics.self_attribution` and `diagnostics.self_repair` preserve self-attribution / self-repair evidence
- evaluator-side self-caused execution mistakes must surface as `structured-exception` rather than being collapsed into implementation failure
- unknown reviewer terminals and other fail-closed terminal outcomes must not be silently folded into `PASS` / `FAIL`
- evaluator-side `BUG` / `STUCK` / `ERROR` must be distinguishable from implementation failures in accepted state
- LOOP nodes submit a structured evaluator submission that includes implementation/manual/effects refs plus node lineage
