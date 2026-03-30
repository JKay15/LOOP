# LOOP Evaluator Prototype Contract

Purpose: define the richer legacy evaluator compatibility surface and the shared evaluator request/report semantics used inside the standalone product repo.

Current positioning:
- `loop-evaluator` and `loop_product.run_evaluator(...)` are the formal thin evaluator product entry aliases.
- `loop_product.evaluator.simple` is the implementation module for that same thin evaluator body.
- `loop_product.evaluator.prototype` remains the richer compatibility/supervision surface for legacy requests and self-eval flows.

This legacy compatibility surface still answers one narrow engineering question:
- can we launch checker + evaluation AI roles from code,
- feed them custom prompts built from the product usage manual and final-effect requirements,
- preserve an auditable evidence chain,
- and produce a reviewer-fed final feedback object that can be checked by a human afterward.

The checker stage is explicitly an auto-repair compiler, not a strict early gatekeeper.

For the thin evaluator body and any richer compatibility path that resumes inside the same evaluator run:
- persist a durable `EvaluatorRunState.json`
- surface retryable same-run interruptions as `RECOVERY_REQUIRED` instead of terminal `ERROR`
- freeze the checker-produced lane plan after checker normalization succeeds
- allow checker-normalized requirements to mark evaluator-owned closure obligations as `runtime_closure`
- keep `runtime_closure` requirements out of delegated reviewer/test_ai/ai_user lanes because those requirements are satisfied only by the evaluator run itself reaching terminal completion
- preserve that frozen checker graph across recovery
- skip already-completed lanes during same-run resume
- run reviewer only after every lane is terminal

For delegated evaluator AI roles:
- prefer the committed argv-based role-launch runtime over ad hoc `bash -c "codex exec ..."` reconstruction
- keep role guidance on committed prompts/runtime metadata rather than transient runtime `AGENTS.md` / `AGENTS.override.md` files
- keep that committed role guidance generic: prefer bounded non-interactive probes, clean up auxiliary browser/server/helper processes, and stop once decisive evidence exists for the current evaluation unit
- if a delegated role has already produced a non-empty authoritative terminal response artifact and a committed provider terminal marker has also been observed on the configured launch streams, the runtime may settle the role from that artifact instead of waiting indefinitely for provider-side exit cleanup
- response-file stability alone is not a success signal; post-response non-zero exits without that terminal marker must remain terminal `ERROR`
- if no authoritative terminal response artifact exists, the runtime must still fail closed rather than inventing success from transport chatter alone
- materialize a runtime-owned `CODEX_HOME` for delegated evaluator roles that shares host auth/config surfaces while isolating mutable state DBs, sessions, and logs from the host default `~/.codex`
- exclude runtime-owned/generated source directories such as `.loop/`, `.loop_runtime/`, `workspace/`, and `.uv-cache/` when copying staged evaluator role workspaces from the source workspace
- generic evaluator submissions should default delegated evaluator reasoning to `high` unless the caller explicitly overrides it

## Prototype entrypoints

- Python:
  - `run_evaluator_prototype(...)`
  - `run_evaluator_prototype_supervised(...)`
  - `build_evaluator_prototype_self_eval_input(...)`
  - `build_checker_prompt(...)`
  - `build_test_designer_prompt(...)`
  - `build_ai_user_prompt(...)`
  - `build_reviewer_prompt(...)`
- CLI:
  - `python -m loop_product.evaluator.prototype --input <request.json>`
  - `python -m loop_product.evaluator.prototype --input <request.json> --mode supervised`
  - `python -m loop_product.evaluator.prototype --input <request.json> --mode raw`

The raw evaluator engine and the caller-facing supervised entrypoint are intentionally separate:
- `run_evaluator_prototype(...)` is the raw evaluator engine.
- `run_evaluator_prototype_supervised(...)` is the caller-facing wrapper that must supervise until a terminal result or a confirmed bug/stuck condition.
- the supervised wrapper must not weaken the raw evaluator semantics, reasoning effort, or role time budgets just to finish faster.
- Codex-host-specific host behavior is frozen separately at the root-instruction/manual layer:
  - `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_CONTRACT.md`
  - `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_PRODUCT_MANUAL.md`
  - root `AGENTS.md`
  Any repo that reuses this product can load those host instructions, and when they are loaded, a conversation-facing Codex caller must not stop mid-run unless a terminal result or confirmed bug/stuck condition has been established.

## Evaluator self-eval packaging

The back-half evaluator is also expected to participate in the same `product manual + final effects -> evaluator feedback -> iterate` loop used for other products.

Packaged helper:
- `build_evaluator_prototype_self_eval_input(...)`

Helper responsibilities:
- when the caller does not pass an explicit outer override, derive bounded session-local live self-eval final-effects surfaces from the canonical evaluator final-effects doc instead of pointing live self-eval directly at the full canonical doc
- materialize a session-local evaluator product manual that embeds the concrete fixture request path chosen for that self-eval run
- make that session-local manual expose a preferred staged-workspace-safe direct-interpreter supervised command for the current provisioned Python interpreter, while keeping the equivalent `uv` command visible for auditability
- materialize a session-local generated fixture request inside `workspace_root/.cache/loop_evaluator_self_eval/`
- materialize a second session-local generated fixture request dedicated to reviewer fail-closed verification
- materialize a third session-local generated fixture request dedicated to reviewer missing-evidence fail-closed verification
- materialize a session-local generated bug-path fixture request dedicated to confirmed `BUG` supervision verification
- materialize a session-local generated stuck-path fixture request dedicated to confirmed `STUCK` supervision verification
- stamp packaged live self-eval requests with `ai_user_scheduler.max_parallel_runs = 1` so nested Codex-backed AI-as-User runs stay within stable capacity without weakening evaluator semantics
- materialize a blocking runtime assertion that points at the dedicated malformed-reviewer fixture's terminal `EvaluationReport.json`
- materialize a blocking runtime assertion that points at the dedicated missing-evidence reviewer fixture's terminal `EvaluationReport.json`
- materialize blocking runtime assertions that point at the dedicated bug/stuck fixtures' `SupervisorReport.json` outputs
- pin that runtime assertion to the dedicated malformed-reviewer fixture's exact `evaluation_id` and `run_id`, so stale reports from another session cannot satisfy the proof
- pin the dedicated missing-evidence runtime assertion to that fixture's exact `evaluation_id` and `run_id`, so stale reports from another session cannot satisfy the proof
- pin the dedicated bug/stuck runtime assertions to those fixtures' exact `evaluation_id` and `run_id`, so stale supervisor reports from another session cannot satisfy the proof
- rewrite that generated fixture request so its `workspace_root` and `output_root` are runnable from a staged workspace copy
- when the packaged positive-path fixture would otherwise inherit the canonical evaluator final-effects doc, rewrite it to a bounded session-local positive-fixture final-effects surface so direct positive-path validation stays affordable while still exercising checker-backed normalization and terminal PASS behavior
- materialize generated fixture `product_manual_ref` / `final_effects_text_ref` files into session-local staged-safe support paths under `.cache/loop_evaluator_self_eval/inputs/**` unless those refs already point at repo-shipped files under that exact support subtree
- materialize a local malformed-reviewer support agent under `workspace_root/.cache/loop_evaluator_self_eval/` for the negative-path fixture
- materialize a local missing-evidence reviewer support agent under `workspace_root/.cache/loop_evaluator_self_eval/` for the dedicated missing-evidence fixture
- materialize local support agents under `workspace_root/.cache/loop_evaluator_self_eval/` for the dedicated bug/stuck fixtures
- when a host adapter root is supplied, materialize a session-local adapter snapshot under `.cache/loop_evaluator_self_eval/inputs/**` so architecture proofs can verify that LeanAtlas remains only an adapter/integration layer without hardcoding host-owned absolute paths into the product manual
- preserve reviewer-visible repo-local evidence for shared-repo architecture checks, and when a generated self-eval request is present that evidence must still machine-verify that its `product_manual_ref` / `final_effects_text_ref` both resolve to repo-shipped `.cache/loop_evaluator_self_eval/inputs/**` files without prescribing one mandatory helper script
- rewrite generated self-eval `agent_cmd` entries so paths under `.cache/loop_evaluator_self_eval/` stay runnable even when reviewer executes from `run_root` instead of the product workspace
- preserve the caller-facing rule that supervised usage must not report completion before a terminal evaluator result exists, unless a confirmed bug/stuck condition is established
- when the current request is itself a packaged self-eval fixture rooted under `.cache/loop_evaluator_self_eval/fixture_output/**`, descendants of that run must not recursively launch another packaged self-eval fixture through the evaluator entrypoint, whether raw or supervised, and the runtime must reject that recursion fail-closed before launch

Runtime assertions may target:
- a nested `EvaluationReport.json` via `kind = evaluation_report`
- a nested `SupervisorReport.json` via `kind = supervisor_report`

Canonical human-facing source manual:
- `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md`

## External input

The generic prototype input is a JSON object validated by:
- `docs/schemas/LoopEvaluatorPrototypeInput.schema.json`

Minimum required fields:
- product usage manual path
- workspace root
- output root
- one of:
  - `final_effects_text_ref` (preferred, checker-backed path)
  - `final_effect_requirements` (legacy pre-normalized compatibility path)
- role requirements for:
  - checker AI when `final_effects_text_ref` is used
  - ordinary-test agent
  - AI-as-User
  - reviewer

When `final_effects_text_ref` is present, the checker AI becomes the source of truth for normalized requirement records.
When the raw goal text is repairable, the checker must auto-repair it and continue the run instead of stopping early.

The packaged helper `build_endpoint_clarification_part1_prototype_input(...)` still freezes the Part 1 endpoint-clarification effect set for later use, and can optionally accept `final_effects_text_ref` for checker-backed runs.
The formal packaged alias `build_endpoint_clarification_part1_input(...)` resolves to that same helper body so product runtime and docs can stop depending on prototype-only naming directly.
The front-half implementation lives under `loop_product.endpoint.clarification`.

## Frozen role order

The preferred checker-backed prototype runs in this order:
1. checker AI
2. ordinary-test agent
3. AI-as-User evaluation-unit runs
4. reviewer

The ordinary-test agent must finish its own ordinary testing and persist reviewer-visible evidence before the reviewer runs.

Each AI-as-User run must test exactly one evaluation unit per run.

The checker-backed path must preserve a `RequirementGraph = {requirements, evaluation_units, dependency_edges}` artifact.

AI-as-User runs may execute in parallel only when the runtime provides isolated evaluation-unit scratch dirs so user-created artifacts do not collide and dependency edges allow that batch to run.
AI-as-User runs must also use staged per-unit workspaces so user probes execute from a dedicated lane `cwd`, while the canonical AI-as-User outputs still land under `run_root/.loop/ai_user/runs/`.
When a repo-shipped Python role agent is launched from one of those staged role workspaces, the runtime must preserve repo importability from that `cwd`, for example by prepending the product repo root to `PYTHONPATH`, rather than falling back to `CODEX_HOME` overrides.
Nested evaluator role launches should avoid login-shell startup files for ordinary execution because host-local shell profile noise is not evaluator evidence.
Those staged role workspaces should exclude runtime-owned/generated source directories such as `.loop/`, `.loop_runtime/`, `workspace/`, and `.uv-cache/` by default so delegated roles inspect the product source rather than stale sibling outputs.
Retryable provider-side failures must stay explicit: `provider_transport`, `provider_capacity`, and startup-level `provider_runtime` are evaluator-owned issue kinds and should receive bounded lane retry before the evaluator settles on terminal `ERROR`.
Non-retryable upstream quota exhaustion must surface explicitly as `provider_quota` instead of being collapsed into `path` or implementation failure.

## Prompt construction rules

### Checker AI
- prompt input:
  - final-effects text
  - checker role requirement
- mission:
  - validate that the final-effects text lists one final effect per listed item
  - auto-repair locally obvious malformed or merged items into one-effect-per-item requirements
  - normalize accepted items into stable requirement records
  - compile or preserve a `RequirementGraph` so later stages know which requirements must be tested together
- hard rule:
  - stop only at a human gate when repair would require semantic guessing about the user's intent

Checker output contract:
- `status = OK | HUMAN_GATE`
- if `status = OK`:
  - `normalized_requirements = [{requirement_id, description, blocking, requirement_kind}]`
  - `requirement_graph = {requirements, evaluation_units, dependency_edges}`
  - `requirement_graph.requirements` must reuse the full `normalized_requirements` objects, not bare requirement ids
  - `requirement_graph.evaluation_units = [{unit_id, requirement_ids}]`; the field name must be exactly `unit_id`, not `evaluation_unit_id`
  - `requirement_graph.evaluation_units` should group co-testable requirements into as few evaluation units as the evidence surface supports; singleton units are only justified when separate user-facing probes or sequencing are actually needed
  - `requirement_graph.dependency_edges = [{from_unit_id, to_unit_id}]` and those ids must reference `evaluation_units[*].unit_id`
  - always include `requirement_kind`; use `product_effect` for ordinary reviewer-visible product checks and `runtime_closure` only for evaluator-owned closure obligations such as authoritative terminal-result/result-sink completion
  - minimal valid example:
    ```json
    {
      "requirements": [
        {"requirement_id": "REQ-001", "description": "...", "blocking": true, "requirement_kind": "product_effect"}
      ],
      "evaluation_units": [
        {"unit_id": "EU-001", "requirement_ids": ["REQ-001"]}
      ],
      "dependency_edges": []
    }
    ```
  - `repair_actions = [{raw_item, action, emitted_requirement_ids}]`
  - `emitted_requirement_ids` may be empty when the ledger item records corroborating or accepted evidence that introduces no new normalized requirement ids
  - `notes = [..]`
- if `status = HUMAN_GATE`:
  - `human_gate_reasons = [..]` and it must be non-empty
- `notes = [..]`

### Ordinary-test agent
- prompt input:
  - product usage manual
  - normalized final-effect requirements
  - ordinary-test role requirement
  - reviewer-visible ordinary-test artifact root under `run_root/ordinary_tests/`
- mission:
  - You own the ordinary-testing loop end-to-end.
  - generate ordinary tests such as unit tests and pressure/stress tests
  - execute those ordinary tests directly instead of delegating execution back to the evaluator runtime
  - stay inside the documented product surface
  - persist reviewer-visible logs and execution records under `run_root/ordinary_tests/`
- hard rule:
  - Do not invent undocumented features.
  - Prefer a small number of focused exploratory probes before finalizing the ordinary tests.
  - when a requirement depends on reliable enter-vs-bypass routing, ordinary tests should prefer at least one clearly objective bypass-style prompt and one clearly fuzzy prompt instead of relying on a subjective guess that some other prompt should bypass.
  - when a requirement claims preserved behavior, isolation, or stability, ordinary tests should include at least one focused repeated or isolated probe from the documented surface so reviewer-visible current-run evidence can support non-regression.
  - ordinary tests should prefer direct documented commands and short single-purpose assertions over generating large helper scripts; helper files should only be created when the evidence genuinely requires them.
  - ordinary tests should prefer documented product commands, repo-local metadata, and current-run visible artifacts over repo-internal validation scripts or fixture helpers.
  - repo-internal validation scripts under `tests/` and packaged self-eval fixture helpers under `.cache/loop_evaluator_self_eval/**` are not ordinary-test probes unless the product manual explicitly documents them as part of the product surface.
  - Prefer staged-safe documented entrypoints and stable workspace-relative commands over brittle interpreter guesses.
  - If a path, interpreter, import, or harness problem is evaluator-side, record it as evaluator-side instead of blaming the product.
  - if a helper file, quoted command, or generated snippet created by the delegated AI fails to parse, import, or execute, that is self-caused until repaired or discarded.
  - if ordinary tests accidentally trigger one of those repo-internal checks and it fails, that failure stays evaluator-side or self-caused unless the documented product surface explicitly required that check.
  - ordinary tests must not keep opening near-duplicate probes once one decisive current-run probe already covers the same requirement family; retries are only for repairing a specific self-caused mistake or resolving objective ambiguity.
  - That diagnosis is AI-owned; the runtime should not replace it with fixed helper-script verdicts or narrow hardcoded failure classifiers.
  - Do not write a test that depends on later evaluator stages having already finished.
  - `ordinary_test_results.executed_tests[*].test_id` must be unique so ordinary-test evidence refs cannot overwrite each other.
  - When the current request is itself a packaged self-eval fixture, ordinary tests must not recursively launch another packaged self-eval fixture through the evaluator entrypoint, whether raw or supervised.

Ordinary-test output contract:
- `status = OK`
- `ordinary_test_results = {all_passed, executed_tests = [{test_id, test_kind, argv, passed, exit_code, stdout_ref, stderr_ref, exec_span_ref}]}`
- `notes = [..]`

### AI-as-User
- prompt input:
  - product usage manual
  - all normalized final-effect requirements
  - `RequirementGraph`
  - one target evaluation unit under test
  - AI-as-User role requirement
- mission:
  - use the product as a real user would
  - test exactly one evaluation unit per run
  - preserve a replayable operation log
- hard rule:
  - Do not claim success unless the recorded operations support it.
  - AI-as-User must explicitly distinguish product-side, evaluator-side, and self-caused probe failures before blaming the product.
  - if a probe fails, AI-as-User must decide whether the problem is product-side, evaluator-side, or your own probe mistake.
  - staged workspace copy may omit heavyweight trees such as `.venv`, `.git`, `.lake`, and unrelated caches; missing staged-only environment affordances are not product failures by default.
  - AI-as-User should prefer direct documented commands and short single-purpose assertions over generating large helper scripts; helper files should only be created when the evidence genuinely requires them.
  - if AI-as-User chooses a brittle, irrelevant, or self-caused probe, it must repair it and continue instead of blaming the product.
  - if a helper file, quoted command, or generated snippet created by AI-as-User fails to parse, import, or execute, that is self-caused until repaired or discarded.
  - AI-as-User must not keep opening near-duplicate probes once one decisive current-run probe already covers the same requirement family; retries are only for repairing a specific self-caused mistake or resolving objective ambiguity.
  - AI-as-User must not turn a subjective guess that a prompt was already specific enough into product FAIL evidence unless that prompt objectively matches documented bypass cues or another visible current-run probe already shows the same prompt class bypassing.
  - when packaging or reuse is under test, AI-as-User should prefer staged-safe local evidence such as repo-local metadata, declared entry points, runnable module help, and offline/local build steps before any network-dependent packaging probe.
  - if a packaging probe needs network access or a missing build backend/toolchain in the staged workspace, that blocker stays evaluator-side; AI-as-User must salvage the local packaging evidence it has and only leave the effect uncertain if product-side packaging still is not evidenced.
  - for non-regression requirements, AI-as-User should prefer current-run ordinary evidence plus small repeated or isolated documented probes over abstract guesses about prior history.
  - AI-as-User should prefer current-run visible artifacts plus documented user-facing commands over repo-internal validation scripts or fixture helpers.
  - repo-internal validation scripts under `tests/` and packaged self-eval fixture helpers under `.cache/loop_evaluator_self_eval/**` are not product probes unless the product manual for that unit explicitly documents them as part of the product surface.
  - if AI-as-User accidentally triggers one of those repo-internal checks and it fails, that failure stays evaluator-side or self-caused unless the documented product surface explicitly required that check.
  - `effect_results[*].outcome` is reviewer-facing free text and only needs to be a non-empty string.
  - evidence refs may be absolute or relative to the emitting artifact directory / run root / workspace root.
  - evidence refs may include file fragments or line suffixes, but only when the underlying base file exists.
  - if an absolute evidence ref drifts only in its higher-level prefix but still carries a suffix that matches the current run root, the runtime should relocate it onto the current run instead of failing a real, existing artifact on a cosmetic path mismatch.
  - operation-log step refs may use `op5`, `operation 5`, or the same anchor plus a short descriptive suffix such as `op5: inspected the terminal report`; the runtime must normalize those back to the canonical operation-log anchor.
  - AI-as-User must receive staged prompt/context launch files inside its staged workspace, but the runtime-owned `result.json`, `response.md`, and `operation_log.md` paths stay under `run_root/.loop/ai_user/runs/`.
  - When the current request is itself a packaged self-eval fixture, AI-as-User must not recursively launch another packaged self-eval fixture through the evaluator entrypoint, whether raw or supervised.

### Reviewer
- prompt input:
  - product usage manual
  - final-effect requirements
  - `RequirementGraph`
  - prompt-sized ordinary-test evidence summary plus refs to raw logs
  - prompt-sized AI-as-User evidence summary plus refs to raw logs and raw result payloads
  - reviewer role requirement
- mission:
  - judge whether the ordinary tests exposed issues
  - judge whether ordinary tests themselves may be criticized by the reviewer
  - judge whether AI-as-User actually reached the final effects
- hard rule:
  - Every PASS or FAIL verdict for a final effect MUST include reproduction_steps.
  - Reviewer artifact writes must stay inside `run_root`; reviewer must not use the product workspace as its execution root.
  - reviewer must distinguish evaluator-side failures from product failures across both ordinary-test and AI-as-User evidence.
  - ordinary-test or AI-as-User interpreter/import/path/harness failures must not be treated as direct product FAIL evidence.
  - if a delegated AI's self-generated helper file, quoted command, or probe snippet fails to parse, import, or execute, reviewer must keep that signal evaluator-side/self-caused unless the documented product surface explicitly required that helper.
  - if an ordinary test or AI-as-User probe depends on repo-internal validation scripts or fixture helpers that the product manual does not document, reviewer must keep any resulting failure evaluator-side/self-caused unless the documented product surface explicitly required that probe.
  - if the record already contains a clear BYPASS probe and a clear VISION_COMPILER probe, reviewer must not turn a delegated AI's subjective `this other prompt should have bypassed` guess into product FAIL evidence without objective support from the documented surface or a matching current-run probe.
  - if repo-local packaging metadata, declared entry points, and runnable module surfaces are present, and the only failed wheel/build probes were blocked by staged-environment network or missing build backend/toolchain limits, reviewer must keep that negative signal evaluator-side and use PASS or UNCERTAIN based on the remaining visible evidence, but not FAIL.
  - for non-regression requirements, reviewer must weigh current-run ordinary evidence of preserved behavior, artifact behavior, isolation, and repeated stability before leaving the effect uncertain.
  - The required JSON contract is intentionally minimal: reviewer must return per-requirement `effect_reviews`; all other reviewer commentary is optional opaque passthrough.
  - The evaluator runtime, not the reviewer, computes the top-level report status from reviewer per-effect verdicts plus requirement blocking flags.
  - The evaluator runtime validates evidence structure and scope, but it must not pre-classify reviewer/product blame with fixed helper scripts or hardcoded harness-only verdict logic.
  - The reviewer prompt must compact large evidence bundles instead of dumping raw argv blobs, role-run metadata, or unbounded evidence-ref lists into the prompt.

## Required evidence chain

Each role run must persist:
- prompt file
- context file
- response file
- result file
- invocation metadata
- command execution span

Each evaluator role must also keep a dedicated role-specific working directory. The per-run runtime artifacts live under `run_root/.loop/<role>/runs/`, but role guidance belongs on committed prompts/runtime metadata instead of transient runtime `AGENTS.md` / `AGENTS.override.md` files.

Nested evaluator role workspaces are runtime-owned isolation surfaces, not copies of the human operator's Codex setup. They must not rely on overriding the default Codex home just to select role-local rules, and they must not copy the host `config.toml` wholesale into a role workspace.
Nested evaluator role launches must also scrub parent `CODEX_*` session/transport env vars such as `CODEX_THREAD_ID` and `CODEX_INTERNAL_ORIGINATOR_OVERRIDE` before nested agent execution begins.

The ordinary-test agent must also persist its reviewer-visible execution evidence under `run_root/ordinary_tests/**`, and the evaluator runtime must preserve a ref to that structured test-designer result in `ordinary_test_results.generated_tests_ref` for backward-compatible report consumers.

Reviewer runtime isolation:
- reviewer must run with write access rooted at `run_root`
- reviewer may read product/manual/evidence refs, but must not receive product-workspace write access just to emit evaluator artifacts
- evaluator role env must expose both `LOOP_PRODUCT_EVAL_WORKSPACE_ROOT` (actual cwd / write root for the role) and `LOOP_PRODUCT_EVAL_SOURCE_WORKSPACE_ROOT` (the source product workspace), so self-eval child commands can still reach staged fixture assets without rebasing reviewer back onto the product workspace

The checker-backed path must also preserve:
- the original `final_effects_text_ref`
- the checker result that normalized it
- the checker `repair_actions` ledger when auto-repair was applied
- a compacted `RequirementGraph` when the checker emitted an obviously exploded singleton unit schedule that the runtime could merge locally without semantic guessing

Each AI-as-User evaluation-unit run must also persist:
- operation log
- one evaluation-unit scratch dir path used for user-created artifacts
- one staged per-unit workspace used as the agent cwd

AI-as-User runtime isolation:
- canonical evaluator artifacts under `run_root/.loop/ai_user/runs/ai_user__<unit_id>/` remain runtime-owned
- the AI-as-User subprocess runs from a staged per-unit workspace, but its canonical `result.json`, `response.md`, and `operation_log.md` are evaluator-owned files under `run_root/.loop/ai_user/runs/`
- losing canonical `prompt/context/invocation/stdout/stderr/exec_span` due to agent-side directory cleanup is a runtime isolation bug, not an acceptable artifact omission
- staged AI-as-User workspaces must exclude heavyweight repository/build trees such as `.git`, `.venv`, `.lake`, `artifacts`, neutral product-cache trees under `.cache/loop_product`, and stale self-eval output trees under `.cache/loop_evaluator_self_eval/{generated_tests,fixture_output}`
- staged AI-as-User workspaces must be materialized under a runtime-owned staging root outside the source workspace and outside unrelated host repository instruction chains, so nested Codex runs do not inherit ancestor maintainer instructions that are not part of the product surface
- when the source workspace contains session-local self-eval inputs under `.cache/loop_evaluator_self_eval/`, staged AI-as-User workspaces must also exclude sibling `.cache/*` temporary output trees created by prior ordinary-test or supervised self-eval runs
- staged AI-as-User workspaces must still preserve session-local self-eval inputs under `.cache/loop_evaluator_self_eval/` when those inputs are needed for the current run
- the evaluator runtime must materialize the same bounded current-run snapshot under both `run_root/.loop_evaluator_internal/current_run_snapshot/` and staged AI-as-User workspaces, including `order.log`, `input_snapshot.json`, checker/test-designer artifacts, prior AI-as-User artifacts already produced for the current run, and ordinary-test artifacts, so ordinary tests and later AI-as-User units can rely on one canonical stage-visible evidence surface without re-exposing heavyweight output trees
- when AI-as-User units run in parallel, the canonical `run_root/.loop_evaluator_internal/current_run_snapshot/` refresh must happen once per serial batch, not once per unit, so parallel units do not race by deleting and rebuilding the same shared snapshot tree
- when `checker` or `test_designer` executes in a staged workspace, its `context.json` must advertise the staged `workspace_root`, not the source repo root
- staged `checker` and staged `test_designer` context file refs such as `final_effects_text_ref` and `product_manual_ref` must resolve inside the staged workspace or a staged runtime-owned inputs area, not back into the source workspace
- staged `test_designer` must receive its staged ordinary-test artifact root in `ordinary_test_artifact_root`; the canonical copyback root under `run_root/ordinary_tests` is runtime-owned and must not replace the staged execution root inside the role context
- the staged AI-as-User control surface under `.loop_evaluator_internal/<unit_id>/control/` is part of the visible current-run evidence for the active unit's launch from code

The final report must preserve:
- ordinary test execution results
- checker-normalized final-effect requirements
- requirement graph
- AI-as-User effect outcomes, grouped by evaluation unit
- reviewer verdicts

When every blocking requirement is evaluator-owned `runtime_closure`, the prototype may legitimately finish without delegated product-effect lanes and without a reviewer payload. In that case terminal evaluator completion itself satisfies those requirements, the durable run state may record `checker.status = SKIPPED` and `reviewer.status = SKIPPED`, and the terminal report must say so in notes instead of fabricating reviewer evidence.

If the run reaches a terminal failure, including preflight request/manual validation failures before checker dispatch, the prototype must still write `EvaluationReport.json` with:
- `status = ERROR`
- `error = {stage, kind, message, evidence_refs?, human_gate_reasons?}`
- `error.stage = preflight` for failures that happen before checker/test/user/reviewer dispatch begins
- `error.kind = HUMAN_GATE` when checker ambiguity is the stop reason
- any partial checker/test/user evidence already produced before the failure

If the evaluator reruns the same `evaluation_id/run_id`, it must clear evaluator-owned stale artifacts from that `run_root` before the new run begins, while preserving any active supervisor request/log files that the new run still owns. Old `EvaluationReport.json`, `SupervisorReport.json`, `roles/**`, `ordinary_tests/**`, and similar evaluator-owned leftovers must not bleed into the next run.

## Fidelity rule

The prototype is fail-closed on reviewer fidelity for final-effect claims, but not on reviewer free-form commentary labels.

If the reviewer says a final effect passed or failed, the report is acceptable only when:
- the verdict cites evidence refs, and
- the verdict includes `reproduction_steps` that a human can follow afterward.

Reviewer fields outside that minimum, such as top-level summary, ordinary-test review, top-level evidence refs, or extra per-effect commentary keys, are optional opaque passthrough. The prototype must not reject a run just because those fields are omitted, reshaped, or use reviewer-chosen labels.
`UNCERTAIN` is an accepted reviewer effect-verdict alias for `INCONCLUSIVE`; the runtime must preserve the reviewer wording while aggregating it as inconclusive.

The earlier `ordinary_test_review.verdict = NON_BLOCKING_HARNESS_FAILURE` failure mode was a runtime/schema validation bug in the prototype, not a reviewer-prompt violation.

If the required per-effect evidence fields are missing, the prototype must reject the run instead of quietly accepting the reviewer output.

Before a reviewer-backed product `FAIL` is accepted, the evaluator must first rule out high-confidence evaluator-side failures in its own ordinary-test/reviewer/harness stack. Obvious evaluator-side faults should be auto-repaired when safe; if they are not repaired, they must surface as evaluator/reviewer failure or inconclusive evidence instead of being blamed on the product implementation.

## Ordinary-test criticism

The reviewer is allowed to say:
- the implementation failed a test, or
- the ordinary tests themselves are weak, misdesigned, or insufficient

That criticism must also remain evidence-backed.
If the only negative evidence comes from evaluator-side ordinary-test/interpreter/import/harness breakage, the reviewer must criticize the harness or mark the effect inconclusive rather than converting that evaluator failure into a product `FAIL`.

## Output

The prototype output is a JSON report validated by:
- `docs/schemas/LoopEvaluatorPrototypeReport.schema.json`

Its role is to summarize:
- checker normalization and repair_actions when used
- ordinary test generation and execution
- requirement graph and AI-as-User usage evidence, one evaluation unit at a time
- reviewer verdicts, with optional reviewer commentary when present
- final status derived by evaluator runtime: `PASS | FAIL | INCONCLUSIVE | ERROR`

## Caller-facing supervision

The back-half now has a caller-facing supervision layer in addition to the raw evaluator engine.

Supervisor contract:
- the caller-facing wrapper must supervise until a terminal result is available
- terminal means a valid `EvaluationReport.json` whose `status` is one of `PASS | FAIL | INCONCLUSIVE | ERROR`
- while the evaluator is still running normally, the wrapper must not treat mid-run progress as a completed result
- only a confirmed bug/stuck condition may end supervision without a terminal evaluator report
- those non-terminal endings must be preserved in a deterministic supervisor report validated by:
  - `docs/schemas/LoopEvaluatorPrototypeSupervisorReport.schema.json`

The supervised wrapper therefore changes caller behavior, not evaluator semantics:
- raw checker / ordinary-test / AI-as-User / reviewer behavior remains unchanged
- the supervision layer only decides whether the caller has seen a terminal evaluator result yet
