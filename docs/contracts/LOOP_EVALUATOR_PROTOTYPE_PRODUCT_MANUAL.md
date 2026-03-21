# Product Manual

This document records the current evaluator product positioning for the standalone LOOP product repo.

## Purpose

The formal thin evaluator product entry aliases are:

- `loop-evaluator --input <fixture_request.json>`
- `from loop_product import run_evaluator`
- `from loop_product.loop import run_evaluator_node_until_terminal`

Use that surface when you want a caller-facing evaluator that:
- accepts a prepared evaluator request JSON,
- launches the raw evaluator role chain,
- and preserves checker/test-AI/AI-as-User/reviewer evidence with minimal glue.

When the caller is already on the evaluator-node adapter path, prefer `run_evaluator_node_until_terminal(...)` or the repo-shipped wrapper `scripts/run_evaluator_node_until_terminal.sh` so retryable same-run recovery stays inside the committed runtime instead of becoming ad hoc rerun glue in the implementer.

The implementation module for that same thin evaluator body is now:

- `loop_product.evaluator.simple`

The legacy richer compatibility surface is still:

- `loop_product.evaluator.prototype`
- `python -m loop_product.evaluator.prototype --input <fixture_request.json> --mode supervised`

That legacy path remains available for compatibility and richer self-eval/supervision flows, but the shared product repo now treats the thin simple evaluator as the formal minimal back-half product surface.

## Product surface

Primary thin-evaluator CLI:

```bash
loop-evaluator --input <fixture_request.json>
```

Primary thin-evaluator package alias:

```python
from loop_product import run_evaluator
```

Implementation layout:

- front-half implementation: `loop_product.endpoint.clarification`
- thin evaluator implementation: `loop_product.evaluator.simple`

Legacy compatibility entrypoint:

```bash
python -m loop_product.evaluator.prototype --input <fixture_request.json> --mode supervised
```

Supporting entrypoint:

```bash
python -m loop_product.evaluator.prototype --input <fixture_request.json> --mode raw
```

## Implementer quick path

For ordinary implementer subagents inside this repo, the preferred route is not to start from a hand-written raw `fixture_request.json`.
The committed first-child bootstrap should already have materialized a task-scoped evaluator submission, plus task-scoped manual/final-effects refs, for that frozen implementer task.

Prefer the repo-local adapter path in `loop_product.loop.evaluator_client`:

- `initialize_evaluator_runtime(...)` from `loop_product.runtime`
- `EvaluatorNodeSubmission`
- `materialize_evaluator_request(...)`
- `run_evaluator_node(...)`

That quick path has two layers. `initialize_evaluator_runtime(...)` prepares the trusted `.loop/...` runtime state for an ordinary implementer caller without exposing kernel authority directly. The adapter path then materializes the evaluator request JSON for you and calls the documented evaluator product surface. This is the normal route when a LOOP implementer already has the frozen final effect, implementation location, and role requirements in hand.

Use the raw `loop-evaluator --input <fixture_request.json>` surface when you genuinely need the generic caller-facing entrypoint. Do not treat the raw surface as proof that evaluator usage is inherently heavy if `initialize_evaluator_runtime(...)` plus `run_evaluator_node(...)` already covers the implementer case.

Explicit evaluator `agent_cmd` overrides are fail-closed. An implementer must not author a task-local evaluator role-agent script and inject it through `.cache/**`, `/tmp/**`, Desktop files, or any other ad hoc path. If explicit role commands are needed at all, they must be repo-shipped evaluator helpers that are already committed inside this repo, such as `python -m loop_product.loop.smoke_role_agent ...` for the bounded smoke path. Otherwise stay on provider/profile execution and let the documented evaluator product surface own the role chain.

## Required input

The product expects a fixture request JSON that already satisfies the evaluator input schema.

The fixture request JSON is the inner evaluation job that the evaluator product must supervise.

## Expected caller behavior

- For caller-facing usage of the legacy compatibility surface, prefer `--mode supervised`.
- In supervised mode, do not report completion before a terminal evaluator result exists.
- A retryable same-run `RECOVERY_REQUIRED` report is not a terminal evaluator result.
- A terminal evaluator result means a valid `EvaluationReport.json` whose `status` is one of `PASS`, `FAIL`, `INCONCLUSIVE`, or `ERROR`.
- If the child evaluator is still running normally, progress updates are allowed, but they are not a completed result.
- Early return is allowed only after a confirmed bug or stuck condition.
- The evaluator must diagnose its own ordinary-test/reviewer/harness failures before blaming the external implementation. Unresolved evaluator-side failures must stay evaluator-owned (`ERROR` or inconclusive), not product-owned `FAIL`s.

Within a run, the ordinary-test agent now owns the ordinary-testing loop end-to-end. It must design its ordinary tests, execute them itself, and save reviewer-visible logs plus execution records under `run_root/ordinary_tests/`. The evaluator runtime should validate and preserve that evidence for reviewer consumption, but it should not take back ordinary-test execution from the test AI.

Every evaluator AI role also gets one dedicated role-specific working directory. The evaluator calling code must select that role by `cwd` / `-C` instead of overriding `CODEX_HOME` or `HOME`. Per-run artifacts live under `run_root/.loop/<role>/runs/`, role guidance stays on committed prompts/runtime metadata instead of transient runtime `AGENTS.md` / `AGENTS.override.md` files, nested role launches must keep host auth/config on the default Codex home, and they must scrub parent `CODEX_*` session/transport env vars such as `CODEX_THREAD_ID` and `CODEX_INTERNAL_ORIGINATOR_OVERRIDE`. If the nested role command is a repo-shipped Python module such as `python -m loop_product...`, the launch must still preserve repo importability from that dedicated `cwd`, for example by prepending the product repo root to `PYTHONPATH`.
Those nested role launches should prefer the committed argv-based role-launch runtime instead of rebuilding ad hoc `bash -c "codex exec ..."` strings in the evaluator loop.
Those nested role launches should use ordinary non-login shell execution so host-local shell profile noise does not contaminate evaluator evidence.
Those delegated evaluator roles should prefer bounded non-interactive probes over headed or long-lived interactive sessions, should clean up any auxiliary browser/server/helper processes before returning, and should stop once decisive evidence exists for the current evaluation unit instead of continuing open-ended exploration.
Unless the caller explicitly overrides it, the generic evaluator-node adapter should default delegated evaluator reasoning effort to `high`.
If a delegated role fails with retryable provider-side transport, startup-runtime, or temporary capacity problems, the thin evaluator should retry that lane with bounded backoff before surfacing `RECOVERY_REQUIRED`. Reports should distinguish `provider_transport`, `provider_runtime`, `provider_capacity`, and `provider_quota` instead of collapsing them into path/config mistakes. `provider_quota` may require waiting for the reset window instead of immediate bounded retry, but it still remains evaluator-owned recovery work rather than terminal implementation failure. When those bounded local retries exhaust, the persisted lane/report error must still preserve the retryable provider issue kind instead of degrading into an untyped terminal `RUNTIME_ERROR` just because the leading stderr lines were local CLI noise.
If `implementation_package_ref` points at a directory artifact, the evaluator-node adapter treats that directory as the exact shipped deliverable. Before evaluator launch it must canonicalize the directory by pruning runtime-owned heavy trees such as `.lake`, `.git`, `.venv`, `.uv-cache`, `build`, and `_lake_build`; if that canonicalization cannot produce a clean artifact, evaluator launch must fail closed instead of silently reviewing a dirty runtime snapshot.

For the thin simple evaluator surface, the same role-order and self-attribution rules still apply, but the runtime stays intentionally thinner:

- checker normalization remains structured
- AI-as-User must use the documented product surface first and only fall back to source/artifact inspection when an executable path would recurse into the evaluator or would otherwise be evaluator-side misuse
- delegated `test_designer` and `ai_user` outputs are forwarded to reviewer as raw text
- the product does not add a second supervisor layer around the thin surface itself

Within the same evaluator run, the thin evaluator must also keep a durable `EvaluatorRunState.json` under the run root. That file is the recovery ledger for the same evaluator run, not a replacement for `EvaluationReport.json`. Retryable evaluator-owned same-run interruptions must surface as `RECOVERY_REQUIRED` plus machine-readable recovery metadata instead of terminal `ERROR`. Once checker normalization succeeds, the evaluator must preserve the frozen checker graph, freeze the checker-produced lane plan, skip already-completed lanes on resume, and run reviewer only after every lane is terminal. Checker-normalized requirements may also mark evaluator-owned closure obligations as `runtime_closure`; those closure requirements must not become delegated reviewer/test_ai/ai_user lanes because they are satisfied only by the evaluator itself reaching terminal completion.

Session-local manuals and prompts should summarize known evaluator-side failure patterns so the ordinary-test agent can self-correct. In particular, staged workspaces may omit a local `.venv`; prefer staged-workspace-safe direct-interpreter commands or other documented stable entrypoints, and if a path, interpreter, import, or harness problem is evaluator-side, record it as evaluator-side instead of blaming the product. That diagnosis must remain AI-owned rather than being hardcoded into fixed helper-script verdicts.

AI-as-User staged workspaces are runtime-owned dedicated copies, not product-owned repo directories. They are the AI-as-User lane `cwd`, they should stay isolated from unrelated host-repository instruction chains as much as the product runtime allows, and their canonical per-run artifacts still live under `run_root/.loop/ai_user/runs/`.
When those staged role workspaces are copied from the source workspace, runtime-owned/generated directories such as `.loop/`, `.loop_runtime/`, `workspace/`, and `.uv-cache/` should stay excluded by default so delegated evaluator roles do not inherit stale sibling outputs or old runtime ledgers. Nested heavyweight environment trees under copied deliverables, especially `.lake/`, `.git/`, `.venv/`, and `.uv-cache/`, should also stay excluded by default so reviewer-visible artifact files remain available without re-copying build caches or repo metadata into every evaluator lane workspace.

## Codex app host-specific expectation

This repo keeps the Codex app host-specific rule in:

- root `AGENTS.md`
- `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_CONTRACT.md`
- `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_PRODUCT_MANUAL.md`

Any repo that reuses this evaluator product can load the same host instructions. When they are loaded, the conversation-facing agent must not stop mid-run or treat partial progress as completion unless a terminal evaluator result or a confirmed bug or stuck condition has been established.

## Observable outputs

Successful thin-evaluator usage produces:
- an `EvaluationReport.json` written under the run root,
- reviewer-visible lane artifacts,
- and a terminal thin-evaluator report with either `COMPLETED`, `HUMAN_GATE`, or `ERROR`.

Successful caller-facing legacy supervised usage produces:
- a supervisor progress log,
- and either:
  - a terminal evaluator report reference, or
  - a deterministic supervisor report explaining the confirmed bug or stuck condition.

Raw mode produces the inner evaluator `EvaluationReport.json` path directly and does not add caller-facing supervision semantics by itself.

## Fixture-driven self-eval

When this product is itself being evaluated, the outer evaluator will provide a concrete fixture request JSON path inside the session-local manual bundle. The expected usage is still the same:

```bash
python -m loop_product.evaluator.prototype --input <fixture_request.json> --mode supervised
```

For self-eval specifically, the session-local manual may also provide a preferred staged-workspace-safe direct-interpreter command using the already provisioned Python interpreter for that run, for example `"${LOOP_PRODUCT_EVAL_SOURCE_WORKSPACE_ROOT:-.}"/.venv/bin/python3`. That command is equivalent evaluator behavior and is preferred when it avoids unrelated `uv` environment bootstrap noise while remaining runnable from staged workspaces that intentionally omit a local `.venv`.

For live self-eval sessions that recursively call Codex-backed roles, the packaged self-eval request should pin `ai_user_scheduler.max_parallel_runs = 1` so the outer evaluator preserves terminal supervision semantics without exhausting nested Codex capacity mid-run. This is a scheduling safeguard only; it must not weaken reasoning effort, role semantics, or fail-closed boundaries.

The session-local self-eval bundle may also provide a supporting negative-path fixture request dedicated to reviewer fail-closed verification. That fixture uses the same evaluator entrypoint, but its prepared inner job intentionally emits malformed reviewer claims so the evaluator product must reject them with a terminal failure instead of accepting them.

When that negative-path fixture is present, the self-eval bundle should also expose the exact nested `EvaluationReport.json` path that must appear for that fixture in the current session. A self-eval `PASS` is not justified unless that dedicated negative-path report exists, matches the dedicated fixture's current-session `run_id`, and shows the expected reviewer-stage fail-closed behavior.

The session-local self-eval bundle may also provide a second reviewer fail-closed fixture dedicated to missing evidence refs. When that fixture is present, a self-eval `PASS` is not justified unless the dedicated nested `EvaluationReport.json` shows a reviewer-stage terminal failure that explicitly rejects the malformed reviewer output for missing `evidence_refs`.

The session-local self-eval bundle may also provide a dedicated bug-path fixture request and a dedicated stuck-path fixture request. Those fixtures use the same caller-facing supervised entrypoint, but their prepared inner jobs intentionally force the supervisor to establish `BUG` and `STUCK` outcomes on purpose.

When those bug/stuck fixtures are present, the self-eval bundle should also expose the exact nested `SupervisorReport.json` path that must appear for each fixture in the current session. A self-eval `PASS` is not justified unless the dedicated bug-path report exists with `status: BUG`, the dedicated stuck-path report exists with `status: STUCK`, and both reports match the current session's exact `evaluation_id` and `run_id`.

The outer evaluator should fail the product if the caller exits before terminal completion while no confirmed bug or stuck condition exists.

The session-local fixture request used for self-eval is expected to live under `.cache/loop_evaluator_self_eval/` so it remains runnable inside a staged workspace copy.

If caller-side ordinary tests or supervision probes need temporary request/output directories during self-eval, prefer placing them under `.cache/loop_evaluator_self_eval/generated_tests/` instead of creating ad hoc sibling trees under `.cache/`.

The repo-shipped independence proof may verify a concrete generated self-eval bundle with:

```bash
python tests/check_loop_product_repo_independence.py --self-eval-request .cache/loop_evaluator_self_eval/fixture_request.generated.json
```

That `fixture_request.generated.json` must stay repo-local and must keep packaging refs such as `final_effects_text_ref` inside `.cache/loop_evaluator_self_eval/inputs/` so staged workspaces can validate the same bundle without host-specific absolute paths.

When the self-eval session is validating standalone shared-repo architecture claims, the session-local manual may summarize relevant repo-local evidence or optional example probes. It should not prescribe a mandatory helper script or a fixed helper-script verdict for those claims; the AI should choose only the probes that are actually needed for the requirement under test and keep reviewer-visible evidence for that choice.
