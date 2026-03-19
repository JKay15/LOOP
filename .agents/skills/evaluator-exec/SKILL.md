---
name: evaluator-exec
description: Let evaluator-side AI own the high-variance execution path instead of delegating it to brittle fixed scripts.
---

## Use when
- Checker, ordinary-test, AI-as-User, or reviewer execution depends on paths, environment, command order, or repo structure.
- You are already using the documented evaluator product surface from `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md` and need help carrying out its high-variance execution path.

## Responsibilities
- Stay anchored to the documented evaluator product surface in `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md`.
- Use `initialize_evaluator_runtime(...)` from `loop_product.runtime` when the caller still needs a trusted `.loop` evaluator runtime bootstrap.
- Prefer the repo-local adapter path such as `run_evaluator_node_until_terminal(...)` when it already covers the implementer use case.
- For ordinary implementer tasks, expect the committed bootstrap to hand you a task-scoped evaluator submission/manual/final-effects surface instead of a product-specific hardcoded template.
- Evaluator-side AI roles should launch through the committed evaluator role-launch runtime rather than ad hoc `bash -c "codex exec ..."` glue.
- Do not author task-local evaluator role-agent scripts or inject ad hoc `.cache/**`, `/tmp/**`, Desktop, or other temporary `agent_cmd` paths into evaluator requests.
- If explicit role commands are required, they must be repo-shipped evaluator helpers already committed in this repo, not task-specific scripts.
- Handle `uv`, path, repo structure, dependency, and command-sequence issues.
- Distinguish evaluator-side mistakes from implementation problems.
- Keep AI-as-User actually using the product surface under test.
- Preserve upward-facing evaluator refs such as `request_ref` and `evaluation_report_ref` so the implementer can justify completion to the kernel.
- If an evaluator in flight already exists, stay attached to the same evaluator attempt and preserve its lineage instead of treating a short `wait_agent` timeout as a reason to switch paths.
- Treat a short `wait_agent` timeout as only a heartbeat window; ordinary waiting after evaluator launch should allow a `wait_agent` timeout of at least 600000 ms (10 minutes).
- Do not replace an in-flight real evaluator with a simpler or lower-fidelity fallback just because the current root poll window expired.
- If evaluator execution must resume inside the same evaluator run, preserve `EvaluatorRunState.json` and the frozen checker graph instead of casually restarting checker and replaying finished lanes.
- If the evaluator returns a retryable `RECOVERY_REQUIRED` outcome, treat that as unfinished same-run recovery work rather than a terminal evaluator conclusion.
- Prefer the committed wrapper `scripts/run_evaluator_node_until_terminal.sh` when the caller needs the same evaluator run to continue until terminal.

## Rules
- Do not replace the documented evaluator product surface with ad hoc execution just because the path is high variance.
- Do not split "test design" and "high-variance execution" into AI plus rigid script glue.
- Fixed scripts may help with low-variance tasks only.

## Not responsible for
- Replacing the product's own runtime or acceptance logic.
- Smuggling root hard rules into evaluator-side skill scope.
