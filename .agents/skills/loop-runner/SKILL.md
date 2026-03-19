---
name: loop-runner
description: Run one bounded LOOP node round using the repo-local kernel, dispatch, local control, and evaluator surfaces.
---

## Use when
- You already have a goal slice for a single node.
- The node should implement, evaluate, interpret feedback, and produce a local control result.
- The kernel has handed you execution ownership and you need the implementer entrypoint for the full node round.

## Do not use when
- You need to change graph topology directly.
- You need to bypass evaluator feedback and hand-wave completion.

## Required reads
- Read the repo-local evaluator product manual at `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md` before choosing the evaluator entrypoint.
- If you do not already have a trusted `.loop/...` runtime prepared for this evaluator round, call `initialize_evaluator_runtime(...)` from `loop_product.runtime` instead of trying to bootstrap kernel state yourself.
- Prefer the repo-local implementer quick path built around `EvaluatorNodeSubmission` and `run_evaluator_node_until_terminal(...)` before assuming the raw evaluator surface is the only option.
- Do not author task-local evaluator role-agent scripts or inject ad hoc `.cache/**`, `/tmp/**`, Desktop, or other temporary `agent_cmd` paths into evaluator requests.
- If explicit evaluator role commands are needed at all, they must be repo-shipped evaluator tools already committed in this repo; otherwise stay on provider/profile execution.
- If the kernel also pointed you at `evaluator-exec`, treat that skill as the high-variance execution helper for the same documented evaluator product surface, not as a replacement for the manual.

## Responsibilities
- Consume the current node goal slice.
- Produce implementation progress and bounded implementation changes.
- Call the evaluator surface after implementation work for the current round, using the documented evaluator product surface from `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md`.
- Use `initialize_evaluator_runtime(...)` when you need a trusted evaluator runtime bootstrap; do not call `kernel_internal_authority()` or `persist_kernel_state(...)` directly from an ordinary child context.
- When this repo's adapter path is available, prefer `EvaluatorNodeSubmission` plus `run_evaluator_node_until_terminal(...)` instead of starting from a raw hand-written evaluator request or ad hoc rerun loop.
- Process the evaluator return.
- Surface the evaluator runtime refs upward in the child completion report, including at least `request_ref`, `evaluation_report_ref`, and the evaluator verdict used for the completion claim.
- Surface `delivered_artifact_ref` in the child completion report and say whether that delivered artifact is exactly the artifact that evaluator PASS was run against.
- If the delivered artifact changes after PASS, do not silently reuse the old PASS; rerun evaluator or surface a deterministic equality proof for the final delivered artifact.
- Keep evaluator status and AI review status separate in your report. `AI review status` must not be used as a substitute for evaluator outcome.
- Attempt evaluator before you present a terminal blocked or incomplete round result.
- A report whose effective meaning is `request_ref: none` and `evaluation_report_ref: none` is non-terminal by default.
- If you discover a fixable product defect before evaluator starts, treat that as more implementation work, not as a blocked outcome; repair the fixable product defect and continue until you can attempt evaluator.
- The only acceptable pre-attempt blocked case is an external prerequisite blocker outside your authority.
- If an evaluator in flight already exists, stay attached to the same evaluator attempt until a terminal evaluator result, confirmed `BUG`, confirmed `STUCK`, or explicit supersession backed by concrete failure evidence.
- Treat a short `wait_agent` timeout as only a heartbeat window; ordinary waiting after evaluator launch should allow a `wait_agent` timeout of at least 600000 ms (10 minutes), so temporary silence is not permission to abandon the in-flight evaluator.
- If an evaluator in flight already exists, do not open a simpler or lower-fidelity fallback or a second backup evaluator lane just because the current root poll window expired.
- If the kernel sends a heartbeat ping, reply with a compact status packet containing `phase`, `evaluator_state`, `request_ref`, `evaluation_report_ref`, `same_attempt`, `stalled`, and `next_action`, then continue the same attempt.
- before claiming completion, require a terminal evaluator result or an evaluator-owned blocked outcome.
- If you do not have those evaluator refs, report the round as incomplete or blocked instead of presenting local spot checks as completion evidence.
- Record roundbook entries and local control output.

## Not responsible for
- Making split / merge / resume become system fact.
- Replacing the kernel.
