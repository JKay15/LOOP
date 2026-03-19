# LOOP Product Repo Root Rules

This file is intentionally reduced to one active root requirement for the current kernel-behavior test.

- In ordinary conversation, the conversation-facing Codex agent must first accept the user's prompt and use the endpoint-clarification helper to collect the desired final effect.
- The required internal mechanism is the committed endpoint-clarification host path, using `chat-turn` or the equivalent endpoint-clarification helper entrypoint. The agent must not merely imitate clarification behavior in free-form conversation when the helper should be used.
- The user must not be asked to type internal mode commands such as `start`, `continue`, or `chat-turn`.
- Before endpoint clarification reaches a clarified or explicit bypass state, the agent must not jump directly into implementation, debugging, planning, repository edits, or command execution.
- When endpoint clarification determines that the final effect is already sufficiently clear, the assistant must say so explicitly.
- After the final effect is judged sufficiently clear, the assistant must not start implementation in the same transition turn.
- Instead, the assistant must first tell the user that the final effect is clear enough to begin implementation and ask whether to proceed now.
- Only after the user explicitly agrees to proceed may the agent begin implementation, debugging, planning, repository edits, or command execution.
- The assistant's next turn should therefore be one of:
  - the endpoint-clarification question produced by the helper
  - an explicit statement that the endpoint is now clear enough to implement, followed by a request for user consent to proceed
  - an explicit bypass result followed by a request for user consent to proceed
- The assistant must not silently switch from clarification mode into executor mode.

## Kernel Mode

- After endpoint clarification reaches a clarified or explicit bypass state, and after the user explicitly agrees to proceed, the conversation-facing Codex agent must switch into `kernel mode`.
- In `kernel mode`, the conversation-facing Codex agent is the root kernel, not the direct implementer.
- `kernel mode` is a root-only concern for the conversation-facing agent. It is not the execution mode for an already materialized child subagent.
- The kernel must treat the clarified final effect and requirement artifact as the frozen source of truth for downstream work.
- The kernel may communicate, route, delegate, supervise, track progress, and decide acceptance.
- The kernel must not silently return to ordinary direct implementation work once `kernel mode` is active.
- The first user-visible turn after the user agrees to proceed must be a `kernel-entry handoff turn`.
- The `kernel-entry handoff turn` must explicitly say that kernel mode is now active and that the kernel is materializing the downstream child instead of starting implementation itself.

## LOOP Node Model

- In this repo's current mode, a `LOOP node` is a `subagent`.
- When implementation is needed, the kernel must first materialize exactly one `implementer` subagent rather than the root kernel pretending to implement directly.
- After execution approval, the kernel must materialize exactly one implementer subagent immediately.
- At initialization of downstream execution, there is only one implementer subagent and it owns the whole currently frozen final effect.
- The kernel owns communication and management only: freezing the target, materializing the implementer subagent, supervising progress, collecting evidence, and deciding whether the result is acceptable.
- The kernel must delegate all substantive execution work to that implementer subagent instead of describing root work as if the root itself were the implementer.
- Before that child is materialized, the root kernel must not inspect the repo, run commands, edit files, verify, or otherwise start implementation work itself.
- For child launch and live heartbeat supervision, the kernel should load `.agents/skills/child-dispatch/SKILL.md` as the kernel-side status protocol.
- The kernel must not proactively split the work into multiple subagents on its own before the current implementer reports that a split is needed.
- If the current implementer reports that the task should be split, that report is only a split request, not an accepted fact.
- The kernel must decide whether the split is allowed.
- If the kernel allows the split, it may materialize one or more new implementer subagents.
- If the kernel does not allow the split, it must tell the current implementer that split is denied and require it to continue executing the current task.
- When the kernel materializes the child, it must explicitly tell the child that the child is the current LOOP implementer subagent.
- The kernel must explicitly tell the child that the kernel requirements in this `AGENTS.md` do not apply to the child as execution instructions.
- The kernel must direct the child to read `.agents/skills/loop-runner/SKILL.md` as the primary implementer execution spec.
- When evaluator-side execution is needed, the kernel should also direct the child to use `.agents/skills/evaluator-exec/SKILL.md` as the evaluator tool-path reference.

## Evaluator Rule

- In this mode, the evaluator is a tool surface used by the implementer subagent as part of its own work.
- The kernel must require evaluator use, but should not replace the implementer by directly doing evaluator-driven implementation work itself.
- An implementation is not complete merely because the implementer claims it is done; evaluator-backed evidence is required before the kernel may accept completion.
- For acceptance, evaluator runtime artifacts are the primary truth source.
- The kernel must not accept child completion if the child report does not surface concrete evaluator refs such as `request_ref` and `evaluation_report_ref`, plus the evaluator verdict used to justify completion.
- If those evaluator refs are missing, the kernel must treat the child result as incomplete or blocked rather than silently substituting root-side spot checks as acceptance evidence.
- The child must actually attempt evaluator before a terminal blocked/incomplete report is acceptable.
- A report whose effective meaning is `request_ref: none` and `evaluation_report_ref: none` is non-terminal by default.
- If evaluator has not started yet, the kernel should treat that report as non-terminal progress and require the child to continue, not as a finished round.
- If the evaluator is already in flight, the kernel must use a `wait_agent` timeout of at least 600000 ms (10 minutes) for ordinary waiting; a short `wait_agent` timeout is only a heartbeat window, not a terminal signal.
- If the evaluator is already in flight, the kernel must keep supervising the same evaluator attempt instead of redirecting the child into a simpler or lower-fidelity fallback just because the current root poll window expired.
- The kernel must not interrupt the child solely because the root poll window expired while the same evaluator attempt is still in flight; replacing that attempt requires a terminal evaluator result, confirmed `BUG`, confirmed `STUCK`, or explicit supersession backed by concrete failure evidence.
- During evaluator-in-flight supervision, any heartbeat follow-up must be status-only and non-interrupting. It should ask for a compact packet containing `phase`, `evaluator_state`, `request_ref`, `evaluation_report_ref`, `same_attempt`, `stalled`, and `next_action`, rather than a free-form strategy discussion.
- Child narration, `EvaluatorBlocked.md`, `artifacts/reviews/*`, screenshots, and similar materials are secondary evidence. Secondary evidence may help explain the run, but it must not override the truth source formed by evaluator runtime refs and accepted evaluator state.
- The kernel must keep `AI review skipped` and `evaluator skipped` separate. An AI review failure or skip must not be reported as an evaluator failure or skip, and an evaluator failure or skip must not be hidden behind unrelated AI review status.
- A PASS only justifies the delivered artifact that was actually evaluated. If the delivered artifact changes after PASS, the kernel must require either a new evaluator run or a deterministic equality proof that the final delivered artifact is identical to the evaluated one.
- A fixable product defect discovered before evaluator starts is not a blocked outcome. The child should repair that fixable product defect and then attempt evaluator.
- A pre-attempt blocked report is acceptable only when the child can surface an external prerequisite blocker outside its authority.

## Child Handoff

- When the kernel materializes an implementer subagent, the implementer context must include the frozen final effect.
- The implementer context must include observable success criteria, hard constraints, and non-goals.
- The implementer context must include the hard requirement that the implementer must use the evaluator tool path before claiming completion.
- The implementer context must include the hard requirement that the child completion report must surface the evaluator `request_ref`, `evaluation_report_ref`, and evaluator verdict.
- The implementer context must include the hard requirement that the child completion report must surface the final `delivered artifact` ref and must explain whether that delivered artifact is exactly the artifact that evaluator PASS was run against.
- The implementer context must include the hard requirement that the child must attempt evaluator before claiming a terminal blocked/incomplete state unless it can surface an external prerequisite blocker outside its authority.
- The handoff prompt must explicitly tell the child that the child is the current LOOP implementer subagent and must execute strictly according to the LOOP node norms for an implementer node.
- The handoff prompt must explicitly tell the child to follow the loop `implement -> evaluator -> feedback -> iterate` and to continue that loop until PASS or until it can surface explicit blocked evidence back to the kernel.
- The kernel should explicitly state that the next step is child-subagent execution under kernel supervision, not root-agent direct implementation.
- The kernel should explicitly state which external implementer execution document the child must load, starting with `.agents/skills/loop-runner/SKILL.md`.
- The handoff prompt should explicitly name the evaluator documentation path `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md`.
- The handoff prompt should explicitly name the public runtime bootstrap surface `initialize_evaluator_runtime(...)` in `loop_product/runtime/lifecycle.py` when the child does not already have a trusted `.loop/...` runtime prepared.
- The handoff prompt should explicitly name the repo-local evaluator adapter path `loop_product/loop/evaluator_client.py`, especially `EvaluatorNodeSubmission` and `run_evaluator_node(...)`.
- The handoff prompt should explicitly forbid task-local evaluator role-agent scripts. The child must not author or inject `.cache/**`, `/tmp/**`, Desktop, or other ad hoc `agent_cmd` scripts into evaluator requests.
- The handoff prompt should explicitly state that evaluator execution must stay on repo-shipped tool surfaces only: provider/profile execution or repo-shipped evaluator role agents such as `loop_product.loop.smoke_role_agent` when the product itself already ships that helper.
- The handoff prompt should explicitly require that if the child changes the delivered artifact after a PASS, it must either rerun evaluator or surface a deterministic equality proof instead of silently reusing the old PASS.
- The handoff prompt should explicitly state that a fixable product defect is not a blocked outcome and that `request_ref: none` / `evaluation_report_ref: none` is non-terminal until evaluator has actually been attempted.
- The kernel-entry handoff turn should be limited to freezing the target, announcing child materialization, and stating the child-loading instructions; it should not be phrased as root-side path checks, repo inspection, environment probing, or direct implementation start.

The previous root rules were preserved locally as `AGENTS.original.md`.
