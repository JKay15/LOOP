# LOOP Product Repo Workspace Rules
## Implementer Mode
- A Codex agent running inside a project folder under `workspace/` is an implementer LOOP node, not the root kernel.
- The desired final effect for this node is already frozen by the kernel; do not restart clarification, do not re-scope the task, and do not ask the user for routine implementation choices.
- Use the current project folder as the node-local `workspace_root` and current working directory.
- Use the current project folder as the primary place for implementation files and workspace mirror artifacts; inspect repo-local or user-local paths directly when the frozen endpoint requires repo-local or user-local input discovery.
- do not write implementation files into sibling project folders under `workspace/`.
- Do not mine sibling `workspace/` task folders or ad hoc historical deliverables as implementation context unless the frozen handoff explicitly names an exact reuse target; otherwise prefer the current project folder plus repo-shipped code/docs.
- If the frozen handoff names an external publish target outside the current project folder, follow the handoff's exact artifact ownership and publish-path instructions instead of inventing a different publish layout.
- In that case, build the workspace mirror artifact inside the current project folder and treat it as the implementer-owned artifact.
- Do not write the external publish target directly unless the frozen handoff explicitly makes publication implementer-owned; the normal owner is `root-kernel`.
- If the frozen handoff exposes both a workspace-local mirror result sink and a separate kernel-visible sink, treat the kernel-visible sink as the authoritative report path and the workspace-local sink only as an optional mirror or debugging aid.
## Required Entry
- First read `.agents/skills/loop-runner/SKILL.md`.
- Then read `.agents/skills/evaluator-exec/SKILL.md` and `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md`.
- After those reads, execute the frozen task in the bounded implementer loop: implement, run evaluator, repair, and report the real evaluator result.
- Prefer the committed evaluator adapter path `run_evaluator_node_until_terminal(...)` or `scripts/run_evaluator_node_until_terminal.sh` over ad hoc evaluator rerun glue; if the bootstrap materializes a task-scoped exact evaluator runner/submission for this node, call that exact runner before reading source/tests or inferring wrapper args.
- If endpoint-required repo-local or user-local file discovery is needed, call the committed filename/path discovery helper directly with targeted `--root`/`--term`/`--ext` flags before reading its source/tests or doing ad hoc content grep; avoid one broad prose query.
- If the handoff requires local/offline user assets, search canonical local roots first (`Desktop`, `Music`, `Downloads`) and do not browse, download, or substitute remote assets unless the handoff explicitly allows remote sourcing.
- For non-trivial content transforms, prefer committed helpers or Python over brittle shell quoting or in-place one-liners on the primary deliverable; build updates through a temp path and atomic replace instead of clobbering the live output in place.
- If evaluator recovery continues inside the same evaluator run, preserve `EvaluatorRunState.json` and the frozen checker graph instead of restarting the evaluator from a new checker pass by default.
- A retryable evaluator-owned `RECOVERY_REQUIRED` result is not terminal node closeout; continue same-run recovery instead of reporting completion.
- Evaluator-side AI roles should run through the committed evaluator role launch path, not through ad hoc `bash -c "codex exec ..."` reconstruction inside the node loop.
- When same-node recovery or relaunch happens, do not open a second live implementer session for the same exact node/workspace if one is already running; rely on the committed launch helper to reuse the existing live child.
- If the backing runtime detaches before terminal closeout, leave enough durable state for the root kernel to use committed same-node orphaned-active retry instead of reconstructing launch inputs from chat.
- If the handoff includes an external publish target outside the current project folder, run evaluator against that workspace mirror artifact and report publish-ready artifact refs for the root kernel.
- In that case, still report publish-ready artifact refs even when the terminal evaluator result is blocked or bug-owned, so the root kernel can perform mechanical publication without redoing evaluator work.
- If the node stops on a nonterminal interruption before task completion, leave durable checkpoint artifacts and explicit recovery evidence in the project folder before stopping on a nonterminal interruption, including current artifact refs, self-attribution, and self-repair so the root kernel can choose retry or relaunch without reconstructing state from chat history.
- If the frozen handoff is missing a required input, report blocked instead of inventing a new scope.
