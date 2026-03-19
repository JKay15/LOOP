# Single-Child Lessons and Split/DAG Upgrade

## Single-Child Lessons
- `clarification` works better when the conversation-facing agent owns it directly and only persists the result, instead of delegating ordinary clarification to a nested agent.
- A fresh task should be `fresh by default`; reuse must come only from an exact already-known node/workspace, not from name similarity or old artifact scanning.
- `kernel mode` is much more stable when it uses a committed bootstrap helper instead of hand-writing folders, handoff prose, prompts, and launch commands in chat.
- The implementer should own one project folder under `workspace/` and treat the `workspace mirror artifact` as its primary deliverable surface.
- External publication should stay root-owned. The child should build and evaluate the workspace mirror, and root should publish only after a `terminal evaluator-backed result`.
- Evaluator recovery needs its own durable ledger. `EvaluatorRunState.json` should stay the source of truth for same-run resume so completed lanes are not rerun.
- Committed helpers should normalize durable results when the data is already present. Root publication should not depend on the implementer hand-authoring duplicated projection fields.
- Root and child both became more stable once recovery was treated as ordinary system work instead of an exceptional manual chat procedure.

## Keep from the Old Split Design
- Keep `implementer proposes, kernel decides`.
- Keep split as a proposal first; a child must not directly mutate topology fact.
- Keep kernel-side normalization and hard checks for target ids, generation, and structural budget.
- Keep child materialization in `child-dispatch`; split-review should form and validate proposals, not apply them.

## Split/DAG Upgrade
- Preserve the existing immediate fan-out path as `parallel`.
- Add a second split mode called `deferred`.
- `parallel` means the source node blocks now and accepted child nodes activate now.
- `deferred` means the source node shrinks its current scope, while the split-out work is recorded for future activation instead of being started immediately.
- A split proposal should therefore carry more than child ids and goal slices. It should also carry:
  - `completed_work`
  - `remaining_work`
  - `split_mode`
  - `depends_on`
  - `activation_condition`
- `depends_on` identifies which node completions or merge points must happen before a deferred child can start.
- `activation_condition` records the kernel-checkable trigger for turning a deferred child into an active materialized node.
- The kernel should continue to own acceptance and materialization, but it should no longer assume that every accepted child becomes active immediately.

## Practical Rule
- Do not make the implementer directly manage a DAG.
- Let the implementer request restructuring when its work becomes too broad or too interdependent.
- Let kernel-owned review decide whether the request becomes a `parallel` split, a `deferred` split, or a rejection.
