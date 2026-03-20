---
name: child-dispatch
description: Materialize frozen kernel delegation into child-local execution without letting the root kernel collapse back into child execution.
---

## Use when
- The kernel has already frozen a delegation and execution policy.
- A child implementer, evaluator, or reviewer lane needs to be launched.
- Phase 1 clarification has already persisted the current endpoint artifact through the committed clarification wrappers.

## Responsibilities
- Materialize frozen delegation and execution policy into child-local execution context.
- Prefer the committed first-child bootstrap helper (`loop_product.runtime.bootstrap_first_implementer_node(...)`) instead of hand-assembling workspace roots, frozen handoff files, child prompts, and launch glue in the root chat.
- Prefer the committed repo wrapper scripts directly instead of probing `--help`, using bare `python -m`, rediscovering helper internals by repository search, or doing schema inspection during kernel-mode execution.
- For ordinary `loop_product_repo` first-child bootstrap, the repo-local kernel/node/child-dispatch surfaces are sufficient; do not detour through generic `loop-mainline`, LeanAtlas mainline docs, or extra root-level workflow guides before calling the wrapper.
- For implementer children, let the committed endpoint bootstrap wrapper choose or create the authoritative project folder under `workspace/`, then pin both the node `workspace_root` and the launch `cwd` / `-C` to that same folder before launch.
- Do not pre-create a guessed project folder for an ordinary fresh task before the committed endpoint bootstrap wrapper returns the authoritative `workspace_root`.
- Reuse a workspace/project folder only when the current task already has authoritative state pointing at that exact folder.
- must not treat a similarly named old workspace or terminal child as the current task's child.
- Unless the current conversation or authoritative state already names an exact workspace folder or node ref, treat the task as fresh and create a new project folder immediately.
- must not scan old workspace folders or old node reports to guess reuse.
- If the frozen final effect includes an external publish target outside the project folder, freeze both the workspace mirror artifact path and the external publish target path.
- In that case, the child owns the workspace mirror artifact and evaluator loop, while the root-kernel publication step owns the external publish target.
- The committed bootstrap surface should freeze the authoritative kernel-visible implementer result sink, task-scoped evaluator submission/manual/final-effects refs, and may also expose a separate workspace-local mirror sink; do not ask the child to invent ad hoc result paths or evaluator surfaces when the committed layout is already known.
- Do not inspect existing external-target files, local asset files, or preflight external publish-target writability during first-child bootstrap; publication-environment checks belong to the later root publication step after a terminal evaluator-backed result.
- That root-kernel publication step is a mechanical copy/publication action after a terminal evaluator-backed result; it must not silently upgrade a blocked or bug-owned evaluator verdict into PASS.
- Use the committed external publication helper `scripts/publish_external_from_result.sh` for that root-kernel publication step instead of ad hoc `cp`, `mv`, or Finder automation.
- Launch the child lane.
- Preserve the materialized child launch policy exactly: keep `-C <workspace_root>`, `cwd`, `sandbox_mode`, and bounded reasoning aligned with the node spec.
- Use the committed child launch helper `scripts/launch_child_from_result.sh` with the exact bootstrap/recovery result ref instead of translating `launch_spec` into a fresh ad hoc shell command in chat; the committed helper already owns bounded startup-health observation plus retry for retryable early provider transport/capacity failures, and it should reuse an already-live exact same-node child instead of starting a duplicate launch.
- Keep the implementer workspace and frozen sink layout explicit: durable implementation files normally live in the materialized workspace, while endpoint-required repo-local or user-local input discovery may inspect exact external paths when the frozen handoff does not already materialize the needed file.
- When the child still needs endpoint-required repo-local or user-local file discovery, point it at the committed filename/path helper `scripts/find_local_input_candidates.sh` instead of letting it guess with ad hoc content grep.
- If the frozen endpoint requires local/offline user assets, keep sourcing local-first: search canonical local roots such as Desktop, Music, and Downloads before inventing narrower ad hoc roots, and do not browse/download remote substitutes unless the frozen handoff explicitly allows remote sourcing.
- A bare `codex exec - < CHILD_PROMPT.md` is not a valid child launch because it can silently fall back to host defaults instead of the node policy.
- If the available host path cannot preserve the node's launch policy exactly, report blocked instead of launching a degraded child.
- Capture heartbeat, local result, and terminal envelopes.
- Keep the root kernel supervising the child instead of silently becoming the child.
- For a live child lane, use a status-only, non-interrupting heartbeat ping rather than a free-form strategy nudge.
- The heartbeat ping should request a compact status packet with: `phase`, `evaluator_state`, `request_ref`, `evaluation_report_ref`, `same_attempt`, `stalled`, and `next_action`.
- Once real evaluator lineage exists, do not ask `whether evaluator has started` as a loose yes/no; ask for `evaluator_state` plus refs in the status packet.
- If the host supports interrupt flags, ordinary heartbeat follow-ups should stay on the non-interrupting path.
- Only the current implementer may propose that the task should split; root/kernel may approve or reject that proposal, but must not invent extra child nodes as if split were already accepted fact.
- Product closeout must say whether split was proposed, accepted, rejected, or never requested, and whether any accepted split changed the outcome.

## Not responsible for
- Declaring topology changes accepted.
- Mutating the authoritative graph directly.

## Minimal kernel pattern
1. Use the clarified endpoint artifact plus the external publish target as the normal bootstrap inputs.
2. Call the committed endpoint-driven first-child bootstrap wrapper directly:

```bash
scripts/bootstrap_first_implementer_from_endpoint.sh --endpoint-artifact-ref <artifact_json> --external-publish-target <target_path>
```

3. Launch the returned child with the committed child launch helper:

```bash
scripts/launch_child_from_result.sh --result-ref <bootstrap_result_json>
```

4. Before same-node orphaned-active recovery, query the committed child runtime status helper:

```bash
scripts/check_child_runtime_status.sh --result-ref <child_launch_result_json>
```

5. If that status helper reports `recovery_eligible=true`, call the committed recovery wrapper directly:

```bash
scripts/recover_orphaned_active.sh --request <request_json>
```

6. If recovery is accepted and returns a canonical launch spec, relaunch through the same committed child launch helper:

```bash
scripts/launch_child_from_result.sh --result-ref <recovery_result_json>
```

7. After a terminal evaluator-backed implementer result that is `READY_FOR_PUBLICATION`, perform external publication through the committed helper:

```bash
scripts/publish_external_from_result.sh --result-ref <implementer_result_json>
```
