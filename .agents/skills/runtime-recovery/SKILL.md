---
name: runtime-recovery
description: Detect stalls or runtime failures and produce resume / retry / relaunch proposals for the kernel.
---

## Use when
- A node or evaluator lane is blocked, stuck, or failed.
- A node still looks `ACTIVE` in authoritative state, but the backing runtime/session/transport has clearly detached and needs orphaned-ACTIVE recovery.

## Responsibilities
- Detect liveness problems.
- Prepare retry / resume / relaunch proposals.
- Check recovery consistency before proposing action.
- Before same-node orphaned-active recovery, query the committed child runtime status helper (`scripts/check_child_runtime_status.sh`) so a quiet child with a still-live PID stays in supervision instead of escalating too early.
- Prefer the committed orphaned-active recovery helper (`loop_product.runtime.recover_orphaned_active_node(...)`) for same-node orphaned-ACTIVE retry instead of hand-assembling retry envelopes, hand-writing recovery request JSON files, or inventing relaunch launch specs in chat.
- Prefer passing direct structured flags to `scripts/recover_orphaned_active.sh` over reading request schemas in chat for ordinary same-node recovery.
- If accepted same-node recovery returns a canonical launch spec, relaunch through `scripts/launch_child_from_result.sh` instead of translating that launch spec into a fresh ad hoc shell command in chat; the committed launch helper already owns bounded startup-health observation plus retry for retryable early provider transport/capacity failures, scrubs parent `CODEX_*` session/transport env vars, disables OTEL exporters for the nested child launch, and should reuse an already-live exact same-node child rather than starting a duplicate launch.
- When recovery stays inside the same evaluator run, preserve `EvaluatorRunState.json` and the frozen checker graph so retryable lane recovery can resume without redoing completed lanes.
- Treat retryable evaluator-owned `RECOVERY_REQUIRED` outcomes as unfinished recovery work, not as node/task terminal closure.

## Not responsible for
- Taking global recovery ownership away from the kernel.
