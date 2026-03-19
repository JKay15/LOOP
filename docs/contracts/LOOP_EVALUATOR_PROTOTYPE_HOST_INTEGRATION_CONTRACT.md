# LOOP Evaluator Prototype Codex Host Integration Contract

This contract freezes the repo-controlled host-specific adapter behavior for using the evaluator product from a conversation-facing Codex app thread.

## Purpose

The evaluator core runtime is only the product-side execution surface. The stronger user-facing effect lives at the host layer:

- a conversation-facing Codex app agent that starts an evaluator run must keep supervising that run
- the user should not receive a completed answer while the evaluator is still running normally
- the only allowed early-exit outcomes are a terminal evaluator result, confirmed `BUG`, or confirmed `STUCK`

This is a host-specific adapter contract for this repository's instruction environment, not a guarantee for uncontrolled external hosts.

## Required instruction surfaces

The repository must expose all of the following:

- root `AGENTS.md` supervision text that points evaluator work at `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_PRODUCT_MANUAL.md`
- `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_CONTRACT.md`
- `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md`
- `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_CONTRACT.md`
- `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_PRODUCT_MANUAL.md`

## Required host behavior

Within this repository's Codex host environment:

1. When a conversation-facing agent starts evaluator supervision, it must not stop mid-run or report partial progress as completion.
2. Mid-run progress updates are allowed, but they are not terminal completion.
3. The only allowed early-exit reasons are a terminal evaluator result, confirmed `BUG`, or confirmed `STUCK`.
4. If the evaluator-side harness or probe fails before a product verdict is established, that remains evaluator-owned rather than becoming a product-owned `FAIL`.

## Boundary

- The evaluator runtime itself remains frozen by `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_CONTRACT.md` and `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md`.
- Host-specific non-early-exit supervision lives in this host-specific contract/manual layer, not in the standalone evaluator runtime contract.
- This host rule lives at the root-instruction/manual layer, not as an extra workflow skill pack under `.agents/skills/`.
- This contract intentionally does not guarantee behavior for uncontrolled external hosts that ignore this repository's instructions.
