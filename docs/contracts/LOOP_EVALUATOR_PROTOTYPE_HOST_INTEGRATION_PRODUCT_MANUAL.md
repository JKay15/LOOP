# Evaluator Prototype Codex Host Integration Product Manual

## Purpose

This manual describes the Codex-host adapter behavior for evaluator supervision inside this repository's instruction environment.

The product here is not the standalone evaluator runtime by itself. The product is:

- a conversation-facing Codex app agent
- operating on a real evaluator run
- that keeps supervising until the run reaches a terminal evaluator result, confirmed `BUG`, or confirmed `STUCK`

## User-facing expectation

The user should be able to ask the Codex app agent to run or continue an evaluator session in ordinary conversation.

The user should not receive:

- a completed answer while the evaluator is still running
- a fake success that only reflects partial progress
- a product-owned `FAIL` when the blocker is still evaluator-side

## Internal host action

The host should prefer this formal thin-evaluator entrypoint internally:

```bash
loop-evaluator --input <fixture_request.json>
```

Under the hood, that thin evaluator implementation lives at `loop_product.evaluator.simple`.

The richer legacy supervised evaluator surface remains available via:

```bash
python -m loop_product.evaluator.prototype --input <fixture_request.json> --mode supervised
```

The host-side rule is the same for any of these entrypoints: keep supervising until a terminal evaluator result, confirmed `BUG`, or confirmed `STUCK` exists.

## Session rule

- If evaluator supervision is active for the current task, later ordinary conversation turns continue against that same supervising task rather than pretending it already completed.
- Progress updates may summarize what happened so far, but they must stay explicitly non-terminal.
- If the evaluator establishes `BUG` or `STUCK`, the host may report that terminal status instead of waiting for a normal evaluator result.

## Relationship to the core evaluator

- The evaluator runtime contract/manual still live at:
  - `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_CONTRACT.md`
  - `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md`
- Host-specific non-early-exit supervision belongs to this host integration manual/contract, not to the standalone evaluator runtime alone.
- This host rule stays at the root-instruction/manual layer. It is not an extra workflow skill pack under `.agents/skills/`.
