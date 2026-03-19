# ExecPlan: Evaluator Provider-Capacity Empty-Envelope Fix

## Context

The evaluator checker lane can fail after `codex exec` loses its upstream sampling stream and writes an empty `response.raw.json`. The current runtime raises `produced an empty structured-output envelope before result.json` before it consults the provider-issue classifier, so a provider-capacity or provider-transport outage is misreported as a checker-output bug.

## Terms

- `raw_response_path`: the structured-output envelope file written by `codex exec -o ...` when the role uses the codex CLI structured-output path.
- `provider issue`: an upstream failure such as `provider_capacity`, `provider_transport`, or `provider_quota`, classified from stderr/stdout text.
- `empty-envelope misclassification`: the bug where an empty `response.raw.json` masks a provider-side failure and turns it into a checker bug.

## Files

- `loop_product/evaluator/prototype.py`
- `tests/check_loop_system_repo_evaluator_node.py`

## Change

1. Add a helper in `loop_product/evaluator/prototype.py` that classifies pre-`result.json` structured-output failures.
2. When `response.raw.json` exists but is empty, prefer provider-side classification if stderr/stdout already match `provider_capacity` or `provider_transport`.
3. Preserve the old empty-envelope error only when no provider issue is detectable.

## Tests First

Add a regression in `tests/check_loop_system_repo_evaluator_node.py` that simulates:

- non-terminal role failure before `result.json`
- empty `response.raw.json`
- stderr containing `We're currently experiencing high demand`

Acceptance:

- the helper returns a `provider_capacity`-style failure message
- a plain empty envelope without provider hints still returns the old empty-envelope message

## Verification

- `uv run --project . python tests/check_loop_system_repo_evaluator_node.py`
- `uv run --project . python tests/check_loop_system_repo_handoff_ready.py` if the focused test passes and the repo runner surface is still script-based

## Notes

- The repository path referenced by the maintainer skill, `docs/agents/PLANS.md`, is absent in this checkout, so this ExecPlan is self-contained instead of referencing a central plan index.
