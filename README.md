# LOOP

LOOP is a standalone router-based multi-agent execution system for long-running coding work.

It is built around a durable event-driven router, explicit actor phases, workspace-local evidence artifacts, and reviewer-driven restart routing.

## What LOOP Does

LOOP runs work through a structured actor pipeline instead of a single monolithic agent:

- `implementer` changes the workspace.
- `evaluator_checker` converts `FINAL_EFFECTS.md` into explicit evaluator tasks.
- `evaluator_tester` performs one global review pass.
- `evaluator_ai_user(task_id)` runs task-scoped adversarial validation lanes in parallel.
- `evaluator_reviewer` adjudicates the current wave and decides whether the run can close or which actors need to rerun.

The router persists durable state in `router.db`, tracks runtime actor state per node, and survives partial failures by restarting from the right layer instead of always falling back to the beginning.

## Current System Highlights

- Durable router state machine backed by SQLite.
- Explicit node runtime model with `current_components` rather than a single current actor.
- Parallel evaluator execution: one global tester plus up to 10 `ai_user(task_id)` lanes at once.
- Actor-targeted reviewer feedback with workspace-local feedback ledgers.
- Feedback-driven restart routing:
  - `implementer` feedback restarts from implementer.
  - `checker` feedback restarts from checker.
  - `tester` and `ai_user` feedback rerun only the affected evaluator lanes.
- Honest closeout model based on durable reports, result refs, and workspace evidence.

## Repository Layout

- `loop/`: router core, runtime, launch, storage, feedback, and CLI API.
- `scripts/routerapi.sh`: router/operator control entrypoint.
- `scripts/routerctl.sh`: actor-facing completion and split entrypoint.
- `tests/`: focused router/runtime/evaluator tests.

## Install

This repository uses `uv`.

```bash
cd LOOP
uv sync
```

## Main Control Surface

Router/operator control goes through:

```bash
./scripts/routerapi.sh --help
```

`routerapi.sh` is a thin wrapper around:

```bash
uv run --project . --locked python -m loop.router_api
```

Actor completions and split requests still go through:

```bash
./scripts/routerctl.sh --help
```

`routerctl.sh` is a thin wrapper around:

```bash
uv run --project . --locked python -m loop.agent_api
```

The router API remains workspace-oriented: actor completions, reviewer reports, feedback ledgers, and runtime artifacts all live under the active workspace.

## Runtime Model

At a high level, one node executes like this:

1. `implementer`
2. `evaluator_checker`
3. `evaluator_tester` + parallel `evaluator_ai_user(task_id)` lanes
4. `evaluator_reviewer`

After reviewer:

- if approval is honest, the run closes;
- otherwise reviewer emits machine-readable feedback and the router restarts from the highest-priority affected layer while preserving lower-priority feedback for later delivery.

This means the actual runtime is no longer a rigid fixed loop. It is feedback-driven.

## Reviewer Feedback Routing

Reviewer output has two layers:

- human-readable `reviewer_report.md`
- machine-readable `feedback.json`

The machine-readable layer is primary for routing. It is stored in workspace-local feedback artifacts such as:

- `.loop/router_runtime/feedback/active_feedback.json`
- `.loop/router_runtime/feedback/feedback_history.jsonl`

Feedback is bucketed by actor:

- `implementer`
- `checker`
- `tester`
- `ai_user` by `task_id`

The router injects only actor-relevant active findings into each actor prompt through the router feedback slot, while also exposing the feedback history path for context.

## Running Tests

The current repository is covered by focused Python test scripts rather than a single unified test runner.

Useful checks:

```bash
python3 -m py_compile loop/*.py tests/*.py
python3 tests/check_loop_system_repo_router_api.py
python3 tests/check_loop_system_repo_router_agent_api.py
python3 tests/check_loop_system_repo_router_agent_api_evaluator.py
python3 tests/check_loop_system_repo_router_core.py
python3 tests/check_loop_system_repo_router_evaluator.py
python3 tests/check_loop_system_repo_router_events.py
python3 tests/check_loop_system_repo_router_node_table.py
python3 tests/check_loop_system_repo_router_proc.py
python3 tests/check_loop_system_repo_router_runtime.py
python3 tests/check_loop_system_repo_router_start.py
python3 tests/check_loop_system_repo_router_store.py
```

Some tests intentionally exercise failure paths and may print tracebacks while still ending in `PASS`.

## Current Status

This repository currently contains the standalone LOOP runtime core and its focused tests.

It already includes:

- evaluator parallelism,
- actor-targeted feedback routing,
- reviewer feedback ledgers,
- checker/tester/AI-user/reviewer prompt contracts in production code.

It does not yet include polished top-level documentation beyond this README, a public paper figure set, or a broader packaged user manual.
