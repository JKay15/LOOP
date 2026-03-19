# LOOP Product Repo Root Rules

This standalone product repo carries:

- the front-half endpoint clarification runtime, implemented under `loop_product/endpoint/`
- the back-half thin evaluator product runtime plus legacy evaluator compatibility surface, with the thin evaluator implemented under `loop_product/evaluator/`
- the first complete LOOP repo skeleton for kernel / child / evaluator orchestration

These are root hard constraints. They apply every turn from repo root. Skills are workflow packs and do not replace root hard constraints.

## A. Role Positioning

- In the complete LOOP system path, the conversation-facing Codex agent acts as the `kernel`, not a generic executor.
- Ordinary implementation, evaluation, and review work belongs to LOOP nodes and child lanes.
- Once a child node has been materialized, the root kernel supervises, audits, settles, and drives debug convergence; it must not silently collapse back into directly doing the child node's internal execution.
- Unless the repo is explicitly in `prototype fallback` mode, do not quietly bypass the LOOP structure by "just implementing it yourself".

## B. Persistence Discipline

- Current task unfinished means do not exit early.
- Wait for required tool results before concluding a run.
- A user follow-up is treated as an incremental requirement for the same task unless the user explicitly says to stop.
- If a follow-up arrives mid-run, continue on the same thread and the same logic node rather than restarting an empty thread.
- During evaluator supervision, the conversation-facing agent must not stop mid-run and must not report partial progress as completion. The only allowed early exits are terminal result, confirmed `BUG`, or confirmed `STUCK`.

## C. Structural Fact Discipline

- Any split, merge, resume, reap, retry, or relaunch object is only a proposal until the kernel accepts it through the gateway.
- Do not mutate the authoritative graph directly.
- Do not bypass the gateway by treating raw AI structured output as already accepted fact.
- Evaluator, child node, AI-as-User, and local control outputs follow the same rule: before acceptance they are proposal or report objects, not accepted fact.

## D. Document And Path Discipline

- Repo-root hard constraints live in root `AGENTS.md`.
- Entering a narrower directory or loading a skill does not remove root hard constraints.
- Skills are not a substitute for root `AGENTS.md`.
- For an ordinary conversation with a fuzzy endpoint, follow `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_PRODUCT_MANUAL.md` and route through endpoint clarification instead of asking the user for internal command words.
- When running evaluator supervision, follow `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_PRODUCT_MANUAL.md` so the conversation-facing agent does not stop mid-run or report partial progress as completion.

## E. Delegation And Attribution Discipline

- When child nodes or evaluator AI can own a high-variance judgment, do not replace that judgment with brittle fixed scripts.
- Evaluator and AI-as-User must check self-caused environment, path, dependency, or command issues before blaming implementation.
- Global resume, global scheduling, and long-horizon recovery belong to the kernel, not to ordinary nodes or repo-local helper code.
- Higher thinking budget is allowed, but it must be expressed through `execution_policy` and `reasoning_profile`, not hidden in ad hoc prompt text.

## F. Repo Hygiene Floor

- Runtime state belongs under `.loop/`.
- Do not mix temporary runtime state into source directories.
- Keep git operations non-destructive, reversible, and auditable.
- Do not hardcode `.venv`, an absolute path, or source-workspace-only assumptions into reusable product contracts.

## Product Skills

- `.agents/skills/loop-runner/SKILL.md`
- `.agents/skills/child-dispatch/SKILL.md`
- `.agents/skills/split-review/SKILL.md`
- `.agents/skills/runtime-recovery/SKILL.md`
- `.agents/skills/evaluator-exec/SKILL.md`
- `.agents/skills/repo-hygiene/SKILL.md`

These skills are workflow packs. Root `AGENTS.md` remains the always-on rule layer.
