---
name: repo-hygiene
description: Keep LOOP repo state, artifacts, cache, quarantine, and git actions layered cleanly and non-destructively.
---

## Use when
- You are deciding where runtime outputs belong.
- You need cleanup or quarantine boundaries.
- You need safe git hygiene.

## Responsibilities
- Separate source, durable state, cache, artifacts, and quarantine.
- Enforce cleanup and quarantine safety boundaries.
- Keep runtime state under `.loop/`.
- Prefer non-destructive, auditable git operations.
- Keep experience assets distinct from cache and durable state.

## Not responsible for
- Reinterpreting product requirements or evaluator verdicts.
- Authorizing destructive git or filesystem actions that bypass root rules.
