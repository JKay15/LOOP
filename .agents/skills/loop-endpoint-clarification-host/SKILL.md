---
name: loop-endpoint-clarification-host
description: Repo-local host adapter for Phase 1 clarification before kernel mode.
---

## Use when
- The current ordinary conversation turn still lacks one endpoint-defining fact.
- You are inside `loop_product_repo` and need the committed local clarification path.

## Routing rules
1. Before `kernel mode`, the conversation-facing Codex agent itself is the clarification agent.
2. Ask exactly one highest-impact endpoint question or restate the final effect for confirmation.
3. For fresh tasks, use a session root under `.cache/endpoint_clarification/<task_slug>`.
4. Persist that turn through `scripts/persist_clarification_question.sh` or `scripts/persist_clarification_confirmation.sh`.
5. The wrappers already own repo-local shared `uv` cache setup, and `--task-type` is only a hint.
6. Do not ask implementation-path questions by default.

## Minimal host pattern
```bash
scripts/persist_clarification_question.sh --session-root .cache/endpoint_clarification/<task_slug> --user-text "<ordinary_user_turn>" --user-request-summary "<summary>" --question "<single_question>"
scripts/persist_clarification_confirmation.sh --session-root .cache/endpoint_clarification/<task_slug> --user-text "<ordinary_user_turn>" --user-request-summary "<summary>" --final-effect "<final_effect>" --observable-success-criterion "<criterion>"
```
