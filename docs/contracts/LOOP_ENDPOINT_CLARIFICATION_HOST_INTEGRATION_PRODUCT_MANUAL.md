# Endpoint Clarification Codex Host Integration Product Manual

## Purpose

This manual describes the Codex-host adapter behavior for endpoint clarification inside this repository's instruction environment.

The product here is not the standalone CLI by itself. The product is:

- a conversation-facing Codex app agent
- operating in ordinary conversation
- that proactively routes fuzzy endpoint turns through front-half clarification on the user's behalf

## User-facing expectation

The user should be able to simply talk to the Codex app agent in ordinary conversation.

The user should not need to:

- type `start`
- type `continue`
- type `chat-turn`
- know that an internal endpoint-clarification helper exists

## Internal host action

When the user's desired end state is still fuzzy, the conversation-facing Codex agent itself should do the clarification reasoning before `kernel mode`, then persist that decision by calling:

```bash
session_root="$(scripts/new_clarification_session_root.sh --task-slug <task_slug>)"  # fresh task only
scripts/persist_clarification_question.sh --session-root <session_dir> --user-text "<ordinary_user_turn>" --task-type <task_type> --user-request-summary "<summary>" --question "<single_question>"
scripts/persist_clarification_confirmation.sh --session-root <session_dir> --user-text "<ordinary_user_turn>" --task-type <task_type> --user-request-summary "<summary>" --final-effect "<final_effect>" --observable-success-criterion "<criterion>"
```

The returned assistant message becomes the next conversation turn shown to the user.
Those committed wrappers own the repo-local shared `uv` cache setup for ordinary full-access execution, and `--task-type` is only a hint: unsupported or omitted values are normalized automatically.
For fresh tasks, allocate the fresh clarification session root through `scripts/new_clarification_session_root.sh`. Reuse the exact same clarification session root only when that same clarification session is still active and already known.
If the same ordinary user turn already explicitly authorizes immediate execution and the endpoint becomes sufficiently clear on that turn, persist confirmation and continue directly instead of forcing an extra proceed-now turn.

Compatibility path:

If a host explicitly wants the front-half helper itself to synthesize the next clarification turn, it may still call:

```bash
python -m loop_product.endpoint.clarification chat-turn --session-root <session_dir> --user-text "<ordinary_user_turn>"
```
The host should rely on the built-in fallback inside the front-half if the default policy adapter has trouble; routine host behavior should not require manual `LOOP_ENDPOINT_POLICY_CMD` injection just to keep clarification moving.

## Session rule

- The host keeps one exact task-scoped session root while clarification is active.
- If the session is still active, the next ordinary user reply is routed through the same session.
- If the helper returns `BYPASS` or a clarified terminal state, the host can stop routing through front-half and proceed downstream.

## Relationship to the core helper

- `chat-turn` is a host adapter helper, not the final effect itself.
- The standalone front-half core contract/manual still live at:
  - `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_CONTRACT.md`
  - `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_PART1_PRODUCT_MANUAL.md`
- The repo-local host adapter skill lives at:
  - `.agents/skills/loop-endpoint-clarification-host/SKILL.md`
- Host-specific proactive routing belongs to this host integration manual/contract, not to the standalone helper contract.
