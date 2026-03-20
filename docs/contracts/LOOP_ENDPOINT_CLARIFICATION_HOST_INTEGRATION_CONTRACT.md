# LOOP Endpoint Clarification Codex Host Integration Contract

This contract freezes the repo-controlled host-specific adapter behavior for using endpoint-clarification front-half from ordinary Codex app GUI conversation.

## Purpose

The front-half core runtime is only a deterministic helper/CLI. The stronger user-facing effect lives at the host layer:

- a conversation-facing agent in Codex app ordinary conversation must proactively route fuzzy endpoint requests through front-half clarification
- before `kernel mode`, that same conversation-facing agent is itself the clarification agent
- the user must not need to invoke scripts or mode commands explicitly
- the internal adapter surface may use `apply-decision`, `chat-turn`, or `handle_endpoint_clarification_turn(...)`
- this is a host-specific adapter contract for this repository's instruction environment, not a guarantee for uncontrolled external hosts

## Required instruction surfaces

The repository must expose all of the following:

- root `AGENTS.md` routing that points fuzzy-endpoint conversation work at `.agents/skills/loop-endpoint-clarification-host/SKILL.md`
- `.agents/skills/loop-endpoint-clarification-host/SKILL.md`
- `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_CONTRACT.md`
- `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_PART1_PRODUCT_MANUAL.md`
- `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_CONTRACT.md`
- `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_PRODUCT_MANUAL.md`

## Required host behavior

Within this repository's Codex host environment:

1. When an ordinary conversation turn shows that the desired final result is still fuzzy, the conversation-facing agent must proactively route the turn through endpoint clarification.
2. The agent must not ask the user to type `start`, `continue`, or `chat-turn`.
3. Before `kernel mode`, the conversation-facing Codex agent itself must perform the clarification reasoning.
4. For fresh tasks, the host should allocate the fresh clarification session root through `scripts/new_clarification_session_root.sh` rather than reusing a static `.cache/endpoint_clarification/<task_slug>` path.
5. The host should persist that host-supplied clarification decision through the committed wrappers `scripts/persist_clarification_question.sh` or `scripts/persist_clarification_confirmation.sh` as the normal path.
6. Those wrappers own repo-local shared `uv` cache setup, and `--task-type` is only a hint rather than a brittle exact enum guess.
7. `chat-turn` or `handle_endpoint_clarification_turn(...)` remain compatibility paths, not the preferred host path.
8. If front-half returns `BYPASS`, the conversation may continue into downstream work.
9. If front-half enters an active clarification session, later ordinary conversation turns for the same task must continue against that same exact session root until clarification terminates or downstream handling takes over.
10. If the default clarification policy path has trouble, the host should rely on the front-half built-in fallback before resorting to host-controlled policy injection.
11. If the same user turn already explicitly authorizes immediate downstream execution and clarification becomes sufficient on that turn, the host should persist confirmation and continue directly instead of forcing a second proceed-now turn.

## Boundary

- The deterministic helper/CLI surface remains frozen by `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_CONTRACT.md`.
- Host-specific proactive ordinary-conversation routing lives in this host-specific contract, not in the standalone core helper contract.
- The committed repo-local skill is part of this repository's host instruction surface for ordinary conversation routing.
- This host contract does not claim that the standalone helper alone can force Codex app behavior.
- This contract intentionally does not guarantee behavior for uncontrolled external hosts that ignore this repository's instructions.
