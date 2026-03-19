# LOOP Endpoint Clarification Contract

This contract freezes the standalone front-half core runtime for endpoint clarification.

## Purpose

The front-half runtime is a deterministic helper/CLI that clarifies a fuzzy endpoint before downstream work begins. It must:

- decide whether to enter `VISION_COMPILER` or `BYPASS`
- expose the documented helper surface for `chat-turn`, `start`, `continue`, and `show-artifact`
- persist a task-scoped endpoint artifact and per-turn results
- preserve confirmed and denied endpoint signals across turns
- persist a structured requirement artifact that restates the intended final effect, observable success criteria, constraints, and persistence readiness
- ask exactly one high-information clarification question per active clarification turn
- stop once endpoint clarity is sufficient

The persisted shell may delegate high-variance language judgment to a configurable AI-visible policy adapter, but artifact persistence, turn accounting, and the committed CLI/helper entrypoints remain the frozen core here. Ordinary provider-driven policy-agent execution should prefer committed argv-based launch instead of rebuilding ad hoc `bash -lc ...` shell strings.
If the default policy adapter attempt fails, stalls, or times out, the front-half runtime must still recover to a contract-valid clarification result through a deterministic fallback rather than hanging indefinitely.
This means the runtime must behave safely even when the default policy adapter attempt fails.
That fallback requirement only applies to the default nested policy path; an explicit override remains fail-closed.

This contract freezes the standalone helper itself. Host-specific proactive Codex app routing is out of scope here and is frozen separately by `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_CONTRACT.md`.

## Required callable surface

`loop_product.endpoint.clarification` must expose:

- `start_endpoint_clarification(...)`
- `apply_endpoint_clarification_decision(...)`
- `continue_endpoint_clarification(...)`
- `handle_endpoint_clarification_turn(...)`
- `load_endpoint_artifact(...)`

## Required CLI surface

`python -m loop_product.endpoint.clarification` must provide:

- `apply-decision`
- `chat-turn`
- `start`
- `continue`
- `show-artifact`

The CLI is the committed product surface for standalone front-half use and evaluator runs. `apply-decision` is the host-owned persistence ingress for a conversation-facing clarification agent, while `chat-turn` remains a compatibility helper ingress rather than a claim that the CLI alone delivers full GUI conversation integration.

## Session storage model

Each session is rooted at a caller-supplied `session_root`.

The runtime must persist at least:

- `EndpointArtifact.json`
- `turns/<turn_id>/TurnResult.json`

The runtime may add supporting files, but those two files remain the authoritative front-half artifacts.

## Artifact contract

`EndpointArtifact.json` must validate against `docs/schemas/LoopEndpointArtifact.schema.json`.

It must preserve at least:

- `mode`
- `status`
- `original_user_prompt`
- `confirmed_requirements`
- `denied_requirements`
- `requirement_artifact`
- `question_history`
- `turn_count`
- `artifact_ref`
- `latest_turn_ref`

## Turn-result contract

Each `TurnResult.json` must validate against `docs/schemas/LoopEndpointTurnResult.schema.json`.

It must preserve at least:

- `action`
- `turn_id`
- `mode_decision`
- `session_status`
- `assistant_message`
- `assistant_question`
- `question_count`
- `artifact_ref`
- `artifact_location_disclosed`
- `requirement_artifact_ready_for_persistence`

## Behavioral requirements

The runtime must satisfy all of the following:

1. `start` must deterministically return either `VISION_COMPILER` or `BYPASS`.
2. `chat-turn` on a session with no artifact must behave like `start`.
3. `chat-turn` on an active `VISION_COMPILER` session must behave like `continue`.
4. If a session enters `VISION_COMPILER`, later `continue` turns must remain in endpoint-clarification mode until a clarity stop is reached.
5. Each active clarification turn must ask exactly one question.
6. The active clarification question must target the single highest-impact missing detail needed to define the intended final effect, acceptance criteria, or hard constraints, and it must not ask the user to choose an implementation path.
7. The active clarification question must not ask implementation-path questions by default.
8. By default, the active clarification question must not ask about code or file paths, exact local asset paths, tooling, working directories, local asset availability, or placeholder-versus-final implementation variants unless one of those details is already part of the endpoint or is the single highest-impact blocker to defining the final effect.
9. Each active clarification turn must contain only one unresolved endpoint question; the runtime must not append a second execution-resource question or a placeholder/fallback implementation choice onto the same clarification turn.
10. If one upstream endpoint answer will determine downstream dependent choices, the runtime should ask only for that upstream answer first instead of bundling the dependent questions into the same clarification turn.
11. If the endpoint needs some asset class or representative asset from a work/domain but the user did not define an exact asset identity, the runtime should treat exact asset choice, exact local-file choice, and replacement-slot strategy as later implementation rather than clarification.
12. If a missing detail can be retrieved later by inspection or search without changing the user-confirmable endpoint, the runtime should leave it to later phases instead of asking during clarification.
13. For bug-fix or repair requests, the highest-impact missing detail should usually come from the minimal bug-fix set: target object if still ambiguous, current observable symptom if the object is clear but the failure is still vague, or intended post-fix observable behavior once object and symptom are already clear.
14. The runtime must not force a rigid fixed sequence when some bug-fix essentials are already explicit; already-clear essentials must be skipped rather than re-asked.
15. User input must remain free-form natural language.
16. The runtime must visibly disclose the endpoint artifact path on every emitted turn.
17. Once clarity is sufficient, the runtime must stop asking questions and mark the session clarified.
18. When clarity is sufficient, the emitted assistant message must restate the final effect, observable success criteria, and key constraints or non-goals in a user-confirmable form instead of only reporting that the request is sufficient.
19. The structured requirement artifact persisted inside `EndpointArtifact.json` must be explicit enough for downstream evaluator handoff.
20. If the default policy adapter attempt fails, the runtime must still emit a contract-valid clarification turn and leave auditable fallback evidence under the session policy directory.
21. An explicit override remains fail-closed: invalid `LOOP_ENDPOINT_POLICY_CMD` output must not silently fall back.

## Non-goals

This contract does not freeze:

- downstream task execution
- full Codex-host routing behavior for ordinary conversation
- any host-specific proactive GUI clarification rule
- kernel scheduling or orchestration behavior
- model-backed clarification-quality tuning
- the exact internal policy implementation under `loop_product.endpoint.policy`
