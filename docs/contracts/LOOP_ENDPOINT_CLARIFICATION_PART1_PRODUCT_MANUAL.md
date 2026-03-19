# Endpoint Clarification Part 1 Product Manual

## Purpose

`loop_product.endpoint.clarification` is a deterministic front-half endpoint-clarification helper. It accepts a user prompt, decides whether endpoint clarification is needed, persists a task-scoped endpoint artifact, and either:

- enters `VISION_COMPILER` and asks one highest-impact clarification question, or
- bypasses clarification when the endpoint is already specific enough.

The committed shell in `loop_product.endpoint.clarification` owns session files and CLI behavior. High-variance language judgment is delegated through the configurable AI-visible policy adapter `loop_product.endpoint.policy`, with `LOOP_ENDPOINT_POLICY_CMD` available as an override for tests or host-controlled policy injection. Ordinary provider-driven policy-agent launches should prefer committed argv execution rather than rebuilding `bash -lc ...` strings.
The default nested policy path is only given a short bounded attempt. If that attempt fails or stalls, the front-half must recover with a deterministic fallback and keep the clarification session moving.
That built-in fallback is the normal recovery path for default-policy trouble. `LOOP_ENDPOINT_POLICY_CMD` is still available, but it is not the routine success path for ordinary clarification.

This manual documents the front-half core/helper surface.
The frozen core runtime contract lives at:

- `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_CONTRACT.md`

Codex-host routing lives separately in:

- `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_CONTRACT.md`
- `docs/contracts/LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_PRODUCT_MANUAL.md`

That host-specific routing rule stays at the root-instruction/manual layer. It is not an extra workflow skill pack under `.agents/skills/`.

## Entry Point

Run the product with:

```bash
python -m loop_product.endpoint.clarification <command> ...
```

## Commands

### 1. Host-owned clarification persistence entrypoint

This is the preferred host path when the conversation-facing Codex agent itself is the clarification agent before `kernel mode`.

```bash
python -m loop_product.endpoint.clarification apply-decision --session-root <session_dir> --user-text "<prompt_or_reply>" --decision-file <decision_json_path>
```

Committed convenience paths for host-owned clarification:

```bash
session_root="$(scripts/new_clarification_session_root.sh --task-slug <task_slug>)"  # fresh task only
scripts/persist_clarification_question.sh --session-root <session_dir> --user-text "<prompt_or_reply>" --task-type <task_type> --user-request-summary "<summary>" --question "<single_question>"
scripts/persist_clarification_confirmation.sh --session-root <session_dir> --user-text "<prompt_or_reply>" --task-type <task_type> --user-request-summary "<summary>" --final-effect "<final_effect>" --observable-success-criterion "<criterion>"
```

These wrappers are thin convenience shells over the `persist-question` and `persist-confirmation` CLI commands in `python -m loop_product.endpoint.clarification`.
They also own the repo-local shared `uv` cache setup needed for ordinary full-access execution, and `--task-type` is only a hint: unsupported or omitted values are normalized to a supported clarification task type automatically.

Expected result:

- the current conversation-facing Codex agent decides whether the endpoint is still unclear or already sufficient
- the host uses `apply-decision` to persist a host-supplied clarification decision
- the committed `persist_clarification_question.sh` and `persist_clarification_confirmation.sh` wrappers are the normal host-owned path over `apply-decision`
- for fresh tasks, the host should allocate the fresh clarification session root through `scripts/new_clarification_session_root.sh`
- only an exact already-known active clarification session root should be reused across turns
- the host passes that structured decision into `apply-decision`
- that host-owned clarification must ask only for the highest-impact missing detail needed to define the final effect, observable success criteria, or hard constraints
- that host-owned clarification must not ask implementation-path questions by default
- that host-owned clarification must not ask about code or file paths, exact local asset paths, tooling, working directories, local asset availability, or placeholder-versus-final implementation variants unless one of those details is already part of the endpoint or is the single highest-impact blocker to defining the final effect
- that host-owned clarification must keep each clarification turn to one unresolved endpoint question rather than appending a second execution-resource or placeholder/fallback question
- if one upstream endpoint answer will determine downstream dependent choices, ask only for that upstream answer first instead of bundling the dependent questions into the same clarification turn
- if the endpoint needs some asset class or representative asset from a work/domain but not an exact asset identity, exact asset choice, exact local-file choice, and replacement-slot strategy belong to later implementation rather than clarification
- if a missing detail can be retrieved later by inspection or search without changing the user-confirmable endpoint, that host-owned clarification should leave it to later phases instead of asking during clarification
- if `<session_dir>/EndpointArtifact.json` does not exist yet, `apply-decision` persists a new clarification session
- if the existing session is an active `VISION_COMPILER` session, `apply-decision` persists the next turn on that same session
- the product prints the endpoint artifact path
- the product persists the same requirement artifact and turn-result structure used by the policy-driven path

### 2. Ordinary conversation compatibility host entrypoint

This is a host adapter surface. Hosts should prefer it when they want front-half behavior to be triggerable from ordinary conversation without forcing the user or caller to choose `start` versus `continue`.

```bash
python -m loop_product.endpoint.clarification chat-turn --session-root <session_dir> --user-text "<prompt_or_reply>"
```

Expected result:

- if `<session_dir>/EndpointArtifact.json` does not exist yet, `chat-turn` automatically behaves like `start`
- if the existing session is an active `VISION_COMPILER` session, `chat-turn` automatically behaves like `continue`
- the host adapter can call `chat-turn` for each ordinary user turn without asking the user to pick a mode
- the product prints the endpoint artifact path
- while clarification remains active, the product prints exactly one clarification question
- if the default policy adapter stalls, the product still returns a clarification result after a short bounded attempt by using the built-in deterministic fallback

Ordinary users should not be asked to invoke `chat-turn` themselves. `chat-turn` is an internal host adapter surface, not the final effect itself.

### 3. Start a new clarification session explicitly

```bash
python -m loop_product.endpoint.clarification start --session-root <session_dir> --user-text "<prompt>"
```

Expected result:

- if the prompt is already specific enough, the product returns `BYPASS`
- otherwise, the product returns `VISION_COMPILER`
- the product creates `<session_dir>/EndpointArtifact.json`
- the product prints the endpoint artifact path
- if the session enters `VISION_COMPILER`, the product prints exactly one clarification question
- if the prompt is already sufficient, the product must restate the final effect, observable success criteria, and key constraints or non-goals, and explicitly ask the user to confirm before downstream implementation starts
- if the default nested policy path does not return in time, the product must still recover to a contract-valid result instead of hanging indefinitely

### 4. Continue an existing clarification session explicitly

```bash
python -m loop_product.endpoint.clarification continue --session-root <session_dir> --user-text "<reply>"
```

Expected result:

- the existing endpoint artifact is loaded and updated
- confirmed and denied endpoint information is preserved across turns
- if the endpoint is still unclear, the product stays in `VISION_COMPILER`
- while the session remains active, the product prints exactly one clarification question
- if the endpoint becomes clear enough, the product stops asking more questions, marks the session clarified, restates the final effect in a user-confirmable form, and reports whether the requirement artifact is ready for persistence
- the product prints the endpoint artifact path
- if the default nested policy path fails mid-session, the product still recovers through deterministic fallback without requiring the host to inject a replacement result

### 5. Show the current endpoint artifact

```bash
python -m loop_product.endpoint.clarification show-artifact --session-root <session_dir>
```

Expected result:

- the current endpoint artifact is printed as JSON
- the JSON reflects the session mode, status, preserved requirements, and latest turn ref

## Interaction rules

While in `VISION_COMPILER`, the product should behave like this:

- focus on the highest-impact missing detail needed to define the intended final effect
- ask only for the highest-impact missing detail needed to define the final effect, observable success criteria, or hard constraints
- for bug-fix or repair requests, treat the target object, the current observable symptom, and the intended post-fix behavior as the usual minimum information set, but skip any of those that are already explicit
- do not ask the user to choose an implementation path
- do not ask implementation-path questions by default
- do not ask about code or file paths, exact local asset paths, tooling, working directories, local asset availability, or placeholder-versus-final implementation variants unless one of those details is already part of the endpoint or is the single highest-impact blocker to defining the final effect
- keep each clarification turn to one unresolved endpoint question instead of appending a second execution-resource or placeholder/fallback question
- if one upstream endpoint answer will determine downstream dependent choices, ask only for that upstream answer first instead of bundling the dependent questions into the same clarification turn
- if the endpoint needs some asset class or representative asset from a work/domain but not an exact asset identity, exact asset choice, exact local-file choice, and replacement-slot strategy belong to later implementation rather than clarification
- if a missing detail can be retrieved later by inspection or search without changing the user-confirmable endpoint, leave it to later phases instead of asking during clarification
- do not collapse the interaction into menu options
- ask one high-information question per turn
- visibly preserve the clarified endpoint state in the endpoint artifact

## Data Files

The session directory contains:

- `EndpointArtifact.json`
- `turns/<turn_id>/TurnResult.json`

`EndpointArtifact.json` includes the structured `requirement_artifact` that captures the current user-request summary, final effect, observable success criteria, hard constraints, non-goals, relevant context, open questions, and persistence readiness for downstream evaluation.

Users do not need to edit these files manually.
Ordinary users also do not need to choose `start` versus `continue`; a host adapter can route each turn through `chat-turn`.
