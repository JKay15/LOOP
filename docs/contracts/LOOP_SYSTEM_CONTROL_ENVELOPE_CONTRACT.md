# LOOP System Control Envelope Contract

This contract freezes `IF-4` for all structured control objects.

## Purpose

All control surfaces must share one structured envelope.

## Runtime anchors

- envelope schema: `docs/schemas/LoopSystemControlEnvelope.schema.json`
- topology payload schema: `docs/schemas/LoopSystemTopologyMutation.schema.json`
- local control payload schema: `docs/schemas/LoopSystemLocalControlDecision.schema.json`
- dispatch payload schema: `docs/schemas/LoopSystemDispatchStatus.schema.json`
- terminal payload schema: `docs/schemas/LoopSystemNodeTerminalResult.schema.json`
- normalization: `loop_product/gateway/normalize.py`
- classification: `loop_product/gateway/classify.py`
- acceptance / fail-closed rejection: `loop_product/gateway/accept.py`
- structured payload dataclasses: `loop_product/protocols/control_objects.py`

## Covered payloads

- split request
- activate request
- merge request
- resume request
- retry / relaunch request
- repo-tree cleanup request
- heavy-object registration request
- local control decision
- child heartbeat / dispatch status
- terminal result
- evaluator return

## Required envelope properties

- source
- generation / round
- payload type via explicit `payload_type`
- proposal / report / accepted / rejected state
- kernel acceptance boundary before becoming fact
- invalid raw envelopes must fail closed as `rejected`, not be silently repaired into accepted fact

## Required behavior

- topology requests stay `proposal` until kernel acceptance
- `activate` requests are topology requests and therefore stay proposals until kernel acceptance
- `resume`, `retry`, and `relaunch` requests are topology requests and therefore stay proposals until kernel acceptance
- `repo_tree_cleanup_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_authority_gap_audit_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_discovery_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_registration_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_observation_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_reference_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_reference_release_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_pin_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_pin_release_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_supersession_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_gc_eligibility_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_reclamation_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- `heavy_object_authority_gap_repo_remediation_request` envelopes stay `proposal` until kernel acceptance even though they reuse `payload_type=local_control_decision` for payload normalization
- local control decisions, child dispatch status, node terminal results, and evaluator returns stay `report` until kernel acceptance
- accepted `repo_tree_cleanup_request` envelopes must preserve their explicit `repo_tree_cleanup` classification so trusted command/trace surfaces do not have to reverse-engineer repo-forest cleanup intent from generic local-control payloads
- accepted `heavy_object_authority_gap_audit_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace/runtime surfaces do not have to reverse-engineer repo-root heavy-object authority-gap audit intent from generic local-control payloads
- accepted `heavy_object_discovery_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace/runtime surfaces do not have to reverse-engineer heavy-object discovery intent from generic local-control payloads
- accepted `heavy_object_registration_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace surfaces do not have to reverse-engineer heavy-object intent from generic local-control payloads
- accepted `heavy_object_observation_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace surfaces do not have to reverse-engineer heavy-object observation intent from generic local-control payloads
- accepted `heavy_object_reference_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace/runtime surfaces do not have to reverse-engineer heavy-object reference intent from generic local-control payloads
- accepted `heavy_object_reference_release_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace/runtime surfaces do not have to reverse-engineer heavy-object reference-release intent from generic local-control payloads
- accepted `heavy_object_pin_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace surfaces do not have to reverse-engineer heavy-object pin intent from generic local-control payloads
- accepted `heavy_object_pin_release_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace/runtime surfaces do not have to reverse-engineer heavy-object pin-release intent from generic local-control payloads
- accepted `heavy_object_supersession_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace surfaces do not have to reverse-engineer heavy-object supersession intent from generic local-control payloads
- accepted `heavy_object_gc_eligibility_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace surfaces do not have to reverse-engineer heavy-object GC eligibility from generic local-control payloads
- accepted `heavy_object_reclamation_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace/runtime surfaces do not have to reverse-engineer heavy-object reclamation intent from generic local-control payloads
- accepted `heavy_object_authority_gap_repo_remediation_request` envelopes must preserve their explicit `heavy_object` classification so trusted command/trace/runtime surfaces do not have to reverse-engineer repo-root heavy-object remediation fanout intent from generic local-control payloads
- accepted envelopes must preserve `payload_type` so read-only callers do not have to reverse-engineer semantics from ad hoc string matching
- accepted `node_terminal_result` envelopes must be able to update the authoritative node lifecycle state
- rejected recovery requests must remain rejected audit facts only and must not mutate authoritative node state
