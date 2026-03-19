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
- local control decisions, child dispatch status, node terminal results, and evaluator returns stay `report` until kernel acceptance
- accepted envelopes must preserve `payload_type` so read-only callers do not have to reverse-engineer semantics from ad hoc string matching
- accepted `node_terminal_result` envelopes must be able to update the authoritative node lifecycle state
- rejected recovery requests must remain rejected audit facts only and must not mutate authoritative node state
