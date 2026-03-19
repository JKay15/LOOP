# LOOP System State Query Contract

This contract freezes `IF-5` for read-only state visibility.

## Purpose

Read-only state queries must stay simple.

## Runtime anchors

- raw state schema: `docs/schemas/LoopSystemKernelState.schema.json`
- authority view schema: `docs/schemas/LoopSystemAuthorityView.schema.json`
- raw state query implementation: `loop_product/kernel/state.py`
- simplified authority view: `loop_product/kernel/query.py`

## Required query surfaces

- current authoritative node graph
- node lifecycle state
- current effective requirement set
- budget / complexity view
- blocked reason
- current delegation map
- active evaluator lane visibility
- active child node visibility
- planned child visibility

## Required behavior

- `query_authority_view(...)` must expose the read-only answers directly rather than requiring callers to reconstruct them from raw accepted envelopes
- the simplified authority view must stay read-only and must not mutate kernel state
- the simplified authority view may omit low-level storage internals such as raw `accepted_envelopes` when a direct higher-level answer already exists
- the simplified authority view must keep active child nodes and planned child nodes separate so deferred split state does not masquerade as already-active work
- once kernel accepts deferred activation, the promoted child must leave `planned_child_nodes` and appear in `active_child_nodes` without callers having to inspect raw envelopes
- once kernel accepts deferred merge, the reactivated source must appear active again without forcing callers to infer runtime reactivation from raw envelopes
