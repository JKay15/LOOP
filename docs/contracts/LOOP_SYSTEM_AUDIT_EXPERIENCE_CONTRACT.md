# LOOP System Audit and Experience Contract

This contract freezes `IF-6` for audit replay and accepted experience data.

## Purpose

The repo must preserve enough audit and experience data to replay decisions and avoid repeated mistakes.

## Runtime anchors

- durable audit tree: `.loop/audit/`
- authoritative experience view: `experience_assets` in `docs/schemas/LoopSystemKernelState.schema.json`
- accepted evaluator self-diagnostics live in `payload.diagnostics`
- accepted recovery self-diagnostics live in accepted topology review payloads
- audit/experience query schema: `docs/schemas/LoopSystemAuditExperienceView.schema.json`
- audit query helpers: `loop_product/kernel/audit.py`

## Required surface

- replay key structural decisions
- query node-local and system-level audit events
- query current hard-rule sources
- record accepted `experience_assets`
- separate `experience_assets` from cache and durable state
- preserve evaluator, AI-as-User, and child-node self-attribution and self-repair conclusions

## Required behavior

- `query_recent_audit_events(...)` must expose recent audit history without requiring callers to inspect raw files manually
- `query_audit_experience_view(...)` must expose:
  - recent structural decisions
  - recent node events
  - current hard rules
  - accepted experience assets
  - explicit storage partition boundaries for audit, cache, durable state, experience assets, and hard rules

## Forbidden

- storing durable audit fact only in transient terminal logs
- treating repeated evaluator-side mistakes as if they were new implementation bugs
- dropping accepted self-attribution / self-repair evidence on the floor after gateway acceptance
