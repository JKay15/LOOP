# LOOP System Real-AI-Agent Gating Matrix

This matrix freezes the real-agent scenarios that must exist before the final event-control migration may claim success.

Deterministic tests are necessary, but not sufficient. Every requirement below must eventually be exercised through a real provider/profile-backed AI-agent path.

## Scenario inventory

### RA-001. Real-agent orphan cleanup and truthful quiescence
- Covers:
  - `RQ-018`
  - `RQ-020`
  - `BUG-009`
  - `CE-010`
- Scenario intent:
  - run a real child/sidecar/supervisor stack
  - interrupt/supersede it in a realistic way
  - prove the final system records durable reap/defer/quiesced facts instead of leaving orphaned background processes behind

### RA-002. Real-agent heavy-object sharing under evaluator/publish churn
- Covers:
  - `RQ-019`
  - `RQ-020`
  - `BUG-010`
  - `CE-011`
- Scenario intent:
  - exercise a real publish/evaluator/workspace flow that currently tends to duplicate `.lake`/mathlib trees
  - prove the final system shares or references heavyweight content through authority-tracked object lifecycle facts
  - prove superseded heavy trees become GC-eligible without breaking rerun/recovery

### RA-003. Real-agent liveness lease and wrong-process rejection
- Covers:
  - `RQ-004`
  - `RQ-005`
  - `RQ-006`
  - `BUG-003`
  - `BUG-004`
  - `CE-002`
  - `CE-003`
  - `CE-004`
- Scenario intent:
  - run a real `codex exec` child
  - induce interruption / reuse / stale observer conditions
  - prove the final system grants live truth only with identity continuity and fresh lease evidence

### RA-004. Real-agent retry/recovery exact-once governance
- Covers:
  - `RQ-003`
  - `RQ-006`
  - `RQ-007`
  - `BUG-002`
  - `BUG-005`
  - `CE-001`
  - `CE-006`
- Scenario intent:
  - exercise a real retry/resume/recovery sequence under AI-agent control
  - prove the final system preserves successful recovery while preventing duplicate logical side effects

### RA-005. Real-root `r59` final recovery rehearsal
- Covers:
  - `RQ-017`
  - `RQ-018`
  - `RQ-019`
  - `RQ-020`
  - `CE-009`
  - `CE-010`
  - `CE-011`
- Scenario intent:
  - recover the preserved `r59` checkpoint through the final architecture
  - drive it to truthful bug-free completion
  - prove that stale-state drift, wrong-process reuse, retry churn, orphan cleanup, and heavy-object duplication all stay under control in a real historically messy root

## Gating rule

- No final cutover is allowed until every scenario above has a concrete real-agent implementation and passing evidence.
- Synthetic deterministic tests may prove local invariants first, but they do not satisfy this matrix on their own.
