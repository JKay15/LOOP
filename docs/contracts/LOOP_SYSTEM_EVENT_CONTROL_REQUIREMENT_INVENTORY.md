# LOOP System Event-Control Requirement Inventory

This inventory freezes the final-state requirements for the event-journal control-plane migration.

It exists for one reason: implementation choices may change, but the endpoint guarantees may not drift.

## Requirement inventory

### RQ-001. Single authoritative truth
- Intent: every logical fact has one authoritative durable source.
- Current baseline: `result.json` already dominates terminal truth on the current trusted path.
- Milestone gate: event journal becomes the only durable fact store; all state surfaces become derived projections.

### RQ-002. Deterministic projection and repair
- Intent: stale state is repaired by reprojection, not by hand-edit.
- Current baseline: trusted self-heal / backfill paths exist for several state surfaces.
- Milestone gate: every projection exposes `last_applied_seq` and can be rebuilt from committed truth.

### RQ-003. Exact-once semantic effect
- Intent: physical replay may happen, logical side effects may not duplicate.
- Current baseline: accepted-envelope replay is now idempotent on the hardened path.
- Milestone gate: command/event replay cannot duplicate node, topology, retry, or terminal side effects.

### RQ-004. Truthful liveness
- Intent: `ACTIVE`/`ATTACHED` must require fresh authority, not a stale snapshot.
- Current baseline: trusted readers can self-heal several stale runtime surfaces.
- Milestone gate: lease/heartbeat/expiry semantics make stale live truth impossible to preserve indefinitely.

### RQ-005. Identity safety
- Intent: pid, wrapper, attempt alias, and sidecar identity may not be conflated.
- Current baseline: hardened runtime checks already reject several wrong-pid / wrong-process cases.
- Milestone gate: launch lineage + process identity + lease ownership are first-class committed facts.

### RQ-006. `codex exec` recovery is P0
- Intent: AI-process continuity is product-critical.
- Current baseline: trusted resume/retry flows can recover several interrupted `codex exec` scenarios today.
- Milestone gate: final architecture preserves all currently working recovery cases and improves the broken ones.

### RQ-007. Strict non-regression relative to the current trusted path
- Intent: cleaner internals may not weaken real behavior.
- Current baseline: parity thinking is already used for several hardening waves.
- Milestone gate: no switchover without passing the frozen capability-parity matrix.

### RQ-008. Projection surfaces are read-only derived artifacts
- Intent: ordinary code may not invent truth by rewriting caches.
- Current baseline: state-surface guardrails and trusted writer helpers exist.
- Milestone gate: only trusted writer/projector paths may materialize projections.

### RQ-009. Historical bug families are contract obligations
- Intent: every historically observed bug family remains a permanent regression target.
- Current baseline: the recent hardening wave already groups failures into recurring bug families.
- Milestone gate: every bug family has explicit tests and no milestone closes without rerunning them.

### RQ-010. Repo-local inspectability and recovery
- Intent: `.loop` remains auditable and recoverable by humans.
- Current baseline: file-level runtime roots and local inspection are central to current operations.
- Milestone gate: journal/projection tooling keeps repo-local debugging intact.

### RQ-011. Schema evolution without semantic drift
- Intent: old durable truth may not be silently reinterpreted under new code.
- Current baseline: schema versions exist in several durable result surfaces, but not yet uniformly.
- Milestone gate: committed event schemas, projections, and queries have explicit version policy.

### RQ-012. Crash consistency and atomic visibility
- Intent: crashes between commit and projection produce one truthful post-crash interpretation.
- Current baseline: atomic replace already protects some state writes.
- Milestone gate: journal commit, projection advance, and read visibility are crash-safe by construction.

### RQ-013. Lease fencing and observer concurrency safety
- Intent: stale sidecars and stale observers may not regain authority.
- Current baseline: some dedupe and supersession logic exists, but not yet as a general lease/fencing model.
- Milestone gate: every lease carries epoch/fencing information and stale writers are rejected.

### RQ-014. Operational observability and explainability
- Intent: every visible state must be explainable from committed facts.
- Current baseline: evidence exists, but explanation still requires ad hoc file spelunking.
- Milestone gate: event trace, projection explanation, and replay tools become first-class surfaces.

### RQ-015. Retention, compaction, snapshot, and GC discipline
- Intent: storage growth must stay bounded without damaging recovery.
- Current baseline: the repo has already suffered from artifact growth and `.lake` duplication.
- Milestone gate: compaction/snapshot/GC rules preserve rebuildability and auditability.

### RQ-016. Safe migration with mixed-version live processes
- Intent: old long-lived processes and new code may coexist temporarily without corrupting truth.
- Current baseline: this remains a known migration risk, not yet a solved contract.
- Milestone gate: compatibility shims and writer fencing make mixed-version migration safe.

### RQ-017. Proven recovery of a real historically damaged runtime root
- Intent: the final architecture must prove itself on a real messy root, not only synthetic tests.
- Current baseline: `r59` is preserved as the designated golden recovery case.
- Milestone gate: the final architecture must recover `r59` and drive it to truthful bug-free completion.

### RQ-018. Event-driven operational reclamation
- Intent: orphaned LOOP processes and stale runtime leftovers must be reclaimed through authoritative events, leases, and defer reasons rather than ad hoc shell cleanup.
- Current baseline: process hygiene and stopped-run GC exist, but they still act imperatively outside the committed authority model.
- Milestone gate: cleanup/reap/defer outcomes become authoritative committed facts with both deterministic and real-agent coverage.

### RQ-019. Event-driven heavy artifact sharing and retention
- Intent: heavy runtime trees such as `.lake`, mathlib packs, evaluator staging roots, and publish staging roots must be shared/pinned/GC'd through authoritative object lifecycle facts.
- Current baseline: the repo has already suffered from duplicated heavy trees and unmanaged `.lake` materialization.
- Milestone gate: large immutable content is content-addressed, referenced by events, and covered by both deterministic and real-agent tests.

### RQ-020. Full authoritative coverage of stateful effects
- Intent: any stateful mutation that changes truth, liveness, identity, retention, sharing, or cleanup status must be event-backed.
- Current baseline: the current migration covers authority/projection truth better than before, but process cleanup and heavy-object lifecycle still sit partly outside the authority boundary.
- Milestone gate: no “important but unmanaged side file/process/object” category remains; all such effects are either committed events or derived consequences of committed events, and real-agent gates prove this under actual runtime pressure.
