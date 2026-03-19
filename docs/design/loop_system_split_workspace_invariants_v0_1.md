# Split Workspace Invariants

This note freezes the invariant layer for multi-child LOOP execution before the repo chooses a concrete workspace backend.

## Purpose

Once `split` can produce multiple implementer children, workspace semantics become part of correctness rather than a convenience detail. The system must make writable ownership, allowed shared reads, and merge inputs explicit before it adds richer DAG execution.

## Writable Isolation

- ACTIVE implementer nodes must never share the same writable directory.
- Each implementer node owns one node-local writable workspace and may mutate only that workspace.
- baseline selection is distinct from writable workspace ownership. Two children may start from the same declared baseline without sharing a writable directory.
- A child must not treat sibling workspaces as ambient implementation context.
- A child must not write directly into another child's workspace, shared resource roots, or final publish targets.

## Explicit Shared Reads

- shared resources must be declared explicitly as read-only inputs.
- A split child may read only:
  - its own node workspace
  - declared baseline inputs
  - declared shared read refs
  - its frozen handoff and delegated runtime artifacts
- Local convenience does not make a path shared. If a resource is not declared, the child must treat it as out of scope.
- Shared read refs may point at common source material, manuals, datasets, or durable upstream outputs, but they do not grant write access.

## Publish Targets

- publish targets are separate from implementer workspaces.
- Implementers should produce workspace mirror artifacts inside their own workspace first.
- Root publication may later copy those exact artifacts to external publish targets, but publication does not change workspace ownership rules.

## Merge Inputs

- merge must consume declared artifacts or result refs.
- must not scan sibling workspaces or historical deliverables implicitly.
- Merge should rely on durable declared surfaces such as:
  - child result refs
  - evaluator-backed node outputs
  - frozen delegation lineage
  - kernel-owned state/query views
- A merge decision may use declared completion evidence from children, but it must not infer convergence by directory similarity, filename similarity, or ad hoc workspace inspection.

## Practical Implications

- Backend choice stays open. These invariants can be satisfied by `git worktree`, copy-on-write materialization, snapshot restore, or another backend.
- Split proposals should eventually freeze:
  - the child's writable workspace root
  - the baseline it starts from
  - the shared read refs it may consume
  - the publish targets owned by root publication
- Deferred/DAG execution should activate children only after these boundaries are explicit, not while relying on implicit sibling visibility.
