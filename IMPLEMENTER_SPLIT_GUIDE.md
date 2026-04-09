# IMPLEMENTER_SPLIT_GUIDE

## Purpose

This guide defines how an implementer should decide whether to request split, how to prepare a valid split bundle, and what split means for later parent/child execution.

This guide is about `request-split` only.
It does not replace the main implementer prompt, FINAL_EFFECTS.md, reviewer feedback, or the router/kernel runtime contract.

Use this guide only when split is seriously under consideration.
If split is not justified, continue implementing the current node.

## When Split Is Worth Considering

Consider split only when all of the following are true:

1. The parent node has already made real implementation progress.
- Real progress means substantive changes to the actual deliverable in the current workspace.
- Reading, planning, decomposition, and writing split files do not count.

2. The remaining unfinished work contains at least two substantial work clusters.
- Each cluster must correspond to a real unfinished part of FINAL_EFFECTS.md.
- Do not split off tiny cleanup, commentary, or bookkeeping work.

3. The remaining clusters can be partitioned without semantic omission.
- Every still-unfinished requirement that matters to honest completion must remain explicitly owned after the split.
- Nothing important may disappear, become weaker, or become ownerless.

4. The proposed child scopes are genuinely parallelizable.
- Children must not normally require repeated simultaneous changes across the same tightly shared implementation surface.
- If the proposed children would predictably need repeated co-editing of the same files, same UI surface, same API boundary, or same integration boundary, the split is probably not worth it.

5. Parallel execution is likely to reduce total coordination cost.
- Split only when it is likely to reduce total delay or coordination risk more than it increases merge and evaluation cost.

Do not split in these cases:

- The parent has not yet made real implementation progress.
- The remaining work is mainly understanding, exploration, or design rather than separable implementation.
- The proposed children would overlap heavily on the same implementation surface.
- The remaining work is small enough that split would mostly add coordination and merge cost.
- You cannot describe exact child ownership without ambiguity.

## Zero-Omission Requirement

A split proposal must preserve the full remaining unfinished meaning of the parent FINAL_EFFECTS.md.

This requirement is stronger than covering only the obvious headline tasks.
The split must continue to own every still-unfinished part of the parent target that matters to honest completion, including:

- required outcomes,
- behavioral requirements,
- acceptance conditions,
- deliverable-form requirements,
- constraints,
- integration dependencies.

Practical rule:
- First identify the remaining unfinished scope of the parent FINAL_EFFECTS.md.
- Then ensure that every such remaining piece is explicitly owned after the split.

Ownership after the split may be assigned only in two ways:
- to exactly one child scope,
- or to `parent_after_merge`.

No remaining requirement may:
- disappear,
- become weaker,
- become ambiguous,
- be shared vaguely by multiple children,
- or become ownerless.

Partial delegation rule:
- If only part of a remaining requirement is delegated to a child, the undelegated remainder must be made explicit in `parent_after_merge`.
- Do not assume that the undelegated remainder is "obvious" or will be recovered later without being written down.

Good sanity check:
- If someone reads the parent FINAL_EFFECTS.md, then reads the split proposal plus all child FINAL_EFFECTS files and optional parent_after_merge FINAL_EFFECTS file, they should still be able to reconstruct the full remaining unfinished target without missing any material requirement.

## Choosing Child Boundaries

Good child boundaries are narrow, explicit, and operationally separate.

A good child scope usually has these properties:
- it owns a clear unfinished part of the parent target;
- its success does not normally require repeated simultaneous edits with another child;
- its interface to other children is relatively small and stable;
- its merge point is understandable and limited.

A bad child boundary usually has one or more of these problems:
- two children would need repeated co-editing of the same files or same tightly shared implementation surface;
- two children would need overlapping ownership of the same remaining requirement;
- the split is defined by a superficial partition, but the real implementation constraints still cut across both children;
- the parent would later have to untangle large amounts of overlapping integration debt.

Non-overlap rule:
- Each remaining coverage unit should belong to exactly one child or to `parent_after_merge`.
- Do not create children whose normal success path depends on vague shared ownership.

Parallelizability rule:
- A child split is worthwhile only if the children can make meaningful progress mostly independently.
- If the proposed children are expected to block on each other repeatedly during ordinary implementation, the split is probably not good.

Simple heuristic:
- If you expect the children to spend most of their time building their own part, the split may be good.
- If you expect the children to spend most of their time waiting on or reworking each other's changes, the split is probably bad.

## `parent_after_merge`

`parent_after_merge` is optional residual scope that remains owned by the parent after child outputs are merged.

Use `parent_after_merge` only when all of the following are true:
- the residual work is real;
- the residual work is explicit;
- the residual work only makes sense after child outputs have been merged.

`parent_after_merge` is allowed for narrow residual work such as:
- final integration work that genuinely depends on combined child outputs;
- explicit remaining requirements that were intentionally kept at the parent level because they depend on merged child state;
- conflict-resolution-adjacent follow-through that still belongs to the parent target.

Do not use `parent_after_merge` as:
- a vague "misc cleanup" bucket;
- a generic "final polish" bucket;
- a way to postpone large unfinished work that should have been assigned concretely to children or finished by the parent before splitting;
- a place to hide unclear ownership.

Important relationship to the parent target:
- `parent_after_merge` does not replace the parent FINAL_EFFECTS.md.
- The parent FINAL_EFFECTS.md remains the ultimate target for the parent node.
- `parent_after_merge` only makes the retained residual scope explicit inside the split proposal.

Practical rule:
- If residual parent work must still exist after child merge, write it narrowly and concretely.
- If you cannot describe the residual scope concretely, do not use `parent_after_merge`.

## `proposal.json v2`

A split bundle must contain one `proposal.json` with `version = 2`.

Minimal example:

```json
{
  "version": 2,
  "coverage_units": [
    {
      "id": "u01",
      "requirement_text": "Implement unfinished layout-shell work."
    },
    {
      "id": "u02",
      "requirement_text": "Implement unfinished login-control work."
    },
    {
      "id": "u03",
      "requirement_text": "Finish explicit residual parent integration work after child merge."
    }
  ],
  "children": [
    {
      "name": "layout-shell",
      "final_effects_file": "children/layout-shell/FINAL_EFFECTS.md",
      "covered_unit_ids": ["u01"]
    },
    {
      "name": "login-controls",
      "final_effects_file": "children/login-controls/FINAL_EFFECTS.md",
      "covered_unit_ids": ["u02"]
    }
  ],
  "parent_after_merge": {
    "final_effects_file": "parent_after_merge/FINAL_EFFECTS.md",
    "covered_unit_ids": ["u03"]
  }
}
```

Required rules:
- `version` must be exactly `2`.
- `coverage_units` must be non-empty.
- Every `coverage_units[*].id` must be unique.
- `children` must be non-empty.
- Every child name must be unique.
- Every child must reference a child FINAL_EFFECTS file that exists inside the split bundle.
- If `parent_after_merge` is present, its FINAL_EFFECTS file must exist inside the split bundle.

Ownership rules:
- Every coverage unit must be assigned exactly once.
- A coverage unit may be owned either by exactly one child or by `parent_after_merge`.
- No coverage unit may be omitted.
- No coverage unit may be assigned twice.
- No coverage unit may have vague or overlapping ownership.

Practical reading rule:
- `proposal.json` is the sole structural truth of the split proposal.
- Child FINAL_EFFECTS files and optional `parent_after_merge` FINAL_EFFECTS file explain the delegated or residual scopes in executable form.

## After Submission

After `routerctl request-split --split-bundle-ref <bundle_dir>` succeeds, the current implementer attempt should stop.
At that point, router and kernel own the split decision.

If kernel rejects the split:
- the parent node remains responsible for the original parent FINAL_EFFECTS.md;
- the parent will be resumed on the same node and should continue implementation;
- do not immediately resubmit the same split;
- only consider another split request after new effective implementation progress and a materially improved split case.

If kernel approves the split:
- child nodes run first using their child FINAL_EFFECTS files;
- child completion does not by itself mean the parent is complete;
- the parent remains responsible for the original parent FINAL_EFFECTS.md.

After child work returns:
- the parent's responsibility is the merged parent workspace, not just the pre-split workspace;
- if merge conflicts exist, resolve them first;
- if `parent_after_merge` exists, finish that explicit residual scope after child outputs are merged;
- only then may the parent call `routerctl complete`.

Completion rule after split:
- parent completion after a split still means the merged parent workspace honestly satisfies the original parent FINAL_EFFECTS.md;
- merged child output plus unresolved residual work is not completion.
