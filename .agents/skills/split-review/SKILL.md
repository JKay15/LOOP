---
name: split-review
description: Review whether work should split and produce a structured split proposal without directly changing the graph.
---

## Use when
- A node looks too broad, risky, or multi-front.

## Responsibilities
- Determine whether split review is needed.
- Produce a structured split request.
- Check the minimum split skeleton, including `S1` through `S5`.
- For dependency-gated children, keep explanatory prose in `activation_rationale`.
- Emit machine-evaluable `activation_condition` values only in `after:<node_id>:<requirement>` form; otherwise omit `activation_condition` and rely on `depends_on_node_ids` or the deferred default gate.

## Not responsible for
- Applying the split directly.
