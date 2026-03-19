# LOOP Product Repo

This standalone product repo is the same independent repo for the LOOP front-half and back-half.

- The `front-half` lives here as endpoint clarification.
- The current `back-half` product lives here as the thin evaluator runtime.
- The richer legacy evaluator prototype also lives here as a compatibility and self-eval surface.
- The next complete LOOP system wave also lives here as kernel / node / dispatch / topology skeletons under the same package root.
- LeanAtlas is not the permanent implementation home of these product surfaces; LeanAtlas is an adapter and integration host.

This repo exists so front-half and back-half can evolve together as one product while still being reused outside LeanAtlas.

## Current package root

All product runtime surfaces live under `loop_product/`.

- accepted front-half runtime: `loop_product.endpoint.clarification`
- formal thin evaluator aliases:
  - CLI: `loop-evaluator`
  - package root: `from loop_product import run_evaluator`
- thin evaluator implementation package: `loop_product.evaluator.simple`
- legacy evaluator compatibility runtime: `loop_product.evaluator.prototype`
- wave-1 complete LOOP runtime skeleton:
  - `loop_product.kernel`
  - `loop_product.gateway`
  - `loop_product.loop`
  - `loop_product.runtime`
  - `loop_product.topology`
  - `loop_product.dispatch`
  - `loop_product.protocols`

## Smoke path

Run the wave-1 smoke path with:

```bash
UV_CACHE_DIR=.uv-cache uv run --project . python tests/check_loop_system_repo_smoke.py
```

Run the formal thin evaluator product surface with:

```bash
UV_CACHE_DIR=.uv-cache uv run --project . loop-evaluator --input <request.json>
```

The formal package-root alias is:

```python
from loop_product import run_evaluator
```

The repo now keeps the front-half and thin-evaluator implementations under dedicated subpackages:

- `loop_product/endpoint/clarification.py`
- `loop_product/evaluator/simple.py`
