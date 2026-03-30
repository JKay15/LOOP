# LOOP System `r59` Golden Recovery Acceptance

This document freezes the real-root final acceptance scenario for the event-journal migration.

## Golden case

- Runtime root:
  - `/Users/xiongjiangkai/xjk_papers/leanatlas/loop_product_repo/.loop/arxiv_2602_11505v2_whole_paper_faithful_formalization_benchmark_r59`
- Preserved checkpoint:
  - `/Users/xiongjiangkai/xjk_papers/leanatlas/.cache/leanatlas/tmp/r59_golden_recovery_checkpoint_20260324T002141.json`

## Why this root

`r59` is not a clean synthetic case. It already exercised many historically important failure classes:
- stale-state drift
- wrong-process / alias confusion
- retry/recovery churn
- observation pipeline gaps
- split ownership complexity
- real evaluator/repair loops

Passing `r59` under the final architecture proves more than passing clean synthetic scenarios.

## Starting point summary

- `utility_learning_appendix_formalization`: already `COMPLETED / TERMINAL`
- `utility_learning_probabilistic_rate_chain`: preserved in an incomplete, historically messy state
- `whole_paper_integration_closeout`: not yet complete
- no trustworthy live child process was present at checkpoint capture time

## Final acceptance criteria

### RG-001. Recover from preserved checkpoint
- The final architecture can ingest the preserved `r59` checkpoint and reconstruct truthful authority/projection state from durable facts.

### RG-002. No fake-live regression
- Recovery may not reintroduce stale `ACTIVE`, stale `ATTACHED`, wrong-pid reuse, or untrusted live truth.

### RG-003. No duplicate-effect regression
- Recovery may not duplicate accepted-envelope side effects, duplicate retries, duplicate sidecars, or duplicate terminalization.

### RG-004. Truthful continuation
- Remaining unfinished work can be resumed/recovered under the final architecture using the same durable truth model.

### RG-005. Bug-free completion
- `r59` reaches truthful bug-free completion without reintroducing the historical bug families frozen in the bug matrix.

### RG-006. Whole-run explainability
- The final visible `r59` state, including any `whole_paper_integration_closeout` conclusion, is explainable from committed causal history.
