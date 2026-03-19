# Final Effects

- The evaluator can launch checker, ordinary tests, AI-as-User, and reviewer roles from code using the product manual plus final-effect requirements as inputs.

- When the raw final-effects text is repairable, the evaluator auto-repairs it into normalized requirements and a requirement graph instead of stopping early.

- The evaluator preserves an auditable evidence chain for every role run, including prompts, contexts, results, responses, invocation metadata, and execution spans.

- Each evaluator AI role uses a dedicated role-specific `cwd`; that role workspace carries role-local `AGENTS.md` / `AGENTS.override.md`, per-run artifacts still live under `run_root/.loop/<role>/runs/`, launch must not override the default Codex home merely to select role rules, repo-shipped Python module role commands must still preserve repo importability from that `cwd` such as by prepending the product repo root to `PYTHONPATH`, and the nested run must not inherit parent `CODEX_*` session/transport env vars such as `CODEX_THREAD_ID` and `CODEX_INTERNAL_ORIGINATOR_OVERRIDE`.

- The evaluator preserves ordinary-test execution results, AI-as-User evidence, reviewer effect reviews, and a runtime-derived final evaluator status.

- Reviewer final-effect claims remain fail-closed on evidence refs plus reproduction steps.

- Optional reviewer commentary stays opaque passthrough.

- The caller-facing supervised evaluator entrypoint must continue supervising until a terminal evaluator result is produced.

- When the child evaluator fails before producing a terminal evaluator report, the caller-facing supervised evaluator entrypoint surfaces a confirmed `BUG` report instead of pretending completion.

- When the child evaluator makes no progress within the configured timeout, the caller-facing supervised evaluator entrypoint surfaces a confirmed `STUCK` report instead of pretending completion.

- While the evaluator is still running normally and no terminal result exists yet, the caller-facing supervised entrypoint does not treat mid-run progress as a completed evaluation result.

- Reusable host-rule host-specific effect: this shared standalone evaluator product repo keeps the Codex app host rule in root `AGENTS.md` plus `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_CONTRACT.md` and `docs/contracts/LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_PRODUCT_MANUAL.md`; any repo that reuses it can load those host instructions, and that loaded rule must make the conversation-facing agent must not stop mid-run or treat partial progress as completion unless a terminal result, confirmed `BUG`, or confirmed `STUCK` condition has been established.

- Final architecture effect: the evaluator back-half is independent of the current LeanAtlas repo, owned by this shared standalone product repo, and treated as part of the same independent repo, namely the same new repo / independent product as the front-half, with LeanAtlas reduced to a host adapter or integration layer rather than the evaluator's permanent home.

- Independence work must preserve already-validated evaluator semantics, evidence guarantees, isolation behavior, supervision behavior, and performance/stability optimizations; extraction into the shared independent repo is not allowed to regress previously accepted back-half behavior.

- Shared independent-repo effect: the front-half and back-half must live inside the same independent product repo rather than two separate repos.

- Shared independent-repo effect: after separation from LeanAtlas, this shared independent repo must still support running, testing, packaging, and reusing the core front-half and back-half product surfaces.

- Shared independent-repo effect: core product code in this shared independent repo must not require runtime imports from LeanAtlas internal modules.

- Shared independent-repo effect: cache directories, environment-variable prefixes, schema ids, and report schema names for the extracted product surfaces must be neutral rather than carrying `leanatlas`-specific names.

- Shared independent-repo effect: product manuals, contracts, example commands, and other user-facing product docs for the extracted product surfaces must not rely on LeanAtlas absolute paths.

- Shared independent-repo effect: other repos that reuse this shared independent product repo should not need LeanAtlas itself as a required runtime dependency.

- Shared independent-repo effect: this shared independent repo must carry its own product manuals, contracts, schemas, tests, and host rules instead of depending on LeanAtlas-owned copies as the permanent source of truth.

- Back-half independent-repo effect: the evaluator runtime must be able to run inside the shared independent repo without LeanAtlas-specific code paths.

- Back-half independent-repo effect: `raw` and `supervised` entrypoints both live in the shared independent repo and remain fully provided there.

- Back-half independent-repo effect: the evaluator must be able to evaluate arbitrary external workspaces rather than assuming the product under test belongs to LeanAtlas.

- Back-half independent-repo effect: checker, ordinary-test generation, AI-as-User, and reviewer runtime conventions must be provided by the shared independent repo itself.

- Back-half independent-repo effect: this shared independent repo ships the evaluator non-early-exit host rule as root/manual/contract instruction surfaces, not as an extra workflow skill pack, so any repo that reuses it can load that rule and make its conversation-facing agent inherit the same “do not stop mid-run unless terminal/BUG/STUCK” behavior.

- Back-half independent-repo effect: self-eval must depend only on manuals, final effects, fixtures, and host rules that ship inside the shared independent repo.
