# Final Effects

- The system can stably decide whether to enter `VISION_COMPILER` or bypass endpoint clarification.

- Host-integration effect: within this repository's Codex app GUI conversation host, the conversation-facing agent proactively routes fuzzy endpoint requests through the front-half rather than waiting for the user to invoke explicit scripts or mode commands.

- The previous item is a host-integration effect, not a claim that the standalone CLI/helper surface by itself can force Codex app behavior in uncontrolled external hosts.

- Final architecture effect: the front-half is independent of the current LeanAtlas repo, owned by this shared standalone product repo, and treated as part of the same independent repo, namely the same new repo / independent product as the back-half evaluator, with LeanAtlas reduced to a host adapter or integration layer rather than the product's permanent home.

- Independence work must preserve already-validated front-half capabilities, artifact behavior, isolation behavior, and performance/stability optimizations; extraction into the shared independent repo is not allowed to regress previously accepted front-half behavior.

- Shared independent-repo effect: the front-half and back-half must live inside the same independent product repo rather than two separate repos.

- Shared independent-repo effect: after separation from LeanAtlas, this shared independent repo must still support running, testing, packaging, and reusing the core front-half and back-half product surfaces.

- Shared independent-repo effect: core product code in this shared independent repo must not require runtime imports from LeanAtlas internal modules.

- Shared independent-repo effect: default cache directories, environment-variable prefixes, schema ids, and report schema names for the extracted product surfaces must be neutral rather than carrying `leanatlas`-specific names.

- Shared independent-repo effect: product manuals, contracts, example commands, and other user-facing product docs for the extracted product surfaces must not rely on LeanAtlas absolute paths.

- Shared independent-repo effect: other repos that reuse this shared independent product repo should not need LeanAtlas itself as a required runtime dependency.

- Shared independent-repo effect: this shared independent repo must carry its own product manuals, contracts, schemas, tests, and host rules instead of depending on LeanAtlas-owned copies as the permanent source of truth.

- Front-half independent-repo effect: the front-half core runtime must be able to run inside the shared independent repo without LeanAtlas-specific code paths.

- Front-half independent-repo effect: front-half product manual, final effects, and schema documents live inside the shared independent repo.

- Front-half independent-repo effect: front-half artifact paths, cache paths, and default directory names must be neutral rather than carrying LeanAtlas-specific naming.

- Front-half independent-repo effect: host-specific requirements for proactive GUI-triggered clarification must live in a dedicated host integration contract rather than being mixed into the core runtime contract.

- Front-half independent-repo effect: this shared independent repo ships a front-half host rule/skill that other repos can load so their conversation-facing agents know when to proactively invoke front-half clarification instead of asking users to run explicit scripts.

- Once the system has entered `VISION_COMPILER`, later turns remain in endpoint-clarification mode until the endpoint is clear enough to stop.

- During active endpoint clarification, the system focuses on the future result the user wants to see, do, or feel, rather than asking the user to choose an implementation path.

- During active endpoint clarification, the user can answer in free-form natural language rather than being forced into a menu or option list.

- During each active clarification turn, the system asks exactly one high-information question.

- The system visibly persists confirmed and denied endpoint information in a task-scoped endpoint artifact.

- The system visibly tells the user where that endpoint artifact is stored.

- Once the endpoint is clear enough, the system stops asking clarification questions instead of drifting into downstream-layer discussion.

- The front-half behaves as an iterative clarification node across multiple turns rather than as a one-shot helper.
