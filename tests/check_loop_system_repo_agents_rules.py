#!/usr/bin/env python3
"""Validate the active root/workspace AGENTS instruction surfaces."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-agents-rules][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    path = ROOT / "AGENTS.md"
    if not path.exists():
        return _fail("missing root AGENTS.md")
    text = path.read_text(encoding="utf-8")
    if len(text.splitlines()) > 40:
        return _fail("root AGENTS must stay compact enough for live kernel instruction loading")

    required_needles = [
        "## Phase 1: Clarification",
        "## Phase 2: Kernel Mode Entry",
        "conversation-facing Codex agent itself is the clarification agent before `kernel mode`",
        ".agents/skills/loop-endpoint-clarification-host/SKILL.md",
        "scripts/new_clarification_session_root.sh",
        "fresh clarification session root",
        "scripts/persist_clarification_question.sh",
        "scripts/persist_clarification_confirmation.sh",
        "`apply-decision`",
        "ask exactly one highest-impact clarification question",
        "Clarification asks only for endpoint-defining facts",
        "do not ask implementation-path questions",
        "exact code, file, or asset locations, tooling, working directories, placeholder-versus-final implementation variants, or local asset discovery",
        "Do not delegate ordinary Phase 1 clarification to a second nested Codex agent",
        "imitate clarification persistence in free-form conversation",
        "jump directly into implementation, debugging, planning, repository edits, or command execution before clarification finishes",
        "reuse the exact same clarification session root only when that same clarification session is still active and already known",
        "same user turn already explicitly authorizes proceeding now",
        "otherwise say so explicitly and ask whether to proceed now",
        "either in that same clarification-completing user turn or in a later reply",
        "switch into `kernel mode`",
        "LOOP_SYSTEM_KERNEL_CONTRACT.md",
        "LOOP_SYSTEM_NODE_CONTRACT.md",
        ".agents/skills/child-dispatch/SKILL.md",
        "do not route through generic `loop-mainline`, LeanAtlas mainline docs, or other extra root-level workflow guides",
        "committed endpoint-driven lifecycle bootstrap surface",
        "scripts/bootstrap_first_implementer_from_endpoint.sh",
        "scripts/launch_child_from_result.sh",
        "scripts/child_progress_snapshot.sh",
        "scripts/check_child_runtime_status.sh",
        "repo's locked `uv` environment",
        "Do not inspect existing external-target files, local asset files",
        "committed orphaned-active recovery helper",
        "scripts/recover_orphaned_active.sh",
        "project folder under `workspace/`",
        "authoritative state already points at that exact workspace folder or node ref",
        "Do not pre-create a guessed project folder before the committed endpoint bootstrap wrapper returns the authoritative `workspace_root`.",
        "Do not scan old workspace folders, node reports, or evaluator artifacts to guess whether reuse applies.",
        "Do not preflight external publish-target writability during first-child bootstrap",
        "workspace mirror artifact",
        "root kernel owns the final publication step",
        "committed external publication helper",
        "scripts/publish_external_from_result.sh",
        "must use the committed publication helper instead of ad hoc `cp`, `mv`, or Finder automation",
        "EvaluatorRunState.json",
        "frozen checker graph",
        "`workspace_root` and current working directory",
        "`workspace/AGENTS.md` plus optional `workspace/AGENTS.override.md`",
        "must not directly implement and must first materialize exactly one LOOP node",
        "committed child launch helper",
        "must not translate launch_spec into a fresh ad hoc shell command",
        "must not hand-launch a bare `codex exec` if that would drop the node's launch policy",
        "child progress snapshot helper",
        "do not end the root conversation",
        "do not stop supervision",
        "do not force heartbeat-style progress chatter",
        "document refs observed from committed launch/evaluator logs",
        "liveness, status, or runtime-recovery supervision only",
        "`retryable=true` or `status = RECOVERY_REQUIRED`",
        "orphaned-active recovery",
        "quiet child with a still-live PID is supervision work, not recovery",
        "same-node orphaned-active retry",
        "pass direct structured flags to `scripts/recover_orphaned_active.sh`",
        "hand-writing a recovery request JSON file",
        "must not run its own acceptance, testing, inspection, or evaluator-like checking",
        "bootstrap-first-implementer-from-endpoint",
        "reading request schemas in chat",
        "probing `--help`",
        "using bare `python -m`",
        "`RECOVERY_REQUIRED`",
        "not yet a terminal evaluator-backed result",
        "terminal evaluator `FAIL` but the failure is still ordinary in-scope product work",
        "Publication may happen only after a terminal evaluator-backed result",
        "must not require evaluator PASS before mechanically publishing the exact workspace mirror artifact",
        "must preserve the evaluator verdict honestly",
        "scripts/supervise_child_from_launch.sh",
        "retryable terminal evaluator `FAIL`",
    ]
    for needle in required_needles:
        if needle not in text:
            return _fail(f"root AGENTS clarification rule missing required text: {needle}")

    workspace_text = (ROOT / "workspace" / "AGENTS.md").read_text(encoding="utf-8")
    if len(workspace_text.splitlines()) > 30:
        return _fail("workspace AGENTS must stay compact enough for implementer instruction loading")
    for needle in (
        "retryable evaluator-owned `RECOVERY_REQUIRED` result is not terminal node closeout",
        "leave durable checkpoint artifacts and explicit recovery evidence in the project folder before stopping on a nonterminal interruption",
        "Do not mine sibling `workspace/` task folders or ad hoc historical deliverables as implementation context",
        "inspect repo-local or user-local paths directly",
        "repo-local or user-local input discovery",
        "filename/path discovery helper",
        "--root`/`--term`/`--ext`",
        "avoid one broad prose query",
        "canonical local roots first",
        "do not browse, download, or substitute remote assets",
        "non-trivial content transforms",
        "temp path and atomic replace",
        "run_evaluator_node_until_terminal(...)",
        "exact evaluator runner/submission",
        "exact frozen refs",
        "guess alternates or broad repo scans",
        "fresh workspace with no deliverable yet",
        "one concrete workspace-local action",
        "Do not write the external publish target directly",
        "terminal evaluator `FAIL` caused by unmet product requirements is still unfinished implementer work by default",
        "Do not end the child conversation",
        "RECOVERY_CONTEXT.md",
        "self-attribution",
        "self-repair",
    ):
        if needle not in workspace_text:
            return _fail(f"workspace AGENTS implementer rule missing required text: {needle}")

    print("[loop-system-agents-rules] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
