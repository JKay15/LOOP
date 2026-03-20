#!/usr/bin/env python3
"""Validate the first-wave LOOP system repo skeleton."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-skeleton][FAIL] {msg}", file=sys.stderr)
    return 2


def _require_path(rel: str) -> Path:
    path = ROOT / rel
    if not path.exists():
        raise FileNotFoundError(rel)
    return path


def main() -> int:
    required_paths = [
        "AGENTS.md",
        "README.md",
        ".gitignore",
        "workspace/AGENTS.md",
        "workspace/AGENTS.override.md",
        ".agents/skills/loop-runner/SKILL.md",
        ".agents/skills/loop-endpoint-clarification-host/SKILL.md",
        ".agents/skills/child-dispatch/SKILL.md",
        ".agents/skills/split-review/SKILL.md",
        ".agents/skills/runtime-recovery/SKILL.md",
        ".agents/skills/evaluator-exec/SKILL.md",
        ".agents/skills/repo-hygiene/SKILL.md",
        "docs/contracts/LOOP_SYSTEM_KERNEL_CONTRACT.md",
        "docs/contracts/LOOP_SYSTEM_NODE_CONTRACT.md",
        "docs/contracts/LOOP_SYSTEM_EVALUATOR_NODE_CONTRACT.md",
        "docs/contracts/LOOP_SYSTEM_CONTROL_ENVELOPE_CONTRACT.md",
        "docs/contracts/LOOP_SYSTEM_STATE_QUERY_CONTRACT.md",
        "docs/contracts/LOOP_SYSTEM_AUDIT_EXPERIENCE_CONTRACT.md",
        "loop_product/kernel/__init__.py",
        "loop_product/kernel/entry.py",
        "loop_product/kernel/state.py",
        "loop_product/kernel/submit.py",
        "loop_product/kernel/audit.py",
        "loop_product/kernel/policy.py",
        "loop_product/gateway/__init__.py",
        "loop_product/gateway/normalize.py",
        "loop_product/gateway/classify.py",
        "loop_product/gateway/accept.py",
        "loop_product/gateway/safe_degrade.py",
        "loop_product/loop/__init__.py",
        "loop_product/loop/runner.py",
        "loop_product/loop/roundbook.py",
        "loop_product/loop/local_control.py",
        "loop_product/loop/evaluator_client.py",
        "loop_product/runtime/__init__.py",
        "loop_product/runtime/liveness.py",
        "loop_product/runtime/recover.py",
        "loop_product/runtime/reconcile.py",
        "loop_product/runtime/lifecycle.py",
        "loop_product/topology/__init__.py",
        "loop_product/topology/activate.py",
        "loop_product/topology/split_review.py",
        "loop_product/topology/budget.py",
        "loop_product/topology/merge.py",
        "loop_product/topology/prune.py",
        "loop_product/topology/hoist.py",
        "loop_product/dispatch/__init__.py",
        "loop_product/dispatch/bootstrap.py",
        "loop_product/dispatch/child_progress_snapshot.py",
        "loop_product/dispatch/recovery_bootstrap.py",
        "loop_product/dispatch/launch_runtime.py",
        "loop_product/dispatch/child_dispatch.py",
        "loop_product/dispatch/heartbeat.py",
        "loop_product/dispatch/launch_policy.py",
        "loop_product/protocols/__init__.py",
        "loop_product/protocols/node.py",
        "loop_product/protocols/control_envelope.py",
        "loop_product/protocols/evaluator.py",
        "loop_product/protocols/topology.py",
        "tests/smoke/.gitkeep",
        "tests/kernel/.gitkeep",
        "tests/loop/.gitkeep",
        "tests/runtime/.gitkeep",
        "tests/topology/.gitkeep",
        "tests/integration/.gitkeep",
        "scripts/smoke.sh",
        "scripts/check_state_tree.sh",
        "scripts/print_audit_tail.sh",
        "scripts/persist_clarification_question.sh",
        "scripts/persist_clarification_confirmation.sh",
        "scripts/new_clarification_session_root.sh",
        "scripts/bootstrap_first_implementer_from_endpoint.sh",
        "scripts/launch_child_from_result.sh",
        "scripts/child_progress_snapshot.sh",
        "scripts/supervise_child_from_launch.sh",
        "scripts/check_child_runtime_status.sh",
        "scripts/recover_orphaned_active.sh",
        "scripts/publish_external_from_result.sh",
        "scripts/run_evaluator_node_until_terminal.sh",
        "tests/check_loop_system_repo_first_child_bootstrap.py",
        "tests/check_loop_system_repo_deferred_activation.py",
        "tests/check_loop_system_repo_deferred_merge_authority.py",
        "tests/check_loop_system_repo_real_split_formalization_minimal.py",
        "tests/check_loop_system_repo_clarification_session_root_helper.py",
        "tests/check_loop_system_repo_child_launch_helper.py",
        "tests/check_loop_system_repo_child_runtime_status_helper.py",
        "tests/check_loop_system_repo_child_progress_snapshot.py",
        "tests/check_loop_system_repo_orphaned_active_recovery_helper.py",
        "tests/check_loop_system_repo_publication_helper.py",
        "tests/check_loop_system_repo_launch_policy.py",
    ]
    try:
        for rel in required_paths:
            _require_path(rel)
    except FileNotFoundError as exc:
        return _fail(f"missing required path: {exc}")

    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    for needle in (".venv/", ".uv-cache/"):
        if needle not in gitignore:
            return _fail(f".gitignore must ignore {needle}")

    agents_text = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    if len(agents_text.splitlines()) > 40:
        return _fail("AGENTS.md must stay compact enough for live kernel instruction loading")
    for needle in (
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
        "exact code, file, or asset locations",
        "tooling",
        "working directories",
        "placeholder-versus-final implementation variants",
        "local asset discovery",
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
        "do not route through generic `loop-mainline`, LeanAtlas mainline docs, or other extra root-level workflow guides",
        "committed endpoint-driven lifecycle bootstrap surface",
        "scripts/bootstrap_first_implementer_from_endpoint.sh",
        "scripts/launch_child_from_result.sh",
        "scripts/child_progress_snapshot.sh",
        "scripts/supervise_child_from_launch.sh",
        "scripts/check_child_runtime_status.sh",
        "repo's locked `uv` environment",
        "committed orphaned-active recovery helper",
        "scripts/recover_orphaned_active.sh",
        "project folder under `workspace/`",
        "authoritative state already points at that exact workspace folder or node ref",
        "Do not pre-create a guessed project folder before the committed endpoint bootstrap wrapper returns the authoritative `workspace_root`.",
        "current working directory",
        "`workspace/AGENTS.md` plus optional `workspace/AGENTS.override.md`",
        "must not directly implement and must first materialize exactly one LOOP node",
        "committed child launch helper",
        "must not translate launch_spec into a fresh ad hoc shell command",
        "child progress snapshot helper",
        "do not end the root conversation",
        "do not stop supervision",
        "do not force heartbeat-style progress chatter",
        "document refs observed from committed launch/evaluator logs",
        "Do not scan old workspace folders, node reports, or evaluator artifacts to guess whether reuse applies.",
        "Do not inspect existing external-target files, local asset files, or preflight external publish-target writability during first-child bootstrap",
        "must not hand-launch a bare `codex exec` if that would drop the node's launch policy",
        "liveness, status, or runtime-recovery supervision only",
        "must not run its own acceptance, testing, inspection, or evaluator-like checking",
        "terminal evaluator-backed result",
        "If the task is still unfinished and a child node or evaluator lane hits a strange interruption",
        "treat that as recovery work rather than final reporting",
        "actively attempt recovery",
        "Stop and report only when the interruption has become an explicit non-recoverable blocker outside the kernel's authority or when the task is already complete.",
        "If authoritative state still says a child node is ACTIVE but the backing runtime or session is gone",
        "classify that as orphaned-active recovery",
        "quiet child with a still-live PID is supervision work, not recovery",
        "same-node orphaned-active retry",
        "pass direct structured flags to `scripts/recover_orphaned_active.sh`",
        "hand-writing a recovery request JSON file",
        "do not stall by rereading old workspace artifacts or recovery docs instead of choosing retry or relaunch from durable evidence.",
        "workspace mirror artifact",
        "root kernel owns the final publication step",
        "committed external publication helper",
        "scripts/publish_external_from_result.sh",
        "must use the committed publication helper instead of ad hoc `cp`, `mv`, or Finder automation",
        "`RECOVERY_REQUIRED`",
        "not yet a terminal evaluator-backed result",
        "Publication may happen only after a terminal evaluator-backed result",
        "scripts/supervise_child_from_launch.sh",
        "retryable terminal evaluator `FAIL`",
        "must not require evaluator PASS before mechanically publishing the exact workspace mirror artifact",
        "must preserve the evaluator verdict honestly",
        "EvaluatorRunState.json",
        "frozen checker graph",
        "bootstrap-first-implementer-from-endpoint",
        "reading request schemas in chat",
        "probing `--help`",
        "using bare `python -m`",
        "`retryable=true` or `status = RECOVERY_REQUIRED`",
    ):
        if needle not in agents_text:
            return _fail(f"AGENTS.md must mention {needle!r}")

    workspace_agents_text = (ROOT / "workspace" / "AGENTS.md").read_text(encoding="utf-8")
    if len(workspace_agents_text.splitlines()) > 30:
        return _fail("workspace/AGENTS.md must stay compact enough for implementer instruction loading")
    for needle in (
        "# LOOP Product Repo Workspace Rules",
        "## Implementer Mode",
        "implementer LOOP node",
        "not the root kernel",
        "already frozen by the kernel",
        "do not restart clarification",
        "workspace_root",
        "current working directory",
        "inspect repo-local or user-local paths directly",
        "user-local input discovery",
        "filename/path discovery helper",
        "sibling project folders",
        ".agents/skills/loop-runner/SKILL.md",
        ".agents/skills/evaluator-exec/SKILL.md",
        "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md",
        "implement, run evaluator, repair, and report the real evaluator result",
        "run_evaluator_node_until_terminal(...)",
        "retryable evaluator-owned `RECOVERY_REQUIRED` result is not terminal node closeout",
        "external publish target outside the current project folder",
        "workspace mirror artifact",
        "run evaluator against that workspace mirror artifact",
        "publish-ready artifact refs",
        "even when the terminal evaluator result is blocked or bug-owned",
        "Do not end the child conversation",
        "RECOVERY_CONTEXT.md",
        "leave durable checkpoint artifacts and explicit recovery evidence in the project folder before stopping on a nonterminal interruption",
        "Do not mine sibling `workspace/` task folders or ad hoc historical deliverables as implementation context",
        "self-attribution",
        "self-repair",
        "EvaluatorRunState.json",
        "frozen checker graph",
    ):
        if needle not in workspace_agents_text:
            return _fail(f"workspace/AGENTS.md must mention {needle!r}")

    if (ROOT / "workspace" / "AGENTS.override.md").read_text(encoding="utf-8").strip():
        return _fail("workspace/AGENTS.override.md must stay empty for now")

    print("[loop-system-skeleton] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
