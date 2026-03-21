"""Deterministic bootstrap helpers for the first implementer node."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from loop_product.dispatch.child_dispatch import materialize_child
from loop_product.dispatch.launch_policy import build_codex_cli_child_launch
from loop_product.kernel.authority import KernelMutationAuthority
from loop_product.kernel.policy import (
    implementer_budget_profile,
    implementer_execution_policy,
    implementer_reasoning_profile,
    kernel_execution_policy,
    kernel_reasoning_profile,
)
from loop_product.kernel.state import KernelState, ensure_runtime_tree, load_kernel_state, persist_kernel_state
from loop_product.protocols.node import NodeSpec, NodeStatus
from loop_product.protocols.schema import validate_repo_object
from loop_product.runtime_paths import (
    implementer_workspace_root,
    product_repo_root,
    require_runtime_root,
    safe_runtime_name,
    state_scope_root,
)


def _nonempty(value: Any) -> str:
    return str(value or "").strip()


_ABSOLUTE_PATH_RE = re.compile(r"/[A-Za-z0-9._~%+=:@,/-]+")
_RELATIVE_PATH_RE = re.compile(r"(?:\.\./|\./)?[A-Za-z0-9._-]+(?:/[A-Za-z0-9._~%+=:@,-]+)+")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _write_executable_text(path: Path, text: str) -> None:
    _write_text(path, text)
    path.chmod(0o755)


def _dedupe_refs(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in items:
        value = _nonempty(raw)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _existing_absolute_path_refs(texts: list[str]) -> list[str]:
    refs: list[str] = []
    for text in texts:
        normalized = _nonempty(text)
        if not normalized:
            continue
        for match in _ABSOLUTE_PATH_RE.finditer(normalized):
            candidate = match.group(0).rstrip(".,;:)]}")
            path = Path(candidate).expanduser()
            if path.exists():
                refs.append(str(path.resolve()))
    return _dedupe_refs(refs)


def _existing_relative_path_refs(*, texts: list[str], search_roots: list[Path]) -> list[str]:
    refs: list[str] = []
    for text in texts:
        normalized = _nonempty(text)
        if not normalized:
            continue
        for match in _RELATIVE_PATH_RE.finditer(normalized):
            candidate = match.group(0).rstrip(".,;:)]}")
            if "://" in candidate:
                continue
            relative_path = Path(candidate)
            if relative_path.is_absolute():
                continue
            for root in search_roots:
                resolved = (root / relative_path).resolve()
                if resolved.exists():
                    refs.append(str(resolved))
                    break
    return _dedupe_refs(refs)


def _derive_endpoint_context_refs(
    *,
    artifact_path: Path,
    artifact_payload: dict[str, Any],
    explicit_context_refs: list[str],
) -> list[str]:
    requirement_artifact = dict(artifact_payload.get("requirement_artifact") or {})
    textual_context: list[str] = []
    textual_context.extend(str(item or "") for item in list(requirement_artifact.get("relevant_context") or []))
    textual_context.extend(str(item.get("text") or "") for item in list(artifact_payload.get("confirmed_requirements") or []))
    textual_context.append(str(artifact_payload.get("original_user_prompt") or ""))
    path_refs = _existing_absolute_path_refs(textual_context)
    repo_root = product_repo_root().resolve()
    relative_refs = _existing_relative_path_refs(
        texts=textual_context,
        search_roots=[repo_root, *repo_root.parents],
    )
    return _dedupe_refs([str(artifact_path.resolve()), *list(explicit_context_refs), *path_refs, *relative_refs])


def _path_missing_or_empty(path: Path) -> bool:
    if not path.exists():
        return True
    if path.is_dir():
        try:
            next(path.iterdir())
        except StopIteration:
            return True
        return False
    return False


def _allocate_fresh_name(task_slug: str) -> str:
    base_name = safe_runtime_name(task_slug)
    workspace_base = implementer_workspace_root().resolve()
    state_base = state_scope_root().resolve()
    candidate = base_name
    suffix = 2
    while (workspace_base / candidate).exists() or (state_base / candidate).exists():
        candidate = f"{base_name}__{suffix}"
        suffix += 1
    return candidate


def _resolve_fresh_roots(*, task_slug: str, workspace_root: str | Path | None, state_root: str | Path | None) -> tuple[Path, Path, str]:
    base_workspace = implementer_workspace_root().resolve()
    base_state = state_scope_root().resolve()
    if workspace_root in (None, "") and state_root in (None, ""):
        name = _allocate_fresh_name(task_slug)
        return (base_workspace / name).resolve(), (base_state / name).resolve(), name

    resolved_workspace = Path(workspace_root).expanduser().resolve() if workspace_root not in (None, "") else None
    resolved_state = require_runtime_root(Path(state_root).expanduser().resolve()) if state_root not in (None, "") else None

    if resolved_workspace is None and resolved_state is not None:
        resolved_workspace = (base_workspace / safe_runtime_name(resolved_state.name)).resolve()
    if resolved_state is None and resolved_workspace is not None:
        resolved_state = require_runtime_root((base_state / safe_runtime_name(resolved_workspace.name)).resolve())
    assert resolved_workspace is not None
    assert resolved_state is not None
    if base_workspace != resolved_workspace and base_workspace not in resolved_workspace.parents:
        raise ValueError(f"fresh bootstrap workspace_root must stay under {base_workspace}: {resolved_workspace}")
    if _path_missing_or_empty(resolved_workspace) and _path_missing_or_empty(resolved_state):
        return resolved_workspace, resolved_state, safe_runtime_name(resolved_workspace.name or task_slug)
    raise ValueError("fresh bootstrap requested an already-populated workspace_root or state_root; choose a fresh path or use continue_exact")


def _ensure_kernel_state(*, state_root: Path, task_slug: str, root_goal: str, authority: KernelMutationAuthority) -> KernelState:
    ensure_runtime_tree(state_root)
    kernel_state_path = state_root / "state" / "kernel_state.json"
    if kernel_state_path.exists():
        return load_kernel_state(state_root)

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice=root_goal,
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy=kernel_execution_policy(),
        reasoning_profile=kernel_reasoning_profile(),
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/runtime_bootstrap_summary.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    kernel_state = KernelState(
        task_id=task_slug,
        root_goal=root_goal,
        root_node_id=root_node.node_id,
    )
    kernel_state.register_node(root_node)
    persist_kernel_state(state_root, kernel_state, authority=authority)
    return kernel_state


def _build_handoff_payload(
    *,
    node: NodeSpec,
    state_root: Path,
    endpoint_artifact_ref: str,
    root_goal: str,
    child_goal_slice: str,
    workspace_root: Path,
    workspace_mirror_relpath: str,
    workspace_live_artifact_relpath: str,
    external_publish_target: str,
    required_output_paths: list[str],
    context_refs: list[str],
    result_sink_ref: str,
    workspace_result_sink_relpath: str,
    workspace_result_sink_ref: str,
    kernel_result_sink_ref: str,
    artifact_publication_receipt_ref: str,
    artifact_publication_runner_ref: str,
    evaluator_submission_ref: str,
    evaluator_runner_ref: str,
) -> dict[str, Any]:
    workspace_mirror_relpath = _nonempty(workspace_mirror_relpath)
    if not workspace_mirror_relpath:
        raise ValueError("workspace_mirror_relpath must be non-empty")
    workspace_mirror_ref = str((workspace_root / workspace_mirror_relpath).resolve())
    workspace_live_artifact_relpath = _nonempty(workspace_live_artifact_relpath)
    if not workspace_live_artifact_relpath:
        raise ValueError("workspace_live_artifact_relpath must be non-empty")
    workspace_live_artifact_ref = str((workspace_root / workspace_live_artifact_relpath).resolve())
    return {
        "node_id": node.node_id,
        "parent_node_id": node.parent_node_id,
        "round_id": node.round_id,
        "lineage_ref": node.lineage_ref,
        "workspace_root": str(workspace_root.resolve()),
        "state_root": str(state_root.resolve()),
        "endpoint_artifact_ref": endpoint_artifact_ref,
        "root_goal": root_goal,
        "child_goal_slice": child_goal_slice,
        "workspace_mirror_relpath": workspace_mirror_relpath,
        "workspace_mirror_ref": workspace_mirror_ref,
        "workspace_live_artifact_relpath": workspace_live_artifact_relpath,
        "workspace_live_artifact_ref": workspace_live_artifact_ref,
        "external_publish_target": external_publish_target,
        "required_output_paths": [str(item).strip() for item in list(required_output_paths or []) if str(item).strip()],
        "context_refs": list(context_refs),
        "result_sink_ref": result_sink_ref,
        "workspace_result_sink_relpath": workspace_result_sink_relpath,
        "workspace_result_sink_ref": workspace_result_sink_ref,
        "kernel_result_sink_ref": kernel_result_sink_ref,
        "artifact_publication_receipt_ref": artifact_publication_receipt_ref,
        "artifact_publication_runner_ref": artifact_publication_runner_ref,
        "evaluator_submission_ref": evaluator_submission_ref,
        "evaluator_runner_ref": evaluator_runner_ref,
        "external_publication_owner": "root-kernel",
        "external_input_policy": "read_only_external_inputs_allowed_when_required_by_frozen_endpoint",
    }


def _render_handoff_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Frozen Handoff",
        "",
        f"- node_id: `{payload['node_id']}`",
        f"- parent_node_id: `{payload['parent_node_id']}`",
        f"- round_id: `{payload['round_id']}`",
        f"- lineage_ref: `{payload['lineage_ref']}`",
        f"- workspace_root: `{payload['workspace_root']}`",
        f"- state_root: `{payload['state_root']}`",
        f"- endpoint_artifact_ref: `{payload['endpoint_artifact_ref']}`",
        "",
        "## Frozen Goal",
        "",
        payload["child_goal_slice"],
        "",
        "## Artifact Paths",
        "",
        f"- workspace_mirror_relpath: `{payload['workspace_mirror_relpath']}`",
        f"- workspace_mirror_ref: `{payload['workspace_mirror_ref']}`",
        f"- workspace_live_artifact_relpath: `{payload['workspace_live_artifact_relpath']}`",
        f"- workspace_live_artifact_ref: `{payload['workspace_live_artifact_ref']}`",
        f"- external_publish_target: `{payload['external_publish_target']}`",
        f"- result_sink_ref: `{payload['result_sink_ref']}`",
        f"- workspace_result_sink_relpath: `{payload['workspace_result_sink_relpath']}`",
        f"- workspace_result_sink_ref: `{payload['workspace_result_sink_ref']}`",
        f"- kernel_result_sink_ref: `{payload['kernel_result_sink_ref']}`",
        f"- artifact_publication_receipt_ref: `{payload['artifact_publication_receipt_ref']}`",
        f"- artifact_publication_runner_ref: `{payload['artifact_publication_runner_ref']}`",
        f"- evaluator_submission_ref: `{payload['evaluator_submission_ref']}`",
        f"- evaluator_runner_ref: `{payload['evaluator_runner_ref']}`",
        f"- external_publication_owner: `{payload['external_publication_owner']}`",
        f"- external_input_policy: `{payload['external_input_policy']}`",
    ]
    context_refs = [str(item) for item in payload.get("context_refs") or []]
    required_output_paths = [str(item) for item in payload.get("required_output_paths") or [] if str(item).strip()]
    if required_output_paths:
        lines.extend(["", "## Required Outputs", ""])
        lines.extend(f"- `{item}`" for item in required_output_paths)
    if context_refs:
        lines.extend(["", "## Context Refs", ""])
        lines.extend(f"- `{item}`" for item in context_refs)
    return "\n".join(lines)


def _render_child_prompt(
    *,
    workspace_root: Path,
    handoff_md_path: Path,
    node: NodeSpec,
    workspace_live_artifact_abs: Path,
    artifact_publication_receipt_abs: Path,
    artifact_publication_runner_abs: Path,
    workspace_result_sink_abs: Path,
    kernel_result_sink_abs: Path,
    evaluator_submission_abs: Path,
    evaluator_runner_abs: Path,
) -> str:
    workspace_agents = (workspace_root.parent / "AGENTS.md").resolve()
    repo_root = product_repo_root().resolve()
    loop_runner_skill = (repo_root / ".agents" / "skills" / "loop-runner" / "SKILL.md").resolve()
    evaluator_exec_skill = (repo_root / ".agents" / "skills" / "evaluator-exec" / "SKILL.md").resolve()
    evaluator_manual = (repo_root / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md").resolve()
    local_input_helper = (repo_root / "scripts" / "find_local_input_candidates.sh").resolve()
    split_submit_helper = (repo_root / "scripts" / "submit_split_request_from_handoff.sh").resolve()
    activate_submit_helper = (repo_root / "scripts" / "submit_activate_request_from_handoff.sh").resolve()
    return "\n".join(
        [
            "Read and follow:",
            "",
            f"- `{workspace_agents}`",
            f"- `{handoff_md_path.resolve()}`",
            "",
            "You are not the root kernel.",
            f"You are the implementer LOOP node `{node.node_id}`.",
            "",
            "Keep durable implementation writes inside the current workspace root, except for the exact authoritative kernel-visible result sink frozen in the handoff.",
            "Build in the live artifact root first and treat the workspace mirror as publish-only.",
            "Publish through the exact publication runner before evaluator or terminal report.",
            "A child-authored WHOLE_PAPER_STATUS.json or branch README is not a publication receipt.",
            "Do not inspect sibling workspace task folders or historical deliverables as templates, evidence, or reuse context unless the frozen handoff explicitly names that exact reuse target.",
            "When this prompt names a committed repo-shipped wrapper or helper, call it directly before reading its source or tests unless the direct path fails.",
            "Treat exact frozen refs in the handoff/prompt as authoritative and do not guess alternate repo-root or lookalike paths before using them.",
            "If this is a fresh workspace with no deliverable or artifact yet, take one concrete workspace-local action before broadening into extra reconnaissance.",
            "That startup action must materialize at least one non-empty file under the workspace mirror or required artifact/result path.",
            "Creating only an empty directory does not satisfy that startup requirement.",
            "If the frozen endpoint already determines the artifact shape, a startup note or checkpoint file alone does not count as substantive startup progress.",
            "Materialize the actual artifact skeleton or first substantive deliverable batch before broad theorem search or helper archaeology.",
            "If the frozen goal already names staged benchmark phases or required sections, do not stop after creating a placeholder skeleton for those sections.",
            "After the skeleton exists, materially advance the first incomplete staged phase with source-backed content before broadening into later phases or open-ended reconnaissance.",
            "planned outputs, `pending` tables, TODO notes, or placeholder headings alone do not count as substantive staged progress.",
            "Do not spend the opening phase on broad repo scans when the exact frozen evaluator runner, evaluator submission, baseline refs, or helper refs are already named.",
            "Do not broad-search repo roots, `.loop/**` history, or unrelated evaluator workspaces for helper or template discovery when the exact frozen refs already name the required helper or baseline artifacts.",
            "If you create or ship a fresh workspace-local Lean package, use any committed shared-cache helper named in the frozen handoff context refs before the first `lake build`.",
            "Run Lean/package tooling only inside the exact live artifact root named below; do not hydrate package trees or build outputs inside the publish root.",
            "Do not treat live `git clone`, `lake update`, or `lake exe cache get` as the normal first path for artifact-local `.lake/packages`; that path is fallback repair and must be reported as an environment defect.",
            "Before evaluator or final report, the final `deliverables/primary_artifact` must not ship runtime-owned heavy trees such as `.lake`, `.git`, `.venv`, `.uv-cache`, `build`, or `_lake_build`.",
            "If local build support rematerializes those trees inside the publish root, treat that as a runtime defect and republish from the live root instead of hand-waving it away.",
            "If a startup search or MCP helper reports rate-limit, tool exhaustion, or validation failure, do not keep retrying the same search family in the opening phase.",
            "Instead downgrade to direct artifact writing, local proof drafting, or the first build path supported by the frozen refs already in hand.",
            "If the frozen endpoint needs repo-local or user-local inputs that are not already materialized in the workspace, inspect the necessary repo-local or user-local paths directly and materialize the required copies into the workspace when useful.",
            "For non-trivial content transforms, prefer committed helpers or Python over brittle shell quoting or in-place one-liners on the primary deliverable.",
            "Build primary deliverable updates through a temp path and atomic replace instead of clobbering the live output in place.",
            "Do not publish directly to the external publish target from this node unless the frozen handoff explicitly makes publication implementer-owned; the normal owner is root-kernel.",
            "If bounded progress reveals a meaningful parallelizable gap, surface a split request upward to the root kernel with the proposed child slices and why the current node should no longer own all remaining work alone.",
            "Do not directly materialize child nodes yourself or mutate topology fact from implementer context; split remains kernel-owned until an explicit acceptance decision exists.",
            "If your context refs include a parent `FROZEN_HANDOFF.json` or `FROZEN_HANDOFF.md`, treat that inherited frozen handoff as authoritative whole-task context in addition to your narrowed branch goal slice.",
            "If you decide split is warranted, materialize a structured split proposal and call the exact split helper named in this prompt instead of only writing the recommendation into deliverable prose such as `PARTITION_PLAN.md` or `TRACEABILITY.md`.",
            "If kernel accepts a deferred split, do not assume the planned children will start automatically.",
            "When a deferred child is genuinely ready, materialize a structured activate proposal and call the exact activate helper named in this prompt instead of treating `PLANNED` child state as self-starting.",
            "A text-only split recommendation is not a submitted kernel proposal.",
            "Do not start evaluator for a staged whole-paper benchmark until the artifact carries structured terminal-classification evidence in `WHOLE_PAPER_STATUS.json`.",
            "If your split slice carries declared required outputs, do not start evaluator or mark the slice terminal until those required outputs exist in the artifact or the frozen slice contract explicitly justifies every missing required output under the same allowed blocked classification.",
            "Extraction inventories, partition plans, intermediate block ledgers, or prose stating that whole-paper closure is still pending remain non-terminal evidence and are not evaluator-ready whole-paper closeout.",
            "For the repo-root reads required by `workspace/AGENTS.md`, use these exact refs instead of guessing relative paths from the project folder:",
            "",
            f"- `{loop_runner_skill}`",
            f"- `{evaluator_exec_skill}`",
            f"- `{evaluator_manual}`",
            f"- `{split_submit_helper}`",
            f"- `{activate_submit_helper}`",
            "",
            "Exact live/publish refs for this node:",
            "",
            f"- live artifact root: `{workspace_live_artifact_abs.resolve()}`",
            f"- publish runner: `{artifact_publication_runner_abs.resolve()}`",
            f"- publication receipt ref: `{artifact_publication_receipt_abs.resolve()}`",
            "",
            "If endpoint-required local file discovery is needed, prefer the committed filename/path helper with targeted --root/--term/--ext flags over ad hoc content grep or one broad prose query:",
            "",
            f"- `{local_input_helper}`",
            "If the handoff requires local/offline user assets, search canonical local roots first (Desktop, Music, Downloads) and do not browse, download, or substitute remote assets unless the handoff explicitly allows remote sourcing.",
            "Run the real evaluator path and write the required structured result to the authoritative kernel-visible sink:",
            "",
            f"- exact evaluator submission: `{evaluator_submission_abs.resolve()}`",
            f"- exact evaluator runner: `{evaluator_runner_abs.resolve()}`",
            f"- `{kernel_result_sink_abs.resolve()}`",
            "",
            "If useful for local debugging or mirror evidence, you may also update the workspace-local mirror result sink:",
            "",
            f"- `{workspace_result_sink_abs.resolve()}`",
            "",
            "In your final implementer report, report whether split was proposed or accepted so root closeout can describe the split path truthfully.",
        ]
    )


def _materialize_evaluator_bundle(
    *,
    state_root: Path,
    workspace_root: Path,
    node: NodeSpec,
    endpoint_artifact_ref: str,
    artifact_payload: dict[str, Any],
    workspace_mirror_relpath: str,
    artifact_publication_receipt_ref: Path,
    external_publish_target: str,
    required_output_paths: list[str],
    handoff_json_path: Path,
    context_refs: list[str],
) -> tuple[Path, Path]:
    from loop_product.loop import build_evaluator_submission_for_frozen_task

    bootstrap_dir = state_root / "artifacts" / "bootstrap"
    submission_path = bootstrap_dir / "EvaluatorNodeSubmission.json"
    runner_path = workspace_root / "RUN_EVALUATOR_NODE_UNTIL_TERMINAL.sh"
    manual_path = bootstrap_dir / "EvaluatorProductManual.md"
    final_effects_path = bootstrap_dir / "EvaluatorFinalEffects.md"
    output_root = (state_root / "artifacts" / "evaluator_runs" / node.node_id).resolve()
    workspace_mirror_ref = str((workspace_root / workspace_mirror_relpath).resolve())
    _write_text(
        manual_path,
        _render_task_scoped_evaluator_manual(
            endpoint_artifact_ref=endpoint_artifact_ref,
            workspace_root=workspace_root,
            workspace_mirror_ref=workspace_mirror_ref,
            external_publish_target=external_publish_target,
            handoff_json_path=handoff_json_path,
        ),
    )
    _write_text(
        final_effects_path,
        _render_task_scoped_evaluator_final_effects_text(
            artifact_payload=artifact_payload,
            workspace_mirror_ref=workspace_mirror_ref,
        ),
    )
    submission = build_evaluator_submission_for_frozen_task(
        target_node=node,
        workspace_root=workspace_root,
        output_root=output_root,
        implementation_package_ref=workspace_mirror_ref,
        artifact_publication_receipt_ref=artifact_publication_receipt_ref,
        product_manual_ref=manual_path,
        required_output_paths=required_output_paths,
        final_effects_text_ref=final_effects_path,
        context_refs=[*list(context_refs), endpoint_artifact_ref, str(handoff_json_path.resolve())],
    )
    _write_json(submission_path, submission.to_dict())
    repo_runner = (product_repo_root().resolve() / "scripts" / "run_evaluator_node_until_terminal.sh").resolve()
    _write_executable_text(
        runner_path,
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f'exec "{repo_runner}" --state-root "{state_root.resolve()}" --submission-ref "{submission_path.resolve()}" "$@"',
            ]
        ),
    )
    return submission_path, runner_path


def _materialize_publication_runner(*, workspace_root: Path, handoff_json_path: Path) -> Path:
    repo_runner = (product_repo_root().resolve() / "scripts" / "publish_workspace_artifact_from_handoff.sh").resolve()
    runner_path = workspace_root / "PUBLISH_WORKSPACE_ARTIFACT.sh"
    _write_executable_text(
        runner_path,
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f'exec "{repo_runner}" --handoff-ref "{handoff_json_path.resolve()}" "$@"',
            ]
        ),
    )
    return runner_path


def _persist_bootstrap_result(*, state_root: Path, request_payload: dict[str, Any], result_payload: dict[str, Any]) -> Path:
    bootstrap_dir = state_root / "artifacts" / "bootstrap"
    request_path = bootstrap_dir / "FirstImplementerBootstrapRequest.json"
    result_path = bootstrap_dir / "FirstImplementerBootstrapResult.json"
    _write_json(request_path, request_payload)
    _write_json(result_path, result_payload)
    return result_path


def _fresh_result_payload(
    *,
    mode: str,
    reuse_decision: str,
    node: NodeSpec,
    state_root: Path,
    workspace_root: Path,
    handoff_json_path: Path,
    handoff_md_path: Path,
    child_prompt_path: Path,
    launch_spec: dict[str, Any],
    workspace_live_artifact_ref: Path,
    artifact_publication_receipt_ref: Path,
    artifact_publication_runner_ref: Path,
    workspace_result_sink_ref: Path,
    kernel_result_sink_ref: Path,
    evaluator_submission_ref: Path,
    evaluator_runner_ref: Path,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "reuse_decision": reuse_decision,
        "node_id": node.node_id,
        "workspace_root": str(workspace_root.resolve()),
        "state_root": str(state_root.resolve()),
        "node_ref": str((state_root / "state" / f"{node.node_id}.json").resolve()),
        "delegation_ref": str((state_root / "state" / "delegations" / f"{node.node_id}.json").resolve()),
        "handoff_json_ref": str(handoff_json_path.resolve()),
        "handoff_md_ref": str(handoff_md_path.resolve()),
        "child_prompt_ref": str(child_prompt_path.resolve()),
        "workspace_live_artifact_ref": str(workspace_live_artifact_ref.resolve()),
        "artifact_publication_receipt_ref": str(artifact_publication_receipt_ref.resolve()),
        "artifact_publication_runner_ref": str(artifact_publication_runner_ref.resolve()),
        "workspace_result_sink_ref": str(workspace_result_sink_ref.resolve()),
        "kernel_result_sink_ref": str(kernel_result_sink_ref.resolve()),
        "evaluator_submission_ref": str(evaluator_submission_ref.resolve()),
        "evaluator_runner_ref": str(evaluator_runner_ref.resolve()),
        "launch_spec": launch_spec,
    }


def _normalize_bootstrap_request(payload: dict[str, Any]) -> dict[str, Any]:
    workspace_mirror_relpath = _nonempty(payload.get("workspace_mirror_relpath"))
    normalized = {
        "mode": _nonempty(payload.get("mode") or "fresh") or "fresh",
        "task_slug": _nonempty(payload.get("task_slug")),
        "root_goal": _nonempty(payload.get("root_goal")),
        "child_goal_slice": _nonempty(payload.get("child_goal_slice")),
        "endpoint_artifact_ref": _nonempty(payload.get("endpoint_artifact_ref")),
        "workspace_root": _nonempty(payload.get("workspace_root")),
        "state_root": _nonempty(payload.get("state_root")),
        "node_id": _nonempty(payload.get("node_id")),
        "round_id": _nonempty(payload.get("round_id") or "R1") or "R1",
        "workspace_mirror_relpath": workspace_mirror_relpath,
        "workspace_live_artifact_relpath": _nonempty(payload.get("workspace_live_artifact_relpath"))
        or _default_workspace_live_artifact_relpath(workspace_mirror_relpath),
        "external_publish_target": _nonempty(payload.get("external_publish_target")),
        "context_refs": [str(item) for item in (payload.get("context_refs") or [])],
        "required_output_paths": [str(item).strip() for item in (payload.get("required_output_paths") or []) if str(item).strip()],
        "result_sink_ref": _nonempty(payload.get("result_sink_ref")),
    }
    validate_repo_object("LoopFirstImplementerBootstrapRequest.schema.json", normalized)
    return normalized


def _load_endpoint_artifact_payload(endpoint_artifact_ref: str) -> tuple[Path, dict[str, Any]]:
    artifact_path = Path(endpoint_artifact_ref).expanduser().resolve()
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    validate_repo_object("LoopEndpointArtifact.schema.json", payload)
    status = _nonempty(payload.get("status"))
    if status not in {"CLARIFIED", "BYPASSED"}:
        raise ValueError(
            f"bootstrap_first_implementer_from_endpoint requires a clarified or bypassed endpoint artifact, got {status!r}"
        )
    return artifact_path, payload


def _render_goal_from_endpoint_artifact(artifact_payload: dict[str, Any]) -> tuple[str, str]:
    requirement_artifact = dict(artifact_payload.get("requirement_artifact") or {})
    final_effect = _nonempty(requirement_artifact.get("final_effect"))
    user_summary = _nonempty(requirement_artifact.get("user_request_summary"))
    root_goal = final_effect or user_summary or _nonempty(artifact_payload.get("original_user_prompt"))
    if not root_goal:
        raise ValueError("endpoint artifact must include a non-empty final_effect, user_request_summary, or original_user_prompt")

    lines = [root_goal]
    observable_success_criteria = [
        _nonempty(item) for item in list(requirement_artifact.get("observable_success_criteria") or []) if _nonempty(item)
    ]
    hard_constraints = [_nonempty(item) for item in list(requirement_artifact.get("hard_constraints") or []) if _nonempty(item)]
    non_goals = [_nonempty(item) for item in list(requirement_artifact.get("non_goals") or []) if _nonempty(item)]
    if observable_success_criteria:
        lines.extend(["", "Observable success criteria:"])
        lines.extend(f"- {item}" for item in observable_success_criteria)
    if hard_constraints:
        lines.extend(["", "Hard constraints:"])
        lines.extend(f"- {item}" for item in hard_constraints)
    if non_goals:
        lines.extend(["", "Non-goals:"])
        lines.extend(f"- {item}" for item in non_goals)
    return root_goal, "\n".join(lines)


def _default_task_slug(*, artifact_payload: dict[str, Any], artifact_path: Path, external_publish_target: str) -> str:
    session_root = _nonempty(artifact_payload.get("session_root"))
    if session_root:
        return safe_runtime_name(Path(session_root).name)
    publish_name = Path(external_publish_target).name if external_publish_target else ""
    if publish_name:
        return safe_runtime_name(Path(publish_name).stem or publish_name)
    return safe_runtime_name(artifact_path.parent.name or artifact_path.stem)


def _default_workspace_mirror_relpath(external_publish_target: str) -> str:
    publish_name = Path(external_publish_target).name if external_publish_target else ""
    if publish_name:
        return f"deliverables/{publish_name}"
    return "deliverables/primary_artifact"


def _default_workspace_live_artifact_relpath(workspace_mirror_relpath: str) -> str:
    publish_path = Path(workspace_mirror_relpath)
    if publish_path.suffix:
        return str(Path(".tmp_primary_artifact") / publish_path.name)
    return ".tmp_primary_artifact"


def _render_task_scoped_evaluator_final_effects_text(*, artifact_payload: dict[str, Any], workspace_mirror_ref: str) -> str:
    requirement_artifact = dict(artifact_payload.get("requirement_artifact") or {})
    final_effect = _nonempty(requirement_artifact.get("final_effect"))
    user_summary = _nonempty(requirement_artifact.get("user_request_summary"))
    root_goal = final_effect or user_summary or _nonempty(artifact_payload.get("original_user_prompt"))
    if not root_goal:
        raise ValueError("endpoint artifact must contain clarified final-effect content for evaluator bootstrap")
    lines = [
        "# Task-Scoped Evaluator Final Effects",
        "",
        f"- The workspace mirror artifact at `{workspace_mirror_ref}` fulfills this frozen endpoint: {root_goal}",
    ]
    for header, items in (
        (
            "Observable Success Criteria",
            [_nonempty(item) for item in list(requirement_artifact.get("observable_success_criteria") or []) if _nonempty(item)],
        ),
        (
            "Hard Constraints",
            [_nonempty(item) for item in list(requirement_artifact.get("hard_constraints") or []) if _nonempty(item)],
        ),
        (
            "Non-Goals",
            [_nonempty(item) for item in list(requirement_artifact.get("non_goals") or []) if _nonempty(item)],
        ),
        (
            "Relevant Context",
            [_nonempty(item) for item in list(requirement_artifact.get("relevant_context") or []) if _nonempty(item)],
        ),
    ):
        if items:
            lines.extend(["", f"## {header}", ""])
            lines.extend(f"- {item}" for item in items)
    return "\n".join(lines)


def _render_task_scoped_evaluator_manual(
    *,
    endpoint_artifact_ref: str,
    workspace_root: Path,
    workspace_mirror_ref: str,
    external_publish_target: str,
    handoff_json_path: Path,
) -> str:
    lines = [
        "# Task-Scoped Evaluator Product Manual",
        "",
        "This evaluator run judges one implementer-owned workspace mirror artifact for one frozen endpoint task.",
        "",
        "## Documented Surface",
        "",
        f"- workspace_root: `{workspace_root.resolve()}`",
        f"- workspace_mirror_ref: `{workspace_mirror_ref}`",
        f"- endpoint_artifact_ref: `{endpoint_artifact_ref}`",
        f"- frozen_handoff_ref: `{handoff_json_path.resolve()}`",
    ]
    if external_publish_target:
        lines.append(f"- external_publish_target: `{external_publish_target}`")
    lines.extend(
        [
            "",
            "## Evaluation Rules",
            "",
            "- Judge the workspace mirror artifact and the reviewer-visible evidence generated from it.",
            "- If the endpoint includes an external publish target outside the workspace, treat that external path as root-kernel publication only; do not require the implementer to prove that publication step.",
            "- You may inspect the workspace mirror artifact directly and any relative or embedded assets it uses inside the same workspace.",
            "- Do not treat sibling workspace folders or unrelated historical deliverables as part of the documented product surface.",
        ]
    )
    return "\n".join(lines)


def bootstrap_first_implementer_node(*, authority: KernelMutationAuthority, **payload: Any) -> dict[str, Any]:
    """Create or exactly continue the first implementer bootstrap bundle."""

    request = _normalize_bootstrap_request(dict(payload))
    mode = request["mode"]

    if mode == "fresh":
        workspace_root, state_root, default_name = _resolve_fresh_roots(
            task_slug=request["task_slug"],
            workspace_root=request["workspace_root"],
            state_root=request["state_root"],
        )
        kernel_state = _ensure_kernel_state(
            state_root=state_root,
            task_slug=request["task_slug"],
            root_goal=request["root_goal"],
            authority=authority,
        )
        node_id = request["node_id"] or default_name
        result_sink_ref = request["result_sink_ref"] or f"artifacts/{node_id}/implementer_result.json"
        child_node = materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id=kernel_state.root_node_id,
            node_id=node_id,
            goal_slice=request["child_goal_slice"],
            round_id=request["round_id"],
            execution_policy=implementer_execution_policy(),
            reasoning_profile=implementer_reasoning_profile(),
            budget_profile=implementer_budget_profile(),
            required_output_paths=request["required_output_paths"],
            workspace_root=workspace_root,
            result_sink_ref=result_sink_ref,
            authority=authority,
        )
    elif mode == "continue_exact":
        if not request["state_root"] or not request["workspace_root"] or not request["node_id"]:
            raise ValueError("continue_exact requires exact state_root, workspace_root, and node_id")
        state_root = require_runtime_root(Path(request["state_root"]).expanduser().resolve())
        workspace_root = Path(request["workspace_root"]).expanduser().resolve()
        node_id = request["node_id"]
        kernel_state = load_kernel_state(state_root)
        node_path = state_root / "state" / f"{node_id}.json"
        if not node_path.exists():
            raise ValueError(f"continue_exact node does not exist: {node_id}")
        child_node = NodeSpec.from_dict(json.loads(node_path.read_text(encoding="utf-8")))
        if Path(str(child_node.workspace_root)).resolve() != workspace_root.resolve():
            raise ValueError("continue_exact workspace_root does not match the persisted node snapshot")
        result_sink_ref = request["result_sink_ref"] or child_node.result_sink_ref
    else:
        raise ValueError(f"unsupported bootstrap mode: {mode!r}")

    handoff_json_path = workspace_root / "FROZEN_HANDOFF.json"
    handoff_md_path = workspace_root / "FROZEN_HANDOFF.md"
    child_prompt_path = workspace_root / "CHILD_PROMPT.md"
    workspace_live_artifact_relpath = request["workspace_live_artifact_relpath"] or _default_workspace_live_artifact_relpath(
        request["workspace_mirror_relpath"]
    )
    request["workspace_live_artifact_relpath"] = workspace_live_artifact_relpath
    workspace_live_artifact_abs = (workspace_root / workspace_live_artifact_relpath).resolve()
    workspace_result_sink_relpath = result_sink_ref
    workspace_result_sink_abs = (workspace_root / workspace_result_sink_relpath).resolve()
    kernel_result_sink_abs = (state_root / result_sink_ref).resolve()
    artifact_publication_receipt_abs = (
        state_root / "artifacts" / "publication" / child_node.node_id / "WorkspaceArtifactPublicationReceipt.json"
    ).resolve()
    endpoint_artifact_payload = json.loads(Path(request["endpoint_artifact_ref"]).read_text(encoding="utf-8"))
    evaluator_submission_path, evaluator_runner_path = _materialize_evaluator_bundle(
        state_root=state_root,
        workspace_root=workspace_root,
        node=child_node,
        endpoint_artifact_ref=request["endpoint_artifact_ref"],
        artifact_payload=endpoint_artifact_payload,
        workspace_mirror_relpath=request["workspace_mirror_relpath"],
        artifact_publication_receipt_ref=artifact_publication_receipt_abs,
        external_publish_target=request["external_publish_target"],
        required_output_paths=request["required_output_paths"],
        handoff_json_path=handoff_json_path,
        context_refs=request["context_refs"],
    )
    artifact_publication_runner_path = _materialize_publication_runner(
        workspace_root=workspace_root,
        handoff_json_path=handoff_json_path,
    )
    handoff_payload = _build_handoff_payload(
        node=child_node,
        state_root=state_root,
        endpoint_artifact_ref=request["endpoint_artifact_ref"],
        root_goal=request["root_goal"],
        child_goal_slice=request["child_goal_slice"],
        workspace_root=workspace_root,
        workspace_mirror_relpath=request["workspace_mirror_relpath"],
        workspace_live_artifact_relpath=workspace_live_artifact_relpath,
        external_publish_target=request["external_publish_target"],
        required_output_paths=request["required_output_paths"],
        context_refs=request["context_refs"],
        result_sink_ref=result_sink_ref,
        workspace_result_sink_relpath=workspace_result_sink_relpath,
        workspace_result_sink_ref=str(workspace_result_sink_abs),
        kernel_result_sink_ref=str(kernel_result_sink_abs),
        artifact_publication_receipt_ref=str(artifact_publication_receipt_abs),
        artifact_publication_runner_ref=str(artifact_publication_runner_path.resolve()),
        evaluator_submission_ref=str(evaluator_submission_path.resolve()),
        evaluator_runner_ref=str(evaluator_runner_path.resolve()),
    )
    _write_json(handoff_json_path, handoff_payload)
    _write_text(handoff_md_path, _render_handoff_md(handoff_payload))
    _write_text(
        child_prompt_path,
        _render_child_prompt(
            workspace_root=workspace_root,
            handoff_md_path=handoff_md_path,
            node=child_node,
            workspace_live_artifact_abs=workspace_live_artifact_abs,
            artifact_publication_receipt_abs=artifact_publication_receipt_abs,
            artifact_publication_runner_abs=artifact_publication_runner_path,
            workspace_result_sink_abs=workspace_result_sink_abs,
            kernel_result_sink_abs=kernel_result_sink_abs,
            evaluator_submission_abs=evaluator_submission_path,
            evaluator_runner_abs=evaluator_runner_path,
        ),
    )

    launch_spec = build_codex_cli_child_launch(
        workspace_root=workspace_root,
        sandbox_mode=str(child_node.execution_policy.get("sandbox_mode") or "danger-full-access"),
        thinking_budget=str(child_node.reasoning_profile.get("thinking_budget") or "medium"),
        prompt_path=child_prompt_path,
    )
    result_payload = _fresh_result_payload(
        mode=mode,
        reuse_decision="fresh" if mode == "fresh" else "continue_exact",
        node=child_node,
        state_root=state_root,
        workspace_root=workspace_root,
        handoff_json_path=handoff_json_path,
        handoff_md_path=handoff_md_path,
        child_prompt_path=child_prompt_path,
        launch_spec=launch_spec,
        workspace_live_artifact_ref=workspace_live_artifact_abs,
        artifact_publication_receipt_ref=artifact_publication_receipt_abs,
        artifact_publication_runner_ref=artifact_publication_runner_path,
        workspace_result_sink_ref=workspace_result_sink_abs,
        kernel_result_sink_ref=kernel_result_sink_abs,
        evaluator_submission_ref=evaluator_submission_path,
        evaluator_runner_ref=evaluator_runner_path,
    )
    bootstrap_result_ref = _persist_bootstrap_result(
        state_root=state_root,
        request_payload=request,
        result_payload=result_payload,
    )
    result_payload["bootstrap_result_ref"] = str(bootstrap_result_ref.resolve())
    validate_repo_object("LoopFirstImplementerBootstrapResult.schema.json", result_payload)
    _write_json(bootstrap_result_ref, result_payload)
    return result_payload


def bootstrap_first_implementer_from_endpoint(*, authority: KernelMutationAuthority, **payload: Any) -> dict[str, Any]:
    """Create the first implementer node from a clarified endpoint artifact with minimal caller inputs."""

    endpoint_artifact_ref = _nonempty(payload.get("endpoint_artifact_ref"))
    if not endpoint_artifact_ref:
        raise ValueError("bootstrap_first_implementer_from_endpoint requires endpoint_artifact_ref")
    external_publish_target = _nonempty(payload.get("external_publish_target"))
    workspace_mirror_relpath = _nonempty(payload.get("workspace_mirror_relpath"))
    artifact_path, artifact_payload = _load_endpoint_artifact_payload(endpoint_artifact_ref)
    root_goal, child_goal_slice = _render_goal_from_endpoint_artifact(artifact_payload)
    context_refs = _derive_endpoint_context_refs(
        artifact_path=artifact_path,
        artifact_payload=artifact_payload,
        explicit_context_refs=[str(item) for item in (payload.get("context_refs") or [])],
    )
    task_slug = _nonempty(payload.get("task_slug")) or _default_task_slug(
        artifact_payload=artifact_payload,
        artifact_path=artifact_path,
        external_publish_target=external_publish_target,
    )
    request = {
        "mode": "fresh",
        "task_slug": task_slug,
        "root_goal": root_goal,
        "child_goal_slice": child_goal_slice,
        "endpoint_artifact_ref": str(artifact_path),
        "workspace_root": _nonempty(payload.get("workspace_root")),
        "state_root": _nonempty(payload.get("state_root")),
        "node_id": _nonempty(payload.get("node_id")),
        "round_id": _nonempty(payload.get("round_id") or "R1") or "R1",
        "workspace_mirror_relpath": workspace_mirror_relpath or _default_workspace_mirror_relpath(external_publish_target),
        "workspace_live_artifact_relpath": _nonempty(payload.get("workspace_live_artifact_relpath"))
        or _default_workspace_live_artifact_relpath(
            workspace_mirror_relpath or _default_workspace_mirror_relpath(external_publish_target)
        ),
        "external_publish_target": external_publish_target,
        "context_refs": context_refs,
        "result_sink_ref": _nonempty(payload.get("result_sink_ref")),
    }
    return bootstrap_first_implementer_node(authority=authority, **request)
