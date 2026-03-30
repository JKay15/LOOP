"""Deterministic bootstrap helpers for the first implementer node."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

from loop_product.control_intent import (
    ARTIFACT_SCOPE_SPEC,
    TERMINAL_AUTHORITY_SCOPE_SPEC,
    WORKFLOW_SCOPE_SPEC,
    default_activate_request_refs,
    default_progress_checkpoints as control_default_progress_checkpoints,
    default_split_request_refs,
    default_startup_required_output_paths as control_default_startup_required_output_paths,
    normalize_machine_choice,
)
from loop_product.dispatch.child_dispatch import materialize_child
from loop_product.dispatch.launch_policy import build_codex_cli_child_launch, merge_runtime_transport_env
from loop_product.kernel.authority import KernelMutationAuthority
from loop_product.kernel.policy import (
    implementer_budget_profile,
    implementer_execution_policy,
    implementer_reasoning_profile,
    kernel_execution_policy,
    kernel_reasoning_profile,
)
from loop_product.kernel.state import (
    KernelState,
    ensure_runtime_tree,
    persist_kernel_state,
    persist_node_snapshot,
    query_kernel_state_object,
)
from loop_product.protocols.node import NodeSpec, NodeStatus, normalize_progress_checkpoints
from loop_product.protocols.schema import validate_repo_object
from loop_product.runtime_paths import (
    implementer_workspace_root,
    node_machine_handoff_ref,
    product_repo_root,
    require_runtime_root,
    safe_runtime_name,
    shared_cache_helper_ref,
    state_scope_root,
)


def _nonempty(value: Any) -> str:
    return str(value or "").strip()


_ABSOLUTE_PATH_RE = re.compile(r"/[A-Za-z0-9._~%+=:@,/-]+")
_RELATIVE_PATH_RE = re.compile(r"(?:\.\./|\./)?[A-Za-z0-9._-]+(?:/[A-Za-z0-9._~%+=:@,-]+)+")
_TITLE_RE = re.compile(r"\\title\{([^}]*)\}")
_SECTION_RE = re.compile(r"\\(section|subsection|subsubsection)\*?\{([^}]*)\}")
_BEGIN_ENV_RE = re.compile(r"\\begin\{([A-Za-z*]+)\}")
_END_ENV_RE = re.compile(r"\\end\{([A-Za-z*]+)\}")
_LABEL_RE = re.compile(r"\\label\{([^}]*)\}")
_REF_RE = re.compile(r"\\(?:ref|eqref|autoref|cref|Cref)\{([^}]*)\}")
_CITE_RE = re.compile(r"\\cite[a-zA-Z*]*\{([^}]*)\}")
_THEOREM_LIKE_ENV_NAMES = frozenset(
    {
        "theorem",
        "lemma",
        "proposition",
        "corollary",
        "definition",
        "assumption",
        "remark",
        "example",
        "claim",
        "fact",
        "observation",
        "conjecture",
    }
)
_REFERENCE_STRUCTURED_ENV_NAMES = frozenset(set(_THEOREM_LIKE_ENV_NAMES) | {"equation"})


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


def _strip_latex_comment(line: str) -> str:
    text = str(line or "")
    escaped = False
    for idx, ch in enumerate(text):
        if ch == "\\":
            escaped = not escaped
            continue
        if ch == "%" and not escaped:
            return text[:idx]
        escaped = False
    return text


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


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


def _preseed_external_live_root_shared_cache_mount(*, workspace_root: Path, live_root: Path, purpose: str) -> None:
    workspace_path = workspace_root.resolve()
    live_path = live_root.resolve()
    if _is_relative_to(live_path, workspace_path):
        return
    helper_path = shared_cache_helper_ref().resolve()
    if not helper_path.exists():
        raise ValueError(f"shared-cache helper not found for external live root preseed: {helper_path}")
    repo_root = product_repo_root().parent.resolve()
    live_path.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        [
            str(helper_path),
            "--repo-root",
            str(repo_root),
            "--workspace-root",
            str(live_path),
            "--purpose",
            purpose,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise ValueError(
            "external live-root shared-cache preseed failed"
            + (f": {detail}" if detail else "")
        )
    lake_root = live_path / ".lake"
    if not lake_root.is_symlink():
        raise ValueError(
            "external live-root shared-cache preseed must externalize the entire .lake mount"
        )


def _context_refs_explicitly_request_shared_cache_mount(*, context_refs: list[str]) -> bool:
    helper_ref = shared_cache_helper_ref().resolve()
    for item in context_refs:
        ref = str(item or "").strip()
        if not ref:
            continue
        try:
            if Path(ref).expanduser().resolve() == helper_ref:
                return True
        except Exception:
            continue
    return False


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


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _curate_endpoint_context_refs(
    *,
    refs: list[str],
    workflow_scope: str,
) -> list[str]:
    if normalize_machine_choice(workflow_scope, WORKFLOW_SCOPE_SPEC) != "whole_paper_formalization":
        return _dedupe_refs(refs)

    repo_root = product_repo_root().resolve()
    leanatlas_root = repo_root.parent.resolve()
    disallowed_exact_refs = {
        str((leanatlas_root / "docs" / "agents" / "OPERATOR_WORKFLOW.md").resolve()),
        str((leanatlas_root / ".agents" / "skills" / "leanatlas-operator-proof-loop" / "SKILL.md").resolve()),
    }
    runtime_roots = [
        (repo_root / ".loop").resolve(),
        (repo_root / "workspace").resolve(),
    ]
    curated: list[str] = []
    seen: set[str] = set()
    for raw_ref in refs:
        normalized_ref = str(Path(str(raw_ref or "")).expanduser().resolve())
        if not normalized_ref or normalized_ref in seen:
            continue
        if normalized_ref in disallowed_exact_refs:
            continue
        resolved_path = Path(normalized_ref)
        if any(_path_is_within(resolved_path, runtime_root) for runtime_root in runtime_roots):
            continue
        seen.add(normalized_ref)
        curated.append(normalized_ref)
    return curated


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
    from loop_product.runtime.gc import ensure_housekeeping_reap_controller_service_for_runtime_root
    from loop_product.runtime.control_plane import ensure_repo_control_plane_service_for_runtime_root

    repo_services = ensure_repo_control_plane_service_for_runtime_root(state_root=state_root)
    if str(repo_services.get("skip_reason") or "").strip() == "non_top_level_runtime_root":
        ensure_housekeeping_reap_controller_service_for_runtime_root(state_root=state_root)
    kernel_state_path = state_root / "state" / "kernel_state.json"
    if kernel_state_path.exists():
        return query_kernel_state_object(state_root, continue_deferred=False)

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
    workflow_scope: str,
    artifact_scope: str,
    terminal_authority_scope: str,
    workspace_root: Path,
    workspace_mirror_relpath: str,
    workspace_live_artifact_relpath: str,
    external_publish_target: str,
    required_output_paths: list[str],
    startup_required_output_paths: list[str],
    progress_checkpoints: list[dict[str, Any]],
    context_refs: list[str],
    result_sink_ref: str,
    workspace_result_sink_relpath: str,
    workspace_result_sink_ref: str,
    kernel_result_sink_ref: str,
    artifact_publication_receipt_ref: str,
    artifact_publication_runner_ref: str,
    shared_cache_runner_ref: str,
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
    agent_context_refs = _sanitize_agent_context_refs(context_refs, endpoint_artifact_ref=endpoint_artifact_ref)
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
        "goal_slice": child_goal_slice,
        "workflow_scope": normalize_machine_choice(workflow_scope, WORKFLOW_SCOPE_SPEC),
        "artifact_scope": normalize_machine_choice(artifact_scope, ARTIFACT_SCOPE_SPEC),
        "terminal_authority_scope": normalize_machine_choice(
            terminal_authority_scope,
            TERMINAL_AUTHORITY_SCOPE_SPEC,
        ),
        "workspace_mirror_relpath": workspace_mirror_relpath,
        "workspace_mirror_ref": workspace_mirror_ref,
        "workspace_live_artifact_relpath": workspace_live_artifact_relpath,
        "workspace_live_artifact_ref": workspace_live_artifact_ref,
        "external_publish_target": external_publish_target,
        "required_output_paths": [str(item).strip() for item in list(required_output_paths or []) if str(item).strip()],
        "required_outputs": [str(item).strip() for item in list(required_output_paths or []) if str(item).strip()],
        "startup_required_output_paths": [
            str(item).strip() for item in list(startup_required_output_paths or []) if str(item).strip()
        ],
        "progress_checkpoints": normalize_progress_checkpoints(progress_checkpoints),
        "context_refs": agent_context_refs,
        "result_sink_ref": result_sink_ref,
        "workspace_result_sink_relpath": workspace_result_sink_relpath,
        "workspace_result_sink_ref": workspace_result_sink_ref,
        "kernel_result_sink_ref": kernel_result_sink_ref,
        "artifact_publication_receipt_ref": artifact_publication_receipt_ref,
        "artifact_publication_runner_ref": artifact_publication_runner_ref,
        "shared_cache_runner_ref": shared_cache_runner_ref,
        "evaluator_submission_ref": evaluator_submission_ref,
        "evaluator_runner_ref": evaluator_runner_ref,
        "external_publication_owner": "root-kernel",
        "external_input_policy": "read_only_external_inputs_allowed_when_required_by_frozen_endpoint",
    }


def _sanitize_agent_context_refs(context_refs: Iterable[str], *, endpoint_artifact_ref: str) -> list[str]:
    endpoint_artifact_path = str(Path(endpoint_artifact_ref).expanduser().resolve())
    sanitized: list[str] = []
    for item in list(context_refs or []):
        item_str = str(item).strip()
        if not item_str:
            continue
        item_path = str(Path(item_str).expanduser().resolve())
        if item_path == endpoint_artifact_path:
            continue
        sanitized.append(item_str)
    return sanitized


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
        f"- shared_cache_runner_ref: `{payload['shared_cache_runner_ref']}`",
        f"- evaluator_submission_ref: `{payload['evaluator_submission_ref']}`",
        f"- evaluator_runner_ref: `{payload['evaluator_runner_ref']}`",
        f"- external_publication_owner: `{payload['external_publication_owner']}`",
        f"- external_input_policy: `{payload['external_input_policy']}`",
    ]
    context_refs = [str(item) for item in payload.get("context_refs") or []]
    required_output_paths = [str(item) for item in payload.get("required_output_paths") or [] if str(item).strip()]
    startup_required_output_paths = [
        str(item) for item in payload.get("startup_required_output_paths") or [] if str(item).strip()
    ]
    progress_checkpoints = normalize_progress_checkpoints(payload.get("progress_checkpoints") or [])
    if required_output_paths:
        lines.extend(["", "## Required Outputs", ""])
        lines.extend(f"- `{item}`" for item in required_output_paths)
    if startup_required_output_paths:
        lines.extend(["", "## Startup Required Outputs", ""])
        lines.extend(f"- `{item}`" for item in startup_required_output_paths)
    if progress_checkpoints:
        lines.extend(["", "## Progress Checkpoints", ""])
        for checkpoint in progress_checkpoints:
            lines.append(f"- `{checkpoint['checkpoint_id']}` ({checkpoint['window_s']}s)")
            if str(checkpoint.get("description") or "").strip():
                lines.append(f"  - {checkpoint['description']}")
            for relpath in checkpoint.get("required_any_of") or []:
                lines.append(f"  - requires any of: `{relpath}`")
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
    shared_cache_runner_abs: Path,
    split_runner_abs: Path,
    activate_runner_abs: Path,
    workspace_result_sink_abs: Path,
    kernel_result_sink_abs: Path,
    evaluator_submission_abs: Path,
    evaluator_runner_abs: Path,
) -> str:
    repo_root = product_repo_root().resolve()
    canonical_workspace_agents = (repo_root / "workspace" / "AGENTS.md").resolve()
    workspace_agents = canonical_workspace_agents if canonical_workspace_agents.exists() else (workspace_root.parent / "AGENTS.md").resolve()
    loop_runner_skill = (repo_root / ".agents" / "skills" / "loop-runner" / "SKILL.md").resolve()
    evaluator_exec_skill = (repo_root / ".agents" / "skills" / "evaluator-exec" / "SKILL.md").resolve()
    evaluator_manual = (repo_root / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md").resolve()
    local_input_helper = (repo_root / "scripts" / "find_local_input_candidates.sh").resolve()
    slice_scope_guard_lines: list[str] = []
    if not _use_whole_paper_evaluator_surface(node):
        slice_scope_guard_lines = [
            "This node is slice-scoped and does not own whole-paper terminal classification authority.",
            "If you emit `WHOLE_PAPER_STATUS.json`, keep it slice-local and do not claim whole-paper `TERMINAL` classifications such as `paper defect exposed`, `external dependency blocked`, or `whole-paper faithful complete formalization`.",
            "If this is a whole-paper split child, the live root already contains a deterministic slice startup bundle (`README.md`, `TRACEABILITY.md`, `SLICE_STATUS.json`, `analysis/SLICE_BOUNDARY.json`). Extend that bundle instead of restarting from a blank workspace.",
            "For a whole-paper split child, do not leave the next step implicit: write a machine-auditable `analysis/SLICE_EXECUTION_DECISION.json` or submit the next split decision before extended theorem search.",
        ]
    whole_paper_source_guard_lines: list[str] = []
    if (
        normalize_machine_choice(node.workflow_scope, WORKFLOW_SCOPE_SPEC) == "whole_paper_formalization"
        and normalize_machine_choice(node.artifact_scope, ARTIFACT_SCOPE_SPEC) == "task"
        and normalize_machine_choice(node.terminal_authority_scope, TERMINAL_AUTHORITY_SCOPE_SPEC) == "whole_paper"
    ):
        whole_paper_source_guard_lines = [
            "This node is the whole-paper source phase.",
            "Its opening responsibility is to advance source control artifacts for extraction, dependency closure surfacing, partition drafting, and split submission.",
            "Do not read evaluator manuals or final-effects docs during the opening extraction/partition phase while the deterministic source control bundle is still the only materialized artifact set.",
            "Advance the source control bundle or submit split before spending cycles on whole-paper terminal-classification analysis.",
            "The startup batch already materializes `split/SPLIT_REQUEST_PROPOSAL.json` and `split/SPLIT_REVIEW.json`; do not reinvent them from scratch.",
            "After the startup control bundle is present, either submit the split through the exact split helper or write `split/SPLIT_DECLINE_REASON.md` with a concrete machine-auditable reason.",
        ]
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
            "If the frozen handoff declares startup required outputs, materialize that exact first batch under the live artifact root before treating the node as substantively underway.",
            *whole_paper_source_guard_lines,
            *slice_scope_guard_lines,
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
            "If you create or ship a fresh workspace-local Lean package, call the exact shared-cache runner named below before the first `lake build`; do not invoke the generic helper path with guessed argv.",
            "Run Lean/package tooling only inside the exact live artifact root named below; do not hydrate package trees or build outputs inside the publish root.",
            "If the exact live artifact root named below is outside the workspace root, do not recreate a local `.tmp_primary_artifact`, root-level `.lake`, or other runtime-owned heavy tree inside the workspace; that is a runtime defect, not an acceptable fallback.",
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
            "If your context refs include a parent frozen handoff markdown, treat that inherited frozen handoff as authoritative whole-task context in addition to your narrowed branch goal slice.",
            "If you decide split is warranted, materialize a structured split proposal and call the exact split helper named in this prompt instead of only writing the recommendation into deliverable prose such as `PARTITION_PLAN.md` or `TRACEABILITY.md`.",
            "If a child needs explanatory gating prose, record it under `activation_rationale`.",
            "If a child needs a machine gate, use `activation_condition` only with machine-evaluable syntax `after:<node_id>:<requirement>`.",
            "If dependencies already express the machine gate, leave `activation_condition` empty and rely on `depends_on_node_ids` alone.",
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
            f"- shared-cache runner: `{shared_cache_runner_abs.resolve()}`",
            f"- split runner: `{split_runner_abs.resolve()}`",
            f"- activate runner: `{activate_runner_abs.resolve()}`",
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
    handoff_md_path: Path,
    context_refs: list[str],
) -> tuple[Path, Path]:
    from loop_product.loop import build_evaluator_submission_for_frozen_task

    bootstrap_dir = state_root / "artifacts" / "bootstrap" / node.node_id
    submission_path = bootstrap_dir / "EvaluatorNodeSubmission.json"
    runner_path = workspace_root / "RUN_EVALUATOR_NODE_UNTIL_TERMINAL.sh"
    manual_path = bootstrap_dir / "EvaluatorProductManual.md"
    final_effects_path = bootstrap_dir / "EvaluatorFinalEffects.md"
    output_root = (state_root / "artifacts" / "evaluator_runs" / node.node_id).resolve()
    workspace_mirror_ref = str((workspace_root / workspace_mirror_relpath).resolve())
    agent_context_refs = _sanitize_agent_context_refs(context_refs, endpoint_artifact_ref=endpoint_artifact_ref)
    if _use_whole_paper_evaluator_surface(node):
        _write_text(
            manual_path,
            _render_task_scoped_evaluator_manual(
                workspace_root=workspace_root,
                workspace_mirror_ref=workspace_mirror_ref,
                external_publish_target=external_publish_target,
                handoff_md_path=handoff_md_path,
            ),
        )
        _write_text(
            final_effects_path,
            _render_task_scoped_evaluator_final_effects_text(
                artifact_payload=artifact_payload,
                workspace_mirror_ref=workspace_mirror_ref,
            ),
        )
    else:
        _write_text(
            manual_path,
            _render_slice_scoped_evaluator_manual(
                node=node,
                workspace_root=workspace_root,
                workspace_mirror_ref=workspace_mirror_ref,
                handoff_md_path=handoff_md_path,
            ),
        )
        _write_text(
            final_effects_path,
            _render_slice_scoped_evaluator_final_effects_text(
                node=node,
                workspace_mirror_ref=workspace_mirror_ref,
                required_output_paths=required_output_paths,
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
        context_refs=[*list(agent_context_refs), str(handoff_md_path.resolve())],
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


def _materialize_publication_runner(*, workspace_root: Path, machine_handoff_ref: Path) -> Path:
    repo_runner = (product_repo_root().resolve() / "scripts" / "publish_workspace_artifact_from_handoff.sh").resolve()
    runner_path = workspace_root / "PUBLISH_WORKSPACE_ARTIFACT.sh"
    _write_executable_text(
        runner_path,
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f'exec "{repo_runner}" --handoff-ref "{machine_handoff_ref.resolve()}" "$@"',
            ]
        ),
    )
    return runner_path


def _materialize_shared_cache_runner(
    *,
    workspace_root: Path,
    workspace_live_artifact_root: Path,
    node_id: str,
) -> Path:
    helper_ref = shared_cache_helper_ref().resolve()
    runner_path = workspace_root / "ENSURE_SHARED_CACHE.sh"
    purpose = f"{str(node_id).strip() or 'implementer'} live-root shared-cache hydration"
    _write_executable_text(
        runner_path,
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f'exec "{helper_ref}" --workspace-root "{workspace_live_artifact_root.resolve()}" --purpose "{purpose}" "$@"',
            ]
        ),
    )
    return runner_path


def _materialize_split_runner(
    *,
    workspace_root: Path,
    machine_handoff_ref: Path,
    proposal_ref: Path,
    result_ref: Path,
) -> Path:
    repo_runner = (product_repo_root().resolve() / "scripts" / "submit_split_request_from_handoff.sh").resolve()
    runner_path = workspace_root / "SUBMIT_SPLIT_REQUEST.sh"
    _write_executable_text(
        runner_path,
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f'exec "{repo_runner}" --handoff-ref "{machine_handoff_ref.resolve()}" --proposal-ref "{proposal_ref.resolve()}" --result-ref "{result_ref.resolve()}" "$@"',
            ]
        ),
    )
    return runner_path


def _materialize_activate_runner(
    *,
    workspace_root: Path,
    machine_handoff_ref: Path,
    proposal_ref: Path,
    result_ref: Path,
) -> Path:
    repo_runner = (product_repo_root().resolve() / "scripts" / "submit_activate_request_from_handoff.sh").resolve()
    runner_path = workspace_root / "SUBMIT_ACTIVATE_REQUEST.sh"
    _write_executable_text(
        runner_path,
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f'exec "{repo_runner}" --handoff-ref "{machine_handoff_ref.resolve()}" --proposal-ref "{proposal_ref.resolve()}" --result-ref "{result_ref.resolve()}" "$@"',
            ]
        ),
    )
    return runner_path


def _persist_bootstrap_result(*, state_root: Path, request_payload: dict[str, Any], result_payload: dict[str, Any]) -> Path:
    node_id = _nonempty(result_payload.get("node_id")) or _nonempty(request_payload.get("node_id")) or "bootstrap"
    bootstrap_dir = state_root / "artifacts" / "bootstrap" / node_id
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
    shared_cache_runner_ref: Path,
    workspace_result_sink_ref: Path,
    kernel_result_sink_ref: Path,
    evaluator_submission_ref: Path,
    evaluator_runner_ref: Path,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "reuse_decision": reuse_decision,
        "node_id": node.node_id,
        "workflow_scope": str(node.workflow_scope),
        "artifact_scope": str(node.artifact_scope),
        "terminal_authority_scope": str(node.terminal_authority_scope),
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
        "shared_cache_runner_ref": str(shared_cache_runner_ref.resolve()),
        "workspace_result_sink_ref": str(workspace_result_sink_ref.resolve()),
        "kernel_result_sink_ref": str(kernel_result_sink_ref.resolve()),
        "evaluator_submission_ref": str(evaluator_submission_ref.resolve()),
        "evaluator_runner_ref": str(evaluator_runner_ref.resolve()),
        "launch_spec": launch_spec,
    }


def _normalize_bootstrap_request(payload: dict[str, Any]) -> dict[str, Any]:
    workspace_mirror_relpath = _nonempty(payload.get("workspace_mirror_relpath"))
    workflow_scope = normalize_machine_choice(payload.get("workflow_scope"), WORKFLOW_SCOPE_SPEC)
    artifact_scope = normalize_machine_choice(payload.get("artifact_scope"), ARTIFACT_SCOPE_SPEC)
    terminal_authority_scope = normalize_machine_choice(
        payload.get("terminal_authority_scope"),
        TERMINAL_AUTHORITY_SCOPE_SPEC,
    )
    startup_required_output_paths = [
        str(item).strip() for item in (payload.get("startup_required_output_paths") or []) if str(item).strip()
    ]
    if not startup_required_output_paths:
        startup_required_output_paths = _default_startup_required_output_paths(
            workflow_scope=workflow_scope,
            artifact_scope=artifact_scope,
            terminal_authority_scope=terminal_authority_scope,
        )
    progress_checkpoints = normalize_progress_checkpoints(payload.get("progress_checkpoints") or [])
    if not progress_checkpoints:
        progress_checkpoints = _default_progress_checkpoints(
            workflow_scope=workflow_scope,
            artifact_scope=artifact_scope,
            terminal_authority_scope=terminal_authority_scope,
        )
    normalized = {
        "mode": _nonempty(payload.get("mode") or "fresh") or "fresh",
        "task_slug": _nonempty(payload.get("task_slug")),
        "root_goal": _nonempty(payload.get("root_goal")),
        "child_goal_slice": _nonempty(payload.get("child_goal_slice")),
        "workflow_scope": workflow_scope,
        "artifact_scope": artifact_scope,
        "terminal_authority_scope": terminal_authority_scope,
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
        "startup_required_output_paths": startup_required_output_paths,
        "progress_checkpoints": progress_checkpoints,
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


def _workflow_scope_from_endpoint_artifact(artifact_payload: dict[str, Any]) -> str:
    requirement_artifact = dict(artifact_payload.get("requirement_artifact") or {})
    return normalize_machine_choice(requirement_artifact.get("workflow_scope"), WORKFLOW_SCOPE_SPEC)


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


def _default_startup_required_output_paths(*, workflow_scope: str, artifact_scope: str, terminal_authority_scope: str) -> list[str]:
    return control_default_startup_required_output_paths(
        workflow_scope=workflow_scope,
        artifact_scope=artifact_scope,
        terminal_authority_scope=terminal_authority_scope,
    )


def _default_progress_checkpoints(*, workflow_scope: str, artifact_scope: str, terminal_authority_scope: str) -> list[dict[str, Any]]:
    return control_default_progress_checkpoints(
        workflow_scope=workflow_scope,
        artifact_scope=artifact_scope,
        terminal_authority_scope=terminal_authority_scope,
    )


def _materialize_whole_paper_slice_startup_batch(
    *,
    live_root: Path,
    node: NodeSpec,
    source_tex_ref: Path | None,
) -> None:
    source_ref_text = str(source_tex_ref.resolve()) if source_tex_ref is not None else ""
    status_payload = {
        "workflow_scope": "whole_paper_formalization",
        "artifact_scope": "slice",
        "terminal_authority_scope": "local",
        "node_id": node.node_id,
        "round_id": node.round_id,
        "status": "IN_PROGRESS",
        "slice_startup_batch_materialized": True,
        "goal_slice": str(node.goal_slice or ""),
        "source_tex_ref": source_ref_text,
    }
    boundary_payload = {
        "node_id": node.node_id,
        "goal_slice": str(node.goal_slice or ""),
        "workflow_scope": "whole_paper_formalization",
        "artifact_scope": "slice",
        "terminal_authority_scope": "local",
        "source_tex_ref": source_ref_text,
        "notes": [
            "This deterministic startup bundle is slice-scoped.",
            "The child still owes substantive formalization or dependency-closure progress after startup.",
            "Whole-paper terminal classification authority remains outside this slice node.",
        ],
    }
    readme_text = "\n".join(
        [
            "# Whole-Paper Slice Startup Bundle",
            "",
            f"- node_id: `{node.node_id}`",
            f"- round_id: `{node.round_id}`",
            f"- source_tex_ref: `{source_ref_text}`" if source_ref_text else "- source_tex_ref: `unresolved`",
            "",
            "This deterministic startup batch establishes the slice-local control surface before substantive formalization or dependency-closure work.",
        ]
    )
    traceability_text = "\n".join(
        [
            "# Slice Startup Traceability",
            "",
            f"- node_id: `{node.node_id}`",
            f"- goal_slice: `{str(node.goal_slice or '').strip()}`",
            f"- source_tex_ref: `{source_ref_text}`" if source_ref_text else "- source_tex_ref: `unresolved`",
            "",
            "This slice is scoped to local closure only and must not claim whole-paper terminal authority.",
        ]
    )
    _write_text(live_root / "README.md", readme_text)
    _write_text(live_root / "TRACEABILITY.md", traceability_text)
    _write_json(live_root / "SLICE_STATUS.json", status_payload)
    _write_json(live_root / "analysis" / "SLICE_BOUNDARY.json", boundary_payload)


def _first_existing_source_tex_ref(context_refs: list[str]) -> Path | None:
    for raw in context_refs:
        candidate = Path(str(raw or "")).expanduser()
        if candidate.suffix.lower() != ".tex":
            continue
        if candidate.exists():
            return candidate.resolve()
    return None


def _resolve_whole_paper_source_tex_ref(*, artifact_path: Path, context_refs: list[str]) -> Path | None:
    direct = _first_existing_source_tex_ref(context_refs)
    if direct is not None:
        return direct

    candidates: list[Path] = []
    for parent in artifact_path.resolve().parents:
        candidates.append(parent / "source" / "main.tex")
        if parent.name == "benchmark_inputs":
            candidates.append(parent.parent / "source" / "main.tex")
    seen: set[str] = set()
    for candidate in candidates:
        resolved_key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if resolved_key in seen:
            continue
        seen.add(resolved_key)
        if candidate.exists() and candidate.is_file():
            return candidate.resolve()
    return None


def _split_latex_csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _parse_whole_paper_source_tex(source_tex_ref: Path) -> dict[str, Any]:
    text = source_tex_ref.read_text(encoding="utf-8")
    lines = text.splitlines()
    title_match = _TITLE_RE.search(text)
    title = str(title_match.group(1)).strip() if title_match else source_tex_ref.stem

    sections: list[dict[str, Any]] = []
    theorem_items: list[dict[str, Any]] = []
    current_item: dict[str, Any] | None = None
    whole_source_citation_keys: list[str] = []

    for line_no, line in enumerate(lines, start=1):
        stripped_line = _strip_latex_comment(line)
        for match in _SECTION_RE.finditer(stripped_line):
            command = str(match.group(1) or "").strip()
            sections.append(
                {
                    "level": command,
                    "title": str(match.group(2) or "").strip(),
                    "line_start": line_no,
                }
            )
        for cite_match in _CITE_RE.finditer(stripped_line):
            for cite_key in _split_latex_csv(str(cite_match.group(1) or "")):
                if cite_key not in whole_source_citation_keys:
                    whole_source_citation_keys.append(cite_key)

        if current_item is None:
            begin_match = _BEGIN_ENV_RE.search(stripped_line)
            if begin_match:
                env_name = str(begin_match.group(1) or "").strip()
                if env_name in _REFERENCE_STRUCTURED_ENV_NAMES:
                    current_item = {
                        "env_name": env_name,
                        "line_start": line_no,
                        "line_end": line_no,
                        "statement_lines": [stripped_line],
                        "labels": [],
                        "internal_refs": [],
                        "citations": [],
                    }
        else:
            current_item["line_end"] = line_no
            current_item["statement_lines"].append(stripped_line)

        if current_item is not None:
            for label_match in _LABEL_RE.finditer(stripped_line):
                label = str(label_match.group(1) or "").strip()
                if label and label not in current_item["labels"]:
                    current_item["labels"].append(label)
            for ref_match in _REF_RE.finditer(stripped_line):
                label = str(ref_match.group(1) or "").strip()
                if label and label not in current_item["internal_refs"]:
                    current_item["internal_refs"].append(label)
            for cite_match in _CITE_RE.finditer(stripped_line):
                for cite_key in _split_latex_csv(str(cite_match.group(1) or "")):
                    if cite_key not in current_item["citations"]:
                        current_item["citations"].append(cite_key)

            end_match = _END_ENV_RE.search(stripped_line)
            if end_match and str(end_match.group(1) or "").strip() == str(current_item["env_name"]):
                labels = list(current_item["labels"])
                node_id = labels[0] if labels else f"{current_item['env_name']}:{current_item['line_start']}"
                theorem_items.append(
                    {
                        "node_id": node_id,
                        "env_name": current_item["env_name"],
                        "label": labels[0] if labels else "",
                        "line_start": int(current_item["line_start"]),
                        "line_end": int(current_item["line_end"]),
                        "statement_tex": "\n".join(current_item["statement_lines"]).strip(),
                        "internal_refs": list(current_item["internal_refs"]),
                        "citations": list(current_item["citations"]),
                    }
                )
                current_item = None

    top_level_sections = [section for section in sections if str(section.get("level") or "") == "section"]

    def _section_title_for_line(line_no: int) -> str:
        current_title = "front_matter"
        for section in top_level_sections:
            try:
                section_line_start = int(section.get("line_start") or 0)
            except (TypeError, ValueError):
                continue
            if section_line_start <= int(line_no):
                current_title = str(section.get("title") or "").strip() or current_title
            else:
                break
        return current_title

    for item in theorem_items:
        item["section_title"] = _section_title_for_line(int(item["line_start"]))

    nodes = [
        {
            "node_id": item["node_id"],
            "env_name": item["env_name"],
            "label": item["label"],
            "line_start": item["line_start"],
            "line_end": item["line_end"],
            "section_title": item["section_title"],
        }
        for item in theorem_items
    ]
    edges = []
    seen_edges: set[tuple[str, str]] = set()
    for item in theorem_items:
        for ref in list(item["internal_refs"]):
            edge = (str(item["node_id"]), str(ref))
            if edge in seen_edges:
                continue
            seen_edges.add(edge)
            edges.append(
                {
                    "from_node_id": str(item["node_id"]),
                    "to_ref": str(ref),
                }
            )
    citation_keys = sorted({str(cite) for item in theorem_items for cite in list(item["citations"]) if str(cite).strip()})
    if whole_source_citation_keys:
        citation_keys = sorted({str(cite).strip() for cite in whole_source_citation_keys if str(cite).strip()})
        citation_count = len(list(whole_source_citation_keys))
    else:
        citation_count = sum(len(list(item["citations"])) for item in theorem_items)
    return {
        "title": title,
        "sections": sections,
        "top_level_sections": top_level_sections,
        "theorem_items": theorem_items,
        "nodes": nodes,
        "edges": edges,
        "citation_keys": citation_keys,
        "citation_count": citation_count,
    }


def _materialize_whole_paper_startup_batch(*, live_root: Path, source_tex_ref: Path) -> None:
    parsed = _parse_whole_paper_source_tex(source_tex_ref)
    live_root.mkdir(parents=True, exist_ok=True)

    source_structure_payload = {
        "source_tex_ref": str(source_tex_ref.resolve()),
        "title": str(parsed["title"]),
        "section_count": len(list(parsed["sections"])),
        "sections": list(parsed["sections"]),
    }
    theorem_inventory_payload = {
        "source_tex_ref": str(source_tex_ref.resolve()),
        "theorem_like_count": len(list(parsed["theorem_items"])),
        "items": list(parsed["theorem_items"]),
    }
    dependency_graph_payload = {
        "source_tex_ref": str(source_tex_ref.resolve()),
        "node_count": len(list(parsed["nodes"])),
        "edge_count": len(list(parsed["edges"])),
        "nodes": list(parsed["nodes"]),
        "edges": list(parsed["edges"]),
    }
    block_order: list[str] = []
    block_map: dict[str, dict[str, Any]] = {}
    for item in list(parsed["theorem_items"]):
        section_title = str(item.get("section_title") or "front_matter").strip() or "front_matter"
        if section_title not in block_map:
            block_order.append(section_title)
            block_map[section_title] = {
                "block_id": safe_runtime_name(section_title) or f"block_{len(block_order)}",
                "block_title": section_title,
                "node_ids": [],
                "labels": [],
                "line_start": int(item["line_start"]),
                "line_end": int(item["line_end"]),
            }
        block = block_map[section_title]
        block["node_ids"].append(str(item["node_id"]))
        label = str(item.get("label") or "").strip()
        if label:
            block["labels"].append(label)
        block["line_start"] = min(int(block["line_start"]), int(item["line_start"]))
        block["line_end"] = max(int(block["line_end"]), int(item["line_end"]))
    block_partition_payload = {
        "source_tex_ref": str(source_tex_ref.resolve()),
        "block_count": len(block_order),
        "blocks": [block_map[title] for title in block_order],
    }
    external_dependency_payload = {
        "source_tex_ref": str(source_tex_ref.resolve()),
        "citation_key_count": len(list(parsed["citation_keys"])),
        "citation_keys": list(parsed["citation_keys"]),
        "candidates": [
            {
                "citation_key": str(key),
                "status": "unresolved_candidate",
            }
            for key in list(parsed["citation_keys"])
        ],
    }
    split_target_nodes: list[dict[str, Any]] = []
    slice_startup_required_output_paths = _default_startup_required_output_paths(
        workflow_scope="whole_paper_formalization",
        artifact_scope="slice",
        terminal_authority_scope="local",
    )
    slice_progress_checkpoints = _default_progress_checkpoints(
        workflow_scope="whole_paper_formalization",
        artifact_scope="slice",
        terminal_authority_scope="local",
    )
    selected_blocks = list(block_partition_payload["blocks"])[:3]
    for block in selected_blocks:
        block_id = str(block.get("block_id") or "").strip() or safe_runtime_name(str(block.get("block_title") or "block"))
        block_title = str(block.get("block_title") or block_id).strip() or block_id
        split_target_nodes.append(
            {
                "node_id": safe_runtime_name(f"{block_id}_formalization"),
                "goal_slice": (
                    f"Faithfully formalize the whole-paper block `{block_title}` from the deterministic partition draft, "
                    "preserving internal references and deferring proof-relevant external dependencies only via explicit closure artifacts."
                ),
                "workflow_scope": "whole_paper_formalization",
                "artifact_scope": "slice",
                "terminal_authority_scope": "local",
                "startup_required_output_paths": list(slice_startup_required_output_paths),
                "progress_checkpoints": list(slice_progress_checkpoints),
            }
        )
    if int(external_dependency_payload["citation_key_count"]) > 0:
        split_target_nodes.append(
            {
                "node_id": "external_dependency_closure",
                "goal_slice": (
                    "Resolve or honestly classify the proof-relevant external dependencies named in the deterministic external dependency ledger."
                ),
                "workflow_scope": "whole_paper_formalization",
                "artifact_scope": "slice",
                "terminal_authority_scope": "local",
                "startup_required_output_paths": list(slice_startup_required_output_paths),
                "progress_checkpoints": list(slice_progress_checkpoints),
            }
        )
    while len(split_target_nodes) < 2:
        split_target_nodes.append(
            {
                "node_id": safe_runtime_name(f"whole_paper_followup_{len(split_target_nodes) + 1}"),
                "goal_slice": (
                    "Faithfully continue the remaining whole-paper formalization work from the deterministic startup partition draft."
                ),
                "workflow_scope": "whole_paper_formalization",
                "artifact_scope": "slice",
                "terminal_authority_scope": "local",
                "startup_required_output_paths": list(slice_startup_required_output_paths),
                "progress_checkpoints": list(slice_progress_checkpoints),
            }
        )
    split_request_payload = {
        "split_mode": "parallel",
        "reason": (
            "Deterministic whole-paper startup analysis identified distinct formalization blocks and an explicit external-dependency lane."
        ),
        "completed_work": (
            "The source startup control bundle, dependency graph, partition draft, and external dependency ledger were materialized deterministically."
        ),
        "remaining_work": (
            "Review this staged split draft, then either submit it through the committed split helper or decline it explicitly."
        ),
        "target_nodes": split_target_nodes,
    }
    split_review_payload = {
        "status": "DRAFT_REQUIRES_SOURCE_DECISION",
        "source_tex_ref": str(source_tex_ref.resolve()),
        "block_count": int(block_partition_payload["block_count"]),
        "citation_key_count": int(external_dependency_payload["citation_key_count"]),
        "expected_decision_artifacts": [
            "split/SPLIT_REQUEST_SUBMISSION_RESULT.json",
            "split/SPLIT_DECLINE_REASON.md",
        ],
        "next_step": "Submit the deterministic split proposal or decline it explicitly.",
    }
    status_payload = {
        "workflow_scope": "whole_paper_formalization",
        "status": "IN_PROGRESS",
        "startup_batch_materialized": True,
        "startup_control_bundle_materialized": True,
        "source_tex_ref": str(source_tex_ref.resolve()),
        "section_count": int(source_structure_payload["section_count"]),
        "theorem_like_count": int(theorem_inventory_payload["theorem_like_count"]),
        "internal_dependency_edge_count": int(dependency_graph_payload["edge_count"]),
        "block_count": int(block_partition_payload["block_count"]),
        "citation_count": int(parsed["citation_count"]),
        "split_proposal_materialized": True,
        "split_decision_checkpoint_pending": True,
    }
    readme_text = "\n".join(
        [
            "# Whole-Paper Startup Extraction Batch",
            "",
            f"- source_tex_ref: `{source_tex_ref.resolve()}`",
            f"- section_count: `{source_structure_payload['section_count']}`",
            f"- theorem_like_count: `{theorem_inventory_payload['theorem_like_count']}`",
            f"- internal_dependency_edge_count: `{dependency_graph_payload['edge_count']}`",
            f"- block_count: `{block_partition_payload['block_count']}`",
            f"- citation_count: `{parsed['citation_count']}`",
            "",
            "This batch was materialized deterministically from the frozen source TeX before implementer-owned split/formalization work begins.",
        ]
    )
    traceability_text = "\n".join(
        [
            "# Whole-Paper Source Control Bundle",
            "",
            "This source bootstrap owns the extraction/partition control surface before split.",
            "",
            f"- source_tex_ref: `{source_tex_ref.resolve()}`",
            f"- theorem_like_count: `{theorem_inventory_payload['theorem_like_count']}`",
            f"- internal_dependency_edge_count: `{dependency_graph_payload['edge_count']}`",
            f"- partition_block_count: `{block_partition_payload['block_count']}`",
            f"- external_dependency_candidate_count: `{external_dependency_payload['citation_key_count']}`",
            "",
            "Next source-phase responsibility:",
            "- refine the initial partition draft if needed,",
            "- advance traceability/external dependency evidence, and",
            "- submit or explicitly decline the deterministic split proposal once the staged fronts are ready.",
        ]
    )

    _write_text(live_root / "README.md", readme_text)
    _write_text(live_root / "TRACEABILITY.md", traceability_text)
    _write_json(live_root / "WHOLE_PAPER_STATUS.json", status_payload)
    _write_json(live_root / "extraction" / "source_structure.json", source_structure_payload)
    _write_json(live_root / "extraction" / "theorem_inventory.json", theorem_inventory_payload)
    _write_json(live_root / "analysis" / "internal_dependency_graph.json", dependency_graph_payload)
    _write_json(live_root / "analysis" / "block_partition_draft.json", block_partition_payload)
    _write_json(live_root / "external_dependencies" / "EXTERNAL_DEPENDENCY_LEDGER.json", external_dependency_payload)
    _write_json(live_root / "split" / "SPLIT_REQUEST_PROPOSAL.json", split_request_payload)
    _write_json(live_root / "split" / "SPLIT_REVIEW.json", split_review_payload)


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


def _render_slice_scoped_evaluator_final_effects_text(
    *,
    node: NodeSpec,
    workspace_mirror_ref: str,
    required_output_paths: list[str],
) -> str:
    lines = [
        "# Slice-Scoped Evaluator Final Effects",
        "",
        (
            f"- The workspace mirror artifact at `{workspace_mirror_ref}` must either faithfully fulfill the narrowed "
            f"split-child slice owned by `{node.node_id}` ({node.goal_slice}) or truthfully close this node locally "
            "by publishing `WHOLE_PAPER_STATUS.json` as either "
            "`classification=slice_local_formalization_complete` with "
            "`status=LOCAL_SLICE_COMPLETE_PENDING_PARENT_INTEGRATION`, or `status=TERMINAL` with "
            "`terminal_classification=SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION` plus accepted child lanes for the "
            "released remaining work."
        ),
        "- This evaluator judges only the slice-local deliverable surface for this node.",
        "- It must not claim whole-paper terminal closure or whole-paper completion for unrelated nodes.",
        "- It must not claim whole-paper terminal classifications reserved for the dedicated whole-paper closeout surface.",
    ]
    if required_output_paths:
        lines.extend(["", "## Required Outputs", ""])
        lines.extend(f"- `{item}`" for item in required_output_paths)
    lines.extend(
        [
            "",
            "## Evaluation Rules",
            "",
            "- Judge only this node's narrowed slice goal, deliverable artifact, and declared required outputs.",
            "- If `WHOLE_PAPER_STATUS.json` truthfully records `classification=slice_local_formalization_complete` and `status=LOCAL_SLICE_COMPLETE_PENDING_PARENT_INTEGRATION`, treat that as a valid slice-local completion whose whole-paper closeout remains parent-owned.",
            "- If `WHOLE_PAPER_STATUS.json` truthfully records `SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION`, treat that as a valid slice-local terminal split-continuation closeout rather than requiring full slice completion.",
            "- In that split-continuation case, do not require the released remaining work to already be completed inside this node.",
            "- A slice-local completion artifact must not be downgraded just because whole-paper terminal classification remains parent-owned.",
            "- Do not treat slice-local status files or README prose as whole-paper terminal evidence.",
            "- Whole-paper success or failure remains reserved for the dedicated whole-paper closeout surface.",
        ]
    )
    return "\n".join(lines)


def _render_task_scoped_evaluator_manual(
    *,
    workspace_root: Path,
    workspace_mirror_ref: str,
    external_publish_target: str,
    handoff_md_path: Path,
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
        f"- frozen_handoff_ref: `{handoff_md_path.resolve()}`",
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


def _render_slice_scoped_evaluator_manual(
    *,
    node: NodeSpec,
    workspace_root: Path,
    workspace_mirror_ref: str,
    handoff_md_path: Path,
) -> str:
    lines = [
        "# Slice-Scoped Evaluator Product Manual",
        "",
        "This evaluator run judges one split-child implementer artifact for one narrowed branch of a larger whole-paper benchmark.",
        "",
        "## Documented Surface",
        "",
        f"- workspace_root: `{workspace_root.resolve()}`",
        f"- workspace_mirror_ref: `{workspace_mirror_ref}`",
        f"- frozen_handoff_ref: `{handoff_md_path.resolve()}`",
        f"- target_node_id: `{node.node_id}`",
        f"- slice_goal: `{node.goal_slice}`",
        "",
        "## Evaluation Rules",
        "",
        "- Judge only the slice-local artifact and reviewer-visible evidence for this node.",
        "- A truthful local slice completion artifact is acceptable when `WHOLE_PAPER_STATUS.json` records `classification=slice_local_formalization_complete`, `status=LOCAL_SLICE_COMPLETE_PENDING_PARENT_INTEGRATION`, and whole-paper terminal classification remains parent-owned.",
        "- A truthful local split-continuation artifact is acceptable when `WHOLE_PAPER_STATUS.json` records `SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION` and released remaining work into accepted child lanes.",
        "- Do not require unrelated whole-paper outputs from sibling or parent nodes.",
        "- Whole-paper closure remains reserved for the dedicated whole-paper integration/closeout surface.",
    ]
    return "\n".join(lines)


def _use_whole_paper_evaluator_surface(node: NodeSpec) -> bool:
    return normalize_machine_choice(node.artifact_scope, ARTIFACT_SCOPE_SPEC) != "slice"


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
            workflow_scope=request["workflow_scope"],
            artifact_scope=request["artifact_scope"],
            terminal_authority_scope=request["terminal_authority_scope"],
            required_output_paths=request["required_output_paths"],
            startup_required_output_paths=request["startup_required_output_paths"],
            progress_checkpoints=request["progress_checkpoints"],
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
        kernel_state = query_kernel_state_object(state_root, continue_deferred=False)
        child_node_payload = dict(kernel_state.nodes.get(node_id) or {})
        if not child_node_payload:
            raise ValueError(f"continue_exact node does not exist: {node_id}")
        child_node = NodeSpec.from_dict(child_node_payload)
        if Path(str(child_node.workspace_root)).resolve() != workspace_root.resolve():
            raise ValueError("continue_exact workspace_root does not match the persisted node snapshot")
        child_node.workflow_scope = request["workflow_scope"]
        child_node.artifact_scope = request["artifact_scope"]
        child_node.terminal_authority_scope = request["terminal_authority_scope"]
        child_node.required_output_paths = list(request["required_output_paths"])
        child_node.startup_required_output_paths = list(request["startup_required_output_paths"])
        child_node.progress_checkpoints = normalize_progress_checkpoints(request["progress_checkpoints"])
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=authority)
        validate_repo_object("LoopSystemNodeSpec.schema.json", child_node.to_dict())
        persist_node_snapshot(state_root, child_node, authority=authority)
        result_sink_ref = request["result_sink_ref"] or child_node.result_sink_ref
    else:
        raise ValueError(f"unsupported bootstrap mode: {mode!r}")

    handoff_json_path = node_machine_handoff_ref(state_root=state_root, node_id=child_node.node_id)
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
        handoff_md_path=handoff_md_path,
        context_refs=request["context_refs"],
    )
    artifact_publication_runner_path = _materialize_publication_runner(
        workspace_root=workspace_root,
        machine_handoff_ref=handoff_json_path,
    )
    shared_cache_runner_path = _materialize_shared_cache_runner(
        workspace_root=workspace_root,
        workspace_live_artifact_root=workspace_live_artifact_abs,
        node_id=child_node.node_id,
    )
    split_proposal_ref, split_result_ref = default_split_request_refs(
        workspace_live_artifact_abs,
        workspace_mirror_relpath=request["workspace_mirror_relpath"],
    )
    activate_proposal_ref, activate_result_ref = default_activate_request_refs(
        workspace_live_artifact_abs,
        workspace_mirror_relpath=request["workspace_mirror_relpath"],
    )
    split_runner_path = _materialize_split_runner(
        workspace_root=workspace_root,
        machine_handoff_ref=handoff_json_path,
        proposal_ref=split_proposal_ref,
        result_ref=split_result_ref,
    )
    activate_runner_path = _materialize_activate_runner(
        workspace_root=workspace_root,
        machine_handoff_ref=handoff_json_path,
        proposal_ref=activate_proposal_ref,
        result_ref=activate_result_ref,
    )
    handoff_payload = _build_handoff_payload(
        node=child_node,
        state_root=state_root,
        endpoint_artifact_ref=request["endpoint_artifact_ref"],
        root_goal=request["root_goal"],
        child_goal_slice=request["child_goal_slice"],
        workflow_scope=request["workflow_scope"],
        artifact_scope=request["artifact_scope"],
        terminal_authority_scope=request["terminal_authority_scope"],
        workspace_root=workspace_root,
        workspace_mirror_relpath=request["workspace_mirror_relpath"],
        workspace_live_artifact_relpath=workspace_live_artifact_relpath,
        external_publish_target=request["external_publish_target"],
        required_output_paths=request["required_output_paths"],
        startup_required_output_paths=request["startup_required_output_paths"],
        progress_checkpoints=request["progress_checkpoints"],
        context_refs=request["context_refs"],
        result_sink_ref=result_sink_ref,
        workspace_result_sink_relpath=workspace_result_sink_relpath,
        workspace_result_sink_ref=str(workspace_result_sink_abs),
        kernel_result_sink_ref=str(kernel_result_sink_abs),
        artifact_publication_receipt_ref=str(artifact_publication_receipt_abs),
        artifact_publication_runner_ref=str(artifact_publication_runner_path.resolve()),
        shared_cache_runner_ref=str(shared_cache_runner_path.resolve()),
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
            shared_cache_runner_abs=shared_cache_runner_path,
            split_runner_abs=split_runner_path,
            activate_runner_abs=activate_runner_path,
            workspace_result_sink_abs=workspace_result_sink_abs,
            kernel_result_sink_abs=kernel_result_sink_abs,
            evaluator_submission_abs=evaluator_submission_path,
            evaluator_runner_abs=evaluator_runner_path,
        ),
    )
    if (
        normalize_machine_choice(child_node.workflow_scope, WORKFLOW_SCOPE_SPEC) == "whole_paper_formalization"
        and normalize_machine_choice(child_node.artifact_scope, ARTIFACT_SCOPE_SPEC) == "slice"
        and normalize_machine_choice(child_node.terminal_authority_scope, TERMINAL_AUTHORITY_SCOPE_SPEC) == "local"
        and list(request["startup_required_output_paths"] or [])
        == _default_startup_required_output_paths(
            workflow_scope="whole_paper_formalization",
            artifact_scope="slice",
            terminal_authority_scope="local",
        )
    ):
        source_tex_ref = _resolve_whole_paper_source_tex_ref(
            artifact_path=Path(request["endpoint_artifact_ref"]).expanduser().resolve(),
            context_refs=[str(item) for item in list(request["context_refs"] or [])],
        )
        _materialize_whole_paper_slice_startup_batch(
            live_root=workspace_live_artifact_abs,
            node=child_node,
            source_tex_ref=source_tex_ref,
        )
    if (
        normalize_machine_choice(child_node.workflow_scope, WORKFLOW_SCOPE_SPEC) == "whole_paper_formalization"
        or _context_refs_explicitly_request_shared_cache_mount(
            context_refs=[str(item) for item in list(request.get("context_refs") or [])]
        )
    ):
        preseed_purpose = (
            f"whole-paper bootstrap preseed for {child_node.node_id}"
            if normalize_machine_choice(child_node.workflow_scope, WORKFLOW_SCOPE_SPEC) == "whole_paper_formalization"
            else f"explicit shared-cache bootstrap preseed for {child_node.node_id}"
        )
        _preseed_external_live_root_shared_cache_mount(
            workspace_root=workspace_root,
            live_root=workspace_live_artifact_abs,
            purpose=preseed_purpose,
        )

    launch_spec = build_codex_cli_child_launch(
        workspace_root=workspace_root,
        sandbox_mode=str(child_node.execution_policy.get("sandbox_mode") or "danger-full-access"),
        thinking_budget=str(child_node.reasoning_profile.get("thinking_budget") or "medium"),
        prompt_path=child_prompt_path,
    )
    launch_spec["env"] = merge_runtime_transport_env(
        state_root=state_root,
        env=dict(launch_spec.get("env") or {}),
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
        shared_cache_runner_ref=shared_cache_runner_path,
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
    workflow_scope = _workflow_scope_from_endpoint_artifact(artifact_payload)
    context_refs = _derive_endpoint_context_refs(
        artifact_path=artifact_path,
        artifact_payload=artifact_payload,
        explicit_context_refs=[str(item) for item in (payload.get("context_refs") or [])],
    )
    context_refs = _curate_endpoint_context_refs(
        refs=context_refs,
        workflow_scope=workflow_scope,
    )
    whole_paper_source_tex_ref: Path | None = None
    if workflow_scope == "whole_paper_formalization":
        whole_paper_source_tex_ref = _resolve_whole_paper_source_tex_ref(
            artifact_path=artifact_path,
            context_refs=context_refs,
        )
        if whole_paper_source_tex_ref is not None:
            source_tex_ref_str = str(whole_paper_source_tex_ref)
            if source_tex_ref_str not in context_refs:
                context_refs = [source_tex_ref_str, *context_refs]
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
        "workflow_scope": workflow_scope,
        "artifact_scope": "task",
        "terminal_authority_scope": "whole_paper" if workflow_scope == "whole_paper_formalization" else "local",
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
        "startup_required_output_paths": _default_startup_required_output_paths(
            workflow_scope=workflow_scope,
            artifact_scope="task",
            terminal_authority_scope="whole_paper" if workflow_scope == "whole_paper_formalization" else "local",
        ),
        "progress_checkpoints": _default_progress_checkpoints(
            workflow_scope=workflow_scope,
            artifact_scope="task",
            terminal_authority_scope="whole_paper" if workflow_scope == "whole_paper_formalization" else "local",
        ),
        "result_sink_ref": _nonempty(payload.get("result_sink_ref")),
    }
    result_payload = bootstrap_first_implementer_node(authority=authority, **request)
    if (
        workflow_scope == "whole_paper_formalization"
        and normalize_machine_choice(request["artifact_scope"], ARTIFACT_SCOPE_SPEC) == "task"
        and normalize_machine_choice(request["terminal_authority_scope"], TERMINAL_AUTHORITY_SCOPE_SPEC) == "whole_paper"
    ):
        if whole_paper_source_tex_ref is not None:
            live_root = Path(str(result_payload.get("workspace_live_artifact_ref") or "")).expanduser().resolve()
            _materialize_whole_paper_startup_batch(
                live_root=live_root,
                source_tex_ref=whole_paper_source_tex_ref,
            )
            workspace_root = Path(str(result_payload.get("workspace_root") or "")).expanduser().resolve()
            _preseed_external_live_root_shared_cache_mount(
                workspace_root=workspace_root,
                live_root=live_root,
                purpose=f"whole-paper source bootstrap preseed for {result_payload.get('node_id') or 'source'}",
            )
    return result_payload
