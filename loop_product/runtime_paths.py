"""Shared runtime and product-layout path helpers."""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path
import re

_PACKAGE_ROOT = Path(str(files("loop_product"))).resolve()
_PRODUCT_REPO_ROOT = _PACKAGE_ROOT.parent.resolve()
_DOCS_ROOT = _PRODUCT_REPO_ROOT / "docs"
_CONTRACTS_ROOT = _DOCS_ROOT / "contracts"
_SCHEMAS_ROOT = _DOCS_ROOT / "schemas"
_IMPLEMENTER_WORKSPACE_ROOT = _PRODUCT_REPO_ROOT / "workspace"
_STATE_SCOPE_ROOT = _PRODUCT_REPO_ROOT / ".loop"


def _safe_workspace_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "__", str(name or "").strip()).strip("._-") or "node"


def product_package_root() -> Path:
    """Return the committed `loop_product/` package root on disk."""

    return _PACKAGE_ROOT


def product_repo_root() -> Path:
    """Return the standalone product-repo root that owns docs and package surfaces."""

    return _PRODUCT_REPO_ROOT


def shared_cache_helper_ref() -> Path:
    """Return the canonical committed shared-cache helper path.

    The helper currently lives in the outer LeanAtlas repo rather than the
    standalone `loop_product_repo/` tree. Prefer the outer committed path when
    it exists, but keep a deterministic fallback for environments that may
    vendor the helper into the product repo later.
    """

    outer_candidate = (_PRODUCT_REPO_ROOT.parent / "scripts" / "ensure_workspace_lake_packages.sh").resolve()
    if outer_candidate.exists():
        return outer_candidate
    inner_candidate = (_PRODUCT_REPO_ROOT / "scripts" / "ensure_workspace_lake_packages.sh").resolve()
    if inner_candidate.exists():
        return inner_candidate
    return outer_candidate


def default_workspace_root() -> Path:
    """Return the default product workspace root for repo-local runs."""

    return _PRODUCT_REPO_ROOT


def state_scope_root() -> Path:
    """Return the committed `.loop/` scope root for runtime state trees."""

    return _STATE_SCOPE_ROOT


def implementer_workspace_root() -> Path:
    """Return the committed implementer workspace scope root."""

    return _IMPLEMENTER_WORKSPACE_ROOT


def safe_runtime_name(name: str) -> str:
    """Return a filesystem-safe deterministic name for workspace/state-root allocation."""

    return _safe_workspace_name(name)


def default_child_workspace_root(*, state_root: str | Path, node_id: str) -> Path:
    """Return the default run-scoped workspace root for one kernel-materialized child."""

    runtime_name = safe_runtime_name(Path(state_root).expanduser().resolve().name)
    return (implementer_workspace_root().resolve() / runtime_name / _safe_workspace_name(node_id)).resolve()


def resolve_implementer_project_root(*, node_id: str, workspace_root: str | Path | None = None) -> Path:
    """Resolve one implementer project directory under the committed workspace scope."""

    base_root = implementer_workspace_root().resolve()
    candidate = Path(workspace_root).expanduser() if workspace_root not in (None, "") else base_root / _safe_workspace_name(node_id)
    if not candidate.is_absolute():
        candidate = base_root / candidate
    resolved = candidate.resolve()
    if base_root != resolved and base_root not in resolved.parents:
        raise ValueError(f"implementer workspace must stay under {base_root}: {resolved}")
    return resolved


def product_contract_path(name: str) -> Path:
    """Return an absolute path to a repo-shipped contract document."""

    return (_CONTRACTS_ROOT / name).resolve()


def product_schema_path(name: str) -> Path:
    """Return an absolute path to a repo-shipped schema document."""

    return (_SCHEMAS_ROOT / name).resolve()


def require_runtime_root(state_root: Path) -> Path:
    """Reject runtime roots that are not anchored under a `.loop` boundary."""

    root = Path(state_root)
    if ".loop" not in root.resolve().parts:
        raise ValueError(f"runtime state_root must resolve under a .loop boundary: {root}")
    return root


def node_live_artifact_root(*, state_root: Path, node_id: str, workspace_mirror_relpath: str) -> Path:
    """Return a runtime-owned live artifact scratch root for one node.

    This root intentionally lives under the durable state tree instead of the
    workspace so local package/build hydration does not contaminate the
    implementer workspace or shipped publish root.
    """

    runtime_root = require_runtime_root(state_root).resolve()
    safe_node_id = _safe_workspace_name(node_id)
    publish_path = Path(str(workspace_mirror_relpath or "").strip() or "deliverables/primary_artifact")
    leaf_name = publish_path.name or "primary_artifact"
    return (runtime_root / "artifacts" / "live_artifacts" / safe_node_id / leaf_name).resolve()


def node_machine_handoff_ref(*, state_root: Path, node_id: str) -> Path:
    """Return the runtime-owned machine handoff ref for one node.

    Machine-only replay/control payloads must stay outside the agent-visible
    workspace. The workspace may contain human-safe markdown and wrapper
    entrypoints, but not the raw JSON handoff.
    """

    runtime_root = require_runtime_root(state_root).resolve()
    safe_node_id = _safe_workspace_name(node_id)
    return (runtime_root / "artifacts" / "bootstrap" / safe_node_id / "FROZEN_HANDOFF.json").resolve()
