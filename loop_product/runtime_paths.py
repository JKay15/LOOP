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
