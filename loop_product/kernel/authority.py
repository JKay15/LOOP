"""Internal authority token for kernel-owned state mutation."""

from __future__ import annotations

from dataclasses import dataclass
from inspect import stack
from pathlib import Path


@dataclass(frozen=True)
class KernelMutationAuthority:
    scope: str = "kernel"


_KERNEL_MUTATION_AUTHORITY = KernelMutationAuthority()
_PRODUCT_REPO_ROOT = Path(__file__).resolve().parents[2]
_TRUSTED_CALLER_PREFIXES = (
    "loop_product/kernel/",
    "loop_product/runtime/",
    "tests/",
)


def _caller_relative_path() -> str | None:
    module_file = Path(__file__).resolve()
    for frame_info in stack()[1:]:
        caller_file = frame_info.frame.f_globals.get("__file__")
        if not caller_file:
            continue
        caller_path = Path(str(caller_file)).resolve()
        if caller_path == module_file:
            continue
        try:
            return caller_path.relative_to(_PRODUCT_REPO_ROOT).as_posix()
        except ValueError:
            continue
    return None


def _caller_is_trusted() -> bool:
    rel = _caller_relative_path()
    if rel is None:
        return False
    return any(rel.startswith(prefix) for prefix in _TRUSTED_CALLER_PREFIXES)


def kernel_internal_authority() -> KernelMutationAuthority:
    """Return the internal authority token for kernel-owned mutations."""

    if not _caller_is_trusted():
        raise PermissionError("kernel_internal_authority is reserved for trusted kernel-owned product surfaces")
    return _KERNEL_MUTATION_AUTHORITY


def require_kernel_authority(authority: KernelMutationAuthority | None, *, surface: str) -> None:
    """Reject direct authoritative mutations that bypass the kernel surface."""

    if authority is not _KERNEL_MUTATION_AUTHORITY:
        raise PermissionError(f"{surface} requires kernel authority; use the exported kernel interaction surface")
