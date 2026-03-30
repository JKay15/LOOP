"""Shared test helpers for product-repo contract tests."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import tempfile
import time


def _cleanup_repo_tree_services(tree_root: Path) -> None:
    """Retire every nested repo root that materialized under a temp tree."""

    from loop_product.runtime import cleanup_repo_tree_services

    receipts: dict[str, dict] = {}
    for _attempt in range(3):
        receipt = cleanup_repo_tree_services(
            tree_root=tree_root,
            settle_timeout_s=4.0,
            poll_interval_s=0.05,
            cleanup_kind="test_repo_tree_cleanup",
            producer="test_support.temporary_repo_tree_root",
        )
        repo_roots = list(receipt.get("repo_roots") or [])
        if not repo_roots:
            return
        receipts = {
            str(item.get("repo_root") or ""): dict(item or {})
            for item in list(receipt.get("repo_receipts") or [])
        }
        if all(dict(receipt).get("quiesced") for receipt in receipts.values()):
            return
        time.sleep(0.25)
    raise RuntimeError(
        "temporary_repo_tree_root cleanup failed to quiesce nested repo services: "
        f"{receipts}"
    )


@contextmanager
def temporary_repo_root(*, prefix: str):
    """Yield a temporary repo root and retire repo-global services before deletion."""

    from loop_product.runtime import cleanup_test_repo_services

    with tempfile.TemporaryDirectory(prefix=prefix) as td:
        repo_root = Path(td)
        try:
            yield repo_root
        finally:
            if repo_root.exists():
                receipt = cleanup_test_repo_services(
                    repo_root=repo_root,
                    settle_timeout_s=4.0,
                    poll_interval_s=0.05,
                )
                # Late-bootstrapping repo services can appear right as cleanup begins.
                # Require a second deterministic sweep instead of silently leaking them.
                if not receipt.get("quiesced"):
                    time.sleep(0.25)
                    receipt = cleanup_test_repo_services(
                        repo_root=repo_root,
                        settle_timeout_s=4.0,
                        poll_interval_s=0.05,
                    )
                if not receipt.get("quiesced"):
                    raise RuntimeError(
                        "temporary_repo_root cleanup failed to quiesce repo services: "
                        f"{receipt}"
                    )


@contextmanager
def temporary_repo_tree_root(*, prefix: str):
    """Yield a temp parent that may contain multiple repo roots, then retire them all."""

    with tempfile.TemporaryDirectory(prefix=prefix) as td:
        tree_root = Path(td)
        try:
            yield tree_root
        finally:
            if tree_root.exists():
                _cleanup_repo_tree_services(tree_root)
