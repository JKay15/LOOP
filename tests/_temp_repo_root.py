"""Shared temp repo-root helper for tests that may bootstrap repo-global services."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import tempfile


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
                cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
