#!/usr/bin/env python3
"""Guard known temp-root tests to require committed cleanup authority."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-service-cleanup-convergence-guardrails][FAIL] {msg}", file=sys.stderr)
    return 2


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def main() -> int:
    required_files = {
        ROOT / "tests" / "check_loop_system_repo_cleanup_authority_runtime_identity_guard.py": "cleanup_test_repo_services(",
        ROOT / "tests" / "check_loop_system_repo_housekeeping_controller_repo_lifecycle_bootstrap.py": "cleanup_test_repo_services(",
        ROOT / "tests" / "check_loop_system_repo_housekeeping_controller_host_supervisor_bootstrap.py": "cleanup_test_repo_services(",
        ROOT / "tests" / "check_loop_system_repo_evaluator_node.py": "cleanup_test_repo_services(",
        ROOT / "tests" / "check_loop_system_repo_repo_control_plane_requirement_event.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_repo_control_plane_host_watchdog_restore.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_repo_control_plane_housekeeping_watchdog_restore.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_repo_control_plane_upper_watchdog_restore.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_repo_global_reactor_restore.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_repo_global_reactor_full_chain_restore.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_repo_global_housekeeping_chain_restore.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_repo_reactor_residency_guard_restore.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_housekeeping_controller_authority_mirror.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_housekeeping_controller_authority_reuse.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_housekeeping_reap_request_autobootstrap.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_housekeeping_controller_service_mode.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_process_reap_gap_characterization.py": "_cleanup_repo_services(repo_root)",
        ROOT / "tests" / "check_loop_system_repo_canonical_launch_started_event.py": "cleanup_test_repo_services(",
        ROOT / "tests" / "check_loop_system_repo_canonical_launch_reused_existing_event.py": "cleanup_test_repo_services(",
        ROOT / "tests" / "check_loop_system_repo_canonical_process_identity_confirmed_event.py": "cleanup_test_repo_services(",
        ROOT / "tests" / "check_loop_system_repo_runtime_loss_confirmed_event.py": "_purge_runtime_loss_roots(",
        ROOT / "tests" / "check_loop_system_repo_sidecar_runtime_context_missing_exit.py": "cleanup_test_repo_services(",
        ROOT / "tests" / "check_loop_system_repo_launch_reuse_pid_identity.py": "cleanup_test_repo_services(",
        ROOT / "tests" / "check_loop_system_repo_lease_aware_authority_view.py": "cleanup_test_repo_services(",
        ROOT / "tests" / "check_loop_system_repo_control_objects.py": "temporary_repo_root(",
        ROOT / "tests" / "check_loop_system_repo_audit_experience.py": "temporary_repo_root(",
        ROOT / "tests" / "check_loop_system_repo_evaluator_authority_status_sync.py": "temporary_repo_tree_root(",
        ROOT / "tests" / "check_loop_system_repo_child_supervision_helper.py": "_cleanup_repo_services_for_root",
        ROOT / "tests" / "check_loop_system_repo_child_runtime_status_helper.py": "_cleanup_repo_services_for_root",
        ROOT / "tests" / "check_loop_system_repo_retryable_recovery_status_refresh.py": "_cleanup_repo_services_for_root",
        ROOT / "tests" / "check_loop_system_repo_host_child_runtime_status_supervisor.py": "_cleanup_repo_services_for_root",
        ROOT / "tests" / "check_loop_system_repo_host_child_launch_supervisor.py": "_cleanup_repo_services_for_root",
        ROOT / "tests" / "check_loop_system_repo_state_surface_guardrails.py": "temporary_repo_root(",
        ROOT / "tests" / "check_loop_system_repo_publish_staging_hygiene.py": "temporary_repo_root(",
    }
    for path, needle in required_files.items():
        if not path.exists():
            return _fail(f"missing repo-service cleanup regression file {path}")
        text = _read(path)
        if needle not in text:
            return _fail(f"{path.name} must keep committed repo-service cleanup authority wired")

    for path in sorted((ROOT / "tests").glob("check_loop_system_repo_event_journal_*.py")):
        text = _read(path)
        if (
            "tempfile.TemporaryDirectory(" not in text
            and "temporary_repo_root(" not in text
            and "temporary_repo_tree_root(" not in text
            and 'Path(td) / ".loop"' not in text
        ):
            continue
        if "temporary_repo_root(" not in text and "temporary_repo_tree_root(" not in text:
            return _fail(f"{path.name} must use temporary_repo_root(...) or temporary_repo_tree_root(...) for temp-root repo-service cleanup")
        if 'Path(td) / ".loop"' in text:
            return _fail(f"{path.name} must not leave raw Path(td) temp-root wiring behind")

    print("[loop-system-repo-service-cleanup-convergence-guardrails] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
