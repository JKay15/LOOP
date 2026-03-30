#!/usr/bin/env python3
"""Guard that Milestone 3 closeout evidence stays frozen and complete."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLOSEOUT_DOC = ROOT / "docs" / "contracts" / "LOOP_SYSTEM_EVENT_JOURNAL_MILESTONE3_CLOSEOUT.md"


REQUIRED_TEST_REFS = {
    "tests/check_loop_system_repo_canonical_launch_started_event.py",
    "tests/check_loop_system_repo_canonical_launch_reused_existing_event.py",
    "tests/check_loop_system_repo_canonical_process_identity_confirmed_event.py",
    "tests/check_loop_system_repo_canonical_supervision_attached_event.py",
    "tests/check_loop_system_repo_canonical_heartbeat_observed_event.py",
    "tests/check_loop_system_repo_supervision_exit_events.py",
    "tests/check_loop_system_repo_runtime_loss_confirmed_event.py",
    "tests/check_loop_system_repo_canonical_lease_expired_event.py",
    "tests/check_loop_system_repo_canonical_supervision_detached_event.py",
    "tests/check_loop_system_repo_lease_expiry_materialization.py",
    "tests/check_loop_system_repo_lease_aware_authority_view.py",
    "tests/check_loop_system_repo_state_query.py",
    "tests/check_loop_system_repo_event_journal_liveness_observations.py",
    "tests/check_loop_system_repo_event_journal_liveness_freshness.py",
    "tests/check_loop_system_repo_event_journal_liveness_epoch_guard.py",
    "tests/check_loop_system_repo_event_journal_liveness_writer_guard.py",
    "tests/check_loop_system_repo_event_journal_liveness_owner_guard.py",
    "tests/check_loop_system_repo_event_journal_liveness_pid_guard.py",
    "tests/check_loop_system_repo_event_journal_liveness_fingerprint_guard.py",
    "tests/check_loop_system_repo_event_journal_liveness_launch_identity_guard.py",
    "tests/check_loop_system_repo_event_journal_liveness_start_evidence_guard.py",
    "tests/check_loop_system_repo_runtime_status_liveness_mirror.py",
    "tests/check_loop_system_repo_child_supervision_liveness_mirror.py",
    "tests/check_loop_system_repo_child_supervision_heartbeat_mirror.py",
    "tests/check_loop_system_repo_sidecar_cleanup_authority_guard.py",
    "tests/check_loop_system_repo_codex_exec_capability_parity_foundation.py",
    "tests/check_loop_system_repo_event_reap_command_foundation.py",
    "tests/check_loop_system_repo_canonical_reap_vocabulary.py",
    "tests/check_loop_system_repo_reap_reconciler_foundation.py",
    "tests/check_loop_system_repo_housekeeping_reap_controller.py",
    "tests/check_loop_system_repo_housekeeping_reap_controller_loop.py",
    "tests/check_loop_system_repo_housekeeping_reap_request_autobootstrap.py",
    "tests/check_loop_system_repo_housekeeping_controller_service_mode.py",
    "tests/check_loop_system_repo_housekeeping_controller_authority_mirror.py",
    "tests/check_loop_system_repo_housekeeping_controller_authority_reuse.py",
    "tests/check_loop_system_repo_housekeeping_controller_authoritative_exit.py",
    "tests/check_loop_system_repo_housekeeping_controller_lease_fencing.py",
    "tests/check_loop_system_repo_housekeeping_controller_proactive_bootstrap.py",
    "tests/check_loop_system_repo_housekeeping_controller_host_supervisor_bootstrap.py",
    "tests/check_loop_system_repo_housekeeping_controller_repo_lifecycle_bootstrap.py",
    "tests/check_loop_system_repo_host_supervisor_authority_mirror.py",
    "tests/check_loop_system_repo_host_supervisor_authority_reuse.py",
    "tests/check_loop_system_repo_host_supervisor_authoritative_exit.py",
    "tests/check_loop_system_repo_host_supervisor_lease_fencing.py",
    "tests/check_loop_system_repo_repo_control_plane_bootstrap.py",
    "tests/check_loop_system_repo_repo_control_plane_service_loop.py",
    "tests/check_loop_system_repo_repo_control_plane_authority_mirror.py",
    "tests/check_loop_system_repo_repo_control_plane_authority_reuse.py",
    "tests/check_loop_system_repo_repo_control_plane_authoritative_exit.py",
    "tests/check_loop_system_repo_repo_control_plane_lease_fencing.py",
    "tests/check_loop_system_repo_repo_control_plane_repo_lifecycle_bootstrap.py",
    "tests/check_loop_system_repo_repo_control_plane_runtime_tree_bootstrap.py",
    "tests/check_loop_system_repo_repo_control_plane_first_child_bootstrap.py",
    "tests/check_loop_system_repo_repo_control_plane_authority_query_bootstrap.py",
    "tests/check_loop_system_repo_repo_control_plane_status_query_bootstrap.py",
    "tests/check_loop_system_repo_repo_control_plane_requirement_event.py",
    "tests/check_loop_system_repo_repo_control_plane_host_watchdog_restore.py",
    "tests/check_loop_system_repo_repo_control_plane_housekeeping_watchdog_restore.py",
    "tests/check_loop_system_repo_repo_control_plane_upper_watchdog_restore.py",
    "tests/check_loop_system_repo_repo_global_reactor_restore.py",
    "tests/check_loop_system_repo_repo_global_reactor_full_chain_restore.py",
    "tests/check_loop_system_repo_repo_global_housekeeping_chain_restore.py",
    "tests/check_loop_system_repo_repo_reactor_residency_guard_restore.py",
    "tests/check_loop_system_repo_repo_service_cleanup_authority.py",
    "tests/check_loop_system_repo_repo_service_cleanup_orphan_fallback.py",
    "tests/check_loop_system_repo_repo_service_cleanup_convergence_guardrails.py",
    "tests/check_loop_system_repo_process_hygiene.py",
    "tests/check_loop_system_repo_launch_reuse_pid_identity.py",
    "tests/check_loop_system_repo_retryable_recovery_status_refresh.py",
    "tests/check_loop_system_repo_host_child_launch_supervisor.py",
    "tests/check_loop_system_repo_child_runtime_status_helper.py",
    "tests/check_loop_system_repo_milestone0_inventory_guardrails.py",
}

REQUIRED_DOC_SNIPPETS = (
    "canonical lease/liveness/reap control plane",
    "launch/supervision/heartbeat/exit/lease-expiry canonical facts",
    "lease-aware runtime projection and truthful ACTIVE downgrade",
    "lease epoch / fencing model and stale-observer rejection",
    "`codex exec` capability-parity harness for the lease model",
    "event-driven reap/defer/reap-confirmed model for orphaned sidecars, supervisors, and abandoned runtime stacks",
    "Milestone 3 does not complete the command-migration/parity-debugger work from Milestone 4.",
    "Milestone 3 does not complete the canonical heavy-object lifecycle/share/retention model from Milestone 5.",
    "legal ambient residency entrypoint for repo-global services",
    "surviving lease-owned repo-global services may restore the next layer up through canonical requirement facts",
    "repo_control_plane_required",
    "python tests/run.py --profile core",
)


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-milestone3-closeout][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if not CLOSEOUT_DOC.exists():
        return _fail(f"missing Milestone 3 closeout doc: {CLOSEOUT_DOC}")

    text = CLOSEOUT_DOC.read_text(encoding="utf-8")
    for snippet in REQUIRED_DOC_SNIPPETS:
        if snippet not in text:
            return _fail(f"Milestone 3 closeout doc missing required snippet: {snippet!r}")

    found_refs = set(re.findall(r"`(tests/check_[^`]+\.py)`", text))
    missing_refs = sorted(REQUIRED_TEST_REFS - found_refs)
    if missing_refs:
        return _fail("Milestone 3 closeout doc missing required evidence refs: " + ", ".join(missing_refs))

    for rel in sorted(found_refs):
        path = ROOT / rel
        if not path.exists():
            return _fail(f"Milestone 3 closeout evidence ref missing on disk: {path}")

    print("[loop-system-event-journal-milestone3-closeout][OK] Milestone 3 closeout evidence is frozen and complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
