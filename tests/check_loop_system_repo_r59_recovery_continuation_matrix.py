#!/usr/bin/env python3
"""Validate truthful continuation classification for the staged r59 recovery rehearsal root."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = ROOT.parent
CHECKPOINT_REF = (
    WORKSPACE_ROOT
    / ".cache"
    / "leanatlas"
    / "tmp"
    / "r59_golden_recovery_checkpoint_20260324T002141.json"
)


def _fail(msg: str) -> int:
    print(f"[loop-system-r59-recovery-continuation-matrix][FAIL] {msg}", file=sys.stderr)
    return 2


def _candidate_by_node(view: dict[str, object], node_id: str) -> dict[str, object]:
    for entry in list(view.get("continuation_candidates") or []):
        candidate = dict(entry or {})
        if str(candidate.get("node_id") or "") == node_id:
            return candidate
    return {}


def _fresh_attached_liveness_payload(*, summary: str, owner_id: str) -> dict[str, object]:
    observed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "attachment_state": "ATTACHED",
        "observation_kind": "child_supervision_heartbeat",
        "observed_at": observed_at,
        "summary": summary,
        "evidence_refs": [f"test:{owner_id}"],
        "pid_alive": True,
        "pid": 43210,
        "lease_duration_s": 3600.0,
        "lease_epoch": 9,
        "lease_owner_id": owner_id,
        "process_fingerprint": f"{owner_id}-fingerprint",
        "process_started_at_utc": observed_at,
        "runtime_ref": "test:runtime-ref",
        "status_result_ref": "test:status-result-ref",
    }


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import commit_guarded_runtime_liveness_observation_event
        from loop_product.kernel import (
            query_real_root_recovery_continuation_matrix_view,
            query_real_root_recovery_rehearsal_view,
        )
        from loop_product.kernel import query as query_module
        from loop_product.kernel import state as kernel_state_module
        from loop_product.runtime import materialize_real_root_recovery_rehearsal
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    if not CHECKPOINT_REF.exists():
        return _fail(f"missing frozen r59 checkpoint {CHECKPOINT_REF}")

    source_runtime_root = Path(
        str(query_module.load_real_root_recovery_checkpoint(checkpoint_ref=CHECKPOINT_REF).get("runtime_root") or "")
    ).resolve()
    original_gap_helper = query_module._heavy_object_authority_gap_summary
    original_pressure_helper = query_module._heavy_object_pressure_summary
    original_query_kernel_state = query_module.query_kernel_state
    original_rebuild_kernel_projections = kernel_state_module.rebuild_kernel_projections

    def _unexpected_heavy_summary(*_args, **_kwargs):
        raise AssertionError("source-root continuation preflight may not invoke heavy-object summaries")

    def _unexpected_query_kernel_state(*_args, **_kwargs):
        raise AssertionError("source-root continuation preflight may not invoke query_kernel_state self-heal")

    def _unexpected_rebuild_kernel_projections(*_args, **_kwargs):
        raise AssertionError("source-root continuation preflight may not rebuild kernel projections")

    query_module._heavy_object_authority_gap_summary = _unexpected_heavy_summary
    query_module._heavy_object_pressure_summary = _unexpected_heavy_summary
    query_module.query_kernel_state = _unexpected_query_kernel_state
    kernel_state_module.rebuild_kernel_projections = _unexpected_rebuild_kernel_projections
    try:
        source_matrix = query_real_root_recovery_continuation_matrix_view(
            source_runtime_root,
            checkpoint_ref=CHECKPOINT_REF,
        )
    except AssertionError as exc:
        return _fail(str(exc))
    finally:
        query_module._heavy_object_authority_gap_summary = original_gap_helper
        query_module._heavy_object_pressure_summary = original_pressure_helper
        query_module.query_kernel_state = original_query_kernel_state
        kernel_state_module.rebuild_kernel_projections = original_rebuild_kernel_projections
    source_summary = dict(source_matrix.get("continuation_summary") or {})
    if str(source_matrix.get("compatibility_status") or "") != "compatible":
        return _fail("source-root continuation preflight must preserve compatible mixed-version truth")
    if bool(source_summary.get("fake_live_regression_visible")):
        return _fail("source-root continuation preflight may not regress to fake-live truth")
    source_utility = _candidate_by_node(dict(source_matrix.get("rehearsal_view") or {}), "utility_learning_probabilistic_rate_chain")
    if source_utility:
        return _fail("source-root continuation preflight must not surface utility_learning_probabilistic_rate_chain while fresh attached liveness is still committed")

    with temporary_repo_root(prefix="loop_system_r59_recovery_continuation_matrix_") as repo_root:
        materialized = materialize_real_root_recovery_rehearsal(
            checkpoint_ref=CHECKPOINT_REF,
            repo_root=repo_root,
            include_rebasing_summary=False,
        )
        state_root = Path(str(materialized.get("runtime_root") or "")).resolve()

        rehearsal = query_real_root_recovery_rehearsal_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
        )
        summary = dict(rehearsal.get("continuation_summary") or {})
        if bool(summary.get("fake_live_regression_visible")):
            return _fail("continuation summary may not regress to fake-live truth")
        probabilistic = _candidate_by_node(rehearsal, "utility_learning_probabilistic_rate_chain")
        if probabilistic:
            return _fail("utility_learning_probabilistic_rate_chain must not remain a continuation candidate while fresh attached liveness is still committed")

        matrix = query_real_root_recovery_continuation_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            current_rehearsal_view=rehearsal,
        )
        if str(matrix.get("alignment_state") or "") != "generic_surfaces_aligned":
            return _fail(f"continuation matrix must report generic_surfaces_aligned, got {matrix.get('alignment_state')!r}")
        if dict(matrix.get("continuation_summary") or {}) != summary:
            return _fail("continuation matrix must preserve the rehearsal continuation_summary verbatim")
        if str(dict(matrix.get("mixed_version_trace") or {}).get("compatibility_status") or "") != "compatible":
            return _fail("continuation matrix must preserve compatible mixed-version truth")
        if str(dict(matrix.get("crash_consistency_matrix") or {}).get("compatibility_status") or "") != "compatible":
            return _fail("continuation matrix must preserve compatible crash-consistency truth")
        if str(dict(matrix.get("lease_fencing_matrix") or {}).get("compatibility_status") or "") != "compatible":
            return _fail("continuation matrix must preserve compatible lease-fencing truth")
        crash_summary = dict(matrix.get("crash_consistency_summary") or {})
        lease_summary = dict(matrix.get("lease_fencing_summary") or {})
        blocker_ids = list(matrix.get("readiness_blockers") or [])
        if bool(crash_summary.get("window_open")) and not any(
            str(item).startswith("projection_crash_window_open:")
            for item in blocker_ids
        ):
            return _fail("continuation matrix must name an open projection crash window as a readiness blocker")
        if str(lease_summary.get("matrix_state") or "") == "lease_truth_drift_visible" and "lease_truth_drift_visible" not in blocker_ids:
            return _fail("continuation matrix must name visible lease-truth drift as a readiness blocker")

        fresh_liveness = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="r59-continuation-fresh-attached-utility",
            node_id="utility_learning_probabilistic_rate_chain",
            payload=_fresh_attached_liveness_payload(
                summary="fresh attached liveness should remove the node from recovery candidates",
                owner_id="test-sidecar:r59-continuation-fresh-attached",
            ),
            producer="loop_system.tests.r59_continuation_fresh_attached",
        )
        if str(fresh_liveness.get("outcome") or "") != "committed":
            return _fail(f"fresh attached liveness injection must commit, got {fresh_liveness!r}")

        refreshed_rehearsal = query_real_root_recovery_rehearsal_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            current_rebasing=rehearsal,
        )
        refreshed_summary = dict(refreshed_rehearsal.get("continuation_summary") or {})
        if refreshed_summary != summary:
            return _fail(
                "fresh attached committed liveness must preserve the already-truthful continuation summary once "
                f"utility_learning_probabilistic_rate_chain is no longer a recovery candidate, got {refreshed_summary!r}"
            )
        refreshed_probabilistic = _candidate_by_node(refreshed_rehearsal, "utility_learning_probabilistic_rate_chain")
        if refreshed_probabilistic:
            return _fail("fresh attached committed liveness must remove utility_learning_probabilistic_rate_chain from continuation candidates")

    print(
        "[loop-system-r59-recovery-continuation-matrix][OK] staged r59 rehearsal root exposes truthful continuation "
        "truth, aligned generic hardening surfaces, and fresh-attached candidate suppression"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
