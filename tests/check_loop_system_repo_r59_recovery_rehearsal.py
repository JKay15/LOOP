#!/usr/bin/env python3
"""Validate canonical r59 recovery rehearsal materialization and read-only truth."""

from __future__ import annotations

import sys
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
    print(f"[loop-system-r59-recovery-rehearsal][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.kernel import query_real_root_recovery_rehearsal_view
        from loop_product.kernel import query as query_module
        from loop_product.runtime import materialize_real_root_recovery_rehearsal
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    if not CHECKPOINT_REF.exists():
        return _fail(f"missing frozen r59 checkpoint {CHECKPOINT_REF}")

    source_runtime_root = Path(
        str(query_module.load_real_root_recovery_checkpoint(checkpoint_ref=CHECKPOINT_REF).get("runtime_root") or "")
    ).resolve()

    original_rebasing_helper = query_module.summarize_real_root_recovery_rebasing

    def _unexpected_source_root_scan(**_kwargs):
        raise AssertionError("source-root preflight may not run the rebasing scan")

    query_module.summarize_real_root_recovery_rebasing = _unexpected_source_root_scan
    try:
        source_root_view = query_real_root_recovery_rehearsal_view(
            source_runtime_root,
            checkpoint_ref=CHECKPOINT_REF,
        )
    except AssertionError as exc:
        return _fail(str(exc))
    finally:
        query_module.summarize_real_root_recovery_rebasing = original_rebasing_helper

    if str(source_root_view.get("rehearsal_state") or "") != "source_root":
        return _fail("real r59 source root must report rehearsal_state=source_root")
    if str(source_root_view.get("rebasing_scan_state") or "") != "not_applicable_source_root":
        return _fail("real r59 source root must report rebasing_scan_state=not_applicable_source_root")
    if bool(source_root_view.get("path_rebase_pending")):
        return _fail("real r59 source-root preflight may not claim staged path rebasing is pending")
    if int(source_root_view.get("scanned_text_file_count") or 0) != 0:
        return _fail("real r59 source-root preflight may not scan text files for rebasing leakage")

    with temporary_repo_root(prefix="loop_system_r59_recovery_rehearsal_") as repo_root:
        materialized = materialize_real_root_recovery_rehearsal(
            checkpoint_ref=CHECKPOINT_REF,
            repo_root=repo_root,
        )
        state_root = Path(str(materialized.get("runtime_root") or "")).resolve()
        if state_root != (repo_root / ".loop" / "arxiv_2602_11505v2_whole_paper_faithful_formalization_benchmark_r59").resolve():
            return _fail(f"unexpected materialized runtime root {state_root}")
        if not state_root.exists():
            return _fail("materialized r59 runtime root must exist")
        if str(materialized.get("checkpoint_ref") or "") != str(CHECKPOINT_REF.resolve()):
            return _fail("materialization receipt must preserve the checkpoint ref")
        if str(materialized.get("source_runtime_root") or "") == str(state_root):
            return _fail("materialized runtime root must not alias the original frozen r59 runtime root")
        if int(materialized.get("runtime_root_replacement_count") or 0) <= 0:
            return _fail("materialization must rewrite old runtime-root absolute paths")
        if int(materialized.get("repo_root_replacement_count") or 0) <= 0:
            return _fail("materialization must rewrite old repo-root absolute paths")
        if int(materialized.get("remaining_source_runtime_root_refs") or 0) != 0:
            return _fail(
                "materialization may not leave old source runtime-root refs behind: "
                f"{materialized.get('remaining_source_runtime_root_refs')!r}"
            )
        if int(materialized.get("remaining_source_repo_root_refs") or 0) != 0:
            return _fail(
                "materialization may not leave old source repo-root refs behind: "
                f"{materialized.get('remaining_source_repo_root_refs')!r}"
            )

        kernel_state_ref = state_root / "state" / "kernel_state.json"
        kernel_state_text = kernel_state_ref.read_text(encoding="utf-8")
        if str(materialized.get("source_runtime_root") or "") in kernel_state_text:
            return _fail("kernel_state.json must not retain the old source runtime root after rebasing")
        if str(materialized.get("source_repo_root") or "") in kernel_state_text:
            return _fail("kernel_state.json must not retain the old source repo root after rebasing")

        rehearsal = query_real_root_recovery_rehearsal_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            current_rebasing=materialized,
        )
        if str(rehearsal.get("compatibility_status") or "") != "compatible":
            return _fail(f"materialized rehearsal root must stay compatible: {rehearsal.get('compatibility_status')!r}")
        if str(rehearsal.get("rehearsal_state") or "") != "isolated_materialized":
            return _fail(
                "rehearsal state must report isolated_materialized after helper staging, got "
                f"{rehearsal.get('rehearsal_state')!r}"
            )
        if not bool(rehearsal.get("rehearsal_ready")):
            return _fail("materialized r59 rehearsal root must report rehearsal_ready")
        if list(rehearsal.get("gaps") or []):
            return _fail(f"materialized r59 rehearsal view must not report gaps: {rehearsal.get('gaps')!r}")
        if bool(rehearsal.get("path_rebase_pending")):
            return _fail("materialized r59 rehearsal view must not report pending path rebasing")

        node_alignment = dict(rehearsal.get("checkpoint_node_alignment") or {})
        appendix = dict(node_alignment.get("utility_learning_appendix_formalization") or {})
        if str(appendix.get("current_status") or "") != "COMPLETED":
            return _fail("appendix node must remain COMPLETED in the rehearsal root")
        if str(appendix.get("current_attachment_state") or "") != "TERMINAL":
            return _fail("appendix node must remain TERMINAL in the rehearsal root")

        probabilistic = dict(node_alignment.get("utility_learning_probabilistic_rate_chain") or {})
        if str(probabilistic.get("current_status") or "") != "ACTIVE":
            return _fail("probabilistic rate chain must preserve ACTIVE checkpoint truth")
        if str(probabilistic.get("current_attachment_state") or "") != "UNOBSERVED":
            return _fail("probabilistic rate chain must preserve UNOBSERVED attachment truth")
        if bool(probabilistic.get("current_live_process_trusted")):
            return _fail("probabilistic rate chain may not regress to fake-live trusted process truth")

        closeout = dict(node_alignment.get("whole_paper_integration_closeout") or {})
        if str(closeout.get("current_status") or "") != "PLANNED":
            return _fail("whole-paper closeout node must preserve PLANNED checkpoint truth")
        if str(closeout.get("current_attachment_state") or "") != "UNOBSERVED":
            return _fail("whole-paper closeout node must preserve UNOBSERVED attachment truth")

        if str(rehearsal.get("checkpoint_goal") or "").strip() == "":
            return _fail("rehearsal view must preserve the frozen checkpoint goal")
        if int(rehearsal.get("checkpoint_node_count") or 0) != 5:
            return _fail("rehearsal view must preserve the frozen checkpoint node count")

    print("[loop-system-r59-recovery-rehearsal][OK] r59 checkpoint materializes into an isolated rehearsal root with rebased truth and aligned node snapshots")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
