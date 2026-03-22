#!/usr/bin/env python3
"""Validate structured control-intent choices replace prose guessing for critical routing."""

from __future__ import annotations

import sys


def _fail(msg: str) -> int:
    print(f"[loop-system-control-intent][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    try:
        from loop_product.control_intent import (
            ARTIFACT_SCOPE_SPEC,
            TERMINAL_AUTHORITY_SCOPE_SPEC,
            WORKFLOW_SCOPE_SPEC,
            normalize_activation_condition,
            resolve_machine_choice,
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(f"control-intent surface imports failed: {exc}")

    workflow = resolve_machine_choice(
        {"workflow_scope": "whole_paper_formalization"},
        WORKFLOW_SCOPE_SPEC,
    )
    if workflow.value != "whole_paper_formalization":
        return _fail("explicit workflow_scope must survive machine-choice normalization")
    if workflow.source != "explicit":
        return _fail("explicit workflow_scope must be recorded as explicit instead of guessed")

    invalid_terminal = resolve_machine_choice(
        {"terminal_authority_scope": "paper defect exposed"},
        TERMINAL_AUTHORITY_SCOPE_SPEC,
    )
    if invalid_terminal.value != "local":
        return _fail("invalid terminal_authority_scope prose must fail closed to the safe default local scope")
    if invalid_terminal.source != "defaulted_invalid":
        return _fail("invalid machine choice must be marked as defaulted_invalid instead of being accepted as prose")

    implicit_artifact = resolve_machine_choice({}, ARTIFACT_SCOPE_SPEC)
    if implicit_artifact.value != "task":
        return _fail("artifact_scope must default to task for unspecialized nodes")
    if implicit_artifact.source != "defaulted_missing":
        return _fail("missing artifact_scope must be recorded as defaulted_missing")

    if normalize_activation_condition("after:linear_core:terminal") != "after:linear_core:terminal":
        return _fail("machine activation_condition syntax must remain stable")
    if normalize_activation_condition("wait until the linear core is done") != "":
        return _fail("prose activation_condition must never survive as a machine gate")

    print("[loop-system-control-intent] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
