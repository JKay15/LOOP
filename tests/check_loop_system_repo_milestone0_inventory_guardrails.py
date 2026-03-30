#!/usr/bin/env python3
"""Guard Milestone 0 inventories for the LOOP event-control migration."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "docs" / "contracts"

REQ_DOC = CONTRACTS / "LOOP_SYSTEM_EVENT_CONTROL_REQUIREMENT_INVENTORY.md"
BUG_DOC = CONTRACTS / "LOOP_SYSTEM_HISTORICAL_BUG_MATRIX.md"
PARITY_DOC = CONTRACTS / "LOOP_SYSTEM_CODEX_EXEC_CAPABILITY_PARITY_MATRIX.md"
R59_DOC = CONTRACTS / "LOOP_SYSTEM_R59_GOLDEN_RECOVERY_ACCEPTANCE.md"
REAL_AGENT_DOC = CONTRACTS / "LOOP_SYSTEM_REAL_AGENT_GATING_MATRIX.md"
R59_CHECKPOINT = Path(
    "/Users/xiongjiangkai/xjk_papers/leanatlas/.cache/leanatlas/tmp/"
    "r59_golden_recovery_checkpoint_20260324T002141.json"
)


def _fail(msg: str) -> int:
    print(f"[loop-system-milestone0-inventory-guardrails][FAIL] {msg}", file=sys.stderr)
    return 2


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _headings(text: str, prefix: str) -> set[str]:
    return set(re.findall(rf"^### ({re.escape(prefix)}-\d{{3}})\b", text, flags=re.MULTILINE))


def _doc_test_refs(text: str) -> list[Path]:
    refs = []
    for match in re.findall(r"`(tests/check_[^`]+\.py)`", text):
        refs.append(ROOT / match)
    return refs


def main() -> int:
    for path in (REQ_DOC, BUG_DOC, PARITY_DOC, R59_DOC, REAL_AGENT_DOC):
        if not path.exists():
            return _fail(f"missing Milestone 0 inventory doc: {path}")

    if not R59_CHECKPOINT.exists():
        return _fail(f"missing preserved r59 checkpoint manifest: {R59_CHECKPOINT}")

    req_text = _read(REQ_DOC)
    bug_text = _read(BUG_DOC)
    parity_text = _read(PARITY_DOC)
    r59_text = _read(R59_DOC)
    real_agent_text = _read(REAL_AGENT_DOC)

    expected_reqs = {f"RQ-{i:03d}" for i in range(1, 21)}
    found_reqs = _headings(req_text, "RQ")
    if found_reqs != expected_reqs:
        return _fail(
            "requirement inventory must freeze RQ-001..RQ-020 exactly; found "
            + ", ".join(sorted(found_reqs))
        )

    expected_bugs = {f"BUG-{i:03d}" for i in range(1, 11)}
    found_bugs = _headings(bug_text, "BUG")
    if found_bugs != expected_bugs:
        return _fail(
            "historical bug matrix must freeze BUG-001..BUG-010 exactly; found "
            + ", ".join(sorted(found_bugs))
        )

    expected_capabilities = {f"CE-{i:03d}" for i in range(1, 12)}
    found_capabilities = _headings(parity_text, "CE")
    if found_capabilities != expected_capabilities:
        return _fail(
            "codex exec parity matrix must freeze CE-001..CE-011 exactly; found "
            + ", ".join(sorted(found_capabilities))
        )

    expected_real_agent = {f"RA-{i:03d}" for i in range(1, 6)}
    found_real_agent = _headings(real_agent_text, "RA")
    if found_real_agent != expected_real_agent:
        return _fail(
            "real-agent gating matrix must freeze RA-001..RA-005 exactly; found "
            + ", ".join(sorted(found_real_agent))
        )

    for ref in _doc_test_refs(bug_text) + _doc_test_refs(parity_text):
        if not ref.exists():
            return _fail(f"inventory references missing regression evidence: {ref}")

    for bug_id in sorted(expected_bugs):
        section_match = re.search(
            rf"^### {re.escape(bug_id)}\b.*?(?=^### BUG-\d{{3}}\b|\Z)",
            bug_text,
            flags=re.MULTILINE | re.DOTALL,
        )
        if section_match is None:
            return _fail(f"missing section for {bug_id}")
        if not re.search(r"`tests/check_[^`]+\.py`", section_match.group(0)):
            return _fail(f"{bug_id} must reference at least one concrete regression test")

    if str(R59_CHECKPOINT) not in r59_text:
        return _fail("r59 golden recovery acceptance must point at the preserved checkpoint manifest")
    if "whole_paper_integration_closeout" not in r59_text:
        return _fail("r59 golden recovery acceptance must mention the whole-paper closeout surface")
    if "bug-free completion" not in r59_text:
        return _fail("r59 golden recovery acceptance must explicitly require bug-free completion")
    if "RQ-018" not in real_agent_text or "RQ-019" not in real_agent_text or "RQ-020" not in real_agent_text:
        return _fail("real-agent gating matrix must cover the new event-driven hygiene/heavy-object requirements")

    print("[loop-system-milestone0-inventory-guardrails][PASS] Milestone 0 inventories are frozen and reference concrete evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
