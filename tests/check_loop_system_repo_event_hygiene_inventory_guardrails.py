#!/usr/bin/env python3
"""Guard the new event-driven hygiene/heavy-object inventory surfaces."""

from __future__ import annotations

import re
import sys
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "docs" / "contracts"
LEANATLAS_ROOT = ROOT.parent
MANIFEST = LEANATLAS_ROOT / "tests" / "manifest.json"

REQ_DOC = CONTRACTS / "LOOP_SYSTEM_EVENT_CONTROL_REQUIREMENT_INVENTORY.md"
BUG_DOC = CONTRACTS / "LOOP_SYSTEM_HISTORICAL_BUG_MATRIX.md"
PARITY_DOC = CONTRACTS / "LOOP_SYSTEM_CODEX_EXEC_CAPABILITY_PARITY_MATRIX.md"
REAL_AGENT_DOC = CONTRACTS / "LOOP_SYSTEM_REAL_AGENT_GATING_MATRIX.md"


def _fail(msg: str) -> int:
    print(f"[loop-system-event-hygiene-inventory-guardrails][FAIL] {msg}", file=sys.stderr)
    return 2


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _section(text: str, heading: str) -> str:
    match = re.search(rf"^### {re.escape(heading)}\b.*?(?=^### [A-Z]{{2,3}}-\d{{3}}\b|\Z)", text, flags=re.MULTILINE | re.DOTALL)
    return match.group(0) if match else ""


def main() -> int:
    for path in (REQ_DOC, BUG_DOC, PARITY_DOC, REAL_AGENT_DOC):
        if not path.exists():
            return _fail(f"missing required inventory doc: {path}")

    req_text = _read(REQ_DOC)
    bug_text = _read(BUG_DOC)
    parity_text = _read(PARITY_DOC)
    real_agent_text = _read(REAL_AGENT_DOC)
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    tests = manifest.get("tests")
    if not isinstance(tests, list):
        return _fail("top-level tests/manifest.json must contain a tests list")
    by_id = {str(item.get("id") or ""): item for item in tests if isinstance(item, dict)}

    for heading in ("RQ-018", "RQ-019", "RQ-020"):
        section = _section(req_text, heading)
        if not section:
            return _fail(f"requirement inventory must define {heading}")
        if "Milestone gate:" not in section:
            return _fail(f"{heading} must state an explicit milestone gate")

    bug_009 = _section(bug_text, "BUG-009")
    if not bug_009:
        return _fail("historical bug matrix must define BUG-009")
    if not re.search(r"`tests/check_[^`]+\.py`", bug_009):
        return _fail("BUG-009 must reference at least one concrete regression or characterization test")
    if "Characterization baseline only:" not in bug_009:
        return _fail("BUG-009 must explicitly label characterization-only evidence")

    bug_010 = _section(bug_text, "BUG-010")
    if not bug_010:
        return _fail("historical bug matrix must define BUG-010")
    if not re.search(r"`tests/check_[^`]+\.py`", bug_010):
        return _fail("BUG-010 must reference at least one concrete regression or characterization test")
    if "Graduated from characterization-only evidence:" not in bug_010:
        return _fail("BUG-010 must explicitly record that heavy-object gap coverage graduated beyond known-gap-only status")

    ce_010 = _section(parity_text, "CE-010")
    if not ce_010:
        return _fail("codex exec parity matrix must define CE-010")
    if "Final parity gate:" not in ce_010:
        return _fail("CE-010 must state a final parity gate")
    if "Characterization baseline only:" not in ce_010:
        return _fail("CE-010 must explicitly label characterization-only evidence")

    ce_011 = _section(parity_text, "CE-011")
    if not ce_011:
        return _fail("codex exec parity matrix must define CE-011")
    if "Final parity gate:" not in ce_011:
        return _fail("CE-011 must state a final parity gate")
    if "Graduated from characterization-only evidence:" not in ce_011:
        return _fail("CE-011 must explicitly record that heavy-object parity coverage graduated beyond known-gap-only status")

    process_reap_item = by_id.get("loop_system_process_reap_gap_characterization")
    if not isinstance(process_reap_item, dict):
        return _fail("manifest missing characterization test id loop_system_process_reap_gap_characterization")
    if str(process_reap_item.get("profile") or "") != "core":
        return _fail("loop_system_process_reap_gap_characterization must stay in the core profile so the known gap is exercised continuously")
    if str(process_reap_item.get("expectation") or "") != "known_gap_baseline":
        return _fail("loop_system_process_reap_gap_characterization must declare expectation=known_gap_baseline")

    heavy_object_gap_item = by_id.get("loop_system_heavy_object_gap_characterization")
    if not isinstance(heavy_object_gap_item, dict):
        return _fail("manifest missing characterization test id loop_system_heavy_object_gap_characterization")
    if str(heavy_object_gap_item.get("profile") or "") != "core":
        return _fail("loop_system_heavy_object_gap_characterization must stay in the core profile so heavy-object authority bridging is exercised continuously")
    if str(heavy_object_gap_item.get("expectation") or "") != "standard":
        return _fail("loop_system_heavy_object_gap_characterization must declare expectation=standard after M5 authority-gap graduation")

    expected_real_agent = {f"RA-{i:03d}" for i in range(1, 6)}
    found_real_agent = set(re.findall(r"^### (RA-\d{3})\b", real_agent_text, flags=re.MULTILINE))
    if found_real_agent != expected_real_agent:
        return _fail(
            "real-agent gating matrix must freeze RA-001..RA-005 exactly; found "
            + ", ".join(sorted(found_real_agent))
        )

    required_refs = {
        "RA-001": ("RQ-018", "BUG-009", "CE-010"),
        "RA-002": ("RQ-019", "BUG-010", "CE-011"),
        "RA-005": ("RQ-017", "RQ-018", "RQ-019", "RQ-020"),
    }
    for scenario_id, refs in required_refs.items():
        section = _section(real_agent_text, scenario_id)
        if not section:
            return _fail(f"missing section for {scenario_id}")
        for ref in refs:
            if ref not in section:
                return _fail(f"{scenario_id} must reference {ref}")
        if "real" not in section.lower() or "agent" not in section.lower():
            return _fail(f"{scenario_id} must clearly describe a real-agent scenario")

    print("[loop-system-event-hygiene-inventory-guardrails][PASS] new hygiene/heavy-object requirements are frozen with deterministic and real-agent obligations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
