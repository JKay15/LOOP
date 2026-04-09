"""Shared split proposal contract parsing and validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json


@dataclass(frozen=True)
class SplitCoverageUnit:
    id: str
    requirement_text: str


@dataclass(frozen=True)
class SplitChildSpec:
    name: str
    final_effects_file: str
    covered_unit_ids: tuple[str, ...]


@dataclass(frozen=True)
class SplitParentAfterMergeSpec:
    final_effects_file: str
    covered_unit_ids: tuple[str, ...]


@dataclass(frozen=True)
class SplitProposal:
    version: int
    coverage_units: tuple[SplitCoverageUnit, ...]
    children: tuple[SplitChildSpec, ...]
    parent_after_merge: SplitParentAfterMergeSpec | None


def _raise(error_type: type[Exception], message: str) -> None:
    raise error_type(message)


def _resolve_bundle_file(
    *,
    bundle_dir: Path,
    relative_path: str,
    error_type: type[Exception],
    label: str,
) -> Path:
    candidate_path = (bundle_dir / relative_path).resolve()
    try:
        candidate_path.relative_to(bundle_dir)
    except ValueError as exc:
        _raise(
            error_type,
            f"{label} must stay inside the bundle directory",
        )
    if not candidate_path.exists() or not candidate_path.is_file():
        _raise(
            error_type,
            f"{label} does not exist",
        )
    return candidate_path


def _normalize_covered_unit_ids(
    *,
    covered_unit_ids_raw: object,
    known_unit_ids: set[str],
    error_type: type[Exception],
    label: str,
) -> tuple[str, ...]:
    if not isinstance(covered_unit_ids_raw, list) or not covered_unit_ids_raw:
        _raise(error_type, f"{label} must define a non-empty covered_unit_ids list")
    normalized_ids: list[str] = []
    seen_ids: set[str] = set()
    for item in covered_unit_ids_raw:
        unit_id = str(item or "").strip()
        if not unit_id:
            _raise(error_type, f"{label} covered_unit_ids entries must be non-empty strings")
        if unit_id not in known_unit_ids:
            _raise(error_type, f"{label} covered_unit_id {unit_id!r} is not declared in coverage_units")
        if unit_id in seen_ids:
            _raise(error_type, f"{label} covered_unit_id {unit_id!r} must not repeat")
        seen_ids.add(unit_id)
        normalized_ids.append(unit_id)
    return tuple(normalized_ids)


def parse_split_bundle(
    bundle_ref: str | Path,
    *,
    error_type: type[Exception] = ValueError,
) -> tuple[Path, SplitProposal]:
    bundle_dir = Path(bundle_ref).expanduser().resolve()
    if not bundle_dir.exists() or not bundle_dir.is_dir():
        _raise(error_type, "split bundle must exist and be a directory")
    proposal_path = (bundle_dir / "proposal.json").resolve()
    if not proposal_path.exists() or not proposal_path.is_file():
        _raise(error_type, "split bundle must contain proposal.json")
    try:
        proposal_raw = json.loads(proposal_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        _raise(error_type, "split bundle proposal.json must be valid JSON")
    if not isinstance(proposal_raw, dict):
        _raise(error_type, "split bundle proposal.json must decode to an object")
    if int(proposal_raw.get("version") or 0) != 2:
        _raise(error_type, "split bundle proposal.json must use version=2")

    coverage_units_raw = proposal_raw.get("coverage_units")
    if not isinstance(coverage_units_raw, list) or not coverage_units_raw:
        _raise(error_type, "split bundle proposal.json must contain a non-empty coverage_units list")
    coverage_units: list[SplitCoverageUnit] = []
    coverage_unit_ids: set[str] = set()
    for index, unit_raw in enumerate(coverage_units_raw, start=1):
        if not isinstance(unit_raw, dict):
            _raise(error_type, f"coverage_units entry #{index} must be an object")
        unit_id = str(unit_raw.get("id") or "").strip()
        requirement_text = str(unit_raw.get("requirement_text") or "").strip()
        if not unit_id:
            _raise(error_type, f"coverage_units entry #{index} must define a non-empty id")
        if not requirement_text:
            _raise(error_type, f"coverage_units entry {unit_id!r} must define non-empty requirement_text")
        if unit_id in coverage_unit_ids:
            _raise(error_type, f"coverage_units id {unit_id!r} must be unique")
        coverage_unit_ids.add(unit_id)
        coverage_units.append(
            SplitCoverageUnit(
                id=unit_id,
                requirement_text=requirement_text,
            )
        )

    children_raw = proposal_raw.get("children")
    if not isinstance(children_raw, list) or not children_raw:
        _raise(error_type, "split bundle proposal.json must contain a non-empty children list")
    children: list[SplitChildSpec] = []
    child_names: set[str] = set()
    assignments: dict[str, str] = {}
    for index, child_raw in enumerate(children_raw, start=1):
        if not isinstance(child_raw, dict):
            _raise(error_type, f"split bundle child #{index} must be an object")
        child_name = str(child_raw.get("name") or "").strip()
        rel_final_effects = str(child_raw.get("final_effects_file") or "").strip()
        if not child_name:
            _raise(error_type, f"split bundle child #{index} must define a non-empty name")
        if child_name in child_names:
            _raise(error_type, f"split bundle child name {child_name!r} must be unique")
        if not rel_final_effects:
            _raise(error_type, f"split bundle child {child_name!r} must define final_effects_file")
        _resolve_bundle_file(
            bundle_dir=bundle_dir,
            relative_path=rel_final_effects,
            error_type=error_type,
            label=f"split bundle child {child_name!r} final_effects_file",
        )
        covered_unit_ids = _normalize_covered_unit_ids(
            covered_unit_ids_raw=child_raw.get("covered_unit_ids"),
            known_unit_ids=coverage_unit_ids,
            error_type=error_type,
            label=f"split bundle child {child_name!r}",
        )
        for unit_id in covered_unit_ids:
            owner = f"child {child_name!r}"
            if unit_id in assignments:
                _raise(
                    error_type,
                    f"coverage unit {unit_id!r} is assigned more than once ({assignments[unit_id]} and {owner})",
                )
            assignments[unit_id] = owner
        child_names.add(child_name)
        children.append(
            SplitChildSpec(
                name=child_name,
                final_effects_file=rel_final_effects,
                covered_unit_ids=covered_unit_ids,
            )
        )

    parent_after_merge_raw = proposal_raw.get("parent_after_merge")
    parent_after_merge: SplitParentAfterMergeSpec | None = None
    if parent_after_merge_raw is not None:
        if not isinstance(parent_after_merge_raw, dict):
            _raise(error_type, "split bundle parent_after_merge must be an object")
        rel_parent_final_effects = str(parent_after_merge_raw.get("final_effects_file") or "").strip()
        if not rel_parent_final_effects:
            _raise(error_type, "split bundle parent_after_merge must define final_effects_file")
        _resolve_bundle_file(
            bundle_dir=bundle_dir,
            relative_path=rel_parent_final_effects,
            error_type=error_type,
            label="split bundle parent_after_merge final_effects_file",
        )
        parent_unit_ids = _normalize_covered_unit_ids(
            covered_unit_ids_raw=parent_after_merge_raw.get("covered_unit_ids"),
            known_unit_ids=coverage_unit_ids,
            error_type=error_type,
            label="split bundle parent_after_merge",
        )
        for unit_id in parent_unit_ids:
            owner = "parent_after_merge"
            if unit_id in assignments:
                _raise(
                    error_type,
                    f"coverage unit {unit_id!r} is assigned more than once ({assignments[unit_id]} and {owner})",
                )
            assignments[unit_id] = owner
        parent_after_merge = SplitParentAfterMergeSpec(
            final_effects_file=rel_parent_final_effects,
            covered_unit_ids=parent_unit_ids,
        )

    missing_unit_ids = sorted(unit_id for unit_id in coverage_unit_ids if unit_id not in assignments)
    if missing_unit_ids:
        _raise(
            error_type,
            "split bundle proposal leaves coverage_units unassigned: "
            + ", ".join(missing_unit_ids),
        )

    return (
        bundle_dir,
        SplitProposal(
            version=2,
            coverage_units=tuple(coverage_units),
            children=tuple(children),
            parent_after_merge=parent_after_merge,
        ),
    )
