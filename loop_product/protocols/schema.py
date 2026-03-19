"""Schema loading and validation helpers for LOOP system protocol objects."""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

from jsonschema import Draft202012Validator
from loop_product.runtime_paths import product_schema_path


@lru_cache(maxsize=None)
def load_repo_schema(name: str) -> dict[str, Any]:
    """Load a repo-shipped JSON schema by filename."""

    path = product_schema_path(name)
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


def validate_repo_object(schema_name: str, payload: dict[str, Any]) -> None:
    """Validate a payload against one of the repo-shipped schemas."""

    Draft202012Validator(load_repo_schema(schema_name)).validate(payload)
